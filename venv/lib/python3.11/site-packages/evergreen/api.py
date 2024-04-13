# -*- encoding: utf-8 -*-
"""API for interacting with evergreen."""
from __future__ import absolute_import

import json
import re
import subprocess
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache
from http import HTTPStatus
from json.decoder import JSONDecodeError
from time import time
from typing import Any, Callable, Dict, Generator, Iterable, Iterator, List, Optional, Union, cast

import requests
import structlog
from requests.exceptions import HTTPError
from structlog.stdlib import LoggerFactory
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from evergreen.alias import VariantAlias
from evergreen.api_requests import IssueLinkRequest, MetadataLinkRequest, SlackAttachment
from evergreen.build import Build
from evergreen.commitqueue import CommitQueue
from evergreen.config import (
    DEFAULT_API_SERVER,
    DEFAULT_NETWORK_TIMEOUT_SEC,
    EvgAuth,
    get_auth_from_config,
    read_evergreen_config,
    read_evergreen_from_file,
)
from evergreen.distro import Distro
from evergreen.host import Host
from evergreen.manifest import Manifest
from evergreen.patch import Patch, PatchCreationDetails
from evergreen.performance_results import PerformanceData
from evergreen.project import Project
from evergreen.resource_type_permissions import (
    PermissionableResourceType,
    RemovablePermission,
    ResourceTypePermissions,
)
from evergreen.stats import TaskStats, TestStats
from evergreen.task import Task
from evergreen.task_annotations import TaskAnnotation
from evergreen.task_reliability import TaskReliability
from evergreen.tst import Tst
from evergreen.users_for_role import UsersForRole
from evergreen.util import evergreen_input_to_output, format_evergreen_date, iterate_by_time_window
from evergreen.version import RecentVersions, Requester, Version

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse  # type: ignore


LOGGER = structlog.getLogger(__name__)

CACHE_SIZE = 5000
DEFAULT_LIMIT = 100
MAX_RETRIES = 3
START_WAIT_TIME_SEC = 2
MAX_WAIT_TIME_SEC = 5

EVERGREEN_URL_REGEX = re.compile(r"(https?)://evergreen\..*?(?=\\n)")
EVERGREEN_PATCH_ID_REGEX = re.compile(r"(?<=ID : )\w{24}")


class EvergreenApi(object):
    """Base methods for building API objects."""

    def __init__(
        self,
        api_server: str = DEFAULT_API_SERVER,
        auth: Optional[EvgAuth] = None,
        timeout: Optional[int] = None,
        session: Optional[requests.Session] = None,
        log_on_error: bool = False,
        use_default_logger_factory: bool = True,
    ) -> None:
        """
        Create a _BaseEvergreenApi object.

        :param api_server: URI of Evergreen API server.
        :param auth: EvgAuth object with auth information.
        :param timeout: Time (in sec) to wait before considering a call as failed.
        :param session: Session to use for requests.
        :param log_on_error: Flag to use for error logs.
        :param use_default_logger_factory: Indicate if the module should configure the default logger factory.
        """
        self._timeout = timeout
        self._api_server = api_server
        self._auth = auth
        self._session = session
        self._log_on_error = log_on_error

        if use_default_logger_factory:
            structlog.configure(logger_factory=LoggerFactory())

    @contextmanager
    def with_session(self) -> Generator["EvergreenApi", None, None]:
        """Yield an instance of the API client with a shared session."""
        session = self._create_session()
        evg_api = EvergreenApi(
            self._api_server, self._auth, self._timeout, session, self._log_on_error
        )
        yield evg_api

    @property
    def session(self) -> requests.Session:
        """
        Get the shared session if it exists, else create a new session.

        :return: Session to query the API with.
        """
        if self._session:
            return self._session

        return self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a new session to query the API with."""
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter()
        session.mount(f"{urlparse(self._api_server).scheme}://", adapter)
        auth = self._auth
        if auth:
            session.headers.update({"Api-User": auth.username, "Api-Key": auth.api_key})
        return session

    def _create_url(self, endpoint: str) -> str:
        """
        Format a call to a v2 REST API endpoint.

        :param endpoint: endpoint to call.
        :return: Full url to get endpoint.
        """
        return f"{self._api_server}/rest/v2{endpoint}"

    def _create_plugin_url(self, endpoint: str) -> str:
        """
        Format the a call to a plugin endpoint.

        :param endpoint: endpoint to call.
        :return: Full url to get endpoint.
        """
        return f"{self._api_server}/plugin/json{endpoint}"

    @staticmethod
    def _log_api_call_time(response: requests.Response, start_time: float) -> None:
        """
        Log how long the api call took.

        :param response: Response from API.
        :param start_time: Time the response was started.
        """
        duration = round(time() - start_time, 2)
        if duration > 10:
            LOGGER.info("Request completed.", url=response.request.url, duration=duration)
        else:
            LOGGER.debug("Request completed.", url=response.request.url, duration=duration)

    def _call_api(
        self,
        url: str,
        params: Optional[Dict] = None,
        method: str = "GET",
        data: Optional[str] = None,
    ) -> requests.Response:
        """
        Make a call to the evergreen api.

        :param url: Url of call to make.
        :param params: parameters to pass to api.
        :param method: HTTP method to make call with.
        :param data: Extra data to send to the endpoint.
        :return: response from api server.
        """
        start_time = time()
        LOGGER.debug(
            "Request to be sent",
            url=url,
            params=params,
            timeout=self._timeout,
            data=data,
            method=method,
        )
        response = self.session.request(
            url=url, params=params, timeout=self._timeout, data=data, method=method
        )

        LOGGER.debug(
            "Response received",
            request_url=response.request.url,
            request_method=response.request.method,
            request_body=response.request.body,
            response_status_code=response.status_code,
            response_text=response.text,
        )
        self._log_api_call_time(response, start_time)

        self._raise_for_status(response)
        return response

    def _stream_api(
        self,
        url: str,
        params: Optional[Dict] = None,
        decode_unicode: bool = True,
        chunk_size: Optional[int] = None,
        is_binary: bool = False,
    ) -> Iterable:
        """
        Make a streaming call based on if artifact is binary or nonbinary.

        :param url: url to call
        :param params: url parameters
        :param decode_unicode: determines if we decode as unicode
        :param chunk_size: the size of the chunks to be read
        :param is_binary: is the data being streamed a binary object
        :return: Iterable over the lines of the returned content.
        """
        start_time = time()

        with self.session.get(url=url, params=params, stream=True, timeout=self._timeout) as res:
            self._log_api_call_time(res, start_time)
            if is_binary:
                for line in res.iter_content(chunk_size=chunk_size, decode_unicode=decode_unicode):
                    yield line
            else:
                for line in res.iter_lines(decode_unicode=decode_unicode):
                    yield line

    def _raise_for_status(self, response: requests.Response) -> None:
        """
        Raise an exception with the evergreen message if it exists.

        :param response: response from evergreen api.
        """
        try:
            json_data = response.json()
            if response.status_code >= 400 and "error" in json_data:
                if self._log_on_error:
                    LOGGER.error(
                        "Error found in json",
                        request_url=response.request.url,
                        request_method=response.request.method,
                        request_body=response.request.body,
                        response_status_code=response.status_code,
                        response_text=response.text,
                    )
                raise requests.exceptions.HTTPError(json_data["error"], response=response)
        except JSONDecodeError:
            pass

        response.raise_for_status()

    def _paginate(
        self, url: str, params: Optional[Dict] = None
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Paginate until all results are returned and return a list of all JSON results.

        :param url: url to make request to.
        :param params: parameters to pass to request.
        :return: json list of all results.
        """
        response = self._call_api(url, params)
        json_data = response.json()
        while "next" in response.links:
            if params and "limit" in params and len(json_data) >= params["limit"]:
                break
            response = self._call_api(response.links["next"]["url"])
            if response.json():
                json_data.extend(response.json())

        return json_data

    def _lazy_paginate(self, url: str, params: Optional[Dict] = None) -> Iterable:
        """
        Lazy paginate, the results are returned lazily.

        :param url: URL to query.
        :param params: Params to pass to url.
        :return: A generator to get results from.
        """
        if not params:
            params = {
                "limit": DEFAULT_LIMIT,
            }

        next_url = url
        while True:
            response = self._call_api(next_url, params)
            json_response = response.json()
            if not json_response:
                break
            for result in json_response:
                yield result
            if "next" not in response.links:
                break

            next_url = response.links["next"]["url"]

    def _lazy_paginate_by_date(self, url: str, params: Optional[Dict] = None) -> Iterable:
        """
        Paginate based on date, the results are returned lazily.

        :param url: URL to query.
        :param params: Params to pass to url.
        :return: A generator to get results from.
        """
        if not params:
            params = {
                "limit": DEFAULT_LIMIT,
            }

        while True:
            data = self._call_api(url, params).json()
            if not data:
                break
            for result in data:
                yield result
            params["start_at"] = evergreen_input_to_output(data[-1]["create_time"])

    def all_distros(self) -> List[Distro]:
        """
        Get all distros in evergreen.

        :return: List of all distros in evergreen.
        """
        url = self._create_url("/distros")
        distro_list = self._paginate(url)
        return [Distro(distro, self) for distro in distro_list]  # type: ignore[arg-type]

    def all_hosts(self, status: Optional[str] = None) -> List[Host]:
        """
        Get all hosts in evergreen.

        :param status: Only return hosts with specified status.
        :return: List of all hosts in evergreen.
        """
        params = {}
        if status is not None:
            params["status"] = status

        url = self._create_url("/hosts")
        host_list = self._paginate(url, params)
        return [Host(host, self) for host in host_list]  # type: ignore[arg-type]

    def host_by_id(self, host_id: str) -> Host:
        """
        Get evergreen host by id.

        :param host_id: host ids, 'i-000cba730e92eb85b'
        :return: host document
        """
        url = self._create_url(f"/hosts/{host_id}")
        return Host(self._paginate(url), self)  # type: ignore[arg-type]

    def configure_task(
        self, task_id: str, activated: Optional[bool] = None, priority: Optional[int] = None
    ) -> None:
        """
        Update a task.

        :param task_id: Id of the task to update
        :param activated: If specified, will update the task to specified value True or False
        :param priority: If specified, will update the task's priority to specified number
        """
        url = self._create_url(f"/tasks/{task_id}")
        data: Dict[str, Union[bool, int]] = {}
        if activated is not None:
            data["activated"] = activated
        if priority is not None:
            data["priority"] = priority
        self._call_api(url, data=json.dumps(data), method="PATCH")

    def restart_task(self, task_id: str) -> None:
        """
        Restart a task.

        :param task_id: Id of the task to restart
        """
        url = self._create_url(f"/tasks/{task_id}/restart")
        self._call_api(url, method="POST")

    def abort_task(self, task_id: str) -> None:
        """
        Abort a task.

        :param task_id: Id of the task to abort
        """
        url = self._create_url(f"/tasks/{task_id}/abort")
        self._call_api(url, method="POST")

    def all_projects(self, project_filter_fn: Optional[Callable] = None) -> List[Project]:
        """
        Get all projects in evergreen.

        :param project_filter_fn: function to filter projects, should accept a project_id argument.
        :return: List of all projects in evergreen.
        """
        url = self._create_url("/projects")
        project_list = self._paginate(url)
        projects = [Project(project, self) for project in project_list]  # type: ignore[arg-type]
        if project_filter_fn is not None:
            return [project for project in projects if project_filter_fn(project)]
        return projects

    def project_by_id(self, project_id: str) -> Project:
        """
        Get a project by project_id.

        :param project_id: Id of project to query.
        :return: Project specified.
        """
        url = self._create_url(f"/projects/{project_id}")
        return Project(self._paginate(url), self)  # type: ignore[arg-type]

    def recent_versions_by_project(
        self, project_id: str, params: Optional[Dict] = None
    ) -> RecentVersions:
        """
        Get recent versions created in specified project.

        :param project_id: Id of project to query.
        :param params: parameters to pass to endpoint.
        :return: List of recent versions.
        """
        url = self._create_url(f"/projects/{project_id}/recent_versions")
        resp = self._call_api(url, params)
        return RecentVersions(resp.json(), self)  # type: ignore[arg-type]

    def send_slack_message(
        self, target: str, msg: str, attachments: Optional[List[SlackAttachment]] = None
    ) -> None:
        """
        Send a Slack message to a user or channel in Slack.

        :param target: The slack name of the user or channel to send the message to.
        :param msg: The slack message to send.
        :param attachments: What additional data to send to the specified target in Slack.
        """
        url = self._create_url("/notifications/slack")
        data: Dict[str, Any] = {
            "target": target,
            "msg": msg,
        }
        if attachments is not None:
            data["attachments"] = [
                attachment.dict(exclude_none=True, exclude_unset=True) for attachment in attachments
            ]
        self._call_api(
            url, data=json.dumps(data), method="POST",
        )

    def send_email(
        self,
        recipients: List[str],
        sender: Optional[str] = None,
        subject: Optional[str] = None,
        body: Optional[str] = None,
        is_plain_text: Optional[bool] = None,
        headers: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Send an email to a user.

        :param recipients: Who to send the email to.
        :param sender: Who the email should be sent on behalf of.
        :param subject: The subject of the email to send.
        :param body: What should be in the body of the email.
        :param is_plain_text: If the email is in plain text or not. If true, will be text/plain. text/html otherwise.
        :param headers: What email headers to attach.
        """
        url = self._create_url("/notifications/email")
        data: Dict[str, Any] = {
            "recipients": recipients,
        }
        if sender is not None:
            data["sender"] = sender
        if subject is not None:
            data["subject"] = subject
        if body is not None:
            data["body"] = body
        if is_plain_text is not None:
            data["is_plain_text"] = is_plain_text
        if headers is not None:
            data["headers"] = headers
        self._call_api(
            url, data=json.dumps(data), method="POST",
        )

    def alias_for_version(
        self, version_id: str, alias: str, include_deps: bool = False
    ) -> List[VariantAlias]:
        """
        Get the tasks and variants that an alias would select for an evergreen version.

        :param version_id: Evergreen version to query against.
        :param alias: Alias to query.
        :param include_deps: If true, will also select tasks that are dependencies.
        :return: List of Variant alias details.
        """
        params = {"version": version_id, "alias": alias, "include_deps": include_deps}
        url = self._create_url("/projects/test_alias")
        variant_alias_list = self._paginate(url, params)
        return [
            VariantAlias(cast(Dict[str, Any], variant_alias), self)
            for variant_alias in variant_alias_list
        ]

    def versions_by_project(
        self,
        project_id: str,
        requester: Requester = Requester.GITTER_REQUEST,
        start: Optional[int] = None,
        limit: Optional[int] = None,
        revision_start: Optional[int] = None,
        revision_end: Optional[int] = None,
    ) -> Iterator[Version]:
        """
        Get the versions created in the specified project.

        :param project_id: Id of project to query.
        :param requester: Type of versions to query.
        :param start: Optional. The revision order number to start after, for pagination.
        :param limit: Optional. The number of versions to be returned per page of pagination.
        :param revision_start: Optional. The version order number to start at, for pagination.
        :param revision_end: Optional. The version order number to end at, for pagination.
        :return: Generator of versions.
        """
        url = self._create_url(f"/projects/{project_id}/versions")
        params: Dict[str, Any] = {"requester": requester}
        if start is not None:
            params["start"] = start
        if limit is not None:
            params["limit"] = limit
        if revision_start is not None:
            params["revision_start"] = revision_start
        if revision_end is not None:
            params["revision_end"] = revision_end
        version_list = self._lazy_paginate(url, params)
        return (Version(version, self) for version in version_list)  # type: ignore[arg-type]

    def versions_by_project_time_window(
        self,
        project_id: str,
        before: datetime,
        after: datetime,
        requester: Requester = Requester.GITTER_REQUEST,
        time_attr: str = "create_time",
    ) -> Iterable[Version]:
        """
        Get an iterator over the patches for the given time window.

        :param project_id: Id of project to query.
        :param requester: Type of version to query
        :param before: Return versions earlier than this timestamp.
        :param after: Return versions later than this timestamp.
        :param time_attr: Attributes to use to window timestamps.
        :return: Iterator for the given time window.
        """
        return iterate_by_time_window(
            self.versions_by_project(project_id, requester), before, after, time_attr
        )

    def patches_by_project(self, project_id: str, params: Optional[Dict] = None) -> Iterable[Patch]:
        """
        Get a list of patches for the specified project.

        :param project_id: Id of project to query.
        :param params: parameters to pass to endpoint.
        :return: List of recent patches.
        """
        url = self._create_url(f"/projects/{project_id}/patches")
        patches = self._lazy_paginate_by_date(url, params)
        return (Patch(patch, self) for patch in patches)  # type: ignore[arg-type]

    def update_patch_status(
        self, patch_id: str, activated: Optional[bool] = None, priority: Optional[int] = None
    ) -> None:
        """
        Update a patch and set its status.

        :param patch_id: Id of the patch to update
        :param activated: If specified, will update the patch to specified value True or False
        :param priority: If specified, will update the patch's priority to specified number
        """
        url = self._create_url(f"/patches/{patch_id}")
        data: Dict[str, Union[bool, int]] = {}
        if activated is not None:
            data["activated"] = activated
        if priority is not None:
            data["priority"] = priority
        self._call_api(url, data=json.dumps(data), method="PATCH")

    def configure_patch(
        self,
        patch_id: str,
        variants: List[Dict[str, Union[str, List[str]]]],
        description: Optional[str] = None,
    ) -> None:
        """
        Update a patch.

        :param patch_id: Id of the patch to update
        :param variants: list of objects with keys "id" who's value is the variant ID, and key "tasks"
            with value of a list of task names to configure for specified variant. See the documentation for more details
            https://github.com/evergreen-ci/evergreen/wiki/REST-V2-Usage#configureschedule-a-patch
        :param description: If specified, will update the patch's description with the string provided
        """
        url = self._create_url(f"/patches/{patch_id}/configure")
        data: Dict[str, Union[List, str]] = {}
        if variants is not None:
            data["variants"] = variants
        if description is not None:
            data["description"] = description

        self._call_api(url, data=json.dumps(data), method="POST")

    def patches_by_project_time_window(
        self,
        project_id: str,
        before: datetime,
        after: datetime,
        params: Optional[Dict] = None,
        time_attr: str = "create_time",
    ) -> Iterable[Patch]:
        """
        Get an iterator over the patches for the given time window.

        :param project_id: Id of project to query.
        :param params: Parameters to pass to endpoint.
        :param before: Return patches earlier than this timestamp
        :param after: Return patches later than this timestamp.
        :param time_attr: Attributes to use to window timestamps.
        :return: Iterator for the given time window.
        """
        return iterate_by_time_window(
            self.patches_by_project(project_id, params), before, after, time_attr
        )

    def patches_by_user(
        self, user_id: str, start_at: Optional[datetime] = None, limit: Optional[int] = None
    ) -> Iterable[Patch]:
        """
        Get an iterable of recent patches by the given user.

        :param user_id: Id of user to query.
        :param start_at: If specified, query starting at the given date.
        :param limit: If specified, limit the output per page.
        """
        params: Dict[str, Any] = {}
        if start_at is not None:
            params["start_at"] = start_at
        if limit is not None:
            params["limit"] = limit
        url = self._create_url(f"/users/{user_id}/patches")
        return (Patch(patch, self) for patch in self._lazy_paginate(url, params))

    def commit_queue_for_project(self, project_id: str) -> CommitQueue:
        """
        Get the current commit queue for the specified project.

        :param project_id: Id of project to query.
        :return: Current commit queue for project.
        """
        url = self._create_url(f"/commit_queue/{project_id}")
        return CommitQueue(self._paginate(url), self)  # type: ignore[arg-type]

    def test_stats_by_project(
        self,
        project_id: str,
        after_date: datetime,
        before_date: datetime,
        group_num_days: Optional[int] = None,
        requesters: Optional[Requester] = None,
        tests: Optional[List[str]] = None,
        tasks: Optional[List[str]] = None,
        variants: Optional[List[str]] = None,
        distros: Optional[List[str]] = None,
        group_by: Optional[str] = None,
        sort: Optional[str] = None,
    ) -> List[TestStats]:
        """
        Get a patch by patch id.

        :param project_id: Id of patch to query for.
        :param after_date: Collect stats after this date.
        :param before_date: Collect stats before this date.
        :param group_num_days: Aggregate statistics to this size.
        :param requesters: Filter by requestors (mainline, patch, trigger, or adhoc).
        :param tests: Only include specified tests.
        :param tasks: Only include specified tasks.
        :param variants: Only include specified variants.
        :param distros: Only include specified distros.
        :param group_by: How to group results (test_task_variant, test_task, or test)
        :param sort: How to sort results (earliest or latest).
        :return: Patch queried for.
        """
        params: Dict[str, Any] = {
            "after_date": format_evergreen_date(after_date),
            "before_date": format_evergreen_date(before_date),
        }
        if group_num_days is not None:
            params["group_num_days"] = group_num_days
        if requesters is not None:
            params["requesters"] = requesters.stats_value()
        if tests is not None:
            params["tests"] = tests
        if tasks is not None:
            params["tasks"] = tasks
        if variants is not None:
            params["variants"] = variants
        if distros is not None:
            params["distros"] = distros
        if group_by is not None:
            params["group_by"] = group_by
        if sort is not None:
            params["sort"] = sort
        url = self._create_url(f"/projects/{project_id}/test_stats")
        test_stats_list = self._paginate(url, params)
        return [TestStats(test_stat, self) for test_stat in test_stats_list]  # type: ignore[arg-type]

    def tasks_by_project(self, project_id: str, statuses: Optional[List[str]] = None) -> List[Task]:
        """
        Get all the tasks for a project.

        :param project_id: The project's id.
        :param statuses: the types of statuses to get tasks for.
        :return: The list of matching tasks.
        """
        url = self._create_url(f"/projects/{project_id}/versions/tasks")
        params = {"status": statuses} if statuses is not None else None
        return [Task(json, self) for json in self._paginate(url, params)]  # type: ignore[arg-type]

    def tasks_by_project_and_commit(
        self, project_id: str, commit_hash: str, params: Optional[Dict] = None
    ) -> List[Task]:
        """
        Get all the tasks for a revision in specified project.

        :param project_id: Project id associated with the revision
        :param commit_hash: Commit to get tasks for
        :param params: Dictionary of parameters to pass to query.
        :return: The list of matching tasks.
        """
        url = self._create_url(f"/projects/{project_id}/revisions/{commit_hash}/tasks")
        return [Task(json, self) for json in self._paginate(url, params)]  # type: ignore[arg-type]

    def tasks_by_project_and_name(
        self,
        project_id: str,
        task_name: str,
        build_variant: Optional[str] = None,
        num_versions: Optional[int] = None,
        start_at: Optional[int] = None,
    ) -> List[Task]:
        """
        Get all the tasks for a project by task name.

        :param project_id: Id of project to query.
        :param task_name: Name of task to query for.
        :param build_variant: Only include tasks that have run on this build variant.
        :param num_versions: The number of latest versions to be searched. Defaults to 20.
        :param start_at: The version order number to start returning results after.
        """
        data: Dict[str, Any] = {}
        if build_variant is not None:
            data["build_variant"] = build_variant
        if num_versions is not None:
            data["num_versions"] = num_versions
        if start_at is not None:
            data["start_at"] = start_at
        url = self._create_url(f"/projects/{project_id}/tasks/{task_name}")
        return [
            Task(task_json, self) for task_json in self._call_api(url, data=json.dumps(data)).json()
        ]

    def task_stats_by_project(
        self,
        project_id: str,
        after_date: datetime,
        before_date: datetime,
        group_num_days: Optional[int] = None,
        requesters: Optional[Requester] = None,
        tasks: Optional[List[str]] = None,
        variants: Optional[List[str]] = None,
        distros: Optional[List[str]] = None,
        group_by: Optional[str] = None,
        sort: Optional[str] = None,
    ) -> List[TaskStats]:
        """
        Get task stats by project id.

        :param project_id: Id of patch to query for.
        :param after_date: Collect stats after this date.
        :param before_date: Collect stats before this date.
        :param group_num_days: Aggregate statistics to this size.
        :param requesters: Filter by requestors (mainline, patch, trigger, or adhoc).
        :param tasks: Only include specified tasks.
        :param variants: Only include specified variants.
        :param distros: Only include specified distros.
        :param group_by: How to group results (test_task_variant, test_task, or test)
        :param sort: How to sort results (earliest or latest).
        :return: Patch queried for.
        """
        params: Dict[str, Any] = {
            "after_date": format_evergreen_date(after_date),
            "before_date": format_evergreen_date(before_date),
        }
        if group_num_days is not None:
            params["group_num_days"] = group_num_days
        if requesters is not None:
            params["requesters"] = requesters.stats_value()
        if tasks is not None:
            params["tasks"] = tasks
        if variants is not None:
            params["variants"] = variants
        if distros is not None:
            params["distros"] = distros
        if group_by is not None:
            params["group_by"] = group_by
        if sort is not None:
            params["sort"] = sort
        url = self._create_url(f"/projects/{project_id}/task_stats")
        task_stats_list = self._paginate(url, params)
        return [TaskStats(task_stat, self) for task_stat in task_stats_list]  # type: ignore[arg-type]

    def task_reliability_by_project(
        self,
        project_id: str,
        after_date: Optional[datetime] = None,
        before_date: Optional[datetime] = None,
        group_num_days: Optional[int] = None,
        requesters: Optional[Requester] = None,
        tasks: Optional[List[str]] = None,
        variants: Optional[List[str]] = None,
        distros: Optional[List[str]] = None,
        group_by: Optional[str] = None,
        sort: Optional[str] = None,
    ) -> List[TaskReliability]:
        """
        Get task reliability scores.

        :param project_id: Id of patch to query for.
        :param after_date: Collect stats after this date.
        :param before_date: Collect stats before this date, defaults to nothing.
        :param group_num_days: Aggregate statistics to this size.
        :param requesters: Filter by requesters (mainline, patch, trigger, or adhoc).
        :param tasks: Only include specified tasks.
        :param variants: Only include specified variants.
        :param distros: Only include specified distros.
        :param group_by: How to group results (test_task_variant, test_task, or test)
        :param sort: How to sort results (earliest or latest).
        :return: Patch queried for.
        """
        params: Dict[str, Any] = {}
        if after_date is not None:
            params["after_date"] = format_evergreen_date(after_date)
        if before_date is not None:
            params["before_date"] = format_evergreen_date(before_date)
        if group_num_days is not None:
            params["group_num_days"] = group_num_days
        if requesters is not None:
            params["requesters"] = requesters.stats_value()
        if tasks is not None:
            params["tasks"] = tasks
        if variants is not None:
            params["variants"] = variants
        if distros is not None:
            params["distros"] = distros
        if group_by is not None:
            params["group_by"] = group_by
        if sort is not None:
            params["sort"] = sort

        url = self._create_url(f"/projects/{project_id}/task_reliability")
        task_reliability_scores = self._paginate(url, params)
        return [
            TaskReliability(task_reliability, self) for task_reliability in task_reliability_scores  # type: ignore[arg-type]
        ]

    def build_by_id(self, build_id: str) -> Build:
        """
        Get a build by id.

        :param build_id: build id to query.
        :return: Build queried for.
        """
        url = self._create_url(f"/builds/{build_id}")
        return Build(self._paginate(url), self)  # type: ignore[arg-type]

    def tasks_by_build(
        self, build_id: str, fetch_all_executions: Optional[bool] = None
    ) -> List[Task]:
        """
        Get all tasks for a given build.

        :param build_id: build_id to query.
        :param fetch_all_executions: Fetch all executions for a given task.
        :return: List of tasks for the specified build.
        """
        params = {}
        if fetch_all_executions:
            params["fetch_all_executions"] = 1

        url = self._create_url(f"/builds/{build_id}/tasks")
        task_list = self._paginate(url, params)
        return [Task(task, self) for task in task_list]  # type: ignore[arg-type]

    def version_by_id(self, version_id: str) -> Version:
        """
        Get version by version id.

        :param version_id: Id of version to query.
        :return: Version queried for.
        """
        url = self._create_url(f"/versions/{version_id}")
        return Version(self._paginate(url), self)  # type: ignore[arg-type]

    def builds_by_version(self, version_id: str, params: Optional[Dict] = None) -> List[Build]:
        """
        Get all builds for a given Evergreen version_id.

        :param version_id: Version Id to query for.
        :param params: Dictionary of parameters to pass to query.
        :return: List of builds for the specified version.
        """
        url = self._create_url(f"/versions/{version_id}/builds")
        build_list = self._paginate(url, params)
        return [Build(build, self) for build in build_list]  # type: ignore[arg-type]

    def patch_by_id(self, patch_id: str, params: Optional[Dict] = None) -> Patch:
        """
        Get a patch by patch id.

        :param patch_id: Id of patch to query for.
        :param params: Parameters to pass to endpoint.
        :return: Patch queried for.
        """
        url = self._create_url(f"/patches/{patch_id}")
        return Patch(self._call_api(url, params).json(), self)  # type: ignore[arg-type]

    def get_patch_diff(self, patch_id: str) -> str:
        """
        Get the diff for a given patch.

        :param patch_id: The id of the patch to request the diff for.
        :return: The diff of the patch represented as plain text.
        """
        url = self._create_url(f"/patches/{patch_id}/raw")
        return self._call_api(url, method="GET").text

    def _execute_patch_file_command(
        self, command: str, author: Optional[str] = None
    ) -> PatchCreationDetails:
        """
        Execute a patch file command.

        :param command: The command
        :param author: An author to attribute the patch to.
        :raises Exception: Exception if command has unexpected output
        :return: The patch creation details.
        """
        if author is not None:
            command = f"{command} --author {author}"

        process = subprocess.run(command, shell=True, capture_output=True)
        output = str(process.stderr)

        url_match = EVERGREEN_URL_REGEX.search(output)
        id_match = EVERGREEN_PATCH_ID_REGEX.search(output)

        if url_match is None or id_match is None:
            raise Exception(
                f"Unable to parse URL or ID from command output: {output}. \nExecuted command: {command}"
            )

        return PatchCreationDetails(url=url_match.group(0), id=id_match.group(0))

    def patch_from_diff(
        self,
        diff_file_path: str,
        params: dict[str, str],
        base: str,
        task: str,
        project: str,
        description: str,
        variant: str,
        author: Optional[str] = None,
    ) -> PatchCreationDetails:
        """
        Start a patch build based on a patch.

        :param diff_file_path: The path to the diff.
        :param params: The params to pass to the build.
        :param base: The build's base commit.
        :param task: The task(s) to run.
        :param project: The project to start the build for.
        :param description: A description of the build.
        :param variant: The variant(s) to build against.
        :param author: The author to attribute for the build.
        :raises Exception: If a build URL is not produced we raise an exception with the output included.
        :return: The patch creation details.
        """
        unpacked_params = " ".join([f"--param '{key}={value}'" for key, value in params.items()])
        command = f"evergreen patch-file --diff-file {diff_file_path} --description '{description}' --base {base} --tasks {task} --variants {variant} --project {project} {unpacked_params} -y -f"
        return self._execute_patch_file_command(command, author)

    def patch_from_patch_id(
        self,
        patch_id: str,
        params: dict[str, str],
        task: str,
        project: str,
        description: str,
        variant: str,
        author: Optional[str] = None,
    ) -> PatchCreationDetails:
        """
        Start a patch build based on a diff.

        :param patch_id: The patch_id to base this build on.
        :param params: The params to pass to the build.
        :param base: The build's base commit.
        :param task: The task(s) to run.
        :param project: The project to start the build for.
        :param description: A description of the build.
        :param variant: The variant(s) to build against.
        :param author: The author to attribute for the build.
        :raises Exception: If a build URL is not produced we raise an exception with the output included.
        :return: The patch creation details.
        """
        unpacked_params = " ".join([f"--param '{key}={value}'" for key, value in params.items()])
        command = f"evergreen patch-file --diff-patchId {patch_id} --description '{description}' --tasks {task} --variants {variant} --project {project} {unpacked_params} -y -f"
        return self._execute_patch_file_command(command, author)

    def task_by_id(
        self,
        task_id: str,
        fetch_all_executions: Optional[bool] = None,
        execution: Optional[int] = None,
    ) -> Task:
        """
        Get a task by task_id.

        :param task_id: Id of task to query for.
        :param execution: Will query for a specific task execution
        :param fetch_all_executions: Should all executions of the task be fetched.
        :return: Task queried for.
        """
        params: Dict[str, Any] = {}
        if execution is not None:
            params["execution"] = execution
        if fetch_all_executions is not None:
            params["fetch_all_executions"] = fetch_all_executions
        url = self._create_url(f"/tasks/{task_id}")
        return Task(self._call_api(url, params).json(), self)  # type: ignore[arg-type]

    def tests_by_task(
        self, task_id: str, status: Optional[str] = None, execution: Optional[int] = None
    ) -> List[Tst]:
        """
        Get all tests for a given task.

        :param task_id: Id of task to query for.
        :param status: Limit results to given status.
        :param execution: Retrieve the specified task execution (defaults to 0).
        :return: List of tests for the specified task.
        """
        params: Dict[str, Any] = {}
        if status is not None:
            params["status"] = status
        if execution is not None:
            params["execution"] = execution
        url = self._create_url(f"/tasks/{task_id}/tests")
        return [Tst(test, self) for test in self._paginate(url, params)]  # type: ignore[arg-type]

    def single_test_by_task_and_test_file(self, task_id: str, test_file: str) -> List[Tst]:
        """
        Get a test for a given task.

        :param task_id: Id of task to query for.
        :param test_file: the name of the test_file of the test.
        :return: the test for the specified task.
        """
        url = self._create_url(f"/tasks/{task_id}/tests")
        param = {"test_name": test_file}
        return [Tst(test, self) for test in self._call_api(url, params=param).json()]

    def num_of_tests_by_task(self, task_id: str) -> int:
        """
        Get the number of tests that ran as part of the given task.

        :param task_id: Id of task to query for.
        :return: Number of tests for the specified task.
        """
        url = self._create_url(f"/tasks/{task_id}/tests/count")
        return int(self._call_api(url).text)

    def manifest_for_task(self, task_id: str) -> Optional[Manifest]:
        """
        Get the manifest for the given task.

        :param task_id: Task Id fo query.
        :return: Manifest for the given task.
        """
        url = self._create_url(f"/tasks/{task_id}/manifest")

        manifest: Optional[Manifest] = None
        try:
            manifest = Manifest(self._call_api(url).json(), self)  # type: ignore[arg-type]
        except HTTPError as e:
            if e.response.status_code != HTTPStatus.NOT_FOUND:
                raise e

        return manifest

    def get_task_annotation(
        self,
        task_id: str,
        execution: Optional[int] = None,
        fetch_all_executions: Optional[bool] = None,
    ) -> List[TaskAnnotation]:
        """
        Get the task annotations for the given task.

        :param task_id: Id of task to query.
        :param execution: Execution number of task to query (defaults to latest).
        :param fetch_all_executions: Get annotations for all executions of this task.
        :return: The task annotations for the given task, if any exists.
        """
        if execution is not None and fetch_all_executions is not None:
            raise ValueError("'execution' and 'fetch_all_executions' are mutually-exclusive")

        url = self._create_url(f"/tasks/{task_id}/annotations")
        params: Dict[str, Any] = {}
        if execution is not None:
            params["execution"] = execution
        if fetch_all_executions is not None:
            params["fetch_all_executions"] = fetch_all_executions

        response = self._call_api(url, params)
        if response.text.strip() == "null":
            return []
        return [TaskAnnotation(annotation, self) for annotation in response.json()]

    def file_ticket_for_task(
        self, task_id: str, execution: int, ticket_link: str, ticket_key: str
    ) -> None:
        """
        Update an Evergreen task with information about a ticket created from it.

        :param task_id: The id of the task to update.
        :param execution: The execution of the task to update.
        :param ticket_link: The url link to the created ticket.
        :param ticket_key: The key of the created ticket.
        """
        url = self._create_url(f"/tasks/{task_id}/created_ticket")
        request: Dict[str, Any] = {"url": ticket_link, "issue_key": ticket_key}
        params = {
            "execution": execution,
        }
        self._call_api(url, method="PUT", data=json.dumps(request), params=params)

    def annotate_task(
        self,
        task_id: str,
        execution: Optional[int] = None,
        message: Optional[str] = None,
        issues: Optional[List[IssueLinkRequest]] = None,
        suspected_issues: Optional[List[IssueLinkRequest]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        metadata_links: Optional[List[MetadataLinkRequest]] = None,
    ) -> None:
        """
        Annotate the specified task.

        :param task_id: ID of task to annotate.
        :param execution: Execution number of task to annotate (default to latest).
        :param message: Message to add to the annotations.
        :param issues: Issues to attach to the annotation.
        :param suspected_issues: Suspected issues to add to the annotation.
        :param metadata: Extra metadata to add to the issue.
        :param metadata_links: Metadata link to add to the annotation.
        """
        url = self._create_url(f"/tasks/{task_id}/annotation")
        request: Dict[str, Any] = {
            "task_id": task_id,
        }

        if execution is not None:
            request["task_execution"] = execution

        if message is not None:
            request["note"] = {"message": message}

        if issues is not None:
            request["issues"] = [issue.as_dict() for issue in issues]

        if suspected_issues is not None:
            request["suspected_issues"] = [issue.as_dict() for issue in suspected_issues]

        if metadata is not None:
            request["metadata"] = metadata

        if metadata_links is not None:
            request["metadata_links"] = [link._asdict() for link in metadata_links]

        self._call_api(url, method="PUT", data=json.dumps(request))

    def performance_results_by_task(self, task_id: str) -> PerformanceData:
        """
        Get the 'perf.json' performance results for a given task_id.

        :param task_id: Id of task to query for.
        :return: Contents of 'perf.json'
        """
        url = self._create_plugin_url(f"/task/{task_id}/perf")
        return PerformanceData(self._paginate(url), self)  # type: ignore[arg-type]

    def performance_results_by_task_name(
        self, task_id: str, task_name: str
    ) -> List[PerformanceData]:
        """
        Get the 'perf.json' performance results for a given task_id and task_name.

        :param task_id: Id of task to query for.
        :param task_name: Name of task to query for.
        :return: Contents of 'perf.json'
        """
        url = f"{self._api_server}/api/2/task/{task_id}/json/history/{task_name}/perf"
        return [PerformanceData(result, self) for result in self._paginate(url)]  # type: ignore[arg-type]

    def json_by_task(self, task_id: str, json_key: str) -> Dict[str, Any]:
        """
        Get the json reported for task {task_id} using the key {json_key}.

        :param task_id: Id of task to query for.
        :param json_key: The key that json was published under, e.g. "perf".
        :return: The json published for that task.
        """
        url = self._create_plugin_url(f"/task/{task_id}/{json_key}")
        return cast(Dict[str, Any], self._paginate(url))

    def json_history_for_task(
        self, task_id: str, task_name: str, json_key: str
    ) -> List[Dict[str, Any]]:
        """
        Get the history of json reported for task {task_id} using the key {json_key}.

        :param task_id: Id of task to query for.
        :param task_name: Name of task to query for.
        :param json_key: The key that json was published under, e.g. "perf".
        :return: A chronological list of json published for that task.
        """
        url = f"{self._api_server}/api/2/task/{task_id}/json/history/{task_name}/{json_key}"
        return cast(List[Dict[str, Any]], self._paginate(url))

    def _create_old_url(self, endpoint: str) -> str:
        """
        Build a url for an pre-v2 endpoint.

        :param endpoint: endpoint to build url for.
        :return: An string pointing to the given endpoint.
        """
        return f"{self._api_server}/{endpoint}"

    def manifest(self, project_id: str, revision: str) -> Manifest:
        """
        Get the manifest for the given revision.

        :param project_id: Project the revision belongs to.
        :param revision: Revision to get manifest of.
        :return: Manifest of the given revision of the given project.
        """
        url = self._create_old_url(f"plugin/manifest/get/{project_id}/{revision}")
        return Manifest(self._call_api(url).json(), self)  # type: ignore[arg-type]

    def retrieve_task_log(self, log_url: str, raw: bool = False) -> str:
        """
        Get the request log file from a task.

        :param log_url: URL of log to retrieve.
        :param raw: Retrieve the raw version of the log
        :return: Contents of specified log file.
        """
        params = {}
        if raw:
            params["text"] = "true"
        return self._call_api(log_url, params=params).text

    def stream_log(self, log_url: str) -> Iterable:
        """
        Stream the given log url as a python generator.

        :param log_url: URL of log file to stream.
        :return: Iterable for contents of log_url.
        """
        params = {"text": "true"}
        return self._stream_api(log_url, params)

    def permissions_for_user(self, user_id: str) -> List[ResourceTypePermissions]:
        """
        Get the permissions a user has on evergreen resources.

        :param user_id: Id of the user whose permissions to get.
        :return: List of permissions the user has.
        """
        url = self._create_url(f"/users/{user_id}/permissions")
        raw_permissions = self._call_api(url).json()
        return [ResourceTypePermissions(r, self) for r in raw_permissions]

    def give_permissions_to_user(
        self,
        user_id: str,
        resource_type: PermissionableResourceType,
        resources: List[str],
        permissions: Dict[str, int],
    ) -> None:
        """
        Grant a user permissions to evergreen resources.

        :param user_id: Id of the user to give permissions to.
        :param resource_type: An evergreen resource type that supports permissions.
        :param resources: A list of evergreen resources of type `resource_type`.
        :param permissions: Permissions to grant.
                            E.g. - [{"project_tasks": 30, "project_patches": 10}]
        """
        url = self._create_url(f"/users/{user_id}/permissions")
        payload = {
            "resource_type": resource_type,
            "resources": resources,
            "permissions": permissions,
        }
        self._call_api(url, method="POST", data=json.dumps(payload))

    def give_roles_to_user(self, user_id: str, roles: List[str], create_user: bool = False) -> None:
        """
        Add the specified role to the specified user.

        :param user_id: Id of the user to give the roles to.
        :param roles: A list of roles to give to the user.
        :param create_user: If true, will also create a user document for the user.
        """
        url = self._create_url(f"/users/{user_id}/roles")
        payload = {
            "user_id": user_id,
            "roles": roles,
            "create_user": create_user,
        }
        self._call_api(url, method="POST", data=json.dumps(payload))

    def delete_user_permissions(
        self, user_id: str, resource_type: RemovablePermission, resource_id: Optional[str] = None
    ) -> None:
        """
        Delete all permissions of a given type for a user.

        :param user_id: Id of the user whose permissions to remove.
        :param resource_type: A permission that can be removed.
        :param resource_id: Resource id for which to delete permissions. Required unless
                            deleting all permissions.
        """
        url = self._create_url(f"/users/{user_id}/permissions")
        payload = {"resource_type": resource_type.value}
        if resource_id is not None:
            payload["resource_id"] = resource_id
        self._call_api(url, method="DELETE", data=json.dumps(payload))

    def get_users_for_role(self, role: str) -> UsersForRole:
        """
        Get a list of users having an evergreen role.

        :param role: Role to fetch users for.
        """
        url = self._create_url(f"/roles/{role}/users")
        return UsersForRole(self._call_api(url, method="GET").json(), self)

    def all_user_permissions_for_resource(
        self, resource_id: str, resource_type: PermissionableResourceType
    ) -> Dict[str, Dict[str, int]]:
        """
        Get all users with their permissions to the resource.

        The returned dict has the following structure -

        .. code-block:: json

            {
              "username_1": {
                 "project_tasks": 30,
                 "project_patches": 10
                },
              "username_2": {
                 "project_settings": 20,
                 "project_patches": 10
              }
            }

        :param resource_id: Id of the resource to get users for.
        :param resource_type: Resource type of the resource.
        :return: A dict containing user to permissions mappings.
        """
        url = self._create_url("/users/permissions")
        return self._call_api(
            url, data=json.dumps({"resource_id": resource_id, "resource_type": resource_type})
        ).json()

    @classmethod
    def get_api(
        cls,
        auth: Optional[EvgAuth] = None,
        use_config_file: bool = False,
        config_file: Optional[str] = None,
        timeout: Optional[int] = DEFAULT_NETWORK_TIMEOUT_SEC,
        log_on_error: bool = False,
    ) -> "EvergreenApi":
        """
        Get an evergreen api instance based on config file settings.

        :param auth: EvgAuth with authentication to use.
        :param use_config_file: attempt to read auth from default config file.
        :param config_file: config file with authentication information.
        :param timeout: Network timeout.
        :return: EvergreenApi instance.
        :param log_on_error: Flag to use for error logs.
        """
        kwargs = EvergreenApi._setup_kwargs(
            timeout=timeout,
            auth=auth,
            use_config_file=use_config_file,
            config_file=config_file,
            log_on_error=log_on_error,
        )
        return cls(**kwargs)

    @staticmethod
    def _setup_kwargs(
        auth: Optional[EvgAuth] = None,
        use_config_file: bool = False,
        config_file: Optional[str] = None,
        timeout: Optional[int] = DEFAULT_NETWORK_TIMEOUT_SEC,
        log_on_error: bool = False,
    ) -> Dict:
        kwargs = {"auth": auth, "timeout": timeout, "log_on_error": log_on_error}
        config = None
        if use_config_file:
            config = read_evergreen_config()
            if config is None:
                raise FileNotFoundError("The Evergreen config file cannot be found.")
        elif config_file is not None:
            config = read_evergreen_from_file(config_file)

        if config is not None:
            auth = get_auth_from_config(config)
            if auth:
                kwargs["auth"] = auth

            # If there is a value for api_server_host, then use it.
            if "evergreen" in config and config["evergreen"].get("api_server_host", None):
                kwargs["api_server"] = config["evergreen"]["api_server_host"]

        return kwargs


class CachedEvergreenApi(EvergreenApi):
    """Access to the Evergreen API server that caches certain calls."""

    def __init__(
        self,
        api_server: str = DEFAULT_API_SERVER,
        auth: Optional[EvgAuth] = None,
        timeout: Optional[int] = None,
        log_on_error: bool = False,
    ) -> None:
        """Create an Evergreen Api object."""
        super(CachedEvergreenApi, self).__init__(
            api_server, auth, timeout, log_on_error=log_on_error
        )

    @lru_cache(maxsize=CACHE_SIZE)
    def build_by_id(self, build_id: str) -> Build:  # type: ignore[override]
        """
        Get a build by id.

        :param build_id: build id to query.
        :return: Build queried for.
        """
        return super(CachedEvergreenApi, self).build_by_id(build_id)

    @lru_cache(maxsize=CACHE_SIZE)
    def version_by_id(self, version_id: str) -> Version:  # type: ignore[override]
        """
        Get version by version id.

        :param version_id: Id of version to query.
        :return: Version queried for.
        """
        return super(CachedEvergreenApi, self).version_by_id(version_id)

    def clear_caches(self) -> None:
        """Clear the cache."""
        cached_functions = [
            self.build_by_id,
            self.version_by_id,
        ]
        for fn in cached_functions:
            fn.cache_clear()  # type: ignore[attr-defined]


class RetryingEvergreenApi(EvergreenApi):
    """An Evergreen Api that retries failed calls."""

    def __init__(
        self,
        api_server: str = DEFAULT_API_SERVER,
        auth: Optional[EvgAuth] = None,
        timeout: Optional[int] = None,
        log_on_error: bool = False,
    ) -> None:
        """Create an Evergreen Api object."""
        super(RetryingEvergreenApi, self).__init__(
            api_server, auth, timeout, log_on_error=log_on_error
        )

    @retry(
        retry=retry_if_exception_type(  # type: ignore[no-untyped-call]
            (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,)
        ),
        stop=stop_after_attempt(MAX_RETRIES),  # type: ignore[no-untyped-call]
        wait=wait_exponential(multiplier=1, min=START_WAIT_TIME_SEC, max=MAX_WAIT_TIME_SEC),  # type: ignore[no-untyped-call]
        reraise=True,
    )
    def _call_api(
        self,
        url: str,
        params: Optional[Dict] = None,
        method: str = "GET",
        data: Optional[str] = None,
    ) -> requests.Response:
        """
        Call into the evergreen api.

        :param url: Url to call.
        :param params: Parameters to pass to api.
        :param method: HTTP method to make call with.
        :param data: Extra data to send to the endpoint.
        :return: Result from calling API.
        """
        return super(RetryingEvergreenApi, self)._call_api(url, params, method, data)
