# -*- encoding: utf-8 -*-
"""Task representation of evergreen."""
from __future__ import absolute_import

from datetime import timedelta
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from evergreen.api_requests import IssueLinkRequest, MetadataLinkRequest
from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_datetime_attrib
from evergreen.manifest import Manifest
from evergreen.task_annotations import TaskAnnotation

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi
    from evergreen.tst import Tst  # noqa: F401

EVG_FAILED_STATUS = "failed"
EVG_SUCCESS_STATUS = "success"
EVG_SETUP_FAILURE_STATUS_TYPE = "setup"
EVG_SYSTEM_FAILURE_STATUS = "system"
EVG_UNDISPATCHED_STATUS = "undispatched"
EVG_TEST_STATUS_TYPE = "test"

_EVG_DATE_FIELDS_IN_TASK = frozenset(
    ["create_time", "dispatch_time", "finish_time", "ingest_time", "scheduled_time", "start_time"]
)

_BINARY_TYPES = ["application"]


class Artifact(_BaseEvergreenObject):
    """Representation of a task artifact from evergreen."""

    name = evg_attrib("name")
    url = evg_attrib("url")
    visibility = evg_attrib("visibility")
    ignore_for_fetch = evg_attrib("ignore_for_fetch")
    content_type = evg_attrib("content_type")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of an evergreen task artifact."""
        super(Artifact, self).__init__(json, api)

    def stream(
        self,
        decode_unicode: bool = True,
        chunk_size: Optional[int] = None,
        is_binary: Optional[bool] = None,
    ) -> Iterable[str]:
        """
        Retrieve an iterator of the streamed contents of this artifact.

        :param decode_unicode: determines if we decode as unicode
        :param chunk_size: the size of the chunks to be read
        :param is_binary: explicit variable, overrides information from content type
        :return: Iterable to stream contents of artifact.
        """
        if is_binary is None:
            is_binary = self._is_binary()

        return self._api._stream_api(
            self.url, decode_unicode=decode_unicode, chunk_size=chunk_size, is_binary=is_binary,
        )

    def _is_binary(self) -> bool:
        """Determine if an artifact is binary based on content_type."""
        _type, subtype = self.content_type.split("/")

        if _type in _BINARY_TYPES:
            return True
        else:
            return False


class StatusScore(IntEnum):
    """Integer score of the task status."""

    SUCCESS = 1
    FAILURE = 2
    FAILURE_SYSTEM = 3
    FAILURE_TIMEOUT = 4
    UNDISPATCHED = 5

    @classmethod
    def get_task_status_score(cls, task: "Task") -> "StatusScore":
        """
        Retrieve the status score based on the task status.

        :return: Status score.
        """
        if task.is_success():
            return StatusScore.SUCCESS
        if task.is_undispatched():
            return StatusScore.UNDISPATCHED
        if task.is_timeout():
            return StatusScore.FAILURE_TIMEOUT
        if task.is_system_failure():
            return StatusScore.FAILURE_SYSTEM
        return StatusScore.FAILURE


class OomTrackerInfo(_BaseEvergreenObject):
    """Representation of a task's OOM Tracker Info from evergreen."""

    detected = evg_attrib("detected")
    pids = evg_attrib("pids")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of an evergreen task OOM Tracker Info."""
        super(OomTrackerInfo, self).__init__(json, api)


class StatusDetails(_BaseEvergreenObject):
    """Representation of a task status details from evergreen."""

    status = evg_attrib("status")
    type = evg_attrib("type")
    desc = evg_attrib("desc")
    timed_out = evg_attrib("timed_out")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of an evergreen task status details."""
        super(StatusDetails, self).__init__(json, api)

    @property
    def oom_tracker_info(self) -> OomTrackerInfo:
        """
        Retrieve the OOM tracker info from the status details for the given task.

        :return: OOM Tracker Info.
        """
        return OomTrackerInfo(self.json["oom_tracker_info"], self._api)


class Task(_BaseEvergreenObject):
    """Representation of an Evergreen task."""

    activated = evg_attrib("activated")
    activated_by = evg_attrib("activated_by")
    build_id = evg_attrib("build_id")
    build_variant = evg_attrib("build_variant")
    build_variant_display_name = evg_attrib("build_variant_display_name")
    create_time = evg_datetime_attrib("create_time")
    depends_on = evg_attrib("depends_on")
    dispatch_time = evg_datetime_attrib("dispatch_time")
    display_name = evg_attrib("display_name")
    display_only = evg_attrib("display_only")
    display_status = evg_attrib("display_status")
    distro_id = evg_attrib("distro_id")
    est_wait_to_start_ms = evg_attrib("est_wait_to_start_ms")
    execution = evg_attrib("execution")
    execution_tasks = evg_attrib("execution_tasks")
    expected_duration_ms = evg_attrib("expected_duration_ms")
    finish_time = evg_datetime_attrib("finish_time")
    generate_task = evg_attrib("generate_task")
    generated_by = evg_attrib("generated_by")
    host_id = evg_attrib("host_id")
    ingest_time = evg_datetime_attrib("ingest_time")
    mainline = evg_attrib("mainline")
    order = evg_attrib("order")
    parent_task_id = evg_attrib("parent_task_id")
    project_id = evg_attrib("project_id")
    project_identifier = evg_attrib("project_identifier")
    priority = evg_attrib("priority")
    restarts = evg_attrib("restarts")
    revision = evg_attrib("revision")
    scheduled_time = evg_datetime_attrib("scheduled_time")
    start_time = evg_datetime_attrib("start_time")
    status = evg_attrib("status")
    task_group = evg_attrib("task_group")
    task_group_max_hosts = evg_attrib("task_group_max_hosts")
    task_id = evg_attrib("task_id")
    time_taken_ms = evg_attrib("time_taken_ms")
    version_id = evg_attrib("version_id")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of an evergreen task."""
        super(Task, self).__init__(json, api)
        self._logs_map: Optional[Dict[Any, Any]] = None

    @property
    def artifacts(self) -> List[Artifact]:
        """
        Retrieve the artifacts for the given task.

        :return: List of artifacts.
        """
        if not self.json.get("artifacts"):
            return []
        return [Artifact(artifact, self._api) for artifact in self.json["artifacts"]]

    @property
    def log_map(self) -> Dict:
        """
        Retrieve a dict of all the logs.

        :return: Dictionary of the logs.
        """
        if not self._logs_map:
            self._logs_map = {key: value for key, value in self.json["logs"].items()}
        return self._logs_map

    def get_project_identifier(self) -> str:
        """
        Return the human-readable project id. Can also be accessed as an attribute.

        :return: Human-readable project id.
        """
        return self.project_identifier

    def retrieve_log(self, log_name: str, raw: bool = False) -> str:
        """
        Retrieve the contents of the specified log.

        :param log_name: Name of log to retrieve.
        :param raw: Retrieve raw version of log.
        :return: Contents of the specified log.
        """
        return self._api.retrieve_task_log(self.log_map[log_name], raw)

    def stream_log(self, log_name: str) -> Iterable[str]:
        """
        Retrieve an iterator of a streamed log contents for the given log.

        :param log_name: Log to stream.
        :return: Iterable log contents.
        """
        return self._api.stream_log(self.log_map[log_name])

    @property
    def status_details(self) -> StatusDetails:
        """
        Retrieve the status details for the given task.

        :return: Status details.
        """
        return StatusDetails(self.json["status_details"], self._api)

    def get_status_score(self) -> StatusScore:
        """
        Retrieve the status score enum for the given task.

        :return: Status score.
        """
        return StatusScore.get_task_status_score(self)

    def get_execution(self, execution: int) -> Optional["Task"]:
        """
        Get the task info for the specified execution.

        :param execution: Index of execution.
        :return: Task info for specified execution.
        """
        if self.execution == execution:
            return self

        if "previous_executions" in self.json:
            for task in self.json["previous_executions"]:
                if task["execution"] == execution:
                    return Task(task, self._api)

        return None

    def get_execution_or_self(self, execution: int) -> "Task":
        """
        Get the specified execution if it exists.

        If the specified execution does not exist, return self.

        :param execution: Index of execution.
        :return: Task info for specified execution or self.
        """
        task_execution = self.get_execution(execution)
        if task_execution:
            return task_execution
        return self

    def wait_time(self) -> Optional[timedelta]:
        """
        Get the time taken until the task started running.

        :return: Time taken until task started running.
        """
        if self.start_time and self.ingest_time:
            return self.start_time - self.ingest_time
        return None

    def wait_time_once_unblocked(self) -> Optional[timedelta]:
        """
        Get the time taken until the task started running.

        Once it is unblocked by task dependencies.

        :return: Time taken until task started running.
        """
        if self.start_time and self.scheduled_time:
            return self.start_time - self.scheduled_time
        return None

    def is_success(self) -> bool:
        """
        Whether task was successful.

        :return: True if task was successful.
        """
        return self.status == EVG_SUCCESS_STATUS

    def is_test_failure(self) -> bool:
        """
        Whether task was a test failure.

        :return: True is task was a test failure.
        """
        if not self.is_success() and self.status_details and self.status_details.type:
            return self.status_details.type == EVG_TEST_STATUS_TYPE
        return False

    def is_undispatched(self) -> bool:
        """
        Whether the task was undispatched.

        :return: True is task was undispatched.
        """
        return self.status == EVG_UNDISPATCHED_STATUS

    def is_system_failure(self) -> bool:
        """
        Whether task resulted in a system failure.

        :return: True if task was a system failure.
        """
        if not self.is_success() and self.status_details and self.status_details.type:
            return self.status_details.type == EVG_SYSTEM_FAILURE_STATUS
        return False

    def is_timeout(self) -> bool:
        """
        Whether task results in a timeout.

        :return: True if task was a timeout.
        """
        if not self.is_success() and self.status_details and self.status_details.timed_out:
            return self.status_details.timed_out
        return False

    def is_setup_failure(self) -> bool:
        """
        Whether task is a setup failure.

        :return: True if task is a setup failure.
        """
        if not self.is_success() and self.status_details and self.status_details.type:
            return self.status_details.type == EVG_SETUP_FAILURE_STATUS_TYPE
        return False

    def is_completed(self) -> bool:
        """
        Whether task is completed.

        :return: True if task is completed.
        """
        return self.status == EVG_SUCCESS_STATUS or self.status == EVG_FAILED_STATUS

    def has_oom(self) -> bool:
        """
        Determine if the given task has an OOM failure.

        :return: True if task has an OOM failure.
        """
        return self.status_details.oom_tracker_info.detected

    def is_active(self) -> bool:
        """
        Determine if the given task is active.

        :return: True if task is active.
        """
        return self.scheduled_time and not self.finish_time

    def get_tests(
        self, status: Optional[str] = None, execution: Optional[int] = None
    ) -> List["Tst"]:
        """
        Get the test results for this task.

        :param status: Only return tests with the given status.
        :param execution: Return results for specified execution, if specified.
        :return: List of test results for the task.
        """
        return self._api.tests_by_task(
            self.task_id,
            status=status,
            execution=self.execution if execution is None else execution,
        )

    def get_num_of_tests(self) -> int:
        """
        Get the number of tests that ran as part of this task.

        :return: Number of tests for the task.
        """
        return self._api.num_of_tests_by_task(self.task_id)

    def get_execution_tasks(
        self, filter_fn: Optional[Callable[["Task"], bool]] = None
    ) -> Optional[List["Task"]]:
        """
        Get a list of execution tasks associated with this task.

        If this is a display task, return the tasks execution tasks associated with it.
        If this is not a display task, returns None.

        :param filter_fn: Function to filter returned results.
        :return: List of execution tasks.
        """
        if self.display_only:
            execution_tasks = [
                self._api.task_by_id(task_id, fetch_all_executions=True)
                for task_id in self.execution_tasks
            ]

            execution_tasks = [
                task.get_execution_or_self(self.execution) for task in execution_tasks
            ]

            if filter_fn:
                return [task for task in execution_tasks if filter_fn(task)]
            return execution_tasks

        return None

    def get_manifest(self) -> Optional[Manifest]:
        """Get the Manifest for this task."""
        return self._api.manifest_for_task(self.task_id)

    def get_task_annotation(self) -> List[TaskAnnotation]:
        """Get the task annotation for this task."""
        return self._api.get_task_annotation(self.task_id, self.execution)

    def get_oom_pids(self) -> List[int]:
        """Get the OOM PIDs for this task."""
        return (
            self.status_details.oom_tracker_info.pids
            if self.status_details.oom_tracker_info.pids
            else []
        )

    def annotate(
        self,
        message: Optional[str] = None,
        issues: Optional[List[IssueLinkRequest]] = None,
        suspected_issues: Optional[List[IssueLinkRequest]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        metadata_links: Optional[List[MetadataLinkRequest]] = None,
    ) -> None:
        """
        Annotate the specified task.

        :param message: Message to add to the annotations.
        :param issues: Issues to attach to the annotation.
        :param suspected_issues: Suspected issues to add to the annotation.
        :param metadata: Extra metadata to add to the issue.
        :param metadata_links: Metadata links to add to the annotation.
        """
        self._api.annotate_task(
            self.task_id,
            self.execution,
            message,
            issues,
            suspected_issues,
            metadata,
            metadata_links,
        )

    def __repr__(self) -> str:
        """
        Get a string representation of Task for debugging purposes.

        :return: String representation of Task.
        """
        return "Task({id})".format(id=self.task_id)
