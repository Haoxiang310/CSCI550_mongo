# -*- encoding: utf-8 -*-
"""Representation of an evergreen build."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_datetime_attrib
from evergreen.metrics.buildmetrics import BuildMetrics

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi
    from evergreen.task import Task  # noqa: F401
    from evergreen.version import Version

EVG_BUILD_STATUS_FAILED = "failed"
EVG_BUILD_STATUS_SUCCESS = "success"
EVG_BUILD_STATUS_CREATED = "created"

COMPLETED_STATES = {
    EVG_BUILD_STATUS_FAILED,
    EVG_BUILD_STATUS_SUCCESS,
}


class StatusCounts(_BaseEvergreenObject):
    """Representation of Evergreen StatusCounts."""

    succeeded = evg_attrib("succeeded")
    failed = evg_attrib("failed")
    started = evg_attrib("started")
    undispatched = evg_attrib("undispatched")
    inactivate = evg_attrib("inactive")
    dispatched = evg_attrib("dispatched")
    timed_out = evg_attrib("timed_out")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create a Status Counts object.

        :param json: Json of status counts object.
        :param api: Evergreen API.
        """
        super(StatusCounts, self).__init__(json, api)


class Build(_BaseEvergreenObject):
    """Representation of an Evergreen build."""

    id = evg_attrib("_id")
    project_id = evg_attrib("project_id")
    project_identifier = evg_attrib("project_identifier")
    create_time = evg_datetime_attrib("create_time")
    start_time = evg_datetime_attrib("start_time")
    finish_time = evg_datetime_attrib("finish_time")
    version = evg_attrib("version")
    branch = evg_attrib("branch")
    git_hash = evg_attrib("git_hash")
    build_variant = evg_attrib("build_variant")
    status = evg_attrib("status")
    activated = evg_attrib("activated")
    activated_by = evg_attrib("activated_by")
    activated_time = evg_datetime_attrib("activated_time")
    order = evg_attrib("order")
    tasks = evg_attrib("tasks")
    time_taken_ms = evg_attrib("time_taken_ms")
    display_name = evg_attrib("display_name")
    predicted_makespan_ms = evg_attrib("predicted_makespan_ms")
    actual_makespan_ms = evg_attrib("actual_makespan_ms")
    origin = evg_attrib("origin")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of an evergreen task.

        :param json: Json of build object.
        :param api: Evergreen API.
        """
        super(Build, self).__init__(json, api)

    @property
    def status_counts(self) -> StatusCounts:
        """Get the status counts of the build."""
        return StatusCounts(self.json["status_counts"], self._api)

    def get_project_identifier(self) -> str:
        """
        Return the human-readable project id. Can also be accessed as an attribute.

        :return: Human-readable project id.
        """
        return self.project_identifier

    def get_tasks(self, fetch_all_executions: bool = False) -> List["Task"]:
        """
        Get all tasks for this build.

        :param fetch_all_executions:  fetch all executions for tasks.
        :return: List of all tasks.
        """
        return self._api.tasks_by_build(self.id, fetch_all_executions)

    def is_completed(self) -> bool:
        """
        Determine if this build has completed running tasks.

        :return: True if build has completed running tasks.
        """
        return self.status in COMPLETED_STATES

    def get_metrics(self, task_filter_fn: Optional[Callable] = None) -> Optional[BuildMetrics]:
        """
        Get metrics for the build.

        Metrics are only available on build that have finished running..

        :param task_filter_fn: function to filter tasks included for metrics, should accept a task
                               argument.
        :return: Metrics for the build.
        """
        if self.status != EVG_BUILD_STATUS_CREATED:
            return BuildMetrics(self).calculate(task_filter_fn)
        return None

    def get_version(self) -> "Version":
        """
        Get the version this build is a part of.

        :return: Version that this build is a part of.
        """
        return self._api.version_by_id(self.version)

    def __repr__(self) -> str:
        """
        Get a string representation of Task for debugging purposes.

        :return: String representation of Task.
        """
        return "Build({id})".format(id=self.id)
