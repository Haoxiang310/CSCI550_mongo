# -*- encoding: utf-8 -*-
"""Metrics for an evergreen version."""
from __future__ import absolute_import, division

from datetime import datetime
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from structlog import get_logger

if TYPE_CHECKING:
    from evergreen.build import Build
    from evergreen.metrics.buildmetrics import BuildMetrics
    from evergreen.version import Version

LOGGER = get_logger(__name__)


class VersionMetrics(object):
    """Metrics about an evergreen version."""

    def __init__(self, version: "Version") -> None:
        """
        Create an instance of version metrics.

        :param version: Version to analyze.
        """
        self.version = version

        self.total_processing_time = 0
        self.task_success_count = 0
        self.task_failure_count = 0
        self.task_timeout_count = 0
        self.task_system_failure_count = 0

        self._create_times: List[datetime] = []
        self._start_times: List[datetime] = []
        self._finish_times: List[datetime] = []

        self.build_metrics: List[BuildMetrics] = []
        self.build_list: Optional[List[Build]] = None

    def calculate(self, task_filter_fn: Optional[Callable] = None) -> "VersionMetrics":
        """
        Calculate metrics for the given build.

        :param task_filter_fn: function to filter tasks included for metrics, should accept a task
                               argument.
        :returns: self.
        """
        self.build_list = self.version.get_builds()
        for build in self.build_list:
            self._count_build(build, task_filter_fn)

        return self

    @property
    def create_time(self) -> Optional[datetime]:
        """
        Time the first task of the version was created.

        :return: Time first task was created.
        """
        if self._create_times:
            return min(self._create_times)
        return None

    @property
    def start_time(self) -> Optional[datetime]:
        """
        Time first task of version was started.

        :return: Time first task was started.
        """
        if self._start_times:
            return min(self._start_times)
        return None

    @property
    def end_time(self) -> Optional[datetime]:
        """
        Time last task of version was completed.

        :return: Time last task was completed.
        """
        if self._finish_times:
            return max(self._finish_times)
        return None

    @property
    def makespan(self) -> Optional[float]:
        """
        Wall clock duration of version.

        :return: Duration of version in seconds.
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def wait_time(self) -> Optional[float]:
        """
        Wall clock duration until version was started.

        :return: Duration until version was started in seconds.
        """
        if self.start_time and self.create_time:
            return (self.start_time - self.create_time).total_seconds()
        return None

    @property
    def total_tasks(self) -> int:
        """
        Get the total tasks in the version.

        :return: total tasks
        """
        return self.task_success_count + self.task_failure_count

    @property
    def pct_tasks_success(self) -> float:
        """
        Get the percentage of successful tasks.

        :return: Percentage of successful tasks.
        """
        return self._percent_tasks(self.task_success_count)

    @property
    def pct_tasks_failure(self) -> float:
        """
        Get the percentage of failure tasks.

        :return: Percentage of failure tasks.
        """
        return self._percent_tasks(self.task_failure_count)

    @property
    def pct_tasks_timeout(self) -> float:
        """
        Get the percentage of timeout tasks.

        :return: Percentage of timeout tasks.
        """
        return self._percent_tasks(self.task_timeout_count)

    @property
    def pct_tasks_system_failure(self) -> float:
        """
        Get the percentage of system failure tasks.

        :return: Percentage of system failure tasks.
        """
        return self._percent_tasks(self.task_system_failure_count)

    def _percent_tasks(self, n_tasks: int) -> float:
        """
        Calculate the percent of n_tasks out of total.

        :param n_tasks: Number of tasks to calculate percent of.
        :return: percentage n_tasks is out of total tasks.
        """
        if self.total_tasks == 0:
            return 0

        return n_tasks / self.total_tasks

    def _count_build(self, build: "Build", task_filter_fn: Optional[Callable]) -> None:
        """
        Add stats for the given build to the metrics.

        :param task_filter_fn: function to filter tasks included for metrics, should accept a task
                               argument.
        :param build: Build to add.
        """
        log = LOGGER.bind(build_id=build.id)
        if build.activated:
            log.debug("Processing metrics for build")
            # If all tasks have been undispatched there is no data.
            if not build.tasks or build.status_counts.undispatched == len(build.tasks):
                log.warning("Build had no tasks or all tasks undispatched")
                return

            build_metrics = build.get_metrics(task_filter_fn)
            if build_metrics:
                self.build_metrics.append(build_metrics)

                self.total_processing_time += build_metrics.total_processing_time
                self.task_success_count += build_metrics.success_count
                self.task_failure_count += build_metrics.failure_count
                self.task_timeout_count += build_metrics.timed_out_count
                self.task_system_failure_count += build_metrics.system_failure_count

                if build_metrics.create_time:
                    self._create_times.append(build_metrics.create_time)

                if build_metrics.start_time:
                    self._start_times.append(build_metrics.start_time)

                if build_metrics.end_time:
                    self._finish_times.append(build_metrics.end_time)

    def as_dict(self, include_children: bool = False) -> Dict:
        """
        Provide a dictionary representation.

        :param include_children: Include child build tasks in dictionary.
        :return: Dictionary of metrics.
        """
        metric = {
            "version": self.version.version_id,
            "total_processing_time": self.total_processing_time,
            "task_total": self.total_tasks,
            "task_success_count": self.task_success_count,
            "task_pct_success": self.pct_tasks_success,
            "task_failure_count": self.task_failure_count,
            "task_pct_failed": self.pct_tasks_failure,
            "task_timeout_count": self.task_timeout_count,
            "task_system_failure_count": self.task_system_failure_count,
        }

        if include_children:
            metric["build_metrics"] = [bm.as_dict(include_children) for bm in self.build_metrics]

        return metric

    def __str__(self) -> str:
        """
        Create string version of metrics.

        :return: String version of metrics.
        """
        process_time_min = self.total_processing_time / 60 if self.total_processing_time else 0
        makespan = self.makespan if self.makespan else 0
        makespan_min = self.makespan / 60 if self.makespan else 0
        waittime = self.wait_time if self.wait_time else 0
        waittime_min = self.wait_time / 60 if self.wait_time else 0

        return """Version Id: {version}
        Total Processing Time: {total_processing_time:.2f}s ({total_processing_time_min:.2f}m)
        Makespan: {makespan:.2f}s ({makespan_min:.2f}m)
        Wait Time: {waittime:.2f}s ({waittime_min:.2f}m)
        Total Tasks: {task_total}
        Successful Tasks: {task_success_count} ({task_pct_success:.2%})
        Failed Tasks: {task_failure_count} ({task_pct_failed:.2%})
        Timed Out Tasks: {task_timeout_count} ({task_pct_timed_out:.2%})
        System Failure Tasks: {task_system_failure_count} ({task_pct_system_failure:.2%})
        """.format(
            version=self.version.version_id,
            total_processing_time=self.total_processing_time,
            total_processing_time_min=process_time_min,
            makespan=makespan,
            makespan_min=makespan_min,
            waittime=waittime,
            waittime_min=waittime_min,
            task_total=self.total_tasks,
            task_success_count=self.task_success_count,
            task_pct_success=self.pct_tasks_success,
            task_failure_count=self.task_failure_count,
            task_pct_failed=self.pct_tasks_failure,
            task_timeout_count=self.task_timeout_count,
            task_pct_timed_out=self.pct_tasks_timeout,
            task_system_failure_count=self.task_system_failure_count,
            task_pct_system_failure=self.pct_tasks_system_failure,
        ).rstrip()
