"""Shrub configuration for an evergreen build variant."""
from dataclasses import dataclass
from itertools import chain
from typing import Any, Dict, Optional, Set, FrozenSet, Sequence, List

from shrub.v2.task import Task, TaskGroup, RunnableTask, ExistingTask
from shrub.v2.dict_creation_util import add_existing_from_dict


@dataclass(frozen=True)
class _DisplayTask(object):
    """Representation of a Display Task."""

    display_name: str
    execution_tasks: FrozenSet[RunnableTask]

    def as_dict(self) -> Dict[str, Any]:
        """Get a dictionary of this display task."""
        return {
            "name": self.display_name,
            "execution_tasks": sorted([task.name for task in self.execution_tasks]),
        }


class BuildVariant(object):
    """Representation of a Build Variant."""

    def __init__(
        self,
        name: str,
        display_name: Optional[str] = None,
        batch_time: Optional[int] = None,
        expansions: Optional[Dict[str, Any]] = None,
        run_on: Optional[Sequence[str]] = None,
        modules: Optional[Sequence[str]] = None,
        activate: Optional[bool] = None,
    ) -> None:
        """
        Create a new build variant.

        :param name: Name of build variant.
        :param display_name: Display name of build variant.
        :param batch_time: Interval of time in minutes that evergreen should wait before activating
            this variant.
        :param expansions: A set of key-value expansions pairs.
        :param run_on: Which distros the tasks should run on.
        :param modules: Which modules to include in this build variant.
        :param activate: Should the build variant be scheduled by default.
        """
        self.name = name
        self.display_name = display_name
        self.batch_time = batch_time
        self.tasks: Set[Task] = set()
        self.task_groups: Set[TaskGroup] = set()
        self.existing_tasks: Set[ExistingTask] = set()
        self.display_tasks: Set[_DisplayTask] = set()
        self.expansions: Dict[str, Any] = expansions if expansions else {}
        self.run_on = run_on
        self.modules = modules
        self.activate = activate
        self.task_to_distro_map: Dict[str, Sequence[str]] = {}
        self.task_to_activate_map: Dict[str, bool] = {}

    def add_task(
        self, task: Task, distros: Optional[Sequence[str]] = None, activate: Optional[bool] = None
    ) -> "BuildVariant":
        """
        Add the given task to this build variant.

        :param task: Task to add to build variant.
        :param distros: Distros to run task on.
        :param activate: Should task be scheduled when created.
        :return: This build variant.
        """
        self.tasks.add(task)
        if distros:
            self.task_to_distro_map[task.name] = distros
        if activate is not None:
            self.task_to_activate_map[task.name] = activate
        return self

    def add_tasks(
        self,
        task_set: Set[Task],
        distros: Optional[Sequence[str]] = None,
        activate: Optional[bool] = None,
    ) -> "BuildVariant":
        """
        Add the given set of tasks to this build variant.

        :param task_set: Set of tasks to add to build variant.
        :param distros: Distros to run task on.
        :param activate: Should task be scheduled when created.
        :return: This build variant.
        """
        self.tasks.update(task_set)
        if distros:
            for task in task_set:
                self.task_to_distro_map[task.name] = distros
        if activate is not None:
            for task in task_set:
                self.task_to_activate_map[task.name] = activate
        return self

    def add_task_group(
        self, task_group: TaskGroup, distros: Optional[Sequence[str]] = None
    ) -> "BuildVariant":
        """
        Add the given task group to the set of tasks in this build variant.

        :param task_group: Task group to add to build variant.
        :param distros: Distros to run task group on.
        :return: This build variant.
        """
        self.task_groups.add(task_group)
        if distros:
            self.task_to_distro_map[task_group.name] = distros
        return self

    def add_task_groups(
        self, task_group_set: Set[TaskGroup], distros: Optional[Sequence[str]] = None
    ) -> "BuildVariant":
        """
        Add the given set of task groups to this build variant.

        :param task_group_set: Set of task groups to add to build variant.
        :param distros: Distros to run task on.
        :return: This build variant.
        """
        self.task_groups.update(task_group_set)
        if distros:
            for task_group in task_group_set:
                self.task_to_distro_map[task_group.name] = distros
        return self

    def add_existing_task(
        self, existing_task: ExistingTask, distros: Optional[Sequence[str]] = None
    ) -> "BuildVariant":
        """
        Add the given existing task to the set of tasks in this build variant.

        :param existing_task: Task to add to build variant.
        :param distros: Distros to run task group on.
        :return: This build variant.
        """
        self.existing_tasks.add(existing_task)
        if distros:
            self.task_to_distro_map[existing_task.name] = distros
        return self

    def add_existing_tasks(
        self, existing_tasks: Set[ExistingTask], distros: Optional[Sequence[str]] = None
    ) -> "BuildVariant":
        """
        Add the given set of existing tasks to this build variant.

        :param existing_tasks: Set of existing tasks to add to build variant.
        :param distros: Distros to run task on.
        :return: This build variant.
        """
        self.existing_tasks.update(existing_tasks)
        if distros:
            for task in existing_tasks:
                self.task_to_distro_map[task.name] = distros
        return self

    def display_task(
        self,
        display_name: str,
        execution_tasks: Optional[Set[Task]] = None,
        execution_task_groups: Optional[Set[TaskGroup]] = None,
        execution_existing_tasks: Optional[Set[ExistingTask]] = None,
        distros: Optional[Sequence[str]] = None,
        activate: Optional[bool] = None,
    ) -> "BuildVariant":
        """
        Add a new display task to this build variant.

        :param display_name: Name of display task.
        :param execution_tasks: Set of tasks that should be part of the display task.
        :param execution_task_groups: Set of task groups that should be part of the display task.
        :param execution_existing_tasks: Set of existing tasks that should be part of the display
               task.
        :param distros: Distros to run tasks on.
        :param activate: Should task be scheduled when created.
        :return: This build variant configuration.
        """
        all_runnable_tasks: Set[RunnableTask] = set()
        if execution_tasks:
            self.add_tasks(execution_tasks, distros, activate)
            all_runnable_tasks.update(execution_tasks)
        if execution_task_groups:
            self.add_task_groups(execution_task_groups, distros)
            all_runnable_tasks.update(execution_task_groups)
        if execution_existing_tasks:
            all_runnable_tasks.update(execution_existing_tasks)
        self.display_tasks.add(_DisplayTask(display_name, frozenset(all_runnable_tasks)))
        return self

    def all_tasks(self) -> Set[Task]:
        """Get a set of all tasks that are part of this build variant."""
        tasks = self.tasks
        tg_tasks = [set(tg.tasks) for tg in self.task_groups]
        return tasks.union(chain.from_iterable(tg_tasks))

    def __task_spec_for_task(self, task: RunnableTask) -> Dict[str, Any]:
        """
        Create a task spec to run the given task on this variant.

        :param task: Task to run.
        :return: Task spec for running task.
        """
        return task.task_spec(
            self.task_to_distro_map.get(task.name), self.task_to_activate_map.get(task.name)
        )

    def __get_task_specs(self, task_list: FrozenSet[RunnableTask]) -> List[Dict[str, Any]]:
        """
        Get a dictionary representation of task specs for the tasks given.

        :param task_list: List of tasks or task groups.
        :return: Dictionary representation of task specs for given list.
        """
        return sorted([self.__task_spec_for_task(t) for t in task_list], key=lambda t: t["name"])

    def as_dict(self) -> Dict[str, Any]:
        """Get the dictionary representation of this build variant."""
        obj: Dict[str, Any] = {
            "name": self.name,
            "tasks": self.__get_task_specs(frozenset(self.tasks))
            + self.__get_task_specs(frozenset(self.task_groups))
            + self.__get_task_specs(frozenset(self.existing_tasks)),
        }

        if self.display_tasks:
            obj["display_tasks"] = sorted(
                [dt.as_dict() for dt in self.display_tasks], key=lambda d: d["name"]
            )

        add_existing_from_dict(
            obj,
            {
                "expansions": self.expansions,
                "run_on": self.run_on,
                "modules": self.modules,
                "display_name": self.display_name,
                "batch_time": self.batch_time,
                "activate": self.activate,
            },
        )

        return obj
