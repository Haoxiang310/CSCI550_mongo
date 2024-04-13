"""Top-level container for shrub configuration."""
import json
from typing import Any, Dict, Optional, Set

import yaml

from shrub.v2.variant import BuildVariant
from shrub.v2.task import Task, TaskGroup
from shrub.v2.dict_creation_util import add_if_exists


class ShrubProject(object):
    """Configuration for an evergreen shrub project."""

    def __init__(self, build_variants: Optional[Set[BuildVariant]] = None) -> None:
        """
        Create a new Shrub Project.

        :param build_variants: Set of build variants to configure.
        """
        self.build_variants = build_variants if build_variants else set()

    @classmethod
    def empty(cls) -> "ShrubProject":
        """Create an empty shrub project."""
        return cls()

    def add_build_variant(self, variant: BuildVariant) -> "ShrubProject":
        """
        Add the given build variant configuration to this project.

        :param variant: Build Variant to add.
        :return: This shrub project.
        """
        self.build_variants.add(variant)
        return self

    def all_tasks(self) -> Set[Task]:
        """
        Get the set of all tasks in this project.

        :return: All tasks in the project.
        """
        return {task for bv in self.build_variants for task in bv.all_tasks()}

    def all_task_groups(self) -> Set[TaskGroup]:
        """
        Get the set of all task group sin this project.

        :return: All task groups in the project.
        """
        return {tg for bv in self.build_variants for tg in bv.task_groups}

    def as_dict(self) -> Dict[str, Any]:
        """
        Convert this project configuration to a dictionary.

        :return: Dictionary of project configuration.
        """
        obj = {
            "buildvariants": [bv.as_dict() for bv in self.build_variants],
            "tasks": sorted([task.as_dict() for task in self.all_tasks()], key=lambda t: t["name"]),
        }

        add_if_exists(
            obj,
            "task_groups",
            sorted([tg.as_dict() for tg in self.all_task_groups()], key=lambda tg: tg["name"]),
        )

        return obj

    def json(self) -> str:
        """Get the json version of this project."""
        return json.dumps(self.as_dict(), indent=4)

    def yaml(self) -> str:
        """Get the yaml version of this project."""
        return yaml.dump(self.as_dict())
