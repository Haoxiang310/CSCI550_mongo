"""Representation of project aliases."""
from typing import TYPE_CHECKING, Any, Dict, List

from evergreen.base import _BaseEvergreenObject, evg_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class DisplayTaskAlias(_BaseEvergreenObject):
    """Representation of a DisplayTask in an alias."""

    name = evg_attrib("Name")
    execution_tasks = evg_attrib("ExecutionTasks")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a display task alias.

        :param json: json representing a display task alias.
        :param api: instance of evergreen api object.
        """
        super().__init__(json, api)


class VariantAlias(_BaseEvergreenObject):
    """Representation of an alias for a particular build variant."""

    variant = evg_attrib("Variant")
    tasks = evg_attrib("Tasks")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a variant alias.

        :param json: json representing variant alias.
        :param api: instance of evergreen api object.
        """
        super().__init__(json, api)

    @property
    def display_tasks(self) -> List[DisplayTaskAlias]:
        """Get a list of display tasks for the alias."""
        if not self.json["DisplayTasks"]:
            return []
        return [DisplayTaskAlias(dt, self._api) for dt in self.json["DisplayTasks"]]
