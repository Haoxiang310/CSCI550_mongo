"""Models to working with task annotations."""
from typing import TYPE_CHECKING, Any, Dict, List

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_datetime_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class Source(_BaseEvergreenObject):
    """Source of where an annotation was generated."""

    author = evg_attrib("author")
    time = evg_datetime_attrib("time")
    requester = evg_attrib("requester")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a task annotation source."""
        super().__init__(json, api)


class IssueLink(_BaseEvergreenObject):
    """Representation of a issue added as a task annotation."""

    url = evg_attrib("url")
    issue_key = evg_attrib("issue_key")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a task annotation issue link."""
        super().__init__(json, api)

    @property
    def source(self) -> Source:
        """Get the source of this issue link."""
        return Source(self.json["source"], self._api)


class Note(_BaseEvergreenObject):
    """Representation of a note associated with a task annotation."""

    message = evg_attrib("message")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a task annotation note."""
        super().__init__(json, api)

    @property
    def source(self) -> Source:
        """Get the source of this note."""
        return Source(self.json["source"], self._api)


class MetadataLink(_BaseEvergreenObject):
    """Representation of a metadata link associated with a task annotation."""

    url = evg_attrib("url")
    text = evg_attrib("text")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a task annotation metadata link."""
        super().__init__(json, api)

    @property
    def source(self) -> Source:
        """Get the source of this metadata link."""
        return Source(self.json["source"], self._api)


class TaskAnnotation(_BaseEvergreenObject):
    """Representation of a task annotation."""

    task_id = evg_attrib("task_id")
    task_execution = evg_attrib("task_execution")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a task annotations."""
        super().__init__(json, api)

    @property
    def issues(self) -> List[IssueLink]:
        """Get the issues this task has been annotated with."""
        return [IssueLink(j, self._api) for j in self.json.get("issues", [])]

    @property
    def suspected_issues(self) -> List[IssueLink]:
        """Get the suspected issues this task has been annotated with."""
        return [IssueLink(j, self._api) for j in self.json.get("suspected_issues", [])]

    @property
    def metadata(self) -> Dict[str, Any]:
        """Get metadata associated with this annotation."""
        return self.json.get("metadata", {})

    @property
    def note(self) -> Note:
        """Get a note about this annotation."""
        return Note(self.json.get("note", {}), self._api)

    @property
    def metadata_links(self) -> List[MetadataLink]:
        """Get metadata links for this annotation."""
        return [MetadataLink(link, self._api) for link in self.json.get("metadata_links", [])]
