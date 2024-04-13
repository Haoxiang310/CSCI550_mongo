"""Representation of an evergreen patch."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Set

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_datetime_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi
    from evergreen.version import Version


class GithubPatchData(_BaseEvergreenObject):
    """Representation of github patch data in a patch object."""

    pr_number = evg_attrib("pr_number")
    base_owner = evg_attrib("base_owner")
    base_repo = evg_attrib("base_repo")
    head_owner = evg_attrib("head_owner")
    head_repo = evg_attrib("head_repo")
    head_hash = evg_attrib("head_hash")
    author = evg_attrib("author")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of github patch data.

        :param json: json representing github patch data.
        :param api: instance of evergreen api object.
        """
        super(GithubPatchData, self).__init__(json, api)


class VariantsTasks(_BaseEvergreenObject):
    """Representation of a variants tasks object."""

    name = evg_attrib("name")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a variants tasks object.

        :param json: json representing variants tasks object.
        :param api: intance of the evergreen api object.
        """
        super(VariantsTasks, self).__init__(json, api)
        self._task_set: Optional[Set[str]] = None

    @property
    def tasks(self) -> Set[str]:
        """
        Retrieve the set of all tasks for this variant.

        :return: Set of all tasks for this variant.
        """
        if not self._task_set:
            self._task_set = set(self.json["tasks"])

        return self._task_set


class FileDiff(_BaseEvergreenObject):
    """Representation of a file diff for a patch."""

    file_name = evg_attrib("file_name")
    additions = evg_attrib("additions")
    deletions = evg_attrib("deletions")
    diff_link = evg_attrib("diff_link")
    description = evg_attrib("description")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a file diff.

        :param json: JSON representing the data.
        :param api: Pointer to the evergreen API client.
        """
        super().__init__(json, api)


class ModuleCodeChanges(_BaseEvergreenObject):
    """Representation of the module code changes for a patch."""

    branch_name = evg_attrib("branch_name")
    html_link = evg_attrib("html_link")
    raw_link = evg_attrib("raw_link")
    commit_messages = evg_attrib("commit_messages")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of the module code changes.

        :param json: JSON representing the data.
        :param api: Pointer to the evergreen API client.
        """
        super().__init__(json, api)

    @property
    def file_diffs(self) -> List[FileDiff]:
        """Retrieve a list of the file diffs for this patch."""
        return [FileDiff(diff, self._api) for diff in self.json["module_code_changes"]]


class PatchCreationDetails(NamedTuple):
    """Details of a patch creation."""

    url: str
    id: str


class Patch(_BaseEvergreenObject):
    """Representation of an Evergreen patch."""

    patch_id = evg_attrib("patch_id")
    description = evg_attrib("description")
    project_id = evg_attrib("project_id")
    branch = evg_attrib("branch")
    git_hash = evg_attrib("git_hash")
    patch_number = evg_attrib("patch_number")
    author = evg_attrib("author")
    version = evg_attrib("version")
    status = evg_attrib("status")
    create_time = evg_datetime_attrib("create_time")
    start_time = evg_datetime_attrib("start_time")
    finish_time = evg_datetime_attrib("finish_time")
    builds = evg_attrib("builds")
    tasks = evg_attrib("tasks")
    activated = evg_attrib("activated")
    alias = evg_attrib("alias")
    commit_queue_position = evg_attrib("commit_queue_position")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of an evergreen patch.

        :param json: json representing patch.
        """
        super(Patch, self).__init__(json, api)
        self._variants_tasks: Optional[List[VariantsTasks]] = None
        self._variant_task_dict: Optional[Dict[str, Set[str]]] = None

    @property
    def github_patch_data(self) -> GithubPatchData:
        """
        Retrieve the github patch data for this patch.

        :return: Github Patch Data for this patch.
        """
        return GithubPatchData(self.json["github_patch_data"], self._api)

    @property
    def module_code_changes(self) -> List[ModuleCodeChanges]:
        """
        Retrieve the modele code changes for this patch.

        :return: Module code changes for this patch.
        """
        return [ModuleCodeChanges(module, self._api) for module in self.json["module_code_changes"]]

    @property
    def variants_tasks(self) -> List[VariantsTasks]:
        """
        Retrieve the variants tasks for this patch.

        :return: variants tasks for this patch.
        """
        if not self._variants_tasks:
            self._variants_tasks = [
                VariantsTasks(vt, self._api) for vt in self.json["variants_tasks"]
            ]
        return self._variants_tasks

    def task_list_for_variant(self, variant: str) -> Set[str]:
        """
        Retrieve the list of tasks for the given variant.

        :param variant: name of variant to search for.
        :return: list of tasks belonging to the specified variant.
        """
        if not self._variant_task_dict:
            self._variant_task_dict = {vt.name: vt.tasks for vt in self.variants_tasks}
        return self._variant_task_dict[variant]

    def get_version(self) -> "Version":
        """
        Get version for this patch.

        :return: Version object.
        """
        return self._api.version_by_id(self.version)

    def is_in_commit_queue(self) -> bool:
        """
        Check whether this patch is currently in the commit queue.

        :return: True if in the commit queue, False otherwise.
        """
        return self.commit_queue_position is not None and self.commit_queue_position >= 0

    def __str__(self) -> str:
        """Get a human readable string version of the patch."""
        return "{}: {}".format(self.patch_id, self.description)
