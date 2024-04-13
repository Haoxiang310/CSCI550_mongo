# -*- encoding: utf-8 -*-
"""Evergreen representation of a project."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Dict

from evergreen.base import _BaseEvergreenObject, evg_attrib
from evergreen.version import Version

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class Project(_BaseEvergreenObject):
    """Representation of an Evergreen project."""

    batch_time = evg_attrib("batch_time")
    branch_name = evg_attrib("branch_name")
    display_name = evg_attrib("display_name")
    enabled = evg_attrib("enabled")
    identifier = evg_attrib("identifier")
    id = evg_attrib("id")
    owner_name = evg_attrib("owner_name")
    private = evg_attrib("private")
    remote_path = evg_attrib("remote_path")
    repo_name = evg_attrib("repo_name")
    tracked = evg_attrib("tracked")
    deactivated_previous = evg_attrib("deactivate_previous")
    admins = evg_attrib("admins")
    tracks_push_events = evg_attrib("tracks_push_events")
    pr_testing_enabled = evg_attrib("pr_testing_enabled")
    commit_queue = evg_attrib("commit_queue")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of an evergreen project.

        :param json: json representing project.
        :param api: evergreen api object.
        """
        super(Project, self).__init__(json, api)

    def __str__(self) -> str:
        """Get a string version of the Project."""
        return self.identifier

    def most_recent_version(self) -> Version:
        """
        Fetch the most recent version.

        :return: Version queried for.
        """
        version_iterator = self._api.versions_by_project(self.identifier)
        return next(version_iterator)
