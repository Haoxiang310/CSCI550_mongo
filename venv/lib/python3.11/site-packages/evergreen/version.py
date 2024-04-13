# -*- encoding: utf-8 -*-
"""Version representation of evergreen."""
from __future__ import absolute_import

from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import structlog

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_datetime_attrib
from evergreen.build import Build
from evergreen.manifest import ManifestModule
from evergreen.metrics.versionmetrics import VersionMetrics

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi
    from evergreen.manifest import Manifest
    from evergreen.patch import Patch  # noqa: F401


LOGGER = structlog.getLogger(__name__)


class Requester(str, Enum):
    """Requester that created version."""

    PATCH_REQUEST = "patch_request"
    GITTER_REQUEST = "gitter_request"
    GITHUB_PULL_REQUEST = "github_pull_request"
    MERGE_TEST = "merge_test"
    AD_HOC = "ad_hoc"
    TRIGGER_REQUEST = "trigger_request"
    UNKNOWN = "UNKNOWN"

    def evg_value(self) -> str:
        """Get the evergreen value for a requester."""
        return self.name.lower()

    def stats_value(self) -> str:
        """Get the value for the stats endpoints."""
        value_mappings = {
            Requester.PATCH_REQUEST: "patch",
            Requester.GITTER_REQUEST: "mainline",
            Requester.GITHUB_PULL_REQUEST: "patch",
            Requester.MERGE_TEST: "",
            Requester.AD_HOC: "adhoc",
            Requester.TRIGGER_REQUEST: "trigger",
            Requester.UNKNOWN: "",
        }

        return value_mappings[self]


PATCH_REQUESTERS = {
    Requester.PATCH_REQUEST,
    Requester.GITHUB_PULL_REQUEST,
    Requester.MERGE_TEST,
}

EVG_VERSION_STATUS_SUCCESS = "success"
EVG_VERSION_STATUS_FAILED = "failed"
EVG_VERSION_STATUS_CREATED = "created"

COMPLETED_STATES = {
    EVG_VERSION_STATUS_FAILED,
    EVG_VERSION_STATUS_SUCCESS,
}


class BuildVariantStatus(_BaseEvergreenObject):
    """Representation of a Build Variants status."""

    build_variant = evg_attrib("build_variant")
    build_id = evg_attrib("build_id")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a Build Variants status."""
        super(BuildVariantStatus, self).__init__(json, api)

    def get_build(self) -> "Build":
        """Get the build object for this build variants status."""
        return self._api.build_by_id(self.build_id)


class Version(_BaseEvergreenObject):
    """Representation of an Evergreen Version."""

    version_id = evg_attrib("version_id")
    create_time = evg_datetime_attrib("create_time")
    start_time = evg_datetime_attrib("start_time")
    finish_time = evg_datetime_attrib("finish_time")
    revision = evg_attrib("revision")
    order = evg_attrib("order")
    project = evg_attrib("project")
    author = evg_attrib("author")
    author_email = evg_attrib("author_email")
    message = evg_attrib("message")
    status = evg_attrib("status")
    repo = evg_attrib("repo")
    branch = evg_attrib("branch")
    errors = evg_attrib("errors")
    warnings = evg_attrib("warnings")
    ignored = evg_attrib("ignored")
    project_identifier = evg_attrib("project_identifier")
    aborted = evg_attrib("aborted")
    parameters = evg_attrib("parameters")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of an evergreen version.

        :param json: json representing version
        """
        super(Version, self).__init__(json, api)

        if "build_variants_status" in self.json and self.json["build_variants_status"]:
            self.build_variants_map = {
                bvs["build_variant"]: bvs["build_id"] for bvs in self.json["build_variants_status"]
            }
            LOGGER.debug(
                "build_variants_map initialized for version",
                version_id=self.version_id,
                build_variants_map=self.build_variants_map,
            )
        else:
            LOGGER.debug(
                "build_variants_status either empty or not found for version",
                version_id=self.version_id,
                json=self.json,
            )
            self.build_variants_map = {}

    @property
    def build_variants_status(self) -> List[BuildVariantStatus]:
        """Get a list of build variant statuses."""
        if "build_variants_status" not in self.json or not self.json["build_variants_status"]:
            return []
        build_variants_status = self.json["build_variants_status"]
        return [BuildVariantStatus(bvs, self._api) for bvs in build_variants_status]

    @property
    def requester(self) -> Requester:
        """Get the requester of this version."""
        return Requester[self.json.get("requester", "UNKNOWN").upper()]

    def build_by_variant(self, build_variant: str) -> "Build":
        """
        Get a build object for the specified variant.

        :param build_variant: Build variant to get build for.
        :return: Build object for variant.
        """
        return self._api.build_by_id(self.build_variants_map[build_variant])

    def get_manifest(self) -> "Manifest":
        """
        Get the manifest for this version.

        :return: Manifest for this version.
        """
        return self._api.manifest(self.project, self.revision)

    def get_modules(self) -> Optional[Dict[str, ManifestModule]]:
        """
        Get the modules for this version.

        :return: ManifestModules for this version.
        """
        return self.get_manifest().modules

    def get_builds(self) -> List["Build"]:
        """
        Get all the builds that are a part of this version.

        :return: List of build that are a part of this version.
        """
        return self._api.builds_by_version(self.version_id)

    def is_patch(self) -> bool:
        """
        Determine if this version from a patch build.

        :return: True if this version is a patch build.
        """
        if self.requester and self.requester != Requester.UNKNOWN:
            return self.requester in PATCH_REQUESTERS
        return not self.version_id.startswith(self.project.replace("-", "_"))

    def is_completed(self) -> bool:
        """
        Determine if this version has completed running tasks.

        :return: True if version has completed.
        """
        return self.status in COMPLETED_STATES

    def get_patch(self) -> Optional["Patch"]:
        """
        Get the patch information for this version.

        :return: Patch for this version.
        """
        if self.is_patch():
            return self._api.patch_by_id(self.version_id)
        return None

    def get_metrics(self, task_filter_fn: Optional[Callable] = None) -> Optional[VersionMetrics]:
        """
        Calculate the metrics for this version.

        Metrics are only available on versions that have finished running.

        :param task_filter_fn: function to filter tasks included for metrics, should accept a task
                               argument.
        :return: Metrics for this version.
        """
        if self.status != EVG_VERSION_STATUS_CREATED:
            return VersionMetrics(self).calculate(task_filter_fn)
        return None

    def __repr__(self) -> str:
        """
        Get the string representation of Version for debugging purposes.

        :return: String representation of Version.
        """
        return "Version({id})".format(id=self.version_id)


class RecentVersionRow(_BaseEvergreenObject):
    """Wrapper for a row of the RecentVersions endpoint."""

    build_variant = evg_attrib("build_variant")

    @property
    def builds(self) -> Dict[str, Build]:
        """Get a map of build IDs to build objects."""
        return {k: Build(v, self._api) for k, v in self.json["builds"].items()}


class RecentVersions(_BaseEvergreenObject):
    """Wrapper for the data object returned by /projects/{project_id}/recent_versions."""

    rows = evg_attrib("rows")
    build_variants = evg_attrib("build_variants")

    @property
    def row_map(self) -> Dict[str, RecentVersionRow]:
        """Get a map of build names to RecentVersionRows."""
        return {k: RecentVersionRow(v, self._api) for k, v in self.json["rows"].items()}

    @property
    def versions(self) -> List[Version]:
        """
        Get the list of versions from the recent versions response object.

        :return: List of versions from the response object
        """
        return [Version(wrapper["versions"], self._api) for wrapper in self.json["versions"]]
