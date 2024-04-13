# -*- encoding: utf-8 -*-
"""Evergreen representation of a user's permissions."""
from __future__ import absolute_import

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict

from evergreen.base import _BaseEvergreenObject, evg_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class PermissionableResourceType(str, Enum):
    """Represents resource types that a user can be granted permissions to."""

    PROJECT = "project"
    DISTRO = "distro"
    SUPERUSER = "superuser"


class RemovablePermission(str, Enum):
    """Represents a permission that can be removed from a user."""

    PROJECT = "project"
    DISTRO = "distro"
    SUPERUSER = "superuser"
    ALL = "all"


class ResourceTypePermissions(_BaseEvergreenObject):
    """Representation of a user's permissions on resources of a specific type."""

    resource_type = evg_attrib("type")
    permissions = evg_attrib("permissions")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a resource type permission set.

        :param json: JSON of resource type permissions.
        :param api: Evergreen API.
        """
        super(ResourceTypePermissions, self).__init__(json, api)
