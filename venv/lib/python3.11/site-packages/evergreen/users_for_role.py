# -*- encoding: utf-8 -*-
"""Representation of users having an evergreen role."""
from typing import TYPE_CHECKING, Any, Dict

from evergreen.base import _BaseEvergreenObject, evg_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class UsersForRole(_BaseEvergreenObject):
    """Representation of a list of users having an evergreen role."""

    users = evg_attrib("users")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a UsersForRole object."""
        super(UsersForRole, self).__init__(json, api)
