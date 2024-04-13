# -*- encoding: utf-8 -*-
"""Task representation of evergreen."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from evergreen.util import (
    parse_evergreen_date,
    parse_evergreen_datetime,
    parse_evergreen_short_datetime,
)

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


def evg_attrib(attrib_name: str, type_fn: Optional[Callable] = None) -> property:
    """
    Create an attribute for the given evergreen property.

    This creates an attribute for the class that looks up the value via json. It is used to
    allow editors to show what attributes are available for a given evergreen object.

    :param attrib_name: name of attribute.
    :param type_fn: method to use to convert attribute by type.
    """

    def attrib_getter(instance: _BaseEvergreenObject) -> Any:
        if attrib_name not in instance.json:
            return None

        if type_fn:
            return type_fn(instance.json[attrib_name])
        return instance.json.get(attrib_name, None)

    return property(attrib_getter, doc=f"value of {attrib_name}")


def evg_datetime_attrib(attrib_name: str) -> property:
    """
    Create a datetime attribute for the given evergreen property.

    :param attrib_name: Name of attribute.
    """
    return evg_attrib(attrib_name, parse_evergreen_datetime)


def evg_short_datetime_attrib(attrib_name: str) -> property:
    """
    Create a shortened datetime attribute for the given evergreen property.

    :param attrib_name: Name of attribute.
    """
    return evg_attrib(attrib_name, parse_evergreen_short_datetime)


def evg_date_attrib(attrib_name: str) -> property:
    """
    Create a date attribute for the given evergreen property.

    :param attrib_name: Name of attribute.
    """
    return evg_attrib(attrib_name, parse_evergreen_date)


class _BaseEvergreenObject(object):
    """Common evergreen object."""

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of an evergreen task."""
        self.json = json
        self._api = api
        self._date_fields = None

    def _is_field_a_date(self, item: str) -> bool:
        """
        Determine if given field is a date.

        :param item: field to check.
        :return: True if field is a date.
        """
        return bool(self._date_fields and item in self._date_fields and self.json[item])

    def __getattr__(self, item: str) -> Any:
        """Lookup an attribute if it exists."""
        if item != "json" and item in self.json:
            if self._is_field_a_date(item):
                return parse_evergreen_datetime(self.json[item])
            return self.json[item]
        raise AttributeError("Unknown attribute {0}".format(item))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _BaseEvergreenObject):
            return self.json == other.json
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)
