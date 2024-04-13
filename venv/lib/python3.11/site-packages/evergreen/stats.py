# -*- encoding: utf-8 -*-
"""Stats representation of evergreen."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Dict

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_date_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class TestStats(_BaseEvergreenObject):
    """Representation of an Evergreen test stats object."""

    test_file = evg_attrib("test_file")
    task_name = evg_attrib("task_name")
    test_name = evg_attrib("test_name")
    variant = evg_attrib("variant")
    distro = evg_attrib("distro")
    date = evg_date_attrib("date")
    num_pass = evg_attrib("num_pass")
    num_fail = evg_attrib("num_fail")
    avg_duration_pass = evg_attrib("avg_duration_pass")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a test stats object.

        :param json: json version of object.
        """
        super(TestStats, self).__init__(json, api)


class TaskStats(_BaseEvergreenObject):
    """Representation of an Evergreen task stats object."""

    task_name = evg_attrib("task_name")
    variant = evg_attrib("variant")
    distro = evg_attrib("distro")
    date = evg_date_attrib("date")
    num_pass = evg_attrib("num_success")
    num_fail = evg_attrib("num_failed")
    num_total = evg_attrib("num_total")
    avg_duration_pass = evg_attrib("avg_duration_success")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a test stats object.

        :param json: json version of object.
        """
        super(TaskStats, self).__init__(json, api)
