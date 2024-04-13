# -*- encoding: utf-8 -*-
"""Stats representation of evergreen."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Dict

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_date_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class TaskReliability(_BaseEvergreenObject):
    """Representation of an Evergreen task reliability object."""

    test_file = evg_attrib("test_file")
    task_name = evg_attrib("task_name")
    variant = evg_attrib("variant")
    distro = evg_attrib("distro")
    evg_date_attrib("date")
    num_success = evg_attrib("num_success")
    num_failed = evg_attrib("num_failed")
    num_total = evg_attrib("num_total")
    num_timeout = evg_attrib("num_timeout")
    num_test_failed = evg_attrib("num_test_failed")
    num_system_failed = evg_attrib("num_system_failed")
    num_setup_failed = evg_attrib("num_setup_failed")
    avg_duration_pass = evg_attrib("avg_duration_pass")
    success_rate = evg_attrib("success_rate")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a test stats object.

        :param json: json version of object.
        """
        super(TaskReliability, self).__init__(json, api)
