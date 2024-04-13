# -*- encoding: utf-8 -*-
"""Test representation of evergreen."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Dict, Iterable

from evergreen.base import _BaseEvergreenObject, evg_attrib, evg_datetime_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi


class Logs(_BaseEvergreenObject):
    """Representation of test logs from evergreen."""

    url = evg_attrib("url")
    line_num = evg_attrib("line_num")
    url_raw = evg_attrib("url_raw")
    url_lobster = evg_attrib("url_lobster")
    url_parsley = evg_attrib("url_parsley")
    log_id = evg_attrib("log_id")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a Test log."""
        super(Logs, self).__init__(json, api)

    def stream(self) -> Iterable[str]:
        """
        Retrieve an iterator of the streamed contents of this log.

        :return: Iterable to stream contents of log.
        """
        return self._api.stream_log(self.url_raw)


class Tst(_BaseEvergreenObject):
    """Representation of a test object from evergreen."""

    task_id = evg_attrib("task_id")
    status = evg_attrib("status")
    test_file = evg_attrib("test_file")
    exit_code = evg_attrib("exit_code")
    start_time = evg_datetime_attrib("start_time")
    end_time = evg_datetime_attrib("end_time")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """Create an instance of a Test object."""
        super(Tst, self).__init__(json, api)

    @property
    def logs(self) -> Logs:
        """
        Get the log object for the given test.

        :return: log object for test.
        """
        return Logs(self.json["logs"], self._api)
