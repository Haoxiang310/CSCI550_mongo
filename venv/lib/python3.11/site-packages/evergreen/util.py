"""Useful utilities for interacting with Evergreen."""
from datetime import date, datetime
from typing import Any, Iterable, Optional

from dateutil.parser import parse

EVG_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
EVG_SHORT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
EVG_DATE_FORMAT = "%Y-%m-%d"
EVG_DATE_INPUT_FORMAT = '"%Y-%m-%dT%H:%M:%S.000Z"'


def parse_evergreen_datetime(evg_date: Optional[Any]) -> Optional[datetime]:
    """
    Convert an evergreen datetime string into a datetime object.

    :param evg_date: String to convert to a datetime.
    :return: datetime version of date.
    """
    if not evg_date:
        return None

    if type(evg_date) in [int, float]:
        return datetime.fromtimestamp(evg_date)

    return parse(evg_date)


def parse_evergreen_short_datetime(evg_date: Optional[str]) -> Optional[datetime]:
    """
    Convert an evergreen datetime string into a datetime object.

    :param evg_date: String to convert to a datetime.
    :return: datetime version of date.
    """
    if not evg_date:
        return None

    return datetime.strptime(evg_date, EVG_SHORT_DATETIME_FORMAT)


def format_evergreen_datetime(when: datetime) -> str:
    """
    Convert a datetime object into an evergreen consumable string.

    :param when: datetime to convert.
    :return: string evergreen can understand.
    """
    return when.strftime(EVG_DATE_INPUT_FORMAT)


def evergreen_input_to_output(input_date: str) -> str:
    """
    Convert a date from evergreen to a date to send back to evergreen.

    :param input_date: date to convert.
    :return: date to send to evergreen.
    """
    intermediate = parse_evergreen_datetime(input_date)
    if intermediate:
        return format_evergreen_datetime(intermediate)
    return input_date


def parse_evergreen_date(evg_date: Optional[str]) -> Optional[date]:
    """
    Convert an evergreen date string into a date object.

    :param evg_date: String to convert to a date.
    :return: date version of date.
    """
    if not evg_date:
        return None
    return datetime.strptime(evg_date, EVG_DATE_FORMAT).date()


def format_evergreen_date(when: datetime) -> str:
    """
    Convert a datetime object into a string date that evergreen understands.

    :param when: Datetime object to convert.
    :return: string version of given date.
    """
    return when.strftime(EVG_DATE_FORMAT)


def iterate_by_time_window(
    iterator: Iterable, before: datetime, after: datetime, time_attr: str
) -> Iterable:
    """
    Iterate over a window of time.

    For a given iterator, generate the items that are within the specified time window.

    Note: Since most evergreen iterators start with the most recent items and then look backwards
    in time, `start` and `end` refer to the start and end of when items will be seen (i.e. `start`
    should be later in time than `end` since we will start seeing new items first.

    :param iterator: Iterator to window.
    :param before: Return items earlier than this timestamp.
    :param after: Return items later than this timestamp.
    :param time_attr: Attribute of items in the iterator containing timestamp to check.
    :return: Iterator for items in the given time window.
    """
    for item in iterator:
        item_time = getattr(item, time_attr)
        if item_time > before:
            continue

        if item_time < after:
            break

        yield item
