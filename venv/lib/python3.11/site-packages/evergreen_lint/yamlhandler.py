"""Yaml handling helpers for evglint."""
import os
from typing import Any, Union

import yaml


class ReadOnlyDict(dict):
    """RO dictionary wrapper to prevent modifications to the yaml dict."""

    # pylint: disable=no-self-use
    def __readonly__(self, *args, **kwargs):
        raise RuntimeError("Rules must not modify the yaml dictionary")

    __setitem__ = __readonly__
    __delitem__ = __readonly__
    pop = __readonly__  # type: ignore
    popitem = __readonly__
    clear = __readonly__
    update = __readonly__  # type: ignore
    setdefault = __readonly__  # type: ignore
    del __readonly__


def load_file(yaml_file: Union[str, os.PathLike]) -> dict:
    """Load yaml from a file on disk."""
    with open(yaml_file) as fh:
        return load(fh)


def load(data: Any) -> dict:
    """Given a file handle or buffer, load yaml."""
    yaml_dict = yaml.safe_load(data)
    return ReadOnlyDict(yaml_dict)
