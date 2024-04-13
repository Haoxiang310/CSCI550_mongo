# -*- encoding: utf-8 -*-
"""Get configuration about connecting to evergreen."""
from __future__ import absolute_import

import os
from collections import namedtuple
from typing import Dict, Optional

import yaml

EvgAuth = namedtuple("EvgAuth", ["username", "api_key"])

DEFAULT_NETWORK_TIMEOUT_SEC = 5 * 60
DEFAULT_API_SERVER = "https://evergreen.mongodb.com"
CONFIG_FILE_LOCATIONS = [
    os.path.expanduser(os.path.join("~", "cli_bin", ".evergreen.yml")),
    os.path.expanduser(os.path.join("~", ".evergreen.yml")),
]


def read_evergreen_from_file(filename: str) -> Dict:
    """
    Read evergreen config from given filename.

    :param filename: Filename to read config.
    :return: Config read from file.
    """
    with open(filename, "r") as fstream:
        return yaml.safe_load(fstream)


def read_evergreen_config() -> Optional[Dict]:
    """
    Search known location for the evergreen config file.

    :return: First found evergreen configuration.
    """
    for filename in [filename for filename in CONFIG_FILE_LOCATIONS if os.path.isfile(filename)]:
        return read_evergreen_from_file(filename)
    return None


def get_auth_from_config(config: Dict) -> EvgAuth:
    """
    Get the evergreen authentication from the specified config dict.

    :param config: Evergreen configuration.
    :return: Authentication information for evergreen.
    """
    return EvgAuth(config["user"], config["api_key"])


def get_auth() -> Optional[EvgAuth]:
    """
    Get the evergreen authentication object from the default locations. Convenience function.

    :return: Authentication information for evergreen.
    """
    conf = read_evergreen_config()
    if conf:
        return get_auth_from_config(conf)
    return None
