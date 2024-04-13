# -*- encoding: utf-8 -*-
"""Host representation of evergreen."""
from __future__ import absolute_import

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from evergreen.base import _BaseEvergreenObject, evg_attrib

if TYPE_CHECKING:
    from evergreen.api import EvergreenApi

AWS_AUTO_PROVIDER = "ec2-auto"
AWS_ON_DEMAND_PROVIDER = "ec2-ondemand"
DOCKER_PROVIDER = "docker"
STATIC_PROVIDER = "static"


class MountPoint(_BaseEvergreenObject):
    """Representation of Mount Point in distro settings."""

    device_name = evg_attrib("device_name")
    size = evg_attrib("size")
    virtual_name = evg_attrib("virtual_name")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of the mount point.

        :param json: Json of mount point.
        :param api: Evergreen API.
        """
        super(MountPoint, self).__init__(json, api)


class StaticDistroSettings(_BaseEvergreenObject):
    """Representation of Evergreen static distro settings."""

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of the distro settings for static images.

        :param json: Json of distro settings.
        :param api: Evergreen API.
        """
        super(StaticDistroSettings, self).__init__(json, api)

    @property
    def hosts(self) -> List[str]:
        """
        Retrieve Hosts of static distro.

        :return: hosts of static distro.
        """
        if "hosts" in self.json:
            return [host["name"] for host in self.json["hosts"]]
        return []


class DockerDistroSettings(_BaseEvergreenObject):
    """Representation of docker distro settings."""

    image_url = evg_attrib("image_url")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of the distro settings for docker.

        :param json: Json of distro settings.
        :param api: Evergreen API.
        """
        super(DockerDistroSettings, self).__init__(json, api)


class AwsDistroSettings(_BaseEvergreenObject):
    """Representation of AWS Distro Settings."""

    ami = evg_attrib("ami")
    aws_access_key_id = evg_attrib("aws_access_key_id")
    aws_access_secret_id = evg_attrib("aws_access_secret_id")
    bid_price = evg_attrib("bid_price")
    instance_type = evg_attrib("instance_type")
    ipv6 = evg_attrib("ipv6")
    is_vpc = evg_attrib("is_vpc")
    key_name = evg_attrib("key_name")
    region = evg_attrib("region")
    security_group = evg_attrib("security_group")
    security_group_ids = evg_attrib("security_group_ids")
    subnet_id = evg_attrib("subnet_id")
    user_data = evg_attrib("user_data")
    vpc_name = evg_attrib("vpc_name")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of the distro settings.

        :param json: Json of the distro settings.
        :param api: Evergreen API.
        """
        super(AwsDistroSettings, self).__init__(json, api)

    @property
    def mount_points(self) -> List[MountPoint]:
        """
        Retrieve list of mount points for the distro.

        :return: List of mount points.
        """
        if "mount_points" in self.json:
            return [MountPoint(mp, self._api) for mp in self.json["mount_points"]]
        return []


class PlannerSettings(_BaseEvergreenObject):
    """Representation of planner settings."""

    version = evg_attrib("version")
    minimum_hosts = evg_attrib("minimum_hosts")
    maximum_hosts = evg_attrib("maximum_hosts")
    target_time = evg_attrib("target_time")
    acceptable_host_idle_time = evg_attrib("acceptable_host_idle_time")
    group_versions = evg_attrib("group_versions")
    patch_zipper_factor = evg_attrib("patch_zipper_factor")
    task_ordering = evg_attrib("task_ordering")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of planner settings for a distro.

        :param json: planner settings json.
        :param api: Evergreen API.
        """
        super(PlannerSettings, self).__init__(json, api)


class FinderSettings(_BaseEvergreenObject):
    """Representation of finder settings."""

    version = evg_attrib("version")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of finder settings for a distro.

        :param json: finder settings json.
        :param api: Evergreen API.
        """
        super(FinderSettings, self).__init__(json, api)


class Distro(_BaseEvergreenObject):
    """Representation of an Evergreen Distro."""

    _PROVIDER_MAP = {
        AWS_ON_DEMAND_PROVIDER: AwsDistroSettings,
        AWS_AUTO_PROVIDER: AwsDistroSettings,
        DOCKER_PROVIDER: DockerDistroSettings,
        STATIC_PROVIDER: StaticDistroSettings,
    }

    name = evg_attrib("name")
    user_spawn_allowed = evg_attrib("user_spawn_allowed")
    provider = evg_attrib("provider")
    image_id = evg_attrib("image_id")
    arch = evg_attrib("arch")
    work_dir = evg_attrib("work_dir")
    pool_size = evg_attrib("pool_size")
    setup_as_sudo = evg_attrib("setup_as_sudo")
    setup = evg_attrib("setup")
    teardown = evg_attrib("teardown")
    user = evg_attrib("user")
    bootstrap_method = evg_attrib("bootstrap_method")
    communication_method = evg_attrib("communication_method")
    clone_method = evg_attrib("clone_method")
    shell_path = evg_attrib("shell_path")
    curator_dir = evg_attrib("curator_dir")
    client_dir = evg_attrib("client_dir")
    jasper_credentials_path = evg_attrib("jasper_credentials_path")
    ssh_key = evg_attrib("ssh_key")
    ssh_options = evg_attrib("ssh_options")
    disabled = evg_attrib("disabled")
    container_pool = evg_attrib("container_pool")

    def __init__(self, json: Dict[str, Any], api: "EvergreenApi") -> None:
        """
        Create an instance of a distro.

        :param json: Json of a distro.
        :param api: Evergreen API.
        """
        super(Distro, self).__init__(json, api)
        self._expansions_dict: Optional[Dict[str, str]] = None

    @property
    def settings(self) -> Optional[Any]:
        """
        Retrieve the settings for the distro.

        :return: settings for distro.
        """
        if "settings" in self.json:
            if self.provider in self._PROVIDER_MAP:
                return self._PROVIDER_MAP[self.provider](self.json["settings"], self._api)
            return self.json["settings"]
        return None

    @property
    def expansions(self) -> Optional[Dict[str, str]]:
        """
        Retrieve dict of expansions for distro.

        :return: dict of expansions.
        """
        if not self._expansions_dict and "expansions" in self.json:
            self._expansions_dict = {exp["key"]: exp["value"] for exp in self.json["expansions"]}
        return self._expansions_dict

    @property
    def planner_settings(self) -> PlannerSettings:
        """
        Retrieve planner settings for distro.

        :return: planner settings.
        """
        return PlannerSettings(self.json["planner_settings"], self._api)

    @property
    def finder_settings(self) -> FinderSettings:
        """
        Retrieve finder settings for distro.

        :return: finder settings.
        """
        return FinderSettings(self.json["finder_settings"], self._api)
