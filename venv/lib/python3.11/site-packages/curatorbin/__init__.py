'''curatorbin serves as a python wrapper for curator
(https://github.com/mongodb/curator)

get_curator_path: Returns path to curator binary, or raises error
run_curator: Passes arguments through to curator'''

import os
import platform
import re
import subprocess
import sys


class OSPlatform(object):
    """Mapping of curator OS to Evergreen build variant name."""
    MACOS_x64 = "macos"
    MACOS_arm64 = "macos-arm64"
    WINDOWS_x64 = "windows-64"
    LINUX_x64 = "ubuntu"
    LINUX_arm64 = "arm"


def get_curator_path():
    """
    returns path to curator binary, after checking it exists and matches
    the hardcoded git hash. If this is not the case, it raises an error.
    """
    current_module = __import__(__name__)
    build_path = current_module.__path__[0]

    os_platform = None
    processor_info = "processor info unavailable"

    if sys.platform == "darwin":
        command = ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"]
        processor_info = subprocess.check_output(command).decode().strip()
        if "Intel" in processor_info:
            os_platform = OSPlatform.MACOS_x64
        elif "Apple" in processor_info:  # E.g. Apple M1, Apple M2.
            os_platform = OSPlatform.MACOS_arm64

    elif sys.platform == "win32":
        processor_info = platform.processor()
        if "Intel" in processor_info:
            os_platform = OSPlatform.WINDOWS_x64

    elif sys.platform.startswith("linux"):
        command = "cat /proc/cpuinfo"
        processor_info = subprocess.check_output(command, shell=True).decode().strip()

        if "Intel" in processor_info:
            os_platform = OSPlatform.LINUX_x64
        elif re.search("CPU implementer.*0x41", processor_info) is not None:
            os_platform = OSPlatform.LINUX_arm64

    if os_platform is None:
        raise OSError("Unrecognized OS and/or CPU architecture. OS: {}, Processor info: {}".format(
            sys.platform, processor_info))

    curator_path = os.path.join(build_path, os_platform, "curator")
    if sys.platform == "win32":
        curator_path += ".exe"
    git_hash = "2230334f0369ea999b8fd2ada0de61e4b4a6e2b0"
    curator_exists = os.path.isfile(curator_path)

    if curator_exists:
        curator_version = subprocess.check_output([curator_path,
                                                   "--version"]).decode('utf-8').split()
        curator_same_version = git_hash in curator_version

        if curator_same_version :
            return curator_path

        errmsg = ("Found a different version of curator. "
            "Looking for '{}', but found '{}'. Something has gone terribly wrong"
            " in the python wrapper for curator").format(git_hash, curator_version)
        raise RuntimeError(errmsg)

    else:
        raise FileNotFoundError("Something has gone terribly wrong."
            "curator binary not found at '{}'".format(curator_path))


def run_curator(*args):
    """runs the curator binary packaged with this module, passing along any arguments."""
    curator_path = get_curator_path()
    return subprocess.check_output([curator_path, *args])

