"""Common sub metrics that are used internally at MongoDB."""

import argparse
import configparser
from datetime import datetime
import multiprocessing
import optparse
import os
import socket
import sys
from typing import Any, Dict, List, Optional, Union

import distro
import git
from mongo_tooling_metrics.base_metrics import MongoMetrics


class HostInfo(MongoMetrics):
    """Class to store host information."""

    ip_address: Optional[str]
    host_os: str
    num_cores: int
    memory: Optional[float]

    @classmethod
    def generate_metrics(cls):  # pylint: disable=arguments-differ
        """Get the host info to the best of our ability."""
        try:
            ip_address = socket.gethostbyname(socket.gethostname())
        except:
            ip_address = None
        try:
            memory = cls._get_memory()
        except:
            memory = None
        return cls(
            ip_address=ip_address,
            host_os=distro.name(pretty=True),
            num_cores=multiprocessing.cpu_count(),
            memory=memory,
        )

    @staticmethod
    def _get_memory():
        """Get total memory of the host system."""
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024.**3)

    def is_malformed(self) -> bool:
        """Confirm whether this instance has all expected fields."""
        return None in [self.memory, self.ip_address]


MODULES_FILEPATH = 'src/mongo/db/modules'
CURRENT_DIR = '.'


class GitInfo(MongoMetrics):
    """Class to store git repo information."""

    filepath: str
    commit_hash: Optional[str]
    branch_name: Optional[str]
    repo_name: Optional[str]

    @classmethod
    def generate_metrics(cls, filepath: str = CURRENT_DIR):
        """Get the git info for a repo to the best of our ability."""
        try:
            commit_hash = git.Repo(filepath).head.commit.hexsha
        except:
            commit_hash = None
        try:
            if git.Repo(filepath).head.is_detached:
                branch_name = commit_hash
            else:
                branch_name = git.Repo(filepath).active_branch.name
        except:
            branch_name = None
        try:
            repo_name = git.Repo(filepath).working_tree_dir.split("/")[-1]
        except:
            repo_name = None
        return cls(
            filepath=filepath,
            commit_hash=commit_hash,
            branch_name=branch_name,
            repo_name=repo_name,
        )

    @classmethod
    def modules_generate_metrics(cls, modules_filepath: str = MODULES_FILEPATH):
        """Get git info for all modules in the path."""
        modules_git_info = []
        try:
            modules_git_info = [
                cls.generate_metrics(os.path.join(modules_filepath, module))
                for module in os.listdir(modules_filepath)
                if os.path.isdir(os.path.join(modules_filepath, module))
            ]
        except:
            pass
        return modules_git_info

    def is_malformed(self) -> bool:
        """Confirm whether this instance has all expected fields."""
        return None in [self.commit_hash, self.branch_name, self.repo_name]


class BuildInfo(MongoMetrics):
    """Class to store build artifact data."""

    build_artifacts: Optional[List[str]]
    artifact_dir: Optional[str]

    @classmethod
    def generate_metrics(
        cls,
        utc_starttime: datetime,
        artifact_dir: str,
    ):
        """Get build artifact data to the best of our ability."""
        return cls(
            build_artifacts=cls.get_artifacts(utc_starttime, artifact_dir),
            artifact_dir=artifact_dir,
        )

    @staticmethod
    def get_artifacts(utc_starttime: datetime, artifact_dir: str) -> List[str]:
        """Search a directory recursively for all artifacts created after the given timestamp."""
        try:
            start_timestamp = datetime.timestamp(utc_starttime)
            artifacts = []
            for root, _, files in os.walk(artifact_dir):
                for file in files:
                    filepath = os.path.join(root, file)
                    _, ext = os.path.splitext(filepath)
                    if ext in ['.a', '.so', ''] and os.path.getmtime(filepath) >= start_timestamp:
                        artifacts.append(filepath)
            return artifacts
        except:
            return None

    def is_malformed(self) -> bool:
        """Confirm whether this instance has all expected fields."""
        return None in [self.artifact_dir, self.build_artifacts]


class CommandInfo(MongoMetrics):
    """Class to store command-line information."""

    command: List[str]
    options: Optional[Dict[str, Any]]
    positional_args: Optional[List[str]]

    @classmethod
    def generate_metrics(cls, parser: Union[argparse.ArgumentParser, optparse.OptionParser]):
        """Get command line information to the best of our ability."""
        try:
            # The SCons parser is of type 'optparse.OptionParser' which does not
            # have 'parse_known_args' but has 'parse_args' -- which has the same effect
            known_args, unknown_args = parser.parse_known_args(sys.argv[1:]) if isinstance(
                parser, argparse.ArgumentParser) else parser.parse_args(sys.argv[1:])
            return cls(command=sys.argv, options=vars(known_args), positional_args=unknown_args)
        except:
            return cls(command=sys.argv, options=None, positional_args=None)

    def is_malformed(self) -> bool:
        """Confirm whether this instance has all expected fields."""
        return None in [self.options, self.positional_args]


SCONS_ENV_FILE = "scons_env.env"
SCONS_SECTION_HEADER = "SCONS_ENV"


class SConsInfo(MongoMetrics):
    """Class to store the SCons specific information."""

    env: Optional[Dict[str, Any]]

    @classmethod
    def generate_metrics(
        cls,
        artifact_dir: str,
        env_vars: "SCons.Variables.Variables",
        env: "SCons.Script.SConscript.SConsEnvironment",
    ):
        """Get SCons info to the best of our ability."""
        return cls(env=cls._get_scons_env_vars_dict(artifact_dir, env_vars, env))

    @staticmethod
    def _get_scons_env_vars_dict(
        artifact_dir: str,
        env_vars: "SCons.Variables.Variables",
        env: "SCons.Script.SConscript.SConsEnvironment",
    ) -> Optional[Dict[str, Any]]:
        """Get the environment variables options that can be set by users."""

        scons_env_filepath = os.path.join(artifact_dir, SCONS_ENV_FILE)
        try:
            # Use SCons built-in method to save environment variables to a file
            env_vars.Save(scons_env_filepath, env)

            # Add a section header to the file so we can easily parse with ConfigParser
            with open(scons_env_filepath, 'r') as original:
                data = original.read()
            with open(scons_env_filepath, 'w') as modified:
                modified.write(f"[{SCONS_SECTION_HEADER}]\n" + data)

            # Parse file using config parser
            config = configparser.ConfigParser()
            config.read(scons_env_filepath)
            str_dict = dict(config[SCONS_SECTION_HEADER])
            return {key: eval(val) for key, val in str_dict.items()}  # pylint: disable=eval-used
        except:
            return None

    def is_malformed(self) -> bool:
        """Confirm whether this instance has all expected fields."""
        return self.env is None
