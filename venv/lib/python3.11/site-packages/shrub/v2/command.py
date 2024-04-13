"""Configuration for commands in shrub."""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional, Sequence, Union

from shrub.v2.dict_creation_util import add_if_exists, add_existing_from_dict

TimeoutType = Union[int, str]  # A timeout can an int or expansion.


class ScriptingHarness(Enum):
    """Scripting harness to use for subprocess.scripting."""

    PYTHON = "python"
    PYTHON2 = "python2"
    GOLANG = "golang"
    ROSWELL = "roswell"


class CommandType(Enum):
    """Type of error to raise on command failure."""

    TEST = "test"
    SYSTEM = "system"
    SETUP = "setup"


class S3Visibility(Enum):
    """Visibility of AWS S3 Uploads."""

    PUBLIC = "public"
    PRIVATE = "private"
    SIGNED = "signed"
    NONE = "none"


class ShrubCommand(ABC):
    """Base object for possible evergreen commands."""

    @abstractmethod
    def as_dict(self) -> Dict[str, Any]:
        """Get a dictionary representing this command."""


class FunctionCall(ShrubCommand):
    """Call to an Evergreen function."""

    def __init__(self, name: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        """
        Create a new FunctionCall.

        :param name: Name of function to call.
        :param parameters: Dictionary of parameters to pass to function.
        """
        self.name = name
        self.parameters = parameters

    def as_dict(self) -> Dict[str, Any]:
        """Get a dictionary representing this command."""
        obj = {
            "func": self.name,
        }

        return add_if_exists(obj, "vars", self.parameters)


class BuiltInCommand(ShrubCommand):
    """Base object for Evergreen's built in commands."""

    def __init__(self, command: str, params: Dict[str, Any]) -> None:
        """
        create a new Shrub command.

        :param command: Name of command.
        :param params: Parameters for command.
        """
        self.command = command
        self.params = params
        self.command_type: Optional[CommandType] = None

    def set_type(self, command_type: CommandType) -> "BuiltInCommand":
        """
        Set the type command should report as.

        The command type determines how a failure appears in the Evergreen UI.

        :param command_type: Type of command to report as.
        :return: This command.
        """
        self.command_type = command_type
        return self

    def as_dict(self) -> Dict[str, Any]:
        """Get a dictionary representing the command."""
        obj = {
            "command": self.command,
        }

        add_if_exists(obj, "params", self.params)

        if self.command_type:
            obj["type"] = self.command_type.value

        return obj


def archive_tarfz_extract(
    path: str, destination: str, exclude_files: Optional[str]
) -> BuiltInCommand:
    """
    Create an Evergreen command to extract files from a gzipped tarball.

    :param path: The path to the tarball.
    :param destination: The target directory.
    :param exclude_files: A list of filename blobs to exclude.
    :return: Evergreen command.
    """
    params = {
        "path": path,
        "destination": destination,
    }
    add_if_exists(params, "exclude_files", exclude_files)

    return BuiltInCommand("archive.targz_extract", params)


def archive_tarfz_pack(
    target: str, source_dir: str, include: Sequence[str], exclude_files: Optional[Sequence[str]]
) -> BuiltInCommand:
    """
    Create an Evergreen command to create a gzipped tarball.

    :param target: The tgz file that will be created.
    :param source_dir: The directory to compress.
    :param include: A list of blobs to include.
    :param exclude_files: A list of filename blobs to exclude.
    :return: Evergreen command.
    """
    params = {
        "target": target,
        "source_dir": source_dir,
        "include": include,
    }
    add_if_exists(params, "exclude_files", exclude_files)

    return BuiltInCommand("archive.targz_pack", params)


def attach_artifacts(files: Sequence[str], prefix: Optional[str]) -> BuiltInCommand:
    """
    Evergreen command to allow user to add artifacts to the running task.

    :param files: List of gitignore file globs to include.
    :param prefix: Path to start processing the files relative to the working directory.
    :return: Evergreen Command.
    """
    params = {
        "files": files,
    }
    add_if_exists(params, "prefix", prefix)

    return BuiltInCommand("attach.artifacts", params)


def attach_results(file_location: str) -> BuiltInCommand:
    """
    Create an Evergreen command to parse results in Evergreen's JSON test results format.

    :param file_location: A .json file to parse and upload.
    """
    params = {"file_location": file_location}

    return BuiltInCommand("attach.results", params)


def attach_xunit_results(file: str) -> BuiltInCommand:
    """
    Create an Evergreen command to parse results in XUnit test results format.

    :param file: A .json file to parse and upload.
    """
    params = {"file": file}

    return BuiltInCommand("attach.xunit_results", params)


def expansions_update(
    file: str, updates: Optional[Dict[str, str]] = None, ignore_missing_file: Optional[bool] = None
) -> BuiltInCommand:
    """
    Create an Evergreen command to Update the task's expansions at runtime.

    :param file: filename for a yaml file containing expansion updates.
    :param updates: Key values to add to expansions.
    :param ignore_missing_file: Do not error if the file is missing.
    """
    params = {
        "file": file,
    }
    add_if_exists(params, "updates", updates)
    add_if_exists(params, "ignore_missing_file", ignore_missing_file)

    return BuiltInCommand("expansions.update", params)


def expansions_write(file: str, redacted: Optional[bool] = None) -> BuiltInCommand:
    """
    Create an Evergreen command to write the task's expansions to a file.

    :param file: Filename to write expansions to.
    :param redacted: include redacted project variables.
    """
    params = {
        "file": file,
    }
    add_if_exists(params, "redacted", redacted)

    return BuiltInCommand("expansions.write", params)


def generate_tasks(files: Sequence[str]) -> BuiltInCommand:
    """
    Create an Evergreen command to dynamically create evergreen config from a json file.

    :param files: List of files to generate configuration from.
    """
    params = {
        "files": files,
    }

    return BuiltInCommand("generate.tasks", params)


def git_get_project(
    directory: str, token: Optional[str] = None, revisions: Optional[Dict[str, str]] = None
) -> BuiltInCommand:
    """
    Create an Evergreen command to clones the tracked project and check current revision.

    Also, applies patches if the task was created by a patch build.

    :param directory: Directory to clone into.
    :param token: Use a token to clone instead of ssh key.
    :param revisions: Map of revisions to use for modules.
    """
    params = {
        "directory": directory,
    }
    add_if_exists(params, "token", token)
    add_if_exists(params, "revisions", revisions)

    return BuiltInCommand("git.get_project", params)


def gotest_parse_files(files: Sequence[str]) -> BuiltInCommand:
    """
    Create an Evergreen command to parse go test results.

    :param files: a list of files (or blobs) to parse and upload.
    """
    params = {
        "files": files,
    }

    return BuiltInCommand("gotest.parse_files", params)


def json_send(file: str, name: str) -> BuiltInCommand:
    """
    Create an Evergreen command to save JSON-formatted task data.

    :param file: the JSON file to save to Evergreen's DB.
    :param name: Name of the file you're saving.
    """
    params = {
        "file": file,
        "name": name,
    }

    return BuiltInCommand("json.send", params)


def manifest_load() -> BuiltInCommand:
    """Create an Evergreen command to update the project's expansions with the manifest."""
    return BuiltInCommand("manifest.load", {})


def s3_get(
    aws_key: str,
    aws_secret: str,
    remote_file: str,
    bucket: str,
    local_file: Optional[str] = None,
    extract_to: Optional[str] = None,
    build_variants: Optional[Sequence[str]] = None,
) -> BuiltInCommand:
    """
    Create an Evergreen command to download a file from Amazon s3.

    :param aws_key: AWS key (use expansion to keep this a secret).
    :param aws_secret: AWS secret (use expansion to keep this a secret).
    :param local_file: The local file to save, do not use with extract_to.
    :param extract_to: The local directory to extract to, do not use with local_file.
    :param remote_file: The S3 path to get the file from.
    :param bucket: The S3 bucket to use.
    :param build_variants: List of build variants to run command for.
    """
    if bool(local_file) == bool(extract_to):
        raise TypeError("Either 'local_file' or 'extract_to' must be used, but not both.")

    params = {
        "aws_key": aws_key,
        "aws_secret": aws_secret,
        "remote_file": remote_file,
        "bucket": bucket,
    }
    add_existing_from_dict(
        params,
        {"local_file": local_file, "extract_to": extract_to, "build_variants": build_variants},
    )

    return BuiltInCommand("s3.get", params)


def s3_put(
    aws_key: str,
    aws_secret: str,
    remote_file: str,
    bucket: str,
    permissions: str,
    content_type: str,
    local_file: Optional[str] = None,
    optional: Optional[bool] = None,
    display_name: Optional[str] = None,
    local_files_include_filter: Optional[Sequence[str]] = None,
    local_files_include_filter_prefix: Optional[str] = None,
    visibility: Optional[S3Visibility] = None,
) -> BuiltInCommand:
    """
    Upload a file to Amazon S3.

    :param aws_key: AWS key (use expansion to keep this a secret).
    :param aws_secret: AWS secret (use expansion to keep this a secret).
    :param local_file: The local file to save, do not use with extract_to.
    :param remote_file: The S3 path to get the file from.
    :param bucket: The S3 bucket to use.
    :param permissions: The permissions string to upload with.
    :param content_type: The MIME type of the file.
    :param optional: Indicates if failure to fine of upload will result in task failure.
    :param display_name: The display string for the file in Evergreen.
    :param local_files_include_filter: An array of globs to include in the put.
    :param local_files_include_filter_prefix: An optional start path for local_files_include_filter.
    :param visibility: Visibility of the uploaded files.
    """
    if local_files_include_filter:
        if local_file is not None:
            raise TypeError("'local_file' cannot be used with 'local_files_include_filter'")
        if optional is not None:
            raise TypeError("'optional' cannot be used with 'local_files_include_filter'")
    else:
        if local_file is None:
            raise TypeError("Either 'local_file' or 'local_files_include_filter' must be used")
        if local_files_include_filter_prefix is not None:
            raise TypeError("'local_file_include_filter_prefix' cannot be used with 'local_file'")

    params = {
        "aws_key": aws_key,
        "aws_secret": aws_secret,
        "remote_file": remote_file,
        "bucket": bucket,
        "content_type": content_type,
        "permissions": permissions,
    }
    add_existing_from_dict(
        params,
        {
            "local_file": local_file,
            "optional": optional,
            "display_name": display_name,
            "local_files_include_filter": local_files_include_filter,
            "local_files_include_filter_prefix": local_files_include_filter_prefix,
            "visibility": visibility,
        },
    )

    return BuiltInCommand("s3.put", params)


class S3CopyFile(object):
    """A file to be copied in AWS S3."""

    def __init__(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        display_name: Optional[str] = None,
        build_variants: Optional[Sequence[str]] = None,
        optional: Optional[bool] = None,
    ) -> None:
        """
        Create a description of how to copy a file in S3.

        :param source_bucket: S3 bucket where source file lives.
        :param source_path: Path to source file.
        :param destination_bucket: S3 bucket to copy file.
        :param destination_path: Path to copy file.
        :param display_name: Name to use for file in Evergreen.
        :param build_variants: List of build variants to perform operation.
        :param optional: If True, do not error file source file does not exist.
        """
        self.source_bucket = source_bucket
        self.source_path = source_path
        self.destination_bucket = destination_bucket
        self.destination_path = destination_path
        self.display_name = display_name
        self.build_variants = build_variants
        self.optional = optional

    def as_dict(self) -> Dict[str, Dict[str, str]]:
        """Get a dictionary representing this command."""
        obj = {
            "source": {"bucket": self.source_bucket, "path": self.source_path},
            "destination": {"bucket": self.destination_bucket, "path": self.destination_path},
        }
        add_existing_from_dict(
            obj,
            {
                "display_name": self.display_name,
                "optional": self.optional,
                "build_variants": self.build_variants,
            },
        )

        return obj


def s3_copy(aws_key: str, aws_secret: str, s3_copy_files: Sequence[S3CopyFile]) -> BuiltInCommand:
    """
    Create an Evergreen command to copy files from one s3 location to another.

    :param aws_key: Your AWS key.
    :param aws_secret: Your AWS secret.
    :param s3_copy_files: How to copy the files in S3.
    """
    params = {
        "aws_key": aws_key,
        "aws_secret": aws_secret,
        "s3_copy_files": [cf.as_dict() for cf in s3_copy_files],
    }

    return BuiltInCommand("s3Copy.copy", params)


def shell_exec(
    script: str,
    working_dir: Optional[str] = None,
    background: Optional[bool] = None,
    silent: Optional[bool] = None,
    continue_on_err: Optional[bool] = None,
    system_log: Optional[bool] = None,
    shell: Optional[str] = None,
    ignore_stdout: Optional[bool] = None,
    ignore_stderr: Optional[bool] = None,
    redirect_stderr_to_stdout: Optional[bool] = None,
) -> BuiltInCommand:
    """
    Create an Evergreen command to parse go test results.

    :param script: the scripts to run.
    :param working_dir: the directory to execute the shell script in.
    :param background: If set to true does not wait for script to exit before running the next
        command.
    :param silent: If set to true does not log any shell output during execution.
    :param continue_on_err: if set to true causes command to exit with success regardless of the
        script's exit code.
    :param system_log: If set to true the script's output will be written to the task's system
        logs.
    :param shell: Shell to use, defaults to sh.
    :param ignore_stdout: If true discard output sent to stdout.
    :param ignore_stderr: If true discard output sent to stderr.
    :param redirect_stderr_to_stdout: If true sends stderr to stdout.
    """
    params = {
        "script": script,
    }
    add_existing_from_dict(
        params,
        {
            "working_dir": working_dir,
            "background": background,
            "silent": silent,
            "continue_on_err": continue_on_err,
            "system_log": system_log,
            "shell": shell,
            "ignore_standard_out": ignore_stdout,
            "ignore_standard_error": ignore_stderr,
            "redirect_standard_error_to_output": redirect_stderr_to_stdout,
        },
    )

    return BuiltInCommand("shell.exec", params)


def subprocess_exec(
    binary: Optional[str] = None,
    args: Optional[Sequence[str]] = None,
    env: Optional[Dict[str, str]] = None,
    command: Optional[str] = None,
    background: Optional[bool] = None,
    silent: Optional[bool] = None,
    system_log: Optional[bool] = None,
    working_dir: Optional[str] = None,
    ignore_stdout: Optional[bool] = None,
    ignore_stderr: Optional[bool] = None,
    redirect_stderr_to_stdout: Optional[bool] = None,
    continue_on_err: Optional[bool] = None,
    add_expansions_to_env: Optional[bool] = None,
    include_expansions_in_env: Optional[Sequence[str]] = None,
    add_to_path: Optional[Sequence[str]] = None,
) -> BuiltInCommand:
    """
    Create an Evergreen command to run a shell command.

    :param binary: A binary to run.
    :param args: List of arguments to the binary.
    :param env: Map of environment variables and their values.
    :param command: A command string (cannot use with `binary` or `args`).
    :param background: If True immediately return to caller.
    :param silent: Do not log output of command.
    :param continue_on_err: if set to true causes command to exit with success regardless of the
        script's exit code.
    :param system_log: If set to true the script's output will be written to the task's system
        logs.
    :param working_dir: Working directory to start shell in.
    :param ignore_stdout: If true discard output sent to stdout.
    :param ignore_stderr: If true discard output sent to stderr.
    :param redirect_stderr_to_stdout: If True sends stderr to stdout.
    :param add_expansions_to_env: If True add all expansions to the command's environment.
    :param include_expansions_in_env: Specify expansions to add to the command's environment.
    :param add_to_path: Paths to prepend to the PATH environment variable.
    """
    if bool(binary) == bool(command):
        raise TypeError("Either 'binary' or 'command' must be specified but not both.")

    if args and command:
        raise TypeError("'args' cannot be specified with 'command'.")

    params: Dict[str, Any] = {}
    add_existing_from_dict(
        params,
        {
            "binary": binary,
            "args": args,
            "env": env,
            "command": command,
            "working_dir": working_dir,
            "background": background,
            "silent": silent,
            "continue_on_err": continue_on_err,
            "system_log": system_log,
            "ignore_standard_out": ignore_stdout,
            "ignore_standard_error": ignore_stderr,
            "redirect_standard_error_to_output": redirect_stderr_to_stdout,
            "add_expansions_to_env": add_expansions_to_env,
            "include_expansions_in_env": include_expansions_in_env,
            "add_to_path": add_to_path,
        },
    )

    return BuiltInCommand("subprocess.exec", params)


def subprocess_scripting(
    harness: ScriptingHarness,
    command: Optional[str] = None,
    args: Optional[Sequence[str]] = None,
    cache_duration_secs: Optional[int] = None,
    cleanup_harness: Optional[bool] = None,
    lock_file: Optional[str] = None,
    packages: Optional[Sequence[str]] = None,
    harness_path: Optional[str] = None,
    silent: Optional[bool] = None,
    system_log: Optional[bool] = None,
    working_dir: Optional[str] = None,
    ignore_stdout: Optional[bool] = None,
    ignore_stderr: Optional[bool] = None,
    redirect_stderr_to_stdout: Optional[bool] = None,
    continue_on_err: Optional[bool] = None,
    add_expansions_to_env: Optional[bool] = None,
    include_expansions_in_env: Optional[Sequence[str]] = None,
    add_to_path: Optional[Sequence[str]] = None,
) -> BuiltInCommand:
    """
    Create an Evergreen command to run a command inside of a script environment.

    :param harness: Type of scripting harness to use.
    :param command: Command line arguments.
    :param args: List of strings to run as a command in the environment.
    :param cache_duration_secs: The duration to cache the configuration.
    :param cleanup_harness: If True cleanup harness after command exits.
    :param lock_file: Specifies dependencies in a lock file.
    :param packages: List of packages that will be installed in the environment.
    :param harness_path: Path within working directory where harness will be located.
    :param silent: Do not log output of command.
    :param continue_on_err: if set to true causes command to exit with success regardless of the
        script's exit code.
    :param system_log: If set to true the script's output will be written to the task's system
        logs.
    :param working_dir: Working directory to start shell in.
    :param ignore_stdout: If true discard output sent to stdout.
    :param ignore_stderr: If true discard output sent to stderr.
    :param redirect_stderr_to_stdout: If True sends stderr to stdout.
    :param add_expansions_to_env: If True add all expansions to the command's environment.
    :param include_expansions_in_env: Specify expansions to add to the command's environment.
    :param add_to_path: Paths to prepend to the PATH environment variable.
    """
    params = {
        "harness": harness.value,
    }
    add_existing_from_dict(
        params,
        {
            "command": command,
            "args": args,
            "cache_duration_secs": cache_duration_secs,
            "cleanup_harness": cleanup_harness,
            "lock_file": lock_file,
            "packages": packages,
            "harness_path": harness_path,
            "working_dir": working_dir,
            "silent": silent,
            "continue_on_err": continue_on_err,
            "system_log": system_log,
            "ignore_standard_out": ignore_stdout,
            "ignore_standard_error": ignore_stderr,
            "redirect_standard_error_to_output": redirect_stderr_to_stdout,
            "add_expansions_to_env": add_expansions_to_env,
            "include_expansions_in_env": include_expansions_in_env,
            "add_to_path": add_to_path,
        },
    )

    return BuiltInCommand("subprocess.scripting", params)


def timeout_update(
    exec_timeout_secs: Optional[TimeoutType] = None, timeout_secs: Optional[TimeoutType] = None
) -> BuiltInCommand:
    """
    Create an Evergreen command to set the times of a task from within the task.

    :param exec_timeout_secs: Maximum amount of time a task may run.
    :param timeout_secs: Maximum amount of time a task may run without any output to stdout.
    """
    params: Dict[str, Any] = {}
    add_if_exists(params, "exec_timeout_secs", exec_timeout_secs)
    add_if_exists(params, "timeout_secs", timeout_secs)

    return BuiltInCommand("timeout.update", params)
