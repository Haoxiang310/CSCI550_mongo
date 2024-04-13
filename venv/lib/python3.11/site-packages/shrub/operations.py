import abc

from shrub.command import CommandDefinition


ARCHIVE_FORMAT_ZIP = "zip"
ARCHIVE_FORMAT_TAR = "tarball"
ARCHIVE_FORMAT_AUTO = "auto"
MIN_N_HOSTS = 1
MAX_N_HOSTS = 10
TIMEOUT_SETUP_MIN = 60
TIMEOUT_SETUP_MAX = 600
TIMEOUT_TEARDOWN_MIN = 60
TIMEOUT_TEARDOWN_MAX = 604800


def _is_outside_of(value, min, max):
    if value < min or value > max:
        return True

    return False


class EvergreenCommand:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def validate(self):
        """Ensure the command is valid."""

    @abc.abstractmethod
    def _command_type(self):
        """Return the type of this command."""

    @abc.abstractmethod
    def _param_list(self):
        """Return a map of the params supported by this command."""

    def _export_params(self):
        """Return a map of the parameters for this command."""
        obj = {}
        self._add_defined_attribs(obj, self._param_list().keys())
        return obj

    def _add_if_defined(self, obj, prop):
        """Add the specified property to the given object if it exists."""
        value = getattr(self, prop)
        if value:
            obj[self._param_list()[prop]] = value

    def _add_defined_attribs(self, obj, attrib_list):
        """Add any defined attributes in the given list to the given map."""
        for attrib in attrib_list:
            self._add_if_defined(obj, attrib)

    def resolve(self):
        """Create a CommandDefinition from this object."""
        cmd = CommandDefinition().command(self._command_type())
        return cmd.params(self._export_params())


class CmdExpansionsUpdate(EvergreenCommand):
    def __init__(self):
        self._updates = {}
        self._file = None
        self._ignore_missing_file = False

    def _command_type(self):
        return "expansions.update"

    def validate(self):
        return self

    def _param_list(self):
        return {
            "_updates": "updates",
            "_file": "file",
            "_ignore_missing_file": "ignore_missing_file",
        }

    def update(self, k, v):
        if not isinstance(k, str):
            raise TypeError("update expects a str")

        self._updates[k] = v
        return self

    def updates(self, kvs):
        if not isinstance(kvs, dict):
            raise TypeError("updates expects a dict")

        for k in kvs:
            self.update(k, kvs[k])

        return self

    def file(self, f):
        if not isinstance(f, str):
            raise TypeError("file expects a str")

        self._file = f
        return self

    def ignore_missing_file(self):
        self._ignore_missing_file = True
        return self


class CmdExpansionsWrite(EvergreenCommand):
    def __init__(self):
        self._file = None
        self._redacted = False

    def _command_type(self):
        return "expansions.write"

    def validate(self):
        return self

    def _param_list(self):
        return {"_file": "file", "_redacted": "redacted"}

    def file(self, f):
        if not isinstance(f, str):
            raise TypeError("file expects a str")

        self._file = f
        return self

    def redacted(self):
        self._redacted = True
        return self


class CmdExec(EvergreenCommand):
    def __init__(self):
        self._background = False
        self._silent = False
        self._continue_on_err = False
        self._system_log = False
        self._combine_output = False
        self._ignore_stderr = False
        self._ignore_stdout = False
        self._keep_empty_args = False
        self._working_dir = None
        self._command = None
        self._binary = None
        self._args = []
        self._env = {}

    def _command_type(self):
        return "subprocess.exec"

    def validate(self):
        return self

    def _param_list(self):
        return {
            "_background": "background",
            "_silent": "silent",
            "_continue_on_err": "continue_on_err",
            "_system_log": "system_log",
            "_combine_output": "redirect_standard_error_to_output",
            "_ignore_stderr": "ignore_standard_error",
            "_ignore_stdout": "ignore_standard_out",
            "_working_dir": "working_dir",
            "_command": "command",
            "_binary": "binary",
            "_args": "args",
            "_env": "env",
        }

    def background(self, background):
        self._background = background
        return self

    def silent(self, silent):
        self._silent = silent
        return self

    def continue_on_err(self, cont):
        self._continue_on_err = cont
        return self

    def system_log(self, log):
        self._system_log = log
        return self

    def combine_output(self, combine):
        self._combine_output = combine
        return self

    def ignore_stderr(self, ignore):
        self._ignore_stderr = ignore
        return self

    def ignore_stdout(self, ignore):
        self._ignore_stdout = ignore
        return self

    def working_dir(self, working_dir):
        self._working_dir = working_dir
        return self

    def command(self, command):
        self._command = command
        return self

    def binary(self, binary):
        self._binary = binary
        return self

    def arg(self, arg):
        self._args.append(arg)
        return self

    def args(self, args):
        self._args += args
        return self

    def env(self, k, v):
        self._env[k] = v
        return self

    def envs(self, kvs):
        for k in kvs:
            self._env[k] = kvs[k]

        return self


class CmdGenerateTasks(EvergreenCommand):
    def __init__(self):
        self._files = []

    def _command_type(self):
        return "generate.tasks"

    def validate(self):
        return self

    def _param_list(self):
        return {"_files": "files"}

    def file(self, f):
        if not isinstance(f, str):
            raise TypeError("file expects str")

        self._files.append(f)
        return self


class CmdExecShell(EvergreenCommand):
    def __init__(self):
        self._background = False
        self._silent = False
        self._continue_on_err = False
        self._system_log = False
        self._combine_output = False
        self._ignore_stderr = False
        self._ignore_stdout = False
        self._working_directory = None
        self._script = None

    def _command_type(self):
        return "shell.exec"

    def validate(self):
        return self

    def _param_list(self):
        return {
            "_background": "background",
            "_silent": "silent",
            "_continue_on_err": "continue_on_err",
            "_system_log": "system_log",
            "_combine_output": "redirect_standard_error_to_output",
            "_ignore_stderr": "ignore_standard_error",
            "_ignore_stdout": "ignore_standard_out",
            "_working_directory": "working_dir",
            "_script": "script",
        }

    def background(self, background):
        self._background = background
        return self

    def silent(self, silent):
        self._silent = silent
        return self

    def continue_on_err(self, cont):
        self._continue_on_err = cont
        return self

    def system_log(self, log):
        self._system_log = log
        return self

    def combine_output(self, combine):
        self._combine_output = combine
        return self

    def ignore_stderr(self, ignore):
        self._ignore_stderr = ignore
        return self

    def ignore_stdout(self, ignore):
        self._ignore_stdout = ignore
        return self

    def working_dir(self, working_dir):
        self._working_directory = working_dir
        return self

    def script(self, script):
        self._script = script
        return self


class CmdTimeoutUpdate(EvergreenCommand):
    def __init__(self):
        self._exec_timeout = None
        self._timeout = None

    def _command_type(self):
        return "timeout.update"

    def validate(self):
        return self

    def _param_list(self):
        return {"_exec_timeout": "exec_timeout_secs", "_timeout": "timeout_secs"}

    def exec_timeout(self, timeout):
        if not isinstance(timeout, int):
            raise TypeError("Expected timeout to be int")

        self._exec_timeout = timeout
        return self

    def timeout(self, timeout):
        if not isinstance(timeout, int):
            raise TypeError("Expected timeout to be int")

        self._timeout = timeout
        return self


class CmdS3Put(EvergreenCommand):
    def __init__(self):
        self._optional = False
        self._local_file = None
        self._local_file_include_filter = []
        self._bucket = None
        self._remote_file = None
        self._display_name = None
        self._content_type = None
        self._aws_key = None
        self._aws_secret = None
        self._permissions = None
        self._visibility = None
        self._build_variants = []

    def _command_type(self):
        return "s3.put"

    def validate(self):
        if not self._aws_key and not self._aws_secret:
            raise ValueError("must specify aws credentials")

        if not self._local_file and len(self._local_file_include_filter) == 0:
            raise ValueError("must specify a local file to upload")

        return self

    def _param_list(self):
        return {
            "_optional": "optional",
            "_local_file": "local_file",
            "_local_file_include_filter": "local_file_include_filter",
            "_bucket": "bucket",
            "_remote_file": "remote_file",
            "_display_name": "display_name",
            "_content_type": "content_type",
            "_aws_key": "aws_key",
            "_aws_secret": "aws_secret",
            "_permissions": "permissions",
            "_visibility": "visibility",
            "_build_variants": "build_variants",
        }

    def optional(self, opt):
        self._optional = opt
        return self

    def local_file(self, file):
        self._local_file = file
        return self

    def include_filter(self, f):
        self._local_file_include_filter.append(f)
        return self

    def include_filters(self, filters):
        self._local_file_include_filter += filters
        return self

    def bucket(self, b):
        self._bucket = b
        return self

    def remote_file(self, f):
        self._remote_file = f
        return self

    def display_name(self, name):
        self._display_name = name
        return self

    def content_type(self, ct):
        self._content_type = ct
        return self

    def aws_key(self, key):
        self._aws_key = key
        return self

    def aws_secret(self, secret):
        self._aws_secret = secret
        return self

    def permissions(self, perm):
        self._permissions = perm
        return self

    def visibility(self, vis):
        self._visibility = vis
        return self

    def build_variant(self, bv):
        self._build_variants.append(bv)
        return self

    def build_variants(self, bvs):
        self._build_variants += bvs
        return self


class CmdS3Get(EvergreenCommand):
    def __init__(self):
        self._aws_key = None
        self._aws_secret = None
        self._remote_file = None
        self._bucket = None
        self._local_file = None
        self._extract_to = None
        self._build_variants = []

    def _command_type(self):
        return "s3.get"

    def validate(self):
        return self

    def _param_list(self):
        return {
            "_aws_key": "aws_key",
            "_aws_secret": "aws_secret",
            "_remote_file": "remote_file",
            "_bucket": "bucket",
            "_local_file": "local_file",
            "_extract_to": "extract_to",
            "_build_variants": "build_variants",
        }

    def aws_key(self, key):
        self._aws_key = key
        return self

    def aws_secret(self, secret):
        self._aws_secret = secret
        return self

    def remote_file(self, file):
        self._remote_file = file
        return self

    def bucket(self, bucket):
        self._bucket = bucket
        return self

    def local_file(self, file):
        self._local_file = file
        return self

    def extract_to(self, to):
        self._extract_to = to
        return self

    def build_variant(self, bv):
        self._build_variants.append(bv)
        return self

    def build_variants(self, bvs):
        self._build_variants += bvs
        return self


class AwsCopyFile:
    def __init__(self):
        self._optional = False
        self._display_name = None
        self._build_variants = []
        self._source = {}
        self._destination = {}

    def optional(self, opt):
        self._optional = opt
        return self

    def display_name(self, name):
        self._display_name = name
        return self

    def build_variant(self, bv):
        self._build_variants.append(bv)
        return self

    def build_variants(self, bvs):
        self._build_variants += bvs
        return self

    def source(self, bucket, path):
        self._source = {"bucket": bucket, "path": path}
        return self

    def destination(self, bucket, path):
        self._destination = {"bucket": bucket, "path": path}
        return self

    def to_map(self):
        obj = {}
        if self._optional:
            obj["optional"] = self._optional
        if self._display_name:
            obj["display_name"] = self._display_name
        if self._build_variants:
            obj["build_variants"] = self._build_variants
        if self._source:
            obj["source"] = self._source
        if self._destination:
            obj["destination"] = self._destination

        return obj


class CmdS3Copy(EvergreenCommand):
    def __init__(self):
        self._aws_key = None
        self._aws_secret = None
        self._files = []
        self._optional = None

    def _command_type(self):
        return "s3Copy.copy"

    def validate(self):
        return self

    def _param_list(self):
        return {"_aws_key": "aws_key", "_aws_secret": "aws_secret", "_optional": "optional"}

    def _export_params(self):
        obj = super(CmdS3Copy, self)._export_params()
        if self._files:
            obj["s3_copy_files"] = [f.to_map() for f in self._files]

        return obj

    def aws_key(self, key):
        self._aws_key = key
        return self

    def aws_secret(self, secret):
        self._aws_secret = secret
        return self

    def file(self, file):
        self._files.append(file)
        return self

    def files(self, files):
        self._files += files
        return self

    def optional(self):
        self._optional = True
        return self


class CmdGetProject(EvergreenCommand):
    def __init__(self):
        self._token = None
        self._dir = None
        self._revisions = {}

    def _command_type(self):
        return "git.get_project"

    def validate(self):
        return self

    def _param_list(self):
        return {"_token": "token", "_dir": "directory", "_revisions": "revisions"}

    def token(self, token):
        self._token = token
        return self

    def directory(self, dir):
        self._dir = dir
        return self

    def revision(self, k, v):
        self._revisions[k] = v
        return self

    def revisions(self, revs):
        for k in revs:
            self._revisions[k] = revs[k]

        return self


class CmdHostCreate(EvergreenCommand):
    def __init__(self):
        self._ami = None
        self._aws_key_id = None
        self._aws_key_secret = None
        self._distro = None
        self._ebs_block_device = {}
        self._instance_type = None
        self._key_name = None
        self._num_hosts = None
        self._provider = "ec2"
        self._region = None
        self._retries = None
        self._scope = None
        self._security_groups = []
        self._spot = False
        self._subnet_id = None
        self._timeout_setup = None
        self._timeout_teardown = None
        self._userdata_file = None
        self._vpc_id = None

    def _command_type(self):
        return "host.create"

    def validate(self):
        if not self._ami and not self._distro:
            raise ValueError("Must set 'ami' or 'distro'")

        if self._ami:
            if self._distro:
                raise ValueError("Must not set both 'ami' and 'distro'")

            if not self._instance_type:
                raise ValueError("'instance_type' must be set if 'ami' is set")

            if not self._security_groups:
                raise ValueError("'security_group_ids' must be set if 'ami' is set")

            if not self._vpc_id:
                raise ValueError("'vpc' must be set if 'ami' is set")

        if self._aws_key_id:
            if not self._aws_key_secret:
                raise ValueError("'aws_secret' must be set if 'aws_id' is set")

            if not self._key_name:
                raise ValueError("'key_name' must be set if 'aws_id' is set")

        if self._aws_key_secret:
            if not self._aws_key_id:
                raise ValueError("'aws_id' must be set if 'aws_secret' is set")

        if self._key_name and not self._aws_key_id:
            raise ValueError("'key_name' must not be set if 'aws_id' or 'aws_secret' is not")

        return self

    def _param_list(self):
        return {
            "_ami": "ami",
            "_aws_key_id": "aws_access_key_id",
            "_aws_key_secret": "aws_secret_access_key",
            "_distro": "distro",
            "_ebs_block_device": "ebs_block_device",
            "_instance_type": "instance_type",
            "_key_name": "key_name",
            "_num_hosts": "num_hosts",
            "_provider": "provider",
            "_region": "region",
            "_retries": "retries",
            "_scope": "scope",
            "_security_groups": "security_group_ids",
            "_spot": "spot",
            "_subnet_id": "subnet_id",
            "_timeout_setup": "timeout_setup_secs",
            "_timeout_teardown": "timeout_teardown_secs",
            "_userdata_file": "userdata_file",
            "_vpc_id": "vpc_id",
        }

    def ami(self, ami):
        if not isinstance(ami, str):
            raise TypeError("Expected ami to be str")

        self._ami = ami
        return self

    def aws_id(self, aws_id):
        if not isinstance(aws_id, str):
            raise TypeError("Expected aws_id to be str")

        self._aws_key_id = aws_id
        return self

    def aws_secret(self, aws_secret):
        if not isinstance(aws_secret, str):
            raise TypeError("Expected aws_secret to be str")

        self._aws_key_secret = aws_secret
        return self

    def distro(self, distro):
        if not isinstance(distro, str):
            raise TypeError("Expected distro to be str")

        self._distro = distro
        return self

    def block_device(self, name, iops, size, snaphshot_id):
        self._ebs_block_device = {
            "device_name": name,
            "ebs_iops": iops,
            "ebs_size": size,
            "ebs_snapshot_id": snaphshot_id,
        }
        return self

    def instance_type(self, it):
        self._instance_type = it
        return self

    def key_name(self, key):
        self._key_name = key
        return self

    def num_hosts(self, n):
        if not isinstance(n, int):
            raise TypeError("Expected num_hosts to be an int")

        if _is_outside_of(n, MIN_N_HOSTS, MAX_N_HOSTS):
            raise ValueError(
                "Expected num_hosts to be between {} and {}".format(MIN_N_HOSTS, MAX_N_HOSTS)
            )

        self._num_hosts = n
        return self

    def provider(self, provider):
        if provider not in ["ec2"]:
            raise ValueError("provider must be 'ec2'")

        self._provider = provider
        return self

    def region(self, region):
        if not isinstance(region, str):
            raise TypeError("Expected region to be a str")

        self._region = region
        return self

    def retries(self, n):
        if not isinstance(n, int):
            raise TypeError("Expected retries to be an int")

        self._retries = n
        return self

    def scope(self, scope):
        if scope not in ["task", "build"]:
            raise ValueError("scope must be 'task' or 'build'")

        self._scope = scope
        return self

    def security_group_id(self, id):
        self._security_groups.append(id)
        return self

    def spot(self):
        self._spot = True
        return self

    def subnet_id(self, id):
        self._subnet_id = id
        return self

    def timeout_setup(self, timeout):
        if not isinstance(timeout, int):
            raise TypeError("Expected timeout to be int")

        if _is_outside_of(timeout, TIMEOUT_SETUP_MIN, TIMEOUT_SETUP_MAX):
            raise ValueError(
                "Expected timeout to be between {} and {}".format(
                    TIMEOUT_SETUP_MIN, TIMEOUT_SETUP_MAX
                )
            )

        self._timeout_setup = timeout
        return self

    def timeout_teardown(self, timeout):
        if not isinstance(timeout, int):
            raise TypeError("Expected timeout to be int")

        if _is_outside_of(timeout, TIMEOUT_TEARDOWN_MIN, TIMEOUT_TEARDOWN_MAX):
            raise ValueError(
                "Expected timeout to be between {} and {}".format(
                    TIMEOUT_TEARDOWN_MIN, TIMEOUT_TEARDOWN_MAX
                )
            )

        self._timeout_teardown = timeout
        return self

    def userdata_file(self, file):
        if not isinstance(file, str):
            raise TypeError("Expected userdata_file to be str")

        self._userdata_file = file
        return self

    def vpc(self, vpc):
        if not isinstance(vpc, str):
            raise TypeError("Expected vpc to be str")

        self._vpc_id = vpc
        return self


class CmdHostList(EvergreenCommand):
    def __init__(self):
        self._num_hosts = None
        self._path = None
        self._silent = False
        self._timeout = False
        self._wait = False

    def _command_type(self):
        return "host.list"

    def validate(self):
        return self

    def _param_list(self):
        return {
            "_num_hosts": "num_hosts",
            "_path": "path",
            "_silent": "silent",
            "_timeout": "timeout_seconds",
            "_wait": "wait",
        }

    def num_hosts(self, n):
        if not isinstance(n, int):
            raise TypeError("Expected num_hosts to be int")

        self._num_hosts = n
        return self

    def path(self, path):
        if not isinstance(path, str):
            raise TypeError("Expected path to be a str")

        self._path = path
        return self

    def silent(self):
        self._silent = True
        return self

    def timeout(self, timeout):
        if not isinstance(timeout, int):
            raise TypeError("Expected timeout to be int")

        self._timeout = timeout
        return self

    def wait(self):
        self._wait = True
        return self


class CmdManifestLoad(EvergreenCommand):
    def _command_type(self):
        return "manifest.load"

    def validate(self):
        return self

    def _param_list(self):
        return {}


class CmdResultsJSON(EvergreenCommand):
    def __init__(self):
        self._file = None

    def _command_type(self):
        return "attach.results"

    def validate(self):
        return self

    def _param_list(self):
        return {"_file": "file_location"}

    def file(self, file):
        self._file = file
        return self


class CmdResultsXunit(EvergreenCommand):
    def __init__(self):
        self._file = None

    def _command_type(self):
        return "attach.xunit_results"

    def validate(self):
        return self

    def _param_list(self):
        return {"_file": "file"}

    def file(self, file):
        self._file = file
        return self


class CmdResultsGoTest(EvergreenCommand):
    def __init__(self, json=False, legacy=False):
        self._json_format = json
        self._legacy_format = legacy
        self._files = []

    def _command_type(self):
        if self._json_format:
            return "gotest.parse_json"
        if self._legacy_format:
            return "gotest.parse_files"

    def validate(self):
        if self._legacy_format == self._json_format:
            raise ValueError("Invalid format specified")

        return self

    def _param_list(self):
        return {"_files": "files"}

    def file(self, file):
        self._files.append(file)
        return self

    def files(self, files):
        self._files += files
        return self


class CmdArchiveCreate(EvergreenCommand):
    def __init__(self, archive_format):
        self._archive_format = archive_format
        self._target = None
        self._source_dir = None
        self._include = []
        self._exclude = []

    def _command_type(self):
        return self._archive_format.create_cmd_name()

    def validate(self):
        return self._archive_format.validate("create")

    def _param_list(self):
        return {
            "_target": "target",
            "_source_dir": "source_dir",
            "_include": "include",
            "_exclude": "exclude_files",
        }

    def target(self, target):
        if not isinstance(target, str):
            raise TypeError("target expects a str")

        self._target = target
        return self

    def source_dir(self, source_dir):
        if not isinstance(source_dir, str):
            raise TypeError("source_dir expects a str")

        self._source_dir = source_dir
        return self

    def include(self, include):
        self._include.append(include)
        return self

    def includes(self, includes):
        self._include += includes
        return self

    def exclude(self, exclude):
        self._exclude.append(exclude)
        return self

    def excludes(self, excludes):
        self._exclude += excludes


class CmdArchiveExtract(EvergreenCommand):
    def __init__(self, archive_format):
        self._archive_format = archive_format
        self._path = None
        self._target = None
        self._exclude = []

    def _command_type(self):
        return self._archive_format.extract_cmd_name()

    def validate(self):
        return self._archive_format.validate("extract")

    def _param_list(self):
        return {"_path": "path", "_target": "destination", "_exclude": "exclude_files"}

    def path(self, path):
        self._path = path
        return self

    def target(self, target):
        self._target = target
        return self

    def exclude(self, exclude):
        self._exclude.append(exclude)
        return self

    def excludes(self, excludes):
        self._exclude += excludes
        return self


class CmdAttachArtifacts(EvergreenCommand):
    def __init__(self):
        self._optional = False
        self._files = []

    def _command_type(self):
        return "attach.artifacts"

    def validate(self):
        return self

    def _param_list(self):
        return {"_optional": "optional", "_files": "files"}

    def optional(self, optional):
        self._optional = optional
        return self

    def file(self, f):
        self._files.append(f)
        return self

    def files(self, fs):
        self._files += fs
        return self


class ArchiveFormat:
    def __init__(self, archive_format):
        self._format = archive_format

    def validate(self, operation):
        valid_formats = {
            "create": [ARCHIVE_FORMAT_ZIP, ARCHIVE_FORMAT_TAR],
            "extract": [ARCHIVE_FORMAT_ZIP, ARCHIVE_FORMAT_TAR, ARCHIVE_FORMAT_AUTO],
        }

        if self._format not in valid_formats[operation]:
            raise ValueError("Invalid archive format: " + self._format)

        return self

    def create_cmd_name(self):
        if self._format == ARCHIVE_FORMAT_ZIP:
            return "archive.zip_pack"

        if self._format == ARCHIVE_FORMAT_TAR:
            return "archive.targz_pack"

        return self.validate()

    def extract_cmd_name(self):
        if self._format == ARCHIVE_FORMAT_ZIP:
            return "archive.zip_extract"

        if self._format == ARCHIVE_FORMAT_TAR:
            return "archive.targz_extract"

        if self._format == "auto":
            return "archive.auto_extract"

        return self.validate()

    @staticmethod
    def zip():
        return ArchiveFormat(ARCHIVE_FORMAT_ZIP)

    @staticmethod
    def tar():
        return ArchiveFormat(ARCHIVE_FORMAT_TAR)

    @staticmethod
    def auto():
        return ArchiveFormat(ARCHIVE_FORMAT_AUTO)
