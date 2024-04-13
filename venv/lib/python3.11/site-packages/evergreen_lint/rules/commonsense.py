import re
from typing import List

from evergreen_lint.helpers import is_shell_command, iterate_commands
from evergreen_lint.model import LintError, Rule


class LimitKeyvalInc(Rule):
    """Prevent/Limit usage of keyval.inc."""

    @staticmethod
    def name() -> str:
        return "limit-keyval-inc"

    @staticmethod
    def defaults() -> dict:
        return {"limit": 0}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        def _out_message(context: str) -> LintError:
            return (
                f"{context} uses keyval.inc. The entire file must not use "
                f"keyval.inc more than {config['limit']} times."
            )

        out: List[LintError] = []
        count = 0
        for context, command in iterate_commands(yaml):
            if "command" in command and command["command"] == "keyval.inc":
                out.append(_out_message(context))
                count += 1

        if count <= config["limit"]:
            return []
        return out


class ShellExecExplicitShell(Rule):
    """Require explicitly specifying shell in uses of shell.exec."""

    @staticmethod
    def name() -> str:
        return "shell-exec-explicit-shell"

    @staticmethod
    def defaults() -> dict:
        return {}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        def _out_message(context: str) -> LintError:
            return (
                f"{context} is a shell.exec command without an explicitly "
                "declared shell. You almost certainly want to add 'shell: "
                "bash' to the parameters list."
            )

        out: List[LintError] = []
        for context, command in iterate_commands(yaml):
            if "command" in command and command["command"] == "shell.exec":
                if "params" not in command or "shell" not in command["params"]:
                    out.append(_out_message(context))

        return out


class NoWorkingDirOnShell(Rule):
    """Do not allow working_dir to be set on shell.exec, subprocess.*."""

    @staticmethod
    def name() -> str:
        return "no-working-dir-on-shell"

    @staticmethod
    def defaults() -> dict:
        return {}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        def _out_message(context: str, cmd: str) -> LintError:
            return (
                f"{context} is a {cmd} command with a working_dir "
                "parameter. Do not set working_dir, instead `cd` into the "
                "directory in the shell script."
            )

        out: List[LintError] = []
        for context, command in iterate_commands(yaml):
            if "command" in command and is_shell_command(command["command"]):
                if "params" in command and "working_dir" in command["params"]:
                    out.append(_out_message(context, command["command"]))

        return out


class InvalidFunctionName(Rule):
    """Enforce naming convention on functions."""

    @staticmethod
    def name() -> str:
        return "invalid-function-name"

    @staticmethod
    def defaults() -> dict:
        return {"regex": "^f_[a-z][A-Za-z0-9_]*"}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        FUNCTION_NAME = config["regex"]
        FUNCTION_NAME_RE = re.compile(FUNCTION_NAME)

        def _out_message(context: str) -> LintError:
            return f"Function '{context}' must have a name matching '{FUNCTION_NAME}'"

        if "functions" not in yaml:
            return []

        out: List[LintError] = []
        for fname in yaml["functions"].keys():
            if not FUNCTION_NAME_RE.fullmatch(fname):
                out.append(_out_message(fname))

        return out


class NoShellExec(Rule):
    """Do not allow shell.exec. Users should use subprocess.exec instead."""

    @staticmethod
    def name() -> str:
        return "no-shell-exec"

    @staticmethod
    def defaults() -> dict:
        return {}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        def _out_message(context: str) -> LintError:
            return (
                f"{context} is a shell.exec command, which is forbidden. "
                "Extract your shell script out of the YAML and into a .sh file "
                "in directory 'evergreen', and use subprocess.exec instead."
            )

        out: List[LintError] = []
        for context, command in iterate_commands(yaml):
            if "command" in command and command["command"] == "shell.exec":
                out.append(_out_message(context))
        return out


class NoMultilineExpansionsUpdate(Rule):
    @staticmethod
    def name() -> str:
        return "no-multiline-expansions-update"

    @staticmethod
    def defaults() -> dict:
        return {}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        """Forbid multi-line values in expansion.updates parameters."""

        def _out_message(context: str, idx: int) -> LintError:
            return (
                f"{context}, key-value pair {idx} is an expansions.update "
                "command with multi-line values embedded in the yaml, which is"
                " forbidden. For long-form values, use the files parameter of "
                "expansions.update."
            )

        out: List[LintError] = []
        for context, command in iterate_commands(yaml):
            if "command" in command and command["command"] == "expansions.update":
                if "params" in command and "updates" in command["params"]:
                    for idx, item in enumerate(command["params"]["updates"]):
                        if "value" in item and "\n" in item["value"]:
                            out.append(_out_message(context, idx))
        return out
