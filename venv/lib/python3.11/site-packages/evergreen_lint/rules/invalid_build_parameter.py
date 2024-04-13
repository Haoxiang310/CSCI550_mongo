import re
from typing import List

from evergreen_lint.model import LintError, Rule


class InvalidBuildParameter(Rule):
    """Require that parameters obey a naming convention and have a description."""

    @staticmethod
    def name() -> str:
        return "invalid-build-parameter"

    @staticmethod
    def defaults() -> dict:
        return {"regex": "[a-z][a-z0-9_]*", "require-description": True}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        BUILD_PARAMETER = config["regex"]
        BUILD_PARAMETER_RE = re.compile(BUILD_PARAMETER)

        def _out_message_key(idx: int) -> LintError:
            return f"Build parameter, pair {idx}, key must match '{BUILD_PARAMETER}'."

        def _out_message_description(idx: int) -> LintError:
            return f"Build parameter, pair {idx}, must have a description."

        if "parameters" not in yaml:
            return []

        out: List[LintError] = []
        for idx, param in enumerate(yaml["parameters"]):
            if "key" not in param or not BUILD_PARAMETER_RE.fullmatch(param["key"]):
                out.append(_out_message_key(idx))
            if (
                config["require-description"]
                and "description" not in param
                or not param["description"]
            ):
                out.append(_out_message_description(idx))
        return out
