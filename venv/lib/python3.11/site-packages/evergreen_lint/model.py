"""Type annotations for evglint."""
from typing import List

from typing_extensions import Protocol

LintError = str


class Rule(Protocol):
    @staticmethod
    def name() -> str:
        """Rule name, something like this-is-my-rule. Must match key in RULES."""
        pass

    @staticmethod
    def defaults() -> dict:
        """A dict of options for this rule, with their defaults. Options
        are guaranteed to exist when passed to __call__."""
        pass

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        """Rule definition."""
        pass
