"""Lint rules."""
from typing import Dict, Type

from evergreen_lint.model import Rule
from evergreen_lint.rules.commonsense import (
    InvalidFunctionName,
    LimitKeyvalInc,
    NoMultilineExpansionsUpdate,
    NoShellExec,
    NoWorkingDirOnShell,
    ShellExecExplicitShell,
)
from evergreen_lint.rules.dependency_for_func import DependencyForFunc
from evergreen_lint.rules.enforce_tags_for_tasks import EnforceTagsForTasks
from evergreen_lint.rules.invalid_build_parameter import InvalidBuildParameter
from evergreen_lint.rules.required_expansions_write import RequiredExpansionsWrite
from evergreen_lint.rules.tasks_for_variants import TasksForVariants

RULES: Dict[str, Type[Rule]] = {
    "limit-keyval-inc": LimitKeyvalInc,
    "shell-exec-explicit-shell": ShellExecExplicitShell,
    "no-working-dir-on-shell": NoWorkingDirOnShell,
    "invalid-function-name": InvalidFunctionName,
    "no-shell-exec": NoShellExec,
    "no-multiline-expansions-update": NoMultilineExpansionsUpdate,
    "invalid-build-parameter": InvalidBuildParameter,
    "required-expansions-write": RequiredExpansionsWrite,
    "dependency-for-func": DependencyForFunc,
    "tasks-for-variants": TasksForVariants,
    "enforce-tags-for-tasks": EnforceTagsForTasks,
}
# Thoughts on Writing Rules
# - see .helpers for reliable iteration helpers
# - Do not assume a key exists, unless it's been mentioned here
# - Do not allow exceptions to percolate outside of the rule function
# - YAML anchors are not available. Unless you want to write your own yaml
#   parser, or fork adrienverge/yamllint, abandon all hope on that idea you have.
# - Anchors are basically copy and paste, so you might see "duplicate" errors
#   that originate from the same anchor, but are reported in multiple locations

# Evergreen YAML Root Structure Reference
# Unless otherwise mentioned, the key is optional. You can infer the
# substructure by reading etc/evergreen.yml

# Function blocks: are dicts with the key 'func', which maps to a string,
# the name of the function
# Command blocks: are dicts with the key 'command', which maps to a string,
# the Evergreen command to run

# variables: List[dict]. These can be any valid yaml and it's very difficult
#   to infer anything
# functions: Dict[str, Union[dict, List[dict]]]. The key is the name of the
#   function, the value is either a dict, or list of dicts, with each dict
#   representing a command
# pre, post, and timeout: List[dict] representing commands or functions to
#   be run before/after/on timeout condition respectively
# tasks: List[dict], each dict is a task definition, key is always present
# task_groups: List[dict]
# modules: List[dict]
# buildvariants: List[dict], key is always present
# parameters: List[dict]
