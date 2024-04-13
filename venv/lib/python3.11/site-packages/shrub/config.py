from shrub.base import EvergreenBuilder
from shrub.base import NAME_KEY
from shrub.base import RECURSE_KEY
from shrub.command import CommandSequence
from shrub.task import Task
from shrub.task import TaskGroup
from shrub.variant import Variant


def _find_name_in_list(name_list, name):
    """Call get_name() on all items in the given list to find a match."""
    for l in name_list:  # noqa: E741
        if l.get_name() == name:
            return l

    return None


class Configuration(EvergreenBuilder):
    """An Evergreen configuration.

    Can be converted into json or yaml.
    """

    def __init__(self):
        self._functions = {}
        self._tasks = []
        self._groups = []
        self._variants = []
        self._pre = None
        self._post = None
        self._timeout = None

        self._exec_timeout_secs = None
        self._batch_time_secs = None
        self._stepback = None
        self._command_type = None
        self._ignore_files = []

    def _yaml_map(self):
        return {
            "_tasks": {NAME_KEY: "tasks", RECURSE_KEY: True},
            "_groups": {NAME_KEY: "task_groups", RECURSE_KEY: True},
            "_variants": {NAME_KEY: "buildvariants", RECURSE_KEY: True},
            "_pre": {NAME_KEY: "pre", RECURSE_KEY: True},
            "_post": {NAME_KEY: "post", RECURSE_KEY: True},
            "_timeout": {NAME_KEY: "timeout", RECURSE_KEY: False},
            "_exec_timeout_secs": {NAME_KEY: "exec_timeout_secs", RECURSE_KEY: False},
            "_batch_time_secs": {NAME_KEY: "batchtime", RECURSE_KEY: False},
            "_stepback": {NAME_KEY: "stepback", RECURSE_KEY: False},
            "_command_type": {NAME_KEY: "command_type", RECURSE_KEY: False},
            "_ignore_files": {NAME_KEY: "ignore", RECURSE_KEY: False},
        }

    def task(self, name):
        """
        Get a task from the task list by name. If the task name is not found,
        create a new task and insert it into the task list.

        :param name: name of task.
        :return: task specified.
        """
        if not isinstance(name, str):
            raise TypeError("task only accepts strings")

        t = _find_name_in_list(self._tasks, name)
        if t:
            return t

        t = Task(name)
        self._tasks.append(t)
        return t

    def task_group(self, name):
        """
        Get a task group from the task group list by name. If the task group
        name is not found, create a new group task and insert it into the task
        group list.

        :param name: name of task group.
        :return: task group specified.
        """
        if not isinstance(name, str):
            raise TypeError("task_group only accepts strings")

        g = _find_name_in_list(self._groups, name)
        if g:
            return g

        g = TaskGroup(name)
        self._groups.append(g)
        return g

    def function(self, name):
        """
        Get a function from the functions by name. If the function does not
        already exist, return a CommandSequence for a new function under the
        specified name.

        :param name: name of function to add.
        :return: Command sequence for function.
        """
        if not isinstance(name, str):
            raise TypeError("function only accepts strings")

        if name in self._functions:
            return self._functions[name]

        seq = CommandSequence()
        self._functions[name] = seq
        return seq

    def variant(self, name):
        """
        Get a variant the variant list by name. If the variant
        name is not found, create a new variant and insert it into the
        variant list.

        :param name: name of variant.
        :return: variant specified.
        """
        if not isinstance(name, str):
            raise TypeError("variant only accepts strings")

        v = _find_name_in_list(self._variants, name)
        if v:
            return v

        v = Variant(name)
        self._variants.append(v)
        return v

    def pre(self, cmd_seq):
        """
        Sets the pre commands to the specified CommandSequence.

        :param cmd_seq: CommandSequence to use.
        :return: instance of config.
        """
        if not isinstance(cmd_seq, CommandSequence):
            raise TypeError("pre only accepts a Sequence")

        self._pre = cmd_seq
        return self

    def post(self, cmd_seq):
        """
        Sets the post commands to the specified CommandSequence.

        :param cmd_seq: CommandSequence to use.
        :return: instance of config.
        """
        if not isinstance(cmd_seq, CommandSequence):
            raise TypeError("pre only accepts a Sequence")

        self._post = cmd_seq
        return self

    def exec_timeout(self, duration):
        """
        Sets the exec_timeout.

        :param duration: timeout to set (in sec).
        :return: instance of config.
        """
        if not isinstance(duration, int):
            raise TypeError("exec_timeout only accepts an int")

        self._exec_timeout_secs = duration
        return self

    def batch_time(self, duration):
        """
        Sets the batch time.

        :param duration: batch time to set (in sec).
        :return: instance of config.
        """
        if not isinstance(duration, int):
            raise TypeError("batch_time only accepts an int")

        self._batch_time_secs = duration
        return self

    def stepback(self):
        """
        Enables stepback for this config.

        :return: instance of config.
        """
        self._stepback = True
        return self

    def command_type(self, type):
        """
        Specify the command type. Possible values are ["system",
        "setup", "task"].

        :param type: command type to set.
        :return: instance of config.
        """
        if type not in ["system", "setup", "task"]:
            raise ValueError("Bad Command Type")

        self._command_type = type
        return self

    def ignore_file(self, file_pattern):
        """
        Append new file pattern to list of patterns to ignore.

        :param file_pattern: file pattern to ignore.
        :return: instance of config.
        """
        if not isinstance(file_pattern, str):
            raise TypeError("ignore_file only accepts a str")

        self._ignore_files.append(file_pattern)
        return self

    def ignore_files(self, file_patterns):
        """
        Append a list of file pattern to list of patterns to ignore.

        :param file_patterns: list of file patterns to ignore.
        :return: instance of config.
        """
        if not isinstance(file_patterns, list):
            raise TypeError("ignore_file only accepts a Sequence")

        for f in file_patterns:
            self.ignore_file(f)

        return self

    def to_map(self):
        """Convert this object to a python dict."""
        obj = {}
        self._add_defined_attribs(obj, self._yaml_map().keys())

        if self._functions:
            obj["functions"] = {}
            for k in self._functions:
                obj["functions"][k] = self._functions[k].to_map()

        return obj
