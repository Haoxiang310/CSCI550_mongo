from shrub.base import EvergreenBuilder
from shrub.base import NAME_KEY
from shrub.base import RECURSE_KEY
from shrub.command import CommandSequence
from shrub.command import CommandDefinition


class Task(EvergreenBuilder):
    """Any discrete job you want Evergreen to run."""

    def __init__(self, name):
        if not isinstance(name, str):
            raise TypeError("Task only accepts a str")

        self._name = name
        self._priority = None
        self._dependencies = []
        self._requires = []
        self._commands = CommandSequence()

    def _yaml_map(self):
        return {
            "_name": {NAME_KEY: "name", RECURSE_KEY: False},
            "_commands": {NAME_KEY: "commands", RECURSE_KEY: True},
            "_priority": {NAME_KEY: "priority", RECURSE_KEY: False},
            "_dependencies": {NAME_KEY: "depends_on", RECURSE_KEY: True},
            "_requires": {NAME_KEY: "requires", RECURSE_KEY: True},
        }

    def get_name(self):
        """@:returns The name of the task."""
        return self._name

    def priority(self, value):
        """
        Sets the priority of the task.

        :param value: priority to set.
        :return: instance of task being modified.
        """
        if not isinstance(value, int):
            raise TypeError("priority only accepts an int")

        self._priority = value
        return self

    def command(self, cmd):
        """
        Appends command to end of command sequence.

        :param cmd: command to append.
        :return: instance of task being modified.
        """
        if not isinstance(cmd, CommandDefinition):
            raise TypeError("command only accepts a CommandDefinition")

        self._commands.add(cmd)
        return self

    def commands(self, commands):
        """
        Appends a list of commands to the end of the command sequence.

        :param commands: list of commands to append.
        :return: instance of task being modified.
        """
        if not isinstance(commands, list):
            raise TypeError("commands only accepts a list")

        for c in commands:
            self.command(c)

        return self

    def dependency(self, dep):
        """
        Add a dependency to this task.

        :param dep: TaskDependency to add.
        :return: instance of task being modified.
        """
        if not isinstance(dep, TaskDependency):
            raise TypeError("dependency only accepts a TaskDependency")

        self._dependencies.append(dep)
        return self

    def requires(self, dep):
        """
        Add a requires to this task.

        :param dep: TaskDependency to add.
        :return: instance of task being modified.
        """
        if not isinstance(dep, TaskDependency):
            raise TypeError("requires only accepts a TaskDependency")

        self._requires.append(dep)
        return self

    def function(self, fn):
        """
        Append a function to the end of the command sequence.

        :param fn: function to append.
        :return: instance of task being modified.
        """
        if not isinstance(fn, str):
            raise TypeError("function only accepts a str")

        self._commands.add(CommandDefinition().function(fn))
        return self

    def functions(self, fns):
        """
        Append a list of functions to the end of the command sequence.

        :param fns: list of functions to append.
        :return:  instance of task being modified.
        """
        if not isinstance(fns, list):
            raise TypeError("function only accepts a list")

        for fn in fns:
            self.function(fn)

        return self

    def function_with_vars(self, fn, var_map):
        """
        Append a function to the end of the command sequence with a list of
        variables to be passed to the function.

        :param fn: function to append
        :param var_map: dictionary of variables to pass to function.
        :return: instance of task being modified.
        """
        if not isinstance(fn, str):
            raise TypeError("function_with_vars only accepts a str")

        if not isinstance(var_map, dict):
            raise TypeError("function_with_vars only accepts a dict")

        self._commands.add(CommandDefinition().function(fn).vars(var_map))
        return self


class TaskDependency(EvergreenBuilder):
    """A dependency a task has on another task."""

    def __init__(self, name):
        """
        Create a task dependency.

        :param name: name of task depended on.
        """
        if not isinstance(name, str):
            raise TypeError("TaskDependency only accepts a str")

        self._name = name
        self._variant = None

    def variant(self, variant):
        """
        Set the variant depended on.

        :param variant: variant depended on.
        :return: instance of TaskDependency.
        """
        if not isinstance(variant, str):
            raise TypeError("variant only accepts a str")

        self._variant = variant
        return self

    def _yaml_map(self):
        return {
            "_name": {NAME_KEY: "name", RECURSE_KEY: False},
            "_variant": {NAME_KEY: "variant", RECURSE_KEY: False},
        }


class TaskGroup(EvergreenBuilder):
    """A group of Tasks that can share command setup and teardown."""

    def __init__(self, name):
        """
        Create a TaskGroup.

        :param name: name of task group.
        """
        if not isinstance(name, str):
            raise TypeError("TaskGroup only accepts a str")

        self._group_name = name
        self._max_hosts = None
        self._setup_group = None
        self._setup_task = None
        self._tasks = []
        self._teardown_task = None
        self._teardown_group = None
        self._timeout = None

    def _yaml_map(self):
        return {
            "_group_name": {NAME_KEY: "name", RECURSE_KEY: False},
            "_tasks": {NAME_KEY: "tasks", RECURSE_KEY: False},
            "_max_hosts": {NAME_KEY: "max_hosts", RECURSE_KEY: False},
            "_setup_group": {NAME_KEY: "setup_group", RECURSE_KEY: True},
            "_setup_task": {NAME_KEY: "setup_task", RECURSE_KEY: True},
            "_teardown_task": {NAME_KEY: "teardown_task", RECURSE_KEY: True},
            "_teardown_group": {NAME_KEY: "teardown_group", RECURSE_KEY: True},
            "_timeout": {NAME_KEY: "timeout", RECURSE_KEY: False},
        }

    def get_name(self):
        """
        Get the name of the task group.

        :return: name of task group.
        """
        return self._group_name

    def max_hosts(self, num):
        """
        Set the max number of hosts to spread this group out to.

        :param num: max number of hosts.
        :return: instance of task group being modified.
        """
        if not isinstance(num, int):
            raise TypeError("max_hosts only accepts an int")

        self._max_hosts = num
        return self

    def timeout(self, timeout):
        """
        Set the timeout for tasks in this group.

        :param timeout: timeout to set.
        :return: instance of task group being modified.
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout only accepts an int")

        self._timeout = timeout
        return self

    def task(self, task_name):
        """
        Append given task to the task group.

        :param task_name: name of task to add.
        :return: instance of task group being modified.
        """
        if not isinstance(task_name, str):
            raise TypeError("task only accepts a str")

        self._tasks.append(task_name)
        return self

    def tasks(self, task_names):
        """
        Append a list of tasks to the task group.

        :param task_names: list of names of tasks to add.
        :return: instance of task group being modified.
        """
        if not isinstance(task_names, list):
            raise TypeError("task only accepts a list")

        self._tasks += task_names
        return self

    def setup_group(self):
        """
        Creates a new command definition and appends it to the setup group.

        :return: command definition that was added.
        """
        if not self._setup_group:
            self._setup_group = CommandSequence()

        c = CommandDefinition()

        self._setup_group.add(c)
        return c

    def teardown_group(self):
        """
        Creates a new command definition and appends it to the teardown group.

        :return: command definition that was added.
        """
        if not self._teardown_group:
            self._teardown_group = CommandSequence()

        c = CommandDefinition()

        self._teardown_group.add(c)
        return c
