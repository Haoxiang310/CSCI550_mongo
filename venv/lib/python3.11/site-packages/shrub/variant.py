from shrub.base import EvergreenBuilder
from shrub.base import NAME_KEY
from shrub.base import RECURSE_KEY


class Variant(EvergreenBuilder):
    """An evergreen buildvariant."""

    def __init__(self, name):
        """
        Create a Variant with the specified name.

        :param name: build name.
        """
        if not isinstance(name, str):
            raise TypeError("Variant only accepts a str")

        self._build_name = name
        self._build_display_name = None
        self._batch_time_secs = None
        self._task_specs = []
        self._distro_run_on = []
        self._expansions = {}
        self._display_task_specs = []
        self._modules = []

    def _yaml_map(self):
        return {
            "_build_name": {NAME_KEY: "name", RECURSE_KEY: False},
            "_build_display_name": {NAME_KEY: "display_name", RECURSE_KEY: False},
            "_batch_time_secs": {NAME_KEY: "batchtime", RECURSE_KEY: False},
            "_task_specs": {NAME_KEY: "tasks", RECURSE_KEY: True},
            "_distro_run_on": {NAME_KEY: "run_on", RECURSE_KEY: False},
            "_expansions": {NAME_KEY: "expansions", RECURSE_KEY: False},
            "_display_task_specs": {NAME_KEY: "display_tasks", RECURSE_KEY: True},
            "_modules": {NAME_KEY: "modules", RECURSE_KEY: False},
        }

    def get_name(self):
        """
        Get the name of this build variant.
        :return: name of build variant.
        """
        return self._build_name

    def display_name(self, name):
        """
        Set the display name of the build variant.

        :param name: display name.
        :return: instance of variant.
        """
        if not isinstance(name, str):
            raise TypeError("display_name only accepts a str")

        self._build_display_name = name
        return self

    def batch_time(self, batch_time):
        """
        Set the batch time of build variant.
        :param batch_time: batch time (sec).
        :return: instance of variant.
        """
        if not isinstance(batch_time, int):
            raise TypeError("batch_time only accepts an int")

        self._batch_time_secs = batch_time
        return self

    def run_on(self, distro):
        """
        Set the default distro this build variant should run on.

        :param distro: distro to run on.
        :return: instance of variant.
        """
        if not isinstance(distro, str):
            raise TypeError("run_on only accepts a str")

        self._distro_run_on = [distro]
        return self

    def expansions(self, expansions):
        """
        Sets the expansions for this build variant.

        :param expansions: dictionary of expansions to set.
        :return: instance of variant.
        """
        if not isinstance(expansions, dict):
            raise TypeError("expansions only accepts a dict")

        for k in expansions:
            self.expansion(k, expansions[k])
        return self

    def expansion(self, name, value):
        """
        Sets an expansion for this build variant.

        :param name: name of expansion.
        :param value: value of expansion.
        :return: instance of variant.
        """
        if not isinstance(name, str):
            raise TypeError("expansion only accepts a str")

        if not isinstance(value, str):
            raise TypeError("expansion only accepts a str")

        self._expansions[name] = value
        return self

    def task(self, task_spec):
        """
        Add a TaskSpec to this variant.

        :param task_spec: task spec to add.
        :return: instance of variant.
        """
        if not isinstance(task_spec, TaskSpec):
            raise TypeError("task only accepts TaskSpec objects")

        self._task_specs.append(task_spec)
        return self

    def tasks(self, task_spec_list):
        """
        Add a list of TaskSpecs to this variant.

        :param task_spec_list: list of task specs.
        :return: instance of variant.
        """
        if not isinstance(task_spec_list, list):
            raise TypeError("tasks only accepts a list")

        for t in task_spec_list:
            self.task(t)

        return self

    def module(self, module):
        """
        Add a Module to this build variant.

        :param module: module to add.
        :return: instance of variant.
        """
        if not isinstance(module, str):
            raise TypeError("module only accepts a str")

        self._modules.append(module)
        return self

    def modules(self, modules):
        """
        Sets the modules for this build variant.

        :param modules: array of modules to set.
        :return: instance of variant.
        """
        if not isinstance(modules, list):
            raise TypeError("modules only accepts a list")

        for module in modules:
            self.module(module)
        return self

    def display_task(self, display_task):
        """
        Add a DisplayTaskDefinition to this build variant.

        :param display_task: display task to add.
        :return: instance of variant.
        """
        if not isinstance(display_task, DisplayTaskDefinition):
            raise TypeError("display_task only accepts a DisplayTaskDefinition")

        self._display_task_specs.append(display_task)
        return self

    def display_tasks(self, display_task_list):
        """
        Add a list of DisplayTaskDefinitions to this build variant.

        :param display_task_list: list of display tasks.
        :return: instance of variant.
        """
        if not isinstance(display_task_list, list):
            raise TypeError("display_tasks only accepts a list")

        for dt in display_task_list:
            self.display_task(dt)

        return self


class TaskSpec(EvergreenBuilder):
    """A spec for adding a task to a variant."""

    def __init__(self, name):
        """
        Create a task spec.

        :param name: name of task.
        """
        self._name = name
        self._stepback = None
        self._distro = []

    def _yaml_map(self):
        return {
            "_name": {NAME_KEY: "name", RECURSE_KEY: False},
            "_stepback": {NAME_KEY: "stepback", RECURSE_KEY: False},
            "_distro": {NAME_KEY: "distros", RECURSE_KEY: False},
        }

    def stepback(self):
        """
        Enable stepback for this task.
        :return:
        """
        self._stepback = True
        return self

    def distro(self, distro):
        """
        Specify distro this task should run on.

        :param distro: distro to run on.
        :return: instance of task spec.
        """
        if not isinstance(distro, str):
            raise TypeError("distro only accepts a str")

        self._distro = [distro]
        return self


class DisplayTaskDefinition(EvergreenBuilder):
    """Defines a display task for evergreen."""

    def __init__(self, name):
        """
        Create a display task definition.

        :param name: name of display task.
        """
        if not isinstance(name, str):
            raise TypeError("DisplayTaskDefinition only accepts a str")

        self._name = name
        self._components = []

    def _yaml_map(self):
        return {
            "_name": {NAME_KEY: "name", RECURSE_KEY: False},
            "_components": {NAME_KEY: "execution_tasks", RECURSE_KEY: False},
        }

    def execution_task(self, task_name):
        """
        Add an execution task to the display task.
        :param task_name: name of task.
        :return: instance of display task.
        """
        if not isinstance(task_name, str):
            raise TypeError("execution_task only accepts a str")

        self._components.append(task_name)
        return self

    def execution_tasks(self, task_name_list):
        """
        Add a list of execution tasks to the display task.

        :param task_name_list: list of task names.
        :return: instance of display task.
        """
        if not isinstance(task_name_list, list):
            raise TypeError("execution_tasks only accepts a list")

        for c in task_name_list:
            self.execution_task(c)

        return self
