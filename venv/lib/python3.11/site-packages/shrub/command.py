from shrub.base import EvergreenBuilder
from shrub.base import NAME_KEY
from shrub.base import RECURSE_KEY


class CommandDefinition(EvergreenBuilder):
    """An evergreen command."""

    def __init__(self):
        self._function_name = None
        self._execution_type = None
        self._display_name = None
        self._command_name = None
        self._timeout = None
        self._variants = []
        self._vars = {}
        self._params = {}

    def _yaml_map(self):
        return {
            "_function_name": {NAME_KEY: "func", RECURSE_KEY: False},
            "_execution_type": {NAME_KEY: "type", RECURSE_KEY: False},
            "_display_name": {NAME_KEY: "display_name", RECURSE_KEY: False},
            "_command_name": {NAME_KEY: "command", RECURSE_KEY: False},
            "_timeout": {NAME_KEY: "timeout_secs", RECURSE_KEY: False},
            "_variants": {NAME_KEY: "variants", RECURSE_KEY: False},
            "_vars": {NAME_KEY: "vars", RECURSE_KEY: False},
            "_params": {NAME_KEY: "params", RECURSE_KEY: False},
        }

    def function(self, fun_name):
        """
        Function to call  for command.

        :param fun_name: name of function to call.
        :return: instance of command definition.
        """
        if not isinstance(fun_name, str):
            raise TypeError("function only accepts a str")

        self._function_name = fun_name
        return self

    def type(self, execution_type):
        """
        Type of command to execute.

        :param execution_type: type of command.
        :return: instance of command definition.
        """
        if not isinstance(execution_type, str):
            raise TypeError("type only accepts a str")

        self._execution_type = execution_type
        return self

    def name(self, name):
        """
        Display name of command.

        :param name: display name.
        :return: instance of command definition.
        """
        if not isinstance(name, str):
            raise TypeError("name only accepts a str")

        self._display_name = name
        return self

    def command(self, name):
        """
        Command name to execute.

        :param name: name of command.
        :return: instance of command definition.
        """
        if not isinstance(name, str):
            raise TypeError("command only accepts a str")

        self._command_name = name
        return self

    def timeout(self, timeout):
        """
        Timeout of command.

        :param timeout: timeout.
        :return: instance of command definition.
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout only accepts an int")

        self._timeout = timeout
        return self

    def variant(self, variant):
        if not isinstance(variant, str):
            raise TypeError("variant only accepts a str")

        self._variants.append(variant)
        return self

    def variants(self, variants):
        if not isinstance(variants, list):
            raise TypeError("variants only accepts a list")

        for v in variants:
            self.variant(v)

        return self

    def var(self, name, value):
        """
        Define a variable to pass to command.

        :param name: name of variable.
        :param value: value of variable.
        :return: instance of command definition.
        """
        if not isinstance(name, str):
            raise TypeError("var only accepts a str")

        self._vars[name] = value
        return self

    def vars(self, vs):
        """
        Define a dictionary of variables to pass to command.

        :param vs: dictionary of variables.
        :return: instance of command definition.
        """
        if not isinstance(vs, dict):
            raise TypeError("vars only accepts a dict")

        for k in vs:
            self.var(k, vs[k])

        return self

    def param(self, name, value):
        """
        Define parameter to pass to command.

        :param name: name of parameter.
        :param value: value of parameter.
        :return: instance of command definition.
        """
        if not isinstance(name, str):
            raise TypeError("param only accepts a str")

        self._params[name] = value
        return self

    def params(self, ps):
        """
        Define a dictionary of parameters to pass to command.

        :param ps: dictionary of parameters.
        :return: instance of command definition.
        """
        if not isinstance(ps, dict):
            raise TypeError("params only accepts a dict")

        for k in ps:
            self.param(k, ps[k])

        return self


class CommandSequence(EvergreenBuilder):
    """Define a sequence of commands to execute in evergreen."""

    def __init__(self):
        self._cmd_seq = []

    def _yaml_map(self):
        return {}

    def len(self):
        """
        Number of commands in sequence.
        :return: number of commands in sequence.
        """
        return len(self._cmd_seq)

    def command(self):
        """
        Add a new CommandDefinition to the sequence.
        :return: new CommandDefinition.
        """
        c = CommandDefinition()
        self._cmd_seq.append(c)
        return c

    def add(self, cmd_def):
        """
        Add CommandDefinition to the sequence.

        :param cmd_def: CommandDefinition to add.
        :return: instance of command sequence.
        """
        if not isinstance(cmd_def, CommandDefinition):
            raise TypeError("add only accepts a CommandDefinition")

        self._cmd_seq.append(cmd_def)
        return self

    def extend(self, cmd_def_list):
        """
        Add a list of CommandDefinitions to the sequence.

        :param cmd_def_list: list of CommandDefinitions.
        :return: instance of command sequence.
        """
        if not isinstance(cmd_def_list, list):
            raise TypeError("extend only accepts a list")

        for c in cmd_def_list:
            self.add(c)

        return self

    def to_map(self):
        """Convert this object to a list of python dict."""
        return [c.to_map() for c in self._cmd_seq]
