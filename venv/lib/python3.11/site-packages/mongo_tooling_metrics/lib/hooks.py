"""Common hooks that can be reused."""

from inspect import getcallargs
from mongo_tooling_metrics.base_hook import BaseHook


class ExitHook(BaseHook):
    """Plumb all sys.exit through this object so that we can access the exit code in atexit."""
    exit_code: int = 0

    def passthrough_logic(self, *args, **kwargs) -> None:
        """Store the exit code when sys.exit is called."""
        args_as_kwargs = getcallargs(self.original_fn, *args, **kwargs)
        self.exit_code = args_as_kwargs.get('status') if args_as_kwargs.get('status') != None else 0
