from abc import abstractmethod
from typing import Any, Callable
from pydantic import BaseModel, Field, validate_model

from mongo_tooling_metrics.errors import InvalidMetricsSetup


class BaseHook(BaseModel):
    """Base class for hooks to collect intra-run function metrics."""

    original_fn: Callable[..., Any] = Field(..., exclude=True)

    @abstractmethod
    def passthrough_logic(self, *args, **kwargs) -> None:
        """Execute logic before calling the original function."""
        raise InvalidMetricsSetup(
            "'passthrough_logic' is called before calling the original function and must be defined."
        )

    def __call__(self, *args, **kwargs):
        """Call 'passthrough_logic' with **kwargs then call the 'original_fn' with **kwargs."""
        self.passthrough_logic(*args, **kwargs)
        self.original_fn(*args, **kwargs)

    def is_malformed(self) -> bool:
        """Make sure the hook still matches the pydantic model -- typing is not enforced intrarun by default."""
        *_, validation_error = validate_model(self.__class__, self.__dict__)
        return True if validation_error else False
