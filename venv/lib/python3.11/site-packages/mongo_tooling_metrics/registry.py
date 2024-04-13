from __future__ import annotations
import threading
from typing import Dict, Type
from mongo_tooling_metrics.base_hook import BaseHook

from mongo_tooling_metrics.errors import InvalidHookAccess


class _HookRegistry:

    registry: Dict = {}
    lock: threading.Lock = threading.Lock()

    def _safe_set_hook(self, hook_instance: BaseHook) -> bool:
        """Add the hook to the registry if it does not exist return the singleton hook instance."""
        with self.lock:
            self.registry.setdefault(hook_instance.__class__, hook_instance)
            return self.registry[hook_instance.__class__]

    def _safe_get_hook(self, hook_class: Type[BaseHook]) -> BaseHook:
        """Get the hook if it exists -- else fail."""
        try:
            with self.lock:
                return self.registry[hook_class]
        except KeyError as exc:
            raise InvalidHookAccess(
                f"Hook was accessed but never initialized: {hook_class}") from exc
