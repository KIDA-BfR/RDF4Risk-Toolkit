"""Shared in-process runtime state for backend workflow services.

The browser application talks to Python through ``mui_backend_server.py``.
Python services keep multi-step workflow state here without depending on any
Python UI framework.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


runtime_state: Dict[str, Any] = {}


@dataclass
class RuntimeHooks:
    """No-op hooks retained for legacy backend tests.

    Production rendering is owned by the React frontend; backend services should
    use ``runtime_state`` directly for state and return JSON snapshots/events.
    """

    state: Dict[str, Any] = field(default_factory=lambda: runtime_state)

    def markdown(self, *_args: Any, **_kwargs: Any) -> None:
        """No-op compatibility hook for legacy tests only."""

    def caption(self, *_args: Any, **_kwargs: Any) -> None:
        """No-op compatibility hook for legacy tests only."""

    def error(self, *_args: Any, **_kwargs: Any) -> None:
        """No-op compatibility hook for legacy tests only."""

    def success(self, *_args: Any, **_kwargs: Any) -> None:
        """No-op compatibility hook for legacy tests only."""

    def info(self, *_args: Any, **_kwargs: Any) -> None:
        """No-op compatibility hook for legacy tests only."""

    def warning(self, *_args: Any, **_kwargs: Any) -> None:
        """No-op compatibility hook for legacy tests only."""


runtime_hooks = RuntimeHooks()


def clear_runtime_state() -> None:
    """Reset backend state; useful for tests or fresh service startup."""

    runtime_state.clear()