"""Runtime helper utilities for agent reconciliation orchestration."""

from __future__ import annotations

import concurrent.futures
import re
import threading
import time
from typing import Any, Iterable, List, Optional

from .agent_models import AgentRunConfig


def run_with_timeout(func, timeout: int, *args, **kwargs):
    """Run a function with a timeout.

    Returns a tuple: (timed_out: bool, result: Any)

    Notes:
    - This enforces a *caller* timeout boundary and returns promptly when the
      deadline is exceeded.
    - It does not force-kill already running work inside `func` (Python threads
      cannot be preemptively terminated safely). Downstream providers should
      still use request-level timeouts and cooperative cancellation where
      possible.
    """
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(func, *args, **kwargs)
    try:
        return False, future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        future.cancel()
        return True, None
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


class WorkflowAdmissionController:
    """Stagger per-term workflow starts while still allowing bounded parallel work."""

    def __init__(self, min_interval_seconds: float = 0.0):
        try:
            interval = float(min_interval_seconds)
        except Exception:
            interval = 0.0
        self.min_interval_seconds = max(0.0, interval)
        self._lock = threading.Lock()
        self._next_start_at = 0.0

    def wait_for_turn(self) -> None:
        if self.min_interval_seconds <= 0:
            return

        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_start_at - now)
            self._next_start_at = max(now, self._next_start_at) + self.min_interval_seconds

        if wait_seconds > 0:
            time.sleep(wait_seconds)


_WorkflowAdmissionController = WorkflowAdmissionController


def _coerce_positive_int(value: Any, default: int, *, upper_bound: Optional[int] = None) -> int:
    try:
        coerced = int(value)
    except Exception:
        coerced = int(default)
    coerced = max(1, coerced)
    if upper_bound is not None:
        coerced = min(coerced, int(upper_bound))
    return coerced


def _chunked(items: List[Any], chunk_size: int) -> Iterable[List[Any]]:
    for start in range(0, len(items), max(1, int(chunk_size))):
        yield items[start : start + max(1, int(chunk_size))]


def _resolve_model_api_key_env(config: AgentRunConfig) -> str:
    candidate = getattr(config, "model_api_key_env", None)
    if candidate and str(candidate).strip():
        return str(candidate).strip()
    legacy = getattr(config, "openai_api_key_env", None)
    if legacy and str(legacy).strip():
        return str(legacy).strip()
    return "OPENAI_API_KEY"


def _resolve_planner_provider(config: AgentRunConfig) -> str:
    provider = str(config.planner_model_provider or "").strip()
    return provider or config.model_provider


def _resolve_planner_model(config: AgentRunConfig) -> str:
    model_name = str(config.planner_model_name or "").strip()
    return model_name or config.model_name


def _resolve_planner_api_key_env(config: AgentRunConfig) -> str:
    planner_env = str(config.planner_model_api_key_env or "").strip()
    return planner_env or _resolve_model_api_key_env(config)


def _is_valid_qid(value: str) -> bool:
    return bool(re.fullmatch(r"Q\d+", str(value or "").strip().upper()))
