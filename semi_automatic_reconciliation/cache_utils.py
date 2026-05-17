# -*- coding: utf-8 -*-
"""Small cache helpers for external reconciliation providers."""

from __future__ import annotations

import functools
import hashlib
import json
import os
import time
from collections import OrderedDict
from typing import Any, Callable, Optional, TypeVar

F = TypeVar("F", bound=Callable[..., Any])
DEFAULT_PROVIDER_CACHE_MAXSIZE = 256
SENSITIVE_KEY_PARTS = ("api_key", "apikey", "token", "authorization", "secret", "password")


def _provider_cache_maxsize() -> int:
    raw = str(os.getenv("RDF4RISK_PROVIDER_CACHE_MAXSIZE", "") or "").strip()
    if not raw:
        return DEFAULT_PROVIDER_CACHE_MAXSIZE
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_PROVIDER_CACHE_MAXSIZE


def _is_sensitive_key(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def _stable_value(value: Any, key_hint: str = "") -> Any:
    if _is_sensitive_key(key_hint):
        token = str(value or "")
        return {"secret_sha256": hashlib.sha256(token.encode("utf-8")).hexdigest() if token else ""}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): _stable_value(item, str(key)) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple, set)):
        return [_stable_value(item, key_hint) for item in value]
    return repr(value)


def _cache_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    stable = {
        "args": [_stable_value(item) for item in args],
        "kwargs": {str(key): _stable_value(item, str(key)) for key, item in sorted(kwargs.items())},
    }
    encoded = json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _cacheable_result(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set, dict)) and not value:
        return False
    return True


def cache_data(ttl: Optional[float] = None, max_entries: Optional[int] = None) -> Callable[[F], F]:
    """Bounded TTL cache decorator used by provider API wrappers.

    The cache is in-memory only. Secret-like argument values are hashed before
    becoming part of cache keys, and empty results are not cached so transient
    provider failures do not stick around.
    """

    def decorator(func: F) -> F:
        cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            limit = _provider_cache_maxsize() if max_entries is None else max(0, int(max_entries))
            if limit == 0:
                return func(*args, **kwargs)

            now = time.time()
            key = _cache_key(args, kwargs)
            cached = cache.get(key)
            if cached is not None:
                created_at, value = cached
                if ttl is None or now - created_at < float(ttl):
                    cache.move_to_end(key)
                    return value
                cache.pop(key, None)

            value = func(*args, **kwargs)
            if _cacheable_result(value):
                cache[key] = (now, value)
                cache.move_to_end(key)
                while len(cache) > limit:
                    cache.popitem(last=False)
            return value

        def cache_clear() -> None:
            cache.clear()

        wrapper.cache_clear = cache_clear  # type: ignore[attr-defined]
        wrapper.cache_size = lambda: len(cache)  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator
