# -*- coding: utf-8 -*-
"""Provider discovery and lookup."""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
from pathlib import Path
from typing import Dict

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

logger = logging.getLogger(__name__)


def _provider_module_names() -> list[str]:
    package_path = Path(__file__).resolve().parent
    names = []
    for module_info in pkgutil.iter_modules([str(package_path)]):
        module_name = module_info.name
        if not module_name.endswith("_provider"):
            continue
        if module_name in {"base_provider"}:
            continue
        names.append(module_name)
    return sorted(names)


def _import_provider_module(module_name: str):
    package = __package__
    if package:
        return importlib.import_module(f"{package}.{module_name}")
    return importlib.import_module(module_name)


def _discover_registry() -> Dict[str, BaseProvider]:
    registry: dict[str, BaseProvider] = {}
    for module_name in _provider_module_names():
        try:
            module = _import_provider_module(module_name)
        except Exception as exc:
            logger.warning("Could not import provider module %s: %s", module_name, exc, exc_info=True)
            continue

        module_provider = getattr(module, "_PROVIDER", None)
        if isinstance(module_provider, BaseProvider) and module_provider.name:
            registry[module_provider.name] = module_provider
            continue

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseProvider or not issubclass(obj, BaseProvider) or inspect.isabstract(obj):
                continue
            if not getattr(obj, "name", ""):
                continue
            provider = obj()
            registry[provider.name] = provider
    return registry


REGISTRY: Dict[str, BaseProvider] = _discover_registry()


def get_provider(name: str) -> BaseProvider:
    try:
        return REGISTRY[name]
    except KeyError as exc:
        available = ", ".join(sorted(REGISTRY)) or "none"
        raise ValueError(f"Unknown provider specified: {name}. Available providers: {available}") from exc


def get_all_providers() -> Dict[str, BaseProvider]:
    return dict(REGISTRY)
