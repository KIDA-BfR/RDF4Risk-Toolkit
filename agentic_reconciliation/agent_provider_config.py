# -*- coding: utf-8 -*-
"""Provider configuration helpers for agent reconciliation.

The helpers in this module are intentionally side-effect free. Service-level
code remains responsible for reading/writing runtime state, config files, and
environment variables.
"""

from __future__ import annotations

from typing import List, Optional


API_KEY_PLACEHOLDERS = {
    "",
    "yourapikey",
    "your_api_key",
    "replace-with-api-key",
    "replace_with_api_key",
    "replace-me",
    "changeme",
    "none",
    "null",
    "<api_key>",
}
OPENAI_CODEX_PROVIDER = "openai_codex"
OPENAI_COMPATIBLE_PROVIDER = "openai_compatible"
OPENAI_COMPATIBLE_BASE_URL_ENV = "OPENAI_COMPATIBLE_BASE_URL"
OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY = "openai_compatible_base_url"
OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY = "openai_compatible_model_registry"
OPENAI_COMPATIBLE_MODEL_REGISTRY_MAX_ITEMS = 50
LEGACY_AGENT_MODEL_CONFIG_KEYS = (
    "model_provider",
    "model_name",
    "definition_model_name",
    "planner_model_provider",
    "planner_model_name",
    "planner_model_api_key_env",
)


def _is_codex_provider(provider: Optional[str]) -> bool:
    return str(provider or "").strip() == OPENAI_CODEX_PROVIDER


def _is_openai_compatible_provider(provider: Optional[str]) -> bool:
    return str(provider or "").strip() == OPENAI_COMPATIBLE_PROVIDER


def _normalize_openai_compatible_base_url(raw_value: Optional[str]) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"http://{value}"
    return value.rstrip("/")


def _normalize_openai_compatible_model_registry(values: List[str]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for raw_value in values:
        model_id = str(raw_value or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        ordered.append(model_id)
    return ordered[:OPENAI_COMPATIBLE_MODEL_REGISTRY_MAX_ITEMS]


def _merge_openai_compatible_model_registry(*model_groups: List[str]) -> List[str]:
    merged: List[str] = []
    for group in model_groups:
        if not isinstance(group, list):
            continue
        merged.extend(group)
    return _normalize_openai_compatible_model_registry(merged)


def _is_populated_api_key(value: Optional[str]) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    lowered = token.lower()
    return lowered not in API_KEY_PLACEHOLDERS and not lowered.startswith("your")
