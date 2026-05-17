# -*- coding: utf-8 -*-
"""Provider-aware LLM utilities for model discovery and text/JSON generation."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .agent_codex_subscription_service import create_codex_response
from .agent_langsmith_monitoring import record_llm_interaction
from .agent_pricing_service import get_pricing_for_provider


PROVIDER_CONFIG = {
    "openai": {
        "label": "OpenAI",
        "default_api_key_env": "OPENAI_API_KEY",
        "default_models": ["gpt-5.1", "gpt-5", "gpt-4.1", "gpt-4o", "o4-mini"],
    },
    "openai_compatible": {
        "label": "OpenAI Compatible (OpenWebUI / LM Studio)",
        "default_api_key_env": "OPENAI_COMPATIBLE_API_KEY",
        "default_models": ["gpt-oss", "llama3.1", "qwen2.5"],
    },
    "anthropic": {
        "label": "Anthropic",
        "default_api_key_env": "ANTHROPIC_API_KEY",
        "default_models": ["claude-sonnet-4-5", "claude-sonnet-4-0", "claude-3-7-sonnet-latest", "claude-3-5-haiku-latest"],
    },
    "google_gemini": {
        "label": "Google Gemini",
        "default_api_key_env": "GOOGLE_API_KEY",
        "default_models": ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-pro"],
    },
    "openai_codex": {
        "label": "ChatGPT Subscription (curated models)",
        "default_api_key_env": "OPENAI_CODEX_SUBSCRIPTION",
        "default_models": ["gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex", "codex-mini-latest"],
    },
}

MAX_MODEL_PAGES = 20
MODEL_PAGE_SIZE = 100
MODEL_PRICING_UNAVAILABLE_NOTE = "Pricing is not provided by provider model-list APIs."
SUBSCRIPTION_REGISTRY_FILE = Path(__file__).resolve().parent / "data" / "chatgpt_subscription_models.json"
REASONING_EFFORT_LEVELS = {"none", "low", "medium", "high", "xhigh"}
OPENAI_COMPATIBLE_PROVIDER = "openai_compatible"
OPENAI_COMPATIBLE_BASE_URL_ENV = "OPENAI_COMPATIBLE_BASE_URL"


def _http_status_code_from_error(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    try:
        status_code = getattr(response, "status_code", None)
        return int(status_code) if status_code is not None else None
    except Exception:
        return None


def _is_auth_required_http_error(exc: Exception) -> bool:
    return _http_status_code_from_error(exc) in {401, 403}


def is_openai_compatible_auth_required_error(exc: Exception) -> bool:
    """Return True when an OpenAI-compatible request failed due to missing auth."""
    if isinstance(exc, requests.HTTPError) and _is_auth_required_http_error(exc):
        return True

    message = str(exc or "").strip().lower()
    return (
        "openai-compatible endpoint requires an api key" in message
        or "openai-compatible endpoint rejected unauthenticated request" in message
    )


def get_supported_llm_providers() -> List[str]:
    return list(PROVIDER_CONFIG.keys())


def get_provider_label(provider: str) -> str:
    return PROVIDER_CONFIG.get(provider, {}).get("label", provider)


def get_default_api_key_env(provider: str) -> str:
    return PROVIDER_CONFIG.get(provider, {}).get("default_api_key_env", "OPENAI_API_KEY")


def get_default_model_options(provider: str) -> List[str]:
    return PROVIDER_CONFIG.get(provider, {}).get("default_models", []).copy()


def resolve_api_key(provider: str, api_key_env: Optional[str] = None) -> Optional[str]:
    env_name = api_key_env or get_default_api_key_env(provider)
    return os.getenv(env_name)


def _normalize_openai_compatible_base_url(raw_value: Optional[str]) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"http://{value}"
    return value.rstrip("/")


def resolve_openai_compatible_base_url(explicit_base_url: Optional[str] = None) -> str:
    return _normalize_openai_compatible_base_url(
        explicit_base_url
        or os.getenv(OPENAI_COMPATIBLE_BASE_URL_ENV)
    )


def _build_openai_compatible_endpoint(base_url: str, path_suffix: str) -> str:
    base = str(base_url or "").rstrip("/")
    suffix = str(path_suffix or "").lstrip("/")
    if base.endswith("/v1"):
        return f"{base}/{suffix}"
    return f"{base}/v1/{suffix}"


def _unique_models(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _normalize_pricing_overrides(pricing_overrides: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(pricing_overrides, dict):
        return {}

    normalized: Dict[str, Dict[str, Any]] = {}
    for key, value in pricing_overrides.items():
        model_key = str(key or "").strip()
        if not model_key:
            continue
        if isinstance(value, dict):
            normalized[model_key] = value
            normalized[model_key.lower()] = value
    return normalized


def _get_fuzzy_matching_keys(model_id: str) -> List[str]:
    """Generate potential keys for fuzzy matching a model ID to a pricing entry."""
    base = str(model_id or "").strip().lower()
    if not base:
        return []
    
    keys = [base]
    
    # Remove common suffixes
    suffixes = [
        "-latest", "-preview", "-preview-latest", 
        "-chat", "-instruct", "-text", "-v1", "-v2", "-v3"
    ]
    current = base
    for s in suffixes:
        if current.endswith(s):
            current = current[:-len(s)]
            if current not in keys:
                keys.append(current)
                
    # Remove version dates (e.g., -20241022)
    date_match = re.search(r"-\d{8}$", base)
    if date_match:
        keys.append(base[:date_match.start()])
        
    return keys


def _pricing_for_model(model_id: str, pricing_overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    normalized = _normalize_pricing_overrides(pricing_overrides)
    
    # Try exact match first, then fuzzy keys
    entry = {}
    for key in _get_fuzzy_matching_keys(model_id):
        if key in normalized:
            entry = normalized[key]
            break
            
    if not isinstance(entry, dict):
        entry = {}

    input_price = _safe_float(entry.get("input_usd_per_mtok"))
    output_price = _safe_float(entry.get("output_usd_per_mtok"))
    cached_input_price = _safe_float(entry.get("cached_input_usd_per_mtok"))
    notes = str(entry.get("notes", "") or "").strip() or None

    return {
        "pricing_input_usd_per_mtok": input_price,
        "pricing_output_usd_per_mtok": output_price,
        "pricing_cached_input_usd_per_mtok": cached_input_price,
        "pricing_notes": notes,
    }


def _build_model_record(
    provider: str,
    model_id: str,
    *,
    display_name: Optional[str] = None,
    max_input_tokens: Any = None,
    max_output_tokens: Any = None,
    pricing_overrides: Optional[Dict[str, Any]] = None,
    reasoning_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    model_id_clean = str(model_id or "").strip()
    pricing = _pricing_for_model(model_id_clean, pricing_overrides)
    pricing_is_known = any(
        pricing.get(key) is not None
        for key in [
            "pricing_input_usd_per_mtok",
            "pricing_output_usd_per_mtok",
            "pricing_cached_input_usd_per_mtok",
        ]
    )

    reasoning_options = reasoning_options if isinstance(reasoning_options, dict) else {}

    return {
        "provider": provider,
        "model_id": model_id_clean,
        "display_name": str(display_name or model_id_clean),
        "max_input_tokens": _safe_int(max_input_tokens),
        "max_output_tokens": _safe_int(max_output_tokens),
        "supports_reasoning": bool(reasoning_options.get("supports_reasoning", False)),
        "reasoning_mode": reasoning_options.get("reasoning_mode"),
        **pricing,
        "pricing_source": "override" if pricing_is_known else "unavailable",
        "pricing_availability_note": None if pricing_is_known else MODEL_PRICING_UNAVAILABLE_NOTE,
    }


def _load_subscription_model_registry() -> Dict[str, Any]:
    if not SUBSCRIPTION_REGISTRY_FILE.exists():
        return {"models": []}
    try:
        parsed = json.loads(SUBSCRIPTION_REGISTRY_FILE.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else {"models": []}
    except Exception:
        return {"models": []}


def _build_subscription_model_catalog(pricing_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    provider = "openai_codex"

    registry = _load_subscription_model_registry()
    items = registry.get("models", []) if isinstance(registry, dict) else []

    records: List[Dict[str, Any]] = []
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                model_id = str(item.get("model_id", "") or "").strip()
                if not model_id:
                    continue
                records.append(
                    _build_model_record(
                        provider,
                        model_id,
                        display_name=item.get("label") or model_id,
                        pricing_overrides=pricing_overrides,
                        reasoning_options={
                            "supports_reasoning": bool(item.get("supports_reasoning", False)),
                            "reasoning_mode": item.get("reasoning_mode"),
                        },
                    )
                )
            elif isinstance(item, str) and item.strip():
                records.append(_build_model_record(provider, item.strip(), pricing_overrides=pricing_overrides))

    if records:
        return {
            "provider": provider,
            "source": "curated_subscription",
            "message": "ChatGPT Subscription models are curated locally.",
            "models": _unique_model_records(records),
        }

    return {
        "provider": provider,
        "source": "curated_subscription",
        "message": "ChatGPT Subscription models are curated locally.",
        "models": _default_model_catalog(provider, message="", pricing_overrides=pricing_overrides)["models"],
    }


def _unique_model_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique: List[Dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        model_id = str(record.get("model_id", "") or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        unique.append(record)
    return unique


def _default_model_catalog(provider: str, *, message: str, pricing_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "provider": provider,
        "source": "fallback",
        "message": message,
        "models": [
            _build_model_record(provider, model_id, pricing_overrides=pricing_overrides)
            for model_id in get_default_model_options(provider)
        ],
    }


def _is_deprecated_model(item: Dict) -> bool:
    status = str(item.get("status", "")).strip().lower()
    state = str(item.get("state", "")).strip().lower()
    if status in {"deprecated", "retired", "legacy"} or state in {"deprecated", "retired", "legacy"}:
        return True
    deprecated_flag = item.get("deprecated")
    if isinstance(deprecated_flag, bool):
        return deprecated_flag
    if deprecated_flag is not None:
        return str(deprecated_flag).strip().lower() in {"true", "yes", "1"}
    return False


def _ensure_dict(payload: Any) -> Dict:
    return payload if isinstance(payload, dict) else {}


def _looks_like_openai_chat_model(model_id: str) -> bool:
    model_id_normalized = (model_id or "").strip().lower()
    if not model_id_normalized:
        return False
    if not model_id_normalized.startswith(("gpt", "o", "chatgpt")):
        return False
    excluded_fragments = (
        "embedding",
        "image",
        "audio",
        "moderation",
        "realtime",
        "tts",
        "transcribe",
        "whisper",
        "dall-e",
    )
    return not any(fragment in model_id_normalized for fragment in excluded_fragments)


def _fetch_openai_model_objects(api_key: str) -> List[Dict]:
    models: List[Dict] = []
    after_id: Optional[str] = None

    for _ in range(MAX_MODEL_PAGES):
        params: Dict[str, str] = {}
        if after_id:
            params["after_id"] = after_id
            params["limit"] = str(MODEL_PAGE_SIZE)

        response = requests.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
            params=params,
            timeout=20,
        )
        response.raise_for_status()

        payload = _ensure_dict(response.json())
        batch = payload.get("data", [])
        if isinstance(batch, list):
            models.extend(item for item in batch if isinstance(item, dict))

        has_more = bool(payload.get("has_more"))
        last_id = str(payload.get("last_id", "") or "").strip()
        if not has_more or not last_id or last_id == after_id:
            break
        after_id = last_id

    return models


def _fetch_openai_compatible_model_objects(base_url: str, api_key: Optional[str]) -> List[Dict]:
    headers: Dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    response = requests.get(
        _build_openai_compatible_endpoint(base_url, "models"),
        headers=headers,
        timeout=20,
    )
    response.raise_for_status()

    payload = _ensure_dict(response.json())
    batch = payload.get("data", [])
    if not isinstance(batch, list):
        return []
    return [item for item in batch if isinstance(item, dict)]


def _fetch_anthropic_model_objects(api_key: str) -> List[Dict]:
    models: List[Dict] = []
    after_id: Optional[str] = None

    for _ in range(MAX_MODEL_PAGES):
        params: Dict[str, str] = {}
        if after_id:
            params["after_id"] = after_id
            params["limit"] = str(MODEL_PAGE_SIZE)

        response = requests.get(
            "https://api.anthropic.com/v1/models",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            params=params,
            timeout=20,
        )
        response.raise_for_status()

        payload = _ensure_dict(response.json())
        batch = payload.get("data", [])
        if isinstance(batch, list):
            models.extend(item for item in batch if isinstance(item, dict))

        has_more = bool(payload.get("has_more"))
        last_id = str(payload.get("last_id", "") or "").strip()
        if not has_more or not last_id or last_id == after_id:
            break
        after_id = last_id

    return models


def _fetch_google_model_objects_from_endpoint(api_key: str, endpoint_url: str) -> List[Dict]:
    models: List[Dict] = []
    page_token: Optional[str] = None

    for _ in range(MAX_MODEL_PAGES):
        params: Dict[str, str] = {"key": api_key, "pageSize": str(MODEL_PAGE_SIZE)}
        if page_token:
            params["pageToken"] = page_token

        response = requests.get(endpoint_url, params=params, timeout=20)
        response.raise_for_status()

        payload = _ensure_dict(response.json())
        batch = payload.get("models", [])
        if isinstance(batch, list):
            models.extend(item for item in batch if isinstance(item, dict))

        next_page_token = str(payload.get("nextPageToken", "") or "").strip()
        if not next_page_token or next_page_token == page_token:
            break
        page_token = next_page_token

    return models


def _fetch_google_model_objects(api_key: str) -> List[Dict]:
    endpoints = [
        "https://generativelanguage.googleapis.com/v1beta/models",
        "https://generativelanguage.googleapis.com/v1/models",
    ]
    last_exception: Optional[Exception] = None

    for endpoint in endpoints:
        try:
            return _fetch_google_model_objects_from_endpoint(api_key, endpoint)
        except requests.HTTPError as exc:
            last_exception = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code in {400, 404}:
                continue
            raise
        except requests.RequestException as exc:
            last_exception = exc
            continue

    if last_exception:
        raise last_exception
    return []


def _catalog_error(source_status: str, message: str) -> Dict[str, Any]:
    return {
        "provider": "unknown",
        "source": source_status,
        "message": message,
        "models": [],
    }


def fetch_available_model_catalog(
    provider: str,
    api_key_env: Optional[str] = None,
    pricing_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    # Merge provider-level dynamic pricing with overrides
    merged_pricing = get_pricing_for_provider(provider).copy()
    if isinstance(pricing_overrides, dict):
        merged_pricing.update(pricing_overrides)
    pricing_overrides = merged_pricing

    if provider == "openai_codex":
        return _build_subscription_model_catalog(pricing_overrides=pricing_overrides)

    api_key = resolve_api_key(provider, api_key_env)
    if provider != OPENAI_COMPATIBLE_PROVIDER and not api_key:
        return {
            "provider": provider,
            "source": "missing_credentials",
            "message": f"No API key in '{api_key_env or get_default_api_key_env(provider)}'.",
            "models": [],
        }

    openai_compatible_base_url = ""

    try:
        records: List[Dict[str, Any]] = []

        if provider == "openai":
            data = _fetch_openai_model_objects(api_key)
            for item in data:
                model_id = item.get("id", "")
                if _is_deprecated_model(item) or not _looks_like_openai_chat_model(model_id):
                    continue
                    
                # OpenAI specific reasoning support detection
                reasoning_meta = {"supports_reasoning": False}
                if model_id.startswith(("o1", "o3", "gpt-4.5")):
                    reasoning_meta = {"supports_reasoning": True, "reasoning_mode": "chain_of_thought"}
                    
                records.append(
                    _build_model_record(
                        provider,
                        model_id,
                        pricing_overrides=pricing_overrides,
                        reasoning_options=reasoning_meta,
                    )
                )

        elif provider == OPENAI_COMPATIBLE_PROVIDER:
            openai_compatible_base_url = resolve_openai_compatible_base_url()
            if not openai_compatible_base_url:
                return {
                    "provider": provider,
                    "source": "missing_base_url",
                    "message": (
                        f"No base URL configured in '{OPENAI_COMPATIBLE_BASE_URL_ENV}'. "
                        "Set it from Agent settings when selecting OpenAI Compatible."
                    ),
                    "models": [],
                }

            data = _fetch_openai_compatible_model_objects(openai_compatible_base_url, api_key)
            for item in data:
                model_id = str(item.get("id", "") or "").strip()
                if not model_id or _is_deprecated_model(item):
                    continue

                records.append(
                    _build_model_record(
                        provider,
                        model_id,
                        display_name=item.get("name") or model_id,
                        pricing_overrides=pricing_overrides,
                        reasoning_options={"supports_reasoning": False},
                    )
                )

        elif provider == "anthropic":
            data = _fetch_anthropic_model_objects(api_key)
            for item in data:
                if _is_deprecated_model(item):
                    continue
                
                model_id = item.get("id", "")
                # Anthropic reasoning support detection
                reasoning_meta = {"supports_reasoning": False}
                if "sonnet" in model_id or "opus" in model_id:
                     reasoning_meta = {"supports_reasoning": True, "reasoning_mode": "deliberation"}

                records.append(
                    _build_model_record(
                        provider,
                        model_id,
                        display_name=item.get("display_name") or model_id,
                        max_input_tokens=item.get("max_input_tokens"),
                        max_output_tokens=item.get("max_tokens"),
                        pricing_overrides=pricing_overrides,
                        reasoning_options=reasoning_meta,
                    )
                )

        elif provider == "google_gemini":
            data = _fetch_google_model_objects(api_key)
            for item in data:
                if _is_deprecated_model(item):
                    continue
                methods = item.get("supportedGenerationMethods", [])
                if not isinstance(methods, list) or "generateContent" not in methods:
                    continue

                model_id = str(item.get("baseModelId", "") or "").strip()
                if not model_id:
                    name = str(item.get("name", "") or "").strip()
                    model_id = name.split("/", 1)[1] if name.startswith("models/") else name

                reasoning_meta: Dict[str, Any] = {"supports_reasoning": False}
                if model_id.startswith("gemini-3"):
                    reasoning_meta = {"supports_reasoning": True, "reasoning_mode": "thinkingLevel"}
                elif model_id.startswith("gemini-2.5"):
                    reasoning_meta = {"supports_reasoning": True, "reasoning_mode": "thinkingBudget"}

                records.append(
                    _build_model_record(
                        provider,
                        model_id,
                        display_name=item.get("displayName") or model_id,
                        max_input_tokens=item.get("inputTokenLimit"),
                        max_output_tokens=item.get("outputTokenLimit"),
                        pricing_overrides=pricing_overrides,
                        reasoning_options=reasoning_meta,
                    )
                )

        else:
            return {
                "provider": provider,
                "source": "unsupported",
                "message": f"Provider '{provider}' not supported for live fetch.",
                "models": [],
            }

        unique_records = _unique_model_records(records)
        if unique_records:
            return {
                "provider": provider,
                "source": "live",
                "message": "Live models fetched from provider API.",
                "models": unique_records,
            }

        return {
            "provider": provider,
            "source": "fetch_failed",
            "message": "Live models were fetched but no compatible ones were returned.",
            "models": [],
        }

    except requests.HTTPError as exc:
        status_code = _http_status_code_from_error(exc)
        response_text = ""
        if getattr(exc, "response", None) is not None:
            response_text = str(getattr(exc.response, "text", "") or "")[:200]

        if provider == OPENAI_COMPATIBLE_PROVIDER and status_code in {401, 403} and not api_key:
            env_name = api_key_env or get_default_api_key_env(provider)
            return {
                "provider": provider,
                "source": "auth_required",
                "message": (
                    "OpenAI-compatible endpoint requires an API key for `/v1/models`. "
                    f"Set `{env_name}` in the environment or in agent settings and refresh the model list."
                ),
                "models": [],
            }

        status_label = status_code if status_code is not None else "unknown"
        message = f"Fetch failed ({status_label}); {response_text}"
        return {
            "provider": provider,
            "source": "fetch_failed",
            "message": message,
            "models": [],
        }
    except Exception as exc:
        return {
            "provider": provider,
            "source": "fetch_failed",
            "message": f"Live fetch error ({type(exc).__name__}): {str(exc)}",
            "models": [],
        }


def fetch_available_models(provider: str, api_key_env: Optional[str] = None) -> List[str]:
    catalog = fetch_available_model_catalog(provider, api_key_env=api_key_env)
    records = catalog.get("models", []) if isinstance(catalog, dict) else []
    model_ids = [
        str(item.get("model_id", "")).strip()
        for item in records
        if isinstance(item, dict)
    ]
    return _unique_models(model_ids) or get_default_model_options(provider)


def _extract_text_from_google_response(payload: Dict) -> str:
    candidates = payload.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    texts = [part.get("text", "") for part in parts if part.get("text")]
    return "\n".join(texts).strip()


def _collect_text_chunks(value: Any, chunks: List[str]) -> None:
    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            chunks.append(normalized)
        return

    if isinstance(value, dict):
        _collect_text_chunks(value.get("text"), chunks)
        _collect_text_chunks(value.get("output_text"), chunks)
        _collect_text_chunks(value.get("content"), chunks)
        return

    if isinstance(value, list):
        for item in value:
            _collect_text_chunks(item, chunks)


def _extract_text_from_openai_chat_payload(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""

    chunks: List[str] = []

    choices = payload.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue

            message = choice.get("message")
            if isinstance(message, dict):
                _collect_text_chunks(message.get("content"), chunks)
                _collect_text_chunks(message.get("text"), chunks)
                _collect_text_chunks(message.get("output_text"), chunks)
            else:
                _collect_text_chunks(message, chunks)

            _collect_text_chunks(choice.get("text"), chunks)
            _collect_text_chunks(choice.get("delta"), chunks)

    _collect_text_chunks(payload.get("output_text"), chunks)
    _collect_text_chunks(payload.get("message"), chunks)
    _collect_text_chunks(payload.get("content"), chunks)

    nested_response = payload.get("response")
    if isinstance(nested_response, dict):
        nested_text = _extract_text_from_openai_chat_payload(nested_response)
        _collect_text_chunks(nested_text, chunks)

    if not chunks:
        return ""

    unique_chunks: List[str] = []
    seen = set()
    for chunk in chunks:
        if chunk in seen:
            continue
        seen.add(chunk)
        unique_chunks.append(chunk)

    return "\n".join(unique_chunks).strip()


def _normalize_reasoning_effort(reasoning_effort: Optional[str]) -> str:
    value = str(reasoning_effort or "none").strip().lower()
    return value if value in REASONING_EFFORT_LEVELS else "none"


def _openai_reasoning_effort_value(reasoning_effort: str) -> str:
    # OpenAI supports: none, minimal, low, medium, high, xhigh.
    # UI intentionally exposes none/low/medium/high/xhigh.
    normalized = _normalize_reasoning_effort(reasoning_effort)
    return normalized


def _supports_openai_reasoning_effort(model_name: str) -> bool:
    model = str(model_name or "").strip().lower()
    if not model:
        return False
    return model.startswith(("o1", "o3", "o4", "gpt-5"))


def _build_openai_reasoning_payload(model_name: str, reasoning_effort: str) -> Dict[str, Any]:
    """Return OpenAI reasoning fields when the selected model likely supports them."""
    normalized = _openai_reasoning_effort_value(reasoning_effort)
    if normalized == "none":
        return {}
    if not _supports_openai_reasoning_effort(model_name):
        return {}

    model = str(model_name or "").strip().lower()
    # Conservative fallback: gpt-5.1 family is known to reject some extra effort values.
    if model.startswith("gpt-5.1") and normalized == "xhigh":
        normalized = "high"

    return {"reasoning_effort": normalized}


def _is_unsupported_reasoning_parameter_message(message: str) -> bool:
    text = str(message or "").strip().lower()
    if not text:
        return False
    has_unsupported_hint = any(token in text for token in ["unsupported parameter", "unknown parameter", "not supported"])
    has_reasoning_hint = any(token in text for token in ["reasoning_effort", "reasoning.effort", "reasoning"])
    return has_unsupported_hint and has_reasoning_hint


def _is_unsupported_reasoning_http_error(exc: Exception) -> bool:
    status = _http_status_code_from_error(exc)
    if status not in {400, 422}:
        return False
    response_text = ""
    response = getattr(exc, "response", None)
    if response is not None:
        response_text = str(getattr(response, "text", "") or "")
    return _is_unsupported_reasoning_parameter_message(response_text) or _is_unsupported_reasoning_parameter_message(str(exc))


def _anthropic_thinking_payload(reasoning_effort: str) -> Optional[Dict[str, Any]]:
    normalized = _normalize_reasoning_effort(reasoning_effort)
    if normalized == "none":
        return None
    budget_by_effort = {
        "low": 1024,
        "medium": 2048,
        "high": 4096,
        "xhigh": 8192,
    }
    return {"type": "enabled", "budget_tokens": budget_by_effort.get(normalized, 2048)}


def _google_thinking_config(model_name: str, reasoning_effort: str) -> Optional[Dict[str, Any]]:
    normalized = _normalize_reasoning_effort(reasoning_effort)
    if normalized == "none":
        # Explicitly disable thinking for Gemini 2.5 when requested.
        if str(model_name or "").strip().lower().startswith("gemini-2.5"):
            return {"thinkingBudget": 0}
        return None

    model = str(model_name or "").strip().lower()
    if model.startswith("gemini-3"):
        return {"thinkingLevel": normalized}

    if model.startswith("gemini-2.5"):
        budget_by_effort = {
            "low": 1024,
            "medium": 4096,
            "high": 8192,
            "xhigh": 16384,
        }
        return {"thinkingBudget": budget_by_effort.get(normalized, 4096)}

    return None


def _estimate_tokens(text: str) -> int:
    """Rough token estimation (approx. 4 characters per token)."""
    if not text:
        return 0
    return len(text) // 4


def _calculate_cost(provider: str, model_id: str, input_tokens: int, output_tokens: int) -> float:
    # Subscription-based models have no per-token cost
    if provider == "openai_codex":
        return 0.0

    pricing = get_pricing_for_provider(provider)
    model_pricing = pricing.get(model_id) or pricing.get(model_id.lower())
    if not model_pricing:
        return 0.0
    
    input_price = model_pricing.get("input_usd_per_mtok", 0)
    output_price = model_pricing.get("output_usd_per_mtok", 0)
    
    return (input_tokens * input_price / 1_000_000) + (output_tokens * output_price / 1_000_000)


def generate_text_completion(
    provider: str,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    *,
    api_key_env: Optional[str] = None,
    temperature: float = 0,
    max_tokens: int = 1024,
    reasoning_effort: str = "none",
    interaction_purpose: Optional[str] = None,
    term_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> str:
    api_key = resolve_api_key(provider, api_key_env)
    if provider not in {"openai_codex", OPENAI_COMPATIBLE_PROVIDER} and not api_key:
        raise RuntimeError(
            f"Missing API key for provider '{provider}' in environment variable '{api_key_env or get_default_api_key_env(provider)}'."
        )

    try:
        normalized_reasoning_effort = _normalize_reasoning_effort(reasoning_effort)
        input_tokens = _estimate_tokens(system_prompt + user_prompt)
        output_tokens = 0
        text = ""

        if provider == "openai":
            request_payload = {
                "model": model_name,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }
            request_payload.update(_build_openai_reasoning_payload(model_name, normalized_reasoning_effort))

            def _post_openai_chat(payload: Dict[str, Any]) -> requests.Response:
                return requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json=dict(payload),
                    timeout=60,
                )

            response = _post_openai_chat(request_payload)
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                if "reasoning_effort" in request_payload and _is_unsupported_reasoning_http_error(exc):
                    request_payload.pop("reasoning_effort", None)
                    response = _post_openai_chat(request_payload)
                    response.raise_for_status()
                else:
                    raise
            payload = response.json()
            text = payload["choices"][0]["message"]["content"].strip()
            
            usage = payload.get("usage", {})
            input_tokens = usage.get("prompt_tokens", input_tokens)
            output_tokens = usage.get("completion_tokens", 0)

        elif provider == OPENAI_COMPATIBLE_PROVIDER:
            base_url = resolve_openai_compatible_base_url()
            if not base_url:
                raise RuntimeError(
                    f"Missing base URL for provider '{OPENAI_COMPATIBLE_PROVIDER}' in '{OPENAI_COMPATIBLE_BASE_URL_ENV}'."
                )

            request_payload = {
                "model": model_name,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }
            headers = {"Content-Type": "application/json"}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            response = requests.post(
                _build_openai_compatible_endpoint(base_url, "chat/completions"),
                headers=headers,
                json=request_payload,
                timeout=60,
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                if _is_auth_required_http_error(exc):
                    env_name = api_key_env or get_default_api_key_env(provider)
                    if api_key:
                        raise RuntimeError(
                            "OpenAI-compatible API key was rejected by the endpoint. "
                            f"Check key in '{env_name}' and endpoint authorization settings."
                        ) from exc
                    raise RuntimeError(
                        "OpenAI-compatible endpoint requires an API key for chat completions. "
                        f"Set '{env_name}' to continue."
                    ) from exc
                raise
            payload = response.json()
            text = _extract_text_from_openai_chat_payload(payload)
            if not text:
                raise RuntimeError(
                    "OpenAI-compatible response did not include text content in "
                    "`choices[].message.content` or equivalent fields."
                )

            usage = payload.get("usage", {})
            input_tokens = usage.get("prompt_tokens", input_tokens)
            output_tokens = usage.get("completion_tokens", _estimate_tokens(text))

        elif provider == "openai_codex":
            text = create_codex_response(
                model_name=model_name,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                reasoning_effort=normalized_reasoning_effort,
            )
            # Codex doesn't return usage info in this format yet, stick to estimate
            output_tokens = _estimate_tokens(text)

        elif provider == "anthropic":
            request_payload = {
                "model": model_name,
                "system": system_prompt,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [{"role": "user", "content": user_prompt}],
            }
            thinking = _anthropic_thinking_payload(normalized_reasoning_effort)
            if thinking:
                request_payload["thinking"] = thinking
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=request_payload,
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            blocks = payload.get("content", [])
            text = "\n".join(block.get("text", "") for block in blocks if block.get("text")).strip()
            
            usage = payload.get("usage", {})
            input_tokens = usage.get("input_tokens", input_tokens)
            output_tokens = usage.get("output_tokens", 0)

        elif provider == "google_gemini":
            generation_config: Dict[str, Any] = {"temperature": temperature, "maxOutputTokens": max_tokens}
            thinking_config = _google_thinking_config(model_name, normalized_reasoning_effort)
            if thinking_config:
                generation_config["thinkingConfig"] = thinking_config
            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}",
                headers={"Content-Type": "application/json"},
                json={
                    "systemInstruction": {"parts": [{"text": system_prompt}]},
                    "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                    "generationConfig": generation_config,
                },
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            text = _extract_text_from_google_response(payload)
            
            usage = payload.get("usageMetadata", {})
            input_tokens = usage.get("promptTokenCount", input_tokens)
            output_tokens = usage.get("candidatesTokenCount", 0)
        
        else:
             raise ValueError(f"Unsupported provider: {provider}")

        cost_usd = _calculate_cost(provider, model_name, input_tokens, output_tokens)
        record_llm_interaction(
            provider=provider,
            model_name=model_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_text=text,
            interaction_purpose=interaction_purpose,
            term_id=term_id,
            run_id=run_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
        )
        return text

        raise ValueError(f"Unsupported provider: {provider}")
    except Exception as exc:
        record_llm_interaction(
            provider=provider,
            model_name=model_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            error=f"{type(exc).__name__}: {exc}",
            interaction_purpose=interaction_purpose,
            term_id=term_id,
            run_id=run_id,
        )
        raise


def _extract_first_json_object(text: str) -> Dict:
    text = (text or "").strip()
    if not text:
        raise ValueError("No content returned for JSON extraction.")

    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if fence_match:
        fenced_payload = (fence_match.group(1) or "").strip()
        if fenced_payload:
            text = fenced_payload

    try:
        return json.loads(text)
    except Exception:
        pass

    decoder = json.JSONDecoder()
    obj_start = text.find("{")
    while obj_start != -1:
        candidate = text[obj_start:].lstrip()
        try:
            parsed, _end = decoder.raw_decode(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        obj_start = text.find("{", obj_start + 1)

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return json.loads(match.group(0))

    raise ValueError("No valid JSON object found in model response.")


def generate_json_completion(
    provider: str,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    *,
    api_key_env: Optional[str] = None,
    temperature: float = 0,
    max_tokens: int = 1024,
    reasoning_effort: str = "none",
    interaction_purpose: Optional[str] = None,
    term_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Dict:
    json_prompt = (
        user_prompt.rstrip()
        + "\n\nReturn valid JSON only. Do not wrap the JSON in markdown fences or add explanatory prose."
    )
    response_text = generate_text_completion(
        provider,
        model_name,
        system_prompt,
        json_prompt,
        api_key_env=api_key_env,
        temperature=temperature,
        max_tokens=max_tokens,
        reasoning_effort=reasoning_effort,
        interaction_purpose=interaction_purpose,
        term_id=term_id,
        run_id=run_id,
    )
    return _extract_first_json_object(response_text)


def _normalize_structured_completion_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return payload


def generate_structured_completion(
    provider: str,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    *,
    api_key_env: Optional[str] = None,
    temperature: float = 0,
    max_tokens: int = 1024,
    reasoning_effort: str = "none",
    retries_on_parse_failure: int = 1,
    interaction_purpose: Optional[str] = None,
    term_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate strict JSON with bounded parse retries.

    This is a backward-compatible helper used by planner workflows.
    """
    attempts = max(1, int(retries_on_parse_failure or 0) + 1)
    last_error: Optional[Exception] = None
    for _ in range(attempts):
        try:
            payload = generate_json_completion(
                provider,
                model_name,
                system_prompt,
                user_prompt,
                api_key_env=api_key_env,
                temperature=temperature,
                max_tokens=max_tokens,
                reasoning_effort=reasoning_effort,
                interaction_purpose=interaction_purpose,
                term_id=term_id,
                run_id=run_id,
            )
            normalized = _normalize_structured_completion_payload(payload)
            if normalized:
                return normalized
            raise ValueError("Structured completion payload was empty or invalid.")
        except Exception as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    raise RuntimeError("Structured completion failed without a concrete error.")
