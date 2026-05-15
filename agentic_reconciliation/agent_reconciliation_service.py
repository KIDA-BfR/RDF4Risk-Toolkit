# -*- coding: utf-8 -*-
"""Backend service for the Material UI agent reconciliation app."""

from __future__ import annotations

import io
import json
import os
import re
import threading
import time
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd
import yaml

try:
    from .agent_runtime_state import runtime_state
    from .agent_codex_subscription_service import clear_codex_credentials, get_codex_auth_status, is_codex_authenticated, start_codex_authorization_flow
    from .agent_definition_service import build_definition_lookup, prepare_used_definitions_df
    from .agent_llm_service import generate_text_completion, fetch_available_model_catalog, get_default_api_key_env, get_default_model_options, get_provider_label, is_openai_compatible_auth_required_error, get_supported_llm_providers
    from .agent_pricing_service import fetch_all_pricing
    from .agent_file_service import make_input_table
    from .agent_models import AgentInputTable, AgentRunConfig
    from .agent_langsmith_monitoring import build_run_url, configure_langsmith_environment, get_langsmith_readiness, get_llm_interactions, reset_llm_interactions
    from .agent_orchestrator import run_agent_batch
    from .agent_skos_service import normalize_mapping_type
    from .agent_reconciliation_ui_state import _sync_selected_source_dataframe, _store_input_tables, _build_run_input_tables
    from .agent_reconciliation_ui_review import _apply_review_action, _get_reviewable_agent_result_indices, _get_review_cell_value
    from .agent_reconciliation_ui_monitoring import _build_monitoring_event_snapshot, _build_cascade_trace_snapshot
    from semi_automatic_reconciliation.reconciliation_core import CONFIG
    from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS, REQUIRED_MATCHING_TABLE_COLUMNS, get_unreconciled_indices, finalize_accepted_results
except ImportError:  # pragma: no cover
    from agent_runtime_state import runtime_state
    from agent_codex_subscription_service import clear_codex_credentials, get_codex_auth_status, is_codex_authenticated, start_codex_authorization_flow
    from agent_definition_service import build_definition_lookup, prepare_used_definitions_df
    from agent_llm_service import generate_text_completion, fetch_available_model_catalog, get_default_api_key_env, get_default_model_options, get_provider_label, is_openai_compatible_auth_required_error, get_supported_llm_providers
    from agent_pricing_service import fetch_all_pricing
    from agent_file_service import make_input_table
    from agent_models import AgentInputTable, AgentRunConfig
    from agent_langsmith_monitoring import build_run_url, configure_langsmith_environment, get_langsmith_readiness, get_llm_interactions, reset_llm_interactions
    from agent_orchestrator import run_agent_batch
    from agent_skos_service import normalize_mapping_type
    from agent_reconciliation_ui_state import _sync_selected_source_dataframe, _store_input_tables, _build_run_input_tables
    from agent_reconciliation_ui_review import _apply_review_action, _get_reviewable_agent_result_indices, _get_review_cell_value
    from agent_reconciliation_ui_monitoring import _build_monitoring_event_snapshot, _build_cascade_trace_snapshot
    from semi_automatic_reconciliation.reconciliation_core import CONFIG
    from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS, REQUIRED_MATCHING_TABLE_COLUMNS, get_unreconciled_indices, finalize_accepted_results

AGENT_DATAFRAME_STATE_KEY = "agent_reconciliation_df"
AGENT_DATA_SOURCE_MESSAGE_KEY = "agent_reconciliation_source_message"
AGENT_LAST_SOURCE_NAME_KEY = "agent_reconciliation_last_source_name"
AGENT_INPUT_TABLES_KEY = "agent_reconciliation_input_tables"
AGENT_RESULTS_BY_SOURCE_KEY = "agent_reconciliation_results_by_source"
AGENT_SELECTED_SOURCE_KEY = "agent_reconciliation_selected_source"
AGENT_DEFINITIONS_BY_SOURCE_KEY = "agent_reconciliation_definitions_by_source"
AGENT_RUN_MESSAGES_KEY = "agent_reconciliation_run_messages"
AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY = "agent_reconciliation_available_models_by_provider"
AGENT_MONITORING_STATE_KEY = "agent_reconciliation_monitoring_state"
AGENT_STOP_EVENT_KEY = "agent_reconciliation_stop_event"
AGENT_UPLOADED_SOURCE_SIGNATURE_KEY = "agent_reconciliation_uploaded_source_signature"
AGENT_WORKFLOW_CONFIG_STATE_KEY = "agent_workflow_config_json"
AGENT_ACTIVE_STEP_KEY = "agent_reconciliation_active_step"
AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY = "agent_workflow_component_action_nonce"
AGENT_RUN_STATUS_STATE_KEY = "agent_reconciliation_run_status"
AGENT_RUN_THREAD_STATE_KEY = "agent_reconciliation_run_thread"
AGENT_SSSOM_EXPORT_PAYLOAD_KEY = "agent_reconciliation_sssom_export_payload"
API_KEY_PLACEHOLDERS = {"", "yourapikey", "your_api_key", "replace-with-api-key", "replace_with_api_key", "replace-me", "changeme", "none", "null", "<api_key>"}
OPENAI_CODEX_PROVIDER = "openai_codex"
OPENAI_COMPATIBLE_PROVIDER = "openai_compatible"
OPENAI_COMPATIBLE_BASE_URL_ENV = "OPENAI_COMPATIBLE_BASE_URL"
OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY = "openai_compatible_base_url"
OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY = "openai_compatible_model_registry"
OPENAI_COMPATIBLE_MODEL_REGISTRY_MAX_ITEMS = 50
ORCID_BASE_URL = "https://orcid.org/"
LEGACY_AGENT_MODEL_CONFIG_KEYS = ("model_provider", "model_name", "definition_model_name", "planner_model_provider", "planner_model_name", "planner_model_api_key_env")
REASONING_EFFORT_OPTIONS = ["none", "low", "medium", "high", "xhigh"]
_STAGE_TO_COMPONENT = {"Setup": "setup", "Run": "run", "Review": "review", "Export": "export"}
_COMPONENT_TO_STAGE = {value: key for key, value in _STAGE_TO_COMPONENT.items()}


def _get_reconciliation_config_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config.yaml",
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

def _get_openai_compatible_base_url_from_config() -> str:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    return _normalize_openai_compatible_base_url(agent_cfg.get(OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY))

def _get_openai_compatible_registered_models_from_config() -> List[str]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    values = agent_cfg.get(OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY, [])
    if not isinstance(values, list):
        return []
    ordered: List[str] = []
    seen = set()
    for raw_value in values:
        model_id = str(raw_value or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        ordered.append(model_id)
    return ordered

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

def _verify_openai_compatible_model_availability(model_name: str) -> tuple[bool, str]:
    candidate = str(model_name or "").strip()
    if not candidate:
        return False, "Model name is empty."

    if not _ensure_openai_compatible_base_url_available():
        return False, "OpenAI-compatible base URL is required before model verification."

    try:
        response = generate_text_completion(
            provider=OPENAI_COMPATIBLE_PROVIDER,
            model_name=candidate,
            system_prompt="Reply with exactly: OK",
            user_prompt="OK",
            api_key_env=get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER),
            temperature=0,
            max_tokens=8,
            interaction_purpose="model_registry_verification",
        )
        if str(response or "").strip():
            return True, "Model verified via OpenAI-compatible completion check."
        return False, "Verification call returned an empty response."
    except Exception as exc:
        if is_openai_compatible_auth_required_error(exc):
            env_name = get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            return False, (
                "OpenAI-compatible endpoint requires authentication for model verification. "
                f"Set `{env_name}` or provide the OpenAI-compatible API key field and try again."
            )
        return False, f"Model verification failed: {type(exc).__name__}: {exc}"

def _save_openai_compatible_model_registry(models: List[str]) -> tuple[bool, str]:
    normalized_models = _normalize_openai_compatible_model_registry(models)
    config_path = _get_reconciliation_config_path()
    try:
        loaded = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            loaded = {}

        agent_cfg = loaded.setdefault("agent_reconciliation", {})
        if not isinstance(agent_cfg, dict):
            agent_cfg = {}
            loaded["agent_reconciliation"] = agent_cfg

        agent_cfg[OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY] = normalized_models

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(loaded, f, sort_keys=False, allow_unicode=True)

        global CONFIG
        if not isinstance(CONFIG, dict):
            CONFIG = {}
        runtime_agent_cfg = CONFIG.setdefault("agent_reconciliation", {})
        if not isinstance(runtime_agent_cfg, dict):
            runtime_agent_cfg = {}
            CONFIG["agent_reconciliation"] = runtime_agent_cfg
        runtime_agent_cfg[OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY] = normalized_models

        return True, "OpenAI-compatible model register saved to config.yaml."
    except Exception as exc:
        return False, f"Unable to save OpenAI-compatible model register: {exc}"

def _ensure_openai_compatible_base_url_available() -> bool:
    configured = _normalize_openai_compatible_base_url(
        runtime_state.get("agent_openai_compatible_base_url")
    )
    if configured:
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = configured
        return True

    env_value = _normalize_openai_compatible_base_url(os.getenv(OPENAI_COMPATIBLE_BASE_URL_ENV))
    if env_value:
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = env_value
        return True

    cfg_value = _get_openai_compatible_base_url_from_config()
    if cfg_value:
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = cfg_value
        return True

    return False

def _resolve_api_key_env_for_provider(
    session_key: Optional[str],
    provider: str,
    config_key: Optional[str] = None,
) -> str:
    """Resolve API key env name with provider-aware defaults, without mutating widget state."""
    if _is_codex_provider(provider):
        return get_default_api_key_env(provider)

    configured_default = get_default_api_key_env(provider)
    if config_key:
        configured_default = (CONFIG or {}).get("agent_reconciliation", {}).get(
            config_key,
            configured_default,
        )

    if session_key:
        env_name = str(runtime_state.get(session_key, configured_default) or "").strip()
    else:
        env_name = str(configured_default or "").strip()

    return env_name or get_default_api_key_env(provider)

def _save_preferred_model_selection(provider: str, model_name: str) -> tuple[bool, str]:
    provider_value = str(provider or "").strip()
    model_value = str(model_name or "").strip()
    if not provider_value or not model_value:
        return False, "Provider and model must be selected before saving preferences."

    config_path = _get_reconciliation_config_path()
    try:
        loaded = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            loaded = {}

        agent_cfg = loaded.setdefault("agent_reconciliation", {})
        if not isinstance(agent_cfg, dict):
            agent_cfg = {}
            loaded["agent_reconciliation"] = agent_cfg

        agent_cfg["preferred_model_provider"] = provider_value
        agent_cfg["preferred_model_name"] = model_value
        if _is_openai_compatible_provider(provider_value):
            compatible_base_url = _normalize_openai_compatible_base_url(
                runtime_state.get("agent_openai_compatible_base_url")
            )
            if compatible_base_url:
                agent_cfg[OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY] = compatible_base_url
            compatible_api_key = str(
                runtime_state.get("agent_openai_compatible_api_key", "")
            ).strip()
            if compatible_api_key:
                provider_keys_cfg = agent_cfg.get("provider_api_keys")
                if not isinstance(provider_keys_cfg, dict):
                    provider_keys_cfg = {}
                provider_keys_cfg[OPENAI_COMPATIBLE_PROVIDER] = compatible_api_key
                agent_cfg["provider_api_keys"] = provider_keys_cfg
        for legacy_key in LEGACY_AGENT_MODEL_CONFIG_KEYS:
            agent_cfg.pop(legacy_key, None)

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(loaded, f, sort_keys=False, allow_unicode=True)

        global CONFIG
        if not isinstance(CONFIG, dict):
            CONFIG = {}
        runtime_agent_cfg = CONFIG.setdefault("agent_reconciliation", {})
        if not isinstance(runtime_agent_cfg, dict):
            runtime_agent_cfg = {}
            CONFIG["agent_reconciliation"] = runtime_agent_cfg
        runtime_agent_cfg["preferred_model_provider"] = provider_value
        runtime_agent_cfg["preferred_model_name"] = model_value
        if _is_openai_compatible_provider(provider_value):
            compatible_base_url = _normalize_openai_compatible_base_url(
                runtime_state.get("agent_openai_compatible_base_url")
            )
            if compatible_base_url:
                runtime_agent_cfg[OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY] = compatible_base_url
                os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = compatible_base_url
            compatible_api_key = str(
                runtime_state.get("agent_openai_compatible_api_key", "")
            ).strip()
            if compatible_api_key:
                provider_keys_runtime = runtime_agent_cfg.get("provider_api_keys")
                if not isinstance(provider_keys_runtime, dict):
                    provider_keys_runtime = {}
                provider_keys_runtime[OPENAI_COMPATIBLE_PROVIDER] = compatible_api_key
                runtime_agent_cfg["provider_api_keys"] = provider_keys_runtime
                os.environ[get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)] = compatible_api_key
        for legacy_key in LEGACY_AGENT_MODEL_CONFIG_KEYS:
            runtime_agent_cfg.pop(legacy_key, None)

        return True, "Preferred model selection saved to config.yaml (API keys are kept in environment variables only)."
    except Exception as exc:
        return False, f"Unable to save preferred model selection: {exc}"

def _normalize_orcid_identifier(value: Optional[str]) -> str:
    """Normalize ORCID input to canonical https://orcid.org/<ID> URL format."""
    raw = str(value or "").strip()
    if not raw:
        return ""

    # Accept full URL and plain ORCID ID forms.
    if raw.lower().startswith("http://") or raw.lower().startswith("https://"):
        match = re.search(r"(\d{4}-\d{4}-\d{4}-[\dXx]{4})", raw)
    else:
        match = re.fullmatch(r"(\d{4}-\d{4}-\d{4}-[\dXx]{4})", raw)

    if not match:
        return ""

    normalized_id = match.group(1).upper()
    return f"{ORCID_BASE_URL}{normalized_id}"

def _get_provenance_defaults_from_config() -> Dict[str, str]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    defaults = agent_cfg.get("provenance_defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}
    return {
        "author_id": str(defaults.get("author_id", "") or "").strip(),
        "author_label": str(defaults.get("author_label", "") or "").strip(),
        "reviewer_id": str(defaults.get("reviewer_id", "") or "").strip(),
        "reviewer_label": str(defaults.get("reviewer_label", "") or "").strip(),
        "creator_id": str(defaults.get("creator_id", "") or "").strip(),
        "creator_label": str(defaults.get("creator_label", "") or "").strip(),
        "mapping_tool": str(defaults.get("mapping_tool", "RDF4Risk Agent-Based Reconciliation") or "").strip(),
        "mapping_tool_version": str(defaults.get("mapping_tool_version", "PoC") or "").strip(),
        "mapping_date": str(defaults.get("mapping_date", "") or "").strip(),
        "publication_date": str(defaults.get("publication_date", "") or "").strip(),
    }

def _build_provenance_defaults_from_state() -> Dict[str, str]:
    author_id = _normalize_orcid_identifier(runtime_state.get("agent_prov_author_orcid", ""))
    reviewer_id = _normalize_orcid_identifier(runtime_state.get("agent_prov_reviewer_orcid", ""))
    creator_id = _normalize_orcid_identifier(runtime_state.get("agent_prov_creator_orcid", ""))

    mapping_date_value = str(runtime_state.get("agent_prov_last_run_mapping_date", "") or "").strip() or date.today().isoformat()
    publication_date_value = str(runtime_state.get("agent_prov_publication_date", "") or "").strip()

    return {
        "author_id": author_id,
        "author_label": str(runtime_state.get("agent_prov_author_name", "") or "").strip(),
        "reviewer_id": reviewer_id,
        "reviewer_label": str(runtime_state.get("agent_prov_reviewer_name", "") or "").strip(),
        "creator_id": creator_id,
        "creator_label": str(runtime_state.get("agent_prov_creator_name", "") or "").strip(),
        "mapping_tool": str(runtime_state.get("agent_prov_mapping_tool", "") or "").strip(),
        "mapping_tool_version": str(runtime_state.get("agent_prov_mapping_tool_version", "") or "").strip(),
        "mapping_date": mapping_date_value,
        "publication_date": publication_date_value,
    }

def _save_preferred_provenance_defaults(defaults: Dict[str, str]) -> tuple[bool, str]:
    config_path = _get_reconciliation_config_path()
    try:
        loaded = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            loaded = {}

        agent_cfg = loaded.setdefault("agent_reconciliation", {})
        if not isinstance(agent_cfg, dict):
            agent_cfg = {}
            loaded["agent_reconciliation"] = agent_cfg

        agent_cfg["provenance_defaults"] = defaults

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(loaded, f, sort_keys=False, allow_unicode=True)

        global CONFIG
        if not isinstance(CONFIG, dict):
            CONFIG = {}
        runtime_agent_cfg = CONFIG.setdefault("agent_reconciliation", {})
        if not isinstance(runtime_agent_cfg, dict):
            runtime_agent_cfg = {}
            CONFIG["agent_reconciliation"] = runtime_agent_cfg
        runtime_agent_cfg["provenance_defaults"] = defaults

        return True, "Preferred provenance defaults saved to config.yaml."
    except Exception as exc:
        return False, f"Unable to save preferred provenance defaults: {exc}"

def _register_openai_compatible_model_from_override(
    custom_model_override: str,
    provider: str,
    api_key_env: str,
) -> tuple[bool, str]:
    model_name = str(custom_model_override or "").strip()
    if not model_name:
        return False, "Enter a custom model name before registering."
    if not _is_openai_compatible_provider(provider):
        return False, "Model registration is only available for the OpenAI-compatible provider."

    ok, verify_message = _verify_openai_compatible_model_availability(model_name)
    if not ok:
        return False, verify_message

    merged_registry = _merge_openai_compatible_model_registry(
        [model_name],
        _get_openai_compatible_registered_models_from_config(),
    )
    saved, save_message = _save_openai_compatible_model_registry(merged_registry)
    if not saved:
        return False, save_message

    _ensure_model_catalog_for_provider(
        provider,
        api_key_env=api_key_env,
        force_refresh=True,
    )
    runtime_state["clear_custom_model_override"] = True
    return True, f"Registered '{model_name}' for OpenAI-compatible provider. It will appear in future sessions."

def _is_populated_api_key(value: Optional[str]) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    lowered = token.lower()
    return lowered not in API_KEY_PLACEHOLDERS and not lowered.startswith("your")

def _get_provider_api_key_from_config(provider: str) -> Optional[str]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    provider_api_keys = agent_cfg.get("provider_api_keys", {})
    if isinstance(provider_api_keys, dict):
        configured = provider_api_keys.get(provider)
        if configured is None:
            configured = provider_api_keys.get(str(provider).lower())
        if _is_populated_api_key(configured):
            return str(configured).strip()

    legacy_field_map = {
        "openai": "openai_api_key",
        "anthropic": "anthropic_api_key",
        "google_gemini": "google_api_key",
    }
    legacy_field = legacy_field_map.get(provider)
    if legacy_field and _is_populated_api_key(agent_cfg.get(legacy_field)):
        return str(agent_cfg.get(legacy_field)).strip()

    return None

def _ensure_provider_api_key_available(provider: str, api_key_env: Optional[str] = None) -> bool:
    if provider == OPENAI_CODEX_PROVIDER:
        return is_codex_authenticated()

    if provider == OPENAI_COMPATIBLE_PROVIDER:
        env_name = (api_key_env or "").strip() or get_default_api_key_env(provider)
        session_key = str(runtime_state.get("agent_openai_compatible_api_key", "") or "").strip()
        if session_key:
            os.environ[env_name] = session_key
            return True
        configured_key = _get_provider_api_key_from_config(provider)
        if configured_key:
            os.environ[env_name] = configured_key
            return True
        if _is_populated_api_key(os.getenv(env_name)):
            return True
        # OpenAI-compatible providers may not require API keys.
        return True

    env_name = (api_key_env or "").strip() or get_default_api_key_env(provider)
    if _is_populated_api_key(os.getenv(env_name)):
        return True

    configured_key = _get_provider_api_key_from_config(provider)
    if configured_key:
        os.environ[env_name] = configured_key
        return True

    return False

def _ensure_langsmith_api_key_available() -> bool:
    if _is_populated_api_key(os.getenv("LANGSMITH_API_KEY")):
        return True

    configured = (CONFIG or {}).get("agent_reconciliation", {}).get("langsmith_api_key")
    if _is_populated_api_key(configured):
        os.environ["LANGSMITH_API_KEY"] = str(configured).strip()
        return True

    return False

def _get_provider_pricing_overrides(provider: str) -> Dict:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    pricing_by_provider = agent_cfg.get("provider_pricing_overrides", {})
    if not isinstance(pricing_by_provider, dict):
        return {}

    entry = pricing_by_provider.get(provider)
    if entry is None:
        entry = pricing_by_provider.get(str(provider).lower())
    return entry if isinstance(entry, dict) else {}

def _extract_model_records_from_catalog(catalog: Optional[Dict]) -> List[Dict]:
    if not isinstance(catalog, dict):
        return []
    records = catalog.get("models", [])
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]

def _extract_model_ids_from_catalog(catalog: Optional[Dict]) -> List[str]:
    model_ids: List[str] = []
    seen = set()
    for record in _extract_model_records_from_catalog(catalog):
        model_id = str(record.get("model_id", "") or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        model_ids.append(model_id)
    return model_ids

def _openai_compatible_catalog_requires_api_key(catalog: Optional[Dict]) -> bool:
    if not isinstance(catalog, dict):
        return False
    return str(catalog.get("source", "") or "").strip() == "auth_required"

def _merge_registered_openai_compatible_models_into_catalog(catalog: Optional[Dict]) -> Dict:
    if not isinstance(catalog, dict):
        return {
            "provider": OPENAI_COMPATIBLE_PROVIDER,
            "source": "fallback",
            "message": "Using registered local model catalog.",
            "models": [],
        }

    if str(catalog.get("provider", "") or "").strip() != OPENAI_COMPATIBLE_PROVIDER:
        return catalog

    registered_models = _get_openai_compatible_registered_models_from_config()
    if not registered_models:
        return catalog

    merged_catalog = dict(catalog)
    existing_records = _extract_model_records_from_catalog(catalog)
    existing_ids = {
        str(record.get("model_id", "") or "").strip()
        for record in existing_records
        if str(record.get("model_id", "") or "").strip()
    }

    merged_records = [dict(record) for record in existing_records]
    for model_id in registered_models:
        if model_id in existing_ids:
            continue
        merged_records.append(
            {
                "provider": OPENAI_COMPATIBLE_PROVIDER,
                "model_id": model_id,
                "display_name": model_id,
                "pricing_source": "unavailable",
                "pricing_availability_note": "Registered local model.",
            }
        )

    merged_catalog["models"] = merged_records
    return merged_catalog

def _find_model_record(catalog: Optional[Dict], model_id: str) -> Optional[Dict]:
    target = str(model_id or "").strip()
    if not target:
        return None
    for record in _extract_model_records_from_catalog(catalog):
        if str(record.get("model_id", "") or "").strip() == target:
            return record
    return None

def _format_model_option_label(catalog: Optional[Dict], model_id: str) -> str:
    record = _find_model_record(catalog, model_id)
    if not record:
        return str(model_id)

    record_model_id = str(record.get("model_id", model_id) or model_id)
    display_name = str(record.get("display_name", record_model_id) or record_model_id)
    label = display_name if display_name == record_model_id else f"{display_name} ({record_model_id})"

    # No price display for subscription models
    if str(record.get("provider", "")).strip() == OPENAI_CODEX_PROVIDER:
        return label

    input_price = record.get("pricing_input_usd_per_mtok")
    output_price = record.get("pricing_output_usd_per_mtok")
    if isinstance(input_price, (int, float)) and isinstance(output_price, (int, float)):
        label += f" — ${float(input_price):.2f}/${float(output_price):.2f} USD/MTok"
    else:
        # Check if pricing source is explicitly unavailable or missing
        pricing_source = str(record.get("pricing_source", "") or "").strip()
        if pricing_source == "fetch_failed" or not pricing_source:
             label += " (Price unavailable)"

    return label

def _format_model_details_caption(catalog: Optional[Dict], model_id: str) -> Optional[str]:
    record = _find_model_record(catalog, model_id)
    if not record:
        return None

    details: List[str] = []
    max_input_tokens = record.get("max_input_tokens")
    max_output_tokens = record.get("max_output_tokens")
    if isinstance(max_input_tokens, int):
        details.append(f"input limit: {max_input_tokens:,} tokens")
    if isinstance(max_output_tokens, int):
        details.append(f"output limit: {max_output_tokens:,} tokens")

    # Subscription models have no per-token pricing to show
    if str(record.get("provider", "")).strip() != OPENAI_CODEX_PROVIDER:
        input_price = record.get("pricing_input_usd_per_mtok")
        output_price = record.get("pricing_output_usd_per_mtok")
        if isinstance(input_price, (int, float)) and isinstance(output_price, (int, float)):
            details.append(f"pricing: ${float(input_price):.2f}/${float(output_price):.2f} USD per 1M tokens")
        else:
            note = str(record.get("pricing_availability_note") or "").strip()
            if note:
                details.append(note)

    notes = str(record.get("pricing_notes") or "").strip()
    if notes:
        details.append(f"pricing note: {notes}")

    return " • ".join(details) if details else None

def _ensure_model_catalog_for_provider(provider: str, api_key_env: Optional[str] = None, force_refresh: bool = False) -> Dict:
    cache = runtime_state.get(AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY, {})
    if not isinstance(cache, dict):
        cache = {}

    env_name = (api_key_env or "").strip() or get_default_api_key_env(provider)
    if _is_openai_compatible_provider(provider):
        _ensure_openai_compatible_base_url_available()
    _ensure_provider_api_key_available(provider, env_name)
    pricing_overrides = _get_provider_pricing_overrides(provider)

    cache_key = f"{provider}::{env_name}"
    if _is_openai_compatible_provider(provider):
        cache_key = "::".join(
            [
                provider,
                env_name,
                _normalize_openai_compatible_base_url(os.getenv(OPENAI_COMPATIBLE_BASE_URL_ENV)),
                "1" if _is_populated_api_key(os.getenv(env_name)) else "0",
            ]
        )
    if force_refresh or cache_key not in cache or not isinstance(cache.get(cache_key), dict):
        catalog = fetch_available_model_catalog(
            provider,
            api_key_env=env_name or None,
            pricing_overrides=pricing_overrides,
        )
        if not isinstance(catalog, dict):
            catalog = {
                "provider": provider,
                "source": "fallback",
                "message": "Model catalog fetch returned an invalid payload; using curated defaults.",
                "models": [{"model_id": model_id, "display_name": model_id} for model_id in get_default_model_options(provider)],
            }
        cache[cache_key] = catalog
        runtime_state[AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY] = cache

    cached_catalog = cache.get(cache_key)
    if isinstance(cached_catalog, dict):
        if _is_openai_compatible_provider(provider):
            return _merge_registered_openai_compatible_models_into_catalog(cached_catalog)
        return cached_catalog

    fallback_catalog = {
        "provider": provider,
        "source": "fallback",
        "message": "No model catalog in cache; using curated defaults.",
        "models": [{"model_id": model_id, "display_name": model_id} for model_id in get_default_model_options(provider)],
    }
    if _is_openai_compatible_provider(provider):
        return _merge_registered_openai_compatible_models_into_catalog(fallback_catalog)
    return fallback_catalog

def _ensure_models_for_provider(provider: str, api_key_env: Optional[str] = None, force_refresh: bool = False) -> List[str]:
    catalog = _ensure_model_catalog_for_provider(provider, api_key_env=api_key_env, force_refresh=force_refresh)
    model_ids = _extract_model_ids_from_catalog(catalog)
    return model_ids or get_default_model_options(provider)

def _build_run_config_from_state() -> AgentRunConfig:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    agentic_expert_mode = bool(runtime_state.get("agent_agentic_expert_mode", False))
    candidate_review_mode = str(
        runtime_state.get("agent_candidate_review_mode", agent_cfg.get("candidate_review_mode", "conservative"))
        or "conservative"
    ).strip().lower()
    if candidate_review_mode not in {"conservative", "exploratory"}:
        candidate_review_mode = "conservative"

    model_provider = runtime_state.get("agent_model_provider", "openai")
    definition_model_provider = model_provider
    model_name = runtime_state.get("agent_model_name", "gpt-5.1")
    reasoning_effort = str(runtime_state.get("agent_reasoning_effort", agent_cfg.get("reasoning_effort", "none")) or "none").strip().lower()
    if reasoning_effort not in REASONING_EFFORT_OPTIONS:
        reasoning_effort = "none"
    definition_model_name = runtime_state.get("agent_definition_model_name", model_name)

    model_api_key_env = _resolve_api_key_env_for_provider(
        "agent_model_api_key_env",
        model_provider,
        "model_api_key_env",
    )

    if _is_openai_compatible_provider(model_provider):
        _ensure_openai_compatible_base_url_available()
    definition_model_api_key_env = _resolve_api_key_env_for_provider(
        "agent_definition_model_api_key_env",
        definition_model_provider,
        "definition_model_api_key_env",
    )

    default_definition_model_name = model_name
    default_timeout_seconds = int(agent_cfg.get("timeout_seconds", 180))
    default_max_iterations = int(agent_cfg.get("max_iterations", 10))
    default_batch_size = int(agent_cfg.get("default_batch_size", 10))
    default_max_workers = int(agent_cfg.get("max_workers", 4))
    default_enable_skos_matching = bool(agent_cfg.get("enable_skos_matching", True))
    enable_skos_matching = bool(
        runtime_state.get("agent_enable_skos_matching", default_enable_skos_matching)
    )

    if agentic_expert_mode:
        use_different_models = bool(runtime_state.get("agent_use_different_models", False))
        if use_different_models:
            definition_model_name = runtime_state.get(
                "agent_definition_model_name",
                default_definition_model_name,
            )
        else:
            definition_model_name = model_name
        timeout_seconds = int(runtime_state.get("agent_timeout_seconds", default_timeout_seconds))
        max_iterations = int(runtime_state.get("agent_max_iterations", default_max_iterations))
        batch_size = int(runtime_state.get("agent_batch_size", default_batch_size))
        max_workers = int(runtime_state.get("agent_max_workers", default_max_workers))
    else:
        definition_model_name = model_name
        timeout_seconds = default_timeout_seconds
        max_iterations = default_max_iterations
        batch_size = default_batch_size
        max_workers = default_max_workers

    default_agentic_trigger_policy = str(
        agent_cfg.get("agentic_trigger_policy", "no_exact_or_low_confidence") or "no_exact_or_low_confidence"
    )
    default_agentic_min_confidence = float(
        agent_cfg.get("agentic_min_confidence_to_skip_refinement", 0.80)
    )
    default_agentic_max_planner_calls = int(agent_cfg.get("agentic_max_planner_calls", 1))
    default_agentic_max_tool_actions = int(agent_cfg.get("agentic_max_tool_actions", 6))
    default_agentic_total_llm_call_budget = int(agent_cfg.get("agentic_total_llm_call_budget", 14))
    default_agentic_max_candidate_rescore = int(agent_cfg.get("agentic_max_candidate_rescore", 8))
    default_candidate_pool_limit = int(agent_cfg.get("candidate_pool_limit", 30))
    default_auto_accept_enabled = bool(agent_cfg.get("auto_accept_enabled", False))
    default_auto_accept_min_confidence = float(agent_cfg.get("auto_accept_min_confidence", 0.80))
    default_auto_accept_require_exact_match = bool(agent_cfg.get("auto_accept_require_exact_match", True))
    default_auto_accept_require_llm_decision = bool(agent_cfg.get("auto_accept_require_llm_decision", True))
    default_auto_accept_require_no_fallback = bool(agent_cfg.get("auto_accept_require_no_fallback", True))
    default_auto_accept_trusted_ontologies_only = bool(agent_cfg.get("auto_accept_trusted_ontologies_only", False))
    default_allow_heuristic_fallback = bool(agent_cfg.get("allow_heuristic_fallback", True))
    allow_heuristic_fallback = bool(
        runtime_state.get("agent_allow_heuristic_fallback", default_allow_heuristic_fallback)
    )

    if agentic_expert_mode:
        agentic_trigger_policy = str(
            runtime_state.get("agentic_trigger_policy", default_agentic_trigger_policy)
            or default_agentic_trigger_policy
        )
        agentic_min_confidence = float(
            runtime_state.get("agentic_min_confidence_to_skip_refinement", default_agentic_min_confidence)
        )
        agentic_max_planner_calls = int(
            runtime_state.get("agentic_max_planner_calls", default_agentic_max_planner_calls)
        )
        agentic_max_tool_actions = int(
            runtime_state.get("agentic_max_tool_actions", default_agentic_max_tool_actions)
        )
        agentic_total_llm_call_budget = int(
            runtime_state.get("agentic_total_llm_call_budget", default_agentic_total_llm_call_budget)
        )
        agentic_max_candidate_rescore = int(
            runtime_state.get("agentic_max_candidate_rescore", default_agentic_max_candidate_rescore)
        )
        candidate_pool_limit = int(
            runtime_state.get("agent_candidate_pool_limit", default_candidate_pool_limit)
        )
        planner_model_provider = runtime_state.get("agent_planner_model_provider") or model_provider
        planner_model_name = runtime_state.get("agent_planner_model_name") or model_name
        planner_model_api_key_env = (
            model_api_key_env
            if planner_model_provider == model_provider
            else _resolve_api_key_env_for_provider(None, planner_model_provider)
        )
    else:
        # Notebook-style behavior: controlled agentic refinement always active with safe defaults.
        # In non-expert mode, planner follows the primary provider/model selected above.
        agentic_trigger_policy = default_agentic_trigger_policy
        agentic_min_confidence = default_agentic_min_confidence
        agentic_max_planner_calls = default_agentic_max_planner_calls
        agentic_max_tool_actions = default_agentic_max_tool_actions
        agentic_total_llm_call_budget = default_agentic_total_llm_call_budget
        agentic_max_candidate_rescore = default_agentic_max_candidate_rescore
        candidate_pool_limit = default_candidate_pool_limit
        planner_model_provider = model_provider
        planner_model_name = model_name
        planner_model_api_key_env = model_api_key_env

    auto_accept_enabled = bool(
        runtime_state.get("agent_auto_accept_enabled", default_auto_accept_enabled)
    )
    auto_accept_min_confidence = float(
        runtime_state.get("agent_auto_accept_min_confidence", default_auto_accept_min_confidence)
    )
    auto_accept_require_exact_match = bool(
        runtime_state.get("agent_auto_accept_require_exact_match", default_auto_accept_require_exact_match)
    )
    auto_accept_require_llm_decision = bool(
        runtime_state.get("agent_auto_accept_require_llm_decision", default_auto_accept_require_llm_decision)
    )
    auto_accept_require_no_fallback = bool(
        runtime_state.get("agent_auto_accept_require_no_fallback", default_auto_accept_require_no_fallback)
    )
    auto_accept_trusted_ontologies_only = bool(
        runtime_state.get(
            "agent_auto_accept_trusted_ontologies_only",
            default_auto_accept_trusted_ontologies_only,
        )
    )

    return AgentRunConfig(
        workflow=runtime_state.get("agent_workflow_select", "wikidata_deep_agent"),
        definition_strategy=runtime_state.get("agent_definition_strategy", "generate_single_shot"),
        model_provider=model_provider,
        definition_model_provider=definition_model_provider,
        model_name=model_name,
        reasoning_effort=reasoning_effort,
        definition_model_name=definition_model_name,
        timeout_seconds=timeout_seconds,
        max_iterations=max_iterations,
        batch_size=batch_size,
        max_workers=max_workers,
        enable_skos_matching=enable_skos_matching,
        auto_apply_on_accept=False,
        auto_accept_enabled=auto_accept_enabled,
        auto_accept_min_confidence=auto_accept_min_confidence,
        auto_accept_require_exact_match=auto_accept_require_exact_match,
        auto_accept_require_llm_decision=auto_accept_require_llm_decision,
        auto_accept_require_no_fallback=auto_accept_require_no_fallback,
        auto_accept_trusted_ontologies_only=auto_accept_trusted_ontologies_only,
        trusted_ontologies=[item.strip() for item in runtime_state.get("agent_trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"]) if str(item).strip()],
        bioportal_agent_ontologies=[item.strip() for item in runtime_state.get("agent_bioportal_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"]) if str(item).strip()],
        model_api_key_env=model_api_key_env,
        definition_model_api_key_env=definition_model_api_key_env,
        enable_agentic_refinement=True,
        agentic_trigger_policy=agentic_trigger_policy,
        agentic_min_confidence_to_skip_refinement=agentic_min_confidence,
        agentic_max_planner_calls=agentic_max_planner_calls,
        agentic_max_tool_actions=agentic_max_tool_actions,
        agentic_total_llm_call_budget=agentic_total_llm_call_budget,
        agentic_max_candidate_rescore=agentic_max_candidate_rescore,
        candidate_pool_limit=candidate_pool_limit,
        planner_model_provider=planner_model_provider,
        planner_model_name=planner_model_name,
        planner_model_api_key_env=planner_model_api_key_env,
        langsmith_project=(runtime_state.get("agent_langsmith_project") or None)
        if bool(runtime_state.get("agent_use_langsmith_monitoring", False))
        else None,
        allow_heuristic_fallback=allow_heuristic_fallback,
        candidate_review_mode=candidate_review_mode,
    )

def _initialize_agent_reconciliation_state():
    """Initialize backend session keys for the agent reconciliation workflow."""
    defaults = {
        AGENT_DATAFRAME_STATE_KEY: None,
        AGENT_DATA_SOURCE_MESSAGE_KEY: None,
        AGENT_INPUT_TABLES_KEY: [],
        AGENT_RESULTS_BY_SOURCE_KEY: {},
        AGENT_SELECTED_SOURCE_KEY: None,
        AGENT_DEFINITIONS_BY_SOURCE_KEY: {},
        AGENT_RUN_MESSAGES_KEY: [],
        AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY: {},
        AGENT_MONITORING_STATE_KEY: {},
        AGENT_STOP_EVENT_KEY: {},
        AGENT_UPLOADED_SOURCE_SIGNATURE_KEY: None,
        AGENT_ACTIVE_STEP_KEY: "Setup",
        "agent_reference_publication_text": "",
        "agent_workflow_select": "wikidata_deep_agent",
    }
    for key, default_value in defaults.items():
        if key not in runtime_state:
            runtime_state[key] = default_value

def _initialize_provenance_state(provenance_defaults_cfg: Dict[str, str]):
    provenance_defaults = {
        "agent_prov_author_orcid": provenance_defaults_cfg.get("author_id", ""),
        "agent_prov_author_name": provenance_defaults_cfg.get("author_label", ""),
        "agent_prov_reviewer_orcid": provenance_defaults_cfg.get("reviewer_id", ""),
        "agent_prov_reviewer_name": provenance_defaults_cfg.get("reviewer_label", ""),
        "agent_prov_creator_orcid": provenance_defaults_cfg.get("creator_id", ""),
        "agent_prov_creator_name": provenance_defaults_cfg.get("creator_label", ""),
        "agent_prov_mapping_tool": provenance_defaults_cfg.get("mapping_tool", "RDF4Risk Agent-Based Reconciliation"),
        "agent_prov_mapping_tool_version": provenance_defaults_cfg.get("mapping_tool_version", "PoC"),
        "agent_prov_last_run_mapping_date": provenance_defaults_cfg.get("mapping_date", ""),
        "agent_prov_publication_date": provenance_defaults_cfg.get("publication_date", ""),
    }
    for key, value in provenance_defaults.items():
        if key not in runtime_state:
            runtime_state[key] = value

def _get_agent_ui_defaults() -> Dict[str, object]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    provider_options = get_supported_llm_providers()
    preferred_model_provider = str(agent_cfg.get("preferred_model_provider", "") or "").strip()
    preferred_model_name = str(agent_cfg.get("preferred_model_name", "") or "").strip()
    default_model_provider = preferred_model_provider if preferred_model_provider in provider_options else "openai"
    default_model = preferred_model_name or "gpt-5.1"
    default_reasoning_effort = str(agent_cfg.get("reasoning_effort", "none") or "none").strip().lower()
    if default_reasoning_effort not in REASONING_EFFORT_OPTIONS:
        default_reasoning_effort = "none"
    return {
        "agent_cfg": agent_cfg,
        "provider_options": provider_options,
        "default_model_provider": default_model_provider,
        "default_model": default_model,
        "default_timeout": int(agent_cfg.get("timeout_seconds", 180)),
        "default_iterations": int(agent_cfg.get("max_iterations", 10)),
        "default_batch_size": int(agent_cfg.get("default_batch_size", 10)),
        "default_workers": int(agent_cfg.get("max_workers", 4)),
        "default_reasoning_effort": default_reasoning_effort,
        "default_auto_accept_enabled": bool(agent_cfg.get("auto_accept_enabled", False)),
    }

def _build_workflow_config_from_state(defaults: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    defaults = defaults or _get_agent_ui_defaults()
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    advanced = {
        "timeout_s": int(runtime_state.get("agent_timeout_seconds", defaults.get("default_timeout", 180))),
        "max_iterations": int(runtime_state.get("agent_max_iterations", defaults.get("default_iterations", 10))),
        "batch_size": int(runtime_state.get("agent_batch_size", defaults.get("default_batch_size", 10))),
        "max_workers": int(runtime_state.get("agent_max_workers", defaults.get("default_workers", 4))),
        "agentic_min_confidence_to_skip_refinement": float(runtime_state.get("agentic_min_confidence_to_skip_refinement", agent_cfg.get("agentic_min_confidence_to_skip_refinement", 0.80))),
        "agentic_max_planner_calls": int(runtime_state.get("agentic_max_planner_calls", agent_cfg.get("agentic_max_planner_calls", 1))),
        "agentic_max_tool_actions": int(runtime_state.get("agentic_max_tool_actions", agent_cfg.get("agentic_max_tool_actions", 6))),
        "agentic_total_llm_call_budget": int(runtime_state.get("agentic_total_llm_call_budget", agent_cfg.get("agentic_total_llm_call_budget", 14))),
        "agentic_max_candidate_rescore": int(runtime_state.get("agentic_max_candidate_rescore", agent_cfg.get("agentic_max_candidate_rescore", 8))),
        "candidate_pool_limit": int(runtime_state.get("agent_candidate_pool_limit", agent_cfg.get("candidate_pool_limit", 30))),
    }
    auto_accept_policy = {
        "min_confidence": float(runtime_state.get("agent_auto_accept_min_confidence", agent_cfg.get("auto_accept_min_confidence", 0.80))),
        "require_exact_match": bool(runtime_state.get("agent_auto_accept_require_exact_match", agent_cfg.get("auto_accept_require_exact_match", True))),
        "require_llm_decision": bool(runtime_state.get("agent_auto_accept_require_llm_decision", agent_cfg.get("auto_accept_require_llm_decision", True))),
        "require_no_fallback": bool(runtime_state.get("agent_auto_accept_require_no_fallback", agent_cfg.get("auto_accept_require_no_fallback", True))),
        "trusted_ontologies_only": bool(runtime_state.get("agent_auto_accept_trusted_ontologies_only", agent_cfg.get("auto_accept_trusted_ontologies_only", False))),
    }
    config = {
        "workflow": runtime_state.get("agent_workflow_select", "wikidata_deep_agent"),
        "provider": runtime_state.get("agent_model_provider", defaults.get("default_model_provider", "openai")),
        "model": runtime_state.get("agent_model_name", defaults.get("default_model", "gpt-5.1")),
        "reasoning_effort": runtime_state.get("agent_reasoning_effort", defaults.get("default_reasoning_effort", "none")),
        "custom_model_override": runtime_state.get("agent_custom_model_override", ""),
        "provider_api_key_env": runtime_state.get("agent_model_api_key_env", get_default_api_key_env(str(runtime_state.get("agent_model_provider", defaults.get("default_model_provider", "openai"))))),
        "openai_compatible_base_url": runtime_state.get("agent_openai_compatible_base_url", _get_openai_compatible_base_url_from_config()),
        "openai_compatible_api_key": runtime_state.get("agent_openai_compatible_api_key", _get_provider_api_key_from_config(OPENAI_COMPATIBLE_PROVIDER) or ""),
        "skos_matching": bool(runtime_state.get("agent_enable_skos_matching", (CONFIG or {}).get("agent_reconciliation", {}).get("enable_skos_matching", True))),
        "auto_accept": bool(runtime_state.get("agent_auto_accept_enabled", defaults.get("default_auto_accept_enabled", False))),
        "auto_accept_policy": auto_accept_policy,
        "langsmith": bool(runtime_state.get("agent_use_langsmith_monitoring", False)),
        "langsmith_project": runtime_state.get("agent_langsmith_project", agent_cfg.get("langsmith_project", "")),
        "expert_mode": bool(runtime_state.get("agent_agentic_expert_mode", False)),
        "candidate_review_mode": str(runtime_state.get("agent_candidate_review_mode", agent_cfg.get("candidate_review_mode", "conservative")) or "conservative").strip().lower()
        if str(runtime_state.get("agent_candidate_review_mode", agent_cfg.get("candidate_review_mode", "conservative")) or "conservative").strip().lower() in {"conservative", "exploratory"}
        else "conservative",
        "allow_heuristic_fallback": bool(runtime_state.get("agent_allow_heuristic_fallback", agent_cfg.get("allow_heuristic_fallback", True))),
        "use_different_models": bool(runtime_state.get("agent_use_different_models", False)),
        "definition_model": runtime_state.get("agent_definition_model_name", runtime_state.get("agent_model_name", defaults.get("default_model", "gpt-5.1"))),
        "agentic_trigger_policy": runtime_state.get("agentic_trigger_policy", agent_cfg.get("agentic_trigger_policy", "no_exact_or_low_confidence")),
        "planner_provider": runtime_state.get("agent_planner_model_provider", runtime_state.get("agent_model_provider", defaults.get("default_model_provider", "openai"))),
        "planner_model": runtime_state.get("agent_planner_model_name", runtime_state.get("agent_model_name", defaults.get("default_model", "gpt-5.1"))),
        "trusted_ontologies": runtime_state.get("agent_trusted_ontologies", agent_cfg.get("trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"])),
        "bioportal_ontologies": runtime_state.get("agent_bioportal_ontologies", agent_cfg.get("bioportal_agent_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"])),
        "definition_preparation": bool(runtime_state.get("agent_enable_definition_preparation", False)),
        "definition_strategy": runtime_state.get("agent_definition_strategy", "generate_single_shot"),
        "definition_context_text": runtime_state.get("agent_definition_context_text", ""),
        "advanced": advanced,
        "provenance": {
            "enabled": bool(runtime_state.get("agent_enable_provenance_metadata", False)),
            **_build_provenance_defaults_from_state(),
        },
    }
    runtime_state[AGENT_WORKFLOW_CONFIG_STATE_KEY] = config
    return config

def _apply_workflow_config_to_runtime_state(config: Optional[Dict[str, object]]) -> bool:
    if not isinstance(config, dict):
        return False
    changed = False

    def _set(key: str, value: object):
        nonlocal changed
        if value is None:
            return
        if runtime_state.get(key) != value:
            runtime_state[key] = value
            changed = True

    workflow = str(config.get("workflow", "") or "").strip()
    if workflow in {"wikidata_deep_agent", "bioportal_wikidata_multiagent"}:
        _set("agent_workflow_select", workflow)
    provider = str(config.get("provider", "") or "").strip()
    if provider:
        _set("agent_model_provider", provider)
        _set("agent_definition_model_provider", provider)
    provider_api_key_env = str(config.get("provider_api_key_env", "") or "").strip()
    if provider_api_key_env:
        _set("agent_model_api_key_env", provider_api_key_env)
        _set("agent_definition_model_api_key_env", provider_api_key_env)
    openai_compatible_base_url = _normalize_openai_compatible_base_url(config.get("openai_compatible_base_url"))
    if openai_compatible_base_url:
        _set("agent_openai_compatible_base_url", openai_compatible_base_url)
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = openai_compatible_base_url
    openai_compatible_api_key = str(config.get("openai_compatible_api_key", "") or "").strip()
    if openai_compatible_api_key:
        _set("agent_openai_compatible_api_key", openai_compatible_api_key)
        os.environ[get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)] = openai_compatible_api_key
    model = str(config.get("model", "") or "").strip()
    if model:
        _set("agent_model_name", model)
        if not bool(config.get("expert_mode", False)):
            _set("agent_definition_model_name", model)
            _set("agent_planner_model_name", model)
    custom_model_override = str(config.get("custom_model_override", "") or "").strip()
    _set("agent_custom_model_override", custom_model_override)
    reasoning = str(config.get("reasoning_effort", "") or "").strip().lower()
    if reasoning in REASONING_EFFORT_OPTIONS:
        _set("agent_reasoning_effort", reasoning)
    _set("agent_enable_skos_matching", bool(config.get("skos_matching", True)))
    _set("agent_auto_accept_enabled", bool(config.get("auto_accept", False)))
    policy = config.get("auto_accept_policy", {}) if isinstance(config.get("auto_accept_policy"), dict) else {}
    if "min_confidence" in policy:
        try:
            _set("agent_auto_accept_min_confidence", float(policy.get("min_confidence")))
        except (TypeError, ValueError):
            pass
    for source_key, session_key in {
        "require_exact_match": "agent_auto_accept_require_exact_match",
        "require_llm_decision": "agent_auto_accept_require_llm_decision",
        "require_no_fallback": "agent_auto_accept_require_no_fallback",
        "trusted_ontologies_only": "agent_auto_accept_trusted_ontologies_only",
    }.items():
        if source_key in policy:
            _set(session_key, bool(policy.get(source_key)))
    _set("agent_use_langsmith_monitoring", bool(config.get("langsmith", False)))
    _set("agent_langsmith_project", str(config.get("langsmith_project", "") or ""))
    _set("agent_agentic_expert_mode", bool(config.get("expert_mode", False)))
    candidate_review_mode = str(config.get("candidate_review_mode", "") or "").strip().lower()
    if candidate_review_mode in {"conservative", "exploratory"}:
        _set("agent_candidate_review_mode", candidate_review_mode)
    _set("agent_allow_heuristic_fallback", bool(config.get("allow_heuristic_fallback", True)))
    _set("agent_use_different_models", bool(config.get("use_different_models", False)))
    definition_model = str(config.get("definition_model", "") or "").strip()
    if definition_model and bool(config.get("use_different_models", False)):
        _set("agent_definition_model_name", definition_model)
    elif model:
        _set("agent_definition_model_name", model)
    trigger_policy = str(config.get("agentic_trigger_policy", "") or "").strip()
    if trigger_policy in {"no_exact_or_low_confidence", "always", "non_exact_only"}:
        _set("agentic_trigger_policy", trigger_policy)
    planner_provider = str(config.get("planner_provider", "") or "").strip()
    if planner_provider:
        _set("agent_planner_model_provider", planner_provider)
    planner_model = str(config.get("planner_model", "") or "").strip()
    if planner_model:
        _set("agent_planner_model_name", planner_model)
    for source_key, session_key in {
        "trusted_ontologies": "agent_trusted_ontologies",
        "bioportal_ontologies": "agent_bioportal_ontologies",
    }.items():
        values = config.get(source_key)
        if isinstance(values, list):
            normalized_values = [str(item).strip().upper() for item in values if str(item).strip()]
            _set(session_key, normalized_values)
    advanced = config.get("advanced", {}) if isinstance(config.get("advanced"), dict) else {}
    numeric_map = {
        "timeout_s": "agent_timeout_seconds",
        "max_iterations": "agent_max_iterations",
        "batch_size": "agent_batch_size",
        "max_workers": "agent_max_workers",
        "agentic_max_planner_calls": "agentic_max_planner_calls",
        "agentic_max_tool_actions": "agentic_max_tool_actions",
        "agentic_total_llm_call_budget": "agentic_total_llm_call_budget",
        "agentic_max_candidate_rescore": "agentic_max_candidate_rescore",
        "candidate_pool_limit": "agent_candidate_pool_limit",
    }
    for source_key, session_key in numeric_map.items():
        if source_key in advanced:
            try:
                _set(session_key, int(advanced.get(source_key)))
            except (TypeError, ValueError):
                pass
    if "agentic_min_confidence_to_skip_refinement" in advanced:
        try:
            _set("agentic_min_confidence_to_skip_refinement", float(advanced.get("agentic_min_confidence_to_skip_refinement")))
        except (TypeError, ValueError):
            pass

    if "definition_preparation" in config:
        _set("agent_enable_definition_preparation", bool(config.get("definition_preparation", False)))
    definition_strategy = str(config.get("definition_strategy", "") or "").strip()
    if definition_strategy in {"uploaded_sheet", "generate_single_shot", "reference_publication"}:
        _set("agent_definition_strategy", definition_strategy)
    if "definition_context_text" in config:
        _set("agent_definition_context_text", str(config.get("definition_context_text", "") or ""))

    provenance = config.get("provenance", {}) if isinstance(config.get("provenance"), dict) else {}
    if provenance:
        _set("agent_enable_provenance_metadata", bool(provenance.get("enabled", False)))
        for source_key, session_key in {
            "author_id": "agent_prov_author_orcid",
            "author_label": "agent_prov_author_name",
            "reviewer_id": "agent_prov_reviewer_orcid",
            "reviewer_label": "agent_prov_reviewer_name",
            "creator_id": "agent_prov_creator_orcid",
            "creator_label": "agent_prov_creator_name",
            "mapping_tool": "agent_prov_mapping_tool",
            "mapping_tool_version": "agent_prov_mapping_tool_version",
            "publication_date": "agent_prov_publication_date",
        }.items():
            if source_key in provenance:
                _set(session_key, str(provenance.get(source_key, "") or ""))

    runtime_state[AGENT_WORKFLOW_CONFIG_STATE_KEY] = _build_workflow_config_from_state()
    return changed

def _get_active_agent_stage() -> str:
    stages = ["Setup", "Run", "Review", "Export"]
    current = runtime_state.get(AGENT_ACTIVE_STEP_KEY, "Setup")
    if current not in stages:
        current = "Setup"
        runtime_state[AGENT_ACTIVE_STEP_KEY] = current
    return current

def _compute_workflow_runtime_context(defaults: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    defaults = defaults or _get_agent_ui_defaults()
    provider_options = defaults.get("provider_options", get_supported_llm_providers())
    primary_provider = str(runtime_state.get("agent_model_provider", defaults.get("default_model_provider", "openai")) or "openai")
    if primary_provider not in provider_options:
        primary_provider = str(defaults.get("default_model_provider", "openai"))
    runtime_state.setdefault("agent_model_provider", primary_provider)
    runtime_state["agent_definition_model_provider"] = primary_provider
    primary_api_key_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", primary_provider, "model_api_key_env")
    runtime_state["agent_definition_model_api_key_env"] = primary_api_key_env
    primary_catalog = _ensure_model_catalog_for_provider(primary_provider, api_key_env=primary_api_key_env)
    primary_models = _extract_model_ids_from_catalog(primary_catalog) or get_default_model_options(primary_provider)
    if primary_models and not runtime_state.get("agent_model_name"):
        runtime_state["agent_model_name"] = defaults.get("default_model", primary_models[0]) if defaults.get("default_model", "") in primary_models else primary_models[0]
    use_different_models = bool(runtime_state.get("agent_use_different_models", False))
    effective_primary_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", primary_provider, "model_api_key_env")
    definition_provider = primary_provider
    effective_definition_env = effective_primary_env
    missing_provider_keys = []
    if _is_openai_compatible_provider(primary_provider):
        if not _ensure_openai_compatible_base_url_available():
            missing_provider_keys.append((primary_provider, OPENAI_COMPATIBLE_BASE_URL_ENV))
    elif not _ensure_provider_api_key_available(primary_provider, effective_primary_env):
        missing_provider_keys.append((primary_provider, effective_primary_env))
    if use_different_models and (definition_provider, effective_definition_env) != (primary_provider, effective_primary_env):
        if not _ensure_provider_api_key_available(definition_provider, effective_definition_env):
            missing_provider_keys.append((definition_provider, effective_definition_env))
    if _is_openai_compatible_provider(primary_provider) and not missing_provider_keys:
        if _openai_compatible_catalog_requires_api_key(primary_catalog):
            env_name = effective_primary_env or get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            missing_provider_keys.append((OPENAI_COMPATIBLE_PROVIDER, env_name))
    return {
        "primary_provider": primary_provider,
        "primary_catalog": primary_catalog,
        "primary_models": primary_models,
        "effective_primary_env": effective_primary_env,
        "missing_provider_keys": missing_provider_keys,
    }

def _build_run_readiness_state(required_columns, missing_provider_keys: Optional[List[tuple]] = None) -> Dict[str, object]:
    missing_provider_keys = missing_provider_keys or []
    workflow_config = _build_workflow_config_from_state()
    input_tables = runtime_state.get(AGENT_INPUT_TABLES_KEY, [])
    agent_df = runtime_state.get(AGENT_DATAFRAME_STATE_KEY)
    shared_df = runtime_state.get("shared_matching_table")
    schema_df = agent_df if isinstance(agent_df, pd.DataFrame) else shared_df if isinstance(shared_df, pd.DataFrame) else None
    if schema_df is None and isinstance(input_tables, list) and input_tables:
        schema_df = getattr(input_tables[0], "dataframe", None)
    rows = len(schema_df) if isinstance(schema_df, pd.DataFrame) else 0
    cols = len(schema_df.columns) if isinstance(schema_df, pd.DataFrame) else 0
    required_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in required_columns))
    legacy_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS))
    provider_configured = not bool(missing_provider_keys)
    workflow_valid = workflow_config.get("workflow") in {"wikidata_deep_agent", "bioportal_wikidata_multiagent"}
    checks = [
        {"key": "matching_table", "label": "Matching table loaded", "ok": bool(input_tables), "detail": f"{rows:,} rows • {cols:,} columns" if rows else "No working input table"},
        {"key": "required_columns", "label": "Required columns detected", "ok": required_detected or legacy_detected, "detail": "Canonical SSSOM columns" if required_detected else "Legacy columns will be normalized" if legacy_detected else "Missing required table columns"},
        {"key": "provider", "label": "LLM Provider configured", "ok": provider_configured, "detail": get_provider_label(str(workflow_config.get("provider", "")))},
        {"key": "workflow", "label": "Workflow configuration valid", "ok": workflow_valid, "detail": "Ready to run" if workflow_valid else "Choose a supported workflow"},
    ]
    batch_size = int(workflow_config.get("advanced", {}).get("batch_size", 10)) if isinstance(workflow_config.get("advanced"), dict) else 10
    est_batches = max(1, (rows + max(batch_size, 1) - 1) // max(batch_size, 1)) if rows else 0
    return {
        "checks": checks,
        "ready": all(check["ok"] for check in checks),
        "summary": {
            "Workflow": "BioPortal + Wikidata" if workflow_config.get("workflow") == "bioportal_wikidata_multiagent" else "Wikidata Deep Agent",
            "Model": str(workflow_config.get("model", "")),
            "SKOS Matching": "Enabled" if workflow_config.get("skos_matching") else "Disabled",
            "Auto-accept": "Enabled" if workflow_config.get("auto_accept") else "Disabled",
            "Batch Size": str(workflow_config.get("advanced", {}).get("batch_size", "")) if isinstance(workflow_config.get("advanced"), dict) else "",
            "Max Workers": str(workflow_config.get("advanced", {}).get("max_workers", "")) if isinstance(workflow_config.get("advanced"), dict) else "",
            "Est. Runtime": f"~{est_batches}-{est_batches * 2} min" if rows else "n/a",
            "Est. Cost": "Available after model pricing/run telemetry",
        },
    }

def _component_stage_from_session() -> str:
    return _STAGE_TO_COMPONENT.get(_get_active_agent_stage(), "setup")

def _stage_from_component(value: object) -> Optional[str]:
    return _COMPONENT_TO_STAGE.get(str(value or "").strip().lower())

def _json_safe_value(value: object):
    if pd.isna(value):
        return ""
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value

def _dataframe_records(df: Optional[pd.DataFrame], limit: int = 25) -> List[Dict[str, object]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    preview = df.head(limit).copy()
    preview = preview.where(pd.notna(preview), "")
    return [
        {str(key): _json_safe_value(value) for key, value in row.items()}
        for row in preview.to_dict(orient="records")
    ]

def _build_data_status_snapshot(required_columns) -> Dict[str, object]:
    shared_df = runtime_state.get("shared_matching_table")
    agent_df = runtime_state.get(AGENT_DATAFRAME_STATE_KEY)
    input_tables = runtime_state.get(AGENT_INPUT_TABLES_KEY, [])
    selected_source = runtime_state.get(AGENT_SELECTED_SOURCE_KEY)
    schema_df = agent_df if isinstance(agent_df, pd.DataFrame) else shared_df if isinstance(shared_df, pd.DataFrame) else None
    filename = ""
    source_name = str(selected_source or "")
    if schema_df is None and isinstance(input_tables, list) and input_tables:
        first_table = input_tables[0]
        schema_df = getattr(first_table, "dataframe", None)
        filename = str(getattr(first_table, "filename", "") or "")
        source_name = str(getattr(first_table, "source_name", "") or source_name)
    elif isinstance(input_tables, list) and input_tables:
        first_table = input_tables[0]
        filename = str(getattr(first_table, "filename", "") or "")
        source_name = str(getattr(first_table, "source_name", "") or source_name)
    if isinstance(shared_df, pd.DataFrame) and not source_name:
        source_name = "Matching Table Generator"
        filename = "shared_matching_table"

    rows = len(schema_df) if isinstance(schema_df, pd.DataFrame) else 0
    columns = len(schema_df.columns) if isinstance(schema_df, pd.DataFrame) else 0
    required_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in required_columns))
    legacy_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS))
    schema_message = "No table loaded"
    if required_detected:
        schema_message = "Canonical SSSOM columns"
    elif legacy_detected:
        schema_message = "Legacy columns will be normalized"
    elif isinstance(schema_df, pd.DataFrame):
        missing = [col for col in required_columns if col not in schema_df.columns]
        schema_message = f"Missing {len(missing)} required column(s)"

    return {
        "has_table": isinstance(schema_df, pd.DataFrame),
        "filename": filename,
        "source_name": source_name,
        "rows": rows,
        "columns": columns,
        "loaded_sources": len(input_tables) if isinstance(input_tables, list) else 0,
        "required_columns_detected": required_detected or legacy_detected,
        "schema_message": schema_message,
        "upload_bridge_available": True,
        "shared_table_available": isinstance(shared_df, pd.DataFrame),
        "preview": _dataframe_records(schema_df, limit=12),
    }

def _build_run_status_snapshot(readiness_state: Dict[str, object]) -> Dict[str, object]:
    monitoring_state = runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
    live_status = runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {})
    if not isinstance(live_status, dict):
        live_status = {}
    results = runtime_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})
    processed = int(
        live_status.get("processed_count", monitoring_state.get("processed_terms", 0) if isinstance(monitoring_state, dict) else 0)
        or 0
    )
    total = int(
        live_status.get("total_count", monitoring_state.get("total_terms", 0) if isinstance(monitoring_state, dict) else 0)
        or 0
    )
    progress = int((processed / max(total, 1)) * 100) if total else (100 if results else 0)
    error = None
    if isinstance(monitoring_state, dict) and monitoring_state.get("stop_reason"):
        error = str(monitoring_state.get("stop_reason"))
    status_message = runtime_state.get("agent_mui_status_message")
    if isinstance(status_message, dict) and status_message.get("severity") == "error":
        error = str(status_message.get("text") or "Agent-based reconciliation failed.")
    messages = runtime_state.get(AGENT_RUN_MESSAGES_KEY, [])
    latest_message = str(
        live_status.get("message")
        or (messages[-1] if isinstance(messages, list) and messages else "")
        or ("Run completed" if results else "Ready to run" if readiness_state.get("ready") else "Waiting for prerequisites")
    )
    elapsed_seconds = live_status.get("elapsed_seconds")
    if elapsed_seconds is None and isinstance(monitoring_state, dict):
        elapsed_seconds = monitoring_state.get("duration_sec")
    estimated_remaining_seconds = live_status.get("estimated_remaining_seconds")
    if estimated_remaining_seconds is None and total and processed > 0 and elapsed_seconds:
        estimated_remaining_seconds = (float(elapsed_seconds) / processed) * max(0, total - processed)
    return {
        "ready": bool(readiness_state.get("ready")),
        "running": bool(live_status.get("running", False)),
        "finished": bool(results),
        "error": error,
        "progress": progress,
        "stage": live_status.get("stage") or ("writing_output" if results else None),
        "message": latest_message,
        "current_term": live_status.get("current_term"),
        "processed_count": processed if total else None,
        "total_count": total if total else None,
        "started_at": live_status.get("started_at") or (monitoring_state.get("started_at") if isinstance(monitoring_state, dict) else None),
        "elapsed_seconds": elapsed_seconds,
        "estimated_remaining_seconds": estimated_remaining_seconds,
        "last_activity": live_status.get("last_activity"),
        "messages": messages[-80:] if isinstance(messages, list) else [],
    }

def _records_from_monitoring_df(monitoring_state: Dict[str, object], key: str, limit: int = 200) -> List[Dict[str, object]]:
    df = monitoring_state.get(key) if isinstance(monitoring_state, dict) else None
    if isinstance(df, pd.DataFrame) and not df.empty:
        return _dataframe_records(df.tail(limit), limit=limit)
    return []

def _build_telemetry_snapshot() -> Dict[str, object]:
    monitoring_state = runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
    if not isinstance(monitoring_state, dict):
        monitoring_state = {}
    langsmith = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
    llm_records = _records_from_monitoring_df(monitoring_state, "llm_interactions_df")
    if not llm_records:
        llm_records = get_llm_interactions(limit=200)
    # Derive failures from per-term event status as source of truth.
    # This prevents stale/misreported counters from showing all processed terms as failures.
    failed_terms = 0
    events_df = monitoring_state.get("events_df")
    if isinstance(events_df, pd.DataFrame) and not events_df.empty and "Status" in events_df.columns:
        failed_terms = int(
            events_df["Status"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["timeout", "error", "failed"])
            .sum()
        )
    else:
        # Fallback to the maintained counter if no event snapshot is available yet.
        failed_terms = int(monitoring_state.get("failed_terms", 0) or 0)
    return {
        "enabled": bool(monitoring_state.get("enabled") or runtime_state.get("agent_use_langsmith_monitoring", False)),
        "run_id": monitoring_state.get("run_id"),
        "started_at": monitoring_state.get("started_at"),
        "finished_at": monitoring_state.get("finished_at"),
        "duration_sec": monitoring_state.get("duration_sec"),
        "total_terms": int(monitoring_state.get("total_terms", 0) or 0),
        "processed_terms": int(monitoring_state.get("processed_terms", 0) or 0),
        "failed_terms": failed_terms,
        "total_cost_usd": float(monitoring_state.get("total_cost_usd", 0.0) or 0.0),
        "langsmith_url": langsmith.get("run_url"),
        "langsmith_project_url": langsmith.get("project_url"),
        "langsmith_message": langsmith.get("message"),
        "llm_calls": llm_records,
        "events": _records_from_monitoring_df(monitoring_state, "events_df"),
        "cascade": _records_from_monitoring_df(monitoring_state, "cascade_trace_df"),
        "logs": runtime_state.get(AGENT_RUN_MESSAGES_KEY, [])[-100:] if isinstance(runtime_state.get(AGENT_RUN_MESSAGES_KEY, []), list) else [],
    }

def _normalize_review_status_for_mui(agent_df: pd.DataFrame, row_index) -> str:
    raw_status = _get_review_cell_value(agent_df, row_index, "Review Status").strip().lower()
    if raw_status in {"accepted", "rejected"}:
        return raw_status
    if raw_status in {"no_match", "timeout"}:
        return "no_match"
    decision_status = _get_review_cell_value(agent_df, row_index, "Agent Decision Status").strip().lower()
    if decision_status in {"matched", "candidate_suggested", "no_match"}:
        return decision_status
    return "pending"

def _build_review_snapshot(agent_df: Optional[pd.DataFrame]) -> Dict[str, object]:
    selected_source = runtime_state.get(AGENT_SELECTED_SOURCE_KEY) or "agent_reconciliation"
    items: List[Dict[str, object]] = []
    counts = {"pending": 0, "matched": 0, "candidate_suggested": 0, "accepted": 0, "rejected": 0, "no_match": 0}
    if isinstance(agent_df, pd.DataFrame):
        candidate_indices = set(_get_reviewable_agent_result_indices(agent_df))
        if "Review Status" in agent_df.columns:
            status_series = agent_df["Review Status"].astype(str).str.strip().str.lower()
            candidate_indices.update(agent_df[status_series.isin(["accepted", "rejected", "no_match", "timeout", "pending", "matched"])].index.tolist())
        if "Suggested URI" in agent_df.columns:
            suggested_uri = agent_df["Suggested URI"].astype(str).str.strip()
            candidate_indices.update(agent_df[suggested_uri != ""].index.tolist())
        for idx in sorted(candidate_indices, key=lambda value: str(value))[:500]:
            status = _normalize_review_status_for_mui(agent_df, idx)
            counts[status] = counts.get(status, 0) + 1
            match_type = normalize_mapping_type(_get_review_cell_value(agent_df, idx, "Suggested Match Type"))
            if not match_type:
                match_type = normalize_mapping_type(_get_review_cell_value(agent_df, idx, "Match Type"))
            is_no_match_outcome = status == "no_match"
            if is_no_match_outcome:
                match_type = "no_match"
            confidence = _get_review_cell_value(agent_df, idx, "Suggested Confidence")
            try:
                confidence_value: object = round(float(confidence), 4)
            except (TypeError, ValueError):
                confidence_value = confidence
            raw_suggested_uri = _get_review_cell_value(agent_df, idx, "Suggested URI")
            raw_suggested_label = _get_review_cell_value(agent_df, idx, "Suggested Label")
            raw_suggested_description = _get_review_cell_value(agent_df, idx, "Suggested Description")
            stale_candidate_note = ""
            if is_no_match_outcome and raw_suggested_uri:
                stale_candidate_note = (
                    "A low-confidence candidate was inspected but rejected by the workflow. "
                    "It is kept only for audit/details and cannot be accepted as a mapping."
                )
            trace_metadata = {}
            trace_raw = _get_review_cell_value(agent_df, idx, "Agent Trace Metadata")
            if trace_raw:
                try:
                    parsed_trace = json.loads(trace_raw)
                    if isinstance(parsed_trace, dict):
                        trace_metadata = parsed_trace
                except Exception:
                    trace_metadata = {}
            items.append(
                {
                    "mapping_id": f"{selected_source}::{idx}",
                    "row_index": idx,
                    "source_name": selected_source,
                    "term": _get_review_cell_value(agent_df, idx, "Term") or _get_review_cell_value(agent_df, idx, "subject_label"),
                    "definition": _get_review_cell_value(agent_df, idx, "Definition"),
                    "status": status,
                    "suggested_uri": "" if is_no_match_outcome else raw_suggested_uri,
                    "suggested_label": "" if is_no_match_outcome else raw_suggested_label,
                    "suggested_description": "" if is_no_match_outcome else raw_suggested_description,
                    "candidate_uri": raw_suggested_uri,
                    "candidate_label": raw_suggested_label,
                    "candidate_description": raw_suggested_description,
                    "can_accept": bool((not is_no_match_outcome) and raw_suggested_uri),
                    "no_match_note": stale_candidate_note,
                    "match_type": match_type or "no_match",
                    "provider": _get_review_cell_value(agent_df, idx, "Suggested Provider"),
                    "confidence": confidence_value,
                    "decision_source": _get_review_cell_value(agent_df, idx, "Suggested Decision Source"),
                    "fallback_reason": _get_review_cell_value(agent_df, idx, "Suggested Fallback Reason"),
                    "trace_metadata": trace_metadata,
                    "review_mode": trace_metadata.get("candidate_review_mode", ""),
                    "explanation": _get_review_cell_value(agent_df, idx, "Agent Explanation"),
                    "auto_accept_reason": _get_review_cell_value(agent_df, idx, "Auto Accept Reason"),
                }
            )
    return {"items": items, "counts": counts, "selected_source": selected_source}

def _parse_mapping_id(mapping_id: object) -> tuple[Optional[str], Optional[object]]:
    value = str(mapping_id or "")
    if "::" not in value:
        return runtime_state.get(AGENT_SELECTED_SOURCE_KEY), value or None
    source_name, raw_index = value.split("::", 1)
    try:
        row_index: object = int(raw_index)
    except ValueError:
        row_index = raw_index
    return source_name, row_index

def _execute_agent_reconciliation_run(input_tables, missing_provider_keys, primary_provider, effective_primary_env):
    """Run the unchanged agent orchestration from a structured MUI event."""
    if (not bool(input_tables)) or bool(missing_provider_keys):
        runtime_state["agent_mui_status_message"] = {"severity": "warning", "text": "Resolve prerequisites before starting reconciliation."}
        return
    try:
        runtime_state["agent_prov_last_run_mapping_date"] = date.today().isoformat()
        config = _build_run_config_from_state()
        stop_event = runtime_state.get(AGENT_STOP_EVENT_KEY, {})
        stop_decision = str(runtime_state.get("agent_llm_error_stop_decision", "Fix issue and rerun") or "Fix issue and rerun")
        continue_with_heuristics = stop_decision == "Continue with heuristic fallback"
        resume_previous = bool(continue_with_heuristics and isinstance(stop_event, dict) and stop_event.get("stop_reason") == "llm_error")
        config.stop_on_llm_error = not continue_with_heuristics
        tables_for_run = _build_run_input_tables(
            input_tables,
            runtime_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {}),
            resume_previous=resume_previous,
        )
        total_terms_for_run = 0
        for table in tables_for_run:
            unreconciled_indices = get_unreconciled_indices(table.dataframe, "No Match")
            if resume_previous and "Run ID" in table.dataframe.columns:
                processed_mask = table.dataframe["Run ID"].astype(str).str.strip().ne("")
                unreconciled_indices = [idx for idx in unreconciled_indices if not bool(processed_mask.loc[idx])]
            total_terms_for_run += len(unreconciled_indices)
        run_started_perf = time.perf_counter()
        run_started_epoch = time.time()
        run_started_iso = datetime.fromtimestamp(run_started_epoch).isoformat()
        runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
            "running": True,
            "finished": False,
            "error": None,
            "stage": "retrieving_candidates",
            "message": "Retrieving candidate matches",
            "current_term": None,
            "processed_count": 0,
            "total_count": total_terms_for_run,
            "started_at": run_started_iso,
            "elapsed_seconds": 0,
            "estimated_remaining_seconds": None,
            "last_activity": "Run started; validating input and preparing candidate retrieval.",
        }
        configured_project = configure_langsmith_environment(config.langsmith_project)
        config.langsmith_project = configured_project
        reset_llm_interactions()
        langsmith_state = get_langsmith_readiness(config.langsmith_project)
        telemetry_enabled = bool(runtime_state.get("agent_use_langsmith_monitoring", False))
        runtime_state[AGENT_MONITORING_STATE_KEY] = {
            "enabled": telemetry_enabled,
            "run_id": None,
            "started_at": run_started_iso,
            "finished_at": None,
            "duration_sec": None,
            "total_terms": total_terms_for_run,
            "processed_terms": 0,
            "failed_terms": 0,
            "stop_reason": None,
            "stop_event": {},
            "events_df": pd.DataFrame(),
            "llm_interactions_df": pd.DataFrame(),
            "cascade_trace_df": pd.DataFrame(),
            "raw_term_events": [],
            "langsmith": {**langsmith_state, "run_url": None},
        }
        definitions_by_source: Dict[str, Dict[str, str]] = {}
        definition_preparation_enabled = bool(runtime_state.get("agent_enable_definition_preparation", False))
        for table in tables_for_run:
            if not definition_preparation_enabled:
                used_defs_df = pd.DataFrame(columns=["Term", "Definition"])
            else:
                strategy = config.definition_strategy
                if strategy == "uploaded_sheet":
                    uploaded_defs = runtime_state.get(AGENT_DEFINITIONS_BY_SOURCE_KEY, {}).get("__uploaded_sheet__")
                    used_defs_df = prepare_used_definitions_df(table.dataframe, strategy, uploaded_definitions_df=uploaded_defs)
                else:
                    context_text = runtime_state.get("agent_reference_publication_text", "") if strategy == "reference_publication" else runtime_state.get("agent_definition_context_text", "")
                    used_defs_df = prepare_used_definitions_df(
                        table.dataframe,
                        strategy,
                        context_text=context_text,
                        model_name=config.definition_model_name,
                        provider=config.definition_model_provider,
                        api_key_env=config.definition_model_api_key_env,
                        reasoning_effort=config.reasoning_effort,
                    )
            definitions_by_source[table.source_name] = build_definition_lookup(used_defs_df)
            runtime_state[AGENT_DEFINITIONS_BY_SOURCE_KEY][table.source_name] = used_defs_df

        latest_batch_state: Dict[str, object] = {"state": None}

        def _progress_callback(state):
            latest_batch_state["state"] = state
            runtime_state[AGENT_RUN_MESSAGES_KEY] = state.messages
            elapsed_seconds = time.perf_counter() - run_started_perf
            processed_terms = int(getattr(state, "processed_terms", 0) or 0)
            total_terms = int(getattr(state, "total_terms", total_terms_for_run) or total_terms_for_run or 0)
            current_event = (list(getattr(state, "term_events", []) or [])[-1] if getattr(state, "term_events", []) else {})
            current_term = str(current_event.get("term") or "").strip() or None
            workflow_name = str(current_event.get("workflow") or getattr(config, "workflow", "") or "")
            decision_source = str(current_event.get("decision_source") or "").strip()
            fallback_reason = str(current_event.get("fallback_reason") or "").strip()
            event_status = str(current_event.get("status") or "").strip()
            estimated_remaining = None
            if total_terms and processed_terms > 0 and elapsed_seconds > 0:
                estimated_remaining = (elapsed_seconds / processed_terms) * max(0, total_terms - processed_terms)
            stage = "retrieving_candidates"
            if processed_terms >= total_terms and total_terms:
                stage = "preparing_review"
            elif decision_source:
                stage = "ranking_candidates"
            if fallback_reason == "llm_error":
                stage = "selecting_match_type"
            message = str(state.messages[-1]) if getattr(state, "messages", None) else "Processing semantic mappings"
            runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
                "running": True,
                "finished": False,
                "error": None,
                "stage": stage,
                "message": message,
                "current_term": current_term,
                "processed_count": processed_terms,
                "total_count": total_terms,
                "started_at": run_started_iso,
                "elapsed_seconds": elapsed_seconds,
                "estimated_remaining_seconds": estimated_remaining,
                "last_activity": (
                    f"{current_term}: {event_status or 'processed'} via {workflow_name or 'agent workflow'}"
                    if current_term
                    else message
                ),
            }
            monitoring_state = runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
            if isinstance(monitoring_state, dict):
                events_df = _build_monitoring_event_snapshot(state)
                monitoring_state["run_id"] = getattr(state, "run_id", None)
                monitoring_state["total_terms"] = int(getattr(state, "total_terms", 0) or 0)
                monitoring_state["processed_terms"] = int(getattr(state, "processed_terms", 0) or 0)
                monitoring_state["failed_terms"] = int(getattr(state, "failed_terms", 0) or 0)
                monitoring_state["duration_sec"] = elapsed_seconds
                monitoring_state["events_df"] = events_df
                interactions_df = pd.DataFrame(get_llm_interactions(limit=500))
                monitoring_state["llm_interactions_df"] = interactions_df
                if not interactions_df.empty and "cost_usd" in interactions_df.columns:
                    monitoring_state["total_cost_usd"] = float(interactions_df["cost_usd"].fillna(0).sum())
                monitoring_state["cascade_trace_df"] = _build_cascade_trace_snapshot(state)
                monitoring_state["raw_term_events"] = list(getattr(state, "term_events", []) or [])
                langsmith_dict = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
                if langsmith_dict.get("project") and getattr(state, "run_id", None):
                    langsmith_dict["run_url"] = build_run_url(str(langsmith_dict.get("project")), str(state.run_id))
                    monitoring_state["langsmith"] = langsmith_dict
                runtime_state[AGENT_MONITORING_STATE_KEY] = monitoring_state

        outputs = run_agent_batch(
            tables_for_run,
            config,
            definitions_by_source=definitions_by_source,
            bioportal_api_key=(CONFIG or {}).get("bioportal", {}).get("api_key"),
            progress_callback=_progress_callback,
            resume_skip_processed_terms=resume_previous,
        )
        runtime_state[AGENT_RESULTS_BY_SOURCE_KEY] = outputs
        if outputs and not runtime_state.get(AGENT_SELECTED_SOURCE_KEY):
            runtime_state[AGENT_SELECTED_SOURCE_KEY] = list(outputs.keys())[0]
        _sync_selected_source_dataframe()
        monitoring_state = runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
        if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
            monitoring_state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            monitoring_state["duration_sec"] = time.perf_counter() - run_started_perf
            runtime_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
        final_elapsed_seconds = time.perf_counter() - run_started_perf
        final_processed = total_terms_for_run
        latest_state_obj = latest_batch_state.get("state")
        if latest_state_obj is not None:
            final_processed = int(getattr(latest_state_obj, "processed_terms", final_processed) or 0)
        runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
            **runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {}),
            "running": False,
            "finished": True,
            "stage": "writing_output",
            "message": "Run completed; preparing review output",
            "processed_count": final_processed,
            "total_count": total_terms_for_run,
            "elapsed_seconds": final_elapsed_seconds,
            "estimated_remaining_seconds": 0,
            "last_activity": "Agent run completed and review suggestions are ready.",
        }
        llm_stop_event = {}
        if latest_state_obj is not None and getattr(latest_state_obj, "stop_reason", None) == "llm_error":
            state_stop_event = getattr(latest_state_obj, "stop_event", {}) or {}
            if isinstance(state_stop_event, dict):
                llm_stop_event = {
                    "stop_reason": "llm_error",
                    "file": state_stop_event.get("file"),
                    "term": state_stop_event.get("term"),
                    "fallback_error_type": state_stop_event.get("fallback_error_type"),
                    "fallback_error_message": state_stop_event.get("fallback_error_message"),
                    "fallback_reason": state_stop_event.get("fallback_reason"),
                    "llm_fix_suggestion": state_stop_event.get("llm_fix_suggestion"),
                    "workflow": state_stop_event.get("workflow"),
                    "decision_source": state_stop_event.get("decision_source"),
                }
        if llm_stop_event:
            runtime_state[AGENT_STOP_EVENT_KEY] = llm_stop_event
            if isinstance(monitoring_state, dict):
                monitoring_state["stop_reason"] = "llm_error"
                monitoring_state["stop_event"] = llm_stop_event
                runtime_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
            stopped_term = str(llm_stop_event.get("term", "") or "").strip() or "(unknown term)"
            runtime_state["agent_mui_status_message"] = {"severity": "warning", "text": f"Run stopped automatically due to LLM error at term '{stopped_term}'."}
        else:
            runtime_state[AGENT_STOP_EVENT_KEY] = {}
            if isinstance(monitoring_state, dict):
                monitoring_state["stop_reason"] = None
                monitoring_state["stop_event"] = {}
                runtime_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
            runtime_state["agent_mui_status_message"] = {"severity": "success", "text": "Agent-based reconciliation run completed."}
    except Exception as exc:
        if _is_openai_compatible_provider(primary_provider) and is_openai_compatible_auth_required_error(exc):
            env_name = effective_primary_env or get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
                **runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {}),
                "running": False,
                "finished": False,
                "error": f"OpenAI-compatible endpoint rejected unauthenticated requests. Set {env_name} and run again.",
                "stage": "preparing_review",
                "message": f"Agent-based reconciliation failed: set {env_name} and run again.",
                "elapsed_seconds": time.perf_counter() - run_started_perf if "run_started_perf" in locals() else None,
                "last_activity": f"Run failed: missing or rejected {env_name}.",
            }
            runtime_state["agent_mui_status_message"] = {"severity": "error", "text": f"OpenAI-compatible endpoint rejected unauthenticated requests. Set {env_name} and run again."}
            return
        monitoring_state = runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
        if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
            monitoring_state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            monitoring_state["duration_sec"] = time.perf_counter() - run_started_perf if "run_started_perf" in locals() else None
            langsmith_dict = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
            existing = str(langsmith_dict.get("message", "") or "").strip()
            langsmith_dict["message"] = (existing + " " if existing else "") + f"Run failed: {exc}"
            monitoring_state["langsmith"] = langsmith_dict
            runtime_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
        runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
            **runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {}),
            "running": False,
            "finished": False,
            "error": str(exc),
            "stage": "preparing_review",
            "message": f"Agent-based reconciliation failed: {exc}",
            "elapsed_seconds": time.perf_counter() - run_started_perf if "run_started_perf" in locals() else None,
            "last_activity": f"Run failed: {exc}",
        }
        runtime_state["agent_mui_status_message"] = {"severity": "error", "text": f"Agent-based reconciliation failed: {exc}"}

def _start_agent_reconciliation_run_async(input_tables, missing_provider_keys, primary_provider, effective_primary_env) -> bool:
    """Launch a reconciliation run without blocking the HTTP event response."""
    existing_thread = runtime_state.get(AGENT_RUN_THREAD_STATE_KEY)
    live_status = runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {})
    if bool(isinstance(live_status, dict) and live_status.get("running")) or (
        isinstance(existing_thread, threading.Thread) and existing_thread.is_alive()
    ):
        runtime_state["agent_mui_status_message"] = {"severity": "info", "text": "Agent-based reconciliation is already running."}
        return False

    started_iso = datetime.now().isoformat()
    runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
        "running": True,
        "finished": False,
        "error": None,
        "stage": "validating_input",
        "message": "Starting agent-based reconciliation",
        "current_term": None,
        "processed_count": 0,
        "total_count": None,
        "started_at": started_iso,
        "elapsed_seconds": 0,
        "estimated_remaining_seconds": None,
        "last_activity": "Run queued; backend worker is starting.",
    }
    previous_monitoring = runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
    previous_langsmith = (
        previous_monitoring.get("langsmith", {})
        if isinstance(previous_monitoring, dict) and isinstance(previous_monitoring.get("langsmith"), dict)
        else {}
    )
    runtime_state[AGENT_MONITORING_STATE_KEY] = {
        "enabled": bool(runtime_state.get("agent_use_langsmith_monitoring", False)),
        "run_id": None,
        "started_at": started_iso,
        "finished_at": None,
        "duration_sec": None,
        "total_terms": 0,
        "processed_terms": 0,
        "failed_terms": 0,
        "stop_reason": None,
        "stop_event": {},
        "events_df": pd.DataFrame(),
        "llm_interactions_df": pd.DataFrame(),
        "cascade_trace_df": pd.DataFrame(),
        "raw_term_events": [],
        "total_cost_usd": 0.0,
        "langsmith": {**previous_langsmith, "run_url": None},
    }
    runtime_state[AGENT_RUN_MESSAGES_KEY] = []
    runtime_state["agent_mui_status_message"] = {"severity": "info", "text": "Agent-based reconciliation run started."}

    thread = threading.Thread(
        target=_execute_agent_reconciliation_run,
        args=(input_tables, missing_provider_keys, primary_provider, effective_primary_env),
        name="agent-reconciliation-run",
        daemon=True,
    )
    runtime_state[AGENT_RUN_THREAD_STATE_KEY] = thread
    thread.start()
    return True

def _handle_agent_mui_event(event: object, readiness_state: Dict[str, object], runtime_context: Dict[str, object], provenance_defaults_cfg: Dict[str, str]) -> bool:
    if not isinstance(event, dict):
        return False
    nonce = event.get("nonce")
    if nonce and runtime_state.get(AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY) == nonce:
        return False
    if nonce:
        runtime_state[AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY] = nonce
    event_type = str(event.get("type", "") or "")
    should_rerun = False

    if event_type == "config_changed":
        should_rerun = _apply_workflow_config_to_runtime_state(event.get("config"))
    elif event_type == "navigate":
        target_stage = _stage_from_component(event.get("stage"))
        if target_stage:
            runtime_state[AGENT_ACTIVE_STEP_KEY] = target_stage
            should_rerun = True
    elif event_type == "upload_csv":
        filename = str(event.get("filename", "") or "uploaded.csv").strip() or "uploaded.csv"
        content = event.get("content", "")
        if not filename.lower().endswith(".csv"):
            runtime_state["agent_mui_status_message"] = {"severity": "error", "text": "Please upload a .csv matching table."}
        elif not isinstance(content, str) or not content.strip():
            runtime_state["agent_mui_status_message"] = {"severity": "error", "text": "The uploaded CSV file is empty."}
        else:
            try:
                dataframe = pd.read_csv(io.StringIO(content)).fillna("")
                table = make_input_table(
                    dataframe,
                    source_name=os.path.splitext(filename)[0] or "Uploaded CSV",
                    filename=filename,
                )
                _store_input_tables(
                    [table],
                    f"Agent-based reconciliation data successfully loaded from uploaded CSV matching table: {filename}.",
                )
                runtime_state[AGENT_UPLOADED_SOURCE_SIGNATURE_KEY] = f"{filename}:{len(content)}"
                runtime_state["agent_mui_status_message"] = {"severity": "success", "text": f"CSV matching table '{filename}' loaded into the agent workflow."}
            except Exception as exc:
                runtime_state["agent_mui_status_message"] = {"severity": "error", "text": f"Failed to parse uploaded CSV file: {exc}"}
        should_rerun = True
    elif event_type == "load_shared_table":
        shared_df = runtime_state.get("shared_matching_table")
        if isinstance(shared_df, pd.DataFrame):
            table = make_input_table(
                shared_df,
                source_name="Matching Table Generator",
                filename="shared_matching_table",
                is_from_shared_matching_table=True,
            )
            _store_input_tables([table], "Agent-based reconciliation data successfully loaded from: Matching Table Generator.")
            runtime_state["agent_mui_status_message"] = {"severity": "success", "text": "Shared matching table loaded into the agent workflow."}
        else:
            runtime_state["agent_mui_status_message"] = {"severity": "warning", "text": "No shared matching table is available in runtime state."}
        should_rerun = True
    elif event_type == "codex_auth_signin":
        try:
            start_codex_authorization_flow(open_browser=True)
            runtime_state["agent_mui_status_message"] = {"severity": "success", "text": "Check your browser to complete ChatGPT sign in."}
        except Exception as exc:
            runtime_state["agent_mui_status_message"] = {"severity": "error", "text": f"Sign in error: {exc}"}
        should_rerun = True
    elif event_type == "codex_auth_signout":
        clear_codex_credentials()
        runtime_state["agent_mui_status_message"] = {"severity": "success", "text": "Logged out of ChatGPT Subscription."}
        should_rerun = True
    elif event_type in {"codex_auth_refresh", "codex_auth_refresh_pending"}:
        should_rerun = True
    elif event_type == "start_run":
        _start_agent_reconciliation_run_async(
            runtime_state.get(AGENT_INPUT_TABLES_KEY, []),
            runtime_context.get("missing_provider_keys", []),
            runtime_context.get("primary_provider", "openai"),
            runtime_context.get("effective_primary_env", get_default_api_key_env("openai")),
        )
        should_rerun = True
    elif event_type in {"accept_mapping", "reject_mapping", "reset_mapping"}:
        source_name, row_index = _parse_mapping_id(event.get("mapping_id"))
        action = {"accept_mapping": "accept", "reject_mapping": "reject", "reset_mapping": "reset"}[event_type]
        if source_name is not None and row_index is not None:
            selected_match_type = event.get("selected_match_type") if action == "accept" else None
            _apply_review_action(
                source_name,
                row_index,
                action,
                selected_match_type=str(selected_match_type or ""),
            )
            runtime_state["agent_mui_status_message"] = {"severity": "success", "text": f"Mapping {action} action applied."}
            should_rerun = True
    elif event_type == "save_configuration":
        provider = str(runtime_state.get("agent_model_provider", runtime_context.get("primary_provider", "openai")) or "openai")
        model = str(runtime_state.get("agent_model_name", "") or "")
        ok, msg = _save_preferred_model_selection(provider, model)
        runtime_state["agent_mui_status_message"] = {"severity": "success" if ok else "error", "text": msg}
        should_rerun = True
    elif event_type == "reload_models":
        provider = str(runtime_state.get("agent_model_provider", runtime_context.get("primary_provider", "openai")) or "openai")
        api_key_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", provider, "model_api_key_env")
        fetch_all_pricing(force_refresh=True)
        _ensure_model_catalog_for_provider(provider, api_key_env=api_key_env, force_refresh=True)
        runtime_state["agent_mui_status_message"] = {"severity": "success", "text": "Model catalog and pricing reloaded."}
        should_rerun = True
    elif event_type == "register_local_model":
        provider = str(runtime_state.get("agent_model_provider", runtime_context.get("primary_provider", "openai")) or "openai")
        api_key_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", provider, "model_api_key_env")
        ok, msg = _register_openai_compatible_model_from_override(str(runtime_state.get("agent_custom_model_override", "") or ""), provider, api_key_env)
        runtime_state["agent_mui_status_message"] = {"severity": "success" if ok else "error", "text": msg}
        should_rerun = True
    elif event_type == "save_provenance_defaults":
        defaults = _build_provenance_defaults_from_state()
        if not defaults.get("mapping_tool"):
            defaults["mapping_tool"] = provenance_defaults_cfg.get("mapping_tool", "RDF4Risk Agent-Based Reconciliation")
        ok, msg = _save_preferred_provenance_defaults(defaults)
        runtime_state["agent_mui_status_message"] = {"severity": "success" if ok else "error", "text": msg}
        should_rerun = True
    elif event_type in {"publish_rdf_handoff", "export_sssom"}:
        agent_df = runtime_state.get(AGENT_DATAFRAME_STATE_KEY)
        if isinstance(agent_df, pd.DataFrame):
            export_df = finalize_accepted_results(agent_df.copy(), provenance_defaults=_build_provenance_defaults_from_state())
            runtime_state["shared_reconciled_matching_table"] = export_df
            if event_type == "export_sssom":
                selected_source = str(runtime_state.get(AGENT_SELECTED_SOURCE_KEY) or "agent_reconciliation").strip() or "agent_reconciliation"
                safe_source = re.sub(r"[^A-Za-z0-9_.-]+", "_", selected_source).strip("_") or "agent_reconciliation"
                export_filename = f"{safe_source}_agent_reconciled_sssom.csv"
                runtime_state[AGENT_SSSOM_EXPORT_PAYLOAD_KEY] = {
                    "nonce": int(time.time() * 1000),
                    "filename": export_filename,
                    "content": export_df.to_csv(index=False),
                    "mime_type": "text/csv;charset=utf-8",
                }
                runtime_state["agent_mui_status_message"] = {"severity": "success", "text": f"SSSOM export download prepared: {export_filename}."}
            else:
                runtime_state["agent_mui_status_message"] = {"severity": "success", "text": "Accepted mappings published to RDF Generator handoff."}
        else:
            runtime_state["agent_mui_status_message"] = {"severity": "warning", "text": "No working table is available for export."}
        should_rerun = True
    return should_rerun
