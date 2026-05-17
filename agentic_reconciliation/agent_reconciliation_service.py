# -*- coding: utf-8 -*-
"""Backend service for the Material UI agent reconciliation app."""

from __future__ import annotations

import base64
import io
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
    from . import agent_reconciliation_config_state as _config_state_impl
    from . import agent_reconciliation_events as _event_impl
    from .agent_reconciliation_keys import (
        AGENT_ACTIVE_STEP_KEY,
        AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY,
        AGENT_DATA_SOURCE_MESSAGE_KEY,
        AGENT_DATAFRAME_STATE_KEY,
        AGENT_DEFINITIONS_BY_SOURCE_KEY,
        AGENT_INPUT_TABLES_KEY,
        AGENT_LAST_SOURCE_NAME_KEY,
        AGENT_MONITORING_STATE_KEY,
        AGENT_RESULTS_BY_SOURCE_KEY,
        AGENT_RUN_CANCEL_EVENT_STATE_KEY,
        AGENT_RUN_MESSAGES_KEY,
        AGENT_RUN_STATUS_STATE_KEY,
        AGENT_RUN_THREAD_STATE_KEY,
        AGENT_SELECTED_SOURCE_KEY,
        AGENT_SSSOM_EXPORT_PAYLOAD_KEY,
        AGENT_STOP_EVENT_KEY,
        AGENT_UPLOADED_SOURCE_SIGNATURE_KEY,
        AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY,
        AGENT_WORKFLOW_CONFIG_STATE_KEY,
        COMPONENT_TO_STAGE as _COMPONENT_TO_STAGE,
        ORCID_BASE_URL,
        REASONING_EFFORT_OPTIONS,
        STAGE_TO_COMPONENT as _STAGE_TO_COMPONENT,
    )
    from .agent_codex_subscription_service import clear_codex_credentials, get_codex_auth_status, is_codex_authenticated, start_codex_authorization_flow
    from .agent_definition_service import build_definition_lookup, extract_reference_publication_text, normalize_uploaded_definitions, prepare_used_definitions_df
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
    from .agent_reconciliation_snapshots import (
        _build_data_status_snapshot,
        _build_review_snapshot,
        _build_run_status_snapshot,
        _build_telemetry_snapshot,
    )
    from .agent_provider_config import (
        LEGACY_AGENT_MODEL_CONFIG_KEYS,
        OPENAI_CODEX_PROVIDER,
        OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY,
        OPENAI_COMPATIBLE_BASE_URL_ENV,
        OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY,
        OPENAI_COMPATIBLE_PROVIDER,
        _is_codex_provider,
        _is_openai_compatible_provider,
        _is_populated_api_key,
        _merge_openai_compatible_model_registry,
        _normalize_openai_compatible_base_url,
        _normalize_openai_compatible_model_registry,
    )
    from .agent_model_catalog import (
        _extract_model_ids_from_catalog,
        _extract_model_records_from_catalog,
        _find_model_record,
        _format_model_details_caption,
        _format_model_option_label,
        _openai_compatible_catalog_requires_api_key,
    )
    from semi_automatic_reconciliation.reconciliation_core import CONFIG
    from semi_automatic_reconciliation.snapshot_utils import dataframe_records as _dataframe_records, json_safe_value as _json_safe_value
    from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS, REQUIRED_MATCHING_TABLE_COLUMNS, get_unreconciled_indices, finalize_accepted_results
except ImportError:  # pragma: no cover
    from agent_runtime_state import runtime_state
    import agent_reconciliation_config_state as _config_state_impl
    import agent_reconciliation_events as _event_impl
    from agent_reconciliation_keys import (
        AGENT_ACTIVE_STEP_KEY,
        AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY,
        AGENT_DATA_SOURCE_MESSAGE_KEY,
        AGENT_DATAFRAME_STATE_KEY,
        AGENT_DEFINITIONS_BY_SOURCE_KEY,
        AGENT_INPUT_TABLES_KEY,
        AGENT_LAST_SOURCE_NAME_KEY,
        AGENT_MONITORING_STATE_KEY,
        AGENT_RESULTS_BY_SOURCE_KEY,
        AGENT_RUN_CANCEL_EVENT_STATE_KEY,
        AGENT_RUN_MESSAGES_KEY,
        AGENT_RUN_STATUS_STATE_KEY,
        AGENT_RUN_THREAD_STATE_KEY,
        AGENT_SELECTED_SOURCE_KEY,
        AGENT_SSSOM_EXPORT_PAYLOAD_KEY,
        AGENT_STOP_EVENT_KEY,
        AGENT_UPLOADED_SOURCE_SIGNATURE_KEY,
        AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY,
        AGENT_WORKFLOW_CONFIG_STATE_KEY,
        COMPONENT_TO_STAGE as _COMPONENT_TO_STAGE,
        ORCID_BASE_URL,
        REASONING_EFFORT_OPTIONS,
        STAGE_TO_COMPONENT as _STAGE_TO_COMPONENT,
    )
    from agent_codex_subscription_service import clear_codex_credentials, get_codex_auth_status, is_codex_authenticated, start_codex_authorization_flow
    from agent_definition_service import build_definition_lookup, extract_reference_publication_text, normalize_uploaded_definitions, prepare_used_definitions_df
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
    from agent_reconciliation_snapshots import (
        _build_data_status_snapshot,
        _build_review_snapshot,
        _build_run_status_snapshot,
        _build_telemetry_snapshot,
    )
    from agent_provider_config import (
        LEGACY_AGENT_MODEL_CONFIG_KEYS,
        OPENAI_CODEX_PROVIDER,
        OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY,
        OPENAI_COMPATIBLE_BASE_URL_ENV,
        OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY,
        OPENAI_COMPATIBLE_PROVIDER,
        _is_codex_provider,
        _is_openai_compatible_provider,
        _is_populated_api_key,
        _merge_openai_compatible_model_registry,
        _normalize_openai_compatible_base_url,
        _normalize_openai_compatible_model_registry,
    )
    from agent_model_catalog import (
        _extract_model_ids_from_catalog,
        _extract_model_records_from_catalog,
        _find_model_record,
        _format_model_details_caption,
        _format_model_option_label,
        _openai_compatible_catalog_requires_api_key,
    )
    from semi_automatic_reconciliation.reconciliation_core import CONFIG
    from semi_automatic_reconciliation.snapshot_utils import dataframe_records as _dataframe_records, json_safe_value as _json_safe_value
    from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS, REQUIRED_MATCHING_TABLE_COLUMNS, get_unreconciled_indices, finalize_accepted_results

def _decode_uploaded_file_bytes(event: Dict[str, object]) -> bytes:
    content_base64 = event.get("content_base64")
    if isinstance(content_base64, str) and content_base64.strip():
        try:
            return base64.b64decode(content_base64, validate=True)
        except Exception as exc:
            raise ValueError("Uploaded file payload is not valid base64.") from exc

    content = event.get("content")
    if isinstance(content, str):
        return content.encode("utf-8")

    raise ValueError("Uploaded file payload is missing.")


def _read_uploaded_definitions_sheet(filename: str, file_bytes: bytes) -> pd.DataFrame:
    lower = filename.lower()
    if lower.endswith(".csv"):
        dataframe = pd.read_csv(io.BytesIO(file_bytes)).fillna("")
    elif lower.endswith((".xlsx", ".xls")):
        dataframe = pd.read_excel(io.BytesIO(file_bytes)).fillna("")
    else:
        raise ValueError("Please upload a CSV or Excel definitions sheet.")
    return normalize_uploaded_definitions(dataframe)


def _extract_reference_publication_text_from_bytes(filename: str, file_bytes: bytes) -> str:
    uploaded = io.BytesIO(file_bytes)
    uploaded.name = filename
    return extract_reference_publication_text(uploaded)


def _get_uploaded_definitions_count() -> int:
    definitions_by_source = runtime_state.get(AGENT_DEFINITIONS_BY_SOURCE_KEY, {})
    if not isinstance(definitions_by_source, dict):
        return 0
    uploaded_definitions = definitions_by_source.get("__uploaded_sheet__")
    return len(uploaded_definitions) if isinstance(uploaded_definitions, pd.DataFrame) else 0


def _get_reconciliation_config_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config.yaml",
    )

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

def _sync_config_state_dependencies() -> None:
    _config_state_impl.CONFIG = CONFIG
    for name in (
        "_build_provenance_defaults_from_state",
        "_ensure_model_catalog_for_provider",
        "_ensure_openai_compatible_base_url_available",
        "_ensure_provider_api_key_available",
        "_get_openai_compatible_base_url_from_config",
        "_get_provider_api_key_from_config",
        "_get_uploaded_definitions_count",
        "_resolve_api_key_env_for_provider",
    ):
        setattr(_config_state_impl, name, globals()[name])


def _build_run_config_from_state() -> AgentRunConfig:
    _sync_config_state_dependencies()
    return _config_state_impl._build_run_config_from_state()


def _initialize_agent_reconciliation_state():
    _sync_config_state_dependencies()
    return _config_state_impl._initialize_agent_reconciliation_state()


def _initialize_provenance_state(provenance_defaults_cfg: Dict[str, str]):
    _sync_config_state_dependencies()
    return _config_state_impl._initialize_provenance_state(provenance_defaults_cfg)


def _get_agent_ui_defaults() -> Dict[str, object]:
    _sync_config_state_dependencies()
    return _config_state_impl._get_agent_ui_defaults()


def _build_workflow_config_from_state(defaults: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    _sync_config_state_dependencies()
    return _config_state_impl._build_workflow_config_from_state(defaults)


def _apply_workflow_config_to_runtime_state(config: Optional[Dict[str, object]]) -> bool:
    _sync_config_state_dependencies()
    return _config_state_impl._apply_workflow_config_to_runtime_state(config)


def _get_active_agent_stage() -> str:
    _sync_config_state_dependencies()
    return _config_state_impl._get_active_agent_stage()


def _compute_workflow_runtime_context(defaults: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    _sync_config_state_dependencies()
    return _config_state_impl._compute_workflow_runtime_context(defaults)


def _build_run_readiness_state(required_columns, missing_provider_keys: Optional[List[tuple]] = None) -> Dict[str, object]:
    _sync_config_state_dependencies()
    return _config_state_impl._build_run_readiness_state(required_columns, missing_provider_keys)


def _component_stage_from_session() -> str:
    _sync_config_state_dependencies()
    return _config_state_impl._component_stage_from_session()


def _stage_from_component(value: object) -> Optional[str]:
    _sync_config_state_dependencies()
    return _config_state_impl._stage_from_component(value)


def _parse_mapping_id(mapping_id: object) -> tuple[Optional[str], Optional[object]]:
    _sync_config_state_dependencies()
    return _config_state_impl._parse_mapping_id(mapping_id)


def _sync_event_dependencies() -> None:
    _event_impl.CONFIG = CONFIG
    patched_execute = globals()["_execute_agent_reconciliation_run"]
    _event_impl._execute_agent_reconciliation_run = (
        patched_execute
        if patched_execute is not _SERVICE_EXECUTE_RUN_WRAPPER
        else _EVENT_EXECUTE_RUN_IMPL
    )
    for name in (
        "_apply_workflow_config_to_runtime_state",
        "_build_provenance_defaults_from_state",
        "_build_run_config_from_state",
        "_decode_uploaded_file_bytes",
        "_ensure_model_catalog_for_provider",
        "_extract_reference_publication_text_from_bytes",
        "_parse_mapping_id",
        "_read_uploaded_definitions_sheet",
        "_register_openai_compatible_model_from_override",
        "_resolve_api_key_env_for_provider",
        "_save_preferred_model_selection",
        "_save_preferred_provenance_defaults",
        "_stage_from_component",
    ):
        setattr(_event_impl, name, globals()[name])


def _execute_agent_reconciliation_run(
    input_tables,
    missing_provider_keys,
    primary_provider,
    effective_primary_env,
    resume_previous_requested: bool = False,
    stop_signal: Optional[threading.Event] = None,
):
    _sync_event_dependencies()
    return _EVENT_EXECUTE_RUN_IMPL(
        input_tables,
        missing_provider_keys,
        primary_provider,
        effective_primary_env,
        resume_previous_requested,
        stop_signal,
    )


def _start_agent_reconciliation_run_async(
    input_tables,
    missing_provider_keys,
    primary_provider,
    effective_primary_env,
    resume_previous: bool = False,
) -> bool:
    _sync_event_dependencies()
    return _event_impl._start_agent_reconciliation_run_async(
        input_tables,
        missing_provider_keys,
        primary_provider,
        effective_primary_env,
        resume_previous,
    )


def _handle_agent_mui_event(event: object, readiness_state: Dict[str, object], runtime_context: Dict[str, object], provenance_defaults_cfg: Dict[str, str]) -> bool:
    _sync_event_dependencies()
    return _event_impl._handle_agent_mui_event(event, readiness_state, runtime_context, provenance_defaults_cfg)


_EVENT_EXECUTE_RUN_IMPL = _event_impl._execute_agent_reconciliation_run
_SERVICE_EXECUTE_RUN_WRAPPER = _execute_agent_reconciliation_run
