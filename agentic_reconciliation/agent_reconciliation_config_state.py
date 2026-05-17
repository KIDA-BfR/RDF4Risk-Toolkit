# -*- coding: utf-8 -*-
"""Runtime configuration state helpers for the agent reconciliation backend."""

from __future__ import annotations

import os
from typing import Dict, List, Optional

import pandas as pd

from .agent_model_catalog import _extract_model_ids_from_catalog, _openai_compatible_catalog_requires_api_key
from .agent_models import AgentRunConfig
from .agent_provider_config import (
    OPENAI_COMPATIBLE_BASE_URL_ENV,
    OPENAI_COMPATIBLE_PROVIDER,
    _is_openai_compatible_provider,
    _normalize_openai_compatible_base_url,
)
from .agent_reconciliation_keys import *
from .agent_runtime_state import runtime_state
from .agent_llm_service import get_default_api_key_env, get_default_model_options, get_provider_label, get_supported_llm_providers
from semi_automatic_reconciliation.reconciliation_core import CONFIG
from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS

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
        "agent_reference_publication_filename": "",
        "agent_uploaded_definitions_filename": "",
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
        "definition_uploaded_filename": runtime_state.get("agent_uploaded_definitions_filename", ""),
        "definition_uploaded_count": _get_uploaded_definitions_count(),
        "definition_reference_filename": runtime_state.get("agent_reference_publication_filename", ""),
        "definition_reference_text": runtime_state.get("agent_reference_publication_text", ""),
        "definition_reference_char_count": len(str(runtime_state.get("agent_reference_publication_text", "") or "")),
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

