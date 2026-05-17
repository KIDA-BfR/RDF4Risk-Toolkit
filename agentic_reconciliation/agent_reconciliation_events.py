# -*- coding: utf-8 -*-
"""Event handling and asynchronous run execution for agent reconciliation."""

from __future__ import annotations

import io
import os
import re
import threading
import time
from datetime import date, datetime
from typing import Dict, Optional

import pandas as pd

from .agent_codex_subscription_service import clear_codex_credentials, start_codex_authorization_flow
from .agent_definition_service import build_definition_lookup, prepare_used_definitions_df
from .agent_file_service import make_input_table
from .agent_langsmith_monitoring import (
    build_run_url,
    configure_langsmith_environment,
    get_langsmith_readiness,
    get_llm_interactions,
    reset_llm_interactions,
)
from .agent_llm_service import get_default_api_key_env, is_openai_compatible_auth_required_error
from .agent_orchestrator import run_agent_batch
from .agent_provider_config import OPENAI_COMPATIBLE_PROVIDER, _is_openai_compatible_provider
from .agent_reconciliation_keys import *
from .agent_reconciliation_ui_monitoring import _build_cascade_trace_snapshot, _build_monitoring_event_snapshot
from .agent_reconciliation_ui_review import _apply_review_action
from .agent_reconciliation_ui_state import _build_run_input_tables, _store_input_tables, _sync_selected_source_dataframe
from .agent_runtime_state import runtime_state
from semi_automatic_reconciliation.reconciliation_core import CONFIG
from semi_automatic_reconciliation.shared_table_io import finalize_accepted_results, get_unreconciled_indices

def _execute_agent_reconciliation_run(
    input_tables,
    missing_provider_keys,
    primary_provider,
    effective_primary_env,
    resume_previous_requested: bool = False,
    stop_signal: Optional[threading.Event] = None,
):
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
        manual_resume = bool(
            resume_previous_requested
            and isinstance(stop_event, dict)
            and stop_event.get("stop_reason") == "user_stopped"
        )
        llm_resume = bool(continue_with_heuristics and isinstance(stop_event, dict) and stop_event.get("stop_reason") == "llm_error")
        resume_previous = bool(manual_resume or llm_resume)
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
            "stop_requested": False,
            "stopped": False,
            "stop_reason": None,
            "resume_previous": resume_previous,
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

        def _stop_requested() -> bool:
            return bool(stop_signal is not None and stop_signal.is_set())

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
                "message": "Stop requested; finishing the current in-flight term(s)." if _stop_requested() else message,
                "current_term": current_term,
                "processed_count": processed_terms,
                "total_count": total_terms,
                "started_at": run_started_iso,
                "elapsed_seconds": elapsed_seconds,
                "estimated_remaining_seconds": estimated_remaining,
                "stop_requested": _stop_requested(),
                "stopped": False,
                "stop_reason": None,
                "resume_previous": resume_previous,
                "last_activity": (
                    "Stop requested; backend will pause before the next unstarted term."
                    if _stop_requested()
                    else f"{current_term}: {event_status or 'processed'} via {workflow_name or 'agent workflow'}"
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
            stop_requested_callback=_stop_requested,
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
        final_stop_reason = str(getattr(latest_state_obj, "stop_reason", "") or "").strip() if latest_state_obj is not None else ""
        stopped_by_user = final_stop_reason == "user_stopped"
        runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
            **runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {}),
            "running": False,
            "finished": not stopped_by_user,
            "stopped": stopped_by_user,
            "stage": "preparing_review" if stopped_by_user else "writing_output",
            "message": (
                f"Run stopped after {final_processed}/{total_terms_for_run} term(s)."
                if stopped_by_user
                else "Run completed; preparing review output"
            ),
            "processed_count": final_processed,
            "total_count": total_terms_for_run,
            "elapsed_seconds": final_elapsed_seconds,
            "estimated_remaining_seconds": None if stopped_by_user else 0,
            "stop_requested": False,
            "stop_reason": final_stop_reason or None,
            "last_activity": (
                "Agent run stopped by user. Resume continues at the next unprocessed term."
                if stopped_by_user
                else "Agent run completed and review suggestions are ready."
            ),
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
        user_stop_event = {}
        if stopped_by_user:
            state_stop_event = getattr(latest_state_obj, "stop_event", {}) or {}
            if isinstance(state_stop_event, dict):
                user_stop_event = {
                    "stop_reason": "user_stopped",
                    "file": state_stop_event.get("file"),
                    "term": state_stop_event.get("term"),
                    "processed_terms": state_stop_event.get("processed_terms", final_processed),
                    "total_terms": state_stop_event.get("total_terms", total_terms_for_run),
                }
        if user_stop_event:
            runtime_state[AGENT_STOP_EVENT_KEY] = user_stop_event
            if isinstance(monitoring_state, dict):
                monitoring_state["stop_reason"] = "user_stopped"
                monitoring_state["stop_event"] = user_stop_event
                runtime_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
            runtime_state["agent_mui_status_message"] = {
                "severity": "warning",
                "text": f"Run stopped by user after {final_processed}/{total_terms_for_run} term(s).",
            }
        elif llm_stop_event:
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

def _start_agent_reconciliation_run_async(
    input_tables,
    missing_provider_keys,
    primary_provider,
    effective_primary_env,
    resume_previous: bool = False,
) -> bool:
    """Launch a reconciliation run without blocking the HTTP event response."""
    existing_thread = runtime_state.get(AGENT_RUN_THREAD_STATE_KEY)
    live_status = runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {})
    if bool(isinstance(live_status, dict) and live_status.get("running")) or (
        isinstance(existing_thread, threading.Thread) and existing_thread.is_alive()
    ):
        runtime_state["agent_mui_status_message"] = {"severity": "info", "text": "Agent-based reconciliation is already running."}
        return False

    started_iso = datetime.now().isoformat()
    cancel_event = threading.Event()
    runtime_state[AGENT_RUN_CANCEL_EVENT_STATE_KEY] = cancel_event
    runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
        "running": True,
        "finished": False,
        "error": None,
        "stage": "validating_input",
        "message": "Resuming agent-based reconciliation" if resume_previous else "Starting agent-based reconciliation",
        "current_term": None,
        "processed_count": 0,
        "total_count": None,
        "started_at": started_iso,
        "elapsed_seconds": 0,
        "estimated_remaining_seconds": None,
        "last_activity": "Run queued; backend worker is starting.",
        "stop_requested": False,
        "stopped": False,
        "stop_reason": None,
        "resume_previous": bool(resume_previous),
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
        args=(input_tables, missing_provider_keys, primary_provider, effective_primary_env, bool(resume_previous), cancel_event),
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
    elif event_type == "upload_definitions_sheet":
        filename = str(event.get("filename", "") or "definitions.csv").strip() or "definitions.csv"
        try:
            normalized_definitions = _read_uploaded_definitions_sheet(filename, _decode_uploaded_file_bytes(event))
            definitions_by_source = runtime_state.get(AGENT_DEFINITIONS_BY_SOURCE_KEY)
            if not isinstance(definitions_by_source, dict):
                definitions_by_source = {}
            definitions_by_source["__uploaded_sheet__"] = normalized_definitions
            runtime_state[AGENT_DEFINITIONS_BY_SOURCE_KEY] = definitions_by_source
            runtime_state["agent_uploaded_definitions_filename"] = filename
            runtime_state["agent_enable_definition_preparation"] = True
            runtime_state["agent_definition_strategy"] = "uploaded_sheet"
            runtime_state["agent_mui_status_message"] = {
                "severity": "success",
                "text": f"Definitions sheet '{filename}' loaded with {len(normalized_definitions)} definition(s).",
            }
        except Exception as exc:
            runtime_state["agent_mui_status_message"] = {"severity": "error", "text": f"Failed to load definitions sheet: {exc}"}
        should_rerun = True
    elif event_type == "upload_reference_publication":
        filename = str(event.get("filename", "") or "reference_publication").strip() or "reference_publication"
        try:
            reference_text = _extract_reference_publication_text_from_bytes(filename, _decode_uploaded_file_bytes(event))
            runtime_state["agent_reference_publication_text"] = reference_text
            runtime_state["agent_reference_publication_filename"] = filename
            runtime_state["agent_enable_definition_preparation"] = True
            runtime_state["agent_definition_strategy"] = "reference_publication"
            runtime_state["agent_mui_status_message"] = {
                "severity": "success",
                "text": f"Reference publication '{filename}' loaded with {len(reference_text)} extracted character(s).",
            }
        except Exception as exc:
            runtime_state["agent_mui_status_message"] = {"severity": "error", "text": f"Failed to load reference publication: {exc}"}
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
            resume_previous=bool(event.get("resume_previous", False)),
        )
        should_rerun = True
    elif event_type == "stop_run":
        cancel_event = runtime_state.get(AGENT_RUN_CANCEL_EVENT_STATE_KEY)
        live_status = runtime_state.get(AGENT_RUN_STATUS_STATE_KEY, {})
        if isinstance(live_status, dict) and live_status.get("running") and hasattr(cancel_event, "set"):
            cancel_event.set()
            runtime_state[AGENT_RUN_STATUS_STATE_KEY] = {
                **live_status,
                "stop_requested": True,
                "message": "Stop requested; finishing the current in-flight term(s).",
                "last_activity": "Stop requested by user.",
            }
            runtime_state["agent_mui_status_message"] = {
                "severity": "warning",
                "text": "Stop requested. The run will pause after the current in-flight term(s).",
            }
        else:
            runtime_state["agent_mui_status_message"] = {
                "severity": "info",
                "text": "No agent-based reconciliation run is currently running.",
            }
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
