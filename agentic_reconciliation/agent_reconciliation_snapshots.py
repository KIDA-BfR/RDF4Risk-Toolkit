"""Snapshot builders for the agent reconciliation Material UI backend."""

from __future__ import annotations

import json
from typing import Dict, List, Optional

import pandas as pd

try:
    from .agent_langsmith_monitoring import get_llm_interactions
    from .agent_reconciliation_keys import (
        AGENT_DATAFRAME_STATE_KEY,
        AGENT_INPUT_TABLES_KEY,
        AGENT_MONITORING_STATE_KEY,
        AGENT_RESULTS_BY_SOURCE_KEY,
        AGENT_RUN_MESSAGES_KEY,
        AGENT_RUN_STATUS_STATE_KEY,
        AGENT_SELECTED_SOURCE_KEY,
        AGENT_STOP_EVENT_KEY,
    )
    from .agent_reconciliation_ui_review import _get_review_cell_value, _get_reviewable_agent_result_indices
    from .agent_runtime_state import runtime_state
    from .agent_skos_service import normalize_mapping_type
    from semi_automatic_reconciliation.snapshot_utils import dataframe_records as _dataframe_records
    from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS
except ImportError:  # pragma: no cover
    from agent_langsmith_monitoring import get_llm_interactions
    from agent_reconciliation_keys import (
        AGENT_DATAFRAME_STATE_KEY,
        AGENT_INPUT_TABLES_KEY,
        AGENT_MONITORING_STATE_KEY,
        AGENT_RESULTS_BY_SOURCE_KEY,
        AGENT_RUN_MESSAGES_KEY,
        AGENT_RUN_STATUS_STATE_KEY,
        AGENT_SELECTED_SOURCE_KEY,
        AGENT_STOP_EVENT_KEY,
    )
    from agent_reconciliation_ui_review import _get_review_cell_value, _get_reviewable_agent_result_indices
    from agent_runtime_state import runtime_state
    from agent_skos_service import normalize_mapping_type
    from semi_automatic_reconciliation.snapshot_utils import dataframe_records as _dataframe_records
    from semi_automatic_reconciliation.shared_table_io import LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS


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
    stop_event = runtime_state.get(AGENT_STOP_EVENT_KEY, {})
    if not isinstance(stop_event, dict):
        stop_event = {}
    stop_reason = str(
        live_status.get("stop_reason")
        or (monitoring_state.get("stop_reason") if isinstance(monitoring_state, dict) else "")
        or stop_event.get("stop_reason")
        or ""
    ).strip()
    stopped = bool(live_status.get("stopped", False) or stop_reason == "user_stopped")
    error = None
    if stop_reason and stop_reason != "user_stopped":
        error = stop_reason
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
        "stopped": stopped,
        "stop_requested": bool(live_status.get("stop_requested", False)),
        "stop_reason": stop_reason or None,
        "stop_event": stop_event,
        "can_resume": bool(stopped and total and processed < total),
        "can_restart": bool(stopped or results or error),
        "finished": bool(results) and not stopped and not error,
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
