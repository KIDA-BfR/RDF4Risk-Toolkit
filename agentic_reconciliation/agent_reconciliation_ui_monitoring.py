# -*- coding: utf-8 -*-
"""Monitoring snapshots for the Material UI agent reconciliation backend."""

import pandas as pd

try:
    from .agent_runtime_state import runtime_state
except ImportError:
    from agent_runtime_state import runtime_state

AGENT_MONITORING_STATE_KEY = "agent_reconciliation_monitoring_state"


def _build_monitoring_event_snapshot(state) -> pd.DataFrame:
    rows = []
    for event in list(getattr(state, "term_events", []) or []):
        if not isinstance(event, dict):
            continue
        rows.append(
            {
                "Source": event.get("file", ""),
                "Term": event.get("term", ""),
                "Status": event.get("status", "processed"),
                "SKOS": event.get("mapping_type", ""),
                "Suggested URI": event.get("suggested_uri", ""),
                "Elapsed (ms)": event.get("elapsed_ms", None),
                "Error": event.get("error", ""),
                "Agentic Triggered": bool(event.get("trace_metadata", {}).get("agentic_triggered", False)) if isinstance(event.get("trace_metadata"), dict) else False,
            }
        )
    return pd.DataFrame(rows)


def _build_cascade_trace_snapshot(state) -> pd.DataFrame:
    rows = []
    for event in list(getattr(state, "term_events", []) or []):
        if not isinstance(event, dict):
            continue
        rows.append(
            {
                "Source": event.get("file", ""),
                "Term": event.get("term", ""),
                "Workflow": event.get("workflow", ""),
                "Decision Source": event.get("decision_source", ""),
                "Fallback Reason": event.get("fallback_reason", ""),
                "Fallback Error Type": event.get("fallback_error_type", ""),
                "Fallback Error Message": event.get("fallback_error_message", ""),
                "Fallback Payload Preview": event.get("fallback_payload_preview", ""),
                "Status": event.get("status", ""),
                "Suggested URI": event.get("suggested_uri", ""),
                "Elapsed (ms)": event.get("elapsed_ms", None),
                "Planner Calls Used": (event.get("trace_metadata", {}) or {}).get("planner_calls_used", 0) if isinstance(event.get("trace_metadata"), dict) else 0,
                "Tool Actions Used": (event.get("trace_metadata", {}) or {}).get("tool_actions_used", 0) if isinstance(event.get("trace_metadata"), dict) else 0,
                "LLM Calls Used": (event.get("trace_metadata", {}) or {}).get("total_llm_calls_used", 0) if isinstance(event.get("trace_metadata"), dict) else 0,
                "Best Confidence": (event.get("trace_metadata", {}) or {}).get("best_confidence", 0) if isinstance(event.get("trace_metadata"), dict) else 0,
                "Agentic Stop Reason": (event.get("trace_metadata", {}) or {}).get("agentic_stop_reason", "") if isinstance(event.get("trace_metadata"), dict) else "",
            }
        )
    return pd.DataFrame(rows)


def _render_monitoring_panel():
    return runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
