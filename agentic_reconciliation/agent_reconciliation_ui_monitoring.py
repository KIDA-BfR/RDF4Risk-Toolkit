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
        trace_metadata = event.get("trace_metadata", {}) if isinstance(event.get("trace_metadata"), dict) else {}
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
                "Provider Escalation": bool(trace_metadata.get("provider_escalation_used", False)),
                "Escalation From": trace_metadata.get("provider_escalation_from", ""),
                "Escalation To": trace_metadata.get("provider_escalation_to", ""),
                "Escalation Reason": trace_metadata.get("provider_escalation_reason", ""),
                "Wikidata Second Pass Status": trace_metadata.get("wikidata_second_pass_status", ""),
                "Wikidata Candidate Found": bool(trace_metadata.get("wikidata_second_pass_has_candidate", False)),
                "Review Mode": trace_metadata.get("candidate_review_mode", ""),
                "Planner Calls Used": trace_metadata.get("planner_calls_used", 0),
                "Tool Actions Used": trace_metadata.get("tool_actions_used", 0),
                "LLM Calls Used": trace_metadata.get("total_llm_calls_used", 0),
                "Best Confidence": trace_metadata.get("best_confidence", 0),
                "Agentic Stop Reason": trace_metadata.get("agentic_stop_reason", ""),
            }
        )
    return pd.DataFrame(rows)


def _render_monitoring_panel():
    return runtime_state.get(AGENT_MONITORING_STATE_KEY, {})
