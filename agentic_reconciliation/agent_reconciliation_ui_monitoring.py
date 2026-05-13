# -*- coding: utf-8 -*-
"""Monitoring snapshots and LangSmith/local telemetry rendering for the UI."""

import pandas as pd
import streamlit as st

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
    monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
    if not isinstance(monitoring_state, dict) or not monitoring_state:
        return

    if not monitoring_state.get("enabled"):
        return

    with st.expander("Monitoring Results (LangSmith + local telemetry)", expanded=False):
        run_id = str(monitoring_state.get("run_id", "") or "").strip()
        started_at = str(monitoring_state.get("started_at", "") or "").strip()
        finished_at = str(monitoring_state.get("finished_at", "") or "").strip()
        duration_sec = monitoring_state.get("duration_sec", None)
        total_terms = int(monitoring_state.get("total_terms", 0) or 0)
        processed_terms = int(monitoring_state.get("processed_terms", 0) or 0)
        failed_terms = int(monitoring_state.get("failed_terms", 0) or 0)
        total_cost_usd = float(monitoring_state.get("total_cost_usd", 0.0) or 0.0)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Processed terms", processed_terms)
        c2.metric("Total terms", total_terms)
        c3.metric("Failures", failed_terms)
        c4.metric("Duration (s)", f"{float(duration_sec):.2f}" if isinstance(duration_sec, (int, float)) else "—")
        c5.metric("Total Cost (USD)", f"${total_cost_usd:.4f}")

        stop_reason = str(monitoring_state.get("stop_reason", "") or "").strip()
        stop_event = monitoring_state.get("stop_event", {}) if isinstance(monitoring_state.get("stop_event"), dict) else {}
        if stop_reason == "llm_error":
            stopped_term = str(stop_event.get("term", "") or "").strip() or "(unknown term)"
            err_type = str(stop_event.get("fallback_error_type", "") or "").strip()
            err_msg = str(stop_event.get("fallback_error_message", "") or "").strip()
            fix_tip = str(stop_event.get("llm_fix_suggestion", "") or "").strip()
            st.error(
                f"Run stopped automatically on LLM error at term '{stopped_term}'. "
                "Choose whether to fix and rerun or continue with heuristic fallback."
            )
            if err_type or err_msg:
                st.caption(f"Stop detail: {err_type}: {err_msg}".strip(": "))
            if fix_tip:
                st.caption(f"Suggested fix: {fix_tip}")

        if run_id:
            st.caption(f"Run ID: `{run_id}`")
        if started_at or finished_at:
            st.caption(f"Started: {started_at or '—'} • Finished: {finished_at or '—'}")

        langsmith = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
        ls_message = str(langsmith.get("message", "") or "").strip()
        ls_project_url = str(langsmith.get("project_url", "") or "").strip()
        ls_run_url = str(langsmith.get("run_url", "") or "").strip()
        ls_ready = bool(langsmith.get("ready", False))

        if ls_message:
            if ls_ready:
                st.success(ls_message)
            else:
                st.info(ls_message)
        if ls_project_url:
            st.markdown(f"LangSmith project: [{ls_project_url}]({ls_project_url})")
        if ls_run_url:
            st.markdown(f"LangSmith run: [{ls_run_url}]({ls_run_url})")

        events_df = monitoring_state.get("events_df")
        if isinstance(events_df, pd.DataFrame) and not events_df.empty:
            st.markdown("##### Term-level monitoring snapshot")
            st.dataframe(events_df.tail(200), use_container_width=True)
        else:
            st.caption("No term-level monitoring events are available for this run.")

        llm_interactions_df = monitoring_state.get("llm_interactions_df")
        if isinstance(llm_interactions_df, pd.DataFrame) and not llm_interactions_df.empty:
            st.markdown("##### LLM prompt/response snapshot")
            st.dataframe(llm_interactions_df.tail(200), use_container_width=True)
        else:
            st.caption("No LLM prompt/response interactions captured for this run yet.")

        cascade_trace_df = monitoring_state.get("cascade_trace_df")
        if isinstance(cascade_trace_df, pd.DataFrame) and not cascade_trace_df.empty:
            st.markdown("##### Cascade trace snapshot")
            llm_terms = int((cascade_trace_df.get("Decision Source", pd.Series(dtype=str)) == "llm").sum())
            fallback_terms = int((cascade_trace_df.get("Decision Source", pd.Series(dtype=str)) != "llm").sum())
            sc1, sc2 = st.columns(2)
            sc1.metric("Terms classified via LLM", llm_terms)
            sc2.metric("Terms using fallback/no-skos", fallback_terms)

            planner_calls_avg = float(cascade_trace_df.get("Planner Calls Used", pd.Series(dtype=float)).fillna(0).mean()) if "Planner Calls Used" in cascade_trace_df.columns else 0.0
            llm_calls_avg = float(cascade_trace_df.get("LLM Calls Used", pd.Series(dtype=float)).fillna(0).mean()) if "LLM Calls Used" in cascade_trace_df.columns else 0.0
            agentic_triggered_count = int(
                (cascade_trace_df.get("Agentic Stop Reason", pd.Series(dtype=str)).astype(str).str.strip() != "").sum()
            ) if "Agentic Stop Reason" in cascade_trace_df.columns else 0
            budget_exhaust_count = int(
                cascade_trace_df.get("Agentic Stop Reason", pd.Series(dtype=str)).astype(str).str.contains("budget", case=False, na=False).sum()
            ) if "Agentic Stop Reason" in cascade_trace_df.columns else 0

            sc3, sc4 = st.columns(2)
            sc3.metric("Avg planner calls / term", f"{planner_calls_avg:.2f}")
            sc4.metric("Avg total LLM calls / term", f"{llm_calls_avg:.2f}")

            sc5, sc6 = st.columns(2)
            sc5.metric("Terms with agentic refinement", agentic_triggered_count)
            sc6.metric("Budget exhaustion count", budget_exhaust_count)

            raw_events = monitoring_state.get("raw_term_events", [])
            if isinstance(raw_events, list) and raw_events:
                st.markdown("##### Per-term cascade timeline")
                for event in raw_events[-30:]:
                    if not isinstance(event, dict):
                        continue
                    term = str(event.get("term", "")).strip() or "(unknown term)"
                    decision_source = str(event.get("decision_source", "") or "n/a")
                    status = str(event.get("status", "") or "n/a")
                    with st.expander(f"{term} — {status} — {decision_source}", expanded=False):
                        steps = event.get("cascade_steps", []) if isinstance(event.get("cascade_steps", []), list) else []
                        if not steps:
                            st.caption("No cascade steps recorded.")
                        for step in steps:
                            if not isinstance(step, dict):
                                continue
                            step_no = step.get("step", "?")
                            label = step.get("label", "")
                            step_status = str(step.get("status", "") or "").lower()
                            prefix = "✅" if step_status == "ok" else ("⚠️" if step_status == "fallback" else "❌")
                            if str(label or "").strip():
                                st.markdown(f"{prefix} **Step {step_no}:** {label}")

                        if event.get("fallback_reason") == "llm_error":
                            err_type = str(event.get("fallback_error_type", "") or "").strip()
                            err_msg = str(event.get("fallback_error_message", "") or "").strip()
                            payload_preview = str(event.get("fallback_payload_preview", "") or "").strip()
                            if err_type or err_msg:
                                st.error(f"LLM error detail: {err_type}: {err_msg}".strip(": "))
                            if payload_preview:
                                st.caption("Payload preview at failure point (truncated):")
                                st.code(payload_preview)
        else:
            st.caption("No cascade trace interactions captured for this run yet.")
