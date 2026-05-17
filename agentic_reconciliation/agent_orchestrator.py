# -*- coding: utf-8 -*-
"""Orchestration logic for agent-based reconciliation workflows."""

from __future__ import annotations

import concurrent.futures
import json
import time
import uuid
from dataclasses import asdict, replace
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd

from .agent_bioportal_service import (
    find_best_definition,
    find_term_in_ontology,
    find_term_in_ontology_with_definition,
    recommend_ontology_acronyms,
    search_bioportal_candidates,
)
from .agent_llm_service import generate_structured_completion
from . import agent_orchestrator_workflows as _workflow_impl
from .agent_orchestrator_runtime import (
    _WorkflowAdmissionController,
    _chunked,
    _coerce_positive_int,
    _is_valid_qid,
    _resolve_model_api_key_env,
    _resolve_planner_api_key_env,
    _resolve_planner_model,
    _resolve_planner_provider,
    run_with_timeout,
)
from .agent_candidate_scoring import (
    VERIFIED_THRESHOLDS,
    _apply_provider_signal_boost,
    _build_no_match_decision,
    _compute_auto_acceptance_score,
    _evaluate_auto_accept,
    _finalize_best_candidate,
    _mapping_priority,
    _merge_candidate_trace_metadata,
    _normalize_mapping_type,
    _normalize_provider_token,
    _provider_is_trusted,
    _safe_confidence,
    _score_meets_suggestion_policy,
    _score_meets_verified_policy,
    _semantic_justification_for_decision,
    _string_similarity,
    _token_overlap_ratio,
)
from .agent_models import (
    AgentCandidate,
    AgentDecision,
    AgentInputTable,
    AgentRunConfig,
    AgenticExecutionStats,
    AgenticPlan,
    AgenticPlanAction,
    BatchRunState,
    CandidateScore,
    SKOSDecision,
)
from .agent_skos_service import classify_skos_match, normalize_mapping_type
from .agent_wikidata_service import (
    WikidataEntityDetails,
    WikidataRateLimitError,
    dedupe_candidates,
    load_candidate_by_qid,
    search_wikidata_candidates,
    search_wikidata_candidates_multiquery,
    search_wikidata_candidates_with_options,
)
from semi_automatic_reconciliation.shared_table_io import (
    SEMANTIC_MAPPING_JUSTIFICATION,
    apply_mapping_justification_for_row,
    ensure_agent_output_columns,
    get_unreconciled_indices,
)


MAX_BATCH_WORKERS = 16
_WORKFLOW_RUN_WIKIDATA_IMPL = _workflow_impl.run_wikidata_deep_agent


def _sync_workflow_dependencies() -> None:
    for name in (
        "WikidataEntityDetails",
        "WikidataRateLimitError",
        "classify_skos_match",
        "dedupe_candidates",
        "find_best_definition",
        "find_term_in_ontology",
        "find_term_in_ontology_with_definition",
        "generate_structured_completion",
        "load_candidate_by_qid",
        "normalize_mapping_type",
        "recommend_ontology_acronyms",
        "search_bioportal_candidates",
        "search_wikidata_candidates",
        "search_wikidata_candidates_multiquery",
        "search_wikidata_candidates_with_options",
    ):
        setattr(_workflow_impl, name, globals()[name])
    patched_wikidata = globals()["run_wikidata_deep_agent"]
    _workflow_impl.run_wikidata_deep_agent = (
        patched_wikidata
        if patched_wikidata is not _ORCHESTRATOR_RUN_WIKIDATA_WRAPPER
        else _WORKFLOW_RUN_WIKIDATA_IMPL
    )


def _score_candidate(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._score_candidate(*args, **kwargs)


def _merge_and_trim_candidate_pool(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._merge_and_trim_candidate_pool(*args, **kwargs)


def _build_baseline_candidate_pool(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._build_baseline_candidate_pool(*args, **kwargs)


def _should_trigger_agentic_refinement(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._should_trigger_agentic_refinement(*args, **kwargs)


def _generate_agentic_plan(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._generate_agentic_plan(*args, **kwargs)


def _execute_agentic_plan_actions(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._execute_agentic_plan_actions(*args, **kwargs)


def _derive_llm_error_fix_suggestion(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._derive_llm_error_fix_suggestion(*args, **kwargs)


def _candidate_to_decision(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._candidate_to_decision(*args, **kwargs)


def _build_notebook_faithful_multiagent_config(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl._build_notebook_faithful_multiagent_config(*args, **kwargs)


def run_wikidata_deep_agent(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl.run_wikidata_deep_agent(*args, **kwargs)


def run_bioportal_wikidata_multiagent(*args, **kwargs):
    _sync_workflow_dependencies()
    return _workflow_impl.run_bioportal_wikidata_multiagent(*args, **kwargs)


_ORCHESTRATOR_RUN_WIKIDATA_WRAPPER = run_wikidata_deep_agent


def apply_agent_decision_to_dataframe(df: pd.DataFrame, row_index, decision: AgentDecision, config: AgentRunConfig) -> pd.DataFrame:
    df_out = df.copy()
    df_out.at[row_index, "Definition"] = decision.definition
    df_out.at[row_index, "Agent Explanation"] = decision.explanation
    df_out.at[row_index, "Agent Workflow"] = config.workflow
    df_out.at[row_index, "Run ID"] = decision.run_id
    df_out.at[row_index, "Agent Decision Status"] = decision.status
    try:
        df_out.at[row_index, "Agent Trace Metadata"] = json.dumps(getattr(decision, "trace_metadata", {}) or {}, ensure_ascii=False)
    except Exception:
        df_out.at[row_index, "Agent Trace Metadata"] = "{}"

    if decision.candidate is None:
        df_out.at[row_index, "Suggested URI"] = ""
        df_out.at[row_index, "Suggested Provider"] = ""
        df_out.at[row_index, "Suggested Label"] = ""
        df_out.at[row_index, "Suggested Description"] = ""
        df_out.at[row_index, "Suggested Match Type"] = ""
        df_out.at[row_index, "Suggested Confidence"] = 0.0
        df_out.at[row_index, "Suggested Decision Source"] = ""
        df_out.at[row_index, "Suggested Fallback Reason"] = ""
        df_out.at[row_index, "Auto Accepted"] = False
        df_out.at[row_index, "Auto Acceptance Score"] = 0.0
        df_out.at[row_index, "Auto Accept Reason"] = "no_candidate"
        df_out.at[row_index, "Auto Accepted At"] = ""
        apply_mapping_justification_for_row(
            df_out,
            row_index,
            default_when_mapped=SEMANTIC_MAPPING_JUSTIFICATION,
            no_match_uri="No Match",
            force_when_mapped=False,
        )
        df_out.at[row_index, "Review Status"] = decision.status
        return df_out

    suggested_confidence = _safe_confidence(getattr(decision.skos, "confidence", None), default=0.0) if decision.skos else 0.0
    suggested_decision_source = str(getattr(decision.skos, "decision_source", "") or "") if decision.skos else ""
    suggested_fallback_reason = str(getattr(decision.skos, "fallback_reason", "") or "") if decision.skos else ""

    df_out.at[row_index, "Suggested URI"] = decision.candidate.uri
    df_out.at[row_index, "Suggested Provider"] = decision.candidate.source_provider
    df_out.at[row_index, "Suggested Label"] = decision.candidate.label
    df_out.at[row_index, "Suggested Description"] = decision.candidate.description
    df_out.at[row_index, "Suggested Match Type"] = normalize_mapping_type(decision.skos.mapping_type if decision.skos else "")
    df_out.at[row_index, "Suggested Confidence"] = round(float(suggested_confidence), 4)
    if decision.skos and decision.skos.llm_confidence is not None:
        df_out.at[row_index, "Suggested LLM Confidence"] = round(float(decision.skos.llm_confidence), 4)
    else:
        df_out.at[row_index, "Suggested LLM Confidence"] = ""
    df_out.at[row_index, "Suggested Decision Source"] = suggested_decision_source
    df_out.at[row_index, "Suggested Fallback Reason"] = suggested_fallback_reason
    auto_accepted, auto_score, auto_reason = _evaluate_auto_accept(term=decision.term, decision=decision, config=config)
    df_out.at[row_index, "Auto Accepted"] = bool(auto_accepted)
    df_out.at[row_index, "Auto Acceptance Score"] = round(float(auto_score), 4)
    df_out.at[row_index, "Auto Accept Reason"] = auto_reason
    df_out.at[row_index, "Auto Accepted At"] = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) if auto_accepted else ""
    )
    df_out.at[row_index, "Review Status"] = "pending"

    should_apply_automatically = bool(config.auto_apply_on_accept or auto_accepted)
    if should_apply_automatically:
        df_out.at[row_index, "URI"] = decision.candidate.uri
        df_out.at[row_index, "object_id"] = decision.candidate.uri
        df_out.at[row_index, "Source Provider"] = decision.candidate.source_provider
        df_out.at[row_index, "source_provider"] = decision.candidate.source_provider
        df_out.at[row_index, "Provider Term"] = decision.candidate.label
        df_out.at[row_index, "provider_term"] = decision.candidate.label
        df_out.at[row_index, "Provider Description"] = decision.candidate.description
        df_out.at[row_index, "provider_description"] = decision.candidate.description
        df_out.at[row_index, "Confirmed Display String"] = decision.candidate.label
        df_out.at[row_index, "confirmed_display_string"] = decision.candidate.label
        df_out.at[row_index, "object_label"] = decision.candidate.label
        df_out.at[row_index, "Match Type"] = normalize_mapping_type(decision.skos.mapping_type if decision.skos else "")
        df_out.at[row_index, "match_type"] = normalize_mapping_type(decision.skos.mapping_type if decision.skos else "")
        df_out.at[row_index, "predicate_id"] = normalize_mapping_type(decision.skos.mapping_type if decision.skos else "")
        df_out.at[row_index, "comment"] = decision.candidate.description
        df_out.at[row_index, "Review Status"] = "accepted"
        if not auto_accepted and config.auto_apply_on_accept:
            df_out.at[row_index, "Auto Accepted"] = False
            df_out.at[row_index, "Auto Acceptance Score"] = round(float(auto_score), 4)
            df_out.at[row_index, "Auto Accept Reason"] = "legacy_auto_apply_on_accept_enabled"
            df_out.at[row_index, "Auto Accepted At"] = ""

    apply_mapping_justification_for_row(
        df_out,
        row_index,
        default_when_mapped=_semantic_justification_for_decision(decision, config),
        no_match_uri="No Match",
        force_when_mapped=should_apply_automatically,
    )
    return df_out


def run_agent_batch_on_dataframe(
    df: pd.DataFrame,
    config: AgentRunConfig,
    definitions_lookup: Optional[Dict[str, str]] = None,
    bioportal_api_key: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int, str, Optional[Dict[str, Any]]], None]] = None,
    source_name: str = "input",
    resume_skip_processed_terms: bool = False,
    stop_requested_callback: Optional[Callable[[], bool]] = None,
) -> pd.DataFrame:
    definitions_lookup = definitions_lookup or {}
    df_out = ensure_agent_output_columns(df)
    indices = get_unreconciled_indices(df_out, "No Match")
    if resume_skip_processed_terms and "Run ID" in df_out.columns:
        processed_mask = df_out["Run ID"].astype(str).str.strip().ne("")
        indices = [idx for idx in indices if not bool(processed_mask.loc[idx])]

    first_pass_results: List[Dict[str, Any]] = []
    stopped_due_to_llm_error = False
    max_workers = _coerce_positive_int(getattr(config, "max_workers", 1), 1, upper_bound=MAX_BATCH_WORKERS)
    batch_size = _coerce_positive_int(getattr(config, "batch_size", max_workers), max_workers)
    effective_workers = min(max_workers, batch_size, max(1, len(indices)))
    admission_controller = _WorkflowAdmissionController(
        getattr(config, "parallel_start_interval_seconds", 0.25)
    )

    def _run_term_decision(local_term: str, local_definition: str, *, related_retry: bool = False) -> AgentDecision:
        if config.workflow == "bioportal_wikidata_multiagent":
            return run_bioportal_wikidata_multiagent(
                local_term,
                local_definition,
                config,
                bioportal_api_key=bioportal_api_key,
                source_name=source_name,
                related_wikidata_bias=bool(related_retry),
            )

        if related_retry:
            return run_wikidata_deep_agent(
                local_term,
                local_definition,
                config,
                source_name=source_name,
                search_profile="focus_related",
            )

        return run_wikidata_deep_agent(
            local_term,
            local_definition,
            config,
            source_name=source_name,
        )

    def _build_progress_event(
        *,
        term: str,
        decision: AgentDecision,
        elapsed_ms: float,
    ) -> Dict[str, Any]:
        skos = decision.skos
        decision_source = getattr(skos, "decision_source", "heuristic_fallback") if skos else "no_skos"
        fallback_reason = getattr(skos, "fallback_reason", None) if skos else None
        fallback_error_type = getattr(skos, "fallback_error_type", None) if skos else None
        fallback_error_message = getattr(skos, "fallback_error_message", None) if skos else None
        fallback_payload_preview = getattr(skos, "fallback_payload_preview", None) if skos else None
        llm_fix_suggestion = ""
        llm_error_stop = False
        if fallback_reason == "llm_error":
            llm_fix_suggestion = _derive_llm_error_fix_suggestion(
                config=config,
                fallback_error_type=fallback_error_type,
                fallback_error_message=fallback_error_message,
            )
            llm_error_stop = bool(getattr(config, "stop_on_llm_error", True))
        trace_metadata = getattr(decision, "trace_metadata", {}) or {}
        runtime_failure = bool(
            str(decision.status or "").strip().lower() in {"timeout", "error", "failed"}
            or str(fallback_reason or "").strip().lower()
            in {"llm_error", "missing_api_key", "codex_not_authenticated", "wikidata_lookup_error"}
            or (isinstance(trace_metadata, dict) and bool(trace_metadata.get("wikidata_fallback_unavailable")))
        )
        event_status = "error" if runtime_failure else str(decision.status or "processed").strip().lower()
        cascade_steps = [
            {"step": 1, "label": "Term accepted for processing", "status": "ok"},
            {"step": 2, "label": f"Workflow selected: {config.workflow}", "status": "ok"},
            {
                "step": 3,
                "label": (
                    f"Candidate retrieval attempted; metadata: {json.dumps(getattr(decision, 'trace_metadata', {}), ensure_ascii=False)}"
                ),
                "status": "ok",
            },
            {
                "step": 4,
                "label": f"SKOS decision source: {decision_source}",
                "status": "fallback" if decision_source != "llm" else "ok",
            },
            {
                "step": 5,
                "label": (
                    f"Fallback reason: {fallback_reason}" if fallback_reason else "No fallback required"
                ),
                "status": "fallback" if fallback_reason else "ok",
            },
            {
                "step": 5.1,
                "label": (
                    f"LLM error detail: {fallback_error_type or ''}: {fallback_error_message or ''}".strip()
                    if fallback_reason == "llm_error"
                    else ""
                ),
                "status": "fallback" if fallback_reason == "llm_error" else "ok",
            },
            {
                "step": 5.2,
                "label": (
                    f"Suggested fix: {llm_fix_suggestion}" if llm_fix_suggestion else ""
                ),
                "status": "error" if llm_error_stop else ("fallback" if fallback_reason == "llm_error" else "ok"),
            },
            {
                "step": 6,
                "label": f"Final status={decision.status}; suggested_uri={getattr(decision.candidate, 'uri', '') if decision.candidate else ''}",
                "status": "error" if runtime_failure else "ok",
            },
        ]
        return {
            "file": source_name,
            "term": term,
            "status": event_status,
            "decision_status": decision.status,
            "mapping_type": getattr(skos, "mapping_type", "") if skos else "",
            "suggested_uri": decision.candidate.uri if decision.candidate else "",
            "elapsed_ms": elapsed_ms,
            "error": decision.explanation if runtime_failure else "",
            "workflow": config.workflow,
            "decision_source": decision_source,
            "fallback_reason": fallback_reason,
            "fallback_error_type": fallback_error_type,
            "fallback_error_message": fallback_error_message,
            "fallback_payload_preview": fallback_payload_preview,
            "llm_fix_suggestion": llm_fix_suggestion,
            "llm_error_stop": llm_error_stop,
            "trace_metadata": getattr(decision, "trace_metadata", {}),
            "cascade_steps": cascade_steps,
            "parallel": {
                "enabled": effective_workers > 1,
                "max_workers": effective_workers,
                "batch_size": batch_size,
                "start_interval_seconds": admission_controller.min_interval_seconds,
            },
        }

    def _process_row(row_index) -> Dict[str, Any]:
        term_started = time.perf_counter()
        term = str(df_out.at[row_index, "Term"]).strip()
        definition = str(definitions_lookup.get(term, df_out.at[row_index, "Definition"])).strip()
        if not term or (stop_requested_callback and stop_requested_callback()):
            return {
                "row_index": row_index,
                "term": term,
                "definition": definition,
                "decision": None,
                "elapsed_ms": 0.0,
                "skip": True,
            }

        admission_controller.wait_for_turn()
        if stop_requested_callback and stop_requested_callback():
            return {
                "row_index": row_index,
                "term": term,
                "definition": definition,
                "decision": None,
                "elapsed_ms": round((time.perf_counter() - term_started) * 1000.0, 2),
                "skip": True,
            }
        decision = _run_term_decision(term, definition, related_retry=False)
        elapsed_ms = round((time.perf_counter() - term_started) * 1000.0, 2)
        return {
            "row_index": row_index,
            "term": term,
            "definition": definition,
            "decision": decision,
            "elapsed_ms": elapsed_ms,
            "skip": False,
        }

    def _apply_processed_result(result: Dict[str, Any], position: int) -> bool:
        nonlocal df_out
        if bool(result.get("skip")):
            return False
        row_index = result["row_index"]
        term = result["term"]
        decision = result["decision"]
        if decision is None:
            return False

        first_pass_results.append(result)
        df_out = apply_agent_decision_to_dataframe(df_out, row_index, decision, config)
        progress_event = _build_progress_event(
            term=term,
            decision=decision,
            elapsed_ms=float(result.get("elapsed_ms", 0.0)),
        )

        # Report progress after each term has actually been processed so the
        # UI progress bar reflects completed work (not just queued work).
        if progress_callback:
            progress_callback(position, len(indices), term, progress_event)
        return bool(progress_event.get("llm_error_stop"))

    completed_positions = 0
    if effective_workers <= 1 or len(indices) <= 1:
        for row_index in indices:
            if stop_requested_callback and stop_requested_callback():
                break
            completed_positions += 1
            if _apply_processed_result(_process_row(row_index), completed_positions):
                stopped_due_to_llm_error = True
                break
            if stop_requested_callback and stop_requested_callback():
                break
    else:
        next_index_position = 0
        accepting_new_work = True

        def _submit_next(executor, future_to_row: Dict[concurrent.futures.Future, object]) -> bool:
            nonlocal next_index_position
            if next_index_position >= len(indices):
                return False
            if stop_requested_callback and stop_requested_callback():
                return False
            row_index = indices[next_index_position]
            next_index_position += 1
            future_to_row[executor.submit(_process_row, row_index)] = row_index
            return True

        with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_row: Dict[concurrent.futures.Future, object] = {}
            while len(future_to_row) < effective_workers and _submit_next(executor, future_to_row):
                pass

            while future_to_row:
                done, _pending = concurrent.futures.wait(
                    future_to_row,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    future_to_row.pop(future, None)
                    completed_positions += 1
                    result = future.result()
                    if _apply_processed_result(result, completed_positions):
                        stopped_due_to_llm_error = True

                if stopped_due_to_llm_error or (stop_requested_callback and stop_requested_callback()):
                    accepting_new_work = False
                    for future in list(future_to_row):
                        if future.cancel():
                            future_to_row.pop(future, None)

                while accepting_new_work and len(future_to_row) < effective_workers:
                    if not _submit_next(executor, future_to_row):
                        break

    if (
        config.workflow == "bioportal_wikidata_multiagent"
        and bool(getattr(config, "enable_second_pass_related_retry", False))
        and not stopped_due_to_llm_error
        and not (stop_requested_callback and stop_requested_callback())
    ):
        retry_items = [
            item
            for item in first_pass_results
            if getattr(item.get("decision"), "candidate", None) is None
            and str(getattr(item.get("decision"), "status", "")).strip().lower() in {"no_match", "timeout"}
        ]
        for item in retry_items:
            row_index = item["row_index"]
            term = item["term"]
            definition = item["definition"]
            retry_decision = _run_term_decision(term, definition, related_retry=True)
            retry_trace = dict(getattr(retry_decision, "trace_metadata", {}) or {})
            retry_trace["second_pass_related_retry"] = True
            retry_trace["second_pass_trigger"] = "initial_no_match"
            retry_decision.trace_metadata = retry_trace
            df_out = apply_agent_decision_to_dataframe(df_out, row_index, retry_decision, config)

    return df_out


def run_agent_batch(
    input_tables: Iterable[AgentInputTable],
    config: AgentRunConfig,
    definitions_by_source: Optional[Dict[str, Dict[str, str]]] = None,
    bioportal_api_key: Optional[str] = None,
    progress_callback: Optional[Callable[[BatchRunState], None]] = None,
    resume_skip_processed_terms: bool = False,
    stop_requested_callback: Optional[Callable[[], bool]] = None,
) -> Dict[str, pd.DataFrame]:
    outputs: Dict[str, pd.DataFrame] = {}
    tables = list(input_tables)
    state = BatchRunState(run_id=str(uuid.uuid4()), total_files=len(tables), status="running")

    def _event_counts_as_failure(event: Optional[Dict[str, Any]]) -> bool:
        """Return True only for real processing failures.

        We intentionally do *not* treat normal terminal outcomes such as
        "no_match" / "candidate_suggested" as failures. Those outcomes can be
        expected and are meant for curator review, not runtime failure metrics.
        """
        if not isinstance(event, dict):
            return False
        status = str(event.get("status", "") or "").strip().lower()
        return status in {"timeout", "error", "failed"}

    for table_index, table in enumerate(tables, start=1):
        if stop_requested_callback and stop_requested_callback():
            state.stop_reason = "user_stopped"
            state.stop_event = {
                "stop_reason": "user_stopped",
                "file": table.source_name,
                "processed_terms": state.processed_terms,
                "total_terms": state.total_terms,
            }
            state.status = "stopped_user"
            state.messages.append("Run stopped by user before the next term started.")
            break
        definitions_lookup = (definitions_by_source or {}).get(table.source_name, {})
        unreconciled = get_unreconciled_indices(table.dataframe, "No Match")
        if resume_skip_processed_terms and "Run ID" in table.dataframe.columns:
            processed_mask = table.dataframe["Run ID"].astype(str).str.strip().ne("")
            unreconciled = [idx for idx in unreconciled if not bool(processed_mask.loc[idx])]
        state.total_terms += len(unreconciled)

        def _progress(current: int, total: int, term: str, event: Optional[Dict[str, Any]] = None):
            state.processed_terms += 1
            if _event_counts_as_failure(event):
                state.failed_terms += 1
            state.messages.append(f"{table.source_name}: processed term '{term}' ({current}/{total})")
            state.term_events.append(
                {
                    "file": table.source_name,
                    "term": term,
                    "progress_current": current,
                    "progress_total": total,
                    **(event or {}),
                }
            )

            if not state.stop_reason and isinstance(event, dict) and bool(event.get("llm_error_stop")):
                state.stop_reason = "llm_error"
                state.stop_event = {
                    "file": table.source_name,
                    "term": term,
                    "fallback_error_type": event.get("fallback_error_type"),
                    "fallback_error_message": event.get("fallback_error_message"),
                    "fallback_reason": event.get("fallback_reason"),
                    "decision_source": event.get("decision_source"),
                    "llm_fix_suggestion": event.get("llm_fix_suggestion"),
                    "workflow": event.get("workflow"),
                    "provider": config.model_provider,
                    "model_name": config.model_name,
                }
                state.status = "stopped_llm_error"
                err_type = str(event.get("fallback_error_type", "") or "").strip()
                err_msg = str(event.get("fallback_error_message", "") or "").strip()
                state.messages.append(
                    f"{table.source_name}: pipeline stopped at term '{term}' due to LLM error ({err_type}: {err_msg}).".strip(" :")
                )
                suggested_fix = str(event.get("llm_fix_suggestion", "") or "").strip()
                if suggested_fix:
                    state.messages.append(f"Suggested fix: {suggested_fix}")

            if progress_callback:
                progress_callback(state)

        outputs[table.source_name] = run_agent_batch_on_dataframe(
            table.dataframe,
            config,
            definitions_lookup=definitions_lookup,
            bioportal_api_key=bioportal_api_key,
            progress_callback=_progress,
            source_name=table.source_name,
            resume_skip_processed_terms=resume_skip_processed_terms,
            stop_requested_callback=stop_requested_callback,
        )

        if state.stop_reason == "llm_error":
            break
        if stop_requested_callback and stop_requested_callback():
            last_event = state.term_events[-1] if state.term_events else {}
            state.stop_reason = "user_stopped"
            state.stop_event = {
                "stop_reason": "user_stopped",
                "file": table.source_name,
                "term": last_event.get("term"),
                "processed_terms": state.processed_terms,
                "total_terms": state.total_terms,
            }
            state.status = "stopped_user"
            state.messages.append(
                f"{table.source_name}: run stopped by user after {state.processed_terms}/{state.total_terms} term(s)."
            )
            if progress_callback:
                progress_callback(state)
            break

        state.completed_files += 1
        if progress_callback:
            progress_callback(state)

    if state.stop_reason == "llm_error":
        state.status = "stopped_llm_error"
    elif state.stop_reason == "user_stopped":
        state.status = "stopped_user"
    else:
        state.status = "completed"
    if progress_callback:
        progress_callback(state)
    return outputs
