# -*- coding: utf-8 -*-
"""Workflow-specific agent reconciliation implementations."""

from __future__ import annotations

import time
import uuid
import copy
import hashlib
import json
import re
from dataclasses import replace
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .agent_bioportal_service import (
    find_best_definition,
    find_term_in_ontology_with_definition,
    recommend_ontology_acronyms,
    search_bioportal_candidates,
)
from .agent_bioportal_annotator_rescue import (
    AnnotatorRescueVariant,
    AnnotatorRescueResult,
    parse_annotator_variant_payload,
    run_bioportal_annotator_rescue,
    should_run_annotator_rescue,
)
from .agent_bioportal_context_service import (
    BioPortalOntologyRegistry,
    REGISTRY_PATH,
    enrich_bioportal_candidate_context,
    get_ontology_context,
    get_ontology_contexts,
    summarize_candidate_context,
)
from .agent_execution_trace import (
    ExecutionTrace,
    TraceApiCallSummary,
    TraceLlmCall,
    explain_verified_gate,
    export_trace_artifacts,
    make_trace_call_id,
    summarize_candidate_context as trace_candidate_context_summary,
    summarize_candidate_score,
    trace_level_at_least,
)
from .agent_ontology_routing import route_ontologies
from .agent_table_context_service import (
    build_term_category_profile as _build_term_category_profile,
    compute_ontology_suitability_score as _compute_ontology_suitability_score,
    term_type_core_ontologies as _term_type_core_ontologies,
    resolve_namespace_term as _resolve_namespace_term,
)
from .agent_rescue_adjudication import (
    should_run_rescue as _should_run_rescue,
    derive_abbreviation_expansions as _derive_abbreviation_expansions,
    derive_definition_queries as _derive_definition_queries,
    rescue_three_way_decision as _rescue_three_way_decision,
    candidate_evidence_summary as _candidate_evidence_summary,
    hard_reject_reason as _hard_reject_reason,
    is_plausible_suggestion as _is_plausible_suggestion,
    select_suggestion_candidate as _select_suggestion_candidate,
    attach_hierarchy_assessment as _attach_hierarchy_assessment,
)
from .agent_llm_service import generate_structured_completion as _default_generate_structured_completion
from . import agent_orchestrator_agentic_planning as _agentic_planning
from .agent_candidate_scoring import (
    VERIFIED_THRESHOLDS,
    _apply_provider_signal_boost,
    _build_no_match_decision,
    _finalize_best_candidate,
    _mapping_priority,
    _meets_strong_exact_identity,
    _merge_candidate_trace_metadata,
    _normalize_mapping_type,
    _provider_is_trusted,
    _safe_confidence,
    _score_meets_suggestion_policy,
    _score_meets_verified_policy,
    broad_registry_candidate_triage,
)
from .agent_models import (
    AgentCandidate,
    AgentDecision,
    AgentRunConfig,
    AgenticExecutionStats,
    CandidateScore,
    SKOSDecision,
)
from .agent_orchestrator_runtime import (
    _resolve_model_api_key_env,
    run_with_timeout,
)
from .agent_skos_service import classify_skos_match
from .agent_wikidata_service import (
    WikidataEntityDetails,
    WikidataRateLimitError,
    dedupe_candidates,
    load_candidate_by_qid as _default_load_candidate_by_qid,
    search_wikidata_candidates,
    search_wikidata_candidates_multiquery as _default_search_wikidata_candidates_multiquery,
    search_wikidata_candidates_with_options,
)

ADJUDICATION_PROMPT_VERSION = "candidate-adjudication-v2"


def _dedupe_str(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        item = str(value or "").strip()
        key = item.upper()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _candidate_ontology_acronym(score: "CandidateScore") -> str:
    provider = str(getattr(getattr(score, "candidate", None), "source_provider", "") or "").strip().upper()
    return provider


def _broad_best_is_exact(score: "CandidateScore") -> bool:
    metadata = getattr(score, "trace_metadata", {}) or {}
    if str(metadata.get("lexical_match_type", "")) in {"exact_label", "exact_synonym", "exact_id"}:
        return True
    return _safe_confidence(getattr(score, "lexical_score", None), default=0.0) >= 0.99


def _reject_registry(trace_metadata: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    registry = trace_metadata.get("rejected_candidate_registry")
    if not isinstance(registry, dict):
        registry = {}
        trace_metadata["rejected_candidate_registry"] = registry
    return registry


def _score_uri(score_or_candidate: Any) -> str:
    candidate = getattr(score_or_candidate, "candidate", score_or_candidate)
    return str(getattr(candidate, "uri", "") or getattr(candidate, "raw_identifier", "") or "").strip()


def _reject_hardness(reject_type: str = "", reason: str = "") -> str:
    text = f"{reject_type} {reason}".strip().lower()
    if any(token in text for token in (
        "hard", "wrong", "contradict", "no_match", "not a match", "different domain",
        "domain mismatch", "wrong sense", "semantic mismatch", "obsolete",
    )):
        return "hard"
    return "soft"


def _register_rejected_candidate(
    trace_metadata: Dict[str, Any],
    score_or_candidate: Any,
    *,
    stage: str,
    reject_type: str = "",
    reason: str = "",
    hard_or_soft: Optional[str] = None,
    execution_trace: Optional[Any] = None,
) -> None:
    uri = _score_uri(score_or_candidate)
    if not uri:
        return
    hardness = (hard_or_soft or _reject_hardness(reject_type, reason)).strip().lower() or "soft"
    registry = _reject_registry(trace_metadata)
    existing = registry.get(uri) if isinstance(registry.get(uri), dict) else {}
    existing_hardness = str(existing.get("hard_or_soft") or "").lower()
    merged_hardness = "hard" if "hard" in {hardness, existing_hardness} else "soft"
    event = {
        "term_id": trace_metadata.get("term_id", ""),
        "uri": uri,
        "reject_type": reject_type or existing.get("reject_type", ""),
        "reason": reason or existing.get("reason", ""),
        "stage": stage,
        "hard_or_soft": merged_hardness,
    }
    history = list(existing.get("history") or [])
    history.append(event)
    registry[uri] = {**existing, **event, "history": history}
    trace_metadata["rejected_candidate_count"] = len(registry)
    trace_metadata["rejected_candidate_uris"] = sorted(registry.keys())
    candidate = getattr(score_or_candidate, "candidate", None)
    metadata_owner = score_or_candidate if hasattr(score_or_candidate, "trace_metadata") else None
    if metadata_owner is not None:
        metadata = getattr(metadata_owner, "trace_metadata", {}) or {}
        metadata["candidate_rejected_previously"] = True
        metadata["blocked_by_reject_registry"] = True
        metadata["reject_reason"] = event["reason"]
        metadata["reject_stage"] = stage
        metadata["reject_hard_or_soft"] = merged_hardness
        metadata_owner.trace_metadata = metadata
    elif candidate is not None and hasattr(candidate, "ontology_context"):
        ctx = getattr(candidate, "ontology_context", {}) or {}
        if isinstance(ctx, dict):
            ctx["candidate_rejected_previously"] = True
            ctx["reject_reason"] = event["reason"]
            candidate.ontology_context = ctx
    if execution_trace is not None:
        execution_trace.add_event(
            "candidate_rejection",
            "candidate_rejection_registered",
            output_summary=event,
            decision="blocked_for_final" if merged_hardness == "hard" else "soft_block_for_final",
            reason=event["reason"],
        )


def _candidate_rejection(trace_metadata: Dict[str, Any], score_or_candidate: Any) -> Optional[Dict[str, Any]]:
    uri = _score_uri(score_or_candidate)
    if not uri:
        return None
    entry = _reject_registry(trace_metadata).get(uri)
    return entry if isinstance(entry, dict) else None


def _score_has_later_llm_acceptance(score: Any, rejection: Dict[str, Any]) -> bool:
    if score is None:
        return False
    mapping = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    if mapping == "none":
        return False
    source = str(getattr(score, "explanation_source", "") or "").lower()
    if source not in {"llm", "llm_adjudication"}:
        return False
    # If a later stage deliberately re-accepts, it can set this marker. Otherwise a
    # reject registered after the LLM call remains authoritative for finalization.
    return bool((getattr(score, "trace_metadata", {}) or {}).get("llm_reaccepted_after_reject"))


def _blocked_by_reject_registry(trace_metadata: Dict[str, Any], score: Any) -> Tuple[bool, Optional[Dict[str, Any]]]:
    rejection = _candidate_rejection(trace_metadata, score)
    if not rejection:
        return False, None
    hardness = str(rejection.get("hard_or_soft") or "soft").lower()
    if hardness == "hard":
        return True, rejection
    return (not _score_has_later_llm_acceptance(score, rejection)), rejection


def _mark_reject_registry_result(score: Any, blocked: bool, rejection: Optional[Dict[str, Any]]) -> None:
    if score is None or rejection is None or not hasattr(score, "trace_metadata"):
        return
    metadata = getattr(score, "trace_metadata", {}) or {}
    metadata["candidate_rejected_previously"] = True
    metadata["blocked_by_reject_registry"] = bool(blocked)
    metadata["reject_reason"] = rejection.get("reason", "")
    metadata["reject_stage"] = rejection.get("stage", "")
    metadata["reject_hard_or_soft"] = rejection.get("hard_or_soft", "")
    score.trace_metadata = metadata


def _annotator_local_variant_hints(
    term: str,
    candidates: Sequence["CandidateScore"],
    *,
    max_items: int = 6,
) -> List[Dict[str, str]]:
    """Return strictly guarded local variants.

    Candidate labels/synonyms from a weak pool are not reliable synonym evidence. The
    rescue LLM should derive variants from the original term, definition and table
    context instead of recirculating BioPortal hits. Keep this hook empty by default so
    future guarded normalizers can be added without reintroducing candidate echoing.
    """
    del term, candidates, max_items
    return []


def _generate_annotator_rescue_variants(
    *,
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    candidates: List["CandidateScore"],
    config: "AgentRunConfig",
    stats: Any,
    execution_trace: Any,
    run_id: Optional[str],
    term_id: Optional[str],
    row_index: Optional[Any],
) -> List[AnnotatorRescueVariant]:
    max_variants = max(1, int(getattr(config, "annotator_rescue_max_variants", 8) or 8))
    local_hints = _annotator_local_variant_hints(term, candidates, max_items=max_variants)
    payload: Dict[str, Any] = {
        "term_id": "T001",
        "original": term,
        "detected_language": "",
        "term_type": term_profile.get("primary_term_type") or "mixed_domain",
        "variants": [],
    }
    system_prompt = (
        "You generate compact ontology lookup variants for BioPortal Annotator rescue. "
        "Return structured JSON only. Do not write explanatory text. Do not invent broad "
        "generic variants like material/substance/ingredient unless the original term is exactly that broad. "
        "Do not copy BioPortal candidate labels or synonyms from a weak candidate pool. "
        "Treat short abbreviations as ambiguous unless the definition/context supports a specific expansion."
    )
    user_prompt = (
        f"Original term: {term}\n"
        f"Definition/context: {definition or '(missing)'}\n"
        f"Detected term type: {term_profile.get('primary_term_type') or 'mixed_domain'}\n"
        f"Secondary term types: {term_profile.get('secondary_term_types') or []}\n"
        f"Table/category context: {json.dumps(term_profile.get('category_context_used') or {}, ensure_ascii=False)}\n"
        f"RDF role/context: {term_profile.get('rdf_role') or term_profile.get('role') or '(unknown)'}\n\n"
        "Return JSON matching this schema:\n"
        "{\n"
        "  \"term_id\": \"T001\",\n"
        "  \"original\": \"...\",\n"
        "  \"detected_language\": \"de|en|unknown\",\n"
        "  \"term_type\": \"chemical_or_food_substance|food_product|chemical_substance|...\",\n"
        "  \"variants\": [\n"
        "    {\"variant_id\": \"V001\", \"text\": \"...\", \"kind\": \"original|spelling_variant|translation|domain_synonym|canonical_name|abbreviation_expansion|chemical_formula|scientific_name|common_name\", \"expected_strength\": \"weak|medium|strong|very_strong\"}\n"
        "  ]\n"
        "}\n"
        f"Use at most {max_variants} variants. Include the original term. Prefer precise synonyms, "
        "translations, canonical domain names, scientifically supported abbreviation expansions, "
        "scientific names, and formulas only when the term is truly a chemical/formula context. "
        "Do not return candidate_label, candidate_synonym, related_candidate, generic_parent or "
        "abbreviation_collision kinds."
    )
    call_id = make_trace_call_id(run_id or "", "annotator_variant_generation")
    try:
        stats.total_llm_calls_used += 1
        payload = globals().get("generate_structured_completion", _default_generate_structured_completion)(
            config.model_provider,
            config.model_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key_env=_resolve_model_api_key_env(config),
            temperature=0,
            max_tokens=700,
            reasoning_effort=config.reasoning_effort,
            retries_on_parse_failure=1,
            interaction_purpose="annotator_rescue_variant_generation",
            run_id=run_id,
            term_id=term_id,
            row_index=row_index,
            stage="annotator_rescue",
            purpose="variant_generation",
            call_id=call_id,
        )
        execution_trace.add_llm_call(
            TraceLlmCall(
                call_id=call_id,
                stage="annotator_rescue",
                purpose="variant_generation",
                model=config.model_name,
                provider=config.model_provider,
                prompt_sent=(
                    {"system_prompt": system_prompt, "user_prompt": user_prompt}
                    if bool(getattr(config, "trace_llm_prompts", False))
                    else None
                ),
                raw_response=payload if bool(getattr(config, "trace_llm_prompts", False)) else None,
                parsed_response=payload,
            )
        )
    except Exception as exc:
        payload = {}
        execution_trace.add_llm_call(
            TraceLlmCall(
                call_id=call_id,
                stage="annotator_rescue",
                purpose="variant_generation",
                model=config.model_name,
                provider=config.model_provider,
                prompt_sent=(
                    {"system_prompt": system_prompt, "user_prompt": user_prompt}
                    if bool(getattr(config, "trace_llm_prompts", False))
                    else None
                ),
                error=f"{type(exc).__name__}: {exc}",
            )
        )
    return parse_annotator_variant_payload(
        payload,
        original_term=term,
        term_id="T001",
        max_variants=max_variants,
        min_variants=1,
        local_variants=local_hints,
        definition=definition,
        term_profile=term_profile,
    )


def _merge_annotator_rescue_evidence(target: AgentCandidate, rescue: AgentCandidate) -> None:
    target_ctx = getattr(target, "ontology_context", {}) or {}
    rescue_ctx = getattr(rescue, "ontology_context", {}) or {}
    if not isinstance(target_ctx, dict):
        target_ctx = {}
    if not isinstance(rescue_ctx, dict):
        rescue_ctx = {}
    existing = list(target_ctx.get("annotator_rescue_evidence") or [])
    incoming = list(rescue_ctx.get("annotator_rescue_evidence") or [])
    seen = {
        (
            str(ev.get("variant_id") or ""),
            str(ev.get("matched_text") or ""),
            str(ev.get("class_uri") or ""),
        )
        for ev in existing
        if isinstance(ev, dict)
    }
    for ev in incoming:
        if not isinstance(ev, dict):
            continue
        key = (
            str(ev.get("variant_id") or ""),
            str(ev.get("matched_text") or ""),
            str(ev.get("class_uri") or ""),
        )
        if key not in seen:
            seen.add(key)
            existing.append(ev)
    target_ctx.update(
        {
            "match_source": "bioportal_annotator_rescue",
            "annotator_rescue_evidence": existing,
            "annotator_rescue_evidence_count": len(existing),
            "annotator_rescue_matched_variants": sorted({
                str(ev.get("variant_text") or ev.get("matched_text") or "")
                for ev in existing
                if isinstance(ev, dict) and str(ev.get("variant_text") or ev.get("matched_text") or "").strip()
            }),
            "annotator_rescue_score": max(
                _safe_confidence(target_ctx.get("annotator_rescue_score"), default=0.0),
                _safe_confidence(rescue_ctx.get("annotator_rescue_score"), default=float(rescue.score or 0.0)),
            ),
        }
    )
    target.ontology_context = target_ctx


def _merge_annotator_rescue_scores(
    existing_scores: List["CandidateScore"],
    rescue_candidates: List[AgentCandidate],
    *,
    term: str,
    definition: str,
    config: "AgentRunConfig",
) -> List["CandidateScore"]:
    by_key = {_candidate_key(score.candidate): score for score in existing_scores if score is not None}
    for rank, rescue_candidate in enumerate(rescue_candidates or []):
        key = _candidate_key(rescue_candidate)
        if key and key in by_key:
            score = by_key[key]
            _merge_annotator_rescue_evidence(score.candidate, rescue_candidate)
            rescue_score = _safe_confidence(
                (getattr(rescue_candidate, "ontology_context", {}) or {}).get("annotator_rescue_score"),
                default=float(rescue_candidate.score or 0.0),
            )
            score.trace_metadata["annotator_rescue_merged_existing"] = True
            score.trace_metadata["annotator_rescue_score"] = rescue_score
            score.trace_metadata["annotator_rescue_evidence_count"] = (
                score.candidate.ontology_context or {}
            ).get("annotator_rescue_evidence_count", 0)
            score.trace_metadata["annotator_rescue_matched_variants"] = (
                score.candidate.ontology_context or {}
            ).get("annotator_rescue_matched_variants", [])
            score.trace_metadata["annotator_rescue_evidence"] = (
                score.candidate.ontology_context or {}
            ).get("annotator_rescue_evidence", [])
            score.combined_confidence = max(_safe_confidence(score.combined_confidence, default=score.confidence), rescue_score)
            score.confidence = max(_safe_confidence(score.confidence, default=0.0), rescue_score)
            continue
        score = _cheap_prerank_score(term, definition, rescue_candidate, config, api_rank=rank)
        rescue_score = _safe_confidence(
            (getattr(rescue_candidate, "ontology_context", {}) or {}).get("annotator_rescue_score"),
            default=float(rescue_candidate.score or 0.0),
        )
        score.from_fallback = True
        score.trace_metadata.update(
            {
                "match_source": "bioportal_annotator_rescue",
                "lexical_match_type": "annotator_variant",
                "annotator_rescue_candidate": True,
                "annotator_rescue_score": rescue_score,
                "annotator_rescue_evidence_count": (
                    getattr(rescue_candidate, "ontology_context", {}) or {}
                ).get("annotator_rescue_evidence_count", 0),
                "annotator_rescue_matched_variants": (
                    getattr(rescue_candidate, "ontology_context", {}) or {}
                ).get("annotator_rescue_matched_variants", []),
                "annotator_rescue_evidence": (
                    getattr(rescue_candidate, "ontology_context", {}) or {}
                ).get("annotator_rescue_evidence", []),
            }
        )
        score.combined_confidence = max(_safe_confidence(score.combined_confidence, default=0.0), rescue_score)
        score.confidence = max(_safe_confidence(score.confidence, default=0.0), rescue_score)
        if score.mapping_type == "none" and rescue_score >= 0.45:
            score.mapping_type = "close"
            score.relation_type = "close"
        existing_scores.append(score)
        if key:
            by_key[key] = score
    return existing_scores


def _run_rescue_adjudication(
    *,
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    candidates: List["CandidateScore"],
    config: "AgentRunConfig",
    bioportal_api_key: Optional[str],
    execution_trace: Any,
    trace_metadata: Dict[str, Any],
    stats: Any,
    run_id: Optional[str],
    term_id: Optional[str],
    row_index: Optional[Any],
    enrich_fn: Optional[Any] = None,
    trigger_reason: str = "",
) -> Tuple[str, Optional["CandidateScore"]]:
    """Expensive second-pass quality layer (broad_retrieval_registry_scored only).

    Enriches the top candidates, derives abbreviation/definition expansions and searches
    for them, runs comparative LLM adjudication, then a three-way decision guarded by
    contradiction + ontology-family checks. Returns ``(status, best_score)`` where status
    is verified/acceptable_suggestion/reject. Purely additive: only runs for hard cases."""
    top = list(candidates or [])[:5]
    execution_trace.add_event(
        "rescue", "rescue_adjudication_started",
        input_summary={"trigger_reason": trigger_reason, "candidate_count": len(top),
                       "primary_term_type": term_profile.get("primary_term_type")},
        decision="rescue_started", reason=trigger_reason,
    )
    # 1. Evidence enrichment (definitions / hierarchy) for the top candidates.
    if enrich_fn is not None:
        for score in top:
            try:
                score.candidate = enrich_fn(score.candidate)
            except Exception:
                pass
    execution_trace.add_event(
        "rescue", "candidate_evidence_enriched",
        output_summary={"candidates": [_candidate_evidence_summary(s, term, definition) for s in top]},
    )

    merged: List[CandidateScore] = list(top)
    pool_limit = _global_candidate_pool_limit(config)
    _annotator_decision = should_run_annotator_rescue(
        term=term,
        term_profile=term_profile,
        ranked_candidates=top,
        config=config,
        bioportal_api_key=bioportal_api_key,
        triage="weak",
        min_score=0.65,
        stage="rescue_adjudication_pass",
        definition=definition,
        llm_decision=trigger_reason,
    )
    _annotator_triggered, _annotator_reason = _annotator_decision
    if trace_metadata.get("annotator_rescue_used"):
        _annotator_triggered = False
        _annotator_reason = "annotator_rescue_already_used"
        _annotator_trace_fields = {
            **_annotator_decision.as_trace_metadata(),
            "annotator_rescue_triggered": False,
            "annotator_decision_reason": _annotator_reason,
        }
    else:
        _annotator_trace_fields = _annotator_decision.as_trace_metadata()
        trace_metadata.update(_annotator_trace_fields)
        trace_metadata["annotator_rescue_reason"] = _annotator_reason
    execution_trace.add_event(
        "annotator_rescue",
        "annotator_rescue_rescue_pass_trigger_decision",
        input_summary={"candidate_count": len(top), "trigger_reason": trigger_reason},
        output_summary=_annotator_trace_fields,
        decision="triggered" if _annotator_triggered else "skipped",
        reason=_annotator_reason,
    )
    if _annotator_triggered:
        variants = _generate_annotator_rescue_variants(
            term=term,
            definition=definition,
            term_profile=term_profile,
            candidates=top,
            config=config,
            stats=stats,
            execution_trace=execution_trace,
            run_id=run_id,
            term_id=term_id,
            row_index=row_index,
        )
        try:
            annotator_result = run_bioportal_annotator_rescue(
                term=term,
                definition=definition,
                variants=variants,
                api_key=bioportal_api_key or "",
                term_profile=term_profile,
                max_candidates=int(getattr(config, "annotator_rescue_max_candidates", 20) or 20),
                timeout_seconds=float(getattr(config, "annotator_rescue_timeout_seconds", 30) or 30),
                debug_broad_ontologies=bool(getattr(config, "annotator_rescue_debug_broad_ontologies", False)),
            )
        except Exception as exc:
            annotator_result = AnnotatorRescueResult(error=f"{type(exc).__name__}: {exc}")
        request_meta = annotator_result.request_metadata or {}
        request_params = request_meta.get("params") if isinstance(request_meta.get("params"), dict) else {}
        trace_metadata["annotator_rescue_used"] = True
        trace_metadata["annotator_rescue_reason"] = _annotator_reason
        trace_metadata["annotator_rescue_failed"] = bool(annotator_result.error)
        trace_metadata["annotator_rescue_error"] = annotator_result.error
        trace_metadata["annotator_rescue_variant_count"] = len(variants)
        trace_metadata["annotator_rescue_variants"] = [
            {"text": variant.text, "kind": variant.kind, "expected_strength": variant.expected_strength}
            for variant in variants
        ]
        trace_metadata["annotator_rescue_candidate_count"] = len(annotator_result.candidates or [])
        trace_metadata["annotator_rescue_annotation_count"] = annotator_result.annotation_count
        trace_metadata["annotator_rescue_grouped_candidate_count"] = annotator_result.grouped_candidate_count
        trace_metadata["annotator_rescue_dropped_params"] = list(annotator_result.dropped_params or [])
        trace_metadata["annotator_rescue_request"] = request_meta
        trace_metadata["annotator_rescue_matched_variants"] = sorted({
            str(ev.get("variant_text") or ev.get("matched_text") or "")
            for candidate in (annotator_result.candidates or [])
            for ev in ((getattr(candidate, "ontology_context", {}) or {}).get("annotator_rescue_evidence") or [])
            if isinstance(ev, dict) and str(ev.get("variant_text") or ev.get("matched_text") or "").strip()
        })
        execution_trace.add_api_call(
            TraceApiCallSummary(
                provider="BioPortal",
                endpoint_or_operation="annotator",
                ontology_acronym=str(request_params.get("ontologies") or ""),
                query=term,
                status="error" if annotator_result.error else "ok",
                result_count=len(annotator_result.candidates or []),
                duration_ms=request_meta.get("duration_ms"),
                error=annotator_result.error or None,
            )
        )
        if annotator_result.candidates:
            merged = _merge_annotator_rescue_scores(
                merged,
                list(annotator_result.candidates or []),
                term=term,
                definition=definition,
                config=config,
            )
        execution_trace.add_event(
            "annotator_rescue",
            "annotator_rescue_rescue_pass_completed",
            output_summary={
                "candidate_count": len(annotator_result.candidates or []),
                "annotation_count": annotator_result.annotation_count,
                "grouped_candidate_count": annotator_result.grouped_candidate_count,
            },
            decision="merged" if annotator_result.candidates else "no_candidates",
            reason=annotator_result.error or "",
        )

    def _extra_search(query: str, label: str) -> int:
        try:
            extra = search_bioportal_candidates(query, api_key=bioportal_api_key, ontologies=None, page_size=pool_limit)
            for rnk, cand in enumerate(extra):
                sc = _cheap_prerank_score(term, definition, cand, config, api_rank=rnk)
                sc.from_fallback = True
                merged.append(sc)
            return len(extra)
        except Exception:
            return 0

    # 2. Abbreviation/alias expansions inferred from candidate evidence (generic).
    texts: List[str] = []
    for score in top:
        ctx = getattr(score.candidate, "ontology_context", {}) or {}
        texts.append(str(getattr(score.candidate, "label", "") or ""))
        texts.append(str(getattr(score.candidate, "description", "") or ""))
        texts.extend([str(x) for x in (ctx.get("synonyms", []) or [])])
    expansions = _derive_abbreviation_expansions(term, texts)
    if expansions and bioportal_api_key:
        execution_trace.add_event("rescue", "abbreviation_expansion_generated",
                                  output_summary={"expansions": expansions})
        for exp in expansions:
            _extra_search(exp, f"expansion:{exp}")

    # 3. Definition-derived queries (only when the definition carries signal).
    dqueries = _derive_definition_queries(definition)
    if dqueries and bioportal_api_key:
        execution_trace.add_event("rescue", "definition_query_generated",
                                  output_summary={"queries": dqueries})
        for q in dqueries:
            _extra_search(q, f"definition:{q}")

    merged = _dedupe_candidate_scores(merged)
    # Attach registry suitability to every (including newly retrieved) candidate.
    for score in merged:
        acr = _candidate_ontology_acronym(score)
        try:
            reg = get_ontology_context(acr, ensure=False)
        except Exception:
            reg = None
        hard = str((score.trace_metadata or {}).get("candidate_domain_mismatch_level", "")).strip().lower() == "hard"
        score.trace_metadata.update(_compute_ontology_suitability_score(
            term_profile, acr, reg, candidate_label=getattr(score.candidate, "label", ""),
            term=term, candidate_definition=getattr(score.candidate, "description", ""),
            hard_domain_mismatch=hard))
    merged.sort(key=lambda s: _safe_confidence((s.trace_metadata or {}).get("ontology_suitability_score"), default=0.0), reverse=True)
    shortlist = merged[:6]
    execution_trace.add_event(
        "rescue", "rescue_bioportal_search_completed",
        output_summary={"merged_candidate_count": len(merged), "shortlist_count": len(shortlist)},
    )

    # Hierarchy/context assessment on the enriched shortlist (parents/children/siblings/
    # ancestors are now available). Wrong-branch senses become hard-blocked; branch
    # support substitutes for missing definitions in the three-way decision below.
    execution_trace.add_event("hierarchy_context", "hierarchy_context_fetch_started",
                              input_summary={"candidate_count": len(shortlist)})
    for _hs in shortlist:
        _attach_hierarchy_assessment(_hs, term, definition, term_profile)
    execution_trace.add_event(
        "hierarchy_context", "hierarchy_context_fetch_completed",
        output_summary={"candidates": [{
            "label": getattr(s.candidate, "label", ""),
            "hierarchy_domain_fit": (s.trace_metadata or {}).get("hierarchy_domain_fit"),
            "wrong_branch_warning": (s.trace_metadata or {}).get("hierarchy_wrong_branch_warning"),
            "granularity": (s.trace_metadata or {}).get("hierarchy_granularity_assessment"),
            "parent_labels": (s.trace_metadata or {}).get("parent_labels"),
            "sibling_labels": (s.trace_metadata or {}).get("sibling_labels"),
        } for s in shortlist]},
    )

    # 4. Comparative LLM adjudication over the enriched shortlist.
    llm_uri = None
    try:
        adjudicated = _adjudicate_candidate_scores(
            term, definition, shortlist, config, stats, trace_metadata, execution_trace,
            run_id=run_id, term_id=term_id, row_index=row_index)
        if adjudicated is not None:
            llm_uri = str(getattr(getattr(adjudicated, "candidate", None), "uri", "") or "") or None
    except Exception:
        llm_uri = None
    execution_trace.add_event(
        "rescue", "pairwise_candidate_adjudication_completed",
        output_summary={"llm_selected_uri": llm_uri, "shortlist_count": len(shortlist)},
    )

    # 5 + 6. Three-way decision with contradiction + ontology-family HARD gates.
    result = _rescue_three_way_decision(term, definition, term_profile, shortlist, config, llm_selected_uri=llm_uri)
    per_candidate = result.get("per_candidate", [])
    best_evidence = result.get("best_evidence", {}) or {}
    # Contradiction-check transparency for the SELECTED candidate (recalibration audit):
    # was a hard cross-domain conflict found, or was it merely low definition overlap that
    # the ontology hierarchy rescued? These drive whether low def-overlap hard-rejects.
    trace_metadata["contradiction_evidence_type"] = best_evidence.get("contradiction_evidence_type", "none")
    trace_metadata["contradiction_is_hard"] = bool(best_evidence.get("contradiction_is_hard", False))
    trace_metadata["definition_overlap_low_but_hierarchy_supportive"] = bool(
        best_evidence.get("definition_overlap_low_but_hierarchy_supportive", False)
    )
    trace_metadata["hierarchy_context_used_in_contradiction_check"] = bool(
        best_evidence.get("hierarchy_context_used_in_contradiction_check", False)
    )
    execution_trace.add_event(
        "rescue", "semantic_contradiction_check_completed",
        output_summary={
            "contradictions": [c for c in per_candidate if c.get("contradiction")],
            "selected_contradiction_evidence_type": best_evidence.get("contradiction_evidence_type", "none"),
            "selected_contradiction_is_hard": bool(best_evidence.get("contradiction_is_hard", False)),
            "definition_overlap_low_but_hierarchy_supportive": bool(
                best_evidence.get("definition_overlap_low_but_hierarchy_supportive", False)
            ),
            "hierarchy_context_used_in_contradiction_check": bool(
                best_evidence.get("hierarchy_context_used_in_contradiction_check", False)
            ),
        },
    )
    execution_trace.add_event(
        "rescue", "ontology_family_sanity_check_completed",
        output_summary={"per_candidate": [{"label": c.get("label"), "domain_fit_reason": c.get("domain_fit_reason")} for c in per_candidate[:6]]},
    )

    status = str(result.get("status") or "reject")
    reject_type = str(result.get("reasons", {}).get("reject_type") or "") if status == "reject" else ""
    best = result.get("best")
    if best is not None:
        best.trace_metadata["broad_registry_verified"] = (status == "verified")
        best.trace_metadata["rescue_status"] = status
        best.trace_metadata["rescue_reject_type"] = reject_type
        # Reflect the rescue outcome in the candidate confidence so the outer verified/
        # suggestion policy accepts it (the rescue confidence is grounded in enriched
        # evidence, not the low lexical prerank of an expansion-derived candidate).
        _llm = _safe_confidence(getattr(best, "llm_confidence", None), default=0.0)
        if status == "verified":
            _floor = max(_llm, 0.72)
        elif status == "acceptable_suggestion":
            _floor = max(_llm, 0.62)
        else:
            _floor = 0.0
        if _floor > 0.0:
            best.combined_confidence = max(_safe_confidence(getattr(best, "combined_confidence", None), default=0.0), _floor)
            best.confidence = max(_safe_confidence(getattr(best, "confidence", None), default=0.0), _floor)
            if getattr(best, "skos_decision", None) is not None:
                best.skos_decision.confidence = best.confidence
        if status == "acceptable_suggestion":
            best.trace_metadata["force_suggestion"] = True
        # A semantic rescue reject is an explicit negative judgement after enrichment
        # and adjudication. Do not let the later top-N suggestion fallback revive wrong
        # senses; keep soft "insufficient evidence" rejects available for manual review.
        _reject_reason_text = str((result.get("reasons") or {}).get("reason") or "").lower()
        _semantic_reject = reject_type == "hard" or any(
            token in _reject_reason_text
            for token in ("wrong", "contradict", "no_match", "not a match", "different domain")
        )
        if status == "reject" and _semantic_reject:
            best.trace_metadata["suppress_suggestion"] = True
            best.trace_metadata["rescue_rejected_do_not_suggest"] = True
        if status == "reject":
            _register_rejected_candidate(
                trace_metadata,
                best,
                stage="rescue_final_decision",
                reject_type=reject_type or "rescue_reject",
                reason=str((result.get("reasons") or {}).get("reason") or "rescue rejected candidate"),
                hard_or_soft="hard" if _semantic_reject else "soft",
                execution_trace=execution_trace,
            )
    trace_metadata["rescue_pass_used"] = True
    trace_metadata["rescue_status"] = status
    trace_metadata["rescue_reject_type"] = reject_type
    trace_metadata["rescue_trigger_reason"] = trigger_reason
    execution_trace.add_event(
        "rescue", "rescue_final_decision",
        output_summary={"status": status,
                        "best": str(getattr(getattr(best, "candidate", None), "uri", "") or "") or None,
                        "reasons": result.get("reasons", {})},
        decision=status, reason=str(result.get("reasons", {}).get("reason", "")),
    )
    return status, best


_ADJUDICATION_CACHE: Dict[str, Dict[str, Any]] = {}


def clear_candidate_adjudication_cache() -> None:
    _ADJUDICATION_CACHE.clear()


def _hash_text(value: str) -> str:
    return hashlib.sha256(str(value or "").strip().encode("utf-8")).hexdigest()


def _stable_json_hash(value: Any) -> str:
    try:
        payload = json.dumps(value or {}, sort_keys=True, default=str, separators=(",", ":"))
    except Exception:
        payload = str(value or "")
    return _hash_text(payload)


def _token_overlap_ratio(text_a: str, text_b: str) -> float:
    tokens_a = {token for token in str(text_a or "").strip().lower().split() if token}
    tokens_b = {token for token in str(text_b or "").strip().lower().split() if token}
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def _lexical_score(term: str, label: str, metadata: Optional[Dict[str, Any]] = None) -> float:
    lexical_type = str((metadata or {}).get("lexical_match_type", "") or "").strip().lower()
    if lexical_type in {"exact_label", "exact_synonym", "exact_id", "exact_curie"}:
        return 1.0
    term_norm = str(term or "").strip().lower()
    label_norm = str(label or "").strip().lower()
    if term_norm and term_norm == label_norm:
        return 1.0
    return _safe_confidence(_token_overlap_ratio(term_norm, label_norm), default=0.0)


# Lightweight domain signal vocabularies used by the deterministic ontology
# context score. Kept as plain token sets so the heuristic stays cheap and
# auditable (no embeddings required).
_FOOD_TOKENS = ("food", "package", "packaging", "ingredient")
_FOOD_COMMODITY_TOKENS = ("food", "ingredient", "commodity", "edible", "dish", "meal")
_CHEMICAL_TOKENS = ("chemical", "compound", "molecule", "metabolite", "substance")
_MATERIAL_TOKENS = (
    "material", "polymer", "plastic", "paper", "paperboard", "glass", "metal", "steel",
    "aluminium", "aluminum", "fibre", "fiber", "fabric", "cardboard", "wood", "ceramic",
    "porcelain", "earthenware", "container", "film", "foil", "resin", "rubber", "textile",
    "wax", "jute", "cellophane", "tinplate", "parchment", "styropor", "casing", "sheet",
)
_GENOMIC_TOKENS = (
    "genomic", "genome", "epidemiology", "outbreak", "surveillance", "pathogen",
    "isolate", "sequencing",
)
# Ontologies carrying substantive material/biomedical/chemical concepts — acceptable
# providers for strong-exact verification even when not in trusted_ontologies.
_RECOGNIZED_SUBSTANTIVE_ONTOLOGIES = {
    "FOODON", "CHEBI", "NCIT", "SNOMEDCT", "MESH", "NCBITAXON", "LOINC", "QUDT", "QUDT2",
    "MEDDRA", "RCD", "BERO", "EDAM", "OBI", "ENVO", "AGROVOC", "NIFSTD", "OCHV", "GENEPIO",
}
# Generic placeholder / local status / answer-option values that must not auto-verify.
_PLACEHOLDER_STATUS_VALUES = {
    "other", "no information", "unknown", "not specified", "not applicable", "n/a", "na",
    "none", "packed", "not packed", "wrapped", "loose", "open", "no match", "misc",
    "miscellaneous", "not stated", "no data", "no value", "undefined",
}


def _candidate_provider_token(candidate: AgentCandidate) -> str:
    context = getattr(candidate, "ontology_context", {}) or {}
    acronym = context.get("ontology_acronym", "") if isinstance(context, dict) else ""
    return str(getattr(candidate, "source_provider", "") or acronym or "").upper()


def _is_placeholder_or_status_value(term: str) -> bool:
    """Generic placeholder / local status / answer-option value (e.g. 'Other',
    'No information', 'Packed'). These must never be auto-verified against a broad
    ontology — they may at most be suggested for manual review."""
    text = str(term or "").strip().lower()
    if not text:
        return False
    if text in _PLACEHOLDER_STATUS_VALUES:
        return True
    base = text.split("(")[0].split(";")[0].strip()  # 'Not packed (loose; open)' -> 'not packed'
    return base in _PLACEHOLDER_STATUS_VALUES


def _provider_recognized(provider: str) -> bool:
    return str(provider or "").upper() in _RECOGNIZED_SUBSTANTIVE_ONTOLOGIES


def _is_material_batch(input_text: str, batch_domain_context: Optional[str]) -> bool:
    ctx = str(batch_domain_context or "").lower()
    if any(key in ctx for key in ("packaging", "material", "container", "substance")):
        return True
    return any(tok in str(input_text or "").lower() for tok in _MATERIAL_TOKENS)


def _infer_batch_domain_context(source_name: str) -> Optional[str]:
    """Infer a coarse batch domain (e.g. 'packaging') from the source table name."""
    name = str(source_name or "").lower()
    for key in ("packaging", "material", "container", "chemical", "clinical", "food"):
        if key in name:
            return key
    return None


def classify_domain_mismatch(
    provider: str, term: str, definition: str, batch_domain_context: Optional[str] = None
) -> tuple:
    """Classify ontology domain fit as ('none'|'soft'|'hard', reason).

    soft = dampen the context score but DO NOT block verification (e.g. a clinical
    or chemical ontology used for a packaging/material term — the concept can still
    be a correct material match). hard = block verification. The token heuristic only
    produces 'none'/'soft'; genuine sense errors (song/album/company/disease-for-
    organism) are caught semantically by the LLM (which returns no_match), so they
    never reach the verified gate. 'hard' is reserved for explicit upstream signals.
    """
    provider_token = str(provider or "").upper()
    text = f"{term} {definition}".lower()
    material_batch = _is_material_batch(text, batch_domain_context)
    has_food = any(tok in text for tok in _FOOD_COMMODITY_TOKENS)
    has_packaging = "package" in text or "packaging" in text
    has_chemical = any(tok in text for tok in _CHEMICAL_TOKENS) or any(tok in text for tok in _MATERIAL_TOKENS)

    # In a material/packaging reconciliation, material-capable ontologies are compatible.
    if material_batch and provider_token in _RECOGNIZED_SUBSTANTIVE_ONTOLOGIES:
        return "none", "material_concept_in_material_batch"
    # Clinical terminology used for a food commodity / packaging term -> SOFT.
    if provider_token in {"SNOMEDCT", "MEDDRA", "RCD"} and (has_food or has_packaging):
        return "soft", "clinical_ontology_for_food_or_packaging_term"
    # ChEBI for a non-chemical food/packaging term -> SOFT.
    if provider_token == "CHEBI" and (has_food or has_packaging) and not has_chemical:
        return "soft", "chebi_for_non_chemical_term"
    return "none", ""


def _domain_penalized(provider: str, input_text: str, batch_domain_context: Optional[str] = None) -> bool:
    """Legacy boolean: True iff there is a soft/hard domain mismatch (used only to
    dampen the ontology-context score — it no longer blocks verification)."""
    level, _ = classify_domain_mismatch(provider, str(input_text or ""), "", batch_domain_context)
    return level != "none"


def _ontology_context_score(
    term: str, definition: str, candidate: AgentCandidate, batch_domain_context: Optional[str] = None
) -> float:
    context = getattr(candidate, "ontology_context", {}) or {}
    if not isinstance(context, dict):
        return 0.0
    input_text = f"{term} {definition}".strip()
    labels: List[str] = []
    for key in ("lineage", "parents", "children", "sibling_classes_from_ontology", "sibling_examples"):
        value = context.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    labels.append(str(item.get("label", "") or ""))
                    labels.append(str(item.get("parent_label", "") or ""))
                else:
                    labels.append(str(item or ""))
    meta = context.get("ontology_meta") if isinstance(context.get("ontology_meta"), dict) else {}
    labels.extend(str(item) for item in (meta.get("domain", []) or []) if str(item).strip())
    labels.append(str(meta.get("description", "") or ""))
    score = _token_overlap_ratio(input_text, " ".join(labels))

    provider = str(getattr(candidate, "source_provider", "") or context.get("ontology_acronym", "") or "").upper()
    input_lower = input_text.lower()
    if provider == "FOODON" and any(token in input_lower for token in _FOOD_TOKENS):
        score = max(score, 0.75)
    if provider == "CHEBI" and any(token in input_lower for token in _CHEMICAL_TOKENS):
        score = max(score, 0.75)
    if provider == "GENEPIO" and any(token in input_lower for token in _GENOMIC_TOKENS):
        score = max(score, 0.75)
    level, _reason = classify_domain_mismatch(provider, term, definition, batch_domain_context)
    if level in ("soft", "hard"):
        score = min(score, 0.20)
    return _safe_confidence(score, default=0.0)


def _populate_confidence_components(score: CandidateScore, term: str, definition: str, config: AgentRunConfig) -> CandidateScore:
    candidate = score.candidate
    metadata = getattr(score, "trace_metadata", {}) or {}
    batch_domain_context = getattr(config, "batch_domain_context", None)
    score.lexical_score = _lexical_score(term, getattr(candidate, "label", ""), metadata)
    score.definition_score = _token_overlap_ratio(definition, getattr(candidate, "description", ""))
    score.ontology_context_score = _ontology_context_score(term, definition, candidate, batch_domain_context)
    score.provider_score = 1.0 if _provider_is_trusted(getattr(candidate, "source_provider", ""), config) else 0.5
    score.llm_confidence = (
        _safe_confidence(getattr(score.skos_decision, "llm_confidence", None), default=score.confidence)
        if score.skos_decision is not None
        else None
    )
    llm_component = score.llm_confidence if score.llm_confidence is not None else score.confidence
    score.combined_confidence = _safe_confidence(
        (0.45 * _safe_confidence(llm_component, default=0.0))
        + (0.25 * _safe_confidence(score.lexical_score, default=0.0))
        + (0.15 * _safe_confidence(score.definition_score, default=0.0))
        + (0.10 * _safe_confidence(score.ontology_context_score, default=0.0))
        + (0.05 * _safe_confidence(score.provider_score, default=0.0)),
        default=0.0,
    )
    score.relation_type = score.mapping_type
    score.confidence_explanation = (
        "combined_confidence = 0.45*llm + 0.25*lexical + 0.15*definition "
        "+ 0.10*ontology_context + 0.05*provider"
    )
    candidate_context = getattr(candidate, "ontology_context", {}) or {}
    provider_token = _candidate_provider_token(candidate)
    mismatch_level, mismatch_reason = classify_domain_mismatch(provider_token, term, definition, batch_domain_context)
    is_placeholder = _is_placeholder_or_status_value(term)
    score.trace_metadata.update(
        {
            "lexical_score": score.lexical_score,
            "definition_score": score.definition_score,
            "ontology_context_score": score.ontology_context_score,
            "provider_score": score.provider_score,
            "llm_confidence": score.llm_confidence,
            "combined_confidence": score.combined_confidence,
            "confidence_explanation": score.confidence_explanation,
            "domain_penalized": mismatch_level != "none",  # legacy; soft no longer blocks
            "candidate_domain_mismatch_level": mismatch_level,
            "domain_mismatch_reason": mismatch_reason,
            "provider_recognized": _provider_recognized(provider_token),
            "batch_domain_context": batch_domain_context,
            "placeholder_or_status_value": is_placeholder,
            "obsolete": bool(candidate_context.get("obsolete")) if isinstance(candidate_context, dict) else False,
        }
    )
    return score


def _adjudication_cache_key(term: str, definition: str, ranked: List[CandidateScore]) -> str:
    candidate_payload = []
    for score in ranked:
        candidate = score.candidate
        candidate_payload.append(
            {
                "uri": str(getattr(candidate, "uri", "") or ""),
                "label": str(getattr(candidate, "label", "") or ""),
                "definition_hash": _hash_text(str(getattr(candidate, "description", "") or "")),
                "provider": str(getattr(candidate, "source_provider", "") or ""),
                "mapping_type": str(getattr(score, "mapping_type", "") or ""),
                "confidence": round(_safe_confidence(getattr(score, "confidence", 0.0), default=0.0), 6),
                "combined_confidence": round(
                    _safe_confidence(
                        getattr(score, "combined_confidence", None)
                        if getattr(score, "combined_confidence", None) is not None
                        else getattr(score, "confidence", 0.0),
                        default=0.0,
                    ),
                    6,
                ),
                "context_hash": _stable_json_hash(getattr(candidate, "ontology_context", {}) or {}),
            }
        )
    return _stable_json_hash(
        {
            "prompt_version": ADJUDICATION_PROMPT_VERSION,
            "term": str(term or "").strip().lower(),
            "definition_hash": _hash_text(definition),
            "candidates": candidate_payload,
        }
    )

def _score_candidate(
    term: str,
    definition: str,
    candidate: AgentCandidate,
    config: AgentRunConfig,
    *,
    stats: Optional[AgenticExecutionStats] = None,
    enrich_with_wikidata_details: bool = True,
) -> CandidateScore:
    candidate_text_for_matching = candidate.description or candidate.label
    raw_identifier = candidate.raw_identifier or candidate.uri.rsplit("/", 1)[-1]
    wikidata_fallback_reason: Optional[str] = None
    wikidata_fallback_error_type: Optional[str] = None
    wikidata_fallback_error_message: Optional[str] = None
    if enrich_with_wikidata_details:
        try:
            details = WikidataEntityDetails(raw_identifier)
        except WikidataRateLimitError as exc:
            details = None
            wikidata_fallback_reason = "wikidata_rate_limit"
            wikidata_fallback_error_type = type(exc).__name__
            wikidata_fallback_error_message = str(exc)[:300]
        except Exception as exc:
            details = None
            wikidata_fallback_reason = "wikidata_lookup_error"
            wikidata_fallback_error_type = type(exc).__name__
            wikidata_fallback_error_message = str(exc)[:300]

        if details:
            candidate_text_for_matching = (
                details.get("definition")
                or details.get("description")
                or candidate_text_for_matching
            )

    if stats is not None:
        stats.skos_calls_used += 1
        stats.total_llm_calls_used += 1

    decision = classify_skos_match(
        term,
        definition,
        candidate.label,
        candidate_text_for_matching,
        model_name=config.model_name,
        provider=config.model_provider,
        use_llm=True,
        allow_heuristic_fallback=bool(getattr(config, "allow_heuristic_fallback", True)),
        api_key_env=_resolve_model_api_key_env(config),
        reasoning_effort=config.reasoning_effort,
        candidate_context=getattr(candidate, "ontology_context", None),
    )

    if wikidata_fallback_reason and decision.decision_source == "heuristic_fallback":
        decision.fallback_reason = wikidata_fallback_reason
        if not getattr(decision, "fallback_error_type", None):
            decision.fallback_error_type = wikidata_fallback_error_type
        if not getattr(decision, "fallback_error_message", None):
            decision.fallback_error_message = wikidata_fallback_error_message

    score = CandidateScore(
        candidate=candidate,
        mapping_type=decision.mapping_type,
        confidence=_safe_confidence(getattr(decision, "confidence", None), default=0.0),
        explanation_source=getattr(decision, "decision_source", "heuristic_fallback"),
        from_fallback=bool(getattr(decision, "fallback_reason", None)),
        explanation=getattr(decision, "explanation", "") or "",
        skos_decision=decision,
    )
    _populate_confidence_components(score, term, definition, config)
    return _apply_provider_signal_boost(score, term) or score


def _merge_and_trim_candidate_pool(
    pool: List[AgentCandidate],
    new_candidates: List[AgentCandidate],
    limit: int,
) -> List[AgentCandidate]:
    merged = dedupe_candidates([*(pool or []), *(new_candidates or [])])
    return merged[: max(1, int(limit or 1))]


def _build_baseline_candidate_pool(term: str, config: AgentRunConfig) -> List[AgentCandidate]:
    limit = max(1, int(config.candidate_pool_limit or config.max_iterations or 1))
    search_profile = str(getattr(config, "_wikidata_search_profile", "") or "").strip().lower()
    if search_profile and search_profile != "default":
        baseline = search_wikidata_candidates_with_options(term, limit=limit, profile=search_profile)
    else:
        baseline = search_wikidata_candidates(term, limit=limit)
    return dedupe_candidates(baseline)[:limit]


def _candidate_score_limit(config: AgentRunConfig) -> int:
    return max(1, int(getattr(config, "candidate_score_limit", None) or getattr(config, "candidate_pool_limit", None) or 1))


def _per_ontology_candidate_limit(config: AgentRunConfig) -> int:
    return max(1, int(getattr(config, "per_ontology_candidate_limit", 3) or 3))


def _global_candidate_pool_limit(config: AgentRunConfig) -> int:
    return max(1, int(getattr(config, "global_candidate_pool_limit", None) or getattr(config, "candidate_pool_limit", 20) or 20))


def _shortlist_context_limit(config: AgentRunConfig) -> int:
    return max(0, int(getattr(config, "shortlist_context_limit", 3) or 0))


def _shortlist_llm_limit(config: AgentRunConfig) -> int:
    return max(0, int(getattr(config, "shortlist_llm_limit", 3) or 0))


def _limited_ontologies(ontologies: List[str], config: AgentRunConfig) -> List[str]:
    limit = getattr(config, "ontology_scan_limit", None)
    try:
        limit_int = int(limit) if limit not in (None, "") else 0
    except Exception:
        limit_int = 0
    if limit_int <= 0:
        return list(ontologies or [])
    return list(ontologies or [])[:limit_int]


def _candidate_context(candidate: AgentCandidate) -> Dict[str, Any]:
    context = getattr(candidate, "ontology_context", {}) or {}
    return context if isinstance(context, dict) else {}


def _candidate_synonyms(candidate: AgentCandidate) -> List[str]:
    synonyms = _candidate_context(candidate).get("synonyms") or []
    if isinstance(synonyms, str):
        return [synonyms]
    if isinstance(synonyms, list):
        return [str(item) for item in synonyms if str(item).strip()]
    return []


def _cheap_prerank_score(term: str, definition: str, candidate: AgentCandidate, config: AgentRunConfig, api_rank: int = 0) -> CandidateScore:
    context = _candidate_context(candidate)
    metadata = {
        "match_source": context.get("match_source", ""),
        "lexical_match_type": context.get("lexical_match_type", ""),
        "api_rank": api_rank,
        "pre_ranked": True,
    }
    term_norm = str(term or "").strip().lower()
    label_norm = str(getattr(candidate, "label", "") or "").strip().lower()
    synonym_norms = [syn.strip().lower() for syn in _candidate_synonyms(candidate)]
    raw_identifier = str(getattr(candidate, "raw_identifier", "") or getattr(candidate, "uri", "") or "").strip().lower()
    uri_tail = str(getattr(candidate, "uri", "") or "").rsplit("/", 1)[-1].strip().lower()
    notation = str(context.get("notation", "") or "").strip().lower()

    exact_label = bool(term_norm and term_norm == label_norm)
    exact_synonym = bool(term_norm and term_norm in synonym_norms)
    exact_id = bool(term_norm and term_norm in {raw_identifier, uri_tail, notation})
    if exact_label:
        metadata.update({"match_source": "prefLabel", "lexical_match_type": "exact_label"})
    elif exact_synonym:
        metadata.update({"match_source": "synonym", "lexical_match_type": "exact_synonym"})
    elif exact_id:
        metadata.update({"match_source": "identifier", "lexical_match_type": "exact_id"})
    _batch_ctx = getattr(config, "batch_domain_context", None)
    _provider_token = _candidate_provider_token(candidate)
    _mismatch_level, _mismatch_reason = classify_domain_mismatch(_provider_token, term, definition, _batch_ctx)
    metadata["domain_penalized"] = _mismatch_level != "none"  # legacy; soft no longer blocks
    metadata["candidate_domain_mismatch_level"] = _mismatch_level
    metadata["domain_mismatch_reason"] = _mismatch_reason
    metadata["provider_recognized"] = _provider_recognized(_provider_token)
    metadata["batch_domain_context"] = _batch_ctx
    metadata["placeholder_or_status_value"] = _is_placeholder_or_status_value(term)
    metadata["obsolete"] = bool(context.get("obsolete"))

    lexical = 1.0 if (exact_label or exact_synonym or exact_id) else _lexical_score(term, getattr(candidate, "label", ""), metadata)
    definition_score = _token_overlap_ratio(definition, getattr(candidate, "description", ""))
    context_score = _ontology_context_score(term, definition, candidate, _batch_ctx)
    provider_score = 1.0 if _provider_is_trusted(getattr(candidate, "source_provider", ""), config) else 0.5
    api_score = max(0.0, 1.0 - (max(0, api_rank) * 0.10))
    confidence = _safe_confidence(
        (0.40 * lexical)
        + (0.25 * definition_score)
        + (0.15 * context_score)
        + (0.10 * provider_score)
        + (0.10 * api_score),
        default=0.0,
    )
    mapping_type = "exact" if (exact_label or exact_synonym or exact_id) else ("close" if confidence >= 0.45 else ("related" if confidence >= 0.25 else "none"))
    score = CandidateScore(
        candidate=candidate,
        mapping_type=mapping_type,
        confidence=confidence,
        explanation_source="deterministic_prerank",
        from_fallback=False,
        explanation="Cheap deterministic pre-rank before context enrichment and LLM classification.",
        trace_metadata=metadata,
        lexical_score=lexical,
        definition_score=definition_score,
        ontology_context_score=context_score,
        provider_score=provider_score,
        combined_confidence=confidence,
        relation_type=mapping_type,
        confidence_explanation=(
            "pre_rank_confidence = 0.40*lexical + 0.25*definition "
            "+ 0.15*ontology_context + 0.10*provider + 0.10*api_rank"
        ),
    )
    score.trace_metadata.update(
        {
            "pre_rank_confidence": confidence,
            "lexical_score": lexical,
            "definition_score": definition_score,
            "ontology_context_score": context_score,
            "provider_score": provider_score,
            "api_score": api_score,
            "combined_confidence": confidence,
            "confidence_explanation": score.confidence_explanation,
        }
    )
    return score


def _candidate_key(candidate: AgentCandidate) -> str:
    return str(getattr(candidate, "uri", "") or getattr(candidate, "raw_identifier", "") or "").strip().lower()


def _dedupe_candidate_scores(scores: List[CandidateScore]) -> List[CandidateScore]:
    best_by_key: Dict[str, CandidateScore] = {}
    for score in scores:
        key = _candidate_key(score.candidate)
        if not key:
            key = f"{score.candidate.source_provider}:{score.candidate.label}".lower()
        existing = best_by_key.get(key)
        if existing is None or _safe_confidence(score.combined_confidence, default=score.confidence) > _safe_confidence(existing.combined_confidence, default=existing.confidence):
            best_by_key[key] = score
    return list(best_by_key.values())


def _rank_preranked_scores(scores: List[CandidateScore]) -> List[CandidateScore]:
    return sorted(
        scores,
        key=lambda item: (
            _safe_confidence(getattr(item, "combined_confidence", None), default=item.confidence),
            _safe_confidence(getattr(item, "ontology_context_score", None), default=0.0),
            _mapping_priority(item.mapping_type),
            -int((getattr(item, "trace_metadata", {}) or {}).get("api_rank", 999)),
        ),
        reverse=True,
    )


def _candidate_from_definition_record(record: Dict[str, Any], ontology: str, term: str) -> AgentCandidate:
    return AgentCandidate(
        uri=record.get("mapped_id", ""),
        label=record.get("label", term),
        description=record.get("definition", "") or record.get("label", term),
        source_provider=ontology,
        source_workflow="bioportal_wikidata_multiagent",
        raw_identifier=record.get("mapped_id", ""),
        ontology_context={
            "ontology_acronym": ontology,
            "class_links": record.get("links", {}) or {},
            "match_source": record.get("match_source", ""),
            "lexical_match_type": record.get("lexical_match_type", ""),
        },
        source_links=record.get("links", {}) or {},
    )


def _candidate_has_strong_identity_evidence(score: CandidateScore, term: str) -> bool:
    metadata = getattr(score, "trace_metadata", {}) or {}
    lexical_type = str(metadata.get("lexical_match_type", "") or "").strip().lower()
    if lexical_type in {"exact_label", "exact_synonym", "exact_id", "exact_curie"}:
        return True
    candidate = getattr(score, "candidate", None)
    if candidate is None:
        return False
    label = str(getattr(candidate, "label", "") or "").strip().lower()
    term_norm = str(term or "").strip().lower()
    if label and label == term_norm:
        return True
    raw_identifier = str(getattr(candidate, "raw_identifier", "") or "").strip().lower()
    uri_tail = str(getattr(candidate, "uri", "") or "").rsplit("/", 1)[-1].strip().lower()
    return bool(term_norm and term_norm in {raw_identifier, uri_tail})


def _is_clear_deterministic_top_candidate(ranked: List[CandidateScore], term: str) -> bool:
    if not ranked:
        return False
    top = ranked[0]
    if _normalize_mapping_type(top.mapping_type) != "exact":
        return False
    top_confidence = _safe_confidence(
        getattr(top, "combined_confidence", None)
        if getattr(top, "combined_confidence", None) is not None
        else getattr(top, "confidence", 0.0),
        default=0.0,
    )
    threshold = 0.86 if str(getattr(top.candidate, "raw_identifier", "") or "").strip().lower() == str(term or "").strip().lower() else 0.90
    if top_confidence < threshold:
        return False
    if not _candidate_has_strong_identity_evidence(top, term):
        return False
    metadata = getattr(top, "trace_metadata", {}) or {}
    if bool(metadata.get("domain_penalized")) or bool(metadata.get("obsolete")):
        return False
    if len(ranked) < 2:
        return True
    runner_up = ranked[1]
    runner_up_confidence = _safe_confidence(
        getattr(runner_up, "combined_confidence", None)
        if getattr(runner_up, "combined_confidence", None) is not None
        else getattr(runner_up, "confidence", 0.0),
        default=0.0,
    )
    margin = top_confidence - runner_up_confidence
    return margin >= 0.15


def _should_trigger_agentic_refinement(
    best_score: Optional[CandidateScore],
    stats: AgenticExecutionStats,
    config: AgentRunConfig,
) -> bool:
    if not config.enable_agentic_refinement:
        return False
    if stats.planner_calls_used >= max(0, int(config.agentic_max_planner_calls or 0)):
        return False
    if stats.total_llm_calls_used >= max(1, int(config.agentic_total_llm_call_budget or 1)):
        return False

    policy = str(config.agentic_trigger_policy or "no_exact_or_low_confidence").strip().lower()
    if best_score is None:
        return True
    if policy == "always":
        return True
    if policy == "no_exact_or_low_confidence":
        if (best_score.mapping_type or "").lower() == "exact":
            return best_score.confidence < float(config.agentic_min_confidence_to_skip_refinement or 0.8)
        return True
    return (best_score.mapping_type or "").lower() != "exact"


def _sync_agentic_planning_dependencies() -> None:
    planning_dependencies = {
        "generate_structured_completion": _default_generate_structured_completion,
        "load_candidate_by_qid": _default_load_candidate_by_qid,
        "search_wikidata_candidates_multiquery": _default_search_wikidata_candidates_multiquery,
        "search_wikidata_candidates_with_options": search_wikidata_candidates_with_options,
    }
    for name, default in planning_dependencies.items():
        setattr(_agentic_planning, name, globals().get(name, default))


def _generate_agentic_plan(*args, **kwargs):
    _sync_agentic_planning_dependencies()
    return _agentic_planning._generate_agentic_plan(*args, **kwargs)


def _execute_agentic_plan_actions(*args, **kwargs):
    _sync_agentic_planning_dependencies()
    return _agentic_planning._execute_agentic_plan_actions(*args, **kwargs)


def _derive_llm_error_fix_suggestion(
    *,
    config: AgentRunConfig,
    fallback_error_type: Optional[str],
    fallback_error_message: Optional[str],
) -> str:
    provider = str(getattr(config, "model_provider", "") or "").strip() or "selected provider"
    model_name = str(getattr(config, "model_name", "") or "").strip() or "selected model"
    error_text = f"{fallback_error_type or ''} {fallback_error_message or ''}".strip().lower()

    if any(token in error_text for token in ["401", "403", "unauthorized", "forbidden", "auth", "api key", "permission"]):
        env_name = _resolve_model_api_key_env(config)
        return (
            f"Check credentials for provider '{provider}'. Verify the API key in '{env_name}' and confirm model "
            f"'{model_name}' is enabled for that key."
        )

    if any(token in error_text for token in ["429", "rate limit", "too many requests"]):
        return (
            "Provider rate limit reached. Wait and retry, lower Max workers, or switch to a model/provider with "
            "higher throughput."
        )

    if "openai_compatible" in provider.lower() and any(token in error_text for token in ["connection", "refused", "host", "name or service not known", "dns"]):
        return (
            "OpenAI-compatible endpoint appears unreachable. Verify OPENAI_COMPATIBLE_BASE_URL and ensure the local/remote "
            "endpoint is running and accessible."
        )

    if any(token in error_text for token in ["timeout", "timed out"]):
        return "LLM request timed out. Increase timeout, reduce batch pressure, or select a faster model."

    if any(token in error_text for token in ["json", "parse", "schema", "format"]):
        return (
            "Model returned an invalid structured payload. Try a model with stronger JSON adherence or reduce reasoning complexity."
        )

    if any(token in error_text for token in ["context length", "token limit", "maximum context"]):
        return "Prompt likely exceeded model context limits. Use a larger-context model or reduce prompt/definition size."

    return (
        f"Review provider '{provider}' availability and model '{model_name}' settings, then retry. "
        "If needed, continue with heuristic fallback for the remaining terms."
    )


def _candidate_to_decision(
    term: str,
    definition: str,
    candidate: Optional[AgentCandidate],
    source_name: str,
    workflow: str,
    run_id: str,
    config: AgentRunConfig,
    candidate_score: Optional[CandidateScore] = None,
) -> AgentDecision:
    if candidate is None:
        return AgentDecision(
            term=term,
            definition=definition,
            candidate=None,
            skos=None,
            status="no_match",
            explanation="No matching candidate was found.",
            run_id=run_id,
            source_name=source_name,
        )

    skos_decision = None
    explanation = candidate.description or ""
    trace_metadata: Dict[str, Any] = {}
    if candidate_score is not None and candidate_score.skos_decision is not None:
        skos_decision = candidate_score.skos_decision
        explanation = skos_decision.explanation or explanation
        trace_metadata["skos_decision_reused"] = True
    elif config.enable_skos_matching:
        skos_decision = classify_skos_match(
            term,
            definition,
            candidate.label,
            candidate.description or candidate.label,
            model_name=config.model_name,
            provider=config.model_provider,
            use_llm=True,
            allow_heuristic_fallback=bool(getattr(config, "allow_heuristic_fallback", True)),
            api_key_env=_resolve_model_api_key_env(config),
            reasoning_effort=config.reasoning_effort,
            candidate_context=getattr(candidate, "ontology_context", None),
        )
        explanation = skos_decision.explanation or explanation
        trace_metadata["skos_decision_reused"] = False

    return AgentDecision(
        term=term,
        definition=definition,
        candidate=candidate,
        skos=skos_decision,
        status="matched",
        explanation=explanation,
        run_id=run_id,
        source_name=source_name,
        trace_metadata=trace_metadata,
    )


def _build_notebook_faithful_multiagent_config(config: AgentRunConfig) -> AgentRunConfig:
    """Apply notebook-faithful defaults for the BioPortal+Wikidata cascade.

    The cascade may surface exact, close, and related SKOS relations, but the
    word "verified" is reserved for model-confirmed, non-fallback decisions.
    Relation-specific confidence floors avoid treating a weak relatedMatch as
    equivalent to an exactMatch while still allowing strongly supported
    non-exact relations to pass.
    """
    return replace(
        config,
        enforce_verified_match=True,
        verified_match_require_exact=False,
        verified_match_min_confidence=0.0,
        verified_match_min_confidence_exact=VERIFIED_THRESHOLDS["exact"],
        verified_match_min_confidence_close=VERIFIED_THRESHOLDS["close"],
        verified_match_min_confidence_related=VERIFIED_THRESHOLDS["related"],
        verified_match_require_llm_decision=True,
        verified_match_require_no_fallback=True,
        allow_unverified_candidate_suggestions=False,
    )


def _adjudicate_candidate_scores(
    term: str,
    definition: str,
    scores: List[CandidateScore],
    config: AgentRunConfig,
    stats: AgenticExecutionStats,
    trace_metadata: Dict[str, Any],
    execution_trace: Optional[ExecutionTrace] = None,
    *,
    run_id: Optional[str] = None,
    term_id: Optional[str] = None,
    row_index: Optional[Any] = None,
) -> Optional[CandidateScore]:
    eligible = [
        score for score in scores
        if score is not None
        and (
            _score_meets_verified_policy(score, config)
            or _score_meets_suggestion_policy(score, config)
        )
    ]
    # Phase 3: include plausible (but not policy-eligible) candidates in the
    # adjudication pool so plausible packaging/material candidates that the cheap
    # prerank scored as 'none' still reach the LLM instead of collapsing to no_skos.
    # This NEVER auto-verifies — the LLM may still return no_match.
    eligible_keys = {_score_dedup_key(score) for score in eligible}
    plausible_extra = [
        score for score in scores
        if score is not None
        and _score_dedup_key(score) not in eligible_keys
        and _candidate_is_plausible(score, term, definition, config)[0]
    ]
    adjudication_pool: List[CandidateScore] = []
    seen_pool_keys: set = set()
    for score in [*eligible, *plausible_extra]:
        key = _score_dedup_key(score)
        if key not in seen_pool_keys:
            seen_pool_keys.add(key)
            adjudication_pool.append(score)
    trace_metadata["candidate_adjudication_candidate_count"] = len(adjudication_pool)
    trace_metadata["candidate_adjudication_plausible_extra_count"] = len(plausible_extra)
    # Adjudicate only when there are >=2 candidates to compare. A single candidate is
    # handled by the per-candidate classify_skos_match path in the caller, so it already
    # carries an LLM SKOS decision; re-adjudicating it would just spend a second call.
    run_adjudication = bool(getattr(config, "enable_candidate_adjudication", True)) and len(adjudication_pool) >= 2
    if not run_adjudication:
        return _finalize_best_candidate(eligible or scores)

    ranked = sorted(
        adjudication_pool,
        key=lambda item: (
            float(getattr(item, "combined_confidence", None) or item.confidence or 0.0),
            _mapping_priority(item.mapping_type),
        ),
        reverse=True,
    )[: min(6, max(2, int(getattr(config, "candidate_pool_limit", 6) or 6)))]
    if execution_trace is not None:
        execution_trace.add_event(
            "candidate_adjudication",
            "candidate_adjudication_llm_call_started",
            input_summary={
                "candidate_count": len(ranked),
                "candidates_sent": [summarize_candidate_score(score) for score in ranked],
            },
            decision="shortlist_prepared",
        )
    if _is_clear_deterministic_top_candidate(ranked, term):
        trace_metadata["candidate_adjudication_used"] = False
        trace_metadata["candidate_adjudication_short_circuited"] = True
        trace_metadata["candidate_adjudication_short_circuit_reason"] = "clear_top_exact_candidate"
        trace_metadata["candidate_adjudication_llm_calls"] = int(trace_metadata.get("candidate_adjudication_llm_calls", 0))
        if execution_trace is not None:
            execution_trace.add_event(
                "candidate_adjudication",
                "candidate_adjudication_llm_call_completed",
                output_summary={"selected_candidate": summarize_candidate_score(ranked[0])},
                decision="short_circuited",
                reason="clear_top_exact_candidate",
            )
        return ranked[0]
    adjudication_cache_key = _adjudication_cache_key(term, definition, ranked)
    if adjudication_cache_key in _ADJUDICATION_CACHE:
        payload = copy.deepcopy(_ADJUDICATION_CACHE[adjudication_cache_key])
        trace_metadata["candidate_adjudication_cache_hit"] = int(trace_metadata.get("candidate_adjudication_cache_hit", 0)) + 1
    else:
        payload = None
        trace_metadata["candidate_adjudication_cache_miss"] = int(trace_metadata.get("candidate_adjudication_cache_miss", 0)) + 1
    candidate_lines = []
    for index, score in enumerate(ranked, start=1):
        candidate = score.candidate
        context_block = summarize_candidate_context(getattr(candidate, "ontology_context", None))
        card = [
            f"{index}. uri: {candidate.uri}",
            f"   label: {candidate.label}",
            f"   provider: {candidate.source_provider}",
            f"   description: {candidate.description or ''}",
            f"   ontology_context:\n      {context_block.replace(chr(10), chr(10) + '      ')}",
            f"   skos: {score.mapping_type}",
            f"   confidence: {round(float(score.confidence or 0.0), 4)}",
            f"   evidence_source: {score.explanation_source}",
            f"   fallback: {score.from_fallback}",
            f"   rationale: {score.explanation or ''}",
        ]
        # Phase 7: surface the table-aware registry/category signals when present so the
        # adjudicator can reject domain-mismatched or category-misaligned candidates.
        _md = getattr(score, "trace_metadata", {}) or {}
        if "ontology_suitability_score" in _md:
            card.append(f"   ontology_suitability_score: {_md.get('ontology_suitability_score')}")
            card.append(f"   category_alignment_score: {_md.get('category_alignment_score')}")
            _warn = []
            if _md.get("hard_domain_mismatch"):
                _warn.append("HARD_DOMAIN_MISMATCH")
            if _md.get("soft_domain_mismatch"):
                _warn.append("soft_domain_mismatch")
            if _md.get("obsolete"):
                _warn.append("OBSOLETE")
            if _md.get("hierarchy_wrong_branch_warning"):
                _warn.append("WRONG_ONTOLOGY_BRANCH")
            card.append(f"   mismatch_warnings: {', '.join(_warn) if _warn else 'none'}")
        _rescue_evidence = _md.get("annotator_rescue_evidence")
        if not _rescue_evidence and isinstance(getattr(candidate, "ontology_context", None), dict):
            _rescue_evidence = candidate.ontology_context.get("annotator_rescue_evidence")
        if isinstance(_rescue_evidence, list) and _rescue_evidence:
            _brief_evidence = []
            for _ev in _rescue_evidence[:5]:
                if not isinstance(_ev, dict):
                    continue
                _brief_evidence.append(
                    {
                        "variant": _ev.get("variant_text") or _ev.get("matched_text") or "",
                        "kind": _ev.get("variant_kind") or "",
                        "match_type": _ev.get("match_type") or "",
                        "offset": f"{_ev.get('from', '')}-{_ev.get('to', '')}",
                    }
                )
            card.append(f"   bioportal_annotator_rescue_evidence: {_brief_evidence}")
        # Ontology hierarchy / branch context -- often the decisive evidence for the
        # intended sense of an ambiguous label when the definition is missing/weak.
        if any(k in _md for k in ("parent_labels", "child_labels", "sibling_labels", "ancestor_labels")):
            card.append(f"   ontology_parents: {(_md.get('parent_labels') or [])[:6]}")
            card.append(f"   ontology_children: {(_md.get('child_labels') or [])[:6]}")
            card.append(f"   ontology_siblings: {(_md.get('sibling_labels') or [])[:6]}")
            card.append(f"   ontology_ancestors: {(_md.get('ancestor_labels') or [])[:6]}")
            card.append(f"   ontology_branch_summary: {_md.get('ontology_branch_summary', '')}")
            card.append(f"   hierarchy_domain_fit: {_md.get('hierarchy_domain_fit', 'unknown')}; "
                        f"ontology_family_fit: {_md.get('ontology_family_fit', 'unknown')}; "
                        f"granularity: {_md.get('hierarchy_granularity_assessment', 'unknown')}")
        candidate_lines.append("\n".join(card))

    system_prompt = (
        "You are a final semantic adjudicator for ontology reconciliation. "
        "Choose the single candidate that best matches the input term and definition. "
        "Prefer exact semantic fit over provider order or raw confidence. "
        "Use the table/chunk category context to interpret ambiguous terms, and reject "
        "candidates flagged with a domain mismatch or that violate the chunk category. "
        "A lexically exact label is NOT sufficient: a candidate can be the WRONG concept "
        "if its ontology branch (parents/children/siblings/ancestors) or ontology family "
        "indicates a different semantic sense. When the definition is missing or weak, use "
        "the ontology hierarchy and branch as the primary evidence for the intended sense. "
        "For each candidate consider: does the ontology branch support the intended meaning; "
        "are the parents/children/siblings compatible with the term definition; is this a "
        "wrong sense of an ambiguous label; is the candidate too broad, too narrow, or at "
        "the correct semantic granularity; should it be verified, suggested, or rejected."
    )
    _ta_context = ""
    _retrieval_mode = str((trace_metadata or {}).get("retrieval_mode", ""))
    if _retrieval_mode == "broad_retrieval_registry_scored":
        _ta_context = (
            "Term category + registry context (registry is EVIDENCE for ontology trust, "
            "NOT a retrieval filter):\n"
            f"  primary term type: {(trace_metadata or {}).get('primary_term_type')}\n"
            f"  secondary term types: {(trace_metadata or {}).get('secondary_term_types')}\n"
            f"  category confidence: {(trace_metadata or {}).get('category_confidence')}\n"
            f"  verification policy: {(trace_metadata or {}).get('broad_registry_verification_policy')}\n"
            "  Reject candidates whose ontology is a domain mismatch for this term type; a "
            "lexically exact label in the wrong ontology/domain is NOT a match.\n\n"
        )
    user_prompt = (
        f"Input term: {term}\n"
        f"Input definition: {definition or '(missing)'}\n\n"
        + _ta_context
        + "Candidate shortlist:\n\n"
        + "\n\n".join(candidate_lines)
        + "\n\nReturn JSON only:\n"
        "{\n"
        "  \"selected_uri\": \"candidate uri or empty string\",\n"
        "  \"skos\": \"skos:exactMatch|skos:closeMatch|skos:relatedMatch|no_match\",\n"
        "  \"confidence\": 0.0,\n"
        "  \"explanation\": \"brief reason\"\n"
        "}"
    )
    call_id = make_trace_call_id(run_id or "", "candidate_adjudication")
    if payload is None:
        try:
            stats.total_llm_calls_used += 1
            trace_metadata["candidate_adjudication_llm_calls"] = int(trace_metadata.get("candidate_adjudication_llm_calls", 0)) + 1
            payload = globals().get("generate_structured_completion", _default_generate_structured_completion)(
                config.model_provider,
                config.model_name,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key_env=_resolve_model_api_key_env(config),
                temperature=0,
                max_tokens=900,
                reasoning_effort=config.reasoning_effort,
                retries_on_parse_failure=1,
                interaction_purpose="candidate_adjudication",
                run_id=run_id,
                term_id=term_id,
                row_index=row_index,
                stage="candidate_adjudication",
                purpose="candidate_adjudication",
                call_id=call_id,
            )
            _ADJUDICATION_CACHE[adjudication_cache_key] = copy.deepcopy(payload)
        except Exception as exc:
            trace_metadata["candidate_adjudication_error_type"] = type(exc).__name__
            trace_metadata["candidate_adjudication_error_message"] = str(exc)[:300]
            if execution_trace is not None:
                execution_trace.add_llm_call(
                    TraceLlmCall(
                        call_id=call_id,
                        stage="candidate_adjudication",
                        purpose="candidate_adjudication",
                        model=config.model_name,
                        provider=config.model_provider,
                        prompt_sent=(
                            {"system_prompt": system_prompt, "user_prompt": user_prompt}
                            if bool(getattr(config, "trace_llm_prompts", False))
                            else None
                        ),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
                execution_trace.add_event(
                    "candidate_adjudication",
                    "candidate_adjudication_llm_call_completed",
                    decision="llm_error_fallback_best_candidate",
                    reason=f"{type(exc).__name__}: {exc}",
                    data={"call_id": call_id},
                )
            return _finalize_best_candidate(ranked)
    else:
        call_id = f"candidate-adjudication-cache-{_hash_text(adjudication_cache_key)[:12]}"

    if execution_trace is not None:
        execution_trace.add_llm_call(
            TraceLlmCall(
                call_id=call_id,
                stage="candidate_adjudication",
                purpose="candidate_adjudication",
                model=config.model_name,
                provider=config.model_provider,
                prompt_sent=(
                    {"system_prompt": system_prompt, "user_prompt": user_prompt}
                    if bool(getattr(config, "trace_llm_prompts", False))
                    else None
                ),
                raw_response=payload if bool(getattr(config, "trace_llm_prompts", False)) else None,
                parsed_response=payload,
            )
        )

    selected_uri = str(payload.get("selected_uri", "") or "").strip()
    if not selected_uri:
        trace_metadata["candidate_adjudication_selected"] = ""
        reason = str(payload.get("explanation", "") or "LLM adjudication selected no candidate.").strip()
        for rejected in ranked:
            _register_rejected_candidate(
                trace_metadata,
                rejected,
                stage="candidate_adjudication",
                reject_type="llm_no_selection",
                reason=reason,
                hard_or_soft=_reject_hardness("llm_no_selection", reason),
                execution_trace=execution_trace,
            )
        if execution_trace is not None:
            execution_trace.add_event(
                "candidate_adjudication",
                "candidate_adjudication_llm_call_completed",
                output_summary={"parsed_decision": payload},
                decision="no_selection_no_match",
                data={"call_id": call_id},
            )
        return None
    selected = next(
        (
            score for score in ranked
            if str(score.candidate.uri).strip() == selected_uri
            or str(score.candidate.raw_identifier or "").strip() == selected_uri
        ),
        None,
    )
    if selected is None:
        trace_metadata["candidate_adjudication_selected_unknown_uri"] = selected_uri
        if execution_trace is not None:
            execution_trace.add_event(
                "candidate_adjudication",
                "candidate_adjudication_llm_call_completed",
                output_summary={"parsed_decision": payload, "selected_uri": selected_uri},
                decision="unknown_selection_fallback_best_candidate",
                data={"call_id": call_id},
            )
        return _finalize_best_candidate(ranked)

    mapping_type = _normalize_mapping_type(payload.get("skos", ""))
    confidence = _safe_confidence(payload.get("confidence"), default=selected.confidence)
    explanation = str(payload.get("explanation", "") or "").strip()
    before_recompute = summarize_candidate_score(selected)
    if mapping_type and mapping_type not in {"none", "no_match"}:
        selected.mapping_type = mapping_type
        if selected.skos_decision is not None:
            selected.skos_decision.mapping_type = mapping_type
            selected.skos_decision.confidence = confidence
            selected.skos_decision.llm_confidence = confidence
            selected.skos_decision.explanation = explanation or selected.skos_decision.explanation
            selected.skos_decision.decision_source = "llm_adjudication"
        else:
            selected.skos_decision = SKOSDecision(
                mapping_type=mapping_type,
                explanation=explanation,
                input_term=term,
                input_definition=definition,
                candidate_term=selected.candidate.label,
                candidate_definition=selected.candidate.description,
                decision_source="llm_adjudication",
                confidence=confidence,
                llm_confidence=confidence,
            )
        selected.confidence = confidence
        selected.llm_confidence = confidence
        selected.explanation_source = "llm_adjudication"
        selected.explanation = explanation or selected.explanation
        # Fold the adjudicator's confidence into combined_confidence so the
        # verified/suggestion gates judge the adjudicated relation on the same
        # combined score as the per-candidate SKOS path — not the stale
        # deterministic pre-rank value (which omits the LLM-confidence term).
        # Without this, an adjudicator-confirmed exactMatch with high
        # llm_confidence stays at its low pre-rank confidence and is wrongly
        # demoted to a suggestion (and may escalate to Wikidata unnecessarily).
        _populate_confidence_components(selected, term, definition, config)
        _annotator_score = _safe_confidence(
            (getattr(selected, "trace_metadata", {}) or {}).get("annotator_rescue_score"),
            default=0.0,
        )
        if _annotator_score > 0.0:
            selected.combined_confidence = max(
                _safe_confidence(selected.combined_confidence, default=0.0),
                _annotator_score,
            )
            selected.trace_metadata["combined_confidence"] = selected.combined_confidence
            selected.trace_metadata["confidence_supported_by_annotator_rescue"] = True
        trace_metadata["candidate_adjudication_confidence_recomputed"] = True
    else:
        selected.confidence = confidence
        selected.mapping_type = "none"
        selected.combined_confidence = 0.0
        selected.trace_metadata["suppress_suggestion"] = True
        selected.trace_metadata["candidate_adjudication_no_match"] = True
        selected.explanation_source = "llm_adjudication"
        selected.explanation = explanation or selected.explanation
        selected.skos_decision = SKOSDecision(
            mapping_type="none",
            explanation=explanation or "LLM adjudication rejected this candidate.",
            input_term=term,
            input_definition=definition,
            candidate_term=selected.candidate.label,
            candidate_definition=selected.candidate.description,
            decision_source="llm_adjudication",
            confidence=confidence,
            llm_confidence=confidence,
        )
        _register_rejected_candidate(
            trace_metadata,
            selected,
            stage="candidate_adjudication",
            reject_type="llm_no_match",
            reason=selected.explanation or "LLM adjudication rejected this candidate.",
            hard_or_soft=_reject_hardness("llm_no_match", selected.explanation),
            execution_trace=execution_trace,
        )
    trace_metadata["candidate_adjudication_used"] = True
    trace_metadata["candidate_adjudication_selected"] = selected.candidate.uri
    trace_metadata["candidate_adjudication_candidate_count"] = len(ranked)
    if bool(getattr(config, "trace_llm_prompts", False)):
        trace_metadata["candidate_adjudication_prompt_debug"] = {
            "stage": "candidate_adjudication",
            "purpose": "candidate_adjudication",
            "call_id": call_id,
            "term": term,
            "definition": definition,
            "candidate_cards": list(candidate_lines),
            "prompt_sent": {"system_prompt": system_prompt, "user_prompt": user_prompt},
            "raw_response": payload if isinstance(payload, dict) else str(payload)[:2000],
            "parsed_response": payload if isinstance(payload, dict) else {},
        }
    if execution_trace is not None:
        execution_trace.add_event(
            "candidate_adjudication",
            "candidate_adjudication_llm_call_completed",
            output_summary={
                "selected_candidate": summarize_candidate_score(selected),
                "relation": selected.mapping_type,
                "llm_confidence": selected.llm_confidence,
                "reason": selected.explanation,
                "parsed_decision": payload,
            },
            decision="candidate_selected",
            reason=selected.explanation,
            data={"call_id": call_id},
        )
        execution_trace.add_event(
            "scoring",
            "confidence_recomputed",
            input_summary={"before_recompute": before_recompute},
            output_summary={
                "after_recompute": summarize_candidate_score(selected),
                "boosts_applied": _candidate_trace_boosts(selected),
            },
            decision="confidence_components_updated",
        )
    return selected


def run_wikidata_deep_agent(
    term: str,
    definition: str,
    config: AgentRunConfig,
    source_name: str = "input",
    run_id: Optional[str] = None,
    search_profile: Optional[str] = None,
) -> AgentDecision:
    run_id = run_id or str(uuid.uuid4())
    started = time.perf_counter()
    stats = AgenticExecutionStats()
    trace_metadata: Dict[str, Any] = {
        "workflow": "wikidata_deep_agent",
        "candidate_count": 0,
        "enrichment_attempted": 0,
        "agentic_enabled": bool(config.enable_agentic_refinement),
        "agentic_triggered": False,
        "agentic_stop_reason": "",
        "baseline_confidence": 0.0,
        "best_confidence": 0.0,
        "wikidata_search_profile": str(search_profile or "default"),
    }

    enforce_verified_match = bool(getattr(config, "enforce_verified_match", False))

    def _search_and_rank() -> Optional[CandidateScore]:
        if search_profile:
            config_for_search = replace(config)
            setattr(config_for_search, "_wikidata_search_profile", str(search_profile or "").strip())
        else:
            config_for_search = config

        baseline_pool = _build_baseline_candidate_pool(term, config_for_search)
        trace_metadata["candidate_count"] = len(baseline_pool)
        trace_metadata["verified_policy_enforced"] = enforce_verified_match

        scored: List[CandidateScore] = []
        for rank, candidate in enumerate(baseline_pool[: _candidate_score_limit(config)]):
            scored_candidate = _score_candidate(
                term,
                definition,
                candidate,
                config,
                stats=stats,
                enrich_with_wikidata_details=(rank < 2),
            )
            scored.append(scored_candidate)
            if not enforce_verified_match and scored_candidate.mapping_type == "exact" and scored_candidate.confidence >= 0.98:
                break

        best = _finalize_best_candidate(scored)
        trace_metadata["baseline_confidence"] = float(best.confidence if best else 0.0)
        baseline_verified = _score_meets_verified_policy(best, config)
        trace_metadata["baseline_verified_match"] = bool(baseline_verified)

        if _should_trigger_agentic_refinement(best, stats, config):
            trace_metadata["agentic_triggered"] = True
            plan = _generate_agentic_plan(term, definition, scored, config, stats)
            trace_metadata["agentic_stop_reason"] = plan.stop_reason

            new_candidates = _execute_agentic_plan_actions(plan, term, config, stats)
            merged_pool = _merge_and_trim_candidate_pool(
                [s.candidate for s in scored],
                new_candidates,
                limit=config.candidate_pool_limit,
            )

            existing_ids = {
                str(s.candidate.raw_identifier or s.candidate.uri).strip().lower()
                for s in scored
            }
            max_rescore = max(0, int(config.agentic_max_candidate_rescore or 0))
            rescored = 0
            for candidate in merged_pool:
                key = str(candidate.raw_identifier or candidate.uri).strip().lower()
                if key in existing_ids:
                    continue
                if rescored >= max_rescore:
                    break
                scored.append(
                    _score_candidate(
                        term,
                        definition,
                        candidate,
                        config,
                        stats=stats,
                        enrich_with_wikidata_details=False,
                    )
                )
                rescored += 1
                stats.candidate_rescore_used += 1

            best = _finalize_best_candidate(scored)
            trace_metadata["refined_verified_match"] = bool(_score_meets_verified_policy(best, config))

        trace_metadata["best_confidence"] = float(best.confidence if best else 0.0)
        trace_metadata["best_verified_match"] = bool(_score_meets_verified_policy(best, config))
        return best

    timed_out, result = run_with_timeout(_search_and_rank, config.timeout_seconds)
    stats.elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    trace_metadata.update(stats.as_dict())

    if timed_out:
        return AgentDecision(
            term=term,
            definition=definition,
            candidate=None,
            skos=None,
            status="timeout",
            explanation="The Wikidata deep-agent workflow timed out.",
            run_id=run_id,
            source_name=source_name,
            trace_metadata={**trace_metadata, "status": "timeout"},
        )

    if result is None:
        return _build_no_match_decision(
            term,
            definition,
            source_name,
            run_id,
            "No matching candidate was found.",
            workflow="wikidata_deep_agent",
            trace_metadata=trace_metadata,
        )

    _merge_candidate_trace_metadata(result, trace_metadata)

    if enforce_verified_match and not _score_meets_verified_policy(result, config):
        if not bool(getattr(config, "allow_unverified_candidate_suggestions", True)):
            return _build_no_match_decision(
                term,
                definition,
                source_name,
                run_id,
                "No candidate satisfied strict verified-match policy; returning No Match.",
                workflow="wikidata_deep_agent",
                trace_metadata={
                    **trace_metadata,
                    "verified_match_rejected": True,
                },
            )

    best_candidate = result.candidate
    decision = _candidate_to_decision(term, definition, best_candidate, source_name, "wikidata_deep_agent", run_id, config, result)
    if result is not None and result.skos_decision is not None:
        decision.skos = result.skos_decision
        decision.explanation = result.skos_decision.explanation or decision.explanation

    if enforce_verified_match and not _score_meets_verified_policy(result, config):
        decision.status = "candidate_suggested"
        decision.explanation = (
            "Candidate found but it did not satisfy strict verified-match policy. "
            "Treat this as a suggestion requiring manual review."
        )

    decision.trace_metadata = {**trace_metadata, "status": decision.status}
    return decision


def _normalized_trace_term(term: str) -> str:
    return " ".join(str(term or "").strip().lower().split())


def _trace_output_dir_for_config(config: AgentRunConfig, run_id: str) -> Optional[str]:
    explicit = str(getattr(config, "trace_output_dir", "") or "").strip()
    if explicit:
        return explicit
    if trace_level_at_least(getattr(config, "trace_level", "summary"), "detailed"):
        return str((REGISTRY_PATH.parent.parent / "trace_runs" / str(run_id)).resolve())
    return None


def _registry_trace_summary(ontologies: List[str]) -> Dict[str, Any]:
    registry = BioPortalOntologyRegistry()
    entries = registry.load()
    public_entries = {
        key: value for key, value in entries.items()
        if not str(key).startswith("_") and isinstance(value, dict)
    }
    configured = [str(item or "").strip().upper() for item in (ontologies or []) if str(item or "").strip()]
    project_relevant = [
        acronym for acronym in configured
        if acronym in {"FOODON", "CHEBI", "GENEPIO", "QUDT", "QUDT2", "MESH", "NCBITAXON"}
    ]
    return {
        "registry_lookup_performed": True,
        "registry_path": str(REGISTRY_PATH),
        "registry_entry_count": len(public_entries),
        "profiles_loaded": len(public_entries),
        "profiles_with_quality_raw": sum(1 for entry in public_entries.values() if isinstance(entry.get("quality_raw"), dict)),
        "profiles_missing_quality_raw": sum(1 for entry in public_entries.values() if not isinstance(entry.get("quality_raw"), dict)),
        "configured_ontologies": configured,
        "project_relevant_ontologies": project_relevant,
    }


def _candidate_trace_boosts(score: Optional[CandidateScore]) -> List[Dict[str, Any]]:
    metadata = getattr(score, "trace_metadata", {}) or {} if score is not None else {}
    boosts: List[Dict[str, Any]] = []
    if "provider_signal_boost_applied" in metadata or "provider_signal_boost_allowed" in metadata:
        boosts.append(
            {
                "name": "provider_signal_boost",
                "value": metadata.get("confidence_after_boost"),
                "applied": bool(metadata.get("provider_signal_boost_applied")),
                "used_by_gate": False,
                "allowed": metadata.get("provider_signal_boost_allowed"),
                "reason": metadata.get("provider_signal_boost_reason", ""),
            }
        )
    return boosts


def _final_trace_reason(decision: AgentDecision, gate: Dict[str, Any]) -> str:
    failed = gate.get("failed_conditions", []) if isinstance(gate, dict) else []
    if failed:
        return f"{decision.explanation} Final verification conditions: {', '.join(str(item) for item in failed)}"
    return decision.explanation or "Final decision recorded."


# Valid (non-no_match) SKOS relations a candidate row may carry in the final state.
# Matches the vocabulary _normalize_mapping_type actually emits (exact/close/related).
_VALID_FINAL_RELATIONS = {"exact", "close", "related"}
_ADJUDICATION_PLAUSIBLE_RELATIONS = {"exact", "close", "related"}
_PACKAGING_CONTEXT_TOKENS = (
    "packaging", "package", "material", "casing", "container", "wrapper", "wrap",
    "foil", "film", "board", "carton", "bottle", "sheet", "fibre", "fiber", "plastic",
)
_TOKEN_STOPWORDS = {
    "the", "a", "an", "of", "or", "and", "to", "for", "with", "made", "type", "other",
    "kind", "used", "by", "in", "on", "as", "is", "from", "any", "not", "no", "information",
}


def _effective_final_relation(skos: Any, score: Any) -> str:
    """Return the candidate's effective SKOS relation as stored (short form), preferring
    the SKOS decision and falling back to the score's mapping_type. 'no_match' if none."""
    for source in (skos, score):
        mapping = getattr(source, "mapping_type", "") if source is not None else ""
        if _normalize_mapping_type(mapping) != "none":
            return str(mapping or "")
    return "no_match"


def _meaningful_tokens(text: str) -> set:
    return {
        tok for tok in re.split(r"[^a-z0-9]+", str(text or "").lower())
        if len(tok) >= 3 and tok not in _TOKEN_STOPWORDS
    }


def _candidate_is_plausible(score: CandidateScore, term: str, definition: str, config: AgentRunConfig):
    """Return (is_plausible, reason): whether a candidate is worth sending to LLM
    adjudication (NOT whether it should be verified). Plausible if ANY heuristic holds.
    Obsolete candidates are never plausible. This only widens who the LLM sees; the LLM
    may still return no_match."""
    if score is None or getattr(score, "candidate", None) is None:
        return (False, "no_score")
    metadata = getattr(score, "trace_metadata", {}) or {}
    if bool(metadata.get("obsolete")):
        return (False, "obsolete")
    candidate = score.candidate
    mapping = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    combined = _safe_confidence(getattr(score, "combined_confidence", None), default=0.0)
    ctx = _safe_confidence(getattr(score, "ontology_context_score", None), default=0.0)
    prov = _safe_confidence(getattr(score, "provider_score", None), default=0.0)
    lex = _safe_confidence(getattr(score, "lexical_score", None), default=0.0)
    if mapping in _ADJUDICATION_PLAUSIBLE_RELATIONS and combined >= 0.25:
        return (True, "relation_and_confidence")
    if ctx >= 0.50 and prov >= 0.70:
        return (True, "context_and_provider")
    if lex >= 0.20 and prov >= 0.70:
        return (True, "lexical_and_provider")
    provider = _candidate_provider_token(candidate)
    text = f"{term} {definition} {getattr(config, 'batch_domain_context', '') or ''}".lower()
    if provider == "FOODON" and any(tok in text for tok in _PACKAGING_CONTEXT_TOKENS):
        return (True, "foodon_packaging_context")
    shared = _meaningful_tokens(getattr(candidate, "label", "")) & (
        _meaningful_tokens(term) | _meaningful_tokens(definition)
    )
    if shared:
        return (True, f"shared_token:{sorted(shared)[0]}")
    return (False, "not_plausible")


def _score_dedup_key(score: CandidateScore) -> str:
    candidate = getattr(score, "candidate", None)
    return str(getattr(candidate, "uri", "") or getattr(candidate, "raw_identifier", "") or id(score))


def _build_adjudication_shortlist(
    ranked: List[CandidateScore], term: str, definition: str, config: AgentRunConfig, limit: int
) -> List[CandidateScore]:
    """Top-`limit` candidates by rank, augmented to guarantee LLM adjudication sees a
    FOODON candidate (for packaging/material terms), a non-FOODON material/chemical
    candidate, and at least one plausible candidate when each is present. Hard-capped at
    limit + 2 so token cost stays bounded. Ordering (exacts first) is preserved by rank."""
    selected = list(ranked[: max(1, limit)])
    selected_keys = {_score_dedup_key(s) for s in selected}
    hard_cap = max(1, limit) + 2

    def _ensure(predicate) -> None:
        if len(selected) >= hard_cap or any(predicate(s) for s in selected):
            return
        for candidate_score in ranked:
            if _score_dedup_key(candidate_score) in selected_keys:
                continue
            if predicate(candidate_score):
                selected.append(candidate_score)
                selected_keys.add(_score_dedup_key(candidate_score))
                return

    if _is_material_batch(f"{term} {definition}", getattr(config, "batch_domain_context", None)):
        _ensure(lambda s: _candidate_provider_token(getattr(s, "candidate", None)) == "FOODON")
    _ensure(lambda s: _candidate_provider_token(getattr(s, "candidate", None)) not in {"FOODON", ""})
    _ensure(lambda s: _candidate_is_plausible(s, term, definition, config)[0])
    return selected


def normalize_final_decision(
    decision: AgentDecision,
    trace_context: Dict[str, Any],
    score: Optional[CandidateScore] = None,
    execution_trace: Optional["ExecutionTrace"] = None,
) -> AgentDecision:
    """Enforce internally-consistent final state (structural repair only — no scoring,
    no new verification). Invariants:
      matched           -> label+uri+valid relation AND a verified final gate;
                           otherwise downgraded.
      candidate_suggested -> label+uri AND relation != no_match (infer relatedMatch if absent).
      no_match          -> no label/uri/relation (candidate+skos stripped).
    Emits final_decision_normalized only when it actually changes a field. Idempotent."""
    candidate = getattr(decision, "candidate", None)
    skos = getattr(decision, "skos", None)
    uri = str(getattr(candidate, "uri", "") or "").strip()
    label = str(getattr(candidate, "label", "") or "").strip()
    has_candidate = bool(uri and label)
    relation = _effective_final_relation(skos, score)
    relation_norm = _normalize_mapping_type(relation)
    ctx = trace_context or {}
    is_verified = str(ctx.get("verified_gate_decision", "") or "").strip().lower() == "verified"
    status = str(decision.status or "").strip().lower()
    before = {
        "status": decision.status,
        "final_uri": uri,
        "final_label": label,
        "final_match_type": relation,
    }
    reason = ""

    if status == "matched":
        if has_candidate and relation_norm in _VALID_FINAL_RELATIONS and is_verified:
            pass
        elif has_candidate and relation_norm in _VALID_FINAL_RELATIONS:
            decision.status = "candidate_suggested"
            reason = "matched_downgraded_unverified"
        else:
            decision.status = "no_match"
            reason = "matched_missing_candidate_to_no_match"
    elif status == "candidate_suggested":
        if not has_candidate:
            decision.status = "no_match"
            reason = "candidate_suggested_without_candidate_to_no_match"
        elif relation_norm == "none":
            if skos is not None:
                skos.mapping_type = "related"
            elif score is not None:
                score.mapping_type = "related"
            else:
                # Neither skos nor score can carry a relation: attach a minimal
                # SKOSDecision so the candidate_suggested row never reports no_match.
                decision.skos = SKOSDecision(
                    mapping_type="related",
                    explanation=str(getattr(decision, "explanation", "") or "inferred suggestion relation"),
                    input_term=str(getattr(decision, "term", "") or ""),
                    input_definition=str(getattr(decision, "definition", "") or ""),
                    candidate_term=label,
                    candidate_definition="",
                    decision_source="inferred",
                    confidence=0.0,
                )
            reason = "candidate_suggested_match_type_inferred_relatedMatch"

    # no_match must never carry a URI/label/relation.
    if str(decision.status or "").strip().lower() == "no_match":
        if decision.candidate is not None or decision.skos is not None:
            if not reason:
                reason = "no_match_with_uri_stripped"
            decision.candidate = None
            decision.skos = None

    after_skos = getattr(decision, "skos", None)
    after_candidate = getattr(decision, "candidate", None)
    after = {
        "status": decision.status,
        "final_uri": str(getattr(after_candidate, "uri", "") or "").strip(),
        "final_label": str(getattr(after_candidate, "label", "") or "").strip(),
        "final_match_type": _effective_final_relation(after_skos, score if after_candidate is not None else None),
    }
    # Make any verified->suggested downgrade explicit in the trace (P1 transparency).
    if reason == "matched_downgraded_unverified":
        dmeta = getattr(decision, "trace_metadata", None)
        if isinstance(dmeta, dict):
            dmeta["downgrade_applied"] = True
            dmeta["downgrade_reason"] = "final_verified_gate_not_verified"
            dmeta["downgrade_policy"] = "matched_requires_verified_final_gate"
            dmeta["downgrade_failed_conditions"] = list(ctx.get("failed_conditions", []) or [])
    if reason and before != after and execution_trace is not None:
        if reason == "matched_downgraded_unverified":
            execution_trace.add_event(
                "finalization",
                "verified_status_downgraded",
                input_summary=before,
                output_summary={**after, "failed_conditions": list(ctx.get("failed_conditions", []) or [])},
                decision="candidate_suggested",
                reason="matched_requires_verified_final_gate",
            )
        execution_trace.add_event(
            "finalization",
            "final_decision_normalized",
            input_summary=before,
            output_summary=after,
            decision=str(decision.status),
            reason=reason,
        )
    return decision


def run_bioportal_wikidata_multiagent(
    term: str,
    definition: str,
    config: AgentRunConfig,
    bioportal_api_key: Optional[str] = None,
    source_name: str = "input",
    run_id: Optional[str] = None,
    row_index: Optional[Any] = None,
    related_wikidata_bias: bool = False,
    table_context: Optional[Any] = None,
) -> AgentDecision:
    run_id = run_id or str(uuid.uuid4())
    started = time.perf_counter()
    stats = AgenticExecutionStats()
    effective_config = _build_notebook_faithful_multiagent_config(config)
    # Derive batch/domain context (e.g. "packaging") from the source table name when
    # not explicitly configured, so material/polymer concepts are treated as
    # domain-compatible for material/packaging reconciliation tables.
    if not getattr(effective_config, "batch_domain_context", None):
        inferred_batch_context = _infer_batch_domain_context(source_name)
        if inferred_batch_context:
            effective_config = replace(effective_config, batch_domain_context=inferred_batch_context)
    enforce_verified_match = bool(getattr(effective_config, "enforce_verified_match", False))
    term_id = f"{source_name}:{row_index}" if row_index is not None else f"{source_name}:{_hash_text(term)[:12]}"
    execution_trace = ExecutionTrace(
        run_id=run_id,
        term_id=term_id,
        row_index=row_index,
        original_term=term,
        normalized_term=_normalized_trace_term(term),
        trace_level=getattr(effective_config, "trace_level", "summary"),
        trace_llm_prompts=bool(getattr(effective_config, "trace_llm_prompts", False)),
    )
    trace_metadata: Dict[str, Any] = {
        "workflow": "bioportal_wikidata_multiagent",
        "run_id": run_id,
        "term_id": term_id,
        "row_index": row_index,
        "trace_level": execution_trace.trace_level,
        "batch_domain_context": getattr(effective_config, "batch_domain_context", None),
        "bioportal_attempts": 0,
        "wikidata_fallback_used": False,
        "provider_escalation_used": False,
        "wikidata_second_pass_started": False,
        "candidate_review_mode": effective_config.candidate_review_mode,
        "verified_policy_enforced": enforce_verified_match,
        "bioportal_trusted_shortcuts_used": 0,
        "related_wikidata_bias": bool(related_wikidata_bias),
        "notebook_faithful_policy_applied": True,
        "agentic_enabled": bool(effective_config.enable_agentic_refinement),
        "agentic_triggered": False,
        "agentic_stop_reason": "",
    }
    execution_trace.add_event(
        "input",
        "input_received",
        input_summary={
            "term_original": term,
            "term_normalized": execution_trace.normalized_term,
            "row_id": row_index,
            "source_name": source_name,
        },
        data={
            "column_context": "",
            "table_context": source_name,
            "rdf_role": "",
        },
    )
    execution_trace.add_event(
        "definition",
        "definition_resolved",
        input_summary={"definition_input": definition or ""},
        output_summary={
            "definition_generated": definition or "",
            "definition_source": "input" if str(definition or "").strip() else "none",
            "llm_definition_generation": "upstream_or_skipped",
        },
        decision="definition_available" if str(definition or "").strip() else "definition_missing",
    )

    def _record_wikidata_fallback_unavailable(exc: Exception) -> None:
        """Record that the optional Wikidata fallback could not be used."""
        trace_metadata["wikidata_fallback_used"] = True
        trace_metadata["wikidata_second_pass_status"] = "unavailable"
        trace_metadata["wikidata_second_pass_has_candidate"] = False
        trace_metadata["wikidata_fallback_unavailable"] = True
        trace_metadata["wikidata_fallback_reason"] = "wikidata_rate_limit"
        trace_metadata["wikidata_fallback_error_type"] = type(exc).__name__
        trace_metadata["wikidata_fallback_error_message"] = str(exc)[:300]
        trace_metadata["notice"] = (
            "Wikidata fallback could not be used in this run because Wikidata maxlag persisted. "
            "The BioPortal part of the BioPortal+Wikidata workflow completed without aborting the run."
        )
        execution_trace.add_event(
            "wikidata_fallback",
            "wikidata_fallback_decision",
            output_summary={
                "wikidata_fallback_considered": True,
                "wikidata_fallback_attempted": True,
                "wikidata_candidate_count": 0,
                "wikidata_selected": None,
            },
            decision="unavailable",
            reason=f"{type(exc).__name__}: {exc}",
        )

    def _enrich_candidate_context(candidate: AgentCandidate) -> AgentCandidate:
        if not bool(getattr(effective_config, "enable_ontology_context_enrichment", True)):
            return candidate
        enrich_started = time.perf_counter()
        execution_trace.add_event(
            "context_enrichment",
            "context_enrichment_started",
            input_summary={
                "candidate": f"{candidate.source_provider}:{candidate.label}",
                "uri": candidate.uri,
            },
        )
        before = bool((getattr(candidate, "ontology_context", {}) or {}).get("enriched"))
        enriched = enrich_bioportal_candidate_context(
            candidate,
            bioportal_api_key,
            children_limit=max(1, int(getattr(effective_config, "ontology_context_children_limit", 5) or 5)),
        )
        context = getattr(enriched, "ontology_context", {}) or {}
        if context:
            trace_metadata["ontology_context_attempted"] = int(trace_metadata.get("ontology_context_attempted", 0)) + (0 if before else 1)
            if context.get("enriched"):
                trace_metadata["ontology_context_enriched"] = int(trace_metadata.get("ontology_context_enriched", 0)) + (0 if before else 1)
            if context.get("lineage"):
                trace_metadata["ontology_context_path_to_root_count"] = int(trace_metadata.get("ontology_context_path_to_root_count", 0)) + (0 if before else 1)
            if context.get("parents"):
                trace_metadata["ontology_context_parent_count"] = int(trace_metadata.get("ontology_context_parent_count", 0)) + (0 if before else 1)
            if context.get("sibling_classes_from_ontology") or context.get("sibling_examples"):
                trace_metadata["ontology_context_sibling_count"] = int(trace_metadata.get("ontology_context_sibling_count", 0)) + (0 if before else 1)
            attempted = int(context.get("parent_children_calls_attempted", 0) or 0)
            if attempted:
                trace_metadata["ontology_context_parent_children_calls_attempted"] = int(
                    trace_metadata.get("ontology_context_parent_children_calls_attempted", 0)
                ) + (0 if before else attempted)
            successful = int(context.get("parent_children_calls_successful", 0) or 0)
            if successful:
                trace_metadata["ontology_context_parent_children_calls_successful"] = int(
                    trace_metadata.get("ontology_context_parent_children_calls_successful", 0)
                ) + (0 if before else successful)
            sibling_count = int(context.get("sibling_count", 0) or 0)
            if sibling_count:
                trace_metadata["ontology_context_real_sibling_class_count"] = int(
                    trace_metadata.get("ontology_context_real_sibling_class_count", 0)
                ) + (0 if before else sibling_count)
            if context.get("children"):
                trace_metadata["ontology_context_child_count"] = int(trace_metadata.get("ontology_context_child_count", 0)) + (0 if before else 1)
            children_state = str(context.get("children_state", "") or "")
            if "confirmed leaf" in children_state:
                trace_metadata["ontology_context_confirmed_leaf_count"] = int(trace_metadata.get("ontology_context_confirmed_leaf_count", 0)) + (0 if before else 1)
            if context.get("endpoint_errors"):
                trace_metadata["ontology_context_failed_hierarchy_lookup_count"] = int(trace_metadata.get("ontology_context_failed_hierarchy_lookup_count", 0)) + (0 if before else 1)
            meta = context.get("ontology_meta") if isinstance(context.get("ontology_meta"), dict) else {}
            if meta.get("metadata_status"):
                trace_metadata["ontology_context_metadata_status"] = meta.get("metadata_status")
            if context.get("hierarchy_status"):
                trace_metadata["ontology_context_hierarchy_status"] = context.get("hierarchy_status")
            if context.get("children_state"):
                trace_metadata["ontology_context_children_state"] = context.get("children_state")
            if context.get("hierarchy_quality"):
                trace_metadata["ontology_context_hierarchy_quality"] = context.get("hierarchy_quality")
            endpoints = context.get("source_endpoints_used", [])
            if isinstance(endpoints, list) and endpoints:
                existing = trace_metadata.get("ontology_context_endpoints_called", [])
                existing = existing if isinstance(existing, list) else []
                trace_metadata["ontology_context_endpoints_called"] = sorted(set([*existing, *[str(item) for item in endpoints]]))
        execution_trace.add_event(
            "context_enrichment",
            "context_enrichment_completed",
            output_summary=trace_candidate_context_summary(enriched),
            decision="enriched" if bool((getattr(enriched, "ontology_context", {}) or {}).get("enriched")) else "partial_or_missing",
            duration_ms=round((time.perf_counter() - enrich_started) * 1000.0, 2),
        )
        return enriched

    def _merge_wikidata_fallback_trace(wikidata_trace: Dict[str, Any]) -> None:
        """Surface nested Wikidata fallback agentic metrics on the multi-agent trace.

        The monitoring UI reads planner/LLM/refinement metrics from the top-level
        per-term trace. BioPortal+Wikidata runs previously stored the Wikidata
        deep-agent trace only under ``wikidata_trace_metadata``, so the UI showed
        zeros even when fallback refinement used planner calls. Keep the nested
        trace for debugging, but aggregate the counters onto this workflow too.
        """
        if not isinstance(wikidata_trace, dict):
            return

        trace_metadata["wikidata_trace_metadata"] = dict(wikidata_trace)
        for field in (
            "planner_calls_used",
            "skos_calls_used",
            "tool_actions_used",
            "total_llm_calls_used",
            "candidate_rescore_used",
        ):
            try:
                current = int(getattr(stats, field, 0) or 0)
                nested = int(wikidata_trace.get(field, 0) or 0)
                setattr(stats, field, current + nested)
            except Exception:
                continue

        if bool(wikidata_trace.get("agentic_triggered", False)):
            trace_metadata["agentic_triggered"] = True
        nested_stop_reason = str(wikidata_trace.get("agentic_stop_reason", "") or "").strip()
        if nested_stop_reason:
            trace_metadata["agentic_stop_reason"] = nested_stop_reason

        for field in (
            "baseline_confidence",
            "best_confidence",
            "best_verified_match",
            "baseline_verified_match",
            "refined_verified_match",
            "wikidata_search_profile",
        ):
            if field in wikidata_trace:
                trace_metadata[f"wikidata_{field}"] = wikidata_trace.get(field)

    def _finalize_decision_trace(
        decision: AgentDecision,
        score: Optional[CandidateScore] = None,
    ) -> AgentDecision:
        # Phase 2: compute the FINAL verified gate on the candidate that actually
        # produced final_status (BioPortal best, Wikidata fallback, or none) so the
        # summary reflects the authoritative gate — not a stale intermediate BioPortal
        # gate. The BioPortal/Wikidata gates are preserved under their own keys.
        pre_norm_candidate = getattr(decision, "candidate", None)
        final_gate = explain_verified_gate(score, effective_config)
        final_gate_provider = (
            str(getattr(pre_norm_candidate, "source_provider", "") or "") if pre_norm_candidate is not None else ""
        )
        final_gate_uri = (
            str(getattr(pre_norm_candidate, "uri", "") or "") if pre_norm_candidate is not None else ""
        )
        trace_metadata["final_verified_gate_decision"] = final_gate.get("verified_gate_decision")
        trace_metadata["final_verified_gate_failed_conditions"] = final_gate.get("failed_conditions", [])
        trace_metadata["final_verified_gate_passed_conditions"] = final_gate.get("passed_conditions", [])
        trace_metadata["final_verified_gate_provider"] = final_gate_provider
        trace_metadata["final_verified_gate_candidate_uri"] = final_gate_uri
        # Legacy alias now reflects the FINAL gate (the BioPortal gate stays under
        # bioportal_verified_gate_*; no external consumer pins the intermediate value).
        trace_metadata["verified_gate_input"] = final_gate.get("verified_gate_input", {})
        trace_metadata["verified_gate_decision"] = final_gate.get("verified_gate_decision")
        trace_metadata["verified_gate_failed_conditions"] = final_gate.get("failed_conditions", [])
        trace_metadata["verified_gate_passed_conditions"] = final_gate.get("passed_conditions", [])
        execution_trace.add_event(
            "verification",
            "final_verified_gate_applied",
            input_summary=final_gate.get("verified_gate_input", {}),
            output_summary={
                "decision": final_gate.get("verified_gate_decision"),
                "failed_conditions": final_gate.get("failed_conditions", []),
                "passed_conditions": final_gate.get("passed_conditions", []),
                "provider": final_gate_provider,
                "candidate_uri": final_gate_uri,
            },
            decision=str(final_gate.get("verified_gate_decision", "")),
            reason=", ".join(final_gate.get("failed_conditions", []) or []),
        )

        # Phase 7/8: broad_retrieval_registry_scored semantic verification gate.
        if str(trace_metadata.get("retrieval_mode", "")) == "broad_retrieval_registry_scored":
            _sc_md = getattr(score, "trace_metadata", {}) or {}
            trace_metadata["final_candidate_ontology"] = _sc_md.get("candidate_ontology_acronym")
            trace_metadata["final_ontology_suitability_score"] = _sc_md.get("ontology_suitability_score")
            trace_metadata["final_category_alignment_score"] = _sc_md.get("category_alignment_score")
            trace_metadata["final_registry_quality_score"] = _sc_md.get("ontology_quality_score")
            trace_metadata["final_registry_profile_found"] = _sc_md.get("registry_profile_found")
            trace_metadata["final_mismatch_flags"] = {
                "hard_domain_mismatch": _sc_md.get("hard_domain_mismatch"),
                "soft_domain_mismatch": _sc_md.get("soft_domain_mismatch"),
                "registry_suitability_block": _sc_md.get("registry_suitability_block"),
                "placeholder_policy_block": _sc_md.get("placeholder_policy_block"),
            }
            trace_metadata["final_gate_reason"] = ", ".join(final_gate.get("failed_conditions", []) or []) or "verified"
            execution_trace.add_event(
                "verification",
                "semantic_verification_gate_applied",
                input_summary={
                    "primary_term_type": trace_metadata.get("primary_term_type"),
                    "category_confidence": trace_metadata.get("category_confidence"),
                    "verification_policy": trace_metadata.get("broad_registry_verification_policy"),
                    "registry_used_as_retrieval_filter": False,
                    "registry_used_as_candidate_scoring_context": True,
                },
                output_summary={
                    "verified_gate_decision": final_gate.get("verified_gate_decision"),
                    "final_candidate_ontology": _sc_md.get("candidate_ontology_acronym"),
                    "ontology_suitability_score": _sc_md.get("ontology_suitability_score"),
                    "category_alignment_score": _sc_md.get("category_alignment_score"),
                    "registry_quality_score": _sc_md.get("ontology_quality_score"),
                    "hard_domain_mismatch": _sc_md.get("hard_domain_mismatch"),
                    "registry_suitability_block": _sc_md.get("registry_suitability_block"),
                    "placeholder_policy_block": _sc_md.get("placeholder_policy_block"),
                    "targeted_fallback_used": bool(trace_metadata.get("targeted_fallback_used")),
                },
                decision=str(final_gate.get("verified_gate_decision", "")),
                reason=", ".join(final_gate.get("failed_conditions", []) or []),
            )

        blocked_by_registry, rejection = _blocked_by_reject_registry(trace_metadata, score)
        if blocked_by_registry and getattr(decision, "candidate", None) is not None:
            _mark_reject_registry_result(score, True, rejection)
            trace_metadata["candidate_rejected_previously"] = True
            trace_metadata["blocked_by_reject_registry"] = True
            trace_metadata["reject_reason"] = (rejection or {}).get("reason", "")
            trace_metadata["reject_stage"] = (rejection or {}).get("stage", "")
            execution_trace.add_event(
                "finalization",
                "final_candidate_blocked_by_reject_registry",
                input_summary={
                    "status_before": decision.status,
                    "uri": str(getattr(getattr(decision, "candidate", None), "uri", "") or ""),
                },
                output_summary={"rejection": rejection},
                decision="no_match",
                reason=str((rejection or {}).get("reason") or ""),
            )
            decision.status = "no_match"
            decision.candidate = None
            decision.skos = None
        else:
            _mark_reject_registry_result(score, False, rejection)

        # Phase 1+4: enforce internally-consistent final state before emitting the row.
        decision = normalize_final_decision(
            decision,
            {
                "verified_gate_decision": trace_metadata.get("final_verified_gate_decision"),
                "fallback_verified": trace_metadata.get("fallback_verified"),
                "failed_conditions": trace_metadata.get("final_verified_gate_failed_conditions", []),
            },
            score=score,
            execution_trace=execution_trace,
        )

        skos = getattr(decision, "skos", None)
        candidate = getattr(decision, "candidate", None)
        is_no_match = str(decision.status or "").strip().lower() == "no_match"
        gate_failed = trace_metadata.get("final_verified_gate_failed_conditions", [])
        gate_failed = gate_failed if isinstance(gate_failed, list) else []
        final_confidence = None
        if not is_no_match:
            if skos is not None:
                final_confidence = getattr(skos, "confidence", None)
            if final_confidence is None and score is not None and candidate is not None:
                final_confidence = getattr(score, "combined_confidence", None)
        final_match_type = "no_match" if is_no_match else _effective_final_relation(
            skos, score if candidate is not None else None
        )
        selected_ontologies = trace_metadata.get("selected_ontologies_for_retrieval")
        if selected_ontologies is None:
            selected_ontologies = trace_metadata.get("bioportal_selected_ontologies", [])
        execution_trace.summary = {
            "final_status": decision.status,
            "ontology_routing_mode": trace_metadata.get("ontology_routing_mode", "routing_disabled"),
            "selected_ontologies": selected_ontologies,
            "raw_candidate_count": trace_metadata.get("bioportal_raw_candidate_count", 0),
            "deduped_candidate_count": trace_metadata.get("bioportal_candidate_count_after_dedupe", 0),
            "adjudication_candidate_count": trace_metadata.get("candidate_adjudication_candidate_count", 0),
            "final_gate_decision": trace_metadata.get("final_verified_gate_decision", ""),
            "final_gate_failed_conditions": gate_failed,
            "final_gate_provider": trace_metadata.get("final_verified_gate_provider", ""),
            "final_gate_candidate_uri": trace_metadata.get("final_verified_gate_candidate_uri", ""),
            "bioportal_gate_decision": trace_metadata.get("bioportal_verified_gate_decision", ""),
            "wikidata_gate_decision": trace_metadata.get("wikidata_verified_gate_decision", ""),
            "wikidata_attempted": bool(trace_metadata.get("wikidata_second_pass_started", False)),
        }
        final_label = getattr(candidate, "label", "") if candidate is not None else ""
        final_uri = getattr(candidate, "uri", "") if candidate is not None else ""
        # UI contract mirrored into the trace so the review grid and audit agree on what
        # is shown/acceptable. A live suggestion exists iff the row is not a terminal
        # no_match and a candidate URI survived; the SKOS relation does not gate display.
        ui_status = str(decision.status or "").strip().lower()
        ui_has_suggestion = (not is_no_match) and bool(str(final_uri or "").strip())
        final_ui_label = final_label if ui_has_suggestion else ""
        final_ui_uri = final_uri if ui_has_suggestion else ""
        review_action_available = bool(
            ui_has_suggestion and ui_status in {"candidate_suggested", "matched", "pending"}
        )
        accepted_suggestion_persistable = bool(
            ui_has_suggestion and str(final_uri or "").strip() and str(final_label or "").strip()
        )
        trace_metadata["final_ui_status"] = decision.status
        trace_metadata["final_ui_label"] = final_ui_label
        trace_metadata["final_ui_uri"] = final_ui_uri
        trace_metadata["review_action_available"] = review_action_available
        trace_metadata["accepted_suggestion_persistable"] = accepted_suggestion_persistable
        final_payload = {
            "final_status": decision.status,
            "final_label": final_label,
            "final_uri": final_uri,
            "final_confidence": final_confidence,
            "final_match_type": final_match_type,
            "provider": getattr(candidate, "source_provider", "") if candidate is not None else "",
            "decision_source": getattr(skos, "decision_source", "") if skos is not None else "no_skos",
            "final_ui_status": decision.status,
            "final_ui_label": final_ui_label,
            "final_ui_uri": final_ui_uri,
            "review_action_available": review_action_available,
            "accepted_suggestion_persistable": accepted_suggestion_persistable,
            "human_readable_reason": _final_trace_reason(
                decision,
                {
                    "failed_conditions": gate_failed,
                    "passed_conditions": trace_metadata.get("final_verified_gate_passed_conditions", []),
                },
            ),
        }
        if not execution_trace.final_decision:
            execution_trace.set_final_decision(final_payload)
        trace_output_dir = _trace_output_dir_for_config(effective_config, run_id)
        artifact_paths = {}
        if trace_output_dir:
            artifact_paths = export_trace_artifacts(execution_trace, trace_output_dir)
        merged_trace = {**trace_metadata, **(getattr(decision, "trace_metadata", {}) or {})}
        if artifact_paths:
            merged_trace["trace_artifacts"] = artifact_paths
        merged_trace["execution_trace"] = execution_trace.as_dict(
            compact=not trace_level_at_least(getattr(effective_config, "trace_level", "summary"), "detailed")
        )
        merged_trace["status"] = decision.status
        decision.trace_metadata = merged_trace
        return decision

    def _search_pipeline() -> Optional[CandidateScore]:
        best_score: Optional[CandidateScore] = None
        best_priority = -1
        scored_candidates: List[CandidateScore] = []

        def _suggested_best_score_or_none() -> Optional[CandidateScore]:
            blocked, rejection = _blocked_by_reject_registry(trace_metadata, best_score)
            if blocked:
                _mark_reject_registry_result(best_score, True, rejection)
                trace_metadata["blocked_by_reject_registry"] = True
                trace_metadata["reject_reason"] = (rejection or {}).get("reason", "")
                trace_metadata["reject_stage"] = (rejection or {}).get("stage", "")
                execution_trace.add_event(
                    "finalization",
                    "candidate_blocked_by_reject_registry",
                    output_summary={"uri": _score_uri(best_score), "rejection": rejection},
                    decision="no_match",
                    reason=str((rejection or {}).get("reason") or ""),
                )
                return None
            _mark_reject_registry_result(best_score, False, rejection)
            if _score_meets_suggestion_policy(best_score, effective_config):
                return best_score
            if best_score is not None:
                trace_metadata["suggestion_policy_rejected"] = True
            return None

        def _remember_score(candidate_score: CandidateScore) -> None:
            nonlocal best_score, best_priority
            scored_candidates.append(candidate_score)
            priority = _mapping_priority(candidate_score.mapping_type)
            best_priority = max(best_priority, priority)
            best_score = _finalize_best_candidate(scored_candidates)

        # Supported modes: "configured_only" and "broad_retrieval_registry_scored".
        # Legacy/removed identifiers degrade to the broad registry-scored mode.
        ontology_search_mode = str(getattr(effective_config, "ontology_search_mode", "") or "").strip().lower()
        if ontology_search_mode in {"bioportal_all_direct", "direct_all_debug", "registry_routed_all", "table_aware_hybrid"}:
            ontology_search_mode = "broad_retrieval_registry_scored"
        if ontology_search_mode not in {"configured_only", "broad_retrieval_registry_scored"}:
            ontology_search_mode = "broad_retrieval_registry_scored" if bool(getattr(effective_config, "bioportal_use_all_ontologies", False)) else "configured_only"
        use_broad_registry_scored = ontology_search_mode == "broad_retrieval_registry_scored"
        configured_ontologies = list(effective_config.bioportal_agent_ontologies or [])

        # broad_retrieval_registry_scored (Phase 1): detect the term category from the
        # term + definition (+ optional table/chunk context). The registry is NOT used
        # to restrict retrieval -- only as post-retrieval candidate scoring context.
        broad_registry_term_profile: Dict[str, Any] = {}
        broad_registry_skip_retrieval = False
        if use_broad_registry_scored:
            try:
                _tcp = getattr(table_context, "table_context_profile", None) if table_context is not None else None
                _ccp = (
                    table_context.chunk_for(term, row_index)
                    if (table_context is not None and hasattr(table_context, "chunk_for"))
                    else None
                )
                broad_registry_term_profile = _build_term_category_profile(
                    term, definition, table_context_profile=_tcp, chunk_context_profile=_ccp
                ) or {}
            except Exception as exc:  # profiling must never break reconciliation
                broad_registry_term_profile = {}
                trace_metadata["broad_registry_profile_error"] = f"{type(exc).__name__}: {exc}"
            _bp_placeholder = (
                str(broad_registry_term_profile.get("verification_policy")) == "do_not_auto_verify_external_match"
                or broad_registry_term_profile.get("primary_term_type") == "placeholder_or_status"
            )
            if _bp_placeholder:
                # Placeholder/status values are never externally verified -> skip retrieval.
                broad_registry_skip_retrieval = True

        if use_broad_registry_scored:
            # NO hard ontology restriction: broad global BioPortal search (the registry
            # is applied AFTER retrieval as candidate scoring context, not as a filter).
            ontologies = []
        else:
            ontologies = configured_ontologies
        trace_metadata["bioportal_use_all_ontologies"] = False
        trace_metadata["ontology_search_mode"] = ontology_search_mode
        trace_metadata["bioportal_selected_ontologies"] = list(ontologies or [])
        trace_metadata["use_all_ontologies_direct"] = False
        trace_metadata["fallback_to_all_ontologies"] = False
        if use_broad_registry_scored:
            trace_metadata["retrieval_mode"] = "broad_retrieval_registry_scored"
            # The key conceptual change: the registry is NOT a retrieval filter here.
            trace_metadata["registry_used_as_retrieval_filter"] = False
            trace_metadata["registry_used_as_candidate_scoring_context"] = True
            trace_metadata["primary_term_type"] = broad_registry_term_profile.get("primary_term_type")
            trace_metadata["secondary_term_types"] = list(broad_registry_term_profile.get("secondary_term_types", []))
            trace_metadata["assigned_categories"] = _dedupe_str(
                [broad_registry_term_profile.get("primary_term_type")]
                + list(broad_registry_term_profile.get("secondary_term_types", []))
            )
            trace_metadata["category_confidence"] = (
                broad_registry_term_profile.get("term_type_confidence")
                or broad_registry_term_profile.get("category_confidence")
            )
            trace_metadata["category_context_used"] = broad_registry_term_profile.get("category_context_used")
            trace_metadata["broad_registry_verification_policy"] = broad_registry_term_profile.get("verification_policy")
            trace_metadata["broad_registry_is_placeholder"] = bool(broad_registry_skip_retrieval)
            trace_metadata["broad_registry_skip_retrieval"] = bool(broad_registry_skip_retrieval)
            execution_trace.add_event(
                "table_context",
                "term_context_profile_built",
                output_summary={
                    "primary_term_type": broad_registry_term_profile.get("primary_term_type"),
                    "secondary_term_types": list(broad_registry_term_profile.get("secondary_term_types", [])),
                    "application_context": broad_registry_term_profile.get("application_context"),
                    "term_type_confidence": broad_registry_term_profile.get("term_type_confidence"),
                    "verification_policy": broad_registry_term_profile.get("verification_policy"),
                    "ambiguity_flags": list(broad_registry_term_profile.get("ambiguity_flags", [])),
                    "category_context_used": broad_registry_term_profile.get("category_context_used"),
                },
                decision="broad_registry_profile",
            )
        # Generic CURIE resolver: a genuine ``prefix:LocalName`` (e.g. skos:exactMatch,
        # dcat:Dataset, geo:Feature) is deterministic RDF syntax in a known namespace
        # family -- resolve it directly and skip BioPortal + Wikidata. Natural-language
        # phrasings are NOT special-cased (resolver returns None) and go through retrieval.
        if use_broad_registry_scored and not broad_registry_skip_retrieval:
            _ns = _resolve_namespace_term(term, broad_registry_term_profile.get("primary_term_type"))
            if _ns:
                trace_metadata["namespace_resolver_used"] = True
                trace_metadata["namespace_resolver"] = _ns["resolver"]
                trace_metadata["final_candidate_ontology"] = _ns["namespace"]
                trace_metadata["registry_used_as_retrieval_filter"] = False
                trace_metadata["registry_used_as_candidate_scoring_context"] = True
                trace_metadata["wikidata_fallback_blocked"] = True
                trace_metadata["wikidata_fallback_blocked_reason"] = "namespace_resolver_match"
                ns_candidate = AgentCandidate(
                    uri=_ns["uri"], label=_ns["label"], description="",
                    source_provider=_ns["namespace"], source_workflow="namespace_resolver",
                    raw_identifier=_ns["uri"], ontology_context={"ontology_acronym": _ns["namespace"], "obsolete": False},
                )
                ns_skos = SKOSDecision(
                    mapping_type=_ns["mapping_type"], explanation=f"Direct namespace mapping to {_ns['label']}",
                    input_term=term, input_definition=definition or "", candidate_term=_ns["label"],
                    candidate_definition="", decision_source="namespace_resolver", confidence=0.98, llm_confidence=0.98,
                )
                ns_score = CandidateScore(
                    candidate=ns_candidate, mapping_type=_ns["mapping_type"], confidence=0.98,
                    explanation_source="namespace_resolver", from_fallback=False, explanation=ns_skos.explanation,
                    skos_decision=ns_skos, lexical_score=1.0, definition_score=1.0, combined_confidence=0.98,
                    llm_confidence=0.98,
                    trace_metadata={
                        "broad_registry_verified": True, "broad_registry_triage": "verified",
                        "candidate_ontology_acronym": _ns["namespace"], "ontology_suitability_score": 0.95,
                        "category_alignment_score": 0.90, "ontology_quality_score": 0.9,
                        "namespace_resolved": True, "registry_profile_found": False,
                        "lexical_match_type": "exact_label",
                    },
                )
                execution_trace.add_event(
                    "namespace_resolver", "namespace_resolver_applied",
                    output_summary={"term": term, "uri": _ns["uri"], "label": _ns["label"],
                                    "namespace": _ns["namespace"], "mapping_type": _ns["mapping_type"]},
                    decision="resolved", reason="direct namespace/vocabulary mapping; BioPortal/Wikidata skipped",
                )
                return ns_score
        if bioportal_api_key and not ontologies and not use_broad_registry_scored:
            ontologies = recommend_ontology_acronyms([term], bioportal_api_key, min_valid=5)

        # Read-only: record which configured ontologies already have local registry
        # context BEFORE retrieval. ensure=False -> no BioPortal fetch and no change
        # to ranking; this is the foundation hook for future ontology routing.
        registry_started = time.perf_counter()
        execution_trace.add_event(
            "registry_lookup",
            "registry_lookup_started",
            input_summary={
                "registry_path": str(REGISTRY_PATH),
                "configured_ontologies": list(configured_ontologies or []),
                "registry_routing_search_space_count": 0,
                "registry_routing_search_space_sample": [],
                "configured_ontologies_used_as_primary": not use_broad_registry_scored,
                "ontology_search_mode": ontology_search_mode,
            },
        )
        try:
            registry_contexts = get_ontology_contexts(list(ontologies or []), ensure=False)
            registry_available = sorted(acr for acr, ctx in registry_contexts.items() if ctx)
            registry_missing = sorted(acr for acr, ctx in registry_contexts.items() if not ctx)
            registry_summary = _registry_trace_summary(list(ontologies or []))
            trace_metadata["ontology_registry_used"] = bool(registry_available)
            trace_metadata["ontology_registry_available"] = registry_available
            trace_metadata["ontology_registry_missing"] = registry_missing
            trace_metadata["ontology_context_available_count"] = len(registry_available)
            trace_metadata["ontology_registry_entry_count"] = registry_summary["registry_entry_count"]
            trace_metadata["ontology_registry_profiles_with_quality_raw"] = registry_summary["profiles_with_quality_raw"]
            trace_metadata["ontology_registry_profiles_missing_quality_raw"] = registry_summary["profiles_missing_quality_raw"]
            execution_trace.add_event(
                "registry_lookup",
                "registry_lookup_completed",
                output_summary={
                    **registry_summary,
                    "registry_available": registry_available,
                    "registry_missing": registry_missing,
                },
                decision="registry_context_loaded" if registry_available else "registry_context_missing",
                duration_ms=round((time.perf_counter() - registry_started) * 1000.0, 2),
            )
        except Exception as exc:  # registry lookup must never break reconciliation
            trace_metadata["ontology_registry_lookup_error"] = f"{type(exc).__name__}: {exc}"
            execution_trace.add_event(
                "registry_lookup",
                "registry_lookup_completed",
                decision="registry_lookup_error",
                reason=f"{type(exc).__name__}: {exc}",
                duration_ms=round((time.perf_counter() - registry_started) * 1000.0, 2),
            )

        # Optional LLM ontology routing (Phase 7). Disabled by default. Only selects
        # WHICH ontologies to query; routing scores never become candidate confidence.
        routing_ontologies = list(ontologies or [])
        # Optional per-term LLM ontology routing over the configured ontology set. The
        # broad registry-scored mode does its own retrieval and never routes per term.
        routing_enabled = (
            bool(getattr(effective_config, "enable_ontology_routing", False))
            and not use_broad_registry_scored
        )
        trace_metadata["ontology_routing_enabled"] = routing_enabled
        trace_metadata["enable_ontology_routing"] = routing_enabled
        if (
            routing_enabled
            and routing_ontologies
        ):
            try:
                routing_call_id = make_trace_call_id(run_id, "ontology_routing")
                execution_trace.add_event(
                    "ontology_routing",
                    "ontology_routing_llm_call_started" if bool(getattr(effective_config, "ontology_routing_use_llm", True)) else "ontology_prefilter_started",
                    input_summary={
                        "programmatic_prefilter_input_count": len(routing_ontologies),
                        "prefilter_limit": int(getattr(effective_config, "ontology_routing_prefilter_limit", 20) or 20),
                        "ontologies_available": list(routing_ontologies),
                        "ontologies_available_count": len(routing_ontologies),
                        "ontology_search_mode": ontology_search_mode,
                        "configured_ontologies_used_as_primary": True,
                    },
                    data={"call_id": routing_call_id},
                )
                selected, routing_result = route_ontologies(
                    term,
                    definition,
                    routing_ontologies,
                    routing_search_mode="configured_only",
                    use_llm=bool(getattr(effective_config, "ontology_routing_use_llm", True)),
                    table_context=source_name,
                    project_context=getattr(effective_config, "ontology_routing_project_context", None),
                    registry_lookup=lambda acr: get_ontology_context(acr, ensure=False),
                    trusted_fallback_ontologies=list(configured_ontologies or []),
                    top_n=int(getattr(effective_config, "ontology_routing_top_n", 5) or 5),
                    prefilter_limit=int(getattr(effective_config, "ontology_routing_prefilter_limit", 20) or 20),
                    include_default_core=bool(getattr(effective_config, "ontology_routing_include_default_core", True)),
                    min_score=float(getattr(effective_config, "ontology_routing_min_score", 0.40) or 0.40),
                    strong_score=float(getattr(effective_config, "ontology_routing_strong_score", 0.80) or 0.80),
                    confident_margin=float(getattr(effective_config, "ontology_routing_confident_margin", 0.10) or 0.10),
                    model_provider=effective_config.model_provider,
                    model_name=str(getattr(effective_config, "ontology_routing_llm_model", None) or effective_config.model_name),
                    api_key_env=_resolve_model_api_key_env(effective_config),
                    reasoning_effort=effective_config.reasoning_effort,
                    blocklist=list(getattr(effective_config, "ontology_quality_blocklist", []) or []),
                    allowlist=list(getattr(effective_config, "ontology_quality_allowlist", []) or []),
                    allow_excluded_for_debug=bool(getattr(effective_config, "allow_excluded_ontologies_for_debug", False)),
                    capture_prompts=bool(getattr(effective_config, "trace_llm_prompts", False)),
                    run_id=run_id,
                    term_id=term_id,
                    row_index=row_index,
                    call_id=routing_call_id,
                )
                if selected:
                    routing_ontologies = selected
                if bool(getattr(effective_config, "ontology_routing_trace", True)):
                    trace_metadata["ontology_routing_used_llm"] = routing_result.used_llm
                    trace_metadata["ontology_routing_mode"] = routing_result.routing_mode
                    trace_metadata["ontology_routing_confident"] = routing_result.routing_confident
                    trace_metadata["ontology_routing_fallback_to_default"] = routing_result.fallback_to_default
                    trace_metadata["ontology_routing_top_score"] = routing_result.top_score
                    trace_metadata["ontology_routing_margin"] = routing_result.margin
                    trace_metadata["ontology_routing_reason"] = routing_result.reason
                    trace_metadata["ontology_quality_gate_applied"] = routing_result.quality_gate_applied
                    trace_metadata["ontology_routing_excluded_summary"] = dict(routing_result.excluded_summary)
                    trace_metadata["ontology_ranking"] = [r.as_dict() for r in routing_result.ranked_ontologies]
                    trace_metadata["selected_ontologies_for_retrieval"] = list(selected)
                    trace_metadata["use_all_ontologies_direct"] = False
                    if bool(getattr(effective_config, "trace_llm_prompts", False)) and routing_result.prompt_debug:
                        trace_metadata["ontology_routing_prompt_debug"] = routing_result.prompt_debug
                    execution_trace.add_event(
                        "ontology_quality",
                        "ontology_quality_gate_applied",
                        output_summary=dict(routing_result.excluded_summary),
                        decision="quality_gate_applied" if routing_result.quality_gate_applied else "quality_gate_skipped",
                    )
                    execution_trace.add_event(
                        "ontology_prefilter",
                        "ontology_prefilter_completed",
                        input_summary={"programmatic_prefilter_input_count": len(ontologies or [])},
                        output_summary={
                            "programmatic_prefilter_output_count": len(routing_result.ranked_ontologies),
                            "prefiltered_ontologies": [r.as_dict() for r in routing_result.ranked_ontologies],
                        },
                    )
                    if routing_result.prompt_debug or routing_result.used_llm:
                        prompt_debug = routing_result.prompt_debug or {}
                        execution_trace.add_llm_call(
                            TraceLlmCall(
                                call_id=str(prompt_debug.get("call_id") or routing_call_id),
                                stage="ontology_routing",
                                purpose="ontology_routing",
                                model=str(prompt_debug.get("model") or getattr(effective_config, "ontology_routing_llm_model", None) or effective_config.model_name),
                                provider=str(prompt_debug.get("provider") or effective_config.model_provider),
                                prompt_sent=(
                                    {
                                        "system_prompt": str(prompt_debug.get("system_prompt", "")),
                                        "user_prompt": str(prompt_debug.get("user_prompt", "")),
                                    }
                                    if bool(getattr(effective_config, "trace_llm_prompts", False)) and prompt_debug
                                    else None
                                ),
                                raw_response=prompt_debug.get("raw_response") if bool(getattr(effective_config, "trace_llm_prompts", False)) else None,
                                parsed_response=prompt_debug.get("parsed_response") or {"ranked_ontologies": [r.as_dict() for r in routing_result.ranked_ontologies]},
                                error=routing_result.error or None,
                            )
                        )
                        execution_trace.add_event(
                            "ontology_routing",
                            "ontology_routing_llm_call_completed",
                            output_summary={
                                "ranked_ontologies": [r.as_dict() for r in routing_result.ranked_ontologies],
                                "ontologies_sent_to_llm": None,
                                "parsed_ranking": None,
                                "routing_confident": routing_result.routing_confident,
                                "fallback_to_default": routing_result.fallback_to_default,
                            },
                            decision=routing_result.routing_mode,
                            reason=routing_result.reason,
                            data={"call_id": routing_call_id},
                        )
                    execution_trace.add_event(
                        "ontology_routing",
                        "selected_ontologies_for_retrieval",
                        output_summary={
                            "selected_ontologies_for_retrieval": list(selected),
                            "routing_mode": routing_result.routing_mode,
                            "ontology_search_mode": ontology_search_mode,
                            "use_all_ontologies_direct": False,
                            "top_score": routing_result.top_score,
                            "margin": routing_result.margin,
                            "include_default_core": bool(getattr(effective_config, "ontology_routing_include_default_core", True)),
                        },
                        decision="selected",
                        reason=routing_result.reason,
                    )
            except Exception as exc:  # routing must never break reconciliation
                trace_metadata["ontology_routing_error"] = f"{type(exc).__name__}: {exc}"
                execution_trace.add_event(
                    "ontology_routing",
                    "selected_ontologies_for_retrieval",
                    output_summary={"selected_ontologies_for_retrieval": list(ontologies or [])},
                    decision="routing_error_fallback_default",
                    reason=f"{type(exc).__name__}: {exc}",
                )
                routing_ontologies = list(ontologies or [])
        else:
            # Routing not used. Make the trace explicit about WHY (Phase 5):
            # routing_disabled vs broad-retrieval vs configured search.
            routing_disabled = not bool(getattr(effective_config, "enable_ontology_routing", False))
            trace_metadata["ontology_routing_used_llm"] = False
            if routing_disabled:
                execution_trace.add_event(
                    "ontology_routing",
                    "ontology_routing_skipped",
                    output_summary={
                        "selected_ontologies_for_retrieval": [],
                        "use_all_ontologies": False,
                    },
                    decision="routing_disabled",
                    reason="enable_ontology_routing=false",
                )
            if use_broad_registry_scored:
                trace_metadata["ontology_routing_mode"] = "broad_retrieval_registry_scored"
                trace_metadata["selected_ontologies_for_retrieval"] = []
                execution_trace.add_event(
                    "ontology_routing",
                    "selected_ontologies_for_retrieval",
                    output_summary={
                        "selected_ontologies_for_retrieval": [],
                        "routing_mode": "broad_retrieval_registry_scored",
                        "registry_used_as_retrieval_filter": False,
                    },
                    decision="broad_all_search" if not broad_registry_skip_retrieval else "placeholder_retrieval_skipped",
                    reason="broad BioPortal retrieval; registry applied after retrieval as scoring context",
                )
            else:
                trace_metadata["ontology_routing_mode"] = "configured_default" if routing_disabled else trace_metadata.get("ontology_routing_mode", "configured_default")
                trace_metadata["selected_ontologies_for_retrieval"] = list(routing_ontologies or [])
                execution_trace.add_event(
                    "ontology_routing",
                    "selected_ontologies_for_retrieval",
                    output_summary={
                        "selected_ontologies_for_retrieval": list(routing_ontologies or []),
                        "use_all_ontologies": False,
                        "routing_mode": trace_metadata["ontology_routing_mode"],
                        "include_default_core": False,
                    },
                    decision="configured_default",
                    reason="routing disabled; querying the configured ontology set without LLM routing",
                )

        if bioportal_api_key and not (use_broad_registry_scored and broad_registry_skip_retrieval):
            raw_candidate_scores: List[CandidateScore] = []
            if use_broad_registry_scored:
                # No per-ontology configured loop -- broad global search only.
                ontology_scan = []
            else:
                ontology_scan = _limited_ontologies(routing_ontologies, effective_config)
            trace_metadata["bioportal_ontologies_scanned"] = 0
            trace_metadata["bioportal_candidate_retrieval_errors"] = []
            trace_metadata["actual_preferred_ontologies_queried"] = None
            bioportal_search_started = time.perf_counter()
            per_ontology_trace: List[Dict[str, Any]] = []
            if use_broad_registry_scored:
                execution_trace.add_event(
                    "bioportal_retrieval",
                    "broad_bioportal_search_started",
                    input_summary={
                        "query": term,
                        "ontology_restriction": None,
                        "primary_term_type": broad_registry_term_profile.get("primary_term_type"),
                        "pool_limit": int(getattr(effective_config, "broad_registry_scored_pool_limit", 30) or 30),
                    },
                    decision="broad_all_search",
                    reason="broad retrieval without hard ontology restriction",
                )
            _search_phase_label = (
                "broad_all_search" if use_broad_registry_scored
                else "configured_search"
            )
            execution_trace.add_event(
                "bioportal_retrieval",
                "bioportal_search_started",
                input_summary={
                    "query": term,
                    "ontologies": list(ontology_scan or []),
                    "use_all_ontologies_direct": False,
                    "search_phase": _search_phase_label,
                },
            )
            execution_trace.add_event(
                "bioportal_retrieval",
                "actual_ontologies_queried",
                output_summary={
                    "phase": "primary",
                    "ontologies": list(ontology_scan or []),
                    "count": len(list(ontology_scan or [])),
                },
                decision="primary_search",
            )

            if use_broad_registry_scored:
                global_started = time.perf_counter()
                _broad_page = (
                    int(getattr(effective_config, "broad_registry_scored_pool_limit", 30) or 30)
                    if use_broad_registry_scored
                    else _global_candidate_pool_limit(effective_config)
                )
                try:
                    search_candidates = search_bioportal_candidates(
                        term,
                        api_key=bioportal_api_key,
                        ontologies=None,
                        page_size=_broad_page,
                    )
                    trace_metadata["bioportal_attempts"] = 1
                    trace_metadata["bioportal_global_search_candidate_count"] = len(search_candidates)
                    trace_metadata["direct_all_candidate_count"] = len(search_candidates)
                    execution_trace.add_api_call(
                        TraceApiCallSummary(
                            provider="BioPortal",
                            endpoint_or_operation="search",
                            ontology_acronym="",
                            query=term,
                            status="ok",
                            result_count=len(search_candidates),
                            duration_ms=round((time.perf_counter() - global_started) * 1000.0, 2),
                        )
                    )
                    for rank, candidate in enumerate(search_candidates):
                        raw_candidate_scores.append(_cheap_prerank_score(term, definition, candidate, effective_config, api_rank=rank))
                except Exception as exc:
                    trace_metadata["bioportal_global_search_error_type"] = type(exc).__name__
                    trace_metadata["bioportal_global_search_error_message"] = str(exc)[:300]
                    execution_trace.add_api_call(
                        TraceApiCallSummary(
                            provider="BioPortal",
                            endpoint_or_operation="search",
                            ontology_acronym="",
                            query=term,
                            status="error",
                            duration_ms=round((time.perf_counter() - global_started) * 1000.0, 2),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )

            for ontology in ontology_scan:
                ontology_started = time.perf_counter()
                raw_before_ontology = len(raw_candidate_scores)
                ontology_errors: List[Dict[str, Any]] = []
                trace_metadata["bioportal_attempts"] = int(trace_metadata.get("bioportal_attempts", 0)) + 1
                trace_metadata["bioportal_ontologies_scanned"] = int(trace_metadata.get("bioportal_ontologies_scanned", 0)) + 1
                per_ontology_rank = 0

                if _provider_is_trusted(ontology, effective_config):
                    best_def = find_term_in_ontology_with_definition(
                        term,
                        ontology,
                        exact=True,
                        case_sensitive=False,
                        api_key=bioportal_api_key,
                        allow_fallback=effective_config.trusted_fastpath_allow_non_exact_fallback,
                    )
                    if best_def:
                        mapped_id = best_def.get("mapped_id", "")
                        gate_passed = True
                        is_chebi = "CHEBI" in ontology.upper()

                        if effective_config.trusted_fastpath_requires_provider_evidence:
                            lexical_match = False
                            label = best_def.get("label", "").lower()
                            if label == term.lower() or term.lower() in [str(s).lower() for s in best_def.get("synonyms", [])]:
                                lexical_match = True
                            if is_chebi:
                                if "obo/CHEBI_" not in mapped_id and "CHEBI:" not in mapped_id:
                                    gate_passed = False
                                if best_def.get("acronym", "").upper() != "CHEBI":
                                    gate_passed = False
                            if not lexical_match:
                                gate_passed = False

                        if gate_passed:
                            trace_metadata["bioportal_trusted_shortcuts_used"] = int(
                                trace_metadata.get("bioportal_trusted_shortcuts_used", 0)
                            ) + 1
                            candidate = _candidate_from_definition_record(best_def, ontology, term)
                            raw_candidate_scores.append(_cheap_prerank_score(term, definition, candidate, effective_config, api_rank=per_ontology_rank))
                            per_ontology_rank += 1
                        else:
                            trace_metadata["bioportal_trusted_fastpath_gate_rejected"] = int(
                                trace_metadata.get("bioportal_trusted_fastpath_gate_rejected", 0)
                            ) + 1
                    else:
                        trace_metadata["bioportal_trusted_fastpath_fallback_used"] = int(
                            trace_metadata.get("bioportal_trusted_fastpath_fallback_used", 0)
                        ) + 1

                best_definition = find_best_definition(term, ontology, api_key=bioportal_api_key, exact=True)
                if best_definition:
                    candidate = _candidate_from_definition_record(best_definition, ontology, term)
                    raw_candidate_scores.append(_cheap_prerank_score(term, definition, candidate, effective_config, api_rank=per_ontology_rank))
                    per_ontology_rank += 1

                try:
                    search_candidates = search_bioportal_candidates(
                        term,
                        api_key=bioportal_api_key,
                        ontologies=[ontology],
                        page_size=_per_ontology_candidate_limit(effective_config),
                    )
                    for offset, candidate in enumerate(search_candidates[: _per_ontology_candidate_limit(effective_config)]):
                        raw_candidate_scores.append(
                            _cheap_prerank_score(
                                term,
                                definition,
                                candidate,
                                effective_config,
                                api_rank=per_ontology_rank + offset,
                            )
                        )
                except Exception as exc:
                    ontology_errors.append(
                        {
                            "ontology": ontology,
                            "error_type": type(exc).__name__,
                            "error_message": str(exc)[:300],
                        }
                    )
                    errors = trace_metadata.get("bioportal_candidate_retrieval_errors", [])
                    if isinstance(errors, list):
                        errors.append(
                            {
                                "ontology": ontology,
                                "error_type": type(exc).__name__,
                                "error_message": str(exc)[:300],
                            }
                        )
                        trace_metadata["bioportal_candidate_retrieval_errors"] = errors
                ontology_scores = raw_candidate_scores[raw_before_ontology:]
                include_raw_candidates = bool(getattr(effective_config, "trace_raw_candidates", False)) or trace_level_at_least(getattr(effective_config, "trace_level", "summary"), "forensic")
                top_raw = [summarize_candidate_score(score) for score in ontology_scores[:5]] if include_raw_candidates else []
                ontology_summary = {
                    "ontology": ontology,
                    "query": term,
                    "raw_candidate_count": len(ontology_scores),
                    "top_raw_candidates": top_raw,
                    "errors": ontology_errors,
                }
                per_ontology_trace.append(ontology_summary)
                execution_trace.add_api_call(
                    TraceApiCallSummary(
                        provider="BioPortal",
                        endpoint_or_operation="search",
                        ontology_acronym=ontology,
                        query=term,
                        status="error" if ontology_errors else "ok",
                        result_count=len(ontology_scores),
                        duration_ms=round((time.perf_counter() - ontology_started) * 1000.0, 2),
                        error="; ".join(error.get("error_message", "") for error in ontology_errors) or None,
                    )
                )

            if not raw_candidate_scores and ontologies:
                fallback_started = time.perf_counter()
                try:
                    search_candidates = search_bioportal_candidates(
                        term,
                        api_key=bioportal_api_key,
                        ontologies=ontology_scan[:5] if ontology_scan else None,
                        page_size=_global_candidate_pool_limit(effective_config),
                    )
                    for rank, fallback_candidate in enumerate(search_candidates):
                        raw_candidate_scores.append(_cheap_prerank_score(term, definition, fallback_candidate, effective_config, api_rank=rank))
                    trace_metadata["bioportal_search_fallback_used"] = bool(search_candidates)
                    execution_trace.add_api_call(
                        TraceApiCallSummary(
                            provider="BioPortal",
                            endpoint_or_operation="fallback_search",
                            ontology_acronym=",".join(ontology_scan[:5] if ontology_scan else []),
                            query=term,
                            status="ok",
                            result_count=len(search_candidates),
                            duration_ms=round((time.perf_counter() - fallback_started) * 1000.0, 2),
                        )
                    )
                except Exception as exc:
                    trace_metadata["bioportal_search_fallback_error_type"] = type(exc).__name__
                    trace_metadata["bioportal_search_fallback_error_message"] = str(exc)[:300]
                    execution_trace.add_api_call(
                        TraceApiCallSummary(
                            provider="BioPortal",
                            endpoint_or_operation="fallback_search",
                            ontology_acronym=",".join(ontology_scan[:5] if ontology_scan else []),
                            query=term,
                            status="error",
                            duration_ms=round((time.perf_counter() - fallback_started) * 1000.0, 2),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )

            trace_metadata["bioportal_raw_candidate_count"] = len(raw_candidate_scores)
            execution_trace.add_event(
                "bioportal_retrieval",
                "bioportal_search_completed",
                output_summary={
                    "ontology_results": per_ontology_trace,
                    "raw_candidate_count": len(raw_candidate_scores),
                    "errors": trace_metadata.get("bioportal_candidate_retrieval_errors", []),
                },
                decision="candidates_found" if raw_candidate_scores else "no_candidates",
                duration_ms=round((time.perf_counter() - bioportal_search_started) * 1000.0, 2),
            )
            deduped_scores = _dedupe_candidate_scores(raw_candidate_scores)
            trace_metadata["bioportal_candidate_count_after_dedupe"] = len(deduped_scores)
            execution_trace.add_event(
                "candidate_pool",
                "candidate_deduplication_completed",
                input_summary={"raw_count": len(raw_candidate_scores)},
                output_summary={
                    "deduplicated_count": len(deduped_scores),
                    "duplicates_removed": max(0, len(raw_candidate_scores) - len(deduped_scores)),
                },
            )
            ranked_candidates = _rank_preranked_scores(deduped_scores)
            global_limit = _global_candidate_pool_limit(effective_config)
            before_global_limit = ranked_candidates
            ranked_candidates = ranked_candidates[:global_limit]
            trace_metadata["bioportal_candidate_pool_limit"] = global_limit
            low_threshold = 0.10
            before_low_filter = list(ranked_candidates)
            # Phase 3: retain plausible non-exact candidates (e.g. FOODON packaging
            # classes for a packaging table) so they can reach LLM adjudication, even
            # when the cheap prerank gave them mapping_type 'none' / a low combined score.
            ranked_candidates = [
                score
                for score in ranked_candidates
                if score.mapping_type != "none"
                or _safe_confidence(score.combined_confidence, default=0.0) >= low_threshold
                or _candidate_is_plausible(score, term, definition, effective_config)[0]
            ]

            # ---- broad_retrieval_registry_scored: registry AS candidate scoring context ----
            # Runs even when the broad pool is empty -- an empty pool is the strongest
            # "weak" signal and must still be able to trigger the targeted core fallback.
            if use_broad_registry_scored:
                _min_suit = float(getattr(effective_config, "broad_registry_scored_min_suitability", 0.70) or 0.70)
                _amb_min_suit = float(getattr(effective_config, "broad_registry_scored_ambiguous_min_suitability", 0.85) or 0.85)
                _informative_min = float(getattr(effective_config, "broad_registry_scored_category_informative_min", 0.35) or 0.35)
                _primary_type = str(broad_registry_term_profile.get("primary_term_type") or "mixed_domain")
                _cat_conf = float(broad_registry_term_profile.get("term_type_confidence") or broad_registry_term_profile.get("category_confidence") or 0.0)
                _category_informative = _cat_conf >= _informative_min and _primary_type not in {"mixed_domain"}

                def _score_broad_candidates(cands):
                    for _s in cands:
                        _acr = _candidate_ontology_acronym(_s)
                        _md = getattr(_s, "trace_metadata", {}) or {}
                        _hard = str(_md.get("candidate_domain_mismatch_level", "")).strip().lower() == "hard"
                        try:
                            _reg = get_ontology_context(_acr, ensure=False)
                        except Exception:
                            _reg = None
                        _suit = _compute_ontology_suitability_score(
                            broad_registry_term_profile, _acr, _reg,
                            candidate_label=getattr(_s.candidate, "label", ""),
                            term=term,
                            candidate_definition=getattr(_s.candidate, "description", ""),
                            hard_domain_mismatch=_hard,
                        )
                        _lex = _safe_confidence(getattr(_s, "lexical_score", None), default=0.0)
                        _syn = 1.0 if str(_md.get("lexical_match_type", "")) == "exact_synonym" else 0.0
                        _defn = _safe_confidence(getattr(_s, "definition_score", None), default=0.0)
                        _prov = _safe_confidence(getattr(_s, "provider_score", None), default=0.0)
                        _penalties = (
                            _suit["broad_ontology_penalty"] + _suit["generic_label_penalty"]
                            + _suit["over_specific_candidate_penalty"] + _suit["exact_but_wrong_domain_penalty"]
                        )
                        _pre = _safe_confidence(
                            0.25 * _lex + 0.10 * _syn + 0.20 * _defn
                            + 0.20 * _suit["ontology_suitability_score"] + 0.15 * _suit["category_alignment_score"]
                            + 0.05 * _suit["ontology_quality_score"] + 0.05 * _prov - _penalties,
                            default=0.0,
                        )
                        _suit_score = _suit["ontology_suitability_score"]
                        # Calibrated 3-level registry evidence (do NOT hard-block merely
                        # low-suitability candidates; the term-type triage decides verified
                        # vs acceptable). Hard-block ONLY clearly-wrong candidates: a
                        # lexically-exact label sitting in a penalized/wrong-domain ontology
                        # (e.g. "olive oil" -> QUDT, "culture" -> a dosage-form ontology).
                        _hard_registry_block = _suit["exact_but_wrong_domain_penalty"] > 0.0
                        _soft_registry_penalty = (
                            bool(_suit["soft_domain_mismatch"])
                            or _suit["broad_ontology_penalty"] > 0.0
                            or (not _suit["registry_profile_found"])
                            or _suit["category_alignment_score"] < 0.40
                        )
                        _registry_support = (
                            _suit["term_type_alignment_score"] >= 0.70
                            or _suit["category_alignment_score"] >= 0.60
                        )
                        _s.trace_metadata.update(_suit)
                        _s.trace_metadata["candidate_pre_llm_score"] = round(float(_pre), 4)
                        _s.trace_metadata["registry_hard_block"] = bool(_hard_registry_block)
                        _s.trace_metadata["registry_soft_penalty"] = bool(_soft_registry_penalty)
                        _s.trace_metadata["registry_support"] = bool(_registry_support)
                        _s.trace_metadata["definition_similarity"] = round(
                            _token_overlap_ratio(definition, getattr(_s.candidate, "description", "") or getattr(_s.candidate, "label", "")), 4)
                        # registry_suitability_block (checked by the verified hard gate) now
                        # fires only for the hard case, so good exact/definition-supported
                        # candidates in mediocre-metadata ontologies are not killed.
                        _s.trace_metadata["registry_suitability_block"] = bool(_hard_registry_block)
                        _s.trace_metadata["category_context_informative"] = bool(_category_informative)
                        # Make the final best-candidate selection registry-aware even on
                        # the heuristic path (no LLM): fold ontology suitability into the
                        # context-score tie-break so a trustworthy-ontology candidate is
                        # preferred over an equal-lexical match in an unsuitable ontology.
                        if _category_informative and not _hard_registry_block:
                            _s.ontology_context_score = max(
                                _safe_confidence(getattr(_s, "ontology_context_score", None), default=0.0),
                                _suit["ontology_suitability_score"],
                            )

                execution_trace.add_event(
                    "candidate_scoring",
                    "candidate_registry_scoring_started",
                    input_summary={"candidate_count_before_registry_scoring": len(ranked_candidates),
                                   "primary_term_type": _primary_type},
                )
                trace_metadata["candidate_count_before_registry_scoring"] = len(ranked_candidates)
                _score_broad_candidates(ranked_candidates)
                # Rerank by registry/category-aware pre-LLM score.
                ranked_candidates.sort(
                    key=lambda s: _safe_confidence((s.trace_metadata or {}).get("candidate_pre_llm_score"), default=0.0),
                    reverse=True,
                )
                _registry_found = sum(1 for s in ranked_candidates if (s.trace_metadata or {}).get("registry_profile_found"))
                execution_trace.add_event(
                    "candidate_scoring",
                    "candidate_registry_scoring_completed",
                    output_summary={
                        "candidate_count_after_registry_scoring": len(ranked_candidates),
                        "registry_scoring_coverage": round(_registry_found / max(1, len(ranked_candidates)), 3),
                        "top_candidates": [
                            {
                                "label": getattr(s.candidate, "label", ""),
                                "ontology": _candidate_ontology_acronym(s),
                                "ontology_suitability_score": (s.trace_metadata or {}).get("ontology_suitability_score"),
                                "category_alignment_score": (s.trace_metadata or {}).get("category_alignment_score"),
                                "candidate_pre_llm_score": (s.trace_metadata or {}).get("candidate_pre_llm_score"),
                                "registry_suitability_block": (s.trace_metadata or {}).get("registry_suitability_block"),
                                "hard_domain_mismatch": (s.trace_metadata or {}).get("hard_domain_mismatch"),
                            }
                            for s in ranked_candidates[:8]
                        ],
                    },
                )
                execution_trace.add_event(
                    "candidate_pool",
                    "candidate_reranking_completed",
                    output_summary={"candidate_count": len(ranked_candidates), "rerank_key": "candidate_pre_llm_score"},
                )

                # ---- Optional targeted core fallback (Phase 2) ----
                _present = {_candidate_ontology_acronym(s) for s in ranked_candidates}
                _best_suit = max((_safe_confidence((s.trace_metadata or {}).get("ontology_suitability_score")) for s in ranked_candidates), default=0.0)
                _best_lex = max((_safe_confidence(getattr(s, "lexical_score", None)) for s in ranked_candidates), default=0.0)
                _core = [o for o in _term_type_core_ontologies(_primary_type) if o not in _present]
                # P7: never pull NCBITAXON unless the term is genuinely an organism/taxon
                # (a misclassified process like "pasteurization" must not trigger a
                # taxonomy fallback and add noise).
                _core = [o for o in _core if o != "NCBITAXON" or _primary_type == "organism_taxon"]
                # Weak = the broad pool lacks a trustworthy candidate (no exact match in a
                # sufficiently-suitable ontology). Combined with "_core missing from pool".
                _weak = _best_lex < 0.90 or _best_suit < _min_suit
                # P7: only spend a targeted fallback call when the term type is confident
                # enough to trust its core ontologies, OR the broad pool is empty (a real
                # gap). Low-confidence types must not drag in off-topic authorities.
                _type_conf = float(broad_registry_term_profile.get("term_type_confidence")
                                   or broad_registry_term_profile.get("category_confidence") or 0.0)
                _broad_pool_empty = len(ranked_candidates) == 0
                _type_confident = _type_conf >= 0.65
                _targeted_used = False
                if (
                    bool(getattr(effective_config, "broad_registry_scored_enable_targeted_fallback", True))
                    and _core and _weak and not broad_registry_skip_retrieval
                    and (_type_confident or _broad_pool_empty)
                ):
                    _fb_reason = "high_value_core_ontology_missing_from_broad_pool"
                    execution_trace.add_event(
                        "bioportal_retrieval", "targeted_core_fallback_started",
                        input_summary={"ontologies": _core, "primary_term_type": _primary_type},
                        decision="targeted_core_fallback", reason=_fb_reason,
                    )
                    _fb_started = time.perf_counter()
                    _added = 0
                    try:
                        _extra = search_bioportal_candidates(term, api_key=bioportal_api_key, ontologies=_core,
                                                             page_size=_global_candidate_pool_limit(effective_config))
                        for _rnk, _cand in enumerate(_extra):
                            _sc = _cheap_prerank_score(term, definition, _cand, effective_config, api_rank=_rnk)
                            _sc.from_fallback = True
                            raw_candidate_scores.append(_sc)
                        _added = len(_extra)
                        execution_trace.add_api_call(TraceApiCallSummary(
                            provider="BioPortal", endpoint_or_operation="targeted_core_fallback_search",
                            ontology_acronym=",".join(_core), query=term, status="ok", result_count=_added,
                            duration_ms=round((time.perf_counter() - _fb_started) * 1000.0, 2)))
                        _targeted_used = True
                        deduped_scores = _dedupe_candidate_scores(raw_candidate_scores)
                        ranked_candidates = _rank_preranked_scores(deduped_scores)[:global_limit]
                        before_global_limit = ranked_candidates
                        before_low_filter = list(ranked_candidates)
                        ranked_candidates = [
                            s for s in ranked_candidates
                            if s.mapping_type != "none"
                            or _safe_confidence(s.combined_confidence, default=0.0) >= low_threshold
                            or _candidate_is_plausible(s, term, definition, effective_config)[0]
                        ]
                        _score_broad_candidates(ranked_candidates)
                        ranked_candidates.sort(
                            key=lambda s: _safe_confidence((s.trace_metadata or {}).get("candidate_pre_llm_score"), default=0.0),
                            reverse=True,
                        )
                    except Exception as _exc:
                        execution_trace.add_api_call(TraceApiCallSummary(
                            provider="BioPortal", endpoint_or_operation="targeted_core_fallback_search",
                            ontology_acronym=",".join(_core), query=term, status="error",
                            duration_ms=round((time.perf_counter() - _fb_started) * 1000.0, 2),
                            error=f"{type(_exc).__name__}: {_exc}"))
                    execution_trace.add_event(
                        "bioportal_retrieval", "targeted_core_fallback_completed",
                        output_summary={"ontologies": _core, "candidates_added": _added, "candidate_count": len(ranked_candidates)},
                    )
                trace_metadata["targeted_fallback_used"] = bool(_targeted_used)
                trace_metadata["targeted_fallback_reason"] = "high_value_core_ontology_missing_from_broad_pool" if _targeted_used else ""

                _annotator_decision = should_run_annotator_rescue(
                    term=term,
                    term_profile=broad_registry_term_profile,
                    ranked_candidates=ranked_candidates,
                    config=effective_config,
                    bioportal_api_key=bioportal_api_key,
                    triage=None,
                    min_score=0.55,
                    stage="initial_broad_retrieval",
                    definition=definition,
                )
                _annotator_triggered, _annotator_reason = _annotator_decision
                if broad_registry_skip_retrieval:
                    _annotator_triggered = False
                    _annotator_reason = "broad_registry_skip_retrieval"
                    _annotator_trace_fields = {
                        **_annotator_decision.as_trace_metadata(),
                        "annotator_rescue_triggered": False,
                        "annotator_decision_reason": _annotator_reason,
                    }
                else:
                    _annotator_trace_fields = _annotator_decision.as_trace_metadata()
                trace_metadata.update(_annotator_trace_fields)
                trace_metadata["annotator_rescue_triggered"] = bool(_annotator_triggered)
                trace_metadata["annotator_rescue_reason"] = _annotator_reason
                execution_trace.add_event(
                    "annotator_rescue",
                    "annotator_rescue_trigger_decision",
                    input_summary={
                        "candidate_count": len(ranked_candidates),
                        "primary_term_type": _primary_type,
                    },
                    output_summary=_annotator_trace_fields,
                    decision="triggered" if _annotator_triggered else "skipped",
                    reason=_annotator_reason,
                )
                if _annotator_triggered:
                    _variants = _generate_annotator_rescue_variants(
                        term=term,
                        definition=definition,
                        term_profile=broad_registry_term_profile,
                        candidates=ranked_candidates,
                        config=effective_config,
                        stats=stats,
                        execution_trace=execution_trace,
                        run_id=run_id,
                        term_id=term_id,
                        row_index=row_index,
                    )
                    trace_metadata["annotator_rescue_variant_count"] = len(_variants)
                    trace_metadata["annotator_rescue_variants"] = [
                        {
                            "text": variant.text,
                            "kind": variant.kind,
                            "expected_strength": variant.expected_strength,
                        }
                        for variant in _variants
                    ]
                    execution_trace.add_event(
                        "annotator_rescue",
                        "annotator_rescue_variants_generated",
                        output_summary={"variant_count": len(_variants), "variants": trace_metadata["annotator_rescue_variants"]},
                    )
                    try:
                        _rescue_result = run_bioportal_annotator_rescue(
                            term=term,
                            definition=definition,
                            variants=_variants,
                            api_key=bioportal_api_key or "",
                            term_profile=broad_registry_term_profile,
                            max_candidates=int(getattr(effective_config, "annotator_rescue_max_candidates", 20) or 20),
                            timeout_seconds=float(getattr(effective_config, "annotator_rescue_timeout_seconds", 30) or 30),
                            debug_broad_ontologies=bool(
                                getattr(effective_config, "annotator_rescue_debug_broad_ontologies", False)
                            ),
                        )
                    except Exception as _exc:
                        _rescue_result = AnnotatorRescueResult(error=f"{type(_exc).__name__}: {_exc}")
                    _request_meta = _rescue_result.request_metadata or {}
                    _request_params = _request_meta.get("params") if isinstance(_request_meta.get("params"), dict) else {}
                    trace_metadata["annotator_rescue_used"] = True
                    trace_metadata["annotator_rescue_failed"] = bool(_rescue_result.error)
                    trace_metadata["annotator_rescue_error"] = _rescue_result.error
                    trace_metadata["annotator_rescue_annotation_count"] = _rescue_result.annotation_count
                    trace_metadata["annotator_rescue_grouped_candidate_count"] = _rescue_result.grouped_candidate_count
                    trace_metadata["annotator_rescue_candidate_count"] = len(_rescue_result.candidates or [])
                    trace_metadata["annotator_rescue_dropped_params"] = list(_rescue_result.dropped_params or [])
                    trace_metadata["annotator_rescue_request"] = _request_meta
                    trace_metadata["annotator_rescue_matched_variants"] = sorted({
                        str(ev.get("variant_text") or ev.get("matched_text") or "")
                        for candidate in (_rescue_result.candidates or [])
                        for ev in ((getattr(candidate, "ontology_context", {}) or {}).get("annotator_rescue_evidence") or [])
                        if isinstance(ev, dict) and str(ev.get("variant_text") or ev.get("matched_text") or "").strip()
                    })
                    execution_trace.add_api_call(
                        TraceApiCallSummary(
                            provider="BioPortal",
                            endpoint_or_operation="annotator",
                            ontology_acronym=str(_request_params.get("ontologies") or ""),
                            query=term,
                            status="error" if _rescue_result.error else "ok",
                            result_count=len(_rescue_result.candidates or []),
                            duration_ms=_request_meta.get("duration_ms"),
                            error=_rescue_result.error or None,
                        )
                    )
                    execution_trace.add_event(
                        "annotator_rescue",
                        "annotator_rescue_completed",
                        output_summary={
                            "candidate_count": len(_rescue_result.candidates or []),
                            "annotation_count": _rescue_result.annotation_count,
                            "grouped_candidate_count": _rescue_result.grouped_candidate_count,
                            "dropped_params": list(_rescue_result.dropped_params or []),
                            "matched_variants": trace_metadata["annotator_rescue_matched_variants"],
                        },
                        decision="merged" if _rescue_result.candidates else "no_candidates",
                        reason=_rescue_result.error or "",
                    )
                    if _rescue_result.candidates:
                        raw_candidate_scores = _merge_annotator_rescue_scores(
                            raw_candidate_scores,
                            list(_rescue_result.candidates or []),
                            term=term,
                            definition=definition,
                            config=effective_config,
                        )
                        deduped_scores = _dedupe_candidate_scores(raw_candidate_scores)
                        ranked_candidates = _rank_preranked_scores(deduped_scores)[:global_limit]
                        before_global_limit = ranked_candidates
                        before_low_filter = list(ranked_candidates)
                        ranked_candidates = [
                            s for s in ranked_candidates
                            if s.mapping_type != "none"
                            or _safe_confidence(s.combined_confidence, default=0.0) >= low_threshold
                            or _candidate_is_plausible(s, term, definition, effective_config)[0]
                        ]
                        _score_broad_candidates(ranked_candidates)
                        ranked_candidates.sort(
                            key=lambda s: _safe_confidence((s.trace_metadata or {}).get("candidate_pre_llm_score"), default=0.0),
                            reverse=True,
                        )
                        execution_trace.add_event(
                            "candidate_pool",
                            "annotator_rescue_candidates_merged",
                            output_summary={
                                "candidate_count": len(ranked_candidates),
                                "rescue_candidate_count": len(_rescue_result.candidates or []),
                                "top_candidates": [summarize_candidate_score(score) for score in ranked_candidates[:8]],
                            },
                        )
                trace_metadata["candidate_count_after_registry_scoring"] = len(ranked_candidates)

            trace_metadata["bioportal_candidates_after_prerank_filter"] = len(ranked_candidates)
            discarded_candidates: List[Dict[str, Any]] = []
            kept_keys = {str(score.candidate.uri or score.candidate.raw_identifier or id(score)) for score in ranked_candidates}
            for score in before_global_limit[global_limit:]:
                item = summarize_candidate_score(score)
                item["discard_reason"] = "above_global_candidate_pool_limit"
                discarded_candidates.append(item)
            for score in before_low_filter:
                key = str(score.candidate.uri or score.candidate.raw_identifier or id(score))
                if key not in kept_keys:
                    item = summarize_candidate_score(score)
                    item["discard_reason"] = "below_low_threshold"
                    discarded_candidates.append(item)
            if bool(getattr(effective_config, "trace_discarded_candidates", True)):
                trace_metadata["bioportal_discarded_candidates"] = discarded_candidates[:50]
            execution_trace.add_event(
                "candidate_pool",
                "candidate_prerank_completed",
                input_summary={
                    "pre_rank_input_count": len(deduped_scores),
                    "global_candidate_pool_limit": global_limit,
                    "low_threshold": low_threshold,
                },
                output_summary={
                    "output_count": len(ranked_candidates),
                    "top_candidates": [summarize_candidate_score(score) for score in ranked_candidates[:10]],
                    "discarded_candidates": discarded_candidates[:50] if bool(getattr(effective_config, "trace_discarded_candidates", True)) else [],
                },
            )

            context_limit = _shortlist_context_limit(effective_config)
            llm_limit = _shortlist_llm_limit(effective_config)
            shortlist_limit = max(context_limit, llm_limit, 1)
            shortlisted_scores = _build_adjudication_shortlist(
                ranked_candidates, term, definition, effective_config, shortlist_limit
            )
            trace_metadata["bioportal_shortlisted_count"] = len(shortlisted_scores)
            trace_metadata["bioportal_context_shortlist_limit"] = context_limit
            trace_metadata["bioportal_llm_shortlist_limit"] = llm_limit

            # Phase 3: send plausible non-exact candidates to adjudication even when
            # fewer than two survive the policy filters, so plausible packaging/material
            # candidates are not silently dropped to no_skos. Adjudication never
            # auto-verifies — the LLM may still return no_match.
            plausible_shortlist = [
                score for score in shortlisted_scores
                if _candidate_is_plausible(score, term, definition, effective_config)[0]
            ]
            trace_metadata["bioportal_plausible_shortlist_count"] = len(plausible_shortlist)
            adjudication_enabled = bool(getattr(effective_config, "enable_candidate_adjudication", True))
            # Adjudication-first requires >=2 candidates to compare (eligible OR plausible).
            # A SINGLE candidate keeps the per-candidate classify_skos_match path below so
            # an exact identity still gets an LLM SKOS decision (and can verify) instead of
            # falling through to the deterministic prerank score.
            use_adjudication_first = adjudication_enabled and (
                len(shortlisted_scores) >= 2 or len(plausible_shortlist) >= 2
            )
            trace_metadata["bioportal_adjudication_first"] = bool(use_adjudication_first)
            if not adjudication_enabled:
                _trigger_decision, _trigger_reason, _skipped = "adjudication_skipped", "adjudication_disabled", "adjudication_disabled"
            elif use_adjudication_first:
                _trigger_decision, _trigger_reason, _skipped = (
                    "adjudication_started",
                    "two_or_more_candidates" if len(shortlisted_scores) >= 2 else "two_or_more_plausible_candidates",
                    None,
                )
            elif len(shortlisted_scores) == 1:
                _trigger_decision, _trigger_reason, _skipped = "per_candidate_skos", "single_candidate_classified_directly", None
            else:
                _trigger_decision, _trigger_reason, _skipped = "adjudication_skipped", "no_candidate", "no_candidate"
            execution_trace.add_event(
                "candidate_adjudication",
                "candidate_adjudication_trigger_decision",
                input_summary={
                    "shortlisted_count": len(shortlisted_scores),
                    "plausible_candidate_count": len(plausible_shortlist),
                },
                output_summary={"skipped_reason": _skipped},
                decision=_trigger_decision,
                reason=_trigger_reason,
            )

            context_enrichment_batch_started = time.perf_counter()
            execution_trace.add_event(
                "context_enrichment",
                "context_enrichment_started",
                input_summary={
                    "shortlisted_count": len(shortlisted_scores),
                    "context_limit": context_limit,
                },
            )
            for index, pre_score in enumerate(shortlisted_scores):
                candidate = pre_score.candidate
                if index < context_limit:
                    candidate = _enrich_candidate_context(candidate)
                    pre_score.candidate = candidate
                    pre_score.ontology_context_score = _ontology_context_score(
                        term, definition, candidate, getattr(effective_config, "batch_domain_context", None)
                    )
                    pre_score.combined_confidence = _safe_confidence(
                        (0.40 * _safe_confidence(pre_score.lexical_score, default=0.0))
                        + (0.25 * _safe_confidence(pre_score.definition_score, default=0.0))
                        + (0.15 * _safe_confidence(pre_score.ontology_context_score, default=0.0))
                        + (0.10 * _safe_confidence(pre_score.provider_score, default=0.0))
                        + (0.10 * max(0.0, 1.0 - (int(pre_score.trace_metadata.get("api_rank", 0) or 0) * 0.10))),
                        default=pre_score.confidence,
                    )
                    _annotator_score = _safe_confidence(
                        (getattr(pre_score, "trace_metadata", {}) or {}).get("annotator_rescue_score"),
                        default=0.0,
                    )
                    if _annotator_score > 0.0:
                        pre_score.combined_confidence = max(pre_score.combined_confidence, _annotator_score)
                        pre_score.trace_metadata["confidence_supported_by_annotator_rescue"] = True
                    pre_score.trace_metadata["ontology_context_score"] = pre_score.ontology_context_score
                    pre_score.trace_metadata["combined_confidence"] = pre_score.combined_confidence
                    enriched_context = getattr(candidate, "ontology_context", {}) or {}
                    if isinstance(enriched_context, dict) and "obsolete" in enriched_context:
                        pre_score.trace_metadata["obsolete"] = bool(enriched_context.get("obsolete"))

                if use_adjudication_first:
                    _remember_score(pre_score)
                elif index < llm_limit:
                    stats.skos_calls_used += 1
                    stats.total_llm_calls_used += 1
                    decision = classify_skos_match(
                        term,
                        definition,
                        candidate.label,
                        candidate.description,
                        provider=effective_config.model_provider,
                        model_name=effective_config.model_name,
                        api_key_env=_resolve_model_api_key_env(effective_config),
                        allow_heuristic_fallback=bool(getattr(effective_config, "allow_heuristic_fallback", True)),
                        reasoning_effort=effective_config.reasoning_effort,
                        candidate_context=getattr(candidate, "ontology_context", None),
                    )
                    candidate_score = CandidateScore(
                        candidate=candidate,
                        mapping_type=decision.mapping_type,
                        confidence=_safe_confidence(getattr(decision, "confidence", None), default=0.0),
                        explanation_source=getattr(decision, "decision_source", "heuristic_fallback"),
                        from_fallback=bool(getattr(decision, "fallback_reason", None)),
                        explanation=getattr(decision, "explanation", "") or "",
                        skos_decision=decision,
                        trace_metadata=dict(pre_score.trace_metadata),
                    )
                    _populate_confidence_components(candidate_score, term, definition, effective_config)
                    _annotator_score = _safe_confidence(
                        candidate_score.trace_metadata.get("annotator_rescue_score"),
                        default=0.0,
                    )
                    if _annotator_score > 0.0:
                        candidate_score.combined_confidence = max(
                            _safe_confidence(candidate_score.combined_confidence, default=0.0),
                            _annotator_score,
                        )
                        candidate_score.trace_metadata["combined_confidence"] = candidate_score.combined_confidence
                        candidate_score.trace_metadata["confidence_supported_by_annotator_rescue"] = True
                    if effective_config.exact_match_requires_provider_lexical_gate and candidate_score.mapping_type == "exact":
                        if not _candidate_has_strong_identity_evidence(candidate_score, term):
                            candidate_score.mapping_type = "close"
                            if candidate_score.skos_decision:
                                candidate_score.skos_decision.mapping_type = "close"
                    _apply_provider_signal_boost(candidate_score, term)
                    _remember_score(candidate_score)
                    if enforce_verified_match and _score_meets_verified_policy(candidate_score, effective_config):
                        trace_metadata["bioportal_verified_match_found"] = True
                else:
                    _remember_score(pre_score)
            execution_trace.add_event(
                "context_enrichment",
                "context_enrichment_completed",
                output_summary={
                    "candidates": [trace_candidate_context_summary(score.candidate) for score in shortlisted_scores[:context_limit]],
                    "ontology_context_attempted": trace_metadata.get("ontology_context_attempted", 0),
                    "ontology_context_enriched": trace_metadata.get("ontology_context_enriched", 0),
                },
                duration_ms=round((time.perf_counter() - context_enrichment_batch_started) * 1000.0, 2),
            )

            # Ontology hierarchy/context assessment (broad mode): now that parents/
            # children/siblings/ancestors are enriched, assess whether each shortlisted
            # candidate sits in the CORRECT semantic branch. Wrong-branch senses are
            # blocked; branch support substitutes for a missing definition. Generic.
            if use_broad_registry_scored:
                execution_trace.add_event(
                    "hierarchy_context", "hierarchy_context_fetch_completed",
                    output_summary={"assessed_candidates": len(shortlisted_scores[:context_limit])},
                )
                _hier_used = False
                _hier_summaries = []
                for _hs in shortlisted_scores[:context_limit]:
                    _assess = _attach_hierarchy_assessment(_hs, term, definition, broad_registry_term_profile)
                    if _assess.get("hierarchy_context_available") or _assess.get("wrong_branch_warning"):
                        _hier_used = True
                    _hier_summaries.append({
                        "label": getattr(_hs.candidate, "label", ""),
                        "ontology": _candidate_ontology_acronym(_hs),
                        "hierarchy_domain_fit": _assess.get("hierarchy_domain_fit"),
                        "ontology_family_fit": _assess.get("ontology_family_fit"),
                        "granularity_from_hierarchy": _assess.get("granularity_from_hierarchy"),
                        "wrong_branch_warning": _assess.get("wrong_branch_warning"),
                        "parent_labels": _assess.get("parent_labels"),
                        "child_labels": _assess.get("child_labels"),
                        "sibling_labels": _assess.get("sibling_labels"),
                        "ancestor_labels": _assess.get("ancestor_labels"),
                        "ontology_branch_summary": _assess.get("ontology_branch_summary"),
                    })
                trace_metadata["hierarchy_used_in_decision"] = bool(_hier_used)
                execution_trace.add_event(
                    "hierarchy_context", "hierarchy_context_available",
                    output_summary={"hierarchy_used_in_decision": bool(_hier_used), "candidates": _hier_summaries},
                    decision="hierarchy_assessed" if _hier_used else "no_hierarchy_available",
                )

        if best_score is not None:
            adjudicated = _adjudicate_candidate_scores(
                term,
                definition,
                scored_candidates,
                effective_config,
                stats,
                trace_metadata,
                execution_trace,
                run_id=run_id,
                term_id=term_id,
                row_index=row_index,
            )
            if adjudicated is not None:
                best_score = adjudicated
            trace_metadata["bioportal_best_mapping_type"] = best_score.mapping_type
            trace_metadata["bioportal_best_confidence"] = float(best_score.confidence)
            trace_metadata["bioportal_best_combined_confidence"] = float(
                _safe_confidence(getattr(best_score, "combined_confidence", None), default=best_score.confidence)
            )
            trace_metadata["final_relation"] = best_score.mapping_type
            _best_md = getattr(best_score, "trace_metadata", {}) or {}
            trace_metadata["final_confidence_components"] = {
                "lexical_score": getattr(best_score, "lexical_score", None),
                "definition_score": getattr(best_score, "definition_score", None),
                "ontology_context_score": getattr(best_score, "ontology_context_score", None),
                "provider_score": getattr(best_score, "provider_score", None),
                "llm_confidence": getattr(best_score, "llm_confidence", None),
                "combined_confidence": getattr(best_score, "combined_confidence", None),
                "candidate_domain_mismatch_level": _best_md.get("candidate_domain_mismatch_level"),
                "domain_mismatch_reason": _best_md.get("domain_mismatch_reason"),
                "placeholder_or_status_value": _best_md.get("placeholder_or_status_value"),
                "provider_recognized": _best_md.get("provider_recognized"),
                "strong_exact_verified": bool(_meets_strong_exact_identity(best_score, effective_config)),
            }
            # Phase 2: the BioPortal candidate's gate is recorded under its own keys.
            # The authoritative FINAL gate is (re)computed in _finalize_decision_trace
            # on whichever candidate ultimately produces final_status.
            gate_trace = explain_verified_gate(best_score, effective_config)
            bioportal_provider = str(getattr(getattr(best_score, "candidate", None), "source_provider", "") or "")
            bioportal_uri = str(getattr(getattr(best_score, "candidate", None), "uri", "") or "")
            trace_metadata["verified_gate_input"] = gate_trace.get("verified_gate_input", {})
            trace_metadata["verified_gate_decision"] = gate_trace.get("verified_gate_decision")
            trace_metadata["verified_gate_failed_conditions"] = gate_trace.get("failed_conditions", [])
            trace_metadata["verified_gate_passed_conditions"] = gate_trace.get("passed_conditions", [])
            trace_metadata["bioportal_verified_gate_decision"] = gate_trace.get("verified_gate_decision")
            trace_metadata["bioportal_verified_gate_failed_conditions"] = gate_trace.get("failed_conditions", [])
            trace_metadata["bioportal_verified_gate_passed_conditions"] = gate_trace.get("passed_conditions", [])
            trace_metadata["bioportal_verified_gate_provider"] = bioportal_provider
            trace_metadata["bioportal_verified_gate_candidate_uri"] = bioportal_uri
            trace_metadata["bioportal_best_verified"] = bool(_score_meets_verified_policy(best_score, effective_config))
            execution_trace.add_event(
                "verification",
                "bioportal_verified_gate_applied",
                input_summary=gate_trace.get("verified_gate_input", {}),
                output_summary={
                    "decision": gate_trace.get("verified_gate_decision"),
                    "failed_conditions": gate_trace.get("failed_conditions", []),
                    "passed_conditions": gate_trace.get("passed_conditions", []),
                    "provider": bioportal_provider,
                    "candidate_uri": bioportal_uri,
                },
                decision=str(gate_trace.get("verified_gate_decision", "")),
                reason=", ".join(gate_trace.get("failed_conditions", []) or []),
            )

            # broad_retrieval_registry_scored: classify the best BioPortal candidate
            # (verified/acceptable/weak/unusable) using term-type-calibrated gates, and
            # store the verified decision so the verified-policy gate honours it.
            if use_broad_registry_scored:
                _triage, _triage_reason = broad_registry_candidate_triage(
                    best_score, broad_registry_term_profile.get("primary_term_type"), effective_config
                )
                trace_metadata["broad_registry_triage"] = _triage
                trace_metadata["broad_registry_triage_reason"] = _triage_reason
                best_score.trace_metadata["broad_registry_triage"] = _triage
                best_score.trace_metadata["broad_registry_verified"] = (_triage == "verified")
                # Ambiguous term whose best candidate is only a coincidental lexical
                # fragment (not exact, weak lexical) is semantically implausible -> do not
                # surface it as a suggestion (e.g. "strain" -> "Sprains and Strains").
                if (
                    str(broad_registry_term_profile.get("primary_term_type")) == "ambiguous_common_word"
                    and _triage != "verified"
                    and not _broad_best_is_exact(best_score)
                    and _safe_confidence(getattr(best_score, "lexical_score", None), default=0.0) < 0.60
                ):
                    best_score.trace_metadata["suppress_suggestion"] = True
                    best_score.trace_metadata["ambiguous_no_plausible_candidate"] = True
                    trace_metadata["ambiguous_suggestion_suppressed"] = True

                # Expensive second-pass rescue for hard/uncertain cases only. Enriches
                # candidates, derives abbreviation/definition expansions, re-adjudicates
                # and re-decides under contradiction + ontology-family gates. Never runs
                # for an already-verified match or a hard-blocked candidate.
                if enforce_verified_match and best_score is not None:
                    _prov_status = (
                        "verified" if _triage == "verified"
                        else "acceptable" if _triage == "acceptable"
                        else "weak"
                    )
                    _do_rescue, _rescue_reason = _should_run_rescue(
                        provisional_status=_prov_status, triage=_triage, best_score=best_score,
                        ranked_candidates=ranked_candidates, term=term,
                        term_profile=broad_registry_term_profile, config=effective_config,
                    )
                    if _do_rescue:
                        try:
                            _rescue_status, _rescue_best = _run_rescue_adjudication(
                                term=term, definition=definition, term_profile=broad_registry_term_profile,
                                candidates=ranked_candidates, config=effective_config,
                                bioportal_api_key=bioportal_api_key, execution_trace=execution_trace,
                                trace_metadata=trace_metadata, stats=stats, run_id=run_id,
                                term_id=term_id, row_index=row_index, enrich_fn=_enrich_candidate_context,
                                trigger_reason=_rescue_reason,
                            )
                        except Exception as _exc:  # rescue must never break reconciliation
                            trace_metadata["rescue_pass_error"] = f"{type(_exc).__name__}: {_exc}"
                            _rescue_status, _rescue_best = None, None
                        if _rescue_best is not None:
                            best_score = _rescue_best
                            _triage = (
                                "verified" if _rescue_status == "verified"
                                else "acceptable" if _rescue_status == "acceptable_suggestion"
                                else "weak"
                            )
                            trace_metadata["broad_registry_triage"] = _triage
                            best_score.trace_metadata["broad_registry_triage"] = _triage

            if not enforce_verified_match:
                return best_score

            if _score_meets_verified_policy(best_score, effective_config):
                return best_score

            trace_metadata["bioportal_best_rejected_by_verified_policy"] = True

            # broad mode: an ACCEPTABLE BioPortal candidate is kept as a suggestion and
            # must NOT trigger Wikidata (the key calibration: Wikidata only for real gaps).
            if use_broad_registry_scored:
                _triage = trace_metadata.get("broad_registry_triage")
                if _triage == "acceptable":
                    _blocked, _rejection = _blocked_by_reject_registry(trace_metadata, best_score)
                    if _blocked:
                        _mark_reject_registry_result(best_score, True, _rejection)
                        trace_metadata["blocked_by_reject_registry"] = True
                        trace_metadata["reject_reason"] = (_rejection or {}).get("reason", "")
                        execution_trace.add_event(
                            "verification",
                            "acceptable_candidate_blocked_by_reject_registry",
                            output_summary={"uri": _score_uri(best_score), "rejection": _rejection},
                            decision="blocked",
                            reason=str((_rejection or {}).get("reason") or ""),
                        )
                    else:
                        _mark_reject_registry_result(best_score, False, _rejection)
                    if _blocked:
                        return None
                    _ptype = str(broad_registry_term_profile.get("primary_term_type") or "")
                    if _ptype == "ambiguous_common_word":
                        _blocked_reason = "ambiguous_term_candidate_suggested"
                    elif _broad_best_is_exact(best_score):
                        _blocked_reason = "exact_bioportal_candidate_present"
                    else:
                        _blocked_reason = "acceptable_bioportal_candidate_present"
                    trace_metadata["wikidata_fallback_blocked"] = True
                    trace_metadata["wikidata_fallback_blocked_reason"] = _blocked_reason
                    execution_trace.add_event(
                        "wikidata_fallback",
                        "wikidata_fallback_decision",
                        output_summary={
                            "wikidata_fallback_considered": True,
                            "wikidata_fallback_attempted": False,
                            "reason": _blocked_reason,
                            "broad_registry_triage": _triage,
                        },
                        decision="blocked",
                        reason=_blocked_reason,
                    )
                    best_score.trace_metadata["force_suggestion"] = True
                    return best_score

                # no_match recalibration: a non-verified term is NOT a no_match if any of
                # the top candidates is plausible and not hard-rejected. Evaluate the
                # top-5, and return the best plausible one as candidate_suggested; only
                # fall through to Wikidata / no_match when everything is hard-rejected or
                # unusable ("no usable candidate exists" is a positive statement).
                _sel = _select_suggestion_candidate(
                    ranked_candidates, term, definition, broad_registry_term_profile, effective_config, top_n=5)
                trace_metadata["top_candidate_reject_reasons"] = _sel["reject_reasons"]
                trace_metadata["plausible_candidate_found"] = bool(_sel["plausible_found"])
                trace_metadata["no_match_requires_all_candidates_rejected"] = True
                if _sel["plausible_found"] and _sel["best"] is not None:
                    _plaus = _sel["best"]
                    _blocked, _rejection = _blocked_by_reject_registry(trace_metadata, _plaus)
                    if _blocked:
                        _mark_reject_registry_result(_plaus, True, _rejection)
                        trace_metadata["blocked_by_reject_registry"] = True
                        trace_metadata["reject_reason"] = (_rejection or {}).get("reason", "")
                        trace_metadata["plausible_candidate_blocked_by_reject_registry"] = str(
                            getattr(_plaus.candidate, "uri", "") or ""
                        )
                        execution_trace.add_event(
                            "verification",
                            "plausible_candidate_blocked_by_reject_registry",
                            output_summary={"uri": _score_uri(_plaus), "rejection": _rejection},
                            decision="blocked",
                            reason=str((_rejection or {}).get("reason") or ""),
                        )
                        trace_metadata["no_match_final_reason"] = "plausible_candidate_previously_rejected"
                    else:
                        _mark_reject_registry_result(_plaus, False, _rejection)
                    if _blocked:
                        return None
                    _floor = 0.55
                    _plaus.combined_confidence = max(_safe_confidence(getattr(_plaus, "combined_confidence", None), default=0.0), _floor)
                    _plaus.confidence = max(_safe_confidence(getattr(_plaus, "confidence", None), default=0.0), _floor)
                    if getattr(_plaus, "skos_decision", None) is not None:
                        _plaus.skos_decision.confidence = _plaus.confidence
                    _plaus.trace_metadata["broad_registry_verified"] = False
                    _plaus.trace_metadata["force_suggestion"] = True
                    trace_metadata["plausible_candidate_selected_for_suggestion"] = str(getattr(_plaus.candidate, "uri", "") or "")
                    trace_metadata["wikidata_fallback_blocked"] = True
                    trace_metadata["wikidata_fallback_blocked_reason"] = "plausible_bioportal_candidate_present"
                    execution_trace.add_event(
                        "verification", "plausible_candidate_selected",
                        output_summary={"uri": str(getattr(_plaus.candidate, "uri", "") or ""),
                                        "ontology": _candidate_ontology_acronym(_plaus),
                                        "reject_reasons": _sel["reject_reasons"]},
                        decision="candidate_suggested",
                        reason="plausible non-hard-rejected candidate found in top-5",
                    )
                    return _plaus
                trace_metadata["no_match_final_reason"] = (
                    "all_top_candidates_hard_rejected" if _sel["all_hard_rejected"] else "no_plausible_candidate"
                )

        # broad mode: placeholder/status terms are never externally verified and never
        # escalate to Wikidata.
        if use_broad_registry_scored and broad_registry_skip_retrieval:
            trace_metadata["wikidata_fallback_blocked"] = True
            trace_metadata["wikidata_fallback_blocked_reason"] = "placeholder_policy"
            execution_trace.add_event(
                "wikidata_fallback",
                "wikidata_fallback_decision",
                output_summary={
                    "wikidata_fallback_considered": True,
                    "wikidata_fallback_attempted": False,
                    "reason": "placeholder_policy",
                },
                decision="blocked",
                reason="placeholder_policy",
            )
            return None

        if not bool(getattr(effective_config, "enable_wikidata_fallback", True)):
            trace_metadata["provider_escalation_used"] = False
            trace_metadata["wikidata_fallback_disabled"] = True
            execution_trace.add_event(
                "wikidata_fallback",
                "wikidata_fallback_decision",
                output_summary={
                    "wikidata_fallback_considered": True,
                    "wikidata_fallback_attempted": False,
                    "reason": "wikidata_fallback_disabled",
                },
                decision="skipped",
                reason="wikidata fallback disabled",
            )
            return _suggested_best_score_or_none()

        trace_metadata["provider_escalation_used"] = True
        trace_metadata["provider_escalation_from"] = "BioPortal"
        trace_metadata["provider_escalation_to"] = "Wikidata"
        trace_metadata["provider_escalation_reason"] = (
            "bioportal_no_verified_match" if best_score else "bioportal_no_candidate"
        )
        trace_metadata["wikidata_second_pass_started"] = True
        trace_metadata["candidate_review_mode"] = effective_config.candidate_review_mode
        execution_trace.add_event(
            "wikidata_fallback",
            "wikidata_fallback_decision",
            output_summary={
                "wikidata_fallback_considered": True,
                "wikidata_fallback_attempted": True,
                "reason": trace_metadata["provider_escalation_reason"],
            },
            decision="attempted",
            reason=trace_metadata["provider_escalation_reason"],
        )

        wikidata_config = replace(
            effective_config,
            enforce_verified_match=False,
            allow_unverified_candidate_suggestions=True,
        )

        try:
            if related_wikidata_bias:
                wikidata_decision = run_wikidata_deep_agent(
                    term,
                    definition,
                    wikidata_config,
                    source_name=source_name,
                    run_id=run_id,
                    search_profile="focus_related",
                )
            else:
                wikidata_decision = run_wikidata_deep_agent(
                    term,
                    definition,
                    wikidata_config,
                    source_name=source_name,
                    run_id=run_id,
                )
        except WikidataRateLimitError as exc:
            _record_wikidata_fallback_unavailable(exc)
            return _suggested_best_score_or_none()
        trace_metadata["wikidata_fallback_used"] = True
        trace_metadata["wikidata_second_pass_status"] = getattr(wikidata_decision, "status", None)
        trace_metadata["wikidata_second_pass_has_candidate"] = bool(getattr(wikidata_decision, "candidate", None))
        trace_metadata["wikidata_second_pass_mapping_type"] = (
            wikidata_decision.skos.mapping_type if wikidata_decision.skos else None
        )
        trace_metadata["wikidata_second_pass_confidence"] = (
            wikidata_decision.skos.confidence if wikidata_decision.skos else None
        )
        trace_metadata["wikidata_second_pass_decision_source"] = (
            wikidata_decision.skos.decision_source if wikidata_decision.skos else None
        )
        trace_metadata["wikidata_second_pass_fallback_reason"] = (
            wikidata_decision.skos.fallback_reason if wikidata_decision.skos else None
        )
        execution_trace.add_api_call(
            TraceApiCallSummary(
                provider="Wikidata",
                endpoint_or_operation="run_wikidata_deep_agent",
                ontology_acronym="Wikidata",
                query=term,
                status=str(getattr(wikidata_decision, "status", "") or ""),
                result_count=1 if getattr(wikidata_decision, "candidate", None) else 0,
            )
        )
        execution_trace.add_event(
            "wikidata_fallback",
            "wikidata_fallback_decision",
            output_summary={
                "wikidata_fallback_considered": True,
                "wikidata_fallback_attempted": True,
                "wikidata_candidate_count": 1 if getattr(wikidata_decision, "candidate", None) else 0,
                "wikidata_selected": getattr(getattr(wikidata_decision, "candidate", None), "uri", None),
                "decision_source": trace_metadata.get("wikidata_second_pass_decision_source"),
            },
            decision=str(getattr(wikidata_decision, "status", "") or ""),
            reason=str(trace_metadata.get("wikidata_second_pass_fallback_reason") or ""),
        )
        if isinstance(getattr(wikidata_decision, "trace_metadata", None), dict):
            _merge_wikidata_fallback_trace(dict(wikidata_decision.trace_metadata))

        wikidata_candidate = wikidata_decision.candidate
        wikidata_skos = wikidata_decision.skos
        if wikidata_candidate is None or wikidata_skos is None:
            return _suggested_best_score_or_none()

        wikidata_score = CandidateScore(
            candidate=wikidata_candidate,
            mapping_type=wikidata_skos.mapping_type,
            confidence=_safe_confidence(getattr(wikidata_skos, "confidence", None), default=0.0),
            explanation_source=getattr(wikidata_skos, "decision_source", "heuristic_fallback"),
            from_fallback=bool(getattr(wikidata_skos, "fallback_reason", None)),
            explanation=getattr(wikidata_skos, "explanation", "") or "",
            skos_decision=wikidata_skos,
        )
        _populate_confidence_components(wikidata_score, term, definition, effective_config)
        _apply_provider_signal_boost(wikidata_score, term)
        scored_candidates.append(wikidata_score)
        adjudicated_with_wikidata = _adjudicate_candidate_scores(
            term,
            definition,
            scored_candidates,
            effective_config,
            stats,
            trace_metadata,
            execution_trace,
            run_id=run_id,
            term_id=term_id,
            row_index=row_index,
        )
        if adjudicated_with_wikidata is not None:
            wikidata_score = adjudicated_with_wikidata
        # Phase 2: record the Wikidata candidate's own verified gate.
        wikidata_gate = explain_verified_gate(wikidata_score, effective_config)
        trace_metadata["wikidata_verified_gate_decision"] = wikidata_gate.get("verified_gate_decision")
        trace_metadata["wikidata_verified_gate_failed_conditions"] = wikidata_gate.get("failed_conditions", [])
        trace_metadata["wikidata_verified_gate_passed_conditions"] = wikidata_gate.get("passed_conditions", [])
        trace_metadata["wikidata_verified_gate_provider"] = str(getattr(wikidata_candidate, "source_provider", "") or "Wikidata")
        trace_metadata["wikidata_verified_gate_candidate_uri"] = str(getattr(wikidata_candidate, "uri", "") or "")
        execution_trace.add_event(
            "verification",
            "wikidata_verified_gate_applied",
            input_summary=wikidata_gate.get("verified_gate_input", {}),
            output_summary={
                "decision": wikidata_gate.get("verified_gate_decision"),
                "failed_conditions": wikidata_gate.get("failed_conditions", []),
                "passed_conditions": wikidata_gate.get("passed_conditions", []),
                "provider": trace_metadata["wikidata_verified_gate_provider"],
                "candidate_uri": trace_metadata["wikidata_verified_gate_candidate_uri"],
            },
            decision=str(wikidata_gate.get("verified_gate_decision", "")),
            reason=", ".join(wikidata_gate.get("failed_conditions", []) or []),
        )
        if _score_meets_verified_policy(wikidata_score, effective_config) or _score_meets_suggestion_policy(wikidata_score, effective_config):
            trace_metadata["wikidata_second_pass_accepted_by_outer_policy"] = True
            return wikidata_score

        trace_metadata["wikidata_second_pass_rejected_by_outer_policy"] = True
        return _suggested_best_score_or_none()

    timed_out, result = run_with_timeout(_search_pipeline, effective_config.timeout_seconds)
    stats.elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    trace_metadata.update(stats.as_dict())
    if timed_out:
        timeout_decision = AgentDecision(
            term=term,
            definition=definition,
            candidate=None,
            skos=None,
            status="timeout",
            explanation="The BioPortal/Wikidata multi-agent workflow timed out.",
            run_id=run_id,
            source_name=source_name,
            trace_metadata={**trace_metadata, "status": "timeout"},
        )
        return _finalize_decision_trace(timeout_decision)
    if result is None:
        unavailable_notice = str(trace_metadata.get("notice") or "").strip()
        no_match_reason = "No candidate satisfied strict verification criteria across BioPortal and Wikidata."
        if unavailable_notice:
            no_match_reason = f"{no_match_reason} {unavailable_notice}"
        no_match_decision = _build_no_match_decision(
            term,
            definition,
            source_name,
            run_id,
            no_match_reason,
            workflow="bioportal_wikidata_multiagent",
            trace_metadata=trace_metadata,
        )
        return _finalize_decision_trace(no_match_decision)

    _merge_candidate_trace_metadata(result, trace_metadata)

    verified = _score_meets_verified_policy(result, effective_config)
    suggested = _score_meets_suggestion_policy(result, effective_config)
    if enforce_verified_match and not verified:
        if not suggested:
            no_match_decision = _build_no_match_decision(
                term,
                definition,
                source_name,
                run_id,
                "No candidate satisfied strict verified-match policy; returning No Match.",
                workflow="bioportal_wikidata_multiagent",
                trace_metadata={
                    **trace_metadata,
                    "verified_match_rejected": True,
                    "suggestion_policy_rejected": True,
                },
            )
            return _finalize_decision_trace(no_match_decision, result)

    decision = _candidate_to_decision(
        term,
        definition,
        result.candidate,
        source_name,
        "bioportal_wikidata_multiagent",
        run_id,
        effective_config,
        result,
    )
    decision.skos = result.skos_decision
    if result.skos_decision is not None:
        decision.explanation = result.skos_decision.explanation or decision.explanation

    if enforce_verified_match and not verified:
        decision.status = "candidate_suggested"
        if str(getattr(result.candidate, "source_provider", "") or "").strip().lower() == "wikidata":
            decision.explanation = (
                "Wikidata candidate found after BioPortal did not produce a verified match. "
                "The candidate did not satisfy the strict verified-match policy and requires manual review."
            )
        else:
            decision.explanation = (
                "Candidate found but it did not satisfy strict verified-match policy. "
                "Treat this as a suggestion requiring manual review."
            )
    if result.candidate is not None:
        decision.provider = result.candidate.source_provider

    decision.trace_metadata = {**trace_metadata, **getattr(decision, "trace_metadata", {}), "status": decision.status}
    return _finalize_decision_trace(decision, result)
