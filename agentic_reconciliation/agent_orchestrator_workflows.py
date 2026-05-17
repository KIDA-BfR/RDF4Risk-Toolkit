# -*- coding: utf-8 -*-
"""Workflow-specific agent reconciliation implementations."""

from __future__ import annotations

import time
import uuid
from dataclasses import replace
from typing import Any, Dict, List, Optional

from .agent_bioportal_service import (
    find_best_definition,
    find_term_in_ontology_with_definition,
    recommend_ontology_acronyms,
    search_bioportal_candidates,
)
from .agent_llm_service import generate_structured_completion as _default_generate_structured_completion
from . import agent_orchestrator_agentic_planning as _agentic_planning
from .agent_candidate_scoring import (
    VERIFIED_THRESHOLDS,
    _apply_provider_signal_boost,
    _build_no_match_decision,
    _finalize_best_candidate,
    _mapping_priority,
    _merge_candidate_trace_metadata,
    _provider_is_trusted,
    _safe_confidence,
    _score_meets_suggestion_policy,
    _score_meets_verified_policy,
)
from .agent_models import (
    AgentCandidate,
    AgentDecision,
    AgentRunConfig,
    AgenticExecutionStats,
    CandidateScore,
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
    if config.enable_skos_matching:
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
        )
        explanation = skos_decision.explanation or explanation

    return AgentDecision(
        term=term,
        definition=definition,
        candidate=candidate,
        skos=skos_decision,
        status="matched",
        explanation=explanation,
        run_id=run_id,
        source_name=source_name,
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
        for rank, candidate in enumerate(baseline_pool[: max(1, int(config.max_iterations or 1))]):
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
    decision = _candidate_to_decision(term, definition, best_candidate, source_name, "wikidata_deep_agent", run_id, config)
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


def run_bioportal_wikidata_multiagent(
    term: str,
    definition: str,
    config: AgentRunConfig,
    bioportal_api_key: Optional[str] = None,
    source_name: str = "input",
    run_id: Optional[str] = None,
    related_wikidata_bias: bool = False,
) -> AgentDecision:
    run_id = run_id or str(uuid.uuid4())
    started = time.perf_counter()
    stats = AgenticExecutionStats()
    effective_config = _build_notebook_faithful_multiagent_config(config)
    enforce_verified_match = bool(getattr(effective_config, "enforce_verified_match", False))
    trace_metadata: Dict[str, Any] = {
        "workflow": "bioportal_wikidata_multiagent",
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

    def _search_pipeline() -> Optional[CandidateScore]:
        best_score: Optional[CandidateScore] = None
        best_priority = -1

        def _suggested_best_score_or_none() -> Optional[CandidateScore]:
            if _score_meets_suggestion_policy(best_score, effective_config):
                return best_score
            if best_score is not None:
                trace_metadata["suggestion_policy_rejected"] = True
            return None

        ontologies = effective_config.bioportal_agent_ontologies
        if bioportal_api_key and not ontologies:
            ontologies = recommend_ontology_acronyms([term], bioportal_api_key, min_valid=5)

        if bioportal_api_key:
            for ontology in ontologies[: effective_config.max_iterations]:
                trace_metadata["bioportal_attempts"] = int(trace_metadata.get("bioportal_attempts", 0)) + 1

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
                            candidate = AgentCandidate(
                                uri=mapped_id,
                                label=best_def.get("label") or term,
                                description=best_def.get("definition") or best_def.get("label") or term,
                                source_provider=ontology,
                                source_workflow="bioportal_wikidata_multiagent",
                                raw_identifier=mapped_id,
                            )
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
                            )
                            candidate_score = CandidateScore(
                                candidate=candidate,
                                mapping_type=decision.mapping_type,
                                confidence=_safe_confidence(getattr(decision, "confidence", None), default=0.0),
                                explanation_source=getattr(decision, "decision_source", "heuristic_fallback"),
                                from_fallback=bool(getattr(decision, "fallback_reason", None)),
                                explanation=getattr(decision, "explanation", "") or "",
                                skos_decision=decision,
                            )
                            
                            if effective_config.exact_match_requires_provider_lexical_gate and candidate_score.mapping_type == "exact":
                                if not lexical_match or (is_chebi and gate_passed is False):
                                    candidate_score.mapping_type = "close"
                                    if candidate_score.skos_decision:
                                        candidate_score.skos_decision.mapping_type = "close"
                            _apply_provider_signal_boost(candidate_score, term)
                            
                            priority = _mapping_priority(candidate_score.mapping_type)
                            if priority > best_priority:
                                best_priority = priority
                                best_score = candidate_score
                            elif priority == best_priority and best_score is not None:
                                if candidate_score.confidence > best_score.confidence:
                                    best_score = candidate_score

                            if enforce_verified_match and _score_meets_verified_policy(candidate_score, effective_config):
                                trace_metadata["bioportal_verified_match_found"] = True
                                return candidate_score
                    continue

                best_definition = find_best_definition(term, ontology, api_key=bioportal_api_key, exact=True)
                if not best_definition:
                    continue
                candidate = AgentCandidate(
                    uri=best_definition.get("mapped_id", ""),
                    label=best_definition.get("label", term),
                    description=best_definition.get("definition", "") or best_definition.get("label", term),
                    source_provider=ontology,
                    source_workflow="bioportal_wikidata_multiagent",
                    raw_identifier=best_definition.get("mapped_id", ""),
                )
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
                )
                candidate_score = CandidateScore(
                    candidate=candidate,
                    mapping_type=decision.mapping_type,
                    confidence=_safe_confidence(getattr(decision, "confidence", None), default=0.0),
                    explanation_source=getattr(decision, "decision_source", "heuristic_fallback"),
                    from_fallback=bool(getattr(decision, "fallback_reason", None)),
                    explanation=getattr(decision, "explanation", "") or "",
                    skos_decision=decision,
                )
                _apply_provider_signal_boost(candidate_score, term)
                priority = _mapping_priority(candidate_score.mapping_type)
                if priority > best_priority:
                    best_priority = priority
                    best_score = candidate_score
                elif priority == best_priority and best_score is not None:
                    if candidate_score.confidence > best_score.confidence:
                        best_score = candidate_score
                if not enforce_verified_match and priority >= 2:
                    return best_score

                if enforce_verified_match and _score_meets_verified_policy(candidate_score, effective_config):
                    trace_metadata["bioportal_verified_match_found"] = True
                    return candidate_score

            if best_score is None:
                search_candidates = search_bioportal_candidates(term, api_key=bioportal_api_key, ontologies=ontologies[:5] if ontologies else None)
                if search_candidates:
                    fallback_candidate = search_candidates[0]
                    fallback_score = _score_candidate(
                        term,
                        definition,
                        fallback_candidate,
                        effective_config,
                        stats=stats,
                    )
                    best_score = fallback_score
                    trace_metadata["bioportal_search_fallback_used"] = True

        if best_score is not None:
            trace_metadata["bioportal_best_mapping_type"] = best_score.mapping_type
            trace_metadata["bioportal_best_confidence"] = float(best_score.confidence)
            trace_metadata["bioportal_best_verified"] = bool(_score_meets_verified_policy(best_score, effective_config))

            if not enforce_verified_match:
                return best_score

            if _score_meets_verified_policy(best_score, effective_config):
                return best_score

            trace_metadata["bioportal_best_rejected_by_verified_policy"] = True

        trace_metadata["provider_escalation_used"] = True
        trace_metadata["provider_escalation_from"] = "BioPortal"
        trace_metadata["provider_escalation_to"] = "Wikidata"
        trace_metadata["provider_escalation_reason"] = (
            "bioportal_no_verified_match" if best_score else "bioportal_no_candidate"
        )
        trace_metadata["wikidata_second_pass_started"] = True
        trace_metadata["candidate_review_mode"] = effective_config.candidate_review_mode

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
        _apply_provider_signal_boost(wikidata_score, term)
        if _score_meets_verified_policy(wikidata_score, effective_config) or _score_meets_suggestion_policy(wikidata_score, effective_config):
            trace_metadata["wikidata_second_pass_accepted_by_outer_policy"] = True
            return wikidata_score

        trace_metadata["wikidata_second_pass_rejected_by_outer_policy"] = True
        return _suggested_best_score_or_none()

    timed_out, result = run_with_timeout(_search_pipeline, effective_config.timeout_seconds)
    stats.elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    trace_metadata.update(stats.as_dict())
    if timed_out:
        return AgentDecision(
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
    if result is None:
        unavailable_notice = str(trace_metadata.get("notice") or "").strip()
        no_match_reason = "No candidate satisfied strict verification criteria across BioPortal and Wikidata."
        if unavailable_notice:
            no_match_reason = f"{no_match_reason} {unavailable_notice}"
        return _build_no_match_decision(
            term,
            definition,
            source_name,
            run_id,
            no_match_reason,
            workflow="bioportal_wikidata_multiagent",
            trace_metadata=trace_metadata,
        )

    _merge_candidate_trace_metadata(result, trace_metadata)

    verified = _score_meets_verified_policy(result, effective_config)
    suggested = _score_meets_suggestion_policy(result, effective_config)
    if enforce_verified_match and not verified:
        if not suggested:
            return _build_no_match_decision(
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

    decision = _candidate_to_decision(
        term,
        definition,
        result.candidate,
        source_name,
        "bioportal_wikidata_multiagent",
        run_id,
        effective_config,
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
    return decision
