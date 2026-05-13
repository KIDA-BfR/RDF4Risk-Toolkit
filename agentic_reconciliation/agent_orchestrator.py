# -*- coding: utf-8 -*-
"""Orchestration logic for agent-based reconciliation workflows."""

from __future__ import annotations

import concurrent.futures
import json
import re
import threading
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


def run_with_timeout(func, timeout: int, *args, **kwargs):
    """Run a function with a timeout.

    Returns a tuple: (timed_out: bool, result: Any)

    Notes:
    - This enforces a *caller* timeout boundary and returns promptly when the
      deadline is exceeded.
    - It does not force-kill already running work inside `func` (Python threads
      cannot be preemptively terminated safely). Downstream providers should
      still use request-level timeouts and cooperative cancellation where
      possible.
    """
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(func, *args, **kwargs)
    try:
        return False, future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        # Best-effort cancellation for not-yet-started work.
        future.cancel()
        return True, None
    finally:
        # Do not wait on timeout: waiting would defeat the timeout boundary.
        executor.shutdown(wait=False, cancel_futures=True)


class _WorkflowAdmissionController:
    """Stagger per-term workflow starts while still allowing bounded parallel work.

    The provider modules already enforce request-level timeouts and, for
    Wikidata, endpoint-specific throttling/backoff. This controller adds a
    second safety layer at the orchestration boundary: parallel workers may run
    concurrently, but their externally-chatty workflows are admitted with a
    configurable minimum spacing so a batch cannot create an initial request
    burst against Wikidata, BioPortal, or LLM endpoints.
    """

    def __init__(self, min_interval_seconds: float = 0.0):
        try:
            interval = float(min_interval_seconds)
        except Exception:
            interval = 0.0
        self.min_interval_seconds = max(0.0, interval)
        self._lock = threading.Lock()
        self._next_start_at = 0.0

    def wait_for_turn(self) -> None:
        if self.min_interval_seconds <= 0:
            return

        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_start_at - now)
            self._next_start_at = max(now, self._next_start_at) + self.min_interval_seconds

        if wait_seconds > 0:
            time.sleep(wait_seconds)


def _coerce_positive_int(value: Any, default: int, *, upper_bound: Optional[int] = None) -> int:
    try:
        coerced = int(value)
    except Exception:
        coerced = int(default)
    coerced = max(1, coerced)
    if upper_bound is not None:
        coerced = min(coerced, int(upper_bound))
    return coerced


def _chunked(items: List[Any], chunk_size: int) -> Iterable[List[Any]]:
    for start in range(0, len(items), max(1, int(chunk_size))):
        yield items[start : start + max(1, int(chunk_size))]


def _mapping_priority(mapping_type: str) -> int:
    normalized = (mapping_type or "none").lower()
    return {"exact": 3, "close": 2, "related": 1, "none": 0}.get(normalized, 0)


def _resolve_model_api_key_env(config: AgentRunConfig) -> str:
    candidate = getattr(config, "model_api_key_env", None)
    if candidate and str(candidate).strip():
        return str(candidate).strip()
    legacy = getattr(config, "openai_api_key_env", None)
    if legacy and str(legacy).strip():
        return str(legacy).strip()
    return "OPENAI_API_KEY"


def _resolve_planner_provider(config: AgentRunConfig) -> str:
    provider = str(config.planner_model_provider or "").strip()
    return provider or config.model_provider


def _resolve_planner_model(config: AgentRunConfig) -> str:
    model_name = str(config.planner_model_name or "").strip()
    return model_name or config.model_name


def _resolve_planner_api_key_env(config: AgentRunConfig) -> str:
    planner_env = str(config.planner_model_api_key_env or "").strip()
    return planner_env or _resolve_model_api_key_env(config)


def _is_valid_qid(value: str) -> bool:
    return bool(re.fullmatch(r"Q\d+", str(value or "").strip().upper()))


def _safe_confidence(value: Any, default: float = 0.0) -> float:
    try:
        score = float(value)
    except Exception:
        score = float(default)
    return max(0.0, min(1.0, score))


def _score_meets_verified_policy(score: Optional[CandidateScore], config: AgentRunConfig) -> bool:
    """Return whether a candidate score satisfies strict verified-match policy gates."""
    if score is None:
        return False

    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    if mapping_type == "none":
        return False

    confidence = _safe_confidence(getattr(score, "confidence", 0.0), default=0.0)
    decision_source = str(getattr(score, "explanation_source", "") or "").strip().lower()
    from_fallback = bool(getattr(score, "from_fallback", False))

    if bool(getattr(config, "verified_match_require_exact", True)) and mapping_type != "exact":
        return False

    relation_specific_threshold = getattr(config, f"verified_match_min_confidence_{mapping_type}", None)
    min_confidence = _safe_confidence(
        relation_specific_threshold
        if relation_specific_threshold is not None
        else getattr(config, "verified_match_min_confidence", 0.80),
        default=0.80,
    )
    if confidence < min_confidence:
        return False

    if bool(getattr(config, "verified_match_require_llm_decision", False)) and decision_source != "llm":
        return False

    if bool(getattr(config, "verified_match_require_no_fallback", False)) and from_fallback:
        return False

    return True


def _build_no_match_decision(
    term: str,
    definition: str,
    source_name: str,
    run_id: str,
    reason: str,
    *,
    workflow: str,
    trace_metadata: Optional[Dict[str, Any]] = None,
) -> AgentDecision:
    """Create a normalized no-match decision with optional trace metadata."""
    explanation = str(reason or "No matching candidate passed verification.").strip()
    return AgentDecision(
        term=term,
        definition=definition,
        candidate=None,
        skos=None,
        status="no_match",
        explanation=explanation,
        run_id=run_id,
        source_name=source_name,
        trace_metadata={**(trace_metadata or {}), "workflow": workflow, "status": "no_match"},
    )


def _semantic_justification_for_decision(decision: AgentDecision, config: AgentRunConfig) -> str:
    """Return SSSOM mapping_justification value for semantic workflows.

    Guide alignment:
    - semantic similarity threshold-based matching should use
      ``semapv:SemanticSimilarityThresholdMatching`` when a score-driven semantic
      process made the mapping decision;
    - manual review/curation actions may override this later in UI flows.
    """
    skos = getattr(decision, "skos", None)
    if skos is None:
        return SEMANTIC_MAPPING_JUSTIFICATION

    fallback_reason = str(getattr(skos, "fallback_reason", "") or "").strip().lower()
    decision_source = str(getattr(skos, "decision_source", "") or "").strip().lower()
    confidence = _safe_confidence(getattr(skos, "confidence", None), default=0.0)
    auto_threshold = _safe_confidence(getattr(config, "auto_accept_min_confidence", 0.95), default=0.95)

    if decision_source == "heuristic_fallback" and fallback_reason in {"heuristic_similarity", "llm_error"}:
        if not bool(getattr(config, "allow_heuristic_fallback", True)):
            return "semapv:MappingReview"
        return SEMANTIC_MAPPING_JUSTIFICATION

    if confidence > 0.0:
        return SEMANTIC_MAPPING_JUSTIFICATION

    # Conservative default for semantic agent workflow.
    _ = auto_threshold
    return SEMANTIC_MAPPING_JUSTIFICATION


def _normalize_mapping_type(mapping_type: Any) -> str:
    normalized = str(mapping_type or "").strip().lower()
    if normalized in {"exact", "skos:exactmatch"}:
        return "exact"
    if normalized in {"close", "skos:closematch"}:
        return "close"
    if normalized in {"related", "skos:relatedmatch"}:
        return "related"
    return "none"


def _normalize_provider_token(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _provider_is_trusted(provider: Any, config: AgentRunConfig) -> bool:
    provider_token = _normalize_provider_token(provider)
    if not provider_token:
        return False
    trusted_tokens = {
        _normalize_provider_token(item)
        for item in (getattr(config, "trusted_ontologies", None) or [])
        if str(item or "").strip()
    }
    if not trusted_tokens:
        return False
    if provider_token in trusted_tokens:
        return True
    return any(token and token in provider_token for token in trusted_tokens)


def _token_overlap_ratio(text_a: str, text_b: str) -> float:
    tokens_a = {token for token in str(text_a or "").strip().lower().split() if token}
    tokens_b = {token for token in str(text_b or "").strip().lower().split() if token}
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def _string_similarity(term: str, label: str) -> float:
    left = str(term or "").strip().lower()
    right = str(label or "").strip().lower()
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return _token_overlap_ratio(left, right)


def _compute_auto_acceptance_score(
    *,
    term: str,
    candidate_label: str,
    mapping_type: str,
    confidence: float,
    decision_source: str,
    fallback_reason: str,
    provider: str,
    config: AgentRunConfig,
) -> float:
    normalized_mapping = _normalize_mapping_type(mapping_type)
    mapping_component = {
        "exact": 1.0,
        "close": 0.6,
        "related": 0.3,
        "none": 0.0,
    }.get(normalized_mapping, 0.0)
    lexical_similarity = _string_similarity(term, candidate_label)
    llm_component = 1.0 if str(decision_source or "").strip().lower() == "llm" else 0.0
    fallback_component = 1.0 if not str(fallback_reason or "").strip() else 0.0
    trusted_component = 1.0 if _provider_is_trusted(provider, config) else 0.0

    composite = (
        0.55 * _safe_confidence(confidence, default=0.0)
        + 0.20 * mapping_component
        + 0.15 * lexical_similarity
        + 0.05 * llm_component
        + 0.05 * fallback_component
    )
    if getattr(config, "auto_accept_trusted_ontologies_only", False):
        composite = 0.90 * composite + 0.10 * trusted_component
    else:
        composite = 0.95 * composite + 0.05 * trusted_component
    return _safe_confidence(composite, default=0.0)


def _evaluate_auto_accept(
    *,
    term: str,
    decision: AgentDecision,
    config: AgentRunConfig,
) -> tuple[bool, float, str]:
    if not getattr(config, "auto_accept_enabled", False):
        return False, 0.0, "auto_accept_disabled"

    candidate = getattr(decision, "candidate", None)
    skos = getattr(decision, "skos", None)
    if candidate is None or skos is None:
        return False, 0.0, "missing_candidate_or_skos"

    mapping_type = _normalize_mapping_type(getattr(skos, "mapping_type", ""))
    confidence = _safe_confidence(getattr(skos, "confidence", None), default=0.0)
    decision_source = str(getattr(skos, "decision_source", "") or "").strip().lower()
    fallback_reason = str(getattr(skos, "fallback_reason", "") or "").strip()
    provider = str(getattr(candidate, "source_provider", "") or "").strip()
    auto_score = _compute_auto_acceptance_score(
        term=term,
        candidate_label=str(getattr(candidate, "label", "") or ""),
        mapping_type=mapping_type,
        confidence=confidence,
        decision_source=decision_source,
        fallback_reason=fallback_reason,
        provider=provider,
        config=config,
    )

    failed_checks: List[str] = []
    if getattr(config, "auto_accept_require_exact_match", True) and mapping_type != "exact":
        failed_checks.append("requires_exact_match")
    if getattr(config, "auto_accept_require_llm_decision", True) and decision_source != "llm":
        failed_checks.append("requires_llm_decision")
    if getattr(config, "auto_accept_require_no_fallback", True) and fallback_reason:
        failed_checks.append("requires_no_fallback")
    if getattr(config, "auto_accept_trusted_ontologies_only", False) and not _provider_is_trusted(provider, config):
        failed_checks.append("requires_trusted_ontology")

    threshold = _safe_confidence(getattr(config, "auto_accept_min_confidence", 0.95), default=0.95)
    if auto_score < threshold:
        failed_checks.append("below_auto_accept_threshold")

    if failed_checks:
        return False, auto_score, ";".join(failed_checks)
    return True, auto_score, "auto_accept_policy_passed"


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

    return CandidateScore(
        candidate=candidate,
        mapping_type=decision.mapping_type,
        confidence=_safe_confidence(getattr(decision, "confidence", None), default=0.0),
        explanation_source=getattr(decision, "decision_source", "heuristic_fallback"),
        from_fallback=bool(getattr(decision, "fallback_reason", None)),
        explanation=getattr(decision, "explanation", "") or "",
        skos_decision=decision,
    )


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


ALLOWED_AGENTIC_ACTIONS = {
    "rewrite_query",
    "broaden_query",
    "narrow_query",
    "request_alias_search",
    "focus_related_concepts",
    "inspect_specific_qid",
}


def _generate_agentic_plan(
    term: str,
    definition: str,
    top_candidates: List[CandidateScore],
    config: AgentRunConfig,
    stats: AgenticExecutionStats,
) -> AgenticPlan:
    if stats.planner_calls_used >= max(0, int(config.agentic_max_planner_calls or 0)):
        return AgenticPlan(actions=[], stop_reason="planner_budget_exhausted", confidence_note="")

    planner_provider = _resolve_planner_provider(config)
    planner_model_name = _resolve_planner_model(config)
    planner_api_key_env = _resolve_planner_api_key_env(config)

    candidate_summary = [
        {
            "label": cs.candidate.label,
            "qid": cs.candidate.raw_identifier,
            "mapping_type": cs.mapping_type,
            "confidence": round(cs.confidence, 4),
        }
        for cs in (top_candidates or [])[:5]
    ]

    system_prompt = (
        "You are a constrained planner for Wikidata reconciliation. "
        "Return JSON only with keys actions (array), stop_reason, confidence_note."
    )
    user_prompt = (
        f"Input term: {term}\n"
        f"Definition: {definition}\n"
        f"Top candidates: {json.dumps(candidate_summary, ensure_ascii=False)}\n\n"
        "Allowed action_type values only: "
        "rewrite_query, broaden_query, narrow_query, request_alias_search, "
        "focus_related_concepts, inspect_specific_qid.\n"
        "Each action must be an object with keys action_type, payload, reason."
    )

    stats.planner_calls_used += 1
    stats.total_llm_calls_used += 1

    try:
        payload = generate_structured_completion(
            planner_provider,
            planner_model_name,
            system_prompt,
            user_prompt,
            api_key_env=planner_api_key_env,
            temperature=0,
            max_tokens=700,
            reasoning_effort=config.reasoning_effort,
            retries_on_parse_failure=1,
            interaction_purpose="planner",
            term_id=term,
        )
    except Exception:
        return AgenticPlan(actions=[], stop_reason="planner_error", confidence_note="")

    actions_payload = payload.get("actions", []) if isinstance(payload, dict) else []
    actions: List[AgenticPlanAction] = []
    if isinstance(actions_payload, list):
        for item in actions_payload:
            if not isinstance(item, dict):
                continue
            action_type = str(item.get("action_type", "")).strip()
            if action_type not in ALLOWED_AGENTIC_ACTIONS:
                continue
            raw_payload = item.get("payload", {})
            actions.append(
                AgenticPlanAction(
                    action_type=action_type,
                    payload=raw_payload if isinstance(raw_payload, dict) else {},
                    reason=str(item.get("reason", "") or ""),
                )
            )

    return AgenticPlan(
        actions=actions,
        stop_reason=str(payload.get("stop_reason", "") if isinstance(payload, dict) else "") or "planned",
        confidence_note=str(payload.get("confidence_note", "") if isinstance(payload, dict) else ""),
    )


def _execute_agentic_plan_actions(
    plan: AgenticPlan,
    term: str,
    config: AgentRunConfig,
    stats: AgenticExecutionStats,
) -> List[AgentCandidate]:
    generated: List[AgentCandidate] = []
    max_actions = max(0, int(config.agentic_max_tool_actions or 0))
    pool_limit = max(1, int(config.candidate_pool_limit or 1))

    for action in plan.actions:
        if stats.tool_actions_used >= max_actions:
            break
        stats.tool_actions_used += 1

        payload = action.payload or {}
        action_type = action.action_type

        if action_type == "inspect_specific_qid":
            qid = str(payload.get("qid", "")).strip().upper()
            if _is_valid_qid(qid):
                candidate = load_candidate_by_qid(qid)
                if candidate is not None:
                    generated.append(candidate)
            continue

        if action_type == "request_alias_search":
            aliases = payload.get("aliases", [])
            if isinstance(aliases, list):
                queries = [str(alias).strip() for alias in aliases if str(alias).strip()]
                generated.extend(
                    search_wikidata_candidates_multiquery(
                        queries,
                        per_query_limit=min(5, pool_limit),
                    )
                )
            continue

        query = str(payload.get("query", "")).strip() or term
        if action_type == "rewrite_query":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="default"))
        elif action_type == "broaden_query":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="broaden"))
        elif action_type == "narrow_query":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="narrow"))
        elif action_type == "focus_related_concepts":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="focus_related"))

    return dedupe_candidates(generated)


def _finalize_best_candidate(pool_scores: List[CandidateScore]) -> Optional[CandidateScore]:
    if not pool_scores:
        return None

    def _rank_key(item: CandidateScore):
        return (
            _mapping_priority(item.mapping_type),
            item.confidence,
            -1 if item.from_fallback else 0,
        )

    return max(pool_scores, key=_rank_key)


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
        verified_match_min_confidence_exact=0.75,
        verified_match_min_confidence_close=0.55,
        verified_match_min_confidence_related=0.35,
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

        try:
            if related_wikidata_bias:
                wikidata_decision = run_wikidata_deep_agent(
                    term,
                    definition,
                    effective_config,
                    source_name=source_name,
                    run_id=run_id,
                    search_profile="focus_related",
                )
            else:
                wikidata_decision = run_wikidata_deep_agent(
                    term,
                    definition,
                    effective_config,
                    source_name=source_name,
                    run_id=run_id,
                )
        except WikidataRateLimitError as exc:
            _record_wikidata_fallback_unavailable(exc)
            return best_score
        trace_metadata["wikidata_fallback_used"] = True
        if isinstance(getattr(wikidata_decision, "trace_metadata", None), dict):
            _merge_wikidata_fallback_trace(dict(wikidata_decision.trace_metadata))

        wikidata_candidate = wikidata_decision.candidate
        wikidata_skos = wikidata_decision.skos
        if wikidata_candidate is None or wikidata_skos is None:
            return None

        return CandidateScore(
            candidate=wikidata_candidate,
            mapping_type=wikidata_skos.mapping_type,
            confidence=_safe_confidence(getattr(wikidata_skos, "confidence", None), default=0.0),
            explanation_source=getattr(wikidata_skos, "decision_source", "heuristic_fallback"),
            from_fallback=bool(getattr(wikidata_skos, "fallback_reason", None)),
            explanation=getattr(wikidata_skos, "explanation", "") or "",
            skos_decision=wikidata_skos,
        )

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

    if enforce_verified_match and not _score_meets_verified_policy(result, effective_config):
        wikidata_fallback_unavailable = bool(trace_metadata.get("wikidata_fallback_unavailable", False))
        if not bool(getattr(effective_config, "allow_unverified_candidate_suggestions", True)) and not wikidata_fallback_unavailable:
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

    if enforce_verified_match and not _score_meets_verified_policy(result, effective_config):
        decision.status = "candidate_suggested"
        decision.explanation = (
            "Candidate found but it did not satisfy strict verified-match policy. "
            "Treat this as a suggestion requiring manual review."
        )

    decision.trace_metadata = {**trace_metadata, **getattr(decision, "trace_metadata", {}), "status": decision.status}
    return decision


def apply_agent_decision_to_dataframe(df: pd.DataFrame, row_index, decision: AgentDecision, config: AgentRunConfig) -> pd.DataFrame:
    df_out = df.copy()
    df_out.at[row_index, "Definition"] = decision.definition
    df_out.at[row_index, "Agent Explanation"] = decision.explanation
    df_out.at[row_index, "Agent Workflow"] = config.workflow
    df_out.at[row_index, "Run ID"] = decision.run_id

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
                "status": "ok" if decision.status == "matched" else "error",
            },
        ]
        return {
            "file": source_name,
            "term": term,
            "status": decision.status,
            "mapping_type": getattr(skos, "mapping_type", "") if skos else "",
            "suggested_uri": decision.candidate.uri if decision.candidate else "",
            "elapsed_ms": elapsed_ms,
            "error": "" if decision.status == "matched" else decision.explanation,
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
        if not term:
            return {
                "row_index": row_index,
                "term": term,
                "definition": definition,
                "decision": None,
                "elapsed_ms": 0.0,
                "skip": True,
            }

        admission_controller.wait_for_turn()
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
            completed_positions += 1
            if _apply_processed_result(_process_row(row_index), completed_positions):
                stopped_due_to_llm_error = True
                break
    else:
        for batch in _chunked(indices, batch_size):
            if stopped_due_to_llm_error:
                break
            batch_workers = min(effective_workers, len(batch))
            with concurrent.futures.ThreadPoolExecutor(max_workers=batch_workers) as executor:
                future_to_row = {executor.submit(_process_row, row_index): row_index for row_index in batch}
                for future in concurrent.futures.as_completed(future_to_row):
                    completed_positions += 1
                    result = future.result()
                    if _apply_processed_result(result, completed_positions):
                        stopped_due_to_llm_error = True
                # A stop condition applies between chunks. Already-started work
                # in this bounded chunk is allowed to finish to avoid abandoned
                # provider calls and partially-written dataframe state.

    if (
        config.workflow == "bioportal_wikidata_multiagent"
        and bool(getattr(config, "enable_second_pass_related_retry", False))
        and not stopped_due_to_llm_error
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
        )

        if state.stop_reason == "llm_error":
            break

        state.completed_files += 1
        if progress_callback:
            progress_callback(state)

    state.status = "stopped_llm_error" if state.stop_reason == "llm_error" else "completed"
    if progress_callback:
        progress_callback(state)
    return outputs
