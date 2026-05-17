# -*- coding: utf-8 -*-
"""Candidate scoring and policy gates for agent reconciliation workflows."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .agent_models import AgentDecision, AgentRunConfig, CandidateScore
from semi_automatic_reconciliation.shared_table_io import SEMANTIC_MAPPING_JUSTIFICATION


VERIFIED_THRESHOLDS = {"exact": 0.65, "close": 0.70, "related": 0.80}
CONSERVATIVE_SUGGESTION_THRESHOLDS = {"exact": 0.50, "close": 0.45, "related": 0.40}
EXPLORATORY_SUGGESTION_THRESHOLDS = {"exact": 0.30, "close": 0.25, "related": 0.15}


def _mapping_priority(mapping_type: str) -> int:
    normalized = (mapping_type or "none").lower()
    return {"exact": 3, "close": 2, "related": 1, "none": 0}.get(normalized, 0)


def _safe_confidence(value: Any, default: float = 0.0) -> float:
    try:
        score = float(value)
    except Exception:
        score = float(default)
    return max(0.0, min(1.0, score))


def _normalize_mapping_type(mapping_type: Any) -> str:
    normalized = str(mapping_type or "").strip().lower()
    if normalized in {"exact", "skos:exactmatch"}:
        return "exact"
    if normalized in {"close", "skos:closematch"}:
        return "close"
    if normalized in {"related", "skos:relatedmatch"}:
        return "related"
    return "none"


def _apply_provider_signal_boost(score: Optional[CandidateScore], term: str) -> Optional[CandidateScore]:
    if score is None:
        return None

    original_confidence = _safe_confidence(getattr(score, "confidence", 0.0), default=0.0)
    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    candidate_label = str(getattr(getattr(score, "candidate", None), "label", "") or "")
    lexical_exact = candidate_label.strip().lower() == str(term or "").strip().lower()
    boosted_confidence = original_confidence
    reason = ""

    if mapping_type == "exact" and lexical_exact:
        boosted_confidence = max(original_confidence, 0.85)
        reason = "exact_mapping_and_lexical_label_match"
    elif mapping_type == "exact":
        boosted_confidence = max(original_confidence, 0.70)
        reason = "exact_mapping"

    score.confidence = _safe_confidence(boosted_confidence, default=original_confidence)
    if getattr(score, "skos_decision", None) is not None:
        score.skos_decision.confidence = score.confidence

    metadata = {
        "provider_signal_boost_applied": bool(reason and score.confidence > original_confidence),
        "confidence_before_boost": original_confidence,
        "confidence_after_boost": score.confidence,
    }
    if reason:
        metadata["provider_signal_boost_reason"] = reason
    score.trace_metadata.update(metadata)
    return score


def _merge_candidate_trace_metadata(score: Optional[CandidateScore], trace_metadata: Dict[str, Any]) -> None:
    if score is None:
        return
    candidate_trace = getattr(score, "trace_metadata", {}) or {}
    if isinstance(candidate_trace, dict):
        trace_metadata.update(candidate_trace)


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
    configured_default = getattr(config, "verified_match_min_confidence", VERIFIED_THRESHOLDS.get(mapping_type, 0.80))
    min_confidence = _safe_confidence(
        relation_specific_threshold
        if relation_specific_threshold is not None
        else VERIFIED_THRESHOLDS.get(mapping_type, configured_default),
        default=VERIFIED_THRESHOLDS.get(mapping_type, 0.80),
    )
    if confidence < min_confidence:
        return False

    if bool(getattr(config, "verified_match_require_llm_decision", False)) and decision_source != "llm":
        return False

    if bool(getattr(config, "verified_match_require_no_fallback", False)) and from_fallback:
        return False

    return True


def _score_meets_suggestion_policy(score: Optional[CandidateScore], config: AgentRunConfig) -> bool:
    """Return whether a candidate is suitable to show for manual review."""
    if score is None:
        return False

    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    if mapping_type == "none":
        return False

    confidence = _safe_confidence(getattr(score, "confidence", 0.0), default=0.0)
    from_fallback = bool(getattr(score, "from_fallback", False))
    mode = str(getattr(config, "candidate_review_mode", "conservative") or "conservative").strip().lower()

    if mode == "exploratory":
        thresholds = EXPLORATORY_SUGGESTION_THRESHOLDS
        allow_fallback = True
    else:
        thresholds = CONSERVATIVE_SUGGESTION_THRESHOLDS
        allow_fallback = False

    if confidence < thresholds.get(mapping_type, 1.0):
        return False
    if not allow_fallback and from_fallback:
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
    """Return SSSOM mapping_justification value for semantic workflows."""
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

    _ = auto_threshold
    return SEMANTIC_MAPPING_JUSTIFICATION


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
