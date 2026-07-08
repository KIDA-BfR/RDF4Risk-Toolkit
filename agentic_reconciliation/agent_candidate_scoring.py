# -*- coding: utf-8 -*-
"""Candidate scoring and policy gates for agent reconciliation workflows."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .agent_models import AgentDecision, AgentRunConfig, CandidateScore
from semi_automatic_reconciliation.shared_table_io import SEMANTIC_MAPPING_JUSTIFICATION


VERIFIED_THRESHOLDS = {"exact": 0.80, "close": 0.75, "related": 0.85}
CONSERVATIVE_SUGGESTION_THRESHOLDS = {"exact": 0.55, "close": 0.55, "related": 0.50}
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


def _score_confidence(score: Any) -> float:
    return _safe_confidence(
        getattr(score, "combined_confidence", None)
        if getattr(score, "combined_confidence", None) is not None
        else getattr(score, "confidence", 0.0),
        default=0.0,
    )


def _apply_provider_signal_boost(score: Optional[CandidateScore], term: str) -> Optional[CandidateScore]:
    if score is None:
        return None

    original_confidence = _safe_confidence(getattr(score, "confidence", 0.0), default=0.0)
    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    candidate_label = str(getattr(getattr(score, "candidate", None), "label", "") or "")
    lexical_exact = candidate_label.strip().lower() == str(term or "").strip().lower()
    boosted_confidence = original_confidence
    reason = ""

    metadata = getattr(score, "trace_metadata", {}) or {}
    domain_penalized = bool(metadata.get("domain_penalized"))
    obsolete = bool(metadata.get("obsolete"))
    definition_score = _safe_confidence(getattr(score, "definition_score", None), default=0.0)
    has_definition = bool(str(getattr(getattr(score, "candidate", None), "description", "") or "").strip())
    exact_boost_allowed = not obsolete and not domain_penalized and (not has_definition or definition_score >= 0.10)

    if mapping_type == "exact" and lexical_exact and exact_boost_allowed:
        boosted_confidence = max(original_confidence, 0.85)
        reason = "exact_mapping_and_lexical_label_match"
    elif mapping_type == "exact" and exact_boost_allowed:
        boosted_confidence = max(original_confidence, 0.70)
        reason = "exact_mapping"

    score.confidence = _safe_confidence(boosted_confidence, default=original_confidence)
    if getattr(score, "skos_decision", None) is not None:
        score.skos_decision.confidence = score.confidence

    metadata = {
        "provider_signal_boost_applied": bool(reason and score.confidence > original_confidence),
        "confidence_before_boost": original_confidence,
        "confidence_after_boost": score.confidence,
        "provider_signal_boost_allowed": exact_boost_allowed if mapping_type == "exact" else True,
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


def _domain_mismatch_level(metadata: Dict[str, Any]) -> str:
    """Read the domain mismatch level, falling back to legacy domain_penalized=soft."""
    level = str(metadata.get("candidate_domain_mismatch_level", "") or "").strip().lower()
    if level in {"none", "soft", "hard"}:
        return level
    return "soft" if bool(metadata.get("domain_penalized")) else "none"


def _verified_hard_block(metadata: Dict[str, Any], config: AgentRunConfig) -> bool:
    """Conditions that must NEVER be auto-verified (may still be suggested):
    obsolete classes, HARD domain mismatch, and generic placeholder/status values.
    Soft domain mismatch (e.g. a material concept in a clinical/chemical ontology
    for a packaging table) does NOT block — it only dampened the context score."""
    if bool(metadata.get("obsolete")):
        return True
    if _domain_mismatch_level(metadata) == "hard" and not bool(getattr(config, "verified_match_allow_domain_penalized", False)):
        return True
    if bool(metadata.get("placeholder_or_status_value")) and not bool(getattr(config, "verified_match_allow_placeholder", False)):
        return True
    # broad_retrieval_registry_scored Phase 7: the candidate's ontology is not trusted
    # for this term/category (ontology_suitability_score below the mode threshold, or an
    # ambiguous common word without strong suitability). Set only by that mode.
    if bool(metadata.get("registry_suitability_block")):
        return True
    # Ontology-hierarchy evidence: the candidate sits in a branch (parents/siblings/
    # ancestors) whose domain contradicts the term sense -- the wrong concept of an
    # ambiguous label (e.g. a "river/finance/bench" branch for "bank"). Set only by the
    # hierarchy assessor; a compensating (near-)exact match in a strong ontology family
    # is exempted upstream, so this only fires for genuine wrong-branch cases.
    if bool(metadata.get("hierarchy_wrong_branch_block")):
        return True
    return False


def _meets_strong_exact_identity(score: CandidateScore, config: AgentRunConfig) -> bool:
    """Strong exact-identity match may verify even with weak definition/context
    evidence (e.g. material/polymer concepts whose ontology class has no definition).
    Requires an LLM-decided exactMatch with high LLM confidence, near-exact lexical
    identity and a recognized/trusted provider; never applies to obsolete / hard
    mismatch / placeholder candidates."""
    if not bool(getattr(config, "enable_strong_exact_verify", True)):
        return False
    if _normalize_mapping_type(getattr(score, "mapping_type", "")) != "exact":
        return False
    metadata = getattr(score, "trace_metadata", {}) or {}
    if _verified_hard_block(metadata, config):
        return False
    decision_source = str(getattr(score, "explanation_source", "") or "").strip().lower()
    if decision_source not in {"llm", "llm_adjudication"}:
        return False
    llm = _safe_confidence(getattr(score, "llm_confidence", None), default=0.0)
    lexical = _safe_confidence(getattr(score, "lexical_score", None), default=0.0)
    provider = _safe_confidence(getattr(score, "provider_score", None), default=0.0)
    if llm < float(getattr(config, "strong_exact_min_llm", 0.93)):
        return False
    if lexical < float(getattr(config, "strong_exact_min_lexical", 0.95)):
        return False
    if provider < float(getattr(config, "strong_exact_min_provider", 0.70)) and not bool(metadata.get("provider_recognized")):
        return False
    # Don't fast-verify if the model's own explanation flags a sense mismatch.
    explanation = str(getattr(getattr(score, "skos_decision", None), "explanation", "") or "").lower()
    if any(token in explanation for token in (
        "no match", "not a match", "not the same", "different entit", "unrelated",
        "ambiguous", "wrong sense", "not equivalent", "no_match",
    )):
        return False
    return True


def _score_meets_verified_policy(score: Optional[CandidateScore], config: AgentRunConfig) -> bool:
    """Return whether a candidate score satisfies strict verified-match policy gates."""
    if score is None:
        return False

    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    if mapping_type == "none":
        return False

    confidence = _score_confidence(score)
    decision_source = str(getattr(score, "explanation_source", "") or "").strip().lower()
    from_fallback = bool(getattr(score, "from_fallback", False))

    # Hard blocks: obsolete / hard domain mismatch / placeholder (never verified).
    metadata = getattr(score, "trace_metadata", {}) or {}
    if _verified_hard_block(metadata, config):
        return False

    # broad_retrieval_registry_scored: the term-type-calibrated triage precomputes the
    # verified decision and stores it here. When present it is authoritative (this flag
    # is only ever set by that mode, so other modes are unaffected).
    if "broad_registry_verified" in metadata:
        return bool(metadata.get("broad_registry_verified"))

    # Path A: strong exact-identity match verifies despite weak def/context evidence.
    if _meets_strong_exact_identity(score, config):
        return True

    # Path B: standard combined-confidence gate.
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

    if bool(getattr(config, "verified_match_require_llm_decision", False)) and decision_source not in {"llm", "llm_adjudication"}:
        return False

    if bool(getattr(config, "verified_match_require_no_fallback", False)) and from_fallback:
        return False

    return True


def _score_meets_suggestion_policy(score: Optional[CandidateScore], config: AgentRunConfig) -> bool:
    """Return whether a candidate is suitable to show for manual review."""
    if score is None:
        return False

    _md = getattr(score, "trace_metadata", {}) or {}
    # broad_retrieval_registry_scored: an ambiguous term whose only candidate is a
    # coincidental lexical fragment in the wrong domain (e.g. "strain" -> MeSH "Sprains
    # and Strains") must NOT be surfaced as a suggestion. Set only by that mode.
    if bool(_md.get("suppress_suggestion")):
        return False
    # A candidate deliberately selected via the broadened plausibility policy (top-N
    # resolution / rescue acceptable) is a valid suggestion regardless of the lexical
    # combined-confidence / no-fallback thresholds. Set only by that mode.
    if bool(_md.get("force_suggestion")):
        return True

    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    if mapping_type == "none":
        return False

    confidence = _score_confidence(score)
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
        evidence_confidence = _safe_confidence(
            getattr(item, "combined_confidence", None)
            if getattr(item, "combined_confidence", None) is not None
            else getattr(item, "confidence", 0.0),
            default=0.0,
        )
        context_score = _safe_confidence(getattr(item, "ontology_context_score", None), default=0.0)
        return (
            evidence_confidence,
            context_score,
            _mapping_priority(item.mapping_type),
            -1 if item.from_fallback else 0,
        )

    return max(pool_scores, key=_rank_key)


# ---------------------------------------------------------------------------
# broad_retrieval_registry_scored: BioPortal candidate triage (calibration).
#
# The decision is treated as a 4-level triage rather than a single hard threshold:
#   verified   -> final matched, no Wikidata
#   acceptable -> final candidate_suggested, no Wikidata
#   weak       -> Wikidata fallback allowed
#   unusable   -> Wikidata fallback allowed (except placeholder -> no_match)
# Verification is calibrated per term type so methods/measurements and clear exact
# terms are not blocked by mediocre ontology-suitability metadata.
# ---------------------------------------------------------------------------

_BROAD_MATERIAL_TYPES = {"material_substance", "packaging_component"}
_BROAD_METHOD_TYPES = {
    "assay_or_method", "measurement_property", "statistical_or_risk_concept",
    "analytical_measurement_property", "experimental_condition", "laboratory_process",
    "food_processing_method",
}
_BROAD_AMBIGUOUS_TYPES = {"ambiguous_common_word"}
_BROAD_PLACEHOLDER_TYPES = {"placeholder_or_status"}

# Per-gate thresholds (starting point; tune here without touching the workflow).
# strong_exact deliberately does NOT gate on combined_confidence: a clear exact/synonym
# match in a semantically-suitable ontology should verify even when the blended
# pre-LLM confidence is modest (that was the main source of verified->suggested drift).
BROAD_GATES = {
    "strong_exact": {"llm": 0.85, "suit": 0.55, "cat_align": 0.35, "def_ctx": 0.55},
    "method": {"llm": 0.88, "suit": 0.50, "def_ctx": 0.60},
    "material": {"llm": 0.88, "suit": 0.55},
    "ambiguous": {"llm": 0.92, "suit": 0.75, "def_ctx": 0.75},
}
BROAD_ACCEPTABLE = {"llm": 0.80, "suit": 0.50, "cat_align": 0.35, "combined": 0.62}


def _broad_llm_confidence(score: CandidateScore) -> float:
    llm = _safe_confidence(getattr(score, "llm_confidence", None), default=0.0)
    if llm > 0.0:
        return llm
    source = str(getattr(score, "explanation_source", "") or "").strip().lower()
    if source in {"llm", "llm_adjudication"}:
        return _safe_confidence(getattr(score, "confidence", None), default=0.0)
    skos = getattr(score, "skos_decision", None)
    if skos is not None and str(getattr(skos, "decision_source", "") or "").lower() in {"llm", "llm_adjudication"}:
        return _safe_confidence(getattr(skos, "confidence", None), default=0.0)
    return 0.0


def _broad_lexical_exact_or_synonym(score: CandidateScore) -> bool:
    metadata = getattr(score, "trace_metadata", {}) or {}
    if str(metadata.get("lexical_match_type", "")) in {"exact_label", "exact_synonym", "exact_id"}:
        return True
    return _safe_confidence(getattr(score, "lexical_score", None), default=0.0) >= 0.99


def broad_registry_candidate_triage(
    score: Optional[CandidateScore],
    primary_term_type: Optional[str],
    config: AgentRunConfig,
) -> tuple:
    """Classify the best BioPortal candidate as verified/acceptable/weak/unusable.

    Returns ``(triage, reason)``. Wikidata may only run for ``weak``/``unusable``
    (and never for ``unusable_placeholder``)."""
    if score is None:
        return "unusable", "no_bioportal_candidate"
    metadata = getattr(score, "trace_metadata", {}) or {}
    ptype = str(primary_term_type or "")

    if ptype in _BROAD_PLACEHOLDER_TYPES or bool(metadata.get("placeholder_or_status_value")) or bool(metadata.get("placeholder_policy_block")):
        return "unusable_placeholder", "placeholder_policy"

    candidate = getattr(score, "candidate", None)
    uri = str(getattr(candidate, "uri", "") or "").strip()
    label = str(getattr(candidate, "label", "") or "").strip()
    if not uri or not label:
        return "unusable", "missing_uri_or_label"

    # Hard exclusions can never be verified. They are still "weak" (not "unusable"),
    # so Wikidata is allowed to look for a better home for the term.
    if _verified_hard_block(metadata, config) or bool(metadata.get("obsolete")) or bool(metadata.get("hard_domain_mismatch")):
        return "weak", "hard_block_or_domain_mismatch"

    llm = _broad_llm_confidence(score)
    suit = _safe_confidence(metadata.get("ontology_suitability_score"), default=0.0)
    cat_align = _safe_confidence(metadata.get("category_alignment_score"), default=0.0)
    def_ctx = _safe_confidence(getattr(score, "definition_score", None), default=0.0)
    # "combined" is the candidate's best confidence estimate: the pre-LLM combined
    # confidence OR the (LLM-decided) final confidence, whichever is higher.
    combined = max(_score_confidence(score), _safe_confidence(getattr(score, "confidence", None), default=0.0))
    lexical = _safe_confidence(getattr(score, "lexical_score", None), default=0.0)
    exact = _broad_lexical_exact_or_synonym(score)
    exact_synonym = str(metadata.get("lexical_match_type", "")) == "exact_synonym"
    mapping = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    generic = _safe_confidence(metadata.get("generic_label_penalty"), default=0.0) > 0.0
    over_specific = _safe_confidence(metadata.get("over_specific_candidate_penalty"), default=0.0) > 0.0
    # Granularity: a candidate broader than the term (generic label) or clearly more
    # specific without context must not verify (P8), even if lexically close.
    broader = bool(metadata.get("candidate_broader_than_term"))
    over_specific = over_specific or bool(metadata.get("candidate_over_specific_no_context"))

    if ptype in _BROAD_AMBIGUOUS_TYPES:
        g = BROAD_GATES["ambiguous"]
        verified = exact and llm >= g["llm"] and suit >= g["suit"] and def_ctx >= g["def_ctx"] and not broader
        gate = "ambiguous"
    elif ptype in _BROAD_MATERIAL_TYPES:
        g = BROAD_GATES["material"]
        # A close match to a more precise class of the same material (e.g. "polyamide"
        # -> "polyamide macromolecule") verifies too, as long as it is not a broader/
        # generic/over-specific label.
        verified = (
            (exact or mapping == "close")
            and llm >= g["llm"] and suit >= g["suit"]
            and not generic and not over_specific and not broader
        )
        gate = "material"
    elif ptype in _BROAD_METHOD_TYPES:
        g = BROAD_GATES["method"]
        # An exact/synonym label match verifies WITHOUT a definition (many method/
        # measurement terms have no gloss, e.g. "limit of detection", "PCR"); def_ctx is
        # only required to promote a non-exact (close) match.
        verified = (
            llm >= g["llm"] and suit >= g["suit"] and not broader and not over_specific
            and (exact or (mapping in {"exact", "close"} and def_ctx >= g["def_ctx"]))
        )
        gate = "method"
    else:
        # A clear exact/synonym match in a semantically-suitable ontology verifies on
        # LLM + suitability + category alignment. Definition context is NOT required for
        # an exact match (many controlled terms have no gloss); it only helps close
        # matches (handled by the method gate). exact_synonym/def_ctx are retained as
        # informative signals but do not add a blocking condition here.
        g = BROAD_GATES["strong_exact"]
        _ = (def_ctx, exact_synonym)
        verified = (
            exact and llm >= g["llm"] and suit >= g["suit"] and cat_align >= g["cat_align"]
            and not broader and not over_specific
        )
        gate = "strong_exact"

    if verified:
        return "verified", f"{gate}_gate_passed"

    a = BROAD_ACCEPTABLE
    acceptable = (
        llm >= a["llm"]
        and suit >= a["suit"]
        and cat_align >= a["cat_align"]
        and combined >= a["combined"]
        and (exact or lexical >= 0.60)
    )
    if acceptable:
        return "acceptable", "acceptable_bioportal_candidate"

    if suit < 0.45:
        return "weak", "low_ontology_suitability"
    if llm < 0.75:
        return "weak", "low_llm_confidence"
    return "weak", "below_acceptable_thresholds"
