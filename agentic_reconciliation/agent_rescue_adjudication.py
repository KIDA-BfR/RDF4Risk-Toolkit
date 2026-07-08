# -*- coding: utf-8 -*-
"""Rescue adjudication (expensive second-pass quality layer) for the
broad_retrieval_registry_scored mode.

This module holds the *pure, deterministic* logic of the rescue pass so it is fully
unit-testable offline:

* :func:`should_run_rescue` -- decide (generically) whether a hard case warrants the
  slow path (unexpected no_match, high-confidence suggestion, exact/close candidate that
  failed the gate, abbreviation/gene/namespace-like or ambiguous term, LLM-vs-gate
  disagreement).
* :func:`derive_abbreviation_expansions` -- infer expansions from candidate labels/
  synonyms/definitions (e.g. "PCR" -> "polymerase chain reaction"), never hard-coded.
* :func:`derive_definition_queries` -- derive noun-phrase queries from the definition.
* :func:`semantic_contradiction` -- generic domain/definition contradiction check.
* :func:`ontology_family_ok` -- does the candidate ontology family fit the term type.
* :func:`rescue_three_way_decision` -- verified / acceptable_suggestion / reject, with
  reasons. Contradiction and family are HARD gates the LLM may not override.

The I/O orchestration (extra BioPortal searches, context enrichment, the comparative LLM
call, trace events) lives in the workflow, which calls into these helpers.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .agent_candidate_scoring import (
    _broad_lexical_exact_or_synonym,
    _broad_llm_confidence,
    _normalize_mapping_type,
    _safe_confidence,
    _score_confidence,
    _verified_hard_block,
)
from .agent_table_context_service import (
    CATEGORY_ONTOLOGY_PRIORS,
    GENERIC_HEAD_LABELS,
    _normalize_search_text,
    categorize_term,
)

__all__ = [
    "should_run_rescue",
    "is_hard_term",
    "derive_abbreviation_expansions",
    "derive_definition_queries",
    "semantic_contradiction",
    "ontology_family_ok",
    "rescue_three_way_decision",
    "candidate_evidence_summary",
    "hard_reject_reason",
    "is_plausible_suggestion",
    "select_suggestion_candidate",
    "assess_candidate_hierarchy_context",
    "attach_hierarchy_assessment",
]

# Suggestion plausibility thresholds (generic; not lexical-only).
_SUGGEST_COMBINED = 0.55
_SUGGEST_SUIT_LEX = (0.55, 0.45)      # ontology_suitability, lexical
_SUGGEST_DEFSIM_SUIT = (0.40, 0.45)   # definition_similarity, ontology_suitability
_SUGGEST_LLM = 0.70


_STOPWORDS = {
    "a", "an", "the", "of", "for", "to", "in", "on", "and", "or", "with", "by", "as",
    "is", "are", "that", "which", "used", "using", "use", "e.g", "eg", "etc", "such",
    "from", "into", "at", "be", "this", "these", "those", "it", "its", "can", "may",
    "usually", "typically", "based", "related", "associated", "kind", "type", "form",
}

# term-type -> coarse ontology/domain family, used for contradiction + family checks.
_CATEGORY_FAMILY = {
    "food_product": "food", "food_processing_method": "food", "food_safety": "food",
    "organism_taxon": "organism",
    "gene_or_variant_symbol": "genomics", "sequencing_or_bioinformatics_method": "genomics",
    "genomic_feature_or_gene": "genomics",
    "clinical_condition": "clinical", "symptom": "clinical",
    "unit_or_quantity": "unit",
    "measurement_property": "measurement", "analytical_measurement_property": "measurement",
    "statistical_or_risk_concept": "measurement",
    "material_substance": "material", "packaging_component": "material",
    "chemical_substance": "chemical", "toxin_or_contaminant": "chemical",
    "environmental_entity": "environment", "climate_variable": "environment",
    "geospatial_statistical_region": "geo",
    "data_object_or_format": "data", "rdf_or_vocabulary_property": "data",
    "semantic_web_concept": "data",
    "assay_or_method": "method", "laboratory_process": "method", "experimental_condition": "method",
    "cell_line": "lab", "lab_material_or_cell_line": "lab",
}

# Clearly disjoint families -- a candidate from one is a domain contradiction for a term
# in the other. Conservative on purpose (only obviously-incompatible pairs).
_HARD_INCOMPATIBLE_FAMILIES = {
    frozenset({"clinical", "food"}), frozenset({"clinical", "unit"}),
    frozenset({"clinical", "material"}), frozenset({"clinical", "geo"}),
    frozenset({"clinical", "genomics"}), frozenset({"clinical", "chemical"}),
    frozenset({"geo", "food"}), frozenset({"geo", "material"}),
    frozenset({"geo", "genomics"}), frozenset({"geo", "organism"}),
    frozenset({"food", "genomics"}), frozenset({"unit", "food"}),
    frozenset({"unit", "organism"}), frozenset({"unit", "clinical"}),
}

_GENE_LIKE_RE = re.compile(r"^(bla[A-Za-z0-9\-]+|[a-z]{2,6}[A-Z][A-Za-z0-9\-]*|[a-z]{2,5}\d{1,3})$")
_GREEK = set("αβγδεζηθικλμνξοπρστυφχψω")


def _tokens(text: str) -> List[str]:
    return [t for t in _normalize_search_text(text).split() if t and t not in _STOPWORDS]


def _overlap(a: str, b: str) -> float:
    ta, tb = set(_tokens(a)), set(_tokens(b))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


# Below this token-overlap ratio the candidate definition is considered "low overlap" with
# the term definition. Low overlap alone is a SOFT signal (never a hard reject) -- a correct
# concept is often worded very differently from the input definition.
_LOW_DEF_SIM = 0.25


def _family(term_type: str) -> str:
    return _CATEGORY_FAMILY.get(str(term_type or ""), "")


# ---------------------------------------------------------------------------
# Trigger logic
# ---------------------------------------------------------------------------

def is_hard_term(term: str, term_profile: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    """Abbreviation-like / gene-like / namespace-like / ambiguous term detection."""
    raw = str(term or "").strip()
    if not raw:
        return False, ""
    profile = term_profile or {}
    ptype = str(profile.get("primary_term_type") or "")
    if "ambiguous_common_word" in (profile.get("ambiguity_flags") or []) or ptype == "ambiguous_common_word":
        return True, "ambiguous_term"
    if ptype in {"rdf_or_vocabulary_property", "geospatial_statistical_region", "data_object_or_format"}:
        return True, "namespace_or_special_vocabulary"
    single = " " not in raw
    if single and _GENE_LIKE_RE.match(raw):
        return True, "gene_like_token"
    if single and raw.isupper() and 2 <= len(raw) <= 6:
        return True, "uppercase_abbreviation"
    if len(raw) <= 6 and single:
        return True, "short_token"
    if any(ch in _GREEK for ch in raw) or ("-" in raw and any(c.isdigit() for c in raw)):
        return True, "symbolic_token"
    return False, ""


def should_run_rescue(
    *,
    provisional_status: str,
    triage: str,
    best_score: Any,
    ranked_candidates: Sequence[Any],
    term: str,
    term_profile: Dict[str, Any],
    config: Any,
) -> Tuple[bool, str]:
    """Decide whether the expensive rescue pass should run. Generic triggers only."""
    status = str(provisional_status or "").strip().lower()
    n_candidates = len(ranked_candidates or [])
    top_llm = _broad_llm_confidence(best_score) if best_score is not None else 0.0
    metadata = getattr(best_score, "trace_metadata", {}) or {}
    hard_block = _verified_hard_block(metadata, config) if best_score is not None else False
    mapping = _normalize_mapping_type(getattr(best_score, "mapping_type", "")) if best_score is not None else "none"
    exact = _broad_lexical_exact_or_synonym(best_score) if best_score is not None else False

    # Never rescue an already-verified match or a hard-blocked candidate.
    if triage == "verified":
        return False, "already_verified"
    if hard_block:
        return False, "hard_block_present"

    if status == "no_match" and n_candidates > 0 and top_llm >= 0.75:
        return True, "unexpected_no_match_with_candidates"
    if status == "candidate_suggested" and top_llm >= 0.90 and mapping in {"exact", "close"}:
        return True, "high_confidence_suggestion"
    if best_score is not None and (exact or mapping in {"exact", "close"}) and triage != "verified":
        return True, "exact_or_close_failed_gate"
    hard, why = is_hard_term(term, term_profile)
    if hard and n_candidates >= 0:
        return True, f"hard_term:{why}"
    # LLM-vs-gate disagreement: model is confident but the gate did not verify.
    if best_score is not None and top_llm >= 0.85 and triage != "verified":
        return True, "llm_gate_disagreement"
    return False, "no_trigger"


# ---------------------------------------------------------------------------
# Generic expansion / query derivation
# ---------------------------------------------------------------------------

def _acronym_of(phrase: str) -> str:
    words = [w for w in re.split(r"[^A-Za-z0-9]+", str(phrase or "")) if w]
    return "".join(w[0] for w in words).upper()


def derive_abbreviation_expansions(
    term: str,
    candidate_texts: Sequence[str],
    *,
    max_expansions: int = 3,
) -> List[str]:
    """Infer likely expansions of an abbreviation from candidate labels/synonyms/defs.

    Generic: a multi-word phrase whose initials equal the term (e.g. "PCR" ->
    "polymerase chain reaction"), or a parenthetical gloss "... (TERM)", is an expansion.
    Never hard-coded to a specific term."""
    raw = str(term or "").strip()
    if not raw or " " in raw:
        return []
    term_up = raw.upper()
    out: List[str] = []
    seen = set()

    def _add(phrase: str) -> None:
        p = " ".join(phrase.split()).strip()
        key = p.lower()
        if p and key not in seen and key != raw.lower() and len(p.split()) >= 2:
            seen.add(key)
            out.append(p)

    for text in candidate_texts:
        text = str(text or "")
        if not text:
            continue
        # Parenthetical gloss: "polymerase chain reaction (PCR)"
        for m in re.finditer(r"([A-Za-z][A-Za-z0-9 \-]{3,}?)\s*\(([A-Za-z0-9\-]{2,8})\)", text):
            phrase, abbr = m.group(1), m.group(2)
            if abbr.upper() == term_up:
                _add(phrase)
        # Acronym match over sliding windows of the text.
        words = [w for w in re.split(r"[^A-Za-z0-9]+", text) if w]
        n = len(term_up)
        for i in range(len(words)):
            for span in range(2, n + 2):
                window = words[i:i + span]
                if len(window) < 2:
                    continue
                if _acronym_of(" ".join(window)) == term_up:
                    _add(" ".join(window))
    return out[:max_expansions]


def derive_definition_queries(definition: str, *, max_queries: int = 3) -> List[str]:
    """Derive 1-3 noun-phrase search queries from the input definition (generic)."""
    text = str(definition or "").strip()
    if not text:
        return []
    out: List[str] = []
    seen = set()
    # Split on clause boundaries, then take the meaningful (stopword-stripped) phrase.
    for chunk in re.split(r"[.;,:/()]|\bthat\b|\bwhich\b|\busing\b|\bused\b", text):
        toks = _tokens(chunk)
        if 1 <= len(toks) <= 5:
            phrase = " ".join(toks[:5])
            key = phrase.lower()
            if phrase and key not in seen and len(phrase) >= 4:
                seen.add(key)
                out.append(phrase)
    # Fall back to the leading noun phrase of the whole definition.
    if not out:
        toks = _tokens(text)
        if toks:
            out.append(" ".join(toks[:4]))
    return out[:max_queries]


# ---------------------------------------------------------------------------
# Semantic gates
# ---------------------------------------------------------------------------

def semantic_contradiction(
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    candidate_label: str,
    candidate_definition: str = "",
) -> Tuple[bool, str]:
    """Generic contradiction check: does the candidate's domain/definition clearly
    contradict the term's? (e.g. term = microbial subtype, candidate = medical injury)."""
    term_type = str(term_profile.get("primary_term_type") or "")
    term_fam = _family(term_type)
    cand_cat = categorize_term(f"{candidate_label} {candidate_definition or ''}")
    cand_fam = _family(cand_cat.primary_category)
    # HARD contradiction only for a genuine cross-domain family conflict (e.g. a clinical
    # candidate for a unit term). Low token-level definition overlap ALONE is NOT a hard
    # contradiction -- a correct concept is often worded very differently from the input
    # definition (e.g. "PCR" defined as "amplify DNA" vs the label "polymerase chain
    # reaction"). Weak definition overlap is only a soft signal (reflected in scoring and
    # surfaced to the LLM), never a hard reject here. Coincidental lexical fragments of an
    # AMBIGUOUS term (e.g. "strain" -> "Sprains and Strains") are rejected by the
    # ambiguity-specific path, not by this generic definition heuristic.
    if term_fam and cand_fam and term_fam != cand_fam:
        if frozenset({term_fam, cand_fam}) in _HARD_INCOMPATIBLE_FAMILIES:
            return True, f"domain_family_conflict:{term_fam}_vs_{cand_fam}"
    return False, ""


def ontology_family_ok(term_type: str, ontology_acronym: str) -> bool:
    """Does the candidate ontology family fit the inferred term type? (preferred/fallback
    ontologies fit; penalized ones do not; unknown ontologies are treated as neutral)."""
    priors = CATEGORY_ONTOLOGY_PRIORS.get(str(term_type or ""), {})
    acr = str(ontology_acronym or "").strip().upper()
    if not acr:
        return True
    if acr in {a.upper() for a in priors.get("penalized_acronyms", [])}:
        return False
    if acr in {a.upper() for a in priors.get("preferred_acronyms", [])}:
        return True
    if acr in {a.upper() for a in priors.get("fallback_acronyms", [])}:
        return True
    # Unknown ontology: neutral (not a family fit, but not a hard family conflict).
    return True


def _is_generic_or_broader(score: Any) -> bool:
    md = getattr(score, "trace_metadata", {}) or {}
    if bool(md.get("candidate_broader_than_term")) or bool(md.get("candidate_over_specific_no_context")):
        return True
    label = str(getattr(getattr(score, "candidate", None), "label", "") or "").strip().lower()
    return label in GENERIC_HEAD_LABELS


def candidate_evidence_summary(score: Any, term: str, definition: str) -> Dict[str, Any]:
    """Compact evidence bundle for one candidate (used in traces + comparative prompt)."""
    candidate = getattr(score, "candidate", None)
    md = getattr(score, "trace_metadata", {}) or {}
    ctx = getattr(candidate, "ontology_context", {}) or {}
    label = str(getattr(candidate, "label", "") or "")
    desc = str(getattr(candidate, "description", "") or "")
    return {
        "label": label,
        "uri": str(getattr(candidate, "uri", "") or ""),
        "ontology": str(getattr(candidate, "source_provider", "") or ""),
        "definition": desc,
        "synonyms": list(ctx.get("synonyms", []) or [])[:6],
        "parents": list(ctx.get("parents", []) or [])[:4] if isinstance(ctx.get("parents"), list) else [],
        "children": list(ctx.get("children", []) or [])[:4] if isinstance(ctx.get("children"), list) else [],
        "ontology_suitability_score": md.get("ontology_suitability_score"),
        "category_alignment_score": md.get("category_alignment_score"),
        "lexical_score": getattr(score, "lexical_score", None),
        "ancestors": list((ctx.get("lineage") or []))[:6] if isinstance(ctx.get("lineage"), list) else [],
        "semantic_type": list((ctx.get("semantic_type") or []))[:4] if isinstance(ctx.get("semantic_type"), list) else [],
        "definition_similarity": round(_overlap(definition, desc or label), 4),
        "llm_confidence": _broad_llm_confidence(score),
        "generic_or_broader": _is_generic_or_broader(score),
        "registry_support": bool(md.get("registry_support")),
        "hierarchy_domain_fit": md.get("hierarchy_domain_fit"),
        "ontology_family_fit": md.get("ontology_family_fit"),
        "hierarchy_supports_candidate": md.get("hierarchy_supports_candidate"),
        "wrong_branch_warning": md.get("hierarchy_wrong_branch_warning"),
        "hierarchy_granularity": md.get("hierarchy_granularity_assessment"),
    }


# ---------------------------------------------------------------------------
# Three-way rescue decision (deterministic; contradiction/family are hard gates)
# ---------------------------------------------------------------------------

def rescue_three_way_decision(
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    scored_candidates: Sequence[Any],
    config: Any,
    *,
    llm_selected_uri: Optional[str] = None,
) -> Dict[str, Any]:
    """Return the rescue outcome over the enriched candidate pool:
    ``{status: verified|acceptable_suggestion|reject, best, reasons, per_candidate}``.

    Contradiction and hard blocks are HARD gates (an LLM selection can never override
    them). Verification additionally requires either ontology-family fit OR a strong
    definition match; otherwise the best candidate is at most an acceptable suggestion."""
    term_type = str(term_profile.get("primary_term_type") or "")
    is_ambiguous = term_type == "ambiguous_common_word" or "ambiguous_common_word" in (term_profile.get("ambiguity_flags") or [])
    per_candidate: List[Dict[str, Any]] = []
    best = None
    best_key = None

    for score in scored_candidates or []:
        if score is None:
            continue
        candidate = getattr(score, "candidate", None)
        label = str(getattr(candidate, "label", "") or "")
        desc = str(getattr(candidate, "description", "") or "")
        onto = str(getattr(candidate, "source_provider", "") or "")
        md = getattr(score, "trace_metadata", {}) or {}
        hard_block = _verified_hard_block(md, config)
        contradiction, contra_reason = semantic_contradiction(term, definition, term_profile, label, desc)
        family_ok = ontology_family_ok(term_type, onto)
        suit = _safe_confidence(md.get("ontology_suitability_score"), default=0.0)
        llm = _broad_llm_confidence(score)
        exact = _broad_lexical_exact_or_synonym(score)
        def_sim = _overlap(definition, desc or label)
        # Ontology-hierarchy evidence (parents/siblings/ancestors branch). Strong branch
        # support can substitute for a missing definition; a broader branch is a broader
        # candidate; a wrong branch is already a hard block (via _verified_hard_block).
        hier_support = bool(md.get("hierarchy_supports_candidate"))
        hier_fit = str(md.get("hierarchy_domain_fit") or "unknown")
        generic = _is_generic_or_broader(score) or str(md.get("hierarchy_granularity_assessment")) == "broader_than_term"
        selected_by_llm = bool(llm_selected_uri) and str(getattr(candidate, "uri", "")) == str(llm_selected_uri)

        if hard_block or contradiction:
            outcome = "reject"
            reason = "hard_block" if hard_block else contra_reason
        elif is_ambiguous:
            # Ambiguous common word: an exact LABEL alone is the trap (which sense?).
            label_lex = _overlap(term, label)
            hard_wrong_sense = (
                not exact and label_lex < 0.60 and def_sim < 0.25 and not hier_support
            )
            if hard_wrong_sense:
                # A coincidental lexical fragment in an unsupported branch (e.g. "strain"
                # -> "Sprains and Strains") is the wrong sense -- hard reject.
                outcome = "reject"
                reason = "ambiguous_coincidental_fragment_wrong_sense"
            elif (def_sim >= 0.55 or (hier_support and hier_fit == "strong")) and suit >= 0.70 and family_ok and llm >= 0.85 and not generic:
                # Verification requires the definition OR the ontology branch to RESOLVE
                # the intended sense (not an exact label alone).
                outcome = "verified"
                reason = "ambiguous_resolved_by_definition_or_hierarchy"
            elif (exact or def_sim >= 0.25 or suit >= 0.55 or hier_support) and llm >= 0.60 and not generic:
                outcome = "acceptable_suggestion"
                reason = "ambiguous_unresolved_suggestion"
            else:
                outcome = "reject"
                reason = "ambiguous_unresolved_insufficient_evidence"
        else:
            # Verification requires strong semantics (exact / definition) OR strong
            # ontology-branch support; and either ontology family fit, a strong
            # definition, or supporting hierarchy. If BOTH definition and hierarchy are
            # weak/contradictory, it does not verify.
            strong_semantic = exact or def_sim >= 0.50 or (hier_support and hier_fit == "strong")
            branch_or_def_support = family_ok or def_sim >= 0.60 or hier_support
            if strong_semantic and branch_or_def_support and suit >= 0.50 and not generic and llm >= 0.80:
                outcome = "verified"
                reason = "strong_semantic_fit_with_family_definition_or_hierarchy"
            elif (exact or def_sim >= 0.30 or suit >= 0.45 or hier_support) and llm >= 0.60:
                outcome = "acceptable_suggestion"
                reason = "plausible_but_broader_or_uncertain"
            else:
                outcome = "reject"
                reason = "insufficient_semantic_domain_or_hierarchy_fit"

        # A rank key so the "best" prefers verified > acceptable, LLM-selected, higher
        # def-similarity / suitability / LLM confidence.
        # A reject is HARD only for a real contradiction/hard block; otherwise it is a
        # SOFT reject (not enough evidence to verify) that may still be suggested.
        reject_type = None
        if outcome == "reject":
            reject_type = "hard" if (hard_block or contradiction or reason == "ambiguous_coincidental_fragment_wrong_sense") else "soft"
        rank = (
            {"verified": 2, "acceptable_suggestion": 1, "reject": 0}[outcome],
            1 if selected_by_llm else 0,
            round(def_sim, 3), round(suit, 3), round(llm, 3),
        )
        # Contradiction-evidence transparency (recalibration audit): distinguish a genuine
        # HARD cross-domain conflict from merely LOW definition overlap, and record when
        # ontology hierarchy support rescued a low-definition-overlap candidate (the PCR
        # case: exact label + supportive branch but weakly-worded definition).
        hier_context_used = "hierarchy_domain_fit" in md
        def_overlap_low = def_sim < _LOW_DEF_SIM
        def_low_but_hier_supportive = bool(def_overlap_low and hier_support)
        if contradiction:
            contradiction_evidence_type = str(contra_reason).split(":", 1)[0] or "domain_family_conflict"
        elif reason == "ambiguous_coincidental_fragment_wrong_sense":
            contradiction_evidence_type = "ambiguous_coincidental_fragment"
        elif def_low_but_hier_supportive:
            contradiction_evidence_type = "low_definition_overlap_hierarchy_supportive"
        elif def_overlap_low and not exact:
            contradiction_evidence_type = "low_definition_overlap_soft"
        else:
            contradiction_evidence_type = "none"
        item = {
            "label": label, "ontology": onto, "outcome": outcome, "reason": reason,
            "reject_type": reject_type,
            "semantic_fit_reason": f"def_similarity={round(def_sim, 3)}; exact={exact}",
            "domain_fit_reason": f"family_ok={family_ok}; hierarchy_domain_fit={hier_fit}; contradiction={contradiction or md.get('hierarchy_wrong_branch_warning')}",
            "granularity_fit_reason": f"generic_or_broader={generic}; hierarchy_granularity={md.get('hierarchy_granularity_assessment')}",
            "hierarchy_domain_fit": hier_fit,
            "hierarchy_supports_candidate": hier_support,
            "wrong_branch_warning": bool(md.get("hierarchy_wrong_branch_warning")),
            "contradiction": contradiction,
            "contradiction_evidence_type": contradiction_evidence_type,
            "contradiction_is_hard": bool(contradiction),
            "definition_overlap_low_but_hierarchy_supportive": def_low_but_hier_supportive,
            "hierarchy_context_used_in_contradiction_check": hier_context_used,
        }
        per_candidate.append(item)
        if best_key is None or rank > best_key:
            best_key, best = rank, score
            best_outcome, best_reason, best_reject_type = outcome, reason, reject_type
            best_item = item

    if best is None:
        return {"status": "reject", "best": None,
                "reasons": {"reason": "no_candidate", "reject_type": "hard"},
                "best_evidence": {}, "per_candidate": per_candidate}
    return {
        "status": best_outcome,
        "best": best,
        "reasons": {"reason": best_reason, "reject_type": best_reject_type},
        "best_evidence": best_item,
        "per_candidate": per_candidate,
    }


# ---------------------------------------------------------------------------
# no_match recalibration: hard-reject reasons, plausibility, and top-N selection.
# no_match is a POSITIVE statement ("no usable candidate"), never the default for an
# uncertain-but-plausible candidate.
# ---------------------------------------------------------------------------

def hard_reject_reason(
    score: Any,
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    config: Any,
) -> Optional[str]:
    """Return a HARD-reject reason if the candidate must never be surfaced (not even as a
    suggestion), else None. Only genuinely-wrong/unusable cases -- generic/broader or
    merely-weak candidates are NOT hard-rejected (they remain suggestible)."""
    if score is None:
        return "no_candidate"
    metadata = getattr(score, "trace_metadata", {}) or {}
    candidate = getattr(score, "candidate", None)
    uri = str(getattr(candidate, "uri", "") or "").strip()
    label = str(getattr(candidate, "label", "") or "").strip()
    if not uri or not label:
        return "missing_uri_or_label"
    if _verified_hard_block(metadata, config):
        if bool(metadata.get("obsolete")):
            return "obsolete"
        if str(metadata.get("candidate_domain_mismatch_level", "")).strip().lower() == "hard":
            return "hard_domain_mismatch"
        if bool(metadata.get("placeholder_or_status_value")):
            return "placeholder_policy"
        if bool(metadata.get("registry_suitability_block")):
            return "wrong_domain_exact"
        return "hard_block"
    if bool(metadata.get("suppress_suggestion")):
        return "suppressed_semantic_mismatch"
    # Ontology-branch evidence: a candidate whose parents/siblings/ancestors sit in a
    # domain that contradicts the term sense is the wrong concept of an ambiguous label.
    if bool(metadata.get("hierarchy_wrong_branch_warning")) and bool(metadata.get("hierarchy_wrong_branch_block")):
        return "wrong_ontology_branch"
    # Ambiguous term whose best candidate is only a coincidental lexical fragment in an
    # unsupported sense (e.g. "strain" -> "Sprains and Strains"): the wrong sense.
    is_ambiguous = (
        str(term_profile.get("primary_term_type")) == "ambiguous_common_word"
        or "ambiguous_common_word" in (term_profile.get("ambiguity_flags") or [])
    )
    if is_ambiguous:
        exact = _broad_lexical_exact_or_synonym(score)
        label_lex = _overlap(term, label)
        def_sim = _overlap(definition, str(getattr(candidate, "description", "") or "") or label)
        hier_support = bool(metadata.get("hierarchy_supports_candidate"))
        if not exact and label_lex < 0.60 and def_sim < 0.25 and not hier_support:
            return "ambiguous_coincidental_fragment_wrong_sense"
    contradiction, contra_reason = semantic_contradiction(
        term, definition, term_profile, label, str(getattr(candidate, "description", "") or ""))
    if contradiction:
        return contra_reason or "semantic_contradiction"
    # rescue explicitly HARD-rejected this candidate
    if str(metadata.get("rescue_status")) == "reject" and str(metadata.get("rescue_reject_type")) == "hard":
        return "rescue_hard_reject"
    return None


def is_plausible_suggestion(score: Any) -> bool:
    """Broadened, NOT lexical-only, plausibility for a manual-review suggestion."""
    if score is None:
        return False
    metadata = getattr(score, "trace_metadata", {}) or {}
    combined = _score_confidence(score)
    suit = _safe_confidence(metadata.get("ontology_suitability_score"), default=0.0)
    lexical = _safe_confidence(getattr(score, "lexical_score", None), default=0.0)
    def_sim = _safe_confidence(metadata.get("definition_similarity"), default=0.0)
    llm = _broad_llm_confidence(score)
    if combined >= _SUGGEST_COMBINED:
        return True
    if suit >= _SUGGEST_SUIT_LEX[0] and lexical >= _SUGGEST_SUIT_LEX[1]:
        return True
    if def_sim >= _SUGGEST_DEFSIM_SUIT[0] and suit >= _SUGGEST_DEFSIM_SUIT[1]:
        return True
    if str(metadata.get("rescue_status")) == "acceptable_suggestion":
        return True
    if llm >= _SUGGEST_LLM:
        return True
    return False


def select_suggestion_candidate(
    candidates: Sequence[Any],
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    config: Any,
    *,
    top_n: int = 5,
) -> Dict[str, Any]:
    """Evaluate the top-N candidates (not just the single best). Return the best plausible,
    non-hard-rejected candidate for a suggestion, or signal that all are hard-rejected.

    Returns ``{best, plausible_found, all_hard_rejected, reject_reasons}``."""
    pool = [c for c in (candidates or []) if c is not None][:top_n]
    reject_reasons: Dict[str, str] = {}
    survivors: List[Any] = []
    for score in pool:
        reason = hard_reject_reason(score, term, definition, term_profile, config)
        label = str(getattr(getattr(score, "candidate", None), "label", "") or "")
        if reason:
            reject_reasons[label] = reason
            continue
        if is_plausible_suggestion(score):
            survivors.append(score)
    all_hard_rejected = bool(pool) and len(reject_reasons) == len(pool)
    if not survivors:
        return {"best": None, "plausible_found": False,
                "all_hard_rejected": all_hard_rejected, "reject_reasons": reject_reasons}

    def _rank(s: Any):
        md = getattr(s, "trace_metadata", {}) or {}
        return (
            _safe_confidence(md.get("ontology_suitability_score"), default=0.0),
            _safe_confidence(md.get("definition_similarity"), default=0.0),
            _score_confidence(s),
            _broad_llm_confidence(s),
        )

    best = max(survivors, key=_rank)
    return {"best": best, "plausible_found": True,
            "all_hard_rejected": False, "reject_reasons": reject_reasons}


# ---------------------------------------------------------------------------
# Ontology hierarchy + context assessment (generic semantic evidence).
# A term can share a label across completely different branches ("bank": finance /
# river / bench). When the definition is missing or ambiguous, the parent/child/
# sibling/ancestor branch and the ontology family are often the decisive evidence for
# the intended sense. This is EVIDENCE for validation -- never a hardcoded mapping.
# ---------------------------------------------------------------------------

_HIER_FIT_SCORE = {"strong": 0.85, "moderate": 0.65, "weak": 0.40, "contradiction": 0.05, "unknown": 0.50}


def _branch_families(labels: Sequence[str]) -> Dict[str, int]:
    fams: Dict[str, int] = {}
    for lbl in labels:
        fam = _family(categorize_term(str(lbl)).primary_category)
        if fam:
            fams[fam] = fams.get(fam, 0) + 1
    return fams


def _norm_set(labels: Sequence[str]) -> set:
    return {_normalize_search_text(str(l)) for l in labels if str(l).strip()}


def assess_candidate_hierarchy_context(
    term: str,
    definition: str,
    term_profile: Dict[str, Any],
    candidate: Any,
) -> Dict[str, Any]:
    """Assess whether a candidate sits in the CORRECT semantic branch for the term, using
    its ontology hierarchy (parents/children/siblings/ancestors), semantic type and
    ontology family. Generic: hierarchy labels are categorised into domain families and
    compared to the term's family; no term-specific branch rules."""
    ctx = getattr(candidate, "ontology_context", {}) or {}
    parents = [str(x) for x in (ctx.get("parents") or []) if str(x).strip()]
    children = [str(x) for x in (ctx.get("children") or []) if str(x).strip()]
    siblings = [str(x) for x in (ctx.get("siblings") or ctx.get("sibling_examples") or []) if str(x).strip()]
    ancestors = [str(x) for x in (ctx.get("lineage") or []) if str(x).strip()]
    semantic_type = [str(x) for x in (ctx.get("semantic_type") or []) if str(x).strip()]
    label = str(getattr(candidate, "label", "") or "")
    onto = str(getattr(candidate, "source_provider", "") or "")

    hierarchy_available = bool(parents or children or siblings or ancestors)
    ontology_context_available = bool(hierarchy_available or semantic_type or onto)
    term_type = str(term_profile.get("primary_term_type") or "")
    term_fam = _family(term_type)

    # The "sense indicators" of the branch: parents + siblings + ancestors + semantic type.
    branch_labels = parents + siblings + ancestors + semantic_type
    branch_fams = _branch_families(branch_labels)

    wrong_branch = False
    wrong_reason = ""
    domain_fit = "unknown"
    if term_fam and branch_fams:
        total = sum(branch_fams.values())
        incompatible = {f: c for f, c in branch_fams.items() if frozenset({term_fam, f}) in _HARD_INCOMPATIBLE_FAMILIES}
        top_fam = max(branch_fams, key=branch_fams.get)
        top_count = branch_fams[top_fam]
        incompat_count = sum(incompatible.values())
        # A contradiction requires the incompatible family to genuinely DOMINATE the
        # branch (top family, >=2 labels, >=half the branch) AND the term family to be
        # absent. This avoids false positives from a single mis-categorised neighbour
        # label (e.g. "Foodborne disease" -> food) on an otherwise-correct concept.
        if term_fam in branch_fams and not incompatible:
            domain_fit = "strong"
        elif term_fam in branch_fams:
            domain_fit = "moderate"
        elif incompatible and top_fam in incompatible and top_count >= 2 and incompat_count >= max(2, 0.5 * total):
            domain_fit = "contradiction"
            wrong_branch = True
            wrong_reason = f"branch dominated by {top_fam} ({top_count}/{total}) but term is {term_fam}"
        elif incompatible:
            domain_fit = "weak"  # some incompatible signal, but not dominant -> not a hard contradiction
        else:
            domain_fit = "weak"

    # Granularity from hierarchy (term relative to the candidate's neighbours).
    tnorm = _normalize_search_text(term)
    children_norm, parents_norm, ancestors_norm = _norm_set(children), _norm_set(parents), _norm_set(ancestors)
    label_exact = _normalize_search_text(label) == tnorm and bool(tnorm)
    if tnorm and tnorm in children_norm:
        granularity = "broader_than_term"          # the term is a child of the candidate
    elif tnorm and (tnorm in parents_norm or tnorm in ancestors_norm):
        granularity = "narrower_than_term"          # the term is an ancestor of the candidate
    elif label_exact and children:
        granularity = "exact_level"
    elif wrong_branch:
        granularity = "sibling_or_wrong_branch"
    else:
        granularity = "unknown"

    # Ontology family fit (registry-family recommendation, generic).
    ontology_family_fit = "unknown"
    if term_type:
        priors = CATEGORY_ONTOLOGY_PRIORS.get(term_type, {})
        acr = onto.upper()
        if acr in {a.upper() for a in priors.get("penalized_acronyms", [])}:
            ontology_family_fit = "contradiction"
        elif acr in {a.upper() for a in priors.get("preferred_acronyms", [])}:
            ontology_family_fit = "strong"
        elif acr in {a.upper() for a in priors.get("fallback_acronyms", [])}:
            ontology_family_fit = "moderate"
        elif acr:
            ontology_family_fit = "weak"

    hierarchy_supports = (domain_fit in {"strong", "moderate"}) and not wrong_branch
    hierarchy_context_score = _HIER_FIT_SCORE.get(domain_fit, 0.50)
    penalty_reason = wrong_reason or (
        "candidate broader than term (term is a child in the branch)" if granularity == "broader_than_term"
        else "candidate narrower/over-specific vs term" if granularity == "narrower_than_term"
        else ""
    )

    return {
        "hierarchy_context_available": bool(hierarchy_available),
        "ontology_context_available": bool(ontology_context_available),
        "parent_labels": parents[:8],
        "child_labels": children[:8],
        "sibling_labels": siblings[:8],
        "ancestor_labels": ancestors[:8],
        "semantic_type": semantic_type[:6],
        "parent_child_sibling_summary": (
            f"parents={parents[:5]}; children={children[:5]}; siblings={siblings[:5]}"
            if hierarchy_available else "no indexed hierarchy available"
        ),
        "ontology_branch_summary": (
            f"ontology={onto}; ancestors={ancestors[:5]}; semantic_type={semantic_type[:3]}"
        ),
        "hierarchy_domain_fit": domain_fit,
        "ontology_family_fit": ontology_family_fit,
        "wrong_branch_warning": bool(wrong_branch),
        "wrong_branch_reason": wrong_reason,
        "granularity_from_hierarchy": granularity,
        "hierarchy_supports_candidate": bool(hierarchy_supports),
        "hierarchy_penalty_reason": penalty_reason,
        "hierarchy_context_score": round(float(hierarchy_context_score), 4),
    }


def attach_hierarchy_assessment(score: Any, term: str, definition: str, term_profile: Dict[str, Any]) -> Dict[str, Any]:
    """Compute and attach the hierarchy/context assessment to a candidate's trace_metadata.
    Sets ``hierarchy_wrong_branch_block`` (a HARD verification block) when the branch
    contradicts the term sense and there is no compensating (near-)exact label + strong
    ontology family. Returns the assessment dict."""
    if score is None:
        return {}
    assessment = assess_candidate_hierarchy_context(term, definition, term_profile, getattr(score, "candidate", None))
    md = getattr(score, "trace_metadata", {}) or {}
    # Store the assessment under stable, namespaced keys the gate/adjudicator read.
    md["hierarchy_context_available"] = assessment.get("hierarchy_context_available")
    md["ontology_context_available"] = assessment.get("ontology_context_available")
    md["hierarchy_domain_fit"] = assessment.get("hierarchy_domain_fit")
    md["ontology_family_fit"] = assessment.get("ontology_family_fit")
    md["hierarchy_context_score"] = assessment.get("hierarchy_context_score")
    md["hierarchy_supports_candidate"] = assessment.get("hierarchy_supports_candidate")
    md["hierarchy_wrong_branch_warning"] = assessment.get("wrong_branch_warning")
    md["hierarchy_wrong_branch_reason"] = assessment.get("wrong_branch_reason")
    md["hierarchy_granularity_assessment"] = assessment.get("granularity_from_hierarchy")
    md["parent_labels"] = assessment.get("parent_labels")
    md["child_labels"] = assessment.get("child_labels")
    md["sibling_labels"] = assessment.get("sibling_labels")
    md["ancestor_labels"] = assessment.get("ancestor_labels")
    md["ontology_branch_summary"] = assessment.get("ontology_branch_summary")
    md["parent_child_sibling_summary"] = assessment.get("parent_child_sibling_summary")
    # A wrong branch HARD-blocks verification only for a NON-exact candidate. A genuine
    # exact/synonym label match is strong evidence for the right concept even when the
    # (often noisy / coarsely-categorised) hierarchy labels look off -- for exact matches
    # the wrong-branch signal stays soft (it still lowers the score and is flagged to the
    # LLM via the candidate card), so canonical exact concepts in fallback ontologies
    # (e.g. a disease in MONDO, a unit in NCIT) are not falsely hard-blocked.
    exact = _broad_lexical_exact_or_synonym(score)
    md["hierarchy_wrong_branch_block"] = bool(assessment.get("wrong_branch_warning") and not exact)
    score.trace_metadata = md
    return assessment
