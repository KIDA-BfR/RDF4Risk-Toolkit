# -*- coding: utf-8 -*-
"""BioPortal Annotator rescue helpers for broad registry-scored reconciliation.

The functions in this module are intentionally small and mostly deterministic so
they can be unit-tested without live BioPortal or LLM calls. The workflow layer is
responsible for deciding when to invoke the live annotator and when to re-run LLM
adjudication over the merged candidate pool.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests

from .agent_models import AgentCandidate


DEFAULT_BIOPORTAL_BASE_URL = "https://data.bioontology.org"
ANNOTATOR_OPTIONAL_PARAMS = ("whole_word_only", "exclude_numbers", "ontologies")
ANNOTATOR_USER_AGENT = "RDF4Risk-BioPortal-Annotator-Rescue/0.1"
MAX_DEFAULT_VARIANTS = 8
MIN_DEFAULT_VARIANTS = 1

MARKER_WORDS = {
    "original", "synonym", "common", "formula", "canonical", "variant", "term",
    "label", "role", "source", "strong", "weak", "medium", "common2",
}
GENERIC_VARIANTS = {
    "salt", "chloride", "sodium", "mineral", "ingredient", "material", "substance",
    "thing", "item", "other", "unknown", "compound", "chemical", "variable",
    "model", "limit",
}
ALLOWED_VARIANT_KINDS = {
    "original",
    "spelling_variant",
    "translation",
    "domain_synonym",
    "canonical_name",
    "abbreviation_expansion",
    "chemical_formula",
    "scientific_name",
    "common_name",
}
VARIANT_KIND_ALIASES = {
    "de_synonym": "translation",
    "en_synonym": "domain_synonym",
    "synonym": "domain_synonym",
    "en_common": "common_name",
    "canonical_chemical_name": "canonical_name",
    "formula": "chemical_formula",
}
BLOCKED_VARIANT_KINDS = {
    "candidate_label",
    "candidate_synonym",
    "generic_parent",
    "related_candidate",
    "abbreviation_collision",
}
KIND_WEIGHT = {
    "canonical_name": 0.14,
    "scientific_name": 0.13,
    "chemical_formula": 0.10,
    "abbreviation_expansion": 0.11,
    "domain_synonym": 0.10,
    "translation": 0.10,
    "common_name": 0.08,
    "spelling_variant": 0.05,
    "original": 0.03,
}
STRENGTH_WEIGHT = {
    "very_strong": 0.12,
    "strong": 0.08,
    "medium": 0.04,
    "weak": 0.01,
}

TERM_TYPE_TO_ANNOTATOR_ONTOLOGIES: Dict[str, List[str]] = {
    "chemical": ["CHEBI", "MESH", "NCIT"],
    "chemical_substance": ["CHEBI", "MESH", "NCIT"],
    "toxin_or_contaminant": ["CHEBI", "MESH", "NCIT"],
    "chemical_or_food_substance": ["CHEBI", "FOODON", "MESH", "NCIT"],
    "food": ["FOODON", "AGROVOC", "CHEBI"],
    "food_product": ["FOODON", "AGROVOC", "CHEBI"],
    "food_processing_method": ["FOODON", "AGROVOC", "OBI"],
    "organism": ["NCBITAXON"],
    "organism_taxon": ["NCBITAXON"],
    "unit": ["QUDT", "UO"],
    "unit_or_quantity": ["QUDT", "UO"],
    "measurement_property": ["QUDT", "UO", "OBI"],
    "analytical_measurement_property": ["QUDT", "UO", "OBI"],
    "method": ["OBI", "EDAM", "NCIT"],
    "assay_or_method": ["OBI", "EDAM", "NCIT"],
    "laboratory_process": ["OBI", "EDAM", "NCIT"],
    "gene": ["NCBIGENE", "SO"],
    "gene_or_variant_symbol": ["NCBIGENE", "SO"],
    "genomic_feature_or_gene": ["NCBIGENE", "SO"],
    "material": ["ENVO", "CHEBI", "NCIT"],
    "material_substance": ["ENVO", "CHEBI", "NCIT"],
    "environmental_entity": ["ENVO"],
    "data_object_or_format": ["EDAM", "NCIT"],
    "rdf_or_vocabulary_property": ["EDAM"],
}
DEFAULT_ANNOTATOR_ONTOLOGIES = ["CHEBI", "FOODON", "MESH", "NCIT"]


@dataclass
class AnnotatorRescueVariant:
    term_id: str
    variant_id: str
    text: str
    kind: str = "variant"
    expected_strength: str = "medium"


@dataclass
class AnnotatorTextSpan:
    term_id: str
    variant_id: str
    variant_text: str
    variant_kind: str
    expected_strength: str
    start: int
    end: int


@dataclass
class AnnotatorRescueResult:
    candidates: List[AgentCandidate] = field(default_factory=list)
    variants: List[Dict[str, Any]] = field(default_factory=list)
    request_metadata: Dict[str, Any] = field(default_factory=dict)
    annotation_count: int = 0
    grouped_candidate_count: int = 0
    dropped_params: List[str] = field(default_factory=list)
    error: str = ""


@dataclass
class AnnotatorRescueDecision:
    should_run: bool
    reason: str
    stage: str = "initial_broad_retrieval"
    primary_gate_passed: bool = False
    primary_gate_reason: Optional[str] = None
    secondary_reason_present: bool = False
    secondary_reason: Optional[str] = None
    blocked: bool = False
    block_reason: Optional[str] = None
    top_candidate_uri: Optional[str] = None
    top_candidate_label: Optional[str] = None
    top_candidate_score: Optional[float] = None
    top_candidate_ontology: Optional[str] = None
    candidate_count: int = 0
    expected_ontology_families: List[str] = field(default_factory=list)
    observed_ontology_families: List[str] = field(default_factory=list)
    llm_decision: Optional[str] = None
    llm_match_type: Optional[str] = None
    policy_version: str = "two_stage_gate_v1"

    def __iter__(self):
        yield self.should_run
        yield self.reason

    def as_trace_metadata(self) -> Dict[str, Any]:
        return {
            "annotator_rescue_stage": self.stage,
            "annotator_rescue_policy_version": self.policy_version,
            "annotator_rescue_triggered": bool(self.should_run),
            "annotator_primary_gate_passed": bool(self.primary_gate_passed),
            "annotator_primary_gate_reason": self.primary_gate_reason,
            "annotator_secondary_reason_present": bool(self.secondary_reason_present),
            "annotator_secondary_reason": self.secondary_reason,
            "annotator_blocked": bool(self.blocked),
            "annotator_block_reason": self.block_reason,
            "annotator_decision_reason": self.reason,
            "top_candidate_before_rescue_label": self.top_candidate_label,
            "top_candidate_before_rescue_uri": self.top_candidate_uri,
            "top_candidate_before_rescue_score": self.top_candidate_score,
            "top_candidate_before_rescue_ontology": self.top_candidate_ontology,
            "candidate_count_before_rescue": int(self.candidate_count or 0),
            "expected_ontology_families": list(self.expected_ontology_families or []),
            "observed_ontology_families": list(self.observed_ontology_families or []),
            "llm_decision_before_rescue": self.llm_decision,
            "llm_match_type_before_rescue": self.llm_match_type,
        }


def _norm_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _variant_key(value: str) -> str:
    return _norm_text(value).casefold()


def _variant_loose_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _variant_key(value)).strip()


def _is_marker_variant(text: str) -> bool:
    norm = _variant_key(text)
    return not norm or norm in MARKER_WORDS


def _normalize_variant_kind(kind: Any) -> str:
    raw = str(kind or "domain_synonym").strip().lower()
    return VARIANT_KIND_ALIASES.get(raw, raw)


def _tokens(value: str) -> List[str]:
    return [tok for tok in re.split(r"[^a-z0-9]+", _variant_key(value)) if tok]


def _initials(value: str) -> str:
    return "".join(tok[:1] for tok in _tokens(value) if tok).upper()


def _primary_term_type(payload: Optional[Dict[str, Any]], term_profile: Optional[Dict[str, Any]]) -> str:
    for source in (payload or {}, term_profile or {}):
        value = str(
            source.get("term_type")
            or source.get("primary_term_type")
            or ""
        ).strip().lower()
        if value:
            return value
    return ""


def _context_text(definition: str = "", term_profile: Optional[Dict[str, Any]] = None) -> str:
    parts = [definition or ""]
    profile = term_profile or {}
    for key in ("category_context_used", "rdf_role", "role", "application_context", "primary_term_type"):
        parts.append(str(profile.get(key) or ""))
    return _variant_loose_key(" ".join(parts))


def _is_chemical_term_type(term_type: str, context: str) -> bool:
    text = f"{term_type} {context}"
    return any(token in text for token in ("chemical", "compound", "substance", "formula"))


def _is_method_term_type(term_type: str, context: str) -> bool:
    text = f"{term_type} {context}"
    return any(token in text for token in ("method", "assay", "process", "laboratory", "protocol"))


def _is_data_or_vocabulary_term_type(term_type: str, context: str) -> bool:
    text = f"{term_type} {context}"
    return any(token in text for token in ("data", "format", "rdf", "vocabulary", "metadata", "convention"))


def _is_formula_like(value: str) -> bool:
    compact = re.sub(r"[^A-Za-z0-9]+", "", str(value or ""))
    return bool(re.fullmatch(r"(?:[A-Z][a-z]?\d*){2,}", compact))


def _is_identifier_like(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]{1,5}[-_ ]?\d+[A-Za-z0-9_-]*", str(value or "").strip()))


def _looks_like_chemical_name(value: str) -> bool:
    tokens = set(_tokens(value))
    functional_terms = {
        "acid", "aldehyde", "ketone", "alcohol", "ester", "amine", "amide", "oxide",
        "chloride", "sulfate", "phosphate", "methyl", "ethyl", "propyl", "butyl",
        "phenyl", "benzyl", "hexyl",
    }
    if tokens & functional_terms:
        return True
    return bool(re.search(r"\b\d+[- ]?(?:en|yn|ol|al|one|oic)\b|(?:enal|enol|one|aldehyde)$", _variant_loose_key(value)))


def _shares_domain_anchor(original_term: str, definition: str, text: str) -> bool:
    original_tokens = set(_tokens(original_term))
    context_tokens = set(_tokens(definition))
    value_tokens = set(_tokens(text))
    anchors = {tok for tok in original_tokens | context_tokens if len(tok) >= 5}
    generic = {"method", "assay", "process", "chemical", "substance", "material", "variable"}
    anchors -= generic
    return bool(anchors and value_tokens & anchors)


def _looks_like_abbreviation_collision(
    original_term: str,
    text: str,
    kind: str,
    *,
    definition: str = "",
    term_profile: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    original = _variant_key(original_term)
    value = _variant_key(text)
    original_tokens = _tokens(original_term)
    value_tokens = _tokens(text)
    term_type = _primary_term_type(payload, term_profile)
    context = _context_text(definition, term_profile)
    if not value or value == original:
        return False
    if value in GENERIC_VARIANTS:
        return True

    original_is_short_abbr = (
        len(original_tokens) == 1
        and 1 < len(original_tokens[0]) <= 5
    )
    if kind == "abbreviation_expansion" and original_is_short_abbr:
        if len(value_tokens) < 2:
            return True
        if _initials(text) != original.upper():
            return True
        if context and not _shares_domain_anchor(original_term, definition, text):
            # For short abbreviations, require either an initialism match plus a context
            # anchor from the definition/profile, or leave it to the LLM to omit it.
            return True

    if _is_identifier_like(text) and not _is_identifier_like(original_term):
        return True

    if kind == "chemical_formula":
        if not (_is_formula_like(original_term) or _is_chemical_term_type(term_type, context)):
            return True
        if original_is_short_abbr and value.upper() != original.upper():
            return True

    if _is_method_term_type(term_type, context):
        clinical_or_product_tokens = {
            "regimen", "drug", "dose", "tablet", "capsule", "device", "oral", "injection",
            "therapy", "treatment",
        }
        if set(value_tokens) & clinical_or_product_tokens:
            return True

    if _is_data_or_vocabulary_term_type(term_type, context):
        biomedical_or_product_tokens = {
            "drug", "diabetic", "tablet", "capsule", "device", "oral", "therapy", "disease",
            "patient", "clinical",
        }
        if set(value_tokens) & biomedical_or_product_tokens:
            return True

    if original_tokens and not _is_chemical_term_type(term_type, context):
        # Prevent lexical fragments of a longer biological/material term from drifting
        # into an unrelated chemical name unless the definition/profile supports chemistry.
        long_original = {tok for tok in original_tokens if len(tok) >= 5}
        if long_original and not (set(value_tokens) & long_original) and kind in {"canonical_name", "chemical_formula"}:
            return True
        if long_original and not (set(value_tokens) & long_original) and _looks_like_chemical_name(text):
            return True
    return False


def _is_allowed_variant(
    raw: Dict[str, Any],
    *,
    original_term: str,
    normalized_kind: str,
    definition: str = "",
    term_profile: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    text = _norm_text(raw.get("text") if isinstance(raw, dict) else raw)
    if _is_marker_variant(text):
        return False
    if normalized_kind in BLOCKED_VARIANT_KINDS:
        return False
    if normalized_kind not in ALLOWED_VARIANT_KINDS:
        return False
    if _looks_like_abbreviation_collision(
        original_term,
        text,
        normalized_kind,
        definition=definition,
        term_profile=term_profile,
        payload=payload,
    ):
        return False
    return True


def parse_annotator_variant_payload(
    payload: Dict[str, Any],
    *,
    original_term: str,
    term_id: str = "T001",
    max_variants: int = MAX_DEFAULT_VARIANTS,
    min_variants: int = MIN_DEFAULT_VARIANTS,
    local_variants: Optional[Sequence[Dict[str, Any]]] = None,
    definition: str = "",
    term_profile: Optional[Dict[str, Any]] = None,
) -> List[AnnotatorRescueVariant]:
    """Parse structured LLM JSON into bounded, deduplicated variants.

    Invalid or empty payloads degrade to the original term only. Local variants are
    prepended so existing normalizations/synonyms win over newly generated variants.
    """
    max_n = max(1, int(max_variants or MAX_DEFAULT_VARIANTS))
    min_n = max(1, int(min_variants or MIN_DEFAULT_VARIANTS))
    parsed: List[AnnotatorRescueVariant] = []
    seen: set[str] = set()

    def add(raw: Dict[str, Any], default_kind: str = "variant") -> None:
        text = _norm_text(raw.get("text") if isinstance(raw, dict) else raw)
        kind = _normalize_variant_kind(raw.get("kind") or default_kind if isinstance(raw, dict) else default_kind)
        if not _is_allowed_variant(
            raw,
            original_term=original_term,
            normalized_kind=kind,
            definition=definition,
            term_profile=term_profile,
            payload=payload,
        ):
            return
        key = _variant_key(text)
        if key in seen:
            return
        seen.add(key)
        idx = len(parsed) + 1
        parsed.append(
            AnnotatorRescueVariant(
                term_id=str(raw.get("term_id") or payload.get("term_id") or term_id or "T001"),
                variant_id=str(raw.get("variant_id") or f"V{idx:03d}"),
                text=text,
                kind=kind,
                expected_strength=str(raw.get("expected_strength") or "medium").strip() or "medium",
            )
        )

    add({"term_id": term_id, "variant_id": "V001", "text": original_term, "kind": "original", "expected_strength": "weak"})
    for item in local_variants or []:
        if isinstance(item, dict):
            add(item)
    for item in (payload.get("variants") if isinstance(payload, dict) else []) or []:
        if isinstance(item, dict):
            add(item)

    if len(parsed) < min_n and _norm_text(original_term):
        add({"term_id": term_id, "text": original_term, "kind": "original", "expected_strength": "weak"})
    return parsed[:max_n]


def build_annotator_text(variants: Sequence[AnnotatorRescueVariant]) -> Tuple[str, List[AnnotatorTextSpan]]:
    """Build marker-free annotator text and a 1-based offset table."""
    chunks: List[str] = []
    spans: List[AnnotatorTextSpan] = []
    cursor = 1
    for variant in variants:
        text = _norm_text(variant.text)
        if not text:
            continue
        if chunks:
            chunks.append("\n")
            cursor += 1
        start = cursor
        chunks.append(text)
        cursor += len(text)
        end = cursor - 1
        chunks.append(".")
        cursor += 1
        spans.append(
            AnnotatorTextSpan(
                term_id=variant.term_id,
                variant_id=variant.variant_id,
                variant_text=text,
                variant_kind=variant.kind,
                expected_strength=variant.expected_strength,
                start=start,
                end=end,
            )
        )
    return "".join(chunks), spans


def map_offset_to_variant(start: Any, end: Any, spans: Sequence[AnnotatorTextSpan]) -> Optional[AnnotatorTextSpan]:
    try:
        start_i = int(start)
        end_i = int(end)
    except Exception:
        return None
    for span in spans:
        if start_i <= span.end and end_i >= span.start:
            return span
    return None


def annotator_ontologies_for_term_type(term_profile: Dict[str, Any], *, debug_broad: bool = False) -> List[str]:
    if debug_broad:
        return []
    term_types = [
        str((term_profile or {}).get("term_type") or ""),
        str((term_profile or {}).get("primary_term_type") or ""),
        *[str(item or "") for item in ((term_profile or {}).get("secondary_term_types") or [])],
    ]
    out: List[str] = []
    seen = set()
    for term_type in term_types:
        key = term_type.strip().lower()
        for onto in TERM_TYPE_TO_ANNOTATOR_ONTOLOGIES.get(key, []):
            if onto not in seen:
                seen.add(onto)
                out.append(onto)
    if out:
        return out
    return DEFAULT_ANNOTATOR_ONTOLOGIES.copy()


def _candidate_ontology(candidate: Any) -> str:
    context = getattr(candidate, "ontology_context", {}) or {}
    if isinstance(context, dict) and context.get("ontology_acronym"):
        return str(context.get("ontology_acronym") or "").strip().upper()
    return str(getattr(candidate, "source_provider", "") or "").strip().upper()


def _score_value(score: Any) -> float:
    for attr in ("combined_confidence", "confidence"):
        try:
            value = getattr(score, attr, None)
            if value is not None:
                return float(value)
        except Exception:
            continue
    return 0.0


def _symbol_like(term: str) -> bool:
    raw = str(term or "").strip()
    if not raw:
        return False
    if "_" in raw or re.search(r"[A-Za-z]+[_-]?[a-z]*\d", raw):
        return True
    if re.fullmatch(r"[A-Z][a-z]?[A-Z]?[a-z]?\d*", raw) and 2 <= len(raw) <= 8:
        return True
    return bool(re.fullmatch(r"[A-Za-z]{1,5}", raw) and any(ch.isupper() for ch in raw[1:]))


def _looks_non_english(term: str) -> bool:
    text = str(term or "").strip()
    if re.search(r"[äöüÄÖÜß]", text):
        return True
    # Lightweight German morphology cue; only used with an already weak pool.
    return bool(re.search(r"(salz|käse|milch|fleisch|weizen|roggen|öl|saft)$", text.lower()))


def _generic_pool_only(ranked_candidates: Sequence[Any]) -> bool:
    if not ranked_candidates:
        return False
    checked = 0
    for score in list(ranked_candidates)[:5]:
        label = str(getattr(getattr(score, "candidate", None), "label", "") or "").strip().lower()
        if not label:
            continue
        checked += 1
        tokens = {tok for tok in re.split(r"[^a-z0-9]+", label) if tok}
        if tokens == {"sodium", "chloride"}:
            return False
        if not tokens or not tokens <= GENERIC_VARIANTS:
            return False
    return checked > 0


def _profile_value(term_profile: Optional[Dict[str, Any]], *keys: str) -> str:
    profile = term_profile or {}
    for key in keys:
        value = profile.get(key) if isinstance(profile, dict) else getattr(profile, key, None)
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            value = " ".join(str(item or "") for item in value)
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _term_profile_text(term_profile: Optional[Dict[str, Any]], definition: str = "") -> str:
    profile = term_profile or {}
    parts = [definition or ""]
    keys = (
        "term_type", "primary_term_type", "secondary_term_types", "domain", "category",
        "category_context_used", "rdf_role", "role", "application_context",
        "preferred_resolver", "resolver", "language",
    )
    for key in keys:
        if isinstance(profile, dict):
            value = profile.get(key)
        else:
            value = getattr(profile, key, None)
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(item or "") for item in value)
        elif value is not None:
            parts.append(str(value or ""))
    return _variant_loose_key(" ".join(parts))


def _best_candidate_snapshot(ranked: Sequence[Any]) -> Dict[str, Any]:
    if not ranked:
        return {
            "score": None,
            "label": None,
            "uri": None,
            "ontology": None,
            "mapping_type": None,
            "domain_fit": None,
            "termtype_fit": None,
        }
    top = ranked[0]
    candidate = getattr(top, "candidate", None)
    metadata = getattr(top, "trace_metadata", {}) or {}
    return {
        "score": _score_value(top),
        "label": str(getattr(candidate, "label", "") or "") or None,
        "uri": str(getattr(candidate, "uri", "") or "") or None,
        "ontology": _candidate_ontology(candidate) or None,
        "mapping_type": str(getattr(top, "mapping_type", "") or "").strip().lower(),
        "domain_fit": metadata.get("candidate_domain_fit") or metadata.get("hierarchy_domain_fit") or metadata.get("domain_fit"),
        "termtype_fit": metadata.get("termtype_fit") or metadata.get("ontology_family_fit"),
    }


def _domain_or_termtype_fit_ok(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return True
    return text not in {"mismatch", "wrong", "wrong_branch", "incompatible", "blocked", "poor", "low"}


def _good_existing_candidate_available(ranked: Sequence[Any], *, min_score: float) -> bool:
    if not ranked:
        return False
    top = ranked[0]
    snapshot = _best_candidate_snapshot(ranked)
    score = float(snapshot["score"] or 0.0)
    mapping = str(snapshot.get("mapping_type") or "").lower()
    metadata = getattr(top, "trace_metadata", {}) or {}
    if bool(metadata.get("hard_domain_mismatch")) or bool(metadata.get("registry_suitability_block")):
        return False
    if not _domain_or_termtype_fit_ok(snapshot.get("domain_fit")):
        return False
    if not _domain_or_termtype_fit_ok(snapshot.get("termtype_fit")):
        return False
    threshold = max(float(min_score or 0.0), 0.70)
    if mapping in {"exact", "close"} and score >= float(min_score or 0.0):
        return True
    return score >= threshold


def _provider_specific_resolver_reason(term: str, term_profile: Optional[Dict[str, Any]], definition: str = "") -> Optional[str]:
    preferred = _profile_value(term_profile, "preferred_resolver", "resolver").strip().lower()
    if preferred and preferred not in {"bioportal", "ontology", "ontologies", "bioportal_annotator"}:
        return "provider_specific_resolver_needed_instead"
    profile_text = _term_profile_text(term_profile, definition)
    provider_markers = {
        "linked data standard", "linked_data_standard", "semantic web concept",
        "semantic_web_concept", "rdf or vocabulary property", "rdf_or_vocabulary_property",
        "schema or namespace term", "schema_or_namespace_term", "namespace",
        "climate variable", "climate_variable", "climate variable standard",
        "geospatial standard", "geospatial_standard", "statistical region code",
        "statistical_region_code",
    }
    if any(marker.replace("_", " ") in profile_text or marker in profile_text for marker in provider_markers):
        return "provider_specific_resolver_needed_instead"
    term_text = _variant_loose_key(term)
    if "region" in term_text and re.search(r"\b[a-z]{2,6}\d+\b", term_text):
        return "provider_specific_resolver_needed_instead"
    return None


def _has_clear_domain_or_termtype_mismatch(
    ranked: Sequence[Any],
    expected: Sequence[str],
    *,
    min_score: float,
) -> bool:
    if not ranked:
        return False
    if any(_score_value(score) >= max(float(min_score or 0.0), 0.70) for score in list(ranked)[:5]):
        return False
    expected_set = {str(item or "").strip().upper() for item in expected if str(item or "").strip()}
    present = {_candidate_ontology(getattr(score, "candidate", None)) for score in list(ranked)[:5]}
    present.discard("")
    if expected_set and present and present.isdisjoint(expected_set):
        return True
    for score in list(ranked)[:3]:
        metadata = getattr(score, "trace_metadata", {}) or {}
        if bool(metadata.get("hard_domain_mismatch")) or bool(metadata.get("registry_suitability_block")):
            return True
        if str(metadata.get("candidate_domain_mismatch_level") or "").lower() in {"hard", "high"}:
            return True
    return False


def _pool_primary_gate_reason(
    term: str,
    ranked: Sequence[Any],
    term_profile: Optional[Dict[str, Any]],
    *,
    min_score: float,
    triage: Optional[str],
    expected: Sequence[str],
) -> Tuple[bool, Optional[str]]:
    if not ranked:
        return True, "pool_empty"
    triage_norm = str(triage or "").strip().lower()
    if triage_norm in {"weak", "reject", "no_match"}:
        return True, "llm_rejected_all_candidates"
    best_score = max((_score_value(score) for score in ranked), default=0.0)
    if len(ranked) <= 1 and best_score < min_score:
        return True, "pool_very_small_and_weak"
    if _generic_pool_only(ranked):
        return True, "all_candidates_generic"
    if _has_clear_domain_or_termtype_mismatch(ranked, expected, min_score=min_score):
        return True, "clear_domain_or_termtype_mismatch"
    if best_score < min_score:
        return True, "no_candidate_above_threshold"
    return False, None


def _context_supports_symbol_expansion(
    term: str,
    term_profile: Optional[Dict[str, Any]],
    definition: str = "",
) -> bool:
    term_type = _profile_value(term_profile, "term_type", "primary_term_type").strip().lower()
    context = _term_profile_text(term_profile, definition)
    if _is_method_term_type(term_type, context):
        return True
    if any(token in context for token in ("measurement", "property", "acidity", "alkalinity", "ph value", "assay", "method")):
        return True
    if _is_formula_like(term) and _is_chemical_term_type(term_type, context):
        return True
    if any(token in context for token in ("gene", "variant", "locus", "allele")):
        return True
    return False


def _composite_term_has_variant_potential(term: str, term_profile: Optional[Dict[str, Any]], definition: str = "") -> bool:
    text = _variant_loose_key(term)
    tokens = _tokens(term)
    profile_text = _term_profile_text(term_profile, definition)
    if "_" in str(term or "") and len(tokens) >= 2:
        return True
    if " in " in f" {text} " and len(tokens) >= 3:
        return True
    domain_terms = {
        "concentration", "activity", "assay", "toxin", "enterotoxin", "limit",
        "migration", "dose", "ratio", "cytotoxicity", "sample", "material",
        "fabric", "film", "foil", "casing",
    }
    if len(tokens) >= 2 and (set(tokens) & domain_terms):
        return True
    if len(tokens) >= 3 and any(token in profile_text for token in ("chemical", "material", "measurement", "assay", "toxin", "method")):
        return True
    return False


def _secondary_reason(
    term: str,
    ranked: Sequence[Any],
    term_profile: Optional[Dict[str, Any]],
    *,
    expected: Sequence[str],
    min_score: float,
    triage: Optional[str],
    llm_decision: Optional[str],
    definition: str = "",
) -> Tuple[bool, Optional[str]]:
    profile_text = _term_profile_text(term_profile, definition)
    language = _profile_value(term_profile, "language", "detected_language").strip().lower()
    if _looks_non_english(term) or language not in {"", "en", "eng", "english"}:
        return True, "non_english_or_crosslingual_variant_needed"
    if _composite_term_has_variant_potential(term, term_profile, definition):
        return True, "composite_term_with_variant_block_potential"
    if _symbol_like(term) and _context_supports_symbol_expansion(term, term_profile, definition):
        return True, "abbreviation_or_symbol_with_context_supported_expansion"
    expected_set = {str(item or "").strip().upper() for item in expected if str(item or "").strip()}
    present = {_candidate_ontology(getattr(score, "candidate", None)) for score in list(ranked)[:10]}
    present.discard("")
    best_score = max((_score_value(score) for score in ranked), default=0.0)
    if expected_set and present and present.isdisjoint(expected_set) and not _good_existing_candidate_available(ranked, min_score=min_score):
        if best_score < 0.75 or _has_clear_domain_or_termtype_mismatch(ranked, expected, min_score=min_score):
            return True, "expected_ontology_family_missing_after_adequate_search"
    llm_text = f"{llm_decision or ''} {triage or ''} {profile_text}".lower()
    if any(phrase in llm_text for phrase in (
        "correct candidate missing", "candidate missing", "likely missing", "try alternative",
        "alternative synonym", "all candidates unsuitable",
    )):
        return True, "llm_reports_likely_missing_candidate"
    return False, None


def should_run_annotator_rescue(
    *,
    term: str,
    term_profile: Dict[str, Any],
    ranked_candidates: Sequence[Any],
    config: Any,
    bioportal_api_key: Optional[str] = None,
    triage: Optional[str] = None,
    min_score: float = 0.55,
    stage: str = "initial_broad_retrieval",
    llm_decision: Optional[str] = None,
    llm_match_type: Optional[str] = None,
    expected_ontologies: Optional[Sequence[str]] = None,
    definition: str = "",
) -> AnnotatorRescueDecision:
    """Central two-stage trigger for the BioPortal Annotator rescue path.

    The return object remains iterable as ``(should_run, reason)`` for existing
    callers, but also carries structured gate, blocker and trace metadata.
    """
    ranked = list(ranked_candidates or [])
    expected = list(expected_ontologies or annotator_ontologies_for_term_type(term_profile or {}))
    observed = sorted({
        ontology for ontology in (
            _candidate_ontology(getattr(score, "candidate", None)) for score in ranked
        )
        if ontology
    })
    top = _best_candidate_snapshot(ranked)

    def decision(
        should_run: bool,
        reason: str,
        *,
        primary_gate_passed: bool = False,
        primary_gate_reason: Optional[str] = None,
        secondary_reason_present: bool = False,
        secondary_reason: Optional[str] = None,
        blocked: bool = False,
        block_reason: Optional[str] = None,
    ) -> AnnotatorRescueDecision:
        return AnnotatorRescueDecision(
            should_run=bool(should_run),
            reason=reason,
            stage=stage,
            primary_gate_passed=bool(primary_gate_passed),
            primary_gate_reason=primary_gate_reason,
            secondary_reason_present=bool(secondary_reason_present),
            secondary_reason=secondary_reason,
            blocked=bool(blocked),
            block_reason=block_reason,
            top_candidate_uri=top.get("uri"),
            top_candidate_label=top.get("label"),
            top_candidate_score=top.get("score"),
            top_candidate_ontology=top.get("ontology"),
            candidate_count=len(ranked),
            expected_ontology_families=list(expected),
            observed_ontology_families=observed,
            llm_decision=llm_decision,
            llm_match_type=llm_match_type,
        )

    if not bool(getattr(config, "enable_bioportal_annotator_rescue", True)):
        return decision(False, "annotator_rescue_disabled")
    if not str(bioportal_api_key or "").strip():
        return decision(False, "missing_bioportal_api_key")
    if str(triage or "").strip().lower() == "verified":
        return decision(False, "already_verified", blocked=True, block_reason="already_verified")

    primary_passed, primary_reason = _pool_primary_gate_reason(
        term,
        ranked,
        term_profile,
        min_score=min_score,
        triage=triage,
        expected=expected,
    )
    secondary_present, secondary = _secondary_reason(
        term,
        ranked,
        term_profile,
        expected=expected,
        min_score=min_score,
        triage=triage,
        llm_decision=llm_decision,
        definition=definition,
    )

    block_reason = _provider_specific_resolver_reason(term, term_profile, definition)
    llm_accept = str(llm_match_type or llm_decision or "").strip().lower() in {
        "matched", "exactmatch", "closematch", "exact", "close", "verified", "acceptable", "accepted",
    }
    if not block_reason and llm_accept:
        block_reason = "llm_accepts_current_best_candidate"
    if not block_reason and _good_existing_candidate_available(ranked, min_score=min_score):
        block_reason = "good_existing_candidate_available"
    if block_reason:
        return decision(
            False,
            block_reason,
            primary_gate_passed=primary_passed,
            primary_gate_reason=primary_reason,
            secondary_reason_present=secondary_present,
            secondary_reason=secondary,
            blocked=True,
            block_reason=block_reason,
        )
    if not primary_passed:
        return decision(
            False,
            "pool_sufficient",
            primary_gate_passed=False,
            primary_gate_reason=primary_reason,
            secondary_reason_present=secondary_present,
            secondary_reason=secondary,
        )
    if not secondary_present:
        return decision(
            False,
            "no_annotator_applicable_secondary_reason",
            primary_gate_passed=True,
            primary_gate_reason=primary_reason,
        )
    return decision(
        True,
        f"{primary_reason}+{secondary}",
        primary_gate_passed=True,
        primary_gate_reason=primary_reason,
        secondary_reason_present=True,
        secondary_reason=secondary,
    )


def _ontology_from_class(annotated_class: Dict[str, Any]) -> str:
    links = annotated_class.get("links") if isinstance(annotated_class, dict) else {}
    ontology_link = links.get("ontology") if isinstance(links, dict) else ""
    if ontology_link:
        return urlparse(str(ontology_link)).path.rstrip("/").split("/")[-1].upper()
    uri = str(annotated_class.get("@id") or "")
    parts = uri.split("/")
    return parts[-2].upper() if len(parts) >= 2 and parts[-2].isupper() else ""


def _label_from_group(uri: str, evidence: List[Dict[str, Any]]) -> str:
    priority = {
        "canonical_name": 0,
        "scientific_name": 1,
        "abbreviation_expansion": 2,
        "domain_synonym": 3,
        "translation": 4,
        "common_name": 5,
        "original": 5,
        "chemical_formula": 6,
    }
    ordered = sorted(
        evidence,
        key=lambda ev: (
            priority.get(str(ev.get("variant_kind") or ""), 9),
            -len(str(ev.get("variant_text") or ev.get("matched_text") or "")),
        ),
    )
    for ev in ordered:
        text = _norm_text(ev.get("variant_text") or ev.get("matched_text"))
        if text and not _is_marker_variant(text):
            return text
    tail = uri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]
    return tail or uri


def _rescue_score(uri: str, ontology: str, evidence: List[Dict[str, Any]], expected_ontologies: Sequence[str]) -> float:
    variant_ids = {str(ev.get("variant_id") or "") for ev in evidence if ev.get("variant_id")}
    good_kinds = {
        "translation",
        "domain_synonym",
        "canonical_name",
        "abbreviation_expansion",
        "chemical_formula",
        "scientific_name",
        "common_name",
        "spelling_variant",
    }
    good_variant_ids = {
        str(ev.get("variant_id") or "")
        for ev in evidence
        if str(ev.get("variant_kind") or "") in good_kinds and ev.get("variant_id")
    }
    score = 0.12 + min(0.18, len(good_variant_ids) * 0.05)
    expected = {str(item).upper() for item in expected_ontologies or []}
    if ontology.upper() in expected:
        score += 0.10
    for ev in evidence:
        kind = str(ev.get("variant_kind") or "").strip().lower()
        strength = str(ev.get("expected_strength") or "").strip().lower()
        score += KIND_WEIGHT.get(kind, 0.04)
        score += STRENGTH_WEIGHT.get(strength, 0.04)
        text_key = _variant_key(str(ev.get("variant_text") or ev.get("matched_text") or ""))
        if text_key in GENERIC_VARIANTS:
            score -= 0.12
        if len(text_key) <= 2:
            score -= 0.08
        if re.fullmatch(r"t\d+v\d+", text_key or ""):
            score -= 0.25
    if "CHEBI_26710" in uri and any("sodium chloride" in _variant_key(str(ev.get("variant_text") or "")) for ev in evidence):
        score += 0.05
    if len(good_variant_ids) < 2:
        score = min(score, 0.55)
    return round(max(0.05, min(0.82, score)), 4)


def parse_annotator_response(
    raw_data: Any,
    spans: Sequence[AnnotatorTextSpan],
    *,
    term_profile: Optional[Dict[str, Any]] = None,
) -> List[AgentCandidate]:
    records = raw_data if isinstance(raw_data, list) else []
    expected_ontologies = annotator_ontologies_for_term_type(term_profile or {})
    grouped: Dict[str, Dict[str, Any]] = {}
    raw_order = 0
    for record in records:
        if not isinstance(record, dict):
            continue
        raw_order += 1
        annotated_class = record.get("annotatedClass") or {}
        if not isinstance(annotated_class, dict):
            continue
        uri = str(annotated_class.get("@id") or "").strip()
        if not uri:
            continue
        ontology = _ontology_from_class(annotated_class)
        annotations = record.get("annotations") or []
        if not isinstance(annotations, list):
            annotations = []
        if not annotations:
            annotations = [{}]
        group = grouped.setdefault(
            uri,
            {
                "uri": uri,
                "ontology": ontology,
                "prefLabel": str(annotated_class.get("prefLabel") or ""),
                "links": annotated_class.get("links") if isinstance(annotated_class.get("links"), dict) else {},
                "evidence": [],
                "raw_order": raw_order,
                "is_mapping_expansion": bool(record.get("mappings") or record.get("mapping")),
            },
        )
        for annotation in annotations:
            if not isinstance(annotation, dict):
                continue
            span = map_offset_to_variant(annotation.get("from"), annotation.get("to"), spans)
            matched_text = _norm_text(annotation.get("text"))
            if span is None and _is_marker_variant(matched_text):
                continue
            ev = {
                "term_id": span.term_id if span else "",
                "variant_id": span.variant_id if span else "",
                "variant_text": span.variant_text if span else "",
                "variant_kind": span.variant_kind if span else "",
                "expected_strength": span.expected_strength if span else "",
                "matched_text": matched_text,
                "from": annotation.get("from", ""),
                "to": annotation.get("to", ""),
                "match_type": annotation.get("matchType") or annotation.get("match_type") or "",
                "ontology": ontology,
                "class_uri": uri,
                "raw_order": raw_order,
            }
            group["evidence"].append(ev)

    candidates: List[AgentCandidate] = []
    for uri, group in grouped.items():
        evidence = group["evidence"]
        if not evidence:
            continue
        ontology = str(group.get("ontology") or "BioPortal").upper()
        rescue_score = _rescue_score(uri, ontology, evidence, expected_ontologies)
        label = _norm_text(group.get("prefLabel")) or _label_from_group(uri, evidence)
        matched_variants = sorted({str(ev.get("variant_text") or ev.get("matched_text") or "") for ev in evidence if str(ev.get("variant_text") or ev.get("matched_text") or "").strip()})
        candidates.append(
            AgentCandidate(
                uri=uri,
                label=label,
                description="",
                source_provider=ontology or "BioPortal",
                source_workflow="bioportal_annotator_rescue",
                raw_identifier=uri,
                score=rescue_score,
                ontology_context={
                    "ontology_acronym": ontology,
                    "class_links": group.get("links") or {},
                    "match_source": "bioportal_annotator_rescue",
                    "lexical_match_type": "annotator_variant",
                    "annotator_rescue_score": rescue_score,
                    "annotator_rescue_evidence": evidence,
                    "annotator_rescue_evidence_count": len(evidence),
                    "annotator_rescue_matched_variants": matched_variants,
                    "annotator_rescue_raw_order": group.get("raw_order"),
                    "is_mapping_expansion": bool(group.get("is_mapping_expansion")),
                },
                source_links=group.get("links") or {},
            )
        )
    candidates.sort(key=lambda item: float(item.score or 0.0), reverse=True)
    return candidates


def _request_annotator_json(
    *,
    text: str,
    api_key: str,
    ontologies: Sequence[str],
    base_url: str,
    timeout: float,
    debug_broad: bool,
) -> Tuple[Any, Dict[str, Any], List[str], str]:
    params: Dict[str, Any] = {
        "text": text,
        "longest_only": "true",
        "exclude_synonyms": "false",
        "expand_mappings": "false",
        "expand_class_hierarchy": "false",
        "exclude_numbers": "true",
        "whole_word_only": "true",
        "apikey": api_key,
    }
    if ontologies and not debug_broad:
        params["ontologies"] = ",".join(ontologies)
    url = f"{base_url.rstrip('/')}/annotator"
    headers = {"Accept": "application/json", "User-Agent": ANNOTATOR_USER_AGENT}
    dropped: List[str] = []
    optional_index = 0
    started = time.perf_counter()
    session = requests.Session()

    while True:
        try:
            response = session.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and str(retry_after).isdigit() else 3.0
                time.sleep(wait)
                response = session.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code < 400:
                request_meta = {
                    "endpoint": "annotator",
                    "params": {k: ("***MASKED***" if k == "apikey" else v) for k, v in params.items()},
                    "status_code": response.status_code,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                }
                return response.json(), request_meta, dropped, ""
            if response.status_code == 400 and optional_index < len(ANNOTATOR_OPTIONAL_PARAMS):
                param = ANNOTATOR_OPTIONAL_PARAMS[optional_index]
                optional_index += 1
                if param in params:
                    params.pop(param, None)
                    dropped.append(param)
                continue
            request_meta = {
                "endpoint": "annotator",
                "params": {k: ("***MASKED***" if k == "apikey" else v) for k, v in params.items()},
                "status_code": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
            }
            return None, request_meta, dropped, response.text[:500]
        except (requests.RequestException, ValueError) as exc:
            request_meta = {
                "endpoint": "annotator",
                "params": {k: ("***MASKED***" if k == "apikey" else v) for k, v in params.items()},
                "status_code": None,
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
            }
            return None, request_meta, dropped, f"{type(exc).__name__}: {exc}"


def run_bioportal_annotator_rescue(
    *,
    term: str,
    definition: str,
    variants: Sequence[AnnotatorRescueVariant],
    api_key: str,
    term_profile: Optional[Dict[str, Any]] = None,
    max_candidates: int = 20,
    timeout_seconds: float = 30.0,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    debug_broad_ontologies: bool = False,
) -> AnnotatorRescueResult:
    del definition  # reserved for future request metadata; variants carry the query text.
    annotator_text, spans = build_annotator_text(variants)
    if not annotator_text.strip():
        return AnnotatorRescueResult(error="empty_annotator_text")
    ontologies = annotator_ontologies_for_term_type(term_profile or {}, debug_broad=debug_broad_ontologies)
    raw, request_meta, dropped, error = _request_annotator_json(
        text=annotator_text,
        api_key=api_key,
        ontologies=ontologies,
        base_url=base_url,
        timeout=timeout_seconds,
        debug_broad=debug_broad_ontologies,
    )
    if error:
        return AnnotatorRescueResult(
            variants=[variant.__dict__.copy() for variant in variants],
            request_metadata=request_meta,
            dropped_params=dropped,
            error=error,
        )
    candidates = parse_annotator_response(raw, spans, term_profile=term_profile or {})
    return AnnotatorRescueResult(
        candidates=candidates[: max(1, int(max_candidates or 20))],
        variants=[variant.__dict__.copy() for variant in variants],
        request_metadata=request_meta,
        annotation_count=sum(len((row.get("annotations") or [])) for row in raw if isinstance(row, dict)) if isinstance(raw, list) else 0,
        grouped_candidate_count=len(candidates),
        dropped_params=dropped,
    )
