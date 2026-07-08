# -*- coding: utf-8 -*-
"""LLM-assisted ontology routing / suitability scoring.

This layer decides WHICH ontologies are worth querying for a given input term +
definition + context. It is deliberately separate from candidate confidence:

  * ontology suitability score  -> "should this ontology be queried for this term?"
  * candidate confidence        -> "is this ontology class the correct mapping?"

A routing score must NEVER be copied into a candidate's confidence. This module
only selects/prioritizes ontologies for retrieval and records routing telemetry.

Administrative metadata (homepage, documentation URLs, ontology_type, license,
"US Edition" blurbs) is intentionally kept out of the routing profile sent to the
LLM — the model only needs to know what kinds of concepts an ontology contains.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .agent_llm_service import generate_structured_completion as _default_generate_structured_completion

# Patchable seam (mirrors the workflow/adjudication pattern) so tests can inject
# a fake LLM without a real provider call.
generate_structured_completion = _default_generate_structured_completion

CAPABILITY_PROFILES_PATH = Path(__file__).resolve().parent / "config" / "ontology_capability_profiles.json"
ROUTING_PROMPT_VERSION = "ontology-routing-v1"

# Broad generalists and project-priority ontologies that should stay eligible for
# retrieval even when their deterministic prefilter score is low.
BROAD_ONTOLOGIES = {"MESH", "SNOMEDCT", "NCIT", "MEDDRA", "RCD", "NCBITAXON"}
PROJECT_PRIORITY_ONTOLOGIES = {"FOODON", "CHEBI", "GENEPIO", "QUDT", "QUDT2"}

_ROUTING_STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "are", "was", "obtained",
    "used", "use", "uses", "using", "such", "into", "out", "via", "per", "any",
    "all", "one", "its", "their", "them", "they", "which", "while", "also", "may",
    "can", "not", "but", "has", "have", "had", "more", "most", "other", "general",
    "type", "types", "kind", "kinds", "term", "terms", "concept", "concepts",
}


@dataclass
class OntologyCapabilityProfile:
    """Semantic routing profile for one ontology (separate from raw metadata)."""

    acronym: str
    display_name: str = ""
    routing_description: str = ""
    contains: List[str] = field(default_factory=list)
    best_for: List[str] = field(default_factory=list)
    not_best_for: List[str] = field(default_factory=list)
    domain_tags: List[str] = field(default_factory=list)
    routing_keywords: List[str] = field(default_factory=list)
    negative_keywords: List[str] = field(default_factory=list)
    example_classes: List[str] = field(default_factory=list)
    generality: float = 0.5
    profile_confidence: float = 0.5
    source: str = "curated"
    last_updated: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "acronym": self.acronym,
            "display_name": self.display_name,
            "routing_description": self.routing_description,
            "contains": list(self.contains),
            "best_for": list(self.best_for),
            "not_best_for": list(self.not_best_for),
            "domain_tags": list(self.domain_tags),
            "routing_keywords": list(self.routing_keywords),
            "negative_keywords": list(self.negative_keywords),
            "example_classes": list(self.example_classes),
            "generality": float(self.generality),
            "profile_confidence": float(self.profile_confidence),
            "source": self.source,
            "last_updated": self.last_updated,
        }

    def compact_for_prompt(self) -> Dict[str, Any]:
        """Decision-useful subset for the LLM prompt — NO administrative metadata."""
        return {
            "acronym": self.acronym,
            "name": self.display_name or self.acronym,
            "routing_description": self.routing_description,
            "contains": list(self.contains)[:10],
            "best_for": list(self.best_for)[:10],
            "not_best_for": list(self.not_best_for)[:8],
            "domain_tags": list(self.domain_tags)[:8],
            "example_classes": list(self.example_classes)[:8],
            "generality": round(float(self.generality), 2),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OntologyCapabilityProfile":
        def _l(value: Any) -> List[str]:
            if isinstance(value, list):
                return [str(item).strip() for item in value if str(item).strip()]
            if isinstance(value, str) and value.strip():
                return [value.strip()]
            return []

        def _f(value: Any, default: float) -> float:
            try:
                return max(0.0, min(1.0, float(value)))
            except Exception:
                return default

        return cls(
            acronym=str(data.get("acronym", "") or "").strip().upper(),
            display_name=str(data.get("display_name", "") or "").strip(),
            routing_description=str(data.get("routing_description", "") or "").strip(),
            contains=_l(data.get("contains")),
            best_for=_l(data.get("best_for")),
            not_best_for=_l(data.get("not_best_for")),
            domain_tags=_l(data.get("domain_tags")),
            routing_keywords=_l(data.get("routing_keywords")),
            negative_keywords=_l(data.get("negative_keywords")),
            example_classes=_l(data.get("example_classes")),
            generality=_f(data.get("generality"), 0.5),
            profile_confidence=_f(data.get("profile_confidence"), 0.5),
            source=str(data.get("source", "curated") or "curated").strip(),
            last_updated=(str(data.get("last_updated")) if data.get("last_updated") else None),
        )


@dataclass
class PrefilteredOntologyProfile:
    profile: OntologyCapabilityProfile
    prefilter_score: float
    forced_included: bool = False
    reasons: List[str] = field(default_factory=list)


@dataclass
class RankedOntology:
    acronym: str
    score: float  # LLM suitability score (NOT candidate confidence)
    reason: str = ""
    positive_signals: List[str] = field(default_factory=list)
    negative_signals: List[str] = field(default_factory=list)
    profile_source: str = ""
    quality_score: Optional[float] = None
    quality_status: str = ""
    effective_score: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "acronym": self.acronym,
            "llm_suitability_score": round(float(self.score), 4),
            "score": round(float(self.score), 4),
            "ontology_quality_score": (round(float(self.quality_score), 4) if self.quality_score is not None else None),
            "quality_status": self.quality_status,
            "effective_routing_score": (round(float(self.effective_score), 4) if self.effective_score is not None else None),
            "reason": self.reason,
            "positive_signals": list(self.positive_signals),
            "negative_signals": list(self.negative_signals),
            "profile_source": self.profile_source,
        }


@dataclass
class OntologyRoutingResult:
    ranked_ontologies: List[RankedOntology] = field(default_factory=list)
    routing_confident: bool = False
    fallback_to_default: bool = True
    used_llm: bool = False
    reason: str = ""
    top_score: float = 0.0
    second_score: float = 0.0
    margin: float = 0.0
    routing_mode: str = "fallback_default"
    error: str = ""
    quality_gate_applied: bool = False
    excluded_summary: Dict[str, Any] = field(default_factory=dict)
    prompt_debug: Optional[Dict[str, Any]] = None  # populated only when capture_prompts=True
    term_context_profile: Optional[Dict[str, Any]] = None
    registry_candidate_count: int = 0
    eligible_after_quality_gate: int = 0
    missing_quality_raw_count: int = 0
    registry_programmatic_top: List[Dict[str, Any]] = field(default_factory=list)
    llm_reranked_top: List[Dict[str, Any]] = field(default_factory=list)
    routed_primary_ontologies: List[str] = field(default_factory=list)
    skipped_default_or_trusted_ontologies: List[Dict[str, Any]] = field(default_factory=list)
    selection_source: str = ""

    def as_dict(self) -> Dict[str, Any]:
        out = {
            "routing_confident": self.routing_confident,
            "fallback_to_default": self.fallback_to_default,
            "used_llm": self.used_llm,
            "reason": self.reason,
            "top_score": round(float(self.top_score), 4),
            "second_score": round(float(self.second_score), 4),
            "margin": round(float(self.margin), 4),
            "routing_mode": self.routing_mode,
            "error": self.error,
            "quality_gate_applied": self.quality_gate_applied,
            "excluded_summary": dict(self.excluded_summary),
            "ranked_ontologies": [r.as_dict() for r in self.ranked_ontologies],
            "term_context_profile": self.term_context_profile,
            "registry_candidate_count": self.registry_candidate_count,
            "eligible_after_quality_gate": self.eligible_after_quality_gate,
            "missing_quality_raw_count": self.missing_quality_raw_count,
            "registry_programmatic_top": list(self.registry_programmatic_top),
            "llm_reranked_top": list(self.llm_reranked_top),
            "routed_primary_ontologies": list(self.routed_primary_ontologies),
            "skipped_default_or_trusted_ontologies": list(self.skipped_default_or_trusted_ontologies),
            "selection_source": self.selection_source,
        }
        if self.prompt_debug is not None:
            out["prompt_debug"] = self.prompt_debug
        return out


@dataclass
class TermContextProfile:
    normalized_term: str
    normalized_definition: str
    table_context: str = ""
    column_context: str = ""
    rdf_role: str = ""
    detected_entity_type: str = "unknown"
    semantic_domain: str = "general"
    application_context: str = ""
    food_context_detected: bool = False
    foodon_boost_allowed: bool = False
    foodon_boost_reason: str = ""
    material_substance_detected: bool = False
    term_head_entity: str = ""
    positive_signals: List[str] = field(default_factory=list)
    negative_domain_signals: List[str] = field(default_factory=list)
    preferred_ontology_domains: List[str] = field(default_factory=list)
    disallowed_or_low_priority_domains: List[str] = field(default_factory=list)
    ambiguity_flags: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "normalized_term": self.normalized_term,
            "normalized_definition": self.normalized_definition,
            "table_context": self.table_context,
            "column_context": self.column_context,
            "rdf_role": self.rdf_role,
            "detected_entity_type": self.detected_entity_type,
            "entity_type": self.detected_entity_type,
            "semantic_domain": self.semantic_domain,
            "application_context": self.application_context,
            "food_context_detected": bool(self.food_context_detected),
            "foodon_boost_allowed": bool(self.foodon_boost_allowed),
            "foodon_boost_reason": self.foodon_boost_reason,
            "material_substance_detected": bool(self.material_substance_detected),
            "term_head_entity": self.term_head_entity,
            "positive_signals": list(self.positive_signals),
            "negative_domain_signals": list(self.negative_domain_signals),
            "preferred_ontology_domains": list(self.preferred_ontology_domains),
            "disallowed_or_low_priority_domains": list(self.disallowed_or_low_priority_domains),
            "ambiguity_flags": list(self.ambiguity_flags),
        }


@dataclass
class RegistryOntologyCandidate:
    acronym: str
    name: str = ""
    searchable_text: str = ""
    source_fields_used: List[str] = field(default_factory=list)
    quality_status: str = "allowed"
    quality_score: float = 0.5
    has_quality_raw: bool = False
    registry_metadata_completeness: float = 0.0
    hard_exclusion_reason: str = ""
    lexical_domain_score: float = 0.0
    category_score: float = 0.0
    root_label_score: float = 0.0
    curated_profile_score: float = 0.0
    negative_domain_penalty: float = 0.0
    broadness_penalty: float = 0.0
    semantic_score: float = 0.0
    final_registry_suitability_score: float = 0.0
    foodon_boost_applied: bool = False
    foodon_penalty_applied: bool = False
    foodon_reason: str = ""
    foodon_score_components: Dict[str, Any] = field(default_factory=dict)
    matched_signals: List[str] = field(default_factory=list)
    negative_signals: List[str] = field(default_factory=list)
    ranking_reason: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "acronym": self.acronym,
            "name": self.name,
            "final_registry_suitability_score": round(float(self.final_registry_suitability_score), 4),
            "semantic_score": round(float(self.semantic_score), 4),
            "foodon_boost_applied": bool(self.foodon_boost_applied),
            "foodon_penalty_applied": bool(self.foodon_penalty_applied),
            "foodon_reason": self.foodon_reason,
            "foodon_score_components": dict(self.foodon_score_components),
            "quality_status": self.quality_status,
            "quality_score": round(float(self.quality_score), 4),
            "has_quality_raw": bool(self.has_quality_raw),
            "registry_metadata_completeness": round(float(self.registry_metadata_completeness), 4),
            "hard_exclusion_reason": self.hard_exclusion_reason,
            "lexical_domain_score": round(float(self.lexical_domain_score), 4),
            "category_score": round(float(self.category_score), 4),
            "root_label_score": round(float(self.root_label_score), 4),
            "curated_profile_score": round(float(self.curated_profile_score), 4),
            "negative_domain_penalty": round(float(self.negative_domain_penalty), 4),
            "broadness_penalty": round(float(self.broadness_penalty), 4),
            "matched_signals": list(self.matched_signals),
            "negative_signals": list(self.negative_signals),
            "ranking_reason": self.ranking_reason,
        }


# ---------------------------------------------------------------------------
# Profile loading (curated + auto-derived)
# ---------------------------------------------------------------------------

def _tokens(text: str) -> set:
    found = re.findall(r"[a-z0-9]+", str(text or "").lower())
    return {tok for tok in found if len(tok) > 2 and tok not in _ROUTING_STOPWORDS}


_PACKAGING_SIGNALS = {
    "packaging", "package", "food packaging", "food contact", "material", "materials",
    "material type", "plastic", "polymer", "film", "foil", "sheet", "board",
    "cardboard", "paperboard", "paper", "container", "wrapper", "casing", "glass",
    "metal", "aluminium", "aluminum", "polycarbonate", "polystyrene", "polyamide",
    "cellophane", "biomaterial", "device material",
}
_MATERIAL_NAME_SIGNALS = {
    "polystyrene", "polycarbonate", "polyamide", "polyethylene", "polypropylene",
    "polyvinyl chloride", "pvc", "pet", "cellophane", "aluminium", "aluminum",
    "aluminium foil", "glass", "metal", "paper", "paperboard", "cardboard",
    "plastic", "polymer", "nylon", "foil", "film", "sheet",
}
_FOOD_PRODUCT_SIGNALS = {
    "milk", "cheese", "sausage", "olive oil", "meat", "fish", "food product",
    "food matrix", "edible", "food",
}
_FOOD_PACKAGING_COMPONENT_SIGNALS = {
    "artificial casing", "sausage casing", "food package", "food packaging",
    "food container", "food contact", "wrapper for food", "paperboard food container",
}
_MATERIAL_PRIORITY_ONTOLOGIES = {"CHEBI", "MATERIALSMINE", "MATRCOMPOUND", "NCIT", "MESH"}
_PACKAGING_LOW_PRIORITY = {
    "taxonomy", "organism", "units", "quantities", "clinical disease",
    "adverse event", "drug therapy", "anatomy", "brain", "gene", "protein",
    "surveillance", "social", "behavioral", "computing",
}
_UNIT_SIGNALS = {"unit", "units", "quantity", "temperature", "degree", "celsius", "kelvin", "meter", "metre", "gram", "liter", "litre"}
_ORGANISM_SIGNALS = {"organism", "taxon", "taxonomy", "species", "bacterial", "bacteria", "virus", "fungus", "listeria", "salmonella"}
_CLINICAL_SIGNALS = {"disease", "infection", "clinical", "syndrome", "symptom", "diagnosis", "adverse", "therapy", "salmonellosis"}
_DOMAIN_SYNONYMS = {
    "aluminium": {"aluminium", "aluminum"},
    "aluminum": {"aluminium", "aluminum"},
    "packaging": {"packaging", "package", "packaged", "packing"},
    "package": {"packaging", "package", "packaged", "packing"},
    "materials": {"material", "materials"},
    "material": {"material", "materials"},
}


def _normalize_search_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    text = text.replace("/", " ").replace("_", " ").replace("-", " ")
    text = text.lower().replace("aluminum", "aluminium")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    words = []
    for word in text.split():
        if len(word) > 4 and word.endswith("s") and not word.endswith("ss"):
            word = word[:-1]
        words.append(word)
    return " ".join(words)


def _token_variants(signal: str) -> set:
    normalized = _normalize_search_text(signal)
    toks = {tok for tok in normalized.split() if tok}
    expanded = set(toks)
    for tok in toks:
        expanded.update(_DOMAIN_SYNONYMS.get(tok, set()))
    return expanded


def _contains_signal(text: str, signal: str) -> bool:
    normalized = _normalize_search_text(signal)
    if not normalized:
        return False
    if " " in normalized:
        return normalized in text
    tokens = set(text.split())
    variants = _token_variants(signal)
    return bool(tokens & variants) or normalized in text


def _contains_exact_signal(text: str, signal: str) -> bool:
    normalized = _normalize_search_text(signal)
    if not normalized:
        return False
    if " " in normalized:
        return normalized in text
    return normalized in set(text.split())


def _dedupe_ordered(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def build_term_context_profile(
    term: str,
    definition: Optional[str],
    table_context: Optional[str] = None,
    column_context: Optional[str] = None,
    rdf_role: Optional[str] = None,
) -> TermContextProfile:
    normalized_term = _normalize_search_text(term)
    normalized_definition = _normalize_search_text(definition)
    context_text = _normalize_search_text(" ".join(filter(None, [term, definition, table_context, column_context, rdf_role])))
    term_text = _normalize_search_text(term)
    definition_text = _normalize_search_text(definition)
    positive: List[str] = []
    ambiguity: List[str] = []
    semantic_domain = "general"
    entity_type = "unknown"
    application_context = ""
    food_context_detected = False
    foodon_boost_allowed = False
    foodon_boost_reason = ""
    material_substance_detected = any(_contains_signal(term_text, sig) for sig in _MATERIAL_NAME_SIGNALS)
    term_head_entity = term_text.split()[-1] if term_text.split() else ""
    packaging_context_detected = any(_contains_signal(context_text, sig) for sig in ("packaging", "package", "container", "wrapper", "casing"))
    food_product_detected = any(
        _contains_signal(term_text, sig)
        for sig in _FOOD_PRODUCT_SIGNALS
        if sig != "food"
    )
    explicit_food_context = any(_contains_signal(context_text, sig) for sig in ("food", "food contact", "sausage", "edible", "food product", "food packaging", "food container"))
    term_component_detected = any(_contains_exact_signal(term_text, sig) for sig in ("artificial casing", "sausage casing", "container", "wrapper", "casing"))
    food_packaging_component_detected = (
        any(_contains_exact_signal(context_text, sig) for sig in _FOOD_PACKAGING_COMPONENT_SIGNALS)
        or bool(term_component_detected and explicit_food_context)
    )

    for signal in sorted(_PACKAGING_SIGNALS):
        if _contains_signal(context_text, signal):
            positive.append(signal)
    if material_substance_detected:
        semantic_domain = "material_substance"
        entity_type = "chemical_or_polymer" if any(_contains_signal(term_text, sig) for sig in ("polymer", "plastic", "polycarbonate", "polystyrene", "polyamide", "polyethylene", "polypropylene", "pvc", "pet", "nylon", "cellophane")) else "material"
        application_context = "packaging" if packaging_context_detected else ""
        food_context_detected = bool(explicit_food_context and not food_packaging_component_detected and food_product_detected)
        foodon_boost_allowed = bool(food_product_detected or food_packaging_component_detected)
        foodon_boost_reason = (
            "food product/material term" if food_product_detected
            else "explicit food packaging/container/component context" if food_packaging_component_detected
            else "material substance; packaging treated as application context"
        )
    elif food_product_detected:
        semantic_domain = "food_product_or_food_matrix"
        entity_type = "food_product_or_food_matrix"
        food_context_detected = True
        foodon_boost_allowed = True
        foodon_boost_reason = "term denotes food product or food matrix"
    elif food_packaging_component_detected:
        semantic_domain = "packaging_component_or_container"
        entity_type = "container_or_packaging_component"
        application_context = "food_packaging"
        food_context_detected = True
        foodon_boost_allowed = True
        foodon_boost_reason = "explicit food packaging/container/component context"
    elif "packaging" in context_text or "material" in context_text or positive:
        semantic_domain = "packaging_material"
        entity_type = "packaging_material"
        application_context = "packaging" if packaging_context_detected else ""
        food_context_detected = bool(explicit_food_context)
        foodon_boost_allowed = False
        foodon_boost_reason = "packaging/material context without explicit food/container concept"
        if any(sig in positive for sig in ("container", "wrapper", "casing", "film", "foil", "sheet", "board")):
            entity_type = "container_or_packaging_component"
        if any(sig in positive for sig in ("polymer", "plastic", "polycarbonate", "polystyrene", "polyamide", "cellophane")):
            entity_type = "chemical_or_polymer"
        if "food" in context_text:
            entity_type = "food_contact_material"

    if any(_contains_signal(context_text, sig) for sig in _UNIT_SIGNALS):
        semantic_domain = "unit_quantity"
        entity_type = "unit"
        positive.extend([sig for sig in _UNIT_SIGNALS if _contains_signal(context_text, sig)])
    elif any(_contains_signal(context_text, sig) for sig in _ORGANISM_SIGNALS):
        semantic_domain = "organism_taxon"
        entity_type = "organism"
        positive.extend([sig for sig in _ORGANISM_SIGNALS if _contains_signal(context_text, sig)])
    elif any(_contains_signal(context_text, sig) for sig in _CLINICAL_SIGNALS):
        semantic_domain = "clinical_disease"
        entity_type = "clinical_disease"
        positive.extend([sig for sig in _CLINICAL_SIGNALS if _contains_signal(context_text, sig)])

    placeholder_values = {"other", "unknown", "none", "not applicable", "no information", "packed", "not packed"}
    if str(term or "").strip().lower() in placeholder_values:
        semantic_domain = "generic_status_or_placeholder"
        entity_type = "generic_status_or_placeholder"
        ambiguity.append("generic_placeholder_value")

    if semantic_domain == "packaging_material":
        preferred = [
            "material", "materials", "chemical", "polymer", "plastic", "biomaterial",
            "device material", "food packaging", "food contact", "container", "packaging",
            "paper", "paperboard", "cardboard", "film", "foil", "glass", "metal",
        ]
        low_priority = sorted(_PACKAGING_LOW_PRIORITY)
        negative = low_priority
    elif semantic_domain in {"material_substance", "packaging_material_substance"}:
        preferred = [
            "chemical", "chemical entity", "compound", "polymer", "plastic", "material",
            "materials", "biomaterial", "device material", "material property", "material class",
            "paper", "paperboard", "cardboard", "film", "foil", "glass", "metal",
        ]
        low_priority = ["food packaging", "food product", *sorted(_PACKAGING_LOW_PRIORITY)]
        negative = low_priority
    elif semantic_domain == "packaging_component_or_container":
        preferred = [
            "food packaging", "food contact", "container", "packaging component",
            "material", "materials", "device material", "paperboard", "wrapper", "casing",
        ]
        low_priority = ["taxonomy", "units", "clinical disease", "adverse event", "gene", "protein"]
        negative = low_priority
    elif semantic_domain == "food_product_or_food_matrix":
        preferred = ["food", "food product", "food matrix", "edible", "ingredient"]
        low_priority = ["unit", "quantity", "taxonomy", "clinical disease", "material property"]
        negative = low_priority
    elif semantic_domain == "unit_quantity":
        preferred = ["unit", "quantity", "measurement", "temperature"]
        low_priority = ["food", "material", "taxonomy", "clinical disease"]
        negative = low_priority
    elif semantic_domain == "organism_taxon":
        preferred = ["taxonomy", "organism", "species"]
        low_priority = ["unit", "quantity", "material", "clinical adverse event"]
        negative = low_priority
    elif semantic_domain == "clinical_disease":
        preferred = ["clinical", "disease", "infection", "medical terminology"]
        low_priority = ["unit", "quantity", "taxonomy-only", "material"]
        negative = low_priority
    else:
        preferred = []
        low_priority = []
        negative = []

    return TermContextProfile(
        normalized_term=normalized_term,
        normalized_definition=normalized_definition,
        table_context=str(table_context or ""),
        column_context=str(column_context or ""),
        rdf_role=str(rdf_role or ""),
        detected_entity_type=entity_type,
        semantic_domain=semantic_domain,
        application_context=application_context,
        food_context_detected=food_context_detected,
        foodon_boost_allowed=foodon_boost_allowed,
        foodon_boost_reason=foodon_boost_reason,
        material_substance_detected=material_substance_detected,
        term_head_entity=term_head_entity,
        positive_signals=_dedupe_ordered(positive),
        negative_domain_signals=_dedupe_ordered(negative),
        preferred_ontology_domains=_dedupe_ordered(preferred),
        disallowed_or_low_priority_domains=_dedupe_ordered(low_priority),
        ambiguity_flags=ambiguity,
    )


def load_capability_profiles(path: Optional[Path] = None) -> Dict[str, OntologyCapabilityProfile]:
    """Load curated capability profiles keyed by upper-case acronym."""
    profile_path = Path(path) if path is not None else CAPABILITY_PROFILES_PATH
    if not profile_path.exists():
        return {}
    try:
        raw = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    profiles: Dict[str, OntologyCapabilityProfile] = {}
    entries = raw.get("profiles", raw) if isinstance(raw, dict) else {}
    if isinstance(entries, dict):
        for acronym, data in entries.items():
            if not isinstance(data, dict):
                continue
            data = {**data, "acronym": data.get("acronym") or acronym}
            profile = OntologyCapabilityProfile.from_dict(data)
            if profile.acronym:
                profiles[profile.acronym] = profile
    return profiles


def build_auto_profile_from_registry(acronym: str, registry_entry: Optional[Dict[str, Any]]) -> OntologyCapabilityProfile:
    """Derive a cautious, lower-confidence routing profile from raw registry fields.

    Uses only semantically-relevant fields (name/description/categories/root labels/
    purpose/keywords). Administrative metadata is ignored. Auto profiles must never
    overrule curated profiles (lower profile_confidence).
    """
    entry = registry_entry if isinstance(registry_entry, dict) else {}
    acronym_norm = str(acronym or entry.get("acronym") or "").strip().upper()

    def _list(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return []

    domains = _list(entry.get("categories_domains")) or _list(entry.get("categories"))
    keywords = _list(entry.get("keywords"))
    roots = _list(entry.get("root_labels_sample"))
    description = str(entry.get("description") or entry.get("purpose_summary_auto") or entry.get("abstract") or "").strip()
    routing_keywords = sorted(_tokens(" ".join([description] + domains + keywords + roots)))[:24]

    has_meta = bool(description or domains or keywords or roots)
    return OntologyCapabilityProfile(
        acronym=acronym_norm,
        display_name=str(entry.get("name") or acronym_norm).strip(),
        routing_description=description[:400] or f"Ontology {acronym_norm} (auto-derived; metadata sparse).",
        contains=domains[:8],
        best_for=[],  # cautious: do not invent precise best_for from vague metadata
        not_best_for=[],
        domain_tags=[d.lower().replace(" ", "_") for d in domains][:8],
        routing_keywords=routing_keywords,
        negative_keywords=[],
        example_classes=roots[:8],
        generality=0.6 if acronym_norm in BROAD_ONTOLOGIES else 0.4,
        profile_confidence=0.5 if has_meta else 0.35,
        source="auto_from_registry",
        last_updated=str(entry.get("last_refreshed") or "") or None,
    )


def get_capability_profile(
    acronym: str,
    *,
    curated: Optional[Dict[str, OntologyCapabilityProfile]] = None,
    registry_entry: Optional[Dict[str, Any]] = None,
) -> OntologyCapabilityProfile:
    """Return the curated profile if present, else an auto-derived one."""
    acronym_norm = str(acronym or "").strip().upper()
    curated = curated if curated is not None else load_capability_profiles()
    if acronym_norm in curated:
        return curated[acronym_norm]
    return build_auto_profile_from_registry(acronym_norm, registry_entry)


def build_profiles_for_acronyms(
    acronyms: List[str],
    *,
    curated: Optional[Dict[str, OntologyCapabilityProfile]] = None,
    registry_lookup: Optional[Any] = None,
) -> List[OntologyCapabilityProfile]:
    """Build a profile per acronym (curated first, auto-derived fallback).

    ``registry_lookup`` is an optional callable acronym -> registry entry dict
    used to auto-derive profiles for non-curated ontologies. It is only consulted
    for acronyms without a curated profile, and never triggers BioPortal calls.
    """
    curated = curated if curated is not None else load_capability_profiles()
    out: List[OntologyCapabilityProfile] = []
    seen = set()
    for raw in acronyms or []:
        acronym = str(raw or "").strip().upper()
        if not acronym or acronym in seen:
            continue
        seen.add(acronym)
        registry_entry = None
        if acronym not in curated and callable(registry_lookup):
            try:
                registry_entry = registry_lookup(acronym)
            except Exception:
                registry_entry = None
        out.append(get_capability_profile(acronym, curated=curated, registry_entry=registry_entry))
    return out


# ---------------------------------------------------------------------------
# Ontology QUALITY profile + gate (separate from suitability)
#   quality  = "is this ontology trustworthy/usable in general?"
#   suitability = "does it fit this specific term/context?" (the LLM's job)
# Quality is computed deterministically BEFORE the LLM and used only to (1) hard
# exclude, (2) dampen suitability into an effective routing score, (3) explain.
# ---------------------------------------------------------------------------

_TEST_ACRONYM_PATTERN = re.compile(r"(?:^|[^a-z0-9])(test|sandbox|demo|dev|tst|sbx)(?:[^a-z0-9]|$)", re.IGNORECASE)


@dataclass
class OntologyQualityProfile:
    acronym: str
    quality_status: str = "allowed"  # preferred | allowed | low_quality | excluded
    quality_score: float = 0.0
    hard_exclusion_reason: Optional[str] = None
    positive_flags: List[str] = field(default_factory=list)
    negative_flags: List[str] = field(default_factory=list)
    metadata_completeness_score: float = 0.0
    structure_score: float = 0.0
    acceptance_score: float = 0.0
    review_score: Optional[float] = None
    curation_score: float = 0.0
    computed_at: Optional[str] = None

    @property
    def excluded(self) -> bool:
        return self.quality_status == "excluded"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "acronym": self.acronym,
            "quality_status": self.quality_status,
            "quality_score": round(float(self.quality_score), 4),
            "hard_exclusion_reason": self.hard_exclusion_reason,
            "positive_flags": list(self.positive_flags),
            "negative_flags": list(self.negative_flags),
            "metadata_completeness_score": round(float(self.metadata_completeness_score), 4),
            "structure_score": round(float(self.structure_score), 4),
            "acceptance_score": round(float(self.acceptance_score), 4),
            "review_score": (round(float(self.review_score), 4) if self.review_score is not None else None),
            "curation_score": round(float(self.curation_score), 4),
            "computed_at": self.computed_at,
        }


def _qget(entry: Optional[Dict[str, Any]], *keys: str) -> Any:
    """Read a quality field from the registry entry, checking the nested
    ``quality_raw`` evidence section as a fallback (kept separate from prompt data)."""
    if not isinstance(entry, dict):
        return None
    for key in keys:
        value = entry.get(key)
        if value not in (None, "", [], {}):
            return value
    quality_raw = entry.get("quality_raw")
    if isinstance(quality_raw, dict):
        for key in keys:
            value = quality_raw.get(key)
            if value not in (None, "", [], {}):
                return value
    return None


def _truthy_flag(entry: Optional[Dict[str, Any]], *keys: str) -> bool:
    value = _qget(entry, *keys)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _looks_like_test_ontology(acronym: str, name: str) -> bool:
    text = f"{acronym} {name}"
    if _TEST_ACRONYM_PATTERN.search(text):
        return True
    low = text.lower()
    return any(token in low for token in ("api-tst", "apitst", "api tst"))


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _review_score(entry: Optional[Dict[str, Any]]) -> Optional[float]:
    reviews = _qget(entry, "reviews", "review")
    if reviews is None:
        return None
    items = reviews if isinstance(reviews, list) else [reviews]
    rating_keys = ("usabilityRating", "coverageRating", "qualityRating",
                   "formalityRating", "correctnessRating", "documentationRating")
    ratings: List[float] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in rating_keys:
            val = item.get(key)
            try:
                if val is not None:
                    ratings.append(float(val))
            except Exception:
                continue
    if not ratings:
        return None
    # BioPortal ratings are 1-10; normalize to 0-1 (clamp defensively).
    return _clamp(sum(ratings) / len(ratings) / 10.0)


def compute_ontology_quality_profile(
    acronym: str,
    registry_entry: Optional[Dict[str, Any]],
    *,
    curated_profile_exists: bool = False,
    is_core: bool = False,
    is_project_relevant: bool = False,
    blocklist: Optional[set] = None,
    allowlist: Optional[set] = None,
    computed_at: Optional[str] = None,
) -> OntologyQualityProfile:
    """Compute a lean quality profile + status for one ontology.

    Hard exclusion fires only on POSITIVE evidence of a problem (blocklist,
    test/sandbox acronym, deprecated/invalid/invalidated/private, failed lookup,
    or genuinely no usable metadata). Absence of a field is never treated as
    evidence of badness. Configured-core / allowlisted ontologies are protected
    from sparse-metadata exclusion (but never from deprecated/invalid/private).
    """
    acronym = str(acronym or "").strip().upper()
    entry = registry_entry if isinstance(registry_entry, dict) else {}
    blocklist = {str(a).strip().upper() for a in (blocklist or set())}
    allowlist = {str(a).strip().upper() for a in (allowlist or set())}
    protected = is_core or is_project_relevant or acronym in allowlist

    name = str(_qget(entry, "name", "display_name") or acronym)
    positive: List[str] = []
    negative: List[str] = []

    def _excluded(reason: str) -> OntologyQualityProfile:
        return OntologyQualityProfile(
            acronym=acronym, quality_status="excluded", quality_score=0.0,
            hard_exclusion_reason=reason, positive_flags=positive, negative_flags=negative,
            computed_at=computed_at,
        )

    # ---- hard exclusions (NOT overridable by allowlist, except sparse-metadata) ----
    if acronym in blocklist:
        return _excluded("blocklisted")
    if _looks_like_test_ontology(acronym, name):
        return _excluded("test_or_sandbox_acronym")
    if _truthy_flag(entry, "deprecated"):
        return _excluded("deprecated")
    valid = _qget(entry, "valid")
    if valid is not None and not _truthy_flag(entry, "valid"):
        return _excluded("submission_invalid")
    if _qget(entry, "wasInvalidatedBy", "was_invalidated_by"):
        return _excluded("was_invalidated_by")
    restriction = str(_qget(entry, "viewingRestriction", "viewing_restriction") or "").strip().lower()
    if restriction and "private" in restriction:
        return _excluded("private_viewing_restriction")
    status = str(_qget(entry, "status", "registry_status") or "").strip().lower()
    if status == "lookup_failed":
        return _excluded("metadata_lookup_failed")

    # ---- evidence gathering ----
    description = _qget(entry, "description", "abstract", "purpose_summary_auto")
    categories = _qget(entry, "categories_domains", "categories", "domain", "groups")
    keywords = _qget(entry, "keywords", "coverage")
    root_labels = _qget(entry, "root_labels_sample", "example_classes")
    metrics = _qget(entry, "metrics") if isinstance(_qget(entry, "metrics"), dict) else {}
    classes = _qget(entry, "classes") or (metrics.get("classes") if isinstance(metrics, dict) else None)
    max_depth = _qget(entry, "max_depth", "maxDepth") or (metrics.get("maxDepth") if isinstance(metrics, dict) else None)
    classes_no_def = metrics.get("classesWithNoDefinition") if isinstance(metrics, dict) else _qget(entry, "classesWithNoDefinition")
    hierarchy_prop = _qget(entry, "hierarchy_property", "hierarchyProperty")
    definition_prop = _qget(entry, "definitionProperty", "definition_property")
    synonym_prop = _qget(entry, "synonymProperty", "synonym_property")
    known_usage = _qget(entry, "knownUsage", "known_usage", "usedBy", "used_by")
    projects = _qget(entry, "projects")
    review_score = _review_score(entry)

    has_useful_metadata = bool(description or categories or root_labels or metrics or classes)
    if not has_useful_metadata:
        negative.append("sparse_metadata")
        if not protected:
            return _excluded("no_useful_metadata")

    # ---- metadata completeness ----
    metadata_completeness = 0.0
    if description:
        metadata_completeness += 0.35
        positive.append("informative_description")
    if categories:
        metadata_completeness += 0.25
        positive.append("categories_present")
    if keywords:
        metadata_completeness += 0.15
    if root_labels:
        metadata_completeness += 0.25
        positive.append("example_classes_present")
    metadata_completeness = _clamp(metadata_completeness)

    # ---- structure ----
    def _num(value: Any) -> Optional[float]:
        try:
            return float(value)
        except Exception:
            return None

    max_depth_n = _num(max_depth)
    classes_n = _num(classes)
    # Hierarchy evidence: an ontology can be hierarchical even when the submission
    # does not explicitly declare hierarchyProperty (BioPortal defaults to
    # subClassOf, exposes roots, depth>1, and parent/child endpoints).
    has_other_hierarchy_evidence = bool(
        root_labels
        or (max_depth_n is not None and max_depth_n > 1)
        or (classes_n is not None and classes_n > 0)
    )

    structure = 0.0
    if hierarchy_prop:
        structure += 0.25
        positive.append("hierarchy_property")
    elif has_other_hierarchy_evidence:
        # Weak signal only — declaration missing but hierarchy clearly exists.
        structure += 0.20
        negative.append("hierarchy_property_not_declared")
    else:
        # Strong penalty only when there is no hierarchy evidence at all.
        negative.append("no_hierarchy_evidence")
    if definition_prop:
        structure += 0.20
    else:
        negative.append("no_definition_property")
    if synonym_prop:
        structure += 0.15
    else:
        negative.append("no_synonym_property")
    if metrics:
        structure += 0.15
    if max_depth_n is not None and max_depth_n > 1:
        structure += 0.15
    if classes_n is not None and classes_n > 0:
        structure += 0.10
    elif classes_n is not None:
        negative.append("very_small_ontology")
    try:
        if classes_n and classes_no_def is not None and classes_n > 0:
            ratio = float(classes_no_def) / classes_n
            if ratio > 0.25:
                structure -= min(0.25, 0.25 * ratio)
                negative.append("high_classes_without_definition_ratio")
    except Exception:
        pass
    structure = _clamp(structure)

    # ---- acceptance ----
    acceptance = 0.0
    if known_usage:
        acceptance += 0.30
        positive.append("known_usage")
    if projects:
        acceptance += 0.25
        positive.append("projects_present")
    if review_score is not None:
        acceptance += 0.20
        positive.append("reviews_present")
    if categories:
        acceptance += 0.10
    if review_score is not None:
        acceptance = 0.7 * acceptance + 0.3 * review_score
    acceptance = _clamp(acceptance)

    # ---- curation ----
    curation = 0.0
    if curated_profile_exists:
        curation += 0.50
        positive.append("curated_profile")
    if is_core:
        curation += 0.25
        positive.append("configured_core")
    if is_project_relevant:
        curation += 0.25
        positive.append("project_relevant")
    curation = _clamp(curation)

    # ---- deprioritization flags (no hard exclusion) ----
    if _qget(entry, "viewOf", "view_of"):
        negative.append("is_view")
    if _truthy_flag(entry, "flat"):
        negative.append("is_flat")
    if _truthy_flag(entry, "summaryOnly", "summary_only"):
        negative.append("summary_only")
    if _truthy_flag(entry, "doNotUpdate", "do_not_update"):
        negative.append("do_not_update")
    if _qget(entry, "missingImports", "missing_imports"):
        negative.append("missing_imports")
    if isinstance(categories, list) and any("aggregat" in str(c).lower() or "application" in str(c).lower() for c in categories):
        negative.append("broad_aggregator")

    validity_score = 1.0 if has_useful_metadata else 0.4
    quality_score = _clamp(
        0.35 * validity_score
        + 0.20 * metadata_completeness
        + 0.20 * structure
        + 0.15 * acceptance
        + 0.10 * curation
    )

    # deprioritization dampening for views/flat/etc.
    if negative:
        penalty = min(0.20, 0.05 * len([f for f in negative if f in {
            "is_view", "is_flat", "summary_only", "do_not_update", "missing_imports", "broad_aggregator"}]))
        quality_score = _clamp(quality_score - penalty)

    if quality_score >= 0.80:
        status = "preferred"
    elif quality_score >= 0.60:
        status = "allowed"
    elif quality_score >= 0.35:
        status = "low_quality"
    else:
        # below 0.35: exclude only if metadata is too sparse AND not protected
        if protected or has_useful_metadata:
            status = "low_quality"
        else:
            return _excluded("low_quality_sparse_metadata")

    # protected ontologies never drop below low_quality (unless hard-excluded above)
    if protected and status == "excluded":
        status = "low_quality"

    return OntologyQualityProfile(
        acronym=acronym, quality_status=status, quality_score=quality_score,
        hard_exclusion_reason=None, positive_flags=positive, negative_flags=negative,
        metadata_completeness_score=metadata_completeness, structure_score=structure,
        acceptance_score=acceptance, review_score=review_score, curation_score=curation,
        computed_at=computed_at,
    )


def build_quality_profiles(
    acronyms: List[str],
    *,
    curated: Optional[Dict[str, OntologyCapabilityProfile]] = None,
    registry_lookup: Optional[Any] = None,
    configured_core: Optional[List[str]] = None,
    project_relevant: Optional[List[str]] = None,
    blocklist: Optional[List[str]] = None,
    allowlist: Optional[List[str]] = None,
) -> Dict[str, OntologyQualityProfile]:
    curated = curated if curated is not None else load_capability_profiles()
    core_set = {str(a).strip().upper() for a in (configured_core or [])}
    project_set = {str(a).strip().upper() for a in (project_relevant or [])}
    block_set = {str(a).strip().upper() for a in (blocklist or [])}
    allow_set = {str(a).strip().upper() for a in (allowlist or [])}
    out: Dict[str, OntologyQualityProfile] = {}
    for raw in acronyms or []:
        acronym = str(raw or "").strip().upper()
        if not acronym or acronym in out:
            continue
        entry = None
        if callable(registry_lookup):
            try:
                entry = registry_lookup(acronym)
            except Exception:
                entry = None
        out[acronym] = compute_ontology_quality_profile(
            acronym, entry,
            curated_profile_exists=acronym in curated,
            is_core=acronym in core_set,
            is_project_relevant=acronym in project_set,
            blocklist=block_set, allowlist=allow_set,
        )
    return out


# ---------------------------------------------------------------------------
# Registry-routed all-ontology scoring
# ---------------------------------------------------------------------------

_REGISTRY_SEARCH_FIELDS = (
    "acronym", "name", "description", "abstract", "purpose_summary_auto",
    "categories", "categories_domains", "root_labels_sample", "keywords",
    "curated_note",
)
_ADMIN_WEAK_FIELDS = ("homepage", "documentation")
_TECHNICAL_TEST_RE = re.compile(
    r"(^test[_-]?[a-z0-9]*$|api[_-]?tst|rocknrolltest|demo ontology|sandbox ontology|dev[- ]only|dsip2324[_-]?demo)",
    re.IGNORECASE,
)


def _as_text_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _field_text(entry: Dict[str, Any], field: str) -> str:
    value = entry.get(field)
    if isinstance(value, list):
        return " ".join(str(item) for item in value if str(item).strip())
    return str(value or "")


def build_registry_search_text(entry: Dict[str, Any]) -> tuple:
    parts: List[str] = []
    fields_used: List[str] = []
    for field in _REGISTRY_SEARCH_FIELDS + _ADMIN_WEAK_FIELDS:
        text = _field_text(entry, field)
        if text.strip():
            fields_used.append(field)
            parts.append(text)
    acronym = str(entry.get("acronym") or "").strip()
    if acronym:
        parts.append(acronym)
    return _normalize_search_text(" ".join(parts)), fields_used


def _registry_hard_exclusion_reason(acronym: str, entry: Dict[str, Any], blocklist: set) -> str:
    acronym = str(acronym or "").strip().upper()
    name = str(entry.get("name") or "").strip()
    text = f"{acronym} {name} {entry.get('description') or ''}".strip()
    if acronym in blocklist:
        return "blocklisted"
    if _TECHNICAL_TEST_RE.search(text):
        return "technical_test_demo_sandbox"
    if _truthy_flag(entry, "deprecated"):
        return "deprecated"
    valid = _qget(entry, "valid")
    if valid is not None and not _truthy_flag(entry, "valid"):
        return "explicitly_invalid"
    restriction = str(_qget(entry, "viewingRestriction", "viewing_restriction") or "").lower()
    if "private" in restriction or "restricted" in restriction:
        return "private_or_restricted"
    status = str(_qget(entry, "metadata_status", "status", "registry_status") or "").strip().lower()
    if status in {"lookup_failed", "failed", "error", "unusable", "invalid", "deprecated"}:
        return "unusable_status"
    return ""


def _metadata_completeness(entry: Dict[str, Any]) -> float:
    score = 0.0
    if _field_text(entry, "description") or _field_text(entry, "abstract") or _field_text(entry, "purpose_summary_auto"):
        score += 0.35
    if _as_text_list(entry.get("categories")) or _as_text_list(entry.get("categories_domains")):
        score += 0.20
    if _as_text_list(entry.get("root_labels_sample")):
        score += 0.20
    if _as_text_list(entry.get("keywords")):
        score += 0.15
    if _field_text(entry, "curated_note"):
        score += 0.10
    return _clamp(score)


def _quality_for_registry_candidate(
    acronym: str,
    entry: Dict[str, Any],
    curated: Dict[str, OntologyCapabilityProfile],
    configured_core: set,
    blocklist: set,
    allowlist: set,
) -> tuple:
    quality_raw = entry.get("quality_raw")
    has_quality_raw = isinstance(quality_raw, dict) and bool(quality_raw)
    quality = compute_ontology_quality_profile(
        acronym,
        entry,
        curated_profile_exists=acronym in curated,
        is_core=acronym in configured_core,
        is_project_relevant=False,
        blocklist=blocklist,
        allowlist=allowlist,
    )
    score = float(quality.quality_score or 0.45)
    if not has_quality_raw:
        score = max(0.0, score - 0.05)
    return quality.quality_status, _clamp(score), has_quality_raw


def _signal_score(signals: List[str], text: str) -> tuple:
    matched = [signal for signal in signals if _contains_signal(text, signal)]
    denom = max(3, min(10, len(signals) or 1))
    return min(1.0, len(matched) / denom), matched


def _negative_domain_penalty(profile: TermContextProfile, acronym: str, search_text: str) -> tuple:
    acr = acronym.upper()
    semantic = profile.semantic_domain
    negative: List[str] = []
    penalty = 0.0
    if semantic in {"material_substance", "packaging_material_substance"}:
        if acr == "FOODON" and not profile.foodon_boost_allowed:
            negative.append("FOODON skipped for material substance; packaging is application context only")
            penalty += 0.50
        for signal in ("food product", "food matrix", "edible"):
            if acr == "FOODON" and _contains_signal(search_text, signal) and not profile.foodon_boost_allowed:
                negative.append("food-domain evidence not specific to material substance")
                penalty += 0.08
        if acr in {"QUDT", "QUDT2"}:
            negative.append("unit ontology not relevant for material substance")
            penalty += 0.65
        if acr == "NCBITAXON":
            negative.append("taxonomy ontology not relevant for material substance")
            penalty += 0.65
        if acr in {"MEDDRA", "RCD"}:
            negative.append("clinical/adverse-event terminology not relevant for material substance")
            penalty += 0.45
        if acr == "GENEPIO":
            negative.append("genomics/surveillance ontology not relevant for material substance")
            penalty += 0.40
    elif semantic == "packaging_material":
        rules = {
            "NCBITAXON": ("taxonomy ontology not relevant for packaging material", 0.65),
            "QUDT": ("unit ontology not relevant for packaging material", 0.65),
            "QUDT2": ("unit ontology not relevant for packaging material", 0.65),
            "MEDDRA": ("clinical adverse-event terminology not relevant for packaging material", 0.45),
            "RCD": ("clinical terminology not relevant for packaging material", 0.45),
            "SNOMEDCT": ("clinical terminology not primary for packaging material", 0.35),
            "GENEPIO": ("genomics/surveillance ontology not relevant for packaging material", 0.40),
        }
        if acr in rules:
            reason, amount = rules[acr]
            negative.append(reason)
            penalty += amount
        for signal in ("taxonomy", "organism", "brain", "anatomy", "disease", "adverse", "therapy", "gene", "protein", "surveillance", "social", "behavioral"):
            if _contains_signal(search_text, signal):
                negative.append(f"{signal} domain not relevant for packaging material")
                penalty += 0.08
    elif semantic == "unit_quantity":
        if acr in {"FOODON", "CHEBI"}:
            negative.append("food/chemical ontology not primary for unit term")
            penalty += 0.25
    elif semantic == "organism_taxon":
        if acr in {"QUDT", "QUDT2"}:
            negative.append("unit ontology not relevant for organism/taxon term")
            penalty += 0.55
    elif semantic == "clinical_disease":
        if acr in {"QUDT", "QUDT2"}:
            negative.append("unit ontology not relevant for clinical disease")
            penalty += 0.55
        if acr == "FOODON":
            negative.append("food ontology not primary for clinical disease")
            penalty += 0.25
    return min(0.85, penalty), _dedupe_ordered(negative)


def _foodon_exact_coverage_evidence(profile: TermContextProfile, search_text: str) -> bool:
    term_tokens = [tok for tok in profile.normalized_term.split() if len(tok) > 2 and tok not in _ROUTING_STOPWORDS]
    if not term_tokens:
        return False
    if not all(tok in search_text.split() for tok in term_tokens):
        return False
    return any(
        _contains_signal(search_text, signal)
        for signal in ("food packaging", "food container", "food contact", "casing", "wrapper", "packaging material")
    )


def _apply_foodon_score_rule(
    profile: TermContextProfile,
    search_text: str,
    lexical_score: float,
    category_score: float,
    root_score: float,
    semantic_score: float,
    neg_penalty: float,
    negative: List[str],
) -> tuple:
    exact_evidence = _foodon_exact_coverage_evidence(profile, search_text)
    boost_applied = False
    penalty_applied = False
    reason = profile.foodon_boost_reason or "no FOODON-specific context"
    components = {
        "lexical_domain_score": round(float(lexical_score), 4),
        "category_score": round(float(category_score), 4),
        "root_label_score": round(float(root_score), 4),
        "semantic_domain": profile.semantic_domain,
        "application_context": profile.application_context,
        "foodon_boost_allowed": bool(profile.foodon_boost_allowed),
        "direct_exact_coverage_evidence": bool(exact_evidence),
    }
    if profile.semantic_domain == "food_product_or_food_matrix":
        semantic_score = min(1.0, semantic_score + 0.30)
        boost_applied = True
        reason = "FOODON boost: term denotes food product or food matrix"
    elif profile.semantic_domain == "packaging_component_or_container" and profile.foodon_boost_allowed:
        semantic_score = min(1.0, semantic_score + 0.20)
        boost_applied = True
        reason = "FOODON boost: explicit food packaging/container/component context"
    elif exact_evidence and profile.foodon_boost_allowed:
        semantic_score = min(1.0, semantic_score + 0.15)
        boost_applied = True
        reason = "FOODON boost: direct exact coverage evidence"
    elif profile.semantic_domain in {"material_substance", "packaging_material_substance"}:
        neg_penalty = min(0.85, neg_penalty + 0.25)
        penalty_applied = True
        reason = "FOODON penalty: material substance; packaging is application context, not food-packaging semantic domain"
        negative = _dedupe_ordered([*negative, reason])
    elif profile.semantic_domain == "packaging_material" and not profile.foodon_boost_allowed:
        neg_penalty = min(0.85, neg_penalty + 0.15)
        penalty_applied = True
        reason = "FOODON penalty: generic packaging/material context lacks explicit food/container signal"
        negative = _dedupe_ordered([*negative, reason])
    components["semantic_score_after_foodon_rule"] = round(float(semantic_score), 4)
    components["negative_domain_penalty_after_foodon_rule"] = round(float(neg_penalty), 4)
    return semantic_score, neg_penalty, negative, boost_applied, penalty_applied, reason, components


def _broadness_penalty(acronym: str, profile: TermContextProfile, search_text: str) -> float:
    acr = acronym.upper()
    if profile.semantic_domain == "packaging_material" and acr in {"MESH", "NCIT", "SNOMEDCT"}:
        return 0.10
    if "metadata" in search_text or "administrative" in search_text:
        return 0.30
    if acr in BROAD_ONTOLOGIES:
        return 0.05
    return 0.0


def _curated_profile_score(acronym: str, curated: Dict[str, OntologyCapabilityProfile], profile: TermContextProfile) -> float:
    cap = curated.get(acronym)
    if cap is None:
        return 0.0
    text = _normalize_search_text(" ".join(cap.best_for + cap.contains + cap.domain_tags + cap.routing_keywords + [cap.routing_description]))
    score, _matched = _signal_score(profile.positive_signals + profile.preferred_ontology_domains, text)
    return min(0.20, score * 0.20)


def score_registry_ontology_candidate(
    acronym: str,
    entry: Dict[str, Any],
    profile: TermContextProfile,
    *,
    curated: Optional[Dict[str, OntologyCapabilityProfile]] = None,
    configured_core: Optional[set] = None,
    blocklist: Optional[set] = None,
    allowlist: Optional[set] = None,
) -> RegistryOntologyCandidate:
    curated = curated or {}
    configured_core = configured_core or set()
    blocklist = blocklist or set()
    allowlist = allowlist or set()
    acronym = str(acronym or entry.get("acronym") or "").strip().upper()
    entry = entry if isinstance(entry, dict) else {}
    search_text, fields_used = build_registry_search_text({**entry, "acronym": acronym})
    name = str(entry.get("name") or acronym).strip()
    hard_reason = _registry_hard_exclusion_reason(acronym, entry, blocklist)
    quality_status, quality_score, has_quality_raw = _quality_for_registry_candidate(
        acronym, entry, curated, configured_core, blocklist, allowlist
    )
    if hard_reason:
        quality_status = "excluded"
        quality_score = 0.0

    lexical_score, matched = _signal_score(profile.positive_signals + profile.preferred_ontology_domains, search_text)
    category_text = _normalize_search_text(" ".join(_as_text_list(entry.get("categories")) + _as_text_list(entry.get("categories_domains"))))
    category_score, category_matched = _signal_score(profile.preferred_ontology_domains + profile.positive_signals, category_text)
    root_text = _normalize_search_text(" ".join(_as_text_list(entry.get("root_labels_sample"))))
    root_score, root_matched = _signal_score(profile.positive_signals, root_text)
    curated_score = _curated_profile_score(acronym, curated, profile)
    neg_penalty, negative = _negative_domain_penalty(profile, acronym, search_text)
    broad_penalty = _broadness_penalty(acronym, profile, search_text)
    completeness = _metadata_completeness(entry)

    semantic_score = _clamp(
        0.50 * lexical_score
        + 0.18 * category_score
        + 0.16 * root_score
        + curated_score
        + 0.06 * completeness
    )
    foodon_boost_applied = False
    foodon_penalty_applied = False
    foodon_reason = ""
    foodon_components: Dict[str, Any] = {}
    if acronym == "FOODON":
        (
            semantic_score,
            neg_penalty,
            negative,
            foodon_boost_applied,
            foodon_penalty_applied,
            foodon_reason,
            foodon_components,
        ) = _apply_foodon_score_rule(
            profile,
            search_text,
            lexical_score,
            category_score,
            root_score,
            semantic_score,
            neg_penalty,
            negative,
        )
    quality_modifier = 0.65 + 0.35 * quality_score
    uncertainty_penalty = 0.05 if not has_quality_raw else 0.0
    tie_breaker = 0.02 if acronym in configured_core else 0.0
    if profile.semantic_domain in {"material_substance", "packaging_material_substance"} and acronym in _MATERIAL_PRIORITY_ONTOLOGIES:
        tie_breaker += 0.30
    final_score = _clamp((semantic_score * quality_modifier) - uncertainty_penalty - neg_penalty - broad_penalty + tie_breaker)
    all_matched = _dedupe_ordered([*matched, *category_matched, *root_matched])
    if not all_matched and profile.semantic_domain == "general" and completeness:
        all_matched = ["metadata_available"]
    reason_bits = []
    if all_matched:
        reason_bits.append("matched " + ", ".join(all_matched[:5]))
    if negative:
        reason_bits.append("penalized " + "; ".join(negative[:3]))
    if not has_quality_raw:
        reason_bits.append("missing quality_raw small uncertainty penalty")
    if not reason_bits:
        reason_bits.append("weak registry evidence")
    return RegistryOntologyCandidate(
        acronym=acronym,
        name=name,
        searchable_text=search_text,
        source_fields_used=fields_used,
        quality_status=quality_status,
        quality_score=quality_score,
        has_quality_raw=has_quality_raw,
        registry_metadata_completeness=completeness,
        hard_exclusion_reason=hard_reason,
        lexical_domain_score=lexical_score,
        category_score=category_score,
        root_label_score=root_score,
        curated_profile_score=curated_score,
        negative_domain_penalty=neg_penalty,
        broadness_penalty=broad_penalty,
        semantic_score=semantic_score,
        final_registry_suitability_score=final_score,
        foodon_boost_applied=foodon_boost_applied,
        foodon_penalty_applied=foodon_penalty_applied,
        foodon_reason=foodon_reason,
        foodon_score_components=foodon_components,
        matched_signals=all_matched,
        negative_signals=negative,
        ranking_reason="; ".join(reason_bits),
    )


def rank_registry_ontology_candidates(
    term: str,
    definition: Optional[str],
    registry_acronyms: List[str],
    *,
    registry_lookup: Optional[Any] = None,
    table_context: Optional[str] = None,
    column_context: Optional[str] = None,
    rdf_role: Optional[str] = None,
    curated: Optional[Dict[str, OntologyCapabilityProfile]] = None,
    configured_core: Optional[List[str]] = None,
    blocklist: Optional[List[str]] = None,
    allowlist: Optional[List[str]] = None,
    allow_excluded_for_debug: bool = False,
) -> tuple:
    curated = curated if curated is not None else load_capability_profiles()
    profile = build_term_context_profile(term, definition, table_context, column_context, rdf_role)
    core_set = {str(a).strip().upper() for a in (configured_core or [])}
    block_set = {str(a).strip().upper() for a in (blocklist or [])}
    allow_set = {str(a).strip().upper() for a in (allowlist or [])}
    candidates: List[RegistryOntologyCandidate] = []
    excluded: List[RegistryOntologyCandidate] = []
    seen = set()
    for raw in registry_acronyms or []:
        acronym = str(raw or "").strip().upper()
        if not acronym or acronym in seen:
            continue
        seen.add(acronym)
        entry = {}
        if callable(registry_lookup):
            try:
                entry = registry_lookup(acronym) or {}
            except Exception:
                entry = {}
        if not isinstance(entry, dict):
            entry = {}
        entry = {**entry, "acronym": entry.get("acronym") or acronym}
        candidate = score_registry_ontology_candidate(
            acronym,
            entry,
            profile,
            curated=curated,
            configured_core=core_set,
            blocklist=block_set,
            allowlist=allow_set,
        )
        if candidate.hard_exclusion_reason and not allow_excluded_for_debug:
            excluded.append(candidate)
            continue
        candidates.append(candidate)
    candidates.sort(key=lambda c: c.final_registry_suitability_score, reverse=True)
    return profile, candidates, excluded


def _ranked_from_registry_candidates(candidates: List[RegistryOntologyCandidate]) -> List[RankedOntology]:
    return [
        RankedOntology(
            acronym=c.acronym,
            score=c.final_registry_suitability_score,
            reason=c.ranking_reason,
            positive_signals=c.matched_signals,
            negative_signals=c.negative_signals,
            profile_source="registry_programmatic",
            quality_score=c.quality_score,
            quality_status=c.quality_status,
            effective_score=c.final_registry_suitability_score,
        )
        for c in candidates
    ]


def _skipped_trusted_defaults(
    trusted: List[str],
    selected: List[str],
    candidate_by_acronym: Dict[str, RegistryOntologyCandidate],
) -> List[Dict[str, Any]]:
    selected_set = {a.upper() for a in selected}
    out = []
    for raw in trusted or []:
        acronym = str(raw or "").strip().upper()
        if not acronym or acronym in selected_set:
            continue
        candidate = candidate_by_acronym.get(acronym)
        reason = "not selected by registry ranking"
        if candidate and candidate.negative_signals:
            reason = "; ".join(candidate.negative_signals[:2])
        elif candidate:
            reason = f"registry score {candidate.final_registry_suitability_score:.3f} below primary cutoff"
        out.append({"acronym": acronym, "reason": reason})
    return out


def _blend_llm_with_programmatic_registry_scores(
    llm_ranked: List[RankedOntology],
    programmatic_top: List[RegistryOntologyCandidate],
    profile: TermContextProfile,
) -> List[RankedOntology]:
    llm_by_acronym = {item.acronym: item for item in llm_ranked}
    blended: List[RankedOntology] = []
    for candidate in programmatic_top:
        llm_item = llm_by_acronym.get(candidate.acronym)
        llm_score = float(llm_item.score) if llm_item is not None else 0.0
        programmatic_score = float(candidate.final_registry_suitability_score)
        final_score = _clamp(0.65 * programmatic_score + 0.35 * llm_score)
        negative_signals = list(candidate.negative_signals)
        reason = candidate.ranking_reason
        if llm_item is not None and llm_item.reason:
            reason = f"{reason}; llm_rerank: {llm_item.reason}"
            negative_signals = _dedupe_ordered([*negative_signals, *llm_item.negative_signals])
        if (
            candidate.acronym == "FOODON"
            and profile.semantic_domain in {"material_substance", "packaging_material_substance"}
            and not profile.foodon_boost_allowed
        ):
            # A generic LLM packaging rationale cannot overcome the programmatic
            # material-substance mismatch.
            final_score = min(final_score, programmatic_score)
            negative_signals = _dedupe_ordered([
                *negative_signals,
                "LLM FOODON promotion blocked: material substance with packaging only as application context",
            ])
            reason = f"{reason}; LLM FOODON promotion blocked by semantic_domain={profile.semantic_domain}"
        blended.append(
            RankedOntology(
                acronym=candidate.acronym,
                score=final_score,
                reason=reason,
                positive_signals=candidate.matched_signals,
                negative_signals=negative_signals,
                profile_source="registry_programmatic_llm_blend" if llm_item else "registry_programmatic",
                quality_score=candidate.quality_score,
                quality_status=candidate.quality_status,
                effective_score=final_score,
            )
        )
    blended.sort(key=lambda item: float(item.effective_score if item.effective_score is not None else item.score), reverse=True)
    return blended


# ---------------------------------------------------------------------------
# Phase 4 — deterministic prefilter (reduce prompt size only)
# ---------------------------------------------------------------------------

def _profile_prefilter_score(
    input_tokens: set,
    profile: OntologyCapabilityProfile,
) -> tuple:
    keyword_tokens = _tokens(" ".join(profile.routing_keywords))
    domain_tokens = _tokens(" ".join(profile.domain_tags))
    example_tokens = _tokens(" ".join(profile.example_classes))
    scope_tokens = _tokens(" ".join(profile.contains + profile.best_for + [profile.routing_description]))
    negative_tokens = _tokens(" ".join(profile.negative_keywords + profile.not_best_for))

    def _frac(toks: set) -> float:
        return (len(input_tokens & toks) / len(input_tokens)) if input_tokens and toks else 0.0

    reasons: List[str] = []
    kw = _frac(keyword_tokens)
    dm = _frac(domain_tokens)
    ex = _frac(example_tokens)
    sc = _frac(scope_tokens)
    neg_hits = len(input_tokens & negative_tokens)
    if kw:
        reasons.append("routing_keyword_overlap")
    if ex:
        reasons.append("example_class_overlap")
    if dm:
        reasons.append("domain_tag_overlap")
    score = (
        0.40 * kw
        + 0.20 * ex
        + 0.15 * dm
        + 0.15 * sc
        + 0.10 * (profile.generality if profile.generality else 0.0)
    )
    score -= 0.15 * min(2, neg_hits)
    if neg_hits:
        reasons.append("negative_keyword_penalty")
    return max(0.0, min(1.0, round(score, 4))), reasons


def prefilter_ontology_profiles_for_routing(
    term: str,
    definition: Optional[str],
    profiles: List[OntologyCapabilityProfile],
    *,
    column_context: Optional[str] = None,
    project_context: Optional[str] = None,
    limit: int = 20,
    configured_core: Optional[List[str]] = None,
) -> List[PrefilteredOntologyProfile]:
    """Deterministically shrink the candidate ontology set before the LLM call.

    Always keeps configured core, broad generalists, and project-priority
    ontologies (when present) so routing never silently drops the safety net.
    """
    input_tokens = _tokens(" ".join(filter(None, [term, definition, column_context, project_context])))
    configured = {str(a).strip().upper() for a in (configured_core or [])}

    scored: List[PrefilteredOntologyProfile] = []
    for profile in profiles:
        score, reasons = _profile_prefilter_score(input_tokens, profile)
        forced = bool(
            profile.acronym in configured
            and (profile.acronym in BROAD_ONTOLOGIES or profile.acronym in PROJECT_PRIORITY_ONTOLOGIES)
        )
        if forced:
            reasons = [*reasons, "forced_core_or_priority"]
        scored.append(PrefilteredOntologyProfile(profile=profile, prefilter_score=score, forced_included=forced, reasons=reasons))

    forced_items = [item for item in scored if item.forced_included]
    rest = sorted(
        [item for item in scored if not item.forced_included],
        key=lambda item: item.prefilter_score,
        reverse=True,
    )

    selected: List[PrefilteredOntologyProfile] = []
    seen = set()
    for item in [*forced_items, *rest]:
        if item.profile.acronym in seen:
            continue
        seen.add(item.profile.acronym)
        selected.append(item)
        if len(selected) >= max(1, int(limit)) and not any(
            x.forced_included and x.profile.acronym not in seen for x in scored
        ):
            break
    return selected[: max(len(forced_items), int(limit))]


# ---------------------------------------------------------------------------
# Phase 5 — LLM ontology suitability scoring
# ---------------------------------------------------------------------------

_ROUTING_SYSTEM_PROMPT = (
    "You are an ontology routing assistant for terminology reconciliation. "
    "Given an input term, you assign an ONTOLOGY SUITABILITY SCORE to each provided "
    "ontology: how well-suited the ontology is to be QUERIED for this term. "
    "This is NOT a final mapping confidence — you are not choosing a class, only "
    "deciding which ontologies are worth searching. "
    "Score each ontology 0.0-1.0. Only rank the provided acronyms; never invent "
    "ontologies. Do not pick an ontology merely because it is broad — favor the "
    "ontology whose scope best matches the term, definition and context, but give "
    "broad ontologies a reasonable score if they plausibly contain the concept. "
    "Score guide: 0.85-1.0 excellent; 0.70-0.85 strong; 0.55-0.70 plausible "
    "secondary; 0.35-0.55 weak; <0.35 poor (only in broad fallback). If no ontology "
    "clearly fits, still rank the best options but set routing_confident=false and "
    "fallback_to_default=true. Respect the detected semantic_domain: for "
    "material_substance, packaging is application context and FOODON must not be "
    "selected solely because text says packaging/package. FOODON is primary only "
    "for food products, explicit food packaging/container/component concepts, or "
    "direct exact registry coverage evidence. Return strict JSON only."
)


_COMPACT_QUALITY_FLAGS = {"curated_profile", "sparse_metadata", "is_view", "is_flat", "broad_aggregator"}


def _build_routing_user_prompt(
    term: str,
    definition: Optional[str],
    prefiltered: List[PrefilteredOntologyProfile],
    *,
    column_context: Optional[str],
    project_context: Optional[str],
    quality_by_acronym: Optional[Dict[str, "OntologyQualityProfile"]] = None,
) -> str:
    profile_payload = []
    for item in prefiltered:
        card = item.profile.compact_for_prompt()
        if quality_by_acronym:
            quality = quality_by_acronym.get(item.profile.acronym)
            if quality is not None:
                # Compact, decision-relevant quality only — NO administrative metadata.
                card["quality_status"] = quality.quality_status
                flags = [f for f in (quality.positive_flags + quality.negative_flags) if f in _COMPACT_QUALITY_FLAGS]
                if flags:
                    card["quality_flags"] = flags
        profile_payload.append(card)
    lines = [
        f"Input term: {term}",
        f"Definition: {definition or '(none provided)'}",
    ]
    if column_context:
        lines.append(f"Column context: {column_context}")
    if project_context:
        lines.append(f"Project context: {project_context}")
    lines.append("")
    lines.append("Candidate ontologies (semantic capability profiles only):")
    lines.append(json.dumps(profile_payload, ensure_ascii=False, indent=2))
    lines.append("")
    lines.append(
        "Return strict JSON:\n"
        "{\n"
        '  "routing_confident": true,\n'
        '  "fallback_to_default": false,\n'
        '  "reason": "short reason",\n'
        '  "ranked_ontologies": [\n'
        '    {"acronym": "FOODON", "score": 0.9, "reason": "...",\n'
        '     "positive_signals": ["oil","food"], "negative_signals": []}\n'
        "  ]\n"
        "}"
    )
    return "\n".join(lines)


def rank_ontologies_with_llm(
    term: str,
    definition: Optional[str],
    profiles: List[OntologyCapabilityProfile],
    *,
    column_context: Optional[str] = None,
    project_context: Optional[str] = None,
    max_ontologies: int = 20,
    prefilter_limit: int = 20,
    configured_core: Optional[List[str]] = None,
    model_provider: str = "openai",
    model_name: str = "gpt-5.1",
    api_key_env: str = "OPENAI_API_KEY",
    reasoning_effort: str = "none",
    min_score: float = 0.40,
    strong_score: float = 0.80,
    confident_margin: float = 0.10,
    quality_by_acronym: Optional[Dict[str, "OntologyQualityProfile"]] = None,
    allow_excluded_for_debug: bool = False,
    capture_prompts: bool = False,
    run_id: Optional[str] = None,
    term_id: Optional[str] = None,
    row_index: Optional[Any] = None,
    call_id: Optional[str] = None,
) -> OntologyRoutingResult:
    """Run LLM ontology suitability ranking over (prefiltered) capability profiles.

    When ``quality_by_acronym`` is provided, hard-excluded ontologies are dropped
    BEFORE the prefilter/LLM prompt, and each ranked ontology's effective routing
    score = llm_suitability * (0.5 + 0.5 * quality_score). Quality is never used as
    candidate confidence; it only filters/dampens which ontologies to query.
    """
    if quality_by_acronym and not allow_excluded_for_debug:
        profiles = [
            p for p in profiles
            if quality_by_acronym.get(p.acronym) is None or not quality_by_acronym[p.acronym].excluded
        ]
    prefiltered = prefilter_ontology_profiles_for_routing(
        term, definition, profiles,
        column_context=column_context, project_context=project_context,
        limit=prefilter_limit, configured_core=configured_core,
    )[: max(1, int(max_ontologies))]
    profile_by_acr = {item.profile.acronym: item.profile for item in prefiltered}
    if not prefiltered:
        return _decide_routing([], min_score=min_score, strong_score=strong_score,
                               confident_margin=confident_margin, used_llm=False,
                               reason="no candidate ontologies", llm_confident=False, llm_fallback=True)

    user_prompt = _build_routing_user_prompt(
        term, definition, prefiltered,
        column_context=column_context, project_context=project_context,
        quality_by_acronym=quality_by_acronym,
    )
    try:
        payload = generate_structured_completion(
            model_provider,
            model_name,
            _ROUTING_SYSTEM_PROMPT,
            user_prompt,
            api_key_env=api_key_env,
            temperature=0,
            max_tokens=900,
            reasoning_effort=reasoning_effort,
            retries_on_parse_failure=1,
            interaction_purpose="ontology_routing",
            run_id=run_id,
            term_id=term_id,
            row_index=row_index,
            stage="ontology_routing",
            purpose="ontology_routing",
            call_id=call_id,
        )
    except Exception as exc:
        return _decide_routing([], min_score=min_score, strong_score=strong_score,
                               confident_margin=confident_margin, used_llm=True,
                               reason=f"llm_error: {type(exc).__name__}", llm_confident=False,
                               llm_fallback=True, error=str(exc)[:300])

    ranked = _parse_ranked_ontologies(payload, profile_by_acr)
    if quality_by_acronym:
        ranked = _apply_quality_to_ranked(ranked, quality_by_acronym, allow_excluded_for_debug)
    llm_confident = bool(payload.get("routing_confident", False)) if isinstance(payload, dict) else False
    llm_fallback = bool(payload.get("fallback_to_default", True)) if isinstance(payload, dict) else True
    reason = str(payload.get("reason", "") or "").strip() if isinstance(payload, dict) else ""
    result = _decide_routing(
        ranked, min_score=min_score, strong_score=strong_score, confident_margin=confident_margin,
        used_llm=True, reason=reason, llm_confident=llm_confident, llm_fallback=llm_fallback,
    )
    result.quality_gate_applied = bool(quality_by_acronym)
    if capture_prompts:
        result.prompt_debug = {
            "stage": "ontology_routing",
            "purpose": "ontology_routing",
            "call_id": call_id,
            "provider": model_provider,
            "model": model_name,
            "system_prompt": _ROUTING_SYSTEM_PROMPT,
            "user_prompt": user_prompt,
            "raw_response": payload if isinstance(payload, dict) else str(payload)[:4000],
            "parsed_response": payload if isinstance(payload, dict) else {},
        }
    return result


def _apply_quality_to_ranked(
    ranked: List[RankedOntology],
    quality_by_acronym: Dict[str, "OntologyQualityProfile"],
    allow_excluded_for_debug: bool,
) -> List[RankedOntology]:
    """Attach quality + compute effective_routing_score = llm * (0.5 + 0.5*quality).

    Hard-excluded ontologies get effective_score 0.0 and are dropped from the
    ranking (unless debug override). Quality is NEVER a candidate confidence.
    """
    out: List[RankedOntology] = []
    for r in ranked:
        quality = quality_by_acronym.get(r.acronym)
        if quality is None:
            r.effective_score = r.score
            out.append(r)
            continue
        r.quality_score = quality.quality_score
        r.quality_status = quality.quality_status
        if quality.excluded:
            r.effective_score = 0.0
            if allow_excluded_for_debug:
                out.append(r)
            continue
        r.effective_score = _clamp(r.score * (0.50 + 0.50 * quality.quality_score))
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# Phase 6 — validation + routing decision
# ---------------------------------------------------------------------------

def _clamp01(value: Any, default: float = 0.0) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return default


def _parse_ranked_ontologies(payload: Any, profile_by_acr: Dict[str, OntologyCapabilityProfile]) -> List[RankedOntology]:
    if not isinstance(payload, dict):
        return []
    raw_items = payload.get("ranked_ontologies", [])
    if not isinstance(raw_items, list):
        return []
    ranked: List[RankedOntology] = []
    seen = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        acronym = str(item.get("acronym", "") or "").strip().upper()
        if not acronym or acronym not in profile_by_acr or acronym in seen:
            continue  # drop unknown / duplicate acronyms
        reason = str(item.get("reason", "") or "").strip()
        if not reason:
            reason = "(no reason provided)"
        seen.add(acronym)

        def _signals(key: str) -> List[str]:
            value = item.get(key, [])
            if isinstance(value, list):
                return [str(s).strip() for s in value if str(s).strip()][:8]
            return []

        ranked.append(
            RankedOntology(
                acronym=acronym,
                score=_clamp01(item.get("score"), 0.0),
                reason=reason,
                positive_signals=_signals("positive_signals"),
                negative_signals=_signals("negative_signals"),
                profile_source=profile_by_acr[acronym].source,
            )
        )
    ranked.sort(key=lambda r: r.score, reverse=True)
    return ranked


def _decide_routing(
    ranked: List[RankedOntology],
    *,
    min_score: float,
    strong_score: float,
    confident_margin: float,
    used_llm: bool,
    reason: str,
    llm_confident: bool,
    llm_fallback: bool,
    error: str = "",
) -> OntologyRoutingResult:
    def _score_of(r: RankedOntology) -> float:
        return float(r.effective_score if r.effective_score is not None else r.score)

    ranked = sorted(ranked, key=_score_of, reverse=True)
    top = _score_of(ranked[0]) if ranked else 0.0
    second = _score_of(ranked[1]) if len(ranked) > 1 else 0.0
    margin = round(top - second, 4)

    invalid = (not ranked) or bool(error)
    if invalid or llm_fallback or top < float(min_score):
        mode = "fallback_default"
    elif top >= float(strong_score) and margin >= float(confident_margin):
        mode = "strong_routing"
    elif top >= 0.60:
        mode = "moderate_routing_plus_default"
    else:
        mode = "weak_routing_plus_default"

    confident = bool(used_llm and llm_confident and mode in {"strong_routing", "moderate_routing_plus_default"} and not invalid)
    fallback_to_default = mode == "fallback_default" or not confident and mode == "weak_routing_plus_default" or invalid
    # weak/fallback always keep the default cascade; strong/moderate trust routing.
    fallback_to_default = mode in {"fallback_default", "weak_routing_plus_default"} or invalid

    return OntologyRoutingResult(
        ranked_ontologies=ranked,
        routing_confident=confident,
        fallback_to_default=fallback_to_default,
        used_llm=used_llm,
        reason=reason,
        top_score=top,
        second_score=second,
        margin=margin,
        routing_mode=mode,
        error=error,
    )


def select_ontologies_for_retrieval(
    routing_result: OntologyRoutingResult,
    configured_default_ontologies: List[str],
    *,
    top_n: int = 5,
    include_default_core: bool = True,
    excluded_acronyms: Optional[set] = None,
) -> List[str]:
    """Translate a routing result into the concrete ontology list to query.

    Routing scores are used ONLY here, to select/prioritize ontologies. The full
    configured cascade is always preserved as a fallback safety net — minus any
    hard-excluded ontologies, which must never enter retrieval from routing.
    """
    excluded = {str(a).strip().upper() for a in (excluded_acronyms or set())}
    default = [str(a).strip().upper() for a in (configured_default_ontologies or []) if str(a).strip() and str(a).strip().upper() not in excluded]
    default_set = set(default)
    routed = [r.acronym for r in routing_result.ranked_ontologies if r.acronym]
    core = [a for a in default if a in BROAD_ONTOLOGIES or a in PROJECT_PRIORITY_ONTOLOGIES]

    mode = routing_result.routing_mode
    if mode == "strong_routing":
        chosen = routed[: max(1, top_n)] + (core if include_default_core else [])
    elif mode == "moderate_routing_plus_default":
        chosen = routed[: max(1, top_n)] + (core if include_default_core else [])
    elif mode == "weak_routing_plus_default":
        chosen = routed[:2] + default
    else:  # fallback_default
        chosen = list(default)

    # keep only acronyms we actually know how to query (configured), dedupe, order-stable
    out: List[str] = []
    seen = set()
    for acronym in chosen:
        acronym = str(acronym).strip().upper()
        if not acronym or acronym in seen or acronym in excluded:
            continue
        if acronym in default_set or mode in {"strong_routing", "moderate_routing_plus_default"}:
            seen.add(acronym)
            out.append(acronym)
    if not out:
        out = list(default)
    return out


def route_ontologies(
    term: str,
    definition: Optional[str],
    configured_default_ontologies: List[str],
    *,
    routing_search_mode: str = "configured_only",
    use_llm: bool = True,
    column_context: Optional[str] = None,
    table_context: Optional[str] = None,
    rdf_role: Optional[str] = None,
    project_context: Optional[str] = None,
    curated: Optional[Dict[str, OntologyCapabilityProfile]] = None,
    registry_lookup: Optional[Any] = None,
    trusted_fallback_ontologies: Optional[List[str]] = None,
    top_n: int = 5,
    prefilter_limit: int = 20,
    include_default_core: bool = True,
    min_score: float = 0.40,
    strong_score: float = 0.80,
    confident_margin: float = 0.10,
    model_provider: str = "openai",
    model_name: str = "gpt-5.1",
    api_key_env: str = "OPENAI_API_KEY",
    reasoning_effort: str = "none",
    enable_quality_gate: bool = True,
    project_relevant: Optional[List[str]] = None,
    blocklist: Optional[List[str]] = None,
    allowlist: Optional[List[str]] = None,
    allow_excluded_for_debug: bool = False,
    capture_prompts: bool = False,
    run_id: Optional[str] = None,
    term_id: Optional[str] = None,
    row_index: Optional[Any] = None,
    call_id: Optional[str] = None,
) -> tuple:
    """High-level convenience: build profiles, apply quality gate, rank, decide, select.

    Returns (selected_ontologies, OntologyRoutingResult). When ``use_llm`` is
    False this skips the LLM and returns a deterministic fallback that preserves
    the configured cascade (minus hard-excluded ontologies). Quality scores filter
    and dampen ontology selection only — never candidate confidence.
    """
    curated = curated if curated is not None else load_capability_profiles()
    routing_search_mode = str(routing_search_mode or "configured_only").strip().lower()
    if routing_search_mode == "registry_routed_all":
        profile, registry_candidates, excluded_candidates = rank_registry_ontology_candidates(
            term,
            definition,
            configured_default_ontologies,
            registry_lookup=registry_lookup,
            table_context=table_context or project_context,
            column_context=column_context,
            rdf_role=rdf_role,
            curated=curated,
            configured_core=trusted_fallback_ontologies or [],
            blocklist=blocklist,
            allowlist=allowlist,
            allow_excluded_for_debug=allow_excluded_for_debug,
        )
        programmatic_top_count = max(30, int(prefilter_limit or 20))
        programmatic_top = registry_candidates[:programmatic_top_count]
        selection_source = "programmatic_registry_ranking"
        ranked_for_selection = _ranked_from_registry_candidates(programmatic_top)
        prompt_debug = None
        llm_top: List[Dict[str, Any]] = []
        used_llm = False
        llm_error = ""
        if use_llm and programmatic_top:
            llm_profiles = [
                OntologyCapabilityProfile(
                    acronym=c.acronym,
                    display_name=c.name or c.acronym,
                    routing_description=c.ranking_reason,
                    contains=c.matched_signals[:8],
                    best_for=c.matched_signals[:8],
                    not_best_for=c.negative_signals[:8],
                    domain_tags=c.matched_signals[:8],
                    routing_keywords=c.matched_signals[:12],
                    negative_keywords=c.negative_signals[:8],
                    example_classes=[],
                    generality=0.4,
                    profile_confidence=0.7,
                    source="registry_programmatic_top",
                )
                for c in programmatic_top
            ]
            quality_by_acronym = {
                c.acronym: OntologyQualityProfile(
                    acronym=c.acronym,
                    quality_status=c.quality_status,
                    quality_score=c.quality_score,
                    hard_exclusion_reason=c.hard_exclusion_reason or None,
                )
                for c in programmatic_top
            }
            profile_context = json.dumps(
                {
                    "semantic_domain": profile.semantic_domain,
                    "entity_type": profile.detected_entity_type,
                    "application_context": profile.application_context,
                    "food_context_detected": profile.food_context_detected,
                    "foodon_boost_allowed": profile.foodon_boost_allowed,
                    "foodon_boost_reason": profile.foodon_boost_reason,
                    "material_substance_detected": profile.material_substance_detected,
                    "term_head_entity": profile.term_head_entity,
                    "preferred_ontology_domains": profile.preferred_ontology_domains,
                    "disallowed_or_low_priority_domains": profile.disallowed_or_low_priority_domains,
                },
                ensure_ascii=False,
            )
            llm_project_context = "\n".join(
                part for part in [project_context or table_context or "", f"Term context profile: {profile_context}"] if part
            )
            llm_result = rank_ontologies_with_llm(
                term,
                definition,
                llm_profiles,
                column_context=column_context,
                project_context=llm_project_context,
                max_ontologies=programmatic_top_count,
                prefilter_limit=programmatic_top_count,
                configured_core=[],
                model_provider=model_provider,
                model_name=model_name,
                api_key_env=api_key_env,
                reasoning_effort=reasoning_effort,
                min_score=min_score,
                strong_score=strong_score,
                confident_margin=confident_margin,
                quality_by_acronym=quality_by_acronym,
                allow_excluded_for_debug=allow_excluded_for_debug,
                capture_prompts=capture_prompts,
                run_id=run_id,
                term_id=term_id,
                row_index=row_index,
                call_id=call_id,
            )
            used_llm = True
            prompt_debug = llm_result.prompt_debug
            llm_error = llm_result.error
            if llm_result.ranked_ontologies:
                ranked_for_selection = _blend_llm_with_programmatic_registry_scores(
                    llm_result.ranked_ontologies,
                    programmatic_top,
                    profile,
                )
                selection_source = "registry_programmatic_llm_blend"
                llm_top = [item.as_dict() for item in llm_result.ranked_ontologies[:programmatic_top_count]]

        selected: List[str] = []
        seen = set()
        min_count = min(3, len(ranked_for_selection))
        max_count = max(min_count, min(5, int(top_n or 5)))
        for item in ranked_for_selection:
            acronym = item.acronym.upper()
            if acronym and acronym not in seen:
                seen.add(acronym)
                selected.append(acronym)
            if len(selected) >= max_count:
                break
        if len(selected) < min_count:
            for candidate in programmatic_top:
                if candidate.acronym not in seen:
                    seen.add(candidate.acronym)
                    selected.append(candidate.acronym)
                if len(selected) >= min_count:
                    break

        selected_scores = [
            float(item.effective_score if item.effective_score is not None else item.score)
            for item in ranked_for_selection
        ]
        top_score = selected_scores[0] if selected_scores else 0.0
        second_score = selected_scores[1] if len(selected_scores) > 1 else 0.0
        candidate_by_acronym = {c.acronym: c for c in registry_candidates}
        excluded_summary = {
            "excluded_count": len(excluded_candidates),
            "exclusion_reason_counts": {},
            "examples": [c.acronym for c in excluded_candidates[:10]],
            "missing_quality_raw_count": sum(1 for c in registry_candidates if not c.has_quality_raw),
        }
        for candidate in excluded_candidates:
            reason = candidate.hard_exclusion_reason or "unknown"
            excluded_summary["exclusion_reason_counts"][reason] = excluded_summary["exclusion_reason_counts"].get(reason, 0) + 1
        result = OntologyRoutingResult(
            ranked_ontologies=ranked_for_selection,
            routing_confident=bool(selected),
            fallback_to_default=False,
            used_llm=used_llm,
            reason="registry_routed_all_primary_selection",
            top_score=top_score,
            second_score=second_score,
            margin=round(top_score - second_score, 4),
            routing_mode="registry_routed_all",
            error=llm_error,
            quality_gate_applied=bool(enable_quality_gate),
            excluded_summary=excluded_summary,
            prompt_debug=prompt_debug,
            term_context_profile=profile.as_dict(),
            registry_candidate_count=len(configured_default_ontologies or []),
            eligible_after_quality_gate=len(registry_candidates),
            missing_quality_raw_count=sum(1 for c in registry_candidates if not c.has_quality_raw),
            registry_programmatic_top=[c.as_dict() for c in programmatic_top],
            llm_reranked_top=llm_top,
            routed_primary_ontologies=list(selected),
            skipped_default_or_trusted_ontologies=_skipped_trusted_defaults(
                trusted_fallback_ontologies or [], selected, candidate_by_acronym
            ),
            selection_source=selection_source,
        )
        return selected, result

    profiles = build_profiles_for_acronyms(
        configured_default_ontologies, curated=curated, registry_lookup=registry_lookup
    )

    quality_by_acronym: Optional[Dict[str, OntologyQualityProfile]] = None
    excluded_set: set = set()
    excluded_summary: Dict[str, Any] = {}
    if enable_quality_gate:
        quality_by_acronym = build_quality_profiles(
            configured_default_ontologies,
            curated=curated, registry_lookup=registry_lookup,
            configured_core=configured_default_ontologies,
            project_relevant=project_relevant or PROJECT_PRIORITY_ONTOLOGIES,
            blocklist=blocklist, allowlist=allowlist,
        )
        if not allow_excluded_for_debug:
            excluded_set = {a for a, q in quality_by_acronym.items() if q.excluded}
        reason_counts: Dict[str, int] = {}
        for acr in sorted(excluded_set):
            reason = quality_by_acronym[acr].hard_exclusion_reason or "unknown"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        excluded_summary = {
            "excluded_count": len(excluded_set),
            "exclusion_reason_counts": reason_counts,
            "examples": sorted(excluded_set)[:10],
        }

    if not use_llm:
        result = OntologyRoutingResult(
            ranked_ontologies=[],
            routing_confident=False,
            fallback_to_default=True,
            used_llm=False,
            reason="llm_routing_disabled",
            routing_mode="fallback_default",
        )
    else:
        result = rank_ontologies_with_llm(
            term, definition, profiles,
            column_context=column_context, project_context=project_context,
            max_ontologies=prefilter_limit, prefilter_limit=prefilter_limit,
            configured_core=configured_default_ontologies,
            model_provider=model_provider, model_name=model_name,
            api_key_env=api_key_env, reasoning_effort=reasoning_effort,
            min_score=min_score, strong_score=strong_score, confident_margin=confident_margin,
            quality_by_acronym=quality_by_acronym,
            allow_excluded_for_debug=allow_excluded_for_debug,
            capture_prompts=capture_prompts,
            run_id=run_id,
            term_id=term_id,
            row_index=row_index,
            call_id=call_id,
        )
    result.quality_gate_applied = bool(enable_quality_gate)
    result.excluded_summary = excluded_summary
    selected = select_ontologies_for_retrieval(
        result, configured_default_ontologies, top_n=top_n,
        include_default_core=include_default_core, excluded_acronyms=excluded_set,
    )
    return selected, result
