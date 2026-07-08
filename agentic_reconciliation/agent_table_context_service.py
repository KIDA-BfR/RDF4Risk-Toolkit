# -*- coding: utf-8 -*-
"""Semantic profiling utilities for reconciliation: term/table category profiling,
registry-derived ontology suitability scoring, and namespace resolution.

These heuristics back the ``broad_retrieval_registry_scored`` ontology search mode
(the registry is applied AFTER retrieval as candidate scoring context) and expose:

* :func:`build_table_context_profile` / :func:`build_chunk_context_profiles`
* :func:`build_term_category_profile` (term profile, optionally *with* table/chunk context)
* :func:`compute_ontology_suitability_score` / :func:`select_category_ontologies_from_registry`

The profiling is heuristic and offline by default (no LLM, no network) so that a
whole table can be profiled once, cheaply, before per-term reconciliation. A
table/chunk profile is a *preferred search profile*: it never becomes a hard
allow-list. When the preferred category ontologies are weak, the retrieval layer
(Phase 5, in the orchestrator) falls back to a broad BioPortal search to preserve
recall.

Design rules honoured here:

* The table/chunk profile must not override obvious term-level signals -- a chunk
  can be ``mixed_domain`` and low category confidence must not force preferences.
* Placeholder/status values are never given external ontology preferences and are
  flagged ``do_not_auto_verify_external_match``.
* Ambiguous common words require category context (``require_category_context``);
  they are never verified on lexical evidence alone.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .agent_ontology_routing import (
    TermContextProfile,
    build_term_context_profile,
    load_capability_profiles,
    score_registry_ontology_candidate,
    _contains_signal,
    _normalize_search_text,
)

try:  # registry access is optional; the module degrades gracefully when absent.
    from .agent_bioportal_context_service import (
        REGISTRY_PATH,
        BioPortalOntologyRegistry,
        get_ontology_context,
    )
except Exception:  # pragma: no cover - defensive
    REGISTRY_PATH = None  # type: ignore
    BioPortalOntologyRegistry = None  # type: ignore

    def get_ontology_context(acronym, **_kwargs):  # type: ignore
        return None


__all__ = [
    "TermCategoryResult",
    "TableContextProfile",
    "ChunkContextProfile",
    "CategoryOntologySelection",
    "categorize_term",
    "build_table_context_profile",
    "build_chunk_context_profiles",
    "build_term_category_profile",
    "select_category_ontologies_from_registry",
    "compute_ontology_suitability_score",
    "term_type_core_ontologies",
    "resolve_namespace_term",
    "CATEGORY_ONTOLOGY_PRIORS",
    "TERM_TYPE_CORE_ONTOLOGIES",
    "NAMESPACE_PREFIXES",
]


# ---------------------------------------------------------------------------
# Category vocabulary + signals (Phase 1/2)
# ---------------------------------------------------------------------------

# Verification policy tokens surfaced to the retrieval / gate layers.
POLICY_STANDARD = "standard"
POLICY_NO_AUTO_VERIFY = "do_not_auto_verify_external_match"  # placeholder/status
POLICY_REQUIRE_CONTEXT = "require_category_context"  # ambiguous common word


# Category -> keyword/phrase signals. Multiword phrases are matched as substrings,
# single tokens as whole tokens (see agent_ontology_routing._contains_signal).
CATEGORY_SIGNALS: Dict[str, set] = {
    "food_product": {
        "food", "food product", "food matrix", "edible", "ingredient", "milk",
        "cheese", "cheddar", "olive oil", "apple juice", "juice", "meat", "fish",
        "sausage", "butter", "yoghurt", "yogurt", "bread", "fruit", "vegetable",
        "beverage", "dairy", "egg", "poultry", "seafood", "wine", "flour",
        # crops / commodities
        "maize", "corn", "wheat", "durum wheat", "rice", "barley", "oat", "oats",
        "rye", "soybean", "soya", "potato", "tomato", "onion", "carrot", "lettuce",
        "spinach", "pepper", "cucumber", "grape", "apple", "banana", "orange",
        "strawberry", "peanut", "almond", "hazelnut", "cocoa", "coffee", "tea",
    },
    "food_safety": {
        "food safety", "hazard", "haccp", "food hygiene", "spoilage",
        "shelf life", "allergen", "foodborne",
    },
    "organism_taxon": {
        "organism", "taxon", "taxonomy", "species", "genus", "bacterium",
        "bacteria", "virus", "fungus", "listeria", "salmonella", "escherichia",
        "campylobacter", "pathogen", "microorganism", "serovar", "yeast", "mould",
        "mold", "protozoa", "nematode",
    },
    "chemical_substance": {
        "chemical", "compound", "molecule", "molecular entity", "substance",
        "acid", "reagent", "solvent", "preservative", "additive",
        "sodium chloride", "nitrate", "sulphate", "sulfate", "ethanol", "buffer",
        "cadmium", "mercury", "arsenic", "chromium", "nickel", "zinc", "copper",
        "selenium", "manganese", "benzoic acid", "citric acid", "glucose", "lactose",
    },
    "toxin_or_contaminant": {
        "toxin", "mycotoxin", "aflatoxin", "contaminant", "residue",
        "heavy metal", "pollutant", "pesticide", "endotoxin", "shiga toxin",
        "enterotoxin", "biotoxin", "cadmium", "mercury", "arsenic",
        "ochratoxin", "acrylamide", "dioxin",
    },
    "clinical_condition": {
        "disease", "infection", "syndrome", "disorder", "illness",
        "salmonellosis", "listeriosis", "gastroenteritis", "diagnosis", "sepsis",
        "adverse event", "comorbidity", "outbreak",
    },
    "symptom": {
        "symptom", "fever", "diarrhea", "diarrhoea", "vomiting", "nausea",
        "abdominal pain", "rash", "cramps", "headache", "fatigue", "dehydration",
        "clinical finding",
    },
    "unit_or_quantity": {
        "unit", "units", "quantity", "degree celsius", "celsius", "kelvin",
        "fahrenheit", "kilogram", "milligram", "microgram", "litre",
        "liter", "millilitre", "milliliter", "percent", "cfu",
        "colony forming unit", "colony-forming unit", "molar", "ppm",
        "ppb", "metre", "count per",
    },
    "measurement_property": {
        "optical density", "absorbance", "concentration", "turbidity", "density",
        "ph value", "viscosity", "mass", "weight", "volume", "pressure",
        "ct value", "cycle threshold", "measurement", "quantification", "od600",
        "titre", "titer", "level of",
    },
    "assay_or_method": {
        "assay", "cytotoxicity", "cytotoxicity assay", "elisa", "test", "method",
        "protocol", "technique", "titration", "plating", "enumeration",
        "serotyping", "antibiogram", "susceptibility test", "bioassay",
        "immunoassay",
    },
    "lab_material_or_cell_line": {
        "culture medium", "agar", "broth", "reference strain", "reagent kit",
        "antibody", "plasmid vector", "cell culture", "growth medium",
    },
    "cell_line": {
        "cell line", "vero cell", "vero cells", "hela", "hela cell", "caco",
        "caco-2", "cho cell", "hek293", "jurkat", "immortalized cell",
        "reference cell line", "cell strain",
    },
    "food_processing_method": {
        "pasteurization", "pasteurisation", "sterilization", "sterilisation",
        "fermentation", "homogenization", "homogenisation", "canning", "smoking",
        "curing", "blanching", "food processing", "heat treatment", "irradiation",
        "preservation treatment", "thermal processing", "ultra high temperature",
    },
    "experimental_condition": {
        "incubation temperature", "incubation time", "storage temperature",
        "growth condition", "culture condition", "ph condition", "incubation",
        "storage condition", "experimental condition", "aerobic", "anaerobic",
        "incubation period",
    },
    "laboratory_process": {
        "dna extraction", "purification", "centrifugation", "filtration",
        "digestion", "lysis", "elution", "amplification step", "sample preparation",
        "sample processing", "homogenisation step",
    },
    "analytical_measurement_property": {
        "calibration curve", "limit of detection", "limit of quantification",
        "recovery rate", "linearity", "repeatability", "reproducibility",
        "standard curve", "detection limit", "measurement uncertainty",
        "quantification limit",
        # generic analytical/regulatory-limit vocabulary tokens (not benchmark phrases)
        "limit", "threshold", "migration", "residue", "dose", "intake",
        "quantification", "detection",
    },
    "geospatial_statistical_region": {
        "administrative region", "region code", "country code", "geographic region",
        "statistical region", "spatial unit", "nuts", "eurostat", "gadm",
    },
    "rdf_or_vocabulary_property": {
        # vocabulary-FAMILY tokens only (generic), not spelled-out property mappings
        "skos", "rdf", "rdfs", "owl", "dcterms", "dcat", "dublin core",
        "vocabulary property", "rdf property", "namespace property", "predicate",
        "wkt", "geosparql", "ogc", "well known text", "geometry",
    },
    "gene_or_variant_symbol": {
        "gene symbol", "allele symbol", "variant symbol", "resistance determinant",
    },
    "genomic_feature_or_gene": {
        "gene", "allele", "genome", "genomic", "operon", "chromosome", "locus",
        "resistance gene", "amr gene", "virulence gene", "mutation", "variant",
        "snp", "coding sequence", "open reading frame", "orf",
        "antimicrobial resistance gene", "antimicrobial resistance", "genotype",
    },
    "sequencing_or_bioinformatics_method": {
        "sequencing", "whole genome sequencing", "wgs", "ngs", "pcr", "qpcr",
        "rt-pcr", "mlst", "sequence type", "amplicon", "assembly", "alignment",
        "blast", "bioinformatics", "fastq", "variant calling", "typing",
        "in silico", "serotype prediction", "read mapping",
    },
    "environmental_entity": {
        "environment", "environmental", "wastewater", "waste water", "sewage",
        "effluent", "soil", "soil sample", "sediment", "water sample",
        "surface water", "groundwater", "river", "biofilm", "air sample",
        "manure", "slurry", "habitat", "ecosystem", "environmental sample",
    },
    "climate_variable": {
        "precipitation", "rainfall", "humidity", "relative humidity",
        "air temperature", "wind speed", "solar radiation", "climate", "weather",
        "evaporation", "snowfall", "dew point",
    },
    "material_substance": {
        "material", "polymer", "plastic", "polyamide", "polystyrene",
        "polycarbonate", "polyethylene", "polypropylene", "pvc", "pet", "nylon",
        "cellophane", "aluminium", "aluminum", "glass", "metal", "paperboard",
        "cardboard", "fibre", "fiber", "fabric", "natural fibre fabric",
        "textile", "rubber", "resin", "foil", "film",
    },
    "packaging_component": {
        "packaging", "package", "wrapper", "casing", "container", "bottle",
        "jar", "tray", "pouch", "sachet", "carton", "lid",
        "blister pack", "food packaging", "food contact material",
    },
    "data_object_or_format": {
        # generic data-format / data-object vocabulary (universal formats + generic tokens)
        "dataset", "database", "file format", "data format", "csv", "tsv", "json",
        "xml", "fasta", "hdf5", "netcdf", "raster", "tiff", "data object",
        "accession number", "identifier", "record", "data array", "shapefile",
    },
    "semantic_web_concept": {
        "ontology", "rdf", "owl", "sparql", "semantic web", "linked data",
        "uri", "iri", "namespace", "knowledge graph", "triple", "vocabulary",
    },
    "statistical_or_risk_concept": {
        "risk", "probability", "prevalence", "incidence", "odds ratio",
        "relative risk", "confidence interval", "hazard ratio", "dose response",
        "dose-response", "standard deviation", "correlation", "p value",
        "exposure assessment", "uncertainty",
    },
}


# Whole-value placeholder/status values (compared case-insensitively, exact).
PLACEHOLDER_STATUS_VALUES = {
    "other", "others", "unknown", "none", "na", "n/a", "not applicable",
    "no information", "not available", "not reported", "missing", "unspecified",
    "undetermined", "not determined", "not tested", "tbd", "-", "--", "n.a.",
    "no data", "blank", "empty", "null",
}
# Packaging / boolean status participles (treated as placeholder/status, note kept).
PACKAGING_STATUS_VALUES = {
    "wrapped", "not wrapped", "packed", "not packed", "unpacked", "sealed",
    "unsealed", "packaged", "loose", "bulk", "vacuum packed", "yes", "no",
    "present", "absent", "positive", "negative",
}

# Common English words with strong domain-specific senses -> must not be verified
# on lexical evidence alone; require category context.
AMBIGUOUS_COMMON_WORDS = {
    "culture", "isolate", "resistance", "lead", "exposure", "sample", "control",
    "medium", "plate", "host", "agent", "expression", "activity", "carrier",
    "vector", "reservoir", "spread", "transmission", "recovery", "matrix",
    "strain", "well", "run", "reading", "load", "count",
}

# Gene-symbol-like single tokens, e.g. qacEdelta1, blaCTX, mecA, tetM, sul1.
_GENE_SYMBOL_RE = re.compile(
    r"^(bla[A-Za-z0-9\-]+|[a-z]{2,6}[A-Z][A-Za-z0-9\-]*|[a-z]{2,5}\d{1,3})$"
)

# Binomial nomenclature: "Genus species" (Capitalised genus + lowercase species). Applied
# on the RAW term only as a fallback when no other category signal fired, so title-cased
# food/material terms (which carry their own signals) are never misread as organisms.
_BINOMIAL_RE = re.compile(r"^[A-Z][a-z]{2,} [a-z][a-z\-]{2,}$")


# ---------------------------------------------------------------------------
# Curated category -> ontology priors (Phase 3)
# ---------------------------------------------------------------------------

def _prior(preferred, domains, fallback, pen_domains, pen_acr):
    return {
        "preferred_acronyms": [a.upper() for a in preferred],
        "preferred_domains": list(domains),
        "fallback_acronyms": [a.upper() for a in fallback],
        "penalized_domains": list(pen_domains),
        "penalized_acronyms": [a.upper() for a in pen_acr],
    }


# Priority profile only -- NOT a hard allow list. Acronyms not present in the
# local registry still form the preferred list (they are simply unverified priors).
CATEGORY_ONTOLOGY_PRIORS: Dict[str, Dict[str, List[str]]] = {
    "food_product": _prior(
        ["FOODON", "FOBI"], ["food", "food product", "food matrix", "edible", "ingredient"],
        ["CHEBI", "NCIT", "AGROVOC"], ["unit", "quantity", "gene", "clinical disease"], ["QUDT", "QUDT2", "UO"],
    ),
    "food_safety": _prior(
        ["FOODON", "CHEBI"], ["food safety", "hazard", "food"],
        ["ENVO", "NCIT", "MESH"], ["unit", "gene"], ["QUDT", "UO"],
    ),
    "organism_taxon": _prior(
        ["NCBITAXON"], ["taxonomy", "organism", "species"],
        ["GENEPIO", "ENVO", "NCIT", "MESH"], ["unit", "material", "food packaging"], ["QUDT", "UO", "FOODON"],
    ),
    "chemical_substance": _prior(
        ["CHEBI"], ["chemical", "compound", "molecular entity"],
        ["NCIT", "MESH", "PR"], ["taxonomy", "unit", "clinical disease"], ["NCBITAXON", "QUDT"],
    ),
    "toxin_or_contaminant": _prior(
        ["CHEBI", "ENVO"], ["toxin", "contaminant", "chemical"],
        ["NCIT", "MESH", "FOODON"], ["unit", "gene"], ["QUDT", "UO"],
    ),
    "clinical_condition": _prior(
        ["SNOMEDCT", "MEDDRA", "ICD10", "RCD", "NCIT"], ["clinical", "disease", "infection", "medical terminology"],
        ["MESH", "DOID", "MONDO"], ["unit", "material", "food"], ["QUDT", "FOODON"],
    ),
    "symptom": _prior(
        ["SNOMEDCT", "MEDDRA", "NCIT"], ["symptom", "clinical finding", "sign"],
        ["MESH", "HP", "ICD10"], ["unit", "material"], ["QUDT", "FOODON"],
    ),
    "unit_or_quantity": _prior(
        ["QUDT", "QUDT2", "UO", "OM", "UNITSONT"], ["unit", "quantity", "measurement"],
        ["NCIT", "OBOE", "PATO"], ["food", "gene", "clinical disease", "organism"], ["FOODON", "NCBITAXON"],
    ),
    "measurement_property": _prior(
        ["OBI", "BAO", "PATO", "UO", "NCIT"], ["measurement", "quality", "property", "assay"],
        ["EFO", "QUDT", "OBOE"], ["food", "organism"], ["FOODON", "NCBITAXON"],
    ),
    "assay_or_method": _prior(
        ["OBI", "EFO", "BAO"], ["assay", "method", "experimental"],
        ["NCIT", "MESH", "EDAM"], ["food", "unit"], ["FOODON", "QUDT"],
    ),
    "lab_material_or_cell_line": _prior(
        ["OBI", "CLO", "BTO", "EFO"], ["cell line", "material", "reagent", "biomaterial"],
        ["NCIT", "CL", "MESH"], ["food", "unit"], ["FOODON", "QUDT"],
    ),
    "genomic_feature_or_gene": _prior(
        ["GENEPIO", "EDAM", "OBI", "SO", "NCIT"], ["gene", "genomic", "sequence feature", "amr"],
        ["MESH", "NCBITAXON", "OGG"], ["food", "unit", "clinical disease"], ["FOODON", "QUDT", "UO"],
    ),
    "sequencing_or_bioinformatics_method": _prior(
        ["EDAM", "OBI", "GENEPIO"], ["bioinformatics", "sequencing", "data", "computational method"],
        ["NCIT", "SWO", "BAO"], ["food", "unit", "clinical disease"], ["FOODON", "QUDT"],
    ),
    "environmental_entity": _prior(
        ["ENVO"], ["environment", "environmental material", "habitat"],
        ["NCIT", "MESH", "AGROVOC"], ["unit", "gene", "clinical disease"], ["QUDT", "UO"],
    ),
    "climate_variable": _prior(
        ["ENVO", "QUDT", "NCIT"], ["climate", "environmental variable", "measurement", "quantity"],
        ["UO", "OM", "SWEET"], ["food", "gene", "organism"], ["FOODON", "NCBITAXON"],
    ),
    "material_substance": _prior(
        ["CHEBI", "MATERIALSMINE", "MATR", "MATRCOMPOUND", "MATRELEMENT", "NCIT"],
        ["material", "chemical", "polymer", "substance"],
        ["MESH", "ENVO", "REX"], ["taxonomy", "clinical disease", "unit"], ["NCBITAXON", "QUDT"],
    ),
    "packaging_component": _prior(
        ["FOODON", "ENVO", "CHEBI", "MATERIALSMINE"], ["packaging", "food packaging", "container", "material"],
        ["NCIT", "OBI", "MESH"], ["gene", "unit", "clinical disease", "organism"], ["QUDT", "NCBITAXON"],
    ),
    "data_object_or_format": _prior(
        ["EDAM", "IAO", "SIO"], ["data", "information", "format"],
        ["NCIT", "SWO"], ["food", "organism", "clinical disease"], ["FOODON", "NCBITAXON"],
    ),
    "semantic_web_concept": _prior(
        ["IAO", "SIO"], ["information", "semantic", "data"],
        ["NCIT", "EDAM", "OMV"], ["food", "organism"], ["FOODON", "NCBITAXON"],
    ),
    "statistical_or_risk_concept": _prior(
        ["STATO", "IAO", "SIO"], ["statistic", "risk", "information"],
        ["NCIT", "OBI", "MESH"], ["food", "organism"], ["FOODON", "NCBITAXON"],
    ),
    "food_processing_method": _prior(
        ["FOODON", "OBI", "NCIT"], ["food processing", "process", "treatment", "method"],
        ["MESH", "AGROVOC", "EFO"], ["unit", "organism", "gene"], ["QUDT", "NCBITAXON"],
    ),
    "experimental_condition": _prior(
        ["OBI", "EFO", "NCIT"], ["experimental condition", "condition", "process", "assay"],
        ["MESH", "PATO", "ENVO"], ["food", "organism", "gene"], ["FOODON", "NCBITAXON"],
    ),
    "laboratory_process": _prior(
        ["OBI", "EFO", "NCIT"], ["process", "laboratory", "technique", "method"],
        ["MESH", "BAO", "EDAM"], ["food", "organism", "unit"], ["FOODON", "NCBITAXON"],
    ),
    "analytical_measurement_property": _prior(
        ["OBI", "BAO", "STATO", "NCIT"], ["measurement", "assay", "statistic", "quality"],
        ["EFO", "PATO", "UO"], ["food", "organism", "taxonomy"], ["FOODON", "NCBITAXON"],
    ),
    "geospatial_statistical_region": _prior(
        ["GEONAMES"], ["region", "geographic", "administrative"],
        ["NCIT", "ENVO"], ["food", "gene", "clinical disease"], ["FOODON", "NCBITAXON"],
    ),
    "rdf_or_vocabulary_property": _prior(
        ["IAO", "SIO"], ["information", "semantic", "vocabulary", "property"],
        ["OMV", "NCIT"], ["food", "organism", "clinical disease"], ["FOODON", "NCBITAXON"],
    ),
    "gene_or_variant_symbol": _prior(
        ["GENEPIO", "SO", "EDAM", "NCIT"], ["gene", "genomic", "sequence feature", "amr"],
        ["MESH", "NCBITAXON", "OGG"], ["food", "unit", "clinical disease"], ["FOODON", "QUDT", "UO"],
    ),
    "cell_line": _prior(
        ["CLO", "BTO", "EFO", "OBI"], ["cell line", "cell", "material", "biomaterial"],
        ["NCIT", "CL", "MESH"], ["food", "unit"], ["FOODON", "QUDT"],
    ),
    "placeholder_or_status": _prior([], [], [], [], []),
    "ambiguous_common_word": _prior([], [], ["NCIT", "MESH"], [], []),
    "mixed_domain": _prior(["NCIT", "MESH"], [], [], [], []),
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TermCategoryResult:
    """Per-term categorization outcome (input to profiles and term-level policy)."""

    term: str
    primary_category: str = "mixed_domain"
    secondary_categories: List[str] = field(default_factory=list)
    confidence: float = 0.0
    evidence_terms: List[str] = field(default_factory=list)
    ambiguity_flags: List[str] = field(default_factory=list)
    is_placeholder: bool = False
    is_ambiguous: bool = False
    verification_policy: str = POLICY_STANDARD
    notes: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "primary_category": self.primary_category,
            "secondary_categories": list(self.secondary_categories),
            "confidence": round(float(self.confidence), 4),
            "evidence_terms": list(self.evidence_terms),
            "ambiguity_flags": list(self.ambiguity_flags),
            "is_placeholder": bool(self.is_placeholder),
            "is_ambiguous": bool(self.is_ambiguous),
            "verification_policy": self.verification_policy,
            "notes": self.notes,
        }


@dataclass
class _ProfileBase:
    term_count: int = 0
    dominant_categories: List[str] = field(default_factory=list)
    secondary_categories: List[str] = field(default_factory=list)
    category_confidence: float = 0.0
    category_evidence_terms: Dict[str, List[str]] = field(default_factory=dict)
    ambiguous_terms: List[str] = field(default_factory=list)
    placeholder_or_status_terms: List[str] = field(default_factory=list)
    likely_domain_mix: List[str] = field(default_factory=list)
    preferred_ontology_domains: List[str] = field(default_factory=list)
    penalized_ontology_domains: List[str] = field(default_factory=list)
    preferred_ontology_acronyms: List[str] = field(default_factory=list)
    fallback_ontology_acronyms: List[str] = field(default_factory=list)
    broad_fallback_allowed: bool = True
    category_notes: str = ""

    def _base_dict(self) -> Dict[str, Any]:
        return {
            "term_count": self.term_count,
            "dominant_categories": list(self.dominant_categories),
            "secondary_categories": list(self.secondary_categories),
            "category_confidence": round(float(self.category_confidence), 4),
            "category_evidence_terms": {k: list(v) for k, v in self.category_evidence_terms.items()},
            "ambiguous_terms": list(self.ambiguous_terms),
            "placeholder_or_status_terms": list(self.placeholder_or_status_terms),
            "likely_domain_mix": list(self.likely_domain_mix),
            "preferred_ontology_domains": list(self.preferred_ontology_domains),
            "penalized_ontology_domains": list(self.penalized_ontology_domains),
            "preferred_ontology_acronyms": list(self.preferred_ontology_acronyms),
            "fallback_ontology_acronyms": list(self.fallback_ontology_acronyms),
            "broad_fallback_allowed": bool(self.broad_fallback_allowed),
            "category_notes": self.category_notes,
        }


@dataclass
class TableContextProfile(_ProfileBase):
    table_id: str = "table"

    def as_dict(self) -> Dict[str, Any]:
        return {"table_id": self.table_id, **self._base_dict()}


@dataclass
class ChunkContextProfile(_ProfileBase):
    chunk_id: str = "chunk"
    table_id: str = "table"
    terms: List[str] = field(default_factory=list)
    row_indices: List[Any] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "table_id": self.table_id,
            "terms": list(self.terms),
            **self._base_dict(),
        }


@dataclass
class CategoryOntologySelection:
    category_id: str
    preferred_ontology_acronyms: List[str] = field(default_factory=list)
    preferred_ontology_domains: List[str] = field(default_factory=list)
    fallback_ontology_acronyms: List[str] = field(default_factory=list)
    excluded_or_penalized_ontology_acronyms: List[str] = field(default_factory=list)
    selection_reasons: Dict[str, str] = field(default_factory=dict)
    evidence_from_registry_fields: Dict[str, List[str]] = field(default_factory=dict)
    quality_scores: Dict[str, float] = field(default_factory=dict)
    suitability_scores: Dict[str, float] = field(default_factory=dict)
    selection_confidence: float = 0.0
    broad_fallback_allowed: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "category_id": self.category_id,
            "preferred_ontology_acronyms": list(self.preferred_ontology_acronyms),
            "preferred_ontology_domains": list(self.preferred_ontology_domains),
            "fallback_ontology_acronyms": list(self.fallback_ontology_acronyms),
            "excluded_or_penalized_ontology_acronyms": list(self.excluded_or_penalized_ontology_acronyms),
            "selection_reasons": dict(self.selection_reasons),
            "evidence_from_registry_fields": {k: list(v) for k, v in self.evidence_from_registry_fields.items()},
            "quality_scores": {k: round(float(v), 4) for k, v in self.quality_scores.items()},
            "suitability_scores": {k: round(float(v), 4) for k, v in self.suitability_scores.items()},
            "selection_confidence": round(float(self.selection_confidence), 4),
            "broad_fallback_allowed": bool(self.broad_fallback_allowed),
        }


# ---------------------------------------------------------------------------
# Phase 1 (part a) -- per-term categorization
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        return max(lo, min(hi, float(value)))
    except Exception:
        return lo


def categorize_term(
    term: str,
    definition: Optional[str] = None,
    *,
    table_context: Optional[str] = None,
    column_context: Optional[str] = None,
) -> TermCategoryResult:
    """Classify a single term into one of the spec categories using heuristics.

    Placeholder/status values and ambiguous common words are detected first and
    marked with the appropriate verification policy; otherwise the category is the
    highest-scoring signal set (term matches weigh double context matches)."""

    raw = str(term or "").strip()
    lowered = raw.lower()
    result = TermCategoryResult(term=raw)

    if not raw:
        result.primary_category = "placeholder_or_status"
        result.is_placeholder = True
        result.verification_policy = POLICY_NO_AUTO_VERIFY
        result.notes = "empty value"
        return result

    # --- placeholder / status (highest priority) ---------------------------
    if lowered in PLACEHOLDER_STATUS_VALUES:
        result.primary_category = "placeholder_or_status"
        result.is_placeholder = True
        result.verification_policy = POLICY_NO_AUTO_VERIFY
        result.ambiguity_flags = ["placeholder_or_status_value"]
        result.notes = "generic placeholder/status value"
        return result
    if lowered in PACKAGING_STATUS_VALUES:
        result.primary_category = "placeholder_or_status"
        result.is_placeholder = True
        result.verification_policy = POLICY_NO_AUTO_VERIFY
        result.ambiguity_flags = ["placeholder_or_status_value", "packaging_status"]
        result.notes = "packaging/boolean status value"
        return result

    term_text = _normalize_search_text(raw)
    # A non-camel-split lowercasing so acronyms like netCDF/geoTIFF/qPCR (which the
    # camelCase-splitting normalizer turns into "net cdf") still match token signals.
    term_alt = re.sub(r"[^a-z0-9]+", " ", raw.lower()).strip()
    ctx_text = _normalize_search_text(" ".join(filter(None, [definition, table_context, column_context])))

    # --- category signal scoring ------------------------------------------
    scores: Dict[str, float] = {}
    evidence: Dict[str, List[str]] = {}
    for category, signals in CATEGORY_SIGNALS.items():
        term_hits = [s for s in signals if _contains_signal(term_text, s) or _contains_signal(term_alt, s)]
        ctx_hits = [s for s in signals if s not in term_hits and _contains_signal(ctx_text, s)]
        weight = 2.0 * len(term_hits) + 1.0 * len(ctx_hits)
        if weight > 0:
            scores[category] = weight
            evidence[category] = _dedupe(term_hits + ctx_hits)

    # --- gene-symbol heuristic (single token, not a plain dictionary word) --
    # Only fires when no strong term-level category signal already exists, so real
    # words that happen to match the pattern (e.g. netCDF -> data_object) are not
    # mislabelled as genes.
    single_token = " " not in lowered and "/" not in lowered
    strong_existing = max(scores.values()) if scores else 0.0
    # blaCTX-M-15 etc. contain hyphens but are still single gene tokens.
    gene_token = (" " not in lowered) and _GENE_SYMBOL_RE.match(raw)
    if (
        gene_token
        and strong_existing < 2.0
        and lowered not in AMBIGUOUS_COMMON_WORDS
    ):
        scores["gene_or_variant_symbol"] = scores.get("gene_or_variant_symbol", 0.0) + 3.0
        evidence.setdefault("gene_or_variant_symbol", []).append("gene_symbol_pattern")

    # --- binomial organism heuristic (fallback; only when no strong signal fired) ------
    if (max(scores.values()) if scores else 0.0) < 2.0 and _BINOMIAL_RE.match(raw):
        scores["organism_taxon"] = scores.get("organism_taxon", 0.0) + 3.0
        evidence.setdefault("organism_taxon", []).append("binomial_nomenclature_pattern")

    # --- ambiguity flag ----------------------------------------------------
    is_ambiguous = lowered in AMBIGUOUS_COMMON_WORDS
    if is_ambiguous:
        result.is_ambiguous = True
        result.ambiguity_flags.append("ambiguous_common_word")

    if not scores:
        # No signal. Ambiguous words fall to their own category; otherwise mixed.
        result.primary_category = "ambiguous_common_word" if is_ambiguous else "mixed_domain"
        result.confidence = 0.20 if is_ambiguous else 0.0
        result.verification_policy = POLICY_REQUIRE_CONTEXT if is_ambiguous else POLICY_STANDARD
        result.notes = "no category signal detected"
        return result

    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    primary, primary_w = ordered[0]
    total = sum(scores.values())
    dominance = primary_w / total if total else 0.0
    confidence = _clamp(0.30 + 0.12 * primary_w + 0.25 * dominance)

    secondary = [cat for cat, w in ordered[1:] if w >= 1.0][:3]

    # An ambiguous common word with only a weak category signal stays ambiguous
    # (requires category context); a strong signal keeps the category but is flagged.
    if is_ambiguous and primary_w < 2.0:
        result.primary_category = "ambiguous_common_word"
        result.secondary_categories = [primary] + secondary
        result.confidence = min(confidence, 0.35)
        result.evidence_terms = evidence.get(primary, [])
        result.verification_policy = POLICY_REQUIRE_CONTEXT
        result.notes = "ambiguous common word; category requires context"
        return result

    result.primary_category = primary
    result.secondary_categories = secondary
    result.confidence = confidence
    result.evidence_terms = evidence.get(primary, [])
    result.verification_policy = POLICY_REQUIRE_CONTEXT if is_ambiguous else POLICY_STANDARD
    if is_ambiguous:
        result.notes = "category inferred but term is an ambiguous common word"
    return result


def _dedupe(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


# ---------------------------------------------------------------------------
# Phase 1 (part b) -- table / chunk aggregation
# ---------------------------------------------------------------------------

def _aggregate_profile(
    results: Sequence[TermCategoryResult],
    *,
    category_confidence_min: float,
    max_dominant: int = 3,
) -> Dict[str, Any]:
    """Aggregate per-term categorizations into the shared profile fields."""
    term_count = len(results)
    # Category tally uses confidence-weighted votes but excludes placeholder-only
    # and pure-ambiguous terms from the *dominant* signal (they carry no domain).
    votes: Counter = Counter()
    evidence: Dict[str, List[str]] = {}
    ambiguous_terms: List[str] = []
    placeholder_terms: List[str] = []
    informative = 0
    for res in results:
        if res.is_placeholder:
            placeholder_terms.append(res.term)
        if res.is_ambiguous:
            ambiguous_terms.append(res.term)
        cat = res.primary_category
        if cat in {"placeholder_or_status", "ambiguous_common_word", "mixed_domain"}:
            # Secondary categories of an ambiguous term still hint at the domain.
            for sec in res.secondary_categories[:1]:
                votes[sec] += 0.5 * max(res.confidence, 0.2)
                evidence.setdefault(sec, []).extend(res.evidence_terms[:2])
            continue
        informative += 1
        votes[cat] += max(res.confidence, 0.25)
        evidence.setdefault(cat, []).extend(res.evidence_terms[:3])
        for sec in res.secondary_categories[:2]:
            votes[sec] += 0.3

    ranked = votes.most_common()
    dominant = [cat for cat, _ in ranked[:max_dominant] if votes[cat] > 0]
    secondary = [cat for cat, _ in ranked[max_dominant : max_dominant + 4] if votes[cat] > 0]

    total_votes = sum(votes.values())
    top_share = (ranked[0][1] / total_votes) if (ranked and total_votes) else 0.0
    informative_share = (informative / term_count) if term_count else 0.0
    category_confidence = _clamp(0.5 * top_share + 0.5 * informative_share)

    # Domain mix: distinct informative categories present.
    domain_mix = [cat for cat, _ in ranked if votes[cat] > 0]
    is_mixed = len([c for c in domain_mix if votes[c] >= max(0.5, 0.25 * (ranked[0][1] if ranked else 1))]) >= 3

    # Union preferred/penalized from the dominant categories' priors. When category
    # confidence is low we still record preferences but the retrieval layer treats
    # them softly (broad fallback readily allowed).
    preferred_domains: List[str] = []
    penalized_domains: List[str] = []
    preferred_acronyms: List[str] = []
    fallback_acronyms: List[str] = []
    for cat in dominant:
        prior = CATEGORY_ONTOLOGY_PRIORS.get(cat, {})
        preferred_domains.extend(prior.get("preferred_domains", []))
        penalized_domains.extend(prior.get("penalized_domains", []))
        preferred_acronyms.extend(prior.get("preferred_acronyms", []))
        fallback_acronyms.extend(prior.get("fallback_acronyms", []))
    # Penalize only where not also preferred by another dominant category.
    preferred_set = {d.lower() for d in preferred_domains}
    penalized_domains = [d for d in penalized_domains if d.lower() not in preferred_set]

    all_placeholder = bool(term_count) and len(placeholder_terms) == term_count
    broad_fallback_allowed = not all_placeholder

    notes_bits: List[str] = []
    if is_mixed:
        notes_bits.append("mixed_domain chunk")
    if category_confidence < category_confidence_min:
        notes_bits.append("low category confidence; preferences applied softly")
    if all_placeholder:
        notes_bits.append("all placeholder/status; external verification blocked")

    return {
        "term_count": term_count,
        "dominant_categories": dominant or (["mixed_domain"] if term_count else []),
        "secondary_categories": secondary,
        "category_confidence": category_confidence,
        "category_evidence_terms": {k: _dedupe(v)[:8] for k, v in evidence.items()},
        "ambiguous_terms": _dedupe(ambiguous_terms),
        "placeholder_or_status_terms": _dedupe(placeholder_terms),
        "likely_domain_mix": domain_mix,
        "preferred_ontology_domains": _dedupe(preferred_domains),
        "penalized_ontology_domains": _dedupe(penalized_domains),
        "preferred_ontology_acronyms": _dedupe(preferred_acronyms),
        "fallback_ontology_acronyms": _dedupe(fallback_acronyms),
        "broad_fallback_allowed": broad_fallback_allowed,
        "category_notes": "; ".join(notes_bits),
    }


def _extract_records(
    input_table: Any,
    *,
    term_column: str = "Term",
    definition_column: str = "Definition",
) -> List[Dict[str, Any]]:
    """Normalize a DataFrame / list-of-dicts / list-of-(term, def) into records."""
    records: List[Dict[str, Any]] = []
    # pandas DataFrame duck-typing (avoid importing pandas here).
    if hasattr(input_table, "iterrows") and hasattr(input_table, "columns"):
        cols = list(getattr(input_table, "columns", []))
        tcol = term_column if term_column in cols else ("term" if "term" in cols else None)
        dcol = definition_column if definition_column in cols else ("definition" if "definition" in cols else None)
        for idx, row in input_table.iterrows():
            term = str(row.get(tcol, "") if tcol else "").strip()
            definition = str(row.get(dcol, "") if dcol else "").strip()
            records.append({"row_index": idx, "term": term, "definition": definition})
        return records
    for i, item in enumerate(input_table or []):
        if isinstance(item, dict):
            term = str(item.get(term_column, item.get("term", "")) or "").strip()
            definition = str(item.get(definition_column, item.get("definition", "")) or "").strip()
            row_index = item.get("row_index", i)
        elif isinstance(item, (list, tuple)):
            term = str(item[0] if len(item) > 0 else "").strip()
            definition = str(item[1] if len(item) > 1 else "").strip()
            row_index = i
        else:
            term = str(item or "").strip()
            definition = ""
            row_index = i
        records.append({"row_index": row_index, "term": term, "definition": definition})
    return records


def build_table_context_profile(
    input_table: Any,
    *,
    table_id: str = "table",
    table_context: Optional[str] = None,
    category_confidence_min: float = 0.35,
    term_column: str = "Term",
    definition_column: str = "Definition",
) -> TableContextProfile:
    """Phase 1: build a single semantic profile for the whole input table."""
    records = _extract_records(input_table, term_column=term_column, definition_column=definition_column)
    results = [
        categorize_term(r["term"], r["definition"], table_context=table_context or table_id)
        for r in records
        if r["term"]
    ]
    agg = _aggregate_profile(results, category_confidence_min=category_confidence_min)
    return TableContextProfile(table_id=str(table_id), **agg)


def build_chunk_context_profiles(
    input_table: Any,
    *,
    table_id: str = "table",
    table_context: Optional[str] = None,
    chunk_size: int = 30,
    strategy: str = "semantic",
    small_table_threshold: int = 12,
    category_confidence_min: float = 0.35,
    term_column: str = "Term",
    definition_column: str = "Definition",
) -> List[ChunkContextProfile]:
    """Phase 1: split the table into chunks and profile each chunk.

    Strategies:
      * ``global`` (or table at/under ``small_table_threshold``) -> one chunk.
      * ``fixed`` -> contiguous fixed-size chunks of ``chunk_size`` terms.
      * ``semantic`` -> group terms by primary category (mixed/ambiguous/placeholder
        collapse into a shared ``mixed_domain`` chunk), then cap each group at
        ``chunk_size``.
    """
    records = [r for r in _extract_records(input_table, term_column=term_column, definition_column=definition_column) if r["term"]]
    if not records:
        return []

    strategy = str(strategy or "semantic").strip().lower()
    if len(records) <= max(1, small_table_threshold) or strategy == "global":
        groups = [("chunk_global", records)]
    elif strategy == "fixed":
        size = max(1, int(chunk_size or 30))
        groups = [
            (f"chunk_{i // size:03d}", records[i : i + size])
            for i in range(0, len(records), size)
        ]
    else:  # semantic clustering by primary category
        by_cat: Dict[str, List[Dict[str, Any]]] = {}
        for rec in records:
            res = categorize_term(rec["term"], rec["definition"], table_context=table_context or table_id)
            rec["_category"] = res.primary_category
            key = res.primary_category
            if key in {"placeholder_or_status", "ambiguous_common_word"}:
                key = "mixed_domain"
            by_cat.setdefault(key, []).append(rec)
        size = max(1, int(chunk_size or 30))
        groups = []
        for cat in sorted(by_cat):
            bucket = by_cat[cat]
            if len(bucket) <= size:
                groups.append((f"chunk_{cat}", bucket))
            else:
                for i in range(0, len(bucket), size):
                    groups.append((f"chunk_{cat}_{i // size:02d}", bucket[i : i + size]))

    profiles: List[ChunkContextProfile] = []
    for chunk_id, bucket in groups:
        results = [
            categorize_term(r["term"], r["definition"], table_context=table_context or table_id)
            for r in bucket
        ]
        agg = _aggregate_profile(results, category_confidence_min=category_confidence_min)
        profiles.append(
            ChunkContextProfile(
                chunk_id=chunk_id,
                table_id=str(table_id),
                terms=[r["term"] for r in bucket],
                row_indices=[r["row_index"] for r in bucket],
                **agg,
            )
        )
    return profiles


# ---------------------------------------------------------------------------
# Phase 2 -- term profile WITH table/chunk context
# ---------------------------------------------------------------------------

def build_term_category_profile(
    term: str,
    definition: Optional[str] = None,
    *,
    table_context_profile: Optional[TableContextProfile] = None,
    chunk_context_profile: Optional[ChunkContextProfile] = None,
    column_context: Optional[str] = None,
    rdf_role: Optional[str] = None,
) -> Dict[str, Any]:
    """Phase 2: build the term-level profile, folding in table/chunk context.

    Reuses the heuristic :func:`build_term_context_profile` for base domain signals
    and enriches it with category context. The table/chunk profile is advisory: it
    does not override an obvious term-level category, but it can *break ties* and set
    ``preferred/penalized`` domains when the term signal alone is weak."""
    table_context = None
    chunk_categories: List[str] = []
    table_categories: List[str] = []
    if chunk_context_profile is not None:
        table_context = chunk_context_profile.table_id
        chunk_categories = list(chunk_context_profile.dominant_categories)
    if table_context_profile is not None:
        table_context = table_context or table_context_profile.table_id
        table_categories = list(table_context_profile.dominant_categories)

    base = build_term_context_profile(term, definition, table_context, column_context, rdf_role)
    cat = categorize_term(
        term,
        definition,
        table_context=table_context,
        column_context=column_context,
    )

    # Decide whether category context influenced the interpretation.
    category_context_used = False
    influence_reason = ""
    primary = cat.primary_category
    if cat.confidence < 0.45 and (chunk_categories or table_categories):
        # Weak term signal -> let the dominant chunk/table category inform it, but
        # never override a placeholder/ambiguous determination.
        ctx_pool = chunk_categories or table_categories
        if primary in {"mixed_domain"} and ctx_pool:
            primary = ctx_pool[0]
            category_context_used = True
            influence_reason = "weak term signal; adopted dominant chunk/table category"
        elif primary not in {"placeholder_or_status", "ambiguous_common_word"} and ctx_pool and primary not in ctx_pool:
            influence_reason = "term category retained; chunk context noted as secondary"

    prior = CATEGORY_ONTOLOGY_PRIORS.get(primary, {})
    preferred_domains = _dedupe(prior.get("preferred_domains", []) + list(base.preferred_ontology_domains))
    penalized_domains = _dedupe(prior.get("penalized_domains", []) + list(base.negative_domain_signals))

    positive_signals = _dedupe(cat.evidence_terms + list(base.positive_signals))
    negative_signals = _dedupe(list(base.negative_domain_signals))

    required_ctx = []
    if cat.is_ambiguous:
        required_ctx.append("explicit domain/definition confirming the intended sense")
    if primary == "placeholder_or_status":
        required_ctx.append("none: placeholder/status is not externally verified")

    return {
        "term": str(term or ""),
        "primary_term_type": primary,
        "secondary_term_types": list(cat.secondary_categories),
        "application_context": base.application_context,
        "table_context_categories": table_categories,
        "chunk_context_categories": chunk_categories,
        "positive_domain_signals": positive_signals,
        "negative_domain_signals": negative_signals,
        "preferred_ontology_domains": preferred_domains,
        "penalized_ontology_domains": penalized_domains,
        "required_context_for_verification": required_ctx,
        "verification_policy": cat.verification_policy,
        "ambiguity_flags": list(cat.ambiguity_flags),
        "category_confidence": round(float(cat.confidence), 4),
        "is_placeholder_or_status": bool(cat.is_placeholder),
        "category_context_used": bool(category_context_used),
        "category_context_influence_reason": influence_reason,
    }


# ---------------------------------------------------------------------------
# Phase 3 -- registry-derived category ontology selection
# ---------------------------------------------------------------------------

def _category_term_profile(
    category_id: str,
    evidence_terms: Sequence[str],
    preferred_domains: Sequence[str],
    penalized_domains: Sequence[str],
) -> TermContextProfile:
    """Build a synthetic TermContextProfile representing a *category* so the existing
    registry scorer (score_registry_ontology_candidate) can rank ontologies for it."""
    representative = " ".join(list(evidence_terms)[:8] + list(preferred_domains)[:6])
    return TermContextProfile(
        normalized_term=_normalize_search_text(representative),
        normalized_definition="",
        table_context=category_id,
        detected_entity_type=category_id,
        semantic_domain=category_id,
        positive_signals=_dedupe(list(preferred_domains) + list(evidence_terms)),
        negative_domain_signals=_dedupe(list(penalized_domains)),
        preferred_ontology_domains=_dedupe(list(preferred_domains)),
        disallowed_or_low_priority_domains=_dedupe(list(penalized_domains)),
    )


def _prefilter_registry_by_domains(
    registry_index: Dict[str, str],
    phrases: Sequence[str],
    *,
    limit: int = 40,
) -> List[str]:
    """Cheaply narrow the registry to acronyms whose metadata blob mentions any of
    the category's domain phrases; ranked by match count, capped at ``limit``."""
    norm_phrases = [p for p in (_normalize_search_text(x) for x in phrases) if p]
    hits: List[Tuple[str, int]] = []
    for acr, blob in registry_index.items():
        count = 0
        for phrase in norm_phrases:
            if not phrase:
                continue
            if " " in phrase:
                if phrase in blob:
                    count += 2
            elif phrase in blob.split():
                count += 1
        if count:
            hits.append((acr, count))
    hits.sort(key=lambda kv: kv[1], reverse=True)
    return [acr for acr, _ in hits[:limit]]


def select_category_ontologies_from_registry(
    category_profile: Dict[str, Any],
    *,
    registry_lookup: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    registry_index: Optional[Dict[str, str]] = None,
    curated: Optional[Dict[str, Any]] = None,
    max_primary_ontologies: int = 5,
    max_fallback_ontologies: int = 5,
    blocklist: Optional[Sequence[str]] = None,
    category_confidence_min: float = 0.35,
) -> CategoryOntologySelection:
    """Phase 3: choose a *preferred* ontology profile for a category from the registry.

    ``category_profile`` is a dict with at least ``category_id``/``primary_category``,
    ``category_evidence_terms`` and (optionally) ``category_confidence``. The result is
    a priority profile, never a hard allow-list: ``broad_fallback_allowed`` stays True
    unless the category is placeholder-only."""
    category_id = str(
        category_profile.get("category_id")
        or category_profile.get("primary_category")
        or "mixed_domain"
    )
    if registry_lookup is None:
        registry_lookup = lambda acr: get_ontology_context(acr, ensure=False)  # noqa: E731
    priors = CATEGORY_ONTOLOGY_PRIORS.get(category_id, CATEGORY_ONTOLOGY_PRIORS["mixed_domain"])
    preferred_curated = [a.upper() for a in priors.get("preferred_acronyms", [])]
    fallback_curated = [a.upper() for a in priors.get("fallback_acronyms", [])]
    preferred_domains = priors.get("preferred_domains", []) or list(category_profile.get("preferred_ontology_domains", []))
    penalized_domains = priors.get("penalized_domains", []) or list(category_profile.get("penalized_ontology_domains", []))
    penalized_acronyms = [a.upper() for a in priors.get("penalized_acronyms", [])]
    evidence_terms = []
    ev = category_profile.get("category_evidence_terms")
    if isinstance(ev, dict):
        for vals in ev.values():
            evidence_terms.extend(vals)
    elif isinstance(ev, (list, tuple)):
        evidence_terms = list(ev)

    category_confidence = float(category_profile.get("category_confidence", 0.5) or 0.0)
    block_set = {str(a).strip().upper() for a in (blocklist or [])}

    # Placeholder-only categories get no external preferences.
    if category_id == "placeholder_or_status":
        return CategoryOntologySelection(
            category_id=category_id,
            preferred_ontology_domains=[],
            selection_reasons={"policy": "placeholder/status values are not externally verified"},
            selection_confidence=0.0,
            broad_fallback_allowed=False,
        )

    profile = _category_term_profile(category_id, evidence_terms, preferred_domains, penalized_domains)
    curated_caps = curated if curated is not None else load_capability_profiles()

    search_space = set(preferred_curated) | set(fallback_curated)
    if registry_index:
        search_space |= set(
            _prefilter_registry_by_domains(registry_index, list(preferred_domains) + list(evidence_terms))
        )

    scored: List[Dict[str, Any]] = []
    for acr in sorted(search_space):
        entry: Dict[str, Any] = {}
        try:
            entry = registry_lookup(acr) or {}
        except Exception:
            entry = {}
        registry_found = bool(entry)
        cand = score_registry_ontology_candidate(
            acr,
            {**entry, "acronym": acr},
            profile,
            curated=curated_caps,
            configured_core={*preferred_curated},
            blocklist=block_set,
        )
        prior_boost = 0.18 if acr in preferred_curated else (0.06 if acr in fallback_curated else 0.0)
        effective = _clamp(cand.final_registry_suitability_score + prior_boost)
        scored.append(
            {
                "acronym": acr,
                "candidate": cand,
                "effective": effective,
                "registry_found": registry_found,
                "is_curated_preferred": acr in preferred_curated,
                "is_curated_fallback": acr in fallback_curated,
                "hard_excluded": bool(cand.hard_exclusion_reason),
                "entry_fields": [f for f in ("name", "description", "abstract", "categories", "categories_domains", "purpose_summary_auto", "keywords", "root_labels_sample", "curated_note") if entry.get(f)],
            }
        )

    # Curated preferred ontologies must remain available even when the local registry
    # has no entry for them (priority profile, not a hard allow-list).
    eligible = [s for s in scored if not s["hard_excluded"] or s["is_curated_preferred"]]
    eligible.sort(key=lambda s: (s["is_curated_preferred"], s["effective"]), reverse=True)

    preferred: List[str] = []
    for s in eligible:
        if s["acronym"] in preferred or s["acronym"] in penalized_acronyms:
            continue
        preferred.append(s["acronym"])
        if len(preferred) >= max(1, max_primary_ontologies):
            break
    # Guarantee curated priors are present (front-loaded) even if not registry-scored.
    for acr in preferred_curated:
        if acr not in preferred and len(preferred) < max(1, max_primary_ontologies):
            preferred.append(acr)

    fallback: List[str] = []
    for acr in fallback_curated + [s["acronym"] for s in eligible]:
        if acr in preferred or acr in fallback or acr in penalized_acronyms:
            continue
        fallback.append(acr)
        if len(fallback) >= max(1, max_fallback_ontologies):
            break

    reasons: Dict[str, str] = {}
    evidence_fields: Dict[str, List[str]] = {}
    quality_scores: Dict[str, float] = {}
    suitability_scores: Dict[str, float] = {}
    by_acr = {s["acronym"]: s for s in scored}
    for acr in preferred + fallback:
        s = by_acr.get(acr)
        if not s:
            reasons[acr] = "curated domain prior (not present in local registry)"
            continue
        cand = s["candidate"]
        reasons[acr] = cand.ranking_reason or ("curated prior" if s["is_curated_preferred"] else "registry evidence")
        evidence_fields[acr] = s["entry_fields"]
        quality_scores[acr] = float(cand.quality_score)
        suitability_scores[acr] = float(cand.final_registry_suitability_score)

    top_suit = max((suitability_scores.get(a, 0.0) for a in preferred), default=0.0)
    selection_confidence = _clamp(0.4 * category_confidence + 0.6 * top_suit) if preferred else 0.0

    excluded = _dedupe(penalized_acronyms + [s["acronym"] for s in scored if s["hard_excluded"] and not s["is_curated_preferred"]])

    return CategoryOntologySelection(
        category_id=category_id,
        preferred_ontology_acronyms=preferred,
        preferred_ontology_domains=_dedupe(list(preferred_domains)),
        fallback_ontology_acronyms=fallback,
        excluded_or_penalized_ontology_acronyms=excluded,
        selection_reasons=reasons,
        evidence_from_registry_fields=evidence_fields,
        quality_scores=quality_scores,
        suitability_scores=suitability_scores,
        selection_confidence=selection_confidence,
        broad_fallback_allowed=bool(category_profile.get("broad_fallback_allowed", True)),
    )


# ---------------------------------------------------------------------------
# broad_retrieval_registry_scored (Phase 2/4/5): registry as POST-retrieval
# candidate scoring context -- "can we trust THIS candidate's ontology for this
# term/category?" -- never as a hard pre-retrieval ontology filter.
# ---------------------------------------------------------------------------

# Term-type -> a small set of high-value core ontologies for OPTIONAL targeted
# fallback (only used when the broad pool is weak; never queried for every term).
TERM_TYPE_CORE_ONTOLOGIES: Dict[str, List[str]] = {
    "food_product": ["FOODON", "FOBI"],
    "chemical_substance": ["CHEBI"],
    "organism_taxon": ["NCBITAXON"],
    "unit_or_quantity": ["QUDT", "QUDT2", "UO", "OM", "UNITSONT"],
    "measurement_property": ["OBI", "BAO", "PATO", "UO"],
    "environmental_entity": ["ENVO"],
    "climate_variable": ["ENVO", "QUDT"],
    "assay_or_method": ["OBI", "EFO", "BAO", "NCIT"],
    "sequencing_or_bioinformatics_method": ["EDAM", "OBI", "GENEPIO"],
    "genomic_feature_or_gene": ["GENEPIO", "EDAM", "OBI", "SO"],
    "clinical_condition": ["SNOMEDCT", "MESH", "MEDDRA", "ICD10"],
    "symptom": ["SNOMEDCT", "MESH", "MEDDRA"],
    "material_substance": ["CHEBI", "MATERIALSMINE", "MATR", "MATRCOMPOUND", "NCIT", "MESH"],
    "toxin_or_contaminant": ["CHEBI", "ENVO"],
    "packaging_component": ["FOODON", "ENVO", "MATERIALSMINE"],
    "data_object_or_format": ["EDAM", "IAO", "SIO"],
    "semantic_web_concept": ["IAO", "SIO"],
    "statistical_or_risk_concept": ["STATO", "IAO", "SIO"],
    "lab_material_or_cell_line": ["OBI", "CLO", "BTO"],
    "food_processing_method": ["FOODON", "OBI"],
    "experimental_condition": ["OBI", "EFO"],
    "laboratory_process": ["OBI", "EFO"],
    "analytical_measurement_property": ["OBI", "BAO", "STATO"],
    "gene_or_variant_symbol": ["GENEPIO", "SO", "EDAM"],
    "cell_line": ["CLO", "BTO", "EFO"],
    # geospatial_statistical_region and rdf_or_vocabulary_property are handled by the
    # namespace resolvers (P5), not by BioPortal targeted fallback -> no core list.
}

# Broad/general biomedical ontologies: useful but attract a broadness penalty unless
# they are the curated authority for the term's category.
BROAD_ONTOLOGIES = {"NCIT", "MESH", "SNOMEDCT", "LOINC", "NCBITAXON", "OMIT", "MEDLINEPLUS"}

# Generic head-word labels: a candidate whose whole label is one of these (e.g.
# "natural fibre fabric" -> "Fabric", "surface swab" -> "Sample") is broader than a
# specific multi-word term and must not verify unless it is an exact match.
GENERIC_HEAD_LABELS = {
    "fabric", "material", "materials", "sample", "specimen", "process", "substance",
    "device", "method", "product", "compound", "entity", "region", "area", "unit",
    "object", "item", "structure", "agent", "system", "component", "molecule",
    "chemical", "organism", "concept", "measurement", "quantity", "property", "tool",
    "data", "format", "file", "container", "packaging", "assay", "test", "reagent",
}


def term_type_core_ontologies(category: str) -> List[str]:
    """Return the OPTIONAL targeted-core fallback ontologies for a term category."""
    return list(TERM_TYPE_CORE_ONTOLOGIES.get(str(category or ""), []))


# ---------------------------------------------------------------------------
# Namespace / vocabulary resolvers (P5): for non-BioPortal-native terms a direct
# namespace mapping is authoritative and must be preferred over BioPortal/Wikidata.
# ---------------------------------------------------------------------------

# Standard-namespace prefix table (a vocabulary-FAMILY registry, not a term dictionary).
# Any CURIE ``prefix:LocalName`` in a known family resolves generically -- this works for
# arbitrary properties/classes (skos:*, dcat:*, geo:*, qudt:* ...), not a hand-listed set.
NAMESPACE_PREFIXES: Dict[str, str] = {
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "dct": "http://purl.org/dc/terms/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "prov": "http://www.w3.org/ns/prov#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "geo": "http://www.opengis.net/ont/geosparql#",
    "gsp": "http://www.opengis.net/ont/geosparql#",
    "sf": "http://www.opengis.net/ont/sf#",
    "qudt": "http://qudt.org/schema/qudt/",
    "unit": "http://qudt.org/vocab/unit/",
    "om": "http://www.ontology-of-units-of-measure.org/resource/om-2/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "schema": "https://schema.org/",
    "void": "http://rdfs.org/ns/void#",
    "vann": "http://purl.org/vocab/vann/",
    "obo": "http://purl.obolibrary.org/obo/",
}

# A genuine CURIE: ``prefix:LocalName`` (deterministic RDF syntax, NOT a natural-language
# term). This is what the resolver recognises -- never a spelled-out phrasing.
_CURIE_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9]{0,15})\s*:\s*([A-Za-z][A-Za-z0-9_\-]*)\s*$")


def resolve_namespace_term(term: str, primary_term_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Resolve a genuine CURIE (``prefix:LocalName``) to its IRI via the standard
    namespace-prefix table -- generic vocabulary-family recognition, not a per-term
    dictionary. Returns None for anything that is not a CURIE in a known family, so a
    natural-language phrasing (e.g. "SKOS exactMatch", "WKT polygon") is NOT special-cased
    and instead goes through normal retrieval + adjudication."""
    match = _CURIE_RE.match(str(term or ""))
    if not match:
        return None
    prefix = match.group(1).lower()
    local = match.group(2)
    base = NAMESPACE_PREFIXES.get(prefix)
    if not base:
        return None
    return {
        "uri": base + local,
        "label": f"{prefix}:{local}",
        "namespace": prefix.upper(),
        "mapping_type": "exact",
        "resolver": "curie_namespace_resolver",
    }


def _registry_quality_score(registry_profile: Optional[Dict[str, Any]]) -> float:
    """A light 0..1 quality proxy from registry-metadata completeness (+ explicit
    quality fields when present). Not a match confidence -- a governance signal."""
    if not registry_profile:
        return 0.40
    explicit = registry_profile.get("quality_raw") or registry_profile.get("quality_score")
    if isinstance(explicit, (int, float)):
        return _clamp(float(explicit) if float(explicit) <= 1.0 else float(explicit) / 100.0)
    fields = ("name", "description", "abstract", "purpose_summary_auto", "categories",
              "categories_domains", "keywords", "root_labels_sample")
    present = sum(1 for f in fields if registry_profile.get(f))
    completeness = present / len(fields)
    status = str(registry_profile.get("status", "") or "").lower()
    status_bonus = 0.05 if status in {"ok", "active", "production"} else 0.0
    return _clamp(0.45 + 0.5 * completeness + status_bonus)


def _registry_domain_alignment(registry_profile: Optional[Dict[str, Any]], preferred_domains: Sequence[str]) -> float:
    """0..1 overlap between the ontology's declared registry domains/categories and the
    term category's preferred domains -- the registry-derived category alignment."""
    if not registry_profile or not preferred_domains:
        return 0.0
    parts: List[str] = []
    for field in ("name", "categories", "categories_domains", "keywords", "purpose_summary_auto", "root_labels_sample", "description"):
        val = registry_profile.get(field)
        if isinstance(val, list):
            parts.append(" ".join(str(x) for x in val))
        elif val:
            parts.append(str(val))
    blob = _normalize_search_text(" ".join(parts))
    if not blob:
        return 0.0
    hits = sum(1 for phrase in preferred_domains if _contains_signal(blob, phrase))
    return _clamp(hits / max(1, len(preferred_domains)))


def compute_ontology_suitability_score(
    term_profile: Dict[str, Any],
    candidate_ontology_acronym: str,
    registry_profile: Optional[Dict[str, Any]],
    *,
    candidate_label: str = "",
    term: str = "",
    candidate_definition: str = "",
    hard_domain_mismatch: bool = False,
) -> Dict[str, Any]:
    """Score whether a retrieved candidate's ontology can be TRUSTED for this term/
    category (Phase 4). Combines curated term-type authority priors, registry-derived
    category alignment and an ontology quality/governance signal, minus penalties.

    ``hard_domain_mismatch`` is passed in from the existing deterministic classifier
    (never inferred here) so obsolete/hard-mismatch/placeholder handling is preserved."""
    category = str(term_profile.get("primary_term_type") or term_profile.get("primary_category") or "mixed_domain")
    category_confidence = float(
        term_profile.get("term_type_confidence")
        or term_profile.get("category_confidence")
        or 0.0
    )
    ambiguity = term_profile.get("ambiguity_flags") or []
    is_ambiguous = "ambiguous_common_word" in ambiguity or category == "ambiguous_common_word"
    is_placeholder = category == "placeholder_or_status" or term_profile.get("verification_policy") == POLICY_NO_AUTO_VERIFY

    acr = str(candidate_ontology_acronym or "").strip().upper()
    priors = CATEGORY_ONTOLOGY_PRIORS.get(category, {})
    preferred = {a.upper() for a in priors.get("preferred_acronyms", [])}
    fallback = {a.upper() for a in priors.get("fallback_acronyms", [])}
    penalized = {a.upper() for a in priors.get("penalized_acronyms", [])}
    preferred_domains = priors.get("preferred_domains", []) or list(term_profile.get("preferred_ontology_domains", []))
    registry_found = bool(registry_profile)

    if acr in preferred:
        term_type_alignment, reason = 0.90, "preferred authority ontology for category"
    elif acr in fallback:
        term_type_alignment, reason = 0.70, "acceptable fallback ontology for category"
    elif acr in penalized:
        term_type_alignment, reason = 0.20, "penalized ontology for this category"
    else:
        term_type_alignment, reason = 0.50, "ontology is not a curated authority for this category"

    align = _registry_domain_alignment(registry_profile, preferred_domains)
    category_alignment = _clamp(0.40 + 0.60 * align) if registry_found else 0.40
    quality = _registry_quality_score(registry_profile)

    lbl = str(candidate_label or "").strip()
    lexical_exact = bool(lbl) and lbl.lower() == str(term or "").strip().lower()
    broad_ontology_penalty = 0.12 if (acr in BROAD_ONTOLOGIES and term_type_alignment < 0.90 and category_confidence >= 0.35) else 0.0
    generic_label_penalty = 0.05 if (lbl and len(lbl.split()) <= 1 and len(lbl) <= 3) else 0.0
    over_specific_candidate_penalty = 0.06 if (term and lbl and len(lbl.split()) >= len(str(term).split()) + 3) else 0.0
    exact_but_wrong_domain_penalty = 0.15 if (lexical_exact and acr in penalized) else 0.0

    # Granularity (P8): a candidate that is a generic head word / a single component of a
    # multi-word term is "broader than term"; a much longer label for a short term is
    # "over specific without context". Neither may verify unless it is an exact match.
    term_tokens = [t for t in _normalize_search_text(term).split() if t]
    label_tokens = [t for t in _normalize_search_text(lbl).split() if t]
    label_norm = lbl.strip().lower()
    candidate_broader_than_term = bool(
        not lexical_exact and label_tokens and (
            (label_norm in GENERIC_HEAD_LABELS and len(term_tokens) > len(label_tokens))
            or (len(label_tokens) == 1 and len(term_tokens) >= 2 and label_tokens[0] in set(term_tokens))
        )
    )
    candidate_over_specific_no_context = bool(
        not lexical_exact and term_tokens and label_tokens
        and len(label_tokens) >= len(term_tokens) + 2 and len(term_tokens) <= 2
    )

    ontology_suitability = _clamp(
        0.50 * term_type_alignment
        + 0.35 * category_alignment
        + 0.15 * quality
        - broad_ontology_penalty
        - exact_but_wrong_domain_penalty
    )
    if not registry_found:
        ontology_suitability = _clamp(ontology_suitability - 0.08)

    soft_domain_mismatch = (acr in penalized) or bool(term_profile.get("negative_ontology_hit"))

    return {
        "candidate_ontology_acronym": acr,
        "registry_profile_found": bool(registry_found),
        "ontology_suitability_score": round(float(ontology_suitability), 4),
        "ontology_quality_score": round(float(quality), 4),
        "category_alignment_score": round(float(category_alignment), 4),
        "term_type_alignment_score": round(float(term_type_alignment), 4),
        "hard_domain_mismatch": bool(hard_domain_mismatch),
        "soft_domain_mismatch": bool(soft_domain_mismatch),
        "broad_ontology_penalty": round(float(broad_ontology_penalty), 4),
        "exact_but_wrong_domain_penalty": round(float(exact_but_wrong_domain_penalty), 4),
        "generic_label_penalty": round(float(generic_label_penalty), 4),
        "over_specific_candidate_penalty": round(float(over_specific_candidate_penalty), 4),
        "candidate_broader_than_term": bool(candidate_broader_than_term),
        "candidate_over_specific_no_context": bool(candidate_over_specific_no_context),
        "placeholder_policy_block": bool(is_placeholder),
        "is_ambiguous_common_word": bool(is_ambiguous),
        "category_confidence": round(float(category_confidence), 4),
        "registry_context_reason": reason,
    }
