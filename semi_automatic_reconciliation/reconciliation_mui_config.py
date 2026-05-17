"""Static configuration for the semi-automatic reconciliation MUI bridge."""

from __future__ import annotations

try:
    from .reconciliation_core import CUSTOM_SPARQL_PROVIDER_NAME, DEFAULT_SPARQL_QUERY_TEMPLATE
except ImportError:  # pragma: no cover - direct script fallback
    from reconciliation_core import CUSTOM_SPARQL_PROVIDER_NAME, DEFAULT_SPARQL_QUERY_TEMPLATE

RECONCILIATION_MUI_ACTIVE_STAGE_KEY = "reconciliation_mui_active_stage"
RECONCILIATION_MUI_EVENT_NONCE_KEY = "reconciliation_mui_event_nonce"
RECONCILIATION_MUI_STATUS_MESSAGE_KEY = "reconciliation_mui_status_message"
RECONCILIATION_UPLOADED_SOURCE_SIGNATURE_KEY = "reconciliation_mui_uploaded_source_signature"

RECONCILIATION_STAGES = {"load", "configure", "run", "reconcile", "export"}
STANDARD_RECONCILIATION_PROVIDERS = [
    "Wikidata",
    "NCBI",
    "BioPortal",
    "OLS (EBI)",
    "AgroPortal",
    "EarthPortal",
    "SemLookP",
    "QUDT",
    "Local Ontology",
]
LOOKUP_ONTOLOGY_PROVIDERS = ["BioPortal", "OLS (EBI)", "SemLookP", "AgroPortal", "EarthPortal"]
SKOS_MATCH_TYPES = ["", "skos:exactMatch", "skos:closeMatch", "skos:broadMatch", "skos:narrowMatch", "skos:relatedMatch"]

ONTOLOGY_PROVIDER_CONFIG_KEYS = {
    "BioPortal": ("bioportal",),
    "OLS (EBI)": ("ols",),
    "SemLookP": ("semlookp",),
    "AgroPortal": ("agroportal",),
    "EarthPortal": ("earthportal",),
}

ONTOLOGY_FAVORITE_CONFIG_FIELDS = (
    "preferred_ontologies",
    "favorite_ontologies",
    "favourite_ontologies",
    "default_ontologies",
)

PROVIDER_TOOLTIPS = {
    "Wikidata": "A large, collaboratively edited multilingual knowledge graph. Good for general concepts, people, places and organizations.",
    "NCBI": "National Center for Biotechnology Information. Biomedical and genomic databases. Requires API key.",
    "BioPortal": "Stanford BioPortal biomedical ontology repository. Requires API key.",
    "OLS (EBI)": "Ontology Lookup Service from EMBL-EBI.",
    "SemLookP": "Semantic Lookup Platform for Life Sciences.",
    "AgroPortal": "Agricultural and food-domain ontology portal. Requires API key.",
    "EarthPortal": "Earth system and environmental science semantic artifact repository.",
    "QUDT": "Units of Measure, Quantity Kinds and Dimensions via SPARQL.",
    "Local Ontology": "Uploaded OWL/OBO/RDF/TTL/JSON-LD/CSV/TSV/XLSX resources indexed locally.",
    CUSTOM_SPARQL_PROVIDER_NAME: "Custom SPARQL endpoint configured by endpoint URL, query template and result variables.",
}


def build_default_reconciliation_state() -> dict[str, object]:
    return {
        "df": None,
        "suggestions": {},
        "selected_uris": {},
        "last_uploaded_filename": None,
        "provider_queue": [],
        "local_resources": [],
        "local_backend": "auto",
        "provider_status": {},
        "total_indices_to_process": [],
        "display_provider": None,
        "display_mixed_results": False,
        "provider_has_results": set(),
        "processing_active": False,
        "stop_processing_requested": False,
        "processed_terms_count": 0,
        "current_term_index_processing": 0,
        "custom_sparql_enabled": False,
        "custom_sparql_endpoint": "",
        "custom_sparql_query_template": DEFAULT_SPARQL_QUERY_TEMPLATE,
        "custom_sparql_var_uri": "uri",
        "custom_sparql_var_label": "label",
        "custom_sparql_var_description": "description",
        "csv_load_error_message": None,
        "data_source_message": None,
        "linked_preprocessed_data_df": None,
        "ncbi_selected_databases": ['taxonomy', 'bioproject', 'gene', 'protein', 'nuccore', 'biosample', 'sra', 'pubmed'],
        "custom_search_terms": {},
        "custom_search_results": {},
        "custom_search_summaries": {},
        "active_reconciliation_index": None,
        "matching_strategy_radio": "API Ranking",
        "suggestion_slider": 10,
        "levenshtein_threshold_slider": 0.7,
        "show_only_matched_terms": False,
        "show_only_unreconciled_terms": False,
        "items_per_page": 10,
        "current_page": 1,
        "skos_matching_enabled": False,
        "available_ontologies_by_provider": {},
        "selected_ontologies_by_provider": {},
        "ontology_loading_status": {},
        RECONCILIATION_MUI_ACTIVE_STAGE_KEY: "load",
    }
