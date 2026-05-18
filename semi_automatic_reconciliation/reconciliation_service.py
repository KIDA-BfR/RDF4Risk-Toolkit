"""Backend service for semi-automatic reconciliation.

This module owns the Python state and event handlers used by the Material UI
frontend through ``mui_backend_server.py``. It intentionally contains no
UI framework imports or rendering code; UI concerns live in the React app.
"""

from __future__ import annotations

import base64
import concurrent.futures
import io
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

try:
    from . import local_resource_provider
    from .provider_registry import get_all_providers, get_provider
    from .processing_service import fetch_suggestions_for_term_from_provider
    from .snapshot_utils import dataframe_records as _records, json_safe_value as _json_safe
    from .reconciliation_core import (
        NO_MATCH_URI,
        NO_MATCH_DISPLAY,
        CUSTOM_SPARQL_PROVIDER_NAME,
        CONFIG,
        USER_AGENT,
        format_suggestion_display,
        get_combined_and_sorted_suggestions,
    )
    from .reconciliation_mui_config import (
        DEFAULT_SPARQL_QUERY_TEMPLATE,
        LOOKUP_ONTOLOGY_PROVIDERS,
        ONTOLOGY_FAVORITE_CONFIG_FIELDS,
        ONTOLOGY_PROVIDER_CONFIG_KEYS,
        PROVIDER_TOOLTIPS,
        RECONCILIATION_MUI_ACTIVE_STAGE_KEY,
        RECONCILIATION_MUI_EVENT_NONCE_KEY,
        RECONCILIATION_MUI_STATUS_MESSAGE_KEY,
        RECONCILIATION_STAGES,
        RECONCILIATION_UPLOADED_SOURCE_SIGNATURE_KEY,
        SKOS_MATCH_TYPES,
        STANDARD_RECONCILIATION_PROVIDERS,
        build_default_reconciliation_state,
    )
    from .shared_table_io import (
        LEXICAL_MAPPING_JUSTIFICATION,
        LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION,
        REVIEW_MAPPING_JUSTIFICATION,
        SEMANTIC_MAPPING_JUSTIFICATION,
        REQUIRED_MATCHING_TABLE_COLUMNS,
        apply_mapping_justification_for_row,
        export_curated_sssom_table,
        finalize_accepted_results,
        prepare_loaded_matching_table,
    )
except ImportError:  # pragma: no cover - direct script fallback
    import local_resource_provider
    from provider_registry import get_all_providers, get_provider
    from processing_service import fetch_suggestions_for_term_from_provider
    from snapshot_utils import dataframe_records as _records, json_safe_value as _json_safe
    from reconciliation_core import (
        NO_MATCH_URI,
        NO_MATCH_DISPLAY,
        CUSTOM_SPARQL_PROVIDER_NAME,
        CONFIG,
        USER_AGENT,
        format_suggestion_display,
        get_combined_and_sorted_suggestions,
    )
    from reconciliation_mui_config import (
        DEFAULT_SPARQL_QUERY_TEMPLATE,
        LOOKUP_ONTOLOGY_PROVIDERS,
        ONTOLOGY_FAVORITE_CONFIG_FIELDS,
        ONTOLOGY_PROVIDER_CONFIG_KEYS,
        PROVIDER_TOOLTIPS,
        RECONCILIATION_MUI_ACTIVE_STAGE_KEY,
        RECONCILIATION_MUI_EVENT_NONCE_KEY,
        RECONCILIATION_MUI_STATUS_MESSAGE_KEY,
        RECONCILIATION_STAGES,
        RECONCILIATION_UPLOADED_SOURCE_SIGNATURE_KEY,
        SKOS_MATCH_TYPES,
        STANDARD_RECONCILIATION_PROVIDERS,
        build_default_reconciliation_state,
    )
    from shared_table_io import (
        LEXICAL_MAPPING_JUSTIFICATION,
        LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION,
        REVIEW_MAPPING_JUSTIFICATION,
        SEMANTIC_MAPPING_JUSTIFICATION,
        REQUIRED_MATCHING_TABLE_COLUMNS,
        apply_mapping_justification_for_row,
        export_curated_sssom_table,
        finalize_accepted_results,
        prepare_loaded_matching_table,
    )

logger = logging.getLogger(__name__)

STATE: dict[str, object] = {}


def _prefill_best_matches_for_backend() -> None:
    """Apply best candidate suggestions to the backend dataframe without UI rendering state."""
    df = STATE.get("df")
    if not isinstance(df, pd.DataFrame):
        return
    display_mode = _current_reconciliation_display_mode()
    if not display_mode:
        return
    for idx in STATE.get("total_indices_to_process", list(df.index)):
        if idx not in df.index:
            continue
        current_uri = str(df.loc[idx, "URI"]).strip() if "URI" in df.columns else ""
        if current_uri and current_uri != NO_MATCH_URI:
            continue
        term = str(df.loc[idx, "Term"]).strip() if "Term" in df.columns else str(df.loc[idx, "subject_label"]).strip()
        options = _suggestion_options_for_row(idx, term, display_mode)
        best_option = next((option for option in options if option.get("uri") and option.get("uri") != NO_MATCH_URI), None)
        if best_option:
            _apply_reconciliation_selection(idx, best_option)

# ---------------------------------------------------------------------------
# Material-UI reconciliation application bridge
# ---------------------------------------------------------------------------
def _initialize_reconciliation_mui_state():
    for key, default in build_default_reconciliation_state().items():
        if key not in STATE:
            STATE[key] = default


def _read_matching_table_payload(event: dict) -> tuple[pd.DataFrame, str]:
    filename = str(event.get("filename", "uploaded_matching_table.csv") or "uploaded_matching_table.csv")
    content_base64 = event.get("content_base64")
    content_text = event.get("content")
    suffix = os.path.splitext(filename)[1].lower()
    if content_base64:
        payload = base64.b64decode(str(content_base64))
    elif isinstance(content_text, str):
        payload = content_text.encode("utf-8")
    else:
        raise ValueError("Uploaded file payload is empty.")

    if suffix == ".csv" or not suffix:
        last_exception = None
        for sep in [",", ";", "\t"]:
            try:
                candidate = pd.read_csv(io.BytesIO(payload), sep=sep, encoding="utf-8", skipinitialspace=True)
                if candidate.shape[1] > 1:
                    return candidate.fillna(""), filename
                last_exception = ValueError(f"Only one column detected with separator {sep!r}.")
            except Exception as exc:
                last_exception = exc
        try:
            return pd.read_csv(io.BytesIO(payload), sep=None, engine="python", skipinitialspace=True).fillna(""), filename
        except Exception as exc:
            raise ValueError(f"Failed to parse CSV. Last error: {last_exception or exc}") from exc
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(io.BytesIO(payload), engine="openpyxl" if suffix == ".xlsx" else None).fillna(""), filename
    raise ValueError(f"Unsupported file type: {suffix}. Please upload CSV, XLSX, or XLS.")


def _reset_reconciliation_state_and_load_df(df_to_load: pd.DataFrame, source_name_msg: str, is_from_shared_generator: bool = False) -> bool:
    STATE["suggestions"] = {}
    STATE["selected_uris"] = {}
    STATE["custom_search_results"] = {}
    STATE["custom_search_summaries"] = {}
    STATE["provider_queue"] = []
    STATE["provider_status"] = {}
    STATE["display_provider"] = None
    STATE["display_mixed_results"] = False
    STATE["provider_has_results"] = set()
    STATE["processing_active"] = False
    STATE["csv_load_error_message"] = None
    STATE["stop_processing_requested"] = False
    STATE["processed_terms_count"] = 0
    STATE["current_term_index_processing"] = 0
    STATE["active_reconciliation_index"] = None

    STATE["df"] = df_to_load.copy().fillna("")
    STATE["last_uploaded_filename"] = source_name_msg
    missing_cols = [col for col in REQUIRED_MATCHING_TABLE_COLUMNS if col not in STATE["df"].columns]
    if missing_cols:
        STATE["csv_load_error_message"] = f"Loaded data is missing required columns: {', '.join(missing_cols)}"
        STATE["df"] = None
        return False

    (
        STATE["df"],
        STATE["total_indices_to_process"],
        all_terms_list,
    ) = prepare_loaded_matching_table(STATE["df"], NO_MATCH_URI)
    STATE["data_source_message"] = f"Data successfully loaded from: {source_name_msg}."
    STATE["all_terms_for_reconciliation"] = all_terms_list if "Term" in STATE["df"].columns else []
    if not is_from_shared_generator:
        STATE["linked_preprocessed_data_df"] = None
    return True


def _available_reconciliation_providers() -> list[str]:
    registered = get_all_providers()
    providers = [provider for provider in STANDARD_RECONCILIATION_PROVIDERS if provider in registered]
    providers.extend(
        provider
        for provider in sorted(registered)
        if provider not in providers and provider != CUSTOM_SPARQL_PROVIDER_NAME
    )
    if STATE.get("custom_sparql_enabled") and CUSTOM_SPARQL_PROVIDER_NAME not in providers:
        providers.append(CUSTOM_SPARQL_PROVIDER_NAME)
    return providers


def _normalize_ontology_acronym(value: object) -> str:
    return str(value or "").strip().upper()


def _split_configured_ontology_values(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = value.replace(";", ",").split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = [value]
    return [normalized for item in raw_values if (normalized := _normalize_ontology_acronym(item))]


def _configured_favorite_ontologies(provider_name: str) -> list[str]:
    """Return provider-specific favorites from config.yaml, normalized for UI matching."""
    favorites: list[str] = []
    if not isinstance(CONFIG, dict):
        return favorites

    for config_key in ONTOLOGY_PROVIDER_CONFIG_KEYS.get(provider_name, ()):  # provider-level config, e.g. agroportal.preferred_ontologies
        provider_config = CONFIG.get(config_key, {})
        if not isinstance(provider_config, dict):
            continue
        for field_name in ONTOLOGY_FAVORITE_CONFIG_FIELDS:
            favorites.extend(_split_configured_ontology_values(provider_config.get(field_name)))

    # BioPortal-backed agent settings are also useful defaults for BioPortal ontology filters.
    if provider_name == "BioPortal":
        agent_config = CONFIG.get("agent_reconciliation", {})
        if isinstance(agent_config, dict):
            for field_name in ("bioportal_agent_ontologies", "trusted_ontologies"):
                favorites.extend(_split_configured_ontology_values(agent_config.get(field_name)))

    return list(dict.fromkeys(favorites))


def _default_selected_ontologies_for_provider(provider_name: str, available_ontologies: list[str]) -> list[str]:
    available_normalized = {_normalize_ontology_acronym(ontology): ontology for ontology in available_ontologies}
    selected = []
    for favorite in _configured_favorite_ontologies(provider_name):
        if favorite in available_normalized:
            selected.append(available_normalized[favorite])
    return selected


def _missing_provider_configurations(provider_queue: list[str]) -> list[str]:
    missing = []
    if CONFIG:
        for provider_name in provider_queue:
            if provider_name == "NCBI" and not CONFIG.get("ncbi", {}).get("api_key"):
                missing.append("NCBI API Key")
            elif provider_name == "BioPortal" and not CONFIG.get("bioportal", {}).get("api_key"):
                missing.append("BioPortal API Key")
            elif provider_name == "AgroPortal" and not CONFIG.get("agroportal", {}).get("api_key"):
                missing.append("AgroPortal API Key")
            elif provider_name == "EarthPortal" and not CONFIG.get("earthportal", {}).get("api_key"):
                missing.append("EarthPortal API Key")
            elif provider_name == "GeoNames":
                geonames_user = CONFIG.get("geonames", {}).get("username")
                if not geonames_user or geonames_user == "YourUsername":
                    missing.append("GeoNames Username")
    return missing


def _ensure_ontology_options_for_queue():
    selected_lookup_providers = [
        provider_name
        for provider_name in STATE.get("provider_queue", [])
        if provider_name in LOOKUP_ONTOLOGY_PROVIDERS
    ]
    selected_lookup_set = set(selected_lookup_providers)
    STATE["selected_ontologies_by_provider"] = {
        provider_name: selections
        for provider_name, selections in STATE.get("selected_ontologies_by_provider", {}).items()
        if provider_name in selected_lookup_set
    }

    for provider_name in selected_lookup_providers:
        if provider_name not in LOOKUP_ONTOLOGY_PROVIDERS:
            continue
        if STATE.get("ontology_loading_status", {}).get(provider_name) in {"loaded", "loading", "error"}:
            continue
        STATE["ontology_loading_status"][provider_name] = "loading"
        try:
            provider = get_provider(provider_name)
            ontologies_list = provider.get_available_ontologies(USER_AGENT, **provider.build_ontology_kwargs(CONFIG))
            available_ontologies = sorted([_normalize_ontology_acronym(o) for o in ontologies_list if _normalize_ontology_acronym(o)])
            STATE["available_ontologies_by_provider"][provider_name] = available_ontologies
            STATE["ontology_loading_status"][provider_name] = "loaded"
            if provider_name not in STATE["selected_ontologies_by_provider"]:
                STATE["selected_ontologies_by_provider"][provider_name] = _default_selected_ontologies_for_provider(
                    provider_name,
                    available_ontologies,
                )
        except Exception as exc:
            STATE["ontology_loading_status"][provider_name] = "error"
            STATE["available_ontologies_by_provider"][provider_name] = []
            logger.error("Error loading ontologies for %s: %s", provider_name, exc, exc_info=True)


def _suggestion_options_for_row(row_index, term: str, display_mode: str) -> list[dict[str, object]]:
    options = [{"display": NO_MATCH_DISPLAY, "uri": NO_MATCH_URI, "source": "", "label": "", "description": ""}]
    selected_ontologies = STATE.get("selected_ontologies_by_provider", {})
    if display_mode == "Mixed Results":
        all_suggs_for_term = STATE.get("suggestions", {}).get(row_index, {})
        processed_suggestions = get_combined_and_sorted_suggestions(
            term,
            all_suggs_for_term,
            STATE.get("suggestion_slider", 10),
            STATE.get("matching_strategy_radio"),
            selected_ontologies,
        )
    else:
        processed_suggestions = STATE.get("suggestions", {}).get(row_index, {}).get(display_mode, []) or []

    seen_displays = {NO_MATCH_DISPLAY}
    for sugg in processed_suggestions:
        if not isinstance(sugg, dict):
            continue
        s_uri = sugg.get("uri")
        s_label = sugg.get("label")
        if not s_uri or not s_label:
            continue
        display_text = format_suggestion_display(sugg, STATE.get("matching_strategy_radio"))
        if display_text in seen_displays:
            continue
        seen_displays.add(display_text)
        options.append(
            {
                "display": display_text,
                "uri": s_uri,
                "source": sugg.get("source_provider") or sugg.get("db") or sugg.get("ontology") or sugg.get("source_db") or display_mode,
                "label": s_label,
                "description": sugg.get("description", ""),
                "levenshtein_score": sugg.get("levenshtein_score"),
                "raw": sugg,
            }
        )
    return options


def _custom_search_summary_for_row(row_index, display_mode: str | None) -> dict[str, object] | None:
    summaries = STATE.get("custom_search_summaries", {})
    if not isinstance(summaries, dict):
        return None
    summary = summaries.get((row_index, display_mode or "Mixed Results", "mui_custom_search"))
    if isinstance(summary, dict):
        return summary
    return None


def _current_reconciliation_display_mode() -> str | None:
    if STATE.get("display_mixed_results"):
        return "Mixed Results"
    if STATE.get("display_provider"):
        return str(STATE.get("display_provider"))
    return None


def _build_reconciliation_rows(limit: int = 250) -> dict[str, object]:
    df = STATE.get("df")
    display_mode = _current_reconciliation_display_mode()
    if not isinstance(df, pd.DataFrame) or df.empty or not display_mode:
        return {"display_mode": display_mode, "rows": [], "total_rows": 0, "page": 1, "total_pages": 1}

    all_indices = list(df.index)
    indices = []
    if STATE.get("show_only_matched_terms", False):
        for idx in all_indices:
            if display_mode == "Mixed Results":
                all_suggs = STATE.get("suggestions", {}).get(idx, {})
                if any(s for s_list in all_suggs.values() if s_list is not None for s in s_list if s is not None):
                    indices.append(idx)
            elif STATE.get("suggestions", {}).get(idx, {}).get(display_mode, []):
                indices.append(idx)
    else:
        indices = all_indices

    if STATE.get("show_only_unreconciled_terms", False):
        filtered = []
        for idx in indices:
            current_uri = str(df.loc[idx, "URI"]).strip() if "URI" in df.columns else ""
            current_match_type = str(df.loc[idx, "predicate_id"]).strip() if "predicate_id" in df.columns else ""
            is_uri_unreconciled = (not current_uri or current_uri == NO_MATCH_URI)
            is_skos_unreconciled = bool(STATE.get("skos_matching_enabled") and not current_match_type)
            if is_uri_unreconciled or is_skos_unreconciled:
                filtered.append(idx)
        indices = filtered

    items_per_page = max(1, int(STATE.get("items_per_page", 10) or 10))
    total_pages = max(1, (len(indices) + items_per_page - 1) // items_per_page)
    current_page = min(max(1, int(STATE.get("current_page", 1) or 1)), total_pages)
    STATE["current_page"] = current_page
    page_indices = indices[(current_page - 1) * items_per_page : current_page * items_per_page]
    rows = []
    for idx in page_indices[:limit]:
        term = str(df.loc[idx, "Term"]).strip() if "Term" in df.columns else str(df.loc[idx, "subject_label"]).strip()
        current_uri = str(df.loc[idx, "URI"]).strip() if "URI" in df.columns else str(df.loc[idx, "object_id"]).strip()
        current_display = str(df.loc[idx, "Confirmed Display String"]).strip() if "Confirmed Display String" in df.columns else ""
        current_source = str(df.loc[idx, "Source Provider"]).strip() if "Source Provider" in df.columns else ""
        match_type = str(df.loc[idx, "predicate_id"]).strip() if "predicate_id" in df.columns else ""
        options = _suggestion_options_for_row(idx, term, display_mode)
        selected_display = NO_MATCH_DISPLAY
        if current_display and current_display != NO_MATCH_DISPLAY:
            selected_display = current_display
            if all(option["display"] != current_display for option in options):
                options.append({"display": current_display, "uri": current_uri, "source": current_source, "label": current_display, "description": ""})
        elif current_uri and current_uri != NO_MATCH_URI:
            selected_display = next((o["display"] for o in options if o.get("uri") == current_uri and o.get("source") == current_source), f"{current_uri} (from {current_source or 'previous selection'})")
            if all(option["display"] != selected_display for option in options):
                options.append({"display": selected_display, "uri": current_uri, "source": current_source, "label": selected_display, "description": ""})
        rows.append(
            {
                "row_index": idx,
                "term": term,
                "subject_label": str(df.loc[idx, "subject_label"]).strip() if "subject_label" in df.columns else term,
                "current_uri": "" if current_uri == NO_MATCH_URI else current_uri,
                "raw_uri": current_uri,
                "object_label": str(df.loc[idx, "object_label"]).strip() if "object_label" in df.columns else "",
                "source_provider": current_source,
                "match_type": match_type,
                "mapping_justification": str(df.loc[idx, "mapping_justification"]).strip() if "mapping_justification" in df.columns else "",
                "selected_display": selected_display,
                "options": options,
                "has_suggestions": len(options) > 1,
                "custom_search_summary": _custom_search_summary_for_row(idx, display_mode),
            }
        )
    return {"display_mode": display_mode, "rows": rows, "total_rows": len(indices), "page": current_page, "total_pages": total_pages}


def _build_provider_status_snapshot() -> list[dict[str, object]]:
    provider_order = {name: i for i, name in enumerate(STATE.get("provider_queue", []))}
    names = sorted(STATE.get("provider_status", {}).keys(), key=lambda name: (provider_order.get(name, float("inf")), name))
    return [
        {
            "name": name,
            "position": provider_order.get(name, 0) + 1,
            "status": STATE.get("provider_status", {}).get(name, {}).get("status", "pending"),
            "results_count": STATE.get("provider_status", {}).get(name, {}).get("results_count", 0),
            "error_msg": STATE.get("provider_status", {}).get(name, {}).get("error_msg", ""),
            "has_results": name in STATE.get("provider_has_results", set()),
        }
        for name in names
    ]


def _download_csv_payload(df: pd.DataFrame | None) -> str:
    if not isinstance(df, pd.DataFrame):
        return ""
    return df.to_csv(index=False)


def _build_reconciliation_mui_snapshot() -> dict[str, object]:
    df = STATE.get("df")
    shared_df = STATE.get("shared_matching_table")
    curated_columns = ["subject_label", "object_id", "predicate_id", "object_label", "mapping_justification"]
    provider_context_columns = ["subject_label", "object_id", "Source Provider", "Provider Term", "Provider Description", "Confirmed Display String", "comment"]
    curated_preview = pd.DataFrame()
    provider_preview = pd.DataFrame()
    if isinstance(df, pd.DataFrame):
        display_copy = df.copy()
        if "URI" in display_copy.columns:
            display_copy["URI"] = display_copy["URI"].replace(NO_MATCH_URI, "")
        curated_preview = display_copy[[col for col in curated_columns if col in display_copy.columns]].copy()
        provider_preview = display_copy[[col for col in provider_context_columns if col in display_copy.columns]].copy()

    sssom_df = export_curated_sssom_table(df.copy()) if isinstance(df, pd.DataFrame) else pd.DataFrame()
    candidate_review_df = pd.DataFrame()
    if isinstance(df, pd.DataFrame):
        candidate_review_columns = [
            "subject_id", "subject_label", "predicate_id", "object_id", "object_label", "mapping_justification",
            "Source Provider", "Provider Term", "Provider Description", "Confirmed Display String", "confidence", "comment",
            "mapping_provider", "object_source", "mapping_tool", "match_string", "semantic_similarity_score",
            "semantic_similarity_measure", "Review Status",
        ]
        candidate_review_df = df[[col for col in candidate_review_columns if col in df.columns]].copy()
        if "object_id" in candidate_review_df.columns:
            candidate_review_df["object_id"] = candidate_review_df["object_id"].replace(NO_MATCH_URI, "")

    total_terms = len(STATE.get("total_indices_to_process", []))
    processed_terms = int(STATE.get("processed_terms_count", 0) or 0)
    progress = min(100, round((processed_terms / max(total_terms, 1)) * 100)) if total_terms else 0
    missing_config_alerts = []
    if CONFIG:
        if not CONFIG.get("ncbi", {}).get("api_key"):
            missing_config_alerts.append("NCBI API Key")
        if not CONFIG.get("bioportal", {}).get("api_key"):
            missing_config_alerts.append("BioPortal API Key")
        if not CONFIG.get("agroportal", {}).get("api_key"):
            missing_config_alerts.append("AgroPortal API Key")

    original_filename = STATE.get("last_uploaded_filename") or "data.csv"
    base_filename = os.path.splitext(str(original_filename))[0]
    return {
        "active_stage": STATE.get(RECONCILIATION_MUI_ACTIVE_STAGE_KEY, "load"),
        "statusMessage": STATE.get(RECONCILIATION_MUI_STATUS_MESSAGE_KEY),
        "data": {
            "has_table": isinstance(df, pd.DataFrame),
            "rows": len(df) if isinstance(df, pd.DataFrame) else 0,
            "columns": len(df.columns) if isinstance(df, pd.DataFrame) else 0,
            "filename": STATE.get("last_uploaded_filename") or "",
            "source_message": STATE.get("data_source_message") or "",
            "shared_table_available": isinstance(shared_df, pd.DataFrame) and not shared_df.empty,
            "shared_rows": len(shared_df) if isinstance(shared_df, pd.DataFrame) else 0,
            "required_columns_detected": bool(isinstance(df, pd.DataFrame) and all(col in df.columns for col in REQUIRED_MATCHING_TABLE_COLUMNS)),
            "total_terms": total_terms,
            "curated_preview": _records(curated_preview, limit=20),
            "provider_context_preview": _records(provider_preview, limit=20),
        },
        "config": {
            "available_providers": _available_reconciliation_providers(),
            "provider_tooltips": PROVIDER_TOOLTIPS,
            "provider_queue": STATE.get("provider_queue", []),
            "provider_status": _build_provider_status_snapshot(),
            "provider_has_results": sorted(list(STATE.get("provider_has_results", set()))),
            "display_provider": STATE.get("display_provider"),
            "display_mixed_results": bool(STATE.get("display_mixed_results", False)),
            "custom_sparql_enabled": bool(STATE.get("custom_sparql_enabled", False)),
            "custom_sparql_endpoint": STATE.get("custom_sparql_endpoint", ""),
            "custom_sparql_query_template": STATE.get("custom_sparql_query_template", DEFAULT_SPARQL_QUERY_TEMPLATE),
            "custom_sparql_var_uri": STATE.get("custom_sparql_var_uri", "uri"),
            "custom_sparql_var_label": STATE.get("custom_sparql_var_label", "label"),
            "custom_sparql_var_description": STATE.get("custom_sparql_var_description", "description"),
            "ncbi_all_databases": ['taxonomy', 'bioproject', 'gene', 'protein', 'nuccore', 'biosample', 'sra', 'pubmed', 'assembly', 'blastdbinfo', 'books', 'cdd', 'clinvar', 'dbgap', 'domains', 'gap', 'gapplus', 'gds', 'geoprofiles', 'homologene', 'medgen', 'mesh', 'ncv', 'nlmcatalog', 'omim', 'pmc', 'popset', 'probe', 'proteinclusters', 'pubchem-compound', 'pubchem-substance', 'pubchem-assay', 'snp', 'structure', 'unigene', 'unists'],
            "ncbi_selected_databases": STATE.get("ncbi_selected_databases", []),
            "local_backend": STATE.get("local_backend", "auto"),
            "local_resources": [
                {"name": res.get("name"), "backend": res.get("backend"), "entities": len(getattr(res.get("index"), "entities", [])), "parse_backend": getattr(res.get("index"), "parse_backend", "")}
                for res in STATE.get("local_resources", [])
                if isinstance(res, dict)
            ],
            "ontology_loading_status": STATE.get("ontology_loading_status", {}),
            "available_ontologies_by_provider": STATE.get("available_ontologies_by_provider", {}),
            "selected_ontologies_by_provider": STATE.get("selected_ontologies_by_provider", {}),
            "matching_strategy": STATE.get("matching_strategy_radio", "API Ranking"),
            "suggestion_slider": STATE.get("suggestion_slider", 10),
            "levenshtein_threshold": STATE.get("levenshtein_threshold_slider", 0.7),
            "show_only_matched_terms": bool(STATE.get("show_only_matched_terms", False)),
            "show_only_unreconciled_terms": bool(STATE.get("show_only_unreconciled_terms", False)),
            "items_per_page": STATE.get("items_per_page", 10),
            "skos_matching_enabled": bool(STATE.get("skos_matching_enabled", False)),
            "missing_config_alerts": missing_config_alerts,
        },
        "run": {
            "processing_active": bool(STATE.get("processing_active", False)),
            "processed_terms": processed_terms,
            "total_terms": total_terms,
            "progress": progress,
            "current_term_index": STATE.get("current_term_index_processing", 0),
            "can_start": bool(isinstance(df, pd.DataFrame) and total_terms > 0 and STATE.get("provider_queue") and not _missing_provider_configurations(STATE.get("provider_queue", []))),
            "missing_start_configs": _missing_provider_configurations(STATE.get("provider_queue", [])),
        },
        "reconciliation": _build_reconciliation_rows(),
        "downloads": {
            "sssom_csv": _download_csv_payload(sssom_df),
            "sssom_filename": f"{base_filename}_sssom_curated.csv",
            "candidate_review_csv": _download_csv_payload(candidate_review_df),
            "candidate_review_filename": f"{base_filename}_candidate_review.csv",
        },
    }


def _apply_provider_queue(provider_queue: list[str]):
    available = set(_available_reconciliation_providers())
    selected = [str(provider) for provider in provider_queue if str(provider) in available]
    STATE["provider_queue"] = selected
    STATE["provider_status"] = {}
    for provider in selected:
        STATE["provider_status"][provider] = {"status": "pending", "results_count": 0, "error_msg": "", "progress": 0.0, "processed_indices": set()}
    STATE["display_provider"] = None
    STATE["display_mixed_results"] = False
    _ensure_ontology_options_for_queue()


def _process_reconciliation_queue():
    df = STATE.get("df")
    if not isinstance(df, pd.DataFrame):
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": "Cannot start processing: no table is loaded."}
        return
    missing_configs = _missing_provider_configurations(STATE.get("provider_queue", []))
    if missing_configs:
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": f"Cannot start: missing configuration values: {', '.join(missing_configs)}"}
        return
    total_indices = list(df[(pd.isnull(df["URI"]) | (df["URI"] == "") | (df["URI"] == NO_MATCH_URI))].index)
    STATE["total_indices_to_process"] = total_indices
    if not total_indices:
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "info", "text": "All terms already have URIs. Nothing to process."}
        return
    provider_queue = list(STATE.get("provider_queue", []))
    if not provider_queue:
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "warning", "text": "Select at least one reconciliation provider before processing."}
        return

    STATE["processing_active"] = True
    STATE["processed_terms_count"] = 0
    STATE["current_term_index_processing"] = 0
    STATE["stop_processing_requested"] = False
    STATE["provider_has_results"] = set()
    for provider in provider_queue:
        STATE["provider_status"][provider] = {"status": "running", "results_count": 0, "error_msg": "", "progress": 0.0}

    for position, actual_df_index in enumerate(total_indices):
        if STATE.get("stop_processing_requested"):
            break
        term_to_process = str(df.loc[actual_df_index, "Term"]).strip()
        STATE["current_term_index_processing"] = position
        if not term_to_process:
            STATE["processed_terms_count"] += 1
            continue
        STATE.setdefault("suggestions", {}).setdefault(actual_df_index, {})
        max_workers = min(len(provider_queue), 8) if provider_queue else 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_provider = {}
            for provider_name in provider_queue:
                current_config_for_provider = CONFIG.copy()
                if provider_name == CUSTOM_SPARQL_PROVIDER_NAME and STATE.get("custom_sparql_enabled"):
                    current_config_for_provider["custom_sparql"] = {
                        "endpoint": STATE.get("custom_sparql_endpoint"),
                        "query_template": STATE.get("custom_sparql_query_template"),
                        "var_uri": STATE.get("custom_sparql_var_uri"),
                        "var_label": STATE.get("custom_sparql_var_label"),
                        "var_description": STATE.get("custom_sparql_var_description"),
                    }
                elif provider_name == "NCBI":
                    current_config_for_provider["ncbi_databases"] = STATE.get("ncbi_selected_databases", [])
                elif provider_name == "Local Ontology":
                    current_config_for_provider["local_resources"] = STATE.get("local_resources", [])
                    current_config_for_provider["local_backend"] = STATE.get("local_backend", "auto")
                if provider_name in STATE.get("selected_ontologies_by_provider", {}):
                    current_config_for_provider["selected_ontologies_by_provider"] = {provider_name: STATE["selected_ontologies_by_provider"].get(provider_name, [])}
                future_to_provider[
                    executor.submit(fetch_suggestions_for_term_from_provider, provider_name, term_to_process, current_config_for_provider, USER_AGENT, STATE.get("suggestion_slider", 10))
                ] = provider_name
            for future in concurrent.futures.as_completed(future_to_provider):
                provider_name = future_to_provider[future]
                try:
                    provider_suggestions = future.result()
                    STATE["suggestions"][actual_df_index][provider_name] = provider_suggestions
                    if provider_suggestions:
                        STATE.setdefault("provider_has_results", set()).add(provider_name)
                        STATE["provider_status"][provider_name]["results_count"] = STATE["provider_status"][provider_name].get("results_count", 0) + 1
                except Exception as exc:
                    logger.error("Error fetching suggestions for term %r from %s: %s", term_to_process, provider_name, exc, exc_info=True)
                    STATE["suggestions"][actual_df_index][provider_name] = []
                    STATE["provider_status"][provider_name]["status"] = "error"
                    STATE["provider_status"][provider_name]["error_msg"] = str(exc)
        STATE["processed_terms_count"] += 1

    STATE["current_term_index_processing"] = len(total_indices)
    STATE["processing_active"] = False
    for provider in provider_queue:
        if STATE["provider_status"].get(provider, {}).get("status") not in {"error", "stopped"}:
            STATE["provider_status"][provider]["status"] = "completed"
    if len(STATE.get("provider_has_results", set())) >= 2:
        STATE["display_mixed_results"] = True
        STATE["display_provider"] = None
    elif STATE.get("provider_has_results"):
        STATE["display_provider"] = sorted(STATE.get("provider_has_results"))[0]
        STATE["display_mixed_results"] = False
    STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Processing queue finished. Review provider suggestions in the Reconcile step."}
    STATE[RECONCILIATION_MUI_ACTIVE_STAGE_KEY] = "reconcile"


def _apply_reconciliation_selection(row_index, selected_option: dict, match_type: str | None = None):
    df = STATE.get("df")
    if not isinstance(df, pd.DataFrame) or row_index not in df.index:
        return
    chosen_uri = str(selected_option.get("uri", NO_MATCH_URI) or NO_MATCH_URI)
    chosen_source = str(selected_option.get("source", "") or "")
    display = str(selected_option.get("display", NO_MATCH_DISPLAY) or NO_MATCH_DISPLAY)
    raw = selected_option.get("raw") if isinstance(selected_option.get("raw"), dict) else selected_option
    df.loc[row_index, "URI"] = chosen_uri
    df.loc[row_index, "object_id"] = chosen_uri
    df.loc[row_index, "Source Provider"] = chosen_source if chosen_uri != NO_MATCH_URI else ""
    df.loc[row_index, "Confirmed Display String"] = display if chosen_uri != NO_MATCH_URI else NO_MATCH_DISPLAY
    if chosen_uri != NO_MATCH_URI:
        df.loc[row_index, "Provider Term"] = raw.get("label", selected_option.get("label", "")) if isinstance(raw, dict) else selected_option.get("label", "")
        df.loc[row_index, "Provider Description"] = raw.get("description", selected_option.get("description", "")) if isinstance(raw, dict) else selected_option.get("description", "")
        df.loc[row_index, "object_label"] = raw.get("label", selected_option.get("label", "")) if isinstance(raw, dict) else selected_option.get("label", "")
        if STATE.get("skos_matching_enabled") and not str(df.loc[row_index, "predicate_id"]).strip():
            df.loc[row_index, "Match Type"] = "skos:exactMatch"
            df.loc[row_index, "predicate_id"] = "skos:exactMatch"
    else:
        df.loc[row_index, "Provider Term"] = ""
        df.loc[row_index, "Provider Description"] = ""
        df.loc[row_index, "object_label"] = ""
        df.loc[row_index, "Match Type"] = ""
        df.loc[row_index, "predicate_id"] = ""
    if match_type is not None:
        final_match_type = "" if not match_type or chosen_uri == NO_MATCH_URI else str(match_type)
        df.loc[row_index, "Match Type"] = final_match_type
        df.loc[row_index, "predicate_id"] = final_match_type
    apply_mapping_justification_for_row(
        df,
        row_index,
        default_when_mapped=(
            REVIEW_MAPPING_JUSTIFICATION
            if not isinstance(raw, dict)
            else (
                LEXICAL_MAPPING_JUSTIFICATION
                if isinstance(raw.get("levenshtein_score"), (int, float)) and float(raw.get("levenshtein_score")) >= 1.0
                else LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION
            )
        ),
        no_match_uri=NO_MATCH_URI,
        force_when_mapped=True,
    )
    STATE["df"] = df


def _perform_custom_search(row_index, search_term: str, display_mode: str):
    providers = []
    if display_mode == "Mixed Results":
        providers = [p for p in STATE.get("provider_queue", []) if STATE.get("provider_status", {}).get(p, {}).get("status") not in ["pending", "error"]]
    elif display_mode:
        providers = [display_mode]
    if not providers:
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "warning", "text": "Select a provider or Mixed Results before custom search."}
        return
    all_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(providers))) as executor:
        future_to_provider = {}
        for provider_name in providers:
            dynamic_config = CONFIG.copy()
            if provider_name in STATE.get("selected_ontologies_by_provider", {}):
                dynamic_config["selected_ontologies_by_provider"] = {provider_name: STATE["selected_ontologies_by_provider"].get(provider_name, [])}
            if provider_name == "NCBI":
                dynamic_config["ncbi_databases"] = STATE.get("ncbi_selected_databases", [])
            if provider_name == "Local Ontology":
                dynamic_config["local_resources"] = STATE.get("local_resources", [])
                dynamic_config["local_backend"] = STATE.get("local_backend", "auto")
            if provider_name == CUSTOM_SPARQL_PROVIDER_NAME and STATE.get("custom_sparql_enabled"):
                dynamic_config["custom_sparql"] = {
                    "endpoint": STATE.get("custom_sparql_endpoint"),
                    "query_template": STATE.get("custom_sparql_query_template"),
                    "var_uri": STATE.get("custom_sparql_var_uri"),
                    "var_label": STATE.get("custom_sparql_var_label"),
                    "var_description": STATE.get("custom_sparql_var_description"),
                }
            future_to_provider[executor.submit(fetch_suggestions_for_term_from_provider, provider_name, search_term, dynamic_config, USER_AGENT, STATE.get("suggestion_slider", 10))] = provider_name
        for future in concurrent.futures.as_completed(future_to_provider):
            provider_name = future_to_provider[future]
            try:
                all_results[provider_name] = future.result()
            except Exception as exc:
                logger.error("Error in custom search for %s: %s", provider_name, exc, exc_info=True)
                all_results[provider_name] = []
    sorted_results = get_combined_and_sorted_suggestions(
        search_term,
        all_results,
        STATE.get("suggestion_slider", 10),
        STATE.get("matching_strategy_radio"),
        STATE.get("selected_ontologies_by_provider", {}),
    )
    key = (row_index, display_mode or "Mixed Results", "mui_custom_search")
    STATE.setdefault("custom_search_summaries", {})[key] = {
        "search_term": search_term,
        "results_count": len(sorted_results),
        "providers": providers,
    }
    STATE["custom_search_results"][key] = sorted_results
    row_suggestions = STATE["suggestions"].setdefault(row_index, {})
    for provider_name, provider_results in all_results.items():
        row_suggestions[provider_name] = provider_results or []
        if provider_results:
            STATE.setdefault("provider_has_results", set()).add(provider_name)
            STATE["provider_status"].setdefault(provider_name, {"status": "completed", "results_count": 0, "error_msg": "", "progress": 1.0})
            if STATE["provider_status"][provider_name].get("status") in {"pending", "error"}:
                STATE["provider_status"][provider_name]["status"] = "completed"
    if display_mode and display_mode != "Mixed Results" and display_mode not in row_suggestions:
        row_suggestions[display_mode] = sorted_results
    STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": f"Found {len(sorted_results)} result(s) for custom search term '{search_term}'."}


def _handle_reconciliation_mui_event(event: object) -> bool:
    if not isinstance(event, dict):
        return False
    nonce = event.get("nonce")
    if nonce and STATE.get(RECONCILIATION_MUI_EVENT_NONCE_KEY) == nonce:
        return False
    if nonce:
        STATE[RECONCILIATION_MUI_EVENT_NONCE_KEY] = nonce
    event_type = str(event.get("type", "") or "")

    if event_type == "navigate":
        stage = str(event.get("stage", "") or "")
        if stage in RECONCILIATION_STAGES:
            STATE[RECONCILIATION_MUI_ACTIVE_STAGE_KEY] = stage
        return True
    if event_type == "upload_table":
        try:
            df_loaded, filename = _read_matching_table_payload(event)
            if _reset_reconciliation_state_and_load_df(df_loaded, filename, is_from_shared_generator=False):
                STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": f"Matching table '{filename}' loaded."}
                STATE[RECONCILIATION_MUI_ACTIVE_STAGE_KEY] = "configure"
            else:
                STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": STATE.get("csv_load_error_message") or "Failed to load matching table."}
        except Exception as exc:
            STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": f"Failed to parse uploaded table: {exc}"}
        return True
    if event_type == "load_shared_table":
        shared_df = STATE.get("shared_matching_table")
        if isinstance(shared_df, pd.DataFrame) and not shared_df.empty:
            if _reset_reconciliation_state_and_load_df(shared_df, "Matching Table Generator", is_from_shared_generator=True):
                STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Shared matching table loaded for reconciliation."}
                STATE[RECONCILIATION_MUI_ACTIVE_STAGE_KEY] = "configure"
            else:
                STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": STATE.get("csv_load_error_message") or "Shared table is missing required SSSOM columns."}
        else:
            STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "warning", "text": "No shared matching table is available."}
        return True
    if event_type == "update_settings":
        patch = event.get("settings", {}) if isinstance(event.get("settings"), dict) else {}
        for key in [
            "custom_sparql_enabled", "custom_sparql_endpoint", "custom_sparql_query_template", "custom_sparql_var_uri",
            "custom_sparql_var_label", "custom_sparql_var_description", "local_backend", "matching_strategy_radio",
            "suggestion_slider", "levenshtein_threshold_slider", "items_per_page", "skos_matching_enabled",
            "show_only_matched_terms", "show_only_unreconciled_terms", "current_page",
        ]:
            if key in patch:
                STATE[key] = patch[key]
        if "ncbi_selected_databases" in patch and isinstance(patch["ncbi_selected_databases"], list):
            STATE["ncbi_selected_databases"] = patch["ncbi_selected_databases"]
        if "selected_ontologies_by_provider" in patch and isinstance(patch["selected_ontologies_by_provider"], dict):
            STATE["selected_ontologies_by_provider"] = patch["selected_ontologies_by_provider"]
        if "provider_queue" in patch and isinstance(patch["provider_queue"], list):
            _apply_provider_queue(patch["provider_queue"])
        return True
    if event_type == "confirm_queue":
        _apply_provider_queue(event.get("provider_queue", []))
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Provider queue updated." if STATE.get("provider_queue") else "Provider queue cleared."}
        return True
    if event_type == "index_local_resources":
        resources = []
        backend = str(event.get("backend") or STATE.get("local_backend", "auto"))
        STATE["local_backend"] = backend
        for uploaded in event.get("files", []) if isinstance(event.get("files"), list) else []:
            try:
                filename = str(uploaded.get("filename", "local_resource"))
                suffix = Path(filename).suffix
                payload = base64.b64decode(str(uploaded.get("content_base64", "")))
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                tmp.write(payload)
                tmp.close()
                idx = local_resource_provider.load_local_resource_index(tmp.name, resource_name=filename, force_backend=backend, max_entities=50000)
                resources.append({"name": filename, "path": tmp.name, "index": idx, "backend": backend})
            except Exception as exc:
                logger.error("Error indexing local resource: %s", exc, exc_info=True)
                STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": f"Error indexing local resource: {exc}"}
        if resources:
            STATE["local_resources"] = resources
            STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": f"Indexed {len(resources)} local resource(s)."}
        return True
    if event_type == "start_processing":
        _process_reconciliation_queue()
        return True
    if event_type == "select_display_provider":
        provider = str(event.get("provider", "") or "")
        if provider == "Mixed Results":
            STATE["display_mixed_results"] = True
            STATE["display_provider"] = None
        elif provider:
            STATE["display_provider"] = provider
            STATE["display_mixed_results"] = False
        STATE[RECONCILIATION_MUI_ACTIVE_STAGE_KEY] = "reconcile"
        return True
    if event_type == "update_mapping":
        row_index = event.get("row_index")
        try:
            if isinstance(row_index, str) and row_index.isdigit():
                row_index = int(row_index)
        except Exception:
            pass
        selected_option = event.get("selected_option", {}) if isinstance(event.get("selected_option"), dict) else {}
        _apply_reconciliation_selection(row_index, selected_option, event.get("match_type"))
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Mapping selection updated."}
        return True
    if event_type == "prefill_best_match":
        _prefill_best_matches_for_backend()
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Best available suggestions were prefilled for unreconciled rows."}
        return True
    if event_type == "custom_search":
        row_index = event.get("row_index")
        try:
            if isinstance(row_index, str) and row_index.isdigit():
                row_index = int(row_index)
        except Exception:
            pass
        search_term = str(event.get("search_term", "") or "").strip()
        if search_term:
            _perform_custom_search(row_index, search_term, str(event.get("display_mode") or _current_reconciliation_display_mode() or ""))
        else:
            STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "warning", "text": "Enter a custom search term first."}
        return True
    if event_type in {"publish_rdf_handoff", "prepare_downloads"}:
        df = STATE.get("df")
        if isinstance(df, pd.DataFrame):
            STATE["shared_reconciled_matching_table"] = finalize_accepted_results(df.copy())
            STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Reconciled matching table published to RDF Generator handoff."}
        else:
            STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "warning", "text": "No reconciled data is available yet."}
        return True
    if event_type == "reset_workflow":
        for key in ["df", "suggestions", "selected_uris", "custom_search_results", "custom_search_summaries", "provider_queue", "provider_status", "provider_has_results", "display_provider", "display_mixed_results", "total_indices_to_process"]:
            if key in STATE:
                del STATE[key]
        _initialize_reconciliation_mui_state()
        STATE[RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "info", "text": "Semi-automatic reconciliation workflow reset."}
        return True
    return False


def initialize_reconciliation_state() -> None:
    _initialize_reconciliation_mui_state()


def build_reconciliation_snapshot() -> dict[str, object]:
    return _build_reconciliation_mui_snapshot()


def handle_reconciliation_event(event: object) -> bool:
    return _handle_reconciliation_mui_event(event)


def set_shared_matching_table(df: pd.DataFrame | None) -> None:
    if isinstance(df, pd.DataFrame):
        STATE["shared_matching_table"] = df.copy()
    else:
        STATE.pop("shared_matching_table", None)


def get_shared_outputs() -> dict[str, object]:
    outputs: dict[str, object] = {}
    shared = STATE.get("shared_reconciled_matching_table")
    if isinstance(shared, pd.DataFrame):
        outputs["shared_reconciled_matching_table"] = shared.copy()
    return outputs
