# -*- coding: utf-8 -*-
"""
Shared helpers for matching-table validation and reconciliation I/O.

Primary contract (strict SSSOM table used for exchange):
    subject_id, subject_label, predicate_id, object_id, object_label, mapping_justification
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


_SSSOM_TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "templates" / "sssom_matching_table.json"
_FALLBACK_SSSOM_TEMPLATE = {
    "core_columns": [
        "subject_id",
        "subject_label",
        "predicate_id",
        "object_id",
        "object_label",
        "mapping_justification",
    ],
    "optional_export_columns": [
        "author_id",
        "confidence",
        "comment",
        "mapping_date",
        "review_date",
        "creator_id",
        "creator_label",
        "reviewer_id",
        "reviewer_label",
        "mapping_provider",
        "object_source",
        "mapping_tool",
        "mapping_tool_version",
        "publication_date",
        "match_string",
    ],
    "legacy_required_columns": ["Term", "URI", "RDF Role", "Match Type"],
    "default_mapping_justifications": {
        "manual": "semapv:ManualMappingCuration",
        "review": "semapv:MappingReview",
        "lexical": "semapv:LexicalMatching",
        "lexical_similarity_threshold": "semapv:LexicalSimilarityThresholdMatching",
        "semantic_similarity_threshold": "semapv:SemanticSimilarityThresholdMatching",
    },
}


def _load_sssom_template() -> Dict[str, object]:
    try:
        payload = json.loads(_SSSOM_TEMPLATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    template = {**_FALLBACK_SSSOM_TEMPLATE, **(payload if isinstance(payload, dict) else {})}
    for key in ("core_columns", "optional_export_columns", "legacy_required_columns"):
        value = template.get(key)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            template[key] = _FALLBACK_SSSOM_TEMPLATE[key]
    justifications = template.get("default_mapping_justifications")
    if not isinstance(justifications, dict):
        template["default_mapping_justifications"] = _FALLBACK_SSSOM_TEMPLATE["default_mapping_justifications"]
    return template


_SSSOM_TEMPLATE = _load_sssom_template()

SSSOM_MATCHING_TABLE_COLUMNS = list(_SSSOM_TEMPLATE["core_columns"])

SSSOM_OPTIONAL_EXPORT_COLUMNS = list(_SSSOM_TEMPLATE["optional_export_columns"])

CURATED_SSSOM_EXPORT_COLUMNS = SSSOM_MATCHING_TABLE_COLUMNS + SSSOM_OPTIONAL_EXPORT_COLUMNS

LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS = list(_SSSOM_TEMPLATE["legacy_required_columns"])

# Kept for backwards import compatibility across the codebase.
REQUIRED_MATCHING_TABLE_COLUMNS = SSSOM_MATCHING_TABLE_COLUMNS.copy()

_DEFAULT_MAPPING_JUSTIFICATIONS = _SSSOM_TEMPLATE["default_mapping_justifications"]

DEFAULT_MAPPING_JUSTIFICATION = str(_DEFAULT_MAPPING_JUSTIFICATIONS["manual"])
REVIEW_MAPPING_JUSTIFICATION = str(_DEFAULT_MAPPING_JUSTIFICATIONS["review"])
LEXICAL_MAPPING_JUSTIFICATION = str(_DEFAULT_MAPPING_JUSTIFICATIONS["lexical"])
LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION = str(_DEFAULT_MAPPING_JUSTIFICATIONS["lexical_similarity_threshold"])
SEMANTIC_MAPPING_JUSTIFICATION = str(_DEFAULT_MAPPING_JUSTIFICATIONS["semantic_similarity_threshold"])

JUSTIFICATION_EXTENSION_COLUMNS = [
    "subject_match_field",
    "object_match_field",
    "match_string",
    "subject_preprocessing",
    "object_preprocessing",
    "semantic_similarity_measure",
    "semantic_similarity_score",
    "author_id",
    "confidence",
    "curation_rule",
    "reviewer_id",
    "reviewer_label",
    "review_date",
    "reviewer_agreement",
]

LEXICAL_JUSTIFICATIONS = {
    LEXICAL_MAPPING_JUSTIFICATION,
    LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION,
}
SEMANTIC_JUSTIFICATIONS = {SEMANTIC_MAPPING_JUSTIFICATION}
MANUAL_JUSTIFICATIONS = {DEFAULT_MAPPING_JUSTIFICATION}
REVIEW_JUSTIFICATIONS = {REVIEW_MAPPING_JUSTIFICATION}

_LEXICAL_EXTENSION_COLUMNS = [
    "subject_match_field",
    "object_match_field",
    "match_string",
    "subject_preprocessing",
    "object_preprocessing",
]
_SEMANTIC_EXTENSION_COLUMNS = ["semantic_similarity_measure", "semantic_similarity_score"]
_REVIEW_EXTENSION_COLUMNS = ["reviewer_id", "reviewer_label", "review_date", "reviewer_agreement"]

PROVENANCE_DEFAULT_COLUMNS = [
    "author_id",
    "author_label",
    "reviewer_id",
    "reviewer_label",
    "creator_id",
    "creator_label",
    "mapping_tool",
    "mapping_tool_version",
    "mapping_date",
    "publication_date",
]

LEGACY_TO_SSSOM_COLUMN_MAP = {
    "Term": "subject_label",
    "URI": "object_id",
    "Match Type": "predicate_id",
    "Provider Term": "object_label",
    "source_provider": "Source Provider",
    "provider_term": "Provider Term",
    "provider_description": "Provider Description",
}

STANDARD_RECONCILIATION_COLUMNS = [
    "Term",
    "URI",
    "Match Type",
    "RDF Role",
    "Source Provider",
    "Provider Term",
    "Provider Description",
    "Confirmed Display String",
]

AGENT_WORKING_COLUMNS = [
    "Suggested URI",
    "Suggested Provider",
    "Suggested Label",
    "Suggested Description",
    "Suggested Match Type",
    "Suggested Confidence",
    "Suggested LLM Confidence",
    "Suggested Decision Source",
    "Suggested Fallback Reason",
    "Agent Decision Status",
    "Agent Trace Metadata",
    "Auto Accepted",
    "Auto Acceptance Score",
    "Auto Accept Reason",
    "Auto Accepted At",
    "Definition",
    "Agent Explanation",
    "Review Status",
    "Agent Workflow",
    "Run ID",
]

_PREDICATE_NORMALIZATION: Dict[str, str] = {
    "exactmatch": "skos:exactMatch",
    "skos:exactmatch": "skos:exactMatch",
    "closematch": "skos:closeMatch",
    "skos:closematch": "skos:closeMatch",
    "broadmatch": "skos:broadMatch",
    "skos:broadmatch": "skos:broadMatch",
    "narrowmatch": "skos:narrowMatch",
    "skos:narrowmatch": "skos:narrowMatch",
    "relatedmatch": "skos:relatedMatch",
    "skos:relatedmatch": "skos:relatedMatch",
}


@dataclass(frozen=True)
class MatchingTableValidationResult:
    is_valid: bool
    missing_columns: List[str]


def _coerce_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _normalize_predicate(value: object) -> str:
    raw = _coerce_str(value).strip()
    if not raw:
        return ""
    return _PREDICATE_NORMALIZATION.get(raw.lower(), raw)


def _is_no_match_uri(value: object, no_match_uri: str = "No Match") -> bool:
    return _coerce_str(value).strip().lower() == _coerce_str(no_match_uri).strip().lower()


def _ensure_justification_extension_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in JUSTIFICATION_EXTENSION_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _row_value(df: pd.DataFrame, row_index, column: str) -> str:
    if column not in df.columns:
        return ""
    return _coerce_str(df.at[row_index, column]).strip()


def _set_row_value_if_empty(df: pd.DataFrame, row_index, column: str, value: object) -> None:
    if column not in df.columns:
        return
    current = _coerce_str(df.at[row_index, column]).strip()
    if current:
        return
    df.at[row_index, column] = _coerce_str(value).strip()


def _to_float(value: object) -> float | None:
    raw = _coerce_str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _infer_match_string(df: pd.DataFrame, row_index) -> str:
    subject = _row_value(df, row_index, "subject_label") or _row_value(df, row_index, "Term")
    object_label = (
        _row_value(df, row_index, "object_label")
        or _row_value(df, row_index, "Provider Term")
        or _row_value(df, row_index, "Suggested Label")
    )
    if subject and object_label and subject.lower() == object_label.lower():
        return subject
    return subject or object_label


def _infer_semantic_similarity_score(df: pd.DataFrame, row_index) -> float | None:
    candidate_columns = [
        "semantic_similarity_score",
        "Suggested Confidence",
        "Auto Acceptance Score",
        "confidence",
    ]
    for col in candidate_columns:
        score = _to_float(_row_value(df, row_index, col))
        if score is not None:
            return max(0.0, min(1.0, score))
    return None


def _clear_columns(df: pd.DataFrame, row_index, columns: List[str]) -> None:
    for col in columns:
        if col in df.columns:
            df.at[row_index, col] = ""


def _is_row_mapped(df: pd.DataFrame, row_index, no_match_uri: str = "No Match") -> bool:
    object_id_value = _row_value(df, row_index, "object_id") or _row_value(df, row_index, "URI")
    if not object_id_value:
        return False
    return not _is_no_match_uri(object_id_value, no_match_uri=no_match_uri)


def apply_provenance_defaults(
    df: pd.DataFrame,
    *,
    defaults: Dict[str, object] | None = None,
    no_match_uri: str = "No Match",
) -> pd.DataFrame:
    """Fill empty provenance metadata columns with configured defaults for mapped rows."""
    if not defaults or not isinstance(defaults, dict):
        return df

    out = df
    for col in PROVENANCE_DEFAULT_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    for idx in out.index:
        if not _is_row_mapped(out, idx, no_match_uri=no_match_uri):
            continue
        for col in PROVENANCE_DEFAULT_COLUMNS:
            value = _coerce_str(defaults.get(col, "")).strip()
            if not value:
                continue
            _set_row_value_if_empty(out, idx, col, value)
    return out


def _apply_justification_extensions(
    df: pd.DataFrame,
    row_index,
    *,
    resolved_justification: str,
) -> pd.DataFrame:
    _ensure_justification_extension_columns(df)
    justification = _coerce_str(resolved_justification).strip()

    if not justification:
        _clear_columns(df, row_index, JUSTIFICATION_EXTENSION_COLUMNS)
        return df

    if justification in LEXICAL_JUSTIFICATIONS:
        _set_row_value_if_empty(df, row_index, "subject_match_field", "rdfs:label")
        _set_row_value_if_empty(df, row_index, "object_match_field", "rdfs:label")
        _set_row_value_if_empty(df, row_index, "match_string", _infer_match_string(df, row_index))
        _clear_columns(df, row_index, _SEMANTIC_EXTENSION_COLUMNS)

    elif justification in SEMANTIC_JUSTIFICATIONS:
        _clear_columns(df, row_index, _LEXICAL_EXTENSION_COLUMNS)
        _set_row_value_if_empty(df, row_index, "semantic_similarity_measure", "workflow_confidence_score")
        score = _infer_semantic_similarity_score(df, row_index)
        if score is not None:
            df.at[row_index, "semantic_similarity_score"] = round(float(score), 4)
            _set_row_value_if_empty(df, row_index, "confidence", round(float(score), 4))

    elif justification in REVIEW_JUSTIFICATIONS:
        _set_row_value_if_empty(df, row_index, "reviewer_label", "human_reviewer")
        review_status = _row_value(df, row_index, "Review Status").lower()
        if review_status == "accepted":
            _set_row_value_if_empty(df, row_index, "reviewer_agreement", "agree")
        elif review_status == "rejected":
            _set_row_value_if_empty(df, row_index, "reviewer_agreement", "disagree")
        _set_row_value_if_empty(
            df,
            row_index,
            "review_date",
            datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        )

    elif justification in MANUAL_JUSTIFICATIONS:
        _clear_columns(df, row_index, _LEXICAL_EXTENSION_COLUMNS + _SEMANTIC_EXTENSION_COLUMNS)

    return df


def resolve_mapping_justification(
    *,
    object_id: object,
    existing_justification: object = "",
    default_when_mapped: str = DEFAULT_MAPPING_JUSTIFICATION,
    no_match_uri: str = "No Match",
    force_when_mapped: bool = False,
) -> str:
    """Resolve mapping justification for a row based on mapping state.

    - Unmapped / No Match rows get an empty justification.
    - Mapped rows keep an existing justification when present (default behavior).
    - When ``force_when_mapped`` is True, mapped rows always receive
      ``default_when_mapped``.
    - Otherwise, mapped rows receive the provided default justification when no
      existing value is present.
    """
    object_id_value = _coerce_str(object_id).strip()
    existing_value = _coerce_str(existing_justification).strip()
    default_value = _coerce_str(default_when_mapped).strip() or DEFAULT_MAPPING_JUSTIFICATION

    if not object_id_value or _is_no_match_uri(object_id_value, no_match_uri=no_match_uri):
        return ""
    if force_when_mapped:
        return default_value
    return existing_value or default_value


def apply_mapping_justification_for_row(
    df: pd.DataFrame,
    row_index,
    *,
    default_when_mapped: str = DEFAULT_MAPPING_JUSTIFICATION,
    no_match_uri: str = "No Match",
    force_when_mapped: bool = False,
) -> pd.DataFrame:
    """Apply justification state for a single row in-place and return the dataframe."""
    _ensure_justification_extension_columns(df)
    if "mapping_justification" not in df.columns:
        df["mapping_justification"] = ""

    object_id_value = ""
    if "object_id" in df.columns:
        object_id_value = _coerce_str(df.at[row_index, "object_id"]).strip()
    if not object_id_value and "URI" in df.columns:
        object_id_value = df.at[row_index, "URI"]

    existing_value = df.at[row_index, "mapping_justification"]
    resolved_justification = resolve_mapping_justification(
        object_id=object_id_value,
        existing_justification=existing_value,
        default_when_mapped=default_when_mapped,
        no_match_uri=no_match_uri,
        force_when_mapped=force_when_mapped,
    )
    df.at[row_index, "mapping_justification"] = resolved_justification
    _apply_justification_extensions(
        df,
        row_index,
        resolved_justification=resolved_justification,
    )
    return df


def _first_non_empty(row: pd.Series, columns: List[str]) -> str:
    for col in columns:
        if col in row.index:
            v = _coerce_str(row[col]).strip()
            if v:
                return v
    return ""


def sync_matching_table_schemas(df: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe normalized to canonical 6-column SSSOM structure.

    Extra columns are preserved.
    """
    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]

    # Normalize known snake_case provider/support columns into their
    # title-cased reconciliation aliases for compatibility with existing UI code.
    if "Source Provider" not in out.columns and "source_provider" in out.columns:
        out["Source Provider"] = out["source_provider"]
    if "Provider Term" not in out.columns and "provider_term" in out.columns:
        out["Provider Term"] = out["provider_term"]
    if "Provider Description" not in out.columns and "provider_description" in out.columns:
        out["Provider Description"] = out["provider_description"]

    # Lift legacy columns into canonical columns when needed.
    for legacy_col, sssom_col in LEGACY_TO_SSSOM_COLUMN_MAP.items():
        if sssom_col not in out.columns and legacy_col in out.columns:
            out[sssom_col] = out[legacy_col]

    for col in SSSOM_MATCHING_TABLE_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    _ensure_justification_extension_columns(out)

    for idx, row in out.iterrows():
        out.at[idx, "subject_id"] = _first_non_empty(row, ["subject_id"])
        out.at[idx, "subject_label"] = _first_non_empty(row, ["subject_label", "Term"])
        out.at[idx, "object_id"] = _first_non_empty(row, ["object_id", "URI"])

        pred_value = _first_non_empty(row, ["predicate_id", "Match Type"])
        out.at[idx, "predicate_id"] = _normalize_predicate(pred_value)

        out.at[idx, "object_label"] = _first_non_empty(
            row,
            ["object_label", "Provider Term", "Confirmed Display String"],
        )

        out.at[idx, "mapping_justification"] = _first_non_empty(row, ["mapping_justification"])

    # Keep canonical columns first.
    extras = [c for c in out.columns if c not in SSSOM_MATCHING_TABLE_COLUMNS]
    return out[SSSOM_MATCHING_TABLE_COLUMNS + extras].copy()


def validate_matching_table(df: pd.DataFrame | None) -> MatchingTableValidationResult:
    """Validate strict SSSOM schema; also accept complete legacy 4-column tables."""
    if df is None or not isinstance(df, pd.DataFrame):
        return MatchingTableValidationResult(False, REQUIRED_MATCHING_TABLE_COLUMNS.copy())

    has_strict_sssom = all(col in df.columns for col in SSSOM_MATCHING_TABLE_COLUMNS)
    has_legacy_migratable = all(col in df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS)

    if has_strict_sssom or has_legacy_migratable:
        return MatchingTableValidationResult(True, [])

    missing = [col for col in SSSOM_MATCHING_TABLE_COLUMNS if col not in df.columns]
    return MatchingTableValidationResult(False, missing)


def _add_legacy_working_aliases(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "Term" not in out.columns:
        out["Term"] = out["subject_label"]
    if "URI" not in out.columns:
        out["URI"] = out["object_id"]
    if "Match Type" not in out.columns:
        out["Match Type"] = out["predicate_id"]
    if "RDF Role" not in out.columns:
        out["RDF Role"] = ""

    # Keep aliases synchronized from canonical values.
    out["Term"] = out["subject_label"].astype(str)
    out["URI"] = out["object_id"].astype(str)
    out["Match Type"] = out["predicate_id"].astype(str)
    out["RDF Role"] = out["RDF Role"].astype(str)
    return out


def ensure_standard_reconciliation_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare a dataframe for reconciliation UI workflows.

    Adds working columns needed by existing UI/agent paths while keeping canonical
    SSSOM columns authoritative.
    """
    out = sync_matching_table_schemas(df)
    out = _add_legacy_working_aliases(out)

    defaults = {
        "Source Provider": "",
        "Provider Term": "",
        "Provider Description": "",
        "Confirmed Display String": "",
        "source_provider": "",
        "provider_term": "",
        "provider_description": "",
        "confirmed_display_string": "",
        "comment": "",
        "match_type": "",
    }
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default
        out[col] = out[col].fillna(default)

    # Keep snake_case aliases synchronized from canonical/title-case values.
    out["source_provider"] = out["Source Provider"].astype(str)
    out["provider_term"] = out["Provider Term"].astype(str)
    out["provider_description"] = out["Provider Description"].astype(str)
    out["confirmed_display_string"] = out["Confirmed Display String"].astype(str)
    out["match_type"] = out["predicate_id"].astype(str)

    # Keep object_label aligned with provider term when available.
    mask_missing_obj_label = out["object_label"].astype(str).str.strip().eq("")
    out.loc[mask_missing_obj_label, "object_label"] = out.loc[mask_missing_obj_label, "Provider Term"].astype(str)
    return out


def ensure_agent_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add working columns needed by the agent-based reconciliation flow."""
    out = ensure_standard_reconciliation_columns(df)
    for col in AGENT_WORKING_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def get_preferred_term_column(df: pd.DataFrame) -> str:
    if "subject_label" in df.columns:
        return "subject_label"
    return "Term"


def get_preferred_uri_column(df: pd.DataFrame) -> str:
    if "object_id" in df.columns:
        return "object_id"
    return "URI"


def get_preferred_rdf_role_column(df: pd.DataFrame) -> str:
    """Backward-compatible helper name: now returns SSSOM predicate column."""
    if "predicate_id" in df.columns:
        return "predicate_id"
    return "Match Type"


def get_preferred_match_type_column(df: pd.DataFrame) -> str:
    """Backward-compatible helper name: now returns SSSOM predicate column."""
    if "predicate_id" in df.columns:
        return "predicate_id"
    if "match_type" in df.columns:
        return "match_type"
    return "Match Type"


def get_unreconciled_indices(df: pd.DataFrame, no_match_uri: str) -> list:
    """Return row indices that still need reconciliation."""
    if df is None or df.empty:
        return []
    local = sync_matching_table_schemas(df)
    uri_col = get_preferred_uri_column(local)
    values = local[uri_col].astype(str).str.strip()
    return list(local[(values == "") | (values == str(no_match_uri).strip())].index)


def extract_all_terms_for_reconciliation(df: pd.DataFrame) -> list[str]:
    """Return all unique terms from the preferred term column."""
    if df is None or df.empty:
        return []
    local = sync_matching_table_schemas(df)
    term_col = get_preferred_term_column(local)
    return local[term_col].astype(str).dropna().astype(str).unique().tolist()


def prepare_loaded_matching_table(df_to_load: pd.DataFrame, no_match_uri: str) -> Tuple[pd.DataFrame, list, list[str]]:
    """Normalize loaded table for reconciliation workflows."""
    normalized = ensure_standard_reconciliation_columns(df_to_load)
    unreconciled = get_unreconciled_indices(normalized, no_match_uri)
    terms = extract_all_terms_for_reconciliation(normalized)
    return normalized, unreconciled, terms


def finalize_accepted_results(
    df: pd.DataFrame,
    *,
    provenance_defaults: Dict[str, object] | None = None,
) -> pd.DataFrame:
    """Return strict 6-column SSSOM table for downstream handoff/export."""
    out = ensure_standard_reconciliation_columns(df)

    if {"Review Status", "Suggested URI"}.issubset(out.columns):
        accepted_mask = (
            out["Review Status"].astype(str).str.strip().str.lower().eq("accepted")
            & out["object_id"].astype(str).str.strip().eq("")
            & out["Suggested URI"].astype(str).str.strip().ne("")
        )

        if accepted_mask.any():
            out.loc[accepted_mask, "object_id"] = out.loc[accepted_mask, "Suggested URI"].astype(str)
            out.loc[accepted_mask, "URI"] = out.loc[accepted_mask, "Suggested URI"].astype(str)

            if "Suggested Label" in out.columns:
                out.loc[accepted_mask, "object_label"] = out.loc[accepted_mask, "Suggested Label"].astype(str)
            if "Suggested Match Type" in out.columns:
                out.loc[accepted_mask, "predicate_id"] = out.loc[accepted_mask, "Suggested Match Type"].map(_normalize_predicate)

    # Normalize explicit "No Match" placeholders to empty object_id for strict export.
    no_match_mask = out["object_id"].astype(str).str.strip().str.lower().eq("no match")
    if no_match_mask.any():
        out.loc[no_match_mask, "object_id"] = ""
        if "URI" in out.columns:
            out.loc[no_match_mask, "URI"] = ""

    for idx in out.index:
        apply_mapping_justification_for_row(out, idx)

    apply_provenance_defaults(out, defaults=provenance_defaults)

    if "comment" not in out.columns:
        out["comment"] = ""

    out = sync_matching_table_schemas(out)
    minimum_columns = ["author_id", "confidence", "comment"]
    export_columns = SSSOM_MATCHING_TABLE_COLUMNS + minimum_columns + [
        col for col in PROVENANCE_DEFAULT_COLUMNS if col not in minimum_columns and col in out.columns
    ] + [
        col for col in JUSTIFICATION_EXTENSION_COLUMNS if col in out.columns
    ]
    # Preserve column order while removing duplicates from concatenation.
    unique_export_columns: List[str] = []
    for col in export_columns:
        if col in out.columns and col not in unique_export_columns:
            unique_export_columns.append(col)
    return out[unique_export_columns].copy()


def export_sssom_columns(df: pd.DataFrame, include_extensions: bool = True) -> pd.DataFrame:
    """Backward-compatible export helper returning strict canonical SSSOM columns."""
    finalized = finalize_accepted_results(df)
    if include_extensions:
        return finalized
    return finalized[SSSOM_MATCHING_TABLE_COLUMNS].copy()


def export_curated_sssom_table(df: pd.DataFrame) -> pd.DataFrame:
    """Return a slim, SSSOM-first export for final curated mappings.

    Includes strict SSSOM core columns plus a small optional allow-list.
    Provider/review context remains available in the working dataframe and
    separate review exports, but not as SSSOM core columns.
    """
    out = ensure_standard_reconciliation_columns(df).copy()

    if "mapping_provider" not in out.columns:
        out["mapping_provider"] = ""
    if "comment" not in out.columns:
        out["comment"] = ""
    if "confidence" not in out.columns:
        out["confidence"] = ""
    if "object_source" not in out.columns:
        out["object_source"] = ""
    if "mapping_tool" not in out.columns:
        out["mapping_tool"] = ""
    if "match_string" not in out.columns:
        out["match_string"] = ""

    # Pragmatic migration rule for current app state:
    # "Source Provider" describes the hit supplier and is mapped to
    # `mapping_provider` for the final SSSOM output if no explicit value exists.
    if "Source Provider" in out.columns:
        source_provider_series = out["Source Provider"].fillna("").astype(str).str.strip()
        existing_mapping_provider = out["mapping_provider"].fillna("").astype(str).str.strip()
        fill_mask = existing_mapping_provider.eq("")
        out.loc[fill_mask, "mapping_provider"] = source_provider_series[fill_mask]

    # Migrate provider term into object_label where object_label is still empty.
    if "Provider Term" in out.columns:
        provider_term_series = out["Provider Term"].fillna("").astype(str).str.strip()
        object_label_series = out["object_label"].fillna("").astype(str).str.strip()
        fill_mask = object_label_series.eq("") & provider_term_series.ne("")
        out.loc[fill_mask, "object_label"] = provider_term_series[fill_mask]

    # Normalize unresolved values for final export.
    no_match_mask = out["object_id"].fillna("").astype(str).str.strip().str.lower().eq("no match")
    if no_match_mask.any():
        out.loc[no_match_mask, "object_id"] = ""

    for idx in out.index:
        apply_mapping_justification_for_row(out, idx)

    for col in CURATED_SSSOM_EXPORT_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    return out[CURATED_SSSOM_EXPORT_COLUMNS].copy()


def reorder_reconciliation_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Place canonical columns first and keep working columns after them."""
    out = ensure_standard_reconciliation_columns(df)
    ordered = [c for c in SSSOM_MATCHING_TABLE_COLUMNS + STANDARD_RECONCILIATION_COLUMNS if c in out.columns]
    extras = [c for c in out.columns if c not in ordered]
    return out[ordered + extras].copy()
