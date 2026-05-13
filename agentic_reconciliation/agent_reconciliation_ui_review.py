# -*- coding: utf-8 -*-
"""Human-review actions and review display helpers for agent reconciliation."""

from typing import Dict, List, Optional

import pandas as pd

try:
    from .agent_runtime_state import runtime_state
except ImportError:
    from agent_runtime_state import runtime_state

try:
    from .agent_skos_service import normalize_mapping_type
    from .agent_reconciliation_ui_state import (
        AGENT_RESULTS_BY_SOURCE_KEY,
        _update_result_for_source,
    )
    from semi_automatic_reconciliation.shared_table_io import (
        REVIEW_MAPPING_JUSTIFICATION,
        SEMANTIC_MAPPING_JUSTIFICATION,
        apply_mapping_justification_for_row,
        sync_matching_table_schemas,
    )
except ImportError:
    from agent_skos_service import normalize_mapping_type
    from agent_reconciliation_ui_state import (
        AGENT_RESULTS_BY_SOURCE_KEY,
        _update_result_for_source,
    )
    from semi_automatic_reconciliation.shared_table_io import (
        REVIEW_MAPPING_JUSTIFICATION,
        SEMANTIC_MAPPING_JUSTIFICATION,
        apply_mapping_justification_for_row,
        sync_matching_table_schemas,
    )

REVIEW_MATCH_GROUP_ORDER = (
    "skos:exactMatch",
    "skos:closeMatch",
    "skos:relatedMatch",
    "no_match",
)

REVIEW_MATCH_TYPE_OPTIONS = (
    "skos:exactMatch",
    "skos:closeMatch",
    "skos:relatedMatch",
)

REVIEW_MATCH_GROUP_LABELS = {
    "skos:exactMatch": "skos:exactMatch",
    "skos:closeMatch": "skos:closeMatch",
    "skos:relatedMatch": "skos:relatedMatch",
    "no_match": "No match",
}

REVIEW_MATCH_GROUP_BADGE_COLORS = {
    "skos:exactMatch": ("#dbeafe", "#1d4ed8"),
    "skos:closeMatch": ("#dcfce7", "#166534"),
    "skos:relatedMatch": ("#ffedd5", "#9a3412"),
    "no_match": ("#f3f4f6", "#374151"),
}


def _apply_review_action(source_name: str, row_index, action: str, selected_match_type: Optional[str] = None):
    results = runtime_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})
    if source_name not in results:
        return
    df = sync_matching_table_schemas(results[source_name].copy())

    allow_heuristic_fallback = bool(runtime_state.get("agent_allow_heuristic_fallback", True))

    if action == "accept":
        chosen_uri = str(df.at[row_index, "Suggested URI"])
        chosen_provider = str(df.at[row_index, "Suggested Provider"])
        chosen_label = str(df.at[row_index, "Suggested Label"])
        chosen_description = str(df.at[row_index, "Suggested Description"])
        suggested_match_type = normalize_mapping_type(str(df.at[row_index, "Suggested Match Type"]))
        override_match_type = normalize_mapping_type(str(selected_match_type or ""))
        chosen_match_type = (
            override_match_type
            if override_match_type in REVIEW_MATCH_TYPE_OPTIONS
            else suggested_match_type
        )

        df.at[row_index, "URI"] = chosen_uri
        df.at[row_index, "object_id"] = chosen_uri
        df.at[row_index, "Source Provider"] = chosen_provider
        df.at[row_index, "source_provider"] = chosen_provider
        df.at[row_index, "Provider Term"] = chosen_label
        df.at[row_index, "provider_term"] = chosen_label
        df.at[row_index, "Confirmed Display String"] = chosen_label
        df.at[row_index, "confirmed_display_string"] = chosen_label
        df.at[row_index, "Provider Description"] = chosen_description
        df.at[row_index, "provider_description"] = chosen_description
        df.at[row_index, "object_label"] = chosen_label
        df.at[row_index, "comment"] = chosen_description
        df.at[row_index, "Match Type"] = chosen_match_type
        df.at[row_index, "match_type"] = chosen_match_type
        df.at[row_index, "predicate_id"] = chosen_match_type
        suggested_confidence = float(df.at[row_index, "Suggested Confidence"] or 0.0)
        suggested_decision_source = str(df.at[row_index, "Suggested Decision Source"] or "").strip().lower()
        apply_mapping_justification_for_row(
            df,
            row_index,
            default_when_mapped=(
                SEMANTIC_MAPPING_JUSTIFICATION
                if (
                    suggested_decision_source == "llm"
                    or (suggested_decision_source == "heuristic_fallback" and allow_heuristic_fallback)
                    or (suggested_confidence > 0.0 and suggested_decision_source != "heuristic_fallback")
                )
                else REVIEW_MAPPING_JUSTIFICATION
            ),
            no_match_uri="No Match",
            force_when_mapped=True,
        )
        df.at[row_index, "Review Status"] = "accepted"
        df.at[row_index, "Auto Accepted"] = False
        df.at[row_index, "Auto Accepted At"] = ""
        df.at[row_index, "Auto Accept Reason"] = "accepted_by_user"
    elif action == "reject":
        df.at[row_index, "Review Status"] = "rejected"
        df.at[row_index, "Auto Accepted"] = False
        df.at[row_index, "Auto Accepted At"] = ""
        df.at[row_index, "Auto Accept Reason"] = "rejected_by_user"
    elif action == "reset":
        df.at[row_index, "Review Status"] = "pending"
        df.at[row_index, "URI"] = ""
        df.at[row_index, "object_id"] = ""
        df.at[row_index, "Source Provider"] = ""
        df.at[row_index, "source_provider"] = ""
        df.at[row_index, "Provider Term"] = ""
        df.at[row_index, "provider_term"] = ""
        df.at[row_index, "Provider Description"] = ""
        df.at[row_index, "provider_description"] = ""
        df.at[row_index, "Confirmed Display String"] = ""
        df.at[row_index, "confirmed_display_string"] = ""
        df.at[row_index, "object_label"] = ""
        df.at[row_index, "comment"] = ""
        df.at[row_index, "Match Type"] = ""
        df.at[row_index, "match_type"] = ""
        df.at[row_index, "predicate_id"] = ""
        apply_mapping_justification_for_row(
            df,
            row_index,
            default_when_mapped=REVIEW_MAPPING_JUSTIFICATION,
            no_match_uri="No Match",
            force_when_mapped=False,
        )
        df.at[row_index, "Auto Accepted"] = False
        df.at[row_index, "Auto Accepted At"] = ""
        df.at[row_index, "Auto Accept Reason"] = "revoked_by_user"

    df = sync_matching_table_schemas(df)
    _update_result_for_source(source_name, df)


def _accept_all_pending(source_name: str):
    results = runtime_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})
    if source_name not in results:
        return
    df = sync_matching_table_schemas(results[source_name].copy())
    allow_heuristic_fallback = bool(runtime_state.get("agent_allow_heuristic_fallback", True))
    pending_indices = list(
        df[
            (df.get("Suggested URI", "").astype(str).str.strip() != "") &
            (df.get("Review Status", "").astype(str).isin(["", "pending", "matched"]))
        ].index
    )
    if not allow_heuristic_fallback:
        pending_indices = [
            idx
            for idx in pending_indices
            if str(df.at[idx, "Suggested Decision Source"] or "").strip().lower() != "heuristic_fallback"
        ]
    for idx in pending_indices:
        chosen_uri = str(df.at[idx, "Suggested URI"])
        chosen_provider = str(df.at[idx, "Suggested Provider"])
        chosen_label = str(df.at[idx, "Suggested Label"])
        chosen_description = str(df.at[idx, "Suggested Description"])
        chosen_match_type = normalize_mapping_type(str(df.at[idx, "Suggested Match Type"]))

        df.at[idx, "URI"] = chosen_uri
        df.at[idx, "object_id"] = chosen_uri
        df.at[idx, "Source Provider"] = chosen_provider
        df.at[idx, "source_provider"] = chosen_provider
        df.at[idx, "Provider Term"] = chosen_label
        df.at[idx, "provider_term"] = chosen_label
        df.at[idx, "Provider Description"] = chosen_description
        df.at[idx, "provider_description"] = chosen_description
        df.at[idx, "Confirmed Display String"] = chosen_label
        df.at[idx, "confirmed_display_string"] = chosen_label
        df.at[idx, "object_label"] = chosen_label
        df.at[idx, "comment"] = chosen_description
        df.at[idx, "Match Type"] = chosen_match_type
        df.at[idx, "match_type"] = chosen_match_type
        df.at[idx, "predicate_id"] = chosen_match_type
        suggested_confidence = float(df.at[idx, "Suggested Confidence"] or 0.0)
        suggested_decision_source = str(df.at[idx, "Suggested Decision Source"] or "").strip().lower()
        apply_mapping_justification_for_row(
            df,
            idx,
            default_when_mapped=(
                SEMANTIC_MAPPING_JUSTIFICATION
                if (
                    suggested_decision_source == "llm"
                    or (suggested_decision_source == "heuristic_fallback" and allow_heuristic_fallback)
                    or (suggested_confidence > 0.0 and suggested_decision_source != "heuristic_fallback")
                )
                else REVIEW_MAPPING_JUSTIFICATION
            ),
            no_match_uri="No Match",
            force_when_mapped=True,
        )
        df.at[idx, "Review Status"] = "accepted"
        df.at[idx, "Auto Accepted"] = False
        df.at[idx, "Auto Accepted At"] = ""
        df.at[idx, "Auto Accept Reason"] = "accepted_all_pending_by_user"
    df = sync_matching_table_schemas(df)
    _update_result_for_source(source_name, df)


def _normalize_review_match_group(mapping_type: object) -> str:
    normalized = normalize_mapping_type(str(mapping_type or "").strip())
    if normalized in REVIEW_MATCH_GROUP_ORDER[:-1]:
        return normalized
    return "no_match"


def _group_pending_review_indices_by_match_type(agent_df: pd.DataFrame, pending_indices: List[int]) -> Dict[str, List[int]]:
    grouped: Dict[str, List[int]] = {group: [] for group in REVIEW_MATCH_GROUP_ORDER}
    if not isinstance(agent_df, pd.DataFrame):
        return grouped

    for idx in pending_indices:
        suggested_match_type = ""
        if "Suggested Match Type" in agent_df.columns:
            suggested_match_type = agent_df.at[idx, "Suggested Match Type"]
        group = _normalize_review_match_group(suggested_match_type)
        grouped.setdefault(group, []).append(idx)

    return grouped


def _get_reviewable_agent_result_indices(agent_df: pd.DataFrame) -> List[int]:
    """Return rows that should be shown in section 5 for human review.

    Suggested mappings have a Suggested URI. No-match outcomes do not, but they
    are still reconciliation results and should appear in the No match category
    with their explanation/fallback reason instead of only being visible in
    telemetry.
    """
    if not isinstance(agent_df, pd.DataFrame):
        return []

    review_status_source = (
        agent_df["Review Status"]
        if "Review Status" in agent_df.columns
        else pd.Series([""] * len(agent_df), index=agent_df.index)
    )
    suggested_uri_source = (
        agent_df["Suggested URI"]
        if "Suggested URI" in agent_df.columns
        else pd.Series([""] * len(agent_df), index=agent_df.index)
    )
    review_status = review_status_source.astype(str).str.strip().str.lower()
    suggested_uri = suggested_uri_source.astype(str).str.strip()
    terminal_no_match_status = review_status.isin(["no_match", "timeout"])

    reviewable_mask = (
        (suggested_uri != "") | terminal_no_match_status
    ) & (~review_status.isin(["accepted", "rejected"]))
    return list(agent_df[reviewable_mask].index)


def _get_review_cell_value(agent_df: pd.DataFrame, row_index, column_name: str) -> str:
    if not isinstance(agent_df, pd.DataFrame) or column_name not in agent_df.columns:
        return ""
    try:
        return str(agent_df.at[row_index, column_name])
    except Exception:
        return ""


def _render_skos_match_badge(match_group: str):
    """Return badge metadata for the MUI frontend.

    Legacy Python UI rendering has been removed; callers that still import this
    helper receive a serializable description instead of rendered HTML.
    """
    group_key = match_group if match_group in REVIEW_MATCH_GROUP_LABELS else "no_match"
    background_color, text_color = REVIEW_MATCH_GROUP_BADGE_COLORS.get(
        group_key,
        REVIEW_MATCH_GROUP_BADGE_COLORS["no_match"],
    )
    return {
        "group": group_key,
        "label": REVIEW_MATCH_GROUP_LABELS.get(group_key, "No match"),
        "background_color": background_color,
        "text_color": text_color,
    }
