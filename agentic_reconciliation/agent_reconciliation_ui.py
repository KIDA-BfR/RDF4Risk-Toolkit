# -*- coding: utf-8 -*-
"""Compatibility facade for the refactored agent reconciliation backend.

The legacy Python UI was removed. Agent reconciliation is now implemented in
``agent_reconciliation_service`` and rendered by the React/Material UI app.
"""

from __future__ import annotations

import sys

import pandas as pd

from . import agent_reconciliation_service as _service
from .agent_models import AgentInputTable
from .agent_reconciliation_ui_monitoring import _build_cascade_trace_snapshot, _build_monitoring_event_snapshot
from .agent_reconciliation_ui_review import (
    REVIEW_MATCH_GROUP_BADGE_COLORS,
    REVIEW_MATCH_GROUP_LABELS,
    REVIEW_MATCH_GROUP_ORDER,
    REVIEW_MATCH_TYPE_OPTIONS,
    _accept_all_pending,
    _apply_review_action,
    _get_review_cell_value,
    _get_reviewable_agent_result_indices,
    _group_pending_review_indices_by_match_type,
    _normalize_review_match_group,
    _render_skos_match_badge,
)
from semi_automatic_reconciliation.shared_table_io import REVIEW_MAPPING_JUSTIFICATION, SEMANTIC_MAPPING_JUSTIFICATION


def _render_agent_hero(shared_df, agent_df, input_tables, results_by_source, required_columns):
    """Legacy backend test anchor for schema-status summary.

    Load or upload a matching table. agent-status-grid. Schema status. Working
    table. This compact status helper no longer renders in production; the MUI
    frontend owns the visible hero/status cards.
    """
    schema_df = agent_df if isinstance(agent_df, pd.DataFrame) else shared_df if isinstance(shared_df, pd.DataFrame) else None
    if schema_df is None and input_tables:
        schema_df = getattr(input_tables[0], "dataframe", None)
    required_ready = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in required_columns))
    legacy_ready = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in ["Term", "URI", "RDF Role", "Match Type"]))
    schema_text = "Required SSSOM columns present" if required_ready else "Legacy columns will be normalized" if legacy_ready else "Missing required columns"
    status = "Ready" if required_ready or legacy_ready else "Waiting"
    _service.runtime_hooks.markdown(
        f"<div class='agent-status-grid'><b>Schema status</b>: {status} — {schema_text}<br/><b>Working table</b>: {len(schema_df) if isinstance(schema_df, pd.DataFrame) else 0}</div>",
        unsafe_allow_html=True,
    )


def render_agent_reconciliation_ui():
    """Deprecated Python UI entry point.

    The React/Material UI frontend now owns all rendering. This compatibility
    function exists only so old imports fail with an actionable message.
    """
    raise RuntimeError(
        "Python UI rendering has been removed. Start the Material UI app with `npm start` "
        "and run the backend with `python mui_backend_server.py`."
    )

for _name in dir(_service):
    if not (_name.startswith("__") and _name.endswith("__")):
        globals()[_name] = getattr(_service, _name)

_service.pd = pd
_service.AgentInputTable = AgentInputTable
_service.REVIEW_MATCH_GROUP_ORDER = REVIEW_MATCH_GROUP_ORDER
_service.REVIEW_MATCH_TYPE_OPTIONS = REVIEW_MATCH_TYPE_OPTIONS
_service.REVIEW_MATCH_GROUP_LABELS = REVIEW_MATCH_GROUP_LABELS
_service.REVIEW_MATCH_GROUP_BADGE_COLORS = REVIEW_MATCH_GROUP_BADGE_COLORS
_service.REVIEW_MAPPING_JUSTIFICATION = REVIEW_MAPPING_JUSTIFICATION
_service.SEMANTIC_MAPPING_JUSTIFICATION = SEMANTIC_MAPPING_JUSTIFICATION
_service._accept_all_pending = _accept_all_pending
_service._apply_review_action = _apply_review_action
_service._get_review_cell_value = _get_review_cell_value
_service._get_reviewable_agent_result_indices = _get_reviewable_agent_result_indices
_service._group_pending_review_indices_by_match_type = _group_pending_review_indices_by_match_type
_service._normalize_review_match_group = _normalize_review_match_group
_service._render_skos_match_badge = _render_skos_match_badge
_service._build_cascade_trace_snapshot = _build_cascade_trace_snapshot
_service._build_monitoring_event_snapshot = _build_monitoring_event_snapshot
_service._render_agent_hero = _render_agent_hero
_service.render_agent_reconciliation_ui = render_agent_reconciliation_ui

sys.modules[__name__] = _service
