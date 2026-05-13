# -*- coding: utf-8 -*-
"""Session-state and input-table helpers for the agent reconciliation UI."""

from typing import Dict, List

import pandas as pd
import streamlit as st

try:
    from .agent_models import AgentInputTable
    from semi_automatic_reconciliation.shared_table_io import (
        REQUIRED_MATCHING_TABLE_COLUMNS,
        ensure_agent_output_columns,
        extract_all_terms_for_reconciliation,
        get_unreconciled_indices,
        prepare_loaded_matching_table,
        reorder_reconciliation_columns,
        sync_matching_table_schemas,
    )
except ImportError:
    from agent_models import AgentInputTable
    from semi_automatic_reconciliation.shared_table_io import (
        REQUIRED_MATCHING_TABLE_COLUMNS,
        ensure_agent_output_columns,
        extract_all_terms_for_reconciliation,
        get_unreconciled_indices,
        prepare_loaded_matching_table,
        reorder_reconciliation_columns,
        sync_matching_table_schemas,
    )

AGENT_DATAFRAME_STATE_KEY = "agent_reconciliation_df"
AGENT_DATA_SOURCE_MESSAGE_KEY = "agent_reconciliation_source_message"
AGENT_LAST_SOURCE_NAME_KEY = "agent_reconciliation_last_source_name"
AGENT_INPUT_TABLES_KEY = "agent_reconciliation_input_tables"
AGENT_RESULTS_BY_SOURCE_KEY = "agent_reconciliation_results_by_source"
AGENT_SELECTED_SOURCE_KEY = "agent_reconciliation_selected_source"
AGENT_DEFINITIONS_BY_SOURCE_KEY = "agent_reconciliation_definitions_by_source"
AGENT_RUN_MESSAGES_KEY = "agent_reconciliation_run_messages"
AGENT_STOP_EVENT_KEY = "agent_reconciliation_stop_event"


def _sync_selected_source_dataframe():
    selected_source = st.session_state.get(AGENT_SELECTED_SOURCE_KEY)
    results_by_source = st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})
    input_tables = st.session_state.get(AGENT_INPUT_TABLES_KEY, [])

    if selected_source and selected_source in results_by_source:
        st.session_state[AGENT_DATAFRAME_STATE_KEY] = results_by_source[selected_source]
        st.session_state["all_terms_for_reconciliation"] = extract_all_terms_for_reconciliation(results_by_source[selected_source])
        st.session_state["agent_total_indices_to_process"] = get_unreconciled_indices(results_by_source[selected_source], "No Match")
        return

    if selected_source:
        for table in input_tables:
            if table.source_name == selected_source:
                st.session_state[AGENT_DATAFRAME_STATE_KEY] = ensure_agent_output_columns(table.dataframe)
                st.session_state["all_terms_for_reconciliation"] = extract_all_terms_for_reconciliation(table.dataframe)
                st.session_state["agent_total_indices_to_process"] = get_unreconciled_indices(table.dataframe, "No Match")
                return


def _store_input_tables(tables: List[AgentInputTable], source_message: str):
    st.session_state[AGENT_INPUT_TABLES_KEY] = tables
    st.session_state[AGENT_RESULTS_BY_SOURCE_KEY] = {}
    st.session_state[AGENT_DEFINITIONS_BY_SOURCE_KEY] = {}
    st.session_state[AGENT_RUN_MESSAGES_KEY] = []
    st.session_state[AGENT_STOP_EVENT_KEY] = {}

    if tables:
        st.session_state[AGENT_SELECTED_SOURCE_KEY] = tables[0].source_name
        st.session_state[AGENT_DATAFRAME_STATE_KEY] = ensure_agent_output_columns(tables[0].dataframe)
        st.session_state[AGENT_LAST_SOURCE_NAME_KEY] = tables[0].filename
        st.session_state["all_terms_for_reconciliation"] = extract_all_terms_for_reconciliation(tables[0].dataframe)
        st.session_state["agent_total_indices_to_process"] = get_unreconciled_indices(tables[0].dataframe, "No Match")
    else:
        st.session_state[AGENT_SELECTED_SOURCE_KEY] = None
        st.session_state[AGENT_DATAFRAME_STATE_KEY] = None
        st.session_state[AGENT_LAST_SOURCE_NAME_KEY] = None
        st.session_state["all_terms_for_reconciliation"] = []
        st.session_state["agent_total_indices_to_process"] = []

    st.session_state[AGENT_DATA_SOURCE_MESSAGE_KEY] = source_message


def _build_run_input_tables(
    input_tables: List[AgentInputTable],
    results_by_source: Dict[str, pd.DataFrame],
    *,
    resume_previous: bool,
) -> List[AgentInputTable]:
    if not resume_previous:
        return input_tables

    run_tables: List[AgentInputTable] = []
    for table in input_tables:
        candidate_df = results_by_source.get(table.source_name) if isinstance(results_by_source, dict) else None
        if isinstance(candidate_df, pd.DataFrame):
            run_tables.append(
                AgentInputTable(
                    source_name=table.source_name,
                    filename=table.filename,
                    dataframe=candidate_df.copy(),
                    sheet_name=table.sheet_name,
                    is_from_shared_matching_table=table.is_from_shared_matching_table,
                )
            )
        else:
            run_tables.append(table)
    return run_tables


def _update_result_for_source(source_name: str, df: pd.DataFrame):
    results = st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})
    results[source_name] = reorder_reconciliation_columns(sync_matching_table_schemas(df))
    st.session_state[AGENT_RESULTS_BY_SOURCE_KEY] = results
    if st.session_state.get(AGENT_SELECTED_SOURCE_KEY) == source_name:
        _sync_selected_source_dataframe()

def _reset_agent_state_and_load_df(df_to_load: pd.DataFrame, source_name_msg: str) -> bool:
    validation_missing = [col for col in REQUIRED_MATCHING_TABLE_COLUMNS if col not in df_to_load.columns]
    if validation_missing:
        st.error(
            "Loaded data is missing required columns: "
            + ", ".join(validation_missing)
        )
        return False

    prepared_df, total_indices, all_terms = prepare_loaded_matching_table(df_to_load.copy().fillna(""), "No Match")
    prepared_df = ensure_agent_output_columns(prepared_df)

    st.session_state[AGENT_DATAFRAME_STATE_KEY] = prepared_df
    st.session_state[AGENT_LAST_SOURCE_NAME_KEY] = source_name_msg
    st.session_state[AGENT_DATA_SOURCE_MESSAGE_KEY] = (
        f"Agent-based reconciliation data successfully loaded from: {source_name_msg}."
    )
    st.session_state["all_terms_for_reconciliation"] = all_terms
    st.session_state["agent_total_indices_to_process"] = total_indices
    return True
