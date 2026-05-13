# -*- coding: utf-8 -*-
"""File-loading helpers for agent-based reconciliation inputs."""

from __future__ import annotations

import os
from typing import Iterable, List, Optional

import pandas as pd

from .agent_models import AgentInputTable
from semi_automatic_reconciliation.shared_table_io import validate_matching_table


def read_matching_table_upload(uploaded_file) -> pd.DataFrame:
    """Read a CSV/XLS/XLSX matching-table upload into a DataFrame."""
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        uploaded_file.seek(0)
        return pd.read_csv(uploaded_file).fillna("")
    if name.endswith((".xlsx", ".xls")):
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file).fillna("")
    raise ValueError(f"Unsupported file type for matching table upload: {uploaded_file.name}")


def make_input_table(
    dataframe: pd.DataFrame,
    source_name: str,
    filename: str,
    *,
    sheet_name: Optional[str] = None,
    is_from_shared_matching_table: bool = False,
) -> AgentInputTable:
    validation = validate_matching_table(dataframe)
    if not validation.is_valid:
        raise ValueError(
            "Matching table is missing required columns: " + ", ".join(validation.missing_columns)
        )
    return AgentInputTable(
        source_name=source_name,
        filename=filename,
        dataframe=dataframe.copy(),
        sheet_name=sheet_name,
        is_from_shared_matching_table=is_from_shared_matching_table,
    )


def load_uploaded_input_tables(uploaded_files: Iterable) -> List[AgentInputTable]:
    tables: List[AgentInputTable] = []
    for uploaded_file in uploaded_files or []:
        dataframe = read_matching_table_upload(uploaded_file)
        source_name = os.path.splitext(uploaded_file.name)[0]
        tables.append(make_input_table(dataframe, source_name, uploaded_file.name))
    return tables
