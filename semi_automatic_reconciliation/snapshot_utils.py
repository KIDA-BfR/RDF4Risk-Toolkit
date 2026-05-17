# -*- coding: utf-8 -*-
"""JSON-safe snapshot helpers shared by backend services."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


def json_safe_value(value: object):
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def dataframe_records(df: Optional[pd.DataFrame], limit: int = 25) -> List[Dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    preview = df.head(limit).copy()
    preview = preview.where(pd.notna(preview), "")
    return [
        {str(key): json_safe_value(value) for key, value in row.items()}
        for row in preview.to_dict(orient="records")
    ]
