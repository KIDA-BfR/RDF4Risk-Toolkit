# -*- coding: utf-8 -*-
"""Backend service for the Material-UI Matching Table Generator.

This module owns data loading, preprocessing, matching-table generation, and
JSON snapshot/event handling for the browser-based Material UI frontend.  It is
intentionally free of UI framework dependencies so it can run as a plain Python
backend service.
"""

from __future__ import annotations

import base64
import csv
import io
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from dateutil.parser import parse

try:  # pragma: no cover - optional speedup dependency can vary locally
    import Levenshtein  # type: ignore
except Exception:  # pragma: no cover
    Levenshtein = None

try:
    from semi_automatic_reconciliation.shared_table_io import SSSOM_MATCHING_TABLE_COLUMNS, sync_matching_table_schemas
except ImportError:  # pragma: no cover - direct execution fallback
    from semi_automatic_reconciliation.shared_table_io import SSSOM_MATCHING_TABLE_COLUMNS, sync_matching_table_schemas

LOGGER = logging.getLogger(__name__)

MTG_COMPONENT_ACTION_NONCE_KEY = "matching_table_generator_component_action_nonce"
MTG_STATUS_MESSAGE_KEY = "matching_table_generator_status_message"
MTG_UPLOADED_FILE_BYTES_KEY = "mtg_uploaded_file_bytes"
MTG_UPLOADED_FILE_NAME_KEY = "mtg_uploaded_file_name"
MTG_UPLOADED_FILE_SIZE_KEY = "mtg_uploaded_file_size"
MTG_EXPAND_DETECTED_CODES_KEY = "mtg_expand_detected_codes"
MTG_EXPAND_DETECTED_COLUMN_KEY = "mtg_expand_detected_column"
MTG_EXPAND_DETECTED_DELIMITER_KEY = "mtg_expand_detected_delimiter"

REQUIRED_MATCHING_COLUMNS = SSSOM_MATCHING_TABLE_COLUMNS.copy()

DEFAULT_STATE: Dict[str, Any] = {
    "original_df": None,
    "df_after_transformations": None,
    "matching_df": None,
    "prepared_split_config": {},
    "prepared_expand_config": {},
    "keep_original_setting_split": True,
    "keep_original_setting_expand": True,
    "current_start_row": 1,
    "uploaded_file_info": None,
    "load_error": None,
    "transformations_prepared": False,
    "preprocessing_applied_in_last_run": False,
    "omitted_columns_selection": [],
    "selected_sheet": None,
    "available_sheets": [],
    "similar_term_groups": [],
    "user_choices_for_similar_terms": {},
    "show_consolidation_review_ui": False,
    "consolidations_staged_for_generation": False,
    "levenshtein_threshold": 0.85,
}


@dataclass
class GeneratedTableResult:
    matching_df: Optional[pd.DataFrame]
    preprocessed_df: Optional[pd.DataFrame]
    preprocessing_applied: bool
    actions: List[str]
    consolidations_applied_count: int = 0
    error: Optional[str] = None


_STATE: Dict[str, Any] = {}


def _clone_default(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, dict):
        return value.copy()
    if isinstance(value, list):
        return value.copy()
    return value


def _init_state() -> None:
    """Initialise backend-local state used by the MUI HTTP service."""
    for key, value in DEFAULT_STATE.items():
        if key not in _STATE:
            _STATE[key] = _clone_default(value)


def _set_status(severity: str, text: str) -> None:
    _STATE[MTG_STATUS_MESSAGE_KEY] = {"severity": severity, "text": text}


def _reset_data_dependent_state(clear_file: bool = False) -> None:
    _STATE["original_df"] = None
    _STATE["df_after_transformations"] = None
    _STATE["matching_df"] = None
    _STATE["prepared_split_config"] = {}
    _STATE["prepared_expand_config"] = {}
    _STATE["transformations_prepared"] = False
    _STATE["preprocessing_applied_in_last_run"] = False
    _STATE["omitted_columns_selection"] = []
    _STATE["similar_term_groups"] = []
    _STATE["user_choices_for_similar_terms"] = {}
    _STATE["show_consolidation_review_ui"] = False
    _STATE["consolidations_staged_for_generation"] = False
    _STATE[MTG_EXPAND_DETECTED_CODES_KEY] = []
    if clear_file:
        for key in (
            MTG_UPLOADED_FILE_BYTES_KEY,
            MTG_UPLOADED_FILE_NAME_KEY,
            MTG_UPLOADED_FILE_SIZE_KEY,
            "uploaded_file_info",
        ):
            _STATE.pop(key, None)
        _STATE["available_sheets"] = []
        _STATE["selected_sheet"] = None
        _STATE["current_start_row"] = 1


def _dataframe_preview_records(df: Optional[pd.DataFrame], limit: int = 8) -> List[Dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    safe_df = df.head(limit).copy().fillna("")
    return safe_df.astype(str).to_dict(orient="records")


def _dataframe_csv(df: Optional[pd.DataFrame]) -> str:
    if not isinstance(df, pd.DataFrame):
        return ""
    return df.to_csv(index=False, encoding="utf-8-sig")


def _dataframe_xlsx_base64(df: Optional[pd.DataFrame], sheet_name: str = "Data") -> str:
    if not isinstance(df, pd.DataFrame):
        return ""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31] or "Data")
    return base64.b64encode(output.getvalue()).decode("ascii")


def detect_delimiter_from_bytes(file_bytes: bytes) -> str:
    """Sniff the delimiter from the uploaded CSV bytes."""
    try:
        sample_bytes = file_bytes[:4096]
        sample = None
        for enc in ("utf-8", "latin-1", "windows-1252"):
            try:
                sample = sample_bytes.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if sample is None:
            sample = sample_bytes.decode("utf-8", errors="ignore")
        sample = sample.lstrip("\ufeff")
        if not sample.strip():
            return ","
        lines = sample.splitlines()
        sample_for_sniffing = "\n".join(lines[:5])
        try:
            return csv.Sniffer().sniff(sample_for_sniffing, delimiters=[",", ";", "\t", "|"]).delimiter
        except csv.Error:
            if not lines:
                return ","
            counts = Counter(lines[0])
            if counts.get(";") > counts.get(",") and counts.get(";") > 0:
                return ";"
            if counts.get("\t") > counts.get(",") and counts.get("\t") > 0:
                return "\t"
            if counts.get("|") > counts.get(",") and counts.get("|") > 0:
                return "|"
            return ","
    except Exception as exc:  # pragma: no cover - defensive fallback
        LOGGER.warning("Delimiter detection failed: %s", exc, exc_info=True)
        return ","


def is_potentially_numeric(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    cleaned = text.replace(",", "")
    if "." in text and "," in text:
        cleaned = text.replace(",", "") if text.rfind(".") > text.rfind(",") else text.replace(".", "").replace(",", ".")
    try:
        float(cleaned)
        return True
    except ValueError:
        return False


def is_probably_date(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text or len(text) < 5 or text.isdigit():
        return False
    if re.match(r"^\d+(\.\d+)+$", text) or re.match(r"^\d+[,.]\d+$", text):
        return False
    if re.match(r"^[A-Z0-9_-]{2,6}$", text):
        return False
    try:
        parse(text, fuzzy=False)
        if re.match(r"^[A-Za-z]+\s+\d{4}$", text):
            return False
        return True
    except (ValueError, OverflowError, TypeError):
        return False


def make_dataframe_arrow_compatible(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    df_fixed = df.copy()

    def decode_possible_bytes(value: Any) -> Any:
        if isinstance(value, (bytes, bytearray, memoryview)):
            raw = bytes(value)
            for enc in ("utf-8", "latin-1", "windows-1252"):
                try:
                    return raw.decode(enc)
                except Exception:
                    continue
            return raw.decode("utf-8", errors="replace")
        return value

    for col in df_fixed.columns:
        series = df_fixed[col]
        if not pd.api.types.is_object_dtype(series):
            continue
        normalized = series.map(decode_possible_bytes)
        unique_types = {type(v) for v in normalized.dropna()}
        if len(unique_types) > 1:
            df_fixed[col] = normalized.map(lambda v: pd.NA if pd.isna(v) else str(v)).astype("string")
        else:
            df_fixed[col] = normalized
    return df_fixed


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    sanitized = []
    for idx, col_name in enumerate(df.columns):
        if pd.isna(col_name):
            name = f"Unnamed_Col_{idx}"
        else:
            name = str(col_name).replace("\n", "_").replace("\r", "_").strip()
        sanitized.append(name or f"Empty_Col_Name_{idx}")
    current = pd.Index(sanitized)
    if current.has_duplicates:
        series = pd.Series(current)
        counts = series.groupby(series).cumcount()
        df.columns = pd.Index([
            f"{series.iloc[i]}_{counts.iloc[i]}" if counts.iloc[i] > 0 else series.iloc[i]
            for i in range(len(series))
        ])
    else:
        df.columns = current
    return df


def load_dataframe_from_bytes(file_bytes: bytes, filename: str, start_row: int, sheet_name: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        start_row = max(1, int(start_row or 1))
        lower = filename.lower()
        if lower.endswith(".csv"):
            delimiter = detect_delimiter_from_bytes(file_bytes)
            df = pd.read_csv(
                io.BytesIO(file_bytes),
                skiprows=start_row - 1,
                skipinitialspace=True,
                delimiter=delimiter,
                keep_default_na=False,
                na_values=["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null", "-"],
                encoding="utf-8",
                low_memory=False,
            )
        elif lower.endswith((".xlsx", ".xls")):
            df = pd.read_excel(
                io.BytesIO(file_bytes),
                sheet_name=sheet_name if sheet_name else 0,
                skiprows=start_row - 1,
                keep_default_na=False,
                na_values=["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null", "-"],
                engine="openpyxl" if lower.endswith(".xlsx") else None,
            )
        else:
            return None, "Unsupported file format. Please upload CSV, XLSX, or XLS."
        df = sanitize_columns(df)
        df = make_dataframe_arrow_compatible(df)
        return df.copy(), None
    except Exception as exc:
        LOGGER.error("Error loading file %s: %s", filename, exc, exc_info=True)
        return None, f"Error loading file '{filename}' from row {start_row}: {exc}"


def get_excel_sheet_names(file_bytes: bytes, filename: str) -> List[str]:
    if not filename.lower().endswith((".xlsx", ".xls")):
        return []
    try:
        excel_file = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl" if filename.lower().endswith(".xlsx") else None)
        sheets = list(excel_file.sheet_names)
        excel_file.close()
        return sheets
    except Exception as exc:
        LOGGER.warning("Could not read Excel sheet names: %s", exc, exc_info=True)
        return []


def _load_current_uploaded_file() -> None:
    file_bytes = _STATE.get(MTG_UPLOADED_FILE_BYTES_KEY)
    filename = _STATE.get(MTG_UPLOADED_FILE_NAME_KEY)
    if not isinstance(file_bytes, bytes) or not filename:
        return
    start_row = int(_STATE.get("current_start_row", 1) or 1)
    sheet = _STATE.get("selected_sheet")
    df, error = load_dataframe_from_bytes(file_bytes, str(filename), start_row, str(sheet) if sheet else None)
    _STATE["load_error"] = error
    if error:
        _STATE["original_df"] = None
        return
    _STATE["original_df"] = df
    _STATE["df_after_transformations"] = None
    _STATE["matching_df"] = None
    _STATE["prepared_split_config"] = {}
    _STATE["prepared_expand_config"] = {}
    _STATE["transformations_prepared"] = False
    _STATE["preprocessing_applied_in_last_run"] = False
    _STATE["omitted_columns_selection"] = []
    _STATE["similar_term_groups"] = []
    _STATE["user_choices_for_similar_terms"] = {}
    _STATE["consolidations_staged_for_generation"] = False


def split_column(df: pd.DataFrame, column_name: str, delimiter: str, new_column_names: List[str]) -> pd.DataFrame:
    if column_name not in df.columns:
        raise ValueError(f"Split Error: column '{column_name}' not found.")
    if not delimiter or not new_column_names:
        raise ValueError("Split Error: delimiter and new column names are required.")
    collisions = {name for name in new_column_names if name in set(df.columns)}
    if collisions:
        raise ValueError(f"Split Error: new column name(s) already exist: {', '.join(sorted(collisions))}.")
    new_cols_data = {name: [pd.NA] * len(df) for name in new_column_names}
    cleaned_delimiter_re = r"\s*" + re.escape(delimiter) + r"\s*"
    for idx, value in df[column_name].items():
        if pd.notna(value) and value != "":
            parts = [p.strip() for p in re.split(cleaned_delimiter_re, str(value), maxsplit=len(new_column_names) - 1)]
            for i, name in enumerate(new_column_names):
                if i < len(parts):
                    new_cols_data[name][idx] = parts[i] if parts[i] else pd.NA
    return pd.concat([df, pd.DataFrame(new_cols_data, index=df.index)], axis=1)


def suggest_split_names(series: pd.Series, delimiter: str, base: str, max_parts_cap: int = 20) -> List[str]:
    if series.empty or not delimiter:
        return []
    max_parts = series.astype(str).str.split(delimiter, expand=False).str.len().max()
    try:
        max_parts_int = min(int(max_parts), max_parts_cap)
    except Exception:
        max_parts_int = 0
    return [f"{base}_{i + 1}" for i in range(max_parts_int)]


def expand_codes_to_indicators(
    df: pd.DataFrame,
    column_name: str,
    delimiter: str,
    codes_to_expand: List[str],
    new_col_prefix: str,
    true_value: Any = "True",
    false_value: Any = None,
) -> Tuple[pd.DataFrame, List[str]]:
    if column_name not in df.columns:
        raise ValueError(f"Expand Error: column '{column_name}' not found.")
    if not delimiter or not codes_to_expand:
        raise ValueError("Expand Error: delimiter and at least one code are required.")
    codes_set_lower = {str(code).strip().lower() for code in codes_to_expand}
    existing_cols = set(df.columns)
    generated = set()
    new_column_details: Dict[str, Dict[str, Any]] = {}
    for code in codes_to_expand:
        code_str = str(code).strip()
        new_name = re.sub(r"[^\w-]", "_", f"{new_col_prefix}{code_str}").strip("_")
        new_name = re.sub(r"_+", "_", new_name)
        if not new_name:
            raise ValueError(f"Expand Error: invalid generated column name for code '{code}'.")
        if new_name in existing_cols or new_name in generated:
            raise ValueError(f"Expand Error: generated column '{new_name}' conflicts with an existing/new column.")
        generated.add(new_name)
        new_column_details[code] = {"new_name": new_name, "data": [pd.NA] * len(df)}
    cleaned_delimiter_re = r"\s*" + re.escape(delimiter) + r"\s*"
    for idx, value in df[column_name].items():
        found_codes = set()
        if pd.notna(value) and value != "":
            parts = [p.strip().lower() for p in re.split(cleaned_delimiter_re, str(value)) if p.strip()]
            found_codes.update(part for part in parts if part in codes_set_lower)
        for code, details in new_column_details.items():
            is_present = str(code).strip().lower() in found_codes
            final_value = true_value if is_present else false_value
            if final_value == "$CODE$":
                final_value = code if is_present else false_value
            details["data"][idx] = pd.NA if final_value is None else final_value
    df_copy = df.copy()
    added = []
    for details in new_column_details.values():
        name = details["new_name"]
        series = pd.Series(details["data"], index=df_copy.index, name=name)
        if str(true_value).lower() == "true" and false_value is not None and str(false_value).lower() == "false":
            try:
                series = series.astype("boolean")
            except Exception:
                pass
        df_copy[name] = series
        added.append(name)
    return df_copy, added


def detect_codes_for_expansion(df: Optional[pd.DataFrame], column: str, delimiter: str) -> List[str]:
    if not isinstance(df, pd.DataFrame) or column not in df.columns or not delimiter:
        return []
    detected = set()
    cleaned_delimiter_re = r"\s*" + re.escape(delimiter) + r"\s*"
    for val in df[column].dropna().head(5000).unique():
        if pd.notna(val) and val != "":
            detected.update(part.strip() for part in re.split(cleaned_delimiter_re, str(val)) if part.strip())
    return sorted(detected, key=lambda item: item.lower())


def _normalize_split_config(raw: Any, columns: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    allowed = set(columns)
    output: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return output
    proposed_names = set()
    for column, config in raw.items():
        col = str(column)
        if col not in allowed or not isinstance(config, dict):
            continue
        delimiter = str(config.get("delimiter", ",") or ",")
        names = [str(name).strip() for name in config.get("new_names", []) if str(name).strip()]
        if not delimiter or not names:
            continue
        collisions = set(names) & (allowed | proposed_names)
        if collisions:
            raise ValueError(f"Split config for '{col}' has colliding new column name(s): {', '.join(sorted(collisions))}")
        proposed_names.update(names)
        output[col] = {"delimiter": delimiter, "new_names": names}
    return output


def _normalize_expand_config(raw: Any, columns: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    allowed = set(columns)
    output: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return output
    for column, config in raw.items():
        col = str(column)
        if col not in allowed or not isinstance(config, dict):
            continue
        delimiter = str(config.get("delimiter", ", ") or ", ")
        prefix = str(config.get("new_col_prefix", "Indicator_") or "Indicator_")
        codes = [str(code).strip() for code in config.get("codes_to_expand", []) if str(code).strip()]
        true_value = config.get("true_value", "True")
        false_value = config.get("false_value", "False")
        if not delimiter or not prefix or not codes:
            continue
        output[col] = {
            "delimiter": delimiter,
            "new_col_prefix": prefix,
            "codes_to_expand": codes,
            "true_value": true_value,
            "false_value": false_value,
        }
        break  # parity with old UI: one code-expansion column at a time
    return output


def _apply_prepared_transformations(base_df: pd.DataFrame) -> Tuple[pd.DataFrame, bool, List[str], Dict[str, Any], Dict[str, Any]]:
    df = base_df.copy()
    actions: List[str] = []
    applied_split: Dict[str, Any] = {}
    applied_expand: Dict[str, Any] = {}
    preprocessing_applied = False

    for col, config in _STATE.get("prepared_split_config", {}).items():
        if col not in df.columns:
            actions.append(f"Skipped missing split column '{col}'")
            continue
        df = split_column(df, col, config["delimiter"], config["new_names"])
        applied_split[col] = config
        preprocessing_applied = True
        actions.append(f"Split '{col}' into {', '.join(config['new_names'])}")
    if applied_split and not bool(_STATE.get("keep_original_setting_split", True)):
        drop_cols = [col for col in applied_split if col in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
            actions.append(f"Dropped original split column(s): {', '.join(drop_cols)}")

    for col, config in _STATE.get("prepared_expand_config", {}).items():
        if col not in df.columns:
            actions.append(f"Skipped missing expansion column '{col}'")
            continue
        df, added_cols = expand_codes_to_indicators(
            df,
            col,
            config["delimiter"],
            config["codes_to_expand"],
            config["new_col_prefix"],
            config["true_value"],
            config["false_value"],
        )
        applied_expand[col] = config
        preprocessing_applied = True
        actions.append(f"Expanded '{col}' into {len(added_cols)} indicator column(s)")
    if applied_expand and not bool(_STATE.get("keep_original_setting_expand", True)):
        drop_cols = [col for col in applied_expand if col in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
            actions.append(f"Dropped original expansion column(s): {', '.join(drop_cols)}")

    return df, preprocessing_applied, actions, applied_split, applied_expand


def _extract_object_terms_for_consolidation(df: pd.DataFrame) -> List[str]:
    omitted = set(_STATE.get("omitted_columns_selection", []))
    terms = set()
    for col in df.columns:
        if col in omitted:
            continue
        for val in df[col].dropna():
            text = str(val).strip()
            if text and len(text) < 250 and not is_potentially_numeric(text) and not is_probably_date(text) and text.lower() not in {"true", "false"}:
                terms.add(text)
    return sorted(terms, key=lambda item: item.lower())


def _similarity_ratio(left: str, right: str) -> float:
    if Levenshtein is not None:
        return float(Levenshtein.ratio(left.lower(), right.lower()))
    # Fallback: stdlib sequence matcher if python-Levenshtein is unavailable.
    from difflib import SequenceMatcher

    return SequenceMatcher(None, left.lower(), right.lower()).ratio()


def find_similar_term_groups(threshold: float) -> List[List[str]]:
    df = _STATE.get("original_df")
    if not isinstance(df, pd.DataFrame):
        return []
    transformed, _, _, _, _ = _apply_prepared_transformations(df)
    object_terms = _extract_object_terms_for_consolidation(transformed)
    processed = [False] * len(object_terms)
    groups: List[List[str]] = []
    for i, term in enumerate(object_terms):
        if processed[i]:
            continue
        group = {term}
        processed[i] = True
        for j in range(i + 1, len(object_terms)):
            if processed[j]:
                continue
            if _similarity_ratio(term, object_terms[j]) >= threshold:
                group.add(object_terms[j])
                processed[j] = True
        if len(group) > 1:
            groups.append(sorted(group, key=lambda item: item.lower()))
    return groups


def _apply_consolidations(data_entries: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if not (
        _STATE.get("consolidations_staged_for_generation")
        and _STATE.get("similar_term_groups")
        and _STATE.get("user_choices_for_similar_terms")
    ):
        return data_entries, 0
    current = data_entries.copy()
    modified = 0
    choices = _STATE.get("user_choices_for_similar_terms", {})
    for idx, group in enumerate(_STATE.get("similar_term_groups", [])):
        choice = choices.get(f"choice_group_{idx}")
        custom = str(choices.get(f"new_term_group_{idx}", "") or "").strip()
        replacement = custom if choice == "Use New Term" and custom else choice if choice not in {None, "Use New Term", "Keep All (No Change)"} else None
        if not replacement:
            continue
        for old_term in group:
            if old_term == replacement:
                continue
            mask = current["subject_label"] == old_term
            if mask.any():
                current.loc[mask, "subject_label"] = replacement
                modified += int(mask.sum())
    if modified:
        current.drop_duplicates(inplace=True)
        current.sort_values(by=["subject_label", "Source Column"], inplace=True)
    _STATE["similar_term_groups"] = []
    _STATE["user_choices_for_similar_terms"] = {}
    _STATE["consolidations_staged_for_generation"] = False
    _STATE["show_consolidation_review_ui"] = False
    return current, modified


def generate_matching_table() -> GeneratedTableResult:
    original_df = _STATE.get("original_df")
    if not isinstance(original_df, pd.DataFrame):
        return GeneratedTableResult(None, None, False, [], error="Please upload and load a data table first.")
    try:
        df_for_matching, preprocessing_applied, actions, applied_split, applied_expand = _apply_prepared_transformations(original_df)
        headers_to_include = [str(col) for col in df_for_matching.columns if not is_potentially_numeric(str(col)) and not is_probably_date(str(col))]
        header_entries = pd.DataFrame(
            {
                "subject_id": "",
                "subject_label": headers_to_include,
                "predicate_id": "",
                "object_id": "",
                "object_label": "",
                "mapping_justification": "",
                "Source Column": headers_to_include,
            }
        )
        unique_terms_with_source = set()
        omitted = set(_STATE.get("omitted_columns_selection", []))
        original_split_cols = set(_STATE.get("prepared_split_config", {}).keys())
        original_expand_cols = set(_STATE.get("prepared_expand_config", {}).keys())
        keep_split = bool(_STATE.get("keep_original_setting_split", True))
        keep_expand = bool(_STATE.get("keep_original_setting_expand", True))

        for col in df_for_matching.columns:
            if col in omitted:
                continue
            skip_original = False
            if col in original_split_cols and keep_split and col in applied_split:
                new_names = applied_split.get(col, {}).get("new_names", [])
                skip_original = any(new_name in df_for_matching.columns for new_name in new_names)
            if not skip_original and col in original_expand_cols and keep_expand and col in applied_expand:
                skip_original = True
            if skip_original:
                continue
            for val in df_for_matching[col].dropna():
                text = str(val).strip()
                if text and len(text) < 250 and not is_potentially_numeric(text) and not is_probably_date(text) and text.lower() not in {"true", "false"}:
                    unique_terms_with_source.add((text, col))

        header_lower = {header.lower() for header in headers_to_include}
        data_terms = sorted(
            [item for item in unique_terms_with_source if item[0].lower() not in header_lower],
            key=lambda item: (item[0].lower(), str(item[1]).lower()),
        )
        terms, sources = zip(*data_terms) if data_terms else ([], [])
        data_entries = pd.DataFrame(
            {
                "subject_id": "",
                "subject_label": list(terms),
                "predicate_id": "",
                "object_id": "",
                "object_label": "",
                "mapping_justification": "",
                "Source Column": list(sources),
            }
        )
        data_entries, consolidation_count = _apply_consolidations(data_entries)
        matching_df = pd.concat([header_entries, data_entries], ignore_index=True)
        matching_df = sync_matching_table_schemas(matching_df)
        matching_df = matching_df[REQUIRED_MATCHING_COLUMNS].copy()
        return GeneratedTableResult(
            matching_df=matching_df,
            preprocessed_df=df_for_matching,
            preprocessing_applied=preprocessing_applied,
            actions=actions or ["No prepared preprocessing rules were applied."],
            consolidations_applied_count=consolidation_count,
        )
    except Exception as exc:
        LOGGER.error("Error generating matching table: %s", exc, exc_info=True)
        return GeneratedTableResult(None, None, False, [], error=f"Error generating matching table: {exc}")


def _build_snapshot() -> Dict[str, Any]:
    """Build the JSON-safe state snapshot consumed by the MUI frontend."""
    original_df = _STATE.get("original_df")
    transformed_df = _STATE.get("df_after_transformations")
    matching_df = _STATE.get("matching_df")
    columns = list(original_df.columns) if isinstance(original_df, pd.DataFrame) else []
    filename = _STATE.get(MTG_UPLOADED_FILE_NAME_KEY) or ""
    preprocessed_df = transformed_df if isinstance(transformed_df, pd.DataFrame) else original_df
    output_base = os.path.splitext(str(filename))[0] + "_preprocessed" if filename else "data_preprocessed"
    detected_codes = _STATE.get(MTG_EXPAND_DETECTED_CODES_KEY, [])
    if not isinstance(detected_codes, list):
        detected_codes = []

    return {
        "file": {
            "name": filename,
            "size": _STATE.get(MTG_UPLOADED_FILE_SIZE_KEY, 0),
            "available_sheets": _STATE.get("available_sheets", []),
            "selected_sheet": _STATE.get("selected_sheet"),
            "start_row": int(_STATE.get("current_start_row", 1) or 1),
            "load_error": _STATE.get("load_error"),
        },
        "data": {
            "has_table": isinstance(original_df, pd.DataFrame),
            "rows": int(original_df.shape[0]) if isinstance(original_df, pd.DataFrame) else 0,
            "columns": int(original_df.shape[1]) if isinstance(original_df, pd.DataFrame) else 0,
            "column_names": columns,
            "preview": _dataframe_preview_records(original_df, 8),
            "used_preview": _dataframe_preview_records(preprocessed_df, 8),
            "preprocessing_applied": bool(_STATE.get("preprocessing_applied_in_last_run", False)),
        },
        "omission": {
            "selected": _STATE.get("omitted_columns_selection", []),
        },
        "preprocessing": {
            "prepared_split_config": _STATE.get("prepared_split_config", {}),
            "prepared_expand_config": _STATE.get("prepared_expand_config", {}),
            "keep_original_split": bool(_STATE.get("keep_original_setting_split", True)),
            "keep_original_expand": bool(_STATE.get("keep_original_setting_expand", True)),
            "transformations_prepared": bool(_STATE.get("transformations_prepared", False)),
            "detected_expansion_codes": detected_codes,
            "detected_expansion_column": _STATE.get(MTG_EXPAND_DETECTED_COLUMN_KEY, ""),
            "detected_expansion_delimiter": _STATE.get(MTG_EXPAND_DETECTED_DELIMITER_KEY, ", "),
        },
        "consolidation": {
            "threshold": float(_STATE.get("levenshtein_threshold", 0.85) or 0.85),
            "groups": _STATE.get("similar_term_groups", []),
            "choices": _STATE.get("user_choices_for_similar_terms", {}),
            "review_visible": bool(_STATE.get("show_consolidation_review_ui", False)),
            "staged": bool(_STATE.get("consolidations_staged_for_generation", False)),
        },
        "matching": {
            "has_table": isinstance(matching_df, pd.DataFrame),
            "rows": int(matching_df.shape[0]) if isinstance(matching_df, pd.DataFrame) else 0,
            "columns": REQUIRED_MATCHING_COLUMNS,
            "preview": _dataframe_preview_records(matching_df, 100),
            "csv": _dataframe_csv(matching_df),
            "csv_filename": "matching_table.csv",
        },
        "downloads": {
            "preprocessed_available": isinstance(preprocessed_df, pd.DataFrame) and bool(_STATE.get("preprocessing_applied_in_last_run", False)),
            "preprocessed_csv": _dataframe_csv(preprocessed_df) if bool(_STATE.get("preprocessing_applied_in_last_run", False)) else "",
            "preprocessed_csv_filename": f"{output_base}.csv",
            "preprocessed_xlsx_base64": _dataframe_xlsx_base64(preprocessed_df, "PreprocessedData") if bool(_STATE.get("preprocessing_applied_in_last_run", False)) else "",
            "preprocessed_xlsx_filename": f"{output_base}.xlsx",
        },
        "statusMessage": _STATE.get(MTG_STATUS_MESSAGE_KEY),
    }


def get_shared_outputs() -> Dict[str, Optional[pd.DataFrame]]:
    """Return generated outputs for downstream backend services."""
    matching_df = _STATE.get("shared_matching_table")
    preprocessed_df = _STATE.get("shared_preprocessed_data")
    return {
        "shared_matching_table": matching_df.copy() if isinstance(matching_df, pd.DataFrame) else None,
        "shared_preprocessed_data": preprocessed_df.copy() if isinstance(preprocessed_df, pd.DataFrame) else None,
    }


def _handle_upload_event(event: Dict[str, Any]) -> None:
    filename = str(event.get("filename", "") or "uploaded.csv").strip() or "uploaded.csv"
    content_b64 = str(event.get("content_base64", "") or "")
    if not content_b64:
        _set_status("error", "Uploaded file content was empty.")
        return
    try:
        file_bytes = base64.b64decode(content_b64)
    except Exception as exc:
        _set_status("error", f"Unable to decode uploaded file: {exc}")
        return
    if not filename.lower().endswith((".csv", ".xlsx", ".xls")):
        _set_status("error", "Please upload a CSV, XLSX, or XLS file.")
        return
    _reset_data_dependent_state(clear_file=False)
    _STATE[MTG_UPLOADED_FILE_BYTES_KEY] = file_bytes
    _STATE[MTG_UPLOADED_FILE_NAME_KEY] = filename
    _STATE[MTG_UPLOADED_FILE_SIZE_KEY] = len(file_bytes)
    _STATE["uploaded_file_info"] = (filename, len(file_bytes))
    _STATE["current_start_row"] = max(1, int(event.get("start_row", 1) or 1))
    sheets = get_excel_sheet_names(file_bytes, filename)
    _STATE["available_sheets"] = sheets
    _STATE["selected_sheet"] = sheets[0] if sheets else None
    _load_current_uploaded_file()
    if _STATE.get("load_error"):
        _set_status("error", _STATE["load_error"])
    else:
        sheet_note = f" (sheet: {_STATE['selected_sheet']})" if _STATE.get("selected_sheet") else ""
        _set_status("success", f"Loaded {filename}{sheet_note}.")


def _handle_mui_event(event: Any) -> bool:
    if not isinstance(event, dict):
        return False
    nonce = event.get("nonce")
    if nonce and _STATE.get(MTG_COMPONENT_ACTION_NONCE_KEY) == nonce:
        return False
    if nonce:
        _STATE[MTG_COMPONENT_ACTION_NONCE_KEY] = nonce

    event_type = str(event.get("type", "") or "")
    should_rerun = True
    original_df = _STATE.get("original_df")

    if event_type == "upload_file":
        _handle_upload_event(event)
    elif event_type == "set_sheet":
        selected = str(event.get("sheet", "") or "")
        if selected in _STATE.get("available_sheets", []):
            _STATE["selected_sheet"] = selected
            _load_current_uploaded_file()
            _set_status("success" if not _STATE.get("load_error") else "error", _STATE.get("load_error") or f"Loaded sheet '{selected}'.")
    elif event_type == "set_start_row":
        _STATE["current_start_row"] = max(1, int(event.get("start_row", 1) or 1))
        _load_current_uploaded_file()
        _set_status("success" if not _STATE.get("load_error") else "error", _STATE.get("load_error") or f"Reloaded from row {_STATE['current_start_row']}.")
    elif event_type == "set_omitted_columns":
        selected = event.get("columns", [])
        allowed = set(original_df.columns) if isinstance(original_df, pd.DataFrame) else set()
        _STATE["omitted_columns_selection"] = sorted([str(col) for col in selected if str(col) in allowed])
        _set_status("success", "Column omission selection updated.")
    elif event_type == "add_omission_by_type":
        if not isinstance(original_df, pd.DataFrame):
            _set_status("warning", "Load data before detecting columns to omit.")
        else:
            mode = str(event.get("mode", "") or "")
            additions: List[str] = []
            for col in original_df.columns:
                series = original_df[col].dropna()
                if mode == "numeric":
                    non_empty = [str(v).strip() for v in series if str(v).strip()]
                    if non_empty and all(is_potentially_numeric(v) and v.lower() not in {"true", "false"} for v in non_empty):
                        additions.append(col)
                elif mode == "date":
                    non_empty = [str(v).strip() for v in series if str(v).strip()]
                    if non_empty and all(is_probably_date(v) for v in non_empty):
                        additions.append(col)
                elif mode == "id":
                    upper = str(col).upper()
                    if "ID" in upper or upper.startswith("ID") or upper.endswith("ID"):
                        additions.append(col)
            current = set(_STATE.get("omitted_columns_selection", []))
            current.update(additions)
            _STATE["omitted_columns_selection"] = sorted(current)
            _set_status("info" if additions else "warning", f"Added {len(additions)} {mode} column(s) to omission list." if additions else f"No {mode} columns detected.")
    elif event_type == "prepare_transformations":
        if not isinstance(original_df, pd.DataFrame):
            _set_status("warning", "Load data before preparing transformations.")
        else:
            try:
                split_config = _normalize_split_config(event.get("split_config"), original_df.columns)
                expand_config = _normalize_expand_config(event.get("expand_config"), original_df.columns)
                _STATE["prepared_split_config"] = split_config
                _STATE["prepared_expand_config"] = expand_config
                _STATE["keep_original_setting_split"] = bool(event.get("keep_original_split", True))
                _STATE["keep_original_setting_expand"] = bool(event.get("keep_original_expand", True))
                _STATE["transformations_prepared"] = bool(split_config or expand_config)
                _set_status("success" if _STATE["transformations_prepared"] else "info", "Transformation rules prepared successfully." if _STATE["transformations_prepared"] else "No valid transformations were enabled/configured.")
            except Exception as exc:
                _set_status("error", f"Could not prepare transformations: {exc}")
    elif event_type == "clear_transformations":
        _STATE["prepared_split_config"] = {}
        _STATE["prepared_expand_config"] = {}
        _STATE["transformations_prepared"] = False
        _STATE["df_after_transformations"] = None
        _STATE["preprocessing_applied_in_last_run"] = False
        _set_status("info", "Prepared transformation rules cleared.")
    elif event_type == "detect_expansion_codes":
        column = str(event.get("column", "") or "")
        delimiter = str(event.get("delimiter", ", ") or ", ")
        codes = detect_codes_for_expansion(original_df, column, delimiter)
        _STATE[MTG_EXPAND_DETECTED_CODES_KEY] = codes
        _STATE[MTG_EXPAND_DETECTED_COLUMN_KEY] = column
        _STATE[MTG_EXPAND_DETECTED_DELIMITER_KEY] = delimiter
        _set_status("success" if codes else "warning", f"Detected {len(codes)} code(s) in '{column}'." if codes else "No codes detected for the selected column/delimiter.")
    elif event_type == "find_similar_terms":
        threshold = max(0.0, min(1.0, float(event.get("threshold", _STATE.get("levenshtein_threshold", 0.85)) or 0.85)))
        _STATE["levenshtein_threshold"] = threshold
        groups = find_similar_term_groups(threshold)
        _STATE["similar_term_groups"] = groups
        _STATE["show_consolidation_review_ui"] = bool(groups)
        _STATE["consolidations_staged_for_generation"] = False
        choices = {}
        for idx, group in enumerate(groups):
            choices[f"choice_group_{idx}"] = group[0] if group else "Keep All (No Change)"
            choices[f"new_term_group_{idx}"] = ""
        _STATE["user_choices_for_similar_terms"] = choices
        _set_status("success" if groups else "info", f"Found {len(groups)} similar-term group(s)." if groups else f"No terms found with similarity >= {threshold:.2f}.")
    elif event_type == "stage_consolidations":
        choices = event.get("choices", {}) if isinstance(event.get("choices", {}), dict) else {}
        _STATE["user_choices_for_similar_terms"] = choices
        _STATE["consolidations_staged_for_generation"] = bool(_STATE.get("similar_term_groups"))
        _STATE["show_consolidation_review_ui"] = False
        _set_status("success", "Consolidation choices staged. They will be applied when generating the matching table.")
    elif event_type == "generate_matching_table":
        result = generate_matching_table()
        if result.error:
            _STATE["matching_df"] = None
            _set_status("error", result.error)
        else:
            _STATE["matching_df"] = result.matching_df.copy() if isinstance(result.matching_df, pd.DataFrame) else None
            _STATE["df_after_transformations"] = result.preprocessed_df.copy() if result.preprocessing_applied and isinstance(result.preprocessed_df, pd.DataFrame) else None
            _STATE["preprocessing_applied_in_last_run"] = bool(result.preprocessing_applied)
            _STATE["shared_matching_table"] = result.matching_df.copy() if isinstance(result.matching_df, pd.DataFrame) else None
            _STATE["shared_preprocessed_data"] = result.preprocessed_df.copy() if isinstance(result.preprocessed_df, pd.DataFrame) else None
            consolidation_note = f" including {result.consolidations_applied_count} consolidation replacement(s)" if result.consolidations_applied_count else ""
            _set_status("success", f"Matching table generated successfully{consolidation_note}.")
    elif event_type == "reset_all":
        _reset_data_dependent_state(clear_file=True)
        _set_status("info", "Matching table generator state reset.")
    elif event_type == "download_ack":
        should_rerun = False
    else:
        should_rerun = False

    return should_rerun
