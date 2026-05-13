# app.py (RDF Generator backend bridge + Material UI component host)

import base64
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import csv
import hashlib
import os
import re
import uuid # Added for unique keys in schema templates
import json # Added for saving/loading templates
import textwrap # Added for dedenting markdown text
import yaml # Added for loading config files
from urllib.parse import urlparse, unquote
from collections import Counter, defaultdict  # defaultdict needed for suggest_groups
from io import BytesIO  # Needed for Excel download if applicable later
import logging
import asyncio
from typing import Any, Dict, List, Optional
from .uri_utils import process_iris_async, load_api_specs
from semi_automatic_reconciliation.shared_table_io import (
    sync_matching_table_schemas,
    get_preferred_term_column,
    get_preferred_uri_column,
    get_preferred_rdf_role_column,
    get_preferred_match_type_column,
)


# --- Configuration ---
# Call set_page_config only when run as a script, not when imported as a module
if __name__ == "__main__":
    st.set_page_config(page_title="RDF Generator", layout="wide")

# Configure logging (adjust level as needed for debugging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Import Backend Functions ---
try:
    from .rdf_processor import create_rdf_with_mappings
    from .rdf_serializer import serialize_rdf
    from .skos_generator import create_skos_graph_and_lookup_map
    from .reference_handler import DOIToSemOpenAlexConverter
    try:
        from .dcat_generator import display_dcat_builder, create_dcat_catalog, THEMES, LICENSES
    except ImportError:
        # Fallback if display_dcat_builder is not available
        def display_dcat_builder():
            st.warning("DCAT builder functionality is currently not available.")
        create_dcat_catalog = None
        THEMES = {}
        LICENSES = {}
except ImportError as e:
    st.error(f"Fatal Error: Could not import backend functions. {e}")
    st.stop()
# --- End Imports ---


# --- Helper Functions (defined directly in the UI script) ---

def load_default_config():
    """Loads the default config.yaml from the filesystem."""
    try:
        # Assuming config.yaml is in the same directory as app.py
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.warning(f"Could not load default config.yaml: {e}")
        return {}

def detect_delimiter(uploaded_file):
    """Sniffs the delimiter from a sample of the uploaded file."""
    try:
        uploaded_file.seek(0)
        sample = uploaded_file.read(2048).decode('utf-8', errors='ignore')
        uploaded_file.seek(0)
        sniffer = csv.Sniffer()
        if sample.startswith('\ufeff'):
            sample = sample[1:]
        if not sample.strip():
            return ','
        try:
            dialect = sniffer.sniff(sample, delimiters=[',', ';', '\t', '|'])
            delimiter = dialect.delimiter
        except csv.Error:
            lines = sample.splitlines()
            counts = Counter(lines[0]) if lines else Counter()
            if counts.get(';') > counts.get(','):
                delimiter = ';'
            elif counts.get('\t') > counts.get(','):
                delimiter = '\t'
            elif counts.get('|') > counts.get(','):
                delimiter = '|'
            else:
                delimiter = ','  # Default to comma
        logging.info(f"Detected delimiter: '{delimiter}'")
        return delimiter
    except Exception as e:
        logging.warning(f"Delimiter detection failed: {e}. Defaulting to ','.")
        return ','

def get_excel_sheets(file):
    """Returns list of sheet names in Excel file."""
    if not file or not file.name.endswith((".xlsx", ".xls")):
        return []
    try:
        file.seek(0)
        excel_file = pd.ExcelFile(file)
        return excel_file.sheet_names
    except Exception as e:
        logging.warning(f"Could not read Excel sheets: {e}")
        return []

def read_uploaded_file(file, header_row, csv_separator=None, sheet_name=None):
    """Reads CSV or Excel, converts object columns to string, returning None on error."""
    if not file:
        return None
    try:
        file.seek(0)  # Ensure reading from start
        df_loaded = None
        if file.name.endswith('.csv'):
            delimiter = csv_separator if csv_separator else detect_delimiter(file)
            df_loaded = pd.read_csv(file, delimiter=delimiter, header=header_row - 1,
                               skipinitialspace=True)
        elif file.name.endswith((".xlsx", ".xls")):
            if sheet_name:
                df_loaded = pd.read_excel(file, sheet_name=sheet_name, header=header_row - 1)
            else:
                df_loaded = pd.read_excel(file, header=header_row - 1)
        else:
            st.error("Unsupported file type.")
            return None

        if df_loaded is not None:
            # Convert object columns to string to prevent PyArrow errors with mixed types
            for col in df_loaded.columns:
                if df_loaded[col].dtype == 'object':
                    try:
                        # Replace NaN values with empty strings before converting to string type
                        df_loaded[col] = df_loaded[col].fillna('').astype(str)
                    except Exception as e:
                        logging.warning(f"Could not convert column '{col}' to string: {e}")
            return df_loaded
        else: # Should not happen if previous checks are fine, but as a safeguard
            return None

    except Exception as e:
        st.error(f"Error reading file '{file.name}' starting at row {header_row}: {e}")
        return None

def is_valid_uri_simple(uri: str) -> bool:
    """Basic check if a string looks like a URI."""
    if not uri or not isinstance(uri, str):
        return False
    try:
        result = urlparse(uri)
        # Check if it has a scheme OR a path (allows relative, URNs like urn:isbn:...)
        return bool(result.scheme or result.path or ':' in uri)  # Added ':' check for URNs
    except ValueError:
        return False

def suggest_groups(columns):
    """Suggests column groups based on frequently co-occurring words/prefixes."""
    prefix_map = defaultdict(list)
    prefix_pattern = re.compile(r'^(.+?)[_.]')
    word_counts = Counter()
    column_words_map = {}

    for col in columns:
        col_str = str(col)
        words = re.findall(r'[A-Z]?[a-z]+|\d+|[A-Z]+(?![a-z])', col_str)
        processed_words = {word.lower() for word in words if len(word) > 1}
        if processed_words:
            column_words_map[col_str] = processed_words
            word_counts.update(processed_words)
        match = prefix_pattern.match(col_str)
        if match:
            prefix = match.group(1)
            if prefix and len(prefix) > 1:
                prefix_map[prefix].append(col_str)

    suggestions = {}
    min_group_size = 2
    for word, count in word_counts.items():
        if count >= min_group_size:
            cols_with_word = [col for col, words in column_words_map.items() if word in words]
            if len(cols_with_word) >= min_group_size:
                group_key = f"word_{word}"
                suggestions[group_key] = {
                    'display': word,
                    'columns': sorted(list(set(cols_with_word)))
                }
    for prefix, cols in prefix_map.items():
        if len(cols) >= min_group_size:
            covered = False
            for word_group in suggestions.values():
                if len(set(cols) & set(word_group['columns'])) / len(cols) > 0.7:
                    covered = True
                    break
            if not covered:
                group_key = f"prefix_{prefix}"
                suggestions[group_key] = {
                    'display': prefix,
                    'columns': sorted(list(set(cols)))
                }
    logging.info(f"Suggested groups: {suggestions}")
    return suggestions

def generate_ids(df, id_column_name, prefix):
    """Generates unique IDs in a new column, handling potential conflicts."""
    if id_column_name in df.columns:
        count = 1
        new_name = f"{id_column_name}_{count}"
        while new_name in df.columns:
            count += 1
            new_name = f"{id_column_name}_{count}"
        id_column_name = new_name
        st.warning(f"Generated ID column name conflicted. Using '{id_column_name}' instead.")

    # Calculate number of digits needed for zero-padding
    total_rows = len(df)
    num_digits = len(str(total_rows))

    # Generate IDs with zero-padding for proper alphabetical sorting
    df[id_column_name] = [f"{prefix}{i+1:0{num_digits}d}" for i in range(total_rows)]
    return df


RDF_COMPONENT_ACTION_NONCE_KEY = "rdf_generator_component_action_nonce"
RDF_STATUS_MESSAGE_KEY = "rdf_generator_status_message"
RDF_DATA_FILE_BYTES_KEY = "rdf_generator_data_file_bytes"
RDF_MAPPING_FILE_BYTES_KEY = "rdf_generator_mapping_file_bytes"
RDF_DATA_FILE_NAME_KEY = "rdf_generator_data_file_name"
RDF_MAPPING_FILE_NAME_KEY = "rdf_generator_mapping_file_name"

RDF_FORMATS = ["Turtle", "N-Quads", "JSON-LD", "RDF/XML", "TriG"]
TEMPLATE_MAP_TYPES = ["Column Value (Literal)", "Column Value (URI)", "Fixed URI", "Nested Template"]


class _NamedBytesIO(BytesIO):
    """Small file-like adapter so the legacy parsers can read MUI-uploaded bytes."""

    def __init__(self, data: bytes, name: str):
        super().__init__(data)
        self.name = name


def _init_rdf_mui_state() -> None:
    defaults = {
        "config": load_default_config(),
        "rdf_data": None,
        "skos_data": None,
        "dcat_catalog_data": None,
        "dcat_metadata_data": None,
        "rdf_preview": None,
        "rdf_graph": None,
        "skos_graph": None,
        "rdf_gen_data_df": None,
        "rdf_gen_mapping_df": None,
        "rdf_gen_data_source_msg": None,
        "rdf_gen_mapping_source_msg": None,
        "rdf_gen_header_row_main": 1,
        "rdf_gen_header_row_mapping": 1,
        "rdf_gen_data_csv_separator": None,
        "rdf_gen_mapping_csv_separator": None,
        "rdf_gen_data_sheet": None,
        "rdf_gen_mapping_sheet": None,
        "rdf_gen_data_sheets": [],
        "rdf_gen_mapping_sheets": [],
        "schema_templates": [],
        "reference_data": None,
        "reference_rdf": None,
        "rdf_gen_reference_method": "DOI",
        "rdf_gen_doi_input": "",
        "rdf_gen_id_option": "Existing ID Column",
        "rdf_gen_default_id_col": "",
        "rdf_gen_use_shared_id": False,
        "rdf_gen_shared_id_col": "",
        "rdf_gen_subject_base_uri": "",
        "rdf_gen_graph_option": "Use Named Graph",
        "rdf_gen_graph_base_uri": None,
        "rdf_gen_use_class": False,
        "rdf_gen_class_uri": "",
        "rdf_gen_group_active": False,
        "rdf_gen_group_suggestions": {},
        "rdf_gen_group_config": {},
        "rdf_gen_active_schema_template": "None (use default column mapping)",
        "rdf_gen_format": "Turtle",
        "rdf_gen_dcat_title": "My Dataset",
        "rdf_gen_dcat_description": "An example dataset.",
        "rdf_gen_dcat_access_rights": "PUBLIC",
        "rdf_gen_dcat_contact_point": "",
        "rdf_gen_dcat_publisher_name": "My Organization",
        "rdf_gen_dcat_publisher_uri": "",
        "rdf_gen_dcat_themes": [],
        "rdf_gen_dcat_license": next(iter(LICENSES.keys()), "CC BY 4.0"),
        "rdf_gen_dcat_link_reference": True,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            if isinstance(value, (dict, list)):
                st.session_state[key] = value.copy()
            else:
                st.session_state[key] = value
    config = st.session_state.get("config") or {}
    if not st.session_state.get("rdf_gen_graph_base_uri"):
        st.session_state["rdf_gen_graph_base_uri"] = config.get("default_namespace", "http://example.com/data/")
    if not st.session_state.get("rdf_gen_dcat_publisher_uri"):
        default_ns = config.get("default_namespace", "http://example.com/data/")
        st.session_state["rdf_gen_dcat_publisher_uri"] = f"{str(default_ns).rstrip('/')}/organization"


def _set_rdf_status(severity: str, text: str) -> None:
    st.session_state[RDF_STATUS_MESSAGE_KEY] = {"severity": severity, "text": text}


def _records(df: Optional[pd.DataFrame], limit: int = 12) -> List[Dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    return df.head(limit).fillna("").astype(str).to_dict(orient="records")


def _template_download_json() -> str:
    return json.dumps(st.session_state.get("schema_templates", []), indent=2)


def _component_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "agentic_reconciliation",
        "components",
        "workflow_config_panel",
        "frontend",
        "build",
    )


def _render_rdf_generator_mui(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    component_path = _component_path()
    if not os.path.exists(os.path.join(component_path, "index.html")):
        st.error(
            "RDFGeneratorMuiApp React/Material-UI component build is missing. "
            "Run `npm install && npm run build` in agentic_reconciliation/components/workflow_config_panel/frontend."
        )
        return None
    rdf_component = components.declare_component("rdf_generator_panel", path=component_path)
    try:
        return rdf_component(app="rdf_generator", snapshot=snapshot, key="rdf_generator_mui_app", default=None)
    except Exception as exc:
        st.error(f"RDFGeneratorMuiApp React/Material-UI component could not be rendered. ({exc})")
        return None


def _load_bytes_as_dataframe(kind: str) -> None:
    bytes_key = RDF_DATA_FILE_BYTES_KEY if kind == "data" else RDF_MAPPING_FILE_BYTES_KEY
    name_key = RDF_DATA_FILE_NAME_KEY if kind == "data" else RDF_MAPPING_FILE_NAME_KEY
    header_key = "rdf_gen_header_row_main" if kind == "data" else "rdf_gen_header_row_mapping"
    sep_key = "rdf_gen_data_csv_separator" if kind == "data" else "rdf_gen_mapping_csv_separator"
    sheet_key = "rdf_gen_data_sheet" if kind == "data" else "rdf_gen_mapping_sheet"
    df_key = "rdf_gen_data_df" if kind == "data" else "rdf_gen_mapping_df"
    msg_key = "rdf_gen_data_source_msg" if kind == "data" else "rdf_gen_mapping_source_msg"
    file_bytes = st.session_state.get(bytes_key)
    filename = st.session_state.get(name_key)
    if not isinstance(file_bytes, bytes) or not filename:
        _set_rdf_status("warning", f"No {kind} file is available to load.")
        return
    file_obj = _NamedBytesIO(file_bytes, filename)
    df = read_uploaded_file(
        file_obj,
        int(st.session_state.get(header_key, 1) or 1),
        st.session_state.get(sep_key),
        st.session_state.get(sheet_key),
    )
    if df is None:
        st.session_state[df_key] = None
        _set_rdf_status("error", f"Failed to load {kind} table from {filename}.")
        return
    if kind == "mapping":
        df = sync_matching_table_schemas(df)
        missing = [col for col in ["subject_label", "object_id", "predicate_id"] if col not in df.columns]
        if missing:
            st.session_state[df_key] = None
            _set_rdf_status("error", f"Uploaded matching table is missing required columns: {', '.join(missing)}.")
            return
    st.session_state[df_key] = df
    sheet_note = f" (sheet: {st.session_state.get(sheet_key)})" if st.session_state.get(sheet_key) else ""
    st.session_state[msg_key] = f"Loaded {'Data Table' if kind == 'data' else 'Matching Table'} from uploaded file: {filename}{sheet_note}."
    _set_rdf_status("success", st.session_state[msg_key])


def _handle_upload(kind: str, event: Dict[str, Any]) -> None:
    filename = str(event.get("filename", "") or "uploaded.csv").strip() or "uploaded.csv"
    content_b64 = str(event.get("content_base64", "") or "")
    if not filename.lower().endswith((".csv", ".xlsx", ".xls")):
        _set_rdf_status("error", "Please upload a CSV, XLSX, or XLS file.")
        return
    try:
        file_bytes = base64.b64decode(content_b64)
    except Exception as exc:
        _set_rdf_status("error", f"Unable to decode uploaded file: {exc}")
        return
    bytes_key = RDF_DATA_FILE_BYTES_KEY if kind == "data" else RDF_MAPPING_FILE_BYTES_KEY
    name_key = RDF_DATA_FILE_NAME_KEY if kind == "data" else RDF_MAPPING_FILE_NAME_KEY
    sheet_key = "rdf_gen_data_sheet" if kind == "data" else "rdf_gen_mapping_sheet"
    sheets_key = "rdf_gen_data_sheets" if kind == "data" else "rdf_gen_mapping_sheets"
    st.session_state[bytes_key] = file_bytes
    st.session_state[name_key] = filename
    sheets = get_excel_sheets(_NamedBytesIO(file_bytes, filename)) if filename.lower().endswith((".xlsx", ".xls")) else []
    st.session_state[sheets_key] = sheets
    st.session_state[sheet_key] = sheets[0] if sheets else None
    if event.get("header_row"):
        st.session_state["rdf_gen_header_row_main" if kind == "data" else "rdf_gen_header_row_mapping"] = max(1, int(event.get("header_row") or 1))
    _load_bytes_as_dataframe(kind)


def _load_shared_data() -> None:
    candidate = st.session_state.get("shared_preprocessed_data")
    if isinstance(candidate, pd.DataFrame):
        st.session_state["rdf_gen_data_df"] = candidate.copy()
        st.session_state["rdf_gen_data_source_msg"] = "Loaded Preprocessed Data from Matching Table Generator."
        st.session_state[RDF_DATA_FILE_NAME_KEY] = "shared_preprocessed_data"
        _set_rdf_status("success", st.session_state["rdf_gen_data_source_msg"])
    else:
        _set_rdf_status("warning", "No preprocessed data from the Matching Table Generator is available.")


def _load_shared_mapping() -> None:
    map_df_candidate = st.session_state.get("shared_reconciled_matching_table")
    if not isinstance(map_df_candidate, pd.DataFrame):
        _set_rdf_status("warning", "No reconciled matching table from Reconciliation is available.")
        return
    current_mapping_df = sync_matching_table_schemas(map_df_candidate.copy())
    all_original_terms = st.session_state.get("all_terms_for_reconciliation")
    if all_original_terms and isinstance(all_original_terms, list):
        existing = set(current_mapping_df["subject_label"].astype(str).unique()) if "subject_label" in current_mapping_df.columns else set()
        missing_rows = [
            {"subject_label": str(term), "object_id": "", "predicate_id": "", "mapping_justification": "", "Source Provider": "", "Confirmed Display String": "--- No Match ---"}
            for term in all_original_terms
            if str(term) not in existing
        ]
        if missing_rows:
            current_mapping_df = pd.concat([current_mapping_df, pd.DataFrame(missing_rows)], ignore_index=True) if not current_mapping_df.empty else pd.DataFrame(missing_rows)
    final_cols = ["subject_id", "subject_label", "predicate_id", "object_id", "object_label", "mapping_justification", "match_type", "Term", "URI", "Match Type", "Source Provider", "Confirmed Display String"]
    for col in final_cols:
        if col not in current_mapping_df.columns:
            current_mapping_df[col] = pd.NA
    current_mapping_df = sync_matching_table_schemas(current_mapping_df[final_cols + [c for c in current_mapping_df.columns if c not in final_cols]])
    st.session_state["rdf_gen_mapping_df"] = current_mapping_df.copy()
    st.session_state["rdf_gen_mapping_source_msg"] = "Loaded Reconciled Matching Table from Reconciliation App (prefilled 'No Match' entries)."
    st.session_state[RDF_MAPPING_FILE_NAME_KEY] = "shared_reconciled_matching_table"
    _set_rdf_status("success", st.session_state["rdf_gen_mapping_source_msg"])


def _suggested_graph_uri() -> str:
    config = st.session_state.get("config", {}) or {}
    graph_base = str(st.session_state.get("rdf_gen_graph_base_uri") or config.get("default_namespace", "http://example.com/data/"))
    filename = str(st.session_state.get(RDF_DATA_FILE_NAME_KEY) or "graph_data")
    file_stem = os.path.splitext(unquote(filename))[0] or "graph_data"
    hash_id = hashlib.md5(file_stem.encode("utf-8")).hexdigest()
    return f"{graph_base.rstrip('/dataset')}/dataset/da{hash_id}ta"


def _current_config_state() -> Dict[str, Any]:
    df = st.session_state.get("rdf_gen_data_df")
    mapping_df = st.session_state.get("rdf_gen_mapping_df")
    data_cols = list(df.columns) if isinstance(df, pd.DataFrame) else []
    map_cols = list(mapping_df.columns) if isinstance(mapping_df, pd.DataFrame) else []
    default_term = get_preferred_term_column(mapping_df) if isinstance(mapping_df, pd.DataFrame) else ""
    default_uri = get_preferred_uri_column(mapping_df) if isinstance(mapping_df, pd.DataFrame) else ""
    default_match = get_preferred_match_type_column(mapping_df) if isinstance(mapping_df, pd.DataFrame) else ""
    if not st.session_state.get("map_term_col") and default_term:
        st.session_state["map_term_col"] = default_term
    if not st.session_state.get("map_uri_col") and default_uri:
        st.session_state["map_uri_col"] = default_uri
    if not st.session_state.get("map_match_col") and default_match:
        st.session_state["map_match_col"] = default_match
    graph_option = st.session_state.get("rdf_gen_graph_option", "Use Named Graph")
    named_graph = st.session_state.get("ng_full") or _suggested_graph_uri()
    return {
        "data_columns": data_cols,
        "mapping_columns": map_cols,
        "id_option": st.session_state.get("rdf_gen_id_option", "Existing ID Column"),
        "default_id_col": st.session_state.get("rdf_gen_default_id_col", ""),
        "use_shared_id": bool(st.session_state.get("rdf_gen_use_shared_id", False)),
        "shared_id_col": st.session_state.get("rdf_gen_shared_id_col", ""),
        "subject_base_uri": st.session_state.get("rdf_gen_subject_base_uri", ""),
        "graph_option": graph_option,
        "graph_base_uri": st.session_state.get("rdf_gen_graph_base_uri", ""),
        "named_graph_uri": named_graph,
        "term_col": st.session_state.get("map_term_col", default_term),
        "uri_col": st.session_state.get("map_uri_col", default_uri),
        "match_type_col": st.session_state.get("map_match_col", default_match),
        "use_class": bool(st.session_state.get("rdf_gen_use_class", False)),
        "class_uri": st.session_state.get("rdf_gen_class_uri", ""),
        "rdf_format": st.session_state.get("rdf_gen_format", "Turtle"),
        "active_template": st.session_state.get("rdf_gen_active_schema_template", "None (use default column mapping)"),
        "group_active": bool(st.session_state.get("rdf_gen_group_active", False)),
        "group_suggestions": st.session_state.get("rdf_gen_group_suggestions", {}),
        "group_config": st.session_state.get("rdf_gen_group_config", {}),
    }


def _validate_ready() -> List[str]:
    issues: List[str] = []
    df = st.session_state.get("rdf_gen_data_df")
    mapping_df = st.session_state.get("rdf_gen_mapping_df")
    cfg = _current_config_state()
    if not isinstance(df, pd.DataFrame):
        issues.append("Load a data table.")
    if not isinstance(mapping_df, pd.DataFrame):
        issues.append("Load a matching table.")
    else:
        missing = [col for col in ["subject_label", "object_id", "predicate_id"] if col not in mapping_df.columns]
        if missing:
            issues.append(f"Matching table is missing: {', '.join(missing)}.")
    if isinstance(df, pd.DataFrame):
        if cfg["id_option"] == "Existing ID Column" and not cfg["default_id_col"]:
            issues.append("Select an existing ID column or choose generated IDs.")
        if cfg["use_shared_id"]:
            if not cfg["shared_id_col"]:
                issues.append("Select the shared identifier column.")
            if not cfg["subject_base_uri"] or not is_valid_uri_simple(str(cfg["subject_base_uri"])):
                issues.append("Enter a valid Subject Base URI for cross-file linking.")
    if isinstance(mapping_df, pd.DataFrame) and (not cfg["term_col"] or not cfg["uri_col"]):
        issues.append("Select term and URI columns from the matching table.")
    if cfg["graph_option"] == "Use Named Graph" and not is_valid_uri_simple(str(cfg["named_graph_uri"])):
        issues.append("Enter a valid Named Graph URI or select No Named Graph.")
    if cfg["use_class"] and cfg["class_uri"] and not is_valid_uri_simple(str(cfg["class_uri"])):
        issues.append("Class URI is not valid.")
    if cfg["group_active"]:
        for name, group in (cfg["group_config"] or {}).items():
            pred = str(group.get("connecting_predicate", "") or "") if isinstance(group, dict) else ""
            if not pred or not is_valid_uri_simple(pred):
                issues.append(f"Group '{name}' needs a valid connecting predicate URI.")
    return issues


def _build_rdf_snapshot() -> Dict[str, Any]:
    df = st.session_state.get("rdf_gen_data_df")
    mapping_df = st.session_state.get("rdf_gen_mapping_df")
    cfg = _current_config_state()
    ready_issues = _validate_ready()
    file_stem = os.path.splitext(str(st.session_state.get(RDF_DATA_FILE_NAME_KEY) or "rdf_output"))[0] or "rdf_output"
    previews = {
        "rdf": (st.session_state.get("rdf_data") or "")[:5000],
        "skos": (st.session_state.get("skos_data") or "")[:5000],
        "dcat_metadata": (st.session_state.get("dcat_metadata_data") or "")[:5000],
        "dcat_catalog": (st.session_state.get("dcat_catalog_data") or "")[:5000],
        "reference": (st.session_state.get("reference_rdf") or "")[:5000],
    }
    return {
        "active_stage": st.session_state.get("rdf_generator_active_stage", "load"),
        "statusMessage": st.session_state.get(RDF_STATUS_MESSAGE_KEY),
        "data": {
            "has_table": isinstance(df, pd.DataFrame),
            "rows": int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0,
            "columns": int(df.shape[1]) if isinstance(df, pd.DataFrame) else 0,
            "column_names": cfg["data_columns"],
            "filename": st.session_state.get(RDF_DATA_FILE_NAME_KEY, ""),
            "source_message": st.session_state.get("rdf_gen_data_source_msg"),
            "shared_available": isinstance(st.session_state.get("shared_preprocessed_data"), pd.DataFrame),
            "preview": _records(df, 20),
        },
        "mapping": {
            "has_table": isinstance(mapping_df, pd.DataFrame),
            "rows": int(mapping_df.shape[0]) if isinstance(mapping_df, pd.DataFrame) else 0,
            "columns": int(mapping_df.shape[1]) if isinstance(mapping_df, pd.DataFrame) else 0,
            "column_names": cfg["mapping_columns"],
            "filename": st.session_state.get(RDF_MAPPING_FILE_NAME_KEY, ""),
            "source_message": st.session_state.get("rdf_gen_mapping_source_msg"),
            "shared_available": isinstance(st.session_state.get("shared_reconciled_matching_table"), pd.DataFrame),
            "preview": _records(mapping_df, 50),
        },
        "load_options": {
            "data_header_row": int(st.session_state.get("rdf_gen_header_row_main", 1) or 1),
            "mapping_header_row": int(st.session_state.get("rdf_gen_header_row_mapping", 1) or 1),
            "data_csv_separator": st.session_state.get("rdf_gen_data_csv_separator"),
            "mapping_csv_separator": st.session_state.get("rdf_gen_mapping_csv_separator"),
            "data_sheets": st.session_state.get("rdf_gen_data_sheets", []),
            "mapping_sheets": st.session_state.get("rdf_gen_mapping_sheets", []),
            "data_sheet": st.session_state.get("rdf_gen_data_sheet"),
            "mapping_sheet": st.session_state.get("rdf_gen_mapping_sheet"),
        },
        "templates": {
            "items": st.session_state.get("schema_templates", []),
            "json": _template_download_json(),
            "map_types": TEMPLATE_MAP_TYPES,
            "guidance": "Schema Mapping Templates define explicit RDF type and predicate rules, including nested blank-node templates for SOSA/QUDT-style structures.",
        },
        "reference": {
            "loaded": st.session_state.get("reference_data") is not None,
            "method": st.session_state.get("rdf_gen_reference_method", "DOI"),
            "doi": st.session_state.get("rdf_gen_doi_input", ""),
            "summary": _reference_summary(),
            "preview": previews["reference"],
        },
        "config": cfg,
        "ready": {"can_generate": not ready_issues, "issues": ready_issues},
        "outputs": {
            "has_rdf": bool(st.session_state.get("rdf_data")),
            "has_skos": bool(st.session_state.get("skos_data")),
            "has_dcat_metadata": bool(st.session_state.get("dcat_metadata_data")),
            "has_dcat_catalog": bool(st.session_state.get("dcat_catalog_data")),
            "has_reference": bool(st.session_state.get("reference_rdf")),
            "file_stem": file_stem,
            "rdf_data": st.session_state.get("rdf_data") or "",
            "skos_data": st.session_state.get("skos_data") or "",
            "dcat_metadata_data": st.session_state.get("dcat_metadata_data") or "",
            "dcat_catalog_data": st.session_state.get("dcat_catalog_data") or "",
            "reference_rdf": st.session_state.get("reference_rdf") or "",
            "previews": previews,
        },
        "dcat": {
            "available": bool(st.session_state.get("rdf_data") and st.session_state.get("last_named_graph_uri")),
            "title": st.session_state.get("rdf_gen_dcat_title", "My Dataset"),
            "description": st.session_state.get("rdf_gen_dcat_description", "An example dataset."),
            "access_rights": st.session_state.get("rdf_gen_dcat_access_rights", "PUBLIC"),
            "contact_point": st.session_state.get("rdf_gen_dcat_contact_point", ""),
            "publisher_name": st.session_state.get("rdf_gen_dcat_publisher_name", "My Organization"),
            "publisher_uri": st.session_state.get("rdf_gen_dcat_publisher_uri", ""),
            "themes": st.session_state.get("rdf_gen_dcat_themes", []),
            "theme_options": list(THEMES.keys()),
            "license": st.session_state.get("rdf_gen_dcat_license", next(iter(LICENSES.keys()), "CC BY 4.0")),
            "license_options": list(LICENSES.keys()),
            "link_reference": bool(st.session_state.get("rdf_gen_dcat_link_reference", True)),
        },
    }


def _reference_summary() -> str:
    ref = st.session_state.get("reference_data")
    if not isinstance(ref, dict):
        return "No publication reference loaded."
    if ref.get("method") == "DOI":
        return f"Publication reference loaded for DOI: {ref.get('doi', '')}"
    metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
    title = (metadata.get("title") or ["Unknown"])[0] if isinstance(metadata.get("title"), list) else metadata.get("title", "Unknown")
    return f"Publication reference loaded: {title}"


def _apply_settings(settings: Dict[str, Any]) -> None:
    key_map = {
        "data_header_row": "rdf_gen_header_row_main",
        "mapping_header_row": "rdf_gen_header_row_mapping",
        "data_csv_separator": "rdf_gen_data_csv_separator",
        "mapping_csv_separator": "rdf_gen_mapping_csv_separator",
        "data_sheet": "rdf_gen_data_sheet",
        "mapping_sheet": "rdf_gen_mapping_sheet",
        "id_option": "rdf_gen_id_option",
        "default_id_col": "rdf_gen_default_id_col",
        "use_shared_id": "rdf_gen_use_shared_id",
        "shared_id_col": "rdf_gen_shared_id_col",
        "subject_base_uri": "rdf_gen_subject_base_uri",
        "graph_option": "rdf_gen_graph_option",
        "graph_base_uri": "rdf_gen_graph_base_uri",
        "named_graph_uri": "ng_full",
        "term_col": "map_term_col",
        "uri_col": "map_uri_col",
        "match_type_col": "map_match_col",
        "use_class": "rdf_gen_use_class",
        "class_uri": "rdf_gen_class_uri",
        "rdf_format": "rdf_gen_format",
        "active_template": "rdf_gen_active_schema_template",
        "group_active": "rdf_gen_group_active",
        "group_config": "rdf_gen_group_config",
        "reference_method": "rdf_gen_reference_method",
        "doi": "rdf_gen_doi_input",
        "dcat_title": "rdf_gen_dcat_title",
        "dcat_description": "rdf_gen_dcat_description",
        "dcat_access_rights": "rdf_gen_dcat_access_rights",
        "dcat_contact_point": "rdf_gen_dcat_contact_point",
        "dcat_publisher_name": "rdf_gen_dcat_publisher_name",
        "dcat_publisher_uri": "rdf_gen_dcat_publisher_uri",
        "dcat_themes": "rdf_gen_dcat_themes",
        "dcat_license": "rdf_gen_dcat_license",
        "dcat_link_reference": "rdf_gen_dcat_link_reference",
    }
    for source, target in key_map.items():
        if source in settings:
            st.session_state[target] = settings[source]


def _run_uri_enrichment() -> None:
    mapping_df = st.session_state.get("rdf_gen_mapping_df")
    if not isinstance(mapping_df, pd.DataFrame):
        _set_rdf_status("warning", "Load a matching table before URI enrichment.")
        return
    try:
        api_specs = load_api_specs(os.path.join(os.path.dirname(__file__), "config.yaml"))
        mapping_df_for_enrichment = mapping_df.rename(columns={"URI": "Mapped ID"})
        enriched_df = asyncio.run(process_iris_async(mapping_df_for_enrichment, api_specs))
        if enriched_df.empty:
            _set_rdf_status("warning", "URI enrichment returned no data.")
            return
        merged = pd.merge(mapping_df, enriched_df, left_on="URI", right_on="iri", how="left", suffixes=("", "_enriched"))
        merged.drop(columns=["iri"], inplace=True, errors="ignore")
        for col in ["label", "ui_link", "acronym", "source", "message"]:
            enriched_col = f"{col}_enriched"
            if enriched_col in merged.columns:
                merged[col] = merged[enriched_col].fillna(merged.get(col, ""))
                merged.drop(columns=[enriched_col], inplace=True)
            elif col not in merged.columns:
                merged[col] = ""
        st.session_state["rdf_gen_mapping_df"] = merged
        _set_rdf_status("success", "URI enrichment complete.")
    except Exception as exc:
        _set_rdf_status("error", f"An error occurred during URI enrichment: {exc}")


def _generate_reference(event: Dict[str, Any]) -> None:
    method = str(event.get("method") or st.session_state.get("rdf_gen_reference_method", "DOI"))
    st.session_state["rdf_gen_reference_method"] = method
    try:
        converter = DOIToSemOpenAlexConverter()
        if method == "DOI":
            doi = str(event.get("doi") or st.session_state.get("rdf_gen_doi_input", "")).strip()
            if not doi:
                _set_rdf_status("warning", "Enter a DOI before fetching publication metadata.")
                return
            graph = converter.convert(doi)
            if not graph:
                _set_rdf_status("error", "Failed to fetch metadata for the provided DOI.")
                return
            st.session_state["reference_data"] = {"method": "DOI", "doi": doi, "graph": graph}
            st.session_state["reference_rdf"] = converter.serialize(format="turtle")
            st.session_state["rdf_gen_doi_input"] = doi
            _set_rdf_status("success", f"Publication metadata fetched for DOI {doi}.")
            return
        metadata = event.get("metadata", {}) if isinstance(event.get("metadata"), dict) else {}
        title = str(metadata.get("title", "") or "").strip()
        authors_raw = str(metadata.get("authors", "") or "").strip()
        journal = str(metadata.get("journal", "") or "").strip()
        if not title or not authors_raw or not journal:
            _set_rdf_status("warning", "Title, Authors, and Journal are required for manual publication references.")
            return
        year = int(metadata.get("year", 2023) or 2023)
        manual_metadata = {"title": [title], "author": [], "container-title": [journal], "published-print": {"date-parts": [[year, 1, 1]]}}
        for author in [part.strip() for part in authors_raw.split(";") if part.strip()]:
            if "," in author:
                family, given = [p.strip() for p in author.split(",", 1)]
            else:
                parts = author.split()
                given = " ".join(parts[:-1]) if len(parts) > 1 else ""
                family = parts[-1] if parts else author
            manual_metadata["author"].append({"given": given, "family": family})
        for src, dest in (("volume", "volume"), ("pages", "page"), ("doi", "DOI")):
            if metadata.get(src):
                manual_metadata[dest] = metadata[src]
        work_uri = converter.create_work_uri(manual_metadata.get("DOI", title))
        converter.add_work_metadata(work_uri, manual_metadata)
        converter.add_authors(work_uri, manual_metadata)
        converter.add_source(work_uri, manual_metadata)
        converter.add_open_access(work_uri, manual_metadata)
        st.session_state["reference_data"] = {"method": "Manual", "metadata": manual_metadata, "graph": converter.graph}
        st.session_state["reference_rdf"] = converter.serialize(format="turtle")
        _set_rdf_status("success", "Manual publication reference RDF generated.")
    except Exception as exc:
        _set_rdf_status("error", f"Error generating publication reference: {exc}")


def _generate_rdf() -> None:
    issues = _validate_ready()
    if issues:
        _set_rdf_status("error", "Address configuration issues first: " + " ".join(issues))
        return
    df = st.session_state.get("rdf_gen_data_df")
    mapping_df = sync_matching_table_schemas(st.session_state.get("rdf_gen_mapping_df"))
    st.session_state["rdf_gen_mapping_df"] = mapping_df
    cfg = _current_config_state()
    config = st.session_state.get("config", {}) or {}
    named_graph_uri = str(cfg["named_graph_uri"]).strip() if cfg["graph_option"] == "Use Named Graph" else None
    try:
        df_for_rdf = df.copy()
        if cfg["use_shared_id"]:
            final_subject_base_uri = str(cfg["subject_base_uri"]).strip()
            final_subject_col_name = cfg["shared_id_col"]
            final_id_col_name_for_backend = cfg["default_id_col"] if cfg["id_option"] == "Existing ID Column" else "_generated_id_internal_"
        else:
            final_subject_base_uri = None
            final_subject_col_name = None
            if cfg["id_option"] == "Generated IDs":
                if "_generated_id_" not in df_for_rdf.columns:
                    df_for_rdf = generate_ids(df_for_rdf, id_column_name="_generated_id_", prefix="id_")
                final_id_col_name_for_backend = "_generated_id_"
            else:
                final_id_col_name_for_backend = cfg["default_id_col"]
        if "terms_graph_uri" not in config:
            base_uri = config.get("default_namespace")
            if not base_uri:
                _set_rdf_status("error", "`default_namespace` must be set in config.yaml for SKOS generation.")
                return
            config["terms_graph_uri"] = f"{str(base_uri).rstrip('/')}/graph/skos-vocabulary"
        skos_graph, term_to_uri_lookup = create_skos_graph_and_lookup_map(mapping_df=mapping_df, config=config, data_graph_uri=named_graph_uri)
        active_template = cfg["active_template"] if cfg["active_template"] != "None (use default column mapping)" else None
        rdf_graph = create_rdf_with_mappings(
            df=df_for_rdf,
            mapping_df=mapping_df,
            id_column=final_id_col_name_for_backend,
            string_column=cfg["term_col"],
            iri_column=cfg["uri_col"],
            rdf_role_column=None,
            instance_class_uri=str(cfg["class_uri"]).strip() if cfg["use_class"] and cfg["class_uri"] else None,
            named_graph_uri=named_graph_uri,
            subject_uri_base=final_subject_base_uri,
            subject_column=final_subject_col_name,
            group_config=cfg["group_config"] if cfg["group_active"] else {},
            schema_templates=st.session_state.get("schema_templates", []),
            active_template_name=active_template,
            config=config,
            term_to_concept_uri_map=term_to_uri_lookup,
            input_data_path=str(st.session_state.get(RDF_DATA_FILE_NAME_KEY) or "default_filename"),
            original_column_order=list(df.columns),
        )
        st.session_state["rdf_graph"] = rdf_graph
        st.session_state["skos_graph"] = skos_graph
        st.session_state["rdf_data"] = rdf_graph.serialize(format="turtle")
        st.session_state["skos_data"] = skos_graph.serialize(format="turtle")
        st.session_state["dcat_catalog_data"] = None
        st.session_state["dcat_metadata_data"] = None
        st.session_state["last_named_graph_uri"] = named_graph_uri
        st.session_state["last_rdf_format_display"] = cfg["rdf_format"]
        _set_rdf_status("success", "RDF data graph and SKOS vocabulary generated successfully.")
    except Exception as exc:
        st.session_state["rdf_data"] = None
        st.session_state["skos_data"] = None
        _set_rdf_status("error", f"Error during RDF generation: {exc}")


def _generate_dcat() -> None:
    if create_dcat_catalog is None:
        _set_rdf_status("error", "DCAT catalog generation is not available.")
        return
    rdf_graph = st.session_state.get("rdf_graph")
    skos_graph = st.session_state.get("skos_graph")
    named_graph_uri = st.session_state.get("last_named_graph_uri")
    if not rdf_graph or not named_graph_uri:
        _set_rdf_status("warning", "Generate RDF with a named graph before creating DCAT metadata.")
        return
    config = st.session_state.get("config", {}) or {}
    default_namespace = config.get("default_namespace")
    if not default_namespace:
        _set_rdf_status("error", "`default_namespace` not found in config.yaml.")
        return
    rdf_format_display = st.session_state.get("last_rdf_format_display", "Turtle")
    format_map = {"Turtle": "turtle", "N-Quads": "nquads", "JSON-LD": "json-ld", "RDF/XML": "pretty-xml", "TriG": "trig"}
    reference_data = st.session_state.get("reference_data")
    metadata_config = {
        "title": st.session_state.get("rdf_gen_dcat_title", "My Dataset"),
        "description": st.session_state.get("rdf_gen_dcat_description", "An example dataset."),
        "keywords": [],
        "identifier": reference_data.get("doi", "") if isinstance(reference_data, dict) and reference_data.get("method") == "DOI" else "",
        "creator": [],
        "access_rights": st.session_state.get("rdf_gen_dcat_access_rights", "PUBLIC"),
        "contact_point": str(st.session_state.get("rdf_gen_dcat_contact_point", "") or "").strip(),
        "publisher_name": st.session_state.get("rdf_gen_dcat_publisher_name", "My Organization"),
        "publisher_uri": st.session_state.get("rdf_gen_dcat_publisher_uri", f"{str(default_namespace).rstrip('/')}/organization"),
        "themes": st.session_state.get("rdf_gen_dcat_themes", []),
        "license": st.session_state.get("rdf_gen_dcat_license", next(iter(LICENSES.keys()), "CC BY 4.0")),
        "link_reference": bool(reference_data and st.session_state.get("rdf_gen_dcat_link_reference", True)),
        "reference_data": reference_data if reference_data and st.session_state.get("rdf_gen_dcat_link_reference", True) else None,
    }
    try:
        st.session_state["dcat_catalog_data"] = create_dcat_catalog(
            rdf_graph=rdf_graph,
            skos_graph=skos_graph,
            rdf_format=format_map.get(rdf_format_display, "turtle"),
            data_graph_uri_str=named_graph_uri,
            metadata_config=metadata_config,
            default_namespace=default_namespace,
        )
        _set_rdf_status("success", "DCAT catalog generated successfully.")
    except Exception as exc:
        _set_rdf_status("error", f"Failed to generate DCAT catalog: {exc}")


def _handle_templates_event(event: Dict[str, Any]) -> None:
    action = str(event.get("action", "") or "")
    templates = list(st.session_state.get("schema_templates", []))
    if action == "load_json":
        try:
            loaded = json.loads(str(event.get("content", "") or "[]"))
            if not isinstance(loaded, list) or not all(isinstance(t, dict) and "template_name" in t and "rdf_type" in t and "properties" in t for t in loaded):
                raise ValueError("Expected a list of template objects with template_name, rdf_type, and properties.")
            names = [t.get("template_name") for t in loaded]
            if len(names) != len(set(names)):
                raise ValueError("Template names must be unique.")
            st.session_state["schema_templates"] = loaded
            _set_rdf_status("success", f"Loaded {len(loaded)} schema template(s).")
        except Exception as exc:
            _set_rdf_status("error", f"Could not load schema templates JSON: {exc}")
    elif action in {"add", "update"}:
        template = event.get("template", {}) if isinstance(event.get("template"), dict) else {}
        name = str(template.get("template_name", "") or "").strip()
        rdf_type = str(template.get("rdf_type", "") or "").strip()
        if not name or not rdf_type or not is_valid_uri_simple(rdf_type):
            _set_rdf_status("error", "Template name and a valid RDF Type URI are required.")
            return
        properties = []
        for prop in template.get("properties", []) if isinstance(template.get("properties", []), list) else []:
            if not isinstance(prop, dict):
                continue
            predicate = str(prop.get("predicate", "") or "").strip()
            map_type = str(prop.get("map_type", "") or "")
            value = str(prop.get("value", "") or "").strip()
            if predicate and is_valid_uri_simple(predicate) and map_type in TEMPLATE_MAP_TYPES and value:
                properties.append({"id": prop.get("id") or str(uuid.uuid4()), "predicate": predicate, "map_type": map_type, "value": value})
        template = {"template_name": name, "rdf_type": rdf_type, "properties": properties}
        existing_idx = next((idx for idx, item in enumerate(templates) if item.get("template_name") == name), None)
        if action == "add" and existing_idx is not None:
            _set_rdf_status("error", f"Template '{name}' already exists.")
            return
        if action == "update" and isinstance(event.get("index"), int) and 0 <= int(event["index"]) < len(templates):
            templates[int(event["index"])] = template
        elif existing_idx is not None:
            templates[existing_idx] = template
        else:
            templates.append(template)
        st.session_state["schema_templates"] = templates
        _set_rdf_status("success", f"Template '{name}' saved.")
    elif action == "delete":
        idx = int(event.get("index", -1))
        if 0 <= idx < len(templates):
            removed = templates.pop(idx)
            st.session_state["schema_templates"] = templates
            _set_rdf_status("info", f"Template '{removed.get('template_name', idx)}' deleted.")


def _handle_rdf_mui_event(event: Any) -> bool:
    if not isinstance(event, dict):
        return False
    nonce = event.get("nonce")
    if nonce and st.session_state.get(RDF_COMPONENT_ACTION_NONCE_KEY) == nonce:
        return False
    if nonce:
        st.session_state[RDF_COMPONENT_ACTION_NONCE_KEY] = nonce
    event_type = str(event.get("type", "") or "")
    should_rerun = True
    if event_type == "navigate":
        st.session_state["rdf_generator_active_stage"] = str(event.get("stage", "load") or "load")
    elif event_type == "upload_data":
        _handle_upload("data", event)
    elif event_type == "upload_mapping":
        _handle_upload("mapping", event)
    elif event_type == "load_shared_data":
        _load_shared_data()
    elif event_type == "load_shared_mapping":
        _load_shared_mapping()
    elif event_type == "update_settings":
        _apply_settings(event.get("settings", {}) if isinstance(event.get("settings"), dict) else {})
        if any(k in event.get("settings", {}) for k in ["data_header_row", "data_csv_separator", "data_sheet"]):
            _load_bytes_as_dataframe("data")
        if any(k in event.get("settings", {}) for k in ["mapping_header_row", "mapping_csv_separator", "mapping_sheet"]):
            _load_bytes_as_dataframe("mapping")
    elif event_type == "enrich_uris":
        _run_uri_enrichment()
    elif event_type == "detect_groups":
        df = st.session_state.get("rdf_gen_data_df")
        if isinstance(df, pd.DataFrame):
            st.session_state["rdf_gen_group_suggestions"] = suggest_groups(df.columns)
            _set_rdf_status("success", f"Detected {len(st.session_state['rdf_gen_group_suggestions'])} potential column group(s).")
        else:
            _set_rdf_status("warning", "Load a data table before detecting groups.")
    elif event_type == "templates":
        _handle_templates_event(event)
    elif event_type == "generate_reference":
        _generate_reference(event)
    elif event_type == "generate_rdf":
        _generate_rdf()
    elif event_type == "generate_dcat":
        _apply_settings(event.get("settings", {}) if isinstance(event.get("settings"), dict) else {})
        _generate_dcat()
    elif event_type == "reset_workflow":
        for key in ["rdf_data", "skos_data", "dcat_catalog_data", "dcat_metadata_data", "rdf_graph", "skos_graph", "rdf_gen_data_df", "rdf_gen_mapping_df", "reference_data", "reference_rdf", RDF_DATA_FILE_BYTES_KEY, RDF_MAPPING_FILE_BYTES_KEY, RDF_DATA_FILE_NAME_KEY, RDF_MAPPING_FILE_NAME_KEY]:
            st.session_state[key] = None
        _set_rdf_status("info", "RDF Generator workflow reset.")
    elif event_type == "download_ack":
        should_rerun = False
    else:
        should_rerun = False
    return should_rerun


def main():
    """Render the RDF Generator through a Material-UI component.

    Streamlit now acts only as the backend/session-state bridge. The React/MUI
    component owns the visible workflow, replacing the former Streamlit UI.
    """
    _init_rdf_mui_state()
    snapshot = _build_rdf_snapshot()
    event = _render_rdf_generator_mui(snapshot)
    if _handle_rdf_mui_event(event):
        st.rerun()

# --- End Helper Functions ---

# --- App Execution ---
if __name__ == "__main__":
    main()
