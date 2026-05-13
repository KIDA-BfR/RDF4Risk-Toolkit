# trig_viewer.py - TriG Data Viewer Streamlit App

from __future__ import annotations

import base64
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Add current directory to path for importing Transform_Trig_to_Excel
sys.path.insert(0, str(Path(__file__).parent))

try:
    from Transform_Trig_to_Excel import TriGConverter
except ImportError as e:
    st.error(f"Failed to import Transform_Trig_to_Excel: {e}")
    st.stop()

try:
    from rdflib.namespace import RDF
except ImportError as e:
    st.error(f"Failed to import rdflib: {e}")
    st.info("Please install rdflib: pip install rdflib")
    st.stop()

# --- Helper Functions ---

def create_markdown_link(uri: str, converter: TriGConverter) -> str:
    """Create markdown link [label](target_uri) for a URI."""
    if not uri:
        return ''

    # Get display label
    if uri in converter.uri_to_label:
        label = converter.uri_to_label[uri]
    elif converter._should_use_local_name(uri):
        label = converter._extract_local_name(uri)
    else:
        label = uri

    # Get target URI (with exactMatch/closeMatch if available)
    target_uri = converter._get_hyperlink_uri(uri)

    # Don't link internal concepts
    if converter._is_internal_concept(uri):
        return label

    return f"[{label}]({target_uri})"

def convert_value_to_markdown(value, converter: TriGConverter) -> str:
    """Convert cell value to markdown link if it's a URI."""
    if pd.isna(value) or value == '':
        return ''

    value_str = str(value)

    # Check if it's a valid URI
    if value_str.startswith('http://') or value_str.startswith('https://'):
        return create_markdown_link(value_str, converter)

    return value_str

def create_preview_dataframe(converter: TriGConverter) -> pd.DataFrame:
    """Convert subjects_data to DataFrame with markdown links."""
    if not converter.subjects_data:
        return pd.DataFrame()

    df = pd.DataFrame(converter.subjects_data)

    # Handle subject_uri column
    if 'subject_uri' in df.columns:
        # Add readable Subject column with links
        df['Subject'] = df['subject_uri'].apply(
            lambda uri: create_markdown_link(uri, converter)
        )

        # Move Subject to first column (subject_uri is excluded here)
        cols = ['Subject'] + [c for c in df.columns if c not in ['Subject', 'subject_uri']]
        df = df[cols]

    # Convert all URI values to markdown links
    for col in df.columns:
        if col != 'Subject':
            df[col] = df[col].apply(
                lambda val: convert_value_to_markdown(val, converter)
            )

    return df

def display_named_graph(triples: list, converter: TriGConverter):
    """Display named graph triples as organized sections."""
    if not triples:
        st.info("No data found in this named graph")
        return

    # Organize by subject
    by_subject = defaultdict(list)
    for s, p, o in triples:
        by_subject[str(s)].append((p, o))

    # Display each subject
    for subject_uri in sorted(by_subject.keys()):
        # Find subject type
        subject_type = None
        for p, o in by_subject[subject_uri]:
            if str(p) == str(RDF.type):
                subject_type = converter._extract_local_name(str(o))
                break

        # Create expandable section for each subject
        with st.expander(f"**{subject_type or 'Resource'}**: {subject_uri}", expanded=True):
            # Create table for properties
            props_data = []
            for p, o in by_subject[subject_uri]:
                if str(p) != str(RDF.type):
                    pred_label = converter._extract_local_name(str(p))

                    # Format value with link if it's a URI
                    value_str = str(o)
                    if value_str in converter.uri_to_label:
                        value_display = f"[{converter.uri_to_label[value_str]}]({value_str})"
                    elif value_str.startswith('http'):
                        # Check for external match
                        target = converter._get_hyperlink_uri(value_str)
                        local_name = converter._extract_local_name(value_str)
                        value_display = f"[{local_name}]({target})"
                    else:
                        value_display = value_str

                    props_data.append({"Property": pred_label, "Value": value_display})

            if props_data:
                df = pd.DataFrame(props_data)
                st.markdown(df.to_markdown(index=False))
            else:
                st.info("No properties found")

def get_property_counts(converter: TriGConverter) -> dict:
    """Get usage counts for each property label."""
    counts = defaultdict(int)
    for subject_data in converter.subjects_data:
        for prop_label in subject_data.keys():
            if prop_label != 'subject_uri':
                counts[prop_label] += 1
    return dict(counts)

# --- Web app backend bridge ---

RDF_TABLE_COMPONENT_ACTION_NONCE_KEY = "rdf_to_table_component_action_nonce"
RDF_TABLE_STATUS_MESSAGE_KEY = "rdf_to_table_status_message"
RDF_TABLE_DOWNLOADS_KEY = "rdf_to_table_downloads"

DCAT_GRAPH_URI = 'https://fskx-graphdb.risk-ai-cloud.com/graph/dcat-metadata'
PUBLICATION_GRAPH_URI = 'https://fskx-graphdb.risk-ai-cloud.com/graph/publication-reference'


def _component_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "agentic_reconciliation",
        "components",
        "workflow_config_panel",
        "frontend",
        "build",
    )


def _render_rdf_to_table_mui(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    component_path = _component_path()
    if not os.path.exists(os.path.join(component_path, "index.html")):
        st.error(
            "RDFToTableApp React/Material-UI component build is missing. "
            "Run `npm install && npm run build` in agentic_reconciliation/components/workflow_config_panel/frontend."
        )
        return None
    rdf_to_table_component = components.declare_component("rdf_to_table_panel", path=component_path)
    try:
        return rdf_to_table_component(app="rdf_to_table", snapshot=snapshot, key="rdf_to_table_mui_app", default=None)
    except Exception as exc:
        st.error(f"RDFToTableApp React/Material-UI component could not be rendered. ({exc})")
        return None


def _init_rdf_to_table_state() -> None:
    defaults = {
        "trig_converter": None,
        "trig_file_name": None,
        "rdf_to_table_active_stage": "load",
        RDF_TABLE_STATUS_MESSAGE_KEY: None,
        RDF_TABLE_DOWNLOADS_KEY: {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value.copy() if isinstance(value, dict) else value


def _set_status(severity: str, text: str) -> None:
    st.session_state[RDF_TABLE_STATUS_MESSAGE_KEY] = {"severity": severity, "text": text}


def _preview_records(df: Optional[pd.DataFrame], limit: int = 100) -> List[Dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    return df.head(limit).fillna("").astype(str).to_dict(orient="records")


def _process_trig_data(data: str, file_name: str) -> None:
    try:
        converter = TriGConverter(data=data)
        if not converter.parse_trig():
            _set_status("error", "Failed to parse TriG data. Please check the file format.")
            return
        converter.extract_all_data()
        converter.expand_list_values()
        st.session_state.trig_converter = converter
        st.session_state.trig_file_name = file_name
        st.session_state[RDF_TABLE_DOWNLOADS_KEY] = {}
        st.session_state["rdf_to_table_active_stage"] = "preview"
        _set_status("success", f"Processed {file_name}: {len(converter.graph):,} triples and {len(converter.subjects_data):,} subject rows.")
    except Exception as exc:
        import traceback
        _set_status("error", f"Error processing TriG data: {exc}\n{traceback.format_exc()}")


def _handle_upload(event: Dict[str, Any]) -> None:
    filename = str(event.get("filename", "") or "uploaded.trig").strip() or "uploaded.trig"
    if not filename.lower().endswith(".trig"):
        _set_status("error", "Please upload a .trig file.")
        return
    try:
        raw = base64.b64decode(str(event.get("content_base64", "") or ""))
        data = raw.decode("utf-8")
    except UnicodeDecodeError:
        data = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        _set_status("error", f"Unable to decode uploaded TriG file: {exc}")
        return
    _process_trig_data(data, filename)


def _load_catalog_from_generator() -> None:
    catalog_data = st.session_state.get('dcat_catalog_data')
    if not catalog_data:
        _set_status("warning", "No generated DCAT catalog found in session. Generate one in the RDF Generator first.")
        return
    _process_trig_data(str(catalog_data), "generated_catalog.trig")


def _named_graph_section(graph_uri: str, triples: list, converter: TriGConverter) -> Dict[str, Any]:
    by_subject = defaultdict(list)
    for s, p, o in triples:
        by_subject[str(s)].append((p, o))

    subjects = []
    for subject_uri in sorted(by_subject.keys()):
        subject_type = None
        properties = []
        for p, o in by_subject[subject_uri]:
            if str(p) == str(RDF.type):
                subject_type = converter._extract_local_name(str(o))
                continue
            pred_label = converter._extract_local_name(str(p))
            value_str = str(o)
            if value_str in converter.uri_to_label:
                value_display = f"[{converter.uri_to_label[value_str]}]({value_str})"
            elif value_str.startswith('http'):
                target = converter._get_hyperlink_uri(value_str)
                local_name = converter._extract_local_name(value_str)
                value_display = f"[{local_name}]({target})"
            else:
                value_display = value_str
            properties.append({"Property": pred_label, "Value": value_display})
        subjects.append({
            "subject_uri": subject_uri,
            "subject_type": subject_type or "Resource",
            "properties": properties,
        })
    return {"graph_uri": graph_uri, "subjects": subjects, "triple_count": len(triples)}


def _metadata_snapshot(converter: Optional[TriGConverter]) -> Dict[str, Any]:
    if converter is None:
        return {"dcat": None, "publication": None, "other_graphs": []}
    graph_items = converter.named_graphs_data
    other_graphs = [g for g in graph_items.keys() if g not in [DCAT_GRAPH_URI, PUBLICATION_GRAPH_URI]]
    return {
        "dcat": _named_graph_section(DCAT_GRAPH_URI, graph_items[DCAT_GRAPH_URI], converter) if DCAT_GRAPH_URI in graph_items else None,
        "publication": _named_graph_section(PUBLICATION_GRAPH_URI, graph_items[PUBLICATION_GRAPH_URI], converter) if PUBLICATION_GRAPH_URI in graph_items else None,
        "other_graphs": [_named_graph_section(str(graph_uri), graph_items[graph_uri], converter) for graph_uri in other_graphs],
    }


def _statistics_snapshot(converter: Optional[TriGConverter]) -> Dict[str, Any]:
    if converter is None:
        return {"total_triples": 0, "subjects": 0, "properties": 0, "external_matches": 0, "exact_matches": 0, "close_matches": 0, "namespaces": [], "property_catalog": []}
    property_counts = get_property_counts(converter)
    property_catalog = [
        {
            "Property URI": f"[{uri}]({uri})",
            "Label": label,
            "Usage Count": property_counts.get(label, 0),
            "External Match": "Yes" if (uri in converter.uri_to_exact_match or uri in converter.uri_to_close_match) else "No",
        }
        for uri, label in sorted(converter.property_labels.items(), key=lambda x: property_counts.get(x[1], 0), reverse=True)
    ]
    return {
        "total_triples": len(converter.graph) if converter.graph is not None else 0,
        "subjects": len(converter.subjects_data),
        "properties": len(converter.property_labels),
        "external_matches": len(converter.uri_to_exact_match) + len(converter.uri_to_close_match),
        "exact_matches": len(converter.uri_to_exact_match),
        "close_matches": len(converter.uri_to_close_match),
        "namespaces": [{"Prefix": f"ns{i + 1}", "Namespace URI": uri} for i, uri in enumerate(sorted(converter.namespaces.keys())[:100])],
        "property_catalog": property_catalog,
    }


def _file_stem() -> str:
    file_name = str(st.session_state.get("trig_file_name") or "data.trig")
    return file_name[:-5] if file_name.lower().endswith(".trig") else os.path.splitext(file_name)[0]


def _prepare_downloads() -> None:
    converter = st.session_state.get("trig_converter")
    if not isinstance(converter, TriGConverter):
        _set_status("warning", "Load TriG data before preparing downloads.")
        return
    stem = _file_stem() or "rdf_table"
    downloads: Dict[str, Any] = {
        "csv": pd.DataFrame(converter.subjects_data).to_csv(index=False),
        "csv_filename": f"{stem}_output.csv",
    }
    try:
        excel_path = Path(tempfile.gettempdir()) / f"{stem}_output.xlsx"
        converter.export_to_excel(excel_path)
        downloads["excel_base64"] = base64.b64encode(excel_path.read_bytes()).decode("ascii")
        downloads["excel_filename"] = f"{stem}_output.xlsx"
        excel_path.unlink(missing_ok=True)
    except Exception as exc:
        downloads["excel_error"] = str(exc)
    try:
        md_path = Path(tempfile.gettempdir()) / f"{stem}_metadata.md"
        converter.export_to_markdown(md_path)
        downloads["markdown"] = md_path.read_text(encoding="utf-8")
        downloads["markdown_filename"] = f"{stem}_metadata.md"
        md_path.unlink(missing_ok=True)
    except Exception as exc:
        downloads["markdown_error"] = str(exc)
    st.session_state[RDF_TABLE_DOWNLOADS_KEY] = downloads
    if downloads.get("excel_error") or downloads.get("markdown_error"):
        _set_status("warning", "Downloads were prepared, but at least one format reported an error.")
    else:
        _set_status("success", "Excel, CSV, and Markdown downloads are ready.")


def _build_snapshot() -> Dict[str, Any]:
    converter = st.session_state.get("trig_converter")
    if not isinstance(converter, TriGConverter):
        converter = None
    preview_df = create_preview_dataframe(converter) if converter else pd.DataFrame()
    return {
        "active_stage": st.session_state.get("rdf_to_table_active_stage", "load"),
        "statusMessage": st.session_state.get(RDF_TABLE_STATUS_MESSAGE_KEY),
        "source": {
            "has_data": converter is not None,
            "filename": st.session_state.get("trig_file_name") or "",
            "catalog_available": bool(st.session_state.get('dcat_catalog_data')),
        },
        "data": {
            "rows": len(preview_df) if isinstance(preview_df, pd.DataFrame) else 0,
            "columns": len(preview_df.columns) if isinstance(preview_df, pd.DataFrame) else 0,
            "preview": _preview_records(preview_df, 100),
        },
        "metadata": _metadata_snapshot(converter),
        "statistics": _statistics_snapshot(converter),
        "downloads": st.session_state.get(RDF_TABLE_DOWNLOADS_KEY, {}),
    }


def _handle_mui_event(event: Any) -> bool:
    if not isinstance(event, dict):
        return False
    nonce = event.get("nonce")
    if nonce and st.session_state.get(RDF_TABLE_COMPONENT_ACTION_NONCE_KEY) == nonce:
        return False
    if nonce:
        st.session_state[RDF_TABLE_COMPONENT_ACTION_NONCE_KEY] = nonce
    event_type = str(event.get("type", "") or "")
    should_rerun = True
    if event_type == "navigate":
        st.session_state["rdf_to_table_active_stage"] = str(event.get("stage", "load") or "load")
    elif event_type == "upload_trig":
        _handle_upload(event)
    elif event_type == "load_catalog":
        _load_catalog_from_generator()
    elif event_type == "prepare_downloads":
        _prepare_downloads()
    elif event_type == "reset_workflow":
        st.session_state.trig_converter = None
        st.session_state.trig_file_name = None
        st.session_state[RDF_TABLE_DOWNLOADS_KEY] = {}
        st.session_state["rdf_to_table_active_stage"] = "load"
        _set_status("info", "RDF-to-Table workflow reset.")
    elif event_type == "download_ack":
        should_rerun = False
    else:
        should_rerun = False
    return should_rerun


def main():
    """Render the RDF-to-Table service through a Material-UI component.

    Streamlit is now only the backend/session-state bridge. The React/MUI
    component owns upload, preview, metadata, statistics, and download views.
    """
    _init_rdf_to_table_state()
    snapshot = _build_snapshot()
    event = _render_rdf_to_table_mui(snapshot)
    if _handle_mui_event(event):
        st.rerun()

if __name__ == "__main__":
    main()
