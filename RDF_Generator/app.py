# app.py (RDF Generator UI - Updated for Named Graph Serialization)

import streamlit as st
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
from .uri_utils import process_iris_async, load_api_specs


# --- Configuration ---
# Call set_page_config only when run as a script, not when imported as a module
if __name__ == "__main__":
    st.set_page_config(page_title="RDF Generator", layout="wide")

st.title("Excel/CSV to RDF Converter")
# Configure logging (adjust level as needed for debugging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
st.markdown("Upload your (potentially preprocessed) data table and the corresponding matching table to generate RDF.")

# --- Import Backend Functions ---
try:
    from .rdf_processor import create_rdf_with_mappings
    from .rdf_serializer import serialize_rdf
    from .skos_generator import create_skos_graph_and_lookup_map
    from .reference_handler import DOIToSemOpenAlexConverter
    try:
        from .dcat_generator import display_dcat_builder
    except ImportError:
        # Fallback if display_dcat_builder is not available
        def display_dcat_builder():
            st.warning("DCAT builder functionality is currently not available.")
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

# --- End Helper Functions ---

# --- Main Streamlit App ---
def main():
    # --- Session State Initialization ---
    if 'config' not in st.session_state:
        st.session_state['config'] = load_default_config()
    if 'rdf_data' not in st.session_state:
        st.session_state['rdf_data'] = None
    if 'skos_data' not in st.session_state:
        st.session_state['skos_data'] = None
    if 'dcat_catalog_data' not in st.session_state:
        st.session_state['dcat_catalog_data'] = None
    if 'dcat_metadata_data' not in st.session_state:
        st.session_state['dcat_metadata_data'] = None
    if 'rdf_preview' not in st.session_state:
        st.session_state['rdf_preview'] = None
    if 'rdf_gen_data_df' not in st.session_state:
        st.session_state['rdf_gen_data_df'] = None
    if 'rdf_gen_mapping_df' not in st.session_state:
        st.session_state['rdf_gen_mapping_df'] = None
    if 'rdf_gen_data_source_msg' not in st.session_state:
        st.session_state['rdf_gen_data_source_msg'] = None
    if 'rdf_gen_mapping_source_msg' not in st.session_state:
        st.session_state['rdf_gen_mapping_source_msg'] = None
    if 'main_uploader_key' not in st.session_state:
        st.session_state['main_uploader_key'] = 0
    if 'map_uploader_key' not in st.session_state:
        st.session_state['map_uploader_key'] = 0
    if 'schema_templates' not in st.session_state:
        st.session_state['schema_templates'] = []
    if 'editing_template_idx' not in st.session_state:
        st.session_state['editing_template_idx'] = None
    if 'active_template_buffer' not in st.session_state:
        st.session_state['active_template_buffer'] = None
    # Reference handler session state
    if 'reference_data' not in st.session_state:
        st.session_state['reference_data'] = None
    if 'reference_rdf' not in st.session_state:
        st.session_state['reference_rdf'] = None
    # --- End Session State Initialization ---

    # Retrieve from session state if already loaded
    df = st.session_state.get('rdf_gen_data_df')
    mapping_df = st.session_state.get('rdf_gen_mapping_df')
    load_success = True # Assume true initially, set to false on errors

    # 1. Data Table Input Section
    st.subheader("1. Load Data Table")
    col1_data, col2_data = st.columns([2,1])
    with col1_data:
        uploaded_file = st.file_uploader("Upload data (Excel/CSV)", type=["xlsx", "csv"], key=f"main_uploader_{st.session_state.main_uploader_key}")
        
        # Persist header row
        if 'rdf_gen_header_row_main' not in st.session_state:
            st.session_state['rdf_gen_header_row_main'] = 1
            
        header_row_main = st.number_input("Header row (1-based)", min_value=1, 
                                          value=st.session_state['rdf_gen_header_row_main'],
                                          key="main_header_input", 
                                          help="Row number containing the column names.")
        st.session_state['rdf_gen_header_row_main'] = header_row_main

        # Excel sheet selection (only show for Excel files)
        sheet_name_main = None
        if uploaded_file and uploaded_file.name.endswith((".xlsx", ".xls")):
            sheets = get_excel_sheets(uploaded_file)
            if sheets:
                if len(sheets) > 1:
                    sheet_name_main = st.selectbox(
                        "Select Excel Sheet",
                        sheets,
                        key="main_sheet_selector",
                        help="Choose which sheet to use from the Excel file"
                    )
                else:
                    sheet_name_main = sheets[0]
                    st.info(f"Using sheet: {sheet_name_main}")
            else:
                st.warning("Could not read Excel sheets. Will use the first sheet.")

        # CSV separator selection (only show for CSV files)
        csv_separator_main = None
        if uploaded_file and uploaded_file.name.endswith('.csv'):
            separator_options = ["Auto-detect", "Comma (,)", "Semicolon (;)", "Tab (\\t)", "Pipe (|)"]
            separator_values = [None, ",", ";", "\t", "|"]
            separator_choice = st.selectbox(
                "CSV Separator",
                separator_options,
                key="main_csv_separator",
                help="Choose the delimiter used in your CSV file"
            )
            csv_separator_main = separator_values[separator_options.index(separator_choice)]
    with col2_data:
        st.write("") # Spacer
        st.write("") # Spacer
        if st.session_state.get('shared_preprocessed_data') is not None:
            if st.button("Load Preprocessed Data from Generator", key="load_shared_data_rdf"):
                df_candidate = st.session_state.shared_preprocessed_data
                if df_candidate is not None and isinstance(df_candidate, pd.DataFrame):
                    st.session_state.rdf_gen_data_df = df_candidate.copy()
                    df = st.session_state.rdf_gen_data_df # Update local df
                    st.session_state.rdf_gen_data_source_msg = "Loaded Preprocessed Data from Matching Table Generator."
                    st.session_state.main_uploader_key += 1 # Reset file uploader
                    st.rerun()
                else:
                    st.warning("Preprocessed data from generator is not a valid DataFrame.")
        else:
            st.caption("No preprocessed data from generator found in session.")

    if uploaded_file:
        df_uploaded = read_uploaded_file(uploaded_file, header_row_main, csv_separator_main, sheet_name_main)
        if df_uploaded is not None:
            st.session_state.rdf_gen_data_df = df_uploaded
            df = st.session_state.rdf_gen_data_df # Update local df
            sheet_info = f" (sheet: {sheet_name_main})" if sheet_name_main else ""
            st.session_state.rdf_gen_data_source_msg = f"Loaded Data Table from uploaded file: {uploaded_file.name}{sheet_info}."
            # No need to rerun here, will flow naturally
        else: # read_uploaded_file shows error
            st.session_state.rdf_gen_data_df = None
            df = None
            load_success = False
            st.session_state.rdf_gen_data_source_msg = "Failed to load data table from upload."


    # 2. Matching Table Input Section
    st.subheader("2. Load Matching Table")
    col1_map, col2_map = st.columns([2,1])
    with col1_map:
        mapping_file = st.file_uploader("Upload mapping table (Excel/CSV)", type=["xlsx", "csv"], key=f"map_uploader_{st.session_state.map_uploader_key}")
        
        # Persist mapping header row
        if 'rdf_gen_header_row_mapping' not in st.session_state:
            st.session_state['rdf_gen_header_row_mapping'] = 1
            
        header_row_mapping = st.number_input("Header row (1-based)", min_value=1, 
                                             value=st.session_state['rdf_gen_header_row_mapping'],
                                             key="map_header_input", help="Header row of the mapping file.")
        st.session_state['rdf_gen_header_row_mapping'] = header_row_mapping

        # Excel sheet selection for mapping table (only show for Excel files)
        sheet_name_mapping = None
        if mapping_file and mapping_file.name.endswith((".xlsx", ".xls")):
            sheets = get_excel_sheets(mapping_file)
            if sheets:
                if len(sheets) > 1:
                    sheet_name_mapping = st.selectbox(
                        "Select Excel Sheet",
                        sheets,
                        key="mapping_sheet_selector",
                        help="Choose which sheet to use from the Excel file"
                    )
                else:
                    sheet_name_mapping = sheets[0]
                    st.info(f"Using sheet: {sheet_name_mapping}")
            else:
                st.warning("Could not read Excel sheets. Will use the first sheet.")

        # CSV separator selection for mapping table (only show for CSV files)
        csv_separator_mapping = None
        if mapping_file and mapping_file.name.endswith('.csv'):
            separator_options = ["Auto-detect", "Comma (,)", "Semicolon (;)", "Tab (\\t)", "Pipe (|)"]
            separator_values = [None, ",", ";", "\t", "|"]
            separator_choice = st.selectbox(
                "CSV Separator",
                separator_options,
                key="mapping_csv_separator",
                help="Choose the delimiter used in your CSV file"
            )
            csv_separator_mapping = separator_values[separator_options.index(separator_choice)]
    with col2_map:
        st.write("") # Spacer
        st.write("") # Spacer
        if st.session_state.get('shared_reconciled_matching_table') is not None:
            if st.button("Load Reconciled Matching Table", key="load_shared_map_rdf"):
                map_df_candidate = st.session_state.shared_reconciled_matching_table
                if map_df_candidate is not None and isinstance(map_df_candidate, pd.DataFrame):
                    current_mapping_df = map_df_candidate.copy()

                    # Try to get all original terms that were sent for reconciliation
                    # This list must be populated by the Reconciliation app page.
                    all_original_terms = st.session_state.get('all_terms_for_reconciliation')

                    if all_original_terms and isinstance(all_original_terms, list):
                        default_no_match_entry = {
                            "URI": "No Match",
                            "RDF Role": "predicate", # Based on common usage and screenshot
                            "Match Type": "No Match",
                            "Source Provider": "", # Or "System Generated"
                            "Confirmed Display String": "--- No Match ---"
                        }
                        
                        existing_terms_in_map = set()
                        if not current_mapping_df.empty and "Term" in current_mapping_df.columns:
                            existing_terms_in_map = set(current_mapping_df['Term'].astype(str).unique())

                        new_rows_for_missing_terms = []
                        for term_obj in all_original_terms:
                            term_str = str(term_obj) # Ensure term is string for comparison
                            if term_str not in existing_terms_in_map:
                                row = {"Term": term_str, **default_no_match_entry}
                                new_rows_for_missing_terms.append(row)

                        if new_rows_for_missing_terms:
                            missing_terms_df = pd.DataFrame(new_rows_for_missing_terms)
                            if current_mapping_df.empty:
                                current_mapping_df = missing_terms_df
                            else:
                                # Concatenate. Pandas handles column alignment (adds NaNs for differing cols).
                                current_mapping_df = pd.concat([current_mapping_df, missing_terms_df], ignore_index=True)
                    
                    # Standardize final DataFrame columns
                    final_df_columns = ["Term", "URI", "RDF Role", "Match Type", "Source Provider", "Confirmed Display String"]
                    if current_mapping_df.empty:
                        # If after all processing, df is empty, create one with standard columns
                        current_mapping_df = pd.DataFrame(columns=final_df_columns)
                    else:
                        # Ensure standard columns exist, add with a default if not.
                        for col in final_df_columns:
                            if col not in current_mapping_df.columns:
                                # Use a consistent default, e.g., pd.NA or "--- No Match ---"
                                # For simplicity, using "--- No Match ---" if it's a new column.
                                current_mapping_df[col] = pd.NA # Using pd.NA for potentially non-string columns
                        
                        # Reorder to have standard columns first, then any extras that might have existed.
                        # This also ensures that if a standard column was added, it's included.
                        extra_cols = [col for col in current_mapping_df.columns if col not in final_df_columns]
                        current_mapping_df = current_mapping_df[final_df_columns + extra_cols]


                    st.session_state.rdf_gen_mapping_df = current_mapping_df.copy()
                    mapping_df = st.session_state.rdf_gen_mapping_df # Update local mapping_df
                    st.session_state.rdf_gen_mapping_source_msg = "Loaded Reconciled Matching Table from Reconciliation App (prefilled 'No Match' entries)."
                    st.session_state.map_uploader_key += 1 # Reset file uploader
                    st.rerun()
                else:
                    st.warning("Reconciled matching table from session is not a valid DataFrame.")
        else:
            st.caption("No reconciled matching table from session found.")

    if mapping_file:
        map_df_uploaded = read_uploaded_file(mapping_file, header_row_mapping, csv_separator_mapping, sheet_name_mapping)
        if map_df_uploaded is not None:
            required_map_cols = ["Term", "URI", "RDF Role"] # "Match Type" is optional
            missing_cols = [col for col in required_map_cols if col not in map_df_uploaded.columns]
            if missing_cols:
                st.warning(f"Uploaded mapping table missing required columns: {', '.join(missing_cols)}.")
                st.session_state.rdf_gen_mapping_df = None
                mapping_df = None
                load_success = False
                st.session_state.rdf_gen_mapping_source_msg = "Uploaded mapping table is invalid (missing columns)."
            else:
                st.session_state.rdf_gen_mapping_df = map_df_uploaded
                mapping_df = st.session_state.rdf_gen_mapping_df # Update local mapping_df
                sheet_info = f" (sheet: {sheet_name_mapping})" if sheet_name_mapping else ""
                st.session_state.rdf_gen_mapping_source_msg = f"Loaded Matching Table from uploaded file: {mapping_file.name}{sheet_info}."
        else: # read_uploaded_file shows error
            st.session_state.rdf_gen_mapping_df = None
            mapping_df = None
            load_success = False
            st.session_state.rdf_gen_mapping_source_msg = "Failed to load matching table from upload."
            
    # Display source messages
    if st.session_state.get('rdf_gen_data_source_msg'):
        st.info(st.session_state.rdf_gen_data_source_msg)
    if st.session_state.get('rdf_gen_mapping_source_msg'):
        st.info(st.session_state.rdf_gen_mapping_source_msg)

    # Validation and Proceed
    if df is None or mapping_df is None:
        # Explicitly ensure these are boolean by checking if the source (upload or session) is not None
        data_table_load_attempted = (uploaded_file is not None) or \
                                    (st.session_state.get('shared_preprocessed_data') is not None)
        mapping_table_load_attempted = (mapping_file is not None) or \
                                       (st.session_state.get('shared_reconciled_matching_table') is not None)

        if df is None and data_table_load_attempted:
            pass # Error/info message for data table loading failure was already shown or will be
        elif mapping_df is None and mapping_table_load_attempted:
            pass # Error/info message for mapping table loading failure was already shown
        elif df is None and not data_table_load_attempted: # df is None and no attempt was made to load it
            st.info("Please load a Data Table to proceed.")
            load_success = False
        elif mapping_df is None and not mapping_table_load_attempted: # mapping_df is None and no attempt was made
            st.info("Please load a Matching Table to proceed.")
            load_success = False
        elif df is None or mapping_df is None: # General catch-all if one is still None for other reasons
             st.info("Please load both a Data Table and a Matching Table to proceed.")
             load_success = False
        # If both are loaded, load_success remains true by default.

    if load_success and df is not None and mapping_df is not None:
        # Final check on mapping_df columns after all loading paths
        required_map_cols_final = ["Term", "URI", "RDF Role"]
        missing_cols_final = [col for col in required_map_cols_final if col not in mapping_df.columns]
        if missing_cols_final:
            st.error(f"The loaded Matching Table is missing critical columns: {', '.join(missing_cols_final)}. Cannot proceed.")
            load_success = False # Prevent further processing

    if load_success and df is not None and mapping_df is not None:
        st.success("Data Table and Matching Table are ready.")
        st.markdown("---")
        # Optional Previews
        with st.expander("Data Preview", expanded=False): # Changed title
            if df is not None:
                st.dataframe(df) # Display full df
            else:
                st.info("Data table not loaded yet for preview.")
        with st.expander("Mapping Table Preview", expanded=False): # Changed title
            required_map_cols_display = ["Term", "URI", "RDF Role"]
            # Ensure mapping_df is not None before trying to access columns
            if mapping_df is not None:
                missing_cols_display = [col for col in required_map_cols_display if col not in mapping_df.columns]
                if missing_cols_display:
                    st.warning(f"Preview might be incomplete. Mapping table is missing columns: {', '.join(missing_cols_display)}.")
                st.dataframe(mapping_df) # Display full mapping_df
            else:
                st.info("Mapping table not loaded yet for preview.")

        # --- 3. Enrich URIs (Optional) ---
        st.subheader("3. Enrich URIs (Optional)")
        with st.expander("Find labels and clickable links for external URIs"):
            st.info("""
                **Enrich URIs** allows you to automatically fetch additional details for the external URIs in your mapping table.
                
                **What happens here?**
                *   The system connects to external services like **BioPortal, OLS (Ontology Lookup Service), and Ontobee**.
                *   For each URI, it retrieves the **human-readable label** (e.g., "Campylobacter") and the **acronym** of its source ontology.
                *   It also generates **clickable links** that take you directly to the term's page in the original ontology.
                
                **Why use this?**
                It transforms abstract URIs into meaningful information, making it much easier to review your mappings and understand the semantic context of your data.
            """)
            enrich_uris = st.checkbox("Enable URI Enrichment?", key="enrich_uris_cb", help="Looks up external URIs in services like BioPortal to fetch labels and create clickable links.")
            if enrich_uris:
                st.info("This process can take some time depending on the number of URIs and the speed of external APIs.")
                if st.button("Run URI Enrichment", key="run_enrichment_btn"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    def update_progress(value):
                        progress_bar.progress(value)
                        status_text.text(f"Enrichment progress: {int(value * 100)}%")

                    with st.spinner("Enriching URIs..."):
                        try:
                            api_specs_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
                            api_specs = load_api_specs(api_specs_path)
                            
                            mapping_df_for_enrichment = mapping_df.rename(columns={"URI": "Mapped ID"})
                            
                            enriched_df = asyncio.run(process_iris_async(
                                mapping_df_for_enrichment,
                                api_specs,
                                progress_callback=update_progress
                            ))

                            if not enriched_df.empty:
                                # Merge results back. Use a temporary suffix for overlapping columns.
                                mapping_df = pd.merge(
                                    mapping_df,
                                    enriched_df,
                                    left_on='URI',
                                    right_on='iri',
                                    how='left',
                                    suffixes=('', '_enriched')
                                )
                                mapping_df.drop(columns=['iri'], inplace=True, errors='ignore')

                                # Update original columns with new data where available
                                for col in ['label', 'ui_link', 'acronym', 'source', 'message']:
                                    enriched_col = f"{col}_enriched"
                                    if enriched_col in mapping_df.columns:
                                        # Use enriched value if it exists, otherwise keep original
                                        mapping_df[col] = mapping_df[enriched_col].fillna(mapping_df.get(col, ''))
                                        mapping_df.drop(columns=[enriched_col], inplace=True)
                                    elif col not in mapping_df.columns:
                                         mapping_df[col] = '' # Ensure column exists

                                st.session_state.rdf_gen_mapping_df = mapping_df
                                st.success("URI enrichment complete!")
                                progress_bar.empty() # Clean up progress bar
                                status_text.empty() # Clean up status text

                                # Preview the enriched data
                                st.subheader("Enriched Data Preview")
                                st.dataframe(mapping_df)
                            else:
                                st.warning("Enrichment process returned no data.")
                                progress_bar.empty()
                                status_text.empty()

                        except Exception as e:
                            st.error(f"An error occurred during URI enrichment: {e}")
                            st.exception(e)
                            progress_bar.empty()
                            status_text.empty()
        
        st.markdown("---")

        # --- 4. Schema Mapping Templates Section ---
        st.subheader("4. Define Schema Mapping Templates (Optional)")

        # --- Help/Guidance Expander (Moved outside the main template management expander) ---
        with st.expander("How to Use Schema Mapping Templates (Examples & Guidance)", expanded=False):
            help_text = """
            Schema Mapping Templates allow you to define explicit rules for transforming your tabular data into
            more complex RDF structures, especially when dealing with specific ontologies and nested information.

            **1. What Schema Mapping Does (Semantic Example):**

            Imagine you have a data table with a column `MycotoxinLevel_ppb` representing the concentration
            of a mycotoxin in parts-per-billion, and another column `SampleID` for the sample identifier.
            You want to model this using the SOSA (Sensor, Observation, Sample, and Actuator) and QUDT
            (Quantities, Units, Dimensions, and Types) ontologies.

            This would typically involve **two schema templates** to create a nested structure:

            1.  An **Observation Template** (e.g., you might name it `SOSA_Observation_Template` in the UI):
                *   Its `RDF Type URI` would be `http://www.w3.org/ns/sosa/Observation`.
                *   It would have properties like:
                    *   Predicate: `http://www.w3.org/ns/sosa/observedProperty`, Mapping Type: `Fixed URI`, Value: (e.g., `http://example.org/props/DONConcentration`).
                    *   Predicate: `http://www.w3.org/ns/sosa/hasFeatureOfInterest`, Mapping Type: `Column Value (URI)`, Value: (your `SampleID` column, assuming it contains URIs).
                *   Crucially, to create the nesting, it would have a property for `sosa:hasResult`:
                    *   Predicate: `http://www.w3.org/ns/sosa/hasResult`.
                    *   **Mapping Type**: `Nested Template`.
                    *   **Value**: You would select the *name* of your second template from the dropdown (e.g., `QUDT_Result_Template`). This tells the system to use that other template to describe the object of `sosa:hasResult`.

            2.  A **Result Template** (e.g., you might name it `QUDT_Result_Template` in the UI):
                *   Its `RDF Type URI` would be `http://qudt.org/vocab/quantitykind/Concentration` (or perhaps `sosa:Result`, which then itself has a value that is a `qudt:QuantityValue`).
                *   It would have properties like:
                    *   Predicate: `http://qudt.org/schema/qudt/numericValue`, Mapping Type: `Column Value (Literal)`, Value: (your `MycotoxinLevel_ppb` column).
                    *   Predicate: `http://qudt.org/schema/qudt/unit`, Mapping Type: `Fixed URI`, Value: `http://qudt.org/vocab/unit/PPB`.

            When the `SOSA_Observation_Template` is applied to a row in your data:
            *   An RDF entity of type `sosa:Observation` is created.
            *   Its direct properties (like `sosa:observedProperty`) are added.
            *   For the `sosa:hasResult` property, because it's a `Nested Template`, a new (usually blank) RDF node is created.
            *   This new node is then described using the rules defined in your `QUDT_Result_Template` (e.g., it gets an `rdf:type` of `qudt:QuantityKind/Concentration`, a `qudt:numericValue` from the data, and a `qudt:unit`).

            This creates a richer, more standardized RDF output than direct column-to-predicate mapping. The JSON example provided further down in this guide illustrates how such a two-template setup would look in the exported/imported JSON file.
            
            **2. How to Fill Template Fields:**

            *   **Template Name:** A unique, descriptive name for your template (e.g., "SOSA Observation for Mycotoxins").
            *   **RDF Type URI:** The main `rdf:type` for entities created using this template (e.g., `http://www.w3.org/ns/sosa/Observation`).
            *   **Properties:**
                *   **Predicate URI:** The URI of the property (e.g., `http://www.w3.org/ns/sosa/hasFeatureOfInterest`).
                *   **Mapping Type:**
                    *   `Column Value (Literal)`: The object will be a Literal, taking its value from the selected data column. Datatype is auto-guessed.
                    *   `Column Value (URI)`: The object will be a URIRef, taking its value from the selected data column. Ensure the column contains valid URIs.
                    *   `Fixed URI`: The object will be the exact URIRef you provide.
                    *   `Nested Template`: The object will be a new Blank Node. This Blank Node will then be described by another template you select from your defined templates. This is how you create nested structures.
                *   **Value:**
                    *   For `Column Value` types: Select the relevant column from your data table.
                    *   For `Fixed URI`: Enter the full URI.
                    *   For `Nested Template`: Select the name of another template you've already defined.

            **3. What You Might Need:**

            *   **Ontology Knowledge:** Familiarity with the ontologies (e.g., SOSA, QUDT, SKOS, FOAF) you want to use. You'll need to know the URIs for classes and properties.
            *   **Data Structure Understanding:** A clear idea of how your input data columns should map to the target ontology concepts.
            *   **Valid URIs:** Ensure all URIs for types, predicates, and fixed values are correct.

            **4. Example JSON Structure for Import/Export:**

            If you save your templates, the JSON file will look something like this:
            ```json
            [
              {
                "template_name": "SOSA Observation Example",
                "rdf_type": "http://www.w3.org/ns/sosa/Observation",
                "properties": [
                  {
                    "id": "some-unique-id-1",
                    "predicate": "http://www.w3.org/ns/sosa/observedProperty",
                    "map_type": "Fixed URI",
                    "value": "http://example.org/props/MycotoxinConcentration"
                  },
                  {
                    "id": "some-unique-id-2",
                    "predicate": "http://www.w3.org/ns/sosa/hasFeatureOfInterest",
                    "map_type": "Column Value (URI)",
                    "value": "Sample_URI_Column"
                  },
                  {
                    "id": "some-unique-id-3",
                    "predicate": "http://www.w3.org/ns/sosa/hasResult",
                    "map_type": "Nested Template",
                    "value": "MycotoxinResultTemplate"
                  }
                ]
              },
              {
                "template_name": "MycotoxinResultTemplate",
                "rdf_type": "http://qudt.org/vocab/quantitykind/Concentration",
                "properties": [
                  {
                    "id": "some-unique-id-4",
                    "predicate": "http://qudt.org/schema/qudt/numericValue",
                    "map_type": "Column Value (Literal)",
                    "value": "MycotoxinLevel_ppb_Column"
                  },
                  {
                    "id": "some-unique-id-5",
                    "predicate": "http://qudt.org/schema/qudt/unit",
                    "map_type": "Fixed URI",
                    "value": "http://qudt.org/vocab/unit/PPB"
                  }
                ]
              }
            ]
            ```
            **Note:** The `id` field for properties is generated internally for UI management and does not need to be manually set when creating a JSON for import, though it will be present in exported files.
            """
            st.markdown(textwrap.dedent(help_text))
        
        # Main expander for creating and managing templates
        with st.expander("Create, Load, Save, and Manage Schema Mapping Templates", expanded=True):
            data_cols_for_mapping = list(df.columns) if df is not None else []
            
            # --- Load/Save Templates ---
            st.markdown("**Load/Save Schema Templates**")
            col_load, col_save = st.columns(2)
            with col_load:
                uploaded_template_file = st.file_uploader(
                    "Load Templates from JSON",
                    type=["json"],
                    key="template_uploader"
                )
                if uploaded_template_file is not None:
                    try:
                        loaded_templates_data = json.load(uploaded_template_file)
                        # Basic validation (can be more thorough)
                        if isinstance(loaded_templates_data, list) and \
                           all(isinstance(t, dict) and "template_name" in t and "rdf_type" in t and "properties" in t for t in loaded_templates_data):
                            
                            # Check for duplicate template names within the loaded file
                            loaded_template_names = [t['template_name'] for t in loaded_templates_data if t.get('template_name')]
                            if len(loaded_template_names) != len(set(loaded_template_names)):
                                st.error("Error: Duplicate template names found within the uploaded file. Please ensure all template names are unique.")
                            else:
                                # For now, replace existing. Offer merge/append options later.
                                st.session_state.schema_templates = loaded_templates_data
                                st.session_state.active_template_buffer = None # Reset any active editing
                                st.session_state.editing_template_idx = None
                                st.success(f"Successfully loaded {len(loaded_templates_data)} templates. Existing templates were replaced.")
                                st.rerun() # Rerun to reflect loaded templates
                        else:
                            st.error("Invalid template file format. Expected a list of template objects, each with 'template_name', 'rdf_type', and 'properties' keys.")
                    except json.JSONDecodeError:
                        st.error("Error decoding JSON from the uploaded template file.")
                    except Exception as e:
                        st.error(f"An error occurred while loading templates: {e}")
            
            with col_save:
                if st.session_state.schema_templates:
                    templates_json_str = json.dumps(st.session_state.schema_templates, indent=2)
                    st.download_button(
                        label="Save Current Templates to JSON",
                        data=templates_json_str,
                        file_name="schema_templates.json",
                        mime="application/json",
                        key="save_templates_button"
                    )
                else:
                    st.caption("No templates defined yet to save.")
            st.markdown("---")
            # --- End Load/Save Templates ---


            # Display existing templates and management buttons
            if st.session_state.schema_templates:
                st.markdown("**Manage Existing Templates:**")
                for idx, template in enumerate(st.session_state.schema_templates):
                    cols = st.columns([0.7, 0.15, 0.15])
                    cols[0].write(f"- {template['template_name']} (Type: {template['rdf_type']})")
                    if cols[1].button("Edit", key=f"edit_template_{idx}"):
                        st.session_state.editing_template_idx = idx
                        # Deepcopy to avoid modifying the original in session state directly during edits
                        st.session_state.active_template_buffer = pd.io.json.loads(pd.io.json.dumps(st.session_state.schema_templates[idx]))
                        st.rerun()
                    if cols[2].button("Delete", key=f"delete_template_{idx}"):
                        st.session_state.schema_templates.pop(idx)
                        if st.session_state.editing_template_idx == idx:
                            st.session_state.editing_template_idx = None
                            st.session_state.active_template_buffer = None
                        elif st.session_state.editing_template_idx is not None and st.session_state.editing_template_idx > idx:
                            st.session_state.editing_template_idx -=1
                        st.rerun()
                st.markdown("---")

            # Button to start defining a new template
            if st.session_state.active_template_buffer is None:
                if st.button("Add New Schema Template"):
                    st.session_state.active_template_buffer = {
                        "template_name": "",
                        "rdf_type": "",
                        "properties": []
                    }
                    st.session_state.editing_template_idx = None # Indicates new template
                    st.rerun()

            # Form for adding/editing a template
            if st.session_state.active_template_buffer is not None:
                current_template = st.session_state.active_template_buffer
                form_title = "Editing Template" if st.session_state.editing_template_idx is not None else "Define New Schema Template"
                
                with st.form(key="schema_template_form"):
                    st.markdown(f"**{form_title}:**")
                    current_template["template_name"] = st.text_input(
                        "Template Name (must be unique)",
                        value=current_template["template_name"],
                        key="template_name_input"
                    )
                    current_template["rdf_type"] = st.text_input(
                        "RDF Type URI (e.g., sosa:Observation)",
                        value=current_template["rdf_type"],
                        key="template_rdf_type_input"
                    )

                    st.markdown("**Properties:**")
                    for i, prop in enumerate(current_template["properties"]):
                        prop_cols = st.columns([0.4, 0.2, 0.3, 0.1])
                        prop_cols[0].text(f"Predicate: {prop['predicate']}")
                        prop_cols[1].text(f"Type: {prop['map_type']}")
                        
                        val_display = prop['value']
                        if prop['map_type'] == "Nested Template" and isinstance(prop['value'], dict): # Should be template name
                             val_display = prop['value'].get('template_name', 'Error: Nested template name missing')

                        prop_cols[2].text(f"Value: {val_display}")
                        
                        if prop_cols[3].form_submit_button("❌", key=f"remove_prop_{prop['id']}"):
                            current_template["properties"].pop(i)
                            st.rerun() # Rerun to reflect removal in the form

                    st.markdown("---")
                    st.markdown("**Add New Property:**")
                    new_prop_predicate = st.text_input("Predicate URI", key="new_prop_predicate")
                    
                    map_type_options = ["Column Value (Literal)", "Column Value (URI)", "Fixed URI", "Nested Template"]
                    new_prop_map_type = st.selectbox("Mapping Type", map_type_options, key="new_prop_map_type")

                    new_prop_value = None
                    if new_prop_map_type in ["Column Value (Literal)", "Column Value (URI)"]:
                        if not data_cols_for_mapping:
                            st.warning("Data table not loaded or has no columns. Cannot map from column.")
                            new_prop_value_col_selected = None
                        else:
                            new_prop_value_col_selected = st.selectbox("Select Data Column", [""] + data_cols_for_mapping, key="new_prop_value_col")
                        new_prop_value = new_prop_value_col_selected
                    elif new_prop_map_type == "Fixed URI":
                        new_prop_value = st.text_input("Fixed URI Value", key="new_prop_value_fixed")
                    elif new_prop_map_type == "Nested Template":
                        available_nested_templates = [
                            t["template_name"] for t_idx, t in enumerate(st.session_state.schema_templates)
                            if st.session_state.editing_template_idx is None or t_idx != st.session_state.editing_template_idx
                        ]
                        if current_template["template_name"]: # Cannot nest itself before it's saved
                             if current_template["template_name"] in available_nested_templates:
                                available_nested_templates.remove(current_template["template_name"])

                        if not available_nested_templates:
                            st.info("No other templates available for nesting. Save this template first or create other templates.")
                            new_prop_value_linked_selected = None
                        else:
                            new_prop_value_linked_selected = st.selectbox("Select Template to Nest", [""] + available_nested_templates, key="new_prop_value_linked")
                        new_prop_value = new_prop_value_linked_selected
                    
                    # Button to add the defined property to the current template buffer
                    if st.form_submit_button("Add This Property to Template"):
                        if not new_prop_predicate or not new_prop_map_type or not new_prop_value:
                            st.warning("Predicate, Mapping Type, and Value are required to add a property.")
                        elif new_prop_map_type == "Fixed URI" and not is_valid_uri_simple(new_prop_value):
                             st.warning(f"The Fixed URI '{new_prop_value}' for predicate '{new_prop_predicate}' is not a valid URI.")
                        elif not is_valid_uri_simple(new_prop_predicate):
                            st.warning(f"The Predicate URI '{new_prop_predicate}' is not a valid URI.")
                        else:
                            current_template["properties"].append({
                                "id": str(uuid.uuid4()), # Unique ID for this property instance
                                "predicate": new_prop_predicate,
                                "map_type": new_prop_map_type,
                                "value": new_prop_value
                            })
                            # Clear inputs for next property - This doesn't work well with st.form_submit_button directly
                            # For simplicity, user has to clear them manually or we need more complex state handling
                            st.rerun() # Rerun to update property list and clear inputs via key change if possible

                    st.markdown("---")
                    # Form submission buttons
                    submit_col1, submit_col2, submit_col3 = st.columns(3)
                    with submit_col1:
                        if st.form_submit_button("Save Template"):
                            is_new_template = st.session_state.editing_template_idx is None
                            # Validate template name uniqueness if new or changed
                            if not current_template["template_name"]:
                                st.error("Template Name cannot be empty.")
                            elif not current_template["rdf_type"] or not is_valid_uri_simple(current_template["rdf_type"]):
                                st.error("Valid RDF Type URI is required.")
                            else:
                                existing_names = [
                                    t["template_name"] for i, t in enumerate(st.session_state.schema_templates)
                                    if (is_new_template or i != st.session_state.editing_template_idx)
                                ]
                                if current_template["template_name"] in existing_names:
                                    st.error(f"Template Name '{current_template['template_name']}' already exists. Please choose a unique name.")
                                else:
                                    saved_template_data = pd.io.json.loads(pd.io.json.dumps(current_template)) # Deepcopy
                                    if is_new_template:
                                        st.session_state.schema_templates.append(saved_template_data)
                                    else:
                                        st.session_state.schema_templates[st.session_state.editing_template_idx] = saved_template_data
                                    
                                    st.session_state.active_template_buffer = None
                                    st.session_state.editing_template_idx = None
                                    st.success(f"Template '{saved_template_data['template_name']}' saved.")
                                    st.rerun()
                    with submit_col2:
                        if st.form_submit_button("Cancel"):
                            st.session_state.active_template_buffer = None
                            st.session_state.editing_template_idx = None
                            st.rerun()
        st.markdown("---") # End of expander for schema templates

        # --- 5. Grouping Configuration ---
        st.markdown("---")
        st.subheader("5. Configure Grouping (Optional)")
        with st.expander("Group Related Columns into Substructures"):
            st.info("""
                **Grouping** allows you to bundle multiple related columns into a **structured sub-entity** (Blank Node) instead of attaching them all directly to the main subject.
                
                **Benefit:** This is useful for modeling complex properties that belong together.
                
                **Example:** If you have columns for `Measurement Value` and `Measurement Unit`, you can group them under a `hasAnalysisResult` predicate. This creates a separate node that contains both the value and the unit, making your RDF semantically richer and more compliant with ontologies like SOSA or QUDT.
            """)
            st.markdown("Group columns (e.g. `plasmid_circular`) into Blank Nodes. Requires 'Connecting Predicate' URI.")
            group_active = st.checkbox("Enable column grouping?", key="group_active")
            group_config = {}
            grouping_valid = True
            if group_active:
                try:
                    suggested_groups_dict = suggest_groups(df.columns)
                    if suggested_groups_dict:
                        st.markdown("**Detected Potential Groups:**")
                        for group_key, group_data in suggested_groups_dict.items():
                            display_name = group_data['display']
                            columns = group_data['columns']
                            activate_group = st.checkbox(
                                f"Group columns related to '{display_name}'?",
                                key=f"group_cb_{group_key}",
                                help=f"Columns: {', '.join(columns)}"
                            )
                            if activate_group:
                                conn_pred = st.text_input(
                                    f"Connecting Predicate URI for '{display_name}'",
                                    key=f"group_pred_{group_key}",
                                    placeholder="e.g. http://example.org/hasAnalysisResult",
                                    help="REQUIRED: Full URI."
                                )
                                group_type = st.text_input(
                                    f"RDF Type URI for '{display_name}' Group (Opt.)",
                                    key=f"group_type_{group_key}",
                                    placeholder="e.g. http://example.org/AnalysisResult",
                                    help="Optional: Full `rdf:type` URI."
                                )
                                if not conn_pred or not is_valid_uri_simple(conn_pred.strip()):
                                    st.warning(f"Valid Connecting URI needed for '{display_name}'.")
                                    grouping_valid = False
                                else:
                                    group_type_uri_final = group_type.strip() if group_type and is_valid_uri_simple(group_type.strip()) else None
                                    if group_type and not group_type_uri_final:
                                        st.warning(f"Group Type URI for '{display_name}' invalid, ignoring.")
                                    group_config[display_name] = {
                                        'columns': columns,
                                        'connecting_predicate': conn_pred.strip(),
                                        'group_type': group_type_uri_final
                                    }
                    else:
                        st.info("No obvious groups found based on column names.")
                except Exception as e:
                    st.error(f"Error suggesting groups: {e}")

        st.markdown("---")
        # --- 6. Reference Handling (Optional) ---
        st.subheader("6. Publication Reference Handling (Optional)")
        
        # Check if reference is already loaded to provide visual feedback
        reference_is_loaded = st.session_state.get('reference_data') is not None
        if reference_is_loaded:
            ref_info = st.session_state['reference_data']
            if ref_info['method'] == 'DOI':
                st.success(f"Publication reference loaded for DOI: **{ref_info['doi']}**")
            else:
                title_val = ref_info['metadata'].get('title', ['Unknown'])[0]
                st.success(f"Publication reference loaded: **{title_val}**")

        with st.expander("Add/Update Publication Reference to Dataset", expanded=not reference_is_loaded):
            st.markdown("""
            Add a publication reference that will be linked to your dataset metadata. 
            You can provide a DOI for automatic metadata retrieval or manually enter publication details.
            """)
            
            # Reference input method selection
            # Persist reference_method
            if 'rdf_gen_reference_method' not in st.session_state:
                st.session_state['rdf_gen_reference_method'] = "DOI"
            
            ref_method_idx = 0 if st.session_state['rdf_gen_reference_method'] == "DOI" else 1
            reference_method = st.radio(
                "Choose reference input method:",
                ("DOI", "Manual Entry"),
                index=ref_method_idx,
                key="reference_method_radio",
                help="DOI will automatically fetch metadata from CrossRef and OpenAlex APIs"
            )
            st.session_state['rdf_gen_reference_method'] = reference_method
            
            if reference_method == "DOI":
                # Persist DOI input
                if 'rdf_gen_doi_input' not in st.session_state:
                    st.session_state['rdf_gen_doi_input'] = ""
                    
                doi_input = st.text_input(
                    "DOI",
                    value=st.session_state['rdf_gen_doi_input'],
                    key="doi_input_text",
                    placeholder="e.g., 10.1038/nature12373",
                    help="Enter the DOI without 'doi:' prefix"
                )
                st.session_state['rdf_gen_doi_input'] = doi_input
                
                if doi_input and st.button("Fetch Publication Metadata", key="fetch_doi_btn"):
                    with st.spinner("Fetching publication metadata..."):
                        try:
                            converter = DOIToSemOpenAlexConverter()
                            rdf_graph_ref = converter.convert(doi_input)
                            
                            if rdf_graph_ref:
                                st.session_state['reference_data'] = {
                                    'method': 'DOI',
                                    'doi': doi_input,
                                    'graph': rdf_graph_ref
                                }
                                st.session_state['reference_rdf'] = converter.serialize(format='turtle')
                                st.rerun() # Rerun to show success message and update expander state
                            else:
                                st.error("Failed to fetch metadata for the provided DOI.")
                        except Exception as e:
                            st.error(f"Error fetching DOI metadata: {e}")
                
                # Show persistent preview if data exists
                if reference_is_loaded and st.session_state['reference_data']['method'] == 'DOI':
                    st.markdown("**Current Fetched Publication Data:**")
                    preview_rdf = st.session_state['reference_rdf'][:2000]
                    if len(st.session_state['reference_rdf']) > 2000:
                        preview_rdf += "\n... (truncated)"
                    st.code(preview_rdf, language="turtle")
                            
            else:  # Manual Entry
                st.markdown("**Manual Publication Entry:**")
                col1, col2 = st.columns(2)
                
                with col1:
                    title = st.text_input("Title*", key="manual_title", help="Publication title (required)")
                    authors = st.text_area("Authors*", key="manual_authors", 
                                         help="Authors separated by semicolons (e.g., Smith, J.; Doe, A.)",
                                         placeholder="Smith, John; Doe, Alice")
                    journal = st.text_input("Journal/Publication*", key="manual_journal", 
                                           help="Journal or publication name (required)")
                    
                with col2:
                    year = st.number_input("Year", key="manual_year", min_value=1900, max_value=2030, 
                                         value=2023, help="Publication year")
                    volume = st.text_input("Volume", key="manual_volume", help="Journal volume (optional)")
                    pages = st.text_input("Pages", key="manual_pages", 
                                        help="Page range (e.g., 123-145)", placeholder="123-145")
                    manual_doi = st.text_input("DOI (Optional)", key="manual_doi", 
                                             placeholder="10.1038/nature12373")
                
                if st.button("Generate Reference RDF", key="generate_manual_ref_btn"):
                    if not title or not authors or not journal:
                        st.warning("Title, Authors, and Journal are required fields.")
                    else:
                        try:
                            # Create a manual metadata structure similar to CrossRef format
                            manual_metadata = {
                                'title': [title],
                                'author': [],
                                'container-title': [journal],
                                'published-print': {
                                    'date-parts': [[year, 1, 1]]  # Default to January 1st
                                }
                            }
                            
                            # Parse authors
                            author_list = [author.strip() for author in authors.split(';')]
                            for author in author_list:
                                if ',' in author:
                                    # Assume "Last, First" format
                                    parts = author.split(',', 1)
                                    family = parts[0].strip()
                                    given = parts[1].strip() if len(parts) > 1 else ""
                                else:
                                    # Assume "First Last" format
                                    parts = author.strip().split()
                                    given = ' '.join(parts[:-1]) if len(parts) > 1 else ""
                                    family = parts[-1] if parts else author
                                
                                manual_metadata['author'].append({
                                    'given': given,
                                    'family': family
                                })
                            
                            # Add optional fields
                            if volume:
                                manual_metadata['volume'] = volume
                            if pages:
                                manual_metadata['page'] = pages
                            if manual_doi:
                                manual_metadata['DOI'] = manual_doi
                            
                            # Generate RDF using the reference handler
                            converter = DOIToSemOpenAlexConverter()
                            
                            # Create a work URI for manual entry
                            work_uri = converter.create_work_uri(manual_doi if manual_doi else title)
                            
                            # Add metadata to the graph
                            converter.add_work_metadata(work_uri, manual_metadata)
                            converter.add_authors(work_uri, manual_metadata)
                            converter.add_source(work_uri, manual_metadata)
                            converter.add_open_access(work_uri, manual_metadata)
                            
                            st.session_state['reference_data'] = {
                                'method': 'Manual',
                                'metadata': manual_metadata,
                                'graph': converter.graph
                            }
                            st.session_state['reference_rdf'] = converter.serialize(format='turtle')
                            st.rerun() # Rerun to show success message and update expander state
                            
                        except Exception as e:
                            st.error(f"Error generating reference RDF: {e}")
                            st.exception(e)

                # Show persistent preview if data exists
                if reference_is_loaded and st.session_state['reference_data']['method'] == 'Manual':
                    st.markdown("**Current Manual Publication RDF:**")
                    preview_rdf = st.session_state['reference_rdf'][:2000]
                    if len(st.session_state['reference_rdf']) > 2000:
                        preview_rdf += "\n... (truncated)"
                    st.code(preview_rdf, language="turtle")

        st.markdown("---")
        # --- 7. RDF Configuration Section ---
        st.subheader("7. RDF Generation Configuration")

        
        config = st.session_state['config'] # Use the session state config

        basic_col1, basic_col2 = st.columns(2)

        with basic_col1:
            # (Subject Identification)
            st.markdown("**Subject Identification**")
            
            # Persist id_option
            if 'rdf_gen_id_option' not in st.session_state:
                st.session_state['rdf_gen_id_option'] = "Existing ID Column"
            
            id_option_idx = 0 if st.session_state['rdf_gen_id_option'] == "Existing ID Column" else 1
            id_option = st.radio("Identify subjects via:", ("Existing ID Column", "Generated IDs"),
                                 index=id_option_idx,
                                 key="id_option_radio", horizontal=True,
                                 help="Choose unique row ID column or auto-generate.")
            st.session_state['rdf_gen_id_option'] = id_option

            default_id_column = None
            default_id_valid = False
            id_column_options = [""] + list(df.columns)
            
            # Persist default_id_column
            if 'rdf_gen_default_id_col' not in st.session_state:
                st.session_state['rdf_gen_default_id_col'] = ""
            
            def_id_idx = id_column_options.index(st.session_state['rdf_gen_default_id_col']) if st.session_state['rdf_gen_default_id_col'] in id_column_options else 0
            
            default_id_column_selected = st.selectbox("Select existing ID column (for default method)",
                                                      id_column_options,
                                                      index=def_id_idx,
                                                      key="default_id_col_selectbox",
                                                      disabled=(id_option == "Generated IDs"),
                                                      help="Column with unique values per row, used if cross-file linking is OFF.")
            st.session_state['rdf_gen_default_id_col'] = default_id_column_selected

            if id_option == "Existing ID Column":
                if not default_id_column_selected:
                    st.warning("Select default ID column if using this method.")
                else:
                    default_id_column = default_id_column_selected
                    default_id_valid = True
            else:
                default_id_column = "_generated_id_"
                default_id_valid = True

            st.markdown("---")
            # (Cross-File Linking)
            st.markdown("**Cross-File Linking (Overrides Default ID)**")
            
            # Persist shared_id checkbox
            if 'rdf_gen_use_shared_id' not in st.session_state:
                st.session_state['rdf_gen_use_shared_id'] = False
            
            use_shared_identifier = st.checkbox("Link subjects across files using a shared identifier?",
                                                value=st.session_state['rdf_gen_use_shared_id'],
                                                key="shared_id_checkbox",
                                                help="Enable for consistent subject URIs across multiple tables using a shared key column.")
            st.session_state['rdf_gen_use_shared_id'] = use_shared_identifier

            shared_id_column = None
            subject_base_uri = None
            shared_id_valid = False
            if use_shared_identifier:
                # Persist shared identifier column
                if 'rdf_gen_shared_id_col' not in st.session_state:
                    st.session_state['rdf_gen_shared_id_col'] = ""
                
                shared_id_idx = id_column_options.index(st.session_state['rdf_gen_shared_id_col']) if st.session_state['rdf_gen_shared_id_col'] in id_column_options else 0
                
                shared_id_column = st.selectbox("Select Shared Identifier Column", [""] + list(df.columns),
                                                index=shared_id_idx,
                                                key="shared_id_col_selectbox",
                                                help="The column containing the shared ID.")
                st.session_state['rdf_gen_shared_id_col'] = shared_id_column

                # Persist subject base uri
                if 'rdf_gen_subject_base_uri' not in st.session_state:
                    st.session_state['rdf_gen_subject_base_uri'] = ""

                subject_base_uri_input = st.text_input("Subject Base URI", 
                                                       value=st.session_state['rdf_gen_subject_base_uri'],
                                                       key="shared_id_base_uri_input",
                                                       placeholder="e.g., http://example.org/samples/",
                                                       help="REQUIRED Base URI (e.g., http://myorg.com/entity/). ID is appended.")
                st.session_state['rdf_gen_subject_base_uri'] = subject_base_uri_input

                if not shared_id_column:
                    st.warning("Select the shared identifier column.")
                elif not subject_base_uri_input or not is_valid_uri_simple(subject_base_uri_input.strip()):
                    st.warning("Enter a valid base URI.")
                else:
                    subject_base_uri = subject_base_uri_input.strip()
                    shared_id_valid = True
            else:
                shared_id_valid = True

            st.markdown("---")
            # (Named Graph) - This section defines 'named_graph_uri'
            st.markdown("**Named Graph (Required)**")
            named_graph_uri = None # Initialize as None
            
            # Persist graph_option
            if 'rdf_gen_graph_option' not in st.session_state:
                st.session_state['rdf_gen_graph_option'] = "Use Named Graph"
                
            graph_opt_idx = 0 if st.session_state['rdf_gen_graph_option'] == "Use Named Graph" else 1
            graph_option = st.radio("Choose graph option:", 
                                   ("Use Named Graph", "No Named Graph"), 
                                   index=graph_opt_idx,
                                   key="graph_option_radio",
                                   help="Choose whether to use a named graph or generate RDF without one.")
            st.session_state['rdf_gen_graph_option'] = graph_option
            
            if graph_option == "Use Named Graph":
                # The base URI for the graph is now derived from the default_namespace in the config
                default_base_uri = config.get('default_namespace', "http://example.com/data/") # Fallback for UI
                
                # Persist graph base uri
                if 'rdf_gen_graph_base_uri' not in st.session_state:
                    st.session_state['rdf_gen_graph_base_uri'] = default_base_uri
                
                graph_base = st.text_input("Base URI for graph", 
                                           value=st.session_state['rdf_gen_graph_base_uri'], 
                                           key="ng_base_input")
                st.session_state['rdf_gen_graph_base_uri'] = graph_base
                try:
                    # Use uploaded_file.name which should be available here
                    if uploaded_file:
                         # Use unquote to handle potential URL encoding in filenames
                         safe_filename = unquote(uploaded_file.name)
                         file_stem = os.path.splitext(safe_filename)[0]
                    else:
                         # Fallback if file somehow not available (shouldn't happen in this flow)
                         file_stem = "graph_data"
                except Exception as e:
                    logging.warning(f"Error processing filename for named graph hash: {e}")
                    file_stem = "graph_data" # Fallback

                # Generate hash based on the (decoded) file stem
                hash_id = hashlib.md5(file_stem.encode("utf-8")).hexdigest()
                suggested_graph_uri = f"{graph_base.rstrip('/dataset')}/dataset/da{hash_id}ta"
                named_graph_uri_input = st.text_input("Full Named Graph URI", value=suggested_graph_uri, key="ng_full")
                if is_valid_uri_simple(named_graph_uri_input.strip()):
                    named_graph_uri = named_graph_uri_input.strip() # Assign the valid URI
                else:
                    st.error("Named Graph URI is required and must be valid when using named graph option.")
                    # named_graph_uri remains None if invalid
                if named_graph_uri: # Check if it was successfully assigned
                    st.markdown(f"**Using Graph URI:** `{named_graph_uri}`")
            else:
                # No Named Graph option selected
                st.info("RDF will be generated without a named graph.")
            # named_graph_uri will be None if "No Named Graph" is selected

        with basic_col2:
            # (Mapping Columns & Class URI)
            st.markdown("**Mapping Table Columns**")
            map_cols_options = [""] + list(mapping_df.columns) # Renamed for clarity

            # Determine default indices for pre-filling
            default_term_col_name = "Term"
            default_uri_col_name = "URI"
            default_role_col_name = "RDF Role"
            default_match_type_col_name = "Match Type"

            term_col_idx = map_cols_options.index(default_term_col_name) if default_term_col_name in map_cols_options else 0
            uri_col_idx = map_cols_options.index(default_uri_col_name) if default_uri_col_name in map_cols_options else 0
            role_col_idx = map_cols_options.index(default_role_col_name) if default_role_col_name in map_cols_options else 0
            
            # SKOS match type uses the same options list
            match_type_col_idx = map_cols_options.index(default_match_type_col_name) if default_match_type_col_name in map_cols_options else 0

            string_column = st.selectbox("Term Column", map_cols_options, index=term_col_idx, key="map_term_col",
                                         help="Mapping: Original text.")
            iri_column = st.selectbox("URI Column", map_cols_options, index=uri_col_idx, key="map_uri_col",
                                      help="Mapping: Target URI.")
            
            mapping_columns_valid = True
            if not string_column or not iri_column:
                st.warning("Select 'Term' and 'URI' columns.")
                mapping_columns_valid = False
            
            rdf_role_column = st.selectbox("RDF Role Column", map_cols_options, index=role_col_idx, key="map_role_col",
                                           help="REQUIRED: 'predicate'/'object'.")
            if not rdf_role_column:
                st.warning("Select 'RDF Role' column.")
                mapping_columns_valid = False
            
            # match_type_column_options is the same as map_cols_options here
            match_type_column = st.selectbox("SKOS Match Type Column (Opt.)", map_cols_options, index=match_type_col_idx,
                                             key="map_match_col", help="Optional: SKOS relation.")
            st.markdown("**RDF Type for Subjects (Opt.)**")
            st.info("""
                Specify what **type of entity** each row in your dataset represents. 
                
                **Example:** If your data describes bacterial isolates, you might provide the URI for Campylobacter isolates from an ontology: `http://purl.obolibrary.org/obo/NCBITaxon_197`.
                
                This assigns an `rdf:type` statement to all main subjects generated from your table.
            """)
            
            # Persist class checkbox
            if 'rdf_gen_use_class' not in st.session_state:
                st.session_state['rdf_gen_use_class'] = False
                
            use_instance_class = st.checkbox("Assign rdf:type?", 
                                             value=st.session_state['rdf_gen_use_class'],
                                             key="use_class_checkbox")
            st.session_state['rdf_gen_use_class'] = use_instance_class

            instance_class_uri = None
            if use_instance_class:
                # Persist class URI
                if 'rdf_gen_class_uri' not in st.session_state:
                    st.session_state['rdf_gen_class_uri'] = ""
                    
                instance_class_input = st.text_input("Class URI", 
                                                     value=st.session_state['rdf_gen_class_uri'],
                                                     key="class_uri_input_text",
                                                     placeholder="e.g., http://schema.org/Dataset",
                                                     help="Full URI for subject class.")
                st.session_state['rdf_gen_class_uri'] = instance_class_input

                if instance_class_input and not is_valid_uri_simple(instance_class_input.strip()):
                    st.warning("Class URI invalid.")
                elif instance_class_input:
                    instance_class_uri = instance_class_input.strip()

        st.markdown("---")
        active_template_name_to_apply = None
        if st.session_state.schema_templates:
            template_names = ["None (use default column mapping)"] + [t['template_name'] for t in st.session_state.schema_templates]
            selected_template_display_name = st.selectbox(
                "Apply a Schema Template to Main Subjects?",
                template_names,
                index=0, # Default to "None"
                key="active_schema_template_selector",
                help="If a template is selected, its rules will be used to generate RDF for the primary subjects, overriding some default behaviors."
            )
            if selected_template_display_name != "None (use default column mapping)":
                active_template_name_to_apply = selected_template_display_name
            st.markdown("---") # Visual separator

        rdf_format_display = st.selectbox("Choose RDF output format",
                                          ["Turtle", "N-Quads", "JSON-LD", "RDF/XML", "TriG"],
                                          key="rdf_format_select")
        if graph_option == "Use Named Graph" and named_graph_uri:
             st.info(
                 f"""
                 **Note on Named Graphs & Output Formats:**

                 You have opted to use the named graph: `{named_graph_uri}`.

                 *   **N-Quads (.nq):** This format *will explicitly include* the graph URI (`{named_graph_uri}`) on each line (triple) in the output file. This is often the clearest format for named graphs.
                 *   **JSON-LD (.jsonld):** This format *can represent* named graph structures within its JSON output.
                 *   **Turtle (.ttl) & RDF/XML (.rdf):** These output files will contain *only the data belonging to* your named graph (`{named_graph_uri}`). However, the graph URI itself *is not explicitly stated* within the standard syntax of these file formats. The content is correct for the graph, but the graph's name isn't written in the file.
                 """
             )
        ready_to_generate = (default_id_valid if not use_shared_identifier else shared_id_valid) and mapping_columns_valid and grouping_valid
        if not ready_to_generate:
            st.error("Address config warnings/errors first.")

        if st.button("Generate RDF", key="generate_button", disabled=not ready_to_generate):
            st.session_state['rdf_data'] = None
            st.session_state['skos_data'] = None
            st.session_state['dcat_catalog_data'] = None
            st.session_state['dcat_metadata_data'] = None
            # Determine final ID configuration to pass to backend
            final_id_col_name_for_backend = None
            final_subject_base_uri = None
            final_subject_col_name = None
            df_for_rdf = df.copy()
            if use_shared_identifier:
                final_subject_base_uri = subject_base_uri
                final_subject_col_name = shared_id_column
                final_id_col_name_for_backend = default_id_column if id_option == "Existing ID Column" else "_generated_id_internal_"
            else:
                final_subject_base_uri = None
                final_subject_col_name = None
                if id_option == "Generated IDs":
                    try:
                        # Ensure generate_ids is called only once if needed
                        if "_generated_id_" not in df_for_rdf.columns:
                             df_for_rdf = generate_ids(df_for_rdf, id_column_name="_generated_id_", prefix="id_")
                        final_id_col_name_for_backend = "_generated_id_"
                    except Exception as e:
                        st.error(f"Failed to generate IDs: {e}")
                        st.stop()
                else:
                    final_id_col_name_for_backend = default_id_column

            st.write("Generating RDF...")
            try:
                # --- SKOS Generation ---
                # Ensure a default terms_graph_uri exists in config if not provided
                if 'terms_graph_uri' not in config:
                    base_uri = config.get('default_namespace')
                    if not base_uri:
                        st.error("`default_namespace` must be set in the config for SKOS generation.")
                        st.stop()
                    config['terms_graph_uri'] = f"{base_uri.rstrip('/')}/graph/skos-vocabulary"
                    st.info(f"Using default SKOS graph URI: {config['terms_graph_uri']}")

                skos_graph, term_to_uri_lookup = create_skos_graph_and_lookup_map(
                    mapping_df=mapping_df,
                    config=config,
                    data_graph_uri=named_graph_uri
                )
                st.session_state['skos_graph'] = skos_graph
                st.session_state['skos_data'] = skos_graph.serialize(format="turtle")
                st.info("SKOS vocabulary for mapped terms generated.")

                # --- Main Data Graph Generation ---
                rdf_graph = create_rdf_with_mappings(
                    df=df_for_rdf,
                    mapping_df=mapping_df,
                    id_column=final_id_col_name_for_backend,
                    string_column=string_column,
                    iri_column=iri_column,
                    rdf_role_column=rdf_role_column,
                    instance_class_uri=instance_class_uri,
                    named_graph_uri=named_graph_uri,
                    subject_uri_base=final_subject_base_uri,
                    subject_column=final_subject_col_name,
                    group_config=group_config if group_active else {},
                    schema_templates=st.session_state.get('schema_templates', []),
                    active_template_name=active_template_name_to_apply,
                    config=config,
                    term_to_concept_uri_map=term_to_uri_lookup,
                    input_data_path=uploaded_file.name if uploaded_file else "default_filename",
                    original_column_order=list(df.columns)
                )

                # --- Finalize Session State ---
                st.session_state['rdf_graph'] = rdf_graph
                st.session_state['rdf_data'] = rdf_graph.serialize(format="turtle")
                st.session_state['last_named_graph_uri'] = named_graph_uri
                st.session_state['last_rdf_format_display'] = rdf_format_display
                st.success("RDF Generation Complete!")

            except Exception as e:
                st.error(f"Error during RDF generation or serialization: {e}")
                st.exception(e)
                st.session_state['rdf_data'] = None
                st.session_state['skos_data'] = None
                st.session_state['rdf_preview'] = None
        
        # --- 8. DCAT Metadata Catalog builder ---
        if st.session_state.get('rdf_data'):
            st.markdown("---")
            st.subheader("8. DCAT Metadata Catalog builder")
            display_dcat_builder()

    # --- Download and Preview Section ---
    if st.session_state.get('rdf_data') or st.session_state.get('dcat_catalog_data'):
        st.markdown("---")
        st.subheader("9. Download & Preview Generated RDF")
        
        uploaded_file_name = uploaded_file.name if uploaded_file else "rdf_output"
        file_stem = os.path.splitext(uploaded_file_name)[0]

        # Determine number of columns based on available data
        num_cols = 4
        if st.session_state.get('reference_rdf'):
            num_cols = 5
            
        cols = st.columns(num_cols)

        with cols[0]:
            if st.session_state.get('rdf_data'):
                st.download_button(
                    label="Download Data Graph (Turtle)",
                    data=st.session_state['rdf_data'],
                    file_name=f"{file_stem}_data.ttl",
                    mime="text/turtle"
                )
        with cols[1]:
            if st.session_state.get('skos_data'):
                st.download_button(
                    label="Download SKOS Vocab (Turtle)",
                    data=st.session_state['skos_data'],
                    file_name=f"{file_stem}_skos.ttl",
                    mime="text/turtle"
                )
        with cols[2]:
            if st.session_state.get('dcat_metadata_data'):
                st.download_button(
                    label="Download DCAT Metadata (Turtle)",
                    data=st.session_state['dcat_metadata_data'],
                    file_name=f"{file_stem}_dcat_metadata.ttl",
                    mime="text/turtle"
                )
        with cols[3]:
            if st.session_state.get('dcat_catalog_data'):
                st.download_button(
                    label="Download Full Catalog (TriG)",
                    data=st.session_state['dcat_catalog_data'],
                    file_name=f"{file_stem}_full_catalog.trig",
                    mime="application/trig"
                )
        
        # Add reference download button if reference RDF exists
        if st.session_state.get('reference_rdf') and num_cols == 5:
            with cols[4]:
                st.download_button(
                    label="Download Reference (Turtle)",
                    data=st.session_state['reference_rdf'],
                    file_name=f"{file_stem}_reference.ttl",
                    mime="text/turtle"
                )

        # Preview Expander
        with st.expander("Generated RDF Previews"):
            if st.session_state.get('rdf_data'):
                st.markdown("**Data Graph Preview**")
                st.code(st.session_state['rdf_data'][:5000], language="turtle")
            if st.session_state.get('skos_data'):
                st.markdown("**SKOS Vocabulary Preview**")
                st.code(st.session_state['skos_data'][:5000], language="turtle")
            if st.session_state.get('dcat_metadata_data'):
                st.markdown("**DCAT Metadata Preview**")
                st.code(st.session_state['dcat_metadata_data'][:5000], language="turtle")
            if st.session_state.get('dcat_catalog_data'):
                st.markdown("**Full Catalog Preview (TriG)**")
                st.code(st.session_state['dcat_catalog_data'][:5000], language="trig")
            if st.session_state.get('reference_rdf'):
                st.markdown("**Publication Reference Preview**")
                st.code(st.session_state['reference_rdf'][:5000], language="turtle")

# --- App Execution ---
if __name__ == "__main__":
    main()
