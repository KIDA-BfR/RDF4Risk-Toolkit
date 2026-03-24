# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import hashlib
import os
from dateutil.parser import parse
import re
from io import StringIO, BytesIO
import csv
import logging
from collections import Counter
import Levenshtein

# --- Configuration ---
def render_matching_table_generator_page():
    st.title("Data Preprocessing & Matching Table Creator")
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    st.markdown("""
    This app helps prepare your tabular data for RDF conversion.
    1.  Load your data and adjust the **Start parsing from row number**. The **Data Preview** updates automatically.
    2.  **(Optional) Configure Column Omission:** Select columns whose values should not be extracted into the matching table.
    3.  **(Optional) Preprocess Data:** Configure and **Prepare** one or both transformation types (Splitting, Expansion). Each section includes an expandable explanation.
    4.  **(Optional) Consolidate Nearly Identical Terms:** Use Levenshtein distance to find and consolidate similar terms.
    5.  Click **Generate / Refresh Matching Table**. This first applies *all prepared* transformations (from Step 3) and then extracts terms (considering omissions from Step 2 and consolidations from Step 4).
    6.  Manually edit the downloaded `matching_table.csv` to add URIs and specify Match Types.
    7.  **(Optional)** Download the final `preprocessed_data.csv/xlsx` containing the transformed data for use with an RDF Generator tool.
    """)

    # --- Session State Initialization ---
    default_state = {
        'original_df': None,
        'df_after_transformations': None,
        'matching_df': None,
        'prepared_split_config': {},
        'prepared_expand_config': {},
        'keep_original_setting_split': True,
        'keep_original_setting_expand': True,
        'current_start_row': 1,
        'uploaded_file_info': None,
        'load_error': None,
        'transformations_prepared': False, # Tracks if any rules are currently prepared
        'preprocessing_applied_in_last_run': False, # Tracks if preprocessing was applied in the *last* matching table generation
        'omitted_columns_selection': [], # For user to select columns to omit values from
        'selected_sheet': None, # For Excel files: tracks the selected sheet name
        'available_sheets': [] # For Excel files: list of available sheet names
    }
    for key, value in default_state.items():
        if key not in st.session_state:
            st.session_state[key] = value

    # --- Helper Functions ---

    def detect_delimiter(uploaded_file_obj):
        """Sniffs the delimiter from a sample of the uploaded file."""
        try:
            uploaded_file_obj.seek(0)
            # Read a larger sample for robustness, handle potential decoding errors
            sample_bytes = uploaded_file_obj.read(4096)
            uploaded_file_obj.seek(0) # VERY IMPORTANT: Reset file pointer

            # Try common encodings
            encodings_to_try = ['utf-8', 'latin-1', 'windows-1252']
            sample = None
            detected_encoding = None
            for enc in encodings_to_try:
                try:
                    sample = sample_bytes.decode(enc)
                    detected_encoding = enc
                    logging.info(f"Successfully decoded sample with {enc}.")
                    break
                except UnicodeDecodeError:
                    logging.debug(f"Failed to decode sample with {enc}.")
                    continue

            if sample is None:
                # Fallback if all common decodings fail
                sample = sample_bytes.decode('utf-8', errors='ignore')
                detected_encoding = 'utf-8 (ignored errors)'
                logging.warning("Could not decode sample with common encodings, using utf-8 with error ignoring.")

            # Handle BOM (Byte Order Mark), especially common in UTF-8 from Windows tools
            if sample.startswith('\ufeff'):
                sample = sample.lstrip('\ufeff')
                logging.info("BOM detected and removed.")

            if not sample.strip():
                logging.warning("File sample is empty or whitespace only. Cannot detect delimiter.")
                return ',' # Default for empty file

            sniffer = csv.Sniffer()
            delimiter = ',' # Default fallback delimiter

            try:
                # Sniff on a few lines if possible, provide common delimiters
                lines = sample.splitlines()
                sample_for_sniffing = "\n".join(lines[:5]) # Use up to 5 lines
                if sample_for_sniffing:
                    dialect = sniffer.sniff(sample_for_sniffing, delimiters=[',', ';', '\t', '|'])
                    delimiter = dialect.delimiter
                else:
                    logging.warning("Sample for sniffing is empty after splitting lines.")

            except csv.Error:
                # Fallback: check first few lines manually for common delimiters if sniff fails
                logging.warning("CSV Sniffer failed. Attempting manual detection on the first line.")
                if lines:
                    first_line = lines[0]
                    counts = Counter(first_line)
                    if counts.get(';') > counts.get(',') and counts.get(';') > 0: delimiter = ';'
                    elif counts.get('\t') > counts.get(',') and counts.get('\t') > 0: delimiter = '\t'
                    elif counts.get('|') > counts.get(',') and counts.get('|') > 0: delimiter = '|'
                    # Keep comma as default otherwise
                    logging.info(f"Manual delimiter detection based on counts resulted in: '{delimiter}'")
                else:
                    logging.warning("No lines found for manual delimiter detection.")

            logging.info(f"Detected delimiter: '{delimiter}' (using encoding: {detected_encoding})")
            return delimiter
            # --- End try block ---
        except Exception as e: # Catch-all for other unexpected errors
            logging.error(f"Error in detect_delimiter: {e}", exc_info=True)
            st.warning(f"An unexpected error occurred during delimiter detection: {e}. Defaulting to ','. Check file encoding and structure.")
            return ','

    def is_potentially_numeric(s):
        """Checks if a string could represent a number (int or float), ignoring surrounding whitespace."""
        if s is None: return False
        if not isinstance(s, str): s = str(s)
        s = s.strip()
        if not s: return False
        # Handle thousand separators (comma) and decimal points (dot or comma)
        # More robust check: remove thousand separators first, then replace decimal comma with dot
        s_cleaned = s.replace(',', '') # Basic removal, might fail for Euro style "1,234.56"
        if '.' in s and ',' in s: # Handle cases like "1,234.56" or "1.234,56"
            if s.rfind('.') > s.rfind(','): # Assume dot is decimal sep: 1,234.56
                s_cleaned = s.replace(',', '')
            else: # Assume comma is decimal sep: 1.234,56
                s_cleaned = s.replace('.', '').replace(',', '.')

        try:
            float(s_cleaned)
            return True
        except ValueError:
            return False

    def is_probably_date(s):
        """Checks if a string can likely be parsed as a date/datetime."""
        if s is None: return False
        if not isinstance(s, str): s = str(s)
        s = s.strip()
        # Add more heuristics: avoid very short strings, purely numeric (already covered?), common non-dates
        # Regex avoids simple version numbers like 1.2 or coordinates like 48.1,11.5
        if not s or len(s) < 5 or s.isdigit() or re.match(r'^\d+(\.\d+)+$', s) or re.match(r'^\d+[,.]\d+$', s):
            return False
        # Avoid strings that are likely codes (e.g., all caps, short) - can be adapted
        if re.match(r'^[A-Z0-9_-]{2,6}$', s):
            return False
        try:
            # fuzzy=False is important to avoid incorrect guesses (like "1" becoming a date)
            # dayfirst=True might be needed depending on locale, but can cause ambiguity
            _ = parse(s, fuzzy=False)
            # Additional check: does it look like a reasonable date? Avoid parsing "Version 2023" as year 2023.
            # This heuristic is tricky and might need refinement based on expected data.
            if re.match(r'^[A-Za-z]+\s+\d{4}$', s): # e.g., "Version 2023"
                return False
            return True
        except (ValueError, OverflowError, TypeError):
            # ValueError: includes "Unknown string format"
            # OverflowError: date out of range
            # TypeError: if input is not suitable (e.g., complex object passed accidentally)
            return False

    def split_column(df, column_name, delimiter, new_column_names):
        """Splits a column into multiple new columns based on a delimiter."""
        if not isinstance(df, pd.DataFrame) or column_name not in df.columns:
            raise ValueError(f"Split Error: Invalid DataFrame or column '{column_name}' not found.")
        if not new_column_names:
            raise ValueError("Split Error: New column names list cannot be empty.")

        num_new_cols = len(new_column_names)
        # Check for immediate name collisions before proceeding
        existing_cols = set(df.columns)
        colliding = {name for name in new_column_names if name in existing_cols}
        if colliding:
            raise ValueError(f"Split Error: New column name(s) '{', '.join(colliding)}' already exist(s) in the DataFrame.")

        # Prepare data structure for new columns efficiently
        new_cols_data = {name: [pd.NA] * len(df) for name in new_column_names} # Use pd.NA for missing values

        # Compile regex for splitting with optional surrounding whitespace
        cleaned_delimiter_re = r'\s*' + re.escape(delimiter) + r'\s*'

        # Iterate through the column using .items() for index and value
        for idx, value in df[column_name].items():
            if pd.notna(value) and value != '': # Ensure value is not NA or an empty string
                try:
                    # Convert value to string just before splitting
                    value_str = str(value)
                    # Split, strip whitespace from parts, handle maxsplit
                    parts = [p.strip() for p in re.split(cleaned_delimiter_re, value_str, maxsplit=num_new_cols - 1)]

                    # Assign parts to the corresponding new columns for this row index
                    for i, name in enumerate(new_column_names):
                        if i < len(parts):
                            # Store None/NA for empty strings resulting from split, otherwise the part
                            new_cols_data[name][idx] = parts[i] if parts[i] else pd.NA
                        # If fewer parts than new columns, the rest remain pd.NA (already initialized)
                except Exception as e:
                    logging.warning(f"Split failed for value '{value}' (type: {type(value)}) at index {idx} in column '{column_name}': {e}")
                    # Option: Put original value in the first new column as fallback?
                    # new_cols_data[new_column_names[0]][idx] = str(value) # Uncomment if desired

        # Create new DataFrame from the collected data, ensuring index alignment
        new_cols_df = pd.DataFrame(new_cols_data, index=df.index)

        # Concatenate new columns to the original DataFrame
        df = pd.concat([df, new_cols_df], axis=1)

        # Optional: Attempt type conversion on new columns (can be slow)
        # for name in new_column_names:
        #     try: df[name] = pd.to_numeric(df[name], errors='ignore')
        #     except Exception as e: logging.debug(f"Could not convert column {name} to numeric: {e}")
        #     # Could add date conversion attempt here too if needed

        return df

    
    def suggest_split_names(
        series: pd.Series,
        delimiter: str,
        base: str,
        max_parts_cap: int = 20,
        ) -> list[str]:
            """
            Inspect the column *series* and guess a sensible number of parts
            (up to *max_parts_cap*) using the chosen *delimiter*, then
            return `["<base>_1", "<base>_2", ...]`.
            """
            if series.empty or delimiter == "":
                return []
            # Look at the longest split we will have to fit
            max_parts = (
                series.astype(str)
                .str.split(delimiter, expand=False)
                .str.len()
                .max()
            )
            # Guard against pathological delimiters
            max_parts = min(max_parts, max_parts_cap)
            return [f"{base}_{i+1}" for i in range(max_parts)]


    def expand_codes_to_indicators(df, column_name, delimiter, codes_to_expand, new_col_prefix, true_value="True", false_value=None):
        """Transforms codes in a string column into multiple boolean/indicator columns."""
        if column_name not in df.columns: raise ValueError(f"Expand Error: Column '{column_name}' not found.")
        if not codes_to_expand: raise ValueError("Expand Error: List of codes to expand cannot be empty.")

        # Normalize codes for comparison and check for conflicts in new column names
        codes_set_lower = {str(code).strip().lower() for code in codes_to_expand}
        new_column_details = {} # Map original code to {'new_name': str, 'data': list}
        existing_cols = set(df.columns)
        temp_new_names_generated = set() # Track names generated within this function call

        # Prepare new column names and data lists, checking for conflicts
        for code in codes_to_expand:
            code_str = str(code).strip()
            base_name = f"{new_col_prefix}{code_str}"
            # Sanitize name: replace invalid chars with underscore, remove leading/trailing underscores, collapse multiple underscores
            new_name = re.sub(r'[^\w-]', '_', base_name).strip('_') # Allow word chars, digits, hyphen, underscore
            new_name = re.sub(r'_+', '_', new_name)
            if not new_name:
                raise ValueError(f"Expand Error: Could not generate a valid column name for code '{code}' with prefix '{new_col_prefix}'.")
            # Check for conflicts with existing columns AND other newly generated columns
            if new_name in existing_cols or new_name in temp_new_names_generated:
                raise ValueError(f"Expand Error: Generated column name '{new_name}' (from code '{code}') conflicts with an existing or other new column name.")
            new_column_details[code] = {'new_name': new_name, 'data': [pd.NA] * len(df)} # Initialize with pd.NA
            temp_new_names_generated.add(new_name)

        # Compile regex for splitting with optional surrounding whitespace
        cleaned_delimiter_re = r'\s*' + re.escape(delimiter) + r'\s*'

        # Iterate through the source column
        for index, value in df[column_name].items():
            found_codes_lower_in_row = set()
            if pd.notna(value) and value != '':
                try:
                    # Split the string value by the delimiter
                    parts = [p.strip().lower() for p in re.split(cleaned_delimiter_re, str(value)) if p.strip()]
                    # Keep only those parts that are in our target codes list (case-insensitive)
                    found_codes_lower_in_row.update(p for p in parts if p in codes_set_lower)
                except Exception as e:
                    logging.warning(f"Expand parsing failed for value '{value}' at index {index} in column '{column_name}': {e}")

            # Set values for this row in the new columns' data lists
            for code, details in new_column_details.items():
                code_lower = str(code).strip().lower()
                # Determine the value based on whether the code was found
                is_present = code_lower in found_codes_lower_in_row
                final_value = true_value if is_present else false_value

                # Handle placeholder replacement for "Code itself"
                if final_value == "$CODE$": final_value = code if is_present else false_value # Only replace if present

                # Assign the final value or pd.NA if the value is None
                details['data'][index] = pd.NA if final_value is None else final_value

        # Add new columns to DataFrame using prepared lists efficiently
        added_col_names = []
        df_copy = df.copy() # Work on a copy to potentially avoid SettingWithCopyWarning
        for code, details in new_column_details.items():
            new_col_name = details['new_name']
            # Create Series with the correct index
            new_series = pd.Series(details['data'], index=df_copy.index, name=new_col_name)

            # Optional boolean conversion attempt if values suggest boolean type
            is_bool_like = False
            if str(true_value).lower() == 'true' and false_value is not None and str(false_value).lower() == 'false':
                is_bool_like = True
                try:
                    # Use pandas nullable boolean type
                    new_series = new_series.astype('boolean')
                except Exception as e:
                    logging.debug(f"Could not convert column '{new_col_name}' to nullable boolean: {e}. Keeping original type.")
                    is_bool_like = False # Revert flag if conversion failed

            # Assign the new series to the DataFrame copy
            df_copy[new_col_name] = new_series
            added_col_names.append(new_col_name)

        return df_copy, added_col_names


    def load_data(uploaded_file_obj, start_row_val, sheet_name=None):
        """Loads data from uploaded file, detects format, and updates session state.

        Args:
            uploaded_file_obj: The uploaded file object
            start_row_val: The row number to start parsing from (1-based)
            sheet_name: Optional sheet name for Excel files (defaults to first sheet if None)
        """
        st.session_state['load_error'] = None
        df = None
        file_name = uploaded_file_obj.name
        try:
            uploaded_file_obj.seek(0)
            if file_name.lower().endswith(".csv"):
                delimiter = detect_delimiter(uploaded_file_obj)
                # Use keep_default_na=False and specify common NA values
                # Consider adding 'na_filter=False' if you *never* want pandas to infer NAs
                df = pd.read_csv(
                    uploaded_file_obj,
                    skiprows=start_row_val - 1,
                    skipinitialspace=True,
                    delimiter=delimiter,
                    keep_default_na=False, # Pandas won't guess standard NAs like 'NA'
                    na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'None', 'n/a', 'nan', 'null', '-'], # Explicit list
                    encoding='utf-8', # Assume utf-8 first, detect_delimiter might inform better choice later
                    low_memory=False # Can help with mixed types, but uses more memory
                )
                logging.info(f"Loaded CSV with delimiter '{delimiter}', starting row {start_row_val}")
            elif file_name.lower().endswith((".xlsx", ".xls")):
                # Engine 'openpyxl' is needed for .xlsx
                df = pd.read_excel(
                    uploaded_file_obj,
                    sheet_name=sheet_name if sheet_name else 0,  # Use specified sheet or default to first sheet (index 0)
                    skiprows=start_row_val - 1,
                    keep_default_na=False, # Pandas won't guess standard NAs
                    na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'None', 'n/a', 'nan', 'null', '-'], # Explicit list
                    engine='openpyxl' if file_name.lower().endswith(".xlsx") else None # Auto-detects for .xls
                )
                sheet_info = f", sheet '{sheet_name}'" if sheet_name else " (first sheet)"
                logging.info(f"Loaded Excel file{sheet_info}, starting row {start_row_val}")
            else:
                st.error(f"Unsupported file format: {file_name}. Please upload CSV or Excel.")
                st.session_state['load_error'] = "Unsupported file format."
                return # Stop processing if format is wrong

            # Post-load processing
            if df is not None:
                # Step 1: Sanitize individual column names
                sanitized_columns = []
                for i, col_name in enumerate(df.columns):
                    if pd.isna(col_name):  # Handles float('nan') and potentially None
                        name_str = f"Unnamed_Col_{i}"
                    elif isinstance(col_name, str):
                        name_str = col_name
                    else:
                        name_str = str(col_name)
                    
                    # Replace newlines and carriage returns with underscore, then strip whitespace
                    name_str = name_str.replace('\n', '_').replace('\r', '_').strip()
                    
                    # Ensure name is not empty after stripping, provide a default if it is
                    if not name_str: # If stripping results in an empty string
                        name_str = f"Empty_Col_Name_{i}"
                    sanitized_columns.append(name_str)
                
                current_columns = pd.Index(sanitized_columns)

                # Step 2: Ensure unique column names
                if current_columns.has_duplicates:
                    s = pd.Series(current_columns)
                    # Calculate cumulative counts for each name.
                    # For a series like ['A', 'B', 'A', 'C', 'A', 'B'],
                    # cumcount() for group 'A' will be [0, 1, 2] (at original indices of 'A's),
                    # for 'B' will be [0, 1], etc.
                    counts = s.groupby(s).cumcount() # This Series is indexed like s
                    
                    final_new_column_names = []
                    # Iterate using the length and iloc to access elements by position from s and counts
                    for i_idx in range(len(s)):
                        original_name = s.iloc[i_idx]
                        count_for_this_occurrence = counts.iloc[i_idx]
                        if count_for_this_occurrence > 0: # If it's a subsequent occurrence (count > 0)
                            final_new_column_names.append(f"{original_name}_{count_for_this_occurrence}")
                        else: # If it's the first occurrence (count == 0)
                            final_new_column_names.append(original_name)
                    df.columns = pd.Index(final_new_column_names)
                else:
                    df.columns = current_columns # No duplicates, just assign the sanitized names

                logging.info(f"DataFrame columns after sanitization and deduplication: {list(df.columns)}")

                logging.info(f"Loaded DataFrame shape: {df.shape}")
                if df.empty:
                    logging.warning("Loaded DataFrame is empty.")
                    st.warning("The loaded file appears to be empty or became empty after skipping rows.")

                # Reset states on successful load of non-empty df
                st.session_state['original_df'] = df.copy()
                st.session_state['df_after_transformations'] = None
                st.session_state['matching_df'] = None
                st.session_state['prepared_split_config'] = {}
                st.session_state['prepared_expand_config'] = {}
                st.session_state['transformations_prepared'] = False
                st.session_state['preprocessing_applied_in_last_run'] = False
                st.session_state['omitted_columns_selection'] = []
                st.session_state['current_start_row'] = start_row_val
            else:
                # This case should ideally not happen if read_csv/excel worked but returned None
                st.error("Failed to load data into DataFrame, result was None.")
                st.session_state['load_error'] = "Failed to create DataFrame."


        except Exception as e:
            st.error(f"Error loading file '{file_name}' from row {start_row_val}: {e}")
            logging.error(f"Error loading file: {e}", exc_info=True)
            # Reset all relevant states on error
            st.session_state['original_df'] = None
            st.session_state['df_after_transformations'] = None
            st.session_state['matching_df'] = None
            st.session_state['prepared_split_config'] = {}
            st.session_state['prepared_expand_config'] = {}
            st.session_state['transformations_prepared'] = False
            st.session_state['preprocessing_applied_in_last_run'] = False
            st.session_state['load_error'] = str(e)

    def handle_start_row_change():
        """Callback to reset state when start row changes, forcing reload on next run."""
        new_start_row = st.session_state.start_row_input
        # Compare against the value *currently* in the session state for start_row
        if new_start_row != st.session_state.current_start_row:
            logging.info(f"Start row changed to {new_start_row} via input widget. Clearing data state for reload.")
            # Clear data-dependent states; main loop will trigger load_data if file exists
            st.session_state['original_df'] = None
            st.session_state['df_after_transformations'] = None
            st.session_state['matching_df'] = None
            st.session_state['prepared_split_config'] = {}
            st.session_state['prepared_expand_config'] = {}
            st.session_state['transformations_prepared'] = False
            st.session_state['preprocessing_applied_in_last_run'] = False
            st.session_state['omitted_columns_selection'] = []
            # Clear Levenshtein state
            if 'similar_term_groups' in st.session_state: st.session_state.similar_term_groups = []
            if 'user_choices_for_similar_terms' in st.session_state: st.session_state.user_choices_for_similar_terms = {}
            if 'show_consolidation_review_ui' in st.session_state: st.session_state.show_consolidation_review_ui = False
            if 'consolidations_staged_for_generation' in st.session_state: st.session_state.consolidations_staged_for_generation = False
            # Keep 'current_start_row' as the *old* value to detect change,
            # load_data will update it upon successful load with the new value.
            # The widget ('start_row_input') now holds the new target value.

    


    # --- Main App Flow ---

    # 1. Upload & Load Data Section
    st.subheader("1. Upload & Load Data")
    uploaded_file = st.file_uploader("Upload data table (Excel or CSV)", type=["csv", "xlsx", "xls"], key="data_uploader")

    # Check for new file upload and reset state if detected
    if uploaded_file:
        # Use a more robust way to identify a *new* file instance (name+size is usually sufficient)
        current_file_info = (uploaded_file.name, uploaded_file.size)
        if current_file_info != st.session_state.get('uploaded_file_info'):
            logging.info(f"New file detected: {uploaded_file.name}. Resetting application state.")
            st.session_state['uploaded_file_info'] = current_file_info
            # Reset everything relevant for the new file, including start row to default
            st.session_state['original_df'] = None
            st.session_state['df_after_transformations'] = None
            st.session_state['matching_df'] = None
            st.session_state['prepared_split_config'] = {}
            st.session_state['prepared_expand_config'] = {}
            st.session_state['transformations_prepared'] = False
            st.session_state['preprocessing_applied_in_last_run'] = False
            st.session_state['omitted_columns_selection'] = []
            st.session_state['current_start_row'] = 1 # Reset start row state to default
            # Clear Levenshtein state
            if 'similar_term_groups' in st.session_state: st.session_state.similar_term_groups = []
            if 'user_choices_for_similar_terms' in st.session_state: st.session_state.user_choices_for_similar_terms = {}
            if 'show_consolidation_review_ui' in st.session_state: st.session_state.show_consolidation_review_ui = False
            if 'consolidations_staged_for_generation' in st.session_state: st.session_state.consolidations_staged_for_generation = False
            # Reset Excel sheet state
            st.session_state['selected_sheet'] = None
            st.session_state['available_sheets'] = []
            # Important: Reset the widget's value via session state key *before* rerun
            st.session_state.start_row_input = 1
            st.session_state['load_error'] = None # Clear previous errors
            st.rerun() # Rerun to apply default start row and trigger loading with the new file

    # Sheet selector for Excel files
    if uploaded_file and uploaded_file.name.lower().endswith(('.xlsx', '.xls')):
        # Get available sheets if not already loaded
        if not st.session_state.get('available_sheets'):
            try:
                uploaded_file.seek(0)
                excel_file = pd.ExcelFile(uploaded_file, engine='openpyxl' if uploaded_file.name.lower().endswith('.xlsx') else None)
                st.session_state['available_sheets'] = excel_file.sheet_names
                # Set default to first sheet if not already set
                if st.session_state['selected_sheet'] is None and st.session_state['available_sheets']:
                    st.session_state['selected_sheet'] = st.session_state['available_sheets'][0]
                excel_file.close()
            except Exception as e:
                logging.error(f"Error reading Excel sheets: {e}")
                st.error(f"Could not read Excel file sheets: {e}")

        # Display sheet selector
        if st.session_state.get('available_sheets'):
            selected_sheet = st.selectbox(
                "Select Excel Sheet",
                options=st.session_state['available_sheets'],
                index=st.session_state['available_sheets'].index(st.session_state['selected_sheet']) if st.session_state['selected_sheet'] in st.session_state['available_sheets'] else 0,
                key="sheet_selector",
                help="Choose which sheet to load from the Excel file"
            )
            # Update selected sheet if changed
            if selected_sheet != st.session_state.get('selected_sheet'):
                st.session_state['selected_sheet'] = selected_sheet
                # Clear loaded data to trigger reload with new sheet
                st.session_state['original_df'] = None
                st.session_state['df_after_transformations'] = None
                st.session_state['matching_df'] = None
                st.session_state['prepared_split_config'] = {}
                st.session_state['prepared_expand_config'] = {}
                st.session_state['transformations_prepared'] = False
                st.session_state['preprocessing_applied_in_last_run'] = False
                st.session_state['omitted_columns_selection'] = []
                st.rerun()

    # Start row input - value linked to state, triggers reload via callback
    # Use st.session_state.current_start_row which reflects the *last successfully loaded* row
    start_row_widget_value = st.number_input(
        "Start parsing from row number (1-based)",
        min_value=1,
        value=st.session_state.current_start_row, # Display the row used for the current data
        key="start_row_input", # Link widget state
        help="Header row number. Changing this will reload the data and clear prepared transformations.",
        on_change=handle_start_row_change # Callback handles state clearing
    )

    # Load data if file exists AND (no data is loaded OR start row input differs from loaded data's start row)
    # The on_change callback handles resetting 'original_df' if start_row_input changes.
    # This block focuses on the initial load or reload after file change.
    if uploaded_file and st.session_state.get('original_df') is None and st.session_state.get('load_error') is None:
        # Use the value from the widget (which might have just been reset or changed)
        # For Excel files, pass the selected sheet; for CSV, sheet_name will be None
        sheet_to_load = st.session_state.get('selected_sheet') if uploaded_file.name.lower().endswith(('.xlsx', '.xls')) else None
        load_data(uploaded_file, st.session_state.start_row_input, sheet_to_load)
        # If load_data fails, it sets load_error, preventing reload loops.
        # If it succeeds, original_df is set, preventing reload until change.
        if st.session_state.get('original_df') is None: # Check if loading failed
            st.warning("Data loading failed. Please check the file and the 'Start parsing from row' setting.")
        else:
            # Rerun might be needed if loading happens *after* the rest of the page drew
            # without data. Only rerun if data was *just* loaded successfully.
            # This avoids rerunning if data was already loaded in a previous cycle.
            # Check a flag or compare state if needed, but often Streamlit handles this okay.
            pass # Avoid automatic rerun here unless strictly necessary

    # --- Display Initial Preview ---
    df_initial_preview = st.session_state.get('original_df')
    if df_initial_preview is not None:
        st.markdown("**Data Preview (first 5 rows - as loaded):**")
        st.dataframe(df_initial_preview.head())
        st.caption(f"Loaded {df_initial_preview.shape[0]} rows and {df_initial_preview.shape[1]} columns.")
    elif st.session_state.get('load_error'):
        st.warning(f"Could not display data preview. Error during loading: {st.session_state.load_error}")
    elif uploaded_file:
        # If file uploaded but no preview and no error, might be loading or empty file
        st.info("Processing uploaded file...")
    # else: No file uploaded yet


    # --- Sections depending on loaded data (df_initial must exist) ---
    if df_initial_preview is not None:

        # 2. Configure Column Omission (Optional)
        st.markdown("---") 
        st.subheader("2. Configure Column Omission (Optional)")
        st.markdown("Select columns whose cell values should NOT be extracted as 'object' terms in the matching table. Their headers WILL still be included as 'predicate' terms. This selection applies to column names as they appear in the *original uploaded data*.")

        columns_for_omission_multiselect = list(df_initial_preview.columns) # df_initial_preview is available here

        def sync_omitted_columns_selection():
            # Sync the primary session state variable from the widget's state
            if "omitted_columns_multiselect_key" in st.session_state:
                st.session_state.omitted_columns_selection = st.session_state.omitted_columns_multiselect_key
            # No else needed as omitted_columns_selection is initialized in default_state

        st.multiselect(
            "Select columns to omit values from:",
            options=columns_for_omission_multiselect,
            default=st.session_state.get('omitted_columns_selection', []), 
            key="omitted_columns_multiselect_key",  # Widget's own state key
            on_change=sync_omitted_columns_selection, # Callback to update primary state
            help="Cell data from these columns will be ignored for 'object' terms. Headers will still be included as 'predicate' terms."
        )

        if st.button("Add Numeric Columns to Omission List", key="add_numeric_to_omit_button"):
            if df_initial_preview is not None:
                numeric_cols_to_add = []
                for col_name in df_initial_preview.columns:
                    is_numeric_col = True
                    has_any_values = False
                    col_series = df_initial_preview[col_name].dropna()
                    if col_series.empty:
                        is_numeric_col = False
                    else:
                        for val in col_series:
                            val_str = str(val).strip()
                            if not val_str:
                                continue
                            has_any_values = True
                            if not is_potentially_numeric(val_str) or val_str.lower() in ['true', 'false']:
                                is_numeric_col = False
                                break
                    if is_numeric_col and has_any_values:
                        numeric_cols_to_add.append(col_name)

                if numeric_cols_to_add:
                    # Update session state for omitted_columns_selection
                    current_omitted = set(st.session_state.get('omitted_columns_selection', []))
                    current_omitted.update(numeric_cols_to_add)
                    st.session_state.omitted_columns_selection = sorted(list(current_omitted))
                    logging.info(f"Added numeric columns to omission list: {numeric_cols_to_add}. New list: {st.session_state.omitted_columns_selection}")
                    st.rerun() # Rerun to update the multiselect widget
                else:
                    st.info("No purely numeric columns found to add to the omission list.")
            else:
                st.warning("Please load data first to identify numeric columns.")

        if st.button("Add Date Columns to Omission List", key="add_date_to_omit_button"):
            if df_initial_preview is not None:
                date_cols_to_add = []
                for col_name in df_initial_preview.columns:
                    is_date_col = True
                    has_any_values = False
                    col_series = df_initial_preview[col_name].dropna()
                    if col_series.empty:
                        is_date_col = False
                    else:
                        for val in col_series:
                            val_str = str(val).strip()
                            if not val_str:
                                continue
                            has_any_values = True
                            if not is_probably_date(val_str):
                                is_date_col = False
                                break
                    if is_date_col and has_any_values:
                        date_cols_to_add.append(col_name)

                if date_cols_to_add:
                    # Update session state for omitted_columns_selection
                    current_omitted = set(st.session_state.get('omitted_columns_selection', []))
                    current_omitted.update(date_cols_to_add)
                    st.session_state.omitted_columns_selection = sorted(list(current_omitted))
                    logging.info(f"Added date columns to omission list: {date_cols_to_add}. New list: {st.session_state.omitted_columns_selection}")
                    st.rerun() # Rerun to update the multiselect widget
                else:
                    st.info("No date columns found to add to the omission list.")
            else:
                st.warning("Please load data first to identify date columns.")

        if st.button("Add ID Columns to Omission List", key="add_id_to_omit_button"):
            if df_initial_preview is not None:
                id_cols_to_add = []
                for col_name in df_initial_preview.columns:
                    # Check if column name contains "ID" (case-insensitive)
                    # Look for "ID" as a standalone word or at word boundaries
                    col_name_upper = col_name.upper()
                    if 'ID' in col_name_upper or '_ID' in col_name_upper or 'ID_' in col_name_upper or col_name_upper.startswith('ID') or col_name_upper.endswith('ID'):
                        id_cols_to_add.append(col_name)

                if id_cols_to_add:
                    # Update session state for omitted_columns_selection
                    current_omitted = set(st.session_state.get('omitted_columns_selection', []))
                    current_omitted.update(id_cols_to_add)
                    st.session_state.omitted_columns_selection = sorted(list(current_omitted))
                    logging.info(f"Added ID columns to omission list: {id_cols_to_add}. New list: {st.session_state.omitted_columns_selection}")
                    st.rerun() # Rerun to update the multiselect widget
                else:
                    st.info("No columns with 'ID' in their name found to add to the omission list.")
            else:
                st.warning("Please load data first to identify ID columns.")

        # The next section (Preprocessing) will start with its own "---"

        # 3. Optional Preprocessing Configuration
        st.markdown("---")
        st.subheader("3. Preprocess Data (Optional)")
        st.markdown("""*Goal: These optional steps help reshape your data to be cleaner and semantically clearer for conversion to Linked Data (RDF). Transformations are applied sequentially: Splitting first, then Expansion.*""")

        # --- Section A: Splitting ---
        st.markdown("**A) Split Columns by Position**")
        # Explanation Expander (NOT nested)
        with st.expander("ℹ️ Show Example & Explanation for Splitting Columns"):
            st.markdown("""
            **What does this feature do?**
            It takes a single column containing multiple pieces of information separated by a delimiter (like a comma or semicolon) and splits it into several new, more specific columns.

            **Example:**

            *   **Initial Situation (Problem):**
                You have a table with a column named `Location` that contains both the city and the country, separated by a comma and space.

                | Sample ID | Location          | Value |
                | :-------- | :---------------- | :---- |
                | P001      | Berlin, Germany   | 10    |
                | P002      | Vienna, Austria   | 15    |
                | P003      | Zurich, Switzerland | 12    |

                *   **Why is this problematic for Linked Data?**
                    Semantically, "City" and "Country" are two distinct concepts (properties). In Linked Data, you ideally want to make separate statements like: `P001 <hasCity> "Berlin"` and `P001 <hasCountry> "Germany"`. A single `Location` column with the combined value `"Berlin, Germany"` makes this difficult. It's harder to query for all samples from "Germany" or distinguish the city from the country without complex string operations when converting to RDF.

            *   **Configuration in the Tool:**
                1.  Check "Enable Positional Splitting?".
                2.  Select the `Location` column under "Select column(s) to split:".
                3.  Enter the delimiter used in your data in "`Location`: Delimiter": `, ` (comma followed by a space in this example).
                4.  Enter the desired names for the new columns in "`Location`: New Names (comma-sep.)": `City, Country`.
                5.  (Optional) Decide whether to keep the original `Location` column using the "Keep original..." checkbox.

            *   **Result After Transformation:**
                The tool generates two new columns. If you unchecked "Keep original...", the table looks like this:

                | Sample ID | City   | Country     | Value |
                | :-------- | :----- | :---------- | :---- |
                | P001      | Berlin | Germany     | 10    |
                | P002      | Vienna | Austria     | 15    |
                | P003      | Zurich | Switzerland | 12    |

            *   **Advantage for Linked Data:**
                Each piece of information (City, Country) now has its own column. This allows for a clear mapping to separate RDF predicates (properties like `schema:addressLocality`, `schema:addressCountry`). It becomes much easier to model the data correctly and perform specific SPARQL queries later (e.g., "Show all samples where `schema:addressCountry` is `Germany`").
            """)
        # END Explanation Expander

        split_enabled = st.checkbox("Enable Positional Splitting?", key="split_enable_config", value=bool(st.session_state.prepared_split_config)) # Reflect prepared state
        split_config_ui = {} # Store UI config temporarily for validation this run
        cols_to_split_selected_ui = []
        split_valid = True
        temp_proposed_names_split = set() # Track generated names during UI build for immediate validation

        if split_enabled:
            available_cols_split = list(df_initial_preview.columns)
            # Default selection based on prepared config if available
            default_split_cols = list(st.session_state.prepared_split_config.keys())
            cols_to_split_selected_ui = st.multiselect("Select column(s) to split:", available_cols_split, key="split_select_cols_config", help="Choose columns like 'Host, Type'.", default=default_split_cols)

            if cols_to_split_selected_ui:
                st.markdown("*Configure Split Rules:*")
               # Global toggle so the user can opt in/out
                auto_gen_toggle = st.checkbox(
                    "Auto-generate *New Names* for me",
                    value=True,
                    help="If checked, default names like <col>_1, <col>_2 … "
                        "will be suggested based on a quick scan of your data. "
                        "You can still edit them afterwards.",
                )

                # Grab the dataframe you are previewing

                split_config_ui: dict[str, dict] = {}
                temp_proposed_names_split: set[str] = set()
                split_valid = True  # will be flipped if any row invalid

                for col in cols_to_split_selected_ui:
                    # --- Defaults from earlier steps (if any) ---------------------
                    prepared_col_config = st.session_state.prepared_split_config.get(col, {})
                    default_delimiter = prepared_col_config.get("delimiter", ",")
                    default_names_str = ", ".join(prepared_col_config.get("new_names", []))


                    #  First column: delimiter
                
                    c1, c2 = st.columns([0.35, 0.65])
                    with c1:
                        delimiter_ui = st.text_input(
                            f"`{col}` • Delimiter",
                            key=f"split_delim_{col}",
                            value=default_delimiter,
                        )


                    #  Second column: names (with on-the-fly auto suggestion)
                    
                    with c2:
                        # Compute an auto-suggestion exactly once per rerun,
                        # but only if the user wants it AND hasn’t manually overridden.
                        auto_default_names = (
                            suggest_split_names(df_initial_preview[col], delimiter_ui, col)
                            if auto_gen_toggle
                            else []
                        )
                        # If we already have a value in session_state (e.g. user typed),
                        # honour that; otherwise fall back to auto suggestion or legacy default
                        existing_value = st.session_state.get(f"split_names_{col}", "")
                        start_value = (
                            existing_value
                            if existing_value
                            else (
                                ", ".join(auto_default_names)
                                if auto_default_names
                                else default_names_str
                            )
                        )

                        new_names_str_ui = st.text_input(
                            f"`{col}` • New Names (comma-sep.)",
                            key=f"split_names_{col}",
                            value=start_value,
                            placeholder="e.g., Host, Type  (auto-filled if enabled)",
                        )

                
                    # Validation logic (unchanged except uses current UI values)

                    new_names_ui = [n.strip() for n in new_names_str_ui.split(",") if n.strip()]
                    if not delimiter_ui or not new_names_ui:
                        split_valid = False
                        st.warning(
                            f"Configuration incomplete for column '{col}'. "
                            f"Please provide a delimiter and at least one new name.",
                            icon="⚠️",
                        )
                    else:
                        current_original_cols = set(df_initial_preview.columns)
                        collisions = {
                            n
                            for n in new_names_ui
                            if n in current_original_cols or n in temp_proposed_names_split
                        }
                        if collisions:
                            split_valid = False
                            st.warning(
                                f"Name collision detected for column '{col}': "
                                f"The name(s) {', '.join(collisions)} conflict with existing "
                                f"or other newly proposed columns.",
                                icon="⚠️",
                            )
                        else:
                            split_config_ui[col] = {
                                "delimiter": delimiter_ui,
                                "new_names": new_names_ui,
                            }
                            temp_proposed_names_split.update(new_names_ui)

                # You can now carry on with split_valid and split_config_ui as before


                # Keep original setting - outside the loop, applies to all selected cols
                st.session_state['keep_original_setting_split'] = st.checkbox(
                    "Keep original split column(s)?",
                    value=st.session_state.keep_original_setting_split, # Use value from state
                    key="split_keep_original_config",
                    help="If checked, the original column(s) selected for splitting will be kept alongside the new columns."
                )
            elif split_enabled: # Enabled but no columns selected
                st.info("Select one or more columns to configure splitting.")
                split_valid = False # Cannot prepare without selected columns


        # --- Section B: Expansion ---
        st.markdown("---") # Add visual separator
        st.markdown("**B) Expand Codes to Indicator Columns**")
        # Explanation Expander (NOT nested)
        with st.expander("ℹ️ Show Example & Explanation for Expanding Codes"):
            st.markdown("""
            **What does this feature do?**
            It takes a column where each cell contains a list of codes or keywords (often separated by a delimiter, e.g., `"CIP, TET, NAL"`) and creates a new, separate column for each relevant code you select. These new "indicator" columns show whether the respective code is present for that row, typically using values like `True`/`False`, `1`/`0`, or even the code itself.

            **Example:**

            *   **Initial Situation (Problem):**
                You have a table of bacterial samples with a `Resistance Profile` column indicating which antibiotics (codes) the bacterium is resistant to. Codes are separated by a comma and space. Some samples might be sensitive to all tested antibiotics ("sensitive") or have no data (empty).

                | Sample ID | Bacterium        | Resistance Profile |
                | :-------- | :--------------- | :----------------- |
                | S101      | E. coli          | CIP, NAL, TET      |
                | S102      | K. pneumoniae    | TET                |
                | S103      | E. coli          | sensitive          |
                | S104      | S. aureus        | GEN, ERY           |
                | S105      | K. pneumoniae    |                    |

                *   **Why is this problematic for Linked Data?**
                    The single string value `"CIP, NAL, TET"` in the `Resistance Profile` column is just text (an RDF literal). While it *describes* multiple resistances, it doesn't represent them individually as distinct facts. Semantically, for sample `S101`, we ideally want to make three separate statements (triples) in RDF:
                        *   `Sample:S101 <exhibitsResistanceTo> Antibiotic:CIP .`
                        *   `Sample:S101 <exhibitsResistanceTo> Antibiotic:NAL .`
                        *   `Sample:S101 <exhibitsResistanceTo> Antibiotic:TET .`
                    A single column with the combined string makes this difficult. You can't easily query for all samples resistant to CIP using SPARQL without complex string matching functions. The value "sensitive" or an empty cell implies the *absence* of resistance to the tested antibiotics, which should also be represented more explicitly, often by the absence of a resistance statement or `False` values in indicator columns.

            *   **Configuration in the Tool:**
                1.  Check "Enable Code Expansion?".
                2.  Select the `Resistance Profile` column under "Select column with codes:".
                3.  Enter the delimiter used in that column in "Codes Delimiter": `, ` (comma followed by a space in this example).
                4.  Enter a prefix for the new columns in "New Column Prefix", e.g., `Resistant_`. This helps group the new columns and avoid name conflicts (e.g., `Resistant_CIP`, `Resistant_TET`).
                5.  The tool will attempt to detect all unique codes present in the selected column (e.g., CIP, NAL, TET, sensitive, GEN, ERY). In the "Codes to expand:" multiselect box, choose the codes for which you want indicator columns. You might exclude general terms like "sensitive" if you only want indicators for specific antibiotic resistances.
                6.  Choose the value to represent presence (e.g., `True`, `1`) under "Value if PRESENT:".
                7.  Choose the value to represent absence (e.g., `False`, `0`, `Empty (None/NA)`) under "Value if ABSENT:". Using `True`/`False` is often best for boolean semantics.
                8.  (Optional) Decide whether to keep the original `Resistance Profile` column.

            *   **Result After Transformation:**
                The tool generates a new column for each selected code (e.g., CIP, NAL, TET, GEN, ERY). If "Keep original..." was *unchecked* and `True`/`False` were selected, the table might look like this:

                | Sample ID | Bacterium     | Resistant_CIP | Resistant_NAL | Resistant_TET | Resistant_GEN | Resistant_ERY |
                | :-------- | :------------ | :------------ | :------------ | :------------ | :------------ | :------------ |
                | S101      | E. coli       | True          | True          | True          | False         | False         |
                | S102      | K. pneumoniae | False         | False         | True          | False         | False         |
                | S103      | E. coli       | False         | False         | False         | False         | False         |
                | S104      | S. aureus     | False         | False         | False         | True          | True          |
                | S105      | K. pneumoniae | False         | False         | False         | False         | False         |

                *Note:* Rows originally having "sensitive" or empty values result in `False` in all new indicator columns, correctly reflecting the absence of the specified resistances for that sample.

            *   **Advantage for Linked Data:**
                Each specific resistance characteristic is now represented by its own column (easily mapped to a specific RDF predicate like `vocab:isResistantToCIP`) with a clear `True`/`False` value (boolean literal). This is semantically precise and machine-readable. It allows for simple and efficient SPARQL queries (e.g., "Find all samples where `vocab:isResistantToTET` is `true`"). The conversion into separate, meaningful RDF triples becomes straightforward.
            """)
        # END Explanation Expander

        expand_enabled = st.checkbox("Enable Code Expansion?", key="expand_enable_config", value=bool(st.session_state.prepared_expand_config)) # Reflect prepared state
        expand_config_ui = {} # Store UI config temporarily
        col_to_expand_selected_ui = None
        expand_valid = True

        if expand_enabled:
            available_cols_expand = list(df_initial_preview.columns)
            # Get default from prepared state if available
            default_expand_col = ""
            if st.session_state.prepared_expand_config:
                # Assumes only one expansion config is stored (as per previous logic)
                default_expand_col = list(st.session_state.prepared_expand_config.keys())[0]

            col_to_expand_selected_ui = st.selectbox(
                "Select column containing codes:",
                [""] + available_cols_expand, # Add empty option
                key="expand_select_col",
                help="Choose the single column containing delimited codes (e.g., 'Resistances', 'Features').",
                index=available_cols_expand.index(default_expand_col) + 1 if default_expand_col in available_cols_expand else 0
            )

            if col_to_expand_selected_ui:
                # Get defaults from prepared state if available for the selected column
                prepared_col_config = st.session_state.prepared_expand_config.get(col_to_expand_selected_ui, {})
                default_delimiter_expand = prepared_col_config.get('delimiter', ', ')
                default_prefix = prepared_col_config.get('new_col_prefix', 'Indicator_')
                default_codes_to_expand = prepared_col_config.get('codes_to_expand', []) # Get list of codes
                default_true_val = prepared_col_config.get('true_value', 'True')
                default_false_val = prepared_col_config.get('false_value', 'False') # Note: None needs special handling below

                # Define UI mapping for True/False/Code/$CODE$
                true_val_options = ["True", "1", "Code itself"]
                true_val_map_to_internal = {"True": "True", "1": "1", "Code itself": "$CODE$"}
                true_val_map_from_internal = {v: k for k, v in true_val_map_to_internal.items()}
                default_true_choice = true_val_map_from_internal.get(str(default_true_val), "True") # Default to "True" if not found

                false_val_options = ["False", "0", "Empty (None/NA)"]
                false_val_map_to_internal = {"False": "False", "0": "0", "Empty (None/NA)": None}
                false_val_map_from_internal = {str(v): k for k, v in false_val_map_to_internal.items()} # Convert internal None to 'None' for lookup
                default_false_choice = false_val_map_from_internal.get(str(default_false_val), "False") # Default to "False" if not found

                c1, c2 = st.columns([1,3]);
                with c1:
                    delimiter_expand_ui = st.text_input("Codes Delimiter", value=default_delimiter_expand, key="expand_delim")
                with c2:
                    new_col_prefix_ui = st.text_input("New Column Prefix", value=default_prefix, key="expand_prefix", help="E.g., 'Resistance_' -> 'Resistance_CIP'. Ensures new columns have unique names.")

                if delimiter_expand_ui and new_col_prefix_ui:
                    detected_codes = set();
                    detected_codes_list = []
                    try: # Detect codes from original df based on UI delimiter
                        # Use a sample for efficiency if column is very large
                        sample_size = min(len(df_initial_preview), 5000)
                        unique_vals_sample = df_initial_preview[col_to_expand_selected_ui].dropna().unique()
                        if len(unique_vals_sample) > sample_size: # Heuristic to sample if too many unique values
                            unique_vals_sample = df_initial_preview[col_to_expand_selected_ui].dropna().sample(sample_size).unique()

                        cleaned_delimiter_detect_re = r'\s*' + re.escape(delimiter_expand_ui) + r'\s*'
                        for val in unique_vals_sample:
                            if pd.notna(val) and val != '':
                                parts = [p.strip() for p in re.split(cleaned_delimiter_detect_re, str(val)) if p.strip()]
                                detected_codes.update(parts)
                        # Sort naturally (handles numbers within strings better) if possible, else standard sort
                        try:
                            import natsort
                            detected_codes_list = natsort.natsorted([c for c in detected_codes if c])
                        except ImportError:
                            detected_codes_list = sorted([c for c in detected_codes if c])

                        st.markdown("*Configure Expansion Rules:*")
                        # Default selection is intersection of detected and previously prepared codes, else all detected
                        default_selection = [code for code in detected_codes_list if code in default_codes_to_expand] if default_codes_to_expand else detected_codes_list
                        codes_to_include_ui = st.multiselect(
                            "Codes to expand:",
                            detected_codes_list,
                            default=default_selection,
                            key="expand_codes_select",
                            help="Select the specific codes/values from the column you want to create indicator columns for. Deselect irrelevant items (like 'sensitive' if only specific resistances are needed)."
                        )

                        c3, c4 = st.columns(2);
                        with c3:
                            val_true_ui = st.selectbox("Value if PRESENT:", true_val_options, index=true_val_options.index(default_true_choice), key="expand_val_true")
                        with c4:
                            val_false_ui = st.selectbox("Value if ABSENT:", false_val_options, index=false_val_options.index(default_false_choice), key="expand_val_false")

                        internal_true_value = true_val_map_to_internal[val_true_ui]
                        internal_false_value = false_val_map_to_internal[val_false_ui]

                        if not codes_to_include_ui:
                            expand_valid = False
                            st.warning("Please select at least one code to expand.", icon="⚠️")
                        else:
                            # Collision Check based on UI settings
                            # Consider names generated by the *splitting* section in this run
                            temp_existing_cols_expand = set(df_initial_preview.columns) | temp_proposed_names_split
                            temp_proposed_cols_expand = set()
                            valid_prefix = True
                            for code in codes_to_include_ui:
                                code_str_check = str(code).strip()
                                base_name_check = f"{new_col_prefix_ui}{code_str_check}"
                                new_name_check = re.sub(r'[^\w-]', '_', base_name_check).strip('_')
                                new_name_check = re.sub(r'_+', '_', new_name_check)
                                if not new_name_check:
                                    valid_prefix = False;
                                    st.error(f"Could not generate a valid column name for code '{code}' with prefix '{new_col_prefix_ui}'. Check prefix and code.", icon="🚨"); break
                                if new_name_check in temp_existing_cols_expand or new_name_check in temp_proposed_cols_expand:
                                    expand_valid=False
                                    st.warning(f"Generated column name '{new_name_check}' (for code '{code}') conflicts with an existing column or another proposed name from splitting/expansion.", icon="⚠️")
                                    break # Stop checking on first collision
                                temp_proposed_cols_expand.add(new_name_check)

                            if not valid_prefix: expand_valid=False # Already handled with error message
                            if expand_valid:
                                # Store valid UI config under the selected column name
                                expand_config_ui[col_to_expand_selected_ui] = {
                                    'delimiter': delimiter_expand_ui,
                                    'codes_to_expand': codes_to_include_ui,
                                    'new_col_prefix': new_col_prefix_ui,
                                    'true_value': internal_true_value,
                                    'false_value': internal_false_value
                                }

                    except Exception as e:
                        expand_valid = False
                        st.error(f"Error detecting or processing codes for column '{col_to_expand_selected_ui}': {e}")
                        logging.error(f"Code detection/processing error: {e}", exc_info=True)

                else: # Delimiter or prefix missing
                    expand_valid = False
                    st.warning("Please provide both a Codes Delimiter and a New Column Prefix.", icon="⚠️")

                # Keep original setting
                st.session_state['keep_original_setting_expand'] = st.checkbox(
                    "Keep original codes column?",
                    value=st.session_state.keep_original_setting_expand, # Use value from state
                    key="expand_keep_original",
                    help="If checked, the original column containing the codes will be kept alongside the new indicator columns."
                )

            elif expand_enabled: # Enabled but no column selected
                st.info("Select a column containing codes to configure expansion.")
                expand_valid = False # Cannot prepare without selected column

        # --- Buttons for Preparing/Clearing ---
        st.markdown("---")
        # Determine if prepare button should be disabled
        # Disabled if Split is enabled but invalid/no cols OR if Expand is enabled but invalid/no col
        disable_prepare = (split_enabled and (not split_valid or not cols_to_split_selected_ui)) or \
                        (expand_enabled and (not expand_valid or not col_to_expand_selected_ui))

        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("Prepare Transformations", key="prepare_transform_button", disabled=disable_prepare, use_container_width=True, help="Save the configured transformation rules. Apply them by clicking 'Generate / Refresh Matching Table'."):
                # Store the validated UI configurations into session state
                st.session_state['prepared_split_config'] = split_config_ui if split_enabled else {}
                st.session_state['prepared_expand_config'] = expand_config_ui if expand_enabled else {}

                # Update the 'transformations_prepared' flag
                st.session_state['transformations_prepared'] = bool(st.session_state['prepared_split_config'] or st.session_state['prepared_expand_config'])

                if st.session_state['transformations_prepared']:
                    st.success("Transformation rules prepared successfully! Click 'Generate / Refresh Matching Table' (Section 3) to apply them.")
                    # Log the prepared config
                    logging.info(f"Prepared Split Config: {st.session_state['prepared_split_config']}")
                    logging.info(f"Prepared Expand Config: {st.session_state['prepared_expand_config']}")
                    # Keep original settings are already updated in state via checkboxes
                else:
                    st.info("No transformations were enabled or configured to be prepared.")
                # No automatic rerun here, user proceeds to generate matching table

        with col_btn2:
            # Show clear button only if rules are actually prepared
            if st.session_state.get('transformations_prepared'):
                if st.button("Clear Prepared Transformation Rules", key="clear_prep_rules", use_container_width=True, help="Remove all saved transformation rules. The original data will be used."):
                    st.session_state['prepared_split_config'] = {}
                    st.session_state['prepared_expand_config'] = {}
                    st.session_state['transformations_prepared'] = False
                    st.session_state['df_after_transformations'] = None # Clear potentially stored transformed data
                    st.session_state['preprocessing_applied_in_last_run'] = False
                    # st.session_state['omitted_columns_selection'] = [] # Keep omission selection as it's independent of transformations
                    # Clear Levenshtein state
                    if 'similar_term_groups' in st.session_state: st.session_state.similar_term_groups = []
                    if 'user_choices_for_similar_terms' in st.session_state: st.session_state.user_choices_for_similar_terms = {}
                    if 'show_consolidation_review_ui' in st.session_state: st.session_state.show_consolidation_review_ui = False
                    if 'consolidations_staged_for_generation' in st.session_state: st.session_state.consolidations_staged_for_generation = False
                    # Reset checkbox states related to keeping original columns? Optional, maybe keep user preference.
                    # st.session_state['keep_original_setting_split'] = True
                    # st.session_state['keep_original_setting_expand'] = True
                    st.info("Prepared rules cleared. The next Matching Table generation will use the original data.")
                    logging.info("Cleared prepared transformation rules.")
                    st.rerun() # Rerun to reflect the cleared state immediately (e.g., hide clear button, reset UI defaults)


        # --- 4. Consolidate Nearly Identical Terms (Optional) ---
        st.markdown("---")
        with st.expander("4. Consolidate Nearly Identical Terms (Optional)", expanded=False):
            st.markdown("Use Levenshtein distance to find and consolidate terms that are very similar. Adjust the threshold and apply.")

            levenshtein_threshold = st.slider(
                "Levenshtein Similarity Threshold:",
                min_value=0.0,
                max_value=1.0,
                value=st.session_state.get('levenshtein_threshold', 0.85), # Default to 0.85
                step=0.01,
                key="levenshtein_threshold_slider",
                help="Higher values mean terms must be more similar. 1.0 means identical."
            )
            st.session_state.levenshtein_threshold = levenshtein_threshold # Store in session state

            if 'similar_term_groups' not in st.session_state:
                st.session_state.similar_term_groups = []
            if 'user_choices_for_similar_terms' not in st.session_state:
                st.session_state.user_choices_for_similar_terms = {}
            if 'show_consolidation_review_ui' not in st.session_state: # Initialize new state variable
                st.session_state.show_consolidation_review_ui = False


            col_lev1, col_lev2 = st.columns(2)
            with col_lev1:
                if st.button("Find Similar Terms for Consolidation", key="find_similar_terms_button", use_container_width=True):
                    st.session_state.similar_term_groups = [] # Reset previous findings
                    st.session_state.user_choices_for_similar_terms = {} # Reset choices
                    st.session_state.consolidations_staged_for_generation = False # Reset staging flag
                    st.session_state.show_consolidation_review_ui = True # Show UI after finding

                    # --- Term extraction for consolidation (based on current data + S2 omissions + S3 prepared transforms) ---
                    if df_initial_preview is None:
                        st.warning("Please load data first (Section 1).")
                        st.session_state.show_consolidation_review_ui = False # Don't show UI if no data
                    else:
                        # 1. Start with the original loaded data
                        df_for_consolidation_terms = df_initial_preview.copy()
                        actions_for_consolidation_terms = []

                        # 2. Apply prepared transformations (Splitting/Expansion from Section 3)
                        temp_split_configs_applied = {}
                        if st.session_state.get('prepared_split_config'):
                            try:
                                for col, config in st.session_state['prepared_split_config'].items():
                                    if col in df_for_consolidation_terms.columns:
                                        df_for_consolidation_terms = split_column(df_for_consolidation_terms, col, config['delimiter'], config['new_names'])
                                        temp_split_configs_applied[col] = config # Track applied splits
                                if not st.session_state.keep_original_setting_split:
                                    cols_to_drop_split = set(temp_split_configs_applied.keys()).intersection(set(df_for_consolidation_terms.columns))
                                    if cols_to_drop_split:
                                        df_for_consolidation_terms = df_for_consolidation_terms.drop(columns=list(cols_to_drop_split))
                                actions_for_consolidation_terms.append("Applied prepared splitting rules.")
                            except Exception as e_split:
                                st.error(f"Error applying split rules for term finding: {e_split}")
                                df_for_consolidation_terms = None # Invalidate on error
                                st.session_state.show_consolidation_review_ui = False

                        if df_for_consolidation_terms is not None and st.session_state.get('prepared_expand_config'):
                            try:
                                temp_expand_configs_applied = {}
                                for col, config in st.session_state['prepared_expand_config'].items():
                                    if col in df_for_consolidation_terms.columns:
                                        df_for_consolidation_terms, _ = expand_codes_to_indicators(
                                            df_for_consolidation_terms, col, config['delimiter'], config['codes_to_expand'],
                                            config['new_col_prefix'], config['true_value'], config['false_value']
                                        )
                                        temp_expand_configs_applied[col] = config # Track applied expansions
                                if not st.session_state.keep_original_setting_expand:
                                    cols_to_drop_expand = set(temp_expand_configs_applied.keys()).intersection(set(df_for_consolidation_terms.columns))
                                    if cols_to_drop_expand:
                                        df_for_consolidation_terms = df_for_consolidation_terms.drop(columns=list(cols_to_drop_expand))
                                actions_for_consolidation_terms.append("Applied prepared expansion rules.")
                            except Exception as e_expand:
                                st.error(f"Error applying expansion rules for term finding: {e_expand}")
                                df_for_consolidation_terms = None # Invalidate on error
                                st.session_state.show_consolidation_review_ui = False
                        
                        if actions_for_consolidation_terms:
                            logging.info(f"For consolidation term finding: {'; '.join(actions_for_consolidation_terms)}")

                        # 3. Extract unique object terms from this df_for_consolidation_terms
                        object_terms = []
                        if df_for_consolidation_terms is not None:
                            unique_terms_for_consolidation = set()
                            omitted_cols = st.session_state.get('omitted_columns_selection', [])
                            
                            for col_name_cons in df_for_consolidation_terms.columns:
                                if col_name_cons in omitted_cols:
                                    continue
                                try:
                                    valid_series_cons = df_for_consolidation_terms[col_name_cons].dropna()
                                    for val_cons in valid_series_cons:
                                        try: val_str_cons = str(val_cons)
                                        except Exception: continue
                                        val_clean_cons = val_str_cons.strip()
                                        if val_clean_cons and len(val_clean_cons) < 250 and \
                                           not is_potentially_numeric(val_clean_cons) and \
                                           not is_probably_date(val_clean_cons) and \
                                           val_clean_cons.lower() not in ['true', 'false']:
                                            unique_terms_for_consolidation.add(val_clean_cons)
                                except Exception: # nosec
                                    pass 
                            object_terms = sorted(list(unique_terms_for_consolidation))
                        
                        if not object_terms:
                            st.info("No 'object' terms found from current data configuration to compare.")
                            st.session_state.show_consolidation_review_ui = False # Don't show UI if no terms
                        else:
                            logging.info(f"Found {len(object_terms)} unique 'object' terms for similarity check (based on current data config).")
                            processed_indices = [False] * len(object_terms)
                            found_groups = []
                            for i in range(len(object_terms)):
                                if processed_indices[i]: continue
                                current_term = object_terms[i]
                                current_group = {current_term}
                                processed_indices[i] = True
                                for j in range(i + 1, len(object_terms)):
                                    if processed_indices[j]: continue
                                    other_term = object_terms[j]
                                    ratio = Levenshtein.ratio(current_term.lower(), other_term.lower())
                                    if ratio >= st.session_state.levenshtein_threshold:
                                        current_group.add(other_term)
                                        processed_indices[j] = True
                                if len(current_group) > 1:
                                    found_groups.append(sorted(list(current_group)))
                            
                            st.session_state.similar_term_groups = found_groups
                            if not found_groups:
                                st.info(f"No terms found with similarity >= {st.session_state.levenshtein_threshold:.2f}.")
                                st.session_state.show_consolidation_review_ui = False # Don't show UI if no groups
                            else:
                                temp_choices = {}
                                for idx, grp in enumerate(found_groups):
                                    choice_key = f"choice_group_{idx}"
                                    temp_choices[choice_key] = grp[0] 
                                    new_term_key = f"new_term_group_{idx}"
                                    if new_term_key not in temp_choices:
                                        temp_choices[new_term_key] = "" 
                                st.session_state.user_choices_for_similar_terms = temp_choices
                                st.session_state.show_consolidation_review_ui = True # Ensure UI is shown
                                st.success(f"Found {len(found_groups)} group(s) of similar terms. Please review and choose replacements below.")
                                logging.info(f"Found {len(found_groups)} similar term groups with threshold {st.session_state.levenshtein_threshold:.2f}.")


            with col_lev2:
                if st.session_state.get('similar_term_groups') and st.session_state.get('show_consolidation_review_ui', False): 
                    if st.button("Apply & Stage Consolidations", key="stage_consolidations_button", use_container_width=True, help="Confirm and stage your choices for consolidation. These will be applied when you generate the main matching table."):
                        st.session_state.consolidations_staged_for_generation = True
                        st.session_state.show_consolidation_review_ui = False # Collapse UI after staging
                        st.success(f"{len(st.session_state.similar_term_groups)} consolidation groups with your choices are staged. They will be applied when you click 'Generate / Refresh Matching Table' in Section 5.")
                        logging.info("Consolidation choices staged for main generation.")
                        st.rerun() # Rerun to collapse the UI

            # Display similar term groups and options
            if st.session_state.get('similar_term_groups') and st.session_state.get('show_consolidation_review_ui', False):
                st.markdown("---")
                st.markdown("**Review and Consolidate Similar Terms:**")
                for i, group in enumerate(st.session_state.similar_term_groups):
                    st.markdown(f"**Group {i+1}:** `{'`, `'.join(group)}`")
                    options = group + ["Use New Term", "Keep All (No Change)"]
                    
                    choice_key = f"choice_group_{i}"
                    new_term_key = f"new_term_group_{i}"
                    # Ensure user_choices_for_similar_terms is accessed safely
                    default_choice_val = st.session_state.user_choices_for_similar_terms.get(choice_key, group[0] if group else "")
                    if default_choice_val not in options and group : default_choice_val = group[0]
                    elif not group and default_choice_val not in options: default_choice_val = "Keep All (No Change)"


                    user_selected_term = st.radio(
                        "Choose term to keep or action:", options, index=options.index(default_choice_val) if default_choice_val in options else 0,
                        key=f"radio_group_{i}", horizontal=True,
                    )
                    st.session_state.user_choices_for_similar_terms[choice_key] = user_selected_term

                    custom_new_term_val = st.session_state.user_choices_for_similar_terms.get(new_term_key, "")
                    if user_selected_term == "Use New Term":
                        custom_new_term_input = st.text_input(
                            "Enter new term:", value=custom_new_term_val, key=f"text_new_term_group_{i}",
                            placeholder="e.g., Chardonnay (Corrected)"
                        )
                        st.session_state.user_choices_for_similar_terms[new_term_key] = custom_new_term_input
                    elif new_term_key in st.session_state.user_choices_for_similar_terms:
                        st.session_state.user_choices_for_similar_terms[new_term_key] = ""
                    st.markdown("---")
            elif st.session_state.get('consolidations_staged_for_generation'):
                 st.info("Consolidation choices are staged and will be applied when the matching table is generated.")


        # --- Generate Matching Table ---
        st.markdown("---")
        st.subheader("5. Generate / Refresh Matching Table")

        st.markdown("Click the button below to apply any *prepared* transformations (from Step 3), consider term consolidations (from Step 4 if applied), and generate/regenerate the `matching_table.csv` based on the resulting data (also considering omissions from Step 2).")

        # Button to trigger the process
        if st.button("Generate / Refresh Matching Table", key="generate_match_button", help="Applies prepared transformations, consolidations, considers omissions, and extracts terms."):

            # Determine the DataFrame to use for this run
            # Always start from the original data for each generation run
            df_to_process_for_matching = df_initial_preview.copy()
            preprocessing_error_apply = False
            actions_performed_info = [] # Log actions for user feedback
            generated_col_names_this_run = set() # Track names added in this run
            preprocessing_applied_this_run = False # Track if *any* prepared rule was applied now
            
            split_configs_applied = {} # Initialize here to ensure it's always defined
            expand_configs_applied = {} # Initialize here to ensure it's always defined

            # --- Apply Step 1: Positional Splitting ---
            if st.session_state.get('prepared_split_config'):
                logging.info("Applying prepared positional splits...")
                try:
                    # Make a temporary copy for this step if multiple steps exist
                    temp_df = df_to_process_for_matching.copy()
                    # split_configs_applied is already initialized

                    for col, config in st.session_state['prepared_split_config'].items():
                        if col in temp_df.columns:
                            logging.debug(f"Splitting column '{col}' with delimiter '{config['delimiter']}' into {config['new_names']}")
                            temp_df = split_column(temp_df, col, config['delimiter'], config['new_names'])
                            generated_col_names_this_run.update(config['new_names'])
                            actions_performed_info.append(f"Split '{col}' into '{', '.join(config['new_names'])}'")
                            split_configs_applied[col] = config # Mark as applied
                            preprocessing_applied_this_run = True
                        else:
                            st.warning(f"Configured split column '{col}' was not found in the data during application. Skipping this rule.")
                            logging.warning(f"Configured split column '{col}' not found during application.")

                    # Handle dropping original columns AFTER processing all splits for this step
                    cols_to_drop_split = set()
                    if not st.session_state.keep_original_setting_split:
                        # Drop only those originals for which the split was actually applied
                        cols_to_drop_split = set(split_configs_applied.keys())

                    if cols_to_drop_split:
                        # Check if columns still exist before dropping
                        cols_to_drop_split = cols_to_drop_split.intersection(set(temp_df.columns))
                        if cols_to_drop_split:
                            logging.info(f"Dropping original split columns: {cols_to_drop_split}")
                            temp_df = temp_df.drop(columns=list(cols_to_drop_split))
                            actions_performed_info.append(f"Dropped original split column(s): {', '.join(cols_to_drop_split)}")

                    df_to_process_for_matching = temp_df # Update the main df for the next step or final use
                except Exception as e:
                    st.error(f"Error during positional split application: {e}")
                    logging.error(f"Error during positional split application: {e}", exc_info=True)
                    preprocessing_error_apply = True

            # --- Apply Step 2: Code Expansion (on the result of step 1) ---
            if st.session_state.get('prepared_expand_config') and not preprocessing_error_apply:
                logging.info("Applying prepared code expansion...")
                try:
                    # Make a temporary copy if needed (e.g., if further steps existed)
                    temp_df = df_to_process_for_matching.copy()
                    # expand_configs_applied is already initialized

                    # Note: Current logic assumes only one expansion config exists
                    for col, config in st.session_state['prepared_expand_config'].items():
                        if col in temp_df.columns:
                            logging.debug(f"Expanding column '{col}' with delimiter '{config['delimiter']}' for codes {config['codes_to_expand']}")
                            temp_df, added_cols = expand_codes_to_indicators(
                                temp_df, col, config['delimiter'], config['codes_to_expand'],
                                config['new_col_prefix'], config['true_value'], config['false_value']
                            )
                            generated_col_names_this_run.update(added_cols)
                            actions_performed_info.append(f"Expanded '{col}' into {len(added_cols)} indicator column(s)")
                            expand_configs_applied[col] = config # Mark as applied
                            preprocessing_applied_this_run = True
                        else:
                            st.warning(f"Configured expand column '{col}' was not found in the data during application (perhaps removed by splitting?). Skipping this rule.")
                            logging.warning(f"Configured expand column '{col}' not found during application.")

                    # Handle dropping original column AFTER processing expansion
                    cols_to_drop_expand = set()
                    if not st.session_state.keep_original_setting_expand:
                        # Drop only the original for which expansion was actually applied
                        cols_to_drop_expand = set(expand_configs_applied.keys())

                    if cols_to_drop_expand:
                        # Check if columns still exist before dropping
                        cols_to_drop_expand = cols_to_drop_expand.intersection(set(temp_df.columns))
                        if cols_to_drop_expand:
                            logging.info(f"Dropping original expand column(s): {cols_to_drop_expand}")
                            temp_df = temp_df.drop(columns=list(cols_to_drop_expand))
                            actions_performed_info.append(f"Dropped original expand column(s): {', '.join(cols_to_drop_expand)}")

                    df_to_process_for_matching = temp_df # Update the main df for final use
                except Exception as e:
                    st.error(f"Error during code expansion application: {e}")
                    logging.error(f"Error during code expansion application: {e}", exc_info=True)
                    preprocessing_error_apply = True

            # --- Post-Application Feedback and State Update ---
            st.session_state['preprocessing_applied_in_last_run'] = False # Reset flag for this run
            st.session_state['df_after_transformations'] = None # Reset stored df

            if preprocessing_error_apply:
                st.error("Matching table generation aborted due to errors during preprocessing application.")
                # Keep df_after_transformations as None
            else:
                if actions_performed_info:
                    st.info("Preprocessing Actions Applied: " + "; ".join(actions_performed_info))
                else:
                    st.info("No prepared preprocessing rules were applied.")

                # Store the final processed DF in session state *only if* transformations were applied
                if preprocessing_applied_this_run:
                    st.session_state['df_after_transformations'] = df_to_process_for_matching.copy()
                    st.session_state['preprocessing_applied_in_last_run'] = True # Mark that preprocessing *was* done

                # --- Term Extraction Logic (using df_to_process_for_matching) ---
                temp_matching_df = None # Use a temporary df for processing
                # st.session_state['matching_df'] = None # Don't clear here yet, allow Section 4 to read it if it exists
                try:
                    df_for_matching = df_to_process_for_matching # Use the potentially transformed DF
                    st.markdown("**Preview of Data Used for Matching Table (first 5 rows):**")
                    st.dataframe(df_for_matching.head())

                    # 1. Extract Headers (Potential Predicates)
                    # Include headers unless they are clearly just numbers or dates
                    headers_to_include = []
                    for col in df_for_matching.columns:
                        col_str = str(col)
                        if not is_potentially_numeric(col_str) and not is_probably_date(col_str):
                            headers_to_include.append(col_str)
                        else:
                            logging.debug(f"Excluding potential header '{col_str}' as it looks numeric or date-like.")

                    header_entries = pd.DataFrame({
                        "Term": headers_to_include,
                        "Source Column": headers_to_include, # For predicates, the source is the term itself
                        "URI": "",
                        "RDF Role": "predicate",
                        "Match Type": ""
                    })
                    logging.info(f"Extracted {len(headers_to_include)} potential predicate terms from headers.")

                    # 2. Extract Unique Cell Values (Potential Objects/Literals)
                    unique_terms_with_source = set() # Store tuples of (term, source_column)
                    # Get original columns that were transformed (split or expanded) - based on *prepared* config keys
                    original_split_cols_configured = set(st.session_state.get('prepared_split_config', {}).keys())
                    original_expand_cols_configured = set(st.session_state.get('prepared_expand_config', {}).keys())
                    keep_split_setting = st.session_state.keep_original_setting_split
                    keep_expand_setting = st.session_state.keep_original_setting_expand
                    
                    omitted_cols_user_selection = st.session_state.get('omitted_columns_selection', [])

                    for col in df_for_matching.columns:
                        # 1. Check if user selected this column for omission (applies to its name in df_for_matching)
                        if col in omitted_cols_user_selection:
                            logging.info(f"Skipping object term extraction from user-omitted column '{col}'.")
                            continue

                        # 2. Check if this column is an original one that was kept AND transformed,
                        # and its derived columns exist/are expected.
                        is_original_col_configured_for_split = col in original_split_cols_configured
                        is_original_col_configured_for_expand = col in original_expand_cols_configured
                        
                        skip_original_due_to_transformation = False
                        if is_original_col_configured_for_split and keep_split_setting:
                            # Check if any new columns from *this specific* split rule (for `col`) exist
                            if col in split_configs_applied: # Ensure split was actually applied for this col
                                split_rule_new_names = split_configs_applied.get(col, {}).get('new_names', [])
                                if any(new_name in df_for_matching.columns for new_name in split_rule_new_names):
                                    skip_original_due_to_transformation = True
                        
                        if not skip_original_due_to_transformation and is_original_col_configured_for_expand and keep_expand_setting:
                            # Check if expansion was actually applied for this `col`
                            if col in expand_configs_applied:
                                # If expand was applied for this col and it was kept, assume derivatives exist or were intended.
                                skip_original_due_to_transformation = True
                                
                        if skip_original_due_to_transformation:
                            logging.debug(f"Skipping object term extraction from kept original column '{col}' as it was transformed and its derivatives are present/expected.")
                            continue
                        
                        # Proceed with term extraction for other columns
                        try:
                            # Process non-null values in the column
                            valid_series = df_for_matching[col].dropna()
                            # Convert to string for consistent processing, handle various types
                            for val in valid_series:
                                if pd.isna(val): continue # Should be handled by dropna, but double-check

                                try: val_str = str(val)
                                except Exception: continue # Skip values that fail string conversion

                                val_clean = val_str.strip()

                                # Add term if non-empty, not purely numeric, not date-like, not boolean-like, and reasonable length
                                if val_clean and len(val_clean) < 250: # Increased length limit slightly
                                    if not is_potentially_numeric(val_clean) and \
                                    not is_probably_date(val_clean) and \
                                    val_clean.lower() not in ['true', 'false']:
                                        unique_terms_with_source.add((val_clean, col))

                        except Exception as e:
                            logging.warning(f"Object term extraction error in column '{col}': {e}", exc_info=True)


                    # 3. Combine and Finalize
                    # Filter out terms that are exactly the same as headers (case-insensitive)
                    header_lower_set = {h.lower() for h in headers_to_include}
                    # Filter terms that are also headers, then sort by term, then by source column
                    data_terms_with_source_filtered = sorted(
                        [item for item in unique_terms_with_source if item[0].lower() not in header_lower_set],
                        key=lambda x: (x[0], x[1])
                    )

                    # Unpack the list of tuples into two lists for the DataFrame
                    if data_terms_with_source_filtered:
                        terms, sources = zip(*data_terms_with_source_filtered)
                    else:
                        terms, sources = [], []

                    data_entries = pd.DataFrame({
                        "Term": list(terms),
                        "Source Column": list(sources),
                        "URI": "",
                        "RDF Role": "object", # Default role for cell values
                        "Match Type": ""
                    })
                    logging.info(f"Extracted {len(data_terms_with_source_filtered)} unique potential object terms from cell values.")

                    # Concatenate header and data terms
                    temp_matching_df = pd.concat([header_entries, data_entries], ignore_index=True)
                    
                    # --- Apply Staged Consolidations from Section 4 (if staged) ---
                    consolidations_applied_count = 0
                    # Use a more explicit flag for staged consolidations
                    if st.session_state.get('consolidations_staged_for_generation') and \
                       st.session_state.get('similar_term_groups') and \
                       st.session_state.get('user_choices_for_similar_terms'):
                        
                        logging.info("Applying staged term consolidations during main table generation...")
                        
                        # We need to modify the 'data_terms_with_source_filtered' list before it becomes a DataFrame column
                        # Or, modify the 'Term' column in 'data_entries' DataFrame just after its creation
                        
                        # Let's work with the data_entries DataFrame
                        current_data_entries = data_entries.copy()
                        modified_terms_in_data_entries = 0

                        for group_idx, group_of_similar_terms in enumerate(st.session_state.similar_term_groups):
                            choice_key = f"choice_group_{group_idx}"
                            new_term_key = f"new_term_group_{group_idx}"
                            
                            user_choice_for_group = st.session_state.user_choices_for_similar_terms.get(choice_key)
                            custom_new_term_for_group = st.session_state.user_choices_for_similar_terms.get(new_term_key, "").strip()

                            final_replacement_term = None
                            if user_choice_for_group == "Use New Term" and custom_new_term_for_group:
                                final_replacement_term = custom_new_term_for_group
                            elif user_choice_for_group and user_choice_for_group != "Use New Term" and user_choice_for_group != "Keep All (No Change)":
                                final_replacement_term = user_choice_for_group

                            if final_replacement_term:
                                for old_term_to_replace in group_of_similar_terms:
                                    if old_term_to_replace != final_replacement_term:
                                        # Apply replacement in the 'Term' column of current_data_entries
                                        term_mask = current_data_entries["Term"] == old_term_to_replace
                                        if term_mask.any():
                                            current_data_entries.loc[term_mask, "Term"] = final_replacement_term
                                            modified_terms_in_data_entries += 1
                                            logging.info(f"Consolidated (main gen): Replaced '{old_term_to_replace}' with '{final_replacement_term}' in data_entries.")
                        
                        if modified_terms_in_data_entries > 0:
                            # After all replacements, data_entries might have duplicate rows.
                            # We drop these duplicates, preserving the Term/Source Column pairs.
                            current_data_entries.drop_duplicates(inplace=True)
                            current_data_entries.sort_values(by=["Term", "Source Column"], inplace=True)
                            data_entries = current_data_entries # Assign the modified DF back
                            
                            consolidations_applied_count = modified_terms_in_data_entries # For user message
                            actions_performed_info.append(f"Applied term consolidations based on Section 4 choices.")
                    
                        # Clear staged consolidations as they have been applied
                        st.session_state.similar_term_groups = []
                        st.session_state.user_choices_for_similar_terms = {}
                        st.session_state.consolidations_staged_for_generation = False # Reset flag
                        logging.info("Cleared staged term consolidation choices after application in main generation.")

                    # Concatenate header and (potentially consolidated and re-uniqued) data terms
                    temp_matching_df = pd.concat([header_entries, data_entries], ignore_index=True)
                    st.session_state['matching_df'] = temp_matching_df # Store final df for display and download
                    
                    if consolidations_applied_count > 0:
                         st.success(f"Matching table generated/refreshed successfully, including {consolidations_applied_count} term consolidations!")
                    else:
                        st.success("Matching table generated/refreshed successfully!")

                    # --- Store data for sharing with other apps ---
                    st.session_state['shared_matching_table'] = temp_matching_df.copy() if temp_matching_df is not None else None
                    
                    df_for_sharing = None
                    if st.session_state.get('preprocessing_applied_in_last_run') and st.session_state.get('df_after_transformations') is not None:
                        df_for_sharing = st.session_state['df_after_transformations']
                        logging.info("Sharing 'df_after_transformations' as preprocessed data.")
                    elif st.session_state.get('original_df') is not None:
                        df_for_sharing = st.session_state['original_df']
                        logging.info("Sharing 'original_df' as preprocessed data (no transformations applied or stored).")
                    else:
                        logging.warning("Neither transformed nor original df available for sharing.")

                    st.session_state['shared_preprocessed_data'] = df_for_sharing.copy() if df_for_sharing is not None else None
                    
                    if st.session_state['shared_matching_table'] is not None and st.session_state['shared_preprocessed_data'] is not None:
                        logging.info("Successfully stored 'shared_matching_table' and 'shared_preprocessed_data' in session state.")
                    else:
                        logging.warning("Failed to store one or both shared DataFrames in session state.")
                    # --- End storing data for sharing ---

                except Exception as e:
                    st.error(f"Error generating matching table: {e}")
                    logging.error(f"Error generating matching table: {e}", exc_info=True)
                    st.session_state['matching_df'] = None # Ensure it's None on error
                    # Also clear any potentially half-processed consolidation states if error occurs before they are cleared
                    # st.session_state.similar_term_groups = [] # Already cleared if successfully applied
                    # st.session_state.user_choices_for_similar_terms = {}


        # 6. Review and Download Matching Table
        # This section now displays the result generated by the button click above
        if st.session_state.get('matching_df') is not None:
            # This section is now inside an expander (Section 4)
            # The old content of 4.5 is moved into the expander above.
            # The "Review and Download Matching Table" section follows this.

            st.markdown("---"); st.subheader("6. Review and Download Matching Table"); st.info("Manually edit 'URI' and 'Match Type' columns in the downloaded CSV file. Use URIs from standard vocabularies (like schema.org, SIO, OBO) or create your own.")
            st.dataframe(st.session_state['matching_df'], use_container_width=True, height=300) # Add height limit for large tables
            try:
                # Ensure consistent encoding, use utf-8-sig for better Excel compatibility
                output_csv = st.session_state['matching_df'].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label="Download Matching Table (CSV)",
                    data=output_csv,
                    file_name="matching_table.csv",
                    mime="text/csv",
                    key="download_match_csv"
                )
            except Exception as e:
                st.error(f"Error preparing matching table download: {e}")
                logging.error(f"Error preparing matching table download: {e}", exc_info=True)


        # 7. Download Preprocessed Data (Optional)
        # Show download button ONLY if preprocessing was actually applied successfully in the *last run*
        if st.session_state.get('preprocessing_applied_in_last_run') and st.session_state.get('df_after_transformations') is not None:
            st.markdown("---"); st.subheader("7. Download Preprocessed Data (Optional)"); st.info("Download the data table including the applied transformations. This can be used as input for an RDF generation tool.", icon="⬇️")
            try:
                df_to_download = st.session_state['df_after_transformations'] # Use the stored result
                # Determine original file extension for output format preference
                original_file_ext = ".csv" # Default
                if uploaded_file:
                    original_file_ext = os.path.splitext(uploaded_file.name)[1].lower()

                output_filename_base = "data_preprocessed"
                if uploaded_file:
                    output_filename_base = os.path.splitext(uploaded_file.name)[0] + "_preprocessed"

                # Offer download in both CSV and Excel formats if possible
                # CSV Download
                try:
                    output_csv_data = df_to_download.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label=f"Download Preprocessed Data as CSV",
                        data=output_csv_data,
                        file_name=f"{output_filename_base}.csv",
                        mime="text/csv",
                        key="download_preprocessed_csv"
                    )
                except Exception as e_csv:
                    st.warning(f"Could not prepare CSV download: {e_csv}")
                    logging.warning(f"Error preparing preprocessed CSV download: {e_csv}", exc_info=True)

                # Excel Download
                try:
                    output_buffer = BytesIO()
                    with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
                        df_to_download.to_excel(writer, index=False, sheet_name='PreprocessedData')
                    # writer.save() # Not needed with context manager
                    output_excel_data = output_buffer.getvalue()
                    st.download_button(
                        label=f"Download Preprocessed Data as Excel (XLSX)",
                        data=output_excel_data,
                        file_name=f"{output_filename_base}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_preprocessed_xlsx"
                    )
                except Exception as e_xlsx:
                    st.warning(f"Could not prepare Excel download: {e_xlsx}")
                    logging.warning(f"Error preparing preprocessed Excel download: {e_xlsx}", exc_info=True)

            except Exception as e:
                st.error(f"Error preparing preprocessed data download options: {e}")
                logging.error(f"Error preparing preprocessed data download buttons: {e}", exc_info=True)
        elif st.session_state.get('transformations_prepared'):
            # If rules are prepared but haven't been applied via the Generate button yet
            st.markdown("---"); st.subheader("7. Download Preprocessed Data (Optional)");
            st.info("Click 'Generate / Refresh Matching Table' first to apply prepared transformations. The download button for preprocessed data will appear here afterwards.")
