import streamlit as st
import sys
import os

# --- Debugging Setup ---
if 'page1_debug_log' not in st.session_state:
    st.session_state.page1_debug_log = []
else:
    st.session_state.page1_debug_log.clear()

def log_debug(message):
    st.session_state.page1_debug_log.append(message)

log_debug(f"Page Script __file__ (for reference): {__file__}")
log_debug(f"Page Script realpath(__file__) (for reference): {os.path.realpath(__file__)}")

# --- Path Setup ---
try:
    # Use the Current Working Directory as the project root.
    # This assumes `streamlit run streamlit_app.py` is executed from the project root.
    project_directory = os.getcwd()
    log_debug(f"Project Directory (using os.getcwd()): {project_directory}")

    log_debug(f"Initial sys.path: {list(sys.path)}")

    # Add the project directory to sys.path if it's not already there.
    # Prepending ensures it's checked first.
    if project_directory not in sys.path:
        sys.path.insert(0, project_directory)
        log_debug(f"SUCCESS: Added to sys.path: {project_directory}")
        log_debug(f"sys.path after insert: {list(sys.path)}")
    else:
        log_debug(f"INFO: Project directory (from getcwd) already in sys.path: {project_directory}")

except Exception as path_ex:
    log_debug(f"ERROR during path setup: {path_ex}")
    st.error(f"An error occurred during Python path setup: {path_ex}")

# --- Import Application Module ---
try:
    from Matching_Table_Generator.generator import render_matching_table_generator_page
    log_debug("SUCCESS: Imported 'render_matching_table_generator_page'.")

    if 'page1_import_error_occurred' in st.session_state:
        del st.session_state['page1_import_error_occurred']

except ImportError as import_err:
    log_debug(f"ERROR: ImportError: {import_err}")
    st.error(f"Failed to import the Matching Table Generator page function.")
    st.error(f"ImportError: {import_err}")
    st.session_state['page1_import_error_occurred'] = True

except Exception as general_ex:
    log_debug(f"ERROR: General exception during import: {general_ex}")
    st.error(f"An unexpected error occurred during import: {general_ex}")
    st.session_state['page1_import_error_occurred'] = True


# --- Render Page or Debug Info ---
if 'page1_import_error_occurred' in st.session_state and st.session_state.page1_import_error_occurred:
    st.subheader("Import Debug Information")
    for item in st.session_state.page1_debug_log:
        st.text(item)
else:
    if 'render_matching_table_generator_page' in globals() and callable(render_matching_table_generator_page):
        render_matching_table_generator_page()
    elif not ('page1_import_error_occurred' in st.session_state and st.session_state.page1_import_error_occurred):
        st.error("Matching Table Generator function was not available to render the page, though no import error was flagged.")
        st.subheader("Import Debug Information (Fallback)")
        for item in st.session_state.page1_debug_log:
            st.text(item)
