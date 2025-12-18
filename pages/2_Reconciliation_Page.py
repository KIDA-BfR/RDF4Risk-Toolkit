import streamlit as st
import sys
import os

st.set_page_config(layout="wide") # Set page config here as it's the main entry point for this page

# --- Debugging Setup ---
if 'page2_debug_log' not in st.session_state:
    st.session_state.page2_debug_log = []
else:
    st.session_state.page2_debug_log.clear()

def log_debug(message):
    st.session_state.page2_debug_log.append(message)

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

# --- Import Application Module and Render Page ---
# Explicitly import the rendering function and call it
try:
    from Reconciliation.reconciliation_ui import render_reconciliation_ui
    log_debug("SUCCESS: Imported 'render_reconciliation_ui' from Reconciliation.reconciliation_ui.")
    
    # The render_reconciliation_ui function handles the entire UI, including the sidebar.
    # No need to duplicate sidebar logic here.
    render_reconciliation_ui()

    if 'page2_import_error_occurred' in st.session_state:
        del st.session_state['page2_import_error_occurred']

except ImportError as import_err:
    log_debug(f"ERROR: ImportError: {import_err}")
    st.error("Failed to import the Reconciliation app page function.")
    st.error(f"ImportError: {import_err}")
    st.session_state['page2_import_error_occurred'] = True

except Exception as general_ex:
    log_debug(f"ERROR: Exception during rendering: {general_ex}")
    st.error("An unexpected error occurred while rendering the Reconciliation page.")
    st.error(f"{general_ex}")
    st.session_state['page2_import_error_occurred'] = True


# --- Display Debug Info if Import Failed ---
if 'page2_import_error_occurred' in st.session_state and st.session_state.page2_import_error_occurred:
    st.subheader("Import Debug Information (Reconciliation Page)")
    for item in st.session_state.page2_debug_log:
        st.text(item)
# No explicit function call is needed here as importing the module runs its code.
# If the import was successful, the Reconciliation app's UI should already be rendered.
