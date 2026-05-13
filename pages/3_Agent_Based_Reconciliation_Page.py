import os
import sys
import importlib

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from style import apply_global_styles

st.set_page_config(layout="wide")
apply_global_styles(active_step=3)

# --- Debugging Setup ---
if 'page3_debug_log' not in st.session_state:
    st.session_state.page3_debug_log = []
else:
    st.session_state.page3_debug_log.clear()


def log_debug(message):
    st.session_state.page3_debug_log.append(message)


log_debug(f"Page Script __file__ (for reference): {__file__}")
log_debug(f"Page Script realpath(__file__) (for reference): {os.path.realpath(__file__)}")

# --- Path Setup ---
try:
    project_directory = os.getcwd()
    log_debug(f"Project Directory (using os.getcwd()): {project_directory}")
    log_debug(f"Initial sys.path: {list(sys.path)}")

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
try:
    module_name = "agentic_reconciliation.agent_reconciliation_ui"
    agent_ui_module = importlib.import_module(module_name)
    agent_ui_module = importlib.reload(agent_ui_module)
    render_agent_reconciliation_ui = getattr(agent_ui_module, "render_agent_reconciliation_ui", None)

    if not callable(render_agent_reconciliation_ui):
        available = ", ".join(sorted(name for name in dir(agent_ui_module) if "reconciliation" in name.lower()))
        raise ImportError(
            "Module loaded but missing callable 'render_agent_reconciliation_ui'. "
            f"Available related attributes: {available or '<none>'}"
        )

    log_debug("SUCCESS: Imported 'render_agent_reconciliation_ui' from agentic_reconciliation.agent_reconciliation_ui.")
    render_agent_reconciliation_ui()

    if 'page3_import_error_occurred' in st.session_state:
        del st.session_state['page3_import_error_occurred']

except ImportError as import_err:
    log_debug(f"ERROR: ImportError: {import_err}")
    st.error("Failed to import the Agent-Based Reconciliation page function.")
    st.error(f"ImportError: {import_err}")
    st.session_state['page3_import_error_occurred'] = True

except Exception as general_ex:
    log_debug(f"ERROR: Exception during rendering: {general_ex}")
    st.error("An unexpected error occurred while rendering the Agent-Based Reconciliation page.")
    st.error(f"{general_ex}")
    st.session_state['page3_import_error_occurred'] = True


if 'page3_import_error_occurred' in st.session_state and st.session_state.page3_import_error_occurred:
    st.subheader("Import Debug Information (Agent-Based Reconciliation Page)")
    for item in st.session_state.page3_debug_log:
        st.text(item)