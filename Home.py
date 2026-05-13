import os

import streamlit as st
import streamlit.components.v1 as components

from style import apply_global_styles


def get_home_component_path() -> str:
    """Return the shared React/Material-UI component build directory."""
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "agentic_reconciliation",
        "components",
        "workflow_config_panel",
        "frontend",
        "build",
    )


st.set_page_config(
    page_title="RDF4Risk Toolkit",
    page_icon="link",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_global_styles(active_step=0)

component_path = get_home_component_path()
if not os.path.exists(os.path.join(component_path, "index.html")):
    st.error(
        "RDF4Risk Home React/Material-UI component build is missing. "
        "Run `npm install && npm run build` in "
        "agentic_reconciliation/components/workflow_config_panel/frontend."
    )
else:
    home_component = components.declare_component("rdf4risk_home_panel", path=component_path)
    home_component(app="home", key="rdf4risk_home_mui_app", default=None)
