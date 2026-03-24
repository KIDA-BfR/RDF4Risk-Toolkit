import streamlit as st
import sys
import os

# Add the parent directory to sys.path to allow imports from RDF Generator
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from style import apply_global_styles
apply_global_styles(active_step=3)

try:
    from RDF_Generator import app as rdf_app
except ImportError as e:
    st.error(f"Failed to import RDF Generator application: {e}")
    st.error("Please ensure 'RDF Generator/app.py' exists and is correctly structured.")
    st.stop()

# Call the main function directly for Streamlit page scripts
try:
    rdf_app.main()
except Exception as e:
    st.error(f"An error occurred while running the RDF Generator page: {e}")
    st.exception(e)
