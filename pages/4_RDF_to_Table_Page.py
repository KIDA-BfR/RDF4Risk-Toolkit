import streamlit as st
import sys
import os

# Add the parent directory to sys.path to allow imports from RDF_to_Table
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from style import apply_global_styles
apply_global_styles(active_step=4)

try:
    from RDF_to_Table import tablegenerator as rdf_to_table_app
except ImportError as e:
    st.error(f"Failed to import RDF to Table Generator application: {e}")
    st.error("Please ensure 'RDF_to_Table/tablegenerator.py' exists and is correctly structured.")
    st.stop()

if __name__ == "__main__":
    try:
        # Call the main function from the imported module
        rdf_to_table_app.main()
    except Exception as e:
        st.error(f"An error occurred while running the RDF to Table Generator page: {e}")
        st.exception(e)
