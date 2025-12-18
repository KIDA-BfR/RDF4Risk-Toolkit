# streamlit_app.py (This is your main landing page)

import streamlit as st

# Set the page configuration for the landing page
st.set_page_config(
    page_title="Linked Data Toolkit",
    page_icon="🔗",  # Updated icon
    layout="wide",   # Optional: "wide" or "centered"
    initial_sidebar_state="expanded" # Optional: "auto", "expanded", "collapsed"
)

# --- Page Content ---
st.title("Welcome to the Linked Data Toolkit! 🔗")

st.markdown("""
This application provides a suite of tools designed to assist you in the Linked Data generation workflow. 
From preparing your data and reconciling it against knowledge bases to generating and converting RDF, these tools aim to streamline your tasks.
Navigate through the different tools using the sidebar on the left.

Below is a brief overview of the available tools:
""")

st.header("Available Tools")

# Descriptions for each tool

st.subheader("1. Matching Table Service")
st.markdown("""
This tool helps you generate matching tables from your data sources. 
It's a preparatory step to create structured mappings, often used before reconciliation or RDF generation, by comparing and aligning datasets based on specified criteria.
*Access this tool via the 'Matching Table Generator Page' link in the sidebar.*
""")

st.subheader("2. Reconciliation Service")
st.markdown("""
The Reconciliation tool allows you to reconcile your terms against external vocabularies and knowledge bases (e.g., Wikidata, NCBI). 
This process enriches your data by linking it to authoritative URIs, a crucial step in creating Linked Data.
*Access this tool via the 'Reconciliation Page' link in the sidebar.*
""")

st.subheader("3. RDF Generator Service")
st.markdown("""
Generate RDF (Resource Description Framework) data from your tabular data and mappings. 
This tool is essential for transforming your structured information into a Linked Data format, making it machine-readable and interoperable.
*Access this tool via the 'RDF Generator Page' link in the sidebar.*
""")

st.subheader("4. RDF to Table Service")
st.markdown("""
Convert RDF data (from Turtle, N-Quads, RDF/XML, or JSON-LD files) back into a conventional tabular format. 
This can be useful for analysis, review, or when you need to make RDF data accessible for spreadsheet applications or relational databases.
*Access this tool via the 'RDF to Table Page' link in the sidebar.*
""")


st.markdown("---")
st.info("👈 Select a tool from the sidebar to get started!")
