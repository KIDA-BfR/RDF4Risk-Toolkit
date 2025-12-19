# trig_viewer.py - TriG Data Viewer Streamlit App

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import os
from collections import defaultdict
import sys

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

# --- Main App ---

def main():
    """Main function to run the TriG Data Viewer app."""
    st.title("TriG Data Viewer")
    st.markdown("View and export TriG/RDF data with linked metadata")

    # Sidebar info
    with st.sidebar:
        st.markdown("### About TriG Viewer")
        st.info("""
    This app processes TriG (RDF) files and provides:
    - **Interactive data preview** with clickable links
    - **DCAT metadata** and publication references
    - **Excel export** with HYPERLINK formulas (bypasses 65k limit)
    - **CSV and Markdown** exports

    **Supported Features:**
    - Multi-valued property expansion
    - External resource linking (skos:exactMatch/closeMatch)
    - Property catalogs with usage statistics
        """)

    # Initialize session state for persistence
    if 'trig_converter' not in st.session_state:
        st.session_state.trig_converter = None
    if 'trig_file_name' not in st.session_state:
        st.session_state.trig_file_name = None

    # File uploader and direct loading
    col_u1, col_u2 = st.columns([2, 1])
    with col_u1:
        uploaded_file = st.file_uploader(
            "Upload TriG file",
            type=['trig'],
            help="Upload a TriG (RDF) file to view and export"
        )
    
    with col_u2:
        st.write("") # Spacer
        st.write("") # Spacer
        catalog_data = st.session_state.get('dcat_catalog_data')
        load_from_session = False
        if catalog_data:
            if st.button("Load Catalog from Generator", use_container_width=True):
                load_from_session = True
        else:
            st.info("No catalog found in session. Generate one in the RDF Generator first.")

    # Determine if we have a new file or use existing state
    process_new_file = False
    if uploaded_file:
        if st.session_state.trig_file_name != uploaded_file.name:
            process_new_file = True
    elif load_from_session:
        process_new_file = True
    
    # Logic to process file or use cached state
    converter = None
    
    if process_new_file:
        try:
            with st.spinner("Processing TriG data..."):
                if uploaded_file:
                    # Save to temp file
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.trig') as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name

                    # Initialize converter from file
                    new_converter = TriGConverter(input_file=Path(tmp_path))
                    file_name = uploaded_file.name
                else:
                    # Initialize converter from session state data
                    new_converter = TriGConverter(data=catalog_data)
                    file_name = "generated_catalog.trig"
                    tmp_path = None

                # Parse with progress
                progress = st.progress(0, text="Parsing TriG...")
                if not new_converter.parse_trig():
                    st.error("Failed to parse TriG file. Please check the file format.")
                    if tmp_path:
                        os.unlink(tmp_path)
                    st.stop()

                progress.progress(50, text="Extracting data...")
                new_converter.extract_all_data()

                progress.progress(80, text="Expanding multi-valued properties...")
                new_converter.expand_list_values()

                progress.progress(100, text="Complete!")
                progress.empty()

                # Clean up temp file if one was created
                if tmp_path:
                    os.unlink(tmp_path)
                
                # Update session state
                st.session_state.trig_converter = new_converter
                st.session_state.trig_file_name = file_name
                converter = new_converter
        except Exception as e:
            st.error(f"Error processing file: {e}")
            with st.expander("Error Details"):
                st.code(str(e))
                import traceback
                st.code(traceback.format_exc())
            st.stop()
            
    elif st.session_state.trig_converter:
        converter = st.session_state.trig_converter
        if not uploaded_file:
            st.info(f"Using previously loaded file: **{st.session_state.trig_file_name}**")

    # Display content if converter is available
    if converter:
        try:
            # Create tabs for different views
            tab1, tab2, tab3, tab4 = st.tabs([
                "Data Preview",
                "Metadata",
                "Statistics",
                "Downloads"
            ])

            # --- Tab 1: Data Preview ---
            with tab1:
                st.subheader("Data Preview")

                if converter.subjects_data:
                    # Create preview DataFrame with markdown links
                    df_preview = create_preview_dataframe(converter)

                    # Display with Streamlit (markdown links are clickable!)
                    st.dataframe(
                        df_preview,
                        use_container_width=True,
                        height=600
                    )

                    st.caption(f"Showing {len(df_preview):,} rows × {len(df_preview.columns)} columns")
                    st.info("Click on any blue link to open the resource in a new tab")
                else:
                    st.warning("No subject data found in the TriG file")

            # --- Tab 2: Metadata ---
            with tab2:
                st.subheader("Metadata & References")

                # DCAT Metadata section
                dcat_graph = 'https://fskx-graphdb.risk-ai-cloud.com/graph/dcat-metadata'
                if dcat_graph in converter.named_graphs_data:
                    st.markdown("### DCAT Metadata")
                    dcat_data = converter.named_graphs_data[dcat_graph]
                    display_named_graph(dcat_data, converter)
                else:
                    st.info("No DCAT metadata found in this file")

                st.markdown("---")

                # Publication References section
                pub_graph = 'https://fskx-graphdb.risk-ai-cloud.com/graph/publication-reference'
                if pub_graph in converter.named_graphs_data:
                    st.markdown("### Publication References")
                    pub_data = converter.named_graphs_data[pub_graph]
                    display_named_graph(pub_data, converter)
                else:
                    st.info("No publication references found in this file")

                # Show all other named graphs if any
                other_graphs = [
                    g for g in converter.named_graphs_data.keys()
                    if g not in [dcat_graph, pub_graph]
                ]

                if other_graphs:
                    st.markdown("---")
                    st.markdown("### Other Named Graphs")
                    for graph_uri in other_graphs:
                        with st.expander(f"Graph: {graph_uri}"):
                            display_named_graph(converter.named_graphs_data[graph_uri], converter)

            # --- Tab 3: Statistics ---
            with tab3:
                st.subheader("Dataset Statistics")

                # Metrics in columns
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Triples", f"{len(converter.graph):,}")

                with col2:
                    st.metric("Subjects", f"{len(converter.subjects_data):,}")

                with col3:
                    st.metric("Properties", len(converter.property_labels))

                with col4:
                    matches = len(converter.uri_to_exact_match) + len(converter.uri_to_close_match)
                    st.metric("External Matches", matches)

                st.markdown("---")

                # Property mappings table
                st.markdown("### Property Catalog")

                if converter.property_labels:
                    property_counts = get_property_counts(converter)

                    props_df = pd.DataFrame([
                        {
                            "Property URI": f"[{uri}]({uri})",
                            "Label": label,
                            "Usage Count": property_counts.get(label, 0),
                            "External Match": "Yes" if (uri in converter.uri_to_exact_match or
                                                     uri in converter.uri_to_close_match) else "No"
                        }
                        for uri, label in sorted(
                            converter.property_labels.items(),
                            key=lambda x: property_counts.get(x[1], 0),
                            reverse=True
                        )
                    ])

                    st.dataframe(props_df, use_container_width=True, height=400)
                else:
                    st.info("No properties found")

                # URI mappings statistics
                if converter.uri_to_exact_match or converter.uri_to_close_match:
                    st.markdown("---")
                    st.markdown("### URI Mappings")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Exact Matches (skos:exactMatch)", len(converter.uri_to_exact_match))
                    with col2:
                        st.metric("Close Matches (skos:closeMatch)", len(converter.uri_to_close_match))

            # --- Tab 4: Downloads ---
            with tab4:
                st.subheader("Download Options")

                st.markdown("### Export Formats")
                st.markdown("Choose your preferred format to download the processed data:")

                col1, col2, col3 = st.columns(3)
                
                # Determine filename base
                file_name_base = st.session_state.trig_file_name if st.session_state.trig_file_name else "data.trig"

                with col1:
                    st.markdown("#### Excel")
                    st.markdown("Excel workbook with HYPERLINK formulas")

                    # Generate Excel file
                    try:
                        excel_path = Path(tempfile.gettempdir()) / f"{file_name_base.replace('.trig', '')}_output.xlsx"
                        converter.export_to_excel(excel_path)

                        with open(excel_path, 'rb') as f:
                            excel_data = f.read()

                        st.download_button(
                            "Download Excel",
                            data=excel_data,
                            file_name=f"{file_name_base.replace('.trig', '')}_output.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                        st.caption("Includes Property Mappings sheet. HYPERLINK formulas (no 65k limit)")

                        # Clean up
                        os.unlink(excel_path)
                    except Exception as e:
                        st.error(f"Failed to generate Excel: {e}")

                with col2:
                    st.markdown("#### CSV")
                    st.markdown("Comma-separated values format")

                    # Generate CSV
                    try:
                        df_output = pd.DataFrame(converter.subjects_data)
                        csv_data = df_output.to_csv(index=False).encode('utf-8')

                        st.download_button(
                            "Download CSV",
                            data=csv_data,
                            file_name=f"{file_name_base.replace('.trig', '')}_output.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        st.caption("Simple CSV format. Compatible with any tool")
                    except Exception as e:
                        st.error(f"Failed to generate CSV: {e}")

                with col3:
                    st.markdown("#### Markdown")
                    st.markdown("Metadata documentation")

                    # Generate Markdown
                    try:
                        md_path = Path(tempfile.gettempdir()) / f"{file_name_base.replace('.trig', '')}_metadata.md"
                        converter.export_to_markdown(md_path)

                        with open(md_path, 'rb') as f:
                            md_data = f.read()

                        st.download_button(
                            "Download Markdown",
                            data=md_data,
                            file_name=f"{file_name_base.replace('.trig', '')}_metadata.md",
                            mime="text/markdown",
                            use_container_width=True
                        )
                        st.caption("Metadata documentation. Human-readable format")

                        # Clean up
                        os.unlink(md_path)
                    except Exception as e:
                        st.error(f"Failed to generate Markdown: {e}")

                st.markdown("---")
                st.info("Tip: Excel format includes clickable HYPERLINK formulas that bypass Excel's native 65,536 hyperlink limit.")

        except Exception as e:
            st.error(f"Error displaying data: {e}")
            with st.expander("Error Details"):
                st.code(str(e))
                import traceback
                st.code(traceback.format_exc())

    else:
        # Show welcome message when no file is uploaded and no state
        st.info("Upload a TriG file to get started")

        st.markdown("---")
        st.markdown("### What is TriG?")
        st.markdown("""
        TriG is an RDF serialization format that extends Turtle to support named graphs.
        This viewer helps you explore and export TriG data in various formats.

        **Key Features:**
        - View data in a tabular format with clickable links
        - Access DCAT metadata and publication references
        - Export to Excel (with HYPERLINK formulas), CSV, or Markdown
        - Automatic external resource linking via skos:exactMatch and skos:closeMatch
        """)

if __name__ == "__main__":
    main()
