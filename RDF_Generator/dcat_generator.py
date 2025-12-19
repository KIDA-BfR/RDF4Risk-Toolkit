# dcat_generator.py
import logging
import streamlit as st
from rdflib import Graph, Namespace, URIRef, Literal, Dataset
from rdflib.namespace import DCTERMS, DCAT, FOAF, RDF, XSD, SKOS
from datetime import date

# Define necessary namespaces
DCATDE = Namespace("http://dcat-ap.de/def/dcatde/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")

THEMES = {
    "AGRI – Agriculture, fisheries, forestry and food": "http://publications.europa.eu/resource/authority/data-theme/AGRI",
    "ECON – Economy and finance": "http://publications.europa.eu/resource/authority/data-theme/ECON",
    "EDUC – Education, culture and sport": "http://publications.europa.eu/resource/authority/data-theme/EDUC",
    "ENER – Energy": "http://publications.europa.eu/resource/authority/data-theme/ENER",
    "ENVI – Environment": "http://publications.europa.eu/resource/authority/data-theme/ENVI",
    "GOVE – Government and public sector": "http://publications.europa.eu/resource/authority/data-theme/GOVE",
    "HEAL – Health": "http://publications.europa.eu/resource/authority/data-theme/HEAL",
    "INTR – International issues": "http://publications.europa.eu/resource/authority/data-theme/INTR",
    "JUST – Justice, legal system and public safety": "http://publications.europa.eu/resource/authority/data-theme/JUST",
    "REGI – Regions and cities": "http://publications.europa.eu/resource/authority/data-theme/REGI",
    "SOCI – Population and society": "http://publications.europa.eu/resource/authority/data-theme/SOCI",
    "TECH – Science and technology": "http://publications.europa.eu/resource/authority/data-theme/TECH",
    "TRAN – Transport": "http://publications.europa.eu/resource/authority/data-theme/TRAN"
}

LICENSES = {
    "CC BY 4.0": "http://creativecommons.org/licenses/by/4.0/",
    "CC0 1.0": "http://creativecommons.org/publicdomain/zero/1.0/",
    "Data Use Agreement for Official Statistics": "http://dcat-ap.de/def/licenses/official",
    "CC-BY-4.0": "https://creativecommons.org/licenses/by/4.0/"
}

def display_dcat_builder():
    """Displays the Streamlit UI for building the DCAT catalog."""
    st.subheader("DCAT Catalog Metadata")

    # Get the generated RDF data from session state
    rdf_graph = st.session_state.get('rdf_graph')
    skos_graph = st.session_state.get('skos_graph') # Get the SKOS graph
    named_graph_uri = st.session_state.get('last_named_graph_uri')
    rdf_format_display = st.session_state.get('last_rdf_format_display', 'Turtle')
    reference_data = st.session_state.get('reference_data')  # Get publication reference

    if not rdf_graph or not named_graph_uri:
        st.info("Please generate RDF data with a named graph first.")
        return

    with st.form("dcat_form"):
        config = st.session_state.get('config', {})
        default_namespace = config.get('default_namespace')
        if not default_namespace:
            st.error("`default_namespace` not found in configuration. Please configure it in `config.yaml`.")
            return
        
        default_publisher_uri = f"{default_namespace.rstrip('/')}/organization"

        st.text_input("Dataset Title", key="dcat_title", value="My Dataset")
        st.text_area("Dataset Description", key="dcat_description", value="An example dataset.")
        st.selectbox("Access Rights", options=["PUBLIC", "RESTRICTED", "NON_PUBLIC"], key="dcat_access_rights")
        st.text_input("Contact Point (Email or URI)", key="dcat_contact_point", placeholder="e.g. mailto:contact@example.org")
        st.text_input("Publisher Name", key="dcat_publisher_name", value="My Organization")
        st.text_input("Publisher URI", key="dcat_publisher_uri", value=default_publisher_uri)
        st.multiselect("Themes", options=list(THEMES.keys()), key="dcat_themes")
        st.selectbox("License", options=list(LICENSES.keys()), key="dcat_license")
        
        # Publication reference linking
        if reference_data:
            st.markdown("---")
            st.markdown("**Publication Reference**")
            if reference_data['method'] == 'DOI':
                st.info(f"📄 Publication reference found: DOI {reference_data['doi']}")
            else:
                title = reference_data['metadata'].get('title', ['Unknown'])[0]
                st.info(f"📄 Publication reference found: {title}")
            
            link_reference = st.checkbox(
                "Link publication reference with dataset?", 
                value=True, 
                key="link_reference",
                help="This will add dcterms:isReferencedBy relation between the dataset and the publication"
            )
        else:
            link_reference = False

        submitted = st.form_submit_button("Generate DCAT Catalog")
        if submitted:
            # Extract basic metadata
            title = st.session_state.dcat_title
            description = st.session_state.dcat_description
            identifier = ""
            keywords = []
            creator = []

            # If publication reference exists, try to pull metadata from it
            if reference_data:
                if reference_method_val := reference_data.get('method'):
                    if reference_method_val == 'DOI':
                        identifier = reference_data.get('doi', "")
                    
                    # Try to extract title, keywords, and authors from the graph if possible
                    # (Simplified for now, using reference data directly if available)
                    if 'metadata' in reference_data:
                        ref_meta = reference_data['metadata']
                        # Use publication title if dataset title is default? 
                        # Or just stick to what's provided for dataset.
                        
                        # DOI is already set above
                        
                        # Extract Authors
                        if 'author' in ref_meta:
                            for auth in ref_meta['author']:
                                creator.append(f"{auth.get('family', '')}, {auth.get('given', '')}")

            metadata_config = {
                "title": title,
                "description": description,
                "keywords": keywords,
                "identifier": identifier,
                "creator": creator,
                "access_rights": st.session_state.dcat_access_rights,
                "contact_point": st.session_state.dcat_contact_point.strip(),
                "publisher_name": st.session_state.dcat_publisher_name,
                "publisher_uri": st.session_state.dcat_publisher_uri,
                "themes": st.session_state.dcat_themes,
                "license": st.session_state.dcat_license,
                "link_reference": link_reference if reference_data else False,
                "reference_data": reference_data if link_reference else None,
            }

            try:
                # The default namespace is now sourced from the centralized config
                format_map = {
                    "Turtle": "turtle",
                    "N-Quads": "nquads",
                    "JSON-LD": "json-ld",
                    "RDF/XML": "pretty-xml"
                }
                rdflib_format = format_map.get(rdf_format_display, "turtle")

                dcat_catalog_trig = create_dcat_catalog(
                    rdf_graph=rdf_graph,
                    skos_graph=skos_graph,
                    rdf_format=rdflib_format,
                    data_graph_uri_str=named_graph_uri,
                    metadata_config=metadata_config,
                    default_namespace=default_namespace
                )
                st.session_state['dcat_catalog_data'] = dcat_catalog_trig
                st.success("DCAT Catalog generated successfully!")
            except Exception as e:
                st.error(f"Failed to generate DCAT catalog: {e}")
                st.exception(e)

    # The preview is now handled in the main app.py to avoid nesting issues.
    pass


def create_dcat_catalog(
    rdf_graph: Graph,
    skos_graph: Graph,
    rdf_format: str,
    data_graph_uri_str: str,
    metadata_config: dict,
    default_namespace: str
) -> str:
    """
    Generates a DCAT catalog in TriG format, including the SKOS vocabulary.

    Args:
        rdf_graph: The ConjunctiveGraph containing the main data.
        skos_graph: The ConjunctiveGraph containing the SKOS vocabulary.
        rdf_format: The format of the original data (e.g., 'turtle').
        data_graph_uri_str: The URI for the named graph containing the data.
        metadata_config: A dictionary with all the required DCAT metadata.

    Returns:
        A string containing the full catalog in TriG format.
    """
    ds = Dataset()
    ds.bind("dct", DCTERMS)
    ds.bind("dcat", DCAT)
    ds.bind("dcatde", DCATDE)
    ds.bind("foaf", FOAF)
    ds.bind("vcard", VCARD)
    ds.bind("skos", SKOS)

    EX = Namespace(default_namespace)
    ds.bind("ex", EX)

    metadata_graph_uri = EX["graph/dcat-metadata"]
    meta_graph = ds.graph(identifier=metadata_graph_uri)
    data_graph_uri = URIRef(data_graph_uri_str)

    try:
        # 1. Process the main data graph
        source_graph = rdf_graph
        if data_graph_uri_str:
            source_context = source_graph.get_context(URIRef(data_graph_uri_str))
            if source_context:
                target_data_graph = ds.graph(identifier=data_graph_uri)
                for triple in source_context:
                    target_data_graph.add(triple)
            else:
                logging.warning(f"DCAT Gen: Named graph '{data_graph_uri_str}' not found in source graph.")
        else:
            for triple in source_graph.default_context:
                ds.default_context.add(triple)
        for prefix, namespace in source_graph.namespaces():
            ds.bind(prefix, namespace)

        # 2. Merge the SKOS vocabulary graph
        if skos_graph:
            for skos_context in skos_graph.contexts():
                skos_graph_id = skos_context.identifier
                target_skos_graph = ds.graph(identifier=skos_graph_id)
                for triple in skos_context:
                    target_skos_graph.add(triple)
                for prefix, namespace in skos_context.namespace_manager.namespaces():
                    if prefix not in ds.namespace_manager:
                        ds.bind(prefix, namespace)
            logging.info("Merged SKOS vocabulary into the DCAT dataset.")

    except Exception as e:
        logging.error(f"Error processing graphs for DCAT generation: {e}")
        raise

    catalog_uri = EX["catalog"]
    dataset_uri = data_graph_uri+"dataset/main"
    distribution_uri = data_graph_uri+"distribution/main"
    publisher_uri = URIRef(metadata_config["publisher_uri"])

    # Catalog
    meta_graph.add((catalog_uri, RDF.type, DCAT.Catalog))
    meta_graph.add((catalog_uri, DCTERMS.title, Literal("Data Catalog", lang="en")))
    meta_graph.add((catalog_uri, DCTERMS.publisher, publisher_uri))
    meta_graph.add((catalog_uri, DCAT.dataset, dataset_uri))
    meta_graph.add((catalog_uri, DCTERMS.description, Literal("This catalog describes datasets generated by the Lab Data Toolkit.", lang="en")))
    meta_graph.add((catalog_uri, DCTERMS.issued, Literal(date.today().isoformat(), datatype=XSD.date)))

    # Publisher
    meta_graph.add((publisher_uri, RDF.type, FOAF.Organization))
    meta_graph.add((publisher_uri, FOAF.name, Literal(metadata_config["publisher_name"])))

    # Dataset
    meta_graph.add((dataset_uri, RDF.type, DCAT.Dataset))
    meta_graph.add((dataset_uri, DCTERMS.title, Literal(metadata_config["title"], lang="en")))
    meta_graph.add((dataset_uri, DCTERMS.description, Literal(metadata_config["description"], lang="en")))
    meta_graph.add((dataset_uri, DCTERMS.publisher, publisher_uri))
    meta_graph.add((dataset_uri, DCTERMS.issued, Literal(date.today().isoformat(), datatype=XSD.date)))
    meta_graph.add((dataset_uri, DCTERMS.modified, Literal(date.today().isoformat(), datatype=XSD.date)))
    meta_graph.add((dataset_uri, DCTERMS.license, URIRef(LICENSES[metadata_config["license"]])))
    meta_graph.add((dataset_uri, DCAT.distribution, distribution_uri))

    # Keywords
    for kw in metadata_config.get("keywords", []):
        meta_graph.add((dataset_uri, DCAT.keyword, Literal(kw, lang="en")))

    # Identifier
    if metadata_config.get("identifier"):
        meta_graph.add((dataset_uri, DCTERMS.identifier, Literal(metadata_config["identifier"])))

    # Creator
    for author in metadata_config.get("creator", []):
        meta_graph.add((dataset_uri, DCTERMS.creator, Literal(author)))

    # Access Rights
    access_rights_uri = f"http://publications.europa.eu/resource/authority/access-right/{metadata_config['access_rights']}"
    meta_graph.add((dataset_uri, DCTERMS.accessRights, URIRef(access_rights_uri)))

    # Contact Point
    if metadata_config.get("contact_point"):
        contact_val = metadata_config["contact_point"]
        if contact_val.startswith("http") or contact_val.startswith("mailto:"):
            meta_graph.add((dataset_uri, DCAT.contactPoint, URIRef(contact_val)))
        else:
            # Create a simple vCard BNode if it's just text/email without mailto
            contact_bnode = BNode()
            meta_graph.add((contact_bnode, RDF.type, VCARD.Individual))
            meta_graph.add((contact_bnode, VCARD.hasEmail, Literal(contact_val)))
            meta_graph.add((dataset_uri, DCAT.contactPoint, contact_bnode))

    # Themes
    for theme_name in metadata_config.get("themes", []):
        if theme_name in THEMES:
            meta_graph.add((dataset_uri, DCAT.theme, URIRef(THEMES[theme_name])))

    
    # Publication reference linking
    if metadata_config.get("link_reference") and metadata_config.get("reference_data"):
        reference_data = metadata_config["reference_data"]
        reference_graph = reference_data.get("graph")
        
        if reference_graph:
            # Add the reference graph to the dataset
            reference_graph_uri = EX["graph/publication-reference"]
            ref_graph = ds.graph(identifier=reference_graph_uri)
            
            # Copy triples from the reference graph
            for triple in reference_graph:
                ref_graph.add(triple)
            
            # Copy namespaces
            for prefix, namespace in reference_graph.namespaces():
                if prefix not in ds.namespace_manager:
                    ds.bind(prefix, namespace)
            
            # Find the work URI from the reference graph (should be the subject with rdf:type soa:Work)
            work_uri = None
            SOA = Namespace("https://semopenalex.org/ontology/")
            DCT = Namespace("http://purl.org/dc/terms/") 
            for subj, pred, obj in reference_graph:
                if pred == DCT.abstract and obj != "":
                    work_uri = subj
                    break
            
            if work_uri:
                # Link the dataset to the publication using dcterms:isReferencedBy
                meta_graph.add((dataset_uri, DCTERMS.isReferencedBy, work_uri))
                logging.info(f"Linked dataset {dataset_uri} to publication {work_uri}")
            else:
                # Fallback: try to find any subject that looks like a work
                for subj, pred, obj in reference_graph:
                    if "work" in str(subj).lower():
                        meta_graph.add((dataset_uri, DCTERMS.isReferencedBy, subj))
                        logging.info(f"Linked dataset {dataset_uri} to publication {subj} (fallback)")
                        break

    # Distribution
    media_type_map = {"turtle": "text/turtle", "nquads": "application/n-quads", "json-ld": "application/ld+json", "xml": "application/rdf+xml", "pretty-xml": "application/rdf+xml", "trig": "application/trig"}
    distribution_media_type = media_type_map.get(rdf_format, "text/plain")
    
    # Serialize the data graph to calculate its byte size
    try:
        data_graph_content = ds.get_context(data_graph_uri).serialize(format=rdf_format)
        byte_size = len(data_graph_content)
    except Exception:
        # Fallback if serialization for size calculation fails
        byte_size = 0
        logging.warning("Could not determine byte size for DCAT distribution.")

    meta_graph.add((distribution_uri, RDF.type, DCAT.Distribution))
    meta_graph.add((distribution_uri, DCAT.accessURL, URIRef(data_graph_uri_str)))
    meta_graph.add((distribution_uri, DCTERMS.title, Literal(f"Distribution of {metadata_config['title']} in {rdf_format} format", lang="en")))
    meta_graph.add((distribution_uri, DCTERMS.license, URIRef(LICENSES[metadata_config["license"]])))
    meta_graph.add((distribution_uri, DCAT.mediaType, URIRef(f"http://www.iana.org/assignments/media-types/{distribution_media_type}")))
    meta_graph.add((distribution_uri, DCAT.byteSize, Literal(byte_size, datatype=XSD.decimal)))

    # Also save the standalone DCAT metadata to session state for separate download
    dcat_metadata_ttl = meta_graph.serialize(format="turtle")
    st.session_state['dcat_metadata_data'] = dcat_metadata_ttl

    return ds.serialize(format="trig")
