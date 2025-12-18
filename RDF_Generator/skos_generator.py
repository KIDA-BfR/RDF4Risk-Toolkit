# skos_generator.py
import logging
import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace, ConjunctiveGraph
from rdflib.namespace import SKOS, RDF, RDFS, DCTERMS, VOID, XSD
from .data_preprocessor import safe_value, is_valid_uri, strip_angle_brackets, clean_string_for_uri
from datetime import datetime
from urllib.parse import quote
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_skos_graph_and_lookup_map(
    mapping_df: 'pd.DataFrame',
    config: dict,
    data_graph_uri: str  # Accept the dynamic data graph URI
) -> (ConjunctiveGraph, dict):
    """
    Creates a SKOS graph in a dedicated named graph from a mapping table.
    Also returns a map for linking data values to the correct URI (either an
    exactMatch URI or the internal concept URI).
    """
    g = ConjunctiveGraph()
    # The SKOS terms get their own graph, defined in the config
    terms_graph_uri = URIRef(config['terms_graph_uri'])
    terms_graph = g.get_context(terms_graph_uri)

    # The data graph URI is now passed in dynamically
    data_graph_uri_ref = URIRef(data_graph_uri)

    # Bind namespaces for a cleaner output
    terms_graph.bind("skos", SKOS)
    terms_graph.bind("rdf", RDF)
    terms_graph.bind("rdfs", RDFS)
    terms_graph.bind("dct", DCTERMS)
    terms_graph.bind("void", VOID)

    term_to_uri_lookup = {}
    creation_timestamp = Literal(datetime.now().isoformat(), datatype=XSD.dateTime)
    
    default_namespace = config.get('default_namespace')
    if not default_namespace:
        raise ValueError("`default_namespace` not found in configuration.")

    base_concept_uri = default_namespace.rstrip('/') + "/concepts/"

    # Create a Concept Scheme
    scheme_uri = URIRef(default_namespace + "concept-scheme")
    terms_graph.add((scheme_uri, RDF.type, SKOS.ConceptScheme))
    terms_graph.add((scheme_uri, RDFS.label, Literal("Concept Scheme from Mapping Table")))

    for _, row in mapping_df.iterrows():
        term = safe_value(row.get('Term'))
        if not term:
            continue

        uri = strip_angle_brackets(safe_value(row.get('URI')))

        # Always generate a consistent internal concept URI for the SKOS concept
        clean_term = clean_string_for_uri(term, config.get('uri_character_replacements', {}))
        if not clean_term:
            # Fallback for empty/invalid terms
            clean_term = f"concept_{abs(hash(term))}"
        concept_uri = URIRef(base_concept_uri + clean_term)

        # Default mapping is to the concept URI itself
        term_to_uri_lookup[term] = concept_uri

        # Add core concept information
        terms_graph.add((concept_uri, RDF.type, SKOS.Concept))
        terms_graph.add((concept_uri, SKOS.inScheme, scheme_uri))
        terms_graph.add((concept_uri, SKOS.prefLabel, Literal(term)))
        terms_graph.add((concept_uri, DCTERMS.created, creation_timestamp))
        # Link the concept to the dynamically provided data graph URI
        terms_graph.add((concept_uri, VOID.inDataset, data_graph_uri_ref))

        provider_term = safe_value(row.get('Provider Term'))
        if provider_term:
            terms_graph.add((concept_uri, SKOS.altLabel, Literal(provider_term)))

        # Use the resolved definition if available, otherwise fall back
        resolved_label = safe_value(row.get('label'))
        provider_desc = safe_value(row.get('Provider Description'))
        definition = resolved_label if pd.notna(resolved_label) and resolved_label else provider_desc
        if definition:
            terms_graph.add((concept_uri, SKOS.definition, Literal(definition)))

        source_provider = safe_value(row.get('source')) # Corrected from 'Source Provider'
        if source_provider:
            terms_graph.add((concept_uri, DCTERMS.source, Literal(source_provider)))

        # Add the resolved UI link as rdfs:seeAlso
        ui_link = safe_value(row.get('ui_link'))
        if ui_link and is_valid_uri(ui_link):
            terms_graph.add((concept_uri, RDFS.seeAlso, URIRef(ui_link)))

        # Handle exact and close matches
        match_type = safe_value(row.get('Match Type'))

        if uri and is_valid_uri(uri) and match_type:
            match_uri = URIRef(uri)
            # Per user request, add rdfs:seeAlso for any valid match found.
            terms_graph.add((concept_uri, RDFS.seeAlso, match_uri))
            
            if match_type:
                match_type_lower = match_type.lower()

                if match_type_lower == "skos:exactmatch":
                    terms_graph.add((concept_uri, SKOS.exactMatch, match_uri))
                    # FIXED Problem 1: Keep the internal concept URI in the lookup map
                    # This ensures data graph uses internal URIs, enabling round-trip conversion
                    # The exactMatch relationship already links internal → external concepts

                    # FIXED Problem 2: Don't add labels to external URIs
                    # External ontology terms have their own canonical labels
                    # Our internal concept URI already has the correct label via skos:prefLabel
                elif match_type_lower == "skos:closematch":
                    terms_graph.add((concept_uri, SKOS.closeMatch, match_uri))
                    # FIXED Problem 2: Don't add labels to external URIs

    return g, term_to_uri_lookup
