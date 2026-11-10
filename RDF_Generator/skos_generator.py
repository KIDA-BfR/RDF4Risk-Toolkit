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


def _normalize_term(value) -> str:
    """Same normalization rdf_processor uses to match terms against the mapping table."""
    return str(value).strip().lower()


def collect_used_terms(df: 'pd.DataFrame', exclude_columns=()) -> set:
    """Return the normalized terms a data table can reference: its column headers and cell values.

    ``exclude_columns`` should hold the subject ID / cross-file linking columns, which
    rdf_processor never maps to concepts.
    """
    excluded = {col for col in exclude_columns if col}
    columns = [col for col in df.columns if col not in excluded]
    used = {_normalize_term(col) for col in columns}
    for col in columns:
        for value in df[col].dropna().unique():
            value_str = safe_value(value)
            if value_str:
                used.add(value_str.lower())
    return used


def create_skos_graph_and_lookup_map(
    mapping_df: 'pd.DataFrame',
    config: dict,
    data_graph_uri: str,  # Accept the dynamic data graph URI
    used_terms: set = None,
) -> (ConjunctiveGraph, dict):
    """
    Creates a SKOS graph in a dedicated named graph from a mapping table.
    Also returns a map for linking data values to the correct URI (either an
    exactMatch URI or the internal concept URI).

    ``used_terms`` (see ``collect_used_terms``) restricts the vocabulary to the terms the
    dataset actually uses. A mapping table shared across datasets otherwise puts every
    dataset's terms into this dataset's SKOS graph. ``None`` keeps all rows.
    """
    g = ConjunctiveGraph()
    # The SKOS terms get their own graph, defined in the config
    terms_graph_uri = URIRef(config['terms_graph_uri'])
    terms_graph = g.get_context(terms_graph_uri)

    # The data graph URI is passed in dynamically; it is None when the user chose no named graph.
    data_graph_uri_ref = URIRef(data_graph_uri) if data_graph_uri else None

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

    skipped_unused = 0
    for _, row in mapping_df.iterrows():
        term = safe_value(row.get('subject_label'))
        if not term:
            term = safe_value(row.get('Term'))
        if not term:
            continue
        if used_terms is not None and _normalize_term(term) not in used_terms:
            skipped_unused += 1
            continue

        uri = strip_angle_brackets(safe_value(row.get('object_id')))
        if not uri:
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
        if data_graph_uri_ref is not None:
            terms_graph.add((concept_uri, VOID.inDataset, data_graph_uri_ref))

        provider_term = safe_value(row.get('object_label'))
        if not provider_term:
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

        # Handle SSSOM predicate mappings
        match_type = safe_value(row.get('predicate_id'))
        if not match_type:
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

    if skipped_unused:
        logging.info(f"SKOS: skipped {skipped_unused} mapping-table term(s) not used in this dataset.")
    return g, term_to_uri_lookup
