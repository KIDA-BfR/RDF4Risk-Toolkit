"""Regression tests for SKOS vocabulary scoping and concept linking in the data graph.

Run from the repository root:  python -m pytest RDF_Generator/tests
"""
from pathlib import Path

import pandas as pd
import pytest
import yaml
from rdflib import URIRef
from rdflib.namespace import RDF, SKOS

from RDF_Generator.rdf_processor import create_rdf_with_mappings
from RDF_Generator.skos_generator import collect_used_terms, create_skos_graph_and_lookup_map

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
GRAPH = "https://test.example/graph/data"
NS = "https://test.example/"
OBO = "http://purl.obolibrary.org/obo/"


@pytest.fixture
def config():
    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    cfg["default_namespace"] = NS
    return cfg


@pytest.fixture
def mapping():
    """Shared mapping table: Campylobacter and 'Wheat yield' belong to another dataset."""
    return pd.DataFrame({
        "Term": ["Pathogen", "Salmonella", "Listeria", "Campylobacter", "Wheat yield"],
        "URI": [OBO + "NCIT_C14250", OBO + "NCBITaxon_590", OBO + "NCBITaxon_1637", OBO + "NCBITaxon_194", "http://example.org/yield"],
        "Match Type": ["skos:exactMatch"] * 5,
    })


def concept(term: str) -> URIRef:
    return URIRef(NS + "concepts/" + term)


def build(df, mapping_df, config, id_column="_generated_id_", **kwargs):
    used = collect_used_terms(df, exclude_columns=(id_column,))
    skos, lookup = create_skos_graph_and_lookup_map(mapping_df.copy(), config, GRAPH, used_terms=used)
    data = create_rdf_with_mappings(
        df=df, mapping_df=mapping_df.copy(), id_column=id_column, string_column="Term", iri_column="URI",
        named_graph_uri=GRAPH, config=config, term_to_concept_uri_map=lookup, **kwargs,
    )
    return skos, lookup, data


def subject_objects(data, predicate):
    return {o for _, _, o in data.triples((None, predicate, None))}


def test_collect_used_terms_normalizes_and_skips_id_columns():
    df = pd.DataFrame({"_generated_id_": ["Salmonella"], " Pathogen ": [" Listeria "], "Count": [None]})
    assert collect_used_terms(df, exclude_columns=("_generated_id_", None)) == {"pathogen", "listeria", "count"}


def test_skos_contains_only_terms_used_in_dataset(mapping, config):
    df = pd.DataFrame({"_generated_id_": ["id_1", "id_2"], "Pathogen": ["Salmonella", "Listeria"]})
    skos, _, _ = build(df, mapping, config)
    labels = {str(o) for o in skos.objects(None, SKOS.prefLabel)}
    assert labels == {"Pathogen", "Salmonella", "Listeria"}


def test_skos_without_used_terms_keeps_all_rows(mapping, config):
    skos, _ = create_skos_graph_and_lookup_map(mapping.copy(), config, GRAPH)
    assert len(set(skos.subjects(RDF.type, SKOS.Concept))) == len(mapping)


def test_case_and_space_variants_link_to_internal_concepts(mapping, config):
    df = pd.DataFrame({"_generated_id_": ["id_1", "id_2"], "pathogen ": ["Salmonella", "listeria "]})
    skos, _, data = build(df, mapping, config)

    assert subject_objects(data, concept("Pathogen")) == {concept("Salmonella"), concept("Listeria")}
    used = {p for _, p, _ in data.triples((None, None, None))} | {o for _, _, o in data.triples((None, None, None))}
    assert not any(str(term).startswith(OBO) for term in used), "external URI used directly in data graph"
    assert set(skos.subjects(RDF.type, SKOS.Concept)) <= used, "SKOS concept not used by data graph"


def test_grouping_keeps_generator_config(mapping, config):
    df = pd.DataFrame({"_generated_id_": ["id_1"], "Größe": ["5"], "Einheit": ["cm"]})
    groups = {"measurement": {"connecting_predicate": "http://example.org/hasMeasurement", "columns": ["Größe", "Einheit"]}}
    _, _, data = build(df, mapping, config, group_config=groups)
    predicates = {p for _, p, _ in data.triples((None, None, None))}
    # Namespace and umlaut replacement come from the generator config, not the group settings.
    assert concept("Groesse") in predicates
    assert concept("Einheit") in predicates


def test_template_values_link_to_internal_concepts(mapping, config):
    df = pd.DataFrame({"_generated_id_": ["id_1"], "Pathogen": ["listeria"]})
    templates = [{"template_name": "T", "properties": [
        {"predicate": "http://example.org/organism", "map_type": "Column Value (Literal)", "value": "Pathogen"},
    ]}]
    _, _, data = build(df, mapping, config, schema_templates=templates, active_template_name="T")
    assert subject_objects(data, URIRef("http://example.org/organism")) == {concept("Listeria")}


def test_template_values_use_configured_iri_column(mapping, config):
    """Without a concept map, template values fall back to the mapping table's IRI column."""
    mapping_df = mapping.rename(columns={"URI": "Mapped ID"})
    df = pd.DataFrame({"_generated_id_": ["id_1"], "Pathogen": ["listeria"]})
    templates = [{"template_name": "T", "properties": [
        {"predicate": "http://example.org/organism", "map_type": "Column Value (Literal)", "value": "Pathogen"},
    ]}]
    data = create_rdf_with_mappings(
        df=df, mapping_df=mapping_df, id_column="_generated_id_", string_column="Term", iri_column="Mapped ID",
        named_graph_uri=GRAPH, config=config, schema_templates=templates, active_template_name="T",
    )
    assert subject_objects(data, URIRef("http://example.org/organism")) == {URIRef(OBO + "NCBITaxon_1637")}


def test_skos_without_named_graph(mapping, config):
    """'No named graph' passes data_graph_uri=None; SKOS must still build, just without void:inDataset."""
    skos, lookup = create_skos_graph_and_lookup_map(mapping.copy(), config, None)
    assert lookup and len(set(skos.subjects(RDF.type, SKOS.Concept))) == len(mapping)
