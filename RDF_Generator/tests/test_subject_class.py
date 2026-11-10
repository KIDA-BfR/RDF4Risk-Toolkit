"""Tests for the subject class (rdf:type on every row subject).

Run from the repository root:  python -m pytest RDF_Generator/tests
"""
from pathlib import Path

import pandas as pd
import yaml
from rdflib import URIRef
from rdflib.namespace import RDF, RDFS

from RDF_Generator.rdf_processor import create_rdf_with_mappings

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
NS = "https://test.example/"
GRAPH = NS + "graph/data"
SOSA_SAMPLE = URIRef("http://www.w3.org/ns/sosa/Sample")
TEMPLATE_TYPE = URIRef("http://www.w3.org/ns/sosa/Observation")


def build(class_uri, templates=None, active=None):
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    config["default_namespace"] = NS
    df = pd.DataFrame({"_generated_id_": ["id_1", "id_2"], "Pathogen": ["Salmonella", "Listeria"]})
    mapping = pd.DataFrame({"Term": ["Pathogen"], "URI": ["http://purl.obolibrary.org/obo/NCIT_C14250"]})
    return create_rdf_with_mappings(
        df=df, mapping_df=mapping, id_column="_generated_id_", string_column="Term", iri_column="URI",
        instance_class_uri=class_uri, named_graph_uri=GRAPH, config=config,
        schema_templates=templates or [], active_template_name=active,
    )


def row_subjects(graph):
    return {s for s in graph.subjects() if "/subjects/" in str(s)}


def test_every_row_gets_the_class():
    graph = build(str(SOSA_SAMPLE))
    subjects = row_subjects(graph)
    assert len(subjects) == 2
    assert all((s, RDF.type, SOSA_SAMPLE) in graph for s in subjects)


def test_external_class_is_not_redeclared():
    graph = build(str(SOSA_SAMPLE))
    assert (SOSA_SAMPLE, RDF.type, RDFS.Class) not in graph
    assert not list(graph.objects(SOSA_SAMPLE, RDFS.label)), "no label guessed from the URI"


def test_own_namespace_class_is_declared():
    own_class = URIRef(NS + "classes/Sample")
    graph = build(str(own_class))
    assert (own_class, RDF.type, RDFS.Class) in graph


def test_template_without_type_keeps_the_class():
    templates = [{"template_name": "T", "properties": []}]
    graph = build(str(SOSA_SAMPLE), templates, "T")
    subjects = row_subjects(graph)
    # A template replaces the column mapping, so with an empty template the class triple is the only
    # thing that makes the rows appear at all; require both (an empty set would pass vacuously).
    assert len(subjects) == 2
    assert all((s, RDF.type, SOSA_SAMPLE) in graph for s in subjects)


def test_template_type_replaces_the_class():
    templates = [{"template_name": "T", "rdf_type": str(TEMPLATE_TYPE), "properties": []}]
    graph = build(str(SOSA_SAMPLE), templates, "T")
    subjects = row_subjects(graph)
    assert all((s, RDF.type, TEMPLATE_TYPE) in graph for s in subjects)
    assert not any((s, RDF.type, SOSA_SAMPLE) in graph for s in subjects)
