"""Tests for the result overview: layers and links are read from the generated graphs.

Run from the repository root:  python -m pytest RDF_Generator/tests
"""
from pathlib import Path

import pandas as pd
import yaml

from RDF_Generator.dcat_generator import create_dcat_catalog_bundle
from RDF_Generator.graph_overview import build_overview
from RDF_Generator.rdf_processor import create_rdf_with_mappings
from RDF_Generator.reference_handler import DOIToSemOpenAlexConverter
from RDF_Generator.skos_generator import collect_used_terms, create_skos_graph_and_lookup_map

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
NS = "https://test.example/"
GRAPH = NS + "dataset/da123ta"


def _reference_graph():
    converter = DOIToSemOpenAlexConverter()
    metadata = {"title": ["Retail survey report"], "author": [{"given": "Alice", "family": "Doe"}], "container-title": ["BfR reports"]}
    work = converter.create_work_uri("Retail survey report")
    converter.add_work_metadata(work, metadata)
    converter.add_authors(work, metadata)
    return converter.graph


def _outputs(link_reference=True, with_catalog=True):
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    config["default_namespace"] = NS
    df = pd.DataFrame({"_generated_id_": ["id_1", "id_2"], "Pathogen": ["Salmonella", "Listeria"]})
    mapping = pd.DataFrame({
        "Term": ["Pathogen", "Salmonella", "Listeria", "Campylobacter"],
        "URI": ["http://purl.obolibrary.org/obo/NCIT_C14250", "http://purl.obolibrary.org/obo/NCBITaxon_590", "http://purl.obolibrary.org/obo/NCBITaxon_1637", "http://purl.obolibrary.org/obo/NCBITaxon_194"],
        "Match Type": ["skos:exactMatch"] * 4,
    })
    skos, lookup = create_skos_graph_and_lookup_map(mapping.copy(), config, GRAPH, used_terms=collect_used_terms(df, ("_generated_id_",)))
    data = create_rdf_with_mappings(df=df, mapping_df=mapping.copy(), id_column="_generated_id_", string_column="Term", iri_column="URI",
                                    instance_class_uri="http://www.w3.org/ns/sosa/Sample", named_graph_uri=GRAPH, config=config, term_to_concept_uri_map=lookup)
    reference = _reference_graph()
    metadata_ttl = None
    if with_catalog:
        _catalog, metadata_ttl = create_dcat_catalog_bundle(
            rdf_graph=data, skos_graph=skos, rdf_format="turtle", data_graph_uri_str=GRAPH, default_namespace=NS,
            metadata_config={"title": "Pathogen survey", "description": "Retail samples.", "keywords": [], "identifier": "", "creator": [],
                             "access_rights": "PUBLIC", "contact_point": "", "publisher_name": "BfR", "publisher_uri": NS + "organization",
                             "themes": [], "license": "cc-by/4.0", "link_reference": link_reference,
                             "reference_data": {"method": "Manual", "graph": reference} if link_reference else None},
        )
    return build_overview(data_graph=data, named_graph_uri=GRAPH, skos_graph=skos, metadata_ttl=metadata_ttl, reference_graph=reference, reference_linked=link_reference)


def _links(overview):
    return {link["id"]: link for link in overview["links"]}


def test_all_layers_present_and_linked():
    overview = _outputs()
    layers = overview["layers"]
    assert all(layers[name]["present"] for name in ("metadata", "publication", "dataset", "vocabulary"))
    assert {link: item["status"] for link, item in _links(overview).items()} == {
        "metadata-dataset": "ok", "metadata-publication": "ok", "dataset-vocabulary": "ok"}


def test_tables_show_labels_with_iris():
    layers = _outputs()["layers"]
    dataset = layers["dataset"]
    assert dataset["subjects"] == 2 and [c["text"] for c in dataset["classes"]] == ["Sample"]
    assert [c["text"] for c in dataset["columns"]] == ["Pathogen"]
    values = {row["cells"][0]["text"] for row in dataset["rows"]}
    assert values == {"Salmonella", "Listeria"}, "concept IRIs shown by their prefLabel"
    assert all(row["cells"][0]["iri"].startswith(NS + "concepts/") for row in dataset["rows"])
    concepts = {row["concept"]["text"]: row for row in layers["vocabulary"]["rows"]}
    assert set(concepts) == {"Pathogen", "Salmonella", "Listeria"}  # Campylobacter is not used
    assert concepts["Salmonella"]["match"]["iri"].endswith("NCBITaxon_590") and concepts["Salmonella"]["match_type"] == "exactMatch"


def test_catalog_dataset_iri_is_well_formed():
    rows = {row["label"]: row for row in _outputs()["layers"]["metadata"]["rows"]}
    assert rows["Dataset"]["iri"] == GRAPH + "/dataset/main"
    assert rows["License"]["iri"] == "http://dcat-ap.de/def/licenses/cc-by/4.0"


def test_unlinked_publication_is_reported():
    link = _links(_outputs(link_reference=False))["metadata-publication"]
    assert link["status"] == "missing" and "switched off" in link["detail"]


def test_without_catalog_links_are_off_not_missing():
    links = _links(_outputs(with_catalog=False))
    assert links["metadata-dataset"]["status"] == "off"
    assert links["metadata-publication"]["status"] == "off"
    assert links["dataset-vocabulary"]["status"] == "ok"
