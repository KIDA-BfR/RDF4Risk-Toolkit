"""Tests for the DCAT-AP.de licence list and its use in the DCAT catalog.

Run from the repository root:  python -m pytest RDF_Generator/tests
"""
import tempfile
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCTERMS, RDF, RDFS

from RDF_Generator import license_registry as registry
from RDF_Generator.dcat_generator import _add_license

DATASET = URIRef("https://test.example/dataset/1")
DCATDE_LICENSES = "http://dcat-ap.de/def/licenses/"


def test_shipped_list_uses_dcat_ap_de_uris():
    licenses = registry.list_licenses()
    ids = [lic["id"] for lic in licenses]
    assert len(ids) == len(set(ids)) and registry.DEFAULT_LICENSE_ID in ids
    assert all(lic["uri"] == DCATDE_LICENSES + lic["id"] for lic in licenses)
    assert all(lic["label"] and lic["spdx"] for lic in licenses)


def test_resolve_accepts_dcat_id_spdx_and_legacy_names():
    assert registry.resolve("cc-by/4.0")["id"] == "cc-by/4.0"
    assert registry.resolve("CC-BY-4.0")["id"] == "cc-by/4.0"   # SPDX, used by the CLI config
    assert registry.resolve("CC BY 4.0")["id"] == "cc-by/4.0"   # saved by older versions
    assert registry.resolve("CC0 1.0")["id"] == "cc-zero"
    assert registry.resolve("unknown licence") is None and registry.resolve("") is None
    assert registry.normalize_id("unknown licence") == registry.DEFAULT_LICENSE_ID


def test_missing_licence_file_gives_empty_list_not_a_crash():
    with tempfile.TemporaryDirectory() as tmp:
        assert registry.list_licenses(Path(tmp) / "missing.json") == []
        assert registry.resolve("cc-by/4.0", Path(tmp) / "missing.json") is None


def test_catalog_license_uses_dcat_ap_de_uri():
    graph = Graph()
    _add_license(graph, DATASET, "CC-BY-4.0")
    lic = URIRef(DCATDE_LICENSES + "cc-by/4.0")
    assert (DATASET, DCTERMS.license, lic) in graph
    assert (lic, RDF.type, DCTERMS.LicenseDocument) in graph
    assert (lic, RDFS.label, Literal("Creative Commons Attribution 4.0 International (CC BY 4.0)", lang="en")) in graph


def test_unknown_license_does_not_break_catalog():
    graph = Graph()
    _add_license(graph, DATASET, "Data Use Agreement for Official Statistics")
    assert len(graph) == 0
