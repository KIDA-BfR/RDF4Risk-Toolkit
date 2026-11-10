"""Compact overview of the generated graphs for the "Result overview" in the RDF generator.

Four layers (dataset metadata, publication, data graph, SKOS vocabulary) as small label/IRI
tables, plus the links between them. Each link is checked against the generated triples, so the
overview shows whether the output is actually connected, not whether it should be.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, FOAF, RDF, RDFS, SKOS, VOID

SOA = Namespace("https://semopenalex.org/ontology/")
PRISM = Namespace("http://prismstandard.org/namespaces/basic/2.0/")
FABIO = Namespace("http://purl.org/spar/fabio/")

MAX_DATA_ROWS = 5
MAX_DATA_COLUMNS = 5
MAX_CONCEPTS = 8


def _cell(value: Any, label: Optional[str] = None) -> Dict[str, Any]:
    """A table cell: readable text plus the IRI behind it (None for literals)."""
    if isinstance(value, URIRef):
        return {"text": label or _short(value), "iri": str(value)}
    if isinstance(value, BNode):
        return {"text": label or "(nested values)", "iri": None}
    return {"text": "" if value is None else str(value), "iri": None}


def _short(iri: URIRef) -> str:
    text = str(iri).rstrip("/#")
    return text.rsplit("#", 1)[-1].rsplit("/", 1)[-1] or str(iri)


def _first(graph: Graph, subject, predicate):
    return next(graph.objects(subject, predicate), None)


def _labeler(*graphs: Graph):
    """Label for an IRI from skos:prefLabel, rdfs:label, foaf:name or dct:title in the given graphs."""
    def label(node) -> Optional[str]:
        if not isinstance(node, URIRef):
            return None
        for graph in graphs:
            if graph is None:
                continue
            for predicate in (SKOS.prefLabel, RDFS.label, FOAF.name, DCTERMS.title):
                value = _first(graph, node, predicate)
                if value is not None:
                    return str(value)
        return None
    return label


def _row(name: str, value: Any, label=None) -> Dict[str, Any]:
    return {"label": name, **_cell(value, label(value) if label else None)}


def _metadata_layer(meta: Optional[Graph], label) -> Dict[str, Any]:
    if meta is None or len(meta) == 0:
        return {"present": False, "rows": [], "triples": 0}
    dataset = next(meta.subjects(RDF.type, DCAT.Dataset), None)
    rows: List[Dict[str, Any]] = []
    if dataset is not None:
        rows.append(_row("Dataset", dataset, lambda _n: str(_first(meta, dataset, DCTERMS.title) or "")))
        for name, predicate in (("Description", DCTERMS.description), ("Publisher", DCTERMS.publisher), ("License", DCTERMS.license), ("Access rights", DCTERMS.accessRights)):
            value = _first(meta, dataset, predicate)
            if value is not None:
                rows.append(_row(name, value, label))
        themes = list(meta.objects(dataset, DCAT.theme))
        if themes:
            rows.append({"label": "Themes", "text": ", ".join(_short(t) for t in themes), "iri": None, "iris": [str(t) for t in themes]})
        distribution = _first(meta, dataset, DCAT.distribution)
        if distribution is not None:
            rows.append(_row("Distribution", distribution, lambda _n: str(_first(meta, distribution, DCAT.mediaType) and _short(_first(meta, distribution, DCAT.mediaType)) or "Distribution")))
    return {"present": dataset is not None, "rows": rows, "triples": len(meta), "iri": str(dataset) if dataset is not None else None}


def _publication_layer(reference: Optional[Graph]) -> Dict[str, Any]:
    if reference is None or len(reference) == 0:
        return {"present": False, "rows": [], "triples": 0}
    work = next(reference.subjects(RDF.type, SOA.Work), None)
    if work is None:
        return {"present": False, "rows": [], "triples": len(reference)}
    rows = [_row("Work", work, lambda _n: str(_first(reference, work, DCTERMS.title) or "Publication"))]
    doi = _first(reference, work, PRISM.doi)
    if doi is not None:
        rows.append({"label": "DOI", **_cell(doi, str(doi).replace("https://doi.org/", ""))})
    authors = [str(name) for author in reference.subjects(RDF.type, SOA.Author) for name in reference.objects(author, FOAF.name)]
    if authors:
        rows.append({"label": "Authors", "text": "; ".join(authors[:4]) + (f" (+{len(authors) - 4})" if len(authors) > 4 else ""), "iri": None})
    year = _first(reference, work, FABIO.hasPublicationYear)
    if year is not None:
        rows.append({"label": "Year", "text": str(year), "iri": None})
    return {"present": True, "rows": rows, "triples": len(reference), "iri": str(work)}


def _row_subjects(data: Graph) -> List[Any]:
    """Subjects that stand for data rows: not property/class declarations or the column-order nodes."""
    skip_types = {RDF.Property, RDFS.Class}
    subjects = []
    for subject in set(data.subjects()):
        if isinstance(subject, BNode):
            continue
        types = set(data.objects(subject, RDF.type))
        if types & skip_types or any(str(t).endswith(("Dataset", "Column")) and str(t) != str(DCAT.Dataset) for t in types):
            continue
        subjects.append(subject)
    return sorted(subjects, key=str)


def _data_layer(data: Optional[Graph], graph_iri: Optional[str], label) -> Dict[str, Any]:
    if data is None or len(data) == 0:
        return {"present": False, "columns": [], "rows": [], "triples": 0}
    subjects = _row_subjects(data)
    predicates = Counter(p for s in subjects for p in data.predicates(s) if p != RDF.type)
    columns = [p for p, _ in predicates.most_common(MAX_DATA_COLUMNS)]
    classes = sorted({o for s in subjects for o in data.objects(s, RDF.type)}, key=str)
    rows = []
    for subject in subjects[:MAX_DATA_ROWS]:
        cells = []
        for predicate in columns:
            value = _first(data, subject, predicate)
            cells.append(_cell(value, label(value)) if value is not None else {"text": "", "iri": None})
        rows.append({"subject": _cell(subject), "cells": cells})
    return {
        "present": True,
        "iri": graph_iri,
        "triples": len(data),
        "subjects": len(subjects),
        "classes": [_cell(c, label(c)) for c in classes],
        "columns": [_cell(p, label(p)) for p in columns],
        "rows": rows,
    }


def _vocabulary_layer(skos: Optional[Graph], graph_iri: Optional[str]) -> Dict[str, Any]:
    if skos is None or len(skos) == 0:
        return {"present": False, "rows": [], "triples": 0}
    concepts = sorted(skos.subjects(RDF.type, SKOS.Concept), key=lambda c: str(_first(skos, c, SKOS.prefLabel) or c))
    rows = []
    for concept in concepts[:MAX_CONCEPTS]:
        match = _first(skos, concept, SKOS.exactMatch) or _first(skos, concept, SKOS.closeMatch)
        match_type = "exactMatch" if _first(skos, concept, SKOS.exactMatch) is not None else "closeMatch" if match is not None else ""
        rows.append({"concept": _cell(concept, str(_first(skos, concept, SKOS.prefLabel) or "")), "match": _cell(match) if match is not None else None, "match_type": match_type})
    return {"present": True, "iri": graph_iri, "triples": len(skos), "concepts": len(concepts), "rows": rows}


def build_overview(
    data_graph=None,
    named_graph_uri: Optional[str] = None,
    skos_graph=None,
    metadata_ttl: Optional[str] = None,
    reference_graph: Optional[Graph] = None,
    reference_linked: bool = False,
) -> Dict[str, Any]:
    """Overview of the generated layers and their verified links (JSON-serialisable)."""
    data = data_graph.get_context(URIRef(named_graph_uri)) if data_graph is not None and named_graph_uri else data_graph
    skos = Graph()
    if skos_graph is not None:
        for triple in skos_graph.triples((None, None, None)):
            skos.add(triple)
    skos_iri = next((str(ctx.identifier) for ctx in skos_graph.contexts() if len(ctx)), None) if skos_graph is not None and hasattr(skos_graph, "contexts") else None
    meta = None
    if metadata_ttl:
        meta = Graph()
        meta.parse(data=metadata_ttl, format="turtle")
    label = _labeler(skos, data, meta, reference_graph)

    layers = {
        "metadata": _metadata_layer(meta, label),
        "publication": _publication_layer(reference_graph),
        "dataset": _data_layer(data, named_graph_uri, label),
        "vocabulary": _vocabulary_layer(skos, skos_iri),
    }

    links = []
    # Metadata -> data graph: a distribution whose accessURL is the data graph.
    if layers["metadata"]["present"] and named_graph_uri:
        ok = any((dist, DCAT.accessURL, URIRef(named_graph_uri)) in meta for dist in meta.objects(None, DCAT.distribution))
        links.append({"id": "metadata-dataset", "from": "metadata", "to": "dataset", "property": "dcat:distribution → dcat:accessURL", "status": "ok" if ok else "missing",
                      "detail": "The catalog's distribution points to the data graph." if ok else "No distribution points to the data graph."})
    else:
        links.append({"id": "metadata-dataset", "from": "metadata", "to": "dataset", "property": "dcat:distribution", "status": "off", "detail": "No catalog yet: complete Describe dataset and generate."})
    # Metadata -> publication: dct:isReferencedBy the publication's work.
    work = layers["publication"].get("iri")
    if not layers["publication"]["present"]:
        links.append({"id": "metadata-publication", "from": "metadata", "to": "publication", "property": "dct:isReferencedBy", "status": "off", "detail": "No publication attached."})
    elif not layers["metadata"]["present"]:
        links.append({"id": "metadata-publication", "from": "metadata", "to": "publication", "property": "dct:isReferencedBy", "status": "off", "detail": "No catalog yet."})
    else:
        ok = any(str(o) == work for o in meta.objects(None, DCTERMS.isReferencedBy))
        links.append({"id": "metadata-publication", "from": "metadata", "to": "publication", "property": "dct:isReferencedBy", "status": "ok" if ok else "missing",
                      "detail": "The catalog references the publication." if ok else ("Linking is switched off in Describe dataset." if not reference_linked else "The catalog does not reference the publication.")})
    # Data graph <-> vocabulary: concepts used in the data, and concepts pointing back with void:inDataset.
    if layers["dataset"]["present"] and layers["vocabulary"]["present"]:
        concepts = set(skos.subjects(RDF.type, SKOS.Concept))
        used = {t for s, p, o in data.triples((None, None, None)) for t in (p, o) if t in concepts}
        back = sum(1 for c in concepts if named_graph_uri and (c, VOID.inDataset, URIRef(named_graph_uri)) in skos)
        ok = bool(used) and back == len(concepts)
        links.append({"id": "dataset-vocabulary", "from": "dataset", "to": "vocabulary", "property": "uses concepts · void:inDataset", "status": "ok" if ok else "missing",
                      "detail": f"The data uses {len(used)} of {len(concepts)} concepts; {back} of {len(concepts)} concepts point back to the data graph.",
                      "counts": {"used": len(used), "concepts": len(concepts), "back": back}})
    else:
        links.append({"id": "dataset-vocabulary", "from": "dataset", "to": "vocabulary", "property": "uses concepts", "status": "off", "detail": "Generate the RDF first."})
    return {"layers": layers, "links": links}

