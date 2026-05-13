# -*- coding: utf-8 -*-
"""BioPortal recommendation and lookup helpers for agent-based reconciliation."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from .agent_models import AgentCandidate


DEFAULT_BIOPORTAL_BASE_URL = "https://data.bioontology.org"


def chunk_terms(terms: List[str], chunk_size: int) -> Iterable[List[str]]:
    for i in range(0, len(terms), chunk_size):
        yield terms[i:i + chunk_size]


def recommend_ontology_acronyms(
    terms: Iterable[str],
    api_key: str,
    *,
    min_valid: int = 5,
    exclude: Optional[set] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
) -> List[str]:
    exclude = exclude or set()
    params = {
        "input": ",".join(str(term).strip() for term in terms if str(term).strip()),
        "input_type": 2,
        "apikey": api_key,
    }
    if not params["input"]:
        return []
    try:
        response = requests.get(f"{base_url}/recommender", params=params, timeout=240)
        response.raise_for_status()
        recs = response.json()
    except requests.RequestException:
        return []

    results: List[str] = []
    for entry in recs:
        try:
            ontology = entry["ontologies"][0]
            acronym = ontology["acronym"]
        except (KeyError, IndexError, TypeError):
            continue
        if acronym in exclude:
            continue
        if acronym not in results:
            results.append(acronym)
        if len(results) >= min_valid:
            break
    return results


def _extract_definition(entry: Dict[str, any]) -> str:
    definitions = entry.get("definition") or []
    if isinstance(definitions, list):
        for item in definitions:
            if isinstance(item, str) and item.strip():
                return item.strip()
    elif isinstance(definitions, str) and definitions.strip():
        return definitions.strip()
    return ""


def _match_label_or_synonym(entry: Dict[str, any], term: str, case_sensitive: bool = False) -> str:
    label = entry.get("prefLabel", "") or ""
    synonyms = entry.get("synonym") or []
    if isinstance(synonyms, str):
        synonyms = [synonyms]

    term_cmp = term if case_sensitive else term.lower()
    label_cmp = label if case_sensitive else label.lower()
    if label_cmp == term_cmp:
        return "exact"
    for synonym in synonyms:
        synonym_cmp = synonym if case_sensitive else str(synonym).lower()
        if synonym_cmp == term_cmp:
            return "synonym"
    return ""


def find_term_in_ontology(
    term: str,
    ontology: str,
    exact: bool = True,
    case_sensitive: bool = False,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
) -> Tuple[str, str]:
    params = {
        "q": term,
        "ontologies": ontology,
        "require_exact_match": str(exact).lower(),
        "include": "prefLabel,definition,synonym,notation,cui,semanticType",
        "pagesize": 20,
        "apikey": api_key,
    }
    try:
        response = requests.get(f"{base_url}/search", params=params, timeout=15)
        response.raise_for_status()
        entries = response.json().get("collection", [])
    except (requests.RequestException, ValueError, KeyError):
        if exact:
            return find_term_in_ontology(term, ontology, exact=False, case_sensitive=case_sensitive, api_key=api_key, base_url=base_url)
        return "", ""

    filtered = []
    if exact:
        for entry in entries:
            if _match_label_or_synonym(entry, term, case_sensitive=case_sensitive) == "exact":
                filtered.append(entry)
    else:
        filtered = entries

    if not filtered and exact:
        return find_term_in_ontology(term, ontology, exact=False, case_sensitive=case_sensitive, api_key=api_key, base_url=base_url)
    if not filtered:
        return "", ""

    best = filtered[0]
    match_type = _match_label_or_synonym(best, term, case_sensitive=case_sensitive) or ("exact" if exact else "broad")
    return best.get("@id", ""), match_type


def find_term_in_ontology_with_definition(
    term: str,
    ontology: str,
    exact: bool = True,
    case_sensitive: bool = False,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    allow_fallback: bool = True,
) -> Optional[Dict[str, str]]:
    params = {
        "q": term,
        "ontologies": ontology,
        "require_exact_match": str(exact).lower(),
        "include": "prefLabel,definition,synonym,notation,cui,semanticType",
        "pagesize": 20,
        "apikey": api_key,
    }
    try:
        response = requests.get(f"{base_url}/search", params=params, timeout=15)
        response.raise_for_status()
        entries = response.json().get("collection", [])
    except (requests.RequestException, ValueError, KeyError):
        if exact and allow_fallback:
            return find_term_in_ontology_with_definition(term, ontology, exact=False, case_sensitive=case_sensitive, api_key=api_key, base_url=base_url, allow_fallback=False)
        return None

    filtered = []
    for entry in entries:
        match_type = _match_label_or_synonym(entry, term, case_sensitive=case_sensitive)
        if exact and match_type != "exact":
            continue
        filtered.append((entry, match_type or ("exact" if exact else "broad")))

    if not filtered and exact and allow_fallback:
        return find_term_in_ontology_with_definition(term, ontology, exact=False, case_sensitive=case_sensitive, api_key=api_key, base_url=base_url, allow_fallback=False)
    if not filtered:
        return None

    best, match_type = filtered[0]
    synonyms = best.get("synonym") or []
    if isinstance(synonyms, str):
        synonyms = [synonyms]
        
    ontology_link = best.get("links", {}).get("ontology", "")
    acronym = urlparse(ontology_link).path.split("/")[-1] if ontology_link else ""

    return {
        "mapped_id": best.get("@id", ""),
        "mapped_type": match_type,
        "label": best.get("prefLabel", "") or "",
        "definition": _extract_definition(best),
        "synonyms": synonyms,
        "acronym": acronym,
    }


def find_indirect_definition(
    term: str,
    ontology: str,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
) -> Optional[Dict[str, str]]:
    search_params = {
        "q": term,
        "ontologies": ontology,
        "require_exact_match": "true",
        "pagesize": 1,
        "apikey": api_key,
    }
    try:
        response = requests.get(f"{base_url}/search", params=search_params, timeout=15)
        response.raise_for_status()
        collection = response.json().get("collection", [])
    except requests.RequestException:
        return None
    if not collection:
        return None

    mappings_url = collection[0].get("links", {}).get("mappings")
    if not mappings_url:
        return None

    try:
        mappings_response = requests.get(mappings_url, params={"apikey": api_key}, timeout=15)
        mappings_response.raise_for_status()
        mappings_data = mappings_response.json()
    except requests.RequestException:
        return None

    records = mappings_data if isinstance(mappings_data, list) else mappings_data.get("collection", [])
    for record in records:
        classes = record.get("classes", [])
        if len(classes) < 2:
            continue
        target = classes[1]
        iri = target.get("@id", "")
        links = target.get("links", {}) or {}
        self_link = links.get("self")
        ontology_link = links.get("ontology", "")
        if not self_link:
            continue
        try:
            class_response = requests.get(self_link, params={"apikey": api_key}, timeout=15)
            class_response.raise_for_status()
            entry = class_response.json()
        except requests.RequestException:
            continue

        definition = _extract_definition(entry)
        if definition:
            return {
                "definition": definition,
                "iri": iri,
                "source_onto": ontology_link,
                "label": entry.get("prefLabel", "") or target.get("prefLabel", ""),
            }
    return None


def find_best_definition(
    term: str,
    ontology: str,
    exact: bool = True,
    case_sensitive: bool = False,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
) -> Optional[Dict[str, str]]:
    try:
        direct = find_term_in_ontology_with_definition(term, ontology, exact=exact, case_sensitive=case_sensitive, api_key=api_key, base_url=base_url)
        if not direct:
            return None

        mapped_id = direct.get("mapped_id", "").strip()
        mapped_type = direct.get("mapped_type", "").strip()
        definition = direct.get("definition", "").strip()
        label = direct.get("label", "").strip()

        if not mapped_id:
            return None

        if definition:
            return {
                "mapped_id": mapped_id,
                "mapped_type": f"{mapped_type}+Definition",
                "definition": definition,
                "definition_source": "original",
                "definition_source_ontology": mapped_id,
                "label": label,
            }

        indirect = find_indirect_definition(term, ontology, api_key=api_key, base_url=base_url)
        if indirect and indirect.get("definition"):
            return {
                "mapped_id": mapped_id,
                "mapped_type": f"{mapped_type}+Definition",
                "definition": indirect["definition"],
                "definition_source": "indirect",
                "definition_source_ontology": indirect.get("iri", ""),
                "label": indirect.get("label", label),
            }

        return {
            "mapped_id": mapped_id,
            "mapped_type": f"{mapped_type}+Unverified",
            "definition": "",
            "definition_source": "",
            "definition_source_ontology": "",
            "label": label,
        }
    except Exception:
        return None


def search_bioportal_candidates(
    term: str,
    api_key: str,
    ontologies: Optional[List[str]] = None,
    page_size: int = 10,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
) -> List[AgentCandidate]:
    params = {
        "q": term,
        "include": "prefLabel,synonym,definition",
        "page": 1,
        "pagesize": page_size,
        "display_context": "false",
        "display_links": "true",
        "apikey": api_key,
    }
    if ontologies:
        params["ontologies"] = ",".join(ontologies)
    response = requests.get(f"{base_url}/search", params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    suggestions = []
    for entry in data.get("collection", []):
        uri = entry.get("@id", "")
        ontology_link = entry.get("links", {}).get("ontology", "")
        ontology_acronym = urlparse(ontology_link).path.split("/")[-1] if ontology_link else "BioPortal"
        suggestions.append(
            AgentCandidate(
                uri=uri,
                label=entry.get("prefLabel", "") or "",
                description=_extract_definition(entry),
                source_provider=ontology_acronym or "BioPortal",
                source_workflow="bioportal_wikidata_multiagent",
                raw_identifier=uri,
            )
        )
    return suggestions
