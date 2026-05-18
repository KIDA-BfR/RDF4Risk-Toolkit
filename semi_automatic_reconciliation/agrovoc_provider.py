# -*- coding: utf-8 -*-
"""AGROVOC reconciliation provider."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

logger = logging.getLogger(__name__)

DEFAULT_AGROVOC_SPARQL_ENDPOINT = "https://agrovoc.fao.org/sparql"

SPARQL_QUERY_TEMPLATE = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?conceptURI ?prefLabel (SAMPLE(?definition) AS ?definitionSample) (SAMPLE(?scopeNote) AS ?scopeNoteSample)
WHERE {{
  GRAPH <http://aims.fao.org/aos/agrovoc/> {{
    {{ ?conceptURI skosxl:prefLabel ?labelLit . }}
    UNION
    {{ ?conceptURI skosxl:altLabel ?labelLit . }}
    ?labelLit skosxl:literalForm ?labelValue .
    FILTER(REGEX(STR(?labelValue), "{term}", "i"))
    FILTER(LANGMATCHES(LANG(?labelValue), "{lang}"))
    ?conceptURI skosxl:prefLabel ?prefLabelLit .
    ?prefLabelLit skosxl:literalForm ?prefLabel .
    FILTER(LANGMATCHES(LANG(?prefLabel), "{lang}"))
    OPTIONAL {{
      ?conceptURI skos:definition ?definition .
      FILTER(LANGMATCHES(LANG(?definition), "{lang}"))
    }}
    OPTIONAL {{
      ?conceptURI skos:scopeNote ?scopeNote .
      FILTER(LANGMATCHES(LANG(?scopeNote), "{lang}"))
    }}
    ?conceptURI a skos:Concept .
  }}
}}
GROUP BY ?conceptURI ?prefLabel
LIMIT {limit}
"""


class AgrovocProvider(BaseProvider):
    name = "AGROVOC"

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        provider_config = config.get("agrovoc", {})
        return {
            "sparql_endpoint": provider_config.get("sparql_endpoint", DEFAULT_AGROVOC_SPARQL_ENDPOINT),
            "lang": provider_config.get("lang", "en"),
        }

    def _fetch(
        self,
        term: str,
        limit: int,
        user_agent: str,
        *,
        sparql_endpoint: str = DEFAULT_AGROVOC_SPARQL_ENDPOINT,
        lang: str = "en",
        **_: Any,
    ) -> List[Dict[str, Any]]:
        safe_term = term.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        query = SPARQL_QUERY_TEMPLATE.format(term=safe_term, lang=lang, limit=limit)
        headers = {"Accept": "application/sparql-results+json", "User-Agent": user_agent}
        data = self._get(sparql_endpoint, params={"query": query}, headers=headers, timeout=30).json()
        bindings = data.get("results", {}).get("bindings", []) if isinstance(data, dict) else []

        results: list[dict[str, Any]] = []
        for binding in bindings:
            try:
                concept_uri = binding.get("conceptURI", {}).get("value")
                pref_label = binding.get("prefLabel", {}).get("value")
                description = binding.get("definitionSample", {}).get("value") or binding.get("scopeNoteSample", {}).get("value") or ""
                if concept_uri and pref_label:
                    results.append(
                        {
                            "label": pref_label,
                            "description": description,
                            "uri": concept_uri,
                            "source_provider": self.name,
                        }
                    )
            except Exception as exc:
                logger.exception("Error processing AGROVOC SPARQL binding: %s", exc)
        return results
