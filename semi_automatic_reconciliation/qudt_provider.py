# -*- coding: utf-8 -*-
"""QUDT reconciliation provider."""

from __future__ import annotations

import logging
import time
from typing import Any

from SPARQLWrapper import JSON, SPARQLExceptions, SPARQLWrapper

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

logger = logging.getLogger(__name__)

DEFAULT_QUDT_ENDPOINT = "https://qudt.org/fuseki/qudt/query"


class QudtProvider(BaseProvider):
    name = "QUDT"

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        return {"config": config}

    def _fetch(self, term: str, limit: int, user_agent: str, *, config: dict | None = None, **_: Any) -> list:
        endpoint_url = (config or {}).get("qudt", {}).get("endpoint", DEFAULT_QUDT_ENDPOINT)
        sanitized_term = term.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX qudt: <http://qudt.org/schema/qudt/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT DISTINCT ?uri ?label ?description WHERE {{
          {{
            ?uri a qudt:Unit ;
                 rdfs:label ?label .
            FILTER (CONTAINS(LCASE(STR(?label)), LCASE("{sanitized_term}")))
            OPTIONAL {{ ?uri rdfs:comment ?description . }}
          }} UNION {{
            ?uri a qudt:QuantityKind ;
                 rdfs:label ?label .
            FILTER (CONTAINS(LCASE(STR(?label)), LCASE("{sanitized_term}")))
            OPTIONAL {{ ?uri rdfs:comment ?description . }}
          }}
          FILTER (lang(?label) = 'en')
        }}
        LIMIT {limit}
        """

        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        sparql.agent = user_agent
        sparql.setTimeout(30)

        logger.info("Querying QUDT endpoint: %s for term %r", endpoint_url, term)
        start_time = time.time()
        try:
            results = sparql.query().convert()
        except SPARQLExceptions.EndPointNotFound as exc:
            raise ConnectionError(f"QUDT SPARQL Endpoint not found: {endpoint_url}") from exc
        except SPARQLExceptions.QueryBadFormed as exc:
            raise ValueError(f"Bad QUDT SPARQL Query. Check template/term syntax. Error: {exc}") from exc
        duration = time.time() - start_time
        bindings = results.get("results", {}).get("bindings", [])
        logger.info("QUDT query for %r took %.2fs, got %d bindings.", term, duration, len(bindings))

        results_list = []
        for result in bindings:
            uri = result.get("uri", {}).get("value")
            label = result.get("label", {}).get("value")
            description = result.get("description", {}).get("value", "")
            if uri and label:
                results_list.append(
                    {
                        "uri": uri,
                        "label": label,
                        "description": description,
                        "score": None,
                        "source_provider": self.name,
                    }
                )
            else:
                logger.warning("Missing URI or label in QUDT result binding: %s", result)
        return results_list[:limit]
