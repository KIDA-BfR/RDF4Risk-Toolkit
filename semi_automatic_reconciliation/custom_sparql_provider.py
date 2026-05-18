# -*- coding: utf-8 -*-
"""Custom SPARQL reconciliation provider."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

from SPARQLWrapper import JSON, SPARQLExceptions, SPARQLWrapper

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

CUSTOM_SPARQL_PROVIDER_NAME = "Custom SPARQL"

logger = logging.getLogger(__name__)


class CustomSparqlProvider(BaseProvider):
    name = CUSTOM_SPARQL_PROVIDER_NAME

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        sparql_config = config.get("custom_sparql") or {}
        if not sparql_config.get("endpoint") or not sparql_config.get("query_template"):
            raise ValueError(f"{self.name} selected but not configured correctly (Endpoint/Query missing).")
        return {"config": config}

    def _fetch(self, term: str, limit: int, user_agent: str, *, config: dict, **_: Any) -> List[Dict[str, Any]]:
        sparql_config = config.get("custom_sparql") or {}
        endpoint_url = sparql_config.get("endpoint")
        query_template = sparql_config.get("query_template")
        var_uri = sparql_config.get("var_uri", "uri")
        var_label = sparql_config.get("var_label", "label")
        var_desc = sparql_config.get("var_description", "description")

        sanitized_term = term.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        query = query_template.format(term=sanitized_term, limit=limit)

        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        sparql.agent = user_agent
        sparql.setTimeout(30)

        logger.info("Querying custom SPARQL endpoint: %s for term %r", endpoint_url, term)
        start_time = time.time()
        try:
            results = sparql.query().convert()
        except SPARQLExceptions.EndPointNotFound as exc:
            raise ConnectionError(f"SPARQL Endpoint not found: {endpoint_url}") from exc
        except SPARQLExceptions.QueryBadFormed as exc:
            raise ValueError(f"Bad SPARQL Query. Check template/term syntax. Error: {exc}") from exc
        duration = time.time() - start_time
        bindings = results.get("results", {}).get("bindings", [])
        logger.info("Custom SPARQL query for %r took %.2fs, got %d bindings.", term, duration, len(bindings))

        suggestions: list[dict[str, Any]] = []
        for result in bindings:
            try:
                uri = result.get(var_uri, {}).get("value")
                label = result.get(var_label, {}).get("value")
                description = result.get(var_desc, {}).get("value") if var_desc else ""
                if uri and label:
                    suggestions.append(
                        {
                            "uri": uri,
                            "label": label,
                            "description": description or "",
                            "score": None,
                            "db": self.name,
                            "source_provider": self.name,
                        }
                    )
                else:
                    logger.warning("Missing expected custom SPARQL variables in binding: %s", result)
            except Exception as exc:
                logger.warning("Error parsing custom SPARQL binding: %s. Error: %s", result, exc)
        return suggestions[:limit]
