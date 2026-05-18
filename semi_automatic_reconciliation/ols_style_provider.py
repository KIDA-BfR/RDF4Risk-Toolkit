# -*- coding: utf-8 -*-
"""Shared implementation for OLS/Solr-compatible lookup APIs."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

try:
    from .base_provider import BaseProvider
    from .cache_utils import cache_data
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider
    from cache_utils import cache_data

logger = logging.getLogger(__name__)


class OlsStyleProvider(BaseProvider):
    base_url: str = ""
    config_key: str = ""
    search_path: str = "/search"
    ontologies_path: str = "/ontologies"
    query_fields = "label,synonym,description,short_form,obo_id,iri"
    field_list = "iri,label,synonym,description,ontology_prefix"
    sleep_time = 0.5

    def _search_url(self, api_url: str | None = None, base_url: str | None = None) -> str:
        if api_url:
            return api_url
        return f"{(base_url or self.base_url).rstrip('/')}{self.search_path}"

    def _ontologies_url(self, api_url: str | None = None, base_url: str | None = None) -> str:
        if api_url:
            return api_url
        return f"{(base_url or self.base_url).rstrip('/')}{self.ontologies_path}"

    def _base_params(
        self,
        term: str,
        limit: int,
        exact: bool = False,
        query_fields: str | None = None,
        field_list: str | None = None,
    ) -> dict[str, Any]:
        return {
            "q": term,
            "rows": limit,
            "exact": str(exact).lower(),
            "queryFields": query_fields or self.query_fields,
            "fieldList": field_list or self.field_list,
        }

    def _fetch(
        self,
        term: str,
        limit: int,
        user_agent: str,
        *,
        ontologies: Any = None,
        api_url: str | None = None,
        base_url: str | None = None,
        exact: bool = False,
        query_fields: str | None = None,
        field_list: str | None = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        params = self._base_params(term, limit, exact=exact, query_fields=query_fields, field_list=field_list)
        ontology_filter = self._format_ontologies(ontologies)
        if ontology_filter:
            params["ontology"] = ontology_filter
        headers = {"Accept": "application/json", "User-Agent": user_agent}
        logger.info("Querying %s: term=%r, ontologies=%r", self.name, term, params.get("ontology", "ALL"))
        data = self._get(self._search_url(api_url=api_url, base_url=base_url), params=params, headers=headers, timeout=30).json()
        docs = data.get("response", {}).get("docs", []) if isinstance(data, dict) else []

        suggestions: list[dict[str, Any]] = []
        for item in docs if isinstance(docs, list) else []:
            try:
                uri = item.get("iri")
                if not uri:
                    continue
                ontology = item.get("ontology_prefix") or item.get("ontology_name") or self.name
                suggestions.append(
                    {
                        "uri": uri,
                        "label": item.get("label", "N/A"),
                        "description": self._first_of(item.get("description", "")).strip(),
                        "source_provider": ontology,
                    }
                )
            except Exception as exc:
                logger.warning("Error parsing %s hit for %r: %s. Item: %s", self.name, term, exc, item, exc_info=True)
        return suggestions

    def get_available_ontologies(self, user_agent: str, *, api_url: str | None = None, base_url: str | None = None, **_: Any) -> List[str]:
        @cache_data(ttl=self.ontology_cache_ttl)
        def fetch_ontologies(fetch_url: str, fetch_user_agent: str) -> List[str]:
            data = self._get(fetch_url, headers={"Accept": "application/json", "User-Agent": fetch_user_agent}, timeout=30).json()
            ontologies = data.get("_embedded", {}).get("ontologies", []) if isinstance(data, dict) else []
            if not isinstance(ontologies, list):
                logger.warning("%s /ontologies returned unexpected data format: %s", self.name, type(data))
                return []
            return [str(item.get("ontologyId", "")).strip() for item in ontologies if item.get("ontologyId")]

        return fetch_ontologies(self._ontologies_url(api_url=api_url, base_url=base_url), user_agent)

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        provider_config = config.get(self.config_key, {})
        kwargs: dict[str, Any] = {}
        selected = self._selected_ontologies(config) or provider_config.get("ontologies")
        if selected:
            kwargs["ontologies"] = selected
        if provider_config.get("base_url"):
            kwargs["base_url"] = provider_config["base_url"]
        return kwargs

    def _format_ontologies(self, ontologies: Any) -> str:
        if isinstance(ontologies, str):
            return ontologies
        if isinstance(ontologies, (list, tuple, set)):
            return ",".join(str(item).strip() for item in ontologies if str(item).strip())
        return ""
