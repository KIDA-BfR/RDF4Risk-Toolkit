# -*- coding: utf-8 -*-
"""Shared implementation for BioPortal-compatible APIs."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from urllib.parse import urlparse

try:
    from .base_provider import BaseProvider
    from .cache_utils import cache_data
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider
    from cache_utils import cache_data

logger = logging.getLogger(__name__)


class BioPortalStyleProvider(BaseProvider):
    base_url: str = ""
    config_key: str = ""
    include_fields: str = "prefLabel,synonym,definition"
    sleep_time = 1.0

    def _headers(self, user_agent: str, api_key: str) -> dict[str, str]:
        return {
            "Authorization": f"apikey token={api_key}",
            "Accept": "application/json",
            "User-Agent": user_agent,
        }

    def _fetch(
        self,
        term: str,
        limit: int,
        user_agent: str,
        *,
        api_key: str,
        ontologies: Any = None,
        base_url: str | None = None,
        include_fields: str | None = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        params = {
            "q": term,
            "include": include_fields or self.include_fields,
            "page": 1,
            "pagesize": limit,
            "display_context": "false",
            "display_links": "true",
        }
        ontology_filter = self._format_ontologies(ontologies)
        if ontology_filter:
            params["ontologies"] = ontology_filter

        url = f"{(base_url or self.base_url).rstrip('/')}/search"
        logger.info("Querying %s: term=%r, ontologies=%r", self.name, term, params.get("ontologies", "ALL"))
        data = self._get(url, params=params, headers=self._headers(user_agent, api_key), timeout=30).json()

        suggestions: list[dict[str, Any]] = []
        for item in data.get("collection", []) if isinstance(data, dict) else []:
            try:
                uri = item.get("@id")
                if not uri:
                    continue
                ontology_link = item.get("links", {}).get("ontology")
                ontology = "Unknown"
                if ontology_link:
                    ontology = urlparse(ontology_link).path.split("/")[-1] or ontology
                suggestions.append(
                    {
                        "uri": uri,
                        "label": item.get("prefLabel", "N/A"),
                        "description": self._first_of(item.get("definition", "")).strip(),
                        "source_provider": ontology or self.name,
                    }
                )
            except Exception as exc:
                logger.warning("Error parsing %s hit for %r: %s. Item: %s", self.name, term, exc, item, exc_info=True)
        return suggestions

    def get_available_ontologies(self, user_agent: str, *, api_key: str | None = None, base_url: str | None = None, **_: Any) -> List[str]:
        if not api_key:
            logger.error("%s API key is required to fetch available ontologies.", self.name)
            return []

        @cache_data(ttl=self.ontology_cache_ttl)
        def fetch_ontologies(fetch_base_url: str, fetch_api_key: str, fetch_user_agent: str) -> List[str]:
            url = f"{fetch_base_url.rstrip('/')}/ontologies"
            data = self._get(url, headers=self._headers(fetch_user_agent, fetch_api_key), timeout=30).json()
            if not isinstance(data, list):
                logger.warning("%s /ontologies returned unexpected data format: %s", self.name, type(data))
                return []
            return [str(item.get("acronym", "")).strip() for item in data if item.get("acronym")]

        return fetch_ontologies(base_url or self.base_url, api_key, user_agent)

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        provider_config = config.get(self.config_key, {})
        api_key = provider_config.get("api_key")
        if not api_key:
            raise ValueError(f"Required {self.name} API key not found in environment variables (.env / OS env).")
        kwargs: dict[str, Any] = {"api_key": api_key}
        selected = self._selected_ontologies(config)
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
