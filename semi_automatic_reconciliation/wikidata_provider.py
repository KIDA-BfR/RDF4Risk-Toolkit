# -*- coding: utf-8 -*-
"""Wikidata reconciliation provider."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

logger = logging.getLogger(__name__)

DEFAULT_WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"


class WikidataProvider(BaseProvider):
    name = "Wikidata"
    sleep_time = 0.05

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        api_url = config.get("wikidata", {}).get("api_url")
        return {"api_url": api_url} if api_url else {}

    def _fetch(
        self,
        term: str,
        limit: int,
        user_agent: str,
        *,
        api_url: str = DEFAULT_WIKIDATA_API_URL,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        params = {"action": "wbsearchentities", "search": term, "language": "en", "format": "json", "limit": limit}
        data = self._get(api_url, params=params, headers={"User-Agent": user_agent}, timeout=15).json()
        suggestions: list[dict[str, Any]] = []
        for result in data.get("search", []) if isinstance(data, dict) else []:
            entity_id = result.get("id")
            uri = result.get("concepturi") or (f"http://www.wikidata.org/entity/{entity_id}" if entity_id else None)
            if uri:
                suggestions.append(
                    {
                        "uri": uri,
                        "label": result.get("label", "N/A"),
                        "description": result.get("description", ""),
                        "source_provider": self.name,
                    }
                )
            else:
                logger.warning("Wikidata hit for %r skipped due to missing URI/ID: %s", term, result)
        return suggestions
