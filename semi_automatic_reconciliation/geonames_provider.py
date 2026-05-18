# -*- coding: utf-8 -*-
"""GeoNames reconciliation provider."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from urllib.parse import urljoin

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

logger = logging.getLogger(__name__)

DEFAULT_GEONAMES_BASE_URL = "https://secure.geonames.org"


class GeoNamesProvider(BaseProvider):
    name = "GeoNames"

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        provider_config = config.get("geonames", {})
        username = provider_config.get("username")
        if not username or username == "YourUsername":
            raise ValueError("Required GeoNames username not found in config.")
        return {
            "username": username,
            "base_url": provider_config.get("base_url", DEFAULT_GEONAMES_BASE_URL),
        }

    def _fetch(
        self,
        term: str,
        limit: int,
        user_agent: str,
        *,
        username: str,
        base_url: str = DEFAULT_GEONAMES_BASE_URL,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        full_api_url = urljoin(base_url.strip("/") + "/", "searchJSON")
        params = {"q": term, "maxRows": limit, "username": username, "style": "full"}
        data = self._get(full_api_url, params=params, headers={"User-Agent": user_agent}, timeout=15).json()
        if isinstance(data, dict) and isinstance(data.get("status"), dict):
            status = data["status"]
            logger.error("GeoNames API error for %r: %s (Code: %s)", term, status.get("message"), status.get("value"))
            return []
        geonames_results = data.get("geonames", []) if isinstance(data, dict) else []

        results: list[dict[str, Any]] = []
        for item in geonames_results if isinstance(geonames_results, list) else []:
            try:
                geoname_id = item.get("geonameId")
                label = item.get("toponymName") or item.get("name")
                if not geoname_id or not label:
                    continue
                desc_parts = [item.get("fcodeName"), item.get("adminName1"), item.get("countryName")]
                results.append(
                    {
                        "label": label,
                        "description": ", ".join(filter(None, desc_parts)),
                        "uri": f"http://sws.geonames.org/{geoname_id}/",
                        "source_provider": self.name,
                    }
                )
            except Exception as exc:
                logger.exception("Error processing GeoNames item %s: %s", item.get("geonameId", "UNKNOWN_ID"), exc)
        return results
