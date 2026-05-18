# -*- coding: utf-8 -*-
"""NCBI E-utilities reconciliation provider."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

import requests

try:
    from .base_provider import BaseProvider
except ImportError:  # pragma: no cover - direct script fallback
    from base_provider import BaseProvider

logger = logging.getLogger(__name__)

EUTILS_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
ESEARCH_URL = EUTILS_BASE_URL + "esearch.fcgi"
ESUMMARY_URL = EUTILS_BASE_URL + "esummary.fcgi"
DEFAULT_NCBI_DATABASES = ["taxonomy", "bioproject", "gene", "protein", "nuccore", "biosample", "sra", "pubmed"]


def _construct_ncbi_uri(db, item_id):
    """Constructs a standard URL for an NCBI object."""
    if not item_id:
        return None
    if db == "taxonomy":
        return f"https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={item_id}"
    if db == "bioproject":
        return f"https://www.ncbi.nlm.nih.gov/bioproject/{item_id}"
    if db == "gene":
        return f"https://www.ncbi.nlm.nih.gov/gene/{item_id}"
    if db == "protein":
        return f"https://www.ncbi.nlm.nih.gov/protein/{item_id}"
    if db == "nuccore":
        return f"https://www.ncbi.nlm.nih.gov/nuccore/{item_id}"
    if db == "biosample":
        return f"https://www.ncbi.nlm.nih.gov/biosample/{item_id}"
    if db == "sra":
        return f"https://www.ncbi.nlm.nih.gov/sra?term={item_id}"
    if db == "pubmed":
        return f"https://pubmed.ncbi.nlm.nih.gov/{item_id}/"
    return f"https://www.ncbi.nlm.nih.gov/{db}/{item_id}"


class NcbiProvider(BaseProvider):
    name = "NCBI"

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        provider_config = config.get("ncbi", {})
        api_key = provider_config.get("api_key")
        if not api_key:
            raise ValueError("Required NCBI API Key not found in environment variables (.env / OS env).")
        return {
            "api_key": api_key,
            "databases_to_search": config.get("ncbi_databases") or DEFAULT_NCBI_DATABASES,
            "tool_name": provider_config.get("tool_name", "RDF4RiskReconTool"),
            "email": provider_config.get("email", "user@example.com"),
        }

    def _fetch(
        self,
        term: str,
        limit: int,
        user_agent: str,
        *,
        databases_to_search: list[str] | None = None,
        api_key: str | None = None,
        tool_name: str = "RDF4RiskReconTool",
        email: str = "user@example.com",
        **_: Any,
    ) -> List[Dict[str, Any]]:
        if not email or "@example.com" in email or email == "no-email-provided@example.com":
            logger.warning("NCBI Provider: A valid email address should be provided. Current: %s", email)

        base_params = {"tool": tool_name, "email": email}
        sleep_time = 0.34
        if api_key:
            base_params["api_key"] = api_key
            sleep_time = 0.11
        else:
            logger.warning("NCBI Provider: No NCBI API Key provided. Rate limit restricted to 3 requests/second.")

        headers = {"User-Agent": user_agent}
        all_suggestions: list[dict[str, Any]] = []

        for db in databases_to_search or DEFAULT_NCBI_DATABASES:
            try:
                search_ids = self._search_ids(term, db, limit, base_params, headers, sleep_time)
                if search_ids:
                    all_suggestions.extend(self._summarize_ids(term, db, search_ids, base_params, headers, sleep_time))
            except Exception as exc:
                logger.warning("NCBI error for %r in database %s: %s", term, db, exc, exc_info=True)
                continue
        logger.info("NCBI query for %r finished across all specified databases.", term)
        return all_suggestions

    def _search_ids(self, term: str, db: str, limit: int, base_params: dict, headers: dict, sleep_time: float) -> list[str]:
        params = dict(base_params)
        params.update({"db": db, "term": term, "retmax": limit, "retmode": "json"})
        response = requests.get(ESEARCH_URL, params=params, headers=headers, timeout=20)
        time.sleep(sleep_time)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            logger.warning("NCBI ESearch (%s) API error for %r: %s", db, term, data.get("error"))
            return []
        result = data.get("esearchresult", {})
        return result.get("idlist", []) if isinstance(result, dict) else []

    def _summarize_ids(
        self,
        term: str,
        db: str,
        search_ids: list[str],
        base_params: dict,
        headers: dict,
        sleep_time: float,
    ) -> list[dict[str, Any]]:
        payload = dict(base_params)
        payload.update({"db": db, "id": ",".join(search_ids), "retmode": "json", "version": "2.0"})
        response = requests.post(ESUMMARY_URL, data=payload, headers=headers, timeout=30)
        time.sleep(sleep_time)
        response.raise_for_status()
        data = response.json()
        result_dict = data.get("result", {}) if isinstance(data, dict) else {}
        processed_uids = result_dict.get("uids", []) if isinstance(result_dict, dict) else []

        suggestions: list[dict[str, Any]] = []
        for uid in processed_uids:
            item = result_dict.get(uid)
            if not isinstance(item, dict):
                continue
            try:
                suggestion = self._parse_summary_item(db, uid, item)
                if suggestion:
                    suggestions.append(suggestion)
            except Exception as exc:
                logger.warning("NCBI Provider: Error parsing ESummary item (%s, UID: %s) for %r: %s", db, uid, term, exc, exc_info=True)
        return suggestions

    def _parse_summary_item(self, db: str, uid: str, item: dict[str, Any]) -> dict[str, Any] | None:
        label = f"Unknown {db} {uid}"
        description = ""
        item_id_for_uri = uid

        if db == "taxonomy":
            label = item.get("scientificname", label)
            description = item.get("rank", "")
        elif db == "bioproject":
            item_id_for_uri = item.get("project_acc") or uid
            label = item.get("name", item.get("project_title", label))
            description = item.get("project_description", "")
        elif db == "gene":
            label = item.get("name", label)
            description = item.get("description", item.get("summary", ""))
        elif db in {"protein", "nuccore"}:
            label = item.get("title", label)
            description = item.get("organism", "")
        elif db == "biosample":
            label = item.get("accession", label)
            item_id_for_uri = item.get("accession", uid)
            description = item.get("description", "")
        elif db == "sra":
            label = item.get("title", label)
            description = item.get("description", "")
        elif db == "pubmed":
            label = item.get("title", label)
            authors = item.get("authors") or []
            description = authors[0].get("name", "") if authors else ""
            description += f" ({item.get('pubdate', '')})" if item.get("pubdate") else ""

        uri = _construct_ncbi_uri(db, item_id_for_uri)
        if not uri:
            return None
        return {
            "db": db,
            "uri": uri,
            "label": label,
            "description": description,
            "id": item_id_for_uri,
            "source_provider": f"NCBI {db}",
        }
