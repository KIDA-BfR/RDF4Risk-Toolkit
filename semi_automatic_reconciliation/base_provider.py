# -*- coding: utf-8 -*-
"""Shared provider infrastructure for semi-automatic reconciliation."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import requests

try:
    from .cache_utils import cache_data
except ImportError:  # pragma: no cover - direct script fallback
    from cache_utils import cache_data

logger = logging.getLogger(__name__)


class BaseProvider(ABC):
    """Base class for reconciliation providers."""

    name: str = ""
    query_cache_ttl: Optional[float] = 3600
    ontology_cache_ttl: Optional[float] = 3600 * 24
    sleep_time: float = 0.0

    def __init__(self) -> None:
        def cached_fetch(term: str, limit: int, user_agent: str, **kwargs: Any) -> List[Dict[str, Any]]:
            return self._fetch(term=term, limit=limit, user_agent=user_agent, **kwargs)

        if self.query_cache_ttl is None or self.query_cache_ttl < 0:
            self._cached_fetch = cached_fetch
        else:
            self._cached_fetch = cache_data(ttl=self.query_cache_ttl)(cached_fetch)

    def query(self, term: str, limit: int, user_agent: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """Return normalized suggestions for a term, handling common failures."""
        term = str(term or "").strip()
        if not term:
            logger.warning("%s query attempted with empty term.", self.name)
            return []
        try:
            return self._cached_fetch(term, int(limit), user_agent, **kwargs)
        except Exception as exc:
            return self._handle_error(term, exc)

    @abstractmethod
    def _fetch(self, term: str, limit: int, user_agent: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """Provider-specific implementation."""

    def get_available_ontologies(self, user_agent: str, **kwargs: Any) -> List[str]:
        """Return provider ontology acronyms/prefixes when supported."""
        return []

    @abstractmethod
    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        """Build provider-specific query kwargs from application config."""

    def build_ontology_kwargs(self, config: dict) -> dict:
        """Build kwargs for ontology-list endpoints."""
        kwargs = self.build_kwargs(config, 0)
        kwargs.pop("ontologies", None)
        return kwargs

    def _get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: float = 30,
    ) -> requests.Response:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        if self.sleep_time:
            logger.debug("%s request sent, sleeping for %.2fs", self.name, self.sleep_time)
            time.sleep(self.sleep_time)
        response.raise_for_status()
        return response

    def _handle_error(self, term: str, exc: Exception) -> List[Dict[str, Any]]:
        if isinstance(exc, ValueError):
            logger.warning("%s: Error parsing/validating response for %r: %s", self.name, term, exc, exc_info=True)
        elif isinstance(exc, requests.exceptions.HTTPError):
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", "N/A")
            reason = getattr(response, "reason", "")
            logger.warning("%s HTTP error for %r: %s %s", self.name, term, status, reason, exc_info=True)
            if response is not None:
                try:
                    logger.warning("%s server response excerpt: %s", self.name, str(response.json())[:500])
                except Exception:
                    logger.warning("%s server response excerpt: %s", self.name, getattr(response, "text", "")[:500])
        elif isinstance(exc, requests.exceptions.RequestException):
            logger.warning("%s network error for %r: %s", self.name, term, exc, exc_info=True)
        else:
            logger.exception("%s unexpected error for %r", self.name, term)
        return []

    def _first_of(self, value: Any) -> str:
        if isinstance(value, list):
            for item in value:
                if item:
                    return str(item)
            return ""
        if value is None:
            return ""
        return str(value)

    def _selected_ontologies(self, config: dict) -> list[str]:
        selected = config.get("selected_ontologies_by_provider", {}).get(self.name, [])
        if isinstance(selected, str):
            return [item.strip() for item in selected.split(",") if item.strip()]
        if isinstance(selected, (list, tuple, set)):
            return [str(item).strip() for item in selected if str(item).strip()]
        return []
