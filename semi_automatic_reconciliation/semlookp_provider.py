# -*- coding: utf-8 -*-
"""SemLookP reconciliation provider."""

from typing import Any

try:
    from .ols_style_provider import OlsStyleProvider
except ImportError:  # pragma: no cover - direct script fallback
    from ols_style_provider import OlsStyleProvider

DEFAULT_SEMLOOKP_SEARCH_URL = "https://semanticlookup.zbmed.de/ols/api/select"
DEFAULT_SEMLOOKP_ONTOLOGIES_URL = "https://semanticlookup.zbmed.de/ols/api/ontologies"


class SemLookPProvider(OlsStyleProvider):
    name = "SemLookP"
    config_key = "semlookp"
    base_url = "https://semanticlookup.zbmed.de/ols/api"
    search_path = "/select"
    field_list = "iri,label,description,ontology_name,ontology_prefix"
    sleep_time = 0.05

    def _base_params(
        self,
        term: str,
        limit: int,
        exact: bool = False,
        query_fields: str | None = None,
        field_list: str | None = None,
    ) -> dict[str, Any]:
        _ = exact, query_fields
        return {
            "q": term,
            "rows": limit,
            "fieldList": field_list or self.field_list,
            "type": "class,property,individual",
            "local": "false",
            "obsoletes": "false",
        }

    def build_kwargs(self, config: dict, num_suggestions: int) -> dict:
        kwargs = super().build_kwargs(config, num_suggestions)
        api_url = config.get("semlookp", {}).get("api_url")
        if api_url:
            kwargs["api_url"] = api_url
        return kwargs

    def build_ontology_kwargs(self, config: dict) -> dict:
        ontology_api_url = config.get("semlookp", {}).get("ontologies_api_url")
        return {"api_url": ontology_api_url} if ontology_api_url else {}
