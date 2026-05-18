# -*- coding: utf-8 -*-
"""EBI OLS reconciliation provider."""

try:
    from .ols_style_provider import OlsStyleProvider
except ImportError:  # pragma: no cover - direct script fallback
    from ols_style_provider import OlsStyleProvider

DEFAULT_OLS_API_URL = "https://www.ebi.ac.uk/ols4/api"
DEFAULT_QUERY_FIELDS = "label,synonym,description,short_form,obo_id,iri"
DEFAULT_INCLUDE_FIELDS = "iri,label,synonym,description,ontology_prefix"


class OlsProvider(OlsStyleProvider):
    name = "OLS (EBI)"
    config_key = "ols"
    base_url = DEFAULT_OLS_API_URL
    query_fields = DEFAULT_QUERY_FIELDS
    field_list = DEFAULT_INCLUDE_FIELDS
