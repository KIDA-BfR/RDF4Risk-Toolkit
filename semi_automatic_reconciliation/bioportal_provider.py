# -*- coding: utf-8 -*-
"""BioPortal reconciliation provider."""

try:
    from .bioportal_style_provider import BioPortalStyleProvider
except ImportError:  # pragma: no cover - direct script fallback
    from bioportal_style_provider import BioPortalStyleProvider

DEFAULT_BIOPORTAL_API_URL = "https://data.bioontology.org"
DEFAULT_INCLUDE_FIELDS = "prefLabel,synonym,definition"


class BioPortalProvider(BioPortalStyleProvider):
    name = "BioPortal"
    config_key = "bioportal"
    base_url = DEFAULT_BIOPORTAL_API_URL
    include_fields = DEFAULT_INCLUDE_FIELDS
