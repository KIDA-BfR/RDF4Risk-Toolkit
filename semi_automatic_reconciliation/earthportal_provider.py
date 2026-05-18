# -*- coding: utf-8 -*-
"""EarthPortal reconciliation provider."""

try:
    from .bioportal_style_provider import BioPortalStyleProvider
except ImportError:  # pragma: no cover - direct script fallback
    from bioportal_style_provider import BioPortalStyleProvider

DEFAULT_EARTHPORTAL_API_URL = "https://data.earthportal.eu"
DEFAULT_INCLUDE_FIELDS = "prefLabel,synonym,definition"


class EarthPortalProvider(BioPortalStyleProvider):
    name = "EarthPortal"
    config_key = "earthportal"
    base_url = DEFAULT_EARTHPORTAL_API_URL
    include_fields = DEFAULT_INCLUDE_FIELDS
