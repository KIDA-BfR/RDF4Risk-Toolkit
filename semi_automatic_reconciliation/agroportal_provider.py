# -*- coding: utf-8 -*-
"""AgroPortal reconciliation provider."""

try:
    from .bioportal_style_provider import BioPortalStyleProvider
except ImportError:  # pragma: no cover - direct script fallback
    from bioportal_style_provider import BioPortalStyleProvider

DEFAULT_AGROPORTAL_API_URL = "https://data.agroportal.lirmm.fr"
DEFAULT_INCLUDE_FIELDS = "prefLabel,synonym,definition"


class AgroPortalProvider(BioPortalStyleProvider):
    name = "AgroPortal"
    config_key = "agroportal"
    base_url = DEFAULT_AGROPORTAL_API_URL
    include_fields = DEFAULT_INCLUDE_FIELDS
