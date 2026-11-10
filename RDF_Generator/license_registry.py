"""Data licences for the DCAT catalog, from the DCAT-AP.de licence vocabulary.

The list lives in ``data/licenses.json`` (the single source): common data licences with their
DCAT-AP.de URI (``http://dcat-ap.de/def/licenses/...``), which the catalog writes as
``dct:license``. Each entry also carries its SPDX ID, so values saved by older versions of the app
("CC BY 4.0", "CC-BY-4.0", "CC0 1.0") still resolve.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)

LICENSES_PATH = Path(__file__).resolve().parent / "data" / "licenses.json"
DEFAULT_LICENSE_ID = "cc-by/4.0"

# Names older versions of the app stored instead of a DCAT-AP.de licence ID.
LEGACY_NAMES = {"CC BY 4.0": "cc-by/4.0", "CC0 1.0": "cc-zero"}


@lru_cache(maxsize=4)
def _load(path: str) -> tuple:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        licenses = data.get("licenses") if isinstance(data, dict) else None
        if isinstance(licenses, list):
            return tuple(lic for lic in licenses if isinstance(lic, dict) and lic.get("id") and lic.get("uri"))
        LOGGER.error("Licence file %s has no 'licenses' list.", path)
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.error("Could not read licence file %s: %s", path, exc)
    return ()


def list_licenses(path: Path = LICENSES_PATH) -> List[Dict[str, Any]]:
    return [dict(lic) for lic in _load(str(path))]


def resolve(license_id: Optional[str], path: Path = LICENSES_PATH) -> Optional[Dict[str, Any]]:
    """Licence record for a DCAT-AP.de ID, an SPDX ID or a legacy name; None if unknown or empty."""
    if not license_id:
        return None
    key = str(license_id).strip()
    key = LEGACY_NAMES.get(key, key)
    licenses = list_licenses(path)
    return next((lic for lic in licenses if lic["id"] == key), None) or next((lic for lic in licenses if lic.get("spdx") == key), None)


def normalize_id(license_id: Optional[str], path: Path = LICENSES_PATH) -> str:
    """Stored licence ID for a saved value; falls back to the default licence."""
    lic = resolve(license_id, path)
    return lic["id"] if lic else DEFAULT_LICENSE_ID
