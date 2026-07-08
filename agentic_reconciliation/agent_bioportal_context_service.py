# -*- coding: utf-8 -*-
"""Compact BioPortal ontology context enrichment for agent decisions."""

from __future__ import annotations

import time
import re
import json
import logging
import os
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from threading import Lock, RLock
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote

import requests

from .agent_bioportal_service import DEFAULT_BIOPORTAL_BASE_URL
from .agent_models import AgentCandidate


_CLASS_CONTEXT_CACHE: Dict[tuple, Dict[str, Any]] = {}
_ONTOLOGY_META_CACHE: Dict[tuple, tuple[float, Dict[str, Any]]] = {}
_CACHE_LOCK = Lock()
_REQUEST_LOCK = Lock()
_LAST_REQUEST_AT = 0.0
_META_TTL_SECONDS = 24 * 60 * 60
_MIN_REQUEST_INTERVAL_SECONDS = 0.08
DEFAULT_REGISTRY_TTL_DAYS = 90
REGISTRY_PATH = Path(__file__).resolve().parent / "config" / "bioportal_ontology_registry.json"

_LOGGER = logging.getLogger(__name__)

# Curated, project-relevant BioPortal ontology set for registry bootstrap. This is
# an explicit allow-list — the bootstrap NEVER fetches the full BioPortal universe.
PROJECT_RELEVANT_BIOPORTAL_ONTOLOGIES = [
    "FOODON", "CHEBI", "NCIT", "SNOMEDCT", "MESH", "GENEPIO", "BERO", "QUDT",
    "QUDT2", "NIFSTD", "OCHV", "MEDDRA", "RCD", "EDAM", "OBI", "ENVO",
    "AGROVOC", "NCBITAXON",
]

# Lightweight stopwords for the deterministic ontology domain-fit score.
_DOMAIN_FIT_STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "are", "was", "were",
    "used", "use", "uses", "using", "such", "into", "out", "via", "per", "any",
    "all", "one", "two", "its", "their", "them", "they", "which", "while", "also",
    "may", "can", "not", "but", "has", "have", "had", "more", "most", "other",
    "ontology", "ontologies", "term", "terms", "concept", "concepts", "class",
    "classes", "definition", "obtained", "based", "related", "general",
}


ONTOLOGY_NOTES: Dict[str, str] = {
    "MESH": "BioPortal MeSH is UMLS-derived; verify MeSH tree numbers when hierarchy-sensitive.",
    "SNOMEDCT": "SNOMED CT is a clinical terminology; use cautiously for general food/material concepts unless the term is explicitly clinical or substance-related.",
    "MEDDRA": "MedDRA is regulatory adverse event terminology; poor fit for general food/material/product terms unless health effect or adverse event context is intended.",
    "FOODON": "FoodOn is strong for food products, food sources, food processing, food packaging, and food systems.",
    "CHEBI": "ChEBI is strong for chemical entities, molecular structures, and chemical roles; less suitable for food product categories.",
    "GENEPIO": "GENEPIO is an application ontology for genomic epidemiology and foodborne infectious disease surveillance; use cautiously for general packaging terms.",
}


@dataclass
class BioPortalOntologySignature:
    acronym: str
    name: str = ""
    description: str = ""
    abstract: str = ""
    keywords: List[str] = field(default_factory=list)
    categories_domains: List[str] = field(default_factory=list)
    groups: List[str] = field(default_factory=list)
    ontology_type: str = ""
    homepage: str = ""
    documentation: str = ""
    hierarchy_property: str = ""
    root_labels_sample: List[str] = field(default_factory=list)
    purpose_summary_auto: str = ""
    metadata_status: str = "not_loaded"
    source_endpoints_used: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BioPortalClassContext:
    candidate_label: str
    candidate_uri: str
    ontology_acronym: str
    definition: str = ""
    synonyms: List[str] = field(default_factory=list)
    notation: str = ""
    cui: str = ""
    semantic_type: List[str] = field(default_factory=list)
    obsolete: Optional[bool] = None
    path_to_root: List[str] = field(default_factory=list)
    direct_parents: List[Dict[str, str]] = field(default_factory=list)
    sibling_classes_from_ontology: List[Dict[str, str]] = field(default_factory=list)
    direct_children: List[Dict[str, str]] = field(default_factory=list)
    children_state: str = "not_checked"
    hierarchy_quality: str = ""
    hierarchy_status: str = "not_loaded"
    source_endpoints_used: List[str] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _normalize_acronym(value: Any) -> str:
    return _clean_text(value, 80).upper()


class BioPortalOntologyRegistry:
    """Persistent, auto-growing registry for BioPortal ontology signatures."""

    def __init__(
        self,
        path: Optional[Path] = None,
        *,
        ttl_days: int = DEFAULT_REGISTRY_TTL_DAYS,
        strict: bool = False,
    ) -> None:
        self.path = Path(path) if path is not None else REGISTRY_PATH
        self.ttl_days = max(1, int(ttl_days or DEFAULT_REGISTRY_TTL_DAYS))
        self.strict = bool(strict)
        self._entries: Optional[Dict[str, Dict[str, Any]]] = None
        # Reentrant so save() can call load() while holding the lock, and so the
        # per-term ThreadPoolExecutor can safely serialize concurrent writes.
        self._lock = RLock()

    def load(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            if self._entries is not None:
                return self._entries
            if not self.path.exists():
                self._entries = {}
                return self._entries
            try:
                parsed = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(parsed, dict):
                    raise ValueError("registry root is not a JSON object")
                self._entries = {
                    _normalize_acronym(key): value
                    for key, value in parsed.items()
                    if _normalize_acronym(key) and isinstance(value, dict)
                }
            except Exception as exc:
                if self.strict:
                    raise
                backup = self.path.with_suffix(self.path.suffix + f".malformed-{int(time.time())}.bak")
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(self.path, backup)
                except Exception:
                    pass
                self._entries = {
                    "_registry_warning": {
                        "status": "malformed_registry_ignored",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                }
            return self._entries

    def save(self) -> None:
        # The whole read-serialize-write-replace sequence runs under the lock so
        # concurrent writers (the per-term ThreadPoolExecutor) can never race on
        # the temp file or observe a half-mutated entries dict. The temp file is
        # additionally made unique per write so two processes sharing this path
        # can never target the same scratch file either.
        with self._lock:
            entries = self.load()
            serializable = {
                key: value
                for key, value in dict(entries).items()
                if key != "_registry_warning" and isinstance(value, dict)
            }
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_name(
                f"{self.path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex[:12]}"
            )
            try:
                tmp_path.write_text(
                    json.dumps(serializable, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                os.replace(tmp_path, self.path)
            except Exception:
                # Never leave an orphaned scratch file behind on failure.
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
                raise

    def _save_best_effort(self) -> None:
        """Persist the registry, logging (not raising) on failure.

        The registry is a cache: a failed write must never abort the caller.
        On the per-term reconciliation path a persistence hiccup used to bubble
        up as a ``FileNotFoundError`` and cause the whole term to be skipped,
        even though the in-memory entry it returned was perfectly valid.
        """
        try:
            self.save()
        except Exception as exc:  # noqa: BLE001 - persistence is best-effort
            _LOGGER.warning("BioPortal ontology registry save failed (continuing): %s", exc)

    def get(self, acronym: str) -> Optional[Dict[str, Any]]:
        return self.load().get(_normalize_acronym(acronym))

    def merge_static_curated_notes(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        acronym = _normalize_acronym(entry.get("acronym"))
        if acronym in ONTOLOGY_NOTES and not str(entry.get("curated_note", "")).strip():
            entry["curated_note"] = ONTOLOGY_NOTES[acronym]
            entry["curated_note_source"] = "static_config"
        elif "curated_note" not in entry:
            entry["curated_note"] = ""
            entry["curated_note_source"] = ""
        return entry

    def is_stale(self, entry: Dict[str, Any]) -> bool:
        refreshed = _parse_iso_datetime(entry.get("last_refreshed"))
        if refreshed is None:
            return True
        age_days = (datetime.now(timezone.utc) - refreshed).days
        return age_days >= self.ttl_days

    def ensure(
        self,
        acronym: str,
        bioportal_client: Callable[[str], Dict[str, Any]],
    ) -> Dict[str, Any]:
        acronym_norm = _normalize_acronym(acronym)
        if not acronym_norm:
            return {"acronym": "", "status": "missing_acronym"}
        with self._lock:
            entries = self.load()
            existing = entries.get(acronym_norm)
            if existing and not self.is_stale(existing):
                existing["last_seen"] = _utc_now_iso()
                self.merge_static_curated_notes(existing)
                self._save_best_effort()
                return dict(existing)
        return self.refresh(acronym_norm, bioportal_client, previous=existing)

    def _build_entry(
        self,
        acronym: str,
        bioportal_client: Callable[[str], Dict[str, Any]],
        *,
        previous: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fetch one ontology's metadata and build a registry entry (no persistence).

        Curated notes from a previous entry are always preserved. On failure a
        ``status=lookup_failed`` entry is returned rather than raising, so a
        single bad ontology never aborts a batch bootstrap.
        """
        acronym_norm = _normalize_acronym(acronym)
        now = _utc_now_iso()
        previous = previous if isinstance(previous, dict) else {}
        try:
            fetched = bioportal_client(acronym_norm)
            entry = _registry_entry_from_meta(fetched, now=now)
            entry["first_seen"] = previous.get("first_seen") or now
            entry["last_seen"] = now
            entry["last_refreshed"] = now
            entry["curated_note"] = previous.get("curated_note", "")
            entry["curated_note_source"] = previous.get("curated_note_source", "")
            entry = self.merge_static_curated_notes(entry)
        except Exception as exc:
            entry = {
                "acronym": acronym_norm,
                "name": previous.get("name", acronym_norm),
                "metadata_source": "BioPortal",
                "first_seen": previous.get("first_seen") or now,
                "last_seen": now,
                "last_refreshed": previous.get("last_refreshed", ""),
                "api_endpoints_used": previous.get("api_endpoints_used", []),
                "status": "lookup_failed",
                "error": f"{type(exc).__name__}: {exc}",
                "curated_note": previous.get("curated_note", ""),
                "curated_note_source": previous.get("curated_note_source", ""),
            }
            entry = self.merge_static_curated_notes(entry)
        return entry

    def refresh(
        self,
        acronym: str,
        bioportal_client: Callable[[str], Dict[str, Any]],
        *,
        previous: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        acronym_norm = _normalize_acronym(acronym)
        # Network fetch stays outside the lock so a slow BioPortal call never
        # blocks other reconciliation threads.
        entry = self._build_entry(acronym_norm, bioportal_client, previous=previous)
        with self._lock:
            entries = self.load()
            entries[acronym_norm] = entry
            self._save_best_effort()
        return dict(entry)

    def ensure_many(
        self,
        acronyms: List[str],
        bioportal_client: Callable[[str], Dict[str, Any]],
        *,
        refresh_stale: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """Ensure context for several ontologies, fetching only missing/stale ones.

        Fresh entries are returned from the local registry with no BioPortal call.
        Stale entries are refreshed only when ``refresh_stale`` is True; otherwise
        the existing (stale) entry is returned for graceful degradation.
        """
        entries = self.load()
        out: Dict[str, Dict[str, Any]] = {}
        changed = False
        now = _utc_now_iso()
        for raw in acronyms or []:
            norm = _normalize_acronym(raw)
            if not norm:
                continue
            existing = entries.get(norm)
            if existing and not self.is_stale(existing):
                existing["last_seen"] = now
                self.merge_static_curated_notes(existing)
                out[norm] = dict(existing)
                changed = True
            elif existing and self.is_stale(existing) and not refresh_stale:
                out[norm] = dict(existing)
            else:
                entry = self._build_entry(norm, bioportal_client, previous=existing or {})
                entries[norm] = entry
                out[norm] = dict(entry)
                changed = True
        if changed:
            self.save()
        return out

    def bootstrap(
        self,
        acronyms: List[str],
        bioportal_client: Callable[[str], Dict[str, Any]],
        *,
        refresh: bool = False,
        dry_run: bool = False,
        max_requests: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Pre-fill the registry for a list of ontology acronyms.

        Only missing entries (and, when ``refresh`` is set, stale entries) are
        fetched from BioPortal; fresh entries are never re-fetched. ``dry_run``
        reports what would be fetched without any BioPortal call or persistence.
        ``max_requests`` caps the number of live BioPortal fetches as a safety
        valve so a bootstrap can never flood the API.
        """
        requested: List[str] = []
        seen = set()
        for raw in acronyms or []:
            norm = _normalize_acronym(raw)
            if norm and norm not in seen:
                seen.add(norm)
                requested.append(norm)

        entries = self.load()
        present_at_start = {a for a in requested if isinstance(entries.get(a), dict)}
        budget = None
        try:
            if max_requests not in (None, "", 0):
                budget = max(0, int(max_requests))
        except Exception:
            budget = None

        now = _utc_now_iso()
        fetched_list: List[str] = []
        refreshed_list: List[str] = []
        failed_list: List[str] = []
        skipped_list: List[str] = []
        missing_list: List[str] = []
        planned_list: List[str] = []
        budget_skipped_list: List[str] = []
        fetches_used = 0
        changed = False

        for acr in requested:
            existing = entries.get(acr)
            is_missing = existing is None
            is_stale = bool(existing) and self.is_stale(existing)
            if is_missing:
                missing_list.append(acr)
            needs_fetch = is_missing or (is_stale and refresh)

            if not needs_fetch:
                if not dry_run and isinstance(existing, dict):
                    existing["last_seen"] = now
                    self.merge_static_curated_notes(existing)
                    changed = True
                skipped_list.append(acr)
                continue

            if dry_run:
                planned_list.append(acr)
                continue

            if budget is not None and fetches_used >= budget:
                budget_skipped_list.append(acr)
                continue

            entry = self._build_entry(acr, bioportal_client, previous=existing or {})
            fetches_used += 1
            entries[acr] = entry
            changed = True
            if str(entry.get("status", "")).strip().lower() == "lookup_failed":
                failed_list.append(acr)
            elif is_missing:
                fetched_list.append(acr)
            else:
                refreshed_list.append(acr)

        if changed and not dry_run:
            self.save()

        entry_count = sum(1 for k, v in entries.items() if k != "_registry_warning" and isinstance(v, dict))
        summary = {
            "registry_path": str(self.path),
            "dry_run": bool(dry_run),
            "refresh": bool(refresh),
            "max_requests": budget,
            "requested_count": len(requested),
            "existing_count": len(present_at_start),
            "missing_count": len(missing_list),
            "fetched_count": len(fetched_list),
            "refreshed_count": len(refreshed_list),
            "failed_count": len(failed_list),
            "skipped_count": len(skipped_list) + len(budget_skipped_list),
            "planned_fetch_count": len(planned_list),
            "budget_skipped_count": len(budget_skipped_list),
            "registry_entry_count": entry_count,
            "requested": requested,
            "missing": missing_list,
            "fetched": fetched_list,
            "refreshed": refreshed_list,
            "failed": failed_list,
            "skipped": skipped_list,
            "budget_skipped": budget_skipped_list,
            "planned_fetch": planned_list,
        }
        _LOGGER.info(
            "bioportal_registry.bootstrap requested=%d existing=%d missing=%d fetched=%d refreshed=%d failed=%d skipped=%d dry_run=%s",
            summary["requested_count"], summary["existing_count"], summary["missing_count"],
            summary["fetched_count"], summary["refreshed_count"], summary["failed_count"],
            summary["skipped_count"], summary["dry_run"],
        )
        return summary


def _clean_text(value: Any, limit: int = 240) -> str:
    if isinstance(value, list):
        value = next((item for item in value if str(item or "").strip()), "")
    text = unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = " ".join(text.split())
    if limit > 0 and len(text) > limit:
        return text[: max(0, limit - 1)].rstrip() + "..."
    return text


def _class_label(item: Dict[str, Any]) -> str:
    return _clean_text(item.get("prefLabel") or item.get("label") or item.get("@id") or "", 120)


def _compact_class(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    label = _class_label(item)
    uri = _clean_text(item.get("@id") or item.get("id") or "", 240)
    if not label and not uri:
        return None
    compact = {"label": label or uri, "uri": uri}
    links = item.get("links") if isinstance(item, dict) else None
    if isinstance(links, dict):
        compact["links"] = links
    return compact


def _collection(payload: Any) -> List[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        collection = payload.get("collection")
        if isinstance(collection, list):
            return collection
    return []


def _first_link(links: Any, *names: str) -> str:
    if not isinstance(links, dict):
        return ""
    for name in names:
        value = links.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _retry_delay(response: Optional[requests.Response], attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        try:
            if retry_after:
                return max(0.0, min(8.0, float(retry_after)))
        except ValueError:
            pass
    return min(8.0, 0.5 * (2 ** attempt))


def _http_get_json(url: str, params: Dict[str, Any], *, timeout: int = 20) -> Any:
    global _LAST_REQUEST_AT
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            with _REQUEST_LOCK:
                elapsed = time.monotonic() - _LAST_REQUEST_AT
                if elapsed < _MIN_REQUEST_INTERVAL_SECONDS:
                    time.sleep(_MIN_REQUEST_INTERVAL_SECONDS - elapsed)
                _LAST_REQUEST_AT = time.monotonic()
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code in {429, 503}:
                time.sleep(_retry_delay(response, attempt))
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(_retry_delay(getattr(exc, "response", None), attempt))
                continue
    if last_exc:
        raise last_exc
    return {}


def _http_error_label(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None) if response is not None else None
    if status_code:
        return f"HTTP {status_code}"
    return type(exc).__name__


def _base_params(api_key: str) -> Dict[str, Any]:
    return {
        "apikey": api_key,
        "display_context": "false",
        "display_links": "false",
    }


def _fetch_ontology_meta_from_bioportal(acronym: str, api_key: str, *, base_url: str) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "acronym": acronym.upper(),
        "name": acronym.upper(),
        "domain": [],
        "categories": [],
        "groups": [],
        "description": "",
        "abstract": "",
        "keywords": [],
        "coverage": "",
        "homepage": "",
        "documentation": "",
        "ontology_type": "",
        "hierarchy_property": "",
        "root_labels_sample": [],
        "purpose_summary_auto": "",
        "metadata_status": "not_loaded",
        "source_endpoints_used": [],
        # Compact, decision-relevant QUALITY evidence — kept SEPARATE from the
        # capability profile and never sent to the LLM routing prompt.
        "quality_raw": {},
        "enriched": False,
    }
    params = _base_params(api_key)
    try:
        ontology_payload = _http_get_json(f"{base_url.rstrip('/')}/ontologies/{acronym}", params)
        meta["source_endpoints_used"].append(f"/ontologies/{acronym}")
        if isinstance(ontology_payload, dict):
            meta["name"] = _clean_text(
                ontology_payload.get("name")
                or ontology_payload.get("acronym")
                or acronym.upper(),
                120,
            )
            groups = ontology_payload.get("group") or []
            if isinstance(groups, list) and groups:
                meta["groups"] = [_clean_text(item, 80) for item in groups if _clean_text(item, 80)]
                meta["domain"] = list(meta["groups"])
            meta["ontology_type"] = _clean_text(ontology_payload.get("ontologyType"), 80)
            qr = meta["quality_raw"]
            for src, dst in (("viewOf", "viewOf"), ("flat", "flat"), ("summaryOnly", "summaryOnly"),
                             ("doNotUpdate", "doNotUpdate"), ("viewingRestriction", "viewingRestriction")):
                value = ontology_payload.get(src)
                if value is not None:
                    qr[dst] = value if isinstance(value, bool) else _clean_text(value, 120)

        submission_params = {
            **params,
            "include": (
                "description,abstract,keywords,coverage,hasDomain,homepage,documentation,status,"
                "hasOntologyLanguage,hierarchyProperty,definitionProperty,synonymProperty,obsoleteProperty,"
                "prefLabelProperty,submissionStatus,missingImports,released,creationDate,modificationDate,"
                "valid,deprecated,wasInvalidatedBy,example,keyClasses,knownUsage,usedBy,designedForOntologyTask"
            ),
        }
        submission_payload = _http_get_json(
            f"{base_url.rstrip('/')}/ontologies/{acronym}/latest_submission",
            submission_params,
        )
        meta["source_endpoints_used"].append(f"/ontologies/{acronym}/latest_submission")
        if isinstance(submission_payload, dict):
            meta["description"] = _clean_text(submission_payload.get("description"), 300)
            meta["abstract"] = _clean_text(submission_payload.get("abstract"), 300)
            meta["coverage"] = _clean_text(submission_payload.get("coverage"), 120)
            meta["homepage"] = _clean_text(submission_payload.get("homepage"), 200)
            meta["documentation"] = _clean_text(submission_payload.get("documentation"), 200)
            meta["hierarchy_property"] = _clean_text(submission_payload.get("hierarchyProperty"), 120)
            keywords = submission_payload.get("keywords") or []
            if isinstance(keywords, str):
                keywords = [keywords]
            if isinstance(keywords, list):
                meta["keywords"] = [_clean_text(item, 60) for item in keywords if _clean_text(item, 60)][:8]
            domains = submission_payload.get("hasDomain") or submission_payload.get("domain") or []
            if isinstance(domains, list):
                domain_labels = []
                for item in domains:
                    if isinstance(item, dict):
                        label = _clean_text(item.get("name") or item.get("prefLabel") or item.get("@id"), 80)
                    else:
                        label = _clean_text(item, 80)
                    if label:
                        domain_labels.append(label)
                if domain_labels:
                    meta["domain"] = domain_labels[:5]

            qr = meta["quality_raw"]
            for key in ("valid", "deprecated", "missingImports"):
                value = submission_payload.get(key)
                if value is not None:
                    qr[key] = value if isinstance(value, bool) else value
            invalidated = submission_payload.get("wasInvalidatedBy")
            if invalidated:
                qr["wasInvalidatedBy"] = _clean_text(invalidated, 200)
            sub_status = submission_payload.get("submissionStatus")
            if sub_status is not None:
                qr["submissionStatus"] = (
                    [_clean_text(s, 60) for s in sub_status][:10] if isinstance(sub_status, list)
                    else _clean_text(sub_status, 120)
                )
            for key in ("released", "creationDate", "modificationDate"):
                value = submission_payload.get(key)
                if value:
                    qr[key] = _clean_text(value, 40)
            for key in ("definitionProperty", "synonymProperty", "obsoleteProperty",
                        "prefLabelProperty", "hierarchyProperty", "designedForOntologyTask"):
                value = submission_payload.get(key)
                if value:
                    qr[key] = _clean_text(value, 160)
            for key in ("example", "keyClasses", "knownUsage", "usedBy"):
                value = submission_payload.get(key)
                if value:
                    if isinstance(value, list):
                        qr[key] = [_clean_text(v, 80) for v in value if _clean_text(v, 80)][:8]
                    else:
                        qr[key] = _clean_text(value, 160)

        try:
            categories_payload = _http_get_json(f"{base_url.rstrip('/')}/ontologies/{acronym}/categories", params)
            meta["source_endpoints_used"].append(f"/ontologies/{acronym}/categories")
            categories = []
            for item in _collection(categories_payload):
                if isinstance(item, dict):
                    label = _clean_text(item.get("name") or item.get("acronym") or item.get("@id"), 80)
                else:
                    label = _clean_text(item, 80)
                if label:
                    categories.append(label)
            if categories:
                meta["categories"] = categories[:8]
                if not meta["domain"]:
                    meta["domain"] = categories[:5]
        except Exception as exc:
            meta["categories_error_type"] = _http_error_label(exc)

        try:
            roots_payload = _http_get_json(
                f"{base_url.rstrip('/')}/ontologies/{acronym}/classes/roots",
                {**params, "page": 1, "pagesize": 5},
            )
            meta["source_endpoints_used"].append(f"/ontologies/{acronym}/classes/roots")
            roots = [_compact_class(item) for item in _collection(roots_payload)]
            meta["root_labels_sample"] = [
                item["label"] for item in roots if isinstance(item, dict) and item.get("label")
            ][:5]
        except Exception as exc:
            meta["roots_error_type"] = _http_error_label(exc)

        # ---- quality metrics (classes/depth/definitions) ----
        try:
            metrics_payload = _http_get_json(f"{base_url.rstrip('/')}/ontologies/{acronym}/metrics", params)
            meta["source_endpoints_used"].append(f"/ontologies/{acronym}/metrics")
            if isinstance(metrics_payload, list) and metrics_payload:
                metrics_obj = metrics_payload[0] if isinstance(metrics_payload[0], dict) else {}
            else:
                metrics_obj = metrics_payload if isinstance(metrics_payload, dict) else {}
            compact_metrics = {}
            for key in ("classes", "properties", "individuals", "maxDepth", "averageChildCount", "classesWithNoDefinition"):
                if metrics_obj.get(key) is not None:
                    compact_metrics[key] = metrics_obj.get(key)
            if compact_metrics:
                meta["quality_raw"]["metrics"] = compact_metrics
        except Exception as exc:
            meta["metrics_error_type"] = _http_error_label(exc)

        # ---- reviews (numeric ratings only) ----
        try:
            reviews_payload = _http_get_json(f"{base_url.rstrip('/')}/ontologies/{acronym}/reviews", params)
            meta["source_endpoints_used"].append(f"/ontologies/{acronym}/reviews")
            rating_keys = ("usabilityRating", "coverageRating", "qualityRating",
                           "formalityRating", "correctnessRating", "documentationRating")
            compact_reviews = []
            for item in _collection(reviews_payload):
                if not isinstance(item, dict):
                    continue
                row = {}
                for key in rating_keys:
                    val = item.get(key)
                    if isinstance(val, dict):
                        val = val.get("ratingValue", val.get("value"))
                    try:
                        if val is not None:
                            row[key] = float(val)
                    except Exception:
                        continue
                if row:
                    compact_reviews.append(row)
            if compact_reviews:
                meta["quality_raw"]["reviews"] = compact_reviews[:10]
        except Exception as exc:
            meta["reviews_error_type"] = _http_error_label(exc)

        # ---- projects / known usage ----
        try:
            projects_payload = _http_get_json(f"{base_url.rstrip('/')}/ontologies/{acronym}/projects", params)
            meta["source_endpoints_used"].append(f"/ontologies/{acronym}/projects")
            names = []
            for item in _collection(projects_payload):
                if isinstance(item, dict):
                    nm = _clean_text(item.get("name") or item.get("acronym") or item.get("@id"), 80)
                    if nm:
                        names.append(nm)
            if names:
                meta["quality_raw"]["projects"] = names[:10]
        except Exception as exc:
            meta["projects_error_type"] = _http_error_label(exc)

        summary_parts = [
            meta.get("description"),
            meta.get("abstract"),
            "; ".join(meta.get("keywords", [])[:5]),
            "; ".join(meta.get("categories", [])[:5]),
        ]
        meta["purpose_summary_auto"] = _clean_text(" ".join(part for part in summary_parts if part), 260)
        meta["metadata_status"] = "metadata loaded"
        meta["enriched"] = True
    except Exception as exc:
        meta["error_type"] = _http_error_label(exc)
        meta["metadata_status"] = f"metadata lookup failed: {meta['error_type']}"

    return meta


def _registry_entry_from_meta(meta: Dict[str, Any], *, now: str) -> Dict[str, Any]:
    acronym = _normalize_acronym(meta.get("acronym"))
    return {
        "acronym": acronym,
        "name": _clean_text(meta.get("name") or acronym, 160),
        "description": _clean_text(meta.get("description"), 500),
        "abstract": _clean_text(meta.get("abstract"), 500),
        "keywords": list(meta.get("keywords", []) or []),
        "categories": list(meta.get("categories", []) or []),
        "categories_domains": list(meta.get("domain", []) or meta.get("categories", []) or []),
        "groups": list(meta.get("groups", []) or []),
        "ontology_type": _clean_text(meta.get("ontology_type"), 120),
        "homepage": _clean_text(meta.get("homepage"), 240),
        "documentation": _clean_text(meta.get("documentation"), 240),
        "hierarchy_property": _clean_text(meta.get("hierarchy_property"), 160),
        "root_labels_sample": list(meta.get("root_labels_sample", []) or []),
        "purpose_summary_auto": _clean_text(meta.get("purpose_summary_auto") or meta.get("purpose_summary"), 500),
        # Compact quality evidence — read by the quality gate, NEVER sent to the LLM.
        "quality_raw": dict(meta.get("quality_raw", {}) or {}),
        "curated_note": "",
        "curated_note_source": "",
        "metadata_source": "BioPortal",
        "first_seen": now,
        "last_seen": now,
        "last_refreshed": now,
        "api_endpoints_used": list(meta.get("source_endpoints_used", []) or []),
        "status": "ok" if meta.get("enriched") else "partial",
        "metadata_status": meta.get("metadata_status") or "metadata loaded",
        "errors": {
            key: value
            for key, value in meta.items()
            if key.endswith("_error_type") or key == "error_type"
        },
    }


def _meta_from_registry_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    acronym = _normalize_acronym(entry.get("acronym"))
    categories_domains = list(entry.get("categories_domains", []) or entry.get("categories", []) or [])
    return {
        "acronym": acronym,
        "name": entry.get("name") or acronym,
        "domain": categories_domains,
        "categories": list(entry.get("categories", []) or []),
        "groups": list(entry.get("groups", []) or []),
        "description": entry.get("description", ""),
        "abstract": entry.get("abstract", ""),
        "keywords": list(entry.get("keywords", []) or []),
        "homepage": entry.get("homepage", ""),
        "documentation": entry.get("documentation", ""),
        "ontology_type": entry.get("ontology_type", ""),
        "hierarchy_property": entry.get("hierarchy_property", ""),
        "root_labels_sample": list(entry.get("root_labels_sample", []) or []),
        "purpose_summary_auto": entry.get("purpose_summary_auto", ""),
        "metadata_status": entry.get("metadata_status") or entry.get("status") or "",
        "source_endpoints_used": list(entry.get("api_endpoints_used", []) or []),
        "curated_note": entry.get("curated_note", ""),
        "curated_note_source": entry.get("curated_note_source", ""),
        "registry_status": entry.get("status", ""),
        "metadata_source": entry.get("metadata_source", "BioPortal"),
        "first_seen": entry.get("first_seen", ""),
        "last_seen": entry.get("last_seen", ""),
        "last_refreshed": entry.get("last_refreshed", ""),
        "enriched": entry.get("status") in {"ok", "partial"},
        "error_type": entry.get("error", ""),
    }


_ONTOLOGY_REGISTRY = BioPortalOntologyRegistry()


def _get_ontology_meta(acronym: str, api_key: str, *, base_url: str) -> Dict[str, Any]:
    acronym_norm = _normalize_acronym(acronym)
    cache_key = (base_url.rstrip("/"), acronym_norm)
    now = time.time()
    with _CACHE_LOCK:
        cached = _ONTOLOGY_META_CACHE.get(cache_key)
        if cached and now - cached[0] < _META_TTL_SECONDS:
            return dict(cached[1])

    def fetcher(value: str) -> Dict[str, Any]:
        return _fetch_ontology_meta_from_bioportal(value, api_key, base_url=base_url)

    entry = _ONTOLOGY_REGISTRY.ensure(acronym_norm, fetcher)
    meta = _meta_from_registry_entry(entry)
    with _CACHE_LOCK:
        _ONTOLOGY_META_CACHE[cache_key] = (time.time(), dict(meta))
    return meta


# ---------------------------------------------------------------------------
# Registry bootstrap + runtime ontology-context lookup API
# ---------------------------------------------------------------------------

_RUNTIME_CONTEXT_CACHE: Dict[tuple, Dict[str, Any]] = {}
_RUNTIME_CONTEXT_LOCK = Lock()


def clear_ontology_context_runtime_cache() -> None:
    """Clear the in-process ontology-context lookup cache (test/runtime helper)."""
    with _RUNTIME_CONTEXT_LOCK:
        _RUNTIME_CONTEXT_CACHE.clear()


def bootstrap_bioportal_ontology_registry(
    acronyms: List[str],
    api_key: Optional[str] = None,
    *,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    refresh: bool = False,
    dry_run: bool = False,
    max_requests: Optional[int] = None,
    registry: Optional[BioPortalOntologyRegistry] = None,
) -> Dict[str, Any]:
    """Pre-fill the persistent registry for an explicit list of ontology acronyms.

    Only missing (and, with ``refresh``, stale) ontologies hit BioPortal. A live
    fetch requires ``api_key``; ``dry_run`` needs none. This never fetches the
    full BioPortal universe — callers pass an explicit acronym list.
    """
    reg = registry or _ONTOLOGY_REGISTRY

    def client(acronym: str) -> Dict[str, Any]:
        if not api_key:
            raise RuntimeError(
                "BioPortal API key required to fetch ontology metadata "
                "(set BIOPORTAL_API_KEY or pass api_key)."
            )
        return _fetch_ontology_meta_from_bioportal(acronym, api_key, base_url=base_url)

    return reg.bootstrap(
        acronyms,
        client,
        refresh=refresh,
        dry_run=dry_run,
        max_requests=max_requests,
    )


def get_ontology_context(
    acronym: str,
    *,
    ensure: bool = False,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    registry: Optional[BioPortalOntologyRegistry] = None,
) -> Optional[Dict[str, Any]]:
    """Return the registry context entry for one ontology, or None if absent.

    Reads the persistent registry first and caches in memory for the process.
    Does NOT call BioPortal unless ``ensure=True`` AND an ``api_key`` is given
    AND the entry is missing/stale — so runtime routing can use local context
    without network access and degrade gracefully when offline.
    """
    reg = registry or _ONTOLOGY_REGISTRY
    norm = _normalize_acronym(acronym)
    if not norm:
        return None
    cache_key = (str(reg.path), norm)
    with _RUNTIME_CONTEXT_LOCK:
        cached = _RUNTIME_CONTEXT_CACHE.get(cache_key)
    if cached is not None:
        return dict(cached)

    entry = reg.get(norm)
    if (entry is None or reg.is_stale(entry)) and ensure and api_key:
        def client(value: str) -> Dict[str, Any]:
            return _fetch_ontology_meta_from_bioportal(value, api_key, base_url=base_url)

        entry = reg.ensure(norm, client)
    if not isinstance(entry, dict) or not entry:
        return None
    reg.merge_static_curated_notes(entry)
    with _RUNTIME_CONTEXT_LOCK:
        _RUNTIME_CONTEXT_CACHE[cache_key] = dict(entry)
    return dict(entry)


def get_ontology_contexts(
    acronyms: List[str],
    *,
    ensure: bool = False,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    registry: Optional[BioPortalOntologyRegistry] = None,
) -> Dict[str, Optional[Dict[str, Any]]]:
    """Look up registry context for several ontologies (acronym -> entry|None)."""
    out: Dict[str, Optional[Dict[str, Any]]] = {}
    for raw in acronyms or []:
        norm = _normalize_acronym(raw)
        if not norm:
            continue
        out[norm] = get_ontology_context(
            norm, ensure=ensure, api_key=api_key, base_url=base_url, registry=registry
        )
    return out


def _domain_tokens(text: str) -> set:
    tokens = re.findall(r"[a-z0-9]+", str(text or "").lower())
    return {tok for tok in tokens if len(tok) > 2 and tok not in _DOMAIN_FIT_STOPWORDS}


def score_ontology_domain_fit(term: str, definition: str, entry: Optional[Dict[str, Any]]) -> float:
    """Deterministic domain-fit score in [0, 1] between an input term and an ontology.

    Uses ONLY registry fields (name/description/abstract/keywords/categories/
    groups/root_labels_sample/purpose_summary_auto/curated_note) — no embeddings,
    no live calls. Intended as the reusable foundation for a future ontology
    router; it does not change any existing retrieval/ranking behavior.
    """
    if not isinstance(entry, dict) or not entry:
        return 0.0
    input_tokens = _domain_tokens(f"{term} {definition}")
    if not input_tokens:
        return 0.0

    def _join(value: Any) -> str:
        if isinstance(value, list):
            return " ".join(str(item) for item in value if str(item).strip())
        return str(value or "")

    profile_text = " ".join(
        _join(entry.get(key))
        for key in (
            "name", "description", "abstract", "purpose_summary_auto", "curated_note",
            "keywords", "categories_domains", "categories", "groups", "root_labels_sample",
        )
    )
    profile_tokens = _domain_tokens(profile_text)
    if not profile_tokens:
        return 0.0

    overlap = len(input_tokens & profile_tokens) / len(input_tokens)
    strong_text = " ".join(
        _join(entry.get(key))
        for key in ("keywords", "categories_domains", "categories", "curated_note")
    )
    strong_tokens = _domain_tokens(strong_text)
    strong_hits = len(input_tokens & strong_tokens)
    strong_component = min(1.0, strong_hits / max(1, len(input_tokens)))
    score = 0.7 * overlap + 0.3 * strong_component
    return round(max(0.0, min(1.0, score)), 4)


def _same_uri(left: str, right: str) -> bool:
    return str(left or "").strip().rstrip("/").lower() == str(right or "").strip().rstrip("/").lower()


def _children_state(context: Dict[str, Any], errors: Dict[str, str]) -> str:
    if errors.get("children"):
        return f"children endpoint failed: {errors['children']}"
    if context.get("has_children") is False or context.get("children_count") in {0, "0"}:
        return "confirmed leaf node; BioPortal reports hasChildren=false"
    if context.get("children"):
        return "children returned from BioPortal first page"
    if context.get("has_children") is True:
        return "BioPortal reports children exist, but first children page returned empty"
    return "no children returned on first children page"


def _hierarchy_status(context: Dict[str, Any], errors: Dict[str, str]) -> str:
    if (
        context.get("lineage")
        or context.get("parents")
        or context.get("children")
        or context.get("sibling_classes_from_ontology")
    ):
        return "hierarchy evidence loaded"
    if errors:
        return "hierarchy lookup partially failed"
    return "no indexed hierarchy available in BioPortal"


def _hierarchy_quality(context: Dict[str, Any]) -> str:
    parts = []
    if context.get("parents"):
        parts.append("direct parents available")
    if context.get("lineage"):
        parts.append("path_to_root available")
    if context.get("sibling_classes_from_ontology"):
        parts.append("ontology sibling classes available")
    if context.get("children"):
        parts.append("direct children available")
    if not parts:
        parts.append("no indexed hierarchy available in BioPortal")
    return "; ".join(parts)


def _fetch_sibling_classes_from_ontology(
    parents: List[Dict[str, Any]],
    *,
    candidate_uri: str,
    ontology_acronym: str,
    api_key: str,
    base_url: str,
    children_limit: int,
    endpoints_used: List[str],
    endpoint_errors: Dict[str, str],
) -> Dict[str, Any]:
    siblings: List[Dict[str, str]] = []
    seen = set()
    attempted = 0
    successful = 0
    acronym = _normalize_acronym(ontology_acronym)
    for parent in parents:
        if len(siblings) >= 5:
            break
        parent_uri = str(parent.get("uri") or "").strip()
        if not parent_uri:
            continue
        parent_links = parent.get("links") if isinstance(parent.get("links"), dict) else {}
        children_url = _first_link(parent_links, "children")
        if not children_url:
            ontology = _first_link(parent_links, "ontology")
            link_acronym = ontology.rstrip("/").rsplit("/", 1)[-1] if ontology else ""
            endpoint_acronym = _normalize_acronym(link_acronym) or acronym
            if not endpoint_acronym:
                continue
            children_url = (
                f"{base_url.rstrip('/')}/ontologies/{endpoint_acronym}/classes/"
                f"{quote(parent_uri, safe='')}/children"
            )
        try:
            attempted += 1
            endpoints_used.append("parent.children")
            payload = _http_get_json(
                children_url,
                {
                    **_base_params(api_key),
                    "page": 1,
                    "pagesize": max(5, int(children_limit or 5)),
                },
            )
            successful += 1
            for item in _collection(payload):
                compact = _compact_class(item)
                if not compact:
                    continue
                label = compact.get("label", "")
                uri = compact.get("uri", "")
                key = (uri or label).strip().rstrip("/").lower()
                if not key or key in seen or _same_uri(uri, candidate_uri):
                    continue
                seen.add(key)
                siblings.append(
                    {
                        "label": label or uri,
                        "uri": uri,
                        "parent_label": str(parent.get("label") or ""),
                        "parent_uri": parent_uri,
                    }
                )
                if len(siblings) >= 5:
                    break
        except Exception as exc:
            endpoint_errors.setdefault("siblings", _http_error_label(exc))
    return {
        "siblings": siblings,
        "parent_children_calls_attempted": attempted,
        "parent_children_calls_successful": successful,
        "sibling_count": len(siblings),
    }


def _get_class_context(
    acronym: str,
    class_iri: str,
    api_key: str,
    *,
    base_url: str,
    children_limit: int,
    class_links: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cache_key = (base_url.rstrip("/"), acronym.upper(), class_iri, int(children_limit or 0))
    with _CACHE_LOCK:
        cached = _CLASS_CONTEXT_CACHE.get(cache_key)
        if cached:
            return dict(cached)

    encoded_class = quote(class_iri, safe="")
    root = f"{base_url.rstrip('/')}/ontologies/{acronym}/classes/{encoded_class}"
    params = _base_params(api_key)
    links = dict(class_links or {})
    endpoints_used: List[str] = []
    context: Dict[str, Any] = {
        "candidate_uri": class_iri,
        "ontology_acronym": acronym.upper(),
        "parents": [],
        "children": [],
        "siblings": [],
        "sibling_examples": [],
        "sibling_classes_from_ontology": [],
        "lineage": [],
        "class_links_used": bool(links),
        "ontology_meta": _get_ontology_meta(acronym, api_key, base_url=base_url),
        "enriched": False,
        "context_source": "bioportal",
        "source_endpoints_used": endpoints_used,
        "ontology_similarity": "not computed",
    }

    endpoint_errors: Dict[str, str] = {}
    try:
        detail_url = _first_link(links, "self") or root
        endpoints_used.append("class_detail")
        detail_payload = _http_get_json(detail_url, params)
        if isinstance(detail_payload, dict):
            context["class_detail_loaded"] = True
            detail_links = detail_payload.get("links", {}) or {}
            if isinstance(detail_links, dict):
                links.update(detail_links)
            if detail_payload.get("childrenCount") is not None:
                context["children_count"] = detail_payload.get("childrenCount")
            if detail_payload.get("hasChildren") is not None:
                context["has_children"] = bool(detail_payload.get("hasChildren"))
            context["candidate_label"] = _class_label(detail_payload)
            context["definition"] = _clean_text(detail_payload.get("definition"), 260)
            synonyms = detail_payload.get("synonym") or []
            if isinstance(synonyms, str):
                synonyms = [synonyms]
            if isinstance(synonyms, list):
                context["synonyms"] = [_clean_text(item, 80) for item in synonyms if _clean_text(item, 80)][:8]
            context["notation"] = _clean_text(detail_payload.get("notation"), 80)
            context["cui"] = _clean_text(detail_payload.get("cui"), 80)
            semantic_type = detail_payload.get("semanticType") or []
            if isinstance(semantic_type, str):
                semantic_type = [semantic_type]
            if isinstance(semantic_type, list):
                context["semantic_type"] = [_clean_text(item, 80) for item in semantic_type if _clean_text(item, 80)][:8]
            obsolete = detail_payload.get("obsolete")
            if obsolete is not None:
                context["obsolete"] = bool(obsolete)
    except Exception as exc:
        endpoint_errors["class_detail"] = _http_error_label(exc)

    try:
        parents_url = _first_link(links, "parents") or f"{root}/parents"
        endpoints_used.append("parents")
        parents_payload = _http_get_json(parents_url, params)
        parents = [_compact_class(item) for item in _collection(parents_payload)]
        context["parents"] = [item for item in parents if item][:8]
    except Exception as exc:
        endpoint_errors["parents"] = _http_error_label(exc)

    try:
        children_params = {**params, "page": 1, "pagesize": max(1, int(children_limit or 5))}
        children_url = _first_link(links, "children") or f"{root}/children"
        endpoints_used.append("children")
        children_payload = _http_get_json(children_url, children_params)
        children = [_compact_class(item) for item in _collection(children_payload)]
        context["children"] = [item for item in children if item][: max(1, int(children_limit or 5))]
    except Exception as exc:
        endpoint_errors["children"] = _http_error_label(exc)

    try:
        paths_url = _first_link(links, "paths_to_root") or f"{root}/paths_to_root"
        endpoints_used.append("paths_to_root")
        paths_payload = _http_get_json(paths_url, params)
        paths = _collection(paths_payload)
        if paths and isinstance(paths[0], list):
            path = [_class_label(item) for item in paths[0] if isinstance(item, dict)]
            context["lineage"] = [label for label in path if label][-6:]
    except Exception as exc:
        endpoint_errors["paths_to_root"] = _http_error_label(exc)

    sibling_lookup = _fetch_sibling_classes_from_ontology(
        context.get("parents", []),
        candidate_uri=class_iri,
        ontology_acronym=acronym,
        api_key=api_key,
        base_url=base_url,
        children_limit=children_limit,
        endpoints_used=endpoints_used,
        endpoint_errors=endpoint_errors,
    )
    context["parent_children_calls_attempted"] = int(sibling_lookup.get("parent_children_calls_attempted", 0))
    context["parent_children_calls_successful"] = int(sibling_lookup.get("parent_children_calls_successful", 0))
    context["sibling_count"] = int(sibling_lookup.get("sibling_count", 0))
    context["sibling_classes_from_ontology"] = list(sibling_lookup.get("siblings", []) or [])
    context["sibling_examples"] = [
        item.get("label", "")
        for item in context["sibling_classes_from_ontology"]
        if isinstance(item, dict) and item.get("label")
    ]
    context["siblings"] = list(context["sibling_examples"])
    context["children_state"] = _children_state(context, endpoint_errors)
    context["hierarchy_status"] = _hierarchy_status(context, endpoint_errors)
    context["hierarchy_quality"] = _hierarchy_quality(context)

    context["enriched"] = bool(
        context["parents"]
        or context["children"]
        or context["lineage"]
        or (isinstance(context.get("ontology_meta"), dict) and context["ontology_meta"].get("enriched"))
    )
    if endpoint_errors:
        context["endpoint_errors"] = endpoint_errors
        context["error_type"] = ";".join(f"{key}:{value}" for key, value in endpoint_errors.items())
    context["class_links"] = links

    with _CACHE_LOCK:
        _CLASS_CONTEXT_CACHE[cache_key] = dict(context)
    return context


def enrich_bioportal_candidate_context(
    candidate: AgentCandidate,
    api_key: Optional[str],
    *,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    children_limit: int = 5,
) -> AgentCandidate:
    """Attach compact ontology hierarchy/scope context to a BioPortal candidate."""
    if not candidate or not api_key:
        return candidate

    existing_context = getattr(candidate, "ontology_context", None)
    if isinstance(existing_context, dict) and existing_context.get("enriched"):
        return candidate

    existing_context = existing_context if isinstance(existing_context, dict) else {}
    class_links = (
        existing_context.get("class_links")
        or existing_context.get("links")
        or getattr(candidate, "source_links", None)
        or {}
    )
    acronym = _clean_text(existing_context.get("ontology_acronym") or getattr(candidate, "source_provider", "") or "", 80).upper()
    class_iri = _clean_text(getattr(candidate, "uri", "") or getattr(candidate, "raw_identifier", "") or "", 500)
    if not acronym or acronym in {"WIKIDATA", "BIOPORTAL"} or not class_iri:
        return candidate

    candidate.ontology_context = _get_class_context(
        acronym,
        class_iri,
        api_key,
        base_url=base_url,
        children_limit=children_limit,
        class_links=class_links if isinstance(class_links, dict) else None,
    )
    return candidate


def summarize_candidate_context(context: Optional[Dict[str, Any]]) -> str:
    """Return a short prompt-safe context block."""
    if not isinstance(context, dict) or not context:
        return "Ontology context: not enriched; candidate did not carry BioPortal context metadata"
    if not context.get("enriched"):
        reason = _clean_text(context.get("error_type") or "no BioPortal evidence loaded", 120)
        return f"Ontology context: enrichment attempted but no usable BioPortal evidence loaded ({reason})"

    errors = context.get("endpoint_errors") if isinstance(context.get("endpoint_errors"), dict) else {}
    parents = "; ".join(item.get("label", "") for item in context.get("parents", []) if isinstance(item, dict) and item.get("label"))
    children = "; ".join(item.get("label", "") for item in context.get("children", []) if isinstance(item, dict) and item.get("label"))
    sibling_items = context.get("sibling_classes_from_ontology") or []
    if sibling_items:
        siblings = "; ".join(
            item.get("label", "")
            for item in sibling_items
            if isinstance(item, dict) and item.get("label")
        )
    else:
        siblings = "; ".join(str(item) for item in context.get("sibling_examples", []) if str(item).strip())
    lineage = " > ".join(label for label in context.get("lineage", []) if label)
    meta = context.get("ontology_meta") if isinstance(context.get("ontology_meta"), dict) else {}
    domain_values = meta.get("domain", []) or meta.get("categories", [])
    domain = "; ".join(str(item) for item in domain_values if str(item).strip())
    description = _clean_text(meta.get("description") or meta.get("abstract") or "", 220)
    curated_note = _clean_text(meta.get("curated_note") or "", 260)
    purpose = _clean_text(meta.get("purpose_summary_auto") or description, 220)
    keywords = "; ".join(str(item) for item in meta.get("keywords", []) if str(item).strip())
    root_labels = "; ".join(str(item) for item in meta.get("root_labels_sample", []) if str(item).strip())

    if domain:
        domain_text = domain
    elif meta.get("error_type"):
        domain_text = "lookup failed (ontology metadata)"
    else:
        domain_text = "not declared in BioPortal metadata"

    if description:
        scope_text = description
    elif meta.get("error_type"):
        scope_text = "lookup failed (latest_submission metadata)"
    else:
        scope_text = "not declared in BioPortal metadata"

    if lineage:
        lineage_text = lineage
    elif errors.get("paths_to_root"):
        lineage_text = f"path_to_root endpoint failed: {errors['paths_to_root']}"
    elif context.get("parents") or context.get("children"):
        lineage_text = "no path_to_root returned by BioPortal"
    else:
        lineage_text = "no indexed hierarchy available in BioPortal"

    if parents:
        parents_text = parents
    elif errors.get("parents"):
        parents_text = f"parents endpoint failed: {errors['parents']}"
    elif lineage and len(context.get("lineage", [])) <= 1:
        parents_text = "root-level class"
    else:
        parents_text = "none returned by BioPortal"

    if children:
        children_text = children
    elif errors.get("children"):
        children_text = f"children endpoint failed: {errors['children']}"
    elif context.get("has_children") is False or context.get("children_count") in {0, "0"}:
        children_text = "none; leaf node"
    else:
        children_text = "none returned on first children page"

    if siblings:
        siblings_text = siblings
    elif errors.get("siblings"):
        siblings_text = f"sibling lookup partially failed: {errors['siblings']}"
    elif parents:
        siblings_text = "no sibling classes returned from parent children endpoint"
    else:
        siblings_text = "sibling lookup skipped because no direct parent was available"

    lines = [
        f"Ontology: {_clean_text(meta.get('name') or meta.get('acronym') or '', 120)}",
        f"Ontology purpose auto-summary: {purpose or scope_text}",
        f"Ontology domain/categories: {domain_text}",
        f"Ontology scope: {scope_text}",
        f"Ontology keywords: {keywords or 'not declared in BioPortal metadata'}",
        f"Ontology root labels sample: {root_labels or 'not retrieved or not declared by BioPortal'}",
        f"Curated ontology note: {curated_note or 'none'}",
        f"Candidate: {_clean_text(context.get('candidate_label') or '', 120) or 'candidate label not returned by BioPortal class detail'}",
        f"Candidate URI: {_clean_text(context.get('candidate_uri') or '', 220)}",
        f"Candidate definition: {_clean_text(context.get('definition') or '', 220) or 'definition not declared in BioPortal class detail'}",
        f"Lineage: {lineage_text}",
        f"Direct parents: {parents_text}",
        f"Sibling classes from ontology: {siblings_text}",
        f"Direct children: {children_text}",
        f"Children state: {context.get('children_state') or children_text}",
        f"Hierarchy quality: {context.get('hierarchy_quality') or _hierarchy_quality(context)}",
        f"Hierarchy status: {context.get('hierarchy_status') or _hierarchy_status(context, errors)}",
        f"Metadata status: {meta.get('metadata_status') or 'metadata status not recorded'}",
        f"Ontology similarity: {context.get('ontology_similarity') or 'not computed'}",
    ]
    return "\n".join(lines)
