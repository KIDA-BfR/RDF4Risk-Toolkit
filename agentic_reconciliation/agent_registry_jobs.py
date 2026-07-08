# -*- coding: utf-8 -*-
"""Background job layer for BioPortal ontology-registry maintenance.

Powers the admin "Fetch / Update BioPortal Ontology Registry" control. Two modes:

  * "catalog"      -> lightweight: discover all BioPortal ontology acronyms and
                      ensure a stub registry entry exists for each (1 listing API
                      call, no per-ontology deep fetch). Fast and safe; the UI
                      default.
  * "full_context" -> deep: fetch full metadata (multiple endpoints) per ontology.
                      For a bounded acronym list by default; "all_bioportal" +
                      "full_context" is the advanced, slow, heavily-warned path.

A full fetch NEVER runs at startup and requires an explicit ``confirmed=True``
(except dry-run). Curated notes are preserved, existing fresh entries are skipped,
the registry is written atomically with periodic checkpoints, and the job is
cancellable.
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .agent_bioportal_service import DEFAULT_BIOPORTAL_BASE_URL, list_bioportal_ontology_acronyms
from .agent_bioportal_context_service import (
    _ONTOLOGY_REGISTRY,
    _fetch_ontology_meta_from_bioportal,
    _utc_now_iso,
    BioPortalOntologyRegistry,
)

_JOBS: Dict[str, Dict[str, Any]] = {}
_JOBS_LOCK = threading.Lock()
_CHECKPOINT_EVERY = 25
_MAX_ERROR_SAMPLES = 10


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_job(mode: str, all_bioportal: bool, dry_run: bool) -> Dict[str, Any]:
    return {
        "job_id": uuid.uuid4().hex,
        "status": "pending",  # pending|running|completed|failed|cancelled
        "mode": mode,
        "all_bioportal": bool(all_bioportal),
        "dry_run": bool(dry_run),
        "total": 0,
        "processed": 0,
        "percent": 0.0,
        "current_acronym": None,
        "fetched": 0,
        "skipped": 0,
        "failed": 0,
        "errors_sample": [],
        "started_at": None,
        "updated_at": _now_iso(),
        "completed_at": None,
        "cancel_requested": False,
        "error": "",
    }


def _set(job_id: str, **fields: Any) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        job.update(fields)
        job["updated_at"] = _now_iso()


def get_bioportal_registry_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    with _JOBS_LOCK:
        job = _JOBS.get(str(job_id))
        return dict(job) if job is not None else None


def latest_bioportal_registry_job() -> Optional[Dict[str, Any]]:
    with _JOBS_LOCK:
        if not _JOBS:
            return None
        job = max(_JOBS.values(), key=lambda j: j.get("started_at") or j.get("updated_at") or "")
        return dict(job)


def cancel_bioportal_registry_job(job_id: str) -> bool:
    with _JOBS_LOCK:
        job = _JOBS.get(str(job_id))
        if job is None:
            return False
        if job["status"] in {"completed", "failed", "cancelled"}:
            return False
        job["cancel_requested"] = True
        job["updated_at"] = _now_iso()
        return True


def clear_registry_jobs() -> None:
    with _JOBS_LOCK:
        _JOBS.clear()


def _cancel_requested(job_id: str) -> bool:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        return bool(job and job.get("cancel_requested"))


def _resolve_acronyms(
    *,
    all_bioportal: bool,
    acronyms: Optional[List[str]],
    api_key: Optional[str],
    base_url: str,
    discover: Optional[Callable[[], List[str]]],
) -> List[str]:
    if all_bioportal:
        if discover is not None:
            found = discover()
        else:
            found = list_bioportal_ontology_acronyms(api_key, base_url=base_url)
        return [str(a).strip().upper() for a in (found or []) if str(a).strip()]
    return [str(a).strip().upper() for a in (acronyms or []) if str(a).strip()]


def _run_job(
    job_id: str,
    *,
    mode: str,
    all_bioportal: bool,
    acronyms: Optional[List[str]],
    api_key: Optional[str],
    base_url: str,
    refresh: bool,
    max_requests: Optional[int],
    dry_run: bool,
    registry: BioPortalOntologyRegistry,
    discover: Optional[Callable[[], List[str]]],
    fetch_meta: Callable[[str], Dict[str, Any]],
) -> None:
    _set(job_id, status="running", started_at=_now_iso())
    try:
        resolved = _resolve_acronyms(
            all_bioportal=all_bioportal, acronyms=acronyms, api_key=api_key,
            base_url=base_url, discover=discover,
        )
        # dedupe preserve order
        seen = set()
        resolved = [a for a in resolved if not (a in seen or seen.add(a))]
        _set(job_id, total=len(resolved))

        budget = None
        try:
            if max_requests not in (None, "", 0):
                budget = max(0, int(max_requests))
        except Exception:
            budget = None

        entries = registry.load()
        fetched = skipped = failed = fetches_used = 0
        errors_sample: List[Dict[str, str]] = []
        now = _utc_now_iso()

        for index, acronym in enumerate(resolved):
            if _cancel_requested(job_id):
                if not dry_run:
                    registry.save()
                _set(
                    job_id, status="cancelled", current_acronym=None,
                    processed=index, percent=round(100.0 * index / max(1, len(resolved)), 1),
                    fetched=fetched, skipped=skipped, failed=failed,
                    errors_sample=list(errors_sample), completed_at=_now_iso(),
                )
                return

            _set(
                job_id, current_acronym=acronym, processed=index,
                percent=round(100.0 * index / max(1, len(resolved)), 1),
                fetched=fetched, skipped=skipped, failed=failed,
            )

            existing = entries.get(acronym)
            is_stale = bool(existing) and registry.is_stale(existing)
            present_and_fresh = bool(existing) and not is_stale

            if present_and_fresh and not refresh:
                if not dry_run:
                    existing["last_seen"] = now
                    registry.merge_static_curated_notes(existing)
                skipped += 1
                continue

            if dry_run:
                # would fetch/upsert; count as fetched-plan via skipped tracking
                continue

            if budget is not None and fetches_used >= budget:
                skipped += 1
                continue

            try:
                if mode == "catalog":
                    # Lightweight: ensure a stub entry exists; do NOT deep-fetch and
                    # do NOT overwrite an existing rich entry. Curated notes kept.
                    if existing is None:
                        entries[acronym] = {
                            "acronym": acronym,
                            "name": acronym,
                            "metadata_source": "BioPortal catalog",
                            "first_seen": now,
                            "last_seen": now,
                            "last_refreshed": "",  # stub -> stays stale until deep fetch
                            "status": "catalog_listed",
                            "curated_note": "",
                            "curated_note_source": "",
                        }
                        registry.merge_static_curated_notes(entries[acronym])
                        fetched += 1
                    else:
                        existing["last_seen"] = now
                        registry.merge_static_curated_notes(existing)
                        skipped += 1
                else:  # full_context (deep)
                    entry = registry._build_entry(acronym, fetch_meta, previous=existing or {})
                    entries[acronym] = entry
                    fetches_used += 1
                    if str(entry.get("status", "")).strip().lower() == "lookup_failed":
                        failed += 1
                        if len(errors_sample) < _MAX_ERROR_SAMPLES:
                            errors_sample.append({"acronym": acronym, "error": str(entry.get("error", ""))[:200]})
                    elif existing is None:
                        fetched += 1
                    else:
                        fetched += 1  # refreshed counted under fetched for UI simplicity
            except Exception as exc:  # never let one ontology crash the whole job
                failed += 1
                if len(errors_sample) < _MAX_ERROR_SAMPLES:
                    errors_sample.append({"acronym": acronym, "error": f"{type(exc).__name__}: {exc}"[:200]})

            if not dry_run and ((index + 1) % _CHECKPOINT_EVERY == 0):
                registry.save()  # checkpoint so interruption does not lose progress

        if not dry_run:
            registry.save()

        _set(
            job_id, status="completed", current_acronym=None,
            processed=len(resolved), percent=100.0,
            fetched=fetched, skipped=skipped, failed=failed,
            errors_sample=list(errors_sample), completed_at=_now_iso(),
        )
    except Exception as exc:
        _set(job_id, status="failed", error=f"{type(exc).__name__}: {exc}"[:300], completed_at=_now_iso())


def start_bioportal_catalog_refresh(
    *,
    mode: str = "catalog",
    all_bioportal: bool = True,
    acronyms: Optional[List[str]] = None,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BIOPORTAL_BASE_URL,
    refresh: bool = False,
    max_requests: Optional[int] = None,
    dry_run: bool = False,
    confirmed: bool = False,
    registry: Optional[BioPortalOntologyRegistry] = None,
    discover: Optional[Callable[[], List[str]]] = None,
    fetch_meta: Optional[Callable[[str], Dict[str, Any]]] = None,
    run_async: bool = True,
) -> str:
    """Start a registry-maintenance job. Returns the job_id.

    Real (non-dry) runs require ``confirmed=True`` — the UI sets this only after the
    user clicks Proceed in the warning modal. A bare GET/poll can never trigger a
    full fetch.
    """
    mode = "full_context" if str(mode).strip().lower() in {"full_context", "full", "deep"} else "catalog"
    if not dry_run and not confirmed:
        raise ValueError("Registry fetch requires explicit confirmation (confirmed=True).")

    reg = registry or _ONTOLOGY_REGISTRY

    def _default_fetch(acronym: str) -> Dict[str, Any]:
        if not api_key:
            raise RuntimeError("BioPortal API key required for full-context fetch.")
        return _fetch_ontology_meta_from_bioportal(acronym, api_key, base_url=base_url)

    job = _new_job(mode, all_bioportal, dry_run)
    with _JOBS_LOCK:
        _JOBS[job["job_id"]] = job
    job_id = job["job_id"]

    kwargs = dict(
        mode=mode, all_bioportal=all_bioportal, acronyms=acronyms, api_key=api_key,
        base_url=base_url, refresh=refresh, max_requests=max_requests, dry_run=dry_run,
        registry=reg, discover=discover, fetch_meta=fetch_meta or _default_fetch,
    )
    if run_async:
        thread = threading.Thread(target=_run_job, args=(job_id,), kwargs=kwargs, daemon=True)
        thread.start()
    else:
        _run_job(job_id, **kwargs)
    return job_id
