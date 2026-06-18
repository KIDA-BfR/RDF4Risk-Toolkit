"""Project persistence + operation history for the RDF4Risk web app.

The 5 MUI services keep their workflow state in 3 in-memory holders:
  * Matching_Table_Generator.generator._STATE
  * semi_automatic_reconciliation.reconciliation_service.STATE
  * agentic_reconciliation.agent_runtime_state.runtime_state  (shared by agent + rdf_generator + rdf_to_table)

A *project* is a named, on-disk snapshot of all three holders. The *history* is an in-memory stack of
pre-event snapshots, giving undo / revert-to-step across the whole pipeline.

Snapshots are taken per-key with a volatile deny-list (live run threads/events, in-memory rdflib graphs,
the TriG converter) so a snapshot never carries a non-picklable object; the dropped keys are all
reconstructable (graphs from the preserved turtle strings, threads/events are transient run handles
that must never be resurrected). Each kept value is pickled individually, so a single bad value is
skipped rather than failing the whole snapshot.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import time
import uuid
from typing import Any, Dict, List

LOGGER = logging.getLogger("rdf4risk.project_store")

PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".rdf4risk_projects")
INDEX_PATH = os.path.join(PROJECT_DIR, "index.json")
MAX_HISTORY = 25

# Keys inside the shared runtime_state that must never be snapshotted/restored.
_RUNTIME_DENY = {
    "agent_reconciliation_run_thread",
    "agent_reconciliation_run_cancel_event",
    "agent_reconciliation_stop_event",
    "trig_converter",
    "rdf_graph",
    "skos_graph",
}

# Event types that don't change durable state — excluded from the operation history.
_NON_HISTORY_EVENTS = {"navigate"}


# --- holder access -------------------------------------------------------------------------------

def _holders():
    """Return (matching_module, semi_module, runtime_dict). Imported lazily to avoid import cycles."""
    from Matching_Table_Generator import generator as matching
    from semi_automatic_reconciliation import reconciliation_service as semi
    from agentic_reconciliation.agent_runtime_state import runtime_state
    return matching, semi, runtime_state


def _reinit_all() -> None:
    """Re-run each service's (fill-missing-only) init to backfill any keys a snapshot omitted."""
    matching, semi, _ = _holders()
    matching._init_state()
    semi.initialize_reconciliation_state()
    from agentic_reconciliation import agent_reconciliation_service as agent
    agent._initialize_agent_reconciliation_state()
    from RDF_Generator import app as rdf_generator
    rdf_generator._init_rdf_mui_state()
    from RDF_to_Table import tablegenerator as rdf_to_table
    rdf_to_table._init_rdf_to_table_state()


# --- snapshot / restore --------------------------------------------------------------------------

def _pickle_filter(holder: Dict[str, Any], deny: frozenset = frozenset()) -> Dict[str, bytes]:
    """Pickle each non-denied value individually; skip any value that can't be pickled."""
    out: Dict[str, bytes] = {}
    for key, value in list(holder.items()):
        if key in deny:
            continue
        try:
            out[key] = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            LOGGER.debug("Skipping non-picklable state key %r", key)
    return out


def capture() -> Dict[str, Dict[str, bytes]]:
    """Capture all three holders as {holder: {key: pickled-bytes}} — isolated from later mutation."""
    matching, semi, runtime = _holders()
    return {
        "matching": _pickle_filter(matching._STATE),
        "semi": _pickle_filter(semi.STATE),
        "runtime": _pickle_filter(runtime, frozenset(_RUNTIME_DENY)),
    }


def _restore_holder(holder: Dict[str, Any], blob: Dict[str, bytes]) -> None:
    holder.clear()
    for key, raw in (blob or {}).items():
        try:
            holder[key] = pickle.loads(raw)
        except Exception:
            LOGGER.warning("Failed to unpickle state key %r during restore", key)


def restore(snapshot: Dict[str, Dict[str, bytes]]) -> None:
    """Restore all three holders from a snapshot, then re-init to backfill safe defaults."""
    matching, semi, runtime = _holders()
    _restore_holder(matching._STATE, snapshot.get("matching", {}))
    _restore_holder(semi.STATE, snapshot.get("semi", {}))
    from agentic_reconciliation.agent_runtime_state import clear_runtime_state
    clear_runtime_state()
    for key, raw in (snapshot.get("runtime") or {}).items():
        try:
            runtime[key] = pickle.loads(raw)
        except Exception:
            LOGGER.warning("Failed to unpickle runtime key %r during restore", key)
    _reinit_all()


# --- on-disk projects ----------------------------------------------------------------------------

def _read_index() -> Dict[str, Any]:
    try:
        with open(INDEX_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_index(index: Dict[str, Any]) -> None:
    os.makedirs(PROJECT_DIR, exist_ok=True)
    tmp = INDEX_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(index, handle, ensure_ascii=False, indent=1)
    os.replace(tmp, INDEX_PATH)


def _project_path(project_id: str) -> str:
    return os.path.join(PROJECT_DIR, f"{project_id}.rdf4risk")


def list_projects() -> List[Dict[str, Any]]:
    index = _read_index()
    items = [
        {"id": pid, "name": meta.get("name", pid), "saved_at": meta.get("saved_at")}
        for pid, meta in index.items()
    ]
    return sorted(items, key=lambda item: item.get("saved_at") or 0, reverse=True)


def save_project(name: str, project_id: str | None = None) -> Dict[str, Any]:
    os.makedirs(PROJECT_DIR, exist_ok=True)
    pid = project_id or uuid.uuid4().hex[:12]
    with open(_project_path(pid), "wb") as handle:
        pickle.dump(capture(), handle, protocol=pickle.HIGHEST_PROTOCOL)
    index = _read_index()
    index[pid] = {"name": str(name or "Untitled project").strip() or "Untitled project", "saved_at": time.time()}
    _write_index(index)
    return {"id": pid, "name": index[pid]["name"], "saved_at": index[pid]["saved_at"]}


def load_project(project_id: str) -> bool:
    path = _project_path(project_id)
    if not os.path.exists(path):
        return False
    with open(path, "rb") as handle:
        snapshot = pickle.load(handle)
    restore(snapshot)
    HISTORY.reset()  # a freshly loaded project starts a clean history
    return True


def delete_project(project_id: str) -> bool:
    index = _read_index()
    existed = index.pop(project_id, None) is not None
    _write_index(index)
    try:
        os.remove(_project_path(project_id))
    except FileNotFoundError:
        pass
    return existed


# --- operation history ---------------------------------------------------------------------------

_EVENT_LABELS = {
    "upload_file": "Upload data table",
    "upload_table": "Upload table",
    "upload_csv": "Upload CSV",
    "upload_data": "Upload data",
    "upload_mapping": "Upload mapping table",
    "upload_trig": "Upload TriG",
    "set_omitted_columns": "Set omitted columns",
    "prepare_transformations": "Prepare transformations",
    "clear_transformations": "Clear transformations",
    "find_similar_terms": "Find similar terms",
    "stage_consolidations": "Stage consolidations",
    "generate_matching_table": "Generate matching table",
    "generate_rdf": "Generate RDF",
    "generate_dcat": "Generate DCAT catalog",
    "accept_mapping": "Accept mapping",
    "reject_mapping": "Reject mapping",
    "reset_mapping": "Reset mapping",
    "accept_mappings": "Accept mappings (bulk)",
    "reject_mappings": "Reject mappings (bulk)",
    "reset_mappings": "Reset mappings (bulk)",
    "start_run": "Start agent run",
    "stop_run": "Stop agent run",
    "reset_all": "Reset workflow",
    "reset_workflow": "Reset workflow",
    "confirm_queue": "Confirm provider queue",
    "start_processing": "Start processing queue",
    "prefill_best_match": "Prefill best matches",
    "update_mapping": "Update mapping",
    "publish_rdf_handoff": "Publish to RDF generator",
}


def _label(service: str, event: Dict[str, Any]) -> str:
    etype = str(event.get("type", "") or "")
    return _EVENT_LABELS.get(etype, etype.replace("_", " ").strip().capitalize() or "Change")


def _is_recipe_value(value: Any) -> bool:
    """True if a value belongs in a portable recipe: JSON-serializable config, not bulk data.

    Excludes DataFrames, raw bytes (uploads) and anything that won't round-trip through JSON — what's
    left is exactly the daten-unabhängige configuration (rules, provider order, model/prompt choices,
    schema templates, DCAT metadata, subject-ID scheme, thresholds).
    """
    import pandas as pd

    if isinstance(value, (pd.DataFrame, pd.Series, bytes, bytearray)):
        return False
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


RECIPE_VERSION = 1


def export_recipe() -> Dict[str, Any]:
    """Return the whole-pipeline configuration as a portable, human-readable JSON recipe."""
    snapshot = capture()
    recipe: Dict[str, Dict[str, Any]] = {}
    for holder_name, blob in snapshot.items():
        section: Dict[str, Any] = {}
        for key, raw in blob.items():
            try:
                value = pickle.loads(raw)
            except Exception:
                continue
            if _is_recipe_value(value):
                section[key] = value
        recipe[holder_name] = section
    return {"version": RECIPE_VERSION, "exported_at": time.time(), "recipe": recipe}


def import_recipe(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Apply a recipe's config keys onto the live state (leaving loaded data intact), then re-init."""
    recipe = payload.get("recipe") if isinstance(payload, dict) else None
    if not isinstance(recipe, dict):
        return {"applied": 0, "error": "Not a valid recipe file."}
    matching, semi, runtime = _holders()
    targets = {"matching": matching._STATE, "semi": semi.STATE, "runtime": runtime}
    applied = 0
    for holder_name, holder in targets.items():
        section = recipe.get(holder_name)
        if not isinstance(section, dict):
            continue
        for key, value in section.items():
            holder[key] = value
            applied += 1
    _reinit_all()
    return {"applied": applied}


class _History:
    """Pre-event snapshot stack: entry[i] is the state BEFORE action i, so reverting to i undoes i..end."""

    def __init__(self) -> None:
        self.entries: List[Dict[str, Any]] = []

    def reset(self) -> None:
        self.entries = []

    def record_before(self, service: str, event: Dict[str, Any]) -> None:
        etype = str(event.get("type", "") or "")
        if etype in _NON_HISTORY_EVENTS:
            return
        try:
            snapshot = capture()
        except Exception:
            LOGGER.exception("Failed to capture history snapshot; skipping")
            return
        self.entries.append({
            "label": _label(service, event),
            "service": service,
            "ts": time.time(),
            "snapshot": snapshot,
        })
        if len(self.entries) > MAX_HISTORY:
            self.entries = self.entries[-MAX_HISTORY:]

    def listing(self) -> List[Dict[str, Any]]:
        return [
            {"index": i, "label": e["label"], "service": e["service"], "ts": e["ts"]}
            for i, e in enumerate(self.entries)
        ]

    def revert_to(self, index: int) -> bool:
        if 0 <= index < len(self.entries):
            restore(self.entries[index]["snapshot"])
            self.entries = self.entries[:index]  # the reverted action and everything after it are undone
            return True
        return False

    def undo(self) -> bool:
        return self.revert_to(len(self.entries) - 1) if self.entries else False


HISTORY = _History()
