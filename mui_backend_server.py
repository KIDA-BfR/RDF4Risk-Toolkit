"""HTTP backend for the RDF4Risk web app.

This module exposes JSON snapshots and event handlers for the browser app.
Python services own workflow state and business logic; React/Material UI owns
all frontend rendering.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import logging
import os
import sys
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Tuple
from urllib.parse import urlparse

import pandas as pd
from Matching_Table_Generator import generator as matching_service
from RDF_Generator import app as rdf_generator_service
from RDF_to_Table import tablegenerator as rdf_to_table_service
from agentic_reconciliation import agent_reconciliation_service as agent_service
from agentic_reconciliation.agent_bioportal_service import list_bioportal_ontology_acronyms
from agentic_reconciliation.agent_bioportal_context_service import _ONTOLOGY_REGISTRY
from agentic_reconciliation.agent_registry_jobs import (
    cancel_bioportal_registry_job,
    get_bioportal_registry_job_status,
    latest_bioportal_registry_job,
    start_bioportal_catalog_refresh,
)
from agentic_reconciliation.agent_runtime_state import runtime_state as agent_runtime_state
from agentic_reconciliation.agent_llm_service import get_provider_label, get_supported_llm_providers
from semi_automatic_reconciliation import reconciliation_service as semi_service
import project_store

LOGGER = logging.getLogger("rdf4risk.mui_backend")

SERVICE_IDS = {
    "matching_table_generator",
    "semi_automatic_reconciliation",
    "agent_reconciliation",
    "rdf_generator",
    "rdf_to_table",
}
DEFAULT_MAX_EVENT_BODY_BYTES = 50 * 1024 * 1024
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


@lru_cache(maxsize=8)
def _cached_bioportal_ontology_options(api_key: str, base_url: str) -> tuple[str, ...]:
    return tuple(list_bioportal_ontology_acronyms(api_key, base_url=base_url))


def _allowed_origins_from_env() -> set[str]:
    raw = os.getenv("RDF4RISK_ALLOWED_ORIGINS", "")
    return {origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()}


def _is_loopback_origin(origin: str) -> bool:
    try:
        parsed = urlparse(origin)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and (parsed.hostname or "") in LOOPBACK_HOSTS


def _resolve_cors_origin(origin: str | None) -> str | None:
    if not origin:
        return None
    normalized = origin.rstrip("/")
    allowed_origins = _allowed_origins_from_env()
    if "*" in allowed_origins:
        return normalized
    if normalized in allowed_origins:
        return normalized
    if not allowed_origins and _is_loopback_origin(normalized):
        return normalized
    return None


def _max_event_body_bytes() -> int:
    raw = str(os.getenv("RDF4RISK_MAX_EVENT_BODY_MB", "") or "").strip()
    if not raw:
        return DEFAULT_MAX_EVENT_BODY_BYTES
    try:
        return max(1, int(float(raw))) * 1024 * 1024
    except ValueError:
        LOGGER.warning("Invalid RDF4RISK_MAX_EVENT_BODY_MB=%r; using default.", raw)
        return DEFAULT_MAX_EVENT_BODY_BYTES


def _json_safe(value: Any) -> Any:
    """Recursively coerce values produced by pandas/backend state to JSON."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return str(value)


def _agent_args() -> Dict[str, Any]:
    agent_service._initialize_agent_reconciliation_state()
    provenance_defaults = agent_service._get_provenance_defaults_from_config()
    agent_service._initialize_provenance_state(provenance_defaults)

    defaults = agent_service._get_agent_ui_defaults()
    runtime_context = agent_service._compute_workflow_runtime_context(defaults)
    required_columns = agent_service.REQUIRED_MATCHING_TABLE_COLUMNS
    readiness = agent_service._build_run_readiness_state(
        required_columns,
        runtime_context.get("missing_provider_keys", []),
    )
    config = agent_service._build_workflow_config_from_state(defaults)

    primary_provider = str(runtime_context.get("primary_provider", config.get("provider", "openai")) or "openai")
    primary_catalog = runtime_context.get("primary_catalog")
    primary_models = list(runtime_context.get("primary_models", []) or [])
    selected_model = str(agent_runtime_state.get("agent_model_name", "") or "")

    config["provider"] = primary_provider
    config["model"] = selected_model

    provider_options = list(defaults.get("provider_options", get_supported_llm_providers()) or ["openai"])
    provider_labels = {provider: get_provider_label(provider) for provider in provider_options}
    model_labels = {model: agent_service._format_model_option_label(primary_catalog, model) for model in primary_models}
    model_details = agent_service._format_model_details_caption(primary_catalog, selected_model) if primary_catalog else None
    if not primary_models and isinstance(primary_catalog, dict):
        model_details = str(primary_catalog.get("message") or "No available models. Use Reload models & pricing in Options.")
    fallback_ontology_options = sorted(
        set(
            list((agent_service.CONFIG or {}).get("agent_reconciliation", {}).get("trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"]))
            + list((agent_service.CONFIG or {}).get("agent_reconciliation", {}).get("bioportal_agent_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"]))
            + ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON", "NIFSTD", "BERO", "OCHV", "SNOMEDCT", "CHEBI", "QUDT"]
        )
    )
    bioportal_cfg = (agent_service.CONFIG or {}).get("bioportal", {})
    bioportal_api_key = str(bioportal_cfg.get("api_key") or os.getenv("BIOPORTAL_API_KEY") or "").strip()
    bioportal_base_url = str(bioportal_cfg.get("base_url") or "https://data.bioontology.org").strip()
    live_ontology_options = list(_cached_bioportal_ontology_options(bioportal_api_key, bioportal_base_url)) if bioportal_api_key else []
    ontology_options = sorted(
        set(live_ontology_options or fallback_ontology_options)
        | set(config.get("bioportal_ontologies") or [])
        | set(config.get("trusted_ontologies") or [])
    )
    provider_kind = (
        "codex"
        if agent_service._is_codex_provider(primary_provider)
        else "openai_compatible"
        if agent_service._is_openai_compatible_provider(primary_provider)
        else "standard"
    )
    return {
        "active_stage": agent_service._component_stage_from_session(),
        "config": config,
        "providers": provider_options,
        "providerLabels": provider_labels,
        "models": primary_models,
        "modelLabels": model_labels,
        "modelDetails": model_details,
        "reasoningOptions": agent_service.REASONING_EFFORT_OPTIONS,
        "readiness": readiness,
        "data_status": agent_service._build_data_status_snapshot(required_columns),
        "run_status": agent_service._build_run_status_snapshot(readiness),
        "telemetry": agent_service._build_telemetry_snapshot(),
        "review": agent_service._build_review_snapshot(agent_runtime_state.get(agent_service.AGENT_DATAFRAME_STATE_KEY)),
        "exportPayload": agent_runtime_state.get(agent_service.AGENT_SSSOM_EXPORT_PAYLOAD_KEY),
        "ontologyOptions": ontology_options,
        "providerKind": provider_kind,
        "statusMessage": agent_runtime_state.get("agent_mui_status_message"),
        "rdf_handoff": agent_runtime_state.get("agent_rdf_handoff"),
        "rdf_handoff_revision": _agent_rdf_source_revision(),
        "codexAuthStatus": agent_service.get_codex_auth_status(),
    }


def _agent_event(event: Dict[str, Any]) -> None:
    agent_service._initialize_agent_reconciliation_state()
    provenance_defaults = agent_service._get_provenance_defaults_from_config()
    agent_service._initialize_provenance_state(provenance_defaults)
    defaults = agent_service._get_agent_ui_defaults()
    runtime_context = agent_service._compute_workflow_runtime_context(defaults)
    readiness = agent_service._build_run_readiness_state(
        agent_service.REQUIRED_MATCHING_TABLE_COLUMNS,
        runtime_context.get("missing_provider_keys", []),
    )
    agent_service._handle_agent_mui_event(event, readiness, runtime_context, provenance_defaults)


def _sync_matching_outputs_to_legacy_state() -> None:
    """Expose matching-generator backend outputs to services not yet refactored.

    The matching table generator no longer uses UI framework state.  Some downstream
    services still read `shared_matching_table` / `shared_preprocessed_data` from
    their legacy state bridge, so this keeps the existing MUI workflow working
    until those services are migrated in later refactoring steps.
    """
    outputs = matching_service.get_shared_outputs()
    for key, value in outputs.items():
        if isinstance(value, pd.DataFrame):
            agent_runtime_state[key] = value.copy()
        elif key in agent_runtime_state:
            del agent_runtime_state[key]
    semi_service.set_shared_matching_table(outputs.get("shared_matching_table"))


def _sync_semi_outputs_to_legacy_state() -> None:
    """Expose semi-automatic reconciliation outputs to downstream services."""
    value = semi_service.get_shared_outputs().get("shared_reconciled_matching_table")
    if isinstance(value, pd.DataFrame):
        agent_runtime_state["shared_reconciled_matching_table"] = value.copy()
    else:
        agent_runtime_state.pop("shared_reconciled_matching_table", None)


def _service_snapshot(service_id: str) -> Dict[str, Any]:
    if service_id == "matching_table_generator":
        matching_service._init_state()
        return {"app": service_id, "snapshot": matching_service._build_snapshot()}
    if service_id == "semi_automatic_reconciliation":
        _sync_matching_outputs_to_legacy_state()
        semi_service.initialize_reconciliation_state()
        return {"app": service_id, "snapshot": semi_service.build_reconciliation_snapshot()}
    if service_id == "agent_reconciliation":
        _sync_matching_outputs_to_legacy_state()
        return _agent_args()
    if service_id == "rdf_generator":
        _sync_matching_outputs_to_legacy_state()
        rdf_generator_service._init_rdf_mui_state()
        return {"app": service_id, "snapshot": rdf_generator_service._build_rdf_snapshot()}
    if service_id == "rdf_to_table":
        rdf_to_table_service._init_rdf_to_table_state()
        return {"app": service_id, "snapshot": rdf_to_table_service._build_snapshot()}
    raise KeyError(service_id)


def _workspace_snapshot() -> Dict[str, Any]:
    return {"projects": project_store.list_projects(), "history": project_store.HISTORY.listing()}


def _workspace_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Cross-service workspace actions: project persistence + operation history."""
    etype = str(event.get("type", "") or "")
    if etype == "save_project":
        return {"saved": project_store.save_project(str(event.get("name", "")), event.get("project_id"))}
    if etype == "load_project":
        return {"loaded": project_store.load_project(str(event.get("project_id", "")))}
    if etype == "delete_project":
        return {"deleted": project_store.delete_project(str(event.get("project_id", "")))}
    if etype == "new_project":
        project_store.HISTORY.reset()
        return {"reset": True}
    if etype == "revert_to":
        # A stable entry id survives the history cap dropping the oldest entry; "index" is the legacy form.
        if event.get("id"):
            return {"reverted": project_store.HISTORY.revert_to(entry_id=str(event.get("id")))}
        try:
            index = int(event.get("index"))
        except (TypeError, ValueError):
            return {"reverted": False}
        return {"reverted": project_store.HISTORY.revert_to(index)}
    if etype == "undo":
        return {"undone": project_store.HISTORY.undo()}
    if etype == "export_recipe":
        return {"recipe_document": project_store.export_recipe()}
    if etype == "import_recipe":
        return project_store.import_recipe(event.get("recipe_document") or {})
    if etype == "list_projects":
        return {}
    return {"error": f"Unknown workspace action: {etype}"}


def _handoff_matching_table(event: Dict[str, Any]) -> None:
    """Load an up-to-date output, requiring a fresh confirmation for occupied targets."""
    with matching_service.STATE_LOCK:
        state = matching_service._STATE
        state["handoff"] = None
        target = event.get("target")
        if target not in {"semi_automatic_reconciliation", "agent_reconciliation"}:
            matching_service._set_status("error", "Choose a reconciliation workflow.")
            return
        if (not matching_service._result_is_current()
                or event.get("expected_revision") != state.get("configuration_revision", 0)):
            matching_service._set_status("error", "The result changed or needs updating. Review it before continuing.")
            return
        output = state.get("matching_df")
        if not isinstance(output, pd.DataFrame) or output.empty:
            matching_service._set_status("error", "There are no terms to transfer. Choose a table with usable terms.")
            return
        if target == "semi_automatic_reconciliation":
            target_state = semi_service.STATE
            existing = target_state.get("df")
            running = bool(target_state.get("processing_active"))
        else:
            target_state = agent_runtime_state
            existing = target_state.get(agent_service.AGENT_DATAFRAME_STATE_KEY)
            run_status = target_state.get(agent_service.AGENT_RUN_STATUS_STATE_KEY, {})
            thread = target_state.get(agent_service.AGENT_RUN_THREAD_STATE_KEY)
            running = bool((isinstance(run_status, dict) and run_status.get("running"))
                           or (hasattr(thread, "is_alive") and thread.is_alive()))
        if running:
            matching_service._set_status("error", "The selected workflow is running. Stop or finish it before replacing its table.")
            return
        if isinstance(existing, pd.DataFrame):
            fingerprint = existing.to_json(orient="split", date_format="iso", default_handler=str)
            token = hashlib.sha256(f"{target}:{state.get('configuration_revision')}:{fingerprint}".encode()).hexdigest()
            if event.get("confirmation_token") != token:
                state["handoff"] = {"status": "confirmation_required", "target": target,
                                    "confirmation_token": token, "rows": len(existing),
                                    "message": f"The selected workflow already contains a table with {len(existing)} rows."}
                matching_service._set_status("info", "Confirm the replacement or save the existing work as a project first.")
                return
        _sync_matching_outputs_to_legacy_state()
        if target == "semi_automatic_reconciliation":
            semi_service.initialize_reconciliation_state()
            semi_service.handle_reconciliation_event({"type": "load_shared_table"})
        else:
            # Loading inputs needs neither credentials nor a model catalog. Those are
            # prerequisites for starting a run, not for handing a table to configuration.
            agent_service._initialize_agent_reconciliation_state()
            agent_service._handle_agent_mui_event({"type": "load_shared_table"}, {}, {}, {})
        state["handoff"] = {"status": "complete", "target": target}
        matching_service._set_status("success", "Current terms transferred. Continue configuring reconciliation.")


def _handoff_reconciliation_to_rdf(event: Dict[str, Any]) -> None:
    state = semi_service.STATE
    state["rdf_handoff"] = None
    if state.get("processing_active"):
        state[semi_service.RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": "Stop or finish the search before continuing to RDF generation."}
        return
    df = state.get("df")
    if (not isinstance(df, pd.DataFrame) or df.empty
            or event.get("expected_revision") != state.get("review_revision", 0)):
        state[semi_service.RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "error", "text": "The table changed or is empty. Review the current result before continuing."}
        return
    rdf_generator_service._init_rdf_mui_state()
    current_mapping = agent_runtime_state.get("rdf_gen_mapping_df")
    current_data = agent_runtime_state.get("rdf_gen_data_df")
    fingerprints = [value.to_json(orient="split", default_handler=str) if isinstance(value, pd.DataFrame) else ""
                    for value in (current_mapping, current_data)]
    token = hashlib.sha256(json.dumps([state.get("review_revision", 0), *fingerprints]).encode()).hexdigest()
    if any(isinstance(value, pd.DataFrame) for value in (current_mapping, current_data)) and event.get("confirmation_token") != token:
        state["rdf_handoff"] = {"status": "confirmation_required", "confirmation_token": token,
                                "message": "The RDF workspace already contains data or mappings. Its mapping table will be replaced; existing source data will be kept. Cancel to save that workspace as a project first."}
        return
    semi_service.handle_reconciliation_event({"type": "publish_rdf_handoff"})
    _sync_semi_outputs_to_legacy_state()
    agent_runtime_state["all_terms_for_reconciliation"] = df["subject_label"].fillna("").astype(str).tolist()
    rdf_generator_service._load_shared_mapping()
    if not isinstance(current_data, pd.DataFrame) and isinstance(agent_runtime_state.get("shared_preprocessed_data"), pd.DataFrame):
        rdf_generator_service._load_shared_data()
    agent_runtime_state["rdf_generator_active_stage"] = rdf_generator_service.handoff_stage()
    state["rdf_handoff"] = {"status": "complete", "target": "rdf_generator"}
    state[semi_service.RECONCILIATION_MUI_STATUS_MESSAGE_KEY] = {"severity": "success", "text": "Current mappings transferred to RDF generation."}


def _agent_rdf_source_revision() -> str:
    table = agent_runtime_state.get(agent_service.AGENT_DATAFRAME_STATE_KEY)
    content = table.to_json(orient="split", default_handler=str) if isinstance(table, pd.DataFrame) else ""
    return hashlib.sha256(content.encode()).hexdigest()


def _handoff_agent_to_rdf(event: Dict[str, Any]) -> None:
    """Load a reviewed agent table only after source/target-bound confirmation."""
    state = agent_runtime_state
    state["agent_rdf_handoff"] = None
    status = state.get(agent_service.AGENT_RUN_STATUS_STATE_KEY) or {}
    table = state.get(agent_service.AGENT_DATAFRAME_STATE_KEY)
    revision = _agent_rdf_source_revision()
    if status.get("running") or not isinstance(table, pd.DataFrame) or table.empty or event.get("expected_revision") != revision:
        state["agent_mui_status_message"] = {"severity": "error", "text": "Stop any running agents and review the current table before continuing to RDF generation."}
        return
    rdf_generator_service._init_rdf_mui_state()
    current_data = state.get("rdf_gen_data_df")
    current_mapping = state.get("rdf_gen_mapping_df")
    target = [value.to_json(orient="split", default_handler=str) if isinstance(value, pd.DataFrame) else ""
              for value in (current_mapping, current_data)]
    target.extend([state.get(key) or "" for key in ("rdf_data", "skos_data", "dcat_catalog_data", "dcat_metadata_data")])
    target.append(rdf_generator_service._current_config_state())
    token = hashlib.sha256(json.dumps([revision, target], sort_keys=True, default=str).encode()).hexdigest()
    occupied = any(isinstance(value, pd.DataFrame) for value in (current_data, current_mapping)) or bool(state.get("rdf_data"))
    if occupied and event.get("confirmation_token") != token:
        state["agent_rdf_handoff"] = {"status": "confirmation_required", "confirmation_token": token,
            "message": "The RDF workspace already contains work. Its mapping table and generated RDF outputs will be replaced. Existing source data, templates and settings will be kept. Cancel to save that workspace as a project first."}
        return
    export_df = agent_service.finalize_accepted_results(table.copy(), provenance_defaults=agent_service._build_provenance_defaults_from_state())
    if export_df.empty:
        state["agent_mui_status_message"] = {"severity": "error", "text": "The current table has no rows to transfer."}
        return
    state["shared_reconciled_matching_table"] = export_df
    state["all_terms_for_reconciliation"] = export_df["subject_label"].fillna("").astype(str).tolist()
    rdf_generator_service._load_shared_mapping()
    if not isinstance(current_data, pd.DataFrame) and isinstance(state.get("shared_preprocessed_data"), pd.DataFrame):
        rdf_generator_service._load_shared_data()
    for key in ("rdf_data", "skos_data", "dcat_catalog_data", "dcat_metadata_data", "rdf_graph", "skos_graph", "rdf_preview", "last_named_graph_uri"):
        state[key] = None
    state["rdf_generator_active_stage"] = rdf_generator_service.handoff_stage()
    state["agent_rdf_handoff"] = {"status": "complete", "target": "rdf_generator"}
    state["agent_mui_status_message"] = {"severity": "success", "text": "Current agent mappings transferred to RDF generation."}


def _service_event(service_id: str, event: Dict[str, Any]) -> None:
    if service_id == "matching_table_generator":
        matching_service._init_state()
        if event.get("type") == "handoff_reconciliation":
            _handoff_matching_table(event)
        elif event.get("type") == "dismiss_handoff":
            matching_service._STATE["handoff"] = None
            matching_service._set_status("info", "Choose how to continue when you are ready.")
        else:
            matching_service._handle_mui_event(event)
    elif service_id == "semi_automatic_reconciliation":
        _sync_matching_outputs_to_legacy_state()
        semi_service.initialize_reconciliation_state()
        if event.get("type") == "handoff_rdf_generator":
            _handoff_reconciliation_to_rdf(event)
        elif event.get("type") == "dismiss_rdf_handoff":
            semi_service.STATE["rdf_handoff"] = None
        else:
            semi_service.handle_reconciliation_event(event)
        _sync_semi_outputs_to_legacy_state()
    elif service_id == "agent_reconciliation":
        _sync_matching_outputs_to_legacy_state()
        if event.get("type") == "handoff_rdf_generator":
            _handoff_agent_to_rdf(event)
        elif event.get("type") == "dismiss_rdf_handoff":
            agent_runtime_state["agent_rdf_handoff"] = None
        else:
            _agent_event(event)
    elif service_id == "rdf_generator":
        _sync_matching_outputs_to_legacy_state()
        rdf_generator_service._init_rdf_mui_state()
        rdf_generator_service._handle_rdf_mui_event(event)
    elif service_id == "rdf_to_table":
        rdf_to_table_service._init_rdf_to_table_state()
        rdf_to_table_service._handle_mui_event(event)
    else:
        raise KeyError(service_id)


def _bioportal_api_key() -> Any:
    key = os.environ.get("BIOPORTAL_API_KEY")
    if key:
        return key
    try:
        cfg = getattr(agent_service, "CONFIG", None) or {}
        return (cfg.get("bioportal", {}) or {}).get("api_key")
    except Exception:
        return None


def _registry_job_preview() -> Dict[str, Any]:
    api_key = _bioportal_api_key()
    if not api_key:
        return {"error": "missing_bioportal_api_key", "total_discovered": 0, "already_in_registry": 0, "planned_for_fetch": 0}
    acronyms = list_bioportal_ontology_acronyms(api_key)
    entries = _ONTOLOGY_REGISTRY.load()
    present = sum(1 for acr in acronyms if acr in entries)
    return {
        "total_discovered": len(acronyms),
        "already_in_registry": present,
        "planned_for_fetch": max(0, len(acronyms) - present),
        "has_api_key": True,
    }


def _start_registry_job(event: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(event.get("mode", "catalog") or "catalog")
    dry_run = bool(event.get("dry_run", False))
    confirmed = bool(event.get("confirmed", False))
    max_requests = event.get("max_requests")
    api_key = _bioportal_api_key()
    if not api_key and not dry_run:
        return {"error": "missing_bioportal_api_key"}
    if not confirmed and not dry_run:
        return {"error": "confirmation_required"}
    job_id = start_bioportal_catalog_refresh(
        mode=mode,
        all_bioportal=bool(event.get("all_bioportal", True)),
        acronyms=event.get("acronyms"),
        api_key=api_key,
        refresh=bool(event.get("refresh", False)),
        max_requests=int(max_requests) if str(max_requests or "").strip() not in {"", "None"} else None,
        dry_run=dry_run,
        confirmed=confirmed,
        run_async=True,
    )
    return {"job_id": job_id, "status": get_bioportal_registry_job_status(job_id)}


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "RDF4RiskMUIBackend/1.0"

    def _send_json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(_json_safe(payload), ensure_ascii=False).encode("utf-8")
        cors_origin = _resolve_cors_origin(self.headers.get("Origin"))
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        if cors_origin:
            self.send_header("Access-Control-Allow-Origin", cors_origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(encoded)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json({"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path.strip("/")
            if path == "api/health":
                self._send_json({"ok": True, "services": sorted(SERVICE_IDS)})
                return
            if path == "api/workspace/snapshot":
                self._send_json(_workspace_snapshot())
                return
            if path == "api/agent/registry-job/status":
                job_id = (urlparse(self.path).query.split("job_id=", 1)[-1].split("&", 1)[0]
                          if "job_id=" in urlparse(self.path).query else "")
                status = get_bioportal_registry_job_status(job_id) if job_id else latest_bioportal_registry_job()
                self._send_json({"status": status})
                return
            if path == "api/services/rdf_generator/data-table":
                # Read-only and on demand: the full table can be large, so it is not in the snapshot.
                query = dict(part.split("=", 1) for part in urlparse(self.path).query.split("&") if "=" in part)
                try:
                    limit = max(1, min(int(query.get("limit", "") or 0) or 5000, 20000))
                except ValueError:
                    limit = 5000
                self._send_json(rdf_generator_service.full_data_table(limit))
                return
            if path == "api/agent/registry-job/preview":
                self._send_json(_registry_job_preview())
                return
            parts = path.split("/")
            if len(parts) == 4 and parts[:2] == ["api", "services"] and parts[3] == "snapshot":
                service_id = parts[2]
                if service_id not in SERVICE_IDS:
                    self._send_json({"error": f"Unknown service: {service_id}"}, HTTPStatus.NOT_FOUND)
                    return
                self._send_json({"service": service_id, "args": _service_snapshot(service_id)})
                return
            self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            LOGGER.exception("GET request failed")
            self._send_json({"error": str(exc), "type": type(exc).__name__}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path.strip("/")
            if path == "api/workspace/event":
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > _max_event_body_bytes():
                    self._send_json({"error": "Event payload is too large."}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                    return
                event = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                if not isinstance(event, dict):
                    self._send_json({"error": "Event payload must be a JSON object."}, HTTPStatus.BAD_REQUEST)
                    return
                result = _workspace_event(event)
                self._send_json({**_workspace_snapshot(), **result})
                return
            if path in {"api/agent/registry-job/start", "api/agent/registry-job/cancel"}:
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > _max_event_body_bytes():
                    self._send_json({"error": "Event payload is too large."}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                    return
                event = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                if not isinstance(event, dict):
                    self._send_json({"error": "Event payload must be a JSON object."}, HTTPStatus.BAD_REQUEST)
                    return
                if path.endswith("/cancel"):
                    cancelled = cancel_bioportal_registry_job(str(event.get("job_id", "")))
                    self._send_json({"cancelled": bool(cancelled), "status": get_bioportal_registry_job_status(str(event.get("job_id", "")))})
                else:
                    self._send_json(_start_registry_job(event))
                return
            parts = path.split("/")
            if len(parts) == 4 and parts[:2] == ["api", "services"] and parts[3] == "event":
                service_id = parts[2]
                if service_id not in SERVICE_IDS:
                    self._send_json({"error": f"Unknown service: {service_id}"}, HTTPStatus.NOT_FOUND)
                    return
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > _max_event_body_bytes():
                    self._send_json({"error": "Event payload is too large."}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                    return
                event = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                if not isinstance(event, dict):
                    self._send_json({"error": "Event payload must be a JSON object."}, HTTPStatus.BAD_REQUEST)
                    return
                project_store.HISTORY.record_before(service_id, event)
                _service_event(service_id, event)
                self._send_json({"service": service_id, "args": _service_snapshot(service_id)})
                return
            self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            LOGGER.exception("POST request failed")
            self._send_json({"error": str(exc), "type": type(exc).__name__}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, fmt: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.address_string(), fmt % args)


def run(host: str, port: int) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    try:
        httpd = ThreadingHTTPServer((host, port), RequestHandler)
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            LOGGER.error(
                "Port %s on %s is already in use. Stop the existing RDF4Risk backend "
                "or start this one with --port <free-port>.",
                port,
                host,
            )
            sys.exit(1)
        raise
    LOGGER.info("RDF4Risk MUI backend listening on http://%s:%s", host, port)
    httpd.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the RDF4Risk Python backend for the web app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    run(args.host, args.port)
