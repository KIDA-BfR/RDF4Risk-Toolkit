"""HTTP backend for the RDF4Risk web app.

This module exposes JSON snapshots and event handlers for the browser app.
Python services own workflow state and business logic; React/Material UI owns
all frontend rendering.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Tuple
from urllib.parse import urlparse

import pandas as pd
from Matching_Table_Generator import generator as matching_service
from RDF_Generator import app as rdf_generator_service
from RDF_to_Table import tablegenerator as rdf_to_table_service
from agentic_reconciliation import agent_reconciliation_service as agent_service
from agentic_reconciliation.agent_runtime_state import runtime_state as agent_runtime_state
from agentic_reconciliation.agent_llm_service import get_default_model_options, get_provider_label, get_supported_llm_providers
from semi_automatic_reconciliation import reconciliation_service as semi_service

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
    if not primary_models:
        primary_models = list(get_default_model_options(primary_provider))
    selected_model = str(agent_runtime_state.get("agent_model_name", config.get("model", primary_models[0] if primary_models else "gpt-5.1")) or "gpt-5.1")
    for model_candidate in (
        selected_model,
        str(agent_runtime_state.get("agent_custom_model_override", "") or "").strip(),
        str(agent_runtime_state.get("agent_definition_model_name", "") or "").strip(),
        str(agent_runtime_state.get("agent_planner_model_name", "") or "").strip(),
    ):
        if model_candidate and model_candidate not in primary_models:
            primary_models.append(model_candidate)

    config["provider"] = primary_provider
    config["model"] = selected_model

    provider_options = list(defaults.get("provider_options", get_supported_llm_providers()) or ["openai"])
    provider_labels = {provider: get_provider_label(provider) for provider in provider_options}
    model_labels = {model: agent_service._format_model_option_label(primary_catalog, model) for model in primary_models}
    model_details = agent_service._format_model_details_caption(primary_catalog, selected_model) if primary_catalog else None
    ontology_options = sorted(
        set(
            list((agent_service.CONFIG or {}).get("agent_reconciliation", {}).get("trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"]))
            + list((agent_service.CONFIG or {}).get("agent_reconciliation", {}).get("bioportal_agent_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"]))
            + ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON", "NIFSTD", "BERO", "OCHV", "SNOMEDCT", "CHEBI", "QUDT"]
        )
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
    for key, value in semi_service.get_shared_outputs().items():
        if isinstance(value, pd.DataFrame):
            agent_runtime_state[key] = value.copy()


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


def _service_event(service_id: str, event: Dict[str, Any]) -> None:
    if service_id == "matching_table_generator":
        matching_service._init_state()
        matching_service._handle_mui_event(event)
    elif service_id == "semi_automatic_reconciliation":
        _sync_matching_outputs_to_legacy_state()
        semi_service.initialize_reconciliation_state()
        semi_service.handle_reconciliation_event(event)
        _sync_semi_outputs_to_legacy_state()
    elif service_id == "agent_reconciliation":
        _sync_matching_outputs_to_legacy_state()
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
    httpd = ThreadingHTTPServer((host, port), RequestHandler)
    LOGGER.info("RDF4Risk MUI backend listening on http://%s:%s", host, port)
    httpd.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the RDF4Risk Python backend for the web app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    run(args.host, args.port)
