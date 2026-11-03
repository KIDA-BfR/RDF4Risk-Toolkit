"""Live ChatGPT model discovery and shared model-selection validation."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
from functools import lru_cache
from typing import Any, Dict

import requests

from .agent_codex_subscription_service import _codex_manager

CHATGPT_MODELS_ENDPOINT = "https://chatgpt.com/backend-api/codex/models"
# Documented schema compatibility target if no Codex CLI is installed.
DEFAULT_CODEX_CATALOG_VERSION = "0.162.1"
MODEL_CATALOG_TTL_SECONDS = 300
MODEL_CATALOG_ERROR_TTL_SECONDS = 15


@lru_cache(maxsize=1)
def _installed_codex_version() -> str:
    executable = shutil.which("codex")
    if executable:
        try:
            result = subprocess.run([executable, "--version"], capture_output=True, text=True, timeout=3, check=True)
            match = re.search(r"codex-cli\s+(\d+\.\d+\.\d+)", result.stdout)
            if match:
                return match.group(1)
        except (OSError, subprocess.SubprocessError):
            pass
    return DEFAULT_CODEX_CATALOG_VERSION


def codex_catalog_client_version() -> str:
    override = os.getenv("RDF4RISK_CODEX_CLIENT_VERSION", "").strip()
    match = re.fullmatch(r"(\d+\.\d+\.\d+)(?:[-+][A-Za-z0-9.-]+)?", override)
    return match.group(1) if match else _installed_codex_version()


def model_catalog_identity(provider: str, api_key: str = "") -> str:
    """Invalidate on credential/account changes without putting secrets in cache keys."""
    if provider == "openai_codex":
        try:
            credentials = _codex_manager._load_credentials() or {}
        except Exception:
            return "unavailable"
        # Include a credential fingerprint for accounts without an account_id.
        api_key = str(credentials.get("access_token", ""))
        account = f"{credentials.get('account_id', '')}:{codex_catalog_client_version()}"
    else:
        account = ""
    return hashlib.sha256(f"{account}:{api_key}".encode()).hexdigest()


def fetch_chatgpt_model_catalog() -> Dict[str, Any]:
    """Fetch the selected account's picker catalog, never substituting API-key models."""
    result = {"provider": "openai_codex", "source": "fetch_failed", "models": []}
    try:
        token = _codex_manager.get_access_token()
        if not token:
            return {**result, "source": "missing_credentials", "message": "Sign in to ChatGPT to load available models."}
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "originator": "rdf4risk-toolkit",
            "User-Agent": "RDF4Risk-Toolkit/agent-reconciliation",
        }
        params = {"client_version": codex_catalog_client_version()}
        account_id = _codex_manager.get_account_id()
        if account_id:
            headers["ChatGPT-Account-Id"] = account_id
        response = requests.get(CHATGPT_MODELS_ENDPOINT, headers=headers, params=params, timeout=20)
        if response.status_code == 401:
            token = _codex_manager.force_refresh_access_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
                response = requests.get(CHATGPT_MODELS_ENDPOINT, headers=headers, params=params, timeout=20)
        if response.status_code == 401:
            return {**result, "source": "auth_required", "http_status": 401,
                    "message": "Codex sign-in expired or was rejected (HTTP 401). Sign in again and reload models."}
        if response.status_code == 403:
            return {**result, "source": "access_denied", "http_status": 403,
                    "message": "Codex model catalog access was denied (HTTP 403). Check the selected ChatGPT account/workspace, Codex access and network restrictions."}
        if not response.ok:
            return {**result, "http_status": response.status_code,
                    "message": f"Codex model catalog request failed (HTTP {response.status_code}). Retry or check the Codex client version and account access."}
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("models"), list):
            return {**result, "message": "ChatGPT returned an unsupported model catalog. Reload models or sign in again."}
        models = []
        seen = set()
        items = [item for item in payload["models"] if isinstance(item, dict)]
        items.sort(key=lambda item: item.get("priority") if isinstance(item.get("priority"), (int, float)) else float("inf"))
        for item in items:
            if not isinstance(item, dict) or item.get("visibility") != "list":
                continue
            model_id = str(item.get("slug", "") or "").strip()
            if not model_id or model_id in seen:
                continue
            seen.add(model_id)
            models.append({
                "provider": "openai_codex", "model_id": model_id,
                "display_name": str(item.get("display_name") or model_id),
                "supported_reasoning_levels": item.get("supported_reasoning_levels", []),
                "default_reasoning_level": item.get("default_reasoning_level"),
            })
        return {**result, "source": "live", "models": models, "message": (
            "Available Codex models loaded from your ChatGPT account." if models
            else "Your ChatGPT account returned no selectable Codex models. Reload models or check account access."
        )}
    except requests.RequestException:
        return {**result, "message": "Could not load ChatGPT models. Check your connection and use Reload models & pricing."}
    except Exception:
        # Do not expose token-store or provider exception payloads to the browser/logs.
        return {**result, "message": "Could not read the ChatGPT session or model catalog. Check secure sign-in storage and sign in again."}


def validate_model_selection(config: Dict[str, Any], catalog_for_provider) -> list[str]:
    """Validate every model used by the workflow against its own provider's catalog."""
    provider = str(config.get("provider", "openai"))
    selections = [(provider, config.get("model"), "Reconciliation")]
    override = config.get("custom_model_override")
    if override:
        selections.append((provider, override, "Custom model"))
    if config.get("expert_mode"):
        if config.get("use_different_models"):
            selections.append((provider, config.get("definition_model"), "Definition"))
        selections.append((str(config.get("planner_provider") or provider), config.get("planner_model") or config.get("model"), "Planner"))
    errors = []
    catalogs = {}
    for selected_provider, model, label in selections:
        if selected_provider not in catalogs:
            env_name = config.get("provider_api_key_env") if selected_provider == provider else None
            catalogs[selected_provider] = (
                catalog_for_provider(selected_provider, api_key_env=env_name)
                if env_name else catalog_for_provider(selected_provider)
            )
        catalog = catalogs[selected_provider]
        ids = {r.get("model_id") for r in catalog.get("models", []) if isinstance(r, dict)}
        if not ids:
            message = str(catalog.get("message") or "No available models. Reload models.")
            if message not in errors:
                errors.append(message)
        elif not model or model not in ids:
            errors.append(f"{label} model '{model or '(none)'}' is not in the current provider catalog. Select an available model.")
    return errors
