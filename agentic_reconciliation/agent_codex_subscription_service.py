# -*- coding: utf-8 -*-
"""OAuth + request helpers for ChatGPT Subscription (OpenAI Codex backend).

This module mirrors the high-level flow used in Cline:
- PKCE OAuth against auth.openai.com
- localhost callback handling
- refresh-token based session reuse
- requests routed to chatgpt.com/backend-api/codex/responses

Important operational note:
This integration depends on undocumented/private endpoints and may change
without notice.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import time
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests

try:
    import keyring
    from keyring.errors import KeyringError
except ImportError:  # pragma: no cover - exercised only when dependency is missing at runtime
    keyring = None  # type: ignore[assignment]

    class KeyringError(Exception):
        pass


OPENAI_CODEX_OAUTH_CONFIG = {
    "authorization_endpoint": "https://auth.openai.com/oauth/authorize",
    "token_endpoint": "https://auth.openai.com/oauth/token",
    # Public client identifier observed in Cline's implementation
    "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
    "redirect_uri": "http://localhost:1455/auth/callback",
    "scope": "openid profile email offline_access",
    "callback_port": 1455,
}

CODEX_RESPONSES_ENDPOINT = "https://chatgpt.com/backend-api/codex/responses"
CODEX_CREDENTIAL_SERVICE = "RDF4Risk-Toolkit"
CODEX_CREDENTIAL_USERNAME = "openai_codex_oauth"


def _is_unsupported_reasoning_parameter_error(status_code: int, response_text: str) -> bool:
    if int(status_code or 0) not in {400, 422}:
        return False
    text = str(response_text or "").strip().lower()
    if not text:
        return False
    has_unsupported_hint = any(token in text for token in ["unsupported parameter", "unknown parameter", "not supported"])
    has_reasoning_hint = any(token in text for token in ["reasoning_effort", "reasoning.effort", "reasoning"])
    return has_unsupported_hint and has_reasoning_hint


def _build_codex_request_body(
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    reasoning_effort: str,
) -> Dict[str, Any]:
    body = {
        "model": model_name,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": user_prompt}]}],
        "instructions": system_prompt,
        # Codex backend currently requires stream=true.
        "stream": True,
        "store": False,
    }
    normalized_reasoning = str(reasoning_effort or "none").strip().lower() or "none"
    if normalized_reasoning != "none":
        body["reasoning_effort"] = normalized_reasoning
    return body


def _token_store_path() -> Path:
    """Return the legacy plaintext token path, used only for cleanup/migration."""
    return Path.home() / ".rdf4risk" / "openai_codex_oauth.json"


def _require_keyring_available() -> None:
    if keyring is None:
        raise RuntimeError(
            "Secure credential storage requires the 'keyring' package. Install project requirements and sign in again."
        )


def _load_credentials_from_keyring() -> Optional[Dict[str, Any]]:
    _require_keyring_available()
    try:
        raw = keyring.get_password(CODEX_CREDENTIAL_SERVICE, CODEX_CREDENTIAL_USERNAME)
    except KeyringError as exc:
        raise RuntimeError(f"Unable to read ChatGPT Subscription credentials from the OS credential store: {exc}") from exc
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Stored ChatGPT Subscription credentials are invalid. Please sign in again.") from exc
    return parsed if isinstance(parsed, dict) else None


def _save_credentials_to_keyring(credentials: Dict[str, Any]) -> None:
    _require_keyring_available()
    try:
        keyring.set_password(
            CODEX_CREDENTIAL_SERVICE,
            CODEX_CREDENTIAL_USERNAME,
            json.dumps(credentials, separators=(",", ":")),
        )
    except KeyringError as exc:
        raise RuntimeError(f"Unable to save ChatGPT Subscription credentials to the OS credential store: {exc}") from exc


def _delete_credentials_from_keyring() -> None:
    if keyring is None:
        return
    try:
        keyring.delete_password(CODEX_CREDENTIAL_SERVICE, CODEX_CREDENTIAL_USERNAME)
    except KeyringError:
        pass


def _delete_legacy_token_file() -> None:
    path = _token_store_path()
    if path.exists():
        try:
            path.unlink()
        except Exception:
            pass


def _load_legacy_credentials_for_migration() -> Optional[Dict[str, Any]]:
    path = _token_store_path()
    if not path.exists():
        return None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _decode_jwt_claims(token: Optional[str]) -> Dict[str, Any]:
    if not token:
        return {}
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        raw = base64.urlsafe_b64decode(payload.encode("utf-8")).decode("utf-8")
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _extract_account_id(id_token: Optional[str], access_token: Optional[str]) -> Optional[str]:
    def _from_claims(claims: Dict[str, Any]) -> Optional[str]:
        if not claims:
            return None
        direct = claims.get("chatgpt_account_id")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        nested = claims.get("https://api.openai.com/auth")
        if isinstance(nested, dict):
            nested_id = nested.get("chatgpt_account_id")
            if isinstance(nested_id, str) and nested_id.strip():
                return nested_id.strip()

        organizations = claims.get("organizations")
        if isinstance(organizations, list) and organizations:
            first = organizations[0]
            if isinstance(first, dict):
                org_id = first.get("id")
                if isinstance(org_id, str) and org_id.strip():
                    return org_id.strip()
        return None

    account_id = _from_claims(_decode_jwt_claims(id_token))
    if account_id:
        return account_id
    return _from_claims(_decode_jwt_claims(access_token))


def _extract_output_text(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""

    chunks: list[str] = []

    def _add(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            chunks.append(value.strip())

    direct = payload.get("output_text")
    if isinstance(direct, str):
        _add(direct)
    elif isinstance(direct, dict):
        _add(direct.get("text"))
        content = direct.get("content")
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    _add(part.get("text"))
                    _add(part.get("output_text"))

    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue

            _add(item.get("text"))
            _add(item.get("output_text"))

            content = item.get("content")
            if isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    _add(part.get("text"))
                    _add(part.get("output_text"))

            message = item.get("message")
            if isinstance(message, dict):
                _add(message.get("content"))

    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0] if isinstance(choices[0], dict) else {}
        message = first_choice.get("message") if isinstance(first_choice, dict) else None
        if isinstance(message, dict):
            content = message.get("content")
            _add(content)

    top_message = payload.get("message")
    if isinstance(top_message, dict):
        _add(top_message.get("content"))

    # Some payloads nest content under "response".
    nested_response = payload.get("response")
    if isinstance(nested_response, dict):
        nested_text = _extract_output_text(nested_response)
        _add(nested_text)

    # SSE reconstruction fallback.
    events = payload.get("_events")
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            _add(event.get("delta"))
            _add(event.get("text"))
            _add(event.get("output_text"))

            event_content = event.get("content")
            if isinstance(event_content, str):
                _add(event_content)
            elif isinstance(event_content, list):
                for part in event_content:
                    if isinstance(part, dict):
                        _add(part.get("text"))
                        _add(part.get("output_text"))

            event_nested = _extract_output_text(event)
            _add(event_nested)

    if chunks:
        return "\n".join(chunks).strip()
    return ""


def _parse_codex_response_payload(response: requests.Response) -> Dict[str, Any]:
    """Parse both JSON and SSE-style codex responses into a payload dict."""
    if response.content:
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                events = [item for item in parsed if isinstance(item, dict)]
                if events:
                    payload = events[-1].copy()
                    payload["_events"] = events
                    return payload
        except Exception:
            pass

    text = (response.text or "").strip()
    if not text:
        return {}

    # SSE fallback: keep all JSON events sent as `data: {...}`.
    events: list[Dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        chunk = line[5:].strip()
        if not chunk or chunk == "[DONE]":
            continue
        try:
            parsed = json.loads(chunk)
            if isinstance(parsed, dict):
                events.append(parsed)
        except Exception:
            pass

    if events:
        payload = events[-1].copy()
        payload["_events"] = events
        return payload
    return {}


class _OpenAiCodexOAuthManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._pending_state: Optional[str] = None
        self._pending_code_verifier: Optional[str] = None
        self._pending_auth_url: Optional[str] = None
        self._pending_error: Optional[str] = None
        self._server: Optional[ThreadingHTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None

    # ---------- credential persistence ----------
    def _load_credentials(self) -> Optional[Dict[str, Any]]:
        credentials = _load_credentials_from_keyring()
        if credentials:
            _delete_legacy_token_file()
            return credentials

        legacy_credentials = _load_legacy_credentials_for_migration()
        if legacy_credentials:
            _save_credentials_to_keyring(legacy_credentials)
            _delete_legacy_token_file()
            return legacy_credentials

        _delete_legacy_token_file()
        return None

    def _save_credentials(self, credentials: Dict[str, Any]) -> None:
        _save_credentials_to_keyring(credentials)
        _delete_legacy_token_file()

    def clear_credentials(self) -> None:
        _delete_credentials_from_keyring()
        _delete_legacy_token_file()

    # ---------- auth status ----------
    def is_authenticated(self) -> bool:
        creds = self._load_credentials()
        if not creds:
            return False
        refresh_token = creds.get("refresh_token")
        return isinstance(refresh_token, str) and bool(refresh_token.strip())

    def get_auth_status(self) -> Dict[str, Any]:
        creds = self._load_credentials() or {}
        return {
            "authenticated": self.is_authenticated(),
            "pending": bool(self._pending_state),
            "pending_auth_url": self._pending_auth_url,
            "error": self._pending_error,
            "email": creds.get("email"),
            "account_id": creds.get("account_id"),
            "expires_at": creds.get("expires_at"),
        }

    def get_pending_authorization_url(self) -> Optional[str]:
        return self._pending_auth_url

    # ---------- oauth start + callback handling ----------
    def start_authorization_flow(self, open_browser: bool = True) -> str:
        with self._lock:
            self._stop_callback_server_locked()

            code_verifier = _base64url(secrets.token_bytes(32))
            code_challenge = _base64url(hashlib.sha256(code_verifier.encode("utf-8")).digest())
            state = secrets.token_hex(16)

            params = {
                "client_id": OPENAI_CODEX_OAUTH_CONFIG["client_id"],
                "redirect_uri": OPENAI_CODEX_OAUTH_CONFIG["redirect_uri"],
                "scope": OPENAI_CODEX_OAUTH_CONFIG["scope"],
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
                "response_type": "code",
                "state": state,
                # mirrored from Cline/Codex CLI behavior
                "codex_cli_simplified_flow": "true",
                "originator": "cline",
            }
            query = "&".join(
                f"{k}={requests.utils.quote(str(v), safe='')}" for k, v in params.items()  # type: ignore[attr-defined]
            )
            auth_url = f"{OPENAI_CODEX_OAUTH_CONFIG['authorization_endpoint']}?{query}"

            self._pending_state = state
            self._pending_code_verifier = code_verifier
            self._pending_auth_url = auth_url
            self._pending_error = None
            self._start_callback_server_locked()

        if open_browser:
            webbrowser.open(auth_url, new=2, autoraise=True)
        return auth_url

    def _start_callback_server_locked(self) -> None:
        manager = self

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path != "/auth/callback":
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(b"Not Found")
                    return

                query = parse_qs(parsed.query)
                code = (query.get("code") or [None])[0]
                state = (query.get("state") or [None])[0]
                error = (query.get("error") or [None])[0]

                if error:
                    manager._complete_pending_with_error(f"OAuth error: {error}")
                    self._send_html(400, "Authentication failed", f"OpenAI reported an OAuth error: {error}")
                    manager._shutdown_callback_server_async()
                    return

                if not code or not state:
                    manager._complete_pending_with_error("Missing code or state in OAuth callback")
                    self._send_html(400, "Authentication failed", "Missing required callback parameters.")
                    manager._shutdown_callback_server_async()
                    return

                try:
                    manager._handle_callback_success(code=code, state=state)
                    self._send_html(
                        200,
                        "Authentication successful",
                        "You are now signed in. You can close this window and return to RDF4Risk-Toolkit.",
                    )
                except Exception as exc:
                    manager._complete_pending_with_error(str(exc))
                    self._send_html(500, "Authentication failed", f"Token exchange failed: {exc}")
                finally:
                    manager._shutdown_callback_server_async()

            def log_message(self, *_args, **_kwargs):  # noqa: A003
                return

            def _send_html(self, status: int, title: str, message: str):
                body = f"""
                <html><head><meta charset='utf-8'><title>{title}</title></head>
                <body style='font-family: sans-serif; max-width: 720px; margin: 2rem auto;'>
                  <h2>{title}</h2>
                  <p>{message}</p>
                </body></html>
                """
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(body.encode("utf-8"))

        self._server = ThreadingHTTPServer(("localhost", OPENAI_CODEX_OAUTH_CONFIG["callback_port"]), CallbackHandler)
        self._server_thread = threading.Thread(target=self._server.serve_forever, name="codex-oauth-callback", daemon=True)
        self._server_thread.start()

    def _stop_callback_server_locked(self) -> None:
        server = self._server
        if server:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                server.server_close()
            except Exception:
                pass
        self._server = None
        self._server_thread = None
        self._pending_state = None
        self._pending_code_verifier = None
        self._pending_auth_url = None

    def _shutdown_callback_server_async(self) -> None:
        def _shutdown():
            with self._lock:
                self._stop_callback_server_locked()

        threading.Thread(target=_shutdown, daemon=True).start()

    def _complete_pending_with_error(self, message: str) -> None:
        self._pending_error = message
        self._pending_state = None
        self._pending_code_verifier = None

    def _handle_callback_success(self, code: str, state: str) -> None:
        if state != self._pending_state:
            raise RuntimeError("OAuth state mismatch. Please retry sign-in.")
        verifier = self._pending_code_verifier
        if not verifier:
            raise RuntimeError("Missing PKCE verifier for token exchange.")

        creds = self._exchange_code_for_tokens(code, verifier)
        self._save_credentials(creds)
        self._pending_error = None
        self._pending_state = None
        self._pending_code_verifier = None

    def _exchange_code_for_tokens(self, code: str, code_verifier: str) -> Dict[str, Any]:
        form = {
            "grant_type": "authorization_code",
            "client_id": OPENAI_CODEX_OAUTH_CONFIG["client_id"],
            "code": code,
            "redirect_uri": OPENAI_CODEX_OAUTH_CONFIG["redirect_uri"],
            "code_verifier": code_verifier,
        }
        response = requests.post(
            OPENAI_CODEX_OAUTH_CONFIG["token_endpoint"],
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Token exchange failed ({response.status_code}): {response.text}")

        payload = response.json() if response.content else {}
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        expires_in = int(payload.get("expires_in", 0) or 0)
        if not access_token or not refresh_token:
            raise RuntimeError("Token exchange succeeded but refresh/access token was missing.")

        return {
            "type": "openai_codex",
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": int(time.time()) + max(expires_in, 60),
            "email": payload.get("email"),
            "account_id": _extract_account_id(payload.get("id_token"), access_token),
            "updated_at": int(time.time()),
        }

    # ---------- token retrieval ----------
    def _is_expired(self, credentials: Dict[str, Any], buffer_seconds: int = 300) -> bool:
        expires_at = int(credentials.get("expires_at", 0) or 0)
        return time.time() >= max(expires_at - buffer_seconds, 0)

    def _refresh_access_token(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        refresh_token = credentials.get("refresh_token")
        if not refresh_token:
            raise RuntimeError("No refresh token available. Please sign in again.")

        form = {
            "grant_type": "refresh_token",
            "client_id": OPENAI_CODEX_OAUTH_CONFIG["client_id"],
            "refresh_token": refresh_token,
        }
        response = requests.post(
            OPENAI_CODEX_OAUTH_CONFIG["token_endpoint"],
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Token refresh failed ({response.status_code}): {response.text}")

        payload = response.json() if response.content else {}
        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 0) or 0)
        if not access_token:
            raise RuntimeError("Token refresh returned no access token.")

        refreshed = {
            **credentials,
            "access_token": access_token,
            "refresh_token": payload.get("refresh_token") or credentials.get("refresh_token"),
            "expires_at": int(time.time()) + max(expires_in, 60),
            "email": payload.get("email") or credentials.get("email"),
            "account_id": _extract_account_id(payload.get("id_token"), access_token) or credentials.get("account_id"),
            "updated_at": int(time.time()),
        }
        self._save_credentials(refreshed)
        return refreshed

    def get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        creds = self._load_credentials()
        if not creds:
            return None

        if force_refresh or self._is_expired(creds):
            try:
                creds = self._refresh_access_token(creds)
            except Exception:
                return None
        token = creds.get("access_token")
        return token if isinstance(token, str) and token.strip() else None

    def force_refresh_access_token(self) -> Optional[str]:
        return self.get_access_token(force_refresh=True)

    def get_account_id(self) -> Optional[str]:
        creds = self._load_credentials() or {}
        account_id = creds.get("account_id")
        if isinstance(account_id, str) and account_id.strip():
            return account_id.strip()
        return None


_codex_manager = _OpenAiCodexOAuthManager()


def is_codex_authenticated() -> bool:
    return _codex_manager.is_authenticated()


def get_codex_auth_status() -> Dict[str, Any]:
    return _codex_manager.get_auth_status()


def get_pending_codex_authorization_url() -> Optional[str]:
    return _codex_manager.get_pending_authorization_url()


def start_codex_authorization_flow(open_browser: bool = True) -> str:
    return _codex_manager.start_authorization_flow(open_browser=open_browser)


def clear_codex_credentials() -> None:
    _codex_manager.clear_credentials()


def create_codex_response(
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float = 0,
    max_tokens: int = 1024,
    reasoning_effort: str = "none",
) -> str:
    """Generate a completion via ChatGPT Subscription Codex backend."""
    access_token = _codex_manager.get_access_token()
    if not access_token:
        raise RuntimeError(
            "ChatGPT Subscription is not authenticated. Please sign in from the Agent-Based Reconciliation settings."
        )

    account_id = _codex_manager.get_account_id()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "originator": "rdf4risk-toolkit",
        "session_id": str(uuid.uuid4()),
        "User-Agent": "RDF4Risk-Toolkit/agent-reconciliation",
    }
    if account_id:
        headers["ChatGPT-Account-Id"] = account_id

    body = _build_codex_request_body(
        model_name=model_name,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        reasoning_effort=reasoning_effort,
    )

    response = requests.post(CODEX_RESPONSES_ENDPOINT, json=body, headers=headers, timeout=120)
    if response.status_code in {401, 403}:
        refreshed = _codex_manager.force_refresh_access_token()
        if not refreshed:
            raise RuntimeError("ChatGPT Subscription authentication expired. Please sign in again.")
        headers["Authorization"] = f"Bearer {refreshed}"
        response = requests.post(CODEX_RESPONSES_ENDPOINT, json=body, headers=headers, timeout=120)

    if not response.ok and "reasoning_effort" in body:
        if _is_unsupported_reasoning_parameter_error(response.status_code, response.text):
            softened_body = dict(body)
            softened_body.pop("reasoning_effort", None)
            response = requests.post(CODEX_RESPONSES_ENDPOINT, json=softened_body, headers=headers, timeout=120)

    if not response.ok:
        raise RuntimeError(f"Codex request failed ({response.status_code}): {response.text}")

    payload = _parse_codex_response_payload(response)
    text = _extract_output_text(payload if isinstance(payload, dict) else {})
    if not text:
        raise RuntimeError("Codex request succeeded but returned no text content.")
    return text
