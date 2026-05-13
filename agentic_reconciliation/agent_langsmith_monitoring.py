# -*- coding: utf-8 -*-
"""Lightweight helpers for optional LangSmith monitoring integration."""

from __future__ import annotations

import os
import threading
import time
from typing import Dict, List, Optional
from urllib.parse import quote


DEFAULT_LANGSMITH_PROJECT = "RDF4RiskAssessment"
DEFAULT_LANGSMITH_ENDPOINT = "https://api.smith.langchain.com"

_LLM_INTERACTION_LOCK = threading.Lock()
_LLM_INTERACTIONS: List[Dict[str, Optional[str]]] = []
_LLM_INTERACTION_LIMIT = 500


def _normalize_base_url(url: Optional[str]) -> str:
    base = str(url or "").strip() or "https://smith.langchain.com"
    return base.rstrip("/")


def resolve_langsmith_project(project: Optional[str]) -> str:
    resolved = str(project or "").strip()
    return resolved or DEFAULT_LANGSMITH_PROJECT


def configure_langsmith_environment(project: Optional[str], endpoint: Optional[str] = None) -> str:
    resolved_project = resolve_langsmith_project(project)
    resolved_endpoint = str(endpoint or os.getenv("LANGSMITH_ENDPOINT") or "").strip() or DEFAULT_LANGSMITH_ENDPOINT
    os.environ.setdefault("LANGSMITH_TRACING", "true")
    os.environ["LANGSMITH_PROJECT"] = resolved_project
    os.environ["LANGSMITH_ENDPOINT"] = resolved_endpoint
    return resolved_project


def build_project_url(project: str, endpoint: Optional[str] = None) -> str:
    base = _normalize_base_url(endpoint or os.getenv("LANGSMITH_ENDPOINT"))
    return f"{base}/o/default/projects/p/{quote(project, safe='')}"


def build_run_url(project: str, run_id: str, endpoint: Optional[str] = None) -> str:
    base = _normalize_base_url(endpoint or os.getenv("LANGSMITH_ENDPOINT"))
    return f"{base}/o/default/projects/p/{quote(project, safe='')}/r/{quote(run_id, safe='')}"


def get_langsmith_readiness(project: Optional[str]) -> Dict[str, Optional[str]]:
    project_name = resolve_langsmith_project(project)

    api_key = str(os.getenv("LANGSMITH_API_KEY") or "").strip()
    if not api_key:
        return {
            "enabled": True,
            "ready": False,
            "status": "missing_api_key",
            "message": "LANGSMITH_API_KEY is not set; showing local monitoring summary only.",
            "project": project_name,
            "project_url": build_project_url(project_name),
        }

    return {
        "enabled": True,
        "ready": True,
        "status": "ready",
        "message": "LangSmith credentials detected. Monitoring metadata will be attached to this run.",
        "project": project_name,
        "project_url": build_project_url(project_name),
    }


def reset_llm_interactions() -> None:
    with _LLM_INTERACTION_LOCK:
        _LLM_INTERACTIONS.clear()


def record_llm_interaction(
    *,
    provider: str,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    response_text: Optional[str] = None,
    error: Optional[str] = None,
    interaction_purpose: Optional[str] = None,
    term_id: Optional[str] = None,
    run_id: Optional[str] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    cost_usd: Optional[float] = None,
    token_estimate: Optional[int] = None,
) -> None:
    with _LLM_INTERACTION_LOCK:
        _LLM_INTERACTIONS.append(
            {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "provider": str(provider or ""),
                "model_name": str(model_name or ""),
                "system_prompt": str(system_prompt or ""),
                "user_prompt": str(user_prompt or ""),
                "response_text": str(response_text or ""),
                "error": str(error or ""),
                "interaction_purpose": str(interaction_purpose or ""),
                "term_id": str(term_id or ""),
                "run_id": str(run_id or ""),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_usd": cost_usd,
                "token_estimate": "" if token_estimate is None else str(token_estimate),
            }
        )
        if len(_LLM_INTERACTIONS) > _LLM_INTERACTION_LIMIT:
            del _LLM_INTERACTIONS[: len(_LLM_INTERACTIONS) - _LLM_INTERACTION_LIMIT]


def get_llm_interactions(limit: int = 200) -> List[Dict[str, Optional[str]]]:
    limit = max(1, int(limit or 1))
    with _LLM_INTERACTION_LOCK:
        return list(_LLM_INTERACTIONS[-limit:])
