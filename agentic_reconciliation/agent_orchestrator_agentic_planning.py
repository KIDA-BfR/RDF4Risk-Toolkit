# -*- coding: utf-8 -*-
"""Agentic planning helpers for Wikidata reconciliation workflows."""

from __future__ import annotations

import json
from typing import List

from .agent_llm_service import generate_structured_completion
from .agent_models import (
    AgentCandidate,
    AgentRunConfig,
    AgenticExecutionStats,
    AgenticPlan,
    AgenticPlanAction,
    CandidateScore,
)
from .agent_orchestrator_runtime import (
    _is_valid_qid,
    _resolve_planner_api_key_env,
    _resolve_planner_model,
    _resolve_planner_provider,
)
from .agent_wikidata_service import (
    dedupe_candidates,
    load_candidate_by_qid,
    search_wikidata_candidates_multiquery,
    search_wikidata_candidates_with_options,
)

ALLOWED_AGENTIC_ACTIONS = {
    "rewrite_query",
    "broaden_query",
    "narrow_query",
    "request_alias_search",
    "focus_related_concepts",
    "inspect_specific_qid",
}


def _generate_agentic_plan(
    term: str,
    definition: str,
    top_candidates: List[CandidateScore],
    config: AgentRunConfig,
    stats: AgenticExecutionStats,
) -> AgenticPlan:
    if stats.planner_calls_used >= max(0, int(config.agentic_max_planner_calls or 0)):
        return AgenticPlan(actions=[], stop_reason="planner_budget_exhausted", confidence_note="")

    planner_provider = _resolve_planner_provider(config)
    planner_model_name = _resolve_planner_model(config)
    planner_api_key_env = _resolve_planner_api_key_env(config)

    candidate_summary = [
        {
            "label": cs.candidate.label,
            "qid": cs.candidate.raw_identifier,
            "mapping_type": cs.mapping_type,
            "confidence": round(cs.confidence, 4),
        }
        for cs in (top_candidates or [])[:5]
    ]

    system_prompt = (
        "You are a constrained planner for Wikidata reconciliation. "
        "Return JSON only with keys actions (array), stop_reason, confidence_note."
    )
    user_prompt = (
        f"Input term: {term}\n"
        f"Definition: {definition}\n"
        f"Top candidates: {json.dumps(candidate_summary, ensure_ascii=False)}\n\n"
        "Allowed action_type values only: "
        "rewrite_query, broaden_query, narrow_query, request_alias_search, "
        "focus_related_concepts, inspect_specific_qid.\n"
        "Each action must be an object with keys action_type, payload, reason."
    )

    stats.planner_calls_used += 1
    stats.total_llm_calls_used += 1

    try:
        payload = generate_structured_completion(
            planner_provider,
            planner_model_name,
            system_prompt,
            user_prompt,
            api_key_env=planner_api_key_env,
            temperature=0,
            max_tokens=700,
            reasoning_effort=config.reasoning_effort,
            retries_on_parse_failure=1,
            interaction_purpose="planner",
            term_id=term,
        )
    except Exception:
        return AgenticPlan(actions=[], stop_reason="planner_error", confidence_note="")

    actions_payload = payload.get("actions", []) if isinstance(payload, dict) else []
    actions: List[AgenticPlanAction] = []
    if isinstance(actions_payload, list):
        for item in actions_payload:
            if not isinstance(item, dict):
                continue
            action_type = str(item.get("action_type", "")).strip()
            if action_type not in ALLOWED_AGENTIC_ACTIONS:
                continue
            raw_payload = item.get("payload", {})
            actions.append(
                AgenticPlanAction(
                    action_type=action_type,
                    payload=raw_payload if isinstance(raw_payload, dict) else {},
                    reason=str(item.get("reason", "") or ""),
                )
            )

    return AgenticPlan(
        actions=actions,
        stop_reason=str(payload.get("stop_reason", "") if isinstance(payload, dict) else "") or "planned",
        confidence_note=str(payload.get("confidence_note", "") if isinstance(payload, dict) else ""),
    )


def _execute_agentic_plan_actions(
    plan: AgenticPlan,
    term: str,
    config: AgentRunConfig,
    stats: AgenticExecutionStats,
) -> List[AgentCandidate]:
    generated: List[AgentCandidate] = []
    max_actions = max(0, int(config.agentic_max_tool_actions or 0))
    pool_limit = max(1, int(config.candidate_pool_limit or 1))

    for action in plan.actions:
        if stats.tool_actions_used >= max_actions:
            break
        stats.tool_actions_used += 1

        payload = action.payload or {}
        action_type = action.action_type

        if action_type == "inspect_specific_qid":
            qid = str(payload.get("qid", "")).strip().upper()
            if _is_valid_qid(qid):
                candidate = load_candidate_by_qid(qid)
                if candidate is not None:
                    generated.append(candidate)
            continue

        if action_type == "request_alias_search":
            aliases = payload.get("aliases", [])
            if isinstance(aliases, list):
                queries = [str(alias).strip() for alias in aliases if str(alias).strip()]
                generated.extend(
                    search_wikidata_candidates_multiquery(
                        queries,
                        per_query_limit=min(5, pool_limit),
                    )
                )
            continue

        query = str(payload.get("query", "")).strip() or term
        if action_type == "rewrite_query":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="default"))
        elif action_type == "broaden_query":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="broaden"))
        elif action_type == "narrow_query":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="narrow"))
        elif action_type == "focus_related_concepts":
            generated.extend(search_wikidata_candidates_with_options(query, limit=min(8, pool_limit), profile="focus_related"))

    return dedupe_candidates(generated)


