# -*- coding: utf-8 -*-
"""Structured per-term execution tracing for agentic reconciliation."""

from __future__ import annotations

import csv
import json
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .agent_candidate_scoring import (
    VERIFIED_THRESHOLDS,
    _meets_strong_exact_identity,
    _normalize_mapping_type,
    _score_confidence,
    _safe_confidence,
    _verified_hard_block,
)


TRACE_LEVELS = {"summary": 1, "detailed": 2, "forensic": 3}
SECRET_KEY_PATTERN = re.compile(r"(api[_-]?key|secret|token|password|authorization|bearer)", re.IGNORECASE)


def normalize_trace_level(value: Any) -> str:
    level = str(value or "summary").strip().lower()
    return level if level in TRACE_LEVELS else "summary"


def trace_level_at_least(level: Any, minimum: str) -> bool:
    return TRACE_LEVELS.get(normalize_trace_level(level), 1) >= TRACE_LEVELS.get(minimum, 1)


def make_trace_call_id(run_id: str, purpose: str) -> str:
    prefix = str(purpose or "llm").strip().lower().replace("_", "-") or "llm"
    run_part = str(run_id or "run").replace("-", "")[:10]
    return f"{prefix}-{run_part}-{uuid.uuid4().hex[:12]}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        safe: Dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if SECRET_KEY_PATTERN.search(key_text):
                safe[key_text] = "[REDACTED]"
            else:
                safe[key_text] = _json_safe(item)
        return safe
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str) and SECRET_KEY_PATTERN.search(value) and len(value) > 20:
            return "[REDACTED]"
        return value
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def _safe_term_slug(term: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", str(term or "").strip())[:80].strip("_")
    return slug or "term"


@dataclass
class TraceEvent:
    step_index: int
    stage: str
    event_type: str
    timestamp: str
    duration_ms: Optional[float] = None
    input_summary: Dict[str, Any] = field(default_factory=dict)
    output_summary: Dict[str, Any] = field(default_factory=dict)
    decision: str = ""
    reason: str = ""
    data: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self, trace_level: str = "detailed") -> Dict[str, Any]:
        payload = asdict(self)
        if normalize_trace_level(trace_level) == "summary":
            payload.pop("data", None)
            if not payload.get("input_summary"):
                payload.pop("input_summary", None)
            if not payload.get("output_summary"):
                payload.pop("output_summary", None)
        return _json_safe(payload)


@dataclass
class TraceLlmCall:
    call_id: str
    stage: str
    purpose: str
    model: str = ""
    provider: str = ""
    prompt_sent: Optional[Dict[str, str]] = None
    raw_response: Optional[Any] = None
    parsed_response: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: Optional[float] = None
    token_usage: Optional[Dict[str, Any]] = None

    def as_dict(self, include_prompts: bool = False) -> Dict[str, Any]:
        payload = asdict(self)
        if not include_prompts:
            payload.pop("prompt_sent", None)
            payload.pop("raw_response", None)
        return _json_safe(payload)


@dataclass
class TraceApiCallSummary:
    provider: str
    endpoint_or_operation: str
    ontology_acronym: str = ""
    query: str = ""
    status: str = ""
    result_count: Optional[int] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class ExecutionTrace:
    run_id: str
    term_id: str
    row_index: Optional[Any]
    original_term: str
    normalized_term: str
    trace_level: str = "summary"
    trace_llm_prompts: bool = False
    events: List[TraceEvent] = field(default_factory=list)
    llm_calls: List[TraceLlmCall] = field(default_factory=list)
    api_calls: List[TraceApiCallSummary] = field(default_factory=list)
    final_decision: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)
    started_at: str = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        self.trace_level = normalize_trace_level(self.trace_level)
        self._started_perf = time.perf_counter()

    def add_event(
        self,
        stage: str,
        event_type: str,
        *,
        input_summary: Optional[Dict[str, Any]] = None,
        output_summary: Optional[Dict[str, Any]] = None,
        decision: str = "",
        reason: str = "",
        data: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
    ) -> TraceEvent:
        event = TraceEvent(
            step_index=len(self.events),
            stage=str(stage or ""),
            event_type=str(event_type or stage or ""),
            timestamp=_utc_now(),
            duration_ms=duration_ms,
            input_summary=input_summary or {},
            output_summary=output_summary or {},
            decision=str(decision or ""),
            reason=str(reason or ""),
            data=data or {},
        )
        self.events.append(event)
        return event

    def add_llm_call(self, call: TraceLlmCall) -> None:
        self.llm_calls.append(call)

    def add_api_call(self, call: TraceApiCallSummary) -> None:
        self.api_calls.append(call)

    def set_final_decision(self, payload: Dict[str, Any]) -> None:
        self.final_decision = _json_safe(payload if isinstance(payload, dict) else {})
        self.add_event(
            "final_decision",
            "final_decision",
            output_summary=self.final_decision,
            decision=str(self.final_decision.get("final_status", "")),
            reason=str(self.final_decision.get("human_readable_reason", "")),
        )

    def human_timeline(self) -> List[str]:
        lines = []
        for event in self.events:
            label = event.event_type or event.stage
            suffix = ""
            if event.decision:
                suffix = f" -> {event.decision}"
            if event.reason:
                suffix = f"{suffix} ({event.reason})"
            lines.append(f"[{event.step_index:02d}] {label}{suffix}")
        return lines

    def as_dict(self, *, compact: bool = False) -> Dict[str, Any]:
        include_detail = trace_level_at_least(self.trace_level, "detailed") and not compact
        include_prompts = bool(self.trace_llm_prompts)
        payload = {
            "run_id": self.run_id,
            "term_id": self.term_id,
            "row_index": self.row_index,
            "original_term": self.original_term,
            "normalized_term": self.normalized_term,
            "trace_level": self.trace_level,
            "started_at": self.started_at,
            "elapsed_ms": round((time.perf_counter() - self._started_perf) * 1000.0, 2),
            "summary": self.summary,
            "final_decision": self.final_decision,
            "human_readable_trace": self.human_timeline(),
            "llm_calls": [call.as_dict(include_prompts=include_prompts) for call in self.llm_calls],
            "api_calls": [call.as_dict() for call in self.api_calls],
        }
        if include_detail:
            payload["events"] = [event.as_dict(self.trace_level) for event in self.events]
        else:
            payload["events"] = [
                event.as_dict("summary")
                for event in self.events
                if event.event_type in {
                    "input_received",
                    "definition_resolved",
                    "ontology_routing_skipped",
                    "selected_ontologies_for_retrieval",
                    "confidence_recomputed",
                    "candidate_adjudication_trigger_decision",
                    "verified_gate_applied",
                    "bioportal_verified_gate_applied",
                    "wikidata_verified_gate_applied",
                    "final_verified_gate_applied",
                    "wikidata_fallback_decision",
                    "final_decision_normalized",
                    "final_decision",
                }
            ]
        return _json_safe(payload)


def summarize_candidate_score(score: Any) -> Dict[str, Any]:
    candidate = getattr(score, "candidate", None)
    metadata = getattr(score, "trace_metadata", {}) or {}
    return _json_safe(
        {
            "label": getattr(candidate, "label", "") if candidate is not None else "",
            "uri": getattr(candidate, "uri", "") if candidate is not None else "",
            "ontology": getattr(candidate, "source_provider", "") if candidate is not None else "",
            "mapping_type": getattr(score, "mapping_type", ""),
            "confidence": getattr(score, "confidence", None),
            "lexical_score": getattr(score, "lexical_score", None),
            "definition_score": getattr(score, "definition_score", None),
            "ontology_context_score": getattr(score, "ontology_context_score", None),
            "provider_score": getattr(score, "provider_score", None),
            "llm_confidence": getattr(score, "llm_confidence", None),
            "combined_confidence": getattr(score, "combined_confidence", None),
            "obsolete": metadata.get("obsolete"),
            "domain_penalized": metadata.get("domain_penalized"),
            "hard_domain_mismatch": metadata.get("candidate_domain_mismatch_level") == "hard",
            "placeholder_or_status_value": metadata.get("placeholder_or_status_value"),
            "api_rank": metadata.get("api_rank"),
            "discard_reason": metadata.get("discard_reason", ""),
        }
    )


def summarize_candidate_context(candidate: Any) -> Dict[str, Any]:
    context = getattr(candidate, "ontology_context", {}) or {}
    if not isinstance(context, dict):
        context = {}
    missing = []
    if not str(getattr(candidate, "description", "") or context.get("definition", "") or "").strip():
        missing.append("definition")
    if not context.get("children"):
        missing.append("children")
    if not context.get("parents") and not context.get("direct_parents"):
        missing.append("parents")
    return _json_safe(
        {
            "candidate": f"{getattr(candidate, 'source_provider', '')}:{getattr(candidate, 'label', '')}",
            "uri": getattr(candidate, "uri", ""),
            "context_requested": True,
            "parents_found": len(context.get("parents") or context.get("direct_parents") or []),
            "children_found": len(context.get("children") or context.get("direct_children") or []),
            "siblings_found": len(context.get("sibling_classes_from_ontology") or context.get("sibling_examples") or []),
            "paths_to_root_found": len(context.get("lineage") or context.get("path_to_root") or []),
            "definition_found": bool(str(getattr(candidate, "description", "") or context.get("definition", "") or "").strip()),
            "ontology_context_found": bool(context),
            "context_quality": context.get("hierarchy_quality") or context.get("hierarchy_status") or ("partial" if context else "missing"),
            "missing_context_fields": missing,
            "errors": context.get("endpoint_errors") or context.get("errors") or {},
        }
    )


def explain_verified_gate(score: Any, config: Any) -> Dict[str, Any]:
    if score is None:
        return {
            "verified_gate_input": {"candidate_present": False},
            "verified_gate_decision": "not_verified",
            "passed_conditions": [],
            "failed_conditions": ["no_candidate"],
        }

    mapping_type = _normalize_mapping_type(getattr(score, "mapping_type", ""))
    confidence = _score_confidence(score)
    metadata = getattr(score, "trace_metadata", {}) or {}
    relation_specific_threshold = getattr(config, f"verified_match_min_confidence_{mapping_type}", None)
    configured_default = getattr(config, "verified_match_min_confidence", VERIFIED_THRESHOLDS.get(mapping_type, 0.80))
    threshold = _safe_confidence(
        relation_specific_threshold
        if relation_specific_threshold is not None
        else VERIFIED_THRESHOLDS.get(mapping_type, configured_default),
        default=VERIFIED_THRESHOLDS.get(mapping_type, 0.80),
    )
    decision_source = str(getattr(score, "explanation_source", "") or "").strip().lower()
    from_fallback = bool(getattr(score, "from_fallback", False))
    hard_domain = str(metadata.get("candidate_domain_mismatch_level", "") or "").strip().lower() == "hard"
    strong_exact = bool(_meets_strong_exact_identity(score, config))
    failed: List[str] = []
    passed: List[str] = []

    if mapping_type == "none":
        failed.append("mapping_type_none")
    else:
        passed.append("mapping_type_present")

    hard_blocked = _verified_hard_block(metadata, config)
    if hard_blocked:
        if bool(metadata.get("obsolete")):
            failed.append("obsolete_blocks_verified")
        if hard_domain:
            failed.append("hard_domain_mismatch_blocks_verified")
        if bool(metadata.get("placeholder_or_status_value")):
            failed.append("placeholder_or_status_value_blocks_verified")
        if bool(metadata.get("registry_suitability_block")) or bool(metadata.get("registry_hard_block")):
            failed.append("registry_hard_block")
    else:
        passed.append("no_hard_verified_block")

    # broad_retrieval_registry_scored: the term-type-calibrated triage is authoritative
    # for verification (mode-isolated flag). It already applied per-term-type thresholds,
    # so the global combined-confidence gate must NOT be re-applied on top -- that caused
    # a verified triage to drift to candidate_suggested at finalization.
    if not hard_blocked and "broad_registry_verified" in metadata:
        if bool(metadata.get("broad_registry_verified")):
            passed.append("broad_registry_triage_verified")
        else:
            failed.append("broad_registry_triage_not_verified")
    elif strong_exact:
        passed.append("strong_exact_identity")
    else:
        if bool(getattr(config, "verified_match_require_exact", True)) and mapping_type != "exact":
            failed.append("relation_not_exact")
        else:
            passed.append("relation_allowed")
        if confidence < threshold:
            failed.append(f"combined_confidence_below_{mapping_type or 'relation'}_threshold")
        else:
            passed.append("combined_confidence_meets_threshold")
        if bool(getattr(config, "verified_match_require_llm_decision", False)) and decision_source not in {"llm", "llm_adjudication"}:
            failed.append("llm_decision_required")
        else:
            passed.append("llm_decision_requirement_satisfied")
        if bool(getattr(config, "verified_match_require_no_fallback", False)) and from_fallback:
            failed.append("fallback_blocks_verified")
        else:
            passed.append("fallback_requirement_satisfied")

    # When strong_exact is True the per-condition failures are not appended (see the
    # else-branch above), so `failed` is empty and this reduces to a clean check.
    decision = "verified" if not failed else "not_verified"
    return _json_safe(
        {
            "verified_gate_input": {
                "relation": mapping_type,
                "combined_confidence": confidence,
                "threshold": threshold,
                "llm_confidence": getattr(score, "llm_confidence", None),
                "obsolete": bool(metadata.get("obsolete")),
                "domain_penalized": bool(metadata.get("domain_penalized")),
                "hard_domain_mismatch": hard_domain,
                "placeholder_or_status_value": bool(metadata.get("placeholder_or_status_value")),
                "decision_source": decision_source,
                "from_fallback": from_fallback,
                "strong_exact_identity": strong_exact,
            },
            "verified_gate_decision": decision,
            "passed_conditions": passed,
            "failed_conditions": failed,
        }
    )


def export_trace_artifacts(trace: ExecutionTrace, output_dir: Optional[Any]) -> Dict[str, str]:
    if not output_dir:
        return {}
    base = Path(output_dir).expanduser()
    base.mkdir(parents=True, exist_ok=True)
    term_dir = base / "term_traces"
    term_dir.mkdir(parents=True, exist_ok=True)

    trace_payload = trace.as_dict(compact=False)
    row_part = "row" if trace.row_index is None else str(trace.row_index)
    term_file = term_dir / f"{row_part}_{_safe_term_slug(trace.original_term)}.json"
    term_file.write_text(json.dumps(trace_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    events_path = base / "trace_events.jsonl"
    with events_path.open("a", encoding="utf-8") as handle:
        for event in trace.events:
            handle.write(json.dumps({
                "run_id": trace.run_id,
                "term_id": trace.term_id,
                "row_index": trace.row_index,
                "term": trace.original_term,
                **event.as_dict(trace.trace_level),
            }, ensure_ascii=False, sort_keys=True) + "\n")

    summary = trace.summary if isinstance(trace.summary, dict) else {}
    final = trace.final_decision if isinstance(trace.final_decision, dict) else {}
    row = {
        "row_index": trace.row_index,
        "term": trace.original_term,
        "final_status": final.get("final_status", ""),
        "final_label": final.get("final_label", ""),
        "final_uri": final.get("final_uri", ""),
        "final_confidence": final.get("final_confidence", ""),
        "decision_source": final.get("decision_source", ""),
        "ontology_routing_mode": summary.get("ontology_routing_mode", ""),
        "selected_ontologies": "|".join(summary.get("selected_ontologies", []) or []),
        "raw_candidate_count": summary.get("raw_candidate_count", ""),
        "deduped_candidate_count": summary.get("deduped_candidate_count", ""),
        "adjudication_candidate_count": summary.get("adjudication_candidate_count", ""),
        "llm_calls": len(trace.llm_calls),
        "bioportal_calls": len([c for c in trace.api_calls if c.provider == "BioPortal"]),
        "wikidata_calls": len([c for c in trace.api_calls if c.provider == "Wikidata"]),
        "final_gate_decision": summary.get("final_gate_decision", ""),
        "final_gate_failed_conditions": "|".join(summary.get("final_gate_failed_conditions", []) or []),
        "wikidata_attempted": summary.get("wikidata_attempted", ""),
        "elapsed_ms": trace_payload.get("elapsed_ms", ""),
    }
    csv_path = base / "trace_summary.csv"
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    return {
        "trace_events_jsonl": str(events_path),
        "term_trace_json": str(term_file),
        "trace_summary_csv": str(csv_path),
    }
