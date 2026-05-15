# -*- coding: utf-8 -*-
"""Data and structured-output models for agent-based reconciliation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from pydantic import BaseModel, Field
except ImportError:  # pragma: no cover - lightweight fallback until deps are installed
    class BaseModel:  # type: ignore
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        def model_dump(self) -> dict:
            return self.__dict__.copy()

    def Field(default=None, description: str = ""):  # type: ignore
        return default


DEFAULT_TRUSTED_ONTOLOGIES = ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"]
DEFAULT_BIOPORTAL_AGENT_ONTOLOGIES = ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"]


@dataclass
class AgentInputTable:
    source_name: str
    filename: str
    dataframe: pd.DataFrame
    sheet_name: Optional[str] = None
    is_from_shared_matching_table: bool = False


@dataclass
class DefinitionRecord:
    term: str
    definition: str
    context: Optional[str] = None
    source: str = "generated_single_shot"


@dataclass
class AgentRunConfig:
    workflow: str = "wikidata_deep_agent"
    definition_strategy: str = "generate_single_shot"
    model_provider: str = "openai"
    definition_model_provider: str = "openai"
    model_name: str = "gpt-5.1"
    definition_model_name: str = "o4-mini"
    timeout_seconds: int = 180
    max_iterations: int = 3
    batch_size: int = 10
    max_workers: int = 4
    parallel_start_interval_seconds: float = 0.25
    enable_skos_matching: bool = True
    auto_apply_on_accept: bool = False
    auto_accept_enabled: bool = False
    auto_accept_min_confidence: float = 0.80
    auto_accept_require_exact_match: bool = True
    auto_accept_require_llm_decision: bool = True
    auto_accept_require_no_fallback: bool = True
    auto_accept_trusted_ontologies_only: bool = False
    use_deepagents: bool = True
    trusted_ontologies: List[str] = field(default_factory=lambda: DEFAULT_TRUSTED_ONTOLOGIES.copy())
    
    trusted_fastpath_requires_provider_evidence: bool = True
    trusted_fastpath_allow_non_exact_fallback: bool = False
    exact_match_requires_provider_lexical_gate: bool = True
    confidence_mode: str = "calibrated"
    publish_raw_llm_confidence: bool = False
    bioportal_agent_ontologies: List[str] = field(default_factory=lambda: DEFAULT_BIOPORTAL_AGENT_ONTOLOGIES.copy())
    model_api_key_env: str = "OPENAI_API_KEY"
    openai_api_key_env: Optional[str] = None  # deprecated compatibility alias
    definition_model_api_key_env: str = "OPENAI_API_KEY"
    langsmith_project: Optional[str] = None
    codex_login_interactive: bool = True
    enable_agentic_refinement: bool = False
    agentic_trigger_policy: str = "no_exact_or_low_confidence"
    agentic_min_confidence_to_skip_refinement: float = 0.80
    agentic_max_planner_calls: int = 1
    agentic_max_tool_actions: int = 0
    agentic_total_llm_call_budget: int = 4
    agentic_max_candidate_rescore: int = 0
    candidate_pool_limit: int = 6
    planner_model_name: Optional[str] = None
    planner_model_provider: Optional[str] = None
    planner_model_api_key_env: Optional[str] = None
    enforce_verified_match: bool = False
    verified_match_require_exact: bool = True
    verified_match_min_confidence: float = 0.65
    verified_match_min_confidence_exact: Optional[float] = None
    verified_match_min_confidence_close: Optional[float] = None
    verified_match_min_confidence_related: Optional[float] = None
    verified_match_require_llm_decision: bool = False
    verified_match_require_no_fallback: bool = False
    allow_unverified_candidate_suggestions: bool = True
    candidate_review_mode: str = "conservative"
    allow_heuristic_fallback: bool = True
    reasoning_effort: str = "none"
    stop_on_llm_error: bool = True
    enable_second_pass_related_retry: bool = False

    def __post_init__(self):
        if (
            self.openai_api_key_env
            and str(self.openai_api_key_env).strip()
            and (not str(self.model_api_key_env).strip() or self.model_api_key_env == "OPENAI_API_KEY")
        ):
            self.model_api_key_env = str(self.openai_api_key_env).strip()
        review_mode = str(self.candidate_review_mode or "conservative").strip().lower()
        if review_mode not in {"conservative", "exploratory"}:
            review_mode = "conservative"
        self.candidate_review_mode = review_mode


@dataclass
class AgentCandidate:
    uri: str
    label: str
    description: str = ""
    source_provider: str = ""
    source_workflow: str = ""
    raw_identifier: Optional[str] = None
    score: Optional[float] = None


@dataclass
class SKOSDecision:
    mapping_type: str
    explanation: str
    input_term: str
    input_definition: str
    candidate_term: str
    candidate_definition: str
    decision_source: str = "heuristic_fallback"
    fallback_reason: Optional[str] = None
    fallback_error_type: Optional[str] = None
    fallback_error_message: Optional[str] = None
    fallback_payload_preview: Optional[str] = None
    confidence: float = 0.0
    llm_confidence: Optional[float] = None


@dataclass
class CandidateScore:
    candidate: AgentCandidate
    mapping_type: str
    confidence: float
    explanation_source: str
    from_fallback: bool
    explanation: str = ""
    skos_decision: Optional[SKOSDecision] = None
    trace_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgenticPlanAction:
    action_type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    reason: str = ""


@dataclass
class AgenticPlan:
    actions: List[AgenticPlanAction] = field(default_factory=list)
    stop_reason: str = ""
    confidence_note: str = ""


@dataclass
class AgenticExecutionStats:
    planner_calls_used: int = 0
    skos_calls_used: int = 0
    tool_actions_used: int = 0
    total_llm_calls_used: int = 0
    candidate_rescore_used: int = 0
    elapsed_ms: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "planner_calls_used": self.planner_calls_used,
            "skos_calls_used": self.skos_calls_used,
            "tool_actions_used": self.tool_actions_used,
            "total_llm_calls_used": self.total_llm_calls_used,
            "candidate_rescore_used": self.candidate_rescore_used,
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass
class AgentDecision:
    term: str
    definition: str
    candidate: Optional[AgentCandidate]
    skos: Optional[SKOSDecision]
    status: str
    explanation: str
    run_id: str
    source_name: str
    trace_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchRunState:
    run_id: str
    total_files: int = 0
    total_terms: int = 0
    processed_terms: int = 0
    completed_files: int = 0
    failed_terms: int = 0
    status: str = "pending"
    stop_reason: Optional[str] = None
    stop_event: Dict[str, Any] = field(default_factory=dict)
    messages: List[str] = field(default_factory=list)
    term_events: List[Dict[str, Any]] = field(default_factory=list)


class SKOSMatch(BaseModel):
    """SKOS-style semantic relationship between two concepts."""

    exact_match: Optional[bool] = Field(default=None, description="Whether the concepts are exact matches.")
    close_match: Optional[bool] = Field(default=None, description="Whether the concepts are close matches.")
    related_match: Optional[bool] = Field(default=None, description="Whether the concepts are related matches.")
    explanation: Optional[str] = Field(default=None, description="Explanation of the SKOS relationship.")
    confidence: Optional[float] = Field(default=None, description="Optional LLM self-reported confidence.")


class WikidataMapping(BaseModel):
    """Structured output for the single-agent Wikidata workflow."""

    qid: str = Field(description="The chosen Wikidata Q-ID.")
    skos: str = Field(description="The normalized SKOS mapping type.")
    explanation: str = Field(description="Explanation of the mapping decision.")


class AgentMapping(BaseModel):
    """Structured output for the multi-agent BioPortal/Wikidata workflow."""

    id: str = Field(description="Either a Wikidata Q-ID or an ontology IRI.")
    skos: str = Field(description="The normalized SKOS mapping type.")
    explanation: str = Field(description="Explanation of the mapping decision.")
