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
    ontology_scan_limit: Optional[int] = None
    candidate_score_limit: int = 6
    agent_step_limit: int = 3
    per_ontology_candidate_limit: int = 3
    global_candidate_pool_limit: int = 20
    shortlist_context_limit: int = 3
    shortlist_llm_limit: int = 3
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
    verified_match_allow_domain_penalized: bool = False
    # Strong-exact-identity verification: lets a confident exact match verify even when
    # the ontology class has weak definition/context evidence (e.g. material/polymer
    # concepts in MESH/SNOMEDCT). Never applies to obsolete/hard-mismatch/placeholder.
    enable_strong_exact_verify: bool = True
    strong_exact_min_llm: float = 0.93
    strong_exact_min_lexical: float = 0.95
    strong_exact_min_provider: float = 0.70
    verified_match_allow_placeholder: bool = False
    # Optional batch/domain context (e.g. "packaging", "material") — when set,
    # material/polymer/chemical ontology concepts are treated as domain-compatible.
    batch_domain_context: Optional[str] = None
    allow_unverified_candidate_suggestions: bool = True
    candidate_review_mode: str = "conservative"
    allow_heuristic_fallback: bool = True
    enable_wikidata_fallback: bool = True
    ontology_search_mode: str = "configured_only"
    bioportal_use_all_ontologies: bool = False
    # broad_retrieval_registry_scored (recommended default candidate): broad BioPortal
    # recall, then use the registry AFTER retrieval as candidate scoring/verification
    # context -- never as a hard pre-retrieval ontology filter.
    broad_registry_scored_pool_limit: int = 30
    broad_registry_scored_enable_targeted_fallback: bool = True
    broad_registry_scored_min_suitability: float = 0.70
    broad_registry_scored_ambiguous_min_suitability: float = 0.85
    broad_registry_scored_category_informative_min: float = 0.35
    enable_bioportal_annotator_rescue: bool = True
    annotator_rescue_max_variants: int = 8
    annotator_rescue_max_candidates: int = 20
    annotator_rescue_timeout_seconds: int = 30
    annotator_rescue_debug_broad_ontologies: bool = False
    enable_candidate_adjudication: bool = True
    enable_ontology_context_enrichment: bool = True
    ontology_context_children_limit: int = 5
    reasoning_effort: str = "none"
    stop_on_llm_error: bool = True
    enable_second_pass_related_retry: bool = False
    # Ontology routing / suitability scoring (decides WHICH ontologies to query;
    # never used as candidate confidence). Disabled by default -> behavior unchanged.
    enable_ontology_routing: bool = False
    ontology_routing_use_llm: bool = True
    ontology_routing_llm_model: Optional[str] = None
    ontology_routing_top_n: int = 5
    ontology_routing_prefilter_limit: int = 20
    ontology_routing_min_score: float = 0.40
    ontology_routing_strong_score: float = 0.80
    ontology_routing_confident_margin: float = 0.10
    ontology_routing_include_default_core: bool = True
    ontology_routing_trace: bool = True
    ontology_routing_project_context: Optional[str] = None
    # Ontology quality gate (trustworthy/usable in general — NOT match confidence).
    ontology_quality_blocklist: List[str] = field(default_factory=list)
    ontology_quality_allowlist: List[str] = field(default_factory=list)
    allow_excluded_ontologies_for_debug: bool = False
    trace_level: str = "summary"
    trace_llm_prompts: bool = False  # capture routing/adjudication prompts into trace (debug)
    trace_raw_candidates: bool = False
    trace_discarded_candidates: bool = True
    trace_api_payloads: bool = False
    trace_output_dir: Optional[str] = None

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
        self.bioportal_agent_ontologies = [
            str(item).strip().upper()
            for item in (self.bioportal_agent_ontologies or [])
            if str(item).strip()
        ]
        self.trusted_ontologies = [
            str(item).strip().upper()
            for item in (self.trusted_ontologies or [])
            if str(item).strip()
        ]
        # Only two ontology search modes are supported: "configured_only" and
        # "broad_retrieval_registry_scored". Legacy/removed identifiers (the old
        # all-ontology, registry-routed and table-aware modes) degrade to the broad
        # registry-scored mode, which preserves their broad-recall intent.
        mode = str(self.ontology_search_mode or "").strip().lower()
        _legacy_broad_modes = {
            "registry_routed_all", "direct_all_debug", "bioportal_all_direct", "table_aware_hybrid",
        }
        if mode in _legacy_broad_modes:
            mode = "broad_retrieval_registry_scored"
        if mode not in {"configured_only", "broad_retrieval_registry_scored"}:
            mode = "broad_retrieval_registry_scored" if self.bioportal_use_all_ontologies else "configured_only"
        self.ontology_search_mode = mode
        # Neither supported mode uses a hard pre-retrieval "all ontologies" filter:
        # configured_only queries the configured set; broad retrieval does an
        # unrestricted BioPortal search and scores with the registry afterwards.
        self.bioportal_use_all_ontologies = False
        trace_level = str(self.trace_level or "summary").strip().lower()
        if trace_level not in {"summary", "detailed", "forensic"}:
            trace_level = "summary"
        self.trace_level = trace_level


@dataclass
class AgentCandidate:
    uri: str
    label: str
    description: str = ""
    source_provider: str = ""
    source_workflow: str = ""
    raw_identifier: Optional[str] = None
    score: Optional[float] = None
    ontology_context: Dict[str, Any] = field(default_factory=dict)
    source_links: Dict[str, Any] = field(default_factory=dict)


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
    lexical_score: Optional[float] = None
    definition_score: Optional[float] = None
    ontology_context_score: Optional[float] = None
    provider_score: Optional[float] = None
    llm_confidence: Optional[float] = None
    combined_confidence: Optional[float] = None
    relation_type: Optional[str] = None
    confidence_explanation: str = ""


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
    telemetry_summary: Dict[str, Any] = field(default_factory=dict)


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
