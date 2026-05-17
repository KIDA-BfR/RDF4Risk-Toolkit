import type { RunStatus } from '../../components/run/AgentRunProgressPanel';

export type Stage = 'setup' | 'run' | 'review' | 'export';

export type AdvancedConfig = {
  timeout_s: number;
  max_iterations: number;
  batch_size: number;
  max_workers: number;
  agentic_min_confidence_to_skip_refinement?: number;
  agentic_max_planner_calls?: number;
  agentic_max_tool_actions?: number;
  agentic_total_llm_call_budget?: number;
  agentic_max_candidate_rescore?: number;
  candidate_pool_limit?: number;
};

export type AutoAcceptPolicy = {
  min_confidence: number;
  require_exact_match: boolean;
  require_llm_decision: boolean;
  require_no_fallback: boolean;
  trusted_ontologies_only: boolean;
};

export type ProvenanceConfig = {
  enabled: boolean;
  author_id?: string;
  author_label?: string;
  reviewer_id?: string;
  reviewer_label?: string;
  creator_id?: string;
  creator_label?: string;
  mapping_tool?: string;
  mapping_tool_version?: string;
  mapping_date?: string;
  publication_date?: string;
};

export type WorkflowConfig = {
  workflow: string;
  provider: string;
  model: string;
  reasoning_effort: string;
  candidate_review_mode: 'conservative' | 'exploratory';
  custom_model_override?: string;
  provider_api_key_env?: string;
  openai_compatible_base_url?: string;
  openai_compatible_api_key?: string;
  skos_matching: boolean;
  auto_accept: boolean;
  auto_accept_policy: AutoAcceptPolicy;
  langsmith: boolean;
  langsmith_project?: string;
  expert_mode: boolean;
  allow_heuristic_fallback?: boolean;
  use_different_models?: boolean;
  definition_model?: string;
  definition_preparation?: boolean;
  definition_strategy?: string;
  definition_context_text?: string;
  definition_uploaded_filename?: string;
  definition_uploaded_count?: number;
  definition_reference_filename?: string;
  definition_reference_text?: string;
  definition_reference_char_count?: number;
  agentic_trigger_policy?: string;
  planner_provider?: string;
  planner_model?: string;
  trusted_ontologies?: string[];
  bioportal_ontologies?: string[];
  advanced: AdvancedConfig;
  provenance?: ProvenanceConfig;
};

export type DataStatus = {
  has_table?: boolean;
  filename?: string;
  source_name?: string;
  rows?: number;
  columns?: number;
  loaded_sources?: number;
  required_columns_detected?: boolean;
  schema_message?: string;
  upload_bridge_available?: boolean;
  shared_table_available?: boolean;
  preview?: Record<string, unknown>[];
};

export type ReadinessCheck = { key: string; label: string; ok: boolean; detail?: string };
export type ReadinessState = { ready?: boolean; checks?: ReadinessCheck[]; summary?: Record<string, string> };

export type Telemetry = {
  enabled?: boolean;
  run_id?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  duration_sec?: number | null;
  total_terms?: number;
  processed_terms?: number;
  failed_terms?: number;
  total_cost_usd?: number;
  langsmith_url?: string | null;
  langsmith_project_url?: string | null;
  langsmith_message?: string | null;
  llm_calls?: Record<string, unknown>[];
  events?: Record<string, unknown>[];
  cascade?: Record<string, unknown>[];
  logs?: string[];
};

export type ReviewCounts = {
  pending: number;
  matched?: number;
  candidate_suggested?: number;
  accepted: number;
  rejected: number;
  no_match: number;
};

export type ExportPayload = {
  nonce?: number | string;
  filename?: string;
  content?: string;
  mime_type?: string;
};

export type StopEvent = {
  stop_reason?: string;
  file?: string;
  term?: string;
  processed_terms?: number;
  total_terms?: number;
};

export type ReviewItem = {
  mapping_id: string;
  row_index?: number;
  source_name?: string;
  term?: string;
  definition?: string;
  status?: string;
  suggested_uri?: string;
  suggested_label?: string;
  suggested_description?: string;
  candidate_uri?: string;
  candidate_label?: string;
  candidate_description?: string;
  can_accept?: boolean;
  no_match_note?: string;
  match_type?: string;
  provider?: string;
  confidence?: number | string;
  decision_source?: string;
  fallback_reason?: string;
  trace_metadata?: Record<string, unknown>;
  review_mode?: string;
  explanation?: string;
  auto_accept_reason?: string;
  input_uri?: string;
  accepted_match_type?: string;
  subject_label?: string;
};

export type ReviewState = { items?: ReviewItem[]; counts?: ReviewCounts; selected_source?: string | null };

export type ComponentArgs = {
  active_stage?: Stage;
  activeStage?: string;
  config?: Partial<WorkflowConfig>;
  providers?: string[];
  providerLabels?: Record<string, string>;
  models?: string[];
  modelLabels?: Record<string, string>;
  modelDetails?: string | null;
  reasoningOptions?: string[];
  readiness?: ReadinessState;
  run_status?: RunStatus;
  data_status?: DataStatus;
  telemetry?: Telemetry;
  review?: ReviewState;
  exportPayload?: ExportPayload | null;
  ontologyOptions?: string[];
  providerKind?: 'codex' | 'openai_compatible' | 'standard';
  statusMessage?: { severity?: 'success' | 'info' | 'warning' | 'error'; text?: string } | null;
  codexAuthStatus?: {
    authenticated: boolean;
    pending_auth_url: string | null;
    [key: string]: any;
  };
};

export type AppEvent = { type: string; [key: string]: unknown };
export type AgentReconciliationAppProps = { args?: ComponentArgs; onEvent?: (event: AppEvent) => void };
