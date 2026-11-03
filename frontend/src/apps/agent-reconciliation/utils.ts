import type { AdvancedConfig, AutoAcceptPolicy, ProvenanceConfig, Stage, WorkflowConfig } from './types';

// Strategy cards on Setup: one line each; the details go into the section's info popover.
export const workflows = [
  { id: 'wikidata_deep_agent', title: 'Wikidata deep agent', description: 'Wikidata only. Fast, broad coverage of general-purpose entities.', details: ['broad coverage', 'fast execution', 'general-purpose terms'] },
  { id: 'bioportal_wikidata_multiagent', title: 'BioPortal + Wikidata', description: 'Domain ontologies from BioPortal first, Wikidata as a fallback.', details: ['domain-aware', 'scientific and medical data', 'expert terminology'] },
];

export const stages: { id: Stage; label: string }[] = [
  { id: 'setup', label: 'Set up' },
  { id: 'run', label: 'Run agents' },
  { id: 'review', label: 'Review matches' },
  { id: 'export', label: 'Export' },
];

export const reviewStatuses = ['all', 'matched', 'candidate_suggested', 'pending', 'accepted', 'rejected', 'no_match'] as const;
export const editableSkosMatchTypes = ['skos:exactMatch', 'skos:closeMatch', 'skos:relatedMatch'] as const;
export const matchTypes = ['all', 'skos:exactMatch', 'skos:closeMatch', 'skos:relatedMatch', 'no_match'] as const;

export function normalizeCandidateReviewMode(value: unknown): 'conservative' | 'exploratory' {
  return String(value || '').trim().toLowerCase() === 'exploratory' ? 'exploratory' : 'conservative';
}

export function normalizeTraceLevel(value: unknown): 'summary' | 'detailed' | 'forensic' {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'detailed' || normalized === 'forensic' ? normalized : 'summary';
}

// Only two ontology search modes are supported. Persisted/legacy configs may still
// carry a removed identifier (bioportal_all_direct, direct_all_debug, registry_routed_all,
// table_aware_hybrid); all of those broad/all-ontology modes degrade to the broad
// registry-scored mode, preserving their intent.
export function normalizeOntologySearchMode(
  value: unknown,
  bioportalUseAllOntologies?: unknown,
): 'configured_only' | 'broad_retrieval_registry_scored' {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'configured_only') return 'configured_only';
  if (normalized === 'broad_retrieval_registry_scored') return 'broad_retrieval_registry_scored';
  if (['bioportal_all_direct', 'direct_all_debug', 'registry_routed_all', 'table_aware_hybrid'].includes(normalized)) {
    return 'broad_retrieval_registry_scored';
  }
  return bioportalUseAllOntologies ? 'broad_retrieval_registry_scored' : 'configured_only';
}

export function formatReviewMode(value?: string) {
  return normalizeCandidateReviewMode(value) === 'exploratory' ? 'Exploratory' : 'Conservative';
}

type ChipTone = 'default' | 'primary' | 'info' | 'success' | 'warning' | 'error';

// One status vocabulary for grid chips, count facets, filters, triage and the legend popover.
// Keys are backend protocol values; only the labels are for people.
export const STATUS_META: Record<string, { label: string; color: ChipTone }> = {
  pending: { label: 'Pending', color: 'warning' },
  matched: { label: 'Matched', color: 'primary' },
  candidate_suggested: { label: 'Review suggestion', color: 'info' },
  accepted: { label: 'Accepted', color: 'success' },
  rejected: { label: 'Rejected', color: 'error' },
  no_match: { label: 'No match', color: 'default' },
  timeout: { label: 'Timed out', color: 'default' },
};

export function statusLabel(status?: string) {
  const value = String(status || '').trim();
  return STATUS_META[value || 'pending']?.label ?? value;
}

export function statusColor(status?: string): ChipTone {
  return STATUS_META[String(status || '').trim() || 'pending']?.color ?? 'default';
}

export function normalizeEditableSkosMatchType(matchType?: string): typeof editableSkosMatchTypes[number] {
  const value = String(matchType || '').trim();
  return editableSkosMatchTypes.includes(value as typeof editableSkosMatchTypes[number])
    ? (value as typeof editableSkosMatchTypes[number])
    : 'skos:closeMatch';
}

// SKOS relation tone for chips (theme palette tokens, so both colour schemes work). The CURIE
// label always carries the meaning; colour only helps scanning.
export function skosColor(matchType?: string): ChipTone {
  const value = String(matchType || '').trim();
  if (value === 'skos:exactMatch') return 'primary';
  if (value === 'skos:closeMatch') return 'success';
  if (value === 'skos:relatedMatch') return 'warning';
  return 'default';
}

const countFormat = new Intl.NumberFormat();
const costFormat = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 4 });

// Formatted numbers for KPIs and summaries. Missing values return '' so MetaList/KpiStrip show "—".
export function formatCount(value: unknown): string {
  if (value === null || value === undefined || value === '') return '';
  const n = Number(value);
  return Number.isFinite(n) ? countFormat.format(n) : '';
}

export function formatSeconds(value: unknown): string {
  const seconds = Number(value);
  if (value === null || value === undefined || value === '' || !Number.isFinite(seconds) || seconds < 0) return '';
  if (seconds < 60) return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)} s`;
  const total = Math.round(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  return hours ? `${hours} h ${minutes} min` : `${minutes} min ${total % 60} s`;
}

export function formatCost(value: unknown): string {
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? costFormat.format(n) : '';
}

export function formatScore(value: unknown): string {
  if (value === null || value === undefined || value === '') return '';
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(2) : String(value);
}

export function asNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function unique(values: string[]): string[] {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

export function splitCsv(value: string): string[] {
  return unique(value.split(',').map((part) => part.trim().toUpperCase()));
}

export function normalizeStage(value: unknown): Stage {
  const lower = String(value || '').trim().toLowerCase();
  return ['setup', 'run', 'review', 'export'].includes(lower) ? (lower as Stage) : 'setup';
}

export function normalizeConfig(raw: Partial<WorkflowConfig> | undefined, providers: string[], models: string[]): WorkflowConfig {
  const advanced = raw?.advanced ?? ({} as AdvancedConfig);
  const policy = raw?.auto_accept_policy ?? ({} as AutoAcceptPolicy);
  const provenance = raw?.provenance ?? ({} as ProvenanceConfig);
  const model = raw?.model && models.includes(raw.model) ? raw.model : models[0] || '';
  return {
    workflow: raw?.workflow || 'wikidata_deep_agent',
    provider: raw?.provider || providers[0] || 'openai',
    model,
    reasoning_effort: raw?.reasoning_effort || 'none',
    candidate_review_mode: normalizeCandidateReviewMode(raw?.candidate_review_mode),
    custom_model_override: raw?.custom_model_override && models.includes(raw.custom_model_override) ? raw.custom_model_override : '',
    provider_api_key_env: raw?.provider_api_key_env || '',
    openai_compatible_base_url: raw?.openai_compatible_base_url || '',
    openai_compatible_api_key: raw?.openai_compatible_api_key || '',
    skos_matching: raw?.skos_matching ?? true,
    auto_accept: raw?.auto_accept ?? false,
    auto_accept_policy: {
      min_confidence: asNumber(policy.min_confidence, 0.8),
      require_exact_match: policy.require_exact_match ?? true,
      require_llm_decision: policy.require_llm_decision ?? true,
      require_no_fallback: policy.require_no_fallback ?? true,
      trusted_ontologies_only: policy.trusted_ontologies_only ?? false,
    },
    langsmith: raw?.langsmith ?? false,
    langsmith_project: raw?.langsmith_project || '',
    expert_mode: raw?.expert_mode ?? false,
    allow_heuristic_fallback: raw?.allow_heuristic_fallback ?? true,
    enable_wikidata_fallback: raw?.enable_wikidata_fallback ?? true,
    ontology_search_mode: normalizeOntologySearchMode(raw?.ontology_search_mode, raw?.bioportal_use_all_ontologies),
    bioportal_use_all_ontologies: false,
    enable_candidate_adjudication: raw?.enable_candidate_adjudication ?? true,
    trace_level: normalizeTraceLevel(raw?.trace_level),
    trace_llm_prompts: raw?.trace_llm_prompts ?? false,
    trace_raw_candidates: raw?.trace_raw_candidates ?? false,
    trace_discarded_candidates: raw?.trace_discarded_candidates ?? true,
    trace_api_payloads: raw?.trace_api_payloads ?? false,
    trace_output_dir: raw?.trace_output_dir || '',
    use_different_models: raw?.use_different_models ?? false,
    definition_model: raw?.definition_model && models.includes(raw.definition_model) ? raw.definition_model : model,
    definition_preparation: raw?.definition_preparation ?? true,
    definition_strategy: raw?.definition_strategy || 'generate_single_shot',
    definition_context_text: raw?.definition_context_text || '',
    definition_uploaded_filename: raw?.definition_uploaded_filename || '',
    definition_uploaded_count: raw?.definition_uploaded_count ?? 0,
    definition_reference_filename: raw?.definition_reference_filename || '',
    definition_reference_text: raw?.definition_reference_text || '',
    definition_reference_char_count: raw?.definition_reference_char_count ?? 0,
    agentic_trigger_policy: raw?.agentic_trigger_policy || 'no_exact_or_low_confidence',
    planner_provider: raw?.planner_provider || raw?.provider || providers[0] || 'openai',
    planner_model: raw?.planner_provider && raw.planner_provider !== raw.provider ? raw.planner_model || '' : (raw?.planner_model && models.includes(raw.planner_model) ? raw.planner_model : model),
    trusted_ontologies: raw?.trusted_ontologies || ['MESH', 'NCIT', 'LOINC', 'FOODON', 'NCBITAXON'],
    bioportal_ontologies: raw?.bioportal_ontologies || ['NCIT', 'NIFSTD', 'BERO', 'OCHV', 'SNOMEDCT'],
    advanced: {
      timeout_s: asNumber(advanced.timeout_s, 180),
      max_iterations: asNumber(advanced.max_iterations, 10),
      batch_size: asNumber(advanced.batch_size, 10),
      max_workers: asNumber(advanced.max_workers, 4),
      agentic_min_confidence_to_skip_refinement: asNumber(advanced.agentic_min_confidence_to_skip_refinement, 0.8),
      agentic_max_planner_calls: asNumber(advanced.agentic_max_planner_calls, 1),
      agentic_max_tool_actions: asNumber(advanced.agentic_max_tool_actions, 6),
      agentic_total_llm_call_budget: asNumber(advanced.agentic_total_llm_call_budget, 14),
      agentic_max_candidate_rescore: asNumber(advanced.agentic_max_candidate_rescore, 8),
      candidate_pool_limit: asNumber(advanced.candidate_pool_limit, 30),
    },
    provenance: {
      enabled: provenance.enabled ?? false,
      author_id: provenance.author_id || '',
      author_label: provenance.author_label || '',
      reviewer_id: provenance.reviewer_id || '',
      reviewer_label: provenance.reviewer_label || '',
      creator_id: provenance.creator_id || '',
      creator_label: provenance.creator_label || '',
      mapping_tool: provenance.mapping_tool || 'RDF4Risk Agent-Based Reconciliation',
      mapping_tool_version: provenance.mapping_tool_version || 'PoC',
      mapping_date: provenance.mapping_date || '',
      publication_date: provenance.publication_date || '',
    },
  };
}
