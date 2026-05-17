import type { AdvancedConfig, AutoAcceptPolicy, ProvenanceConfig, Stage, WorkflowConfig } from './types';

export const workflows = [
  { id: 'wikidata_deep_agent', title: 'Wikidata Deep Agent', badge: 'FAST & BROAD', badgeColor: '#2563eb', description: 'Searches Wikidata only. Optimized for general-purpose entities and high-speed reconciliation.', bullets: ['Broad coverage', 'Fast execution', 'General purpose'] },
  { id: 'bioportal_wikidata_multiagent', title: 'BioPortal + Wikidata', badge: 'DOMAIN FOCUS', badgeColor: '#059669', description: 'Prioritizes domain-specific ontologies via BioPortal, using Wikidata as a fallback.', bullets: ['Domain-aware', 'Scientific/medical data', 'Expert terminology'] },
];

export const stages: { id: Stage; label: string; caption: string }[] = [
  { id: 'setup', label: 'Setup', caption: 'Data & config' },
  { id: 'run', label: 'Run', caption: 'Execute agents' },
  { id: 'review', label: 'Review', caption: 'Curate mappings' },
  { id: 'export', label: 'Export', caption: 'SSSOM & handoff' },
];

export const reviewStatuses = ['all', 'matched', 'candidate_suggested', 'pending', 'accepted', 'rejected', 'no_match'] as const;
export const editableSkosMatchTypes = ['skos:exactMatch', 'skos:closeMatch', 'skos:relatedMatch'] as const;
export const matchTypes = ['all', 'skos:exactMatch', 'skos:closeMatch', 'skos:relatedMatch', 'no_match'] as const;

export function normalizeCandidateReviewMode(value: unknown): 'conservative' | 'exploratory' {
  return String(value || '').trim().toLowerCase() === 'exploratory' ? 'exploratory' : 'conservative';
}

export function formatReviewMode(value?: string) {
  return normalizeCandidateReviewMode(value) === 'exploratory' ? 'Exploratory' : 'Conservative';
}

export function statusLabel(status?: string) {
  const value = String(status || '').trim();
  if (value === 'matched') return 'Matched';
  if (value === 'candidate_suggested') return 'Review suggested candidate';
  if (value === 'no_match') return 'No match';
  if (value === 'pending') return 'Pending review';
  if (value === 'accepted') return 'Accepted';
  if (value === 'rejected') return 'Rejected';
  return value || 'Pending review';
}

export function normalizeEditableSkosMatchType(matchType?: string): typeof editableSkosMatchTypes[number] {
  const value = String(matchType || '').trim();
  return editableSkosMatchTypes.includes(value as typeof editableSkosMatchTypes[number])
    ? (value as typeof editableSkosMatchTypes[number])
    : 'skos:closeMatch';
}

export function skosChipSx(matchType?: string) {
  const value = String(matchType || '').trim();

  if (value === 'skos:exactMatch') {
    return { bgcolor: '#dbeafe', color: '#1d4ed8', fontWeight: 700 };
  }

  if (value === 'skos:closeMatch') {
    return { bgcolor: '#dcfce7', color: '#166534', fontWeight: 700 };
  }

  if (value === 'skos:relatedMatch') {
    return { bgcolor: '#ffedd5', color: '#9a3412', fontWeight: 700 };
  }

  if (value === 'no_match') {
    return { bgcolor: '#f3f4f6', color: '#374151', fontWeight: 700 };
  }

  return { bgcolor: '#f3f4f6', color: '#374151', fontWeight: 700 };
}

export function reviewStatusChipSx(status?: string) {
  const value = String(status || '').trim();

  if (value === 'matched') {
    return { bgcolor: '#dcfce7', color: '#166534', fontWeight: 700 };
  }

  if (value === 'candidate_suggested') {
    return { bgcolor: '#e0f2fe', color: '#075985', fontWeight: 700 };
  }

  if (value === 'pending') {
    return { bgcolor: '#fef3c7', color: '#92400e', fontWeight: 700 };
  }

  if (value === 'accepted') {
    return { bgcolor: '#dcfce7', color: '#166534', fontWeight: 700 };
  }

  if (value === 'rejected') {
    return { bgcolor: '#fee2e2', color: '#991b1b', fontWeight: 700 };
  }

  if (value === 'no_match') {
    return { bgcolor: '#f3f4f6', color: '#374151', fontWeight: 700 };
  }

  return { bgcolor: '#f3f4f6', color: '#374151', fontWeight: 700 };
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
  return {
    workflow: raw?.workflow || 'wikidata_deep_agent',
    provider: raw?.provider || providers[0] || 'openai',
    model: raw?.model || models[0] || 'gpt-5.1',
    reasoning_effort: raw?.reasoning_effort || 'none',
    candidate_review_mode: normalizeCandidateReviewMode(raw?.candidate_review_mode),
    custom_model_override: raw?.custom_model_override || '',
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
    use_different_models: raw?.use_different_models ?? false,
    definition_model: raw?.definition_model || raw?.model || models[0] || 'gpt-5.1',
    definition_preparation: raw?.definition_preparation ?? false,
    definition_strategy: raw?.definition_strategy || 'generate_single_shot',
    definition_context_text: raw?.definition_context_text || '',
    definition_uploaded_filename: raw?.definition_uploaded_filename || '',
    definition_uploaded_count: raw?.definition_uploaded_count ?? 0,
    definition_reference_filename: raw?.definition_reference_filename || '',
    definition_reference_text: raw?.definition_reference_text || '',
    definition_reference_char_count: raw?.definition_reference_char_count ?? 0,
    agentic_trigger_policy: raw?.agentic_trigger_policy || 'no_exact_or_low_confidence',
    planner_provider: raw?.planner_provider || raw?.provider || providers[0] || 'openai',
    planner_model: raw?.planner_model || raw?.model || models[0] || 'gpt-5.1',
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
