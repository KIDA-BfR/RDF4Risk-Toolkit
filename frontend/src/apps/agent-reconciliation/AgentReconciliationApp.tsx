import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  ButtonBase,
  Card,
  CardContent,
  Checkbox,
  Chip,
  Collapse,
  Divider,
  Drawer,
  FormControl,
  FormControlLabel,
  InputAdornment,
  InputLabel,
  LinearProgress,
  MenuItem,
  Paper,
  Select,
  Stack,
  Step,
  StepButton,
  Stepper,
  Switch,
  Tab,
  Tabs,
  TextField,
  Typography,
} from '@mui/material';
import { AgentRunProgressPanel, type AgentRunWorkflow, type RunStatus } from '../../components/run/AgentRunProgressPanel';
const AgentIcon = () => (
  <svg
    width="64"
    height="64"
    viewBox="0 0 64 64"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    role="img"
    style={{ marginRight: '12px' }}
  >
    <defs>
      <linearGradient id="mainGradient" x1="9" y1="9" x2="55" y2="55" gradientUnits="userSpaceOnUse">
        <stop stopColor="#2563EB"/>
        <stop offset="0.52" stopColor="#0891B2"/>
        <stop offset="1" stopColor="#14B8A6"/>
      </linearGradient>
      <linearGradient id="accentGradient" x1="40" y1="14" x2="56" y2="30" gradientUnits="userSpaceOnUse">
        <stop stopColor="#38BDF8"/>
        <stop offset="1" stopColor="#14B8A6"/>
      </linearGradient>
      <filter id="softShadow" x="-40%" y="-40%" width="180%" height="180%">
        <feDropShadow dx="0" dy="4" stdDeviation="4" floodColor="#0F172A" floodOpacity="0.18"/>
      </filter>
    </defs>
    <circle cx="13.5" cy="18" r="5.5" fill="#F8FAFC" stroke="#2563EB" strokeWidth="2"/>
    <circle cx="13.5" cy="32" r="5.5" fill="#F8FAFC" stroke="#0891B2" strokeWidth="2"/>
    <circle cx="13.5" cy="46" r="5.5" fill="#F8FAFC" stroke="#14B8A6" strokeWidth="2"/>
    <path d="M19.5 18 C25 18 25.5 27 30.5 29" stroke="url(#mainGradient)" strokeWidth="2.6" strokeLinecap="round"/>
    <path d="M19.5 32 H29" stroke="url(#mainGradient)" strokeWidth="2.6" strokeLinecap="round"/>
    <path d="M19.5 46 C25 46 25.5 37 30.5 35" stroke="url(#mainGradient)" strokeWidth="2.6" strokeLinecap="round"/>
    <path
      d="M32 20.5L42 26.25V37.75L32 43.5L22 37.75V26.25L32 20.5Z"
      fill="url(#mainGradient)"
      filter="url(#softShadow)"
    />
    <circle cx="32" cy="32" r="4.5" fill="#FFFFFF" opacity="0.96"/>
    <path d="M28.8 32H35.2" stroke="#2563EB" strokeWidth="1.8" strokeLinecap="round"/>
    <path d="M32 28.8V35.2" stroke="#14B8A6" strokeWidth="1.8" strokeLinecap="round"/>
    <path d="M42.5 32H47.5" stroke="url(#mainGradient)" strokeWidth="2.6" strokeLinecap="round"/>
    <circle cx="52" cy="32" r="8.5" fill="url(#accentGradient)" filter="url(#softShadow)"/>
    <path d="M48.3 32.1L50.9 34.7L56.1 29.2" stroke="#FFFFFF" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round"/>
    <circle cx="25.2" cy="24.6" r="1.7" fill="#38BDF8"/>
    <circle cx="25.2" cy="39.4" r="1.7" fill="#14B8A6"/>
  </svg>
);

type Stage = 'setup' | 'run' | 'review' | 'export';

type AdvancedConfig = {
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

type AutoAcceptPolicy = {
  min_confidence: number;
  require_exact_match: boolean;
  require_llm_decision: boolean;
  require_no_fallback: boolean;
  trusted_ontologies_only: boolean;
};

type ProvenanceConfig = {
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

type WorkflowConfig = {
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

type DataStatus = {
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

type ReadinessCheck = { key: string; label: string; ok: boolean; detail?: string };
type ReadinessState = { ready?: boolean; checks?: ReadinessCheck[]; summary?: Record<string, string> };
type Telemetry = {
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
type ReviewCounts = {
  pending: number;
  matched?: number;
  candidate_suggested?: number;
  accepted: number;
  rejected: number;
  no_match: number;
};
type ExportPayload = {
  nonce?: number | string;
  filename?: string;
  content?: string;
  mime_type?: string;
};
type ReviewItem = {
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
type ReviewState = { items?: ReviewItem[]; counts?: ReviewCounts; selected_source?: string | null };
type ComponentArgs = {
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
type AgentReconciliationAppProps = { args?: ComponentArgs; onEvent?: (event: AppEvent) => void };

type AppEvent = { type: string; [key: string]: unknown };

const workflows = [
  { id: 'wikidata_deep_agent', title: 'Wikidata Deep Agent', badge: 'FAST & BROAD', badgeColor: '#2563eb', description: 'Searches Wikidata only. Optimized for general-purpose entities and high-speed reconciliation.', bullets: ['Broad coverage', 'Fast execution', 'General purpose'] },
  { id: 'bioportal_wikidata_multiagent', title: 'BioPortal + Wikidata', badge: 'DOMAIN FOCUS', badgeColor: '#059669', description: 'Prioritizes domain-specific ontologies via BioPortal, using Wikidata as a fallback.', bullets: ['Domain-aware', 'Scientific/medical data', 'Expert terminology'] },
];
const stages: { id: Stage; label: string; caption: string }[] = [
  { id: 'setup', label: 'Setup', caption: 'Data & config' },
  { id: 'run', label: 'Run', caption: 'Execute agents' },
  { id: 'review', label: 'Review', caption: 'Curate mappings' },
  { id: 'export', label: 'Export', caption: 'SSSOM & handoff' },
];
const reviewStatuses = ['all', 'matched', 'candidate_suggested', 'pending', 'accepted', 'rejected', 'no_match'] as const;
const editableSkosMatchTypes = ['skos:exactMatch', 'skos:closeMatch', 'skos:relatedMatch'] as const;
const matchTypes = ['all', 'skos:exactMatch', 'skos:closeMatch', 'skos:relatedMatch', 'no_match'] as const;

function normalizeCandidateReviewMode(value: unknown): 'conservative' | 'exploratory' {
  return String(value || '').trim().toLowerCase() === 'exploratory' ? 'exploratory' : 'conservative';
}

function formatReviewMode(value?: string) {
  return normalizeCandidateReviewMode(value) === 'exploratory' ? 'Exploratory' : 'Conservative';
}

function statusLabel(status?: string) {
  const value = String(status || '').trim();
  if (value === 'matched') return 'Matched';
  if (value === 'candidate_suggested') return 'Review suggested candidate';
  if (value === 'no_match') return 'No match';
  if (value === 'pending') return 'Pending review';
  if (value === 'accepted') return 'Accepted';
  if (value === 'rejected') return 'Rejected';
  return value || 'Pending review';
}

function normalizeEditableSkosMatchType(matchType?: string): typeof editableSkosMatchTypes[number] {
  const value = String(matchType || '').trim();
  return editableSkosMatchTypes.includes(value as typeof editableSkosMatchTypes[number])
    ? (value as typeof editableSkosMatchTypes[number])
    : 'skos:closeMatch';
}

function skosChipSx(matchType?: string) {
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

function reviewStatusChipSx(status?: string) {
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

function asNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}
function unique(values: string[]): string[] {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}
function splitCsv(value: string): string[] {
  return unique(value.split(',').map((part) => part.trim().toUpperCase()));
}
async function fileToBase64(file: File) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = '';
  bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
  return window.btoa(binary);
}
function normalizeStage(value: unknown): Stage {
  const lower = String(value || '').trim().toLowerCase();
  return ['setup', 'run', 'review', 'export'].includes(lower) ? (lower as Stage) : 'setup';
}
function normalizeConfig(raw: Partial<WorkflowConfig> | undefined, providers: string[], models: string[]): WorkflowConfig {
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

function DataTable({ rows, empty }: { rows?: Record<string, unknown>[]; empty: string }) {
  const safeRows = rows ?? [];
  const columns = safeRows.length ? Object.keys(safeRows[0]).slice(0, 8) : [];
  if (!safeRows.length) return <Typography variant="body2" color="text.secondary">{empty}</Typography>;
  return (
    <Box sx={{ overflow: 'auto', border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
      <Box component="table" sx={{ width: '100%', borderCollapse: 'collapse', minWidth: 620 }}>
        <Box component="thead" sx={{ bgcolor: '#f8fafc' }}><tr>{columns.map((col) => <Box component="th" key={col} sx={{ p: 1, textAlign: 'left', fontSize: 12 }}>{col}</Box>)}</tr></Box>
        <tbody>{safeRows.slice(0, 80).map((row, i) => <tr key={i}>{columns.map((col) => <Box component="td" key={col} sx={{ p: 1, borderTop: '1px solid #e2e8f0', fontSize: 12, maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(row[col] ?? '')}</Box>)}</tr>)}</tbody>
      </Box>
    </Box>
  );
}

function SummaryRow({ label, value }: { label: string; value: React.ReactNode }) {
  return <Stack direction="row" justifyContent="space-between" spacing={2}><Typography variant="body2" color="text.secondary">{label}</Typography><Typography variant="body2" sx={{ fontWeight: 700, textAlign: 'right' }}>{value}</Typography></Stack>;
}

function ComparisonRow({ leftLabel, leftValue, rightLabel, rightValue }: { leftLabel: string; leftValue: React.ReactNode; rightLabel: string; rightValue: React.ReactNode }) {
  const renderValue = (value: React.ReactNode) => {
    if (value === null || value === undefined || value === '') {
      return '—';
    }
    if (React.isValidElement(value)) {
      return value;
    }
    return String(value);
  };

  return (
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.2 }}>
      <Paper variant="outlined" sx={{ p: 1.2, borderRadius: 2 }}>
        <Typography variant="caption" color="text.secondary">{leftLabel}</Typography>
        <Typography variant="body2" sx={{ fontWeight: 700, mt: 0.4, wordBreak: 'break-word' }}>{renderValue(leftValue)}</Typography>
      </Paper>
      <Paper variant="outlined" sx={{ p: 1.2, borderRadius: 2 }}>
        <Typography variant="caption" color="text.secondary">{rightLabel}</Typography>
        <Typography variant="body2" sx={{ fontWeight: 700, mt: 0.4, wordBreak: 'break-word' }}>{renderValue(rightValue)}</Typography>
      </Paper>
    </Box>
  );
}
function ToggleCard({ checked, title, description, onChange }: { checked: boolean; title: string; description: string; onChange: (value: boolean) => void }) {
  return <Paper variant="outlined" sx={{ p: 1.5, minHeight: 94, borderRadius: 3, borderColor: checked ? 'primary.main' : 'divider', bgcolor: checked ? 'rgba(37,99,235,.035)' : 'background.paper' }}><Stack direction="row" spacing={1} alignItems="flex-start"><Checkbox checked={checked} onChange={(e) => onChange(e.target.checked)} sx={{ p: 0 }} /><Stack><Typography variant="body2" sx={{ fontWeight: 800 }}>{title}</Typography><Typography variant="caption" color="text.secondary">{description}</Typography></Stack></Stack></Paper>;
}

function AppShell({ activeStage, onNavigate, children, dataStatus, runStatus, review }: { activeStage: Stage; onNavigate: (stage: Stage) => void; children: React.ReactNode; dataStatus: DataStatus; runStatus: RunStatus; review: ReviewState }) {
  const activeStep = stages.findIndex((s) => s.id === activeStage);
  return <Box sx={{ bgcolor: '#eef7fb', minHeight: '100vh', p: { xs: 1, md: 2 }, borderRadius: 4 }}><Stack spacing={2}>
    <Paper variant="outlined" sx={{ p: { xs: 2, md: 2.5 }, borderRadius: 4, background: 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 50%,#eff6ff 100%)', boxShadow: '0 18px 48px rgba(15,23,42,.08)' }}>
      <Stack direction={{ xs: 'column', lg: 'row' }} spacing={2} alignItems={{ xs: 'stretch', lg: 'center' }} justifyContent="space-between">
        <Stack direction="row" spacing={1.5} alignItems="center"><AgentIcon /><Stack><Typography variant="h5">Agent-Based Reconciliation</Typography><Typography variant="body2" color="text.secondary">Agent-based workflow for semantic reconciliation, curation, and SSSOM export</Typography></Stack></Stack>
        <Stepper nonLinear activeStep={activeStep} sx={{ minWidth: { lg: 560 } }}>{stages.map((stage, idx) => <Step key={stage.id} completed={idx < activeStep}><StepButton onClick={() => onNavigate(stage.id)}><Stack spacing={0}><Typography variant="body2" sx={{ fontWeight: 800 }}>{stage.label}</Typography><Typography variant="caption" color="text.secondary">{stage.caption}</Typography></Stack></StepButton></Step>)}</Stepper>
      </Stack>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', md: 'repeat(4, 1fr)' }, gap: 1.2, mt: 2 }}>
        <Chip label={dataStatus.has_table ? `${dataStatus.rows ?? 0} rows loaded` : 'No table loaded'} color={dataStatus.has_table ? 'success' : 'warning'} />
        <Chip label={dataStatus.required_columns_detected ? 'Schema valid' : 'Schema pending'} color={dataStatus.required_columns_detected ? 'success' : 'warning'} />
        <Chip label={runStatus.finished ? 'Run finished' : runStatus.running ? 'Running' : 'Ready state'} color={runStatus.finished ? 'success' : runStatus.running ? 'info' : 'default'} />
        <Chip label={`${review.counts?.pending ?? 0} pending review`} color={(review.counts?.pending ?? 0) ? 'warning' : 'default'} />
      </Box>
    </Paper>
    {children}
  </Stack></Box>;
}

function FileUploadPanel({ dataStatus, emit }: { dataStatus: DataStatus; emit: (event: AppEvent) => void }) {
  const [uploadError, setUploadError] = useState('');

  async function handleCsvUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setUploadError('');
    if (!file) return;
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setUploadError('Please choose a .csv matching table.');
      return;
    }
    try {
      const content = await file.text();
      emit({ type: 'upload_csv', filename: file.name, content });
    } catch (error) {
      setUploadError(error instanceof Error ? error.message : 'Unable to read the selected CSV file.');
    }
  }

  return <Card variant="outlined"><CardContent><Stack spacing={1.5}>
    <Stack direction="row" justifyContent="space-between" alignItems="center"><Typography variant="subtitle1">File Upload</Typography><Chip size="small" label={dataStatus.has_table ? 'file selected' : 'waiting'} color={dataStatus.has_table ? 'success' : 'warning'} /></Stack>
    <Paper variant="outlined" sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed', borderRadius: 3, bgcolor: '#f8fafc' }}>
      <Typography variant="h6">Upload matching table</Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mt: .5 }}>Choose a CSV matching table directly in the browser app, or load the shared table from the Matching Table Generator.</Typography>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} justifyContent="center" alignItems="center" sx={{ mt: 1.5 }}>
        <Button component="label" variant="contained" disabled={dataStatus.upload_bridge_available === false}>
          Upload CSV file
          <input hidden type="file" accept=".csv,text/csv" onChange={handleCsvUpload} />
        </Button>
        <Button variant="outlined" onClick={() => emit({ type: 'load_shared_table' })} disabled={!dataStatus.shared_table_available}>Load shared matching table</Button>
      </Stack>
      {uploadError && <Alert severity="warning" variant="outlined" sx={{ mt: 1.5, textAlign: 'left' }}>{uploadError}</Alert>}
    </Paper>
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1 }}>
      <SummaryRow label="File" value={dataStatus.filename || dataStatus.source_name || '—'} />
      <SummaryRow label="Rows" value={dataStatus.rows ?? 0} />
      <SummaryRow label="Columns" value={dataStatus.columns ?? 0} />
      <SummaryRow label="Schema" value={dataStatus.schema_message || 'No schema detected'} />
    </Box>
    <DataTable rows={dataStatus.preview} empty="No table preview available yet." />
  </Stack></CardContent></Card>;
}

function WorkflowConfigPanelInner({ config, providers, providerLabels, modelOptions, modelLabels, modelDetails, reasoningOptions, ontologyOptions, providerKind, codexAuthStatus, update, emit }: { config: WorkflowConfig; providers: string[]; providerLabels: Record<string, string>; modelOptions: string[]; modelLabels: Record<string, string>; modelDetails?: string | null; reasoningOptions: string[]; ontologyOptions: string[]; providerKind: string; codexAuthStatus?: { authenticated: boolean; pending_auth_url: string | null; [key: string]: any }; update: (patch: Partial<WorkflowConfig>) => void; emit: (event: AppEvent) => void }) {
  useEffect(() => {
    if ((config.provider === 'google' || config.provider === 'google_gemini') && config.provider_api_key_env !== 'GOOGLE_API_KEY') {
      update({ provider_api_key_env: 'GOOGLE_API_KEY' });
    } else if (config.provider === 'anthropic' && config.provider_api_key_env !== 'ANTHROPIC_API_KEY') {
      update({ provider_api_key_env: 'ANTHROPIC_API_KEY' });
    } else if (config.provider === 'openai' && (config.provider_api_key_env === 'GOOGLE_API_KEY' || config.provider_api_key_env === 'ANTHROPIC_API_KEY' || config.provider_api_key_env === 'GEMINI_API_KEY' || config.provider_api_key_env === 'OPENAI_CODEX_SUBSCRIPTION')) {
      update({ provider_api_key_env: 'OPENAI_API_KEY' });
    } else if (config.provider === 'openai_codex' && config.provider_api_key_env !== 'OPENAI_CODEX_SUBSCRIPTION') {
      update({ provider_api_key_env: 'OPENAI_CODEX_SUBSCRIPTION' });
    }
  }, [config.provider]);

  const selectedWorkflow = workflows.find((item) => item.id === config.workflow) ?? workflows[0];
  const updateAdvanced = (patch: Partial<AdvancedConfig>) => update({ advanced: { ...config.advanced, ...patch } });
  const updatePolicy = (patch: Partial<AutoAcceptPolicy>) => update({ auto_accept_policy: { ...config.auto_accept_policy, ...patch } });
  return <Card variant="outlined"><CardContent><Stack spacing={2}>
    <Stack><Typography variant="subtitle1">Workflow Configuration</Typography><Typography variant="body2" color="text.secondary">Agent strategy, model provider, policies and advanced execution settings.</Typography></Stack>
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5 }}>{workflows.map((workflow) => <ButtonBase key={workflow.id} onClick={() => update({ workflow: workflow.id })} sx={{ textAlign: 'left', borderRadius: 3 }}><Paper variant="outlined" sx={{ p: 2, width: '100%', minHeight: 150, borderRadius: 3, borderColor: config.workflow === workflow.id ? 'primary.main' : 'divider', bgcolor: config.workflow === workflow.id ? 'rgba(37,99,235,.04)' : 'white' }}><Stack spacing={1}><Stack direction="row" justifyContent="space-between"><Typography variant="subtitle2">{workflow.title}</Typography><Chip size="small" label={workflow.badge} sx={{ color: workflow.badgeColor, bgcolor: `${workflow.badgeColor}18` }} /></Stack><Typography variant="body2" color="text.secondary">{workflow.description}</Typography>{workflow.bullets.map((b) => <Typography key={b} variant="caption" color="text.secondary">✓ {b}</Typography>)}</Stack></Paper></ButtonBase>)}</Box>
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1.2fr 1.2fr .8fr' }, gap: 1.3 }}>
      <FormControl fullWidth size="small"><InputLabel>LLM Provider</InputLabel><Select label="LLM Provider" value={config.provider} onChange={(e) => update({ provider: String(e.target.value) })}>{providers.map((p) => <MenuItem key={p} value={p}>{providerLabels[p] ?? p}</MenuItem>)}</Select></FormControl>
      <FormControl fullWidth size="small"><InputLabel>Model</InputLabel><Select label="Model" value={config.model} onChange={(e) => update({ model: String(e.target.value) })}>{modelOptions.map((m) => <MenuItem key={m} value={m}>{modelLabels[m] ?? m}</MenuItem>)}</Select></FormControl>
      <FormControl fullWidth size="small"><InputLabel>Reasoning</InputLabel><Select label="Reasoning" value={config.reasoning_effort} onChange={(e) => update({ reasoning_effort: String(e.target.value) })}>{reasoningOptions.map((r) => <MenuItem key={r} value={r}>{r}</MenuItem>)}</Select></FormControl>
    </Box>
    {modelDetails && <Alert severity="info" variant="outlined">{modelDetails}</Alert>}
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr auto auto' }, gap: 1.2 }}><TextField label="Custom Model Override" value={config.custom_model_override} onChange={(e) => update({ custom_model_override: e.target.value })} />{providerKind === 'openai_compatible' && <Button variant="outlined" onClick={() => emit({ type: 'register_local_model' })}>Register model</Button>}<Button variant="outlined" onClick={() => emit({ type: 'reload_models' })}>Reload models & pricing</Button></Box>
    {providerKind === 'openai_compatible' ? <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.2 }}><TextField label="OpenAI-compatible base URL" value={config.openai_compatible_base_url} onChange={(e) => update({ openai_compatible_base_url: e.target.value })} /><TextField label="OpenAI-compatible API key" type="password" value={config.openai_compatible_api_key} onChange={(e) => update({ openai_compatible_api_key: e.target.value })} /></Box> : providerKind === 'codex' ? (
      <Box sx={{ p: 2, border: '1px solid', borderColor: 'divider', borderRadius: 3, bgcolor: 'background.paper' }}>
        <Typography variant="subtitle2" sx={{ mb: 1 }}>ChatGPT Subscription Auth</Typography>
        {codexAuthStatus?.authenticated ? (
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                <Alert severity="success" sx={{ flexGrow: 1, py: 0 }}>Connected</Alert>
                <Button variant="outlined" onClick={() => emit({ type: 'codex_auth_refresh' })}>Refresh</Button>
                <Button variant="outlined" color="error" onClick={() => emit({ type: 'codex_auth_signout' })}>Sign out</Button>
            </Box>
        ) : (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                    <Button variant="contained" onClick={() => emit({ type: 'codex_auth_signin' })}>Sign in with ChatGPT</Button>
                    <Button variant="outlined" onClick={() => emit({ type: 'codex_auth_refresh_pending' })}>I completed login</Button>
                </Box>
                {codexAuthStatus?.pending_auth_url && (
                    <Alert severity="info">
                        Please complete sign in: <a href={codexAuthStatus.pending_auth_url} target="_blank" rel="noreferrer">Login Link</a>
                    </Alert>
                )}
            </Box>
        )}
      </Box>
    ) : <TextField label="Provider API key env var" value={config.provider_api_key_env} onChange={(e) => update({ provider_api_key_env: e.target.value })} InputProps={{ startAdornment: <InputAdornment position="start">ENV</InputAdornment> }} />}
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2,1fr)', lg: 'repeat(4,1fr)' }, gap: 1.2 }}><ToggleCard checked={config.skos_matching} title="SKOS matching" description="Generate SKOS predicates for mappings." onChange={(v) => update({ skos_matching: v })} /><ToggleCard checked={config.auto_accept} title="Auto-accept" description="Accept high-confidence mappings by policy." onChange={(v) => update({ auto_accept: v })} /><ToggleCard checked={config.langsmith} title="LangSmith" description="Enable single MUI monitoring panel." onChange={(v) => update({ langsmith: v })} /><ToggleCard checked={config.expert_mode} title="Expert mode" description="Expose planner, budgets and limits." onChange={(v) => update({ expert_mode: v })} /></Box>
    <Collapse in={config.langsmith}><TextField fullWidth label="LangSmith project" value={config.langsmith_project} onChange={(e) => update({ langsmith_project: e.target.value })} /></Collapse>
    <Collapse in={config.workflow === 'bioportal_wikidata_multiagent'}><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.2 }}><TextField label="Trusted ontologies" helperText={ontologyOptions.length ? `Available: ${ontologyOptions.slice(0, 8).join(', ')}…` : 'Comma separated'} value={(config.trusted_ontologies ?? []).join(', ')} onChange={(e) => update({ trusted_ontologies: splitCsv(e.target.value) })} /><TextField label="BioPortal ontologies" value={(config.bioportal_ontologies ?? []).join(', ')} onChange={(e) => update({ bioportal_ontologies: splitCsv(e.target.value) })} /></Box></Collapse>
    <Collapse in={config.auto_accept}><Paper variant="outlined" sx={{ p: 1.5, borderRadius: 3 }}><Stack spacing={1}><Typography variant="subtitle2">Auto-Accept Policy</Typography><TextField type="number" label="Minimum confidence" inputProps={{ min: 0, max: 1, step: .01 }} value={config.auto_accept_policy.min_confidence} onChange={(e) => updatePolicy({ min_confidence: asNumber(e.target.value, .8) })} /><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1 }}><FormControlLabel control={<Checkbox checked={config.auto_accept_policy.require_exact_match} onChange={(e) => updatePolicy({ require_exact_match: e.target.checked })} />} label="Exact match" /><FormControlLabel control={<Checkbox checked={config.auto_accept_policy.require_llm_decision} onChange={(e) => updatePolicy({ require_llm_decision: e.target.checked })} />} label="LLM decision" /><FormControlLabel control={<Checkbox checked={config.auto_accept_policy.require_no_fallback} onChange={(e) => updatePolicy({ require_no_fallback: e.target.checked })} />} label="No fallback" /><FormControlLabel control={<Checkbox checked={config.auto_accept_policy.trusted_ontologies_only} onChange={(e) => updatePolicy({ trusted_ontologies_only: e.target.checked })} />} label="Trusted only" /></Box></Stack></Paper></Collapse>
    <Paper variant="outlined" sx={{ borderRadius: 3, overflow: 'hidden' }}><Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ p: 1.5 }}><Stack><Typography variant="subtitle2">Advanced Settings</Typography><Typography variant="caption" color="text.secondary">Execution limits, review policy and agentic refinement controls.</Typography></Stack><Switch checked={config.expert_mode} onChange={(e) => update({ expert_mode: e.target.checked })} /></Stack><Collapse in={config.expert_mode}><Divider /><Stack spacing={1.5} sx={{ p: 1.5 }}><Paper variant="outlined" sx={{ p: 1.5, borderRadius: 2, bgcolor: '#f8fafc' }}><Stack spacing={1}><Typography variant="subtitle2">Candidate review policy</Typography><FormControl fullWidth size="small"><InputLabel>Candidate review policy</InputLabel><Select label="Candidate review policy" value={config.candidate_review_mode} onChange={(e) => update({ candidate_review_mode: normalizeCandidateReviewMode(e.target.value) })}><MenuItem value="conservative">Conservative</MenuItem><MenuItem value="exploratory">Exploratory</MenuItem></Select></FormControl><Typography variant="caption" color="text.secondary"><strong>Conservative</strong>: automatically accepts only strong candidates but still shows plausible exact/close matches for review. <strong>Exploratory</strong>: also shows weaker close or related candidates for manual review. Useful for sparse ontologies or uncommon terms.</Typography></Stack></Paper><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1.2 }}><TextField type="number" label="Timeout" value={config.advanced.timeout_s} onChange={(e) => updateAdvanced({ timeout_s: asNumber(e.target.value, 180) })} /><TextField type="number" label="Iterations" value={config.advanced.max_iterations} onChange={(e) => updateAdvanced({ max_iterations: asNumber(e.target.value, 10) })} /><TextField type="number" label="Batch size" value={config.advanced.batch_size} onChange={(e) => updateAdvanced({ batch_size: asNumber(e.target.value, 10) })} /><TextField type="number" label="Workers" value={config.advanced.max_workers} onChange={(e) => updateAdvanced({ max_workers: asNumber(e.target.value, 4) })} /></Box><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(3,1fr)' }, gap: 1.2 }}><FormControlLabel control={<Switch checked={Boolean(config.allow_heuristic_fallback)} onChange={(e) => update({ allow_heuristic_fallback: e.target.checked })} />} label="Allow heuristic fallbacks" /><FormControlLabel control={<Switch checked={Boolean(config.use_different_models)} onChange={(e) => update({ use_different_models: e.target.checked })} />} label="Different definition model" /><TextField type="number" label="LLM call budget" value={config.advanced.agentic_total_llm_call_budget} onChange={(e) => updateAdvanced({ agentic_total_llm_call_budget: asNumber(e.target.value, 14) })} /></Box></Stack></Collapse></Paper>
    <Alert severity="info" variant="outlined">Selected strategy: <strong>{selectedWorkflow.title}</strong>. Configuration changes are emitted as structured <code>config_changed</code> events.</Alert>
  </Stack></CardContent></Card>;
}

function DefinitionPreparationPanel({ config, update, emit }: { config: WorkflowConfig; update: (patch: Partial<WorkflowConfig>) => void; emit: (event: AppEvent) => void }) {
  const [uploadError, setUploadError] = useState('');
  const strategy = config.definition_strategy || 'generate_single_shot';

  async function handleDefinitionsUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setUploadError('');
    if (!file) return;
    if (!/\.(csv|xlsx|xls)$/i.test(file.name)) {
      setUploadError('Choose a CSV or Excel sheet with Term and Definition columns.');
      return;
    }
    try {
      emit({ type: 'upload_definitions_sheet', filename: file.name, content_base64: await fileToBase64(file) });
    } catch (error) {
      setUploadError(error instanceof Error ? error.message : 'Unable to read the selected definitions sheet.');
    }
  }

  async function handleReferenceUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setUploadError('');
    if (!file) return;
    if (!/\.(pdf|doc|docx)$/i.test(file.name)) {
      setUploadError('Choose a PDF, DOC, or DOCX reference publication.');
      return;
    }
    try {
      emit({ type: 'upload_reference_publication', filename: file.name, content_base64: await fileToBase64(file) });
    } catch (error) {
      setUploadError(error instanceof Error ? error.message : 'Unable to read the selected reference publication.');
    }
  }

  return <Card variant="outlined"><CardContent><Stack spacing={1.5}>
    <Stack direction="row" justifyContent="space-between" alignItems="flex-start" spacing={2}>
      <Stack><Typography variant="subtitle1">Definition Preparation</Typography><Typography variant="body2" color="text.secondary">Optional contextual definitions for ambiguous terms.</Typography></Stack>
      <Switch checked={Boolean(config.definition_preparation)} onChange={(e) => update({ definition_preparation: e.target.checked })} />
    </Stack>
    <Collapse in={Boolean(config.definition_preparation)}>
      <Stack spacing={1.3} sx={{ pt: .5 }}>
        <FormControl fullWidth size="small"><InputLabel>Definition strategy</InputLabel><Select label="Definition strategy" value={strategy} onChange={(e) => { setUploadError(''); update({ definition_strategy: String(e.target.value) }); }}><MenuItem value="uploaded_sheet">Upload definitions sheet</MenuItem><MenuItem value="generate_single_shot">Generate from context</MenuItem><MenuItem value="reference_publication">Reference publication</MenuItem></Select></FormControl>
        {strategy === 'uploaded_sheet' && (
          <Paper variant="outlined" sx={{ p: 2, borderStyle: 'dashed', borderRadius: 3, bgcolor: '#f8fafc' }}>
            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} justifyContent="space-between" alignItems={{ xs: 'stretch', sm: 'center' }}>
              <Stack spacing={.4}>
                <Typography variant="subtitle2">Definitions sheet</Typography>
                <Typography variant="body2" color="text.secondary">CSV or Excel with Term and Definition columns.</Typography>
                {config.definition_uploaded_filename && <Chip size="small" color="success" sx={{ alignSelf: 'flex-start' }} label={`${config.definition_uploaded_filename} - ${config.definition_uploaded_count ?? 0} definitions`} />}
              </Stack>
              <Button component="label" variant="contained">
                Choose sheet
                <input hidden type="file" accept=".csv,.xlsx,.xls,text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" onChange={handleDefinitionsUpload} />
              </Button>
            </Stack>
          </Paper>
        )}
        {strategy === 'reference_publication' && (
          <Stack spacing={1}>
            <Paper variant="outlined" sx={{ p: 2, borderStyle: 'dashed', borderRadius: 3, bgcolor: '#f8fafc' }}>
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} justifyContent="space-between" alignItems={{ xs: 'stretch', sm: 'center' }}>
                <Stack spacing={.4}>
                  <Typography variant="subtitle2">Reference publication</Typography>
                  <Typography variant="body2" color="text.secondary">PDF, DOC, or DOCX. Extracted text becomes the definition context.</Typography>
                  {config.definition_reference_filename && <Chip size="small" color="success" sx={{ alignSelf: 'flex-start' }} label={`${config.definition_reference_filename} - ${config.definition_reference_char_count ?? 0} characters`} />}
                </Stack>
                <Button component="label" variant="contained">
                  Choose publication
                  <input hidden type="file" accept=".pdf,.doc,.docx,application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document" onChange={handleReferenceUpload} />
                </Button>
              </Stack>
            </Paper>
            <TextField label="Extracted reference text" multiline minRows={4} value={config.definition_reference_text || ''} InputProps={{ readOnly: true }} placeholder="Upload a reference publication to preview extracted text." />
          </Stack>
        )}
        {strategy === 'generate_single_shot' && <TextField label="Context text for definition generation" multiline minRows={4} value={config.definition_context_text} onChange={(e) => update({ definition_context_text: e.target.value })} />}
        {uploadError && <Alert severity="warning" variant="outlined">{uploadError}</Alert>}
      </Stack>
    </Collapse>
  </Stack></CardContent></Card>;
}

function ProvenancePanel({ config, update, emit }: { config: WorkflowConfig; update: (patch: Partial<WorkflowConfig>) => void; emit: (event: AppEvent) => void }) {
  const prov = config.provenance ?? { enabled: false };
  const updateProv = (patch: Partial<ProvenanceConfig>) => update({ provenance: { ...prov, ...patch } });
  return <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between"><Stack><Typography variant="subtitle1">Provenance & Curation Metadata</Typography><Typography variant="body2" color="text.secondary">Collapsed by default; fields appear only when enabled.</Typography></Stack><FormControlLabel control={<Switch checked={Boolean(prov.enabled)} onChange={(e) => updateProv({ enabled: e.target.checked })} />} label="Include provenance metadata" /></Stack><Collapse in={Boolean(prov.enabled)}><Stack spacing={1.2}><Alert severity="info" variant="outlined">Mapping Date is generated automatically when the workflow runs.</Alert><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.2 }}><TextField label="Author ORCID" value={prov.author_id} onChange={(e) => updateProv({ author_id: e.target.value })} /><TextField label="Author Name" value={prov.author_label} onChange={(e) => updateProv({ author_label: e.target.value })} /><TextField label="Reviewer ORCID" value={prov.reviewer_id} onChange={(e) => updateProv({ reviewer_id: e.target.value })} /><TextField label="Reviewer Name" value={prov.reviewer_label} onChange={(e) => updateProv({ reviewer_label: e.target.value })} /><TextField label="Creator ORCID" value={prov.creator_id} onChange={(e) => updateProv({ creator_id: e.target.value })} /><TextField label="Creator Name" value={prov.creator_label} onChange={(e) => updateProv({ creator_label: e.target.value })} /><TextField label="Mapping Tool" value={prov.mapping_tool} onChange={(e) => updateProv({ mapping_tool: e.target.value })} /><TextField label="Tool Version" value={prov.mapping_tool_version} onChange={(e) => updateProv({ mapping_tool_version: e.target.value })} /><TextField label="Publication Date" value={prov.publication_date} onChange={(e) => updateProv({ publication_date: e.target.value })} /></Box><Button variant="outlined" onClick={() => emit({ type: 'save_provenance_defaults' })}>Save provenance defaults</Button></Stack></Collapse></Stack></CardContent></Card>;
}

function SetupPage(props: { config: WorkflowConfig; dataStatus: DataStatus; readiness: ReadinessState; providers: string[]; providerLabels: Record<string, string>; modelOptions: string[]; modelLabels: Record<string, string>; modelDetails?: string | null; reasoningOptions: string[]; ontologyOptions: string[]; providerKind: string; codexAuthStatus?: { authenticated: boolean; pending_auth_url: string | null; [key: string]: any }; update: (patch: Partial<WorkflowConfig>) => void; emit: (event: AppEvent) => void }) {
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: 'minmax(0, 1fr) 360px' }, gap: 2 }}><Stack spacing={2}><FileUploadPanel dataStatus={props.dataStatus} emit={props.emit} /><DefinitionPreparationPanel config={props.config} update={props.update} emit={props.emit} /><WorkflowConfigPanelInner {...props} /><ProvenancePanel config={props.config} update={props.update} emit={props.emit} /></Stack><Stack spacing={2}><RunPrerequisitesPanel readiness={props.readiness} /><RunSummaryPanel readiness={props.readiness} config={props.config} /><Button variant="contained" size="large" disabled={!props.readiness.ready} onClick={() => props.emit({ type: 'navigate', stage: 'run' })}>Continue to Run</Button><Button variant="outlined" onClick={() => props.emit({ type: 'save_configuration' })}>Save Configuration</Button></Stack></Box>;
}
function RunPrerequisitesPanel({ readiness }: { readiness: ReadinessState }) { return <Card variant="outlined"><CardContent><Stack spacing={1.2}><Stack direction="row" justifyContent="space-between"><Typography variant="subtitle1">Run Prerequisites</Typography><Chip size="small" color={readiness.ready ? 'success' : 'warning'} label={readiness.ready ? 'All Good' : 'Action Needed'} /></Stack>{(readiness.checks ?? []).map((c) => <Stack key={c.key} direction="row" spacing={1}><Box sx={{ color: c.ok ? 'success.main' : 'warning.main' }}>{c.ok ? '●' : '▲'}</Box><Stack><Typography variant="body2" sx={{ fontWeight: 750 }}>{c.label}</Typography><Typography variant="caption" color="text.secondary">{c.detail}</Typography></Stack></Stack>)}</Stack></CardContent></Card>; }
function RunSummaryPanel({ readiness, config }: { readiness: ReadinessState; config: WorkflowConfig }) { const summary = readiness.summary ?? {}; return <Card variant="outlined"><CardContent><Stack spacing={1.1}><Typography variant="subtitle1">Run Summary</Typography><SummaryRow label="Workflow" value={summary.Workflow || config.workflow} /><SummaryRow label="Model" value={summary.Model || config.model} /><SummaryRow label="SKOS Matching" value={summary['SKOS Matching'] || (config.skos_matching ? 'Enabled' : 'Disabled')} /><SummaryRow label="Auto-accept" value={summary['Auto-accept'] || (config.auto_accept ? 'Enabled' : 'Disabled')} /><SummaryRow label="Batch Size" value={summary['Batch Size'] || String(config.advanced.batch_size)} /><SummaryRow label="Max Workers" value={summary['Max Workers'] || String(config.advanced.max_workers)} /><SummaryRow label="Est. Runtime" value={summary['Est. Runtime'] || 'n/a'} /><SummaryRow label="Est. Cost" value={summary['Est. Cost'] || 'Available after run telemetry'} /></Stack></CardContent></Card>; }
function MonitoringMetric({ label, value, tone = 'default' }: { label: string; value: React.ReactNode; tone?: 'default' | 'error' }) {
  return (
    <Paper variant="outlined" sx={{ p: 1.2, borderRadius: 2, bgcolor: tone === 'error' ? 'rgba(254,242,242,.72)' : '#f8fafc', borderColor: tone === 'error' ? 'error.light' : 'divider' }}>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>{label}</Typography>
      <Typography variant="body1" sx={{ fontWeight: 900, color: tone === 'error' ? 'error.main' : 'text.primary' }}>{value}</Typography>
    </Paper>
  );
}
function MonitoringPanel({ telemetry, runStatus }: { telemetry: Telemetry; runStatus: RunStatus }) { const [tab, setTab] = useState(0); const hasRealProgress = typeof runStatus.processed_count === 'number' && typeof runStatus.total_count === 'number' && runStatus.total_count > 0; const progress = hasRealProgress ? Math.min(100, Math.round(((runStatus.processed_count as number) / (runStatus.total_count as number)) * 100)) : null; const processedValue = runStatus.processed_count ?? telemetry.processed_terms ?? 0; const totalValue = runStatus.total_count ?? telemetry.total_terms ?? 0; const failureValue = Math.max(0, Number(telemetry.failed_terms ?? 0)); return <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between"><Typography variant="subtitle1">Monitoring / Telemetry</Typography><Chip size="small" label={telemetry.enabled ? 'enabled' : 'disabled'} color={telemetry.enabled ? 'success' : 'default'} /></Stack><Tabs value={tab} onChange={(_, v) => setTab(v)} variant="scrollable"><Tab label="Run Status" /><Tab label="LLM Calls" /><Tab label="LangSmith" /><Tab label="Logs" /></Tabs>{tab === 0 && <Stack spacing={1}><LinearProgress variant="determinate" value={hasRealProgress ? progress ?? 0 : 0} /><Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'repeat(2, minmax(0, 1fr))', md: 'repeat(5, minmax(0, 1fr))' }, gap: 1 }}><MonitoringMetric label="Processed terms" value={processedValue} /><MonitoringMetric label="Total terms" value={totalValue} /><MonitoringMetric label="Run failures" value={failureValue} tone={failureValue > 0 ? 'error' : 'default'} /><MonitoringMetric label="Duration" value={telemetry.duration_sec ? `${telemetry.duration_sec.toFixed(2)}s` : '—'} /><MonitoringMetric label="Cost" value={`$${(telemetry.total_cost_usd ?? 0).toFixed(4)}`} /></Box><DataTable rows={telemetry.events} empty="No term-level events captured yet." /></Stack>}{tab === 1 && <DataTable rows={telemetry.llm_calls} empty="No LLM prompt/response interactions captured yet." />}{tab === 2 && <Stack spacing={1}><Alert severity={telemetry.enabled ? 'info' : 'warning'} variant="outlined">{telemetry.langsmith_message || (telemetry.enabled ? 'LangSmith monitoring is enabled.' : 'LangSmith monitoring is disabled.')}</Alert>{telemetry.langsmith_project_url && <Button href={telemetry.langsmith_project_url} target="_blank">Open LangSmith project</Button>}{telemetry.langsmith_url && <Button href={telemetry.langsmith_url} target="_blank">Open LangSmith run</Button>}<DataTable rows={telemetry.cascade} empty="No cascade trace captured yet." /></Stack>}{tab === 3 && <Stack spacing={.7}>{(telemetry.logs?.length ? telemetry.logs : ['No logs captured yet.']).slice(-80).map((log, idx) => <Typography key={idx} variant="caption" sx={{ fontFamily: 'monospace' }}>{log}</Typography>)}</Stack>}</Stack></CardContent></Card>; }
function workflowForRunPanel(workflow: string): AgentRunWorkflow { return workflow === 'wikidata_deep_agent' ? 'wikidata_deep_agent' : 'bioportal_wikidata'; }
function RunStartPanel({ readiness, running, runStatus, onStart, onBack }: { readiness: ReadinessState; running: boolean; runStatus: RunStatus; onStart: () => void; onBack: () => void }) { return <Card variant="outlined"><CardContent><Stack spacing={1.5}><Typography variant="subtitle1">Run Agent-Based Reconciliation</Typography><Alert severity={readiness.ready ? 'success' : 'warning'} variant="outlined">{runStatus.message || (readiness.ready ? 'Ready to run' : 'Resolve prerequisites before running.')}</Alert><Button variant="contained" disabled={!readiness.ready || running} onClick={onStart}>Start Reconciliation</Button><Button variant="outlined" onClick={onBack}>Back to Setup</Button></Stack></CardContent></Card>; }
function RunSuccessPanel({ runStatus, telemetry, onContinue }: { runStatus: RunStatus; telemetry: Telemetry; onContinue: () => void }) { const processed = runStatus.processed_count ?? telemetry.processed_terms ?? 0; const total = runStatus.total_count ?? telemetry.total_terms ?? 0; const duration = telemetry.duration_sec ?? runStatus.elapsed_seconds; return <Card variant="outlined" sx={{ borderRadius: 4, borderColor: 'success.light', bgcolor: 'rgba(240,253,244,.72)' }}><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between" alignItems="center"><Typography variant="h6" sx={{ fontWeight: 900 }}>Run completed successfully</Typography><Chip color="success" label="Success" /></Stack><Alert severity="success" variant="outlined">Agent-based reconciliation completed. Review the generated mapping suggestions next.</Alert><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(3, 1fr)' }, gap: 1 }}><SummaryRow label="Processed" value={total ? `${processed} / ${total}` : processed} /><SummaryRow label="Duration" value={typeof duration === 'number' ? `${duration.toFixed(1)}s` : '—'} /><SummaryRow label="Cost" value={`$${(telemetry.total_cost_usd ?? 0).toFixed(4)}`} /></Box><LinearProgress variant="determinate" value={100} sx={{ height: 10, borderRadius: 999 }} /><Button variant="contained" color="success" onClick={onContinue}>Continue to Review</Button></Stack></CardContent></Card>; }
function RunErrorPanel({ error, onBack, onRetry }: { error: string; onBack: () => void; onRetry: () => void }) { return <Card variant="outlined" sx={{ borderRadius: 4, borderColor: 'error.light', bgcolor: 'rgba(254,242,242,.72)' }}><CardContent><Stack spacing={1.5}><Typography variant="h6" sx={{ fontWeight: 900 }}>Run failed</Typography><Alert severity="error" variant="outlined">{error}</Alert><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}><Button variant="contained" color="error" onClick={onRetry}>Retry Reconciliation</Button><Button variant="outlined" onClick={onBack}>Back to Setup</Button></Stack></Stack></CardContent></Card>; }
function RunPage({ config, readiness, runStatus, telemetry, dataStatus, emit }: { config: WorkflowConfig; readiness: ReadinessState; runStatus: RunStatus; telemetry: Telemetry; dataStatus: DataStatus; emit: (event: AppEvent) => void }) { const [optimisticRunning, setOptimisticRunning] = useState(false); const [optimisticStartedAt, setOptimisticStartedAt] = useState<string | null>(null); const optimisticTotalCount = runStatus.total_count ?? telemetry.total_terms ?? dataStatus.rows ?? null; useEffect(() => { if (runStatus.finished || runStatus.error || runStatus.running) { setOptimisticRunning(false); if (runStatus.finished || runStatus.error) setOptimisticStartedAt(null); } }, [runStatus.finished, runStatus.error, runStatus.running]); const running = Boolean(runStatus.running || optimisticRunning); const panelStatus = optimisticRunning && !runStatus.running ? { ...runStatus, running: true, finished: false, error: null, progress: null, started_at: runStatus.started_at ?? optimisticStartedAt, message: runStatus.message || null, total_count: runStatus.total_count ?? telemetry.total_terms ?? dataStatus.rows ?? null } : runStatus; const startRun = () => { setOptimisticStartedAt(new Date().toISOString()); setOptimisticRunning(true); emit({ type: 'start_run' }); }; return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: running ? 'minmax(0, 1fr)' : '360px minmax(0,1fr)' }, gap: 2 }}><Stack spacing={2}>{running ? <AgentRunProgressPanel runStatus={panelStatus} workflow={workflowForRunPanel(config.workflow)} optimisticTotalCount={optimisticTotalCount} /> : runStatus.error ? <RunErrorPanel error={runStatus.error} onRetry={startRun} onBack={() => emit({ type: 'navigate', stage: 'setup' })} /> : runStatus.finished ? <RunSuccessPanel runStatus={runStatus} telemetry={telemetry} onContinue={() => emit({ type: 'navigate', stage: 'review' })} /> : <><RunPrerequisitesPanel readiness={readiness} /><RunSummaryPanel readiness={readiness} config={config} /><RunStartPanel readiness={readiness} running={running} runStatus={runStatus} onStart={startRun} onBack={() => emit({ type: 'navigate', stage: 'setup' })} /></>}</Stack><MonitoringPanel telemetry={telemetry} runStatus={panelStatus} /></Box>; }
function ReviewPage({ review, dataStatus, emit }: { review: ReviewState; dataStatus: DataStatus; emit: (event: AppEvent) => void }) {
  const [status, setStatus] = useState('all');
  const [matchType, setMatchType] = useState('all');
  const [provider, setProvider] = useState('all');
  const [selected, setSelected] = useState<ReviewItem | null>(null);
  const [selectedMatchTypes, setSelectedMatchTypes] = useState<Record<string, string>>({});
  const providers = unique((review.items ?? []).map((i) => String(i.provider || '')).filter(Boolean));
  const filtered = (review.items ?? []).filter((item) =>
    (status === 'all' || String(item.status || 'pending') === status)
    && (matchType === 'all' || String(item.match_type || 'no_match') === matchType)
    && (provider === 'all' || String(item.provider || '') === provider)
  );
  const isNoMatchItem = (item: ReviewItem) => String(item.status || '').toLowerCase() === 'no_match' || String(item.match_type || '').toLowerCase() === 'no_match';
  const canAcceptItem = (item: ReviewItem) => item.can_accept !== false && !isNoMatchItem(item) && Boolean(String(item.suggested_uri || '').trim());
  const selectedMatchTypeFor = (item: ReviewItem) => selectedMatchTypes[item.mapping_id] || normalizeEditableSkosMatchType(item.match_type || item.accepted_match_type);
  const updateSelectedMatchType = (item: ReviewItem, value: string) => setSelectedMatchTypes((current) => ({ ...current, [item.mapping_id]: normalizeEditableSkosMatchType(value) }));
  const traceOf = (item: ReviewItem) => item.trace_metadata ?? {};

  return (
    <Stack spacing={2}>
      <Card variant="outlined">
        <CardContent>
          <Stack spacing={1.5}>
            <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}>
              <Stack>
                <Typography variant="subtitle1">Working table preview</Typography>
                <Typography variant="body2" color="text.secondary">
                  Live matching table snapshot. Accept/reject/reset actions update this table through the Python backend event handler.
                </Typography>
              </Stack>
              <Chip size="small" label={dataStatus.has_table ? 'live table loaded' : 'no table loaded'} color={dataStatus.has_table ? 'success' : 'warning'} />
            </Stack>
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1 }}>
              <SummaryRow label="File" value={dataStatus.filename || dataStatus.source_name || '—'} />
              <SummaryRow label="Rows" value={dataStatus.rows ?? 0} />
              <SummaryRow label="Columns" value={dataStatus.columns ?? 0} />
              <SummaryRow label="Schema" value={dataStatus.schema_message || 'No schema detected'} />
            </Box>
            <DataTable rows={dataStatus.preview} empty="No live working table preview available yet." />
          </Stack>
        </CardContent>
      </Card>

      <Card variant="outlined">
        <CardContent>
          <Stack spacing={1.5}>
            <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}>
              <Typography variant="subtitle1">Review Suggestions</Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap">
                <Chip label={`Pending ${review.counts?.pending ?? 0}`} color="warning" />
                <Chip label={`Matched ${review.counts?.matched ?? 0}`} color="success" />
                <Chip label={`Review suggested ${review.counts?.candidate_suggested ?? 0}`} color="info" />
                <Chip label={`Accepted ${review.counts?.accepted ?? 0}`} color="success" />
                <Chip label={`Rejected ${review.counts?.rejected ?? 0}`} />
                <Chip label={`No match ${review.counts?.no_match ?? 0}`} />
              </Stack>
            </Stack>
            <Alert severity="info" variant="outlined">
              Rows marked <strong>Review suggested candidate</strong> are visible manual-review candidates, often from Wikidata after BioPortal did not produce a verified match. Rows marked <strong>no_match</strong> are negative decisions. Any low-confidence candidate kept for audit is shown only in Details; accepting is disabled so “Accept” never writes a rejected URI.
              For reviewable suggestions, adjust the SKOS match type in the table before clicking <strong>Accept</strong> if the agent’s proposed predicate is not appropriate.
            </Alert>

            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4, 1fr)' }, gap: 1 }}>
              <FormControl size="small"><InputLabel>Status</InputLabel><Select label="Status" value={status} onChange={(e) => setStatus(String(e.target.value))}>{reviewStatuses.map((s) => <MenuItem key={s} value={s}>{s}</MenuItem>)}</Select></FormControl>
              <FormControl size="small"><InputLabel>Match type</InputLabel><Select label="Match type" value={matchType} onChange={(e) => setMatchType(String(e.target.value))}>{matchTypes.map((m) => <MenuItem key={m} value={m}>{m}</MenuItem>)}</Select></FormControl>
              <FormControl size="small"><InputLabel>Provider</InputLabel><Select label="Provider" value={provider} onChange={(e) => setProvider(String(e.target.value))}><MenuItem value="all">all</MenuItem>{providers.map((p) => <MenuItem key={p} value={p}>{p}</MenuItem>)}</Select></FormControl>
              <Button variant="contained" onClick={() => emit({ type: 'navigate', stage: 'export' })}>Continue to Export</Button>
            </Box>

            <Typography variant="subtitle2">Results table (live)</Typography>
            <Box sx={{ overflow: 'auto' }}>
              <Box component="table" sx={{ width: '100%', borderCollapse: 'collapse', minWidth: 980 }}>
                <thead>
                  <tr>{['Term','Status','Match Type','Provider','Confidence','Suggested Label','Suggested URI','Actions'].map((h) => <Box component="th" key={h} sx={{ textAlign: 'left', p: 1, bgcolor: '#f8fafc', fontSize: 12 }}>{h}</Box>)}</tr>
                </thead>
                <tbody>
                  {filtered.map((item) => {
                    const noMatch = isNoMatchItem(item);
                    const canAccept = canAcceptItem(item);
                    const selectedMatchType = selectedMatchTypeFor(item);
                    return (
                      <tr key={item.mapping_id}>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}>{item.term}</Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}><Chip size="small" label={statusLabel(item.status)} sx={reviewStatusChipSx(item.status)} /></Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0', minWidth: 210 }}>
                          {canAccept ? (
                            <FormControl size="small" fullWidth>
                              <Select
                                value={selectedMatchType}
                                onChange={(event) => updateSelectedMatchType(item, String(event.target.value))}
                                sx={{
                                  fontSize: 13,
                                  fontWeight: 700,
                                  borderRadius: 999,
                                  ...skosChipSx(selectedMatchType),
                                  '& .MuiSelect-select': { py: 0.55, px: 1.4 },
                                  '& fieldset': { borderColor: 'transparent' },
                                }}
                              >
                                {editableSkosMatchTypes.map((option) => <MenuItem key={option} value={option}>{option}</MenuItem>)}
                              </Select>
                            </FormControl>
                          ) : (
                            <Chip size="small" label={item.match_type || 'no_match'} sx={skosChipSx(item.match_type)} />
                          )}
                        </Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}>{noMatch ? '—' : item.provider}</Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}>{item.confidence}</Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}>{noMatch ? <Typography variant="body2" color="text.secondary">No acceptable suggestion</Typography> : item.suggested_label}</Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0', maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{noMatch ? <Typography variant="body2" color="text.secondary">—</Typography> : item.suggested_uri}</Box>
                        <Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}>
                          <Stack direction="row" spacing={0.5}>
                            <Button size="small" onClick={() => setSelected(item)}>Details</Button>
                            {canAccept ? (
                              <Button size="small" color="success" onClick={() => emit({ type: 'accept_mapping', mapping_id: item.mapping_id, selected_match_type: selectedMatchType })}>Accept</Button>
                            ) : noMatch ? (
                              <Button size="small" color="warning" onClick={() => emit({ type: 'reject_mapping', mapping_id: item.mapping_id })}>Acknowledge no match</Button>
                            ) : (
                              <Button size="small" color="success" disabled>Accept</Button>
                            )}
                            {!noMatch && <Button size="small" color="warning" onClick={() => emit({ type: 'reject_mapping', mapping_id: item.mapping_id })}>Reject</Button>}
                            <Button size="small" onClick={() => emit({ type: 'reset_mapping', mapping_id: item.mapping_id })}>Reset</Button>
                          </Stack>
                        </Box>
                      </tr>
                    );
                  })}
                </tbody>
              </Box>
            </Box>

            {!filtered.length && <Alert severity="info">No review rows match the selected filters.</Alert>}
          </Stack>
        </CardContent>
      </Card>

      <Drawer anchor="right" open={Boolean(selected)} onClose={() => setSelected(null)}>
        <Box sx={{ width: 520, p: 2 }}>
          <Stack spacing={1.5}>
            <Typography variant="h6">Input vs Agent suggestion</Typography>
            {selected && (
              <>
                {isNoMatchItem(selected) && <Alert severity="warning" variant="outlined">{selected.no_match_note || 'This row is a no-match decision. A candidate may be shown below for audit only, but accepting is intentionally disabled.'}</Alert>}
                {String(selected.status || '').toLowerCase() === 'candidate_suggested' && <Alert severity="info" variant="outlined">This candidate was found after BioPortal did not produce a verified match. It requires manual review because it did not satisfy the strict verified-match policy.</Alert>}
                {Boolean(traceOf(selected).provider_escalation_used) && <Paper variant="outlined" sx={{ p: 1.2, borderRadius: 2, bgcolor: '#f8fafc' }}><Stack direction="row" spacing={1} flexWrap="wrap"><Chip size="small" label={`${traceOf(selected).provider_escalation_from || 'BioPortal'} checked → no verified match`} /><Chip size="small" color="info" label={`${traceOf(selected).provider_escalation_to || 'Wikidata'} second pass started`} /><Chip size="small" color={traceOf(selected).wikidata_second_pass_has_candidate ? 'success' : 'default'} label={traceOf(selected).wikidata_second_pass_has_candidate ? 'Wikidata candidate found' : 'Wikidata checked → no suitable candidate'} /></Stack></Paper>}
                <ComparisonRow leftLabel="Input term" leftValue={selected.term} rightLabel="Suggested term" rightValue={selected.suggested_label} />
                <ComparisonRow leftLabel="Input definition" leftValue={selected.definition} rightLabel="Suggested description" rightValue={selected.suggested_description} />
                <ComparisonRow leftLabel="Input URI" leftValue={selected.input_uri} rightLabel="Suggested URI" rightValue={selected.suggested_uri} />
                {isNoMatchItem(selected) && (selected.candidate_uri || selected.candidate_label) && <>
                  <ComparisonRow leftLabel="Audit candidate label" leftValue={selected.candidate_label} rightLabel="Audit candidate URI" rightValue={selected.candidate_uri} />
                  <ComparisonRow leftLabel="Audit candidate description" leftValue={selected.candidate_description} rightLabel="Why not accepted" rightValue={selected.no_match_note || selected.explanation} />
                </>}
                <ComparisonRow leftLabel="Current/accepted match type" leftValue={selected.accepted_match_type} rightLabel="Suggested match type" rightValue={<Chip size="small" label={selected.match_type || 'no_match'} sx={skosChipSx(selected.match_type)} />} />
                <ComparisonRow leftLabel="Input subject label" leftValue={selected.subject_label} rightLabel="Suggested provider" rightValue={selected.provider} />
                <Divider />
                {Boolean(traceOf(selected).provider_signal_boost_applied) && <Alert severity="info" variant="outlined">Confidence adjusted by lexical/provider signal.</Alert>}
                <SummaryRow label="Provider" value={selected.provider || '—'} />
                <SummaryRow label="Mapping type" value={selected.match_type || 'no_match'} />
                <SummaryRow label="Decision source" value={selected.decision_source || '—'} />
                <SummaryRow label="Confidence" value={String(selected.confidence ?? '—')} />
                <SummaryRow label="Confidence before boost" value={String(traceOf(selected).confidence_before_boost ?? '—')} />
                <SummaryRow label="Confidence after boost" value={String(traceOf(selected).confidence_after_boost ?? '—')} />
                <SummaryRow label="Candidate review mode" value={selected.review_mode ? formatReviewMode(selected.review_mode) : '—'} />
                <SummaryRow label="Fallback" value={selected.fallback_reason ? `yes (${selected.fallback_reason})` : 'no'} />
                <SummaryRow label="Boost reason" value={String(traceOf(selected).provider_signal_boost_reason ?? '—')} />
                <SummaryRow label="Wikidata mapping type" value={String(traceOf(selected).wikidata_second_pass_mapping_type ?? '—')} />
                <SummaryRow label="Wikidata decision source" value={String(traceOf(selected).wikidata_second_pass_decision_source ?? '—')} />
                <SummaryRow label="Wikidata fallback" value={traceOf(selected).wikidata_second_pass_fallback_reason ? `yes (${traceOf(selected).wikidata_second_pass_fallback_reason})` : 'no'} />
                <SummaryRow label="Explanation" value={selected.explanation || '—'} />
              </>
            )}
          </Stack>
        </Box>
      </Drawer>
    </Stack>
  );
}
function ExportPage({ review, dataStatus, exportPayload, emit }: { review: ReviewState; dataStatus: DataStatus; exportPayload?: ExportPayload | null; emit: (event: AppEvent) => void }) {
  const preparedContent = exportPayload?.content ?? '';
  const preparedMimeType = exportPayload?.mime_type || 'text/csv;charset=utf-8';
  const preparedFilename = exportPayload?.filename || 'agent_reconciliation_sssom_export.csv';
  const preparedDownloadUrl = useMemo(() => {
    if (!preparedContent) return '';
    const blob = new Blob([preparedContent], { type: preparedMimeType });
    return window.URL.createObjectURL(blob);
  }, [preparedContent, preparedMimeType, exportPayload?.nonce]);

  useEffect(() => () => {
    if (preparedDownloadUrl) window.URL.revokeObjectURL(preparedDownloadUrl);
  }, [preparedDownloadUrl]);

  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: '1fr 1fr' }, gap: 2 }}><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Export Summary</Typography><SummaryRow label="Source" value={dataStatus.source_name || dataStatus.filename || '—'} /><SummaryRow label="Accepted mappings" value={review.counts?.accepted ?? 0} /><SummaryRow label="Rejected" value={review.counts?.rejected ?? 0} /><SummaryRow label="No match" value={review.counts?.no_match ?? 0} /><SummaryRow label="Pending" value={review.counts?.pending ?? 0} /></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.3}><Typography variant="subtitle1">SSSOM Export</Typography><Typography variant="body2" color="text.secondary">Download the finalized accepted mapping table as an SSSOM-oriented CSV snapshot.</Typography><Button variant="contained" onClick={() => emit({ type: 'export_sssom' })}>Prepare SSSOM Export</Button>{preparedDownloadUrl ? <><Alert severity="success" variant="outlined">SSSOM export is prepared. If your browser blocked the automatic download, use the download button below.</Alert><Button variant="contained" component="a" href={preparedDownloadUrl} download={preparedFilename}>Download Prepared SSSOM CSV</Button></> : <Alert severity="info" variant="outlined">Prepare the export first. The download button appears here after the Python backend creates the CSV snapshot.</Alert>}</Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.3}><Typography variant="subtitle1">RDF Generator Handoff</Typography><Typography variant="body2" color="text.secondary">Publish accepted mappings to the shared backend handoff for the RDF Generator.</Typography><Button variant="contained" color="success" onClick={() => emit({ type: 'publish_rdf_handoff' })}>Publish to RDF Generator</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.3}><Typography variant="subtitle1">Rejected / No-match Summary</Typography><Alert severity="info" variant="outlined">Rejected and no-match rows remain visible for audit and curation records; export currently finalizes accepted mappings.</Alert></Stack></CardContent></Card></Box>;
}

export function AgentReconciliationMuiApp({ args, onEvent }: AgentReconciliationAppProps) {
  const providers = args?.providers ?? [];
  const providerLabels = args?.providerLabels ?? {};
  const baseModels = args?.models ?? [];
  const modelLabels = args?.modelLabels ?? {};
  const reasoningOptions = args?.reasoningOptions ?? ['none', 'low', 'medium', 'high', 'xhigh'];
  const readiness = args?.readiness ?? { ready: false, checks: [], summary: {} };
  const dataStatus = args?.data_status ?? {};
  const runStatus = args?.run_status ?? { ready: readiness.ready, running: false, finished: false, progress: 0, message: 'Ready to run' };
  const telemetry = args?.telemetry ?? { enabled: false, llm_calls: [], logs: [] };
  const review = args?.review ?? { items: [], counts: { pending: 0, accepted: 0, rejected: 0, no_match: 0 } };
  const providerKind = args?.providerKind ?? 'standard';
  const ontologyOptions = args?.ontologyOptions ?? [];
  const downloadedExportNonceRef = useRef<number | string | null>(null);
  const [config, setConfig] = useState<WorkflowConfig>(() => normalizeConfig(args?.config, providers, baseModels));
  useEffect(() => setConfig(normalizeConfig(args?.config, providers, baseModels)), [args?.config, providers, baseModels]);
  const modelOptions = useMemo(() => unique([...baseModels, config.custom_model_override || '', config.model, config.definition_model || '', config.planner_model || '']), [baseModels, config.custom_model_override, config.model, config.definition_model, config.planner_model]);
  const activeStage = normalizeStage(args?.active_stage ?? args?.activeStage);
  useEffect(() => {
    window.scrollTo(0, 0);
    try {
        const mainContent = window.parent.document.querySelector('section.main');
        if (mainContent) {
            mainContent.scrollTo({ top: 0, behavior: 'smooth' });
        } else {
            window.parent.scrollTo({ top: 0, behavior: 'smooth' });
        }
    } catch (e) {
        console.error("Failed to scroll parent window", e);
    }
  }, [activeStage]);
  useEffect(() => {
    const payload = args?.exportPayload;
    const content = payload?.content ?? '';
    const nonce = payload?.nonce ?? null;
    if (!payload || !content || nonce === null || downloadedExportNonceRef.current === nonce) return;

    downloadedExportNonceRef.current = nonce;
    const blob = new Blob([content], { type: payload.mime_type || 'text/csv;charset=utf-8' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = payload.filename || 'agent_reconciliation_sssom_export.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.setTimeout(() => window.URL.revokeObjectURL(url), 0);
  }, [args?.exportPayload]);
  function emit(event: AppEvent) { onEvent?.({ ...event, nonce: Date.now() }); }
  function update(patch: Partial<WorkflowConfig>) { const next = normalizeConfig({ ...config, ...patch }, providers, modelOptions); setConfig(next); emit({ type: 'config_changed', config: next }); }
  function navigate(stage: Stage) { emit({ type: 'navigate', stage }); }
  let page: React.ReactNode;
  if (activeStage === 'setup') page = <SetupPage config={config} dataStatus={dataStatus} readiness={readiness} providers={providers} providerLabels={providerLabels} modelOptions={modelOptions} modelLabels={modelLabels} modelDetails={args?.modelDetails} reasoningOptions={reasoningOptions} ontologyOptions={ontologyOptions} providerKind={providerKind} codexAuthStatus={args?.codexAuthStatus} update={update} emit={emit} />;
  else if (activeStage === 'run') page = <RunPage config={config} readiness={readiness} runStatus={runStatus} telemetry={telemetry} dataStatus={dataStatus} emit={emit} />;
  else if (activeStage === 'review') page = <ReviewPage review={review} dataStatus={dataStatus} emit={emit} />;
  else page = <ExportPage review={review} dataStatus={dataStatus} exportPayload={args?.exportPayload} emit={emit} />;
  return <AppShell activeStage={activeStage} onNavigate={navigate} dataStatus={dataStatus} runStatus={runStatus} review={review}>{args?.statusMessage?.text && <Alert severity={args.statusMessage.severity ?? 'info'}>{args.statusMessage.text}</Alert>}{page}</AppShell>;
}

export const WorkflowConfigPanel = AgentReconciliationMuiApp;
