import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Checkbox,
  Chip,
  Collapse,
  Divider,
  FormControl,
  FormControlLabel,
  InputLabel,
  LinearProgress,
  MenuItem,
  Paper,
  Select,
  Slider,
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
import { Streamlit } from 'streamlit-component-lib';

type Severity = 'success' | 'info' | 'warning' | 'error';
type AppEvent = { type: string; [key: string]: unknown };
type TableRow = Record<string, unknown>;
type Stage = 'load' | 'configure' | 'run' | 'reconcile' | 'export';

type ProviderStatus = { name: string; position?: number; status?: string; results_count?: number; error_msg?: string; has_results?: boolean };
type SuggestionOption = { display: string; uri: string; source?: string; label?: string; description?: string; [key: string]: unknown };
type ReconciliationRow = {
  row_index: number | string;
  term?: string;
  subject_label?: string;
  current_uri?: string;
  raw_uri?: string;
  object_label?: string;
  source_provider?: string;
  match_type?: string;
  mapping_justification?: string;
  selected_display?: string;
  options?: SuggestionOption[];
  has_suggestions?: boolean;
  custom_search_summary?: { search_term?: string; results_count?: number; providers?: string[] } | null;
};

type Snapshot = {
  active_stage?: Stage;
  statusMessage?: { severity?: Severity; text?: string } | null;
  data?: {
    has_table?: boolean;
    rows?: number;
    columns?: number;
    filename?: string;
    source_message?: string;
    shared_table_available?: boolean;
    shared_rows?: number;
    required_columns_detected?: boolean;
    total_terms?: number;
    curated_preview?: TableRow[];
    provider_context_preview?: TableRow[];
  };
  config?: {
    available_providers?: string[];
    provider_tooltips?: Record<string, string>;
    provider_queue?: string[];
    provider_status?: ProviderStatus[];
    provider_has_results?: string[];
    display_provider?: string | null;
    display_mixed_results?: boolean;
    custom_sparql_enabled?: boolean;
    custom_sparql_endpoint?: string;
    custom_sparql_query_template?: string;
    custom_sparql_var_uri?: string;
    custom_sparql_var_label?: string;
    custom_sparql_var_description?: string;
    ncbi_all_databases?: string[];
    ncbi_selected_databases?: string[];
    local_backend?: string;
    local_resources?: { name?: string; backend?: string; entities?: number; parse_backend?: string }[];
    ontology_loading_status?: Record<string, string>;
    available_ontologies_by_provider?: Record<string, string[]>;
    selected_ontologies_by_provider?: Record<string, string[]>;
    matching_strategy?: string;
    suggestion_slider?: number;
    levenshtein_threshold?: number;
    show_only_matched_terms?: boolean;
    show_only_unreconciled_terms?: boolean;
    items_per_page?: number;
    skos_matching_enabled?: boolean;
    missing_config_alerts?: string[];
  };
  run?: {
    processing_active?: boolean;
    processed_terms?: number;
    total_terms?: number;
    progress?: number;
    current_term_index?: number;
    can_start?: boolean;
    missing_start_configs?: string[];
  };
  reconciliation?: { display_mode?: string | null; rows?: ReconciliationRow[]; total_rows?: number; page?: number; total_pages?: number };
  downloads?: { sssom_csv?: string; sssom_filename?: string; candidate_review_csv?: string; candidate_review_filename?: string };
};

type StreamlitProps = { args?: { app?: string; snapshot?: Snapshot } };

const stages: { id: Stage; label: string; caption: string }[] = [
  { id: 'load', label: 'Load', caption: 'Table input' },
  { id: 'configure', label: 'Configure', caption: 'Sources & filters' },
  { id: 'run', label: 'Run', caption: 'Fetch candidates' },
  { id: 'reconcile', label: 'Reconcile', caption: 'Curate mappings' },
  { id: 'export', label: 'Export', caption: 'SSSOM & handoff' },
];

const skosTypes = ['', 'skos:exactMatch', 'skos:closeMatch', 'skos:broadMatch', 'skos:narrowMatch', 'skos:relatedMatch'];
const ontologyProviderOrder = ['BioPortal', 'OLS (EBI)', 'SemLookP', 'AgroPortal', 'EarthPortal'];

const ReconciliationIcon = () => (
  <svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg" role="img" style={{ marginRight: 12 }}>
    <defs><linearGradient id="reconGradient" x1="8" y1="10" x2="56" y2="54" gradientUnits="userSpaceOnUse"><stop stopColor="#2563EB"/><stop offset=".52" stopColor="#0891B2"/><stop offset="1" stopColor="#14B8A6"/></linearGradient><filter id="reconShadow" x="-40%" y="-40%" width="180%" height="180%"><feDropShadow dx="0" dy="5" stdDeviation="5" floodColor="#0F172A" floodOpacity="0.18"/></filter></defs>
    <circle cx="18" cy="20" r="7" fill="#fff" stroke="#2563EB" strokeWidth="2.4" filter="url(#reconShadow)"/>
    <circle cx="18" cy="44" r="7" fill="#fff" stroke="#14B8A6" strokeWidth="2.4"/>
    <circle cx="46" cy="32" r="10" fill="url(#reconGradient)" filter="url(#reconShadow)"/>
    <path d="M24.8 21.8C31 23 34 27 37 30" stroke="url(#reconGradient)" strokeWidth="3" strokeLinecap="round"/>
    <path d="M24.8 42.2C31 41 34 37 37 34" stroke="url(#reconGradient)" strokeWidth="3" strokeLinecap="round"/>
    <path d="M41.7 32.2L44.7 35.2L51.5 28" stroke="#fff" strokeWidth="2.7" strokeLinecap="round" strokeLinejoin="round"/>
  </svg>
);

function emit(event: AppEvent) { Streamlit.setComponentValue({ ...event, nonce: Date.now() }); window.setTimeout(() => Streamlit.setFrameHeight(), 0); }
function asNumber(value: unknown, fallback: number) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : fallback; }
function unique(values: string[]) { return [...new Set(values.map((v) => String(v || '').trim()).filter(Boolean))]; }
function triggerDownload(content: string, filename: string, mime = 'text/csv;charset=utf-8') { const blob = new Blob([content], { type: mime }); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = filename; a.click(); window.setTimeout(() => URL.revokeObjectURL(url), 1000); }

function DataTable({ rows, empty, maxColumns = 8 }: { rows?: TableRow[]; empty: string; maxColumns?: number }) {
  const safeRows = rows ?? [];
  const columns = safeRows.length ? Object.keys(safeRows[0]).slice(0, maxColumns) : [];
  if (!safeRows.length) return <Typography variant="body2" color="text.secondary">{empty}</Typography>;
  return <Box sx={{ overflow: 'auto', border: '1px solid', borderColor: 'divider', borderRadius: 2 }}><Box component="table" sx={{ width: '100%', minWidth: 720, borderCollapse: 'collapse' }}><thead><tr>{columns.map((column) => <Box component="th" key={column} sx={{ p: 1, textAlign: 'left', fontSize: 12, bgcolor: '#f8fafc', position: 'sticky', top: 0 }}>{column}</Box>)}</tr></thead><tbody>{safeRows.slice(0, 100).map((row, idx) => <tr key={idx}>{columns.map((column) => <Box component="td" key={column} sx={{ p: 1, borderTop: '1px solid #e2e8f0', fontSize: 12, maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(row[column] ?? '')}</Box>)}</tr>)}</tbody></Box></Box>;
}
function SummaryRow({ label, value }: { label: string; value: React.ReactNode }) { return <Stack direction="row" justifyContent="space-between" spacing={2}><Typography variant="body2" color="text.secondary">{label}</Typography><Typography variant="body2" sx={{ fontWeight: 800, textAlign: 'right' }}>{value}</Typography></Stack>; }
function ToggleCard({ checked, title, description, onChange }: { checked: boolean; title: string; description: string; onChange: (value: boolean) => void }) { return <Paper variant="outlined" sx={{ p: 1.5, minHeight: 94, borderRadius: 3, borderColor: checked ? 'primary.main' : 'divider', bgcolor: checked ? 'rgba(37,99,235,.04)' : 'background.paper' }}><Stack direction="row" spacing={1} alignItems="flex-start"><Checkbox checked={checked} onChange={(e) => onChange(e.target.checked)} sx={{ p: 0 }} /><Stack><Typography variant="body2" sx={{ fontWeight: 850 }}>{title}</Typography><Typography variant="caption" color="text.secondary">{description}</Typography></Stack></Stack></Paper>; }

function AppShell({ snapshot, activeStage, children }: { snapshot: Snapshot; activeStage: Stage; children: React.ReactNode }) {
  const data = snapshot.data ?? {};
  const run = snapshot.run ?? {};
  const activeStep = stages.findIndex((s) => s.id === activeStage);
  return <Box sx={{ bgcolor: '#eef7fb', minHeight: '100vh', p: { xs: 1, md: 2 }, borderRadius: 4 }}><Stack spacing={2}>
    <Paper variant="outlined" sx={{ p: { xs: 2, md: 2.5 }, borderRadius: 4, background: 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 50%,#eff6ff 100%)', boxShadow: '0 18px 48px rgba(15,23,42,.08)' }}>
      <Stack direction={{ xs: 'column', lg: 'row' }} spacing={2} alignItems={{ xs: 'stretch', lg: 'center' }} justifyContent="space-between">
        <Stack direction="row" spacing={1.5} alignItems="center"><ReconciliationIcon /><Stack><Typography variant="h5">Semi-Automatic Reconciliation</Typography><Typography variant="body2" color="text.secondary">Guided workflow for provider queues, candidate review, SKOS curation, and SSSOM export</Typography></Stack></Stack>
        <Stepper nonLinear activeStep={activeStep} sx={{ minWidth: { lg: 700 } }}>{stages.map((stage, idx) => <Step key={stage.id} completed={idx < activeStep}><StepButton onClick={() => emit({ type: 'navigate', stage: stage.id })}><Stack spacing={0}><Typography variant="body2" sx={{ fontWeight: 800 }}>{stage.label}</Typography><Typography variant="caption" color="text.secondary">{stage.caption}</Typography></Stack></StepButton></Step>)}</Stepper>
      </Stack>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', md: 'repeat(4,1fr)' }, gap: 1.2, mt: 2 }}>
        <Chip label={data.has_table ? `${data.rows ?? 0} rows loaded` : 'No table loaded'} color={data.has_table ? 'success' : 'warning'} />
        <Chip label={data.required_columns_detected ? 'SSSOM schema valid' : 'Schema pending'} color={data.required_columns_detected ? 'success' : 'warning'} />
        <Chip label={`${data.total_terms ?? 0} terms need URI`} color={(data.total_terms ?? 0) ? 'info' : 'default'} />
        <Chip label={run.processing_active ? 'Processing' : `${run.processed_terms ?? 0}/${run.total_terms ?? 0} processed`} color={run.processing_active ? 'info' : 'default'} />
      </Box>
    </Paper>
    {snapshot.statusMessage?.text && <Alert severity={snapshot.statusMessage.severity ?? 'info'}>{snapshot.statusMessage.text}</Alert>}
    {children}
  </Stack></Box>;
}

function LoadPage({ snapshot }: { snapshot: Snapshot }) {
  const data = snapshot.data ?? {};
  const [uploadError, setUploadError] = useState('');
  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]; event.target.value = ''; setUploadError(''); if (!file) return;
    try { const buffer = await file.arrayBuffer(); const bytes = new Uint8Array(buffer); let binary = ''; bytes.forEach((b) => { binary += String.fromCharCode(b); }); emit({ type: 'upload_table', filename: file.name, content_base64: window.btoa(binary) }); } catch (error) { setUploadError(error instanceof Error ? error.message : 'Unable to read uploaded file.'); }
  }
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: 'minmax(0,1fr) 360px' }, gap: 2 }}><Stack spacing={2}>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}>
      <Stack direction="row" justifyContent="space-between"><Stack><Typography variant="subtitle1">1. Select Data Source for Reconciliation</Typography><Typography variant="body2" color="text.secondary">Load a strict SSSOM matching table from the generator or upload CSV/XLSX/XLS directly.</Typography></Stack><Chip label={data.has_table ? 'loaded' : 'waiting'} color={data.has_table ? 'success' : 'warning'} /></Stack>
      <Paper variant="outlined" sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed', borderRadius: 3, bgcolor: '#f8fafc' }}><Typography variant="h6">Matching table input</Typography><Typography variant="body2" color="text.secondary" sx={{ mt: .5 }}>Required columns: subject_id, subject_label, predicate_id, object_id, object_label, mapping_justification.</Typography><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} justifyContent="center" sx={{ mt: 1.5 }}><Button component="label" variant="contained">Upload CSV / Excel<input hidden type="file" accept=".csv,.xlsx,.xls,text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" onChange={handleUpload} /></Button><Button variant="outlined" disabled={!data.shared_table_available} onClick={() => emit({ type: 'load_shared_table' })}>Load from Matching Table Generator</Button></Stack>{uploadError && <Alert severity="warning" sx={{ mt: 1.5 }}>{uploadError}</Alert>}</Paper>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1 }}><SummaryRow label="File" value={data.filename || '—'} /><SummaryRow label="Rows" value={data.rows ?? 0} /><SummaryRow label="Columns" value={data.columns ?? 0} /><SummaryRow label="Shared rows" value={data.shared_rows ?? 0} /></Box>
      <Typography variant="subtitle2">Curated SSSOM preview</Typography><DataTable rows={data.curated_preview} empty="No matching table loaded yet." maxColumns={6} />
    </Stack></CardContent></Card>
  </Stack><Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.1}><Typography variant="subtitle1">Workflow guide</Typography>{['Load or upload a matching table.', 'Configure sources, local files, ontology filters and query strategy.', 'Process the provider queue to fetch candidate URIs.', 'Review mappings inline and run custom searches.', 'Export SSSOM or publish to RDF Generator.'].map((item) => <Typography key={item} variant="body2" color="text.secondary">✓ {item}</Typography>)}<Button variant="contained" disabled={!data.has_table} onClick={() => emit({ type: 'navigate', stage: 'configure' })}>Continue to configuration</Button></Stack></CardContent></Card></Stack></Box>;
}

function ConfigurePage({ snapshot }: { snapshot: Snapshot }) {
  const cfg = snapshot.config ?? {};
  const [queue, setQueue] = useState<string[]>(cfg.provider_queue ?? []);
  const [customEnabled, setCustomEnabled] = useState(Boolean(cfg.custom_sparql_enabled));
  const [sparqlEndpoint, setSparqlEndpoint] = useState(cfg.custom_sparql_endpoint ?? '');
  const [sparqlQuery, setSparqlQuery] = useState(cfg.custom_sparql_query_template ?? '');
  const [uriVar, setUriVar] = useState(cfg.custom_sparql_var_uri ?? 'uri');
  const [labelVar, setLabelVar] = useState(cfg.custom_sparql_var_label ?? 'label');
  const [descVar, setDescVar] = useState(cfg.custom_sparql_var_description ?? 'description');
  const [ncbiDbs, setNcbiDbs] = useState<string[]>(cfg.ncbi_selected_databases ?? []);
  const [localBackend, setLocalBackend] = useState(cfg.local_backend ?? 'auto');
  const [localFiles, setLocalFiles] = useState<{ filename: string; content_base64: string }[]>([]);
  const ontologySelections = cfg.selected_ontologies_by_provider ?? {};
  useEffect(() => setQueue(cfg.provider_queue ?? []), [cfg.provider_queue]);
  useEffect(() => setCustomEnabled(Boolean(cfg.custom_sparql_enabled)), [cfg.custom_sparql_enabled]);
  useEffect(() => setSparqlEndpoint(cfg.custom_sparql_endpoint ?? ''), [cfg.custom_sparql_endpoint]);
  useEffect(() => setSparqlQuery(cfg.custom_sparql_query_template ?? ''), [cfg.custom_sparql_query_template]);
  useEffect(() => setUriVar(cfg.custom_sparql_var_uri ?? 'uri'), [cfg.custom_sparql_var_uri]);
  useEffect(() => setLabelVar(cfg.custom_sparql_var_label ?? 'label'), [cfg.custom_sparql_var_label]);
  useEffect(() => setDescVar(cfg.custom_sparql_var_description ?? 'description'), [cfg.custom_sparql_var_description]);
  useEffect(() => setNcbiDbs(cfg.ncbi_selected_databases ?? []), [cfg.ncbi_selected_databases]);
  useEffect(() => setLocalBackend(cfg.local_backend ?? 'auto'), [cfg.local_backend]);
  const saveSettings = (extra: Record<string, unknown> = {}) => emit({ type: 'update_settings', settings: { custom_sparql_enabled: customEnabled, custom_sparql_endpoint: sparqlEndpoint, custom_sparql_query_template: sparqlQuery, custom_sparql_var_uri: uriVar, custom_sparql_var_label: labelVar, custom_sparql_var_description: descVar, ncbi_selected_databases: ncbiDbs, local_backend: localBackend, ...extra } });
  async function handleLocalFiles(event: React.ChangeEvent<HTMLInputElement>) { const files = Array.from(event.target.files ?? []); event.target.value = ''; const encoded = await Promise.all(files.map(async (file) => { const bytes = new Uint8Array(await file.arrayBuffer()); let binary = ''; bytes.forEach((b) => { binary += String.fromCharCode(b); }); return { filename: file.name, content_base64: window.btoa(binary) }; })); setLocalFiles(encoded); }
  const selectedOntologyProviders = ontologyProviderOrder.filter((provider) => queue.includes(provider));
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: 'minmax(0,1fr) 380px' }, gap: 2 }}><Stack spacing={2}>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between"><Stack><Typography variant="subtitle1">Reconciliation Sources</Typography><Typography variant="body2" color="text.secondary">Select providers and confirm the queue before running candidate retrieval.</Typography></Stack><Chip label={`${queue.length} selected`} color={queue.length ? 'success' : 'warning'} /></Stack><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2,1fr)', lg: 'repeat(3,1fr)' }, gap: 1 }}>{(cfg.available_providers ?? []).map((provider) => <ToggleCard key={provider} checked={queue.includes(provider)} title={provider} description={cfg.provider_tooltips?.[provider] ?? ''} onChange={(checked) => setQueue(checked ? unique([...queue, provider]) : queue.filter((p) => p !== provider))} />)}</Box><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}><Button variant="contained" onClick={() => emit({ type: 'confirm_queue', provider_queue: queue })}>Confirm / Update Queue</Button><Button variant="outlined" onClick={() => setQueue([])}>Clear selection</Button></Stack></Stack></CardContent></Card>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between"><Stack><Typography variant="subtitle1">Local ontology / thesaurus files</Typography><Typography variant="body2" color="text.secondary">Index local resources for the Local Ontology provider.</Typography></Stack><Chip label={`${cfg.local_resources?.length ?? 0} indexed`} /></Stack><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '220px 1fr auto' }, gap: 1.2 }}><FormControl size="small"><InputLabel>Parser backend</InputLabel><Select label="Parser backend" value={localBackend} onChange={(e) => setLocalBackend(String(e.target.value))}>{['auto','oak','rdflib','tabular'].map((v) => <MenuItem key={v} value={v}>{v}</MenuItem>)}</Select></FormControl><Button component="label" variant="outlined">Upload OWL/OBO/RDF/TTL/JSON-LD/CSV/TSV/XLSX<input hidden multiple type="file" onChange={handleLocalFiles} /></Button><Button variant="contained" disabled={!localFiles.length} onClick={() => emit({ type: 'index_local_resources', backend: localBackend, files: localFiles })}>Index uploaded files</Button></Box><DataTable rows={cfg.local_resources as unknown as TableRow[]} empty="No local resources indexed yet." /></Stack></CardContent></Card>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between"><Stack><Typography variant="subtitle1">Custom SPARQL Provider</Typography><Typography variant="body2" color="text.secondary">Add a configurable SPARQL endpoint as an extra provider in the queue.</Typography></Stack><Switch checked={customEnabled} onChange={(e) => { setCustomEnabled(e.target.checked); saveSettings({ custom_sparql_enabled: e.target.checked }); }} /></Stack><Collapse in={customEnabled}><Stack spacing={1.2}><TextField label="SPARQL Endpoint URL" value={sparqlEndpoint} onChange={(e) => setSparqlEndpoint(e.target.value)} /><TextField label="SPARQL Query Template" multiline minRows={5} value={sparqlQuery} onChange={(e) => setSparqlQuery(e.target.value)} /><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(3,1fr)' }, gap: 1 }}><TextField label="URI Var" value={uriVar} onChange={(e) => setUriVar(e.target.value)} /><TextField label="Label Var" value={labelVar} onChange={(e) => setLabelVar(e.target.value)} /><TextField label="Desc Var" value={descVar} onChange={(e) => setDescVar(e.target.value)} /></Box><Button variant="outlined" onClick={() => saveSettings()}>Save SPARQL settings</Button></Stack></Collapse></Stack></CardContent></Card>
  </Stack><Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">NCBI Database Selection</Typography><FormControl fullWidth size="small"><InputLabel>NCBI databases</InputLabel><Select multiple label="NCBI databases" value={ncbiDbs} onChange={(e) => setNcbiDbs(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value as string[])} renderValue={(selected) => (selected as string[]).join(', ')}>{(cfg.ncbi_all_databases ?? []).map((db) => <MenuItem key={db} value={db}><Checkbox checked={ncbiDbs.includes(db)} />{db}</MenuItem>)}</Select></FormControl><Button variant="outlined" onClick={() => saveSettings()}>Save database selection</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Stack><Typography variant="subtitle1">Ontology Filters</Typography><Typography variant="body2" color="text.secondary">Filters are shown for the currently selected ontology providers after you confirm/update the queue. Favorites from config.yaml are preselected when available.</Typography></Stack>{selectedOntologyProviders.length ? selectedOntologyProviders.map((provider) => { const available = cfg.available_ontologies_by_provider?.[provider] ?? []; const selected = ontologySelections[provider] ?? []; const status = cfg.ontology_loading_status?.[provider] ?? 'pending'; return <Box key={provider}><Typography variant="subtitle2">{provider}</Typography><Typography variant="caption" color="text.secondary">Status: {status}</Typography>{available.length ? <FormControl fullWidth size="small" sx={{ mt: .5 }}><InputLabel>Ontologies</InputLabel><Select multiple label="Ontologies" value={selected.filter((ontology) => available.includes(ontology))} onChange={(e) => emit({ type: 'update_settings', settings: { selected_ontologies_by_provider: { ...ontologySelections, [provider]: typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value } } })} renderValue={(values) => (values as string[]).join(', ')}>{available.map((ontology) => <MenuItem key={ontology} value={ontology}><Checkbox checked={selected.includes(ontology)} />{ontology}</MenuItem>)}</Select></FormControl> : <Typography variant="caption" color="text.secondary" display="block">{status === 'loading' ? 'Loading ontology list…' : status === 'error' ? 'Ontology list could not be loaded. Check provider credentials/configuration.' : 'No ontology list loaded yet.'}</Typography>}</Box>; }) : <Alert severity="info" variant="outlined">Select and confirm BioPortal, OLS (EBI), SemLookP, AgroPortal, or EarthPortal to configure ontology filters.</Alert>}</Stack></CardContent></Card><Button variant="contained" disabled={!snapshot.data?.has_table} onClick={() => emit({ type: 'navigate', stage: 'run' })}>Continue to Run</Button></Stack></Box>;
}

function RunPage({ snapshot }: { snapshot: Snapshot }) {
  const cfg = snapshot.config ?? {}; const run = snapshot.run ?? {};
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: '360px minmax(0,1fr)' }, gap: 2 }}><Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.4}><Stack direction="row" justifyContent="space-between"><Typography variant="subtitle1">Processing Queue Status</Typography><Chip label={run.processing_active ? 'running' : 'idle'} color={run.processing_active ? 'info' : 'default'} /></Stack>{(cfg.provider_status ?? []).length ? (cfg.provider_status ?? []).map((provider) => <Paper key={provider.name} variant="outlined" sx={{ p: 1.2, borderRadius: 2 }}><Stack direction="row" justifyContent="space-between"><Stack><Typography variant="body2" sx={{ fontWeight: 800 }}>{provider.name}</Typography><Typography variant="caption" color="text.secondary">Position {provider.position} • {provider.results_count ?? 0} term(s) with results</Typography>{provider.error_msg && <Typography variant="caption" color="error">{provider.error_msg}</Typography>}</Stack><Chip size="small" label={provider.status ?? 'pending'} color={provider.status === 'completed' ? 'success' : provider.status === 'error' ? 'error' : provider.status === 'running' ? 'info' : 'default'} /></Stack></Paper>) : <Alert severity="info">Queue is empty. Configure providers first.</Alert>}<Button variant="outlined" onClick={() => emit({ type: 'navigate', stage: 'configure' })}>Back to configuration</Button></Stack></CardContent></Card></Stack><Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.5}><Typography variant="subtitle1">Processing Progress</Typography><LinearProgress variant="determinate" value={run.progress ?? 0} sx={{ height: 10, borderRadius: 99 }} /><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', md: 'repeat(4,1fr)' }, gap: 1 }}><SummaryRow label="Processed" value={run.processed_terms ?? 0} /><SummaryRow label="Total" value={run.total_terms ?? 0} /><SummaryRow label="Progress" value={`${run.progress ?? 0}%`} /><SummaryRow label="Current index" value={run.current_term_index ?? 0} /></Box>{(run.missing_start_configs?.length ?? 0) > 0 && <Alert severity="warning">Missing configuration: {run.missing_start_configs?.join(', ')}</Alert>}<Button variant="contained" size="large" disabled={!run.can_start || run.processing_active} onClick={() => emit({ type: 'start_processing' })}>Start Processing Queue</Button><Button variant="outlined" disabled={!cfg.provider_has_results?.length} onClick={() => emit({ type: 'navigate', stage: 'reconcile' })}>Continue to Reconcile</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Candidate display</Typography><Stack direction="row" flexWrap="wrap" gap={1}>{(cfg.provider_has_results?.length ?? 0) >= 2 && <Button variant={cfg.display_mixed_results ? 'contained' : 'outlined'} onClick={() => emit({ type: 'select_display_provider', provider: 'Mixed Results' })}>Mixed Results</Button>}{(cfg.provider_status ?? []).filter((p) => p.status !== 'pending').map((provider) => <Button key={provider.name} variant={cfg.display_provider === provider.name ? 'contained' : 'outlined'} onClick={() => emit({ type: 'select_display_provider', provider: provider.name })}>{provider.name} ({provider.results_count ?? 0})</Button>)}</Stack></Stack></CardContent></Card></Stack></Box>;
}

function ReconcilePage({ snapshot }: { snapshot: Snapshot }) {
  const cfg = snapshot.config ?? {}; const rec = snapshot.reconciliation ?? {}; const [tab, setTab] = useState(0);
  const [customSearch, setCustomSearch] = useState<Record<string, string>>({});
  const updateSettings = (patch: Record<string, unknown>) => emit({ type: 'update_settings', settings: patch });
  const rows = rec.rows ?? [];
  return <Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between"><Stack><Typography variant="subtitle1">Current Data</Typography><Typography variant="body2" color="text.secondary">Curated result and candidate/provider context are split into table tabs like the former Streamlit expanders.</Typography></Stack><Chip label={`${snapshot.data?.total_terms ?? 0} total terms requiring reconciliation`} color="info" /></Stack><Tabs value={tab} onChange={(_, v) => setTab(v)}><Tab label="Curated SSSOM Result" /><Tab label="Candidate / Provider Context" /></Tabs>{tab === 0 ? <DataTable rows={snapshot.data?.curated_preview} empty="No curated SSSOM preview available." maxColumns={6} /> : <DataTable rows={snapshot.data?.provider_context_preview} empty="No provider context yet." />}</Stack></CardContent></Card>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between"><Stack><Typography variant="subtitle1">Review and Reconcile with: {rec.display_mode || 'No provider selected'}</Typography><Typography variant="body2" color="text.secondary">Strategy: {cfg.matching_strategy}. Use inline selects for URI confirmation and per-row custom search where needed.</Typography></Stack><Button variant="contained" disabled={!rec.display_mode} onClick={() => emit({ type: 'prefill_best_match' })}>Prefill with Best Match</Button></Stack><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(5,1fr)' }, gap: 1 }}><FormControlLabel control={<Switch checked={Boolean(cfg.skos_matching_enabled)} onChange={(e) => updateSettings({ skos_matching_enabled: e.target.checked })} />} label="Enable SKOS Matching" /><FormControlLabel control={<Switch checked={Boolean(cfg.show_only_matched_terms)} onChange={(e) => updateSettings({ show_only_matched_terms: e.target.checked, current_page: 1 })} />} label="Only with suggestions" /><FormControlLabel control={<Switch checked={Boolean(cfg.show_only_unreconciled_terms)} onChange={(e) => updateSettings({ show_only_unreconciled_terms: e.target.checked, current_page: 1 })} />} label="Only unreconciled" /><TextField size="small" type="number" label="Terms per page" value={cfg.items_per_page ?? 10} onChange={(e) => updateSettings({ items_per_page: Math.max(1, asNumber(e.target.value, 10)), current_page: 1 })} /><Stack><Typography variant="caption" color="text.secondary">Levenshtein threshold</Typography><Slider min={0} max={1} step={0.01} value={cfg.levenshtein_threshold ?? 0.7} onChange={(_, v) => updateSettings({ levenshtein_threshold_slider: v })} /></Stack></Box>{!rec.display_mode && <Alert severity="info">Select a provider or Mixed Results from the Run step to enable reconciliation.</Alert>}<Box sx={{ overflow: 'auto' }}><Box component="table" sx={{ width: '100%', borderCollapse: 'collapse', minWidth: 1120 }}><thead><tr>{['Term','Select / Confirm URI','Match Type','Source','Custom Search','Status'].map((h) => <Box component="th" key={h} sx={{ textAlign: 'left', p: 1, bgcolor: '#f8fafc', fontSize: 12 }}>{h}</Box>)}</tr></thead><tbody>{rows.map((row) => { const options = row.options ?? []; const selectedOption = options.find((option) => option.display === row.selected_display) ?? options[0]; const rowKey = String(row.row_index); const customSummary = row.custom_search_summary; return <tr key={rowKey}><Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0', maxWidth: 220 }}><Typography variant="body2" sx={{ fontWeight: 800 }}>{row.term}</Typography><Typography variant="caption" color="text.secondary">Row {row.row_index} • {row.subject_label}</Typography></Box><Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0', minWidth: 320 }}><FormControl fullWidth size="small"><InputLabel>URI</InputLabel><Select label="URI" value={selectedOption?.display ?? ''} onChange={(e) => { const opt = options.find((o) => o.display === String(e.target.value)); if (opt) emit({ type: 'update_mapping', row_index: row.row_index, selected_option: opt, match_type: row.match_type }); }}>{options.map((option) => <MenuItem key={option.display} value={option.display}>{option.display}</MenuItem>)}</Select></FormControl><Typography variant="caption" color="text.secondary">{row.current_uri || 'No URI selected'}</Typography></Box><Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0', minWidth: 190 }}><FormControl fullWidth size="small"><InputLabel>SKOS</InputLabel><Select disabled={!cfg.skos_matching_enabled} label="SKOS" value={row.match_type ?? ''} onChange={(e) => emit({ type: 'update_mapping', row_index: row.row_index, selected_option: selectedOption ?? {}, match_type: String(e.target.value) })}>{skosTypes.map((type) => <MenuItem key={type || 'empty'} value={type}>{type || '—'}</MenuItem>)}</Select></FormControl></Box><Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}>{row.source_provider || '—'}</Box><Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0', minWidth: 260 }}><Stack spacing={.8}><TextField size="small" label="Search term" value={customSearch[rowKey] ?? ''} onChange={(e) => setCustomSearch({ ...customSearch, [rowKey]: e.target.value })} /><Button size="small" variant="outlined" onClick={() => emit({ type: 'custom_search', row_index: row.row_index, search_term: customSearch[rowKey] ?? '', display_mode: rec.display_mode })}>New Search…</Button>{customSummary && <Alert severity={customSummary.results_count ? 'success' : 'info'} variant="outlined" sx={{ py: 0 }}><Typography variant="caption">Found {customSummary.results_count ?? 0} result(s) for “{customSummary.search_term}”.</Typography></Alert>}</Stack></Box><Box component="td" sx={{ p: 1, borderTop: '1px solid #e2e8f0' }}><Chip size="small" label={row.has_suggestions ? 'suggestions' : 'no suggestions'} color={row.has_suggestions ? 'success' : 'default'} /><Typography variant="caption" color="text.secondary" display="block">{row.mapping_justification || '—'}</Typography></Box></tr>; })}</tbody></Box></Box>{!rows.length && rec.display_mode && <Alert severity="info">No terms match the current reconciliation filters.</Alert>}<Stack direction="row" spacing={1} justifyContent="space-between"><Button variant="outlined" disabled={(rec.page ?? 1) <= 1} onClick={() => updateSettings({ current_page: (rec.page ?? 1) - 1 })}>Previous</Button><Typography variant="body2" color="text.secondary">Page {rec.page ?? 1} / {rec.total_pages ?? 1} • {rec.total_rows ?? 0} rows</Typography><Button variant="outlined" disabled={(rec.page ?? 1) >= (rec.total_pages ?? 1)} onClick={() => updateSettings({ current_page: (rec.page ?? 1) + 1 })}>Next</Button></Stack><Button variant="contained" onClick={() => emit({ type: 'navigate', stage: 'export' })}>Continue to Export</Button></Stack></CardContent></Card></Stack>;
}

function ExportPage({ snapshot }: { snapshot: Snapshot }) {
  const downloads = snapshot.downloads ?? {};
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: '1fr 1fr' }, gap: 2 }}><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">SSSOM Download</Typography><Typography variant="body2" color="text.secondary">Final curated mapping table with SSSOM core + optional SSSOM fields only.</Typography><SummaryRow label="Rows" value={snapshot.data?.rows ?? 0} /><Button variant="contained" disabled={!downloads.sssom_csv} onClick={() => { triggerDownload(downloads.sssom_csv ?? '', downloads.sssom_filename ?? 'sssom_curated.csv'); emit({ type: 'prepare_downloads' }); }}>Download SSSOM CSV</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Candidate / Review Download</Typography><Typography variant="body2" color="text.secondary">Extended review/context export including provider metadata and candidate context.</Typography><Button variant="contained" disabled={!downloads.candidate_review_csv} onClick={() => triggerDownload(downloads.candidate_review_csv ?? '', downloads.candidate_review_filename ?? 'candidate_review.csv')}>Download Candidate Review CSV</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">RDF Generator Handoff</Typography><Typography variant="body2" color="text.secondary">Publish finalized accepted results to <code>shared_reconciled_matching_table</code>.</Typography><Button variant="contained" color="success" disabled={!snapshot.data?.has_table} onClick={() => emit({ type: 'publish_rdf_handoff' })}>Publish to RDF Generator</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Reset workflow</Typography><Typography variant="body2" color="text.secondary">Clear loaded data, provider queues, suggestions, and curation state.</Typography><Button variant="outlined" color="warning" onClick={() => emit({ type: 'reset_workflow' })}>Reset Semi-Automatic Reconciliation</Button></Stack></CardContent></Card></Box>;
}

export function SemiAutomaticReconciliationApp({ args }: StreamlitProps) {
  const snapshot = args?.snapshot ?? {};
  const activeStage = (snapshot.active_stage && stages.some((s) => s.id === snapshot.active_stage) ? snapshot.active_stage : 'load') as Stage;
  useEffect(() => { window.scrollTo(0, 0); try { const mainContent = window.parent.document.querySelector('section.main'); if (mainContent) mainContent.scrollTo({ top: 0, behavior: 'smooth' }); else window.parent.scrollTo({ top: 0, behavior: 'smooth' }); } catch { /* ignore cross-frame scroll issues */ } }, [activeStage]);
  useEffect(() => { Streamlit.setFrameHeight(); }, [snapshot, activeStage]);
  const page = useMemo(() => {
    if (activeStage === 'load') return <LoadPage snapshot={snapshot} />;
    if (activeStage === 'configure') return <ConfigurePage snapshot={snapshot} />;
    if (activeStage === 'run') return <RunPage snapshot={snapshot} />;
    if (activeStage === 'reconcile') return <ReconcilePage snapshot={snapshot} />;
    return <ExportPage snapshot={snapshot} />;
  }, [activeStage, snapshot]);
  return <AppShell snapshot={snapshot} activeStage={activeStage}>{page}</AppShell>;
}
