import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Alert, Box, Button, Checkbox, Chip, type ChipProps, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, FormControl, FormControlLabel, FormHelperText, IconButton, InputLabel, LinearProgress, MenuItem, Pagination, Radio, RadioGroup, Select, Slider, Stack, Switch, Table, TableBody, TableCell, TableContainer, TableHead, TableRow as MuiTableRow, TextField, Tooltip, Typography } from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined';
import StopIcon from '@mui/icons-material/Stop';
import { emitAppEvent, type AppEvent } from '../../shared/appBridge';
import { DownloadButton, GenerateButton, PrepareButton, SearchActionButton, UploadButton, UseResultButton } from '../../components/actions/WorkflowActionButtons';
import { ToolMenu } from '../../components/actions/ToolMenu';
import { InfoButton, type InfoContent } from '../../components/help/InfoButton';
import { Disclosure } from '../../components/layout/Disclosure';
import { KpiStrip, type Kpi } from '../../components/layout/KpiStrip';
import { MetaList } from '../../components/layout/MetaList';
import { Section } from '../../components/layout/Section';
import { WizardPage, type WizardStep } from '../../components/layout/WizardPage';
import { EmptyState } from '../../components/states/EmptyState';
import { fonts } from '../../theme/tokens';

type Severity = 'success' | 'info' | 'warning' | 'error';
type TableRow = Record<string, unknown>;
type Stage = 'load' | 'configure' | 'reconcile' | 'export';

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
  active_stage?: Stage | 'run';
  review_revision?: number;
  review_summary?: { total?: number; matched?: number; unmatched?: number; missing_relation?: number };
  rdf_handoff?: { status?: string; confirmation_token?: string; message?: string } | null;
  statusMessage?: { severity?: Severity; text?: string } | null;
  data?: {
    table_revision?: number;
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
    search_performed?: boolean;
    status?: 'idle' | 'running' | 'completed' | 'stopped' | 'failed';
    results_ready?: boolean;
    has_run?: boolean;
    processing_active?: boolean;
    processed_terms?: number;
    total_terms?: number;
    progress?: number;
    current_term_index?: number;
    can_start?: boolean;
    missing_start_configs?: string[];
  };
  reconciliation?: { display_mode?: string | null; rows?: ReconciliationRow[]; total_rows?: number; page?: number; total_pages?: number };
  downloads?: { sssom_preview?: TableRow[]; candidate_review_preview?: TableRow[]; sssom_csv?: string; sssom_filename?: string; candidate_review_csv?: string; candidate_review_filename?: string };
};


type Emit = (event: AppEvent) => Promise<boolean>;
function completedReconciliationRun(snapshot: Snapshot): boolean {
  const run = snapshot.run ?? {};
  if (run.processing_active) return false;
  if (typeof run.results_ready === 'boolean') return run.results_ready;
  if (run.status) return run.status === 'completed';
  // Compatibility with a still-running older backend: it has no lifecycle fields.
  // Require every selected provider to have finished and the full batch processed.
  const queue = snapshot.config?.provider_queue ?? [];
  return queue.length > 0 && (run.total_terms ?? 0) > 0 &&
    (run.processed_terms ?? 0) >= (run.total_terms ?? 0) &&
    queue.every(name => snapshot.config?.provider_status?.some(provider => provider.name === name && provider.status === 'completed'));
}
type AppProps = { args?: { app?: string; snapshot?: Snapshot }; onEvent?: (event: AppEvent) => unknown | Promise<unknown>; busy?: boolean; onDraftChange?: (dirty: boolean) => void };
const stages: { id: Stage; label: string; title: string; lead: string; info: InfoContent }[] = [
  { id: 'load', label: 'Load table', title: 'Load the terms you want to reconcile', lead: 'Use your prepared matching table or upload an existing SSSOM file.', info: { title: 'Loading a table', content: <><p>Prepared terms come straight from the Matching table tool, or from a CSV / Excel file in SSSOM format.</p><p>Replacing a loaded table clears its candidate results and your mappings.</p></> } },
  { id: 'configure', label: 'Choose sources', title: 'Choose where to look for matches', lead: 'Select at least one source to search for candidates.', info: { title: 'Choosing sources', content: <><p>Every selected source is searched for candidate concepts for each term. Pick the sources that cover your subject.</p><p>Search settings and filters are optional. If your table already contains curated mappings, you can continue without a source. Nothing is mapped automatically.</p></> } },
  { id: 'reconcile', label: 'Review matches', title: 'Find candidates and review your matches', lead: 'Start reconciliation, then compare candidates and choose a mapping for each term.', info: { title: 'Reviewing matches', content: <ul><li>Reconciliation searches every selected source for candidates.</li><li>Select a term, compare its candidates and choose the one that fits, or leave it unmapped.</li><li>Changes save automatically. Nothing is mapped automatically, and running again keeps your saved mappings.</li></ul> } },
  { id: 'export', label: 'Export', title: 'Check your result and export', lead: 'Download your curated mappings or send them to the RDF generator.', info: { title: 'Exporting mappings', content: <><p>Downloads include every term; unmapped terms keep empty mapping fields.</p><p>Sending the mappings to the RDF generator does not generate RDF yet. If source data is missing there, you will be asked to load it.</p></> } },
];
const skosTypes = ['', 'skos:exactMatch', 'skos:closeMatch', 'skos:broadMatch', 'skos:narrowMatch', 'skos:relatedMatch'];
const ontologyProviderOrder = ['BioPortal', 'OLS (EBI)', 'SemLookP', 'AgroPortal', 'EarthPortal'];
const sssomColumns = ['subject_id', 'subject_label', 'predicate_id', 'object_id', 'object_label', 'mapping_justification'];
// Backend strategy values double as protocol strings; only the visible labels are mapped.
const strategyLabels: Record<string, string> = { 'API Ranking': 'Source ranking', 'Levenshtein Similarity': 'Spelling similarity' };
const sparqlVariableLabels = { uri: 'URI variable', label: 'Label variable', description: 'Description variable' } as const;
const TOUCH_TARGET = 44;
const monoSx = { fontFamily: fonts.mono, fontSize: '0.8125rem', overflowWrap: 'anywhere' } as const;
const numberFormat = new Intl.NumberFormat();
const fmt = (value?: number | null) => numberFormat.format(value ?? 0);
// For labelled counts: a missing value stays empty (rendered as "—"), a real zero prints as 0.
const fmtCount = (value?: number | null) => value === undefined || value === null ? undefined : numberFormat.format(value);
const sentenceCase = (value: string) => value.charAt(0).toUpperCase() + value.slice(1);
const candidateCount = (entry: ReconciliationRow) => (entry.options ?? []).filter(option => option.uri && option.uri !== 'No Match').length;
// One status vocabulary for the term list and the comparison pane.
function entryStatus(entry: ReconciliationRow, skos: boolean): { label: string; color: ChipProps['color'] } {
  if (entry.current_uri) return skos && !entry.match_type ? { label: 'Needs relation', color: 'info' } : { label: 'Mapped', color: 'success' };
  return candidateCount(entry) ? { label: 'Needs review', color: 'warning' } : { label: 'No candidates', color: 'default' };
}
const providerTone = (status?: string): ChipProps['color'] => status === 'completed' ? 'success' : status === 'error' ? 'error' : status === 'stopped' ? 'warning' : status === 'running' ? 'info' : 'default';
const formatInfo: InfoContent = { title: 'File format', content: <><p>CSV or Excel with these SSSOM columns:</p><Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, my: 1 }}>{sssomColumns.map(column => <Chip key={column} size="small" variant="outlined" label={column} sx={{ fontFamily: fonts.mono }} />)}</Box><p>Existing mappings are kept when a valid file is loaded.</p></> };
function triggerDownload(content: string, filename: string) {
  const url = URL.createObjectURL(new Blob([content], { type: 'text/csv;charset=utf-8' }));
  const link = document.createElement('a'); link.href = url; link.download = filename; link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function MessageList({ items }: { items: string[] }) {
  return items.length === 1 ? <>{items[0]}</> : <Box component="ul" sx={{ m: 0, pl: 2.5 }}>{items.map(item => <li key={item}>{item}</li>)}</Box>;
}
const monoColumn = (column: string) => /(^|_)(id|uri|iri|curie|justification)$/i.test(column);
const isMissing = (value: unknown) => value === null || value === undefined || value === '';
function PreviewTable({ rows = [], label }: { rows?: TableRow[]; label: string }) {
  const columns = Object.keys(rows[0] ?? {});
  if (!rows.length) return <Typography variant="body2" color="text.secondary">No rows available yet.</Typography>;
  return <TableContainer tabIndex={0} role="region" aria-label={label} sx={{ maxHeight: 320, maxWidth: '100%' }}>
    <Table size="small" stickyHeader aria-label={label}><TableHead><MuiTableRow>{columns.map(column => <TableCell key={column} scope="col" sx={{ minWidth: 140 }}>{column}</TableCell>)}</MuiTableRow></TableHead><TableBody>{rows.map((row, index) => <MuiTableRow key={index}>{columns.map(column => <TableCell key={column} sx={{ maxWidth: 300, overflowWrap: 'anywhere', verticalAlign: 'top', ...(monoColumn(column) ? monoSx : {}) }}>{isMissing(row[column]) ? <Box component="span" sx={{ color: 'text.secondary' }}>—</Box> : String(row[column])}</TableCell>)}</MuiTableRow>)}</TableBody></Table>
  </TableContainer>;
}
function LoadPage({ snapshot, emit, onReading, busy }: { snapshot: Snapshot; emit: Emit; onReading: (value: boolean) => void; busy: boolean }) {
  const data = snapshot.data ?? {};
  const input = useRef<HTMLInputElement>(null);
  const [replacement, setReplacement] = useState<AppEvent | null>(null);
  const [reading, setReading] = useState(false);
  const [readError, setReadError] = useState('');
  const preview = data.curated_preview ?? [];
  const shared = Boolean(data.shared_table_available);
  async function load(event: AppEvent) { if (data.has_table) setReplacement(event); else await emit(event); }
  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]; event.target.value = ''; if (!file) return;
    setReading(true); onReading(true); setReadError('');
    try { const bytes = new Uint8Array(await file.arrayBuffer()); let binary = ''; bytes.forEach(byte => { binary += String.fromCharCode(byte); }); await load({ type: 'upload_table', filename: file.name, content_base64: window.btoa(binary) }); }
    catch { setReadError('The file could not be read. Choose it again or use another file.'); }
    finally { setReading(false); onReading(false); }
  }
  // Contained only while it is the stage's main action (nothing loaded yet, DESIGN.md §7).
  const useResult = <UseResultButton variant={shared && !data.has_table ? 'contained' : 'outlined'} disabled={!shared || reading} onClick={() => load({ type: 'load_shared_table' })}>Use prepared table</UseResultButton>;
  const facts = [
    ...(data.has_table ? [{ label: 'File', value: data.filename, mono: true }, { label: 'Rows', value: fmtCount(data.rows), numeric: true }, { label: 'Columns', value: fmtCount(data.columns), numeric: true }] : []),
    ...(shared ? [{ label: 'Prepared table', value: data.shared_rows === undefined ? undefined : `${fmt(data.shared_rows)} terms`, numeric: true }] : []),
  ];
  return <Stack spacing={3}>
    <Section title={data.has_table ? 'Loaded table' : 'Choose a matching table'} info={formatInfo} description="CSV or Excel (.csv, .xlsx, .xls)."><Stack spacing={2}>
      {facts.length > 0 && <MetaList items={facts} />}
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ sm: 'center' }}>
        {shared ? useResult : <Tooltip title="Prepare a table in the Matching table tool first."><span>{useResult}</span></Tooltip>}
        <UploadButton variant={!data.has_table && !shared ? 'contained' : 'outlined'} disabled={reading} onClick={() => input.current?.click()}>{reading ? 'Reading file…' : data.has_table ? 'Replace file' : 'Upload file'}</UploadButton>
        <input ref={input} hidden type="file" accept=".csv,.xlsx,.xls" onChange={handleUpload} />
      </Stack>
      {readError && <Alert severity="error">{readError}</Alert>}
    </Stack></Section>
    {data.has_table && <Section title="Preview" description={`Showing ${fmt(preview.length)} of ${fmt(data.rows)} rows`}><PreviewTable rows={preview} label="Loaded matching table preview" /></Section>}
    <Dialog open={Boolean(replacement)} onClose={busy ? undefined : () => setReplacement(null)} aria-labelledby="replace-table-title" aria-describedby="replace-table-message"><DialogTitle id="replace-table-title">Replace this matching table?</DialogTitle><DialogContent><DialogContentText id="replace-table-message">This clears the current candidates and your mappings. You can undo this from History.</DialogContentText></DialogContent><DialogActions><Button autoFocus disabled={busy} onClick={() => setReplacement(null)}>Cancel</Button><Button variant="contained" color="warning" disabled={busy} onClick={async () => { if (replacement && await emit(replacement)) setReplacement(null); }}>Replace matching table</Button></DialogActions></Dialog>
  </Stack>;
}

type SettingsDraft = { custom_sparql_endpoint: string; custom_sparql_query_template: string; custom_sparql_var_uri: string; custom_sparql_var_label: string; custom_sparql_var_description: string; ncbi_selected_databases: string[]; local_backend: string; suggestion_slider: number; matching_strategy_radio: string; levenshtein_threshold_slider: number };
function settingsFrom(cfg: NonNullable<Snapshot['config']>): SettingsDraft {
  return { custom_sparql_endpoint: cfg.custom_sparql_endpoint ?? '', custom_sparql_query_template: cfg.custom_sparql_query_template ?? '', custom_sparql_var_uri: cfg.custom_sparql_var_uri ?? 'uri', custom_sparql_var_label: cfg.custom_sparql_var_label ?? 'label', custom_sparql_var_description: cfg.custom_sparql_var_description ?? 'description', ncbi_selected_databases: cfg.ncbi_selected_databases ?? [], local_backend: cfg.local_backend ?? 'auto', suggestion_slider: cfg.suggestion_slider ?? 10, matching_strategy_radio: cfg.matching_strategy ?? 'API Ranking', levenshtein_threshold_slider: cfg.levenshtein_threshold ?? .7 };
}
const LOCAL_FILES = 'Local ontology files';
// The Disclosure each draft field lives in, so the unapplied-edits notice can name it. Typed as a
// full record so a new draft field cannot be left out of the dirty check.
const draftSectionOf: Record<keyof SettingsDraft, string> = {
  local_backend: LOCAL_FILES,
  ncbi_selected_databases: 'NCBI databases',
  suggestion_slider: 'Search settings', matching_strategy_radio: 'Search settings', levenshtein_threshold_slider: 'Search settings',
  custom_sparql_endpoint: 'Custom SPARQL source', custom_sparql_query_template: 'Custom SPARQL source', custom_sparql_var_uri: 'Custom SPARQL source', custom_sparql_var_label: 'Custom SPARQL source', custom_sparql_var_description: 'Custom SPARQL source',
};
// Unapplied work on the Choose sources stage: setting edits per section, and selected local files.
type ConfigDraft = { sections: string[]; files: 'none' | 'reading' | 'unindexed' };
const noConfigDraft: ConfigDraft = { sections: [], files: 'none' };
const sameConfigDraft = (a: ConfigDraft, b: ConfigDraft) => a.files === b.files && a.sections.join('\n') === b.sections.join('\n');
const joinNames = (names: string[]) => names.length > 1 ? `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}` : names[0] ?? '';
function ConfigurePage({ snapshot, emit, onDraft }: { snapshot: Snapshot; emit: Emit; onDraft: (draft: ConfigDraft) => void }) {
  const cfg = snapshot.config ?? {};
  const queue = cfg.provider_queue ?? [];
  const savedDraft = settingsFrom(cfg);
  const saved = JSON.stringify(savedDraft);
  const [draft, setDraft] = useState<SettingsDraft>(settingsFrom(cfg));
  const [files, setFiles] = useState<{ filename: string; content_base64: string }[]>([]);
  const [reading, setReading] = useState(false);
  const [fileError, setFileError] = useState('');
  const input = useRef<HTMLInputElement>(null);
  const dirtySections = Array.from(new Set((Object.keys(draftSectionOf) as (keyof SettingsDraft)[]).filter(key => JSON.stringify(draft[key]) !== JSON.stringify(savedDraft[key])).map(key => draftSectionOf[key])));
  // The parser is sent with the files (and saved by indexing), so a changed parser alone never blocks adding files.
  const dirtyForIndex = dirtySections.some(section => section !== LOCAL_FILES);
  const sectionsKey = dirtySections.join('\n');
  const fileState: ConfigDraft['files'] = reading ? 'reading' : files.length > 0 ? 'unindexed' : 'none';
  useEffect(() => setDraft(JSON.parse(saved)), [saved]);
  useEffect(() => onDraft({ sections: sectionsKey ? sectionsKey.split('\n') : [], files: fileState }), [sectionsKey, fileState, onDraft]);
  function update(patch: Partial<SettingsDraft>) { setDraft(previous => ({ ...previous, ...patch })); }
  // Applies every pending setting edit at once: a partial save would reset the other drafts.
  const applyRow = (section: string) => dirtySections.includes(section) ? <Box><PrepareButton onClick={() => emit({ type: 'update_settings', settings: draft })}>Apply settings</PrepareButton></Box> : null;
  async function indexFiles(encoded = files) { if (await emit({ type: 'index_local_resources', backend: draft.local_backend, files: encoded })) setFiles([]); }
  async function handleLocalFiles(event: React.ChangeEvent<HTMLInputElement>) {
    const selected = Array.from(event.target.files ?? []); event.target.value = ''; if (!selected.length) return;
    setReading(true); setFileError('');
    try { const encoded = await Promise.all(selected.map(async file => { const bytes = new Uint8Array(await file.arrayBuffer()); let binary = ''; bytes.forEach(byte => { binary += String.fromCharCode(byte); }); return { filename: file.name, content_base64: window.btoa(binary) }; })); setFiles(encoded); await indexFiles(encoded); }
    catch { setFiles([]); setFileError('The selected files could not be read. Choose them again.'); }
    finally { setReading(false); }
  }
  const lookup = ontologyProviderOrder.filter(provider => queue.includes(provider));
  const filtered = lookup.filter(provider => (cfg.selected_ontologies_by_provider?.[provider] ?? []).length > 0).length;
  const localResources = cfg.local_resources ?? [];
  const missing = snapshot.run?.missing_start_configs ?? [];
  return <Stack spacing={3}>
    <Section title="Sources" description="Changes save automatically."><Stack spacing={1.5}>
      {/* Fills, not outlined tiles: no boxes inside the Section (DESIGN.md §7). */}
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1 }}>{(cfg.available_providers ?? []).map(provider => { const checked = queue.includes(provider); return <Box key={provider} sx={{ pl: 1.5, pr: 1, py: 0.5, borderRadius: 2, bgcolor: checked ? 'action.selected' : 'rdf.subtle' }}><Stack direction="row" justifyContent="space-between" alignItems="center" spacing={1}><FormControlLabel sx={{ minWidth: 0 }} control={<Checkbox checked={checked} onChange={event => emit({ type: 'confirm_queue', provider_queue: event.target.checked ? [...queue, provider] : queue.filter(value => value !== provider) })} />} label={provider} /><InfoButton title={provider}>{cfg.provider_tooltips?.[provider] || 'Select this source to include it in the candidate search.'}</InfoButton></Stack></Box>; })}</Box>
      {queue.length > 0 && <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap spacing={0.5}><Typography variant="body2" color="text.secondary" sx={{ mr: 0.5 }}>Selected</Typography>{queue.map(provider => <Chip key={provider} size="small" label={provider} />)}</Stack>}
      {missing.length > 0 && <Alert severity="warning"><MessageList items={missing} /></Alert>}
    </Stack></Section>
    <Stack spacing={1.5}>
      {queue.includes('Local Ontology') && <Disclosure key="local-resources" defaultExpanded={!localResources.length} title={LOCAL_FILES} summary={`${fmt(localResources.length)} indexed`}><Stack spacing={1.5}>
        <FormControl fullWidth size="small"><InputLabel id="local-parser">Local file parser</InputLabel><Select labelId="local-parser" label="Local file parser" value={draft.local_backend} onChange={event => update({ local_backend: String(event.target.value) })}>{['auto', 'oak', 'rdflib', 'tabular'].map(value => <MenuItem key={value} value={value}>{value}</MenuItem>)}</Select></FormControl>
        <Stack direction="row" alignItems="center" spacing={0.5}><UploadButton disabled={dirtyForIndex || reading} onClick={() => input.current?.click()}>{reading ? 'Reading files…' : localResources.length ? 'Replace ontology files' : 'Upload ontology files'}</UploadButton><InfoButton title="Local ontology files"><p>Add an ontology (OWL, OBO, RDF, Turtle, JSON-LD) or a table with URI and label columns (CSV, TSV, Excel).</p><p>Files are indexed with the parser above when you select them; searches use this local index.</p></InfoButton><input ref={input} hidden multiple type="file" accept=".owl,.obo,.rdf,.ttl,.json,.jsonld,.csv,.tsv,.xlsx" disabled={dirtyForIndex || reading} onChange={handleLocalFiles} /></Stack>
        {dirtyForIndex && <Typography variant="caption" color="text.secondary">Apply or discard your other setting edits before adding files.</Typography>}
        {applyRow(LOCAL_FILES)}
        {fileError && <Alert severity="error">{fileError}</Alert>}
        {files.length > 0 && !reading && <Alert severity="error" action={<Button color="inherit" size="small" onClick={() => indexFiles()}>Retry indexing</Button>}>Selected files were not indexed successfully.</Alert>}
        {localResources.length > 0 && <Box component="ul" aria-label="Indexed local files" sx={{ listStyle: 'none', p: 0, m: 0 }}>{localResources.map(resource => <Stack component="li" key={resource.name} direction="row" justifyContent="space-between" alignItems="baseline" spacing={2} sx={{ py: 0.5, borderBottom: 1, borderColor: 'divider', '&:last-of-type': { borderBottom: 0 } }}><Box component="code" sx={{ ...monoSx, minWidth: 0 }}>{resource.name}</Box><Typography variant="body2" color="text.secondary" sx={{ flexShrink: 0, fontVariantNumeric: 'tabular-nums' }}>{fmt(resource.entities)} concepts</Typography></Stack>)}</Box>}
      </Stack></Disclosure>}
      {lookup.length > 0 && <Disclosure title="Ontology filters" optional summary={filtered ? `${fmt(filtered)} of ${fmt(lookup.length)} sources filtered` : 'Off'}><Stack spacing={2}>{lookup.map(provider => { const available = cfg.available_ontologies_by_provider?.[provider] ?? []; const selected = cfg.selected_ontologies_by_provider?.[provider] ?? []; const status = cfg.ontology_loading_status?.[provider]; return <Box key={provider}><Typography variant="subtitle2" component="h3">{provider}</Typography>{available.length > 0 ? <FormControl fullWidth size="small" sx={{ mt: 1 }}><InputLabel id={`ontology-${provider}`}>Ontologies for {provider}</InputLabel><Select multiple labelId={`ontology-${provider}`} label={`Ontologies for ${provider}`} value={selected.filter(value => available.includes(value))} onChange={event => emit({ type: 'update_settings', settings: { selected_ontologies_by_provider: { ...cfg.selected_ontologies_by_provider, [provider]: typeof event.target.value === 'string' ? event.target.value.split(',') : event.target.value } } })}>{available.map(value => <MenuItem key={value} value={value}><Checkbox checked={selected.includes(value)} />{value}</MenuItem>)}</Select><FormHelperText>Leave empty to search all ontologies.</FormHelperText></FormControl> : status === 'error' ? <Alert severity="warning" sx={{ mt: 1 }}>The ontology list could not be loaded. Check the source connection; unfiltered search remains available if its prerequisites are met.</Alert> : <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>{status === 'loading' ? 'Loading ontology list…' : 'No ontology list available.'}</Typography>}</Box>; })}</Stack></Disclosure>}
      {queue.includes('NCBI') && <Disclosure title="NCBI databases" summary={`${fmt(draft.ncbi_selected_databases.length)} selected`}><Stack spacing={1.5}><FormControl fullWidth size="small"><InputLabel id="ncbi-databases">NCBI databases</InputLabel><Select multiple labelId="ncbi-databases" label="NCBI databases" value={draft.ncbi_selected_databases} onChange={event => update({ ncbi_selected_databases: typeof event.target.value === 'string' ? event.target.value.split(',') : event.target.value })}>{(cfg.ncbi_all_databases ?? []).map(value => <MenuItem key={value} value={value}><Checkbox checked={draft.ncbi_selected_databases.includes(value)} />{value}</MenuItem>)}</Select></FormControl>{applyRow('NCBI databases')}</Stack></Disclosure>}
      <Disclosure title="Search settings" optional summary={`${fmt(cfg.suggestion_slider ?? 10)} candidates per source, ${(strategyLabels[cfg.matching_strategy ?? 'API Ranking'] ?? cfg.matching_strategy ?? '').toLocaleLowerCase()}`}><Stack spacing={2}>
        <Box><Typography id="candidate-limit-label" variant="body2">Maximum candidates per source and term: <Box component="span" sx={{ fontVariantNumeric: 'tabular-nums' }}>{draft.suggestion_slider}</Box></Typography><Slider aria-labelledby="candidate-limit-label" min={1} max={30} step={1} valueLabelDisplay="auto" value={draft.suggestion_slider} onChange={(_, value) => update({ suggestion_slider: value as number })} /></Box>
        <FormControl fullWidth size="small"><InputLabel id="ranking-strategy">Candidate order</InputLabel><Select labelId="ranking-strategy" label="Candidate order" value={draft.matching_strategy_radio} onChange={event => update({ matching_strategy_radio: String(event.target.value) })}>{Object.entries(strategyLabels).map(([value, label]) => <MenuItem key={value} value={value}>{label}</MenuItem>)}</Select></FormControl>
        <Box><Stack direction="row" alignItems="center" spacing={0.5}><Typography id="similarity-label" variant="body2">Spelling similarity threshold: <Box component="span" sx={{ fontVariantNumeric: 'tabular-nums' }}>{draft.levenshtein_threshold_slider.toFixed(2)}</Box></Typography><InfoButton title="Spelling similarity threshold"><p>Spelling similarity compares a term with a candidate label, from 0 (unrelated) to 1 (identical spelling).</p><p>Prefill top candidates only selects a term’s top candidate when its spelling similarity is at or above this threshold; other terms stay unmapped for review. Set it to 0 to prefill every top candidate.</p></InfoButton></Stack><Slider aria-labelledby="similarity-label" min={0} max={1} step={.01} value={draft.levenshtein_threshold_slider} onChange={(_, value) => update({ levenshtein_threshold_slider: value as number })} /></Box>
        {applyRow('Search settings')}
      </Stack></Disclosure>
      <Disclosure title="Custom SPARQL source" optional summary={!cfg.custom_sparql_enabled ? 'Off' : queue.includes('Custom SPARQL') ? 'On, selected as a source' : 'On, not selected as a source'}><Stack spacing={1.5}><FormControlLabel control={<Switch checked={Boolean(cfg.custom_sparql_enabled)} onChange={event => emit({ type: 'update_settings', settings: { custom_sparql_enabled: event.target.checked, provider_queue: event.target.checked ? queue : queue.filter(provider => provider !== 'Custom SPARQL') } })} />} label="Enable custom SPARQL source" /><Typography variant="body2" color="text.secondary">Configure the endpoint, apply its settings, then select Custom SPARQL under Sources.</Typography>{cfg.custom_sparql_enabled && <><TextField label="SPARQL endpoint URL" value={draft.custom_sparql_endpoint} onChange={event => update({ custom_sparql_endpoint: event.target.value })} /><TextField label="SPARQL query template" multiline minRows={4} value={draft.custom_sparql_query_template} onChange={event => update({ custom_sparql_query_template: event.target.value })} inputProps={{ style: { fontFamily: fonts.mono } }} /><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>{(['uri', 'label', 'description'] as const).map(name => <TextField key={name} label={sparqlVariableLabels[name]} value={draft[`custom_sparql_var_${name}`]} onChange={event => update({ [`custom_sparql_var_${name}`]: event.target.value })} />)}</Stack></>}{applyRow('Custom SPARQL source')}</Stack></Disclosure>
    </Stack>
  </Stack>;
}

function ReviewPage({ snapshot, emit, searching, drafts, setDrafts }: { snapshot: Snapshot; emit: Emit; searching: boolean; drafts: Record<string, string>; setDrafts: React.Dispatch<React.SetStateAction<Record<string, string>>> }) {
  const cfg = snapshot.config ?? {}; const run = snapshot.run ?? {}; const rec = snapshot.reconciliation ?? {};
  const ready = completedReconciliationRun(snapshot);
  const hasRun = Boolean(run.has_run || run.search_performed || ready || ['stopped', 'failed', 'completed'].includes(run.status ?? ''));
  const [active, setActive] = useState('');
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [prefill, setPrefill] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [termFilter, setTermFilter] = useState('');
  const [detailsOpen, setDetailsOpen] = useState(false);
  const detailsRef = useRef<HTMLDivElement>(null);
  const rows = (rec.rows ?? []).filter(row => String(row.term ?? '').toLocaleLowerCase().includes(termFilter.toLocaleLowerCase()));
  const row = rows.find(item => String(item.row_index) === active) ?? rows[0];
  const key = String(row?.row_index ?? '');
  const options = row?.options ?? [];
  const selected = options.find(option => option.display === row?.selected_display) ?? options[0];
  const position = rows.findIndex(item => item === row);
  const issues = (cfg.provider_status ?? []).filter(provider => provider.status === 'error');
  const summary = snapshot.review_summary ?? {};
  const total = summary.total ?? 0;
  const savedCount = summary.matched ?? 0;
  const queue = cfg.provider_queue ?? [];
  const skos = Boolean(cfg.skos_matching_enabled);
  const page = rec.page ?? 1; const totalPages = rec.total_pages ?? 1;
  const choose = (option: SuggestionOption) => emit({ type: 'update_mapping', row_index: row?.row_index, selected_option: option, match_type: row?.match_type ?? '' });
  const goToPage = (next: number) => { setTermFilter(''); void emit({ type: 'update_settings', settings: { current_page: next } }); };
  // The Disclosure's summary is its toggle button; found through a wrapper so no id is duplicated.
  const showRunDetails = () => { setDetailsOpen(true); const header = detailsRef.current?.querySelector<HTMLElement>('[aria-expanded]'); header?.scrollIntoView({ block: 'center' }); header?.focus({ preventScroll: true }); };
  useEffect(() => { setSearchOpen(false); }, [key]);
  const runTitle = ready ? 'Run finished' : searching ? 'Run in progress' : run.status === 'stopped' ? 'Run stopped' : run.status === 'failed' ? 'Run failed' : 'Ready to run reconciliation';
  const runLine = ready ? undefined : searching ? 'Results appear when the run finishes.' : run.status === 'stopped' || run.status === 'failed' ? 'Run again to finish before reviewing.' : 'Search the selected sources for candidates for every term.';
  const runTone = ready ? issues.length ? 'warning' : 'default' : run.status === 'failed' ? 'error' : run.status === 'stopped' && !searching ? 'warning' : 'default';
  const runAction = searching
    ? <Button variant="outlined" color="warning" startIcon={<StopIcon />} onClick={() => emit({ type: 'stop_processing' })}>Stop run</Button>
    : <GenerateButton variant={ready ? 'outlined' : 'contained'} disabled={!run.can_start} onClick={() => emit({ type: 'start_processing' })}>{hasRun ? 'Rerun reconciliation' : 'Start reconciliation'}</GenerateButton>;
  const rowStatus = row ? entryStatus(row, skos) : undefined;
  return <Stack spacing={3}>
    <KpiStrip ariaLabel="Review progress" items={[{ label: 'Terms', value: fmtCount(summary.total) }, { label: 'Mapped', value: fmtCount(summary.matched) }, { label: 'Unmapped', value: fmtCount(summary.unmatched) }]} />
    <Section title={runTitle} description={runLine} tone={runTone} actions={runAction}><Stack spacing={2}>
      <MetaList items={[{ label: 'Sources', value: queue.length ? <Stack direction="row" flexWrap="wrap" useFlexGap spacing={0.5} component="span">{queue.map(provider => <Chip key={provider} size="small" label={provider} />)}</Stack> : undefined }]} />
      {searching && <Box><LinearProgress variant="determinate" value={run.progress ?? 0} aria-label="Candidate search progress" /><Typography role="status" variant="body2" sx={{ mt: 1, fontVariantNumeric: 'tabular-nums' }}>{fmt(run.processed_terms)} of {fmt(run.total_terms)} terms searched</Typography></Box>}
      {!ready && !searching && !!run.missing_start_configs?.length && savedCount < total && <Alert severity="warning"><MessageList items={run.missing_start_configs} /></Alert>}
      {ready && !!issues.length && <Alert severity="warning" action={<Button color="inherit" size="small" onClick={showRunDetails}>Show run details</Button>}>The run finished with errors from {issues.map(provider => provider.name).join(', ')}. Some candidates may be missing.</Alert>}
    </Stack></Section>
    {ready && <>
      <Section title="Terms" description="Changes save automatically." actions={<Button variant="outlined" startIcon={<SettingsOutlinedIcon />} onClick={() => setSettingsOpen(true)}>View options</Button>}><Stack spacing={2}>
        <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.5}>
          <TextField size="small" label="Find a term on this page" value={termFilter} onChange={event => setTermFilter(event.target.value)} sx={{ flex: 1 }} />
          <FormControl size="small" sx={{ minWidth: 180 }}><InputLabel id="candidate-source">Candidate source</InputLabel><Select labelId="candidate-source" label="Candidate source" value={rec.display_mode ?? 'Mixed Results'} onChange={event => emit({ type: 'select_display_provider', provider: event.target.value })}><MenuItem value="Mixed Results">All sources combined</MenuItem>{Array.from(new Set([...queue, ...(cfg.provider_has_results ?? []), ...(rec.display_mode && rec.display_mode !== 'Mixed Results' ? [rec.display_mode] : [])])).map(provider => <MenuItem key={provider} value={provider}>{provider}</MenuItem>)}</Select></FormControl>
        </Stack>
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'minmax(250px, 0.85fr) minmax(0, 1.65fr)' } }}>
          <Box sx={{ minWidth: 0, pb: { xs: 2, md: 0 }, pr: { md: 2.5 }, borderBottom: { xs: 1, md: 0 }, borderRight: { xs: 0, md: 1 }, borderColor: 'divider' }}>
            <TableContainer role="region" aria-label="Terms to review" tabIndex={0} sx={{ maxHeight: { xs: 240, md: 560 }, overflow: 'auto' }}><Table size="small" stickyHeader aria-label="Terms to review"><TableHead><MuiTableRow><TableCell>Term</TableCell><TableCell>Status</TableCell></MuiTableRow></TableHead><TableBody>
              {rows.map(entry => { const isActive = entry === row; const status = entryStatus(entry, skos); const count = candidateCount(entry); return <MuiTableRow key={String(entry.row_index)} selected={isActive}><TableCell component="th" scope="row" sx={{ p: 0.5, maxWidth: 210 }}><Button aria-label={`Review ${entry.term}`} aria-pressed={isActive} onClick={() => setActive(String(entry.row_index))} sx={{ justifyContent: 'flex-start', textAlign: 'left', width: '100%', whiteSpace: 'normal', overflowWrap: 'anywhere', fontWeight: isActive ? 600 : 500, minHeight: TOUCH_TARGET }}>{entry.term}</Button></TableCell><TableCell sx={{ minWidth: 100 }}><Chip size="small" variant="outlined" color={status.color} label={status.label} /><Typography variant="caption" color="text.secondary" component="p" sx={{ mt: 0.5, fontVariantNumeric: 'tabular-nums' }}>{fmt(count)} {count === 1 ? 'candidate' : 'candidates'}</Typography></TableCell></MuiTableRow>; })}
            </TableBody></Table>{!rows.length && <Typography sx={{ p: 2 }} variant="body2" color="text.secondary">No terms match the current filters.</Typography>}</TableContainer>
            {totalPages > 1 && <Box sx={{ display: 'flex', justifyContent: 'center', pt: 1 }}><Pagination size="small" siblingCount={0} count={totalPages} page={page} onChange={(_, next) => { if (next !== page) goToPage(next); }} aria-label="Term list pages" /></Box>}
          </Box>
          <Box component="section" aria-label="Candidate comparison" sx={{ minWidth: 0, pt: { xs: 2, md: 0 }, pl: { md: 2.5 } }}>
            {row && rowStatus ? <Stack spacing={2}>
              <Stack direction="row" justifyContent="space-between" alignItems="flex-start" spacing={1}><Box sx={{ minWidth: 0 }}><Typography variant="caption" color="text.secondary" component="p">Candidates for</Typography><Typography variant="subtitle1" component="h4" sx={{ overflowWrap: 'anywhere' }}>{row.term}</Typography></Box><Stack direction="row" alignItems="center" spacing={0.5} sx={{ flexShrink: 0 }}><Tooltip title="Previous term"><span><IconButton aria-label="Previous term" disabled={position <= 0} onClick={() => setActive(String(rows[position - 1].row_index))}><ChevronLeftIcon /></IconButton></span></Tooltip><Typography variant="caption" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums', whiteSpace: 'nowrap' }}>{fmt(position + 1)} of {fmt(rows.length)}</Typography><Tooltip title="Next term"><span><IconButton aria-label="Next term" disabled={position >= rows.length - 1} onClick={() => setActive(String(rows[position + 1].row_index))}><ChevronRightIcon /></IconButton></span></Tooltip></Stack></Stack>
              <Stack role="status" direction="row" spacing={1} alignItems="center" sx={{ minWidth: 0 }}><Chip size="small" variant="outlined" color={rowStatus.color} label={rowStatus.label} sx={{ flexShrink: 0 }} />{row.current_uri && <Typography variant="body2" sx={{ minWidth: 0, overflowWrap: 'anywhere' }}>Saved: {row.object_label || <Box component="code" sx={monoSx}>{row.current_uri}</Box>}</Typography>}</Stack>
              <FormControl component="fieldset"><RadioGroup aria-label={`Mapping for ${row.term}`} value={selected?.display ?? ''} onChange={(_, value) => { const option = options.find(item => item.display === value); if (option) void choose(option); }} sx={{ gap: 1 }}>
                {options.map(option => { const noMatch = !option.uri || option.uri === 'No Match'; const chosen = option.display === selected?.display; return <Box key={option.display} sx={{ borderRadius: 2, bgcolor: chosen ? 'action.selected' : 'rdf.subtle' }}><FormControlLabel value={option.display} control={<Radio />} sx={{ m: 0, p: 1, width: '100%', boxSizing: 'border-box', alignItems: 'flex-start', '& .MuiFormControlLabel-label': { minWidth: 0, flex: 1 }, '& .MuiRadio-root': { pt: 0.5 } }} label={<Stack component="span" spacing={0.5} sx={{ pt: 0.75, minWidth: 0 }}><Typography variant="subtitle2" component="span" sx={{ overflowWrap: 'anywhere' }}>{noMatch ? 'Leave unmapped' : option.label || option.display}</Typography>{noMatch ? <Typography variant="caption" component="span" color="text.secondary">None of these concepts fits.</Typography> : <><Box component="span"><Chip size="small" variant="outlined" label={option.source || 'Source not provided'} /></Box><Box component="code" sx={{ ...monoSx, fontSize: '0.75rem' }}>{option.uri}</Box>{option.description && <Typography variant="body2" component="span" color="text.secondary" sx={{ overflowWrap: 'anywhere' }}>{option.description}</Typography>}</>}</Stack>} /></Box>; })}
              </RadioGroup></FormControl>
              {skos && <FormControl fullWidth size="small"><InputLabel id={`relation-${key}`}>Relation for {row.term}</InputLabel><Select labelId={`relation-${key}`} label={`Relation for ${row.term}`} value={row.match_type ?? ''} disabled={!row.current_uri} onChange={event => emit({ type: 'update_mapping', row_index: row.row_index, selected_option: selected ?? {}, match_type: String(event.target.value) })}>{skosTypes.map(value => <MenuItem key={value || 'none'} value={value}>{value || 'Choose a relation'}</MenuItem>)}</Select></FormControl>}
              <Box><Button variant="text" aria-expanded={searchOpen} onClick={() => setSearchOpen(value => !value)}>Find other candidates</Button></Box>
              {searchOpen && <Stack spacing={1}><TextField size="small" label={`Search label for ${row.term}`} value={drafts[key] ?? ''} onChange={event => setDrafts(previous => ({ ...previous, [key]: event.target.value }))} /><Box><SearchActionButton disabled={!drafts[key]?.trim() || !queue.length} onClick={() => emit({ type: 'custom_search', row_index: row.row_index, search_term: drafts[key]?.trim(), display_mode: rec.display_mode ?? 'Mixed Results' })}>Search label</SearchActionButton></Box>{row.custom_search_summary && <Typography role="status" variant="body2" sx={{ fontVariantNumeric: 'tabular-nums' }}>{fmt(row.custom_search_summary.results_count)} candidates found for “{row.custom_search_summary.search_term}”.</Typography>}</Stack>}
              {row.mapping_justification && <MetaList items={[{ label: 'Mapping justification', value: row.mapping_justification, mono: true }]} />}
            </Stack> : <Typography variant="body2" color="text.secondary">Choose a term from the list. If it is empty, adjust your filters.</Typography>}
          </Box>
        </Box>
      </Stack></Section>
      <Dialog open={settingsOpen} onClose={() => setSettingsOpen(false)} fullWidth maxWidth="sm" aria-labelledby="review-settings-title"><DialogTitle id="review-settings-title">View options</DialogTitle><DialogContent><Stack spacing={1.5} sx={{ pt: 1 }}>
        <Typography variant="body2" color="text.secondary">These options change how results are shown, not the search itself.</Typography>
        <FormControlLabel control={<Switch checked={Boolean(cfg.show_only_matched_terms)} onChange={event => emit({ type: 'update_settings', settings: { show_only_matched_terms: event.target.checked, current_page: 1 } })} />} label="Only terms with candidates" />
        <FormControlLabel control={<Switch checked={Boolean(cfg.show_only_unreconciled_terms)} onChange={event => emit({ type: 'update_settings', settings: { show_only_unreconciled_terms: event.target.checked, current_page: 1 } })} />} label="Only terms still needing a mapping or relation" />
        <Stack direction="row" alignItems="center" spacing={0.5}><FormControlLabel control={<Switch checked={skos} onChange={event => emit({ type: 'update_settings', settings: { skos_matching_enabled: event.target.checked } })} />} label="Describe the relation using SKOS" /><InfoButton title="SKOS relations"><p>SKOS states how a chosen concept relates to the term: an exact, close, broader, narrower or related match.</p><p>When this is on, choose a relation for every mapped term.</p></InfoButton></Stack>
        <FormControl fullWidth size="small"><InputLabel id="review-page-size">Terms per page</InputLabel><Select labelId="review-page-size" label="Terms per page" value={cfg.items_per_page ?? 10} onChange={event => emit({ type: 'update_settings', settings: { items_per_page: Number(event.target.value), current_page: 1 } })}>{Array.from(new Set([5, 10, 20, 50, cfg.items_per_page ?? 10])).map(value => <MenuItem key={value} value={value}>{value}</MenuItem>)}</Select></FormControl>
        <Box><Button variant="outlined" disabled={!cfg.provider_has_results?.length} onClick={() => { setSettingsOpen(false); setPrefill(true); }}>Prefill top candidates…</Button></Box>
      </Stack></DialogContent><DialogActions><Button onClick={() => setSettingsOpen(false)}>Close</Button></DialogActions></Dialog>
      <Dialog open={prefill} onClose={() => setPrefill(false)} aria-labelledby="prefill-title" aria-describedby="prefill-message"><DialogTitle id="prefill-title">Prefill top candidates?</DialogTitle><DialogContent><DialogContentText id="prefill-message">{`This selects the top candidate for each still-unmapped term whose spelling similarity is at least ${(cfg.levenshtein_threshold ?? 0.7).toFixed(2)}; other terms stay unmapped. Existing mappings are kept. These are suggestions, not verified semantic equivalences; review the selections afterwards.`}</DialogContentText></DialogContent><DialogActions><Button autoFocus onClick={() => setPrefill(false)}>Cancel</Button><Button variant="contained" onClick={async () => { if (await emit({ type: 'prefill_best_match' })) setPrefill(false); }}>Apply suggestions</Button></DialogActions></Dialog>
    </>}
    {!!cfg.provider_status?.length && <Box ref={detailsRef}><Disclosure title="Run details" summary={issues.length ? `${fmt(issues.length)} ${issues.length === 1 ? 'source' : 'sources'} with errors` : `${fmt(cfg.provider_status.length)} ${cfg.provider_status.length === 1 ? 'source' : 'sources'}`} expanded={detailsOpen} onChange={setDetailsOpen}>
      <TableContainer><Table size="small" aria-label="Run details by source"><TableHead><MuiTableRow><TableCell>Source</TableCell><TableCell>Status</TableCell><TableCell align="right">Terms with candidates</TableCell></MuiTableRow></TableHead><TableBody>{cfg.provider_status.map(provider => <React.Fragment key={provider.name}><MuiTableRow sx={provider.error_msg ? { '& > *': { borderBottom: 0 } } : undefined}><TableCell component="th" scope="row">{provider.name}</TableCell><TableCell><Chip size="small" variant="outlined" color={providerTone(provider.status)} label={sentenceCase(provider.status ?? 'pending')} /></TableCell><TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>{fmt(provider.results_count)}</TableCell></MuiTableRow>{provider.error_msg && <MuiTableRow><TableCell colSpan={3} sx={{ pt: 0, color: 'error.main', overflowWrap: 'anywhere' }}>{provider.error_msg}</TableCell></MuiTableRow>}</React.Fragment>)}</TableBody></Table></TableContainer>
    </Disclosure></Box>}
  </Stack>;
}
function ExportPage({ snapshot }: { snapshot: Snapshot }) {
  const summary = snapshot.review_summary ?? {}; const downloads = snapshot.downloads ?? {};
  const sssomRows = downloads.sssom_preview ?? snapshot.data?.curated_preview ?? [];
  const contextRows = downloads.candidate_review_preview ?? snapshot.data?.provider_context_preview ?? [];
  const total = summary.total ?? snapshot.data?.rows ?? sssomRows.length;
  const kpis: Kpi[] = [{ label: 'Terms', value: fmtCount(total) }, { label: 'Mapped', value: fmtCount(summary.matched) }, { label: 'Unmapped', value: fmtCount(summary.unmatched) }];
  if (snapshot.config?.skos_matching_enabled || summary.missing_relation) kpis.push({ label: 'Missing relation', value: fmtCount(summary.missing_relation), tone: summary.missing_relation ? 'warning' : 'default' });
  return <Stack spacing={3}>
    <KpiStrip ariaLabel="Curation result" items={kpis} />
    <Section title="Curated SSSOM table" info={{ title: 'SSSOM export', content: 'Downloads include every term. Unmapped terms keep their empty mapping fields.' }} description={`Showing ${fmt(sssomRows.length)} of ${fmt(total)} rows`} actions={<DownloadButton disabled={!downloads.sssom_csv} onClick={() => triggerDownload(downloads.sssom_csv ?? '', downloads.sssom_filename ?? 'sssom_curated.csv')}>Download SSSOM (CSV)</DownloadButton>}>
      <PreviewTable rows={sssomRows} label="Curated SSSOM result" />
    </Section>
    <Disclosure title="Candidate context and review export" optional summary={downloads.candidate_review_csv ? 'Ready to download' : 'Not available yet'}><Stack spacing={1.5}>
      <PreviewTable rows={contextRows} label="Candidate review context" />
      <Stack direction="row" alignItems="center" spacing={0.5}><DownloadButton disabled={!downloads.candidate_review_csv} onClick={() => triggerDownload(downloads.candidate_review_csv ?? '', downloads.candidate_review_filename ?? 'candidate_review.csv')}>Download review context (CSV)</DownloadButton><InfoButton title="Review context export">Provider metadata and candidate context document how the mappings were chosen. This export is separate from the SSSOM table.</InfoButton></Stack>
    </Stack></Disclosure>
  </Stack>;
}
export function SemiAutomaticReconciliationApp({ args, onEvent, busy = false, onDraftChange }: AppProps) {
  const snapshot = args?.snapshot ?? {};
  const loaded = Boolean(snapshot.data?.has_table);
  const activeStage: Stage = !loaded ? 'load' : snapshot.active_stage === 'run' ? 'reconcile' : snapshot.active_stage ?? 'load';
  const index = Math.max(0, stages.findIndex(stage => stage.id === activeStage));
  const meta = stages[index]; const last = index === stages.length - 1;
  const [pending, setPending] = useState(0); const [error, setError] = useState('');
  const [draft, setDraft] = useState<{ reading: boolean; config: ConfigDraft }>({ reading: false, config: noConfigDraft }); const [editVersion, setEditVersion] = useState(0);
  const [searchLabels, setSearchLabels] = useState<Record<string, string>>({});
  const [handoffOpen, setHandoffOpen] = useState(false);
  const focusBeforeSave = useRef<HTMLElement | null>(null);
  const working = busy || pending > 0; const searching = Boolean(snapshot.run?.processing_active);
  const hasDraft = draft.reading || draft.config.sections.length > 0 || draft.config.files !== 'none';
  const blocked = working || hasDraft || searching;
  const sourceKey = JSON.stringify([snapshot.data?.filename, snapshot.data?.rows, snapshot.data?.table_revision]);
  const resultsReady = completedReconciliationRun(snapshot);
  const summary = snapshot.review_summary ?? {};
  const queue = snapshot.config?.provider_queue ?? [];
  useEffect(() => { onDraftChange?.(hasDraft); return () => onDraftChange?.(false); }, [hasDraft, onDraftChange]);
  useEffect(() => { if (!hasDraft) return; const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; }; window.addEventListener('beforeunload', warn); return () => window.removeEventListener('beforeunload', warn); }, [hasDraft]);
  useEffect(() => { setDraft({ reading: false, config: noConfigDraft }); setSearchLabels({}); setEditVersion(value => value + 1); }, [sourceKey]);
  // Saving makes the stage inert, which drops keyboard focus to <body>. Put it back on the control
  // the curator used once the save has finished (unless focus has moved on, e.g. to a new stage heading).
  useEffect(() => {
    if (working) return;
    const target = focusBeforeSave.current; focusBeforeSave.current = null;
    if (target?.isConnected && (!document.activeElement || document.activeElement === document.body)) target.focus({ preventScroll: true });
  }, [working]);
  const emit: Emit = useCallback(async event => {
    const focused = document.activeElement;
    if (!focusBeforeSave.current && focused instanceof HTMLElement && focused !== document.body) focusBeforeSave.current = focused;
    setPending(value => value + 1); setError('');
    try {
      if (!onEvent) { emitAppEvent(event); return true; }
      const result = await onEvent({ ...event, nonce: crypto.randomUUID() });
      // A failed request is shown by the shell (WizardPage status); a backend error arrives as statusMessage.
      if (result === false) return false;
      return (result as any)?.args?.snapshot?.statusMessage?.severity !== 'error';
    }
    catch (failure) { setError(failure instanceof Error ? failure.message : String(failure)); return false; }
    finally { setPending(value => Math.max(0, value - 1)); }
  }, [onEvent]);
  function navigate(stage: Stage) { if (!working && !searching && !hasDraft && (stage === 'load' || loaded)) void emit({ type: 'navigate', stage }); }
  const onConfigDraft = useCallback((next: ConfigDraft) => setDraft(previous => sameConfigDraft(previous.config, next) ? previous : { ...previous, config: next }), []);
  const discardSettings = () => { setEditVersion(value => value + 1); setDraft(previous => ({ ...previous, config: noConfigDraft })); };
  const closeHandoff = () => { setHandoffOpen(false); void emit({ type: 'dismiss_rdf_handoff' }); };
  const configReady = Boolean((snapshot.config?.provider_queue?.length && !snapshot.run?.missing_start_configs?.length) || snapshot.review_summary?.matched);
  const disabled = blocked || !loaded || (activeStage === 'configure' && !configReady) || ((activeStage === 'reconcile' || activeStage === 'export') && !resultsReady);
  // The stepper may only reach what a chain of Continue clicks from here would reach (each stage in
  // between must be leavable, as for Continue); going back is always allowed.
  const canLeave: Record<Stage, boolean> = { load: loaded, configure: configReady, reconcile: resultsReady, export: false };
  const reachable = stages.map((_, position) => position <= index || stages.slice(index, position).every(stage => canLeave[stage.id]));
  // A finished run alone does not complete the review: the curator has to map something or move on.
  const completed = [loaded, loaded && configReady, loaded && resultsReady && (index > 2 || Boolean(summary.matched)), snapshot.rdf_handoff?.status === 'complete'];
  const steps: WizardStep[] = stages.map((stage, position) => ({ id: stage.id, label: stage.label, completed: completed[position], disabled: position !== index && (blocked || !reachable[position]) }));
  const hint = working ? 'Saving changes…'
    : searching ? 'Reconciliation is running. Stop the run to change steps.'
    : draft.reading ? 'Reading the file…'
    : draft.config.files === 'reading' ? 'Reading the files…'
    : draft.config.sections.length ? 'Apply or discard your edits to continue.'
    : draft.config.files === 'unindexed' ? 'Index or discard the selected files to continue.'
    : !loaded ? 'Load a matching table to continue.'
    : activeStage === 'load' ? 'Next: choose your sources.'
    : activeStage === 'configure' ? configReady ? 'Next: find and review candidates.' : queue.length ? 'Complete the source settings to continue.' : 'Choose a source to continue.'
    : !resultsReady ? 'Finish a reconciliation run to continue.'
    : activeStage === 'reconcile' ? 'Next: check the result and export.'
    : 'Next: send your mappings to the RDF generator.';
  const status = snapshot.statusMessage; const message = error || (status?.severity === 'error' || status?.severity === 'warning' ? status.text : '');
  // Unapplied edits are named here, under the stage header; their Apply buttons sit next to the fields.
  const draftNotice = draft.config.files === 'reading' ? <Alert severity="info">Reading the selected files…</Alert>
    : draft.config.sections.length || draft.config.files === 'unindexed' ? <Alert severity="info" action={<Button color="inherit" size="small" onClick={discardSettings}>Discard edits</Button>}>{draft.config.sections.length ? `Unapplied edits in ${joinNames(draft.config.sections)}. Apply them there, or discard them to continue.` : `Unindexed files in ${LOCAL_FILES}. Retry indexing there, or discard them to continue.`}</Alert>
    : null;
  const confirmRequired = snapshot.rdf_handoff?.status === 'confirmation_required';
  const skos = Boolean(snapshot.config?.skos_matching_enabled);
  return <WizardPage
    steps={steps}
    activeIndex={index}
    onStepClick={position => { if (position !== index) navigate(stages[position].id); }}
    title={meta.title}
    lead={meta.lead}
    info={meta.info}
    actions={<ToolMenu items={[{ label: 'Reset manual curation', icon: <RestartAltIcon fontSize="small" />, disabled: working || searching, onSelect: () => { void emit({ type: 'reset_workflow' }); }, confirm: { title: 'Reset manual curation?', message: 'This clears the loaded table, the selected sources, candidates and your mappings. You can undo this from History.', confirmLabel: 'Reset manual curation' } }]} />}
    status={message || draftNotice ? <>{message ? <Alert severity={!error && status?.severity === 'warning' ? 'warning' : 'error'}>{message}</Alert> : null}{draftNotice}</> : undefined}
    nav={{
      // On the first stage Back falls through to the shell (previous tool); it is still blocked while busy.
      back: index > 0 ? { onClick: () => navigate(stages[index - 1].id), disabled: blocked, tooltip: `Back to ${stages[index - 1].label}` } : { disabled: blocked },
      next: { onClick: last ? () => setHandoffOpen(true) : () => navigate(stages[index + 1].id), disabled, tooltip: last ? 'Continue to RDF generator' : `Continue to ${stages[index + 1].label}` },
      hint,
    }}
  >
    <Box component="fieldset" disabled={working} {...{ inert: working ? '' : undefined }} aria-busy={working} aria-label="Manual curation" sx={{ p: 0, m: 0, border: 0, minWidth: 0 }}>
      <Box hidden={activeStage !== 'load'}><Box component="fieldset" disabled={searching} {...{ inert: searching ? '' : undefined }} sx={{ p: 0, m: 0, border: 0, minWidth: 0 }}><LoadPage snapshot={snapshot} emit={emit} busy={working} onReading={value => setDraft(previous => ({ ...previous, reading: value }))} /></Box></Box>
      <Box hidden={activeStage !== 'configure'}><ConfigurePage key={sourceKey + editVersion} snapshot={snapshot} emit={emit} onDraft={onConfigDraft} /></Box>
      <Box hidden={activeStage !== 'reconcile'}><ReviewPage snapshot={snapshot} emit={emit} searching={searching} drafts={searchLabels} setDrafts={setSearchLabels} /></Box>
      <Box hidden={activeStage !== 'export'}>{resultsReady ? <ExportPage snapshot={snapshot} /> : <EmptyState title="No result to export yet" description="Go back to Review matches and finish a reconciliation run first." />}</Box>
    </Box>
    <Dialog open={handoffOpen} onClose={working ? undefined : closeHandoff} fullWidth maxWidth="sm" aria-labelledby="rdf-handoff-title" aria-describedby="rdf-handoff-message"><DialogTitle id="rdf-handoff-title">Send mappings to the RDF generator?</DialogTitle><DialogContent><Stack spacing={2}>
      <DialogContentText id="rdf-handoff-message">The current mappings will be loaded into the RDF generator. This does not generate RDF automatically.</DialogContentText>
      <MetaList items={[{ label: 'Terms', value: fmtCount(summary.total), numeric: true }, { label: 'Mapped', value: fmtCount(summary.matched), numeric: true }, { label: 'Unmapped', value: fmtCount(summary.unmatched), numeric: true }, ...(skos || summary.missing_relation ? [{ label: 'Missing relation', value: fmtCount(summary.missing_relation), numeric: true }] : [])]} />
      {skos && Boolean(summary.missing_relation) && <Typography variant="body2" color="text.secondary">{fmt(summary.missing_relation)} mapped {summary.missing_relation === 1 ? 'term has' : 'terms have'} no SKOS relation yet.</Typography>}
      {confirmRequired && <Alert severity="warning">{snapshot.rdf_handoff?.message}</Alert>}
    </Stack></DialogContent><DialogActions><Button autoFocus disabled={working} onClick={closeHandoff}>Cancel</Button><Button variant="contained" color={confirmRequired ? 'warning' : 'primary'} disabled={working || searching} onClick={() => emit({ type: 'handoff_rdf_generator', expected_revision: snapshot.review_revision ?? 0, confirmation_token: snapshot.rdf_handoff?.confirmation_token })}>{working ? 'Sending…' : confirmRequired ? 'Replace mappings in RDF generator' : 'Send to RDF generator'}</Button></DialogActions></Dialog>
  </WizardPage>;
}
