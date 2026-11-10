import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  AlertTitle,
  Box,
  Button,
  Checkbox,
  Chip,
  Collapse,
  Divider,
  FormControlLabel,
  IconButton,
  MenuItem,
  Stack,
  Switch,
  Tab,
  Tabs,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
} from '@mui/material';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import { emitAppEvent, notifyLayoutChanged, type AppEvent } from '../../shared/appBridge';
import { ConfirmActionButton } from '../../components/dialogs/ConfirmActionButton';
import { DataTable } from '../../components/table/DataTable';
import { RdfCodeView } from '../../components/code/RdfCodeView';
import { DownloadButton, GenerateButton, SearchActionButton, UploadButton, UseResultButton } from '../../components/actions/WorkflowActionButtons';
import { ToolMenu } from '../../components/actions/ToolMenu';
import { InfoButton, type InfoContent } from '../../components/help/InfoButton';
import { Disclosure } from '../../components/layout/Disclosure';
import { KpiStrip } from '../../components/layout/KpiStrip';
import { MetaList, type MetaItem } from '../../components/layout/MetaList';
import { Section } from '../../components/layout/Section';
import { WizardPage, type WizardStep } from '../../components/layout/WizardPage';
import { EmptyState } from '../../components/states/EmptyState';
import { useToolNavigation } from '../../shared/toolNavigation';
import { fonts, motion } from '../../theme/tokens';
import { ResultOverview, type Overview } from './ResultOverview';

type Severity = 'success' | 'info' | 'warning' | 'error';
type TableRow = Record<string, unknown>;
type Stage = 'metadata' | 'load' | 'enrich' | 'templates' | 'model' | 'reference' | 'generate' | 'export';

type TemplateProperty = { id?: string; predicate?: string; map_type?: string; value?: string };
type SchemaTemplate = { template_name?: string; rdf_type?: string; properties?: TemplateProperty[] };
type GroupSuggestion = { display?: string; columns?: string[] };

type Snapshot = {
  active_stage?: Stage;
  statusMessage?: { severity?: Severity; text?: string } | null;
  data?: { has_table?: boolean; rows?: number; columns?: number; column_names?: string[]; filename?: string; source_message?: string; shared_available?: boolean; preview?: TableRow[] };
  mapping?: { has_table?: boolean; rows?: number; columns?: number; column_names?: string[]; filename?: string; source_message?: string; shared_available?: boolean; preview?: TableRow[] };
  load_options?: { data_header_row?: number; mapping_header_row?: number; data_csv_separator?: string | null; mapping_csv_separator?: string | null; data_sheets?: string[]; mapping_sheets?: string[]; data_sheet?: string | null; mapping_sheet?: string | null };
  templates?: { items?: SchemaTemplate[]; json?: string; map_types?: string[]; guidance?: string };
  reference?: { loaded?: boolean; method?: string; doi?: string; summary?: string; preview?: string; manual?: Partial<Record<ManualField, string>>; manual_missing?: string[] };
  config?: {
    data_columns?: string[];
    mapping_columns?: string[];
    id_option?: string;
    default_id_col?: string;
    use_shared_id?: boolean;
    shared_id_col?: string;
    subject_base_uri?: string;
    graph_option?: string;
    graph_base_uri?: string;
    graph_base_uri_default?: string;
    named_graph_uri?: string;
    term_col?: string;
    uri_col?: string;
    match_type_col?: string;
    use_class?: boolean;
    class_uri?: string;
    rdf_format?: string;
    active_template?: string;
    group_active?: boolean;
    group_suggestions?: Record<string, GroupSuggestion>;
    group_config?: Record<string, { columns?: string[]; connecting_predicate?: string; group_type?: string | null }>;
  };
  ready?: { can_generate?: boolean; issues?: string[] };
  outputs?: { has_rdf?: boolean; has_skos?: boolean; has_dcat_metadata?: boolean; has_dcat_catalog?: boolean; has_reference?: boolean; file_stem?: string; rdf_data?: string; skos_data?: string; dcat_metadata_data?: string; dcat_catalog_data?: string; reference_rdf?: string; previews?: Record<string, string>; overview?: Overview | null };
  dcat?: { available?: boolean; confirmed?: boolean; missing?: string[]; title?: string; description?: string; access_rights?: string; contact_point?: string; publisher_name?: string; publisher_uri?: string; themes?: string[]; theme_options?: string[]; license?: string; license_options?: LicenseOption[]; link_reference?: boolean };
};

type ManualField = 'title' | 'journal' | 'authors' | 'year' | 'volume' | 'pages' | 'doi';
type LicenseOption = { id: string; label: string; uri?: string };

type AppProps = { args?: { app?: string; snapshot?: Snapshot }; busy?: boolean; onDraftChange?: (dirty: boolean) => void };

// Backend protocol values are sent unchanged; only their visible labels are sentence case.
type Option = { value: string; label: React.ReactNode };
const NO_TEMPLATE = 'None (use default column mapping)';
const GENERATED_IDS = 'Generated IDs';
const formatOptions: Option[] = ['Turtle', 'N-Quads', 'JSON-LD', 'RDF/XML', 'TriG'].map((value) => ({ value, label: value }));
const NO_REFERENCE = 'None';
const referenceMethods: Option[] = [{ value: NO_REFERENCE, label: 'No publication' }, { value: 'DOI', label: 'Look up by DOI' }, { value: 'Manual Entry', label: 'Enter manually' }];
const accessRights: Option[] = [{ value: 'PUBLIC', label: 'Public' }, { value: 'RESTRICTED', label: 'Restricted' }, { value: 'NON_PUBLIC', label: 'Non-public' }];
const mapTypeLabels: Record<string, string> = { 'Column Value (Literal)': 'Column value (literal)', 'Column Value (URI)': 'Column value (URI)', 'Fixed URI': 'Fixed URI', 'Nested Template': 'Nested template' };
// Columns URI enrichment adds to the mapping table; all present means it has run.
const ENRICHMENT_COLUMNS = ['label', 'ui_link', 'acronym', 'source', 'message'];

const csvSeparators = [
  { label: 'Auto-detect', value: '', summary: 'auto-detected separator' },
  { label: 'Comma (,)', value: ',', summary: 'comma separator' },
  { label: 'Semicolon (;)', value: ';', summary: 'semicolon separator' },
  { label: 'Tab (\\t)', value: '\t', summary: 'tab separator' },
  { label: 'Pipe (|)', value: '|', summary: 'pipe separator' },
];

const numberFormat = new Intl.NumberFormat();
const sizeFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const none = <Box component="span" sx={{ color: 'text.secondary' }}>—</Box>;

// Counts sent events, so a control can tell whether its change was saved (which makes the page busy).
let emitted = 0;
function emit(event: AppEvent) { emitted += 1; emitAppEvent(event); }
function asNumber(value: unknown, fallback: number) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : fallback; }
function triggerDownload(content: string, filename: string, mime: string) { const blob = new Blob([content], { type: mime }); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = filename; a.click(); window.setTimeout(() => URL.revokeObjectURL(url), 1000); }
async function fileToBase64(file: File) { const bytes = new Uint8Array(await file.arrayBuffer()); let binary = ''; bytes.forEach((b) => { binary += String.fromCharCode(b); }); return window.btoa(binary); }
function plural(count: number, singular: string, pluralForm = `${singular}s`) { return `${numberFormat.format(count)} ${count === 1 ? singular : pluralForm}`; }
function joinNames(names: string[]) { return names.length < 2 ? names[0] ?? '' : `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`; }
function formatSize(bytes: number) { return bytes < 1024 ? `${numberFormat.format(bytes)} B` : bytes < 1048576 ? `${sizeFormat.format(bytes / 1024)} KB` : `${sizeFormat.format(bytes / 1048576)} MB`; }
function columnOptions(columns: string[]): Option[] { return [{ value: '', label: none }, ...columns.map((column) => ({ value: column, label: column }))]; }
function mapTypeLabel(type?: string) { return type ? mapTypeLabels[type] ?? type : ''; }

type OutputFile = { label: string; what: string; format: string; value?: string; suffix: string; mime: string; preview?: string; source: string };
function outputFiles(out: NonNullable<Snapshot['outputs']>): OutputFile[] {
  return [
    { label: 'Data graph', what: 'data graph', format: 'Turtle', value: out.rdf_data, suffix: '_data.ttl', mime: 'text/turtle', preview: out.previews?.rdf, source: 'Generate RDF' },
    { label: 'SKOS vocabulary', what: 'SKOS vocabulary', format: 'Turtle', value: out.skos_data, suffix: '_skos.ttl', mime: 'text/turtle', preview: out.previews?.skos, source: 'Generate RDF' },
    { label: 'DCAT metadata', what: 'DCAT metadata', format: 'Turtle', value: out.dcat_metadata_data, suffix: '_dcat_metadata.ttl', mime: 'text/turtle', preview: out.previews?.dcat_metadata, source: 'Generate RDF, needs a named graph' },
    { label: 'Full catalog', what: 'full catalog', format: 'TriG', value: out.dcat_catalog_data, suffix: '_full_catalog.trig', mime: 'application/trig', preview: out.previews?.dcat_catalog, source: 'Generate RDF, needs a named graph' },
    { label: 'Publication reference', what: 'publication reference', format: 'Turtle', value: out.reference_rdf, suffix: '_reference.ttl', mime: 'text/turtle', preview: out.previews?.reference, source: 'Describe dataset › Publication reference' },
  ];
}

// --- Text fields that save once per edit -------------------------------------------------------
// Every update_settings makes the shell busy, which disables and inerts the stage fieldset. Text
// settings therefore keep what is typed locally and send the same payload once, on blur or Enter,
// instead of on every keystroke (which used to steal focus and drop characters).
type CommitScopeValue = {
  // True from a pointer press inside the tool until the click it produces.
  pressing: React.MutableRefObject<boolean>;
  register: (commit: () => void) => () => void;
  // Saves pending text edits now: before navigating, discarding or a fetch that reads them.
  flush: () => void;
  // Element to refocus once the busy state has cleared (the inert fieldset drops focus).
  remember: (target: EventTarget | null) => void;
};
const CommitScope = React.createContext<CommitScopeValue | null>(null);

// A press that ends an edit saves after its click, so the click lands before the page turns busy.
const PRESS_END = ['click', 'auxclick', 'contextmenu', 'pointercancel'] as const;
function afterPress(run: () => void) {
  let done = false;
  const finish = () => {
    if (done) return;
    done = true;
    window.clearTimeout(fallback);
    PRESS_END.forEach((type) => window.removeEventListener(type, finish, true));
    window.setTimeout(run, 0);
  };
  const fallback = window.setTimeout(finish, 1500);
  PRESS_END.forEach((type) => window.addEventListener(type, finish, true));
}

function CommitTextField({ value, onCommit, onDraftChange, helperText, ...field }: { label: string; value: string; onCommit: (value: string) => void; onDraftChange?: (value: string) => void; placeholder?: string; helperText?: React.ReactNode; fullWidth?: boolean; required?: boolean; error?: boolean; multiline?: boolean; minRows?: number }) {
  const scope = useContext(CommitScope);
  const input = useRef<HTMLInputElement>(null);
  const [draft, setDraft] = useState(value);
  const saved = useRef(value);
  // Re-renders after a save, so the unsaved hint and the unload guard follow saved.current.
  const [, setSaves] = useState(0);
  const latest = useRef({ draft, onCommit, onDraftChange });
  latest.current = { draft, onCommit, onDraftChange };
  // Server echoes replace the text unless the user is typing an unsaved change into it.
  useEffect(() => {
    const previous = saved.current;
    saved.current = value;
    if (document.activeElement === input.current && latest.current.draft !== previous) return;
    setDraft(value);
    latest.current.onDraftChange?.(value);
  }, [value]);
  const commit = useCallback(() => {
    const next = latest.current.draft;
    if (next === saved.current) return false;
    saved.current = next;
    setSaves((count) => count + 1);
    latest.current.onCommit(next);
    return true;
  }, []);
  useEffect(() => scope?.register(commit), [scope, commit]);
  const unsaved = draft !== saved.current;
  // Typed text is only sent on blur or Enter, so warn before a reload or tab close would drop it.
  useEffect(() => {
    if (!unsaved) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [unsaved]);
  return <TextField
    {...field}
    helperText={unsaved ? (field.multiline ? 'Leave the field to save.' : 'Press Enter or leave the field to save.') : helperText}
    inputRef={input}
    value={draft}
    onChange={(event) => { setDraft(event.target.value); onDraftChange?.(event.target.value); }}
    onBlur={(event) => {
      const next = event.relatedTarget;
      if (scope?.pressing.current) afterPress(() => { if (input.current?.isConnected && commit()) scope.remember(document.activeElement); });
      else if (commit()) scope?.remember(next);
    }}
    onKeyDown={(event) => { if (event.key === 'Enter' && !field.multiline && commit()) scope?.remember(input.current); }}
  />;
}

function SelectField({ label, value, options, onChange, disabled, helperText }: { label: string; value: string; options: Option[]; onChange: (value: string) => void; disabled?: boolean; helperText?: React.ReactNode }) {
  const scope = useContext(CommitScope);
  const root = useRef<HTMLDivElement>(null);
  const change = (next: string) => {
    const before = emitted;
    onChange(next);
    // A saved choice makes the page busy while the menu closes, so its focus return fails. Refocus
    // this select afterwards (the menu item that had focus is gone by then).
    if (emitted !== before) scope?.remember(root.current?.querySelector('[role="combobox"]') ?? null);
  };
  return <TextField select fullWidth ref={root} label={label} value={value} disabled={disabled} helperText={helperText} onChange={(event) => change(event.target.value)}>{options.map((option) => <MenuItem key={option.value || 'none'} value={option.value}>{option.label}</MenuItem>)}</TextField>;
}

function SwitchField({ label, info, checked, onChange }: { label: string; info?: InfoContent; checked: boolean; onChange: (checked: boolean) => void }) {
  return <Stack direction="row" alignItems="center" spacing={0.5}><FormControlLabel sx={{ mr: 0 }} control={<Switch checked={checked} onChange={(event) => onChange(event.target.checked)} />} label={label} />{info && <InfoButton title={info.title}>{info.content}</InfoButton>}</Stack>;
}

function CodePreview({ value, full }: { value?: string; full?: string }) {
  if (!value) return <Typography variant="body2" color="text.secondary">No preview yet.</Typography>;
  // The backend cuts previews at 5,000 characters; say so when the file is longer.
  const cut = full && full.length > value.length ? full.length : 0;
  return <Stack spacing={1}><RdfCodeView value={value} />{cut > 0 && <Typography variant="caption" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums' }}>Showing the first {numberFormat.format(value.length)} of {numberFormat.format(cut)} characters. Download the file for the full content.</Typography>}</Stack>;
}

// label: the stepper (verb + object); title: the stage h2, a short sentence (DESIGN.md wording table).
const mainStages: { id: Stage; label: string; title: string; lead: string; info: InfoContent }[] = [
  {
    id: 'metadata',
    label: 'Describe dataset',
    title: 'Describe the dataset and its publication',
    lead: 'Enter the DCAT metadata and, optionally, the publication behind the data.',
    info: { title: 'Describing the dataset', content: <><p>The metadata describes the dataset as a whole: title, publisher, license, access rights and themes. It becomes the DCAT catalog that is generated together with the RDF.</p><p>The publication reference is optional. It is exported as its own file and can be linked from the catalog.</p></> },
  },
  {
    id: 'load',
    label: 'Load tables',
    title: 'Load your data and mapping tables',
    lead: 'Upload CSV or Excel files, or use the results of the earlier tools.',
    info: { title: 'Loading tables', content: <><p>The generator combines two tables: the data table, with one row per observation, and the mapping table, which links the terms in that data to ontology URIs.</p><p>Prepared data comes from the Matching table tool. Reconciled mappings come from Manual curation or AI-assisted matching.</p></> },
  },
  {
    id: 'model',
    label: 'Configure RDF',
    title: 'Choose how your rows become RDF',
    lead: 'Check subject IDs and the named graph; everything else is optional.',
    info: { title: 'Configuring RDF', content: <><p>Subject IDs and the named graph are generated automatically. The optional sections change how subjects are identified, link subjects across files, group columns, enrich URIs or apply schema templates. The mapping-table columns are detected automatically.</p><p>Text fields are saved when you leave them or press Enter.</p></> },
  },
  {
    id: 'generate',
    label: 'Generate RDF',
    title: 'Turn your tables into RDF',
    lead: 'Generate the RDF and check the result.',
    info: { title: 'Generating RDF', content: <><p>Generating builds the data graph from your rows, a SKOS vocabulary from the mapped terms and, with a named graph, the DCAT catalog from the dataset metadata.</p><p>The RDF inspector can load the catalog.</p></> },
  },
  {
    id: 'export',
    label: 'Export',
    title: 'Download the generated files',
    lead: 'Pick the files you need and check their content in the preview.',
    info: { title: 'Exporting files', content: <ul><li><strong>Data graph</strong>: your rows as RDF (Turtle).</li><li><strong>SKOS vocabulary</strong>: the mapped terms.</li><li><strong>DCAT metadata</strong> and <strong>full catalog</strong> (TriG): built with the RDF when it uses a named graph. Continue opens the catalog in the RDF inspector.</li><li><strong>Publication reference</strong>: from Describe dataset.</li></ul> },
  },
];

function LoadPage({ snapshot }: { snapshot: Snapshot }) {
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', lg: 'repeat(2, minmax(0, 1fr))' }, gap: 3, alignItems: 'start' }}>
    <TableSource kind="data" snapshot={snapshot} />
    <TableSource kind="mapping" snapshot={snapshot} />
  </Box>;
}

function TableSource({ kind, snapshot }: { kind: 'data' | 'mapping'; snapshot: Snapshot }) {
  const isData = kind === 'data';
  const load = snapshot.load_options ?? {};
  const input = snapshot[kind] ?? {};
  const headerOption = isData ? load.data_header_row : load.mapping_header_row;
  const [header, setHeader] = useState(asNumber(headerOption, 1));
  const [separator, setSeparator] = useState((isData ? load.data_csv_separator : load.mapping_csv_separator) ?? '');
  useEffect(() => setHeader(asNumber(headerOption, 1)), [headerOption]);
  const fileInput = useRef<HTMLInputElement>(null);
  const label = isData ? 'Data table' : 'Mapping table';
  const object = isData ? 'data table' : 'mapping table';
  const sheets = (isData ? load.data_sheets : load.mapping_sheets) ?? [];
  const sheet = (isData ? load.data_sheet : load.mapping_sheet) ?? '';
  const rows = input.rows ?? 0;
  const previewRows = input.preview?.length ?? 0;
  const separatorName = csvSeparators.find((option) => option.value === separator)?.summary ?? 'custom separator';
  async function upload(event: React.ChangeEvent<HTMLInputElement>) { const file = event.target.files?.[0]; event.target.value = ''; if (!file) return; emit({ type: isData ? 'upload_data' : 'upload_mapping', filename: file.name, content_base64: await fileToBase64(file), header_row: header }); }
  return <Stack spacing={1.5} sx={{ minWidth: 0 }}>
    <Section
      title={label}
      description="CSV or Excel (.csv, .xlsx, .xls)."
      info={isData
        ? { title: 'Data table', content: 'The data table holds your original observations, one row per subject. Its terms are linked to ontology URIs through the mapping table.' }
        : { title: 'Mapping table', content: 'The mapping table holds the reconciled terms with their ontology URIs, in SSSOM columns such as subject_label, object_id and predicate_id. A reconciliation tool usually sends it here.' }}
    >
      <Stack spacing={2}>
        {input.has_table
          ? <MetaList items={[{ label: 'File', value: input.filename, mono: true }, { label: 'Rows', value: numberFormat.format(rows), numeric: true }, { label: 'Columns', value: numberFormat.format(input.columns ?? 0), numeric: true }]} />
          : <EmptyState dense title={`No ${object} loaded`} description={isData ? 'Use the prepared data or upload a file.' : 'Use the reconciled mappings or upload a file.'} />}
        <Stack direction="row" useFlexGap flexWrap="wrap" gap={1}>
          <Tooltip title={input.shared_available ? '' : isData ? 'Available once the Matching table tool has prepared your data.' : 'Available once a reconciliation tool has sent its mappings.'}>
            <span><UseResultButton disabled={!input.shared_available} onClick={() => emit({ type: isData ? 'load_shared_data' : 'load_shared_mapping' })}>{isData ? 'Use prepared data' : 'Use reconciled mappings'}</UseResultButton></span>
          </Tooltip>
          {/* A native button opening a hidden input: Enter and Space work without extra handlers. */}
          <input ref={fileInput} hidden type="file" accept=".csv,.xlsx,.xls" onChange={upload} />
          <UploadButton onClick={() => fileInput.current?.click()}>{input.has_table ? `Replace ${object}` : `Upload ${object}`}</UploadButton>
        </Stack>
      </Stack>
    </Section>
    <Disclosure title="Import settings" optional summary={`Header row ${header}, ${separatorName}${sheet ? `, sheet ${sheet}` : ''}`}>
      <Stack spacing={2} sx={{ pt: 1 }}>
        <TextField type="number" label="Header row" helperText="Used for the next upload." value={header} inputProps={{ min: 1 }} onChange={(event) => setHeader(Math.max(1, asNumber(event.target.value, 1)))} />
        <SelectField label="CSV separator" value={separator} options={csvSeparators} onChange={(value) => { setSeparator(value); emit({ type: 'update_settings', settings: { [isData ? 'data_csv_separator' : 'mapping_csv_separator']: value || null } }); }} />
        {sheets.length > 0 && <SelectField label="Excel sheet" value={sheet} options={sheets.map((name) => ({ value: name, label: name }))} onChange={(value) => emit({ type: 'update_settings', settings: { [isData ? 'data_sheet' : 'mapping_sheet']: value } })} />}
      </Stack>
    </Disclosure>
    <Disclosure title={`${label} preview`} summary={!input.has_table ? 'No table loaded' : previewRows < rows ? `Showing ${numberFormat.format(previewRows)} of ${plural(rows, 'row')}` : plural(rows, 'row')}>
      <DataTable rows={input.preview} empty="No table loaded yet." />
    </Disclosure>
  </Stack>;
}

function EnrichPage({ snapshot }: { snapshot: Snapshot }) {
  const ready = Boolean(snapshot.mapping?.has_table);
  return <Stack spacing={2}>
    <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap gap={1}>
      <GenerateButton disabled={!ready} onClick={() => emit({ type: 'enrich_uris' })}>Run URI enrichment</GenerateButton>
      <Typography variant="body2" color="text.secondary">{ready ? 'Large tables can take a few minutes.' : 'Load a mapping table first.'}</Typography>
    </Stack>
    <Box>
      <Typography variant="subtitle2" component="h3" sx={{ mb: 1 }}>Mapping table preview</Typography>
      <DataTable rows={snapshot.mapping?.preview} empty="No mapping table loaded yet." />
    </Box>
  </Stack>;
}

function TemplatesPage({ snapshot, onDirty }: { snapshot: Snapshot; onDirty: (dirty: boolean) => void }) {
  const templates = snapshot.templates?.items ?? []; const columns = snapshot.config?.data_columns ?? []; const mapTypes = snapshot.templates?.map_types ?? [];
  const emptyTemplate = { template_name: '', rdf_type: '', properties: [] as TemplateProperty[] };
  const emptyProperty = (): TemplateProperty => ({ predicate: '', map_type: mapTypes[0] ?? 'Column Value (Literal)', value: '' });
  const [editingIndex, setEditingIndex] = useState<number | null>(null); const [draft, setDraft] = useState<SchemaTemplate>(emptyTemplate); const [jsonText, setJsonText] = useState(''); const [prop, setProp] = useState<TemplateProperty>(emptyProperty);
  let jsonChanged = Boolean(jsonText.trim());
  try { if (jsonText.trim()) jsonChanged = JSON.stringify(JSON.parse(jsonText)) !== JSON.stringify(templates); } catch { /* Keep invalid JSON as an unapplied draft. */ }
  const editorDirty = Boolean(JSON.stringify(draft) !== JSON.stringify(editingIndex === null ? emptyTemplate : templates[editingIndex]) || prop.predicate || prop.value);
  const isDirty = jsonChanged || editorDirty;
  useEffect(() => { onDirty(isDirty); return () => onDirty(false); }, [isDirty, onDirty]);
  useEffect(() => { setDraft(emptyTemplate); setJsonText(''); setEditingIndex(null); setProp(emptyProperty()); }, [snapshot.templates?.json]);
  // Back to an empty "New template" form; clears only the template editor, not the JSON draft.
  function resetEditor() { setEditingIndex(null); setDraft(emptyTemplate); setProp(emptyProperty()); }
  const templateNames = templates.map((t) => t.template_name || '').filter(Boolean);
  // The template picker lives here, next to the templates it applies (immediate update_settings).
  const activeTemplate = snapshot.config?.active_template ?? NO_TEMPLATE;
  const [applied, setApplied] = useState(activeTemplate); useEffect(() => setApplied(activeTemplate), [activeTemplate]);
  const fileInput = useRef<HTMLInputElement>(null);
  function startEdit(idx: number) { setEditingIndex(idx); setDraft(JSON.parse(JSON.stringify(templates[idx] ?? emptyTemplate))); }
  function addProperty() { if (!prop.predicate || !prop.map_type || !prop.value) return; setDraft({ ...draft, properties: [...(draft.properties ?? []), { ...prop, id: `${Date.now()}` }] }); setProp(emptyProperty()); }
  const properties = draft.properties ?? [];
  // pt clears the floating label of the first outlined field from the Disclosure summary.
  return <Stack spacing={3} sx={{ pt: 1 }}>
    <Stack direction="row" alignItems="center" spacing={0.5}>
      <SelectField label="Template to apply" value={applied} options={[{ value: NO_TEMPLATE, label: 'None (default column mapping)' }, ...templateNames.map((name) => ({ value: name, label: name }))]} onChange={(value) => { setApplied(value); emit({ type: 'update_settings', settings: { active_template: value } }); }} />
      <InfoButton title="Schema templates"><p>{snapshot.templates?.guidance || 'Templates define explicit RDF types and predicate rules, including nested templates.'}</p><p>Example: an Observation template with fixed SOSA predicates and a nested QUDT Result template for numeric values and units.</p></InfoButton>
    </Stack>
    <Box>
      <Typography variant="subtitle2" component="h3" sx={{ mb: 0.5 }}>Saved templates</Typography>
      {templates.length ? <Stack divider={<Divider />}>{templates.map((template, idx) => {
        const name = template.template_name || 'Untitled';
        const count = template.properties?.length ?? 0;
        return <Stack key={`${template.template_name}-${idx}`} direction={{ xs: 'column', sm: 'row' }} alignItems={{ xs: 'flex-start', sm: 'center' }} spacing={1} sx={{ py: 1 }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="body2" sx={{ fontWeight: 600, overflowWrap: 'anywhere' }}>{name}</Typography>
            <Typography variant="caption" component="code" color="text.secondary" sx={{ display: 'block', fontFamily: fonts.mono, overflowWrap: 'anywhere' }}>{template.rdf_type || '—'}</Typography>
          </Box>
          <Chip size="small" variant="outlined" label={plural(count, 'property', 'properties')} sx={{ fontVariantNumeric: 'tabular-nums' }} />
          <Stack direction="row" spacing={1}>
            <Button size="small" variant="outlined" onClick={() => startEdit(idx)}>Edit</Button>
            <ConfirmActionButton size="small" color="warning" variant="outlined" dialogTitle={`Delete template “${name}”?`} dialogMessage={`This removes the schema template “${name}” and its ${plural(count, 'property', 'properties')}. You can undo this from History.`} confirmLabel="Delete template" onConfirm={() => emit({ type: 'templates', action: 'delete', index: idx })}>Delete</ConfirmActionButton>
          </Stack>
        </Stack>;
      })}</Stack> : <Typography variant="body2" color="text.secondary">No templates saved yet.</Typography>}
    </Box>
    <Box>
      <Stack direction="row" alignItems="center" spacing={0.5} sx={{ mb: 1 }}>
        <Typography variant="subtitle2" component="h3">Import and export</Typography>
        <InfoButton title="Template files">Template files are JSON arrays of objects with template_name, rdf_type and properties. Property IDs are generated automatically. Applying JSON replaces all saved templates.</InfoButton>
      </Stack>
      <Stack spacing={1.5}>
        <Stack direction="row" useFlexGap flexWrap="wrap" gap={1}>
          <input ref={fileInput} hidden type="file" accept=".json,application/json" onChange={async (e) => { const file = e.target.files?.[0]; e.target.value = ''; if (file) setJsonText(await file.text()); }} />
          <UploadButton onClick={() => fileInput.current?.click()}>Upload templates (JSON)</UploadButton>
          <DownloadButton disabled={!templates.length} onClick={() => triggerDownload(snapshot.templates?.json ?? '[]', 'schema_templates.json', 'application/json')}>Download templates (JSON)</DownloadButton>
        </Stack>
        <TextField label="Template JSON" multiline minRows={4} value={jsonText} onChange={(e) => setJsonText(e.target.value)} InputProps={{ sx: { fontFamily: fonts.mono, fontSize: '0.8125rem' } }} />
        <Box><Button variant="outlined" disabled={!jsonText.trim()} onClick={() => emit({ type: 'templates', action: 'load_json', content: jsonText })}>Apply JSON</Button></Box>
      </Stack>
    </Box>
    <Box>
      <Stack direction="row" alignItems="center" justifyContent="space-between" spacing={1} sx={{ mb: 1.5 }}>
        <Typography variant="subtitle2" component="h3" sx={{ overflowWrap: 'anywhere' }}>{editingIndex === null ? 'New template' : `Edit “${templates[editingIndex]?.template_name || 'Untitled'}”`}</Typography>
        {editingIndex !== null
          ? <Button size="small" onClick={resetEditor}>New template</Button>
          : editorDirty && <Button size="small" onClick={resetEditor}>Clear</Button>}
      </Stack>
      <Stack spacing={2}>
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'repeat(2, minmax(0, 1fr))' }, gap: 2 }}>
          <TextField label="Template name" helperText="Must be unique." value={draft.template_name ?? ''} onChange={(e) => setDraft({ ...draft, template_name: e.target.value })} />
          <TextField label="RDF type URI" value={draft.rdf_type ?? ''} onChange={(e) => setDraft({ ...draft, rdf_type: e.target.value })} />
        </Box>
        {properties.length > 0 && <Stack divider={<Divider />}>{properties.map((p, idx) => <Stack key={p.id ?? idx} direction="row" alignItems="center" spacing={1} sx={{ py: 0.75 }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="body2" component="code" sx={{ display: 'block', fontFamily: fonts.mono, fontSize: '0.8125rem', overflowWrap: 'anywhere' }}>{p.predicate}</Typography>
            <Stack direction="row" alignItems="center" spacing={1} sx={{ mt: 0.5, minWidth: 0 }}>
              <Chip size="small" variant="outlined" label={mapTypeLabel(p.map_type)} />
              <Typography variant="caption" component="code" color="text.secondary" sx={{ fontFamily: fonts.mono, overflowWrap: 'anywhere', minWidth: 0 }}>{p.value}</Typography>
            </Stack>
          </Box>
          <Tooltip title="Remove property"><IconButton size="small" aria-label={`Remove property ${p.predicate ?? ''}`} onClick={() => setDraft({ ...draft, properties: properties.filter((_, i) => i !== idx) })}><DeleteOutlineIcon fontSize="small" /></IconButton></Tooltip>
        </Stack>)}</Stack>}
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: '1.2fr .8fr 1fr auto' }, gap: 1.5, alignItems: 'start' }}>
          <TextField label="Predicate URI" value={prop.predicate ?? ''} onChange={(e) => setProp({ ...prop, predicate: e.target.value })} />
          <SelectField label="Mapping type" value={prop.map_type ?? ''} options={mapTypes.map((type) => ({ value: type, label: mapTypeLabel(type) }))} onChange={(value) => setProp({ ...prop, map_type: value, value: '' })} />
          {prop.map_type?.startsWith('Column')
            ? <SelectField label="Data column" value={prop.value ?? ''} options={columns.map((column) => ({ value: column, label: column }))} onChange={(value) => setProp({ ...prop, value })} />
            : prop.map_type === 'Nested Template'
              ? <SelectField label="Template to nest" value={prop.value ?? ''} options={templateNames.filter((name) => name !== draft.template_name).map((name) => ({ value: name, label: name }))} onChange={(value) => setProp({ ...prop, value })} />
              : <TextField label="Fixed URI value" value={prop.value ?? ''} onChange={(e) => setProp({ ...prop, value: e.target.value })} />}
          <Button variant="outlined" disabled={!prop.predicate || !prop.map_type || !prop.value} onClick={addProperty}>Add property</Button>
        </Box>
        <Box><Button variant="outlined" onClick={() => emit({ type: 'templates', action: editingIndex === null ? 'add' : 'update', index: editingIndex, template: draft })}>Save template</Button></Box>
      </Stack>
    </Box>
  </Stack>;
}

// Common classes for the subjects of risk-assessment data (SOSA, W3C Semantic Sensor Network).
const CLASS_SUGGESTIONS = [
  { label: 'sosa:Sample', uri: 'http://www.w3.org/ns/sosa/Sample', hint: 'Each row is a sample, e.g. a food, feed or swab sample.' },
  { label: 'sosa:Observation', uri: 'http://www.w3.org/ns/sosa/Observation', hint: 'Each row is an observation, e.g. a measurement or test result.' },
  { label: 'sosa:FeatureOfInterest', uri: 'http://www.w3.org/ns/sosa/FeatureOfInterest', hint: 'Each row is the thing observed, e.g. a farm, batch or animal.' },
];

// One block of the Custom options panel: heading and explanation, then its controls.
function OptionBlock({ title, description, children }: { title: string; description: React.ReactNode; children: React.ReactNode }) {
  return <Stack spacing={1.5}>
    <Box>
      <Typography variant="subtitle2" component="h3">{title}</Typography>
      <Typography variant="body2" color="text.secondary">{description}</Typography>
    </Box>
    {children}
  </Stack>;
}

function ModelPage({ snapshot, children, activeExtras = [], openOptions = false }: { snapshot: Snapshot; children?: React.ReactNode; activeExtras?: string[]; openOptions?: boolean }) {
  const scope = useContext(CommitScope);
  const cfg = snapshot.config ?? {}; const columns = cfg.data_columns ?? []; const groupSuggestions = Object.entries(cfg.group_suggestions ?? {});
  const [local, setLocal] = useState(cfg); useEffect(() => setLocal(cfg), [snapshot.config]);
  // Same update_settings payloads as before; text fields call this once per edit.
  const patch = (settings: Record<string, unknown>) => { setLocal((previous) => ({ ...previous, ...settings })); emit({ type: 'update_settings', settings }); scope?.remember(document.activeElement); };
  const groupConfig = local.group_config ?? {};
  const ownId = local.id_option !== GENERATED_IDS;
  const linking = Boolean(local.use_shared_id);
  // A template with its own rdf:type replaces the subject class (rdf_processor); say so.
  const appliedTemplate = (snapshot.templates?.items ?? []).find((template) => template.template_name === local.active_template);
  const templateType = appliedTemplate?.rdf_type?.trim() || '';
  const active = [ownId && !linking && 'own ID column', linking && 'cross-file linking', Boolean(local.graph_base_uri && local.graph_base_uri !== cfg.graph_base_uri_default) && 'custom base URI', Boolean(local.group_active) && 'grouping', ...activeExtras].filter((name): name is string => Boolean(name));
  const subjectIds = linking ? `From column “${local.shared_id_col || '…'}”, linked across files` : ownId ? `From column “${local.default_id_col || '…'}”` : 'Generated: id_1, id_2, … (row numbers)';
  return <Stack spacing={3}>
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', lg: 'repeat(2, minmax(0, 1fr))' }, gap: 3, alignItems: 'start' }}>
      <Section title="Subjects" info={{ title: 'Subjects', content: 'Every row of the data table becomes one RDF subject. By default the generator numbers the rows and builds each subject URI from the named graph, for example …/subjects/id_1. Other ID sources are under Subject ID options.' }}>
        <Stack spacing={2}>
          <MetaList items={[{ label: 'Subject IDs', value: subjectIds }]} />
          <Box>
            <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap gap={1}>
              <SwitchField label="Subject class (rdf:type)" checked={Boolean(local.use_class)} onChange={(checked) => patch({ use_class: checked })} />
              {!local.use_class && <Chip size="small" color="primary" variant="outlined" label="Recommended" />}
            </Stack>
            <Typography variant="body2" color="text.secondary">States what kind of thing each row is, such as a sample or an observation: every row gets rdf:type with this class. That makes the data queryable by type, and it is what validation (SHACL) shapes attach to.</Typography>
            <Collapse in={Boolean(local.use_class)}><Stack spacing={1.5} sx={{ pt: 1.5 }}>
              <CommitTextField fullWidth label="Class URI" placeholder="http://www.w3.org/ns/sosa/Sample" value={local.class_uri ?? ''} onCommit={(value) => patch({ class_uri: value })} />
              <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap gap={1}>
                <Typography variant="caption" color="text.secondary">Common choices:</Typography>
                {CLASS_SUGGESTIONS.map((option) => <Tooltip key={option.uri} title={<>{option.hint}<br />{option.uri}</>}><Chip size="small" clickable label={option.label} variant={local.class_uri === option.uri ? 'filled' : 'outlined'} color={local.class_uri === option.uri ? 'primary' : 'default'} onClick={() => patch({ class_uri: option.uri })} /></Tooltip>)}
              </Stack>
              {templateType && <Alert severity="info">The applied schema template “{local.active_template}” sets its own type ({templateType}), which replaces this class.</Alert>}
            </Stack></Collapse>
          </Box>
        </Stack>
      </Section>
      <Section title="Named graph" info={{ title: 'Named graph', content: <><p>All triples of this dataset go into one named graph. Its URI is generated from the base URI and the data file name, so the same file always gets the same graph. The DCAT catalog records it.</p><p>The data graph download is Turtle, which holds the graph contents without the graph name.</p></> }}>
        <Stack spacing={2}>
          <MetaList items={[{ label: 'Graph URI', value: cfg.named_graph_uri, mono: true }]} />
          <Stack direction="row" alignItems="center" spacing={0.5}>
            <SelectField label="DCAT distribution format" value={local.rdf_format ?? 'Turtle'} options={formatOptions} onChange={(value) => patch({ rdf_format: value })} />
            <InfoButton title="DCAT distribution format">This sets the format the DCAT catalog gives for the data distribution. It is applied when you generate the RDF. The data graph and SKOS downloads are always Turtle.</InfoButton>
          </Stack>
        </Stack>
      </Section>
    </Box>
    <Disclosure title="Custom options" optional defaultExpanded={openOptions || active.length > 0} summary={active.length ? `Active: ${joinNames(active)}` : 'None active, defaults are used'}>
      <Stack spacing={3} divider={<Divider />} sx={{ pt: 1 }}>
        <OptionBlock title="Own ID column" description="By default every row gets a generated number. Use a column instead when each row already has a unique identifier, such as a sample number; the subject URIs then contain that value. Values must be unique, otherwise rows merge into one subject.">
          <SwitchField label="Use a column of the data as ID" checked={ownId} onChange={(checked) => patch({ id_option: checked ? 'Existing ID Column' : GENERATED_IDS })} />
          <Collapse in={ownId}><SelectField label="ID column" value={local.default_id_col ?? ''} options={columnOptions(columns)} onChange={(value) => patch({ default_id_col: value })} /></Collapse>
        </OptionBlock>
        <OptionBlock title="Cross-file linking" description={<>Normally a subject URI belongs to one file (…/subjects/id_1). With linking, the URI is built from a key column and a base URI you choose, e.g. https://example.org/sample/S1, so rows with the same key in different files become the same subject in the knowledge graph. Use the same base URI in every file that should link; keys must be unique within a file.</>}>
          <SwitchField label="Link subjects across files" checked={linking} onChange={(checked) => patch({ use_shared_id: checked })} />
          <Collapse in={linking}><Stack spacing={2}>
            <SelectField label="Shared key column" value={local.shared_id_col ?? ''} options={columnOptions(columns)} onChange={(value) => patch({ shared_id_col: value })} />
            <CommitTextField fullWidth label="Subject base URI" placeholder="https://example.org/sample/" value={local.subject_base_uri ?? ''} onCommit={(value) => patch({ subject_base_uri: value })} />
          </Stack></Collapse>
        </OptionBlock>
        <OptionBlock title="Named graph base URI" description="The named graph URI is generated from this base and the data file name. Change it to publish under your own domain.">
          <CommitTextField fullWidth label="Base URI" placeholder="https://example.org/" value={local.graph_base_uri ?? ''} onCommit={(value) => patch({ graph_base_uri: value })} />
        </OptionBlock>
        <OptionBlock title="Grouping" description="Bundles related columns, such as a value and its unit, into a nested node linked to the subject by a connecting predicate (optionally typed with rdf:type).">
          <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap gap={1.5}>
            <SwitchField label="Enable grouping" checked={Boolean(local.group_active)} onChange={(checked) => patch({ group_active: checked })} />
            <SearchActionButton onClick={() => emit({ type: 'detect_groups' })}>Detect groups</SearchActionButton>
          </Stack>
          <Collapse in={Boolean(local.group_active)}><Box>
            {groupSuggestions.length ? <Stack divider={<Divider />} spacing={2}>{groupSuggestions.map(([key, group]) => {
              const name = group.display || key;
              const current = groupConfig[name] ?? { columns: group.columns ?? [] };
              return <Stack key={key} spacing={1.5}>
                <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap gap={1}>
                  <Typography variant="body2" component="h4" sx={{ fontWeight: 600 }}>{group.display}</Typography>
                  {(group.columns ?? []).map((column) => <Chip key={column} size="small" variant="outlined" label={column} />)}
                </Stack>
                <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'repeat(2, minmax(0, 1fr))' }, gap: 2 }}>
                  <CommitTextField fullWidth label="Connecting predicate URI" value={current.connecting_predicate ?? ''} onCommit={(value) => patch({ group_config: { ...groupConfig, [name]: { ...current, columns: group.columns, connecting_predicate: value } } })} />
                  <CommitTextField fullWidth label="Group rdf:type URI (optional)" value={current.group_type ?? ''} onCommit={(value) => patch({ group_config: { ...groupConfig, [name]: { ...current, columns: group.columns, group_type: value || null } } })} />
                </Box>
              </Stack>;
            })}</Stack> : <Typography variant="body2" color="text.secondary">No column groups detected yet. Use Detect groups to find them.</Typography>}
          </Box></Collapse>
        </OptionBlock>
        {children}
      </Stack>
    </Disclosure>
  </Stack>;
}

function ReferencePage({ snapshot }: { snapshot: Snapshot }) {
  const scope = useContext(CommitScope);
  const ref = snapshot.reference ?? {};
  const [method, setMethod] = useState(ref.method ?? NO_REFERENCE);
  useEffect(() => setMethod(ref.method ?? NO_REFERENCE), [ref.method]);
  const [doi, setDoi] = useState(ref.doi ?? '');
  const choose = (next: string | null) => { if (!next || next === method) return; setMethod(next); emit({ type: 'set_reference_method', method: next }); };
  return <Stack spacing={2}>
    {/* All three ways are visible at once, so it is clear that a DOI is not required. */}
    <ToggleButtonGroup exclusive size="small" color="primary" value={method} onChange={(_event, next: string | null) => choose(next)} aria-label="How to add the publication" sx={{ flexWrap: 'wrap' }}>
      {referenceMethods.map((option) => <ToggleButton key={option.value} value={option.value} sx={{ textTransform: 'none', px: 2 }}>{option.label}</ToggleButton>)}
    </ToggleButtonGroup>
    {method === NO_REFERENCE && <Typography variant="body2" color="text.secondary">No publication is attached. Add one at any time: look it up by DOI, or enter title, authors and journal by hand if it has no DOI.</Typography>}
    {method === 'DOI' && <Stack spacing={1}>
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.5} alignItems={{ md: 'flex-start' }}>
        <CommitTextField fullWidth label="DOI" placeholder="10.1038/nature12373" value={ref.doi ?? ''} onDraftChange={setDoi} onCommit={(value) => emit({ type: 'update_settings', settings: { doi: value } })} helperText="Title, authors and journal are fetched from Crossref and OpenAlex." />
        {/* A real lookup at Crossref/OpenAlex, so it stays an explicit action. */}
        <Button variant="outlined" disabled={!doi.trim()} sx={{ flexShrink: 0 }} onClick={() => { scope?.flush(); emit({ type: 'generate_reference', method: 'DOI', doi }); }}>Fetch publication metadata</Button>
      </Stack>
      <Typography variant="body2" color="text.secondary">No DOI? <Button size="small" sx={{ textTransform: 'none', p: 0, minWidth: 0, verticalAlign: 'baseline' }} onClick={() => choose('Manual Entry')}>Enter the publication manually</Button>.</Typography>
    </Stack>}
    {method === 'Manual Entry' && <ManualReference snapshot={snapshot} />}
    {ref.preview && method !== NO_REFERENCE && <Box>
      <Typography variant="subtitle2" component="h3" sx={{ mb: 1 }}>Reference preview</Typography>
      <CodePreview value={ref.preview} full={snapshot.outputs?.reference_rdf} />
    </Box>}
  </Stack>;
}

// Manual entry saves field by field, like the dataset metadata. The reference exists once title,
// journal and authors are filled in, and follows every later edit.
const MANUAL_REQUIRED: ManualField[] = ['title', 'journal', 'authors'];
function ManualReference({ snapshot }: { snapshot: Snapshot }) {
  const scope = useContext(CommitScope);
  const ref = snapshot.reference ?? {};
  const saved = ref.manual ?? {};
  const [touched, setTouched] = useState<Partial<Record<ManualField, boolean>>>({});
  const [drafts, setDrafts] = useState<Partial<Record<ManualField, string>>>({});
  const value = (key: ManualField) => drafts[key] ?? saved[key] ?? '';
  const field = (key: ManualField, label: string, extra: { required?: boolean; multiline?: boolean; minRows?: number; placeholder?: string; helperText?: string } = {}) => {
    const empty = Boolean(extra.required) && !value(key).trim();
    return <Box onBlur={() => setTouched((previous) => ({ ...previous, [key]: true }))} sx={{ minWidth: 0 }}>
      <CommitTextField fullWidth label={label} {...extra} value={saved[key] ?? ''} error={empty && Boolean(touched[key])} helperText={empty && touched[key] ? `${label} is required.` : extra.helperText}
        onDraftChange={(next) => setDrafts((previous) => ({ ...previous, [key]: next }))}
        onCommit={(next) => { emit({ type: 'save_manual_reference', fields: { [key]: next } }); scope?.remember(document.activeElement); }} />
    </Box>;
  };
  const missing = MANUAL_REQUIRED.filter((key) => !value(key).trim()).length;
  return <Stack spacing={2}>
    <Typography variant="body2" color="text.secondary">For publications without a DOI, such as reports, theses or grey literature. Fields marked * are required; the reference is saved as soon as they are filled in.</Typography>
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'repeat(2, minmax(0, 1fr))' }, gap: 2 }}>
      {field('title', 'Title', { required: true })}
      {field('journal', 'Journal or publisher', { required: true })}
      {field('authors', 'Authors', { required: true, multiline: true, minRows: 2, placeholder: 'Smith, John; Doe, Alice', helperText: 'Separate authors with semicolons.' })}
      <Stack spacing={2}>
        {field('year', 'Year', { placeholder: String(new Date().getFullYear()) })}
        {field('volume', 'Volume')}
        {field('pages', 'Pages')}
        {field('doi', 'DOI (optional)')}
      </Stack>
    </Box>
    {ref.loaded && ref.method !== 'DOI' && !missing ? <Alert severity="success">Publication reference saved.</Alert> : null}
  </Stack>;
}

function LicenseField({ dcat, onChange }: { dcat: NonNullable<Snapshot['dcat']>; onChange: (value: string) => void }) {
  const options = dcat.license_options ?? [];
  const selected = options.find((option) => option.id === dcat.license);
  // Licences from the DCAT-AP.de vocabulary; the helper shows the URI the catalog will use.
  return <SelectField label="License" value={dcat.license ?? ''} options={options.map((option) => ({ value: option.id, label: option.label }))} onChange={onChange}
    helperText={selected?.uri ? <Box component="span" sx={{ fontFamily: fonts.mono, overflowWrap: 'anywhere' }}>{selected.uri}</Box> : undefined} />;
}

type MetadataField = 'dcat_title' | 'dcat_description' | 'dcat_publisher_name';
// DCAT-AP mandatory: dataset title and description, catalog publisher. Labels match the backend's.
const REQUIRED_METADATA: { key: MetadataField; label: string }[] = [
  { key: 'dcat_title', label: 'dataset title' },
  { key: 'dcat_description', label: 'description' },
  { key: 'dcat_publisher_name', label: 'publisher name' },
];

function MetadataPage({ snapshot, onMissingChange }: { snapshot: Snapshot; onMissingChange: (missing: string[] | null) => void }) {
  const scope = useContext(CommitScope);
  const dcat = snapshot.dcat ?? {};
  const ref = snapshot.reference ?? {};
  // Every field saves on its own, like Configure RDF. Typed but unsaved text already counts, so
  // Continue unlocks while typing (Continue saves pending text before it navigates).
  const saved: Record<MetadataField, string> = { dcat_title: dcat.title ?? '', dcat_description: dcat.description ?? '', dcat_publisher_name: dcat.publisher_name ?? '' };
  const [drafts, setDrafts] = useState(saved);
  const [touched, setTouched] = useState<Partial<Record<MetadataField, boolean>>>({});
  const missing = REQUIRED_METADATA.filter(({ key }) => !drafts[key].trim()).map(({ label }) => label);
  const missingKey = missing.join('|');
  useEffect(() => { onMissingChange(missingKey ? missingKey.split('|') : []); }, [missingKey, onMissingChange]);
  useEffect(() => () => onMissingChange(null), [onMissingChange]);
  const save = (settings: Record<string, unknown>) => { emit({ type: 'save_metadata', settings }); scope?.remember(document.activeElement); };
  const required = (key: MetadataField, label: string, helper: string, extra: { multiline?: boolean; minRows?: number } = {}) => {
    const empty = !drafts[key].trim();
    return <Box onBlur={() => setTouched((previous) => ({ ...previous, [key]: true }))} sx={{ minWidth: 0 }}>
      <CommitTextField fullWidth required label={label} {...extra} value={saved[key]} error={empty && Boolean(touched[key])} helperText={empty && touched[key] ? `${label} is required.` : helper} onDraftChange={(value) => setDrafts((previous) => ({ ...previous, [key]: value }))} onCommit={(value) => save({ [key]: value })} />
    </Box>;
  };
  return <Stack spacing={3}>
    <Section
      title="Dataset metadata (DCAT)"
      description="Fields marked * are required. Changes are saved as you go."
      info={{ title: 'Dataset metadata', content: 'DCAT describes the dataset: title, description, publisher, license, access rights and themes. Title, description and publisher are mandatory in DCAT-AP. The catalog is generated together with the RDF and bundles data, SKOS vocabulary and metadata in one TriG file.' }}
    >
      <Stack spacing={2}>
        {missing.length > 0 && <Alert severity="info">Still needed: {joinNames(missing)}.</Alert>}
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'repeat(2, minmax(0, 1fr))' }, gap: 2 }}>
          {required('dcat_title', 'Dataset title', 'A short, specific name, e.g. "Salmonella in poultry, Germany 2024".')}
          {required('dcat_publisher_name', 'Publisher name', 'The organisation that publishes the dataset.')}
        </Box>
        {required('dcat_description', 'Description', 'What the data contains and how, where and when it was collected.', { multiline: true, minRows: 3 })}
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'repeat(2, minmax(0, 1fr))' }, gap: 2 }}>
          <CommitTextField fullWidth label="Publisher URI" placeholder="https://www.example.org" value={dcat.publisher_uri ?? ''} onCommit={(value) => save({ dcat_publisher_uri: value })} />
          <CommitTextField fullWidth label="Contact point" placeholder="data@example.org" value={dcat.contact_point ?? ''} onCommit={(value) => save({ dcat_contact_point: value })} />
          <LicenseField dcat={dcat} onChange={(value) => save({ dcat_license: value })} />
          <SelectField label="Access rights" value={dcat.access_rights ?? 'PUBLIC'} options={accessRights} onChange={(value) => save({ dcat_access_rights: value })} />
        </Box>
        <TextField select fullWidth label="Themes" value={dcat.themes ?? []} SelectProps={{ multiple: true, renderValue: (selected) => (selected as string[]).join(', ') }} onChange={(e) => { const value = e.target.value as unknown as string | string[]; save({ dcat_themes: typeof value === 'string' ? value.split(',') : value }); }}>
          {(dcat.theme_options ?? []).map((theme) => <MenuItem key={theme} value={theme}><Checkbox size="small" checked={(dcat.themes ?? []).includes(theme)} />{theme}</MenuItem>)}
        </TextField>
      </Stack>
    </Section>
    <Section title="Publication reference" description={ref.loaded ? ref.summary : 'Optional. The publication that describes this dataset, with or without a DOI.'} info={{ title: 'Publication reference', content: 'The publication is exported as its own file (SemOpenAlex RDF). Look it up by DOI, or enter it by hand when it has no DOI. It can be linked from the DCAT catalog.' }}>
      <Stack spacing={2}>
        <ReferencePage snapshot={snapshot} />
        {ref.loaded && <SwitchField label="Link the publication in the DCAT catalog" checked={Boolean(dcat.link_reference ?? true)} onChange={(checked) => save({ dcat_link_reference: checked })} />}
      </Stack>
    </Section>
  </Stack>;
}

function GeneratePage({ snapshot }: { snapshot: Snapshot }) {
  const scope = useContext(CommitScope);
  const dcat = snapshot.dcat ?? {};
  const out = snapshot.outputs ?? {}; const cfg = snapshot.config ?? {};
  const hasRdf = Boolean(out.has_rdf);
  const [showIris, setShowIris] = useState(false);
  const files = outputFiles(out).filter((file) => Boolean(file.value));
  const generatedIds = cfg.id_option === GENERATED_IDS;
  const hasClass = Boolean(cfg.use_class && cfg.class_uri);
  const summary: MetaItem[] = [
    { label: 'Dataset', value: dcat.title },
    { label: 'DCAT catalog', value: out.has_dcat_catalog ? 'Generated' : 'Built with the RDF' },
    { label: 'Generated files', value: files.length ? <Stack direction="row" useFlexGap flexWrap="wrap" gap={0.5}>{files.map((file) => <Chip key={file.label} size="small" variant="outlined" label={file.label} />)}</Stack> : null },
    { label: 'Subject IDs', value: generatedIds ? 'Generated IDs' : cfg.default_id_col, mono: !generatedIds },
    { label: 'Cross-file linking', value: cfg.use_shared_id ? cfg.shared_id_col : 'Off', mono: Boolean(cfg.use_shared_id) },
    { label: 'Subject class', value: hasClass ? cfg.class_uri : 'None', mono: hasClass },
    { label: 'Mapping columns (detected)', value: [cfg.term_col, cfg.uri_col, cfg.match_type_col].filter(Boolean).join(', '), mono: true },
    { label: 'Named graph', value: cfg.named_graph_uri, mono: true },
    { label: 'Schema template', value: !cfg.active_template || cfg.active_template === NO_TEMPLATE ? 'None' : cfg.active_template },
    { label: 'Grouping', value: cfg.group_active ? `On, ${plural(Object.keys(cfg.group_suggestions ?? {}).length, 'group')}` : 'Off' },
    { label: 'Publication reference', value: snapshot.reference?.loaded ? 'Added' : 'None' },
  ];
  return <Stack spacing={3}>
    <KpiStrip ariaLabel="Generation summary" items={[
      { label: 'Data rows', value: snapshot.data?.has_table ? numberFormat.format(snapshot.data.rows ?? 0) : null },
      { label: 'Mapping rows', value: snapshot.mapping?.has_table ? numberFormat.format(snapshot.mapping.rows ?? 0) : null },
      { label: 'Files ready', value: numberFormat.format(files.length) },
    ]} />
    <Section
      title="Data graph, SKOS and DCAT"
      info={{ title: 'Generating', content: 'Generating builds the data graph from your rows, a SKOS vocabulary from the terms the data uses and, with a named graph, the DCAT catalog from the dataset metadata. Regenerating replaces all three.' }}
      actions={<GenerateButton variant={hasRdf ? 'outlined' : 'contained'} disabled={!snapshot.ready?.can_generate} onClick={() => { scope?.flush(); emit({ type: 'generate_rdf' }); }}>{hasRdf ? 'Regenerate RDF' : 'Generate RDF'}</GenerateButton>}
    >
      <MetaList columns={2} items={summary} />
    </Section>
    {hasRdf && out.overview && (out.overview.error
      ? <Alert severity="warning">The result overview could not be built: {out.overview.error}</Alert>
      : <Section
        title="Result overview"
        description="What was generated and how the parts are linked. Each line is checked in the generated RDF."
        info={{ title: 'Result overview', content: <><p>The four parts of the output and the links between them. A solid line with ✓ means the link was found in the generated triples; a dashed line with ⚠ means it is missing; a grey dashed line means that part is not there yet.</p><p>Labels are shown by default; hover a value for its IRI, or switch on Show IRIs.</p></> }}
        actions={<FormControlLabel sx={{ mr: 0 }} control={<Switch size="small" checked={showIris} onChange={(event) => setShowIris(event.target.checked)} />} label="Show IRIs" />}
      ><ResultOverview overview={out.overview} showIris={showIris} /></Section>)}
    <Section title="Data graph preview"><CodePreview value={out.previews?.rdf} full={out.rdf_data} /></Section>
  </Stack>;
}

function ExportPage({ snapshot }: { snapshot: Snapshot }) {
  const out = snapshot.outputs ?? {};
  const stem = out.file_stem ?? 'rdf_output';
  const [tab, setTab] = useState(0);
  const outputs = useMemo(() => outputFiles(snapshot.outputs ?? {}).map((file) => ({ ...file, size: file.value ? new Blob([file.value]).size : 0 })), [snapshot.outputs]);
  const files = outputs.filter((file) => Boolean(file.value));
  const missing = outputs.filter((file) => !file.value);
  // Clamp the selected preview when the set of available files shrinks.
  const index = Math.min(tab, Math.max(0, files.length - 1));
  const current = files[index];
  if (!current) return <EmptyState dense title="No files yet" description="Create the RDF in the Generate RDF step. The data graph and SKOS vocabulary then appear here." />;
  const tabbed = files.length > 1;
  return <Stack spacing={3}>
    <Section title="Files">
      <Stack divider={<Divider />}>
        {files.map((file) => <Stack key={file.label} direction={{ xs: 'column', sm: 'row' }} alignItems={{ xs: 'flex-start', sm: 'center' }} spacing={{ xs: 1, sm: 2 }} sx={{ py: 1.5, '&:first-of-type': { pt: 0 }, '&:last-of-type': { pb: 0 } }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>{file.label}</Typography>
            <Typography variant="caption" component="code" color="text.secondary" sx={{ display: 'block', fontFamily: fonts.mono, overflowWrap: 'anywhere' }}>{stem + file.suffix}</Typography>
          </Box>
          <Typography variant="body2" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums', whiteSpace: 'nowrap' }}>{formatSize(file.size)}</Typography>
          <DownloadButton sx={{ whiteSpace: { xs: 'normal', sm: 'nowrap' } }} onClick={() => triggerDownload(file.value || '', stem + file.suffix, file.mime)}>{`Download ${file.what} (${file.format})`}</DownloadButton>
        </Stack>)}
        {missing.map((file) => <Stack key={file.label} direction={{ xs: 'column', sm: 'row' }} alignItems={{ xs: 'flex-start', sm: 'center' }} spacing={{ xs: 0.5, sm: 2 }} sx={{ py: 1.5, '&:first-of-type': { pt: 0 }, '&:last-of-type': { pb: 0 } }}>
          <Typography variant="body2" color="text.secondary" sx={{ flex: 1, minWidth: 0 }}>{file.label}</Typography>
          <Typography variant="body2" color="text.secondary">Not generated yet ({file.source})</Typography>
        </Stack>)}
      </Stack>
    </Section>
    <Section title={tabbed ? 'Preview' : `${current.label} preview`}>
      {tabbed && <Tabs value={index} onChange={(_, value: number) => setTab(value)} variant="scrollable" scrollButtons="auto" allowScrollButtonsMobile aria-label="Generated files" sx={{ mb: 2 }}>{files.map((file, position) => <Tab key={file.label} label={file.label} id={`rdf-preview-tab-${position}`} aria-controls="rdf-preview-panel" />)}</Tabs>}
      <Box {...(tabbed ? { role: 'tabpanel', id: 'rdf-preview-panel', 'aria-labelledby': `rdf-preview-tab-${index}` } : {})}>
        <CodePreview value={current.preview} full={current.value} />
      </Box>
    </Section>
  </Stack>;
}

export function RDFGeneratorApp({ args, busy = false, onDraftChange }: AppProps) {
  const snapshot = args?.snapshot ?? {};
  const rawStage = snapshot.active_stage ?? 'metadata';
  // Legacy backend stages: enrich/templates open their section on Configure RDF; the publication
  // reference now lives on Describe dataset.
  const activeStage: Stage = rawStage === 'reference' ? 'metadata' : ['enrich', 'templates'].includes(rawStage) ? 'model' : mainStages.some(stage => stage.id === rawStage) ? rawStage : 'metadata';
  const [drafts, setDrafts] = useState({ templates: false });
  // Mandatory metadata still empty on the Describe dataset page, including unsaved typing (null elsewhere).
  const [draftMissing, setDraftMissing] = useState<string[] | null>(null);
  const [editVersion, setEditVersion] = useState(0);
  const hasDraft = Object.values(drafts).some(Boolean);
  const templatesDirty = useCallback((dirty: boolean) => setDrafts(previous => previous.templates === dirty ? previous : { ...previous, templates: dirty }), []);
  useEffect(() => { onDraftChange?.(hasDraft); return () => onDraftChange?.(false); }, [hasDraft, onDraftChange]);
  useEffect(() => { if (!hasDraft) return; const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; }; window.addEventListener('beforeunload', warn); return () => window.removeEventListener('beforeunload', warn); }, [hasDraft]);
  useEffect(() => { notifyLayoutChanged(); }, [snapshot, activeStage]);

  const pressing = useRef(false);
  const commits = useRef(new Set<() => void>());
  const restoreFocus = useRef<HTMLElement | null>(null);
  const commitScope = useMemo<CommitScopeValue>(() => ({
    pressing,
    register: (commit) => { commits.current.add(commit); return () => { commits.current.delete(commit); }; },
    flush: () => commits.current.forEach((commit) => commit()),
    remember: (target) => { if (target instanceof HTMLElement && target !== document.body) restoreFocus.current = target; },
  }), []);
  useEffect(() => {
    const release = () => { pressing.current = false; };
    const types = [...PRESS_END, 'keydown'];
    types.forEach((type) => window.addEventListener(type, release, true));
    return () => types.forEach((type) => window.removeEventListener(type, release, true));
  }, []);
  // While busy the fieldset is disabled + inert and drops focus; give it back afterwards to the
  // control that saved (text field, switch or select).
  useEffect(() => {
    if (busy) return;
    const target = restoreFocus.current;
    restoreFocus.current = null;
    if (!target) return;
    const restore = () => {
      const active = document.activeElement;
      if (target.isConnected && (!active || active === document.body)) target.focus({ preventScroll: true });
    };
    restore();
    // A select's menu can keep focus while it closes; try again once it has gone.
    const retry = window.setTimeout(restore, motion.popover.exit + 50);
    return () => window.clearTimeout(retry);
  }, [busy]);

  const tools = useToolNavigation();
  const discard = () => { commitScope.flush(); setEditVersion(value => value + 1); setDrafts({ templates: false }); };
  const go = (stage: Stage) => { if (busy || hasDraft) return; commitScope.flush(); emit({ type: 'navigate', stage }); };
  // Last stage: hand the generated catalog to the RDF inspector in one step (DESIGN.md §3).
  const continueToInspector = () => { if (busy || hasDraft) return; commitScope.flush(); void tools?.continueWith?.({ type: 'load_catalog' }); };

  const index = Math.max(0, mainStages.findIndex(stage => stage.id === activeStage));
  const meta = mainStages[index];
  const previousStage = mainStages[index - 1];
  const nextStage = mainStages[index + 1];
  const loaded = Boolean(snapshot.data?.has_table && snapshot.mapping?.has_table);
  const canGenerate = Boolean(snapshot.ready?.can_generate);
  const hasRdf = Boolean(snapshot.outputs?.has_rdf);
  // Same condition as the RDF inspector's load_catalog (source.catalog_available).
  const hasCatalog = Boolean(snapshot.outputs?.has_dcat_catalog);
  const handOffCatalog = hasCatalog && Boolean(tools?.continueWith);
  const issues = snapshot.ready?.issues ?? [];
  // gates[k] is what Continue on stage k requires. The stepper reaches a later stage only when
  // Continue could get there step by step; earlier stages are always reachable (as with Back).
  const metadataMissing = draftMissing ?? snapshot.dcat?.missing ?? [];
  const metadataDone = metadataMissing.length === 0;
  const gates = [metadataDone, loaded, canGenerate, hasRdf];
  const blocked = busy || hasDraft;
  const steps: WizardStep[] = mainStages.map((stage, position) => ({ id: stage.id, label: stage.label, completed: gates[position] ?? false, disabled: blocked || (position > index && !gates.slice(index, position).every(Boolean)) }));
  const draftSections = joinNames([drafts.templates && 'Schema templates'].filter((name): name is string => Boolean(name)));
  // Backend issue texts stay in the status alert; the hint keeps the fixed patterns.
  const stageHint = activeStage === 'metadata'
    ? metadataDone ? 'Next: load your data and mapping tables.' : `Enter the ${joinNames(metadataMissing)} to continue.`
    : activeStage === 'load'
    ? loaded ? 'Next: configure subject IDs, mapping columns and the named graph.' : 'Load a data table and a mapping table to continue.'
    : activeStage === 'model'
      ? canGenerate ? 'Next: generate the RDF and check the result.' : 'Complete the RDF settings to continue.'
      : activeStage === 'generate'
        ? hasRdf ? 'Next: download the generated files.' : 'Generate the RDF to continue.'
        : !hasRdf ? 'Create the RDF in the Generate RDF step first.'
          : hasCatalog ? 'Next: open the generated catalog in the RDF inspector.'
            : 'Next: open the RDF inspector. Complete Describe dataset and regenerate to load the DCAT catalog there.';
  const hint = busy ? 'Saving changes…' : hasDraft ? `Apply or discard the edits in ${draftSections} to continue.` : stageHint;

  const templates = snapshot.templates?.items ?? [];
  const activeTemplate = snapshot.config?.active_template;
  const enriched = ENRICHMENT_COLUMNS.every((column) => snapshot.mapping?.column_names?.includes(column));
  let page: React.ReactNode;
  if (activeStage === 'metadata') page = <MetadataPage key={'metadata-' + editVersion} snapshot={snapshot} onMissingChange={setDraftMissing} />;
  else if (activeStage === 'load') page = <LoadPage snapshot={snapshot} />;
  else if (activeStage === 'model') page = <ModelPage snapshot={snapshot} openOptions={rawStage === 'enrich' || rawStage === 'templates' || drafts.templates}
    activeExtras={[enriched && 'URI enrichment', Boolean(activeTemplate && activeTemplate !== NO_TEMPLATE) && `template “${activeTemplate}”`].filter((name): name is string => Boolean(name))}>
    <OptionBlock title="URI enrichment" description={!snapshot.mapping?.has_table ? 'Looks up every mapped URI in BioPortal, OLS and Ontobee and adds readable labels, sources and links to the mapping table. Needs a mapping table.' : enriched ? 'Labels, sources and links have been added to the mapping table. Run it again after changing the mappings.' : 'Looks up every mapped URI in BioPortal, OLS and Ontobee and adds readable labels, sources and links to the mapping table.'}><EnrichPage snapshot={snapshot} /></OptionBlock>
    <OptionBlock title="Schema templates" description={`Explicit rules for the RDF structure: an rdf:type and predicates per subject, including nested nodes (for example SOSA/QUDT observations). ${!templates.length ? 'None saved yet.' : `${plural(templates.length, 'template')} saved, ${!activeTemplate || activeTemplate === NO_TEMPLATE ? 'none applied' : `applying “${activeTemplate}”`}.`}`}><TemplatesPage key={'templates-' + editVersion} snapshot={snapshot} onDirty={templatesDirty} /></OptionBlock>
  </ModelPage>;
  else if (activeStage === 'generate') page = <GeneratePage snapshot={snapshot} />;
  else page = <ExportPage snapshot={snapshot} />;

  return <CommitScope.Provider value={commitScope}>
    <Box onPointerDownCapture={() => { pressing.current = true; }}>
      <WizardPage
        steps={steps}
        activeIndex={index}
        onStepClick={(_, step) => go(step.id as Stage)}
        title={meta.title}
        lead={meta.lead}
        info={meta.info}
        actions={<ToolMenu items={[{ label: 'Reset RDF generator', icon: <RestartAltIcon fontSize="small" />, disabled: busy, onSelect: () => emit({ type: 'reset_workflow' }), confirm: { title: 'Reset RDF generator?', message: 'This clears the loaded tables, the publication reference and all generated files. Dataset metadata, schema templates and RDF settings are kept. You can undo this from History.', confirmLabel: 'Reset RDF generator' } }]} />}
        status={<>
          {hasDraft && <Alert severity="info" action={<Button color="inherit" size="small" disabled={busy} onClick={discard}>Discard edits</Button>}>Unapplied edits in {draftSections}. Apply them there, or discard them to continue.</Alert>}
          {snapshot.statusMessage?.text && <Alert severity={snapshot.statusMessage.severity ?? 'info'}>{snapshot.statusMessage.text}</Alert>}
          {(activeStage === 'model' || activeStage === 'generate') && issues.length > 0 && <Alert severity="warning"><AlertTitle>Before you can generate RDF</AlertTitle><Box component="ul" sx={{ m: 0, pl: 2.5 }}>{issues.map((issue) => <li key={issue}>{issue}</li>)}</Box></Alert>}
        </>}
        nav={{
          back: previousStage ? { onClick: () => go(previousStage.id), disabled: blocked, tooltip: `Back to ${previousStage.label}` } : { disabled: blocked },
          // Without a catalog, Continue just opens the RDF inspector (the shell's next tool).
          next: nextStage
            ? { onClick: () => go(nextStage.id), disabled: blocked || !gates[index], tooltip: `Continue to ${nextStage.label}` }
            : handOffCatalog ? { onClick: continueToInspector, disabled: blocked, tooltip: 'Continue to RDF inspector' } : { disabled: blocked },
          hint,
        }}
      >
        <Box component="fieldset" disabled={busy} {...{ inert: busy ? '' : undefined }} aria-label="RDF generation settings" aria-busy={busy} sx={{ border: 0, p: 0, m: 0, minWidth: 0 }}>{page}</Box>
      </WizardPage>
    </Box>
  </CommitScope.Provider>;
}
