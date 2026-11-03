import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  ButtonBase,
  Checkbox,
  Chip,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Drawer,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  LinearProgress,
  MenuItem,
  Paper,
  Select,
  Stack,
  Switch,
  Tab,
  Tabs,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import CloseIcon from '@mui/icons-material/Close';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import { DataTable } from '../../components/table/DataTable';
import { AgentRunProgressPanel, type RunStatus } from '../../components/run/AgentRunProgressPanel';
import { ToolMenu } from '../../components/actions/ToolMenu';
import { WizardPage, type WizardStep } from '../../components/layout/WizardPage';
import { Section } from '../../components/layout/Section';
import { Disclosure } from '../../components/layout/Disclosure';
import { MetaList, type MetaItem } from '../../components/layout/MetaList';
import { KpiStrip } from '../../components/layout/KpiStrip';
import { InfoButton, type InfoContent } from '../../components/help/InfoButton';
import { ConfirmActionButton, ConfirmDialog } from '../../components/dialogs/ConfirmActionButton';
import { DownloadButton, GenerateButton, ResetButton, UploadButton, UseResultButton } from '../../components/actions/WorkflowActionButtons';
import { EmptyState } from '../../components/states/EmptyState';
import { fonts } from '../../theme/tokens';
import { CommitTextField } from './CommitTextField';
import { ReviewGrid } from './ReviewGrid';
import { AgentOptionsDialog } from './AgentOptionsDialog';
import { ReviewProvenancePanel } from './ProvenancePanel';
import { LineageStrip } from './LineageStrip';
import { ComparisonRow } from './ComparisonRow';
import type {
  AgentReconciliationAppProps,
  AppEvent,
  DataStatus,
  ExportPayload,
  ReadinessCheck,
  ReadinessState,
  ReviewItem,
  ReviewState,
  Stage,
  StopEvent,
  Telemetry,
  WorkflowConfig,
} from './types';
import {
  formatCost,
  formatCount,
  formatReviewMode,
  formatScore,
  formatSeconds,
  normalizeConfig,
  normalizeStage,
  skosColor,
  splitCsv,
  stages,
  statusColor,
  statusLabel,
  unique,
  workflows,
} from './utils';

async function fileToBase64(file: File) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = '';
  bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
  return window.btoa(binary);
}

function downloadFile(content: string, type: string, filename: string) {
  const url = window.URL.createObjectURL(new Blob([content], { type }));
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.setTimeout(() => window.URL.revokeObjectURL(url), 0);
}

const READ_ERROR = 'This file could not be read. Choose it again or use another file.';
const SSSOM_FILENAME = 'agent_reconciliation_sssom_export.csv';
const SSSOM_MIME = 'text/csv;charset=utf-8';

// What fixes each failing readiness check (keys are the backend's), for the Setup hint.
const SETUP_FIXES: Record<string, string> = {
  matching_table: 'Load a matching table',
  required_columns: 'Load a table with the required columns',
  provider: 'Configure the LLM provider in Options',
  models: 'Choose an available model in Options',
  workflow: 'Choose a matching strategy',
};

const monoSx = { fontFamily: fonts.mono, fontSize: '0.8125rem' } as const;
const codeSx = { m: 0, p: 1, maxHeight: 280, overflow: 'auto', fontFamily: fonts.mono, fontSize: '0.75rem', lineHeight: 1.6, bgcolor: 'rdf.subtle', border: 1, borderColor: 'divider', borderRadius: 1.5, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' } as const;
const OPEN_STATUSES = ['pending', 'matched', 'candidate_suggested'] as const;

// One instruction line per stage; the how-it-works copy sits behind the stage ⓘ.
const STAGE_COPY: Record<Stage, { title: string; lead: string; info: InfoContent }> = {
  setup: {
    title: 'Prepare your agent-assisted reconciliation',
    lead: 'Check your matching table and choose how the agents should find matches.',
    info: { title: 'Setting up a run', content: <><p>Load the matching table, then choose a strategy. The agents search the chosen sources, rank candidates and propose a SKOS match type for every term.</p><p>Provider, model, keys, monitoring and provenance are in Options. Changes save automatically.</p></> },
  },
  run: {
    title: 'Run and monitor reconciliation',
    lead: 'Start the agents and follow their progress; you can stop at any time.',
    info: { title: 'Running the agents', content: <><p>The agents process the terms one by one. Finished terms stay available for review when you stop.</p><ul><li><strong>Resume run</strong> continues with the next term.</li><li><strong>Skip failed term and resume</strong> passes over a term that hit an LLM or runtime error.</li><li><strong>Restart from the beginning</strong> processes every term again.</li></ul></> },
  },
  review: {
    title: 'Review the suggested mappings',
    lead: 'Check the evidence, adjust match types, and accept or reject each suggestion.',
    info: { title: 'Reviewing suggestions', content: <><p>Nothing is used without your decision. Accept, reject or confirm a no-match for each suggestion, and adjust the SKOS match type before accepting if needed.</p><p>Decisions are saved to this workspace. Undo in the notification reverts the last action.</p></> },
  },
  export: {
    title: 'Export your reviewed mappings',
    lead: 'Download accepted mappings as SSSOM, or send them to the RDF generator.',
    info: { title: 'Exporting mappings', content: <><p>The SSSOM export downloads your accepted mappings as a CSV.</p><p>Continue sends the current mapping table to the RDF generator, where you build RDF from it. Rejected and no-match rows stay in this workspace for audit.</p></> },
  },
};

// A field with its explanation in an ⓘ beside it (not inside the floating label, where a button
// would compete with the input for clicks).
function WithInfo({ title, info, children }: { title: string; info: React.ReactNode; children: React.ReactNode }) {
  return (
    <Stack direction="row" spacing={0.5} alignItems="center" sx={{ minWidth: 0 }}>
      <Box sx={{ flex: 1, minWidth: 0 }}>{children}</Box>
      <InfoButton title={title}>{info}</InfoButton>
    </Stack>
  );
}

function tableSummaryItems(dataStatus: DataStatus): MetaItem[] {
  return [
    { label: 'File', value: dataStatus.filename || dataStatus.source_name, mono: true },
    { label: 'Rows', value: formatCount(dataStatus.rows ?? 0), numeric: true },
    ...((dataStatus.reconciled_rows ?? 0) > 0 ? [{ label: 'To reconcile', value: `${formatCount(dataStatus.unreconciled_rows ?? 0)} of ${formatCount(dataStatus.rows ?? 0)} (${formatCount(dataStatus.reconciled_rows)} already reconciled)`, numeric: true }] : []),
    { label: 'Columns', value: formatCount(dataStatus.columns ?? 0), numeric: true },
    { label: 'Schema', value: dataStatus.schema_message || 'No schema detected' },
  ];
}

type TableSource = 'upload' | 'prepared';

// `continueAvailable`: Continue can already move on, so it stays the view's one contained button.
function MatchingTableSection({ dataStatus, hasResults, continueAvailable, emit }: { dataStatus: DataStatus; hasResults: boolean; continueAvailable: boolean; emit: (event: AppEvent) => void }) {
  const [uploadError, setUploadError] = useState('');
  const [replacing, setReplacing] = useState<TableSource | null>(null);
  const csvInput = useRef<HTMLInputElement>(null);
  const loaded = Boolean(dataStatus.has_table);
  const uploadDisabled = dataStatus.upload_bridge_available === false;
  const preparedAvailable = Boolean(dataStatus.shared_table_available);
  // Until a table is loaded, loading one is the stage's main action: the prepared table when there is one.
  const primary = !loaded && !continueAvailable;
  const previewRows = dataStatus.preview ?? [];

  const load = (source: TableSource) => {
    if (source === 'upload') csvInput.current?.click();
    else emit({ type: 'load_shared_table' });
  };
  // A new table clears the suggestions and review decisions, so ask first when there are any.
  const requestLoad = (source: TableSource) => {
    if (loaded && hasResults) setReplacing(source);
    else load(source);
  };

  async function handleCsvUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setUploadError('');
    if (!file) return;
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setUploadError('This is not a CSV file. Choose a .csv matching table.');
      return;
    }
    try {
      const content = await file.text();
      emit({ type: 'upload_csv', filename: file.name, content });
    } catch {
      setUploadError(READ_ERROR);
    }
  }

  return <Section
    title="Matching table"
    description="A CSV file (.csv) with one row per term."
    info={{ title: 'Matching table', content: <><p>A CSV with one row per term to reconcile, in SSSOM-style columns.</p><p><strong>Use prepared table</strong> picks up the table you prepared in the Matching table tool.</p></> }}
    actions={<Chip size="small" variant="outlined" label={loaded ? 'Loaded' : 'No table yet'} color={loaded ? 'success' : 'warning'} />}
  >
    {/* Outside the Stack so the hidden input does not shift its spacing. */}
    <input ref={csvInput} hidden type="file" accept=".csv,text/csv" onChange={handleCsvUpload} />
    <Stack spacing={2}>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ xs: 'stretch', sm: 'center' }}>
        <Tooltip title={preparedAvailable ? '' : 'No prepared table yet. Prepare one in the Matching table tool.'}>
          <span><UseResultButton variant={primary && preparedAvailable ? 'contained' : 'outlined'} disabled={!preparedAvailable} fullWidth onClick={() => requestLoad('prepared')}>Use prepared table</UseResultButton></span>
        </Tooltip>
        <Tooltip title={uploadDisabled ? 'File upload is not available right now.' : ''}>
          <span><UploadButton variant={primary && !preparedAvailable ? 'contained' : 'outlined'} disabled={uploadDisabled} fullWidth onClick={() => requestLoad('upload')}>{loaded ? 'Replace table' : 'Upload table'}</UploadButton></span>
        </Tooltip>
      </Stack>
      {uploadError && <Alert severity="error">{uploadError}</Alert>}
      <MetaList columns={2} items={tableSummaryItems(dataStatus)} />
      {(loaded || previewRows.length > 0) && <Disclosure plain title="Preview" summary={`Showing ${formatCount(previewRows.length)} of ${formatCount(dataStatus.rows ?? previewRows.length)} rows`}>
        <DataTable rows={previewRows} empty="No table preview available yet." />
      </Disclosure>}
    </Stack>
    <ConfirmDialog
      open={replacing !== null}
      title="Replace the matching table?"
      message="Loading another table clears the current suggestions and review decisions. You can undo this from History."
      confirmLabel="Replace table"
      onClose={() => setReplacing(null)}
      // Runs inside the confirm click, so the file picker still opens as a user action.
      onConfirm={() => { if (replacing) load(replacing); }}
    />
  </Section>;
}

function StrategySection({ config, providerLabels, modelLabels, ontologyOptions, update }: { config: WorkflowConfig; providerLabels: Record<string, string>; modelLabels: Record<string, string>; ontologyOptions: string[]; update: (patch: Partial<WorkflowConfig>) => void }) {
  const ontologySelectOptions = unique([...(ontologyOptions || []), ...(config.bioportal_ontologies || [])]).sort();
  const ontologyMenuProps = { PaperProps: { sx: { maxHeight: 360 } } };
  return <Section
    title="Matching strategy"
    description="Model and provider are set in Options."
    info={{ title: 'Matching strategies', content: <>{workflows.map((workflow) => <p key={workflow.id}><strong>{workflow.title}</strong>: {workflow.details.join(', ')}.</p>)}</> }}
  >
    <Stack spacing={2}>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5 }}>{workflows.map((workflow) => {
        const selected = config.workflow === workflow.id;
        return <ButtonBase key={workflow.id} aria-pressed={selected} onClick={() => update({ workflow: workflow.id })} sx={{ display: 'block', textAlign: 'left', p: 2, minWidth: 0, border: 1, borderRadius: 2, borderColor: selected ? 'primary.main' : 'divider', bgcolor: selected ? 'action.selected' : 'background.paper' }}>
          <Stack direction="row" spacing={1} alignItems="center" component="span"><Typography variant="subtitle2" component="span">{workflow.title}</Typography>{selected && <CheckCircleIcon color="primary" fontSize="small" sx={{ ml: 'auto' }} />}</Stack>
          <Typography variant="body2" color="text.secondary" component="span" sx={{ display: 'block', mt: 0.5 }}>{workflow.description}</Typography>
        </ButtonBase>;
      })}</Box>
      <Collapse in={config.workflow === 'bioportal_wikidata_multiagent'}>
        <Stack spacing={1.5}>
          <Typography variant="subtitle2" component="h4">BioPortal settings</Typography>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5, alignItems: 'center' }}>
            <WithInfo title="Ontology search mode" info={<ul><li><strong>Configured ontologies only</strong> searches just the ontologies you select.</li><li><strong>Broad retrieval</strong> searches all BioPortal ontologies and uses the local registry to score candidates, not to filter them.</li></ul>}>
              <FormControl fullWidth size="small"><InputLabel>Ontology search mode</InputLabel><Select label="Ontology search mode" value={config.ontology_search_mode || (config.bioportal_use_all_ontologies ? 'broad_retrieval_registry_scored' : 'configured_only')} onChange={(e) => update({ ontology_search_mode: e.target.value as WorkflowConfig['ontology_search_mode'], bioportal_use_all_ontologies: false })}><MenuItem value="configured_only">Configured ontologies only</MenuItem><MenuItem value="broad_retrieval_registry_scored">Broad retrieval, registry-scored (recommended)</MenuItem></Select></FormControl>
            </WithInfo>
            <FormControl fullWidth size="small" disabled={config.ontology_search_mode !== 'configured_only'}><InputLabel>BioPortal ontologies</InputLabel><Select multiple displayEmpty label="BioPortal ontologies" MenuProps={ontologyMenuProps} value={config.ontology_search_mode !== 'configured_only' ? [] : (config.bioportal_ontologies ?? [])} onChange={(e) => update({ bioportal_ontologies: typeof e.target.value === 'string' ? splitCsv(e.target.value) : (e.target.value as string[]) })} renderValue={(selected) => config.ontology_search_mode === 'broad_retrieval_registry_scored' ? 'All ontologies (registry-scored)' : (selected as string[]).join(', ')}>{ontologySelectOptions.map((ontology) => <MenuItem key={ontology} value={ontology}><Checkbox size="small" checked={config.ontology_search_mode === 'configured_only' && (config.bioportal_ontologies ?? []).includes(ontology)} /><Typography variant="body2">{ontology}</Typography></MenuItem>)}</Select></FormControl>
            <WithInfo title="Trusted ontologies" info="Trusted ontologies are your preferred BioPortal sources. Their matches count as more reliable for automated acceptance and review prioritization.">
              {/* Saved on blur or Enter: per-keystroke saves would drop a trailing comma before the next acronym is typed. */}
              <CommitTextField fullWidth label="Trusted ontologies" helperText="Comma-separated acronyms, saved when you leave the field" value={(config.trusted_ontologies ?? []).join(', ')} normalize={(text) => splitCsv(text).join(', ')} onCommit={(text) => update({ trusted_ontologies: splitCsv(text) })} />
            </WithInfo>
            <FormControlLabel control={<Switch checked={Boolean(config.enable_wikidata_fallback)} onChange={(e) => update({ enable_wikidata_fallback: e.target.checked })} />} label="Allow Wikidata fallback" />
          </Box>
        </Stack>
      </Collapse>
      <MetaList columns={2} items={[{ label: 'Model', value: modelLabels[config.model] ?? config.model }, { label: 'Provider', value: providerLabels[config.provider] ?? config.provider }]} />
    </Stack>
  </Section>;
}

function DefinitionPreparationPanel({ config, update, emit }: { config: WorkflowConfig; update: (patch: Partial<WorkflowConfig>) => void; emit: (event: AppEvent) => void }) {
  const [uploadError, setUploadError] = useState('');
  const sheetInput = useRef<HTMLInputElement>(null);
  const publicationInput = useRef<HTMLInputElement>(null);
  const strategy = config.definition_strategy || 'generate_single_shot';

  async function handleDefinitionsUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setUploadError('');
    if (!file) return;
    if (!/\.(csv|xlsx|xls)$/i.test(file.name)) {
      setUploadError('This is not a CSV or Excel file. Choose a sheet with Term and Definition columns.');
      return;
    }
    try {
      emit({ type: 'upload_definitions_sheet', filename: file.name, content_base64: await fileToBase64(file) });
    } catch {
      setUploadError(READ_ERROR);
    }
  }

  async function handleReferenceUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setUploadError('');
    if (!file) return;
    if (!/\.(pdf|doc|docx)$/i.test(file.name)) {
      setUploadError('This is not a PDF or Word file. Choose a .pdf, .doc or .docx publication.');
      return;
    }
    try {
      emit({ type: 'upload_reference_publication', filename: file.name, content_base64: await fileToBase64(file) });
    } catch {
      setUploadError(READ_ERROR);
    }
  }

  return <Stack spacing={1.5}>
    <Stack direction="row" spacing={0.5} alignItems="center">
      <FormControlLabel control={<Switch checked={Boolean(config.definition_preparation)} onChange={(e) => update({ definition_preparation: e.target.checked })} />} label="Prepare definitions for ambiguous terms" sx={{ mr: 0 }} />
      <InfoButton title="Definition preparation">Definitions give the agents context for ambiguous terms. Generate one per term with the LLM, upload a sheet of definitions, or extract context from a reference publication.</InfoButton>
    </Stack>
    <Collapse in={Boolean(config.definition_preparation)}>
      <Stack spacing={2}>
        <FormControl fullWidth size="small"><InputLabel>Definition strategy</InputLabel><Select label="Definition strategy" value={strategy} onChange={(e) => { setUploadError(''); update({ definition_strategy: String(e.target.value) }); }}><MenuItem value="generate_single_shot">Generate LLM definitions per term</MenuItem><MenuItem value="uploaded_sheet">Upload definitions sheet</MenuItem><MenuItem value="reference_publication">Reference publication</MenuItem></Select></FormControl>
        {strategy === 'uploaded_sheet' && <Stack spacing={1.5}>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} justifyContent="space-between" alignItems={{ xs: 'stretch', sm: 'center' }}>
            <Box>
              <Typography variant="subtitle2" component="h4">Definitions sheet</Typography>
              <Typography variant="body2" color="text.secondary">CSV or Excel with Term and Definition columns.</Typography>
            </Box>
            <input ref={sheetInput} hidden type="file" accept=".csv,.xlsx,.xls,text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" onChange={handleDefinitionsUpload} />
            <UploadButton onClick={() => sheetInput.current?.click()}>{config.definition_uploaded_filename ? 'Replace definitions sheet' : 'Upload definitions sheet'}</UploadButton>
          </Stack>
          {config.definition_uploaded_filename && <MetaList items={[{ label: 'File', value: config.definition_uploaded_filename, mono: true }, { label: 'Definitions', value: formatCount(config.definition_uploaded_count ?? 0), numeric: true }]} />}
        </Stack>}
        {strategy === 'reference_publication' && <Stack spacing={1.5}>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} justifyContent="space-between" alignItems={{ xs: 'stretch', sm: 'center' }}>
            <Box>
              <Stack direction="row" spacing={0.5} alignItems="center"><Typography variant="subtitle2" component="h4">Reference publication</Typography><InfoButton title="Reference publication">The text extracted from the publication becomes the definition context for the agents.</InfoButton></Stack>
              <Typography variant="body2" color="text.secondary">PDF, DOC or DOCX.</Typography>
            </Box>
            <input ref={publicationInput} hidden type="file" accept=".pdf,.doc,.docx,application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document" onChange={handleReferenceUpload} />
            <UploadButton onClick={() => publicationInput.current?.click()}>{config.definition_reference_filename ? 'Replace publication' : 'Upload publication'}</UploadButton>
          </Stack>
          {config.definition_reference_filename && <MetaList items={[{ label: 'File', value: config.definition_reference_filename, mono: true }, { label: 'Characters', value: formatCount(config.definition_reference_char_count ?? 0), numeric: true }]} />}
          {config.definition_reference_text && <TextField label="Extracted reference text" multiline minRows={3} maxRows={10} value={config.definition_reference_text} InputProps={{ readOnly: true }} />}
        </Stack>}
        {uploadError && <Alert severity="error">{uploadError}</Alert>}
      </Stack>
    </Collapse>
  </Stack>;
}

function ReadinessList({ checks }: { checks: ReadinessCheck[] }) {
  return <Stack component="ul" spacing={1} sx={{ m: 0, p: 0, listStyle: 'none' }}>{checks.map((check) => <Stack component="li" key={check.key} direction="row" spacing={1} alignItems="flex-start">
    {check.ok ? <CheckCircleOutlineIcon fontSize="small" color="success" titleAccess="Passed" /> : <WarningAmberIcon fontSize="small" color="warning" titleAccess="Needs attention" />}
    <Box sx={{ minWidth: 0 }}><Typography variant="body2" sx={{ fontWeight: 500 }}>{check.label}</Typography>{check.detail && <Typography variant="caption" color="text.secondary" component="p" sx={{ overflowWrap: 'anywhere' }}>{check.detail}</Typography>}</Box>
  </Stack>)}</Stack>;
}

function runSummaryItems(readiness: ReadinessState, config: WorkflowConfig): MetaItem[] {
  const summary = readiness.summary ?? {};
  const runtime = summary['Est. Runtime'];
  const cost = summary['Est. Cost'] || '';
  return [
    { label: 'Strategy', value: workflows.find((workflow) => workflow.id === config.workflow)?.title ?? (summary.Workflow || config.workflow) },
    { label: 'Model', value: summary.Model || config.model },
    { label: 'SKOS matching', value: summary['SKOS Matching'] || (config.skos_matching ? 'Enabled' : 'Disabled') },
    { label: 'Auto-accept', value: summary['Auto-accept'] || (config.auto_accept ? 'Enabled' : 'Disabled') },
    { label: 'Batch size', value: summary['Batch Size'] || String(config.advanced.batch_size), numeric: true },
    { label: 'Max workers', value: summary['Max Workers'] || String(config.advanced.max_workers), numeric: true },
    { label: 'Estimated runtime', value: runtime && runtime !== 'n/a' ? runtime : '', numeric: true },
    // Until telemetry reports a figure the backend sends an explanation, not a cost.
    { label: 'Estimated cost', value: /\d/.test(cost) ? cost : <Tooltip title={cost || 'Available after run telemetry'}><Box component="span" sx={{ color: 'text.secondary' }}>—</Box></Tooltip> },
  ];
}

function SetupPage(props: { config: WorkflowConfig; dataStatus: DataStatus; readiness: ReadinessState; hasResults: boolean; continueAvailable: boolean; providerLabels: Record<string, string>; modelLabels: Record<string, string>; ontologyOptions: string[]; update: (patch: Partial<WorkflowConfig>) => void; emit: (event: AppEvent) => void }) {
  const { config, dataStatus, readiness } = props;
  const definitionSummary = !config.definition_preparation ? 'Off' : config.definition_strategy === 'uploaded_sheet' ? `On, definitions sheet${config.definition_uploaded_filename ? ` (${config.definition_uploaded_filename})` : ''}` : config.definition_strategy === 'reference_publication' ? 'On, reference publication' : 'On, a generated definition for each term';
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr)', xl: 'minmax(0, 1fr) 360px' }, gap: 3, alignItems: 'start' }}>
    <Stack spacing={3} sx={{ minWidth: 0 }}>
      <MatchingTableSection dataStatus={dataStatus} hasResults={props.hasResults} continueAvailable={props.continueAvailable} emit={props.emit} />
      <StrategySection config={config} providerLabels={props.providerLabels} modelLabels={props.modelLabels} ontologyOptions={props.ontologyOptions} update={props.update} />
      <Disclosure title="Definition preparation" optional summary={definitionSummary}><DefinitionPreparationPanel config={config} update={props.update} emit={props.emit} /></Disclosure>
    </Stack>
    <Stack spacing={3} sx={{ minWidth: 0 }}>
      <Section title="Run prerequisites" actions={<Chip size="small" variant="outlined" color={readiness.ready ? 'success' : 'warning'} label={readiness.ready ? 'Ready' : 'Action needed'} />}>
        <ReadinessList checks={readiness.checks ?? []} />
      </Section>
      <Section title="Run summary"><MetaList items={runSummaryItems(readiness, config)} /></Section>
    </Stack>
  </Box>;
}

function MonitoringPanel({ telemetry, runStatus }: { telemetry: Telemetry; runStatus: RunStatus }) {
  const [tab, setTab] = useState(0);
  const hasRealProgress = typeof runStatus.processed_count === 'number' && typeof runStatus.total_count === 'number' && runStatus.total_count > 0;
  const progress = hasRealProgress ? Math.min(100, Math.round(((runStatus.processed_count as number) / (runStatus.total_count as number)) * 100)) : null;
  const processedValue = runStatus.processed_count ?? telemetry.processed_terms ?? 0;
  const totalValue = runStatus.total_count ?? telemetry.total_terms ?? 0;
  const failureValue = Math.max(0, Number(telemetry.failed_terms ?? 0));
  const summary = `${formatCount(processedValue)} of ${formatCount(totalValue)} terms processed${failureValue ? `, ${formatCount(failureValue)} failed` : ''}`;
  return <Disclosure title="Monitoring and telemetry" summary={summary}>
    <Stack spacing={2}>
      <Stack direction="row" spacing={1} alignItems="center">
        <Tabs value={tab} onChange={(_, v) => setTab(v)} variant="scrollable" allowScrollButtonsMobile sx={{ flex: 1, minWidth: 0 }}><Tab label="Run status" /><Tab label="LLM calls" /><Tab label="LangSmith" /><Tab label="Logs" /></Tabs>
        <Chip size="small" variant="outlined" label={telemetry.enabled ? 'LangSmith on' : 'LangSmith off'} color={telemetry.enabled ? 'success' : 'default'} />
      </Stack>
      {tab === 0 && <Stack spacing={2}>
        {hasRealProgress && <LinearProgress aria-label="Terms processed" variant="determinate" value={progress ?? 0} />}
        {/* Inside a Disclosure the figures are a fact list; KpiStrip belongs at page level. */}
        <MetaList columns={2} items={[
          { label: 'Terms processed', value: formatCount(processedValue), numeric: true },
          { label: 'Total terms', value: formatCount(totalValue), numeric: true },
          { label: 'Run failures', value: failureValue > 0 ? <Box component="span" sx={{ color: 'error.main' }}>{formatCount(failureValue)}</Box> : formatCount(failureValue), numeric: true },
          { label: 'Duration', value: telemetry.duration_sec ? formatSeconds(telemetry.duration_sec) : '', numeric: true },
          { label: 'Cost', value: formatCost(telemetry.total_cost_usd ?? 0), numeric: true },
        ]} />
        <DataTable rows={telemetry.events} empty="No term-level events captured yet." />
      </Stack>}
      {tab === 1 && <DataTable rows={telemetry.llm_calls} empty="No LLM prompt/response interactions captured yet." />}
      {tab === 2 && <Stack spacing={1.5} alignItems="flex-start">
        <Alert severity={telemetry.enabled ? 'info' : 'warning'} sx={{ alignSelf: 'stretch' }}>{telemetry.langsmith_message || (telemetry.enabled ? 'LangSmith monitoring is enabled.' : 'LangSmith monitoring is off. Turn it on in Options under Monitoring and tracing.')}</Alert>
        {(telemetry.langsmith_project_url || telemetry.langsmith_url) && <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
          {telemetry.langsmith_project_url && <Button href={telemetry.langsmith_project_url} target="_blank" rel="noreferrer" endIcon={<OpenInNewIcon />}>Open LangSmith project</Button>}
          {telemetry.langsmith_url && <Button href={telemetry.langsmith_url} target="_blank" rel="noreferrer" endIcon={<OpenInNewIcon />}>Open LangSmith run</Button>}
        </Stack>}
        <Box sx={{ alignSelf: 'stretch' }}><DataTable rows={telemetry.cascade} empty="No cascade trace captured yet." /></Box>
      </Stack>}
      {tab === 3 && <Box component="pre" tabIndex={0} aria-label="Run logs" sx={{ ...codeSx, p: 1.5, maxHeight: 360 }}>{(telemetry.logs?.length ? telemetry.logs : ['No logs captured yet.']).slice(-80).join('\n')}</Box>}
    </Stack>
  </Disclosure>;
}

// `primary`: Continue cannot move on yet, so starting is the main thing to do on this view.
function RunStartSection({ readiness, config, dataStatus, running, runStatus, primary, onStart }: { readiness: ReadinessState; config: WorkflowConfig; dataStatus: DataStatus; running: boolean; runStatus: RunStatus; primary: boolean; onStart: () => void }) {
  const failing = (readiness.checks ?? []).filter((check) => !check.ok);
  const summary = readiness.summary ?? {};
  const runtime = summary['Est. Runtime'];
  return <Section title="Start the agents" actions={<GenerateButton variant={primary ? 'contained' : 'outlined'} disabled={!readiness.ready || running} onClick={onStart}>Start reconciliation</GenerateButton>}>
    <Stack spacing={2}>
      <Alert severity={readiness.ready ? 'success' : 'warning'}>{runStatus.message || (readiness.ready ? 'Ready to run' : 'Resolve prerequisites before running.')}</Alert>
      {failing.length > 0 && <ReadinessList checks={failing} />}
      <MetaList columns={2} items={[
        { label: 'Strategy', value: workflows.find((workflow) => workflow.id === config.workflow)?.title ?? config.workflow },
        { label: 'Model', value: summary.Model || config.model },
        { label: 'Terms', value: formatCount(dataStatus.unreconciled_rows ?? dataStatus.rows ?? 0), numeric: true },
        { label: 'Estimated runtime', value: runtime && runtime !== 'n/a' ? runtime : '', numeric: true },
      ]} />
    </Stack>
  </Section>;
}

function RunSuccessSection({ runStatus, telemetry }: { runStatus: RunStatus; telemetry: Telemetry }) {
  const processed = runStatus.processed_count ?? telemetry.processed_terms ?? 0;
  const total = runStatus.total_count ?? telemetry.total_terms ?? 0;
  const duration = telemetry.duration_sec ?? runStatus.elapsed_seconds;
  // The headline figures sit at page level above the status section (KpiStrip never nests).
  return <>
    <KpiStrip ariaLabel="Run result" items={[
      { label: 'Terms processed', value: total ? `${formatCount(processed)} of ${formatCount(total)}` : formatCount(processed) },
      { label: 'Duration', value: typeof duration === 'number' ? formatSeconds(duration) : '' },
      { label: 'Cost', value: formatCost(telemetry.total_cost_usd ?? 0) },
    ]} />
    <Section tone="success" title="Run finished" description={runStatus.message || 'The suggestions are ready for review.'} />
  </>;
}

function RunErrorSection({ error, primary, onRetry }: { error: string; primary: boolean; onRetry: () => void }) {
  return <Section tone="error" title="Run failed">
    <Stack spacing={2} alignItems="flex-start">
      <Alert severity="error" sx={{ alignSelf: 'stretch' }}>{error}</Alert>
      <GenerateButton variant={primary ? 'contained' : 'outlined'} onClick={onRetry}>Rerun reconciliation</GenerateButton>
    </Stack>
  </Section>;
}

// `primary`: Continue cannot move on yet, so resuming is the main thing to do on this view.
function RunStoppedSection({ runStatus, telemetry, primary, onResume, onSkipFailedTerm, onRestart }: { runStatus: RunStatus; telemetry: Telemetry; primary: boolean; onResume: () => void; onSkipFailedTerm: () => void; onRestart: () => void }) {
  const processed = runStatus.processed_count ?? telemetry.processed_terms ?? 0;
  const total = runStatus.total_count ?? telemetry.total_terms ?? 0;
  const stopEvent = (runStatus.stop_event ?? {}) as StopEvent;
  const stoppedByLlm = runStatus.stop_reason === 'llm_error' || stopEvent.stop_reason === 'llm_error';
  const stoppedByRuntimeError = runStatus.stop_reason === 'runtime_error' || stopEvent.stop_reason === 'runtime_error';
  const canSkipFailed = stoppedByLlm || stoppedByRuntimeError;
  const nextTerm = total ? Math.min(total, Number(processed) + 1) : null;
  return <Section tone="warning" title="Run stopped" actions={<Chip size="small" variant="outlined" color="warning" label={stoppedByLlm ? 'Needs attention' : 'Stopped'} />}>
    <Stack spacing={2}>
      <Alert severity="warning">{runStatus.message || (stoppedByLlm ? 'The run stopped on a timeout or LLM/API error.' : 'The run was stopped by the user.')}</Alert>
      <MetaList columns={2} items={[
        { label: 'Terms processed', value: total ? `${formatCount(processed)} of ${formatCount(total)}` : formatCount(processed), numeric: true },
        { label: 'Resumes at term', value: nextTerm ? `${formatCount(nextTerm)} of ${formatCount(total)}` : '', numeric: true },
        { label: 'Stopped at', value: stopEvent.term || 'Last completed term' },
      ]} />
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} useFlexGap flexWrap="wrap">
        {runStatus.can_resume && <GenerateButton variant={primary ? 'contained' : 'outlined'} onClick={onResume}>Resume run</GenerateButton>}
        {canSkipFailed && <Button variant="outlined" startIcon={<SkipNextIcon />} disabled={!runStatus.can_resume} onClick={onSkipFailedTerm}>Skip failed term and resume</Button>}
        <ConfirmActionButton
          ButtonComponent={ResetButton}
          dialogTitle="Restart from the beginning?"
          dialogMessage="The agents start again with the first term. Progress from the stopped run is not resumed."
          confirmLabel="Restart run"
          onConfirm={onRestart}
        >
          Restart from the beginning
        </ConfirmActionButton>
      </Stack>
    </Stack>
  </Section>;
}

function RunPage({ config, readiness, runStatus, telemetry, dataStatus, optimisticRunning, optimisticStartedAt, continueAvailable, onStart, onSkipFailedTerm, emit }: { config: WorkflowConfig; readiness: ReadinessState; runStatus: RunStatus; telemetry: Telemetry; dataStatus: DataStatus; optimisticRunning: boolean; optimisticStartedAt: string | null; continueAvailable: boolean; onStart: (resumePrevious?: boolean) => void; onSkipFailedTerm: () => void; emit: (event: AppEvent) => void }) {
  const optimisticTotalCount = runStatus.total_count ?? telemetry.total_terms ?? dataStatus.rows ?? null;
  const running = Boolean(runStatus.running || optimisticRunning);
  const panelStatus = optimisticRunning && !runStatus.running ? { ...runStatus, running: true, stopped: false, finished: false, error: null, progress: null, started_at: runStatus.started_at ?? optimisticStartedAt, message: runStatus.message || null, total_count: runStatus.total_count ?? telemetry.total_terms ?? dataStatus.rows ?? null } : runStatus;
  const stopRun = () => emit({ type: 'stop_run' });
  // One stable column: the run state on top, monitoring below, whatever the state.
  return <Stack spacing={3}>
    {running ? <AgentRunProgressPanel runStatus={panelStatus} optimisticTotalCount={optimisticTotalCount} onStop={stopRun} />
      : runStatus.stopped ? <RunStoppedSection runStatus={runStatus} telemetry={telemetry} primary={!continueAvailable} onResume={() => onStart(true)} onSkipFailedTerm={onSkipFailedTerm} onRestart={() => onStart(false)} />
      : runStatus.error ? <RunErrorSection error={runStatus.error} primary={!continueAvailable} onRetry={() => onStart(false)} />
      : runStatus.finished ? <RunSuccessSection runStatus={runStatus} telemetry={telemetry} />
      : <RunStartSection readiness={readiness} config={config} dataStatus={dataStatus} running={running} runStatus={runStatus} primary={!continueAvailable} onStart={() => onStart(false)} />}
    <MonitoringPanel telemetry={telemetry} runStatus={panelStatus} />
  </Stack>;
}

function ReviewPage({ review, dataStatus, emit, onDraftChange }: { review: ReviewState; dataStatus: DataStatus; emit: (event: AppEvent) => void; onDraftChange: (dirty: boolean) => void }) {
  const items = review.items ?? [];
  // The drawer follows the grid's visible order; the item is read live from the snapshot so it
  // reflects backend updates, falling back to the row as it was when opened.
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [opened, setOpened] = useState<ReviewItem | null>(null);
  const [order, setOrder] = useState<string[]>([]);
  const [detailTab, setDetailTab] = useState(0);
  const selected = selectedId ? items.find((item) => item.mapping_id === selectedId) ?? opened : null;
  const position = selected ? order.indexOf(selected.mapping_id) : -1;
  // "No match" is a property of status + whether a candidate URI survived — NOT of the
  // SKOS relation. A candidate_suggested row with a weak/blank relation still has a
  // suggestion to show and accept.
  const isNoMatchItem = (item: ReviewItem) => {
    const status = String(item.final_ui_status || item.status || '').toLowerCase();
    if (status === 'no_match' || status === 'timeout') return true;
    return !String(item.final_ui_uri ?? item.suggested_uri ?? '').trim();
  };
  const traceOf = (item: ReviewItem) => item.trace_metadata ?? {};
  const executionTraceOf = (item: ReviewItem): Record<string, any> | null => {
    const trace = traceOf(item);
    const executionTrace = trace.execution_trace;
    return executionTrace && typeof executionTrace === 'object' ? executionTrace as Record<string, any> : null;
  };
  const traceEventsOf = (item: ReviewItem): Record<string, any>[] => {
    const executionTrace = executionTraceOf(item);
    return Array.isArray(executionTrace?.events) ? executionTrace.events as Record<string, any>[] : [];
  };
  const traceLlmCallsOf = (item: ReviewItem): Record<string, any>[] => {
    const executionTrace = executionTraceOf(item);
    return Array.isArray(executionTrace?.llm_calls) ? executionTrace.llm_calls as Record<string, any>[] : [];
  };
  const traceCandidatesOf = (item: ReviewItem): Record<string, any>[] => {
    const event = traceEventsOf(item).find((entry) => entry.event_type === 'candidate_prerank_completed');
    const candidates = event?.output_summary?.top_candidates;
    return Array.isArray(candidates) ? candidates as Record<string, any>[] : [];
  };
  const rawTraceJsonOf = (item: ReviewItem) => JSON.stringify(executionTraceOf(item) ?? traceOf(item), null, 2);
  const downloadTraceJson = (item: ReviewItem) => downloadFile(rawTraceJsonOf(item), 'application/json;charset=utf-8', `term_trace_${item.row_index ?? item.mapping_id}.json`);
  useEffect(() => setDetailTab(0), [selected?.mapping_id]);

  const openDetails = (item: ReviewItem, visibleOrder: string[]) => { setOpened(item); setOrder(visibleOrder.includes(item.mapping_id) ? visibleOrder : [item.mapping_id]); setSelectedId(item.mapping_id); };
  const closeDetails = () => setSelectedId(null);
  const step = (offset: number) => {
    const id = order[position + offset];
    if (!id) return;
    setOpened(items.find((item) => item.mapping_id === id) ?? null);
    setSelectedId(id);
  };
  const trace = selected ? traceOf(selected) : {};
  const wikidataPass = Boolean(trace.provider_escalation_used || trace.wikidata_second_pass_mapping_type || trace.wikidata_second_pass_decision_source || trace.wikidata_second_pass_fallback_reason);

  return (
    <Stack spacing={3}>
      <Disclosure title="Working table" summary={dataStatus.has_table ? `${dataStatus.filename || dataStatus.source_name || 'Table loaded'}, ${formatCount(dataStatus.rows ?? 0)} rows` : 'No table loaded'}>
        <Stack spacing={2}>
          <MetaList columns={2} items={tableSummaryItems(dataStatus)} />
          <DataTable rows={dataStatus.preview} empty="No live working table preview available yet." />
        </Stack>
      </Disclosure>

      <Section
        title="Review suggestions"
        info={{ title: 'Status legend', content: <><ul><li><strong>Review suggestion</strong>: a candidate that needs manual review, often from Wikidata after BioPortal found no verified match.</li><li><strong>No match</strong>: a negative decision. A low-confidence candidate kept for audit appears only in Details and cannot be accepted.</li></ul><p>If the proposed SKOS predicate is not right, change the match type in the table before accepting.</p></> }}
      >
        <ReviewGrid items={items} counts={review.counts} emit={emit} onOpenDetails={openDetails} onDraftChange={onDraftChange} />
      </Section>

      <Drawer anchor="right" open={Boolean(selected)} onClose={closeDetails} PaperProps={{ role: 'dialog', 'aria-labelledby': 'agent-details-title', sx: { width: { xs: '100%', sm: 520 } } } as any}>
        {selected && <>
          <Box sx={{ position: 'sticky', top: 0, zIndex: 1, bgcolor: 'background.paper', borderBottom: 1, borderColor: 'divider', px: 2, py: 1 }}>
            <Stack direction="row" alignItems="center" spacing={0.5}>
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography variant="caption" color="text.secondary" component="p">Suggestion details</Typography>
                <Typography id="agent-details-title" variant="subtitle1" component="h2" noWrap title={selected.term}>{selected.term || '—'}</Typography>
              </Box>
              <Tooltip title="Previous term"><span><IconButton aria-label="Previous term" disabled={position <= 0} onClick={() => step(-1)}><ChevronLeftIcon /></IconButton></span></Tooltip>
              {position >= 0 && <Typography variant="caption" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums' }}>{formatCount(position + 1)} of {formatCount(order.length)}</Typography>}
              <Tooltip title="Next term"><span><IconButton aria-label="Next term" disabled={position < 0 || position >= order.length - 1} onClick={() => step(1)}><ChevronRightIcon /></IconButton></span></Tooltip>
              <Tooltip title="Close"><IconButton aria-label="Close details" onClick={closeDetails}><CloseIcon /></IconButton></Tooltip>
            </Stack>
          </Box>
          <Stack spacing={2} sx={{ p: 2 }}>
            <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
              <Chip size="small" variant="outlined" color={statusColor(selected.status)} label={statusLabel(selected.status)} />
              {String(selected.status || '').toLowerCase() === 'candidate_suggested' && <Stack direction="row" spacing={0.5} alignItems="center">
                <Typography variant="body2" color="text.secondary">Needs manual review: not a verified match.</Typography>
                <InfoButton title="Review suggestion">This candidate was found after BioPortal did not produce a verified match. It requires manual review because it did not satisfy the strict verified-match policy.</InfoButton>
              </Stack>}
            </Stack>
            <LineageStrip item={selected} />
            <ReviewProvenancePanel item={selected} />
            {isNoMatchItem(selected) && <Alert severity="warning">{selected.no_match_note || 'No-match decision. Any candidate shown here is for audit only and cannot be accepted.'}</Alert>}
            <Tabs value={detailTab} onChange={(_e, value) => setDetailTab(value)} variant="scrollable" allowScrollButtonsMobile>
              <Tab label="Details" />
              <Tab label="Candidates" />
              <Tab label="Trace" />
              <Tab label="LLM prompts" />
              <Tab label="Raw JSON" />
            </Tabs>
            {detailTab === 0 && <Stack spacing={2}>
              <Stack spacing={1}>
                <ComparisonRow leftLabel="Input term" leftValue={selected.term} rightLabel="Suggested term" rightValue={selected.suggested_label} />
                <ComparisonRow leftLabel="Input definition" leftValue={selected.definition} rightLabel="Suggested description" rightValue={selected.suggested_description} />
                <ComparisonRow mono leftLabel="Input URI" leftValue={selected.input_uri} rightLabel="Suggested URI" rightValue={selected.suggested_uri} />
                {isNoMatchItem(selected) && (selected.candidate_uri || selected.candidate_label) && <>
                  <ComparisonRow leftLabel="Audit candidate label" leftValue={selected.candidate_label} rightLabel="Audit candidate URI" rightValue={selected.candidate_uri ? <Box component="span" sx={monoSx}>{selected.candidate_uri}</Box> : ''} />
                  <ComparisonRow leftLabel="Audit candidate description" leftValue={selected.candidate_description} rightLabel="Why not accepted" rightValue={selected.no_match_note || selected.explanation} />
                </>}
                <ComparisonRow leftLabel="Current or accepted match type" leftValue={selected.accepted_match_type ? <Box component="span" sx={monoSx}>{selected.accepted_match_type}</Box> : ''} rightLabel="Suggested match type" rightValue={<Chip size="small" variant="outlined" color={skosColor(selected.match_type)} label={selected.match_type || 'no_match'} sx={monoSx} />} />
              </Stack>
              <Box>
                <Typography variant="subtitle2" component="h3" sx={{ mb: 1 }}>Decision</Typography>
                <MetaList items={[
                  { label: 'Subject label', value: selected.subject_label },
                  { label: 'Provider', value: selected.provider },
                  { label: 'Decision source', value: selected.decision_source },
                  { label: 'Confidence', value: isNoMatchItem(selected) || !String(selected.suggested_uri || '').trim() ? '' : <>{formatScore(selected.confidence)}{Boolean(trace.provider_signal_boost_applied) && <Chip size="small" variant="outlined" label="Adjusted by lexical/provider signal" sx={{ ml: 1 }} />}</>, numeric: true },
                  { label: 'Candidate review mode', value: selected.review_mode ? formatReviewMode(selected.review_mode) : '' },
                  { label: 'Fallback', value: selected.fallback_reason ? `Yes (${selected.fallback_reason})` : 'No' },
                  { label: 'Explanation', value: selected.explanation },
                ]} />
              </Box>
              {wikidataPass && <Disclosure title="Wikidata second pass" summary={trace.wikidata_second_pass_has_candidate ? 'Candidate found' : 'No suitable candidate'}>
                <MetaList items={[
                  { label: 'Mapping type', value: String(trace.wikidata_second_pass_mapping_type ?? ''), mono: true },
                  { label: 'Decision source', value: String(trace.wikidata_second_pass_decision_source ?? '') },
                  { label: 'Fallback', value: trace.wikidata_second_pass_fallback_reason ? `Yes (${trace.wikidata_second_pass_fallback_reason})` : 'No' },
                ]} />
              </Disclosure>}
              {Boolean(trace.annotator_rescue_used) && <Disclosure title="Annotator rescue" summary={trace.annotator_rescue_failed ? 'Failed' : 'Used'}>
                <MetaList items={[
                  { label: 'Result', value: `${trace.annotator_rescue_failed ? 'Failed' : 'Used'} (${String(trace.annotator_rescue_reason ?? 'triggered')})` },
                  { label: 'Matched variants', value: Array.isArray(trace.annotator_rescue_matched_variants) ? (trace.annotator_rescue_matched_variants as unknown[]).join(', ') : String(trace.annotator_rescue_matched_variants ?? '') },
                ]} />
              </Disclosure>}
            </Stack>}
            {detailTab === 1 && <Stack spacing={1}>{traceCandidatesOf(selected).length ? traceCandidatesOf(selected).map((candidate, index) => <Paper key={`${candidate.uri || candidate.label || index}`} variant="outlined" sx={{ p: 1.5 }}>
              <Stack spacing={1}>
                <Stack direction="row" spacing={0.75} alignItems="center" flexWrap="wrap" useFlexGap><Chip size="small" label={String(candidate.ontology || 'provider')} /><Typography variant="subtitle2" component="p" sx={{ overflowWrap: 'anywhere' }}>{String(candidate.label || 'Candidate')}</Typography></Stack>
                {candidate.uri && <Typography variant="caption" color="text.secondary" component="p" sx={{ fontFamily: fonts.mono, overflowWrap: 'anywhere' }}>{String(candidate.uri)}</Typography>}
                <MetaList columns={2} items={[
                  { label: 'Combined', value: formatScore(candidate.combined_confidence), numeric: true },
                  { label: 'Lexical', value: formatScore(candidate.lexical_score), numeric: true },
                  { label: 'Definition', value: formatScore(candidate.definition_score), numeric: true },
                  { label: 'Context', value: formatScore(candidate.ontology_context_score), numeric: true },
                ]} />
                {candidate.discard_reason && <Typography variant="caption" color="warning.main" component="p">{String(candidate.discard_reason)}</Typography>}
              </Stack>
            </Paper>) : <EmptyState dense title="No candidate shortlist for this term" description="Raw and discarded candidates are traced when you turn them on in Options under Monitoring and tracing." />}</Stack>}
            {detailTab === 2 && <Stack spacing={1}>{traceEventsOf(selected).length ? traceEventsOf(selected).map((event, index) => <Paper key={`${event.step_index ?? index}-${event.event_type || event.stage}`} variant="outlined" sx={{ p: 1.5 }}>
              <Stack spacing={1}>
                <Stack direction="row" spacing={0.75} alignItems="center" flexWrap="wrap" useFlexGap><Chip size="small" label={`#${String(event.step_index ?? index).padStart(2, '0')}`} sx={{ fontVariantNumeric: 'tabular-nums' }} /><Chip size="small" variant="outlined" label={String(event.stage || 'stage')} />{event.decision && <Chip size="small" color={String(event.decision).includes('not') || String(event.decision).includes('error') ? 'warning' : 'default'} label={String(event.decision)} />}</Stack>
                <Typography variant="subtitle2" component="p">{String(event.event_type || event.stage || 'event')}</Typography>
                {event.reason && <Typography variant="body2" color="text.secondary">{String(event.reason)}</Typography>}
                <Box component="pre" tabIndex={0} sx={codeSx}>{JSON.stringify(event.output_summary || event.data || {}, null, 2)}</Box>
              </Stack>
            </Paper>) : <EmptyState dense title="No trace events for this term" description="Structured trace events appear here when the run records them." />}</Stack>}
            {detailTab === 3 && <Stack spacing={1}>{traceLlmCallsOf(selected).some((call) => call.prompt_sent || call.raw_response) ? traceLlmCallsOf(selected).map((call, index) => <Disclosure key={`${call.call_id || index}`} title={String(call.purpose || call.stage || 'llm')} summary={String(call.call_id || 'No call id')}>
              <Stack spacing={1.5}>
                <Box><Typography variant="caption" color="text.secondary" component="p" sx={{ mb: 0.5 }}>Prompt</Typography><Box component="pre" tabIndex={0} sx={codeSx}>{JSON.stringify(call.prompt_sent || {}, null, 2)}</Box></Box>
                <Box><Typography variant="caption" color="text.secondary" component="p" sx={{ mb: 0.5 }}>Response</Typography><Box component="pre" tabIndex={0} sx={codeSx}>{JSON.stringify(call.raw_response || call.parsed_response || {}, null, 2)}</Box></Box>
              </Stack>
            </Disclosure>) : <EmptyState dense title="Prompt logging was off for this run" description="Turn on LLM prompts in Options under Monitoring and tracing, then run again." />}</Stack>}
            {detailTab === 4 && <Stack spacing={1.5} alignItems="flex-start">
              <DownloadButton onClick={() => downloadTraceJson(selected)}>Download trace (JSON)</DownloadButton>
              <Box component="pre" tabIndex={0} aria-label="Raw trace JSON" sx={{ ...codeSx, maxHeight: 480, alignSelf: 'stretch' }}>{rawTraceJsonOf(selected)}</Box>
            </Stack>}
          </Stack>
        </>}
      </Drawer>
    </Stack>
  );
}

// `onExport` asks the backend for a fresh SSSOM file; the app downloads it when the payload arrives.
function ExportPage({ review, dataStatus, exportPayload, onExport }: { review: ReviewState; dataStatus: DataStatus; exportPayload?: ExportPayload | null; onExport: () => void }) {
  const preparedContent = exportPayload?.content ?? '';
  const preparedFilename = exportPayload?.filename || SSSOM_FILENAME;
  const counts = review.counts;
  const open = OPEN_STATUSES.reduce((sum, key) => sum + Number(counts?.[key] ?? 0), 0);
  return <Stack spacing={3}>
    <KpiStrip ariaLabel="Review decisions" items={[
      { label: 'Accepted mappings', value: formatCount(counts?.accepted ?? 0), hint: 'Exported' },
      { label: 'Still open', value: formatCount(open), hint: 'Pending, matched or suggested' },
      { label: 'Rejected', value: formatCount(counts?.rejected ?? 0) },
      { label: 'No match', value: formatCount(counts?.no_match ?? 0) },
    ]} />
    <Section
      title="SSSOM export"
      description="A CSV snapshot of your accepted mappings."
      info={{ title: 'SSSOM export', content: <><p>SSSOM (Simple Standard for Sharing Ontological Mappings) is a tabular format for mappings. The file contains your accepted mappings.</p><p>Rejected and no-match rows stay in this workspace for audit and curation records.</p></> }}
    >
      <Stack spacing={2} alignItems="flex-start">
        <MetaList items={[{ label: 'Source', value: dataStatus.source_name || dataStatus.filename, mono: true }]} />
        <DownloadButton onClick={onExport}>Download SSSOM (CSV)</DownloadButton>
        {preparedContent && <Stack role="status" direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
          <CheckCircleOutlineIcon color="success" fontSize="small" />
          <Typography variant="body2">Prepared <Box component="code" sx={{ ...monoSx, overflowWrap: 'anywhere' }}>{preparedFilename}</Box></Typography>
          {/* Fallback when the browser blocked the automatic download. */}
          <DownloadButton size="small" variant="text" onClick={() => downloadFile(preparedContent, exportPayload?.mime_type || SSSOM_MIME, preparedFilename)}>Download SSSOM (CSV) again</DownloadButton>
        </Stack>}
      </Stack>
    </Section>
  </Stack>;
}

export function AgentReconciliationMuiApp({ args, onEvent, busy = false, onDraftChange }: AgentReconciliationAppProps) {
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
  useEffect(() => {
    if (providerKind !== 'codex' || !args?.codexAuthStatus?.pending) return undefined;
    let refreshing = false;
    const timer = window.setInterval(() => {
      if (refreshing) return;
      refreshing = true;
      void Promise.resolve(onEvent?.({ type: 'codex_auth_refresh_pending', nonce: Date.now() }))
        .catch(() => undefined)
        .finally(() => { refreshing = false; });
    }, 2000);
    return () => window.clearInterval(timer);
  }, [onEvent, providerKind, args?.codexAuthStatus?.pending]);
  const ontologyOptions = args?.ontologyOptions ?? [];
  // The export payload stays in the backend snapshot, so remounting the tool (switching tools,
  // reloading, a project restore) brings it back. Only a payload answering this view's own
  // Download click is downloaded automatically; the nonce seen at mount counts as handled.
  const handledExportNonceRef = useRef<number | string | null>(args?.exportPayload?.nonce ?? null);
  const exportRequestedRef = useRef(false);
  const [config, setConfig] = useState<WorkflowConfig>(() => normalizeConfig(args?.config, providers, baseModels));
  useEffect(() => setConfig(normalizeConfig(args?.config, providers, baseModels)), [args?.config, providers, baseModels]);
  const providerOptions = useMemo(() => unique([...providers, config.provider, config.planner_provider || '']), [providers, config.provider, config.planner_provider]);
  const modelOptions = useMemo(() => unique(baseModels), [baseModels]);
  const activeStage = normalizeStage(args?.active_stage ?? args?.activeStage);
  useEffect(() => {
    const payload = args?.exportPayload;
    const content = payload?.content ?? '';
    const nonce = payload?.nonce ?? null;
    if (!payload || !content || nonce === null || handledExportNonceRef.current === nonce) return;
    handledExportNonceRef.current = nonce;
    if (!exportRequestedRef.current) return;
    exportRequestedRef.current = false;
    downloadFile(content, payload.mime_type || SSSOM_MIME, payload.filename || SSSOM_FILENAME);
  }, [args?.exportPayload]);
  const [optionsOpen, setOptionsOpen] = useState(false);
  const [navigationPending, setNavigationPending] = useState(false);
  const [handoffOpen, setHandoffOpen] = useState(false);
  const [handoffBusy, setHandoffBusy] = useState(false);
  const [handoffError, setHandoffError] = useState('');
  // Optimistic run state bridges the gap between Start/Resume/Skip and the first snapshot that
  // reports the run. It lives here so navigation is locked during that window too.
  const [optimisticRunning, setOptimisticRunning] = useState(false);
  const [optimisticStartedAt, setOptimisticStartedAt] = useState<string | null>(null);
  useEffect(() => {
    if (runStatus.finished || runStatus.error || runStatus.running || runStatus.stopped) {
      setOptimisticRunning(false);
      if (runStatus.finished || runStatus.error || runStatus.stopped) setOptimisticStartedAt(null);
    }
  }, [runStatus.finished, runStatus.error, runStatus.running, runStatus.stopped]);
  const navigationLock = useRef(false);
  // Returns the host's postEvent promise so callers (options dialog Save) can await the round-trip.
  function emit(event: AppEvent) { return onEvent?.({ ...event, nonce: Date.now() }); }
  function update(patch: Partial<WorkflowConfig>) { const next = normalizeConfig({ ...config, ...patch }, providerOptions, modelOptions); setConfig(next); emit({ type: 'config_changed', config: next }); }
  const running = Boolean(runStatus.running || optimisticRunning);
  function startOptimistic(event: AppEvent) {
    setOptimisticStartedAt(new Date().toISOString());
    setOptimisticRunning(true);
    // If the request failed or the returned snapshot shows no run, drop the optimistic state so
    // the page and navigation are never stuck in a run that did not start.
    void Promise.resolve(emit(event)).then((result: any) => {
      if (result === false || (result?.args?.run_status && !result.args.run_status.running)) setOptimisticRunning(false);
    }, () => setOptimisticRunning(false));
  }
  const startRun = (resumePrevious = false) => startOptimistic({ type: 'start_run', resume_previous: resumePrevious });
  const skipFailedTerm = () => startOptimistic({ type: 'skip_failed_term_and_resume' });
  const requestExport = () => {
    const nonceBefore = args?.exportPayload?.nonce ?? null;
    exportRequestedRef.current = true;
    // A request that produced no new file must not leave the flag armed for a later restore.
    void Promise.resolve(emit({ type: 'export_sssom' })).then((result: any) => {
      const nonce = result?.args?.exportPayload?.nonce ?? null;
      if (result === false || nonce === null || nonce === nonceBefore) exportRequestedRef.current = false;
    }, () => { exportRequestedRef.current = false; });
  };

  // Match-type edits in the review grid are only sent with Accept; leaving the stage drops them.
  const reviewDraftRef = useRef(false);
  const onReviewDraft = useCallback((dirty: boolean) => {
    reviewDraftRef.current = dirty;
    onDraftChange?.(dirty);
  }, [onDraftChange]);

  const processed = Number(runStatus.processed_count ?? telemetry.processed_terms ?? 0);
  // Leaving a stopped run for Review emits review_partial_results (as the former "Review
  // completed terms" button did), so the backend shows its partial-results notice.
  const partialReview = activeStage === 'run' && !running && Boolean(runStatus.stopped) && processed > 0;
  async function sendNavigation(event: AppEvent) {
    if (navigationLock.current || running) return;
    if (reviewDraftRef.current && !window.confirm('Discard unsaved match-type changes? Accepted and rejected decisions are kept.')) return;
    navigationLock.current = true; setNavigationPending(true);
    try { await emit(event); }
    finally { navigationLock.current = false; setNavigationPending(false); }
  }
  const navigate = (stage: Stage) => sendNavigation(partialReview && stage === 'review' ? { type: 'review_partial_results' } : { type: 'navigate', stage });
  const openHandoff = () => { setHandoffError(''); setHandoffOpen(true); void emit({ type: 'dismiss_rdf_handoff' }); };

  const activeIndex = Math.max(0, stages.findIndex((stage) => stage.id === activeStage));
  const ready = Boolean(readiness.ready);
  const hasResults = Boolean(runStatus.finished || Number(runStatus.processed_count) > 0 || review.items?.some(item => ['matched', 'candidate_suggested', 'accepted', 'rejected', 'no_match', 'timeout'].includes(String(item.status))));
  const navLocked = busy || navigationPending || running;
  // Continue's gate out of each stage (the last one is the RDF hand-off). The stepper walks the
  // same gates, so it never reaches a stage Continue would not; earlier stages stay open like Back.
  // Existing results keep Review and Export reachable when setup breaks later (e.g. a missing
  // key); Start stays gated on readiness on the Run stage.
  const canLeave = [ready || hasResults, hasResults, hasResults, hasResults];
  const reachable = (index: number) => index <= activeIndex || canLeave.slice(activeIndex, index).every(Boolean);
  const counts = review.counts;
  const openCount = OPEN_STATUSES.reduce((sum, key) => sum + Number(counts?.[key] ?? 0), 0);
  const decidedCount = Number(counts?.accepted ?? 0) + Number(counts?.rejected ?? 0) + Number(counts?.no_match ?? 0);
  const completed = [ready, Boolean(runStatus.finished), hasResults && decidedCount > 0 && openCount === 0, Boolean(args?.exportPayload?.content) || args?.rdf_handoff?.status === 'complete'];
  const steps: WizardStep[] = stages.map((stage, index) => ({ id: stage.id, label: stage.label, completed: completed[index], disabled: navLocked || !reachable(index) }));

  // The failing check's own detail stays in "Run prerequisites"; the hint names the fix.
  const blockedCheck = readiness.checks?.find(check => !check.ok);
  const setupFix = (blockedCheck && SETUP_FIXES[blockedCheck.key]) || 'Complete the run prerequisites';
  const hint = running ? 'Reconciliation is running. Stop the run to change steps.'
    : navigationPending || busy ? 'Saving changes…'
    : activeStage === 'setup' ? ready ? 'Next: start the agents.' : hasResults ? `Next: check the existing results. ${setupFix} before starting a new run.` : `${setupFix} to continue.`
    : !hasResults ? activeStage === 'run' ? 'Start reconciliation to continue.' : 'Run reconciliation to continue.'
    : activeStage === 'run' ? partialReview ? 'Next: review the terms completed before the run stopped.' : 'Next: review the mapping suggestions.'
    : activeStage === 'review' ? 'Next: export the reviewed mappings.'
    : 'Next: send the current mappings to the RDF generator.';
  const previous = stages[activeIndex - 1];
  const following = stages[activeIndex + 1];
  const copy = STAGE_COPY[activeStage];
  // The RDF generator already holds mappings: the confirm button names the overwrite.
  const replaceInRdf = args?.rdf_handoff?.status === 'confirmation_required';

  let page: React.ReactNode;
  if (activeStage === 'setup') page = <SetupPage config={config} dataStatus={dataStatus} readiness={readiness} hasResults={hasResults} continueAvailable={canLeave[0]} providerLabels={providerLabels} modelLabels={modelLabels} ontologyOptions={ontologyOptions} update={update} emit={emit} />;
  else if (activeStage === 'run') page = <RunPage config={config} readiness={readiness} runStatus={runStatus} telemetry={telemetry} dataStatus={dataStatus} optimisticRunning={optimisticRunning} optimisticStartedAt={optimisticStartedAt} continueAvailable={hasResults} onStart={startRun} onSkipFailedTerm={skipFailedTerm} emit={emit} />;
  else if (activeStage === 'review') page = <ReviewPage review={review} dataStatus={dataStatus} emit={emit} onDraftChange={onReviewDraft} />;
  else page = <ExportPage review={review} dataStatus={dataStatus} exportPayload={args?.exportPayload} onExport={requestExport} />;
  return <WizardPage
    steps={steps}
    activeIndex={activeIndex}
    onStepClick={(index) => { if (index !== activeIndex) navigate(stages[index].id); }}
    title={copy.title}
    lead={copy.lead}
    info={copy.info}
    actions={<>
      <Tooltip title="Provider, model, keys, monitoring and metadata" describeChild><Button variant="outlined" startIcon={<SettingsOutlinedIcon />} onClick={() => setOptionsOpen(true)}>Options</Button></Tooltip>
      <ToolMenu items={[{ label: 'Reset AI-assisted matching', icon: <RestartAltIcon fontSize="small" />, disabled: navLocked, onSelect: () => { void emit({ type: 'reset_workflow' }); }, confirm: { title: 'Reset AI-assisted matching?', message: 'This clears the loaded table, the agent results, the run record and uploaded definition sources. Model, prompt and provenance settings are kept, and mappings already sent to the RDF generator stay there. You can undo this from History.', confirmLabel: 'Reset AI-assisted matching' } }]} />
    </>}
    status={args?.statusMessage?.text ? <Alert severity={args.statusMessage.severity ?? 'info'}>{args.statusMessage.text}</Alert> : undefined}
    nav={{
      // On the first stage Back falls through to the shell (previous tool).
      back: previous ? { onClick: () => navigate(previous.id), disabled: navLocked, tooltip: `Back to ${previous.label}` } : { disabled: navLocked },
      next: following
        ? { onClick: () => navigate(following.id), disabled: navLocked || !canLeave[activeIndex], tooltip: `Continue to ${following.label}` }
        : { onClick: openHandoff, disabled: navLocked || !hasResults, tooltip: 'Continue to RDF generator' },
      hint,
    }}
  >
    {page}
    <Dialog open={handoffOpen} onClose={handoffBusy ? undefined : () => { setHandoffOpen(false); void emit({ type: 'dismiss_rdf_handoff' }); }} fullWidth maxWidth="sm" aria-labelledby="agent-rdf-handoff-title">
      <DialogTitle id="agent-rdf-handoff-title">Send mappings to the RDF generator?</DialogTitle>
      <DialogContent><Stack spacing={2}>
        <DialogContentText>The current mapping table will be loaded into the RDF generator, and you will continue there. This does not generate RDF automatically.</DialogContentText>
        <MetaList items={[
          { label: 'Terms', value: formatCount(dataStatus.rows ?? 0), numeric: true },
          { label: 'Accepted mappings', value: formatCount(counts?.accepted ?? 0), numeric: true },
          { label: 'Still open', value: formatCount(openCount), numeric: true },
        ]} />
        <Typography variant="body2" color="text.secondary">Confirmed mappings are transferred; terms without a confirmed mapping remain unmapped.</Typography>
        {replaceInRdf && <Alert severity="warning">{args?.rdf_handoff?.message}</Alert>}
        {handoffError && <Alert severity="error">{handoffError}</Alert>}
        {!args?.rdf_handoff_revision && <Alert severity="warning">The updated backend must be running before this handoff is available.</Alert>}
      </Stack></DialogContent>
      <DialogActions>
        <Button autoFocus disabled={handoffBusy} onClick={() => { setHandoffOpen(false); void emit({ type: 'dismiss_rdf_handoff' }); }}>Cancel</Button>
        <Button variant="contained" color={replaceInRdf ? 'warning' : 'primary'} disabled={handoffBusy || Boolean(runStatus.running) || !args?.rdf_handoff_revision} onClick={async () => {
          setHandoffBusy(true); setHandoffError('');
          try {
            const result = await emit({ type: 'handoff_rdf_generator', expected_revision: args?.rdf_handoff_revision, confirmation_token: args?.rdf_handoff?.confirmation_token }) as any;
            // The shell's request error sits behind this modal, so the dialog says it too.
            if (!result || result === false) setHandoffError('The transfer did not complete. Try again.');
            else if (result.args?.rdf_handoff?.status === 'complete') setHandoffOpen(false);
            else if (result.args?.rdf_handoff?.status !== 'confirmation_required') setHandoffError(result.args?.statusMessage?.text || 'The table changed. Review it before trying again.');
          } catch (error) { setHandoffError(error instanceof Error ? error.message : 'The transfer failed.'); }
          finally { setHandoffBusy(false); }
        }}>{handoffBusy ? 'Sending…' : replaceInRdf ? 'Replace mappings in RDF generator' : 'Send to RDF generator'}</Button>
      </DialogActions>
    </Dialog>
    <AgentOptionsDialog
      open={optionsOpen}
      onClose={() => setOptionsOpen(false)}
      config={config}
      providers={providerOptions}
      providerLabels={providerLabels}
      modelOptions={modelOptions}
      modelLabels={modelLabels}
      modelDetails={args?.modelDetails}
      reasoningOptions={reasoningOptions}
      providerKind={providerKind}
      codexAuthStatus={args?.codexAuthStatus}
      statusMessage={args?.statusMessage}
      update={update}
      emit={emit}
    />
  </WizardPage>;
}

export const WorkflowConfigPanel = AgentReconciliationMuiApp;
