import React, { useEffect, useRef, useState } from 'react';
import { Alert, Box, Chip, Link, Stack, Tab, Tabs, Tooltip, Typography } from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import RefreshIcon from '@mui/icons-material/Refresh';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import { emitAppEvent, notifyLayoutChanged, type AppEvent } from '../../shared/appBridge';
import { DataTable, type DataTableProps } from '../../components/table/DataTable';
import { DownloadButton, PrepareButton, UploadButton, UseResultButton } from '../../components/actions/WorkflowActionButtons';
import { ToolMenu } from '../../components/actions/ToolMenu';
import type { WorkflowNavProps } from '../../components/actions/WorkflowNav';
import { InfoButton, type InfoContent } from '../../components/help/InfoButton';
import { WizardPage, type WizardStep } from '../../components/layout/WizardPage';
import { Section } from '../../components/layout/Section';
import { Disclosure } from '../../components/layout/Disclosure';
import { MetaList } from '../../components/layout/MetaList';
import { KpiStrip } from '../../components/layout/KpiStrip';
import { EmptyState } from '../../components/states/EmptyState';

type Severity = 'success' | 'info' | 'warning' | 'error';
type TableRow = Record<string, unknown>;
type Stage = 'load' | 'preview' | 'metadata' | 'statistics' | 'export';

type NamedGraphSubject = { subject_uri?: string; subject_type?: string; properties?: TableRow[] };
type NamedGraphSection = { graph_uri?: string; triple_count?: number; subjects?: NamedGraphSubject[] } | null;

type Snapshot = {
  active_stage?: Stage;
  statusMessage?: { severity?: Severity; text?: string } | null;
  source?: { has_data?: boolean; filename?: string; catalog_available?: boolean };
  data?: { rows?: number; columns?: number; preview?: TableRow[] };
  metadata?: { dcat?: NamedGraphSection; publication?: NamedGraphSection; other_graphs?: NamedGraphSection[] };
  statistics?: {
    total_triples?: number;
    subjects?: number;
    properties?: number;
    external_matches?: number;
    exact_matches?: number;
    close_matches?: number;
    namespaces?: TableRow[];
    property_catalog?: TableRow[];
  };
  downloads?: {
    csv?: string;
    csv_filename?: string;
    excel_base64?: string;
    excel_filename?: string;
    markdown?: string;
    markdown_filename?: string;
    excel_error?: string;
    markdown_error?: string;
  };
};

type AppProps = { args?: { app?: string; snapshot?: Snapshot }; busy?: boolean };

const stages: { id: Stage; label: string; title: string; lead: string; info: InfoContent }[] = [
  {
    id: 'load', label: 'Load RDF', title: 'Choose the RDF catalog to inspect', lead: 'Use the catalog from the RDF generator, or upload a TriG file.',
    info: { title: 'Loading a catalog', content: <><p>The inspector reads TriG files (<code>.trig</code>): the data graphs plus named graphs for metadata and publication references.</p><p><strong>Use generated catalog</strong> loads the DCAT catalog made with the RDF generator’s optional metadata tools. Loading another catalog replaces the current one and clears prepared exports.</p></> },
  },
  {
    id: 'preview', label: 'Inspect RDF', title: 'Explore your RDF results', lead: 'Browse the subjects, then switch to metadata or statistics as needed.',
    info: { title: 'Inspecting the graph', content: <ul><li><strong>Subjects</strong>: one row per subject in the data graphs.</li><li><strong>Metadata</strong>: DCAT, publication references and other named graphs.</li><li><strong>Statistics</strong>: counts, property catalog, namespaces and SKOS matches.</li></ul> },
  },
  {
    id: 'export', label: 'Export', title: 'Download tables and documentation', lead: 'Prepare the exports, then download Excel, CSV or Markdown for your next task.',
    info: { title: 'Exporting tables', content: <><p>Exports are built on the server from the loaded catalog.</p><p>Loading another catalog or resetting the RDF inspector clears them, so prepare them again afterwards.</p></> },
  },
];
const inspectStages: Stage[] = ['preview', 'metadata', 'statistics'];
const inspectTabs: { id: Stage; label: string }[] = [{ id: 'preview', label: 'Subjects' }, { id: 'metadata', label: 'Metadata' }, { id: 'statistics', label: 'Statistics' }];
const SUBJECT_COLUMNS = 12;

function emit(event: AppEvent) { emitAppEvent(event); }

const numberFormat = new Intl.NumberFormat();
// Missing counts stay empty so KpiStrip/MetaList print a grey dash; a real 0 prints as 0.
function formatCount(value?: number | null) { return typeof value === 'number' ? numberFormat.format(value) : ''; }

async function fileToBase64(file: File) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = '';
  bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
  return window.btoa(binary);
}

function triggerDownload(content: string, filename: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function triggerBase64Download(base64: string, filename: string, mime: string) {
  const binary = window.atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  const blob = new Blob([bytes], { type: mime });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// The backend sends linked values as "[label](https://…)"; internal concepts arrive as plain labels.
function renderMarkdownLink(value: unknown) {
  const text = String(value ?? '');
  const match = text.match(/^\[([^\]]+)\]\((https?:\/\/[^)]+)\)$/);
  if (!match) return text;
  return <Link href={match[2]} target="_blank" rel="noreferrer" underline="hover" aria-label={`${match[1]} (opens in a new tab)`}>{match[1]}<OpenInNewIcon sx={{ fontSize: '0.875em', ml: 0.25, verticalAlign: '-0.125em' }} /></Link>;
}

// The shared DataTable with linked values rendered as links (a stable renderer keeps its columns memoised).
function RdfTable({ rows, empty, maxColumns = 10 }: Pick<DataTableProps, 'rows' | 'empty' | 'maxColumns'>) {
  return <DataTable rows={rows} empty={empty} maxColumns={maxColumns} minWidth={820} renderCell={renderMarkdownLink} />;
}

// Processing errors carry a Python traceback (tablegenerator.py); keep the message inline and the trace collapsed.
function StatusMessage({ severity, text }: { severity?: Severity; text: string }) {
  const cut = text.indexOf('\nTraceback (most recent call last):');
  const summary = cut >= 0 ? text.slice(0, cut) : text;
  const details = cut >= 0 ? text.slice(cut + 1) : '';
  return <>
    <Alert severity={severity ?? 'info'} sx={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{summary}</Alert>
    {details && <Disclosure title="Technical details" summary="Server traceback, useful for bug reports"><Box component="pre" sx={{ m: 0, fontSize: '0.75rem', whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', maxHeight: 320, overflow: 'auto' }}>{details}</Box></Disclosure>}
  </>;
}

function LoadedContext({ snapshot }: { snapshot: Snapshot }) {
  const filename = snapshot.source?.filename;
  return <Stack direction="row" spacing={1} useFlexGap flexWrap="wrap" alignItems="center">
    <Typography variant="body2" color="text.secondary">Loaded</Typography>
    <Chip size="small" variant="outlined" title={filename || undefined} label={filename ? <Box component="code" sx={{ fontSize: '0.75rem' }}>{filename}</Box> : 'TriG catalog'} sx={{ maxWidth: '100%' }} />
    <Chip size="small" variant="outlined" label={`${formatCount(snapshot.data?.rows ?? 0)} subject rows`} sx={{ fontVariantNumeric: 'tabular-nums' }} />
    <Chip size="small" variant="outlined" label={`${formatCount(snapshot.statistics?.total_triples ?? 0)} triples`} sx={{ fontVariantNumeric: 'tabular-nums' }} />
  </Stack>;
}

function LoadPage({ snapshot, busy }: { snapshot: Snapshot; busy: boolean }) {
  const [uploadError, setUploadError] = useState('');
  const [reading, setReading] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const loaded = Boolean(snapshot.source?.has_data);
  const catalogAvailable = Boolean(snapshot.source?.catalog_available);
  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]; event.target.value = ''; setUploadError(''); if (!file) return;
    if (!file.name.toLowerCase().endsWith('.trig')) { setUploadError('This file is not a TriG file. Choose a .trig file.'); return; }
    setReading(true);
    try { emit({ type: 'upload_trig', filename: file.name, content_base64: await fileToBase64(file) }); } catch { setUploadError('This file could not be read. Choose it again or use another file.'); } finally { setReading(false); }
  }
  // One contained action while Continue is still disabled: the generated catalog when there is one, otherwise the upload.
  return <Section title="RDF catalog" description={<>TriG (<Box component="code">.trig</Box>).</>}>
    <Stack spacing={2}>
      {loaded
        ? <MetaList items={[
          { label: 'File', value: snapshot.source?.filename, mono: true },
          { label: 'Subject rows', value: formatCount(snapshot.data?.rows), numeric: true },
          { label: 'Triples', value: formatCount(snapshot.statistics?.total_triples), numeric: true },
        ]} />
        : <Typography variant="body2" color="text.secondary">No catalog loaded yet.</Typography>}
      <Stack direction="row" useFlexGap flexWrap="wrap" gap={1}>
        <Tooltip title={catalogAvailable ? '' : 'Available once the RDF generator has created a DCAT catalog.'}>
          <span><UseResultButton variant={catalogAvailable && !loaded ? 'contained' : 'outlined'} disabled={busy || reading || !catalogAvailable} onClick={() => emit({ type: 'load_catalog' })}>Use generated catalog</UseResultButton></span>
        </Tooltip>
        <input ref={fileInput} hidden type="file" accept=".trig,application/trig,text/plain" onChange={handleUpload} />
        <UploadButton variant={!loaded && !catalogAvailable ? 'contained' : 'outlined'} disabled={busy || reading} onClick={() => fileInput.current?.click()}>
          {reading ? 'Reading file…' : loaded ? 'Replace TriG file' : 'Upload TriG file'}
        </UploadButton>
      </Stack>
      {uploadError && <Alert severity="error">{uploadError}</Alert>}
    </Stack>
  </Section>;
}

function SubjectsPage({ snapshot }: { snapshot: Snapshot }) {
  const rows = snapshot.data?.rows ?? 0;
  const columns = snapshot.data?.columns ?? 0;
  const preview = snapshot.data?.preview ?? [];
  // Rows only: the table's own caption reports hidden columns and follows the Columns menu.
  return <Stack spacing={1.5}>
    {rows > 0 && <Stack direction="row" alignItems="center" spacing={0.5}>
      <Typography variant="body2" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums' }}>{preview.length < rows ? `Showing ${formatCount(preview.length)} of ${formatCount(rows)} rows; the CSV export has them all.` : `${formatCount(rows)} rows, ${formatCount(columns)} columns.`}</Typography>
      <InfoButton title="Subject rows"><p>The table has one row per subject in the data graphs and one column per property.</p><p>URI values show as labels. Linked labels open the resource, or its external SKOS match, in a new tab; internal concept URIs stay plain labels.</p></InfoButton>
    </Stack>}
    <RdfTable rows={preview} empty="No subject data found in the TriG file." maxColumns={SUBJECT_COLUMNS} />
  </Stack>;
}

function GraphSection({ title, section, empty }: { title: string; section: NonNullable<NamedGraphSection>; empty: string }) {
  const subjects = section.subjects ?? [];
  return <Section title={title} description={section.graph_uri ? <Box component="code" sx={{ fontSize: '0.8125rem', overflowWrap: 'anywhere' }}>{section.graph_uri}</Box> : undefined} actions={<Chip size="small" variant="outlined" label={`${formatCount(section.triple_count ?? 0)} triples`} sx={{ fontVariantNumeric: 'tabular-nums' }} />}>
    {subjects.length ? <Box>{subjects.map((subject, idx) => <Disclosure plain key={`${subject.subject_uri}-${idx}`} defaultExpanded={idx < 2} title={subject.subject_type || 'Resource'} summary={subject.subject_uri ? <Box component="code" sx={{ wordBreak: 'break-all' }}>{subject.subject_uri}</Box> : undefined}>
      {subject.properties?.length ? <MetaList items={subject.properties.map((row) => { const [property, value] = Object.values(row); return { label: String(property ?? ''), value: renderMarkdownLink(value) }; })} /> : <Typography variant="body2" color="text.secondary">No properties found for this subject.</Typography>}
    </Disclosure>)}</Box> : <Typography variant="body2" color="text.secondary">{empty}</Typography>}
  </Section>;
}

function MetadataPage({ snapshot }: { snapshot: Snapshot }) {
  const metadata = snapshot.metadata ?? {};
  const otherGraphs = (metadata.other_graphs ?? []).filter((graph): graph is NonNullable<NamedGraphSection> => Boolean(graph));
  if (!metadata.dcat && !metadata.publication && !otherGraphs.length) return <EmptyState dense title="No metadata graphs in this catalog" description="Subjects and statistics are still available." />;
  return <Stack spacing={3}>
    {metadata.dcat && <GraphSection title="DCAT metadata" section={metadata.dcat} empty="No DCAT properties found." />}
    {metadata.publication && <GraphSection title="Publication references" section={metadata.publication} empty="No publication properties found." />}
    {otherGraphs.map((graph, idx) => <GraphSection key={`${graph.graph_uri}-${idx}`} title={`Additional named graph ${idx + 1}`} section={graph} empty="No properties found in this named graph." />)}
  </Stack>;
}

const statisticsViews = ['Property catalog', 'Namespaces', 'URI mappings'];

function StatisticsPage({ snapshot }: { snapshot: Snapshot }) {
  const stats = snapshot.statistics ?? {}; const [tab, setTab] = useState(0);
  return <Stack spacing={3}>
    <KpiStrip ariaLabel="Dataset statistics" items={[
      { label: 'Total triples', value: formatCount(stats.total_triples) },
      { label: 'Subjects', value: formatCount(stats.subjects) },
      { label: 'Properties', value: formatCount(stats.properties) },
      { label: 'External matches', value: formatCount(stats.external_matches), hint: `${formatCount(stats.exact_matches ?? 0)} exact, ${formatCount(stats.close_matches ?? 0)} close` },
    ]} />
    <Section title="Properties, namespaces and mappings">
      <Tabs value={tab} onChange={(_, value: number) => setTab(value)} variant="scrollable" allowScrollButtonsMobile aria-label="Statistics details" sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}>
        {statisticsViews.map((label, position) => <Tab key={label} label={label} id={`statistics-tab-${position}`} aria-controls="statistics-panel" />)}
      </Tabs>
      <Box role="tabpanel" id="statistics-panel" aria-labelledby={`statistics-tab-${tab}`}>
        {tab === 0 && <RdfTable rows={stats.property_catalog} empty="No property catalog found." maxColumns={4} />}
        {tab === 1 && <RdfTable rows={stats.namespaces} empty="No namespaces extracted." maxColumns={2} />}
        {tab === 2 && <MetaList items={[
          { label: <>Exact matches (<code>skos:exactMatch</code>)</>, value: formatCount(stats.exact_matches ?? 0), numeric: true },
          { label: <>Close matches (<code>skos:closeMatch</code>)</>, value: formatCount(stats.close_matches ?? 0), numeric: true },
        ]} />}
      </Box>
    </Section>
  </Stack>;
}

function ExportFormat({ title, description, filename, error, children }: { title: string; description: string; filename?: string; error?: string; children: React.ReactNode }) {
  return <Section title={title} description={description}>
    <Stack spacing={1.5}>
      {error && <Alert severity="warning">{error}</Alert>}
      <MetaList items={[{ label: 'File', value: filename, mono: true }]} />
      <Box>{children}</Box>
    </Stack>
  </Section>;
}

function ExportPage({ snapshot, busy }: { snapshot: Snapshot; busy: boolean }) {
  const downloads = snapshot.downloads ?? {};
  const loaded = Boolean(snapshot.source?.has_data);
  const ready = Boolean(downloads.csv || downloads.excel_base64 || downloads.markdown);
  // The page is inert while the request runs, which drops focus to <body>. One button (not two
  // that swap) keeps its DOM node, and focus goes back to it once the request finishes.
  const prepareSlot = useRef<HTMLSpanElement>(null);
  const restoreFocus = useRef(false);
  useEffect(() => {
    if (busy || !restoreFocus.current) return;
    restoreFocus.current = false;
    const active = document.activeElement;
    if (!active || active === document.body) prepareSlot.current?.querySelector('button')?.focus();
  }, [busy]);
  const prepare = () => { restoreFocus.current = true; emit({ type: 'prepare_downloads' }); };
  return <Stack spacing={3}>
    <Section
      title={ready ? 'Your exports are ready' : 'Prepare your exports'}
      description={ready ? undefined : loaded ? 'The download buttons become available once the exports are prepared.' : 'Load a TriG file before preparing exports.'}
      actions={<span ref={prepareSlot}><PrepareButton variant={ready ? 'outlined' : 'contained'} startIcon={ready ? <RefreshIcon /> : undefined} disabled={busy || !loaded} onClick={prepare}>{ready ? 'Refresh downloads' : 'Prepare downloads'}</PrepareButton></span>}
    />
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(3, minmax(0, 1fr))' }, gap: 2 }}>
      <ExportFormat title="Excel workbook" description="Subject data and property mappings, with resource links." filename={downloads.excel_base64 ? downloads.excel_filename : undefined} error={downloads.excel_error}>
        <DownloadButton disabled={busy || !downloads.excel_base64} onClick={() => triggerBase64Download(downloads.excel_base64 ?? '', downloads.excel_filename ?? 'rdf_table_output.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}>Download tables (Excel)</DownloadButton>
      </ExportFormat>
      <ExportFormat title="CSV table" description="The full subject table for further processing." filename={downloads.csv ? downloads.csv_filename : undefined}>
        <DownloadButton disabled={busy || !downloads.csv} onClick={() => triggerDownload(downloads.csv ?? '', downloads.csv_filename ?? 'rdf_table_output.csv', 'text/csv;charset=utf-8')}>Download subject table (CSV)</DownloadButton>
      </ExportFormat>
      <ExportFormat title="Markdown documentation" description="Metadata, references, namespaces and properties." filename={downloads.markdown ? downloads.markdown_filename : undefined} error={downloads.markdown_error}>
        <DownloadButton disabled={busy || !downloads.markdown} onClick={() => triggerDownload(downloads.markdown ?? '', downloads.markdown_filename ?? 'rdf_table_metadata.md', 'text/markdown;charset=utf-8')}>Download documentation (Markdown)</DownloadButton>
      </ExportFormat>
    </Box>
  </Stack>;
}

export function RDFToTableApp({ args, busy = false }: AppProps) {
  const snapshot = args?.snapshot ?? {};
  const activeStage = (snapshot.active_stage && [...stages.map(stage => stage.id), ...inspectStages].includes(snapshot.active_stage) ? snapshot.active_stage : 'load') as Stage;
  const loaded = Boolean(snapshot.source?.has_data);
  const downloads = snapshot.downloads ?? {};
  const ready = Boolean(downloads.csv || downloads.excel_base64 || downloads.markdown);
  const index = activeStage === 'load' ? 0 : activeStage === 'export' ? 2 : 1;
  const meta = stages[index];
  // The backend does not remember the Inspect sub-view, so the client does: the stepper, Continue
  // from Load and Back from Export return to the view the curator last had open.
  const lastInspect = useRef<Stage>(inspectStages.includes(activeStage) ? activeStage : 'preview');
  useEffect(() => { if (inspectStages.includes(activeStage)) lastInspect.current = activeStage; }, [activeStage]);
  useEffect(() => { notifyLayoutChanged(); }, [snapshot, activeStage]);
  const navigate = (stage: Stage) => emit({ type: 'navigate', stage });
  // Everything past Load needs loaded data, exactly like Continue; nothing moves while a request runs.
  // Inspect counts as done once the curator has moved on to Export or prepared the exports.
  const completed = [loaded, loaded && (index > 1 || ready), loaded && ready];
  const steps: WizardStep[] = stages.map((stage, position) => ({ id: stage.id, label: stage.label, completed: completed[position], disabled: busy || (position > 0 && !loaded) }));
  const busyHint = 'Saving changes…';
  const nav: WorkflowNavProps = index === 0
    ? { back: { disabled: busy }, next: { onClick: () => navigate(lastInspect.current), disabled: busy || !loaded, tooltip: 'Continue to Inspect RDF' }, hint: busy ? busyHint : loaded ? 'Next: inspect subjects, metadata and statistics.' : 'Load a TriG file to continue.' }
    : index === 1
      ? { back: { onClick: () => navigate('load'), disabled: busy, tooltip: 'Back to Load RDF' }, next: { onClick: () => navigate('export'), disabled: busy || !loaded, tooltip: 'Continue to Export' }, hint: busy ? busyHint : loaded ? 'Next: export tables and documentation.' : 'Load a TriG file to continue.' }
      : { back: { onClick: () => navigate(lastInspect.current), disabled: busy, tooltip: 'Back to Inspect RDF' }, hint: busy ? busyHint : !loaded ? 'Load a TriG file to prepare exports.' : ready ? 'Workflow complete — download your tables above.' : 'Next: prepare the downloads, then save the formats you need.' };
  const page = activeStage === 'load' ? <LoadPage snapshot={snapshot} busy={busy} />
    : activeStage === 'preview' ? <SubjectsPage snapshot={snapshot} />
      : activeStage === 'metadata' ? <MetadataPage snapshot={snapshot} />
        : activeStage === 'statistics' ? <StatisticsPage snapshot={snapshot} />
          : <ExportPage snapshot={snapshot} busy={busy} />;
  return <WizardPage
    steps={steps}
    activeIndex={index}
    onStepClick={(position) => { if (position !== index) navigate(position === 1 ? lastInspect.current : stages[position].id); }}
    title={meta.title}
    lead={meta.lead}
    info={meta.info}
    actions={<ToolMenu items={[{ label: 'Reset RDF inspector', icon: <RestartAltIcon fontSize="small" />, disabled: busy, onSelect: () => emit({ type: 'reset_workflow' }), confirm: { title: 'Reset RDF inspector?', message: 'This clears the loaded TriG catalog, the extracted tables and statistics, and any prepared downloads. You can undo this from History.', confirmLabel: 'Reset RDF inspector' } }]} />}
    status={snapshot.statusMessage?.text ? <StatusMessage severity={snapshot.statusMessage.severity} text={snapshot.statusMessage.text} /> : undefined}
    nav={nav}
  >
    <Stack spacing={2}>
      {/* Load lists the same facts in its MetaList; later stages keep them as a context line. */}
      {loaded && index > 0 && <LoadedContext snapshot={snapshot} />}
      {/* Tabs stay outside the inert fieldset so keyboard focus survives the server round trip of a tab switch. */}
      {index === 1 && <Tabs value={activeStage} onChange={(_, stage: Stage) => { if (!busy) navigate(stage); }} variant="scrollable" allowScrollButtonsMobile aria-label="Inspect views" sx={{ borderBottom: 1, borderColor: 'divider' }}>
        {inspectTabs.map((tab) => <Tab key={tab.id} value={tab.id} label={tab.label} id={`inspect-${tab.id}`} aria-controls="inspect-panel" />)}
      </Tabs>}
      <Box {...(index === 1 ? { role: 'tabpanel', id: 'inspect-panel', 'aria-labelledby': `inspect-${activeStage}` } : {})}>
        <Box component="fieldset" disabled={busy} {...{ inert: busy ? '' : undefined }} aria-busy={busy} sx={{ border: 0, p: 0, m: 0, minWidth: 0 }}>{page}</Box>
      </Box>
    </Stack>
  </WizardPage>;
}
