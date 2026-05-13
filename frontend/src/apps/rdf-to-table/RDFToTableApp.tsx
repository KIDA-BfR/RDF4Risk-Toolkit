import React, { useEffect, useMemo, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  Paper,
  Stack,
  Step,
  StepButton,
  Stepper,
  Tab,
  Tabs,
  Typography,
} from '@mui/material';
import { emitAppEvent, notifyLayoutChanged, type AppEvent } from '../../shared/appBridge';

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

type AppProps = { args?: { app?: string; snapshot?: Snapshot } };

const stages: { id: Stage; label: string; caption: string }[] = [
  { id: 'load', label: 'Load', caption: 'TriG input' },
  { id: 'preview', label: 'Preview', caption: 'Tabular data' },
  { id: 'metadata', label: 'Metadata', caption: 'Named graphs' },
  { id: 'statistics', label: 'Statistics', caption: 'Catalogs' },
  { id: 'export', label: 'Export', caption: 'Downloads' },
];

function emit(event: AppEvent) { emitAppEvent(event); }

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

const TableIcon = () => (
  <svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg" role="img" style={{ marginRight: 12 }}>
    <defs><linearGradient id="rdfTableGradient" x1="8" y1="10" x2="56" y2="54" gradientUnits="userSpaceOnUse"><stop stopColor="#2563EB"/><stop offset=".52" stopColor="#0891B2"/><stop offset="1" stopColor="#14B8A6"/></linearGradient><filter id="rdfTableShadow" x="-40%" y="-40%" width="180%" height="180%"><feDropShadow dx="0" dy="5" stdDeviation="5" floodColor="#0F172A" floodOpacity="0.18"/></filter></defs>
    <circle cx="18" cy="18" r="7" fill="#fff" stroke="#2563EB" strokeWidth="2.4" filter="url(#rdfTableShadow)" />
    <circle cx="46" cy="19" r="8" fill="url(#rdfTableGradient)" filter="url(#rdfTableShadow)" />
    <circle cx="20" cy="46" r="8" fill="#fff" stroke="#14B8A6" strokeWidth="2.4" />
    <path d="M25 19h13M41 25L26 40M19 25l1 13" stroke="url(#rdfTableGradient)" strokeWidth="2.8" strokeLinecap="round" />
    <rect x="32" y="35" width="22" height="17" rx="4" fill="#FFFFFF" stroke="url(#rdfTableGradient)" strokeWidth="2.2" />
    <path d="M32 41h22M39 35v17M47 35v17" stroke="#CBD5E1" strokeWidth="1.5" />
  </svg>
);

function renderMarkdownLink(value: unknown) {
  const text = String(value ?? '');
  const match = text.match(/^\[([^\]]+)\]\((https?:\/\/[^)]+)\)$/);
  if (!match) return text;
  return <a href={match[2]} target="_blank" rel="noreferrer">{match[1]}</a>;
}

function DataTable({ rows, empty, maxColumns = 10 }: { rows?: TableRow[]; empty: string; maxColumns?: number }) {
  const safeRows = rows ?? [];
  const columns = safeRows.length ? Object.keys(safeRows[0]).slice(0, maxColumns) : [];
  if (!safeRows.length) return <Typography variant="body2" color="text.secondary">{empty}</Typography>;
  return <Box sx={{ overflow: 'auto', border: '1px solid', borderColor: 'divider', borderRadius: 2 }}><Box component="table" sx={{ width: '100%', borderCollapse: 'collapse', minWidth: 820 }}><thead><tr>{columns.map((column) => <Box component="th" key={column} sx={{ p: 1, textAlign: 'left', fontSize: 12, bgcolor: '#f8fafc', position: 'sticky', top: 0 }}>{column}</Box>)}</tr></thead><tbody>{safeRows.slice(0, 100).map((row, idx) => <tr key={idx}>{columns.map((column) => <Box component="td" key={column} sx={{ p: 1, borderTop: '1px solid #e2e8f0', fontSize: 12, maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{renderMarkdownLink(row[column])}</Box>)}</tr>)}</tbody></Box></Box>;
}

function SummaryRow({ label, value }: { label: string; value: React.ReactNode }) { return <Stack direction="row" justifyContent="space-between" spacing={2}><Typography variant="body2" color="text.secondary">{label}</Typography><Typography variant="body2" sx={{ fontWeight: 800, textAlign: 'right' }}>{value}</Typography></Stack>; }

function MetricCard({ label, value, helper }: { label: string; value: React.ReactNode; helper?: string }) { return <Paper variant="outlined" sx={{ p: 1.5, borderRadius: 3 }}><Typography variant="caption" color="text.secondary">{label}</Typography><Typography variant="h6" sx={{ fontWeight: 900 }}>{value}</Typography>{helper && <Typography variant="caption" color="text.secondary">{helper}</Typography>}</Paper>; }

function AppShell({ snapshot, activeStage, children }: { snapshot: Snapshot; activeStage: Stage; children: React.ReactNode }) {
  const source = snapshot.source ?? {}; const data = snapshot.data ?? {}; const stats = snapshot.statistics ?? {};
  const activeStep = stages.findIndex((stage) => stage.id === activeStage);
  return <Box sx={{ bgcolor: '#eef7fb', minHeight: '100vh', p: { xs: 1, md: 2 }, borderRadius: 4 }}><Stack spacing={2}>
    <Paper variant="outlined" sx={{ p: { xs: 2, md: 2.5 }, borderRadius: 4, background: 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 50%,#eff6ff 100%)', boxShadow: '0 18px 48px rgba(15,23,42,.08)' }}>
      <Stack direction={{ xs: 'column', lg: 'row' }} spacing={2} alignItems={{ xs: 'stretch', lg: 'center' }} justifyContent="space-between">
        <Stack direction="row" spacing={1.5} alignItems="center"><TableIcon /><Stack><Typography variant="h5">RDF to Table</Typography><Typography variant="body2" color="text.secondary">Guided workflow for TriG/RDF exploration, metadata inspection, statistics, and Excel/CSV/Markdown export</Typography></Stack></Stack>
        <Stepper nonLinear activeStep={activeStep} sx={{ minWidth: { lg: 700 } }}>{stages.map((stage, idx) => <Step key={stage.id} completed={idx < activeStep}><StepButton onClick={() => emit({ type: 'navigate', stage: stage.id })}><Stack spacing={0}><Typography variant="body2" sx={{ fontWeight: 800 }}>{stage.label}</Typography><Typography variant="caption" color="text.secondary">{stage.caption}</Typography></Stack></StepButton></Step>)}</Stepper>
      </Stack>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', md: 'repeat(4,1fr)' }, gap: 1.2, mt: 2 }}>
        <Chip label={source.has_data ? source.filename || 'TriG loaded' : 'No TriG loaded'} color={source.has_data ? 'success' : 'warning'} />
        <Chip label={`${data.rows ?? 0} table rows`} color={data.rows ? 'info' : 'default'} />
        <Chip label={`${stats.total_triples ?? 0} triples`} color={stats.total_triples ? 'info' : 'default'} />
        <Chip label={`${stats.external_matches ?? 0} external matches`} color={stats.external_matches ? 'success' : 'default'} />
      </Box>
    </Paper>
    {snapshot.statusMessage?.text && <Alert severity={snapshot.statusMessage.severity ?? 'info'} sx={{ whiteSpace: 'pre-wrap' }}>{snapshot.statusMessage.text}</Alert>}
    {children}
  </Stack></Box>;
}

function LoadPage({ snapshot }: { snapshot: Snapshot }) {
  const [uploadError, setUploadError] = useState('');
  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]; event.target.value = ''; setUploadError(''); if (!file) return;
    if (!file.name.toLowerCase().endsWith('.trig')) { setUploadError('Please choose a .trig file.'); return; }
    try { emit({ type: 'upload_trig', filename: file.name, content_base64: await fileToBase64(file) }); } catch (error) { setUploadError(error instanceof Error ? error.message : 'Unable to read the selected file.'); }
  }
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: 'minmax(0,1fr) 360px' }, gap: 2 }}><Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}><Stack><Typography variant="subtitle1">1. Load TriG/RDF Catalog</Typography><Typography variant="body2" color="text.secondary">Upload a TriG file, or load the generated catalog produced by the RDF Generator.</Typography></Stack><Chip label={snapshot.source?.has_data ? 'loaded' : 'waiting'} color={snapshot.source?.has_data ? 'success' : 'warning'} /></Stack><Paper variant="outlined" sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed', borderRadius: 3, bgcolor: '#f8fafc' }}><Typography variant="h6">TriG input</Typography><Typography variant="body2" color="text.secondary" sx={{ mt: .5 }}>Supported: .trig. The backend extracts subject rows, DCAT metadata, publication references, property mappings, namespaces, and SKOS exact/close-match links.</Typography><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} justifyContent="center" sx={{ mt: 1.5 }}><Button component="label" variant="contained">Upload TriG file<input hidden type="file" accept=".trig,application/trig,text/plain" onChange={handleUpload} /></Button><Button variant="outlined" disabled={!snapshot.source?.catalog_available} onClick={() => emit({ type: 'load_catalog' })}>Load Catalog from RDF Generator</Button></Stack>{uploadError && <Alert severity="warning" sx={{ mt: 1.5 }}>{uploadError}</Alert>}</Paper><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1 }}><SummaryRow label="File" value={snapshot.source?.filename || '—'} /><SummaryRow label="Rows" value={snapshot.data?.rows ?? 0} /><SummaryRow label="Columns" value={snapshot.data?.columns ?? 0} /><SummaryRow label="Generator catalog" value={snapshot.source?.catalog_available ? 'Available' : 'Not available'} /></Box></Stack></CardContent></Card></Stack><Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.1}><Typography variant="subtitle1">Workflow guide</Typography>{['Load a generated TriG catalog or upload a .trig file.', 'Preview subject data as a linked table.', 'Inspect DCAT metadata, publication reference, and other named graphs in expandables.', 'Review dataset statistics, property catalog, namespaces, and SKOS match counts.', 'Prepare and download Excel, CSV, and Markdown exports.'].map((item) => <Typography key={item} variant="body2" color="text.secondary">✓ {item}</Typography>)}<Button variant="contained" disabled={!snapshot.source?.has_data} onClick={() => emit({ type: 'navigate', stage: 'preview' })}>Continue to Preview</Button></Stack></CardContent></Card></Stack></Box>;
}

function PreviewPage({ snapshot }: { snapshot: Snapshot }) { return <Stack spacing={2}><Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}><Stack><Typography variant="subtitle1">Data Preview</Typography><Typography variant="body2" color="text.secondary">Subject rows extracted from data graphs. URI values are displayed as clickable labels when labels or external SKOS matches are available.</Typography></Stack><Chip label={`${snapshot.data?.rows ?? 0} rows × ${snapshot.data?.columns ?? 0} columns`} color={snapshot.data?.rows ? 'info' : 'default'} /></Stack><Alert severity="info" variant="outlined">Click a blue linked label to open the target resource in a new tab. Internal concept URIs are rendered as labels without external links.</Alert><DataTable rows={snapshot.data?.preview} empty="No subject data found in the TriG file." maxColumns={12} /><Button variant="contained" disabled={!snapshot.source?.has_data} onClick={() => emit({ type: 'navigate', stage: 'metadata' })}>Continue to Metadata</Button></Stack></CardContent></Card></Stack>; }

function GraphSection({ title, section, empty }: { title: string; section: NamedGraphSection; empty: string }) {
  if (!section) return <Alert severity="info" variant="outlined">{empty}</Alert>;
  return <Card variant="outlined"><CardContent><Stack spacing={1.2}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}><Stack><Typography variant="subtitle1">{title}</Typography><Typography variant="caption" color="text.secondary">{section.graph_uri}</Typography></Stack><Chip label={`${section.triple_count ?? 0} triples`} /></Stack>{(section.subjects ?? []).map((subject, idx) => <Accordion key={`${subject.subject_uri}-${idx}`} defaultExpanded={idx < 2}><AccordionSummary><Stack><Typography sx={{ fontWeight: 850 }}>{subject.subject_type || 'Resource'}</Typography><Typography variant="caption" color="text.secondary" sx={{ wordBreak: 'break-all' }}>{subject.subject_uri}</Typography></Stack></AccordionSummary><AccordionDetails><DataTable rows={subject.properties} empty="No properties found for this subject." maxColumns={2} /></AccordionDetails></Accordion>)}</Stack></CardContent></Card>;
}

function MetadataPage({ snapshot }: { snapshot: Snapshot }) {
  const otherGraphs = snapshot.metadata?.other_graphs ?? [];
  return <Stack spacing={2}><GraphSection title="DCAT Metadata" section={snapshot.metadata?.dcat ?? null} empty="No DCAT metadata graph found in this file." /><GraphSection title="Publication References" section={snapshot.metadata?.publication ?? null} empty="No publication reference graph found in this file." />{otherGraphs.length > 0 && <Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Other Named Graphs</Typography>{otherGraphs.map((graph, idx) => <GraphSection key={`${graph?.graph_uri}-${idx}`} title={`Graph ${idx + 1}`} section={graph} empty="No data found in this named graph." />)}</Stack></CardContent></Card>}<Button variant="contained" disabled={!snapshot.source?.has_data} onClick={() => emit({ type: 'navigate', stage: 'statistics' })}>Continue to Statistics</Button></Stack>;
}

function StatisticsPage({ snapshot }: { snapshot: Snapshot }) {
  const stats = snapshot.statistics ?? {}; const [tab, setTab] = useState(0);
  return <Stack spacing={2}><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', md: 'repeat(4,1fr)' }, gap: 1.2 }}><MetricCard label="Total Triples" value={(stats.total_triples ?? 0).toLocaleString()} /><MetricCard label="Subjects" value={(stats.subjects ?? 0).toLocaleString()} /><MetricCard label="Properties" value={(stats.properties ?? 0).toLocaleString()} /><MetricCard label="External Matches" value={(stats.external_matches ?? 0).toLocaleString()} helper={`${stats.exact_matches ?? 0} exact • ${stats.close_matches ?? 0} close`} /></Box><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Dataset Statistics</Typography><Tabs value={tab} onChange={(_, value) => setTab(value)} variant="scrollable"><Tab label="Property Catalog" /><Tab label="Namespaces" /><Tab label="URI Mappings" /></Tabs>{tab === 0 && <DataTable rows={stats.property_catalog} empty="No property catalog found." maxColumns={4} />}{tab === 1 && <DataTable rows={stats.namespaces} empty="No namespaces extracted." maxColumns={2} />}{tab === 2 && <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1.2 }}><MetricCard label="Exact Matches (skos:exactMatch)" value={stats.exact_matches ?? 0} /><MetricCard label="Close Matches (skos:closeMatch)" value={stats.close_matches ?? 0} /></Box>}<Button variant="contained" disabled={!snapshot.source?.has_data} onClick={() => emit({ type: 'navigate', stage: 'export' })}>Continue to Export</Button></Stack></CardContent></Card></Stack>;
}

function ExportPage({ snapshot }: { snapshot: Snapshot }) {
  const downloads = snapshot.downloads ?? {};
  return <Stack spacing={2}><Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: 'repeat(3,1fr)' }, gap: 2 }}><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Excel Workbook</Typography><Typography variant="body2" color="text.secondary">RDF Data and Property Mappings sheets with safe HYPERLINK formulas.</Typography>{downloads.excel_error && <Alert severity="warning">{downloads.excel_error}</Alert>}<Button variant="contained" disabled={!downloads.excel_base64} onClick={() => triggerBase64Download(downloads.excel_base64 ?? '', downloads.excel_filename ?? 'rdf_table_output.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}>Download Excel</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">CSV</Typography><Typography variant="body2" color="text.secondary">Simple comma-separated subject table for downstream processing.</Typography><Button variant="contained" disabled={!downloads.csv} onClick={() => triggerDownload(downloads.csv ?? '', downloads.csv_filename ?? 'rdf_table_output.csv', 'text/csv;charset=utf-8')}>Download CSV</Button></Stack></CardContent></Card><Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Markdown Metadata</Typography><Typography variant="body2" color="text.secondary">Human-readable metadata, references, namespace, and property documentation.</Typography>{downloads.markdown_error && <Alert severity="warning">{downloads.markdown_error}</Alert>}<Button variant="contained" disabled={!downloads.markdown} onClick={() => triggerDownload(downloads.markdown ?? '', downloads.markdown_filename ?? 'rdf_table_metadata.md', 'text/markdown;charset=utf-8')}>Download Markdown</Button></Stack></CardContent></Card></Box><Card variant="outlined"><CardContent><Stack spacing={1.2}><Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}><Stack><Typography variant="subtitle1">Prepare Exports</Typography><Typography variant="body2" color="text.secondary">Export generation happens in Python so Excel and Markdown exactly match the converter backend.</Typography></Stack><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}><Button variant="contained" disabled={!snapshot.source?.has_data} onClick={() => emit({ type: 'prepare_downloads' })}>Prepare / Refresh Downloads</Button><Button variant="outlined" color="warning" onClick={() => emit({ type: 'reset_workflow' })}>Reset RDF to Table</Button></Stack></Stack><Alert severity="info" variant="outlined">Excel uses HYPERLINK formulas to bypass Excel’s native 65,536 hyperlink limit.</Alert></Stack></CardContent></Card></Stack>;
}

export function RDFToTableApp({ args }: AppProps) {
  const snapshot = args?.snapshot ?? {};
  const activeStage = (snapshot.active_stage && stages.some((stage) => stage.id === snapshot.active_stage) ? snapshot.active_stage : 'load') as Stage;
  useEffect(() => { window.scrollTo(0, 0); try { const mainContent = window.parent.document.querySelector('section.main'); if (mainContent) mainContent.scrollTo({ top: 0, behavior: 'smooth' }); } catch { /* ignore */ } }, [activeStage]);
  useEffect(() => { notifyLayoutChanged(); }, [snapshot, activeStage]);
  const page = useMemo(() => {
    if (activeStage === 'load') return <LoadPage snapshot={snapshot} />;
    if (activeStage === 'preview') return <PreviewPage snapshot={snapshot} />;
    if (activeStage === 'metadata') return <MetadataPage snapshot={snapshot} />;
    if (activeStage === 'statistics') return <StatisticsPage snapshot={snapshot} />;
    return <ExportPage snapshot={snapshot} />;
  }, [activeStage, snapshot]);
  return <AppShell snapshot={snapshot} activeStage={activeStage}>{page}</AppShell>;
}
