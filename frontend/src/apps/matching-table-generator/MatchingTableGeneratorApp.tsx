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
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Slider,
  Stack,
  Step,
  StepButton,
  Stepper,
  Switch,
  TextField,
  Typography,
} from '@mui/material';

type Severity = 'success' | 'info' | 'warning' | 'error';
type AppEvent = { type: string; [key: string]: unknown };
type TableRow = Record<string, unknown>;

type Snapshot = {
  file?: {
    name?: string;
    size?: number;
    available_sheets?: string[];
    selected_sheet?: string | null;
    start_row?: number;
    load_error?: string | null;
  };
  data?: {
    has_table?: boolean;
    rows?: number;
    columns?: number;
    column_names?: string[];
    preview?: TableRow[];
    used_preview?: TableRow[];
    preprocessing_applied?: boolean;
  };
  omission?: { selected?: string[] };
  preprocessing?: {
    prepared_split_config?: Record<string, { delimiter?: string; new_names?: string[] }>;
    prepared_expand_config?: Record<string, { delimiter?: string; codes_to_expand?: string[]; new_col_prefix?: string; true_value?: string | null; false_value?: string | null }>;
    keep_original_split?: boolean;
    keep_original_expand?: boolean;
    transformations_prepared?: boolean;
    detected_expansion_codes?: string[];
    detected_expansion_column?: string;
    detected_expansion_delimiter?: string;
  };
  consolidation?: {
    threshold?: number;
    groups?: string[][];
    choices?: Record<string, string>;
    review_visible?: boolean;
    staged?: boolean;
  };
  matching?: {
    has_table?: boolean;
    rows?: number;
    columns?: string[];
    preview?: TableRow[];
    csv?: string;
    csv_filename?: string;
  };
  downloads?: {
    preprocessed_available?: boolean;
    preprocessed_csv?: string;
    preprocessed_csv_filename?: string;
    preprocessed_xlsx_base64?: string;
    preprocessed_xlsx_filename?: string;
  };
  statusMessage?: { severity?: Severity; text?: string } | null;
};

type MatchingTableGeneratorProps = {
  args?: { app?: string; snapshot?: Snapshot };
  onEvent?: (event: AppEvent) => void;
};

type Stage = 'load' | 'omit' | 'preprocess' | 'consolidate' | 'generate' | 'export';

const stages: { id: Stage; label: string; caption: string }[] = [
  { id: 'load', label: 'Load', caption: 'Upload & parse' },
  { id: 'omit', label: 'Omit', caption: 'Filter values' },
  { id: 'preprocess', label: 'Preprocess', caption: 'Split & expand' },
  { id: 'consolidate', label: 'Consolidate', caption: 'Similar terms' },
  { id: 'generate', label: 'Generate', caption: 'SSSOM table' },
  { id: 'export', label: 'Export', caption: 'Downloads' },
];

const MatchingIcon = () => (
  <svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg" role="img" style={{ marginRight: 12 }}>
    <defs>
      <linearGradient id="mtgMainGradient" x1="8" y1="10" x2="56" y2="54" gradientUnits="userSpaceOnUse">
        <stop stopColor="#2563EB" />
        <stop offset="0.55" stopColor="#0891B2" />
        <stop offset="1" stopColor="#14B8A6" />
      </linearGradient>
      <filter id="mtgShadow" x="-40%" y="-40%" width="180%" height="180%">
        <feDropShadow dx="0" dy="5" stdDeviation="5" floodColor="#0F172A" floodOpacity="0.18" />
      </filter>
    </defs>
    <rect x="11" y="13" width="42" height="38" rx="9" fill="#FFFFFF" stroke="url(#mtgMainGradient)" strokeWidth="2.5" filter="url(#mtgShadow)" />
    <path d="M18 25H46" stroke="url(#mtgMainGradient)" strokeWidth="2.8" strokeLinecap="round" />
    <path d="M18 34H46" stroke="#94A3B8" strokeWidth="2.2" strokeLinecap="round" />
    <path d="M18 43H36" stroke="#94A3B8" strokeWidth="2.2" strokeLinecap="round" />
    <circle cx="43" cy="43" r="8" fill="url(#mtgMainGradient)" />
    <path d="M39.8 43.1L42.1 45.4L47 40.2" stroke="#FFFFFF" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M28 18V49" stroke="#E2E8F0" strokeWidth="1.6" />
    <path d="M39 18V36" stroke="#E2E8F0" strokeWidth="1.6" />
  </svg>
);

function unique(values: string[]): string[] {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function asNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
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

function DataTable({ rows, empty, maxColumns = 8 }: { rows?: TableRow[]; empty: string; maxColumns?: number }) {
  const safeRows = rows ?? [];
  const columns = safeRows.length ? Object.keys(safeRows[0]).slice(0, maxColumns) : [];
  if (!safeRows.length) return <Typography variant="body2" color="text.secondary">{empty}</Typography>;
  return (
    <Box sx={{ overflow: 'auto', border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
      <Box component="table" sx={{ width: '100%', borderCollapse: 'collapse', minWidth: 720 }}>
        <thead>
          <tr>{columns.map((column) => <Box component="th" key={column} sx={{ p: 1, textAlign: 'left', fontSize: 12, bgcolor: '#f8fafc', position: 'sticky', top: 0 }}>{column}</Box>)}</tr>
        </thead>
        <tbody>
          {safeRows.slice(0, 100).map((row, idx) => (
            <tr key={idx}>{columns.map((column) => <Box component="td" key={column} sx={{ p: 1, borderTop: '1px solid #e2e8f0', fontSize: 12, maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(row[column] ?? '')}</Box>)}</tr>
          ))}
        </tbody>
      </Box>
    </Box>
  );
}

function SummaryRow({ label, value }: { label: string; value: React.ReactNode }) {
  return <Stack direction="row" justifyContent="space-between" spacing={2}><Typography variant="body2" color="text.secondary">{label}</Typography><Typography variant="body2" sx={{ fontWeight: 800, textAlign: 'right' }}>{value}</Typography></Stack>;
}

function ToggleCard({ checked, title, description, onChange }: { checked: boolean; title: string; description: string; onChange: (value: boolean) => void }) {
  return (
    <Paper variant="outlined" sx={{ p: 1.5, minHeight: 96, borderRadius: 3, borderColor: checked ? 'primary.main' : 'divider', bgcolor: checked ? 'rgba(37,99,235,.035)' : 'background.paper' }}>
      <Stack direction="row" spacing={1} alignItems="flex-start">
        <Checkbox checked={checked} onChange={(e) => onChange(e.target.checked)} sx={{ p: 0 }} />
        <Stack><Typography variant="body2" sx={{ fontWeight: 850 }}>{title}</Typography><Typography variant="caption" color="text.secondary">{description}</Typography></Stack>
      </Stack>
    </Paper>
  );
}

function AppShell({ snapshot, activeStage, setActiveStage, children }: { snapshot: Snapshot; activeStage: Stage; setActiveStage: (stage: Stage) => void; children: React.ReactNode }) {
  const activeStep = stages.findIndex((stage) => stage.id === activeStage);
  const data = snapshot.data ?? {};
  const matching = snapshot.matching ?? {};
  const preprocessing = snapshot.preprocessing ?? {};
  return (
    <Box sx={{ bgcolor: '#eef7fb', minHeight: '100vh', p: { xs: 1, md: 2 }, borderRadius: 4 }}>
      <Stack spacing={2}>
        <Paper variant="outlined" sx={{ p: { xs: 2, md: 2.5 }, borderRadius: 4, background: 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 50%,#eff6ff 100%)', boxShadow: '0 18px 48px rgba(15,23,42,.08)' }}>
          <Stack direction={{ xs: 'column', lg: 'row' }} spacing={2} alignItems={{ xs: 'stretch', lg: 'center' }} justifyContent="space-between">
            <Stack direction="row" spacing={1.5} alignItems="center"><MatchingIcon /><Stack><Typography variant="h5">Matching Table Generator</Typography><Typography variant="body2" color="text.secondary">Guided workflow for preprocessing tabular data and producing strict SSSOM matching tables</Typography></Stack></Stack>
            <Stepper nonLinear activeStep={activeStep} sx={{ minWidth: { lg: 760 } }}>{stages.map((stage, idx) => <Step key={stage.id} completed={idx < activeStep}><StepButton onClick={() => setActiveStage(stage.id)}><Stack spacing={0}><Typography variant="body2" sx={{ fontWeight: 800 }}>{stage.label}</Typography><Typography variant="caption" color="text.secondary">{stage.caption}</Typography></Stack></StepButton></Step>)}</Stepper>
          </Stack>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', md: 'repeat(4, 1fr)' }, gap: 1.2, mt: 2 }}>
            <Chip label={data.has_table ? `${data.rows ?? 0} rows loaded` : 'No table loaded'} color={data.has_table ? 'success' : 'warning'} />
            <Chip label={`${data.columns ?? 0} columns`} color={data.has_table ? 'info' : 'default'} />
            <Chip label={preprocessing.transformations_prepared ? 'Transforms prepared' : 'No transforms prepared'} color={preprocessing.transformations_prepared ? 'success' : 'default'} />
            <Chip label={matching.has_table ? `${matching.rows ?? 0} SSSOM rows` : 'Matching table pending'} color={matching.has_table ? 'success' : 'warning'} />
          </Box>
        </Paper>
        {snapshot.statusMessage?.text && <Alert severity={snapshot.statusMessage.severity ?? 'info'}>{snapshot.statusMessage.text}</Alert>}
        {children}
      </Stack>
    </Box>
  );
}

function LoadPage({ snapshot, emit, goNext }: { snapshot: Snapshot; emit: (event: AppEvent) => void; goNext: () => void }) {
  const file = snapshot.file ?? {};
  const data = snapshot.data ?? {};
  const [startRow, setStartRow] = useState(asNumber(file.start_row, 1));
  useEffect(() => setStartRow(asNumber(file.start_row, 1)), [file.start_row]);

  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const uploaded = event.target.files?.[0];
    event.target.value = '';
    if (!uploaded) return;
    const buffer = await uploaded.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    let binary = '';
    bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
    emit({ type: 'upload_file', filename: uploaded.name, content_base64: window.btoa(binary), start_row: startRow });
  }

  return (
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: 'minmax(0,1fr) 360px' }, gap: 2 }}>
      <Stack spacing={2}>
        <Card variant="outlined"><CardContent><Stack spacing={1.5}>
          <Stack direction="row" justifyContent="space-between" alignItems="center"><Stack><Typography variant="subtitle1">1. Upload & Load Data</Typography><Typography variant="body2" color="text.secondary">Load CSV/XLSX/XLS data, choose an Excel sheet, and set the 1-based header row.</Typography></Stack><Chip label={data.has_table ? 'loaded' : 'waiting'} color={data.has_table ? 'success' : 'warning'} /></Stack>
          <Paper variant="outlined" sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed', borderRadius: 3, bgcolor: '#f8fafc' }}>
            <Typography variant="h6">Upload data table</Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: .5 }}>Supported: CSV, XLSX, XLS. The parser keeps text terms but excludes numeric/date-like values when generating mapping terms.</Typography>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.2} justifyContent="center" alignItems="center" sx={{ mt: 1.5 }}>
              <TextField type="number" label="Start parsing from row" value={startRow} inputProps={{ min: 1 }} onChange={(e) => setStartRow(Math.max(1, asNumber(e.target.value, 1)))} sx={{ maxWidth: 220 }} />
              <Button component="label" variant="contained">Upload CSV / Excel<input hidden type="file" accept=".csv,.xlsx,.xls,text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" onChange={handleUpload} /></Button>
              {data.has_table && <Button variant="outlined" onClick={() => emit({ type: 'set_start_row', start_row: startRow })}>Reload with row</Button>}
            </Stack>
          </Paper>
          {(file.available_sheets?.length ?? 0) > 0 && <FormControl fullWidth size="small"><InputLabel>Excel sheet</InputLabel><Select label="Excel sheet" value={file.selected_sheet ?? ''} onChange={(e) => emit({ type: 'set_sheet', sheet: String(e.target.value) })}>{(file.available_sheets ?? []).map((sheet) => <MenuItem key={sheet} value={sheet}>{sheet}</MenuItem>)}</Select></FormControl>}
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(4,1fr)' }, gap: 1 }}>
            <SummaryRow label="File" value={file.name || '—'} />
            <SummaryRow label="Rows" value={data.rows ?? 0} />
            <SummaryRow label="Columns" value={data.columns ?? 0} />
            <SummaryRow label="Header row" value={file.start_row ?? startRow} />
          </Box>
          {file.load_error && <Alert severity="error" variant="outlined">{file.load_error}</Alert>}
          <Typography variant="subtitle2">Data Preview (first rows as loaded)</Typography>
          <DataTable rows={data.preview} empty="No data preview available yet." />
        </Stack></CardContent></Card>
      </Stack>
      <Stack spacing={2}>
        <Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Workflow guide</Typography>{[
          'Load your table and confirm the header row.',
          'Optionally omit numeric/date/ID value columns from object-term extraction.',
          'Prepare split and expansion transformations.',
          'Optionally consolidate nearly identical terms.',
          'Generate and download the strict SSSOM matching table.',
        ].map((item) => <Typography key={item} variant="body2" color="text.secondary">✓ {item}</Typography>)}<Button variant="contained" disabled={!data.has_table} onClick={goNext}>Continue to omission</Button></Stack></CardContent></Card>
      </Stack>
    </Box>
  );
}

function OmissionPage({ snapshot, emit, goNext }: { snapshot: Snapshot; emit: (event: AppEvent) => void; goNext: () => void }) {
  const columns = snapshot.data?.column_names ?? [];
  const [selected, setSelected] = useState<string[]>(snapshot.omission?.selected ?? []);
  useEffect(() => setSelected(snapshot.omission?.selected ?? []), [snapshot.omission?.selected]);
  const selectedSet = new Set(selected);
  const toggle = (column: string) => {
    const next = selectedSet.has(column) ? selected.filter((item) => item !== column) : [...selected, column];
    setSelected(next);
  };
  return <Stack spacing={2}>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}>
      <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}><Stack><Typography variant="subtitle1">2. Configure Column Omission</Typography><Typography variant="body2" color="text.secondary">Omitted columns still contribute header/predicate terms, but their cell values are not extracted as object terms.</Typography></Stack><Chip label={`${selected.length} omitted`} color={selected.length ? 'warning' : 'default'} /></Stack>
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1}><Button variant="outlined" onClick={() => emit({ type: 'add_omission_by_type', mode: 'numeric' })}>Add numeric columns</Button><Button variant="outlined" onClick={() => emit({ type: 'add_omission_by_type', mode: 'date' })}>Add date columns</Button><Button variant="outlined" onClick={() => emit({ type: 'add_omission_by_type', mode: 'id' })}>Add ID columns</Button><Button variant="contained" onClick={() => emit({ type: 'set_omitted_columns', columns: selected })}>Save omission list</Button></Stack>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2,1fr)', lg: 'repeat(3,1fr)' }, gap: 1 }}>{columns.map((column) => <ToggleCard key={column} checked={selectedSet.has(column)} title={column} description={selectedSet.has(column) ? 'Cell values omitted from object terms.' : 'Cell values included if term-like.'} onChange={() => toggle(column)} />)}</Box>
      {!columns.length && <Alert severity="info">Load data first to configure omitted columns.</Alert>}
      <Button variant="contained" disabled={!snapshot.data?.has_table} onClick={goNext}>Continue to preprocessing</Button>
    </Stack></CardContent></Card>
  </Stack>;
}

function PreprocessPage({ snapshot, emit, goNext }: { snapshot: Snapshot; emit: (event: AppEvent) => void; goNext: () => void }) {
  const columns = snapshot.data?.column_names ?? [];
  const prep = snapshot.preprocessing ?? {};
  const preparedSplitColumns = Object.keys(prep.prepared_split_config ?? {});
  const preparedExpandColumn = Object.keys(prep.prepared_expand_config ?? {})[0] ?? '';
  const [splitEnabled, setSplitEnabled] = useState(preparedSplitColumns.length > 0);
  const [splitColumns, setSplitColumns] = useState<string[]>(preparedSplitColumns);
  const [splitRules, setSplitRules] = useState<Record<string, { delimiter: string; names: string }>>({});
  const [autoNames, setAutoNames] = useState(true);
  const [keepSplit, setKeepSplit] = useState(prep.keep_original_split ?? true);
  const [expandEnabled, setExpandEnabled] = useState(Boolean(preparedExpandColumn));
  const [expandColumn, setExpandColumn] = useState(preparedExpandColumn);
  const [expandDelimiter, setExpandDelimiter] = useState(prep.prepared_expand_config?.[preparedExpandColumn]?.delimiter ?? ', ');
  const [expandPrefix, setExpandPrefix] = useState(prep.prepared_expand_config?.[preparedExpandColumn]?.new_col_prefix ?? 'Indicator_');
  const [expandCodes, setExpandCodes] = useState<string[]>(prep.prepared_expand_config?.[preparedExpandColumn]?.codes_to_expand ?? []);
  const [trueValue, setTrueValue] = useState(String(prep.prepared_expand_config?.[preparedExpandColumn]?.true_value ?? 'True'));
  const [falseValue, setFalseValue] = useState(String(prep.prepared_expand_config?.[preparedExpandColumn]?.false_value ?? 'False'));
  const [keepExpand, setKeepExpand] = useState(prep.keep_original_expand ?? true);
  const detectedCodes = prep.detected_expansion_codes ?? [];

  useEffect(() => {
    const nextRules: Record<string, { delimiter: string; names: string }> = {};
    splitColumns.forEach((column) => {
      nextRules[column] = splitRules[column] ?? {
        delimiter: prep.prepared_split_config?.[column]?.delimiter ?? ',',
        names: prep.prepared_split_config?.[column]?.new_names?.join(', ') ?? (autoNames ? `${column}_1, ${column}_2` : ''),
      };
    });
    setSplitRules(nextRules);
  }, [splitColumns.join('|')]);

  function prepare() {
    const splitConfig: Record<string, { delimiter: string; new_names: string[] }> = {};
    if (splitEnabled) {
      splitColumns.forEach((column) => {
        const rule = splitRules[column];
        if (rule?.delimiter && rule.names) splitConfig[column] = { delimiter: rule.delimiter, new_names: unique(rule.names.split(',').map((part) => part.trim())) };
      });
    }
    const expandConfig: Record<string, { delimiter: string; codes_to_expand: string[]; new_col_prefix: string; true_value: string | null; false_value: string | null }> = {};
    if (expandEnabled && expandColumn && expandDelimiter && expandPrefix && expandCodes.length) {
      expandConfig[expandColumn] = { delimiter: expandDelimiter, codes_to_expand: expandCodes, new_col_prefix: expandPrefix, true_value: trueValue === 'Code itself' ? '$CODE$' : trueValue, false_value: falseValue === 'Empty (None/NA)' ? null : falseValue };
    }
    emit({ type: 'prepare_transformations', split_config: splitConfig, expand_config: expandConfig, keep_original_split: keepSplit, keep_original_expand: keepExpand });
  }

  return <Stack spacing={2}>
    <Card variant="outlined"><CardContent><Stack spacing={2}>
      <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={1}><Stack><Typography variant="subtitle1">3. Preprocess Data</Typography><Typography variant="body2" color="text.secondary">Transformations are staged here and applied only when the matching table is generated.</Typography></Stack><Chip label={prep.transformations_prepared ? 'rules prepared' : 'optional'} color={prep.transformations_prepared ? 'success' : 'default'} /></Stack>
      <Paper variant="outlined" sx={{ p: 1.5, borderRadius: 3 }}><Stack spacing={1.5}>
        <Stack direction="row" justifyContent="space-between"><Stack><Typography variant="subtitle2">A) Split Columns by Position</Typography><Typography variant="caption" color="text.secondary">Example: split “Berlin, Germany” into City and Country columns.</Typography></Stack><Switch checked={splitEnabled} onChange={(e) => setSplitEnabled(e.target.checked)} /></Stack>
        <Collapse in={splitEnabled}><Stack spacing={1.2}>
          <FormControl fullWidth size="small"><InputLabel>Columns to split</InputLabel><Select multiple label="Columns to split" value={splitColumns} onChange={(e) => setSplitColumns(typeof e.target.value === 'string' ? e.target.value.split(',') : (e.target.value as string[]))} renderValue={(selected) => (selected as string[]).join(', ')}>{columns.map((column) => <MenuItem key={column} value={column}><Checkbox checked={splitColumns.includes(column)} />{column}</MenuItem>)}</Select></FormControl>
          <FormControlLabel control={<Checkbox checked={autoNames} onChange={(e) => setAutoNames(e.target.checked)} />} label="Auto-generate new names as <column>_1, <column>_2 (editable)" />
          {splitColumns.map((column) => <Box key={column} sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '.35fr .65fr' }, gap: 1 }}><TextField label={`${column} • Delimiter`} value={splitRules[column]?.delimiter ?? ','} onChange={(e) => setSplitRules({ ...splitRules, [column]: { ...(splitRules[column] ?? { names: '' }), delimiter: e.target.value } })} /><TextField label={`${column} • New Names (comma-sep.)`} value={splitRules[column]?.names ?? ''} onChange={(e) => setSplitRules({ ...splitRules, [column]: { ...(splitRules[column] ?? { delimiter: ',' }), names: e.target.value } })} placeholder="e.g., City, Country" /></Box>)}
          <FormControlLabel control={<Checkbox checked={keepSplit} onChange={(e) => setKeepSplit(e.target.checked)} />} label="Keep original split column(s)" />
        </Stack></Collapse>
      </Stack></Paper>
      <Paper variant="outlined" sx={{ p: 1.5, borderRadius: 3 }}><Stack spacing={1.5}>
        <Stack direction="row" justifyContent="space-between"><Stack><Typography variant="subtitle2">B) Expand Codes to Indicator Columns</Typography><Typography variant="caption" color="text.secondary">Example: expand “CIP, TET” into boolean/code indicator columns.</Typography></Stack><Switch checked={expandEnabled} onChange={(e) => setExpandEnabled(e.target.checked)} /></Stack>
        <Collapse in={expandEnabled}><Stack spacing={1.2}>
          <FormControl fullWidth size="small"><InputLabel>Column containing codes</InputLabel><Select label="Column containing codes" value={expandColumn} onChange={(e) => { setExpandColumn(String(e.target.value)); setExpandCodes([]); }}>{columns.map((column) => <MenuItem key={column} value={column}>{column}</MenuItem>)}</Select></FormControl>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '.4fr 1fr auto' }, gap: 1 }}><TextField label="Codes delimiter" value={expandDelimiter} onChange={(e) => setExpandDelimiter(e.target.value)} /><TextField label="New column prefix" value={expandPrefix} onChange={(e) => setExpandPrefix(e.target.value)} /><Button variant="outlined" onClick={() => emit({ type: 'detect_expansion_codes', column: expandColumn, delimiter: expandDelimiter })}>Detect codes</Button></Box>
          <FormControl fullWidth size="small"><InputLabel>Codes to expand</InputLabel><Select multiple label="Codes to expand" value={expandCodes} onChange={(e) => setExpandCodes(typeof e.target.value === 'string' ? e.target.value.split(',') : (e.target.value as string[]))} renderValue={(selected) => (selected as string[]).join(', ')}>{detectedCodes.map((code) => <MenuItem key={code} value={code}><Checkbox checked={expandCodes.includes(code)} />{code}</MenuItem>)}</Select></FormControl>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(3,1fr)' }, gap: 1 }}><FormControl size="small"><InputLabel>Value if present</InputLabel><Select label="Value if present" value={trueValue} onChange={(e) => setTrueValue(String(e.target.value))}>{['True', '1', 'Code itself'].map((value) => <MenuItem key={value} value={value}>{value}</MenuItem>)}</Select></FormControl><FormControl size="small"><InputLabel>Value if absent</InputLabel><Select label="Value if absent" value={falseValue} onChange={(e) => setFalseValue(String(e.target.value))}>{['False', '0', 'Empty (None/NA)'].map((value) => <MenuItem key={value} value={value}>{value}</MenuItem>)}</Select></FormControl><FormControlLabel control={<Checkbox checked={keepExpand} onChange={(e) => setKeepExpand(e.target.checked)} />} label="Keep original codes column" /></Box>
        </Stack></Collapse>
      </Stack></Paper>
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1}><Button variant="contained" onClick={prepare} disabled={!snapshot.data?.has_table}>Prepare transformations</Button><Button variant="outlined" onClick={() => emit({ type: 'clear_transformations' })} disabled={!prep.transformations_prepared}>Clear prepared rules</Button><Button variant="contained" disabled={!snapshot.data?.has_table} onClick={goNext}>Continue to consolidation</Button></Stack>
    </Stack></CardContent></Card>
  </Stack>;
}

function ConsolidatePage({ snapshot, emit, goNext }: { snapshot: Snapshot; emit: (event: AppEvent) => void; goNext: () => void }) {
  const cons = snapshot.consolidation ?? {};
  const [threshold, setThreshold] = useState(asNumber(cons.threshold, 0.85));
  const [choices, setChoices] = useState<Record<string, string>>(cons.choices ?? {});
  useEffect(() => { setChoices(cons.choices ?? {}); setThreshold(asNumber(cons.threshold, 0.85)); }, [cons.choices, cons.threshold]);
  return <Stack spacing={2}>
    <Card variant="outlined"><CardContent><Stack spacing={1.5}>
      <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between"><Stack><Typography variant="subtitle1">4. Consolidate Nearly Identical Terms</Typography><Typography variant="body2" color="text.secondary">Use Levenshtein similarity to group terms and stage replacement choices before generation.</Typography></Stack><Chip label={cons.staged ? 'staged' : `${cons.groups?.length ?? 0} groups`} color={cons.staged ? 'success' : (cons.groups?.length ? 'warning' : 'default')} /></Stack>
      <Box sx={{ px: 1 }}><Typography variant="body2" sx={{ fontWeight: 800 }}>Similarity threshold: {threshold.toFixed(2)}</Typography><Slider min={0} max={1} step={0.01} value={threshold} onChange={(_, value) => setThreshold(value as number)} /></Box>
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1}><Button variant="outlined" onClick={() => emit({ type: 'find_similar_terms', threshold })} disabled={!snapshot.data?.has_table}>Find similar terms</Button><Button variant="contained" onClick={() => emit({ type: 'stage_consolidations', choices })} disabled={!(cons.groups?.length)}>Apply & stage consolidations</Button><Button variant="contained" disabled={!snapshot.data?.has_table} onClick={goNext}>Continue to generation</Button></Stack>
      {(cons.groups ?? []).map((group, idx) => {
        const choiceKey = `choice_group_${idx}`;
        const newTermKey = `new_term_group_${idx}`;
        const selected = choices[choiceKey] ?? group[0] ?? 'Keep All (No Change)';
        const options = [...group, 'Use New Term', 'Keep All (No Change)'];
        return <Paper key={idx} variant="outlined" sx={{ p: 1.5, borderRadius: 3 }}><Stack spacing={1}><Typography variant="subtitle2">Group {idx + 1}</Typography><Stack direction="row" flexWrap="wrap" gap={0.8}>{group.map((term) => <Chip key={term} label={term} />)}</Stack><FormControl fullWidth size="small"><InputLabel>Choose term/action</InputLabel><Select label="Choose term/action" value={selected} onChange={(e) => setChoices({ ...choices, [choiceKey]: String(e.target.value) })}>{options.map((option) => <MenuItem key={option} value={option}>{option}</MenuItem>)}</Select></FormControl><Collapse in={selected === 'Use New Term'}><TextField fullWidth label="Enter new term" value={choices[newTermKey] ?? ''} onChange={(e) => setChoices({ ...choices, [newTermKey]: e.target.value })} /></Collapse></Stack></Paper>;
      })}
      {!(cons.groups?.length) && !cons.staged && <Alert severity="info" variant="outlined">No consolidation groups are currently visible. Run the similarity search to review candidates.</Alert>}
      {cons.staged && <Alert severity="success" variant="outlined">Consolidation choices are staged and will be applied when the matching table is generated.</Alert>}
    </Stack></CardContent></Card>
  </Stack>;
}

function GeneratePage({ snapshot, emit, goNext }: { snapshot: Snapshot; emit: (event: AppEvent) => void; goNext: () => void }) {
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', xl: '360px minmax(0,1fr)' }, gap: 2 }}>
    <Stack spacing={2}>
      <Card variant="outlined"><CardContent><Stack spacing={1.3}><Typography variant="subtitle1">5. Generate / Refresh Matching Table</Typography><Typography variant="body2" color="text.secondary">Applies prepared transformations, staged consolidations, column omissions, and extracts predicate/object terms into strict SSSOM columns.</Typography><Button variant="contained" size="large" disabled={!snapshot.data?.has_table} onClick={() => emit({ type: 'generate_matching_table' })}>Generate / Refresh Matching Table</Button><Button variant="outlined" onClick={goNext} disabled={!snapshot.matching?.has_table}>Continue to Export</Button></Stack></CardContent></Card>
      <Card variant="outlined"><CardContent><Stack spacing={1}><Typography variant="subtitle1">Current configuration</Typography><SummaryRow label="Omitted columns" value={(snapshot.omission?.selected?.length ?? 0)} /><SummaryRow label="Transforms prepared" value={snapshot.preprocessing?.transformations_prepared ? 'Yes' : 'No'} /><SummaryRow label="Consolidations staged" value={snapshot.consolidation?.staged ? 'Yes' : 'No'} /></Stack></CardContent></Card>
    </Stack>
    <Stack spacing={2}>
      <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between"><Typography variant="subtitle1">Data Used for Matching Table</Typography><Chip label={snapshot.data?.preprocessing_applied ? 'preprocessed' : 'as loaded'} /></Stack><DataTable rows={snapshot.data?.used_preview} empty="No data preview available yet." /></Stack></CardContent></Card>
      <Card variant="outlined"><CardContent><Stack spacing={1.5}><Stack direction="row" justifyContent="space-between"><Typography variant="subtitle1">Review Matching Table</Typography><Chip label={snapshot.matching?.has_table ? `${snapshot.matching.rows} rows` : 'pending'} color={snapshot.matching?.has_table ? 'success' : 'warning'} /></Stack><Alert severity="info" variant="outlined">The generated table uses strict SSSOM columns: subject_id, subject_label, predicate_id, object_id, object_label, mapping_justification.</Alert><DataTable rows={snapshot.matching?.preview} empty="Generate a matching table to preview it here." maxColumns={6} /></Stack></CardContent></Card>
    </Stack>
  </Box>;
}

function ExportPage({ snapshot, emit }: { snapshot: Snapshot; emit: (event: AppEvent) => void }) {
  const matching = snapshot.matching ?? {};
  const downloads = snapshot.downloads ?? {};
  return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: '1fr 1fr' }, gap: 2 }}>
    <Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">6. Matching Table Export</Typography><Typography variant="body2" color="text.secondary">Download the strict SSSOM matching table for reconciliation workflows.</Typography><SummaryRow label="Rows" value={matching.rows ?? 0} /><Button variant="contained" disabled={!matching.csv} onClick={() => { triggerDownload(matching.csv ?? '', matching.csv_filename ?? 'matching_table.csv', 'text/csv;charset=utf-8'); emit({ type: 'download_ack' }); }}>Download Matching Table (CSV)</Button></Stack></CardContent></Card>
    <Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">7. Preprocessed Data Export</Typography><Typography variant="body2" color="text.secondary">Available after generation if transformations were applied. Use this as input for RDF generation.</Typography><Alert severity={downloads.preprocessed_available ? 'success' : 'info'} variant="outlined">{downloads.preprocessed_available ? 'Preprocessed data is ready for download.' : snapshot.preprocessing?.transformations_prepared ? 'Generate first to apply prepared transformations.' : 'No preprocessing was applied in the last run.'}</Alert><Button variant="contained" disabled={!downloads.preprocessed_csv} onClick={() => triggerDownload(downloads.preprocessed_csv ?? '', downloads.preprocessed_csv_filename ?? 'data_preprocessed.csv', 'text/csv;charset=utf-8')}>Download Preprocessed Data as CSV</Button><Button variant="outlined" disabled={!downloads.preprocessed_xlsx_base64} onClick={() => triggerBase64Download(downloads.preprocessed_xlsx_base64 ?? '', downloads.preprocessed_xlsx_filename ?? 'data_preprocessed.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}>Download Preprocessed Data as Excel (XLSX)</Button></Stack></CardContent></Card>
    <Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Backend handoff</Typography><Typography variant="body2" color="text.secondary">Generated outputs are stored in the Python backend service for downstream reconciliation and RDF generator workflows.</Typography><Chip label={matching.has_table ? 'handoff ready' : 'handoff pending'} color={matching.has_table ? 'success' : 'warning'} /></Stack></CardContent></Card>
    <Card variant="outlined"><CardContent><Stack spacing={1.2}><Typography variant="subtitle1">Reset workflow</Typography><Typography variant="body2" color="text.secondary">Clear uploaded data, transformations, generated tables, and handoff state.</Typography><Button variant="outlined" color="warning" onClick={() => emit({ type: 'reset_all' })}>Reset Matching Table Generator</Button></Stack></CardContent></Card>
  </Box>;
}

export function MatchingTableGeneratorApp({ args, onEvent }: MatchingTableGeneratorProps) {
  const snapshot = args?.snapshot ?? {};
  const [activeStage, setActiveStage] = useState<Stage>('load');
  useEffect(() => {
    try {
      const mainContent = window.parent.document.querySelector('section.main');
      if (mainContent) mainContent.scrollTo({ top: 0, behavior: 'smooth' });
    } catch {
      // ignore cross-frame scroll issues
    }
  }, [activeStage]);
  function emit(event: AppEvent) { onEvent?.({ ...event, nonce: Date.now() }); }
  const goNext = () => {
    const idx = stages.findIndex((stage) => stage.id === activeStage);
    setActiveStage(stages[Math.min(stages.length - 1, idx + 1)].id);
  };
  const page = useMemo(() => {
    if (activeStage === 'load') return <LoadPage snapshot={snapshot} emit={emit} goNext={goNext} />;
    if (activeStage === 'omit') return <OmissionPage snapshot={snapshot} emit={emit} goNext={goNext} />;
    if (activeStage === 'preprocess') return <PreprocessPage snapshot={snapshot} emit={emit} goNext={goNext} />;
    if (activeStage === 'consolidate') return <ConsolidatePage snapshot={snapshot} emit={emit} goNext={goNext} />;
    if (activeStage === 'generate') return <GeneratePage snapshot={snapshot} emit={emit} goNext={goNext} />;
    return <ExportPage snapshot={snapshot} emit={emit} />;
  }, [activeStage, snapshot]);
  return <AppShell snapshot={snapshot} activeStage={activeStage} setActiveStage={setActiveStage}>{page}</AppShell>;
}
