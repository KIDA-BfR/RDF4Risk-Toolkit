import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Alert, Box, Button, Checkbox, Chip, Collapse, Dialog, DialogActions, DialogContent, DialogTitle, Divider, FormControl, FormControlLabel, InputLabel, MenuItem, Select, Slider, Stack, Switch, Table, TableBody, TableCell, TableContainer, TableHead, TableRow as MuiTableRow, TextField, Typography, useMediaQuery } from '@mui/material';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import { DownloadButton, PrepareButton, RefreshActionButton, ResetButton, SearchActionButton, UploadButton } from '../../components/actions/WorkflowActionButtons';
import { ToolMenu } from '../../components/actions/ToolMenu';
import type { WorkflowNavProps } from '../../components/actions/WorkflowNav';
import { ConfirmActionButton } from '../../components/dialogs/ConfirmActionButton';
import { InfoButton, type InfoContent } from '../../components/help/InfoButton';
import { Disclosure } from '../../components/layout/Disclosure';
import { MetaList, type MetaItem } from '../../components/layout/MetaList';
import { Section } from '../../components/layout/Section';
import { WizardPage } from '../../components/layout/WizardPage';
import { EmptyState } from '../../components/states/EmptyState';
import { serviceMeta, type ReconciliationTool, type ServiceId } from '../../standalone/services';
import { DEMO_FILENAME, demoCsvBase64 } from '../../shared/demoData';
import { useShellStatus } from '../../shared/toolNavigation';
import { fonts } from '../../theme/tokens';

type AppEvent = { type: string; [key: string]: unknown };
type TableRow = Record<string, unknown>;
type ColumnProfile = { name: string; text_values: number; numeric_only: boolean; suggest_omit: boolean; suggestion_kind?: string | null; suggestion_reason?: string; replaced_by_rule: boolean; examples: string[] };
type Snapshot = {
 file?: { name?: string; available_sheets?: string[]; selected_sheet?: string | null; start_row?: number; load_error?: string | null };
 data?: { has_table?: boolean; rows?: number; columns?: number; column_names?: string[]; preview?: TableRow[]; selection_columns?: string[]; selection_preview?: TableRow[]; column_profiles?: ColumnProfile[] };
 omission?: { selected?: string[] };
 preprocessing?: { prepared_split_config?: Record<string, { delimiter?: string; new_names?: string[] }>; prepared_expand_config?: Record<string, { delimiter?: string; codes_to_expand?: string[]; new_col_prefix?: string; true_value?: string | null; false_value?: string | null }>; keep_original_split?: boolean; keep_original_expand?: boolean; transformations_prepared?: boolean; detected_expansion_codes?: string[] };
 consolidation?: { searched?: boolean; threshold?: number; groups?: string[][]; choices?: Record<string, string>; staged?: boolean };
 matching?: { has_table?: boolean; is_current?: boolean; rows?: number; preview?: TableRow[]; term_preview?: TableRow[]; term_csv?: string; term_csv_filename?: string; csv?: string; csv_filename?: string; configuration_revision?: number; error?: string | null };
 downloads?: { preprocessed_available?: boolean; preprocessed_csv?: string; preprocessed_csv_filename?: string; preprocessed_xlsx_base64?: string; preprocessed_xlsx_filename?: string };
 handoff?: { status?: string; target?: string; confirmation_token?: string; rows?: number; message?: string } | null;
 statusMessage?: { severity?: 'success' | 'info' | 'warning' | 'error'; text?: string } | null;
};
type Emit = (event: AppEvent) => Promise<boolean>;
type Stage = 0 | 1 | 2;
type MatchingTableGeneratorProps = { args?: { app?: string; snapshot?: Snapshot }; onEvent?: (event: AppEvent) => unknown | Promise<unknown>; busy?: boolean; onDraftChange?: (dirty: boolean) => void };

const stages: { id: string; label: string; title: string; lead: string; info: InfoContent }[] = [
  {
    id: 'load', label: 'Load table', title: 'Load your research table', lead: 'Start with the table whose terms you want to link to ontologies.',
    info: { title: 'Loading a table', content: <><p>Load a CSV or Excel file (.csv, .xlsx, .xls), or try the example data. Your original file is never changed.</p><p>For Excel files with several sheets, choose the sheet. If the table starts with notes, set the header row under Import settings.</p></> },
  },
  {
    id: 'choose', label: 'Choose terms', title: 'Choose the terms to reconcile', lead: 'Include the cell values that describe concepts you want to match.',
    info: { title: 'Choosing terms', content: <><p>Every included cell value becomes a term to reconcile. Identifiers and measurements rarely need an ontology match, so exclude those columns.</p><ul><li>Numbers and recognized dates are filtered automatically.</li><li>Original data and column headings are kept.</li><li>Finding a column only filters the view; your selection stays the same.</li></ul></> },
  },
  {
    id: 'review', label: 'Review terms', title: 'Review your terms', lead: 'Check the prepared terms, then choose how to reconcile them.',
    info: { title: 'Reviewing terms', content: <><p>The matching table is generated from your choices automatically when you open this stage. Use Refresh to retry an update.</p><p>Then choose manual curation or AI-assisted matching. The table is transferred; nothing runs automatically.</p></> },
  },
];

const handoffTargets: { id: ReconciliationTool; description: string; action: string }[] = [
  { id: 'semi_automatic_reconciliation', description: 'Choose providers and review suggestions yourself.', action: 'Open manual curation' },
  { id: 'agent_reconciliation', description: 'Configure AI assistance, then review its proposed matches.', action: 'Open AI-assisted matching' },
];

// Backend protocol values that double as option labels; only their display text is sentence case.
const KEEP_ALL = 'Keep All (No Change)';
const NEW_TERM = 'Use New Term';
const choiceLabel = (option: string) => option === KEEP_ALL ? 'Keep all (no change)' : option === NEW_TERM ? 'Use a new term' : option;
const SSSOM_IDENTIFIER_COLUMNS = ['subject_id', 'predicate_id', 'object_id', 'mapping_justification'];

const numberFormat = new Intl.NumberFormat();
const formatCount = (value?: number) => typeof value === 'number' ? numberFormat.format(value) : '';
const plural = (count: number, one: string, many: string) => `${numberFormat.format(count)} ${count === 1 ? one : many}`;
const tabular = { fontVariantNumeric: 'tabular-nums' } as const;

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

function Missing() {
  return <Box component="span" sx={{ color: 'text.secondary' }}>—</Box>;
}

function cellValue(value: unknown): React.ReactNode {
  return value === null || value === undefined || value === '' ? <Missing /> : String(value);
}

function PreviewTable({ rows = [], columns = Object.keys(rows[0] ?? {}), label, monoColumns = [] }: { rows?: TableRow[]; columns?: string[]; label: string; monoColumns?: string[] }) {
  return <TableContainer tabIndex={0} role="region" aria-label={label} sx={{ maxHeight: 420, maxWidth: '100%' }}>
    <Table size="small" stickyHeader aria-label={label}><TableHead><MuiTableRow>{columns.map(column => <TableCell key={column} scope="col" sx={{ minWidth: 150 }}>{column}</TableCell>)}</MuiTableRow></TableHead>
    <TableBody>{rows.map((row, index) => <MuiTableRow key={index}>{columns.map(column => <TableCell key={column} sx={{ verticalAlign: 'top', overflowWrap: 'anywhere', maxWidth: 280, ...(monoColumns.includes(column) ? { fontFamily: fonts.mono, fontSize: '0.8125rem' } : {}) }}>{cellValue(row[column])}</TableCell>)}</MuiTableRow>)}</TableBody></Table>
    {!rows.length && <Typography variant="body2" sx={{ p: 2 }} color="text.secondary">No rows to preview.</Typography>}
  </TableContainer>;
}

function LoadPage({ snapshot, emit, onReading }: { snapshot: Snapshot; emit: Emit; onReading: (reading: boolean) => void }) {
  const file = snapshot.file ?? {};
  const data = snapshot.data ?? {};
  const sheets = file.available_sheets ?? [];
  const [startRow, setStartRow] = useState(file.start_row ?? 1);
  const [readError, setReadError] = useState('');
  const [reading, setReading] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  useEffect(() => setStartRow(file.start_row ?? 1), [file.start_row]);
  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const uploaded = event.target.files?.[0]; event.target.value = '';
    if (!uploaded) return;
    setReading(true); onReading(true); setReadError('');
    try {
      const bytes = new Uint8Array(await uploaded.arrayBuffer());
      let binary = ''; bytes.forEach(byte => { binary += String.fromCharCode(byte); });
      await emit({ type: 'upload_file', filename: uploaded.name, content_base64: window.btoa(binary), start_row: startRow });
    } catch { setReadError('This file could not be read. Choose it again or use another file.'); }
    finally { setReading(false); onReading(false); }
  }
  // Rendered once: in the section header when a table is loaded, otherwise as the stage's main action.
  // A native button opens the hidden input, so Enter and Space work without extra key handling.
  const uploadButton = <span>
    <input ref={fileInput} hidden type="file" accept=".csv,.xlsx,.xls" disabled={reading} onChange={handleUpload} />
    <UploadButton variant={data.has_table ? 'outlined' : 'contained'} size={data.has_table ? 'small' : undefined} disabled={reading} onClick={() => fileInput.current?.click()}>
      {reading ? 'Reading file…' : data.has_table ? 'Replace table' : 'Upload table'}
    </UploadButton>
  </span>;
  const fileFacts: MetaItem[] = [
    { label: 'File', value: file.name, mono: true },
    ...(sheets.length === 1 && file.selected_sheet ? [{ label: 'Sheet', value: file.selected_sheet }] : []),
    { label: 'Rows', value: formatCount(data.rows), numeric: true },
    { label: 'Columns', value: formatCount(data.columns), numeric: true },
  ];
  return <Stack spacing={3}>
    {(readError || file.load_error) && <Alert severity="error">{readError || file.load_error}</Alert>}
    <Section title="Research table" description={data.has_table ? undefined : 'CSV or Excel (.csv, .xlsx, .xls). Your original file is never changed.'} actions={data.has_table ? uploadButton : undefined}>
      <Stack spacing={2}>
        {data.has_table ? <MetaList columns={2} items={fileFacts} /> : <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>{uploadButton}<Button variant="outlined" disabled={reading} onClick={() => emit({ type: 'upload_file', filename: DEMO_FILENAME, content_base64: demoCsvBase64(), start_row: 1 })}>Try example data</Button></Stack>}
        {sheets.length > 1 && <FormControl size="small" fullWidth><InputLabel>Excel sheet</InputLabel><Select label="Excel sheet" inputProps={{ 'aria-label': 'Excel sheet' }} value={file.selected_sheet ?? ''} onChange={event => emit({ type: 'set_sheet', sheet: event.target.value })}>{sheets.map(sheet => <MenuItem key={sheet} value={sheet}>{sheet}</MenuItem>)}</Select></FormControl>}
      </Stack>
    </Section>
    <Disclosure title="Import settings" optional summary={`Header row ${formatCount(file.start_row ?? 1)}`}><Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems="start"><TextField size="small" type="number" label="Header row" value={startRow} inputProps={{ min: 1 }} onChange={event => setStartRow(Math.max(1, asNumber(event.target.value, 1)))} helperText="Use a later row if the file starts with notes." />{data.has_table && <RefreshActionButton onClick={() => emit({ type: 'set_start_row', start_row: startRow })}>Reload file</RefreshActionButton>}</Stack></Disclosure>
    {data.has_table && <Section title="Preview" description={<Box component="span" sx={tabular}>Showing {formatCount(data.preview?.length ?? 0)} of {formatCount(data.rows ?? 0)} rows</Box>}><PreviewTable rows={data.preview} label="Uploaded data preview" /></Section>}
  </Stack>;
}

function OmissionPage({ snapshot, emit, onDirty }: { snapshot: Snapshot; emit: Emit; onDirty: (dirty: boolean) => void }) {
  const columns = snapshot.data?.selection_columns ?? snapshot.data?.column_names ?? [];
  const rows = snapshot.data?.selection_preview ?? snapshot.data?.preview ?? [];
  const profiles = snapshot.data?.column_profiles ?? [];
  const serverSelection = JSON.stringify(snapshot.omission?.selected ?? []);
  const [selected, setSelected] = useState<string[]>(snapshot.omission?.selected ?? []);
  const [saveError, setSaveError] = useState(false);
  const [ignored, setIgnored] = useState<string[]>([]);
  const [query, setQuery] = useState('');
  useEffect(() => { setSelected(JSON.parse(serverSelection)); setSaveError(false); onDirty(false); }, [serverSelection]);
  async function save(next: string[]) {
    setSelected(next); onDirty(true);
    const ok = await emit({ type: 'set_omitted_columns', columns: next });
    setSaveError(!ok); onDirty(!ok);
  }
  const visible = columns.filter(column => column.toLowerCase().includes(query.toLowerCase()));
  const suggestions = profiles.filter(profile => profile.suggest_omit && !profile.replaced_by_rule && !selected.includes(profile.name) && !ignored.includes(profile.name));
  return <Stack spacing={3}>
    {saveError && <Alert severity="error" action={<Button color="inherit" size="small" onClick={() => save(selected)}>Retry save</Button>}>Your selection has not been saved. Retry before continuing.</Alert>}
    {suggestions.length > 0 && <Disclosure defaultExpanded title="Suggested exclusions" summary={`${plural(suggestions.length, 'column', 'columns')} to review`}>
      <Stack spacing={1.5}>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}><Button variant="outlined" size="small" onClick={() => save(unique([...selected, ...suggestions.map(profile => profile.name)]))}>Exclude all suggested columns</Button><Button size="small" onClick={() => setIgnored(unique([...ignored, ...suggestions.map(profile => profile.name)]))}>Dismiss all suggestions</Button></Stack>
        <Box role="region" aria-label="All suggested column exclusions" tabIndex={0} sx={{ maxHeight: 340, overflowY: 'auto' }}><Stack divider={<Divider flexItem />} spacing={1.5}>
          {suggestions.map(profile => <Stack key={profile.name} direction={{ xs: 'column', md: 'row' }} alignItems={{ md: 'center' }} spacing={1}>
            <Box sx={{ flex: 1, minWidth: 0 }}><Typography variant="subtitle2" component="p" sx={{ overflowWrap: 'anywhere' }}>{profile.name}</Typography><Typography variant="body2" color="text.secondary">{profile.suggestion_reason || 'These values may not need concept reconciliation.'}</Typography><Typography variant="caption" color="text.secondary">{profile.examples.length ? `Examples: ${profile.examples.join(', ')}` : 'No non-empty examples.'}</Typography></Box>
            <Stack direction="row" spacing={1}><Button variant="outlined" size="small" aria-label={`Exclude values from ${profile.name}`} onClick={() => save(unique([...selected, profile.name]))}>Exclude values</Button><Button size="small" aria-label={`Dismiss suggestion for ${profile.name}`} onClick={() => setIgnored([...ignored, profile.name])}>Dismiss</Button></Stack>
          </Stack>)}
        </Stack></Box>
      </Stack>
    </Disclosure>}
    <Section title="Columns" description="Cell values from ticked columns become terms. Changes save automatically." actions={selected.length > 0 && <Button variant="outlined" size="small" onClick={() => save([])}>Restore all values</Button>}>
      <Stack spacing={1.5}>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} justifyContent="space-between" alignItems={{ sm: 'center' }}>
          <Typography variant="body2" color="text.secondary" sx={tabular}>{plural(selected.length, 'column', 'columns')} excluded</Typography>
          {columns.length > 10 && <TextField size="small" label="Find a column" value={query} onChange={event => setQuery(event.target.value)} helperText={`${formatCount(visible.length)} of ${formatCount(columns.length)} columns shown`} />}
        </Stack>
        <TableContainer tabIndex={0} role="region" aria-label="Choose cell values by column" sx={{ maxHeight: 430, maxWidth: '100%' }}><Table size="small" stickyHeader aria-label="Column selection and example values"><TableHead><MuiTableRow>{visible.map(column => {
          const profile = profiles.find(item => item.name === column);
          const excluded = selected.includes(column);
          return <TableCell key={column} scope="col" sx={{ minWidth: 190, bgcolor: excluded ? 'action.hover' : 'background.paper', verticalAlign: 'top' }}><FormControlLabel sx={{ m: 0, alignItems: 'flex-start' }} control={<Checkbox size="small" checked={!excluded && !profile?.replaced_by_rule} disabled={profile?.replaced_by_rule} onChange={() => save(excluded ? selected.filter(item => item !== column) : [...selected, column])} inputProps={{ 'aria-label': `Include cell values from ${column}` }} />} label={<Typography variant="subtitle2" component="span" sx={{ display: 'block', pt: 1, color: 'text.primary', overflowWrap: 'anywhere' }}>{column}</Typography>} /><Typography variant="caption" display="block" color="text.secondary" sx={tabular}>{profile?.replaced_by_rule ? 'Values replaced by applied rules' : excluded ? 'Values excluded' : profile ? `${formatCount(profile.text_values)} text values` : <Missing />}</Typography></TableCell>;
        })}</MuiTableRow></TableHead><TableBody>{rows.slice(0, 5).map((row, index) => <MuiTableRow key={index}>{visible.map(column => <TableCell key={column} sx={{ bgcolor: selected.includes(column) ? 'action.hover' : undefined, color: 'text.secondary', maxWidth: 280, overflowWrap: 'anywhere' }}>{cellValue(row[column])}</TableCell>)}</MuiTableRow>)}</TableBody></Table></TableContainer>
        {!visible.length && <Typography variant="body2" color="text.secondary">No columns match this search.</Typography>}
      </Stack>
    </Section>
  </Stack>;
}

function ConsolidatePage({ snapshot, emit, onDirty }: { snapshot: Snapshot; emit: Emit; onDirty: (dirty: boolean) => void }) {
  const cons = snapshot.consolidation ?? {};
  const [threshold, setThreshold] = useState(cons.threshold ?? .85);
  const [choices, setChoices] = useState<Record<string, string>>(cons.choices ?? {});
  const [dirty, setDirty] = useState(false);
  useEffect(() => { setChoices(cons.choices ?? {}); setDirty(false); onDirty(false); }, [JSON.stringify(cons.choices), cons.staged]);
  function choose(key: string, value: string) { setChoices(previous => ({ ...previous, [key]: value })); setDirty(true); onDirty(true); }
  return <Stack spacing={2}>
    <Box sx={{ maxWidth: 360 }}>
      <Stack direction="row" alignItems="center" spacing={0.5}><Typography id="similarity-label" variant="body2" sx={tabular}>Spelling similarity: {threshold.toFixed(2)}</Typography><InfoButton title="Similar spellings"><p>Finds terms whose spelling is at least this similar (0.5 to 1). Higher values find fewer, closer variants.</p><p>Similarity alone does not prove that two terms mean the same thing. Nothing is merged until you apply your choices.</p></InfoButton></Stack>
      <Slider aria-labelledby="similarity-label" min={.5} max={1} step={.01} value={threshold} onChange={(_, value) => setThreshold(value as number)} />
    </Box>
    <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ sm: 'center' }}><Box><SearchActionButton disabled={dirty} onClick={() => emit({ type: 'find_similar_terms', threshold })}>Find similar terms</SearchActionButton></Box>{dirty && <Typography variant="body2" color="text.secondary">Apply or discard your choices before searching again.</Typography>}</Stack>
    {!!cons.groups?.length && <Stack divider={<Divider flexItem />} spacing={2}>{cons.groups.map((group, index) => {
      const key = `choice_group_${index}`; const value = choices[key] ?? KEEP_ALL;
      const newTerm = choices[`new_term_group_${index}`] ?? '';
      return <Stack key={index} spacing={1.5}>
        <Typography variant="subtitle2" component="p">Group {index + 1}</Typography>
        {/* Labels wrap: near-identical spellings must be readable in full to compare them. */}
        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>{group.map((term, termIndex) => <Chip key={`${termIndex}-${term}`} size="small" variant="outlined" label={term} sx={{ height: 'auto', '& .MuiChip-label': { whiteSpace: 'normal', overflowWrap: 'anywhere', py: 0.25 } }} />)}</Stack>
        <FormControl size="small" fullWidth><InputLabel id={`choice-label-${index}`}>How to handle group {index + 1}</InputLabel><Select labelId={`choice-label-${index}`} label={`How to handle group ${index + 1}`} value={value} onChange={event => choose(key, String(event.target.value))}>{[KEEP_ALL, ...group, NEW_TERM].map(option => <MenuItem key={option} value={option}>{choiceLabel(option)}</MenuItem>)}</Select></FormControl>
        {value === NEW_TERM && <TextField size="small" required label={`New term for group ${index + 1}`} value={newTerm} error={!newTerm.trim()} helperText={newTerm.trim() ? undefined : 'Enter the new term to apply your choices.'} onChange={event => choose(`new_term_group_${index}`, event.target.value)} />}
      </Stack>;
    })}</Stack>}
    {!!cons.groups?.length && <Box><PrepareButton disabled={!dirty || (cons.groups ?? []).some((_, index) => choices[`choice_group_${index}`] === NEW_TERM && !choices[`new_term_group_${index}`]?.trim())} onClick={async () => { if (await emit({ type: 'stage_consolidations', choices })) { setDirty(false); onDirty(false); } }}>Apply choices</PrepareButton></Box>}
    {cons.searched && !cons.groups?.length && <Typography role="status" variant="body2" color="text.secondary">No similar terms found at this threshold.</Typography>}
    {cons.staged && <Typography variant="body2" color="text.secondary">Confirmed choices are included in the result.</Typography>}
  </Stack>;
}

function ReviewPage({ snapshot, emit, updating, error, handoffOpen, onClose }: { snapshot: Snapshot; emit: Emit; updating: boolean; error: string; handoffOpen: boolean; onClose: () => void }) {
  const matching = snapshot.matching ?? {};
  const handoff = snapshot.handoff;
  const current = matching.is_current && !updating;
  const ready = current && Boolean(matching.rows);
  const confirming = handoff?.status === 'confirmation_required';
  const excluded = snapshot.omission?.selected?.length ?? 0;
  const compact = useMediaQuery('(max-width:599.95px)', { noSsr: true });
  const shell = useShellStatus();
  // The modal hides the page's status area, so a failed hand-off reports inside the dialog.
  const dialogError = error || shell?.error;
  const cancelHandoff = useRef<HTMLButtonElement>(null);
  // The replace confirmation replaces the clicked target button, so focus moves to the safe choice.
  useEffect(() => { if (handoffOpen && confirming && !updating) cancelHandoff.current?.focus(); }, [handoffOpen, confirming, updating]);
  const status = updating ? 'Updating the result…' : matching.error ? 'Update failed. Review the error and retry.' : current ? matching.rows ? 'Up to date and ready for reconciliation.' : 'No terms to reconcile.' : 'Result needs updating.';
  const termDownload = <DownloadButton size="small" disabled={!current || !matching.term_csv} onClick={() => triggerDownload(matching.term_csv ?? '', matching.term_csv_filename ?? 'term_list.csv', 'text/csv;charset=utf-8')}>Download term list (CSV)</DownloadButton>;
  return <Stack spacing={3}>
    {matching.error && <Alert severity="error">{matching.error}</Alert>}
    {current && !matching.rows && <Alert severity="info">No usable text terms were found. Review the selected columns or choose a different source table.</Alert>}
    <Section title="Result" tone={matching.error ? 'error' : ready ? 'success' : 'default'} actions={<RefreshActionButton size="small" disabled={updating} onClick={() => emit({ type: 'generate_matching_table', expected_revision: matching.configuration_revision })}>{matching.error ? 'Retry update' : 'Refresh'}</RefreshActionButton>}>
      <Stack spacing={2}>
        {/* Not a live region: the WorkflowNav hint already announces the same state. */}
        <Stack direction="row" spacing={1} alignItems="center">{ready && <CheckCircleOutlineIcon color="success" fontSize="small" />}<Typography variant="body2">{status}</Typography></Stack>
        <MetaList columns={2} items={[
          { label: 'Terms prepared', value: current ? formatCount(matching.rows ?? 0) : '', numeric: true },
          { label: 'Excluded columns', value: formatCount(excluded), numeric: true },
          { label: 'Value preparation', value: snapshot.preprocessing?.transformations_prepared ? 'Rules applied' : 'Original values' },
          { label: 'Spelling choices', value: snapshot.consolidation?.staged ? 'Applied' : 'No term merges' },
        ]} />
      </Stack>
    </Section>
    {/* On phones the download sits in the body so the title and description keep the full width. */}
    <Section title="Term list" info={{ title: 'Term list', content: <p>The term list documents where each term comes from: its source column and whether it is a cell value or a column heading. Download it to record your selection.</p> }} description={current ? <Box component="span" sx={tabular}>Showing {formatCount(matching.term_preview?.length ?? 0)} of {formatCount(matching.rows ?? 0)} terms. The CSV contains all terms.</Box> : undefined} actions={compact ? undefined : termDownload}>
      <Stack spacing={1.5}>
        {compact && <Box>{termDownload}</Box>}
        {current ? <PreviewTable rows={matching.term_preview} label="Prepared terms and source columns" /> : <EmptyState dense title="No term list yet" description={matching.error ? 'Retry the update in Result to create it.' : 'It appears when the result is up to date.'} />}
        {current && !matching.term_csv && <Typography variant="body2" color="text.secondary">Refresh this saved result to enable the term-list download.</Typography>}
      </Stack>
    </Section>
    <Disclosure title="SSSOM table and more downloads" summary={current ? snapshot.downloads?.preprocessed_available ? 'SSSOM CSV, prepared data CSV and Excel' : 'SSSOM CSV' : 'Available when the result is up to date'}>
      <Stack spacing={2}>
        <Stack direction="row" alignItems="center" spacing={0.5}><Typography variant="subtitle2" component="h3">SSSOM matching table</Typography><InfoButton title="SSSOM matching table"><p>A six-column CSV in the SSSOM mapping format, used for reconciliation and the rest of the workflow.</p><p>It holds the same terms as the term list, without the source columns. Empty URI and mapping fields are filled in during reconciliation.</p></InfoButton></Stack>
        {current && <PreviewTable rows={matching.preview} label="Technical SSSOM preview" monoColumns={SSSOM_IDENTIFIER_COLUMNS} />}
        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
          <DownloadButton disabled={!current || !matching.csv} onClick={() => triggerDownload(matching.csv ?? '', matching.csv_filename ?? 'matching_table.csv', 'text/csv;charset=utf-8')}>Download SSSOM (CSV)</DownloadButton>
          {current && snapshot.downloads?.preprocessed_available && <><DownloadButton onClick={() => triggerDownload(snapshot.downloads?.preprocessed_csv ?? '', snapshot.downloads?.preprocessed_csv_filename ?? 'preprocessed.csv', 'text/csv;charset=utf-8')}>Download prepared data (CSV)</DownloadButton><DownloadButton onClick={() => triggerBase64Download(snapshot.downloads?.preprocessed_xlsx_base64 ?? '', snapshot.downloads?.preprocessed_xlsx_filename ?? 'preprocessed.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}>Download prepared data (Excel)</DownloadButton></>}
        </Stack>
      </Stack>
    </Disclosure>
    <Dialog open={handoffOpen} onClose={updating ? undefined : onClose} fullWidth maxWidth="sm" aria-labelledby="handoff-title" aria-describedby="handoff-message">
      <DialogTitle id="handoff-title">{confirming ? `Replace the table in ${serviceMeta((handoff?.target ?? '') as ServiceId).title}?` : 'Choose how to reconcile'}</DialogTitle>
      <DialogContent><Stack spacing={2}>{dialogError && <Alert severity="error" role="alert">{dialogError}</Alert>}{confirming ? <Alert id="handoff-message" severity="warning">{handoff?.message} Existing work will be replaced. Cancel to save it as a project first.</Alert> : <>
        <Typography id="handoff-message" variant="body2" color="text.secondary">The current table is transferred and the chosen tool opens. Nothing runs automatically.</Typography>
        <Stack divider={<Divider flexItem />} sx={{ border: 1, borderColor: 'divider', borderRadius: 2 }}>{handoffTargets.map(target => <Stack key={target.id} direction={{ xs: 'column', sm: 'row' }} spacing={{ xs: 1, sm: 2 }} alignItems={{ sm: 'center' }} sx={{ p: 2 }}>
          <Box sx={{ flex: 1, minWidth: 0 }}><Typography variant="subtitle2" component="h3">{serviceMeta(target.id).title}</Typography><Typography variant="body2" color="text.secondary">{target.description}</Typography></Box>
          <Button variant="outlined" disabled={updating || !current} onClick={() => emit({ type: 'handoff_reconciliation', target: target.id, expected_revision: matching.configuration_revision })}>{target.action}</Button>
        </Stack>)}</Stack>
      </>}</Stack></DialogContent>
      <DialogActions><Button ref={cancelHandoff} autoFocus={confirming} disabled={updating} onClick={() => { onClose(); }}>Cancel</Button>{confirming && <Button variant="contained" color="warning" disabled={updating || !current} onClick={() => emit({ type: 'handoff_reconciliation', target: handoff?.target, expected_revision: matching.configuration_revision, confirmation_token: handoff?.confirmation_token })}>Replace existing table</Button>}</DialogActions>
    </Dialog>
  </Stack>;
}
function PreprocessPage({ snapshot, emit, onDirty }: { snapshot: Snapshot; emit: Emit; onDirty: (dirty: boolean) => void }) {
  const columns = snapshot.data?.column_names ?? [];
  const prep = snapshot.preprocessing ?? {};
  const preparedSplitColumns = Object.keys(prep.prepared_split_config ?? {});
  const preparedExpandColumn = Object.keys(prep.prepared_expand_config ?? {})[0] ?? '';
  const [splitEnabled, setSplitEnabled] = useState(preparedSplitColumns.length > 0);
  const [splitColumns, setSplitColumns] = useState<string[]>(preparedSplitColumns);
  const [splitRules, setSplitRules] = useState<Record<string, { delimiter: string; names: string }>>(Object.fromEntries(Object.entries(prep.prepared_split_config ?? {}).map(([name, rule]) => [name, { delimiter: rule.delimiter ?? ',', names: (rule.new_names ?? []).join(', ') }])));
  const [autoNames, setAutoNames] = useState(true);
  const [keepSplit, setKeepSplit] = useState(prep.keep_original_split ?? true);
  const [expandEnabled, setExpandEnabled] = useState(Boolean(preparedExpandColumn));
  const [expandColumn, setExpandColumn] = useState(preparedExpandColumn);
  const [expandDelimiter, setExpandDelimiter] = useState(prep.prepared_expand_config?.[preparedExpandColumn]?.delimiter ?? ', ');
  const [expandPrefix, setExpandPrefix] = useState(prep.prepared_expand_config?.[preparedExpandColumn]?.new_col_prefix ?? 'Indicator_');
  const [expandCodes, setExpandCodes] = useState<string[]>(prep.prepared_expand_config?.[preparedExpandColumn]?.codes_to_expand ?? []);
  const [trueValue, setTrueValue] = useState(prep.prepared_expand_config?.[preparedExpandColumn]?.true_value === '$CODE$' ? 'Code itself' : String(prep.prepared_expand_config?.[preparedExpandColumn]?.true_value ?? 'True'));
  const [falseValue, setFalseValue] = useState(prep.prepared_expand_config?.[preparedExpandColumn]?.false_value === null ? 'Empty (None/NA)' : String(prep.prepared_expand_config?.[preparedExpandColumn]?.false_value ?? 'False'));
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

  const signature = JSON.stringify({ splitEnabled, splitColumns, splitRules, autoNames, keepSplit, expandEnabled, expandColumn, expandDelimiter, expandPrefix, expandCodes, trueValue, falseValue, keepExpand });
  const baseline = useRef(signature);
  // The first field Apply rules still needs, shown next to the disabled button.
  function missingRequirement(): string {
    if (splitEnabled) {
      if (!splitColumns.length) return 'Choose at least one column to split.';
      const noDelimiter = splitColumns.find(column => !splitRules[column]?.delimiter);
      if (noDelimiter) return `Enter a delimiter for ${noDelimiter}.`;
      const noNames = splitColumns.find(column => !splitRules[column]?.names.trim());
      if (noNames) return `Enter new names for ${noNames}.`;
    }
    if (expandEnabled) {
      if (!expandColumn) return 'Choose the column containing codes.';
      if (!expandDelimiter) return 'Enter the codes delimiter.';
      if (!expandPrefix) return 'Enter a new column prefix.';
      if (!expandCodes.length) return 'Choose the codes to expand.';
    }
    return '';
  }
  const missing = missingRequirement();
  const edited = signature !== baseline.current;
  useEffect(() => onDirty(signature !== baseline.current), [signature, onDirty]);
  async function prepare() {
    const splitConfig: Record<string, { delimiter: string; new_names: string[] }> = {};
    if (splitEnabled) {
      splitColumns.forEach((column) => {
        const rule = splitRules[column];
        if (rule?.delimiter && rule.names) splitConfig[column] = { delimiter: rule.delimiter, new_names: rule.names.split(',').map(part => part.trim()).filter(Boolean) };
      });
    }
    const expandConfig: Record<string, { delimiter: string; codes_to_expand: string[]; new_col_prefix: string; true_value: string | null; false_value: string | null }> = {};
    if (expandEnabled && expandColumn && expandDelimiter && expandPrefix && expandCodes.length) {
      expandConfig[expandColumn] = { delimiter: expandDelimiter, codes_to_expand: expandCodes, new_col_prefix: expandPrefix, true_value: trueValue === 'Code itself' ? '$CODE$' : trueValue, false_value: falseValue === 'Empty (None/NA)' ? null : falseValue };
    }
    if (splitEnabled && (!splitColumns.length || splitColumns.some(column => !splitRules[column]?.delimiter || !splitRules[column]?.names.trim()))) return;
    if (expandEnabled && (!expandColumn || !expandCodes.length || !expandDelimiter || !expandPrefix)) return;
    if (await emit({ type: 'prepare_transformations', split_config: splitConfig, expand_config: expandConfig, keep_original_split: keepSplit, keep_original_expand: keepExpand })) { baseline.current = signature; onDirty(false); }
  }

  return <Stack spacing={2} divider={<Divider flexItem />}>
    <Stack spacing={1.5}>
      <Stack direction="row" justifyContent="space-between" alignItems="flex-start" spacing={2}><Box sx={{ minWidth: 0 }}><Stack direction="row" alignItems="center" spacing={0.5}><Typography variant="subtitle2" component="p">Split columns by position</Typography><InfoButton title="Splitting columns"><p>Use this when one cell holds several values in a fixed order.</p><p>Example: “Berlin, Germany” split at the comma becomes City = “Berlin” and Country = “Germany”. Name the new columns in the order the values appear.</p></InfoButton></Stack><Typography variant="body2" color="text.secondary">Split one cell into several new columns.</Typography></Box><Switch checked={splitEnabled} onChange={(e) => setSplitEnabled(e.target.checked)} inputProps={{ 'aria-label': 'Enable column splitting' }} /></Stack>
      <Collapse in={splitEnabled}><Stack spacing={1.5}>
        <FormControl fullWidth size="small"><InputLabel>Columns to split</InputLabel><Select multiple label="Columns to split" inputProps={{ 'aria-label': 'Columns to split' }} value={splitColumns} onChange={(e) => setSplitColumns(typeof e.target.value === 'string' ? e.target.value.split(',') : (e.target.value as string[]))} renderValue={(selected) => (selected as string[]).join(', ')}>{columns.map((column) => <MenuItem key={column} value={column}><Checkbox checked={splitColumns.includes(column)} />{column}</MenuItem>)}</Select></FormControl>
        <FormControlLabel control={<Checkbox checked={autoNames} onChange={(e) => setAutoNames(e.target.checked)} />} label="Auto-generate new names as <column>_1, <column>_2 (editable)" />
        {splitColumns.map((column) => <Box key={column} sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '.35fr .65fr' }, gap: 1.5 }}><TextField label={`Delimiter for ${column}`} value={splitRules[column]?.delimiter ?? ','} onChange={(e) => setSplitRules({ ...splitRules, [column]: { ...(splitRules[column] ?? { names: '' }), delimiter: e.target.value } })} /><TextField label={`New names for ${column}`} value={splitRules[column]?.names ?? ''} onChange={(e) => setSplitRules({ ...splitRules, [column]: { ...(splitRules[column] ?? { delimiter: ',' }), names: e.target.value } })} placeholder="For example: City, Country" helperText="Comma-separated, in the order the values appear" /></Box>)}
        <FormControlLabel control={<Checkbox checked={keepSplit} onChange={(e) => setKeepSplit(e.target.checked)} />} label="Keep original split column(s)" />
      </Stack></Collapse>
    </Stack>
    <Stack spacing={1.5}>
      <Stack direction="row" justifyContent="space-between" alignItems="flex-start" spacing={2}><Box sx={{ minWidth: 0 }}><Stack direction="row" alignItems="center" spacing={0.5}><Typography variant="subtitle2" component="p">Expand codes to indicator columns</Typography><InfoButton title="Expanding codes"><p>Use this when one cell lists several codes.</p><p>Example: “ERY, NAL, TET” becomes the columns Indicator_ERY, Indicator_NAL and Indicator_TET. Each shows whether that code is present in the row, using the values you choose.</p></InfoButton></Stack><Typography variant="body2" color="text.secondary">Turn a list of codes into one column per code.</Typography></Box><Switch checked={expandEnabled} onChange={(e) => setExpandEnabled(e.target.checked)} inputProps={{ 'aria-label': 'Enable code expansion' }} /></Stack>
      <Collapse in={expandEnabled}><Stack spacing={1.5}>
        <FormControl fullWidth size="small"><InputLabel>Column containing codes</InputLabel><Select label="Column containing codes" inputProps={{ 'aria-label': 'Column containing codes' }} value={expandColumn} onChange={(e) => { setExpandColumn(String(e.target.value)); setExpandCodes([]); }}>{columns.map((column) => <MenuItem key={column} value={column}>{column}</MenuItem>)}</Select></FormControl>
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '.4fr 1fr auto' }, gap: 1.5, alignItems: 'start' }}><TextField label="Codes delimiter" value={expandDelimiter} onChange={(e) => setExpandDelimiter(e.target.value)} /><TextField label="New column prefix" value={expandPrefix} onChange={(e) => setExpandPrefix(e.target.value)} /><SearchActionButton onClick={() => emit({ type: 'detect_expansion_codes', column: expandColumn, delimiter: expandDelimiter })}>Detect codes</SearchActionButton></Box>
        <FormControl fullWidth size="small"><InputLabel>Codes to expand</InputLabel><Select multiple label="Codes to expand" inputProps={{ 'aria-label': 'Codes to expand' }} value={expandCodes} onChange={(e) => setExpandCodes(typeof e.target.value === 'string' ? e.target.value.split(',') : (e.target.value as string[]))} renderValue={(selected) => (selected as string[]).join(', ')}>{unique([...detectedCodes, ...expandCodes]).map((code) => <MenuItem key={code} value={code}><Checkbox checked={expandCodes.includes(code)} />{code}</MenuItem>)}</Select></FormControl>
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(3,1fr)' }, gap: 1.5 }}><FormControl size="small"><InputLabel>Value if present</InputLabel><Select label="Value if present" inputProps={{ 'aria-label': 'Value if present' }} value={trueValue} onChange={(e) => setTrueValue(String(e.target.value))}>{['True', '1', 'Code itself'].map((value) => <MenuItem key={value} value={value}>{value}</MenuItem>)}</Select></FormControl><FormControl size="small"><InputLabel>Value if absent</InputLabel><Select label="Value if absent" inputProps={{ 'aria-label': 'Value if absent' }} value={falseValue} onChange={(e) => setFalseValue(String(e.target.value))}>{['False', '0', 'Empty (None/NA)'].map((value) => <MenuItem key={value} value={value}>{value}</MenuItem>)}</Select></FormControl><FormControlLabel control={<Checkbox checked={keepExpand} onChange={(e) => setKeepExpand(e.target.checked)} />} label="Keep original codes column" /></Box>
      </Stack></Collapse>
    </Stack>
    <Stack spacing={1}>
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1}><PrepareButton onClick={prepare} disabled={!edited || !snapshot.data?.has_table || Boolean(missing)}>Apply rules</PrepareButton><ConfirmActionButton ButtonComponent={ResetButton} disabled={!prep.transformations_prepared} dialogTitle="Clear the prepared rules?" dialogMessage="This removes the applied split and expand rules and any applied spelling choices, so the original values are used again. You can undo this from History." confirmLabel="Clear prepared rules" onConfirm={() => { void emit({ type: 'clear_transformations' }); }}>Clear prepared rules</ConfirmActionButton></Stack>
      {edited && missing && <Typography variant="body2" color="text.secondary">{missing}</Typography>}
    </Stack>
  </Stack>;
}


export function MatchingTableGeneratorApp({ args, onEvent, busy = false, onDraftChange }: MatchingTableGeneratorProps) {
  const snapshot = args?.snapshot ?? {};
  const [activeStage, setActiveStage] = useState<Stage>(() => {
    if (!snapshot.data?.has_table) return 0;
    try { const saved = Number(sessionStorage.getItem('rdf4risk-preparation-stage')); return saved === 1 || saved === 2 ? saved : 0; }
    catch { return 0; }
  });
  const [pendingCount, setPendingCount] = useState(0);
  const pending = pendingCount > 0;
  const [error, setError] = useState('');
  const [drafts, setDrafts] = useState({ file: false, selection: false, preparation: false, consolidation: false });
  const [draftVersion, setDraftVersion] = useState(0);
  const [handoffOpen, setHandoffOpen] = useState(false);
  const attemptedRevision = useRef<number | null>(null);
  const working = busy || pending;
  const hasDraft = Object.values(drafts).some(Boolean);
  const revision = snapshot.matching?.configuration_revision ?? 0;
  const current = Boolean(snapshot.matching?.is_current);
  const dataLoaded = Boolean(snapshot.data?.has_table);
  useEffect(() => { try { sessionStorage.setItem('rdf4risk-preparation-stage', String(activeStage)); } catch { /* storage can be unavailable */ } }, [activeStage]);
  useEffect(() => { if (!dataLoaded) setActiveStage(0); }, [dataLoaded]);
  useEffect(() => { onDraftChange?.(hasDraft); return () => onDraftChange?.(false); }, [hasDraft, onDraftChange]);
  useEffect(() => { if (!hasDraft) return; const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; }; window.addEventListener('beforeunload', warn); return () => window.removeEventListener('beforeunload', warn); }, [hasDraft]);
  const sourceKey = JSON.stringify([snapshot.file?.name, snapshot.data?.column_names, snapshot.data?.preview]);
  const rulesKey = JSON.stringify([snapshot.preprocessing?.prepared_split_config, snapshot.preprocessing?.prepared_expand_config, snapshot.preprocessing?.keep_original_split, snapshot.preprocessing?.keep_original_expand]);
  useEffect(() => { setDrafts({ file: false, selection: false, preparation: false, consolidation: false }); setDraftVersion(value => value + 1); }, [sourceKey]);
  const emit: Emit = useCallback(async event => {
    setPendingCount(count => count + 1); setError('');
    try {
      if (!onEvent) throw new Error('The backend is not connected. Reload the app.');
      const result = await onEvent({ ...event, nonce: crypto.randomUUID() });
      // A failed request is reported once by the shell, at the top of the WizardPage status area.
      if (result === false) return false;
      const status = (result as any)?.args?.snapshot?.statusMessage;
      if (status?.severity === 'error') throw new Error(status.text);
      return true;
    } catch (failure) { setError(failure instanceof Error ? failure.message : String(failure)); return false; }
    finally { setPendingCount(count => Math.max(0, count - 1)); }
  }, [onEvent]);
  useEffect(() => {
    if (current) { attemptedRevision.current = null; return; }
    if (activeStage !== 2 || !dataLoaded || current || working || hasDraft || snapshot.matching?.error || attemptedRevision.current === revision) return;
    attemptedRevision.current = revision;
    void emit({ type: 'generate_matching_table', expected_revision: revision });
  }, [activeStage, dataLoaded, current, working, hasDraft, revision, snapshot.matching?.error, emit]);
  function setDraft(name: keyof typeof drafts, value: boolean) { setDrafts(previous => previous[name] === value ? previous : { ...previous, [name]: value }); }
  function navigate(stage: Stage) { if (!working && !hasDraft && (stage === 0 || dataLoaded)) setActiveStage(stage); }
  function discardEdits() { setDraftVersion(value => value + 1); setDrafts(previous => ({ ...previous, preparation: false, consolidation: false })); }
  async function resetPreparation() { if (await emit({ type: 'reset_all' })) { setDrafts({ file: false, selection: false, preparation: false, consolidation: false }); setActiveStage(0); attemptedRevision.current = null; } }
  const ready = current && Boolean(snapshot.matching?.rows);
  // Continue and the stepper share one gate: a stage is reachable only when Continue could get there.
  const canMove = !working && !hasDraft && dataLoaded;
  const appliedRules = [...Object.keys(snapshot.preprocessing?.prepared_split_config ?? {}).map(name => `${name} (split values)`), ...Object.keys(snapshot.preprocessing?.prepared_expand_config ?? {}).map(name => `${name} (expand codes)`)];
  const preparationSummary = drafts.preparation ? 'Edits not applied yet' : appliedRules.length ? `Applied, ${appliedRules.join(', ')}` : 'No rules applied';
  const draftSections = [drafts.preparation && 'Prepare combined values', drafts.consolidation && 'Review similar spellings'].filter(Boolean).join(' and ');
  const consolidationSummary = drafts.consolidation ? 'Choices not applied yet' : snapshot.consolidation?.staged ? 'Choices applied' : 'No spelling choices';
  const reconciliationTools = handoffTargets.map(target => serviceMeta(target.id).title).join(' or ');
  const steps = stages.map((stage, index) => ({
    id: stage.id,
    label: stage.label,
    completed: [dataLoaded, dataLoaded && ready && !hasDraft, ready && snapshot.handoff?.status === 'complete'][index],
    disabled: working || hasDraft || (index > 0 && !dataLoaded),
  }));
  const hint = working ? (activeStage === 2 ? 'Updating the result…' : 'Saving changes…')
    : drafts.file ? 'Reading the file…'
    : drafts.selection ? 'Retry saving your column selection to continue.'
    : hasDraft ? 'Apply or discard your edits to continue.'
    : !dataLoaded ? 'Upload a table or try the example data to continue.'
    : activeStage === 0 ? 'Next: choose which values to include.'
    : activeStage === 1 ? 'Next: review the prepared terms.'
    : snapshot.matching?.error ? 'Retry the update to continue.'
    // Not attempted yet means the auto-generation effect is about to start it.
    : !current ? attemptedRevision.current === revision ? 'Refresh the result to continue.' : 'Updating the result…'
    : !snapshot.matching?.rows ? 'Include more columns to continue.'
    : 'Next: choose manual curation or AI-assisted matching.';
  const nav: WorkflowNavProps = {
    // On the first stage Back is the shell's: it returns to the overview (draft guards still apply).
    back: activeStage === 0 ? { disabled: working || hasDraft } : { onClick: () => navigate((activeStage - 1) as Stage), disabled: working || hasDraft, tooltip: `Back to ${stages[activeStage - 1].label}` },
    next: activeStage === 2
      ? { onClick: () => setHandoffOpen(true), disabled: !canMove || !ready, tooltip: `Continue to ${reconciliationTools}` }
      : { onClick: () => navigate((activeStage + 1) as Stage), disabled: !canMove, tooltip: `Continue to ${stages[activeStage + 1].label}` },
    hint,
  };
  const stage = stages[activeStage];
  return <WizardPage
    steps={steps}
    activeIndex={activeStage}
    onStepClick={index => navigate(index as Stage)}
    title={stage.title}
    lead={stage.lead}
    info={stage.info}
    actions={<ToolMenu items={[{ label: 'Reset matching table', icon: <RestartAltIcon fontSize="small" />, disabled: working, onSelect: () => { void resetPreparation(); }, confirm: { title: 'Reset matching table?', message: 'This clears the uploaded table, excluded columns, prepared rules, spelling choices and the generated result. You can undo this from History.', confirmLabel: 'Reset matching table' } }]} />}
    status={<>
      {error && <Alert severity="error">{error}</Alert>}
      {draftSections && <Alert severity="info" action={<Button color="inherit" size="small" disabled={working} onClick={discardEdits}>Discard edits</Button>}>Unapplied edits in {draftSections}. Apply them there, or discard them to continue.</Alert>}
    </>}
    nav={nav}
  >
    <Box component="fieldset" {...{ inert: working ? '' : undefined }} disabled={working} sx={{ border: 0, p: 0, m: 0, minWidth: 0, '& button:disabled': { cursor: 'default', color: 'text.secondary', borderColor: 'divider' } }} aria-label="Matching table settings" aria-busy={working}>
      <Box hidden={activeStage !== 0}><LoadPage snapshot={snapshot} emit={emit} onReading={value => setDraft('file', value)} /></Box>
      <Box hidden={activeStage !== 1}><Stack spacing={3}>
        <OmissionPage key={sourceKey} snapshot={snapshot} emit={emit} onDirty={value => setDraft('selection', value)} />
        <Disclosure title="Prepare combined values" optional summary={preparationSummary}><PreprocessPage key={sourceKey + rulesKey + draftVersion} snapshot={snapshot} emit={emit} onDirty={value => setDraft('preparation', value)} /></Disclosure>
        <Disclosure title="Review similar spellings" optional summary={consolidationSummary}><Stack spacing={2}><ConsolidatePage key={draftVersion} snapshot={snapshot} emit={emit} onDirty={value => setDraft('consolidation', value)} />{snapshot.consolidation?.staged && <Box><ConfirmActionButton ButtonComponent={ResetButton} dialogTitle="Remove the spelling choices?" dialogMessage="This removes your applied spelling choices, so every term keeps its original spelling. You can undo this from History." confirmLabel="Remove spelling choices" onConfirm={() => { void emit({ type: 'clear_consolidations' }); }}>Remove spelling choices</ConfirmActionButton></Box>}</Stack></Disclosure>
      </Stack></Box>
      <Box hidden={activeStage !== 2}><ReviewPage snapshot={snapshot} emit={emit} updating={working} error={error} handoffOpen={handoffOpen} onClose={() => { setHandoffOpen(false); void emit({ type: 'dismiss_handoff' }); }} /></Box>
    </Box>
  </WizardPage>;
}
