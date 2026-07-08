import React, { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Snackbar,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
} from '@mui/material';
import {
  DataGrid,
  type GridColDef,
  type GridRenderCellParams,
  type GridRowSelectionModel,
} from '@mui/x-data-grid';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import KeyboardIcon from '@mui/icons-material/Keyboard';
import DoneAllIcon from '@mui/icons-material/DoneAll';
import type { AppEvent, ReviewItem } from './types';
import {
  editableSkosMatchTypes,
  matchTypes,
  normalizeEditableSkosMatchType,
  reviewStatuses,
  reviewStatusChipSx,
  skosChipSx,
  statusLabel,
  unique,
} from './utils';
import { ConfidenceCell } from '../../components/review/ConfidenceCell';
import { confidenceBand, toConfidenceNumber, type ConfidenceBand } from '../../components/review/confidence';
import { TriageDialog } from './TriageDialog';

// The displayed URI/label: prefer the backend's explicit UI contract, fall back to the
// legacy suggestion fields for older snapshots.
function suggestionUri(item: ReviewItem) {
  return String(item.final_ui_uri ?? item.suggested_uri ?? '').trim();
}
function suggestionLabel(item: ReviewItem) {
  return String(item.final_ui_label ?? item.suggested_label ?? '').trim();
}
// "No suggestion" is a property of the STATUS + whether a candidate URI survived — NOT of
// match_type. A candidate_suggested/matched row with a valid URI but a weak/blank SKOS
// relation must still render its label/URI and stay acceptable.
function isNoMatch(item: ReviewItem) {
  const status = String(item.final_ui_status || item.status || '').toLowerCase();
  if (status === 'no_match' || status === 'timeout') return true;
  return !suggestionUri(item);
}
function canAcceptItem(item: ReviewItem) {
  if (typeof item.review_action_available === 'boolean') {
    return item.can_accept !== false && item.review_action_available;
  }
  return item.can_accept !== false && !isNoMatch(item) && Boolean(suggestionUri(item));
}

export function ReviewGrid({
  items,
  emit,
  onOpenDetails,
}: {
  items: ReviewItem[];
  emit: (event: AppEvent) => void;
  onOpenDetails: (item: ReviewItem) => void;
}) {
  const [status, setStatus] = useState('all');
  const [matchType, setMatchType] = useState('all');
  const [provider, setProvider] = useState('all');
  const [band, setBand] = useState<ConfidenceBand | 'all'>('all');
  // Scope: when auto-accept produced auto-accepts, default to only the matches that
  // still need human judgment; the curator can switch to "All" to see everything.
  // scopeOverride=null means "follow the data-driven default" until the user picks.
  const [scopeOverride, setScopeOverride] = useState<'needs_review' | 'all' | null>(null);
  const [selectedMatchTypes, setSelectedMatchTypes] = useState<Record<string, string>>({});
  const [selection, setSelection] = useState<GridRowSelectionModel>([]);
  const [threshold, setThreshold] = useState(0.9);
  const [triageOpen, setTriageOpen] = useState(false);
  const [snack, setSnack] = useState<{ open: boolean; message: string; undoIds: string[] }>({ open: false, message: '', undoIds: [] });

  const notify = (message: string, undoIds: string[] = []) => setSnack({ open: true, message, undoIds });
  const undoLast = () => {
    if (snack.undoIds.length) emit({ type: 'reset_mappings', mapping_ids: snack.undoIds });
    setSnack((s) => ({ ...s, open: false }));
  };

  const providers = unique(items.map((i) => String(i.provider || '')).filter(Boolean));

  // The number the grid sorts/filters/bulk-acts on: the acceptance score when auto-accept
  // is active (server provides it, capped below the accept line for held rows), else the raw
  // match confidence. Keeps the column, sort, band facet, and "Accept all" all consistent.
  const scoreOf = (item: ReviewItem): number | null =>
    typeof item.acceptance_score === 'number' ? item.acceptance_score : toConfidenceNumber(item.confidence);

  const autoAcceptedCount = useMemo(() => items.filter((i) => i.auto_accepted).length, [items]);
  // Default to "needs review" only when the auto-accept pattern actually accepted
  // something; otherwise there is nothing to hide, so show everything.
  const scope: 'needs_review' | 'all' = scopeOverride ?? (autoAcceptedCount > 0 ? 'needs_review' : 'all');

  const filtered = useMemo(
    () =>
      items.filter(
        (item) =>
          (scope === 'all' || !item.auto_accepted) &&
          (status === 'all' || String(item.status || 'pending') === status) &&
          (matchType === 'all' || String(item.match_type || 'no_match') === matchType) &&
          (provider === 'all' || String(item.provider || '') === provider) &&
          (band === 'all' || confidenceBand(scoreOf(item)) === band),
      ),
    [items, status, matchType, provider, band, scope],
  );

  const byId = useMemo(() => new Map(items.map((i) => [i.mapping_id, i])), [items]);
  const matchTypeFor = (item: ReviewItem) => selectedMatchTypes[item.mapping_id] || normalizeEditableSkosMatchType(item.match_type || item.accepted_match_type);

  const selectedItems = (selection as Array<string | number>).map((id) => byId.get(String(id))).filter(Boolean) as ReviewItem[];
  const selectedAcceptable = selectedItems.filter(canAcceptItem);

  // Single-row actions, routed through notify() so they get an Undo snackbar.
  const acceptOne = (item: ReviewItem) => { emit({ type: 'accept_mapping', mapping_id: item.mapping_id, selected_match_type: matchTypeFor(item) }); notify(`Accepted “${item.term}”`, [item.mapping_id]); };
  const rejectOne = (item: ReviewItem) => { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); notify(`Rejected “${item.term}”`, [item.mapping_id]); };
  const resetOne = (item: ReviewItem) => { emit({ type: 'reset_mapping', mapping_id: item.mapping_id }); notify(`Reset “${item.term}”`); };

  function bulkAcceptSelected() {
    const ids = selectedAcceptable.map((i) => i.mapping_id);
    if (!ids.length) return;
    const match_types: Record<string, string> = {};
    selectedAcceptable.forEach((i) => { match_types[i.mapping_id] = matchTypeFor(i); });
    emit({ type: 'accept_mappings', mapping_ids: ids, match_types });
    setSelection([]);
    notify(`Accepted ${ids.length} mapping(s)`, ids);
  }
  function bulkRejectSelected() {
    const ids = selectedItems.filter((i) => !isNoMatch(i)).map((i) => i.mapping_id);
    if (!ids.length) return;
    emit({ type: 'reject_mappings', mapping_ids: ids });
    setSelection([]);
    notify(`Rejected ${ids.length} mapping(s)`, ids);
  }
  function acceptAllAboveThreshold() {
    const targets = filtered.filter((i) => canAcceptItem(i) && (scoreOf(i) ?? 0) >= threshold);
    if (!targets.length) return;
    const match_types: Record<string, string> = {};
    targets.forEach((i) => { match_types[i.mapping_id] = matchTypeFor(i); });
    const ids = targets.map((i) => i.mapping_id);
    emit({ type: 'accept_mappings', mapping_ids: ids, match_types });
    setSelection([]);
    notify(`Accepted ${ids.length} mapping(s) ≥ ${threshold.toFixed(2)}`, ids);
  }

  const aboveThresholdCount = filtered.filter((i) => canAcceptItem(i) && (scoreOf(i) ?? 0) >= threshold).length;

  const columns: GridColDef<ReviewItem>[] = [
    { field: 'term', headerName: 'Term', flex: 1.1, minWidth: 150 },
    {
      field: 'status',
      headerName: 'Status',
      width: 150,
      sortable: true,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => <Chip size="small" label={statusLabel(p.row.status)} sx={reviewStatusChipSx(p.row.status)} />,
    },
    {
      field: 'confidence',
      headerName: 'Confidence',
      description: 'When auto-accept is on, this is an acceptance score: auto-accepted matches sit in the top band; matches still needing review rank below them (a strong one can still be “High”). Hover a cell for details.',
      width: 130,
      valueGetter: (_v, row) => scoreOf(row) ?? -1,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => {
        const acc = typeof p.row.acceptance_score === 'number';
        const noCandidate = isNoMatch(p.row) || !suggestionUri(p.row);
        return (
          <ConfidenceCell
            value={acc ? p.row.acceptance_score : p.row.confidence}
            trace={p.row.trace_metadata}
            rawConfidence={acc ? p.row.confidence : undefined}
            acceptanceMode={acc}
            autoAccepted={p.row.auto_accepted}
            noCandidate={noCandidate}
          />
        );
      },
    },
    {
      field: 'match_type',
      headerName: 'Match type',
      width: 200,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => {
        const item = p.row;
        if (!canAcceptItem(item)) return <Chip size="small" label={item.match_type || 'no_match'} sx={skosChipSx(item.match_type)} />;
        const value = matchTypeFor(item);
        return (
          <FormControl size="small" fullWidth>
            <Select
              value={value}
              onChange={(e) => setSelectedMatchTypes((c) => ({ ...c, [item.mapping_id]: normalizeEditableSkosMatchType(String(e.target.value)) }))}
              sx={{ fontSize: 13, borderRadius: 999, ...skosChipSx(value), '& .MuiSelect-select': { py: 0.4, px: 1.2 }, '& fieldset': { borderColor: 'transparent' } }}
            >
              {editableSkosMatchTypes.map((o) => <MenuItem key={o} value={o}>{o}</MenuItem>)}
            </Select>
          </FormControl>
        );
      },
    },
    { field: 'provider', headerName: 'Provider', width: 120, renderCell: (p) => (isNoMatch(p.row) ? '—' : p.row.provider) },
    { field: 'suggested_label', headerName: 'Suggested label', flex: 1, minWidth: 140, renderCell: (p) => (isNoMatch(p.row) ? <Typography variant="body2" color="text.secondary">No suggestion</Typography> : suggestionLabel(p.row)) },
    {
      field: 'suggested_uri',
      headerName: 'Suggested URI',
      flex: 1.2,
      minWidth: 200,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => {
        if (isNoMatch(p.row)) return <Typography variant="body2" color="text.secondary">—</Typography>;
        const uri = suggestionUri(p.row);
        return (
          <Stack direction="row" spacing={0.5} alignItems="center" sx={{ width: '100%' }}>
            <Tooltip title={uri}><Typography variant="body2" noWrap sx={{ flex: 1 }}>{uri}</Typography></Tooltip>
            <Tooltip title="Copy URI"><IconButton size="small" aria-label={`Copy URI for ${p.row.term}`} onClick={() => navigator.clipboard?.writeText(uri)}><ContentCopyIcon sx={{ fontSize: 15 }} /></IconButton></Tooltip>
          </Stack>
        );
      },
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 240,
      sortable: false,
      filterable: false,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => {
        const item = p.row;
        const noMatch = isNoMatch(item);
        return (
          <Stack direction="row" spacing={0.5}>
            <Button size="small" onClick={() => onOpenDetails(item)}>Details</Button>
            {canAcceptItem(item) ? (
              <Button size="small" color="success" onClick={() => acceptOne(item)}>Accept</Button>
            ) : noMatch ? (
              <Button size="small" color="warning" onClick={() => rejectOne(item)}>No match</Button>
            ) : (
              <Button size="small" color="success" disabled>Accept</Button>
            )}
            {!noMatch && <Button size="small" color="warning" onClick={() => rejectOne(item)}>Reject</Button>}
            <Button size="small" onClick={() => resetOne(item)}>Reset</Button>
          </Stack>
        );
      },
    },
  ];

  return (
    <Stack spacing={1.5}>
      {/* Facets */}
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems={{ md: 'center' }} flexWrap="wrap" useFlexGap>
        <ToggleButtonGroup size="small" exclusive value={scope} onChange={(_e, v) => v && setScopeOverride(v)} aria-label="Review scope">
          <ToggleButton value="needs_review">Needs review{autoAcceptedCount ? ` (${items.length - autoAcceptedCount})` : ''}</ToggleButton>
          <ToggleButton value="all">All ({items.length})</ToggleButton>
        </ToggleButtonGroup>
        {scope === 'needs_review' && autoAcceptedCount > 0 && (
          <Tooltip title="Matches auto-accepted by your auto-accept policy are hidden. Switch to “All” to review them too.">
            <Chip size="small" color="success" variant="outlined" label={`${autoAcceptedCount} auto-accepted hidden`} />
          </Tooltip>
        )}
        <FormControl size="small" sx={{ minWidth: 150 }}><InputLabel>Status</InputLabel><Select label="Status" value={status} onChange={(e) => setStatus(String(e.target.value))}>{reviewStatuses.map((s) => <MenuItem key={s} value={s}>{s}</MenuItem>)}</Select></FormControl>
        <FormControl size="small" sx={{ minWidth: 170 }}><InputLabel>Match type</InputLabel><Select label="Match type" value={matchType} onChange={(e) => setMatchType(String(e.target.value))}>{matchTypes.map((m) => <MenuItem key={m} value={m}>{m}</MenuItem>)}</Select></FormControl>
        <FormControl size="small" sx={{ minWidth: 140 }}><InputLabel>Provider</InputLabel><Select label="Provider" value={provider} onChange={(e) => setProvider(String(e.target.value))}><MenuItem value="all">all</MenuItem>{providers.map((p) => <MenuItem key={p} value={p}>{p}</MenuItem>)}</Select></FormControl>
        <ToggleButtonGroup size="small" exclusive value={band} onChange={(_e, v) => v && setBand(v)} aria-label="Confidence band filter">
          <ToggleButton value="all">All</ToggleButton>
          <ToggleButton value="high" color="success">High</ToggleButton>
          <ToggleButton value="medium" color="warning">Medium</ToggleButton>
          <ToggleButton value="low" color="error">Low</ToggleButton>
        </ToggleButtonGroup>
        <Button startIcon={<KeyboardIcon />} variant="outlined" sx={{ ml: { md: 'auto' } }} onClick={() => setTriageOpen(true)} disabled={!filtered.length}>Review mode</Button>
      </Stack>

      {/* Bulk toolbar */}
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems={{ md: 'center' }} flexWrap="wrap" useFlexGap sx={{ p: 1, borderRadius: 2, bgcolor: 'background.default', border: '1px solid', borderColor: 'divider' }}>
        <Typography variant="body2" sx={{ fontWeight: 700 }}>{selection.length ? `${selection.length} selected` : `${filtered.length} in view`}</Typography>
        {selection.length > 0 && (
          <>
            <Button size="small" variant="contained" color="success" onClick={bulkAcceptSelected} disabled={!selectedAcceptable.length}>Accept selected ({selectedAcceptable.length})</Button>
            <Button size="small" variant="outlined" color="warning" onClick={bulkRejectSelected}>Reject selected</Button>
          </>
        )}
        <Box sx={{ flexGrow: 1 }} />
        <TextField size="small" type="number" label="Threshold" value={threshold} onChange={(e) => setThreshold(Math.max(0, Math.min(1, Number(e.target.value) || 0)))} inputProps={{ min: 0, max: 1, step: 0.05 }} sx={{ width: 110 }} />
        <Tooltip title="Accept the best candidate for every acceptable row in view at or above the threshold">
          <span>
            <Button size="small" variant="contained" startIcon={<DoneAllIcon />} onClick={acceptAllAboveThreshold} disabled={!aboveThresholdCount}>
              Accept all ≥ {threshold.toFixed(2)} ({aboveThresholdCount})
            </Button>
          </span>
        </Tooltip>
      </Stack>

      <Box sx={{ height: 540, width: '100%' }}>
        <DataGrid
          rows={filtered}
          columns={columns}
          getRowId={(r) => r.mapping_id}
          density="compact"
          checkboxSelection
          disableRowSelectionOnClick
          rowSelectionModel={selection}
          onRowSelectionModelChange={(m) => setSelection(m)}
          pageSizeOptions={[25, 50, 100]}
          initialState={{ pagination: { paginationModel: { pageSize: 25 } }, sorting: { sortModel: [{ field: 'confidence', sort: 'asc' }] } }}
          sx={{ borderColor: 'divider', borderRadius: 2, '& .MuiDataGrid-columnHeaders': { bgcolor: 'background.default' }, '& .MuiDataGrid-columnHeaderTitle': { fontWeight: 700 } }}
        />
      </Box>
      {!filtered.length && (
        <Alert severity={scope === 'needs_review' && autoAcceptedCount > 0 ? 'success' : 'info'}>
          {scope === 'needs_review' && autoAcceptedCount > 0
            ? `Nothing needs manual review — all ${autoAcceptedCount} match(es) were auto-accepted. Switch to “All” to review them.`
            : 'No review rows match the selected facets.'}
        </Alert>
      )}

      <TriageDialog open={triageOpen} items={filtered} emit={emit} onClose={() => setTriageOpen(false)} />

      <Snackbar
        open={snack.open}
        autoHideDuration={6000}
        onClose={(_e, reason) => { if (reason !== 'clickaway') setSnack((s) => ({ ...s, open: false })); }}
        message={snack.message}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        action={
          snack.undoIds.length ? (
            <Button color="primary" size="small" onClick={undoLast}>Undo</Button>
          ) : (
            <Button color="inherit" size="small" onClick={() => setSnack((s) => ({ ...s, open: false }))}>Dismiss</Button>
          )
        }
      />
    </Stack>
  );
}
