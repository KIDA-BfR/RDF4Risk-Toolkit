import React, { useEffect, useMemo, useState } from 'react';
import {
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
  gridFilteredSortedRowIdsSelector,
  useGridApiRef,
  type GridColDef,
  type GridRenderCellParams,
  type GridRowSelectionModel,
} from '@mui/x-data-grid';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import KeyboardIcon from '@mui/icons-material/Keyboard';
import DoneAllIcon from '@mui/icons-material/DoneAll';
import type { AppEvent, ReviewCounts, ReviewItem } from './types';
import {
  editableSkosMatchTypes,
  formatCount,
  matchTypes,
  normalizeEditableSkosMatchType,
  reviewStatuses,
  skosColor,
  statusColor,
  statusLabel,
  unique,
} from './utils';
import { fonts, NAV_SNACKBAR_OFFSET } from '../../theme/tokens';
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

// Count facets above the grid, in workflow order; each one toggles the Status filter.
const COUNT_KEYS = ['pending', 'matched', 'candidate_suggested', 'accepted', 'rejected', 'no_match'] as const;
const DECIDED_KEYS = ['accepted', 'rejected', 'no_match'] as const;
// Rows whose decision is still open: a changed match type there is unsaved until Accept.
const OPEN_KEYS: readonly string[] = ['pending', 'matched', 'candidate_suggested'];
const monoSx = { fontFamily: fonts.mono, fontSize: '0.8125rem' } as const;

export function ReviewGrid({
  items,
  counts,
  emit,
  onOpenDetails,
  onDraftChange,
}: {
  items: ReviewItem[];
  counts?: ReviewCounts;
  emit: (event: AppEvent) => void;
  // `order` is the grid's visible row order (facets + sort), so the drawer can step through it.
  onOpenDetails: (item: ReviewItem, order: string[]) => void;
  // True while a changed match type on an open row has not been accepted yet.
  onDraftChange?: (dirty: boolean) => void;
}) {
  const apiRef = useGridApiRef();
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
  const hasUnsavedTypes = useMemo(
    () => Object.entries(selectedMatchTypes).some(([id, value]) => {
      const item = byId.get(id);
      return Boolean(item) && OPEN_KEYS.includes(String(item?.status || 'pending')) && canAcceptItem(item as ReviewItem) && value !== normalizeEditableSkosMatchType(item?.match_type || item?.accepted_match_type);
    }),
    [selectedMatchTypes, byId],
  );
  useEffect(() => { onDraftChange?.(hasUnsavedTypes); }, [hasUnsavedTypes, onDraftChange]);
  // Leaving the stage unmounts the grid and its edits with it.
  useEffect(() => () => onDraftChange?.(false), [onDraftChange]);

  const selectedItems = (selection as Array<string | number>).map((id) => byId.get(String(id))).filter(Boolean) as ReviewItem[];
  const selectedAcceptable = selectedItems.filter(canAcceptItem);

  // Single-row actions, routed through notify() so they get an Undo snackbar.
  const acceptOne = (item: ReviewItem) => { emit({ type: 'accept_mapping', mapping_id: item.mapping_id, selected_match_type: matchTypeFor(item) }); notify(`Accepted “${item.term}”`, [item.mapping_id]); };
  const rejectOne = (item: ReviewItem) => { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); notify(`Rejected “${item.term}”`, [item.mapping_id]); };
  // Confirming a no-match decision is the same backend event as Reject, reported in its own words.
  const confirmNoMatchOne = (item: ReviewItem) => { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); notify(`Confirmed no match for “${item.term}”`, [item.mapping_id]); };
  // "Reset" is reserved for resetting a whole tool; per row this clears the decision.
  const clearDecision = (item: ReviewItem) => { emit({ type: 'reset_mapping', mapping_id: item.mapping_id }); notify(`Cleared the decision for “${item.term}”`); };
  const openDetails = (item: ReviewItem) => onOpenDetails(item, gridFilteredSortedRowIdsSelector(apiRef).map(String));

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
  const selectedRejectable = selectedItems.filter((i) => !isNoMatch(i)).length;
  const countTotal = COUNT_KEYS.reduce((sum, key) => sum + Number(counts?.[key] ?? 0), 0);
  const decided = DECIDED_KEYS.reduce((sum, key) => sum + Number(counts?.[key] ?? 0), 0);
  const allAutoAccepted = scope === 'needs_review' && autoAcceptedCount > 0;
  const emptyMessage = allAutoAccepted
    ? `Nothing needs manual review: all ${autoAcceptedCount} match(es) were auto-accepted. Switch to “All” to review them.`
    : 'No review rows match the selected facets.';
  const NoRowsOverlay = useMemo(() => function NoRows() {
    return (
      <Stack role="status" direction="row" spacing={1} alignItems="center" justifyContent="center" sx={{ height: '100%', px: 2, textAlign: 'center', color: 'text.secondary' }}>
        {allAutoAccepted && <CheckCircleOutlineIcon color="success" fontSize="small" />}
        <Typography variant="body2">{emptyMessage}</Typography>
      </Stack>
    );
  }, [allAutoAccepted, emptyMessage]);

  const columns: GridColDef<ReviewItem>[] = [
    { field: 'term', headerName: 'Term', flex: 1.1, minWidth: 150 },
    {
      field: 'status',
      headerName: 'Status',
      width: 150,
      sortable: true,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => <Chip size="small" variant="outlined" color={statusColor(p.row.status)} label={statusLabel(p.row.status)} />,
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
        if (!canAcceptItem(item)) return <Chip size="small" variant="outlined" color={skosColor(item.match_type)} label={item.match_type || 'no_match'} sx={monoSx} />;
        const value = matchTypeFor(item);
        return (
          <FormControl size="small" fullWidth>
            <Select
              value={value}
              inputProps={{ 'aria-label': `SKOS match type for ${item.term}` }}
              onChange={(e) => setSelectedMatchTypes((c) => ({ ...c, [item.mapping_id]: normalizeEditableSkosMatchType(String(e.target.value)) }))}
              sx={{ ...monoSx, '& .MuiSelect-select': { py: 0.5, px: 1 } }}
            >
              {editableSkosMatchTypes.map((o) => <MenuItem key={o} value={o} sx={monoSx}>{o}</MenuItem>)}
            </Select>
          </FormControl>
        );
      },
    },
    { field: 'provider', headerName: 'Provider', width: 120, renderCell: (p) => (isNoMatch(p.row) ? <Typography variant="body2" color="text.secondary">—</Typography> : p.row.provider) },
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
            <Tooltip title={uri}><Typography variant="body2" noWrap sx={{ flex: 1, ...monoSx }}>{uri}</Typography></Tooltip>
            <Tooltip title="Copy URI"><IconButton size="small" aria-label={`Copy URI for ${p.row.term}`} onClick={() => navigator.clipboard?.writeText(uri)}><ContentCopyIcon fontSize="inherit" /></IconButton></Tooltip>
          </Stack>
        );
      },
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 280,
      sortable: false,
      filterable: false,
      renderCell: (p: GridRenderCellParams<ReviewItem>) => {
        const item = p.row;
        const noMatch = isNoMatch(item);
        return (
          <Stack direction="row" spacing={0.5}>
            <Button size="small" onClick={() => openDetails(item)}>Details</Button>
            {canAcceptItem(item) ? (
              <Button size="small" color="success" onClick={() => acceptOne(item)}>Accept</Button>
            ) : noMatch ? (
              <Button size="small" color="warning" onClick={() => confirmNoMatchOne(item)}>Confirm no match</Button>
            ) : (
              <Button size="small" color="success" disabled>Accept</Button>
            )}
            {!noMatch && <Button size="small" color="warning" onClick={() => rejectOne(item)}>Reject</Button>}
            <Button size="small" onClick={() => clearDecision(item)}>Clear decision</Button>
          </Stack>
        );
      },
    },
  ];

  return (
    <Stack spacing={1.5}>
      {/* Status counts double as quick filters */}
      {counts && (
        <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
          <Typography variant="body2" sx={{ fontVariantNumeric: 'tabular-nums', mr: 0.5 }}>
            {formatCount(decided)} of {formatCount(countTotal)} decided
          </Typography>
          {COUNT_KEYS.map((key) => (
            <Chip
              key={key}
              size="small"
              color={statusColor(key)}
              variant={status === key ? 'filled' : 'outlined'}
              aria-pressed={status === key}
              onClick={() => {
                const next = status === key ? 'all' : key;
                setStatus(next);
                // The chip counts are totals, so a chosen status shows every row, auto-accepted included.
                if (next !== 'all') setScopeOverride('all');
              }}
              label={<>{statusLabel(key)} <Box component="span" sx={{ fontVariantNumeric: 'tabular-nums' }}>{formatCount(counts[key] ?? 0)}</Box></>}
            />
          ))}
        </Stack>
      )}

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
        <FormControl size="small" sx={{ minWidth: 170 }}><InputLabel>Status</InputLabel><Select label="Status" value={status} onChange={(e) => setStatus(String(e.target.value))}>{reviewStatuses.map((s) => <MenuItem key={s} value={s}>{s === 'all' ? 'All' : statusLabel(s)}</MenuItem>)}</Select></FormControl>
        <FormControl size="small" sx={{ minWidth: 170 }}><InputLabel>Match type</InputLabel><Select label="Match type" value={matchType} onChange={(e) => setMatchType(String(e.target.value))}>{matchTypes.map((m) => <MenuItem key={m} value={m} sx={m.startsWith('skos:') ? monoSx : undefined}>{m === 'all' ? 'All' : m === 'no_match' ? 'No match' : m}</MenuItem>)}</Select></FormControl>
        <FormControl size="small" sx={{ minWidth: 140 }}><InputLabel>Provider</InputLabel><Select label="Provider" value={provider} onChange={(e) => setProvider(String(e.target.value))}><MenuItem value="all">All</MenuItem>{providers.map((p) => <MenuItem key={p} value={p}>{p}</MenuItem>)}</Select></FormControl>
        <ToggleButtonGroup size="small" exclusive value={band} onChange={(_e, v) => v && setBand(v)} aria-label="Confidence band filter">
          <ToggleButton value="all">All</ToggleButton>
          <ToggleButton value="high" color="success">High</ToggleButton>
          <ToggleButton value="medium" color="warning">Medium</ToggleButton>
          <ToggleButton value="low" color="error">Low</ToggleButton>
        </ToggleButtonGroup>
        <Tooltip title="Step through the rows in view one at a time, with keyboard shortcuts">
          <Box component="span" sx={{ ml: { md: 'auto' } }}>
            <Button startIcon={<KeyboardIcon />} variant="outlined" onClick={() => setTriageOpen(true)} disabled={!filtered.length}>Review one by one</Button>
          </Box>
        </Tooltip>
      </Stack>

      {/* Bulk toolbar */}
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems={{ md: 'center' }} flexWrap="wrap" useFlexGap sx={{ p: 1, borderRadius: 2, bgcolor: 'rdf.subtle', border: 1, borderColor: 'divider' }}>
        <Typography variant="body2" sx={{ fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{selection.length ? `${formatCount(selection.length)} selected` : `${formatCount(filtered.length)} in view`}</Typography>
        {selection.length > 0 && (
          <>
            <Button size="small" variant="outlined" color="success" onClick={bulkAcceptSelected} disabled={!selectedAcceptable.length}>Accept selected ({selectedAcceptable.length})</Button>
            <Button size="small" variant="outlined" color="warning" onClick={bulkRejectSelected} disabled={!selectedRejectable}>Reject selected ({selectedRejectable})</Button>
          </>
        )}
        <Box sx={{ flexGrow: 1 }} />
        <TextField size="small" type="number" label="Threshold" value={threshold} onChange={(e) => setThreshold(Math.max(0, Math.min(1, Number(e.target.value) || 0)))} inputProps={{ min: 0, max: 1, step: 0.05 }} sx={{ width: 110 }} />
        <Tooltip title="Accept the best candidate for every acceptable row in view at or above the threshold">
          <span>
            <Button size="small" variant="outlined" startIcon={<DoneAllIcon />} onClick={acceptAllAboveThreshold} disabled={!aboveThresholdCount}>
              Accept all ≥ {threshold.toFixed(2)} ({aboveThresholdCount})
            </Button>
          </span>
        </Tooltip>
      </Stack>

      <Box sx={{ height: 540, width: '100%' }}>
        <DataGrid
          apiRef={apiRef}
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
          slots={{ noRowsOverlay: NoRowsOverlay }}
          sx={{ borderColor: 'divider', borderRadius: 2, '& .MuiDataGrid-columnHeaders': { bgcolor: 'background.default' }, '& .MuiDataGrid-columnHeaderTitle': { fontWeight: 600 } }}
        />
      </Box>

      <TriageDialog open={triageOpen} items={filtered} emit={emit} onClose={() => setTriageOpen(false)} />

      <Snackbar
        open={snack.open}
        autoHideDuration={6000}
        onClose={(_e, reason) => { if (reason !== 'clickaway') setSnack((s) => ({ ...s, open: false })); }}
        message={snack.message}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        // Above the fixed WorkflowNav, never over Back, Continue or the hint.
        sx={{ bottom: { xs: NAV_SNACKBAR_OFFSET.xs, sm: NAV_SNACKBAR_OFFSET.sm } }}
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
