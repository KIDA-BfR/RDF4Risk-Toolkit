import React, { useEffect, useId, useRef, useState } from 'react';
import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogContent,
  DialogTitle,
  FormControl,
  IconButton,
  InputLabel,
  LinearProgress,
  MenuItem,
  Select,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import type { AppEvent, ReviewItem } from './types';
import { editableSkosMatchTypes, formatCount, normalizeEditableSkosMatchType, statusColor, statusLabel } from './utils';
import { ConfidenceCell } from '../../components/review/ConfidenceCell';
import { InfoButton } from '../../components/help/InfoButton';
import { MetaList } from '../../components/layout/MetaList';
import { EmptyState } from '../../components/states/EmptyState';
import { fonts } from '../../theme/tokens';
import { ComparisonRow } from './ComparisonRow';

const KEY_LEGEND: Array<[string, string]> = [
  ['A / Enter', 'Accept'],
  ['R', 'Reject'],
  ['N', 'Confirm no match'],
  ['S / J / ↓', 'Next term (skip)'],
  ['K / ↑', 'Previous term'],
  ['U', 'Clear decision'],
  ['Esc', 'Close'],
];

// Enter on a focused control already activates it (button click, select opening). Handling it
// globally as well would fire a second action — possibly on the following term.
const INTERACTIVE = 'button, a[href], [role="button"], [role="combobox"], [role="option"], [role="menuitem"], [role="tab"]';

function isNoMatch(item: ReviewItem) {
  return String(item.status || '').toLowerCase() === 'no_match' || String(item.match_type || '').toLowerCase() === 'no_match';
}
function canAccept(item: ReviewItem) {
  return item.can_accept !== false && !isNoMatch(item) && Boolean(String(item.suggested_uri || '').trim());
}

// Focused, keyboard-driven one-row-at-a-time review over the currently faceted list. Single
// keystrokes resolve each term and auto-advance — the hands-on-keyboard flow for hundreds of
// repetitive judgments. Reuses the existing accept/reject/reset backend events.
export function TriageDialog({
  open,
  items,
  emit,
  onClose,
}: {
  open: boolean;
  items: ReviewItem[];
  emit: (event: AppEvent) => void;
  onClose: () => void;
}) {
  const titleId = useId();
  // Track the current row by id, not by position: when a backend refresh or a facet drops a row
  // from the list, the view stays on the same term instead of silently skipping one.
  const [currentId, setCurrentId] = useState<string | null>(null);
  const lastIndex = useRef(0);
  const found = currentId == null ? -1 : items.findIndex((entry) => entry.mapping_id === currentId);
  // A row that left the list keeps its position, which now holds the following term.
  const index = found >= 0 ? found : Math.min(lastIndex.current, Math.max(0, items.length - 1));
  const item = items[index];
  const [matchType, setMatchType] = useState('');

  useEffect(() => { lastIndex.current = index; });

  useEffect(() => {
    if (open) {
      lastIndex.current = 0;
      setCurrentId(items[0]?.mapping_id ?? null);
    }
  }, [open]);

  useEffect(() => {
    if (item) setMatchType(normalizeEditableSkosMatchType(item.match_type || item.accepted_match_type));
  }, [item]);

  const goTo = (target: number) => {
    const entry = items[Math.max(0, Math.min(items.length - 1, target))];
    if (entry) setCurrentId(entry.mapping_id);
  };
  const next = () => goTo(index + 1);
  const prev = () => goTo(index - 1);

  useEffect(() => {
    if (!open) return undefined;
    const handler = (e: KeyboardEvent) => {
      const k = e.key.toLowerCase();
      // Don't hijack typing inside the match-type select / inputs.
      const target = e.target;
      if (target instanceof HTMLElement) {
        if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.closest('[role="listbox"]')) return;
        // A popover on top (the shortcuts legend, a select menu) owns the keyboard, Esc included:
        // a keystroke there must not decide the term hidden behind it.
        if (target.closest('.MuiPopover-root')) return;
        if (k === 'enter' && target.closest(INTERACTIVE)) return;
      }
      if (k === 'escape') { onClose(); return; }
      if (!item) return;
      if (k === 'arrowdown' || k === 'j') { e.preventDefault(); next(); }
      else if (k === 'arrowup' || k === 'k') { e.preventDefault(); prev(); }
      else if (k === 's') { e.preventDefault(); next(); }
      else if (k === 'a' || k === 'enter') {
        if (canAccept(item)) { emit({ type: 'accept_mapping', mapping_id: item.mapping_id, selected_match_type: matchType }); next(); }
      } else if (k === 'r') {
        if (!isNoMatch(item)) { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); next(); }
      } else if (k === 'n') {
        if (isNoMatch(item)) { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); next(); }
      } else if (k === 'u') {
        emit({ type: 'reset_mapping', mapping_id: item.mapping_id });
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, item, matchType, items.length, index]);

  const accept = () => { if (item && canAccept(item)) { emit({ type: 'accept_mapping', mapping_id: item.mapping_id, selected_match_type: matchType }); next(); } };
  const reject = () => { if (item) { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); next(); } };
  const position = Math.min(index + 1, items.length);

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth aria-labelledby={titleId}>
      <DialogTitle sx={{ py: 1.5 }}>
        <Stack direction="row" alignItems="center" spacing={0.5}>
          <Typography id={titleId} variant="subtitle1" component="span">Review one by one</Typography>
          <InfoButton title="Keyboard shortcuts">
            <Box component="dl" sx={{ m: 0, display: 'grid', gridTemplateColumns: 'max-content 1fr', columnGap: 1.5, rowGap: 0.5 }}>
              {KEY_LEGEND.map(([key, label]) => (
                <React.Fragment key={key}>
                  <Box component="dt" sx={{ fontFamily: fonts.mono, color: 'text.primary' }}>{key}</Box>
                  <Box component="dd" sx={{ m: 0 }}>{label}</Box>
                </React.Fragment>
              ))}
            </Box>
          </InfoButton>
          <Box sx={{ flex: 1 }} />
          <Tooltip title="Previous term">
            <span><IconButton size="small" aria-label="Previous term" onClick={prev} disabled={!item || index <= 0}><ChevronLeftIcon /></IconButton></span>
          </Tooltip>
          <Typography variant="body2" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums', minWidth: 56, textAlign: 'center' }}>
            {formatCount(position)} of {formatCount(items.length)}
          </Typography>
          <Tooltip title="Next term">
            <span><IconButton size="small" aria-label="Next term" onClick={next} disabled={!item || index >= items.length - 1}><ChevronRightIcon /></IconButton></span>
          </Tooltip>
          <IconButton aria-label="Close review" onClick={onClose} sx={{ ml: 0.5 }}>
            <CloseIcon />
          </IconButton>
        </Stack>
      </DialogTitle>
      <DialogContent>
        {!item ? (
          <EmptyState dense title="No terms to review in this view" description="Change the filters or the review scope to bring terms back." />
        ) : (
          <Stack spacing={2}>
            <LinearProgress variant="determinate" aria-label="Position in the list" value={items.length ? (position / items.length) * 100 : 0} />
            <Stack direction="row" alignItems="center" spacing={1}>
              <Chip size="small" variant="outlined" color={statusColor(item.status)} label={statusLabel(item.status)} />
              <Box sx={{ ml: 'auto' }}>
                {(() => {
                  const noCandidate = isNoMatch(item) || !String(item.suggested_uri || '').trim();
                  return typeof item.acceptance_score === 'number'
                    ? <ConfidenceCell value={item.acceptance_score} trace={item.trace_metadata} rawConfidence={item.confidence} acceptanceMode autoAccepted={item.auto_accepted} noCandidate={noCandidate} />
                    : <ConfidenceCell value={item.confidence} trace={item.trace_metadata} noCandidate={noCandidate} />;
                })()}
              </Box>
            </Stack>
            {/* Side-by-side input vs suggestion so the reviewer compares both term names AND
                both definitions before deciding — mirrors the details Drawer. */}
            <ComparisonRow
              leftLabel="Input term"
              leftValue={item.term}
              rightLabel="Suggested term"
              rightValue={isNoMatch(item) ? 'No suggestion' : item.suggested_label}
            />
            <ComparisonRow
              leftLabel="Input definition"
              leftValue={item.definition}
              rightLabel="Suggested description"
              rightValue={isNoMatch(item) ? '' : item.suggested_description}
            />
            {isNoMatch(item) ? (
              <Typography variant="body2" color="text.secondary">No acceptable suggestion (no-match decision).</Typography>
            ) : (
              <MetaList items={[{ label: 'Suggested URI', value: item.suggested_uri, mono: true }, { label: 'Provider', value: item.provider }]} />
            )}
            {canAccept(item) && (
              <FormControl size="small" sx={{ maxWidth: 260 }}>
                <InputLabel>SKOS match type</InputLabel>
                <Select label="SKOS match type" value={matchType} onChange={(e) => setMatchType(normalizeEditableSkosMatchType(String(e.target.value)))} sx={{ fontFamily: fonts.mono, fontSize: '0.8125rem' }}>
                  {editableSkosMatchTypes.map((o) => <MenuItem key={o} value={o} sx={{ fontFamily: fonts.mono, fontSize: '0.8125rem' }}>{o}</MenuItem>)}
                </Select>
              </FormControl>
            )}
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              {canAccept(item) ? (
                <Button variant="contained" color="success" startIcon={<CheckCircleIcon />} onClick={accept}>Accept (A)</Button>
              ) : isNoMatch(item) ? (
                <Button variant="contained" color="warning" onClick={reject}>Confirm no match (N)</Button>
              ) : (
                <Button variant="contained" color="success" disabled>Accept</Button>
              )}
              {!isNoMatch(item) && <Button variant="outlined" color="warning" startIcon={<CancelIcon />} onClick={reject}>Reject (R)</Button>}
              <Button variant="text" onClick={next} disabled={index >= items.length - 1} sx={{ ml: 'auto' }}>Skip (S)</Button>
            </Stack>
          </Stack>
        )}
      </DialogContent>
    </Dialog>
  );
}
