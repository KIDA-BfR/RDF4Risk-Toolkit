import React, { useEffect, useState } from 'react';
import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
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
import KeyboardIcon from '@mui/icons-material/Keyboard';
import type { AppEvent, ReviewItem } from './types';
import { editableSkosMatchTypes, normalizeEditableSkosMatchType, skosChipSx, statusLabel } from './utils';
import { ConfidenceCell } from '../../components/review/ConfidenceCell';
import { ComparisonRow } from './ComparisonRow';

const KEY_LEGEND: Array<[string, string]> = [
  ['A / Enter', 'Accept'],
  ['R', 'Reject'],
  ['N', 'No match'],
  ['J / ↓', 'Next'],
  ['K / ↑', 'Previous'],
  ['U', 'Reset row'],
  ['Esc', 'Close'],
];

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
  const [index, setIndex] = useState(0);
  const [matchType, setMatchType] = useState('');
  const item = items[index];

  useEffect(() => {
    if (open) setIndex(0);
  }, [open]);

  useEffect(() => {
    if (item) setMatchType(normalizeEditableSkosMatchType(item.match_type || item.accepted_match_type));
  }, [item]);

  const next = () => setIndex((i) => Math.min(items.length - 1, i + 1));
  const prev = () => setIndex((i) => Math.max(0, i - 1));

  useEffect(() => {
    if (!open) return undefined;
    const handler = (e: KeyboardEvent) => {
      // Don't hijack typing inside the match-type select / inputs.
      const target = e.target;
      if (target instanceof HTMLElement && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.getAttribute('role') === 'listbox')) return;
      const k = e.key.toLowerCase();
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
  }, [open, item, matchType, items.length]);

  const accept = () => { if (item && canAccept(item)) { emit({ type: 'accept_mapping', mapping_id: item.mapping_id, selected_match_type: matchType }); next(); } };
  const reject = () => { if (item) { emit({ type: 'reject_mapping', mapping_id: item.mapping_id }); next(); } };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ pr: 6 }}>
        <Stack direction="row" alignItems="center" spacing={1}>
          <KeyboardIcon fontSize="small" />
          <span>Review mode</span>
          <Chip size="small" label={`${Math.min(index + 1, items.length)} / ${items.length}`} sx={{ ml: 'auto' }} />
        </Stack>
        <IconButton aria-label="Close review mode" onClick={onClose} sx={{ position: 'absolute', right: 8, top: 8 }}>
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent>
        {!item ? (
          <Typography variant="body2" color="text.secondary" sx={{ py: 3, textAlign: 'center' }}>
            No rows to review in the current view.
          </Typography>
        ) : (
          <Stack spacing={1.5}>
            <LinearProgress variant="determinate" value={items.length ? ((index + 1) / items.length) * 100 : 0} sx={{ height: 6, borderRadius: 999 }} />
            <Stack direction="row" alignItems="center" spacing={1}>
              <Chip size="small" label={statusLabel(item.status)} />
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
              rightValue={isNoMatch(item) ? 'No suggestion' : (item.suggested_label || '—')}
            />
            <ComparisonRow
              leftLabel="Input definition"
              leftValue={item.definition}
              rightLabel="Suggested description"
              rightValue={isNoMatch(item) ? '—' : item.suggested_description}
            />
            <Box>
              {isNoMatch(item) ? (
                <Typography variant="body2" color="text.secondary">No acceptable suggestion (no-match decision).</Typography>
              ) : (
                <>
                  <Tooltip title={String(item.suggested_uri ?? '')}>
                    <Typography variant="body2" color="text.secondary" sx={{ wordBreak: 'break-all' }}>{item.suggested_uri}</Typography>
                  </Tooltip>
                  {item.provider && <Typography variant="caption" color="text.secondary">via {item.provider}</Typography>}
                </>
              )}
            </Box>
            {canAccept(item) && (
              <FormControl size="small" sx={{ maxWidth: 260 }}>
                <InputLabel>SKOS match type</InputLabel>
                <Select label="SKOS match type" value={matchType} onChange={(e) => setMatchType(normalizeEditableSkosMatchType(String(e.target.value)))} sx={skosChipSx(matchType)}>
                  {editableSkosMatchTypes.map((o) => <MenuItem key={o} value={o}>{o}</MenuItem>)}
                </Select>
              </FormControl>
            )}
            <Stack direction="row" spacing={1}>
              {canAccept(item) ? (
                <Button variant="contained" color="success" startIcon={<CheckCircleIcon />} onClick={accept}>Accept (A)</Button>
              ) : isNoMatch(item) ? (
                <Button variant="contained" color="warning" onClick={reject}>Acknowledge no match (N)</Button>
              ) : (
                <Button variant="contained" color="success" disabled>Accept</Button>
              )}
              {!isNoMatch(item) && <Button variant="outlined" color="warning" startIcon={<CancelIcon />} onClick={reject}>Reject (R)</Button>}
              <Button variant="text" onClick={next} sx={{ ml: 'auto' }}>Skip (S) →</Button>
            </Stack>
            <Divider />
            <Stack direction="row" spacing={0.75} flexWrap="wrap" useFlexGap>
              {KEY_LEGEND.map(([key, label]) => (
                <Chip key={key} size="small" variant="outlined" label={`${key} · ${label}`} sx={{ fontWeight: 600 }} />
              ))}
            </Stack>
          </Stack>
        )}
      </DialogContent>
    </Dialog>
  );
}
