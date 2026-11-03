import React, { useId, useState } from 'react';
import { Box, Button, Drawer, IconButton, Stack, Tooltip, Typography } from '@mui/material';
import {
  Timeline,
  TimelineConnector,
  TimelineContent,
  TimelineDot,
  TimelineItem,
  TimelineOppositeContent,
  TimelineSeparator,
} from '@mui/lab';
import CloseIcon from '@mui/icons-material/Close';
import UndoIcon from '@mui/icons-material/Undo';
import RestoreIcon from '@mui/icons-material/Restore';
import HistoryIcon from '@mui/icons-material/History';
import type { HistoryEntry } from '../../standalone/backendClient';
import { serviceMeta, type ServiceId } from '../../standalone/services';
import { InfoButton } from '../help/InfoButton';
import { EmptyState } from '../states/EmptyState';
import { ConfirmDialog } from '../dialogs/ConfirmActionButton';

const timeOnly = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const dayAndTime = new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
const fullStamp = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'medium' });

function relTime(ts: number): string {
  try {
    const diff = Date.now() / 1000 - ts;
    if (diff < 60) return 'just now';
    if (diff < 3600) return `${Math.round(diff / 60)} min ago`;
    const when = new Date(ts * 1000);
    return when.toDateString() === new Date().toDateString() ? timeOnly.format(when) : dayAndTime.format(when);
  } catch {
    return '';
  }
}

function absTime(ts: number): { label: string; iso?: string } {
  try {
    const when = new Date(ts * 1000);
    return { label: fullStamp.format(when), iso: when.toISOString() };
  } catch {
    return { label: '' };
  }
}

// The canonical tool names (standalone/services.ts), so History says what the drawer says.
// serviceMeta falls back to the Overview for unknown ids, so check the id before trusting it.
function toolName(id: string): string {
  const meta = serviceMeta(id as ServiceId);
  return meta.id === id ? meta.title : id;
}

const oppositeSx = { flex: 0.35, px: 1 } as const;

// Snapshots leave out the RDF inspector's parsed catalog (project_store._RUNTIME_DENY), and a
// restore clears it.

type PendingRestore = { entry: HistoryEntry; later: number; undo: boolean };

// Operation-history drawer: every durable action this session, newest first. Reverting to a step
// restores the whole pipeline to just before that action (server-side snapshot replay) and drops
// that action and everything after it from the history (project_store._History.revert_to) — the
// FAIR audit-trail + time-travel undo the review asked for. Revert confirms because of that reach;
// Undo is one click unless the active tool holds unapplied edits it would drop.
export function HistoryDrawer({
  open,
  history,
  onClose,
  onRevert,
  onUndo,
  unsavedEditsNote,
}: {
  open: boolean;
  history: HistoryEntry[];
  onClose: () => void;
  // `id` is the entry's stable id; the index is kept for backends that predate it.
  onRevert: (index: number, id?: string) => void;
  onUndo: () => void;
  /** Set while the active tool holds unapplied edits, e.g. "Unapplied edits in RDF generator will be lost." */
  unsavedEditsNote?: string;
}) {
  const ordered = [...history].reverse(); // newest first
  const latest = ordered[0];
  const titleId = useId();
  // Kept after closing so the dialog text doesn't blank out during its exit transition.
  const [pending, setPending] = useState<PendingRestore | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const askRestore = (restore: PendingRestore) => { setPending(restore); setConfirmOpen(true); };
  const askRevert = (entry: HistoryEntry, later: number) => askRestore({ entry, later, undo: false });
  const undo = () => {
    if (!unsavedEditsNote) onUndo();
    else if (latest) askRestore({ entry: latest, later: 0, undo: true });
  };
  const confirmMessage = (() => {
    if (!pending) return '';
    const { entry, later } = pending;
    const dropped = later === 0
      ? 'This action is removed from the history'
      : `This action and the ${later === 1 ? 'one' : later} after it are removed from the history`;
    const sentences = [
      `All tools return to the state they were in just before “${entry.label}”.`,
      `${dropped} and cannot be redone.`,
    ];
    if (unsavedEditsNote) sentences.push(unsavedEditsNote);
    return sentences.join(' ');
  })();

  return (
    <Drawer anchor="right" open={open} onClose={onClose} PaperProps={{ role: 'dialog', 'aria-labelledby': titleId }}>
      <Stack spacing={1.5} sx={{ width: { xs: '100vw', sm: 380 }, maxWidth: '100vw', p: 2 }}>
        <Stack direction="row" alignItems="center" spacing={0.5}>
          <Typography id={titleId} variant="h6" component="h2">Operation history</Typography>
          <InfoButton title="Operation history">
            <p>Every change you make in any tool is recorded here, newest first; the latest 25 are kept.</p>
            <p><strong>Undo</strong> and <strong>Revert</strong> return every tool to its state just before an action, including a catalog loaded in the RDF inspector. That action and all later ones leave the history and cannot be redone.</p>
            <p>Opening a project or starting a new one clears the history.</p>
          </InfoButton>
          <Box sx={{ flex: 1 }} />
          <IconButton aria-label="Close history" edge="end" onClick={onClose}><CloseIcon /></IconButton>
        </Stack>
        {/* An empty title while disabled: a disabled button fires no events for the tooltip. */}
        <Tooltip describeChild title={latest ? `Undo “${latest.label}”. This cannot be redone.` : ''}>
          <Button
            fullWidth
            variant="outlined"
            startIcon={<UndoIcon />}
            onClick={undo}
            disabled={!history.length}
          >
            Undo last action
          </Button>
        </Tooltip>

        {!history.length ? (
          <EmptyState
            dense
            icon={<HistoryIcon />}
            title="No actions recorded yet"
            description="Changes you make in any tool (upload, omit, generate, accept…) appear here and can be undone."
          />
        ) : (
          <Timeline sx={{ m: 0, p: 0 }}>
            <TimelineItem>
              <TimelineOppositeContent sx={oppositeSx} color="text.secondary">
                <Typography variant="caption">Now</Typography>
              </TimelineOppositeContent>
              <TimelineSeparator>
                <TimelineDot color="primary" />
                <TimelineConnector />
              </TimelineSeparator>
              <TimelineContent>
                <Typography variant="body2" sx={{ fontWeight: 600 }}>Current state</Typography>
              </TimelineContent>
            </TimelineItem>
            {ordered.map((entry, idx) => (
              <TimelineItem key={entry.index}>
                <TimelineOppositeContent sx={oppositeSx} color="text.secondary">
                  <Tooltip describeChild title={absTime(entry.ts).label}>
                    <Typography variant="caption" component="time" dateTime={absTime(entry.ts).iso} sx={{ fontVariantNumeric: 'tabular-nums' }}>
                      {relTime(entry.ts)}
                    </Typography>
                  </Tooltip>
                </TimelineOppositeContent>
                <TimelineSeparator>
                  <TimelineDot color="grey" variant="outlined" />
                  {idx < ordered.length - 1 && <TimelineConnector />}
                </TimelineSeparator>
                <TimelineContent>
                  <Stack direction="row" alignItems="flex-start" justifyContent="space-between" spacing={1}>
                    <Box sx={{ minWidth: 0 }}>
                      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>{entry.label}</Typography>
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>{toolName(entry.service)}</Typography>
                    </Box>
                    <Tooltip title="Revert all tools to just before this action">
                      <IconButton size="small" aria-label={`Revert to before ${entry.label}`} onClick={() => askRevert(entry, idx)}>
                        <RestoreIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                </TimelineContent>
              </TimelineItem>
            ))}
          </Timeline>
        )}
      </Stack>
      <ConfirmDialog
        open={confirmOpen}
        title={pending ? `${pending.undo ? 'Undo' : 'Revert to before'} “${pending.entry.label}”?` : ''}
        message={confirmMessage}
        confirmLabel={pending?.undo ? 'Undo last action' : 'Revert all tools'}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => {
          if (!pending) return;
          if (pending.undo) onUndo();
          else onRevert(pending.entry.index, pending.entry.id);
        }}
      />
    </Drawer>
  );
}
