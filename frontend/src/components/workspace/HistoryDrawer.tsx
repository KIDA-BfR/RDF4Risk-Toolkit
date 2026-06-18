import React from 'react';
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
import type { HistoryEntry } from '../../standalone/backendClient';

function relTime(ts: number): string {
  try {
    const diff = Date.now() / 1000 - ts;
    if (diff < 60) return 'just now';
    if (diff < 3600) return `${Math.round(diff / 60)} min ago`;
    return new Date(ts * 1000).toLocaleTimeString();
  } catch {
    return '';
  }
}

const SERVICE_SHORT: Record<string, string> = {
  matching_table_generator: 'Matching',
  semi_automatic_reconciliation: 'Reconcile',
  agent_reconciliation: 'Agent',
  rdf_generator: 'RDF Gen',
  rdf_to_table: 'RDF→Table',
};

// Operation-history drawer: every durable action this session, newest first. Reverting to a step
// restores the whole pipeline to just before that action (server-side snapshot replay) — the FAIR
// audit-trail + time-travel undo the review asked for.
export function HistoryDrawer({
  open,
  history,
  onClose,
  onRevert,
  onUndo,
}: {
  open: boolean;
  history: HistoryEntry[];
  onClose: () => void;
  onRevert: (index: number) => void;
  onUndo: () => void;
}) {
  const ordered = [...history].reverse(); // newest first

  return (
    <Drawer anchor="right" open={open} onClose={onClose}>
      <Box sx={{ width: 380, p: 2 }}>
        <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 1 }}>
          <Typography variant="h6">Operation history</Typography>
          <IconButton aria-label="Close history" onClick={onClose}><CloseIcon /></IconButton>
        </Stack>
        <Button
          fullWidth
          variant="outlined"
          startIcon={<UndoIcon />}
          onClick={onUndo}
          disabled={!history.length}
          sx={{ mb: 1.5 }}
        >
          Undo last action
        </Button>

        {!history.length ? (
          <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
            No actions recorded yet. Edits you make (upload, omit, generate, accept…) appear here and can be reverted.
          </Typography>
        ) : (
          <Timeline sx={{ m: 0, p: 0 }}>
            {ordered.map((entry, idx) => (
              <TimelineItem key={entry.index}>
                <TimelineOppositeContent sx={{ flex: 0.35, px: 1 }} color="text.secondary">
                  <Typography variant="caption">{SERVICE_SHORT[entry.service] || entry.service}</Typography>
                  <Typography variant="caption" sx={{ display: 'block' }}>{relTime(entry.ts)}</Typography>
                </TimelineOppositeContent>
                <TimelineSeparator>
                  <TimelineDot color={idx === 0 ? 'primary' : 'grey'} variant={idx === 0 ? 'filled' : 'outlined'} />
                  {idx < ordered.length - 1 && <TimelineConnector />}
                </TimelineSeparator>
                <TimelineContent>
                  <Stack direction="row" alignItems="center" justifyContent="space-between" spacing={1}>
                    <Typography variant="body2" sx={{ fontWeight: idx === 0 ? 700 : 500 }}>{entry.label}</Typography>
                    <Tooltip title="Revert the workspace to just before this action">
                      <IconButton size="small" aria-label={`Revert to before ${entry.label}`} onClick={() => onRevert(entry.index)}>
                        <RestoreIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                </TimelineContent>
              </TimelineItem>
            ))}
          </Timeline>
        )}
      </Box>
    </Drawer>
  );
}
