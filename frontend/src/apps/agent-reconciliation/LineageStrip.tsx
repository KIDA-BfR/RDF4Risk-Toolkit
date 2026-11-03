import React from 'react';
import { Box, Paper, Stack, Tooltip, Typography } from '@mui/material';
import EastIcon from '@mui/icons-material/East';
import TableRowsIcon from '@mui/icons-material/TableRows';
import HubIcon from '@mui/icons-material/Hub';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import type { ReviewItem } from './types';
import { confidenceBand, BAND_META, toConfidenceNumber } from '../../components/review/confidence';

function Node({ icon, step, title, subtitle, color }: { icon: React.ReactNode; step: string; title: string; subtitle?: string; color?: string }) {
  return (
    <Paper variant="outlined" sx={{ p: 1, minWidth: 0, flex: 1, borderColor: color || 'divider' }}>
      <Stack direction="row" spacing={0.5} alignItems="center" sx={{ mb: 0.25, color: 'text.secondary' }}>
        <Box sx={{ display: 'flex', fontSize: '1rem' }}>{icon}</Box>
        <Typography variant="caption">{step}</Typography>
      </Stack>
      <Tooltip title={title}>
        <Typography variant="body2" sx={{ fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{title}</Typography>
      </Tooltip>
      {subtitle && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{subtitle}</Typography>
      )}
    </Paper>
  );
}

// Points right between nodes on wide drawers and down when the strip stacks on phones.
const Arrow = () => (
  <Box aria-hidden sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'text.disabled', px: 0.25, transform: { xs: 'rotate(90deg)', sm: 'none' } }}>
    <EastIcon fontSize="small" />
  </Box>
);

// Honest, data-driven cross-step lineage for one mapping: the path from the source term (matching
// table) → the reconciliation decision (provider/confidence/match type, human vs agent) → the RDF
// statement it produces. Built entirely from fields already on the review item — it makes the
// existing chain legible; it does not fabricate triple-level source-cell tracking (which the backend
// doesn't record).
export function LineageStrip({ item }: { item: ReviewItem }) {
  const noMatch = String(item.status || '').toLowerCase() === 'no_match' || String(item.match_type || '').toLowerCase() === 'no_match';
  const conf = toConfidenceNumber(item.confidence);
  const band = confidenceBand(item.confidence);
  const decidedByHuman = ['accepted', 'rejected'].includes(String(item.status || '').toLowerCase());
  const matchType = item.match_type || (noMatch ? 'no_match' : '—');
  const subject = item.subject_label || item.term;

  return (
    <Box>
      <Typography variant="subtitle2" component="h3" sx={{ mb: 0.75 }}>
        Lineage
      </Typography>
      <Stack direction={{ xs: 'column', sm: 'row' }} alignItems="stretch" spacing={0.25}>
        <Node
          icon={<TableRowsIcon fontSize="inherit" />}
          step="Source term"
          title={item.term || '—'}
          subtitle={item.subject_label ? `Subject: ${item.subject_label}` : 'From the matching table'}
        />
        <Arrow />
        <Node
          icon={<HubIcon fontSize="inherit" />}
          step="Reconciliation"
          title={noMatch ? 'No verified match' : (item.suggested_label || item.suggested_uri || '—')}
          subtitle={
            noMatch
              ? `No match, decided by ${decidedByHuman ? 'curator' : 'agent'}`
              : `Via ${item.provider || 'agent'}${conf != null ? `, ${BAND_META[band].label} ${conf.toFixed(2)}` : ''}`
          }
          color={noMatch ? undefined : (band === 'high' ? 'success.main' : band === 'low' ? 'error.main' : 'warning.main')}
        />
        <Arrow />
        <Node
          icon={<AccountTreeIcon fontSize="inherit" />}
          step="RDF statement"
          title={noMatch ? '(not emitted)' : `${matchType}`}
          subtitle={noMatch ? 'No triple generated' : `${subject} → ${item.suggested_uri ? item.suggested_uri.split(/[\/#]/).pop() : 'object'}`}
        />
      </Stack>
    </Box>
  );
}
