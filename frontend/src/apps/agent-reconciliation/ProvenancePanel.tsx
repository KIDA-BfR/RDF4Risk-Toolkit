import React from 'react';
import { Box, Chip, Paper, Stack, Tooltip, Typography } from '@mui/material';
import PersonIcon from '@mui/icons-material/Person';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import HubIcon from '@mui/icons-material/Hub';
import RuleIcon from '@mui/icons-material/Rule';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';
import type { ReviewItem } from './types';
import { formatReviewMode } from './utils';
import { toConfidenceNumber } from '../../components/review/confidence';

function val(x: unknown): string {
  return x == null || x === '' ? '' : String(x);
}

// Prominent provenance strip for the review detail Drawer: who/what decided this mapping, via which
// provider/path, and how the confidence was derived — so every mapping is a verifiable, auditable
// scientific claim (the trust core of a FAIR tool). Read-only, from existing item + trace fields.
export function ReviewProvenancePanel({ item }: { item: ReviewItem }) {
  const trace = item.trace_metadata ?? {};
  const status = String(item.status || '').toLowerCase();
  const decidedByHuman = status === 'accepted' || status === 'rejected';
  const before = toConfidenceNumber(trace.confidence_before_boost);
  const after = toConfidenceNumber(trace.confidence_after_boost);
  const boostReason = val(trace.provider_signal_boost_reason);
  const escFrom = val(trace.provider_escalation_from);
  const escTo = val(trace.provider_escalation_to);
  const decisionSource = val(item.decision_source);
  const reviewMode = item.review_mode ? formatReviewMode(item.review_mode) : '';
  const autoAccept = val(item.auto_accept_reason);

  return (
    <Paper variant="outlined" sx={{ p: 1.5, borderRadius: 2, bgcolor: 'background.default' }}>
      <Typography variant="overline" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
        Provenance
      </Typography>
      <Stack direction="row" spacing={0.75} flexWrap="wrap" useFlexGap>
        <Chip
          size="small"
          icon={decidedByHuman ? <PersonIcon /> : <SmartToyIcon />}
          color={decidedByHuman ? 'primary' : 'default'}
          label={decidedByHuman ? `Decided by curator (${status})` : 'Proposed by agent'}
        />
        {item.provider && <Chip size="small" icon={<HubIcon />} label={`Provider: ${item.provider}`} />}
        {decisionSource && <Chip size="small" icon={<RuleIcon />} label={`Decision: ${decisionSource}`} />}
        {reviewMode && <Chip size="small" variant="outlined" label={`Review mode: ${reviewMode}`} />}
        {item.fallback_reason && <Chip size="small" color="warning" variant="outlined" label={`Fallback: ${item.fallback_reason}`} />}
      </Stack>

      {(escFrom || escTo) && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="caption" color="text.secondary">Provider escalation path</Typography>
          <Stack direction="row" spacing={0.75} alignItems="center" flexWrap="wrap" useFlexGap sx={{ mt: 0.25 }}>
            <Chip size="small" label={`${escFrom || 'BioPortal'} → no verified match`} />
            <Chip size="small" color="info" label={`${escTo || 'Wikidata'} second pass`} />
            <Chip
              size="small"
              color={trace.wikidata_second_pass_has_candidate ? 'success' : 'default'}
              label={trace.wikidata_second_pass_has_candidate ? 'candidate found' : 'no suitable candidate'}
            />
          </Stack>
        </Box>
      )}

      {(before != null || after != null) && (
        <Box sx={{ mt: 1 }}>
          <Stack direction="row" spacing={0.5} alignItems="center">
            <TrendingUpIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
            <Typography variant="caption" color="text.secondary">
              Confidence{before != null ? ` ${before.toFixed(2)}` : ''}{before != null && after != null ? ' → ' : ''}{after != null ? `${after.toFixed(2)}` : ''}
            </Typography>
          </Stack>
          {boostReason && (
            <Tooltip title={boostReason}>
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontStyle: 'italic', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {boostReason}
              </Typography>
            </Tooltip>
          )}
        </Box>
      )}

      {autoAccept && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
          Auto-accepted: {autoAccept}
        </Typography>
      )}
    </Paper>
  );
}
