import React from 'react';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { BAND_META, confidenceBand, toConfidenceNumber } from './confidence';

// Confidence as a first-class visual: a banded chip that names the band and prints the score
// (colour is never the only signal). The tooltip explains what the band means.
//
// In `acceptanceMode` (auto-accept policy is on) `value` is the acceptance score — already
// banded server-side so auto-accepted rows sit on top and held rows sit below them — and the
// tooltip stays short: a status word, one plain sentence, and the underlying match confidence.
// Without `acceptanceMode` it behaves exactly as before, showing raw match confidence.
export function ConfidenceCell({
  value,
  trace,
  rawConfidence,
  acceptanceMode,
  autoAccepted,
  noCandidate,
}: {
  value: unknown;
  trace?: Record<string, unknown>;
  rawConfidence?: unknown;
  acceptanceMode?: boolean;
  autoAccepted?: boolean;
  noCandidate?: boolean;
}) {
  const n = toConfidenceNumber(value);
  const band = confidenceBand(value);
  const meta = BAND_META[band];
  // Rows with no suitable candidate must not show a red "Low — verify" bar: there is
  // nothing to verify. Show a neutral em dash regardless of any residual score.
  if (noCandidate || n == null) {
    return (
      <Typography variant="body2" color="text.secondary">
        —
      </Typography>
    );
  }
  const before = toConfidenceNumber(trace?.confidence_before_boost);
  const after = toConfidenceNumber(trace?.confidence_after_boost);
  const reason = trace?.provider_signal_boost_reason ? String(trace.provider_signal_boost_reason) : '';
  const raw = toConfidenceNumber(rawConfidence);

  return (
    <Tooltip
      title={
        <Box sx={{ maxWidth: 260 }}>
          {acceptanceMode ? (
            <>
              <Typography variant="caption" sx={{ fontWeight: 600, display: 'block' }}>
                {autoAccepted ? 'Auto-accepted' : 'Needs review'}
              </Typography>
              <Typography variant="caption" color="inherit" sx={{ display: 'block', mt: 0.5 }}>
                {autoAccepted ? 'Met the auto-accept policy.' : 'Not auto-accepted — check before accepting.'}
              </Typography>
              {raw != null && (
                <Typography variant="caption" sx={{ display: 'block', mt: 0.5, opacity: 0.8 }}>
                  Match confidence {raw.toFixed(2)}
                </Typography>
              )}
            </>
          ) : (
            <>
              <Typography variant="caption" sx={{ fontWeight: 600, display: 'block' }}>
                Confidence {n.toFixed(2)} — {meta.label}
              </Typography>
              <Typography variant="caption" color="inherit" sx={{ display: 'block', mt: 0.5 }}>
                {band === 'high' && 'Strong evidence; safe to accept after a glance.'}
                {band === 'medium' && 'Plausible; verify the candidate before accepting.'}
                {band === 'low' && 'Weak evidence; review carefully or reject.'}
              </Typography>
              {(before != null || after != null) && (
                <Typography variant="caption" sx={{ display: 'block', mt: 0.5 }}>
                  {before != null ? `Before boost ${before.toFixed(2)}` : ''}
                  {before != null && after != null ? ' → ' : ''}
                  {after != null ? `after ${after.toFixed(2)}` : ''}
                </Typography>
              )}
              {reason && (
                <Typography variant="caption" sx={{ display: 'block', mt: 0.5 }}>
                  {reason}
                </Typography>
              )}
            </>
          )}
        </Box>
      }
    >
      <Chip
        size="small"
        variant="outlined"
        color={meta.color}
        label={<>{meta.label} <Box component="span" sx={{ fontVariantNumeric: 'tabular-nums' }}>{n.toFixed(2)}</Box></>}
      />
    </Tooltip>
  );
}
