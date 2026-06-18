import React from 'react';
import { Box, Paper, Typography } from '@mui/material';

// Side-by-side "input vs suggestion" cell pair. Shared by the details Drawer and the
// keyboard Review mode dialog so both compare the same fields with identical styling.
export function ComparisonRow({
  leftLabel,
  leftValue,
  rightLabel,
  rightValue,
}: {
  leftLabel: string;
  leftValue: React.ReactNode;
  rightLabel: string;
  rightValue: React.ReactNode;
}) {
  const renderValue = (value: React.ReactNode) => {
    if (value === null || value === undefined || value === '') {
      return '—';
    }
    if (React.isValidElement(value)) {
      return value;
    }
    return String(value);
  };

  return (
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.2 }}>
      <Paper variant="outlined" sx={{ p: 1.2, borderRadius: 2 }}>
        <Typography variant="caption" color="text.secondary">{leftLabel}</Typography>
        <Typography variant="body2" sx={{ fontWeight: 700, mt: 0.4, wordBreak: 'break-word' }}>{renderValue(leftValue)}</Typography>
      </Paper>
      <Paper variant="outlined" sx={{ p: 1.2, borderRadius: 2 }}>
        <Typography variant="caption" color="text.secondary">{rightLabel}</Typography>
        <Typography variant="body2" sx={{ fontWeight: 700, mt: 0.4, wordBreak: 'break-word' }}>{renderValue(rightValue)}</Typography>
      </Paper>
    </Box>
  );
}
