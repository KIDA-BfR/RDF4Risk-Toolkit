import React from 'react';
import { Box, Paper, Typography } from '@mui/material';
import { fonts } from '../../theme/tokens';

// Side-by-side "input vs suggestion" cell pair. Shared by the details Drawer and the
// keyboard review dialog so both compare the same fields with identical styling.
// `mono` renders identifiers (URIs, CURIEs) in the monospace face.
export function ComparisonRow({
  leftLabel,
  leftValue,
  rightLabel,
  rightValue,
  mono,
}: {
  leftLabel: string;
  leftValue: React.ReactNode;
  rightLabel: string;
  rightValue: React.ReactNode;
  mono?: boolean;
}) {
  const renderValue = (value: React.ReactNode) => {
    if (value === null || value === undefined || value === '') {
      return <Box component="span" sx={{ color: 'text.secondary', fontWeight: 400 }}>—</Box>;
    }
    if (React.isValidElement(value)) {
      return value;
    }
    return String(value);
  };

  const cell = (label: string, value: React.ReactNode) => (
    <Paper variant="outlined" sx={{ p: 1.5, minWidth: 0 }}>
      <Typography variant="caption" color="text.secondary" component="p">{label}</Typography>
      <Typography
        variant="body2"
        component="div"
        sx={{ mt: 0.5, overflowWrap: 'anywhere', ...(mono ? { fontFamily: fonts.mono, fontSize: '0.8125rem' } : { fontWeight: 500 }) }}
      >
        {renderValue(value)}
      </Typography>
    </Paper>
  );

  return (
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1 }}>
      {cell(leftLabel, leftValue)}
      {cell(rightLabel, rightValue)}
    </Box>
  );
}
