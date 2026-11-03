import React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { fonts } from '../../theme/tokens';

export type MetaItem = {
  label: React.ReactNode;
  value: React.ReactNode;
  // Identifiers (IRIs, CURIEs, file names) render monospace and may wrap anywhere.
  mono?: boolean;
  // Right-align-free numbers still get tabular figures.
  numeric?: boolean;
};

const isEmpty = (value: React.ReactNode) => value === null || value === undefined || value === '';

// Labelled key/value data (DESIGN.md §9) as a definition grid — replaces "A · B · C" strings and
// stat cards. Missing values show a small grey dash; a real 0 prints as 0.
export function MetaList({ items, columns = 1 }: { items: MetaItem[]; columns?: 1 | 2 }) {
  return (
    <Box
      component="dl"
      sx={{
        m: 0,
        display: 'grid',
        gridTemplateColumns: {
          xs: 'max-content minmax(0, 1fr)',
          md: columns === 2 ? 'max-content minmax(0, 1fr) max-content minmax(0, 1fr)' : 'max-content minmax(0, 1fr)',
        },
        columnGap: 2,
        rowGap: 0.75,
        alignItems: 'baseline',
      }}
    >
      {items.map((item, index) => (
        <React.Fragment key={index}>
          <Typography component="dt" variant="body2" color="text.secondary">
            {item.label}
          </Typography>
          <Typography
            component="dd"
            variant="body2"
            sx={{
              m: 0,
              minWidth: 0,
              overflowWrap: 'anywhere',
              ...(item.mono ? { fontFamily: fonts.mono, fontSize: '0.8125rem' } : {}),
              ...(item.numeric ? { fontVariantNumeric: 'tabular-nums' } : {}),
            }}
          >
            {isEmpty(item.value) ? <Box component="span" sx={{ color: 'text.secondary' }}>—</Box> : item.value}
          </Typography>
        </React.Fragment>
      ))}
    </Box>
  );
}
