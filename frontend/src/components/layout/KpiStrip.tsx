import React from 'react';
import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';

export type Kpi = {
  label: React.ReactNode;
  value: React.ReactNode;
  hint?: React.ReactNode;
  tone?: 'default' | 'success' | 'warning' | 'error' | 'info';
};

const isEmpty = (value: React.ReactNode) => value === null || value === undefined || value === '';

// A row of 2–5 headline numbers (DESIGN.md §9): one outlined container split by hairlines, label
// above value. Numbers are type, not tiles — no icons, no tinted boxes. Wraps to two columns on
// phones; the 1px grid gap over a divider-coloured ground draws the hairlines at any wrap.
export function KpiStrip({ items, ariaLabel }: { items: Kpi[]; ariaLabel?: string }) {
  const columns = Math.min(Math.max(items.length, 1), 5);
  return (
    <Paper variant="outlined" sx={{ overflow: 'hidden', bgcolor: 'divider' }}>
      <Box
        component="dl"
        aria-label={ariaLabel}
        sx={{
          m: 0,
          display: 'grid',
          gap: '1px',
          gridTemplateColumns: { xs: `repeat(${Math.min(columns, 2)}, minmax(0, 1fr))`, sm: `repeat(${columns}, minmax(0, 1fr))` },
          // An odd last item spans the row on phones so no empty hairline cell is left behind.
          '& > :last-child:nth-of-type(odd)': { gridColumn: { xs: '1 / -1', sm: 'auto' } },
        }}
      >
        {items.map((item, index) => (
          <Box key={index} sx={{ bgcolor: 'background.paper', px: 2, py: 1.5, minWidth: 0 }}>
            <Typography component="dt" variant="caption" color="text.secondary" sx={{ display: 'block' }}>
              {item.label}
            </Typography>
            <Typography
              component="dd"
              sx={{
                m: 0,
                mt: 0.25,
                fontSize: '1.375rem',
                fontWeight: 600,
                lineHeight: 1.25,
                fontVariantNumeric: 'tabular-nums',
                color: !item.tone || item.tone === 'default' ? 'text.primary' : `${item.tone}.main`,
                overflowWrap: 'anywhere',
              }}
            >
              {isEmpty(item.value) ? <Box component="span" sx={{ color: 'text.secondary', fontSize: '0.875rem' }}>—</Box> : item.value}
            </Typography>
            {item.hint && (
              <Typography variant="caption" color="text.secondary" component="p" sx={{ mt: 0.25 }}>
                {item.hint}
              </Typography>
            )}
          </Box>
        ))}
      </Box>
    </Paper>
  );
}
