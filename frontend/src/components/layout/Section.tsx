import React, { useId } from 'react';
import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import type { SxProps, Theme } from '@mui/material/styles';
import { InfoButton, type InfoContent } from '../help/InfoButton';

type SectionProps = {
  title?: React.ReactNode;
  info?: InfoContent;
  // One short line under the title. Anything longer goes into `info`.
  description?: React.ReactNode;
  actions?: React.ReactNode;
  // A state tone for status sections (run finished / failed); tints the border only.
  tone?: 'default' | 'success' | 'warning' | 'error' | 'info';
  children?: React.ReactNode;
  id?: string;
  sx?: SxProps<Theme>;
};

// The one in-page container (DESIGN.md §7): a flat, outlined surface with an optional header
// (h3 title + ⓘ + actions on the right). Use spacing and dividers inside — never nest Sections.
export function Section({ title, info, description, actions, tone = 'default', children, id, sx }: SectionProps) {
  const autoId = useId();
  const headingId = `${id ?? autoId}-title`;
  const hasHeader = Boolean(title || actions);
  return (
    <Paper
      variant="outlined"
      component="section"
      id={id}
      aria-labelledby={title ? headingId : undefined}
      sx={[
        { p: { xs: 2, md: 2.5 }, minWidth: 0, borderColor: tone === 'default' ? 'divider' : `${tone}.main` },
        ...(Array.isArray(sx) ? sx : sx ? [sx] : []),
      ]}
    >
      {hasHeader && (
        <Stack direction="row" alignItems="flex-start" spacing={2} sx={{ mb: children ? 2 : 0 }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            {title && (
              <Stack direction="row" alignItems="center" spacing={0.5}>
                <Typography id={headingId} variant="subtitle1" component="h3">
                  {title}
                </Typography>
                {info && <InfoButton title={info.title}>{info.content}</InfoButton>}
              </Stack>
            )}
            {description && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5, maxWidth: '72ch' }}>
                {description}
              </Typography>
            )}
          </Box>
          {actions && (
            <Stack direction="row" alignItems="center" spacing={1} sx={{ flexShrink: 0, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              {actions}
            </Stack>
          )}
        </Stack>
      )}
      {children}
    </Paper>
  );
}
