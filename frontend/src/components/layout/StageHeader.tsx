import React from 'react';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import { InfoButton, type InfoContent } from '../help/InfoButton';

type StageHeaderProps = {
  title: React.ReactNode;
  // One short instruction line (≤ ~90 characters). Longer explanations belong in `info`.
  lead?: React.ReactNode;
  info?: InfoContent;
  // Tool-level controls shown on every stage (e.g. Options, ToolMenu), always top right.
  actions?: React.ReactNode;
  headingRef?: React.Ref<HTMLHeadingElement>;
};

export function StageHeader({ title, lead, info, actions, headingRef }: StageHeaderProps) {
  return (
    <Box sx={{ mb: 3 }}>
      <Stack direction="row" alignItems="flex-start" spacing={2}>
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Stack direction="row" alignItems="center" spacing={0.5}>
            <Typography
              ref={headingRef}
              tabIndex={-1}
              variant="h5"
              component="h2"
              // Focus lands here programmatically on every stage change (for screen readers). The
              // heading is not in the tab order, so a visible ring would only be noise.
              sx={{ '&:focus, &:focus-visible': { outline: 'none' } }}
            >
              {title}
            </Typography>
            {info && <InfoButton title={info.title}>{info.content}</InfoButton>}
          </Stack>
          {lead && (
            <Typography color="text.secondary" sx={{ mt: 0.75, maxWidth: '72ch' }}>
              {lead}
            </Typography>
          )}
        </Box>
        {actions && (
          <Stack direction="row" alignItems="center" spacing={1} sx={{ flexShrink: 0 }}>
            {actions}
          </Stack>
        )}
      </Stack>
    </Box>
  );
}
