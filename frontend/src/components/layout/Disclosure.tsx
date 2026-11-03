import React from 'react';
import Accordion from '@mui/material/Accordion';
import AccordionDetails from '@mui/material/AccordionDetails';
import AccordionSummary from '@mui/material/AccordionSummary';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import type { SxProps, Theme } from '@mui/material/styles';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

type DisclosureProps = {
  title: React.ReactNode;
  // The current state in a few words ("Off", "3 templates", "header row 1"), so collapsed content
  // never hides what is configured.
  summary?: React.ReactNode;
  optional?: boolean;
  // Inside a Section: no border of its own (a divider above instead), so containers never nest.
  plain?: boolean;
  defaultExpanded?: boolean;
  expanded?: boolean;
  onChange?: (expanded: boolean) => void;
  children: React.ReactNode;
  id?: string;
  sx?: SxProps<Theme>;
};

// The one expandable (DESIGN.md §4) for optional/advanced settings, longer explanations and
// technical details. Content stays mounted when collapsed so drafts inside are never lost.
export function Disclosure({ title, summary, optional, plain, defaultExpanded, expanded, onChange, children, id, sx }: DisclosureProps) {
  return (
    <Accordion
      defaultExpanded={defaultExpanded}
      expanded={expanded}
      onChange={onChange ? (_event, isExpanded) => onChange(isExpanded) : undefined}
      sx={[
        plain ? { border: 0, borderTop: 1, borderColor: 'divider', borderRadius: '0 !important', bgcolor: 'transparent', '& > .MuiAccordionSummary-root': { px: 0 }, '& .MuiAccordionDetails-root': { px: 0 } } : {},
        ...(Array.isArray(sx) ? sx : sx ? [sx] : []),
      ]}
    >
      <AccordionSummary expandIcon={<ExpandMoreIcon />} aria-controls={id ? `${id}-content` : undefined} id={id ? `${id}-header` : undefined}>
        <Box sx={{ minWidth: 0 }}>
          <Stack direction="row" alignItems="center" spacing={1}>
            <Typography variant="subtitle2" component="span">{title}</Typography>
            {optional && <Chip size="small" variant="outlined" label="Optional" sx={{ height: 20, fontSize: '0.6875rem', color: 'text.secondary' }} />}
          </Stack>
          {summary && (
            <Typography variant="caption" color="text.secondary" component="p" sx={{ mt: 0.25 }}>
              {summary}
            </Typography>
          )}
        </Box>
      </AccordionSummary>
      <AccordionDetails id={id ? `${id}-content` : undefined} sx={{ pt: 1, pb: 2, px: 2 }}>
        {children}
      </AccordionDetails>
    </Accordion>
  );
}
