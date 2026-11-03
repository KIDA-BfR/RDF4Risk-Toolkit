import React, { useId, useState } from 'react';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Popover from '@mui/material/Popover';
import Typography from '@mui/material/Typography';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';

export type InfoContent = { title: string; content: React.ReactNode };

// The one "explain on demand" affordance (DESIGN.md §4): an ⓘ button next to a heading or field
// that opens a click popover. Click (not hover) so it works on touch and keyboard; Esc and an
// outside click close it. Body copy stays short (≤ ~60 words, optionally a short list).
export function InfoButton({ title, children, label }: { title: string; children: React.ReactNode; label?: string }) {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const id = useId();
  const open = Boolean(anchor);
  return (
    <>
      <IconButton
        size="small"
        aria-label={label ?? `About ${title}`}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls={open ? id : undefined}
        onClick={(event) => setAnchor(open ? null : event.currentTarget)}
        sx={{ p: 0.5, color: open ? 'primary.main' : 'text.secondary', flexShrink: 0, verticalAlign: 'middle' }}
      >
        <InfoOutlinedIcon sx={{ fontSize: 18 }} />
      </IconButton>
      <Popover
        id={id}
        open={open}
        anchorEl={anchor}
        onClose={() => setAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
        slotProps={{ paper: { role: 'dialog', 'aria-labelledby': `${id}-title`, sx: { p: 2, maxWidth: 380, mt: 0.5 } } as any }}
      >
        <Typography id={`${id}-title`} variant="subtitle2" component="p">{title}</Typography>
        <Box
          sx={{
            typography: 'body2',
            color: 'text.secondary',
            mt: 0.75,
            '& p': { m: 0 },
            '& p + p': { mt: 1 },
            '& ul, & ol': { pl: 2.5, my: 0.75 },
            '& li + li': { mt: 0.5 },
            '& strong': { color: 'text.primary', fontWeight: 600 },
          }}
        >
          {children}
        </Box>
      </Popover>
    </>
  );
}

// Heading text with an optional trailing ⓘ, aligned on one baseline.
export function TitleWithInfo({ children, info }: { children: React.ReactNode; info?: InfoContent }) {
  if (!info) return <>{children}</>;
  return (
    <Box component="span" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, minWidth: 0 }}>
      <span>{children}</span>
      <InfoButton title={info.title}>{info.content}</InfoButton>
    </Box>
  );
}
