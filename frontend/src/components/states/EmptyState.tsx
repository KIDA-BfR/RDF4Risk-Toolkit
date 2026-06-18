import React from 'react';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';

// Reusable empty state: an icon, a why-it's-empty title + description, and up to two actions
// (primary CTA + secondary). Replaces bare "No data" strings so empty panels tell the user what
// to do next. Render only when not loading to avoid the loading/empty flash.
export function EmptyState({
  icon,
  title,
  description,
  primaryAction,
  secondaryAction,
  dense,
}: {
  icon?: React.ReactNode;
  title: string;
  description?: string;
  primaryAction?: React.ReactNode;
  secondaryAction?: React.ReactNode;
  dense?: boolean;
}) {
  return (
    <Stack
      alignItems="center"
      justifyContent="center"
      spacing={1.25}
      sx={{
        textAlign: 'center',
        py: dense ? 3 : 5,
        px: 2,
        border: '1px dashed',
        borderColor: 'divider',
        borderRadius: 3,
        bgcolor: 'background.default',
      }}
    >
      {icon && <Box sx={{ color: 'text.secondary', display: 'flex', '& svg': { fontSize: 40 } }}>{icon}</Box>}
      <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
        {title}
      </Typography>
      {description && (
        <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 440 }}>
          {description}
        </Typography>
      )}
      {(primaryAction || secondaryAction) && (
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} sx={{ mt: 0.5 }}>
          {primaryAction}
          {secondaryAction}
        </Stack>
      )}
    </Stack>
  );
}
