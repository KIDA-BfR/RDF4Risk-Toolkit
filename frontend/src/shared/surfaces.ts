import type { Theme } from '@mui/material/styles';

// Branded hero surface: a soft light-blue gradient in light mode, a flat paper surface in
// dark mode (the bright gradient would glare on a dark page). Used by every view's AppShell.
export const heroSurface = (theme: Theme): string =>
  theme.palette.mode === 'dark'
    ? (theme as any).vars.palette.background.paper
    : 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 50%,#eff6ff 100%)';
