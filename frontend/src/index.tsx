import React from 'react';
import { createRoot } from 'react-dom/client';
import {
  experimental_extendTheme as extendTheme,
  Experimental_CssVarsProvider as CssVarsProvider,
  getInitColorSchemeScript,
} from '@mui/material/styles';
import { CssBaseline } from '@mui/material';
import { StandaloneApp } from './standalone/StandaloneApp';

// CSS-variable theme with light + dark colorSchemes. Palette lives per-scheme; typography,
// shape and component overrides are shared. Brand greens/ambers are darkened in light mode
// so white chip/button text clears WCAG AA 4.5:1, and lightened in dark mode so MUI auto-
// derives dark (AA) contrastText. Component overrides read theme tokens (theme.vars / palette
// callbacks) so both schemes stay consistent.
const theme = extendTheme({
  cssVarPrefix: 'rdf4risk',
  colorSchemes: {
    light: {
      palette: {
        primary: { main: '#2563eb' },
        success: { main: '#047857' }, // white text -> 5.48:1
        warning: { main: '#b45309' }, // white text -> 5.02:1
        info: { main: '#0369a1' }, // white text -> 5.93:1 (default #0288d1 was 3.86:1)
        error: { main: '#c62828' }, // white text -> 5.62:1
        background: { default: '#f8fafc', paper: '#ffffff' },
        text: { primary: '#0f172a', secondary: '#475569' },
        divider: '#e2e8f0',
      },
    },
    dark: {
      palette: {
        primary: { main: '#60a5fa' },
        success: { main: '#34d399' },
        warning: { main: '#fbbf24' },
        info: { main: '#38bdf8' },
        error: { main: '#dc2626' }, // white text -> 4.83:1
        background: { default: '#0b1220', paper: '#131c2e' },
        text: { primary: '#e8eef7', secondary: '#9fb0c6' },
        divider: 'rgba(148, 163, 184, 0.22)',
      },
    },
  },
  shape: { borderRadius: 12 },
  typography: {
    fontFamily: ['Inter', 'Roboto', 'Arial', 'sans-serif'].join(','),
    h5: { fontWeight: 800, letterSpacing: '-0.025em' },
    h6: { fontWeight: 800, letterSpacing: '-0.02em' },
    subtitle1: { fontWeight: 750 },
    subtitle2: { fontWeight: 750 },
    button: { fontWeight: 700, textTransform: 'none' },
  },
  components: {
    // Visible keyboard-focus ring on every ButtonBase descendant (ListItemButton,
    // Button, Checkbox, StepButton, ...) — WCAG 2.4.7.
    MuiButtonBase: {
      styleOverrides: {
        root: ({ theme: t }) => ({
          '&.Mui-focusVisible': {
            outline: `2px solid ${t.vars.palette.primary.main}`,
            outlineOffset: 2,
          },
        }),
      },
    },
    MuiButton: {
      defaultProps: { disableElevation: true },
      styleOverrides: {
        root: ({ theme: t }) => ({
          borderRadius: 10,
          textTransform: 'none',
          // Default disabled text (rgba(0,0,0,0.26)) measures 1.88:1; these labels are the
          // workflow's forward CTAs, so keep them legible (text.secondary is AA in both schemes).
          '&.Mui-disabled': { color: t.vars.palette.text.secondary },
        }),
        contained: ({ theme: t }) => ({
          '&.Mui-disabled': { backgroundColor: t.vars.palette.action.disabledBackground },
        }),
      },
    },
    MuiCard: { styleOverrides: { root: { borderRadius: 16 } } },
    MuiPaper: { styleOverrides: { root: { backgroundImage: 'none' } } },
    MuiTextField: { defaultProps: { size: 'small' } },
    MuiSelect: { defaultProps: { size: 'small' } },
    MuiChip: { styleOverrides: { root: { fontWeight: 700 } } },
  },
});

const root = document.getElementById('root');

if (root) {
  createRoot(root).render(
    <React.StrictMode>
      {getInitColorSchemeScript({ defaultMode: 'system' })}
      <CssVarsProvider theme={theme} defaultMode="system">
        <CssBaseline />
        <StandaloneApp />
      </CssVarsProvider>
    </React.StrictMode>,
  );
}
