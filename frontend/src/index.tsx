import React from 'react';
import { createRoot } from 'react-dom/client';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';
import { StandaloneApp } from './standalone/StandaloneApp';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#2563eb' },
    success: { main: '#059669' },
    warning: { main: '#d97706' },
    background: { default: '#f8fafc', paper: '#ffffff' },
    text: { primary: '#0f172a', secondary: '#475569' },
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
    MuiButton: {
      defaultProps: { disableElevation: true },
      styleOverrides: { root: { borderRadius: 10, textTransform: 'none' } },
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
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <StandaloneApp />
      </ThemeProvider>
    </React.StrictMode>,
  );
}