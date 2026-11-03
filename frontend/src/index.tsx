import React from 'react';
import { createRoot } from 'react-dom/client';
import { Experimental_CssVarsProvider as CssVarsProvider, getInitColorSchemeScript } from '@mui/material/styles';
import { CssBaseline } from '@mui/material';
import { StandaloneApp } from './standalone/StandaloneApp';
import { theme } from './theme';

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
