import React from 'react';
import { createRoot } from 'react-dom/client';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';
import { withStreamlitConnection } from 'streamlit-component-lib';
import { WorkflowConfigPanel } from './apps/agent-reconciliation/AgentReconciliationApp';
import { MatchingTableGeneratorApp } from './apps/matching-table-generator/MatchingTableGeneratorApp';
import { SemiAutomaticReconciliationApp } from './apps/semi-automatic-reconciliation/SemiAutomaticReconciliationApp';
import { RDFGeneratorApp } from './apps/rdf-generator/RDFGeneratorApp';
import { RDFToTableApp } from './apps/rdf-to-table/RDFToTableApp';
import { HomeApp } from './apps/home/HomeApp';
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

const StreamlitRouter = (props: any) => {
  if (props.args?.app === 'home') {
    return <HomeApp />;
  }
  if (props.args?.app === 'matching_table_generator') {
    return <MatchingTableGeneratorApp {...props} />;
  }
  if (props.args?.app === 'semi_automatic_reconciliation') {
    return <SemiAutomaticReconciliationApp {...props} />;
  }
  if (props.args?.app === 'rdf_generator') {
    return <RDFGeneratorApp {...props} />;
  }
  if (props.args?.app === 'rdf_to_table') {
    return <RDFToTableApp {...props} />;
  }
  return <WorkflowConfigPanel {...props} />;
};

const ConnectedWorkflowConfigPanel = withStreamlitConnection(StreamlitRouter);
const isStandaloneRuntime = window.self === window.top;

const root = document.getElementById('root');

if (root) {
  createRoot(root).render(
    <React.StrictMode>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        {isStandaloneRuntime ? <StandaloneApp /> : <ConnectedWorkflowConfigPanel />}
      </ThemeProvider>
    </React.StrictMode>,
  );
}