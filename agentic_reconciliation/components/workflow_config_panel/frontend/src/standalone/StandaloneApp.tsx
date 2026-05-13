import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Card,
  CardActionArea,
  CardContent,
  Chip,
  CircularProgress,
  Divider,
  Drawer,
  List,
  ListItemButton,
  ListItemText,
  Paper,
  Stack,
  Toolbar,
  Typography,
  alpha,
} from '@mui/material';
import { Streamlit } from 'streamlit-component-lib';
import { WorkflowConfigPanel } from '../apps/agent-reconciliation/AgentReconciliationApp';
import { HomeApp } from '../apps/home/HomeApp';
import { MatchingTableGeneratorApp } from '../apps/matching-table-generator/MatchingTableGeneratorApp';
import { RDFGeneratorApp } from '../apps/rdf-generator/RDFGeneratorApp';
import { RDFToTableApp } from '../apps/rdf-to-table/RDFToTableApp';
import { SemiAutomaticReconciliationApp } from '../apps/semi-automatic-reconciliation/SemiAutomaticReconciliationApp';

type ServiceId = 'home' | 'matching_table_generator' | 'semi_automatic_reconciliation' | 'agent_reconciliation' | 'rdf_generator' | 'rdf_to_table';
type BackendPayload = { service: string; args: Record<string, unknown> };
type AppEvent = { type: string; [key: string]: unknown };

const drawerWidth = 312;
const API_BASE = (import.meta as any).env?.VITE_RDF4RISK_API_BASE || 'http://127.0.0.1:8765';

const services: Array<{ id: ServiceId; step: string; title: string; short: string; description: string; accent: string }> = [
  { id: 'home', step: '00', title: 'Home', short: 'Toolkit overview', description: 'Start page with direct links to all RDF4Risk workflow services.', accent: '#0f172a' },
  { id: 'matching_table_generator', step: '01', title: 'Matching Table Service', short: 'Prepare mappings', description: 'Load tabular data, preprocess values, consolidate terms, and generate SSSOM-ready matching tables.', accent: '#2563eb' },
  { id: 'semi_automatic_reconciliation', step: '02', title: 'Reconciliation Service', short: 'Manual curation', description: 'Use provider queues, ontology filters, and custom searches to reconcile terms against external authorities.', accent: '#0891b2' },
  { id: 'agent_reconciliation', step: '03', title: 'Agent-Based Reconciliation', short: 'AI-assisted matching', description: 'Run LLM-assisted semantic reconciliation with review, telemetry, SSSOM export, and ChatGPT subscription auth status.', accent: '#7c3aed' },
  { id: 'rdf_generator', step: '04', title: 'RDF Generator Service', short: 'Generate RDF', description: 'Combine data and mapping tables, enrich URI context, apply schema templates, and generate RDF/SKOS/DCAT outputs.', accent: '#14b8a6' },
  { id: 'rdf_to_table', step: '05', title: 'RDF to Table Service', short: 'Inspect RDF', description: 'Load TriG catalogs, inspect named graphs and statistics, then export CSV, Excel, and Markdown documentation.', accent: '#f59e0b' },
];

function serviceFromHash(): ServiceId {
  const raw = window.location.hash.replace(/^#\/?/, '') as ServiceId;
  return services.some((service) => service.id === raw) ? raw : 'home';
}

async function fetchSnapshot(service: ServiceId): Promise<BackendPayload | null> {
  if (service === 'home') return null;
  const response = await fetch(`${API_BASE}/api/services/${service}/snapshot`);
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

async function postEvent(service: ServiceId, event: AppEvent): Promise<BackendPayload> {
  const response = await fetch(`${API_BASE}/api/services/${service}/event`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(event),
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function HomeDashboard({ onOpen }: { onOpen: (service: ServiceId) => void }) {
  const workflowServices = services.filter((service) => service.id !== 'home');
  return (
    <Box sx={{ bgcolor: '#eef7fb', minHeight: '100%', p: { xs: 2, md: 3 } }}>
      <Stack spacing={3}>
        <Paper variant="outlined" sx={{ p: { xs: 3, md: 6 }, borderRadius: 5, background: 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 48%,#eff6ff 100%)', boxShadow: '0 18px 48px rgba(15,23,42,.08)' }}>
          <Stack spacing={2}>
            <Chip label="RDF4Risk web app" sx={{ alignSelf: 'flex-start', color: '#0369a1', bgcolor: alpha('#0ea5e9', 0.1), fontWeight: 850 }} />
            <Typography variant="h1" sx={{ maxWidth: 950, fontSize: 'clamp(2.35rem, 5vw, 4.8rem)', lineHeight: 0.98, fontWeight: 950, letterSpacing: '-0.055em' }}>
              RDF4Risk Toolkit
            </Typography>
            <Typography color="text.secondary" sx={{ maxWidth: 980, fontSize: '1.1rem', lineHeight: 1.75 }}>
              The app now starts from npm with a browser-based interface. Python remains the backend for data loading, reconciliation, RDF generation, conversion, and agent orchestration.
            </Typography>
          </Stack>
        </Paper>

        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: 'repeat(2, minmax(0, 1fr))' }, gap: 2 }}>
          {workflowServices.map((service) => (
            <Card key={service.id} variant="outlined" sx={{ borderRadius: 4, borderColor: alpha(service.accent, 0.24), overflow: 'hidden' }}>
              <CardActionArea onClick={() => onOpen(service.id)} sx={{ height: '100%' }}>
                <CardContent sx={{ minHeight: 220, display: 'flex', flexDirection: 'column', gap: 1.4 }}>
                  <Stack direction="row" justifyContent="space-between" spacing={2}>
                    <Chip label={`Step ${service.step}`} sx={{ color: service.accent, bgcolor: alpha(service.accent, 0.08), fontWeight: 850 }} />
                    <Box sx={{ color: '#fff', bgcolor: service.accent, borderRadius: '50%', width: 34, height: 34, display: 'grid', placeItems: 'center', fontWeight: 900 }}>→</Box>
                  </Stack>
                  <Typography variant="h5">{service.title}</Typography>
                  <Typography sx={{ color: service.accent, fontWeight: 850, textTransform: 'uppercase', fontSize: '.78rem', letterSpacing: '.08em' }}>{service.short}</Typography>
                  <Typography color="text.secondary" sx={{ lineHeight: 1.65 }}>{service.description}</Typography>
                </CardContent>
              </CardActionArea>
            </Card>
          ))}
        </Box>
      </Stack>
    </Box>
  );
}

function AgentSidebarInfo({ args, onEvent }: { args?: Record<string, any>; onEvent: (event: AppEvent) => void }) {
  const auth = args?.codexAuthStatus;
  const providerKind = args?.providerKind;
  if (!args || providerKind !== 'codex') {
    return (
      <Alert severity="info" variant="outlined" sx={{ mx: 2, mb: 2 }}>
        Select the ChatGPT Subscription provider in Agent-Based Reconciliation to manage subscription login here.
      </Alert>
    );
  }
  return (
    <Box sx={{ mx: 2, mb: 2 }}>
      <Stack spacing={1}>
        <Typography variant="subtitle2">ChatGPT Subscription</Typography>
        <Alert severity={auth?.authenticated ? 'success' : 'warning'} variant="outlined" sx={{ py: 0.5 }}>
          {auth?.authenticated ? 'Connected' : 'Not connected'}
        </Alert>
        {auth?.pending_auth_url && <Button size="small" href={auth.pending_auth_url} target="_blank" variant="outlined">Open login link</Button>}
        <Stack direction="row" spacing={1}>
          {auth?.authenticated ? (
            <Button size="small" color="error" variant="outlined" onClick={() => onEvent({ type: 'codex_auth_signout' })}>Log out</Button>
          ) : (
            <Button size="small" variant="contained" onClick={() => onEvent({ type: 'codex_auth_signin' })}>Log in</Button>
          )}
          <Button size="small" variant="outlined" onClick={() => onEvent({ type: 'codex_auth_refresh' })}>Refresh</Button>
        </Stack>
      </Stack>
    </Box>
  );
}

export function StandaloneApp() {
  const [activeService, setActiveService] = useState<ServiceId>(() => serviceFromHash());
  const [payload, setPayload] = useState<BackendPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const activeServiceRef = useRef(activeService);
  const handleEventRef = useRef<(event: AppEvent) => void>(() => undefined);

  const activeMeta = useMemo(() => services.find((service) => service.id === activeService) ?? services[0], [activeService]);

  const openService = useCallback((service: ServiceId) => {
    setActiveService(service);
    window.location.hash = service === 'home' ? '' : service;
  }, []);

  const refresh = useCallback(async (service = activeService) => {
    activeServiceRef.current = service;
    if (service === 'home') {
      setPayload(null);
      setError(null);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      setPayload(await fetchSnapshot(service));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [activeService]);

  const emitEvent = useCallback(async (event: AppEvent) => {
    const service = activeServiceRef.current;
    if (service === 'home') return;
    setLoading(true);
    setError(null);
    try {
      setPayload(await postEvent(service, event));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { handleEventRef.current = emitEvent; }, [emitEvent]);
  useEffect(() => { activeServiceRef.current = activeService; refresh(activeService); }, [activeService, refresh]);
  useEffect(() => {
    const onHashChange = () => setActiveService(serviceFromHash());
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  useEffect(() => {
    const streamlit = Streamlit as any;
    streamlit.setFrameHeight = () => undefined;
    streamlit.setComponentValue = (event: AppEvent) => handleEventRef.current(event);
  }, []);

  const args = payload?.args as any;
  let content: React.ReactNode = <HomeDashboard onOpen={openService} />;
  if (activeService === 'matching_table_generator') content = <MatchingTableGeneratorApp args={args} />;
  if (activeService === 'semi_automatic_reconciliation') content = <SemiAutomaticReconciliationApp args={args} />;
  if (activeService === 'agent_reconciliation') content = <WorkflowConfigPanel args={args} />;
  if (activeService === 'rdf_generator') content = <RDFGeneratorApp args={args} />;
  if (activeService === 'rdf_to_table') content = <RDFToTableApp args={args} />;

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh', bgcolor: '#eef7fb' }}>
      <Drawer variant="permanent" sx={{ width: drawerWidth, flexShrink: 0, '& .MuiDrawer-paper': { width: drawerWidth, boxSizing: 'border-box', borderRight: '1px solid #dbeafe', bgcolor: '#f8fafc' } }}>
        <Toolbar sx={{ alignItems: 'flex-start', flexDirection: 'column', py: 2 }}>
          <Typography variant="h6">RDF4Risk Toolkit</Typography>
          <Typography variant="caption" color="text.secondary">RDF4Risk app</Typography>
        </Toolbar>
        <Divider />
        <List sx={{ px: 1.2 }}>
          {services.map((service) => (
            <ListItemButton key={service.id} selected={activeService === service.id} onClick={() => openService(service.id)} sx={{ borderRadius: 2, mb: 0.5 }}>
              <ListItemText primary={`${service.step} · ${service.title}`} secondary={service.short} primaryTypographyProps={{ fontWeight: 850, fontSize: '.92rem' }} />
            </ListItemButton>
          ))}
        </List>
        <Box sx={{ mt: 'auto' }}>
          {activeService === 'agent_reconciliation' && <AgentSidebarInfo args={args} onEvent={emitEvent} />}
          <Box sx={{ px: 2, pb: 2 }}>
            <Button fullWidth variant="outlined" onClick={() => refresh()} disabled={loading || activeService === 'home'}>Refresh service</Button>
          </Box>
        </Box>
      </Drawer>
      <Box component="main" sx={{ flexGrow: 1, minWidth: 0 }}>
        <Paper square elevation={0} sx={{ position: 'sticky', top: 0, zIndex: 5, px: 3, py: 1.5, borderBottom: '1px solid #dbeafe', bgcolor: 'rgba(255,255,255,.92)', backdropFilter: 'blur(10px)' }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" spacing={2}>
            <Box>
              <Typography variant="h6">{activeMeta.title}</Typography>
              <Typography variant="caption" color="text.secondary">{activeMeta.description}</Typography>
            </Box>
            {loading && <CircularProgress size={24} />}
          </Stack>
        </Paper>
        {error && <Alert severity="error" sx={{ m: 2, whiteSpace: 'pre-wrap' }}>{error}</Alert>}
        <Box sx={{ p: { xs: 1, md: 2 } }}>{content}</Box>
      </Box>
    </Box>
  );
}