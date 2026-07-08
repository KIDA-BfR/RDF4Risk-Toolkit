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
  IconButton,
  Toolbar,
  Tooltip,
  Typography,
  alpha,
} from '@mui/material';
import { useColorScheme } from '@mui/material/styles';
import DarkModeIcon from '@mui/icons-material/DarkMode';
import LightModeIcon from '@mui/icons-material/LightMode';
import RefreshIcon from '@mui/icons-material/Refresh';
import { WorkflowConfigPanel } from '../apps/agent-reconciliation/AgentReconciliationApp';
import { HomeApp } from '../apps/home/HomeApp';
import { MatchingTableGeneratorApp } from '../apps/matching-table-generator/MatchingTableGeneratorApp';
import { RDFGeneratorApp } from '../apps/rdf-generator/RDFGeneratorApp';
import { RDFToTableApp } from '../apps/rdf-to-table/RDFToTableApp';
import { SemiAutomaticReconciliationApp } from '../apps/semi-automatic-reconciliation/SemiAutomaticReconciliationApp';
import { setAppEventHandler, type AppEvent } from '../shared/appBridge';
import { fetchSnapshot, postEvent, fetchWorkspace, postWorkspaceEvent, type BackendPayload, type WorkspaceSnapshot } from './backendClient';
import { serviceFromHash, services, type ServiceId } from './services';
import { WorkspaceControls } from '../components/workspace/WorkspaceControls';
import { HistoryDrawer } from '../components/workspace/HistoryDrawer';
import rdf4RiskLogo from '../../../img/Logo_cropped_white.png';

const drawerWidth = 312;

// Light hero gradient in light mode; a flat paper surface in dark mode (the bright gradient
// would glare against a dark page).
const heroBg = (t: any) =>
  t.palette.mode === 'dark' ? t.vars.palette.background.paper : 'linear-gradient(135deg,#ffffff 0%,#f0fdfa 50%,#eff6ff 100%)';

function ColorModeToggle() {
  const { mode, systemMode, setMode } = useColorScheme();
  const resolved = mode === 'system' ? systemMode : mode;
  if (!mode) return null; // not yet mounted
  const isDark = resolved === 'dark';
  return (
    <Tooltip title={isDark ? 'Switch to light mode' : 'Switch to dark mode'}>
      <IconButton aria-label="Toggle color mode" onClick={() => setMode(isDark ? 'light' : 'dark')} size="small">
        {isDark ? <LightModeIcon fontSize="small" /> : <DarkModeIcon fontSize="small" />}
      </IconButton>
    </Tooltip>
  );
}

function HomeDashboard({ onOpen }: { onOpen: (service: ServiceId) => void }) {
  const workflowServices = services.filter((service) => service.id !== 'home');
  return (
    <Box sx={{ bgcolor: 'background.default', minHeight: '100%', p: { xs: 2, md: 3 } }}>
      <Stack spacing={3}>
        <Paper variant="outlined" sx={{ p: { xs: 3, md: 6 }, borderRadius: 5, background: heroBg, boxShadow: '0 18px 48px rgba(15,23,42,.08)' }}>
          <Stack spacing={2}>
            <Chip label="RDF4Risk web app" sx={{ alignSelf: 'flex-start', color: '#0369a1', bgcolor: alpha('#0ea5e9', 0.1), fontWeight: 850 }} />
            {/* Decorative hero wordmark — the page h1 lives in the title bar. */}
            <Typography variant="h1" component="p" sx={{ maxWidth: 950, fontSize: 'clamp(2.35rem, 5vw, 4.8rem)', lineHeight: 0.98, fontWeight: 950, letterSpacing: '-0.055em' }}>
              RDF4Risk Toolkit
            </Typography>
            <Typography color="text.secondary" sx={{ maxWidth: 980, fontSize: '1.1rem', lineHeight: 1.75 }}>
              RDF4Risk brings together practical tools for turning tabular research data into FAIR Linked Data for risk assessment and life sciences. Prepare matching tables, reconcile terms with trusted vocabularies, generate RDF, and review or export results through one guided workflow workspace.
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
                    <Box aria-hidden="true" sx={{ color: '#fff', bgcolor: service.accent, borderRadius: '50%', width: 34, height: 34, display: 'grid', placeItems: 'center', fontWeight: 900 }}>→</Box>
                  </Stack>
                  <Typography variant="h5" component="h2">{service.title}</Typography>
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

export function StandaloneApp() {
  const [activeService, setActiveService] = useState<ServiceId>(() => serviceFromHash());
  const [payload, setPayload] = useState<BackendPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [workspace, setWorkspace] = useState<WorkspaceSnapshot>({ projects: [], history: [] });
  const [historyOpen, setHistoryOpen] = useState(false);
  const activeServiceRef = useRef(activeService);
  const handleEventRef = useRef<(event: AppEvent) => void>(() => undefined);

  const activeMeta = useMemo(() => services.find((service) => service.id === activeService) ?? services[0], [activeService]);

  const openService = useCallback((service: ServiceId) => {
    setActiveService(service);
    window.location.hash = service === 'home' ? '' : service;
  }, []);

  const refresh = useCallback(async (service = activeService, options?: { quiet?: boolean }) => {
    activeServiceRef.current = service;
    if (service === 'home') {
      setPayload(null);
      setError(null);
      return;
    }
    if (!options?.quiet) {
      setLoading(true);
      setError(null);
    }
    try {
      setPayload(await fetchSnapshot(service));
    } catch (err) {
      // Quiet background polls keep the last good payload instead of flashing an error banner.
      if (!options?.quiet) setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (!options?.quiet) setLoading(false);
    }
  }, [activeService]);

  const refreshWorkspace = useCallback(async () => {
    try {
      setWorkspace(await fetchWorkspace());
    } catch {
      // workspace is non-critical; ignore transient failures
    }
  }, []);

  const emitEvent = useCallback(async (event: AppEvent) => {
    const service = activeServiceRef.current;
    if (service === 'home') return;
    setLoading(true);
    setError(null);
    try {
      setPayload(await postEvent(service, event));
      refreshWorkspace(); // a mutating event may have added a history entry
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [refreshWorkspace]);

  // Run a cross-service workspace action (project save/open/delete, history revert/undo), then
  // re-fetch the active service since load/revert restore backend state under the current view.
  const runWorkspace = useCallback(async (event: Record<string, unknown>, options?: { reloadService?: boolean }) => {
    setLoading(true);
    setError(null);
    try {
      setWorkspace(await postWorkspaceEvent(event));
      if (options?.reloadService) await refresh(activeServiceRef.current);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [refresh]);

  // Export the whole-pipeline configuration as a downloadable, paper-attachable JSON recipe.
  const exportRecipe = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await postWorkspaceEvent({ type: 'export_recipe' });
      const doc = (result as any).recipe_document;
      const blob = new Blob([JSON.stringify(doc, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `rdf4risk_recipe_${new Date().toISOString().slice(0, 10)}.json`;
      anchor.click();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const importRecipe = useCallback(async (file: File) => {
    setLoading(true);
    setError(null);
    try {
      const document = JSON.parse(await file.text());
      setWorkspace(await postWorkspaceEvent({ type: 'import_recipe', recipe_document: document }));
      await refresh(activeServiceRef.current);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not read the recipe file.');
    } finally {
      setLoading(false);
    }
  }, [refresh]);

  useEffect(() => { handleEventRef.current = emitEvent; }, [emitEvent]);
  useEffect(() => { activeServiceRef.current = activeService; refresh(activeService); }, [activeService, refresh]);
  const runStatus = (payload?.args as any)?.run_status;
  // Semi-automatic reconciliation reports background-queue progress under snapshot.run.
  const snapshotRun = (payload?.args as any)?.snapshot?.run;
  const shouldPollActiveRun = activeService !== 'home' && Boolean(runStatus?.running || snapshotRun?.processing_active);
  useEffect(() => {
    if (!shouldPollActiveRun) return undefined;
    const timer = window.setInterval(() => {
      refresh(activeServiceRef.current, { quiet: true });
    }, 1000);
    return () => window.clearInterval(timer);
  }, [refresh, shouldPollActiveRun]);
  useEffect(() => {
    const onHashChange = () => setActiveService(serviceFromHash());
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  useEffect(() => setAppEventHandler((event: AppEvent) => handleEventRef.current(event)), []);
  useEffect(() => { refreshWorkspace(); }, [refreshWorkspace]);

  const args = payload?.args as any;
  let content: React.ReactNode = <HomeDashboard onOpen={openService} />;
  if (activeService === 'matching_table_generator') content = <MatchingTableGeneratorApp args={args} onEvent={emitEvent} />;
  if (activeService === 'semi_automatic_reconciliation') content = <SemiAutomaticReconciliationApp args={args} />;
  if (activeService === 'agent_reconciliation') content = <WorkflowConfigPanel args={args} onEvent={emitEvent} />;
  if (activeService === 'rdf_generator') content = <RDFGeneratorApp args={args} />;
  if (activeService === 'rdf_to_table') content = <RDFToTableApp args={args} />;

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh', bgcolor: 'background.default' }}>
      <Drawer variant="permanent" sx={{ width: drawerWidth, flexShrink: 0, '& .MuiDrawer-paper': { width: drawerWidth, boxSizing: 'border-box', borderRight: '1px solid', borderColor: 'divider', bgcolor: 'background.default' } }}>
        <Toolbar sx={{ alignItems: 'flex-start', flexDirection: 'column', py: 2, bgcolor: 'background.paper' }}>
          <Typography variant="h6" component="p">RDF4Risk Toolkit</Typography>
          <Typography variant="caption" color="text.secondary">RDF4Risk app</Typography>
          <Box
            component="img"
            src={rdf4RiskLogo}
            alt="RDF4Risk logo"
            sx={{
              width: 88,
              height: 88,
              mt: 1.5,
              borderRadius: 2,
              objectFit: 'contain',
              bgcolor: 'common.white',
            }}
          />
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
          <Divider sx={{ mb: 1 }} />
          <WorkspaceControls
            projects={workspace.projects}
            historyCount={workspace.history.length}
            onSave={(name) => runWorkspace({ type: 'save_project', name })}
            onOpen={(id) => runWorkspace({ type: 'load_project', project_id: id }, { reloadService: true })}
            onDelete={(id) => runWorkspace({ type: 'delete_project', project_id: id })}
            onNew={() => runWorkspace({ type: 'new_project' })}
            onOpenHistory={() => setHistoryOpen(true)}
            onExportRecipe={exportRecipe}
            onImportRecipe={importRecipe}
          />
          <Box sx={{ px: 2, pb: 2 }}>
            <Button fullWidth variant="outlined" startIcon={<RefreshIcon />} onClick={() => refresh(activeServiceRef.current)} disabled={loading || activeService === 'home'}>Refresh service</Button>
          </Box>
        </Box>
      </Drawer>
      <HistoryDrawer
        open={historyOpen}
        history={workspace.history}
        onClose={() => setHistoryOpen(false)}
        onRevert={(index) => runWorkspace({ type: 'revert_to', index }, { reloadService: true })}
        onUndo={() => runWorkspace({ type: 'undo' }, { reloadService: true })}
      />
      <Box component="main" sx={{ flexGrow: 1, minWidth: 0 }}>
        <Paper square elevation={0} sx={{ position: 'sticky', top: 0, zIndex: 5, px: 3, py: 1.5, borderBottom: '1px solid', borderColor: 'divider', bgcolor: (t) => alpha(t.palette.background.paper, 0.92), backdropFilter: 'blur(10px)' }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" spacing={2}>
            <Box>
              {/* The page-title bar carries the single h1 of every view (visually h6-sized). */}
              <Typography variant="h6" component="h1">{activeMeta.title}</Typography>
              <Typography variant="caption" color="text.secondary">{activeMeta.description}</Typography>
            </Box>
            <Stack direction="row" alignItems="center" spacing={1}>
              {loading && <CircularProgress size={24} />}
              <ColorModeToggle />
            </Stack>
          </Stack>
        </Paper>
        {error && (
          <Alert
            severity="error"
            role="alert"
            sx={{ m: 2, whiteSpace: 'pre-wrap' }}
            onClose={() => setError(null)}
            action={
              <Button color="inherit" size="small" onClick={() => refresh()}>
                Retry
              </Button>
            }
          >
            {error}
          </Alert>
        )}
        <Box sx={{ p: { xs: 1, md: 2 } }}>{content}</Box>
      </Box>
    </Box>
  );
}
