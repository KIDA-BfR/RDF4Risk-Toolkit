import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
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
  useMediaQuery,
  alpha,
} from '@mui/material';
import { useColorScheme } from '@mui/material/styles';
import DarkModeIcon from '@mui/icons-material/DarkMode';
import LightModeIcon from '@mui/icons-material/LightMode';
import RefreshIcon from '@mui/icons-material/Refresh';
import MenuIcon from '@mui/icons-material/Menu';
import { WorkflowConfigPanel } from '../apps/agent-reconciliation/AgentReconciliationApp';
import { WorkflowOverview } from './WorkflowOverview';
import { MatchingTableGeneratorApp } from '../apps/matching-table-generator/MatchingTableGeneratorApp';
import { RDFGeneratorApp } from '../apps/rdf-generator/RDFGeneratorApp';
import { RDFToTableApp } from '../apps/rdf-to-table/RDFToTableApp';
import { SemiAutomaticReconciliationApp } from '../apps/semi-automatic-reconciliation/SemiAutomaticReconciliationApp';
import { setAppEventHandler, type AppEvent } from '../shared/appBridge';
import { fetchSnapshot, postEvent, fetchWorkspace, postWorkspaceEvent, type BackendPayload, type WorkspaceSnapshot } from './backendClient';
import { nextTool, previousTool, serviceFromHash, serviceMeta, workflowPhases, type ReconciliationTool, type ServiceId } from './services';
import { WorkspaceControls } from '../components/workspace/WorkspaceControls';
import { HistoryDrawer } from '../components/workspace/HistoryDrawer';
import { InfoButton } from '../components/help/InfoButton';
import { ShellStatusProvider, ToolNavigationProvider, type ShellStatus, type ToolNavigation } from '../shared/toolNavigation';
import { CONTENT_MAX_WIDTH, DRAWER_WIDTH, WIDE_LAYOUT_QUERY } from '../theme/tokens';
import rdf4RiskLogo from '../../../img/Logo_cropped_white.png';

const LAST_RECONCILIATION_KEY = 'rdf4risk-last-reconciliation-tool';

function readLastReconciliation(): ReconciliationTool {
  try {
    return window.sessionStorage.getItem(LAST_RECONCILIATION_KEY) === 'agent_reconciliation' ? 'agent_reconciliation' : 'semi_automatic_reconciliation';
  } catch {
    return 'semi_automatic_reconciliation';
  }
}

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

// One drawer row style for Overview and every tool: a neutral pill for the current page, the
// label unchanged (no coloured bar, no weight jump), so selection reads as place, not emphasis.
const navItemSx = {
  borderRadius: 1.5,
  minHeight: 40,
  mb: 0.25,
  px: 1.5,
  color: 'text.secondary',
  '&:hover': { bgcolor: 'action.hover', color: 'text.primary' },
  '&.Mui-selected, &.Mui-selected:hover': {
    bgcolor: 'action.selected',
    color: 'text.primary',
    // A fill alone is ~1.2:1 against the ground; the inset bar and weight give a ≥3:1 cue.
    boxShadow: (t: any) => `inset 3px 0 0 ${t.vars.palette.primary.main}`,
    '& .MuiListItemText-primary': { fontWeight: 600 },
  },
} as const;

export function StandaloneApp() {
  const wideLayout = useMediaQuery(WIDE_LAYOUT_QUERY);
  const [navigationOpen, setNavigationOpen] = useState(false);
  const [activeService, setActiveService] = useState<ServiceId>(() => serviceFromHash());
  const [payload, setPayload] = useState<BackendPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [workspace, setWorkspace] = useState<WorkspaceSnapshot>({ projects: [], history: [] });
  const [historyOpen, setHistoryOpen] = useState(false);
  const activeServiceRef = useRef(activeService);
  const handleEventRef = useRef<(event: AppEvent) => void>(() => undefined);
  const mutationQueue = useRef<Promise<unknown>>(Promise.resolve());
  const pendingMutations = useRef(0);
  const snapshotRequest = useRef(0);
  // Draft flags: refs for the synchronous guards, plus state so workspace dialogs can warn about
  // unapplied edits inside their own confirmation.
  const [draftTools, setDraftTools] = useState<Partial<Record<ServiceId, boolean>>>({});
  const markDraft = useCallback((service: ServiceId, dirty: boolean) => setDraftTools((flags) => (Boolean(flags[service]) === dirty ? flags : { ...flags, [service]: dirty })), []);
  const matchingDraft = useRef(false);
  const onMatchingDraft = useCallback((dirty: boolean) => { matchingDraft.current = dirty; markDraft('matching_table_generator', dirty); }, [markDraft]);
  const rdfDraft = useRef(false);
  const onRdfDraft = useCallback((dirty: boolean) => { rdfDraft.current = dirty; markDraft('rdf_generator', dirty); }, [markDraft]);
  const semiDraft = useRef(false);
  const onSemiDraft = useCallback((dirty: boolean) => { semiDraft.current = dirty; markDraft('semi_automatic_reconciliation', dirty); }, [markDraft]);

  const activeMeta = useMemo(() => serviceMeta(activeService), [activeService]);
  // Generate's Back returns to whichever reconciliation tool the curator used last.
  const [lastReconciliation, setLastReconciliation] = useState<ReconciliationTool>(readLastReconciliation);
  useEffect(() => {
    if (activeService !== 'semi_automatic_reconciliation' && activeService !== 'agent_reconciliation') return;
    setLastReconciliation(activeService);
    try { window.sessionStorage.setItem(LAST_RECONCILIATION_KEY, activeService); } catch { /* storage unavailable */ }
  }, [activeService]);

  // Unapplied edits in the active tool are only local drafts; leaving the tool drops them, so ask
  // first. (Project/history actions warn inside their own confirmation dialog instead.)
  const confirmDiscardDrafts = useCallback(() => {
    const current = activeServiceRef.current;
    const drafts: [ServiceId, React.MutableRefObject<boolean>, string][] = [
      ['rdf_generator', rdfDraft, 'Discard unapplied RDF edits? Saved settings and generated results are kept.'],
      ['semi_automatic_reconciliation', semiDraft, 'Discard unconfirmed reconciliation edits? Saved mappings and settings are kept.'],
      ['matching_table_generator', matchingDraft, 'Discard unconfirmed preparation edits? Applied rules and saved selections are kept.'],
    ];
    for (const [service, draft, message] of drafts) {
      if (current !== service || !draft.current) continue;
      if (!window.confirm(message)) return false;
      draft.current = false;
    }
    return true;
  }, []);

  const openService = useCallback((service: ServiceId) => {
    if (service !== activeServiceRef.current && !confirmDiscardDrafts()) {
      // Undo the hash change without adding a browser-history entry.
      const current = activeServiceRef.current;
      window.history.replaceState(null, '', current === 'home' ? window.location.pathname + window.location.search : `#${current}`);
      return;
    }
    activeServiceRef.current = service;
    setActiveService(service);
    setNavigationOpen(false);
    window.location.hash = service === 'home' ? '' : service;
  }, [confirmDiscardDrafts]);

  const refresh = useCallback(async (service = activeService, options?: { quiet?: boolean }) => {
    const request = ++snapshotRequest.current;
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
      await mutationQueue.current.catch(() => undefined);
      const result = await fetchSnapshot(service);
      if (request === snapshotRequest.current && activeServiceRef.current === service) setPayload(result);
    } catch (err) {
      // Quiet background polls keep the last good payload instead of flashing an error banner.
      if (!options?.quiet && activeServiceRef.current === service) setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (!options?.quiet && request === snapshotRequest.current) setLoading(pendingMutations.current > 0);
    }
  }, [activeService]);

  const refreshWorkspace = useCallback(async () => {
    try {
      setWorkspace(await fetchWorkspace());
    } catch {
      // workspace is non-critical; ignore transient failures
    }
  }, []);

  const emitEvent = useCallback(async (event: AppEvent, sourceService?: ServiceId) => {
    const service = sourceService ?? activeServiceRef.current;
    if (service === 'home') return;
    // Capture the service at dispatch time and serialize mutations. A late response must
    // never replace a newer selection or the payload of a different workspace.
    pendingMutations.current += 1;
    const previous = mutationQueue.current;
    let complete: () => void = () => undefined;
    mutationQueue.current = new Promise<void>(resolve => { complete = resolve; });
    setLoading(true);
    setError(null);
    try {
      await previous.catch(() => undefined);
      const result = await postEvent(service, event);
      ++snapshotRequest.current;
      if (activeServiceRef.current === service) setPayload(result);
      refreshWorkspace(); // a mutating event may have added a history entry
      const handoff = (result.args as any)?.snapshot?.handoff;
      if (service === 'matching_table_generator' && event.type === 'handoff_reconciliation' && handoff?.status === 'complete' && activeServiceRef.current === service) {
        openService(handoff.target as ServiceId);
      }
      const rdfHandoff = (result.args as any)?.snapshot?.rdf_handoff;
      if (service === 'semi_automatic_reconciliation' && event.type === 'handoff_rdf_generator' && rdfHandoff?.status === 'complete' && activeServiceRef.current === service) openService('rdf_generator');
      if (service === 'agent_reconciliation' && event.type === 'handoff_rdf_generator' && (result.args as any)?.rdf_handoff?.status === 'complete' && activeServiceRef.current === service) openService('rdf_generator');
      return result;
    } catch (err) {
      if (activeServiceRef.current === service) setError(err instanceof Error ? err.message : String(err));
      return false;
    } finally {
      complete();
      pendingMutations.current -= 1;
      setLoading(pendingMutations.current > 0);
    }
  }, [refreshWorkspace, openService]);
  const emitMatchingEvent = useCallback((event: AppEvent) => emitEvent(event, 'matching_table_generator'), [emitEvent]);
  const emitSemiEvent = useCallback((event: AppEvent) => emitEvent(event, 'semi_automatic_reconciliation'), [emitEvent]);

  // Back on a tool's first stage and Continue on its last stage step through the pipeline, so the
  // two buttons mean the same thing everywhere. Draft guards in openService still apply.
  const toolNavigation = useMemo<ToolNavigation>(() => {
    const previous = previousTool(activeService, lastReconciliation);
    const next = nextTool(activeService);
    return {
      goBack: previous ? () => openService(previous) : undefined,
      backLabel: previous ? serviceMeta(previous).title : undefined,
      goNext: next ? () => openService(next) : undefined,
      nextLabel: next ? serviceMeta(next).title : undefined,
      continueWith: next
        ? async (event: AppEvent) => {
          const result = await emitEvent(event, next);
          // emitEvent only reports errors for the active tool; this one targets the next tool.
          if (!result) {
            setError(`Could not pass the result on to ${serviceMeta(next).title}. Try again, or load it there directly.`);
            return false;
          }
          openService(next);
          return true;
        }
        : undefined,
    };
  }, [activeService, lastReconciliation, openService, emitEvent]);

  const shellStatus = useMemo<ShellStatus>(() => ({
    error,
    dismissError: () => setError(null),
    reload: () => { setError(null); refresh(activeServiceRef.current); },
  }), [error, refresh]);
  const unsavedEditsNote = draftTools[activeService] ? `Unapplied edits in ${serviceMeta(activeService).title} will be lost.` : undefined;

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
    const onHashChange = () => openService(serviceFromHash());
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, [openService]);

  useEffect(() => setAppEventHandler((event: AppEvent) => handleEventRef.current(event)), []);
  useEffect(() => { refreshWorkspace(); }, [refreshWorkspace]);

  const args = (payload?.service === activeService ? payload.args : undefined) as any;
  let content: React.ReactNode = <WorkflowOverview onOpen={openService} />;
  if (activeService === 'matching_table_generator') content = <MatchingTableGeneratorApp args={args} onEvent={emitMatchingEvent} busy={loading} onDraftChange={onMatchingDraft} />;
  if (activeService === 'semi_automatic_reconciliation') content = <SemiAutomaticReconciliationApp args={args} onEvent={emitSemiEvent} busy={loading} onDraftChange={onSemiDraft} />;
  if (activeService === 'agent_reconciliation') content = <WorkflowConfigPanel args={args} onEvent={emitEvent} busy={loading} />;
  if (activeService === 'rdf_generator') content = <RDFGeneratorApp args={args} busy={loading} onDraftChange={onRdfDraft} />;
  if (activeService === 'rdf_to_table') content = <RDFToTableApp args={args} busy={loading} />;
  if (activeService !== 'home' && !args) content = <Stack role="status" alignItems="center" spacing={2} sx={{ p: 5 }}><CircularProgress /><Typography>Loading workspace…</Typography></Stack>;

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh', bgcolor: 'background.default' }}>
      <Drawer variant={wideLayout ? 'permanent' : 'temporary'} open={wideLayout || navigationOpen} onClose={() => setNavigationOpen(false)} sx={{ width: wideLayout ? DRAWER_WIDTH : 0, flexShrink: 0, '& .MuiDrawer-paper': { width: DRAWER_WIDTH, boxSizing: 'border-box', borderRight: '1px solid', borderColor: 'divider', bgcolor: 'background.default' } }}>
        <Toolbar sx={{ gap: 1.5, py: 2.5, px: '20px !important', minHeight: 'auto !important' }}>
          <Box component="img" src={rdf4RiskLogo} alt="" sx={{ width: 40, height: 40, borderRadius: 2, objectFit: 'contain', bgcolor: 'common.white', border: 1, borderColor: 'divider' }} />
          <Box>
            <Typography component="p" sx={{ fontWeight: 600, letterSpacing: '-.01em', fontSize: '1rem', lineHeight: 1.3 }}>RDF4Risk</Typography>
            <Typography variant="caption" color="text.secondary">Linked Data toolkit</Typography>
          </Box>
        </Toolbar>
        <Divider />
        <Box component="nav" aria-label="Toolkit navigation" sx={{ px: 1.5, py: 1.5 }}>
          <List disablePadding>
            <ListItemButton selected={activeService === 'home'} aria-current={activeService === 'home' ? 'page' : undefined} onClick={() => openService('home')} sx={navItemSx}>
              <ListItemText primary={serviceMeta('home').title} primaryTypographyProps={{ fontWeight: 500, fontSize: '.875rem' }} />
            </ListItemButton>
          </List>
          {workflowPhases.map((phase, index) => (
            <Box key={phase.title} sx={{ mt: 2.5 }}>
              <Typography component="p" variant="caption" color="text.secondary" sx={{ px: 1.5, mb: 0.75, display: 'flex', alignItems: 'baseline', gap: 1 }}>
                <Box component="span" aria-hidden="true" sx={{ fontVariantNumeric: 'tabular-nums', minWidth: 10 }}>{index + 1}</Box>
                {phase.title}
              </Typography>
              <List disablePadding aria-label={phase.title}>
                {phase.services.map(id => {
                  const selected = activeService === id;
                  return <ListItemButton key={id} selected={selected} aria-current={selected ? 'page' : undefined} onClick={() => openService(id)} sx={{ ...navItemSx, pl: 4 }}>
                    <ListItemText primary={serviceMeta(id).title} primaryTypographyProps={{ fontWeight: 500, fontSize: '.875rem' }} />
                  </ListItemButton>;
                })}
              </List>
            </Box>
          ))}
        </Box>
        <Box sx={{ mt: 'auto', pb: 1.5 }}>
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
            unsavedEditsNote={unsavedEditsNote}
          />
        </Box>
      </Drawer>
      <HistoryDrawer
        open={historyOpen}
        history={workspace.history}
        onClose={() => setHistoryOpen(false)}
        onRevert={(index, id) => runWorkspace({ type: 'revert_to', index, ...(id ? { id } : {}) }, { reloadService: true })}
        onUndo={() => runWorkspace({ type: 'undo' }, { reloadService: true })}
        unsavedEditsNote={unsavedEditsNote}
      />
      <Box component="main" sx={{ flexGrow: 1, minWidth: 0 }}>
        <Paper square elevation={0} sx={{ position: 'sticky', top: 0, zIndex: 5, px: { xs: 2, md: 3 }, minHeight: 56, display: 'flex', alignItems: 'center', borderBottom: '1px solid', borderColor: 'divider', bgcolor: (t) => alpha(t.palette.background.paper, 0.92), backdropFilter: 'blur(10px)' }}>
          <Stack direction="row" alignItems="center" spacing={1} sx={{ width: '100%', minWidth: 0 }}>
            {!wideLayout && <IconButton aria-label="Open navigation" aria-expanded={navigationOpen} onClick={() => setNavigationOpen(true)} sx={{ ml: -1 }}><MenuIcon /></IconButton>}
            {/* The title bar carries the single h1 of every view; what the tool does sits behind ⓘ. */}
            <Stack direction="row" alignItems="center" spacing={0.5} sx={{ flex: 1, minWidth: 0 }}>
              <Typography variant="h6" component="h1" noWrap>{activeMeta.title}</Typography>
              <InfoButton title={activeMeta.title}>
                {activeMeta.details.map((paragraph) => <p key={paragraph}>{paragraph}</p>)}
              </InfoButton>
            </Stack>
            {loading && <CircularProgress size={20} aria-label="Working" />}
            {activeService !== 'home' && (
              <Tooltip title="Reload this tool's data">
                <span>
                  <IconButton aria-label="Reload this tool's data" size="small" onClick={() => refresh(activeServiceRef.current)} disabled={loading}>
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </span>
              </Tooltip>
            )}
            <ColorModeToggle />
          </Stack>
        </Paper>
        <ToolNavigationProvider value={toolNavigation}>
          <ShellStatusProvider value={shellStatus}>
          <Box sx={{ p: { xs: 2, md: 3 } }}>
            {error && (activeService === 'home' || !args) && (
              <Alert
                severity="error"
                role="alert"
                sx={{ maxWidth: CONTENT_MAX_WIDTH, mx: 'auto', mb: 2, whiteSpace: 'pre-wrap' }}
                onClose={() => setError(null)}
                action={
                  <Button color="inherit" size="small" onClick={() => refresh()}>
                    Reload
                  </Button>
                }
              >
                {error}
              </Alert>
            )}
            {content}
          </Box>
          </ShellStatusProvider>
        </ToolNavigationProvider>
      </Box>
    </Box>
  );
}
