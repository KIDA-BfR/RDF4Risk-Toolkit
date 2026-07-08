import React, { useEffect, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  FormControlLabel,
  IconButton,
  InputAdornment,
  InputLabel,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  MenuItem,
  Paper,
  Select,
  Stack,
  Switch,
  Tab,
  Tabs,
  TextField,
  Typography,
  alpha,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import FingerprintIcon from '@mui/icons-material/Fingerprint';
import InsightsIcon from '@mui/icons-material/Insights';
import LoginIcon from '@mui/icons-material/Login';
import LogoutIcon from '@mui/icons-material/Logout';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import RefreshIcon from '@mui/icons-material/Refresh';
import SaveIcon from '@mui/icons-material/Save';
import ScienceIcon from '@mui/icons-material/Science';
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import StorageIcon from '@mui/icons-material/Storage';
import TuneIcon from '@mui/icons-material/Tune';
import VpnKeyIcon from '@mui/icons-material/VpnKey';
import RegistryAdminPanel from './RegistryAdminPanel';
import type { AdvancedConfig, AppEvent, AutoAcceptPolicy, ComponentArgs, ProvenanceConfig, WorkflowConfig } from './types';
import { asNumber, normalizeCandidateReviewMode, normalizeTraceLevel } from './utils';

type SectionId = 'model' | 'connection' | 'matching' | 'observability' | 'advanced' | 'registry' | 'provenance';

const SECTIONS: { id: SectionId; label: string; icon: React.ReactElement }[] = [
  { id: 'model', label: 'Provider & Model', icon: <SmartToyIcon fontSize="small" /> },
  { id: 'connection', label: 'Connection & Keys', icon: <VpnKeyIcon fontSize="small" /> },
  { id: 'matching', label: 'Matching & Review', icon: <TuneIcon fontSize="small" /> },
  { id: 'observability', label: 'Monitoring & Tracing', icon: <InsightsIcon fontSize="small" /> },
  { id: 'advanced', label: 'Advanced', icon: <ScienceIcon fontSize="small" /> },
  { id: 'registry', label: 'BioPortal Registry', icon: <StorageIcon fontSize="small" /> },
  { id: 'provenance', label: 'Provenance Metadata', icon: <FingerprintIcon fontSize="small" /> },
];

function ToggleCard({ checked, title, description, onChange }: { checked: boolean; title: string; description: string; onChange: (value: boolean) => void }) {
  return (
    <Paper
      variant="outlined"
      sx={{
        p: 1.5,
        minHeight: 94,
        borderRadius: 3,
        borderColor: checked ? 'primary.main' : 'divider',
        bgcolor: checked ? (t) => alpha(t.palette.primary.main, 0.04) : 'background.paper',
      }}
    >
      <FormControlLabel
        sx={{ alignItems: 'flex-start', m: 0 }}
        control={<Checkbox checked={checked} onChange={(e) => onChange(e.target.checked)} sx={{ p: 0, mr: 1 }} />}
        label={
          <Stack>
            <Typography variant="body2" sx={{ fontWeight: 800 }}>{title}</Typography>
            <Typography variant="caption" color="text.secondary">{description}</Typography>
          </Stack>
        }
      />
    </Paper>
  );
}

function SectionHeading({ title, description }: { title: string; description: string }) {
  return (
    <Stack spacing={0.5}>
      <Typography variant="subtitle1" component="h3">{title}</Typography>
      <Typography variant="body2" color="text.secondary">{description}</Typography>
    </Stack>
  );
}

export type AgentOptionsDialogProps = {
  open: boolean;
  onClose: () => void;
  config: WorkflowConfig;
  providers: string[];
  providerLabels: Record<string, string>;
  modelOptions: string[];
  modelLabels: Record<string, string>;
  modelDetails?: string | null;
  reasoningOptions: string[];
  providerKind: string;
  codexAuthStatus?: ComponentArgs['codexAuthStatus'];
  statusMessage?: ComponentArgs['statusMessage'];
  update: (patch: Partial<WorkflowConfig>) => void;
  emit: (event: AppEvent) => void | Promise<unknown>;
};

export function AgentOptionsDialog({
  open,
  onClose,
  config,
  providers,
  providerLabels,
  modelOptions,
  modelLabels,
  modelDetails,
  reasoningOptions,
  providerKind,
  codexAuthStatus,
  statusMessage,
  update,
  emit,
}: AgentOptionsDialogProps) {
  const [section, setSection] = useState<SectionId>('model');
  const [saveRequested, setSaveRequested] = useState(false);
  const [saving, setSaving] = useState(false);
  const preSaveMessageRef = useRef<string | undefined>(undefined);

  // Keep the provider API key env var aligned with the selected provider. The dialog is
  // always mounted (open only toggles visibility), so this sync survives dialog closes.
  useEffect(() => {
    if ((config.provider === 'google' || config.provider === 'google_gemini') && config.provider_api_key_env !== 'GOOGLE_API_KEY') {
      update({ provider_api_key_env: 'GOOGLE_API_KEY' });
    } else if (config.provider === 'anthropic' && config.provider_api_key_env !== 'ANTHROPIC_API_KEY') {
      update({ provider_api_key_env: 'ANTHROPIC_API_KEY' });
    } else if (config.provider === 'openai' && (config.provider_api_key_env === 'GOOGLE_API_KEY' || config.provider_api_key_env === 'ANTHROPIC_API_KEY' || config.provider_api_key_env === 'GEMINI_API_KEY' || config.provider_api_key_env === 'OPENAI_CODEX_SUBSCRIPTION')) {
      update({ provider_api_key_env: 'OPENAI_API_KEY' });
    } else if (config.provider === 'openai_codex' && config.provider_api_key_env !== 'OPENAI_CODEX_SUBSCRIPTION') {
      update({ provider_api_key_env: 'OPENAI_CODEX_SUBSCRIPTION' });
    }
  }, [config.provider]);

  const updateAdvanced = (patch: Partial<AdvancedConfig>) => update({ advanced: { ...config.advanced, ...patch } });
  const updatePolicy = (patch: Partial<AutoAcceptPolicy>) => update({ auto_accept_policy: { ...config.auto_accept_policy, ...patch } });
  const prov = config.provenance ?? ({ enabled: false } as ProvenanceConfig);
  const updateProv = (patch: Partial<ProvenanceConfig>) => update({ provenance: { ...prov, ...patch } });

  const handleSave = async () => {
    // Show feedback only after the backend round-trip so a stale, unrelated status
    // message is never presented as save confirmation.
    setSaveRequested(false);
    setSaving(true);
    preSaveMessageRef.current = statusMessage?.text;
    try {
      await Promise.resolve(emit({ type: 'save_user_settings' }));
    } finally {
      setSaving(false);
      setSaveRequested(true);
    }
  };

  const handleClose = () => {
    setSaveRequested(false);
    onClose();
  };

  const sectionIndex = SECTIONS.findIndex((item) => item.id === section);

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      fullWidth
      maxWidth="md"
      keepMounted
      aria-labelledby="agent-options-title"
      PaperProps={{ sx: { height: { sm: 'min(720px, calc(100% - 64px))' } } }}
    >
      <DialogTitle id="agent-options-title" sx={{ py: 1.5 }}>
        <Stack direction="row" spacing={1} alignItems="center">
          <SettingsOutlinedIcon color="primary" />
          <Stack sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="subtitle1" component="span">Agent Service Options</Typography>
            <Typography variant="caption" color="text.secondary">Provider, connection, features, tracing and metadata for agent-based reconciliation.</Typography>
          </Stack>
          <IconButton aria-label="Close options" onClick={handleClose}><CloseIcon /></IconButton>
        </Stack>
      </DialogTitle>
      <DialogContent dividers sx={{ p: 0, display: 'flex', flexDirection: 'column' }}>
        <Tabs
          value={sectionIndex < 0 ? 0 : sectionIndex}
          onChange={(_e, value) => setSection(SECTIONS[value].id)}
          variant="scrollable"
          allowScrollButtonsMobile
          sx={{ display: { xs: 'flex', sm: 'none' }, borderBottom: '1px solid', borderColor: 'divider', flexShrink: 0 }}
        >
          {SECTIONS.map((item) => <Tab key={item.id} label={item.label} />)}
        </Tabs>
        <Box sx={{ display: 'flex', flex: 1, minHeight: 0 }}>
          <Box component="nav" aria-label="Option categories" sx={{ width: 224, flexShrink: 0, borderRight: '1px solid', borderColor: 'divider', display: { xs: 'none', sm: 'block' }, overflowY: 'auto', py: 1 }}>
            <List dense>
              {SECTIONS.map((item) => (
                <ListItemButton key={item.id} selected={section === item.id} onClick={() => setSection(item.id)} sx={{ mx: 1, borderRadius: 1 }}>
                  <ListItemIcon sx={{ minWidth: 34, color: section === item.id ? 'primary.main' : 'text.secondary' }}>{item.icon}</ListItemIcon>
                  <ListItemText primary={item.label} primaryTypographyProps={{ variant: 'body2', fontWeight: section === item.id ? 700 : 500 }} />
                </ListItemButton>
              ))}
            </List>
          </Box>
          <Box sx={{ flex: 1, minWidth: 0, overflowY: 'auto', p: { xs: 2, sm: 2.5 } }}>
            {section === 'model' && (
              <Stack spacing={2}>
                <SectionHeading title="Provider & Model" description="Choose the LLM provider, the model used for reconciliation, and the reasoning effort." />
                <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5 }}>
                  <FormControl fullWidth size="small"><InputLabel>LLM Provider</InputLabel><Select label="LLM Provider" value={config.provider} onChange={(e) => update({ provider: String(e.target.value) })}>{providers.map((p) => <MenuItem key={p} value={p}>{providerLabels[p] ?? p}</MenuItem>)}</Select></FormControl>
                  <FormControl fullWidth size="small"><InputLabel>Model</InputLabel><Select label="Model" value={config.model} onChange={(e) => update({ model: String(e.target.value) })}>{modelOptions.map((m) => <MenuItem key={m} value={m}>{modelLabels[m] ?? m}</MenuItem>)}</Select></FormControl>
                  <FormControl fullWidth size="small"><InputLabel>Reasoning</InputLabel><Select label="Reasoning" value={config.reasoning_effort} onChange={(e) => update({ reasoning_effort: String(e.target.value) })}>{reasoningOptions.map((r) => <MenuItem key={r} value={r}>{r}</MenuItem>)}</Select></FormControl>
                </Box>
                {modelDetails && <Alert severity="info" variant="outlined">{modelDetails}</Alert>}
                <Divider />
                <Typography variant="subtitle2" component="h4">Model catalog</Typography>
                <TextField fullWidth size="small" label="Custom Model Override" value={config.custom_model_override} onChange={(e) => update({ custom_model_override: e.target.value })} helperText="Optional: force a model id that is not listed in the catalog." />
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
                  {providerKind === 'openai_compatible' && <Button variant="outlined" startIcon={<SaveIcon />} onClick={() => emit({ type: 'register_local_model' })}>Register model</Button>}
                  <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => emit({ type: 'reload_models' })}>Reload models & pricing</Button>
                </Stack>
              </Stack>
            )}
            {section === 'connection' && (
              <Stack spacing={2}>
                <SectionHeading title="Connection & Keys" description="How the app authenticates against the selected provider. Secrets stay in environment variables or the OS keyring — they are never written to config.yaml." />
                {providerKind === 'openai_compatible' ? (
                  <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5 }}>
                    <TextField size="small" label="OpenAI-compatible base URL" value={config.openai_compatible_base_url} onChange={(e) => update({ openai_compatible_base_url: e.target.value })} />
                    <TextField size="small" label="OpenAI-compatible API key" type="password" value={config.openai_compatible_api_key} onChange={(e) => update({ openai_compatible_api_key: e.target.value })} />
                  </Box>
                ) : providerKind === 'codex' ? (
                  <Paper variant="outlined" sx={{ p: 2, borderRadius: 3 }}>
                    <Typography variant="subtitle2" component="h4" sx={{ mb: 1 }}>ChatGPT Subscription Auth</Typography>
                    {codexAuthStatus?.authenticated ? (
                      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ sm: 'center' }}>
                        <Alert severity="success" sx={{ flexGrow: 1, py: 0 }} role="status">Connected</Alert>
                        <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => emit({ type: 'codex_auth_refresh' })}>Refresh</Button>
                        <Button variant="outlined" color="error" startIcon={<LogoutIcon />} onClick={() => emit({ type: 'codex_auth_signout' })}>Sign out</Button>
                      </Stack>
                    ) : (
                      <Stack spacing={1}>
                        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
                          <Button variant="contained" startIcon={<LoginIcon />} onClick={() => emit({ type: 'codex_auth_signin' })}>Sign in with ChatGPT</Button>
                          <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => emit({ type: 'codex_auth_refresh_pending' })}>I completed login</Button>
                        </Stack>
                        {codexAuthStatus?.pending_auth_url && (
                          <Alert severity="info">
                            Please complete sign in: <OpenInNewIcon fontSize="inherit" sx={{ verticalAlign: 'text-bottom' }} /> <a href={codexAuthStatus.pending_auth_url} target="_blank" rel="noreferrer">Login Link</a>
                          </Alert>
                        )}
                      </Stack>
                    )}
                  </Paper>
                ) : (
                  <TextField size="small" label="Provider API key env var" value={config.provider_api_key_env} onChange={(e) => update({ provider_api_key_env: e.target.value })} InputProps={{ startAdornment: <InputAdornment position="start">ENV</InputAdornment> }} helperText="Name of the environment variable that holds the API key for the selected provider." />
                )}
              </Stack>
            )}
            {section === 'matching' && (
              <Stack spacing={2}>
                <SectionHeading title="Matching & Review" description="Mapping predicates and automatic acceptance of high-confidence suggestions." />
                <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1.5 }}>
                  <ToggleCard checked={config.skos_matching} title="SKOS matching" description="Generate SKOS predicates for mappings." onChange={(v) => update({ skos_matching: v })} />
                  <ToggleCard checked={config.auto_accept} title="Auto-accept" description="Accept high-confidence mappings by policy." onChange={(v) => update({ auto_accept: v })} />
                </Box>
                <Collapse in={config.auto_accept}>
                  <Stack spacing={1.5}>
                    <Divider><Typography variant="caption" color="text.secondary">Auto-accept policy</Typography></Divider>
                    <TextField size="small" type="number" label="Minimum confidence" inputProps={{ min: 0, max: 1, step: 0.01 }} value={config.auto_accept_policy.min_confidence} onChange={(e) => updatePolicy({ min_confidence: asNumber(e.target.value, 0.8) })} />
                    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1 }}>
                      <FormControlLabel control={<Checkbox checked={config.auto_accept_policy.require_exact_match} onChange={(e) => updatePolicy({ require_exact_match: e.target.checked })} />} label="Require exact match" />
                      <FormControlLabel control={<Checkbox checked={config.auto_accept_policy.require_llm_decision} onChange={(e) => updatePolicy({ require_llm_decision: e.target.checked })} />} label="Require LLM decision" />
                      <FormControlLabel control={<Checkbox checked={config.auto_accept_policy.require_no_fallback} onChange={(e) => updatePolicy({ require_no_fallback: e.target.checked })} />} label="Require no fallback" />
                      <FormControlLabel control={<Checkbox checked={config.auto_accept_policy.trusted_ontologies_only} onChange={(e) => updatePolicy({ trusted_ontologies_only: e.target.checked })} />} label="Trusted ontologies only" />
                    </Box>
                  </Stack>
                </Collapse>
              </Stack>
            )}
            {section === 'observability' && (
              <Stack spacing={2}>
                <SectionHeading title="Monitoring & Tracing" description="Live run monitoring via LangSmith and per-term execution trace artifacts on disk." />
                <ToggleCard checked={config.langsmith} title="Agent Monitoring" description="Show run progress, traces, and model activity in one panel." onChange={(v) => update({ langsmith: v })} />
                <Collapse in={config.langsmith}>
                  <TextField fullWidth size="small" label="LangSmith project" value={config.langsmith_project} onChange={(e) => update({ langsmith_project: e.target.value })} helperText="Requires the LANGSMITH_API_KEY environment variable on the backend." />
                </Collapse>
                <Divider><Typography variant="caption" color="text.secondary">Execution tracing</Typography></Divider>
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} alignItems={{ sm: 'center' }} flexWrap="wrap" useFlexGap>
                  <FormControl size="small" sx={{ minWidth: 180 }}><InputLabel>Trace level</InputLabel><Select label="Trace level" value={config.trace_level || 'summary'} onChange={(e) => update({ trace_level: normalizeTraceLevel(e.target.value) })}><MenuItem value="summary">summary</MenuItem><MenuItem value="detailed">detailed</MenuItem><MenuItem value="forensic">forensic</MenuItem></Select></FormControl>
                  <FormControlLabel control={<Switch checked={Boolean(config.trace_llm_prompts)} onChange={(e) => update({ trace_llm_prompts: e.target.checked })} />} label="LLM prompts" />
                  <FormControlLabel control={<Switch checked={Boolean(config.trace_raw_candidates)} onChange={(e) => update({ trace_raw_candidates: e.target.checked })} />} label="Raw candidates" />
                  <FormControlLabel control={<Switch checked={config.trace_discarded_candidates !== false} onChange={(e) => update({ trace_discarded_candidates: e.target.checked })} />} label="Discarded candidates" />
                  <FormControlLabel control={<Switch checked={Boolean(config.trace_api_payloads)} onChange={(e) => update({ trace_api_payloads: e.target.checked })} />} label="API payloads" />
                </Stack>
                <TextField
                  fullWidth
                  size="small"
                  label="Trace output directory"
                  value={config.trace_output_dir || ''}
                  onChange={(e) => update({ trace_output_dir: e.target.value })}
                  helperText="Relative paths resolve from the repo root, so 'logs' writes to <repo>/logs. Each run writes its artifacts into a new timestamped subfolder here, e.g. log_20260703_144703. Press Save settings to keep a changed path."
                />
              </Stack>
            )}
            {section === 'advanced' && (
              <Stack spacing={2}>
                <SectionHeading title="Advanced" description="Expert settings: execution limits, review policy and agentic refinement controls." />
                <FormControlLabel
                  control={<Switch checked={config.expert_mode} onChange={(e) => update({ expert_mode: e.target.checked })} />}
                  label={
                    <Stack>
                      <Typography variant="body2" sx={{ fontWeight: 700 }}>Expert mode</Typography>
                      <Typography variant="caption" color="text.secondary">Expose planner, budgets and limits.</Typography>
                    </Stack>
                  }
                />
                <Collapse in={config.expert_mode}>
                  <Stack spacing={2}>
                    <Divider><Typography variant="caption" color="text.secondary">Candidate review policy</Typography></Divider>
                    <FormControl fullWidth size="small"><InputLabel>Candidate review policy</InputLabel><Select label="Candidate review policy" value={config.candidate_review_mode} onChange={(e) => update({ candidate_review_mode: normalizeCandidateReviewMode(e.target.value) })}><MenuItem value="conservative">Conservative</MenuItem><MenuItem value="exploratory">Exploratory</MenuItem></Select></FormControl>
                    <Typography variant="caption" color="text.secondary"><strong>Conservative</strong>: automatically accepts only strong candidates but still shows plausible exact/close matches for review. <strong>Exploratory</strong>: also shows weaker close or related candidates for manual review. Useful for sparse ontologies or uncommon terms.</Typography>
                    <Divider><Typography variant="caption" color="text.secondary">Execution limits</Typography></Divider>
                    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5 }}>
                      <TextField size="small" type="number" label="Timeout (seconds)" value={config.advanced.timeout_s} onChange={(e) => updateAdvanced({ timeout_s: asNumber(e.target.value, 180) })} />
                      <TextField size="small" type="number" label="Iterations" value={config.advanced.max_iterations} onChange={(e) => updateAdvanced({ max_iterations: asNumber(e.target.value, 10) })} />
                      <TextField size="small" type="number" label="Batch size" value={config.advanced.batch_size} onChange={(e) => updateAdvanced({ batch_size: asNumber(e.target.value, 10) })} />
                      <TextField size="small" type="number" label="Workers" value={config.advanced.max_workers} onChange={(e) => updateAdvanced({ max_workers: asNumber(e.target.value, 4) })} />
                      <TextField size="small" type="number" label="LLM call budget" value={config.advanced.agentic_total_llm_call_budget} onChange={(e) => updateAdvanced({ agentic_total_llm_call_budget: asNumber(e.target.value, 14) })} />
                    </Box>
                    <Divider><Typography variant="caption" color="text.secondary">Refinement</Typography></Divider>
                    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1 }}>
                      <FormControlLabel control={<Switch checked={Boolean(config.allow_heuristic_fallback)} onChange={(e) => update({ allow_heuristic_fallback: e.target.checked })} />} label="Allow heuristic fallbacks" />
                      <FormControlLabel control={<Switch checked={Boolean(config.enable_candidate_adjudication)} onChange={(e) => update({ enable_candidate_adjudication: e.target.checked })} />} label="Final candidate adjudication" />
                      <FormControlLabel control={<Switch checked={Boolean(config.use_different_models)} onChange={(e) => update({ use_different_models: e.target.checked })} />} label="Different definition model" />
                    </Box>
                  </Stack>
                </Collapse>
              </Stack>
            )}
            {/* Registry stays mounted across section switches and dialog close (keepMounted)
                so a running registry job keeps polling and its progress is not lost. */}
            <Box sx={{ display: section === 'registry' ? 'block' : 'none' }}>
              <Stack spacing={2}>
                <SectionHeading title="BioPortal Registry" description="Maintain the local BioPortal ontology registry used for ontology routing and scoring." />
                <RegistryAdminPanel embedded />
              </Stack>
            </Box>
            {section === 'provenance' && (
              <Stack spacing={2}>
                <SectionHeading title="Provenance Metadata" description="Curation metadata stamped onto SSSOM exports. Saved defaults are written to config.yaml." />
                <FormControlLabel control={<Switch checked={Boolean(prov.enabled)} onChange={(e) => updateProv({ enabled: e.target.checked })} />} label="Include provenance metadata" />
                <Collapse in={Boolean(prov.enabled)}>
                  <Stack spacing={1.5}>
                    <Alert severity="info" variant="outlined">Mapping Date is generated automatically when the workflow runs.</Alert>
                    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 1.5 }}>
                      <TextField size="small" label="Author ORCID" value={prov.author_id} onChange={(e) => updateProv({ author_id: e.target.value })} />
                      <TextField size="small" label="Author Name" value={prov.author_label} onChange={(e) => updateProv({ author_label: e.target.value })} />
                      <TextField size="small" label="Reviewer ORCID" value={prov.reviewer_id} onChange={(e) => updateProv({ reviewer_id: e.target.value })} />
                      <TextField size="small" label="Reviewer Name" value={prov.reviewer_label} onChange={(e) => updateProv({ reviewer_label: e.target.value })} />
                      <TextField size="small" label="Creator ORCID" value={prov.creator_id} onChange={(e) => updateProv({ creator_id: e.target.value })} />
                      <TextField size="small" label="Creator Name" value={prov.creator_label} onChange={(e) => updateProv({ creator_label: e.target.value })} />
                      <TextField size="small" label="Mapping Tool" value={prov.mapping_tool} onChange={(e) => updateProv({ mapping_tool: e.target.value })} />
                      <TextField size="small" label="Tool Version" value={prov.mapping_tool_version} onChange={(e) => updateProv({ mapping_tool_version: e.target.value })} />
                      <TextField size="small" label="Publication Date" value={prov.publication_date} onChange={(e) => updateProv({ publication_date: e.target.value })} />
                    </Box>
                  </Stack>
                </Collapse>
              </Stack>
            )}
          </Box>
        </Box>
      </DialogContent>
      {saveRequested && statusMessage?.text && (statusMessage.text !== preSaveMessageRef.current || /saved/i.test(statusMessage.text)) && (
        <Alert
          severity={statusMessage.severity ?? 'info'}
          onClose={() => setSaveRequested(false)}
          sx={{ mx: 2, mt: 1.5, mb: 0 }}
          role={statusMessage.severity === 'error' ? 'alert' : 'status'}
        >
          {statusMessage.text}
        </Alert>
      )}
      <DialogActions sx={{ px: 2.5, py: 1.5, gap: 1, flexWrap: 'wrap' }}>
        <Typography variant="caption" color="text.secondary" sx={{ flex: 1, minWidth: 220 }}>
          Changes apply to the current session immediately. Save settings keeps them as defaults for the next app start.
        </Typography>
        <Button onClick={handleClose}>Close</Button>
        <Button variant="contained" startIcon={<SaveIcon />} onClick={handleSave} disabled={saving}>{saving ? 'Saving…' : 'Save settings'}</Button>
      </DialogActions>
    </Dialog>
  );
}
