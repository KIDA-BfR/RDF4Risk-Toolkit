import React, { useEffect, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  InputLabel,
  LinearProgress,
  MenuItem,
  Paper,
  Select,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import StorageIcon from '@mui/icons-material/Storage';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import {
  cancelRegistryJob,
  fetchRegistryJobPreview,
  fetchRegistryJobStatus,
  startRegistryJob,
  type RegistryJobPreview,
  type RegistryJobStatus,
} from '../../standalone/backendClient';
import { InfoButton } from '../../components/help/InfoButton';
import { Disclosure } from '../../components/layout/Disclosure';
import { KpiStrip } from '../../components/layout/KpiStrip';
import { MetaList } from '../../components/layout/MetaList';

type Mode = 'catalog' | 'full_context';
const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
// Human labels for the job's protocol values.
const JOB_STATUS: Record<string, { label: string; color: 'default' | 'info' | 'success' | 'error' | 'warning' }> = {
  pending: { label: 'Queued', color: 'info' },
  running: { label: 'Running', color: 'info' },
  completed: { label: 'Completed', color: 'success' },
  failed: { label: 'Failed', color: 'error' },
  cancelled: { label: 'Cancelled', color: 'warning' },
};
const MODE_LABEL: Record<string, string> = { catalog: 'Catalog refresh', full_context: 'Deep-fetch' };
const countFormat = new Intl.NumberFormat();

export default function RegistryAdminPanel({ embedded = false }: { embedded?: boolean }): JSX.Element {
  const [open, setOpen] = useState(false);
  const [mode, setMode] = useState<Mode>('catalog');
  const [refresh, setRefresh] = useState(false);
  const [maxRequests, setMaxRequests] = useState<string>('');
  const [preview, setPreview] = useState<RegistryJobPreview | null>(null);
  const [previewError, setPreviewError] = useState<string>('');
  const [job, setJob] = useState<RegistryJobStatus>(null);
  const [error, setError] = useState<string>('');
  const [busy, setBusy] = useState(false);
  const pollRef = useRef<number | null>(null);

  const running = Boolean(job && !TERMINAL.has(job.status));

  const stopPolling = () => {
    if (pollRef.current !== null) {
      window.clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  useEffect(() => () => stopPolling(), []);

  const openDialog = async () => {
    setOpen(true);
    setError('');
    setPreview(null);
    setPreviewError('');
    try {
      const p = await fetchRegistryJobPreview();
      setPreview(p);
      if (p.error) setPreviewError(p.error === 'missing_bioportal_api_key' ? 'No BioPortal API key configured on the backend.' : p.error);
    } catch (e: any) {
      setPreviewError(e?.message || 'Could not contact the backend.');
    }
  };

  const beginPolling = (jobId: string) => {
    stopPolling();
    pollRef.current = window.setInterval(async () => {
      try {
        const status = await fetchRegistryJobStatus(jobId);
        setJob(status);
        if (status && TERMINAL.has(status.status)) stopPolling();
      } catch (e: any) {
        setError(e?.message || 'Lost contact with the backend.');
        stopPolling();
      }
    }, 1500);
  };

  const proceed = async () => {
    setBusy(true);
    setError('');
    try {
      const parsedMax = maxRequests.trim() === '' ? null : Math.max(1, Number(maxRequests) || 0) || null;
      const res = await startRegistryJob({
        mode,
        all_bioportal: true,
        confirmed: true,
        dry_run: false,
        refresh,
        max_requests: parsedMax,
      });
      if (res.error) {
        setError(res.error === 'missing_bioportal_api_key' ? 'No BioPortal API key configured on the backend.' : res.error);
      } else if (res.job_id) {
        setJob(res.status ?? null);
        beginPolling(res.job_id);
      }
    } catch (e: any) {
      setError(e?.message || 'Failed to start the registry job.');
    } finally {
      setBusy(false);
    }
  };

  const doCancel = async () => {
    if (!job?.job_id) return;
    try {
      await cancelRegistryJob(job.job_id);
    } catch (e: any) {
      setError(e?.message || 'Failed to cancel.');
    }
  };

  const closeDialog = () => {
    if (running) return; // don't close mid-run; user can cancel first
    setOpen(false);
    setJob(null);
    stopPolling();
  };

  const jobStatus = job ? JOB_STATUS[job.status] ?? { label: job.status, color: 'default' as const } : null;

  const trigger = (
    <Stack direction={{ xs: 'column', sm: 'row' }} justifyContent="space-between" alignItems={{ xs: 'stretch', sm: 'center' }} spacing={1}>
      <Typography variant="body2" color="text.secondary">
        A heavy operation that needs explicit confirmation.
      </Typography>
      <Button size="small" variant="outlined" startIcon={<StorageIcon />} onClick={openDialog} sx={{ flexShrink: 0 }}>
        Update registry…
      </Button>
    </Stack>
  );

  const dialog = (
      <Dialog open={open} onClose={closeDialog} maxWidth="sm" fullWidth aria-labelledby="registry-update-title">
        <DialogTitle id="registry-update-title">
          <Stack direction="row" spacing={1} alignItems="center">
            <WarningAmberIcon color="warning" />
            <span>Update the BioPortal registry</span>
          </Stack>
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Alert severity="warning">
              Needs a BioPortal API key on the backend and should not be run repeatedly. Curated notes are kept, and a running job can be cancelled.
            </Alert>
            <Disclosure title="About the two modes">
              <Stack spacing={1}>
                <Typography variant="body2"><strong>Catalog refresh (recommended)</strong> only discovers the list of BioPortal ontologies and stubs missing entries. Fast, one listing request.</Typography>
                <Typography variant="body2"><strong>Deep-fetch (advanced)</strong> fetches full quality and context metadata per ontology and can issue thousands of BioPortal API requests across the whole BioPortal universe, taking a long time.</Typography>
              </Stack>
            </Disclosure>

            {previewError && <Alert severity="error">{previewError}</Alert>}
            {preview && !preview.error && (
              <KpiStrip
                ariaLabel="Registry preview"
                items={[
                  { label: 'Discovered on BioPortal', value: countFormat.format(preview.total_discovered ?? 0) },
                  { label: 'Already in registry', value: countFormat.format(preview.already_in_registry ?? 0) },
                  { label: 'Planned for update', value: countFormat.format(preview.planned_for_fetch ?? 0) },
                ]}
              />
            )}

            <FormControl size="small" fullWidth disabled={running}>
              <InputLabel>Fetch depth</InputLabel>
              <Select label="Fetch depth" value={mode} onChange={(e) => setMode(e.target.value as Mode)}>
                <MenuItem value="catalog">Refresh BioPortal ontology catalog (recommended)</MenuItem>
                <MenuItem value="full_context">Deep-fetch full context for all ontologies (advanced, slow)</MenuItem>
              </Select>
            </FormControl>
            {mode === 'full_context' && (
              <Alert severity="error">
                Deep-fetch makes several API calls <strong>per ontology</strong>{preview?.total_discovered ? ` (~${countFormat.format(preview.total_discovered)} ontologies discovered)` : ''}, potentially thousands of requests and a very long run. Set a max-requests cap.
              </Alert>
            )}

            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1.5, alignItems: 'center' }}>
              <Stack direction="row" alignItems="center" spacing={0.5}>
                <FormControlLabel
                  control={<Checkbox checked={refresh} onChange={(e) => setRefresh(e.target.checked)} disabled={running} />}
                  label="Force refresh"
                  sx={{ mr: 0 }}
                />
                <InfoButton title="Force refresh">Re-fetch entries even when they are still fresh. Fetched metadata is overwritten; curated notes are kept.</InfoButton>
              </Stack>
              <TextField
                size="small"
                type="number"
                label="Max requests (optional cap)"
                value={maxRequests}
                onChange={(e) => setMaxRequests(e.target.value)}
                disabled={running}
              />
            </Box>

            {error && <Alert severity="error">{error}</Alert>}

            {job && jobStatus && (
              <Box role="status" sx={{ p: 1.5, border: 1, borderColor: 'divider', borderRadius: 2 }}>
                <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
                  <Chip size="small" variant="outlined" color={jobStatus.color} label={jobStatus.label} />
                  <Typography variant="body2" color="text.secondary">{MODE_LABEL[job.mode] ?? job.mode}{job.dry_run ? ' (dry run)' : ''}</Typography>
                </Stack>
                <LinearProgress
                  aria-label="Registry update progress"
                  variant={running && job.total === 0 ? 'indeterminate' : 'determinate'}
                  value={Math.max(0, Math.min(100, job.percent || 0))}
                  sx={{ my: 1.5 }}
                />
                <MetaList
                  columns={2}
                  items={[
                    { label: 'Processed', value: `${countFormat.format(job.processed ?? 0)} of ${countFormat.format(job.total ?? 0)} (${(job.percent || 0).toFixed(1)}%)`, numeric: true },
                    { label: 'Current', value: job.current_acronym, mono: true },
                    { label: 'Fetched', value: countFormat.format(job.fetched ?? 0), numeric: true },
                    { label: 'Skipped', value: countFormat.format(job.skipped ?? 0), numeric: true },
                    { label: 'Failed', value: countFormat.format(job.failed ?? 0), numeric: true },
                  ]}
                />
                {job.error && <Alert severity="error" sx={{ mt: 1.5 }}>{job.error}</Alert>}
              </Box>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          {running ? (
            <Button color="warning" onClick={doCancel}>Cancel run</Button>
          ) : (
            <Button onClick={closeDialog}>Close</Button>
          )}
          <Button variant="contained" onClick={proceed} disabled={busy || running || Boolean(preview?.error)}>
            Start update
          </Button>
        </DialogActions>
      </Dialog>
  );

  if (embedded) {
    return <>{trigger}{dialog}</>;
  }
  return (
    <Paper variant="outlined" sx={{ p: 1.5 }}>
      {trigger}
      {dialog}
    </Paper>
  );
}
