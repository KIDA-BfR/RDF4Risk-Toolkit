import React, { useEffect, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
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

type Mode = 'catalog' | 'full_context';
const TERMINAL = new Set(['completed', 'failed', 'cancelled']);

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

  const trigger = (
    <Stack direction={{ xs: 'column', sm: 'row' }} justifyContent="space-between" alignItems={{ xs: 'stretch', sm: 'center' }} spacing={1}>
      <Stack>
        <Typography variant="subtitle2">BioPortal ontology registry</Typography>
        <Typography variant="caption" color="text.secondary">
          Pre-fill local ontology context used for routing. Heavy operation — explicit confirmation required.
        </Typography>
      </Stack>
      <Button size="small" variant="outlined" startIcon={<StorageIcon />} onClick={openDialog}>
        Update BioPortal Ontology Registry
      </Button>
    </Stack>
  );

  const dialog = (
      <Dialog open={open} onClose={closeDialog} maxWidth="sm" fullWidth>
        <DialogTitle>
          <Stack direction="row" spacing={1} alignItems="center">
            <WarningAmberIcon color="warning" />
            <span>Update BioPortal Ontology Registry</span>
          </Stack>
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={1.5}>
            <Alert severity="warning">
              Two distinct modes: <strong>Catalog refresh (recommended)</strong> only discovers the list of
              BioPortal ontologies and stubs missing entries — fast, one listing request. <strong>Deep-fetch
              (advanced)</strong> fetches full quality/context metadata per ontology and can issue <strong>thousands
              of BioPortal API requests</strong> across the whole BioPortal universe, taking a long time. Both
              require a BioPortal API key on the backend, should not be run repeatedly, and preserve existing
              curated notes. You can cancel a running job at any time.
            </Alert>

            {previewError && <Alert severity="error">{previewError}</Alert>}
            {preview && !preview.error && (
              <Paper variant="outlined" sx={{ p: 1.25, borderRadius: 2, bgcolor: 'background.default' }}>
                <Typography variant="body2">
                  Discovered on BioPortal: <strong>{preview.total_discovered}</strong> ontologies.
                </Typography>
                <Typography variant="body2">
                  Already in local registry: <strong>{preview.already_in_registry}</strong>.
                </Typography>
                <Typography variant="body2">
                  Planned for fetch/update: <strong>{preview.planned_for_fetch}</strong>.
                </Typography>
              </Paper>
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
                Deep-fetch performs multiple API calls <strong>per ontology</strong> across the entire BioPortal
                universe{preview?.total_discovered ? ` (~${preview.total_discovered} ontologies discovered)` : ''} —
                potentially <strong>thousands of BioPortal API requests</strong> and a very long run. Strongly
                consider setting a <strong>max-requests</strong> cap. Catalog
                refresh is the recommended default for routine updates.
              </Alert>
            )}

            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1.2 }}>
              <FormControlLabel
                control={<Checkbox checked={refresh} onChange={(e) => setRefresh(e.target.checked)} disabled={running} />}
                label="Force refresh (re-fetch even fresh entries; overwrites fetched metadata — curated notes are kept)"
              />
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

            {job && (
              <Paper variant="outlined" sx={{ p: 1.25, borderRadius: 2 }} role="status">
                <Divider sx={{ mb: 1 }} />
                <Typography variant="body2" gutterBottom>
                  Status: <strong>{job.status}</strong> · mode {job.mode}
                  {job.dry_run ? ' · dry-run' : ''}
                </Typography>
                <LinearProgress
                  variant={running && job.total === 0 ? 'indeterminate' : 'determinate'}
                  value={Math.max(0, Math.min(100, job.percent || 0))}
                  sx={{ height: 8, borderRadius: 1, my: 1 }}
                />
                <Typography variant="caption" color="text.secondary">
                  {job.processed}/{job.total} ({(job.percent || 0).toFixed(1)}%)
                  {job.current_acronym ? ` · current: ${job.current_acronym}` : ''}
                  {' · '}fetched {job.fetched} · skipped {job.skipped} · failed {job.failed}
                </Typography>
                {job.error && <Alert severity="error" sx={{ mt: 1 }}>{job.error}</Alert>}
              </Paper>
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
            Proceed
          </Button>
        </DialogActions>
      </Dialog>
  );

  if (embedded) {
    return <>{trigger}{dialog}</>;
  }
  return (
    <Paper variant="outlined" sx={{ p: 1.5, borderRadius: 3 }}>
      {trigger}
      {dialog}
    </Paper>
  );
}
