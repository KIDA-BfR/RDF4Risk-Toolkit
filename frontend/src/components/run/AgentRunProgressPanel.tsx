import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  LinearProgress,
  Stack,
  Typography,
} from '@mui/material';
import StopCircleIcon from '@mui/icons-material/StopCircle';
import { AnimatePresence, motion } from 'motion/react';

const agentLoaderMp4 = new URL('../../assets/animations/agent-loader.mp4', import.meta.url).href;

export type AgentRunWorkflow = 'wikidata_deep_agent' | 'bioportal_wikidata';

export type RunStage =
  | 'validating_input'
  | 'retrieving_candidates'
  | 'extracting_metadata'
  | 'ranking_candidates'
  | 'selecting_match_type'
  | 'preparing_review'
  | 'writing_output';

export type RunStatus = {
  ready?: boolean;
  running?: boolean;
  stopped?: boolean;
  finished?: boolean;
  error?: string | null;
  progress?: number | null;
  stage?: RunStage | string | null;
  message?: string | null;
  messages?: string[];
  current_term?: string | null;
  processed_count?: number | null;
  total_count?: number | null;
  started_at?: string | null;
  elapsed_seconds?: number | null;
  estimated_remaining_seconds?: number | null;
  last_activity?: string | null;
  stop_requested?: boolean;
  stop_reason?: string | null;
  stop_event?: Record<string, unknown>;
  can_resume?: boolean;
  can_restart?: boolean;
};

export type AgentRunProgressPanelProps = {
  runStatus: RunStatus;
  workflow?: AgentRunWorkflow;
  optimisticTotalCount?: number | null;
  onStop?: () => void;
};

const stageLabels: Record<string, string> = {
  validating_input: 'Validating input',
  retrieving_candidates: 'Retrieving candidates',
  extracting_metadata: 'Extracting metadata',
  ranking_candidates: 'Ranking semantic matches',
  selecting_match_type: 'Selecting SKOS match type',
  preparing_review: 'Preparing review output',
  writing_output: 'Writing output',
};

const baseMessages = [
  'Validating SSSOM-compatible table columns',
  'Normalizing source labels and identifiers',
  'Comparing candidate labels and aliases',
  'Checking semantic context from definitions',
  'Ranking candidates by confidence and evidence',
  'Selecting suitable SKOS match types',
  'Checking exactMatch versus closeMatch evidence',
  'Filtering weak or ambiguous candidates',
  'Preparing reviewable mapping suggestions',
  'Writing provenance-aware mapping metadata',
  'Preparing SSSOM-compatible output rows',
];

const bioportalMessages = [
  'Searching BioPortal for ontology candidates',
  'Retrieving labels, synonyms, and definitions',
  'Checking domain-specific ontology evidence',
  'Querying Wikidata fallback candidates',
  'Comparing ontology and Wikidata candidates',
  'Ranking semantic matches',
];

const wikidataMessages = [
  'Searching Wikidata for candidate entities',
  'Retrieving labels, aliases, and descriptions',
  'Checking entity context and semantic type',
  'Comparing candidates against source terms',
  'Ranking semantic matches',
];

function toFiniteNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—';
  const rounded = Math.floor(seconds);
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const secs = rounded % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
  }
  return `${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
}

function formatRemaining(seconds: number | null | undefined): string | null {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return null;
  if (seconds < 90) return `~${Math.max(1, Math.round(seconds))} sec`;
  const minutes = Math.max(1, Math.round(seconds / 60));
  return `~${minutes} min`;
}

function elapsedFromStartedAt(startedAt?: string | null): number | null {
  if (!startedAt) return null;
  const timestamp = Date.parse(startedAt);
  if (!Number.isFinite(timestamp)) return null;
  return Math.max(0, (Date.now() - timestamp) / 1000);
}

export function AgentRunProgressPanel({
  runStatus,
  workflow = 'bioportal_wikidata',
  optimisticTotalCount = null,
  onStop,
}: AgentRunProgressPanelProps) {
  const workflowMessages = useMemo(() => {
    return workflow === 'wikidata_deep_agent'
      ? [...baseMessages.slice(0, 2), ...wikidataMessages, ...baseMessages.slice(2)]
      : [...baseMessages.slice(0, 2), ...bioportalMessages, ...baseMessages.slice(2)];
  }, [workflow]);
  const activityMessages = baseMessages;
  const [messageIndex, setMessageIndex] = useState(0);
  const [clockTick, setClockTick] = useState(0);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setMessageIndex((idx) => (idx + 1) % activityMessages.length);
    }, 3000);
    return () => window.clearInterval(timer);
  }, [activityMessages.length]);

  useEffect(() => {
    const timer = window.setInterval(() => setClockTick((tick) => tick + 1), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    setMessageIndex(0);
  }, [workflow]);

  const processedCount = toFiniteNumber(runStatus.processed_count);
  const reportedTotalCount = toFiniteNumber(runStatus.total_count);
  const optimisticTotal = toFiniteNumber(optimisticTotalCount);
  const totalCount = reportedTotalCount ?? optimisticTotal;
  const hasRealProgress = processedCount != null && totalCount != null && totalCount > 0;
  const hasReportedProgress = processedCount != null && reportedTotalCount != null && reportedTotalCount > 0;
  const progress = hasRealProgress
    ? Math.min(100, Math.round((Math.max(0, processedCount) / totalCount) * 100))
    : null;
  const activityQueuePosition = totalCount && totalCount > 0 ? (messageIndex % totalCount) + 1 : null;

  const elapsedSeconds = useMemo(() => {
    const backendElapsed = toFiniteNumber(runStatus.elapsed_seconds);
    return backendElapsed ?? elapsedFromStartedAt(runStatus.started_at) ?? clockTick;
  }, [clockTick, runStatus.elapsed_seconds, runStatus.started_at]);

  const estimatedRemainingSeconds = useMemo(() => {
    const backendEstimate = toFiniteNumber(runStatus.estimated_remaining_seconds);
    if (backendEstimate != null) return backendEstimate;
    if (!hasRealProgress || !processedCount || processedCount <= 0 || !elapsedSeconds || elapsedSeconds <= 0 || totalCount == null) {
      return null;
    }
    return (elapsedSeconds / processedCount) * Math.max(0, totalCount - processedCount);
  }, [elapsedSeconds, hasRealProgress, processedCount, runStatus.estimated_remaining_seconds, totalCount]);

  const stage = String(runStatus.stage || '').trim();
  const phaseLabel = stageLabels[stage] || (stage ? stage.replace(/_/g, ' ') : 'Agent is running');
  const visibleMessage = workflowMessages[messageIndex % workflowMessages.length];
  const remainingLabel = hasRealProgress ? formatRemaining(estimatedRemainingSeconds) : null;

  return (
    <Card
      variant="outlined"
      sx={{
        borderRadius: 5,
        borderColor: '#FBFBFD',
        overflow: 'hidden',
        background: '#FBFBFD',
        boxShadow: '0 18px 60px rgba(15,23,42,.10)',
      }}
    >
      <CardContent sx={{ p: { xs: 3, md: 4 } }}>
        <Stack spacing={2.5} alignItems="center">
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems="center" justifyContent="center" sx={{ width: '100%' }}>
            <Chip label={runStatus.stop_requested ? 'Stopping run' : 'Run in progress'} color={runStatus.stop_requested ? 'warning' : 'primary'} variant="outlined" sx={{ fontWeight: 800 }} />
            {onStop && (
              <Button
                variant="contained"
                color="error"
                size="large"
                startIcon={<StopCircleIcon />}
                onClick={onStop}
                disabled={Boolean(runStatus.stop_requested)}
                sx={{ fontWeight: 900, px: 3, boxShadow: '0 10px 24px rgba(220,38,38,.22)' }}
              >
                {runStatus.stop_requested ? 'Stopping...' : 'Stop Run'}
              </Button>
            )}
          </Stack>

          <Box
            component="video"
            autoPlay
            loop
            muted
            playsInline
            preload="auto"
            sx={{
              width: { xs: 220, md: 280 },
              height: { xs: 220, md: 280 },
              objectFit: 'contain',
              display: 'block',
              mx: 'auto',
            }}
          >
            <source src={agentLoaderMp4} type="video/mp4" />
          </Box>

          <Box sx={{ textAlign: 'center', width: '100%' }}>
            <Typography variant="h5" fontWeight={900}>
              Reconciling semantic mappings
            </Typography>
            <AnimatePresence mode="wait">
              <motion.div
                key={visibleMessage}
                initial={{ opacity: 0, y: 0 }}
                animate={{
                  opacity: [0, 1, 1, 0],
                  y: [0, 0, 0, 0],
                }}
                exit={{ opacity: 0 }}
                transition={{
                  duration: 3,
                  times: [0, 0.3, 0.7, 1],
                  ease: 'easeInOut',
                }}
              >
                <Typography
                  variant="h6"
                  sx={{
                    mt: 1.25,
                    minHeight: 40,
                    color: 'primary.main',
                    fontWeight: 850,
                    letterSpacing: '-0.01em',
                    textAlign: 'center',
                  }}
                >
                  {visibleMessage}
                </Typography>
              </motion.div>
            </AnimatePresence>
          </Box>

          {runStatus.current_term && (
            <Alert severity="info" variant="outlined" sx={{ width: '100%', borderRadius: 3, bgcolor: 'rgba(255,255,255,.72)' }}>
              <strong>Current term:</strong> {runStatus.current_term}
            </Alert>
          )}

          <Box sx={{ width: '100%', display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(3, 1fr)' }, gap: 1.2 }}>
            <Box sx={{ p: 1.5, borderRadius: 3, bgcolor: 'rgba(248,250,252,.86)', border: '1px solid', borderColor: 'divider' }}>
              <Typography variant="caption" color="text.secondary">Current phase</Typography>
              <Typography variant="body2" sx={{ fontWeight: 850 }}>{phaseLabel}</Typography>
            </Box>
            <Box sx={{ p: 1.5, borderRadius: 3, bgcolor: 'rgba(248,250,252,.86)', border: '1px solid', borderColor: 'divider' }}>
              <Typography variant="caption" color="text.secondary">Elapsed</Typography>
              <Typography variant="body2" sx={{ fontWeight: 850 }}>{formatDuration(elapsedSeconds)}</Typography>
            </Box>
            <Box sx={{ p: 1.5, borderRadius: 3, bgcolor: 'rgba(248,250,252,.86)', border: '1px solid', borderColor: 'divider' }}>
              <Typography variant="caption" color="text.secondary">Estimated remaining</Typography>
              <Typography variant="body2" sx={{ fontWeight: 850 }}>{remainingLabel || (hasRealProgress ? 'calculating…' : '—')}</Typography>
            </Box>
          </Box>

          <Box sx={{ width: '100%' }}>
            <Stack direction={{ xs: 'column', sm: 'row' }} justifyContent="space-between" alignItems={{ xs: 'flex-start', sm: 'baseline' }} sx={{ mb: 0.75 }} spacing={0.5}>
              <Typography variant="body2" sx={{ fontWeight: 850 }}>Progress</Typography>
              {hasRealProgress ? (
                <Typography variant="body2" color="text.secondary">
                  {hasReportedProgress
                    ? `${Math.max(0, processedCount ?? 0)} / ${totalCount} terms processed · ${progress}%`
                    : `Activity cycle ${activityQueuePosition} / ${totalCount} queued terms`}
                </Typography>
              ) : (
                <Typography variant="body2" color="text.secondary">Waiting for the first processed term.</Typography>
              )}
            </Stack>
            <LinearProgress
              variant={hasRealProgress ? 'determinate' : 'indeterminate'}
              value={hasRealProgress ? progress ?? 0 : undefined}
              sx={{ height: 10, borderRadius: 999 }}
            />
          </Box>

          {runStatus.last_activity && (
            <Typography variant="caption" color="text.secondary" sx={{ alignSelf: 'stretch' }}>
              Last activity: {runStatus.last_activity}
            </Typography>
          )}
        </Stack>
      </CardContent>
    </Card>
  );
}
