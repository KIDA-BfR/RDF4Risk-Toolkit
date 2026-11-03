import React, { useEffect, useMemo, useState } from 'react';
import {
  Box,
  Button,
  Chip,
  LinearProgress,
  Stack,
  Typography,
  useMediaQuery,
} from '@mui/material';
import StopIcon from '@mui/icons-material/Stop';
import { AgentActivityTimeline } from './AgentActivityTimeline';
import { KpiStrip } from '../layout/KpiStrip';
import { Section } from '../layout/Section';

const agentLoaderMp4 = new URL('../../assets/animations/agent-loader.mp4', import.meta.url).href;
const countFormat = new Intl.NumberFormat();

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

function toFiniteNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '';
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

// The live run view: headline figures in a KpiStrip at page level (never inside the Section), then
// the activity trace and progress in one flat Section with the Stop action in its header.
export function AgentRunProgressPanel({
  runStatus,
  optimisticTotalCount = null,
  onStop,
}: AgentRunProgressPanelProps) {
  const [clockTick, setClockTick] = useState(0);
  // The loader video is decorative: it stays on its first frame when the user prefers less motion.
  const reduceMotion = useMediaQuery('(prefers-reduced-motion: reduce)', { noSsr: true });

  useEffect(() => {
    const timer = window.setInterval(() => setClockTick((tick) => tick + 1), 1000);
    return () => window.clearInterval(timer);
  }, []);

  const processedCount = toFiniteNumber(runStatus.processed_count);
  const reportedTotalCount = toFiniteNumber(runStatus.total_count);
  const optimisticTotal = toFiniteNumber(optimisticTotalCount);
  const totalCount = reportedTotalCount ?? optimisticTotal;
  const hasRealProgress = processedCount != null && totalCount != null && totalCount > 0;
  const progress = hasRealProgress
    ? Math.min(100, Math.round((Math.max(0, processedCount) / totalCount) * 100))
    : null;

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
  const remainingLabel = hasRealProgress ? formatRemaining(estimatedRemainingSeconds) : null;
  const stopping = Boolean(runStatus.stop_requested);

  return (
    <>
      <KpiStrip
        ariaLabel="Run figures"
        items={[
          { label: 'Terms processed', value: hasRealProgress ? `${countFormat.format(Math.max(0, processedCount ?? 0))} of ${countFormat.format(totalCount ?? 0)}` : '', hint: progress != null ? `${progress}%` : undefined },
          { label: 'Elapsed', value: formatDuration(elapsedSeconds) },
          { label: 'Estimated remaining', value: remainingLabel || (hasRealProgress ? 'Calculating…' : '') },
        ]}
      />
      <Section
        title="Run in progress"
        description={`Current phase: ${phaseLabel}`}
        actions={
          <>
            {stopping && <Chip size="small" variant="outlined" color="warning" label="Stopping" />}
            {onStop && (
              <Button variant="outlined" color="warning" startIcon={<StopIcon />} onClick={onStop} disabled={stopping}>
                {stopping ? 'Stopping…' : 'Stop run'}
              </Button>
            )}
          </>
        }
      >
        <Stack spacing={2.5}>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems={{ xs: 'center', sm: 'flex-start' }}>
            <Box
              component="video"
              autoPlay={!reduceMotion}
              loop={!reduceMotion}
              muted
              playsInline
              preload="auto"
              aria-hidden
              sx={{ width: 96, height: 96, objectFit: 'contain', display: 'block', flexShrink: 0 }}
            >
              <source src={agentLoaderMp4} type="video/mp4" />
            </Box>
            <Box sx={{ flex: 1, width: '100%', minWidth: 0 }}>
              <AgentActivityTimeline runStatus={runStatus} stageLabels={stageLabels} />
            </Box>
          </Stack>

          <Box>
            <LinearProgress
              aria-label="Run progress"
              variant={hasRealProgress ? 'determinate' : 'indeterminate'}
              value={hasRealProgress ? progress ?? 0 : undefined}
            />
            {!hasRealProgress && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>Waiting for the first processed term.</Typography>
            )}
            {runStatus.current_term && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 1, overflowWrap: 'anywhere' }}>
                Current term: <Box component="span" sx={{ color: 'text.primary' }}>{runStatus.current_term}</Box>
              </Typography>
            )}
          </Box>
        </Stack>
      </Section>
    </>
  );
}
