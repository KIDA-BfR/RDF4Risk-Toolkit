import React, { useEffect, useRef, useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import {
  Timeline,
  TimelineItem,
  TimelineSeparator,
  TimelineDot,
  TimelineConnector,
  TimelineContent,
  timelineItemClasses,
} from '@mui/lab';
import { AnimatePresence, motion } from 'motion/react';
import type { RunStatus } from './AgentRunProgressPanel';

type Entry = { id: string; sig: string; time: string; text: string; kind: 'stage' | 'term' | 'message' | 'activity' };

const KIND_COLOR: Record<Entry['kind'], 'primary' | 'info' | 'success' | 'grey'> = {
  stage: 'primary',
  term: 'info',
  message: 'grey',
  activity: 'success',
};

function clockNow(): string {
  const d = new Date();
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
}

// Live, append-only activity trace derived from the signals the backend already emits on each poll
// (stage transitions, current term, status messages, last activity). Turns "looks busy" into "I can
// see exactly what the agent is doing right now" — the LangSmith-style run trace.
export function AgentActivityTimeline({ runStatus, stageLabels }: { runStatus: RunStatus; stageLabels: Record<string, string> }) {
  const [entries, setEntries] = useState<Entry[]>([]);
  const seen = useRef<Set<string>>(new Set());
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const counter = useRef(0);

  useEffect(() => {
    const candidates: Array<Pick<Entry, 'sig' | 'text' | 'kind'>> = [];
    const stage = String(runStatus.stage || '').trim();
    if (stage) candidates.push({ sig: `stage:${stage}`, kind: 'stage', text: stageLabels[stage] || stage.replace(/_/g, ' ') });
    if (runStatus.current_term) candidates.push({ sig: `term:${runStatus.current_term}`, kind: 'term', text: `Processing “${runStatus.current_term}”` });
    if (runStatus.message) candidates.push({ sig: `msg:${runStatus.message}`, kind: 'message', text: String(runStatus.message) });
    if (Array.isArray(runStatus.messages)) {
      runStatus.messages.forEach((m, i) => candidates.push({ sig: `marr:${i}:${m}`, kind: 'message', text: String(m) }));
    }
    if (runStatus.last_activity) candidates.push({ sig: `act:${runStatus.last_activity}`, kind: 'activity', text: String(runStatus.last_activity) });

    const fresh = candidates.filter((c) => !seen.current.has(c.sig));
    if (!fresh.length) return;
    fresh.forEach((c) => seen.current.add(c.sig));
    setEntries((prev) => {
      const next = [...prev, ...fresh.map((c) => ({ ...c, id: `e${counter.current++}`, time: clockNow() }))];
      return next.slice(-60); // keep the tail bounded
    });
  }, [runStatus, stageLabels]);

  useEffect(() => {
    // Auto-scroll to the newest entry.
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [entries.length]);

  if (!entries.length) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ py: 1 }}>
        Waiting for the first activity from the agent…
      </Typography>
    );
  }

  return (
    <Box ref={scrollRef} sx={{ width: '100%', maxHeight: 240, overflowY: 'auto', borderRadius: 2, border: '1px solid', borderColor: 'divider', bgcolor: 'background.default', px: 1, py: 0.5 }}>
      <Timeline sx={{ m: 0, p: 0, [`& .${timelineItemClasses.root}:before`]: { flex: 0, p: 0 } }}>
        <AnimatePresence initial={false}>
          {entries.map((entry, idx) => {
            const isLast = idx === entries.length - 1;
            return (
              <motion.div key={entry.id} initial={{ opacity: 0, x: -6 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.25 }}>
                <TimelineItem sx={{ minHeight: 38 }}>
                  <TimelineSeparator>
                    <TimelineDot color={KIND_COLOR[entry.kind] as any} variant={isLast ? 'filled' : 'outlined'} sx={isLast ? { boxShadow: '0 0 0 4px rgba(37,99,235,0.18)' } : undefined} />
                    {!isLast && <TimelineConnector />}
                  </TimelineSeparator>
                  <TimelineContent sx={{ py: 0.25 }}>
                    <Typography variant="body2" sx={{ fontWeight: isLast ? 700 : 500 }}>{entry.text}</Typography>
                    <Typography variant="caption" color="text.secondary">{entry.time}</Typography>
                  </TimelineContent>
                </TimelineItem>
              </motion.div>
            );
          })}
        </AnimatePresence>
      </Timeline>
    </Box>
  );
}
