import React from 'react';
import { Box, ButtonBase, Chip, Paper, Stack, Typography } from '@mui/material';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import { serviceMeta, workflowPhases, type ToolId } from './services';
import { CONTENT_MAX_WIDTH } from '../theme/tokens';

const NODE = 28;
const COLUMN_GAP = 3; // theme spacing units; the rail line bridges it

function ToolTile({ id, start, onOpen }: { id: ToolId; start: boolean; onOpen: (id: ToolId) => void }) {
  const meta = serviceMeta(id);
  return (
    <Paper variant="outlined" sx={{ borderColor: start ? 'primary.main' : 'divider' }}>
      <ButtonBase
        onClick={() => onOpen(id)}
        sx={{
          width: '100%',
          borderRadius: 'inherit',
          textAlign: 'left',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          gap: 1,
          p: 1.5,
          '&:hover': { bgcolor: 'action.hover' },
          '&:hover .tool-chevron': { color: 'primary.main' },
        }}
      >
        <Box component="span" sx={{ display: 'block', minWidth: 0 }}>
          <Stack component="span" direction="row" alignItems="center" spacing={1} sx={{ flexWrap: 'wrap', rowGap: 0.5 }}>
            <Typography variant="subtitle2" component="span">{meta.title}</Typography>
            {start && <Chip component="span" size="small" color="primary" variant="outlined" label="Start here" sx={{ height: 20, fontSize: '0.6875rem' }} />}
          </Stack>
          <Typography variant="body2" color="text.secondary" component="span" sx={{ display: 'block', mt: 0.5 }}>
            {meta.summary}
          </Typography>
        </Box>
        <ChevronRightIcon className="tool-chevron" aria-hidden="true" sx={{ color: 'text.secondary', mt: 0.25, flexShrink: 0, transition: (t) => t.transitions.create('color', { duration: t.transitions.duration.shortest }) }} />
      </ButtonBase>
    </Paper>
  );
}

// The overview is the app's one signature moment: the pipeline route from table to Linked Data.
// It is also the mental model behind Back/Continue — every tool is one stop on this route.
export function WorkflowOverview({ onOpen }: { onOpen: (service: ToolId) => void }) {
  const last = workflowPhases.length - 1;
  return (
    <Box sx={{ maxWidth: CONTENT_MAX_WIDTH, mx: 'auto', pb: 4 }}>
      <Box sx={{ mb: { xs: 4, md: 6 }, mt: { md: 2 } }}>
        <Typography component="h2" sx={{ fontSize: { xs: '1.75rem', md: '2.25rem' }, fontWeight: 600, letterSpacing: '-0.025em', lineHeight: 1.15, textWrap: 'balance' }}>
          From research tables to Linked Data
        </Typography>
        <Typography color="text.secondary" sx={{ mt: 1.5, fontSize: '1.0625rem', maxWidth: '62ch' }}>
          Four phases, one route. Start with your table, or step in at any phase if your data is already prepared.
        </Typography>
      </Box>

      <Box
        component="ol"
        aria-label="Workflow phases"
        sx={{ listStyle: 'none', m: 0, p: 0, display: 'grid', gridTemplateColumns: { xs: '1fr', md: `repeat(${workflowPhases.length}, minmax(0, 1fr))` }, columnGap: COLUMN_GAP }}
      >
        {workflowPhases.map((phase, index) => (
          <Box component="li" key={phase.title} sx={{ position: 'relative', pl: { xs: `${NODE + 16}px`, md: 0 }, pb: { xs: index < last ? 4 : 0, md: 0 } }}>
            {/* Route rail: node + connector. Horizontal on desktop (bridging the column gap), vertical on phones. */}
            <Box
              aria-hidden="true"
              sx={{
                display: 'flex',
                alignItems: 'center',
                flexDirection: { xs: 'column', md: 'row' },
                position: { xs: 'absolute', md: 'static' },
                left: 0,
                top: 0,
                bottom: 0,
                mb: { md: 2.5 },
                mr: { md: index < last ? -COLUMN_GAP : 0 },
              }}
            >
              <Box
                sx={{
                  width: NODE,
                  height: NODE,
                  flexShrink: 0,
                  borderRadius: '50%',
                  display: 'grid',
                  placeItems: 'center',
                  border: 1,
                  borderColor: index === 0 ? 'primary.main' : 'rdf.control',
                  color: index === 0 ? 'primary.main' : 'text.secondary',
                  bgcolor: 'background.paper',
                  fontSize: '0.8125rem',
                  fontWeight: 600,
                  fontVariantNumeric: 'tabular-nums',
                }}
              >
                {index + 1}
              </Box>
              {index < last && <Box sx={{ flex: 1, bgcolor: 'divider', height: { xs: 'auto', md: '1px' }, width: { xs: '1px', md: 'auto' }, minHeight: { xs: 24, md: 0 } }} />}
            </Box>
            <Typography component="h3" variant="subtitle1">{phase.title}</Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5, mb: 2, minHeight: { md: '3em' } }}>
              {phase.summary}
            </Typography>
            <Stack spacing={1}>
              {phase.services.map((id) => <ToolTile key={id} id={id} start={index === 0} onOpen={onOpen} />)}
            </Stack>
            {phase.services.length > 1 && (
              <Typography variant="caption" color="text.secondary" component="p" sx={{ mt: 1 }}>
                Two alternatives — pick one.
              </Typography>
            )}
          </Box>
        ))}
      </Box>
    </Box>
  );
}
