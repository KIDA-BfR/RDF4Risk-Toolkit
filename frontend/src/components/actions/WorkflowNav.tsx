import React from 'react';
import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { BackButton, ContinueButton } from './WorkflowActionButtons';
import { useToolNavigation } from '../../shared/toolNavigation';
import { CONTENT_MAX_WIDTH, DRAWER_WIDTH } from '../../theme/tokens';

export type NavAction = {
  // Omit onClick to fall back to the shell's cross-tool step (previous / next tool).
  onClick?: () => void;
  disabled?: boolean;
  // Where the button leads, e.g. "Back to Choose terms". Shown as a tooltip, never as the label.
  tooltip?: string;
};

export type WorkflowNavProps = {
  back?: NavAction;
  next?: NavAction;
  // Status line: what comes next, or why Continue is disabled. Announced politely.
  hint?: React.ReactNode;
};

const SLOT_WIDTH = 140;

function Slot({ align, tooltip, children }: { align: 'left' | 'right'; tooltip?: string; children: React.ReactElement }) {
  return (
    <Box sx={{ width: { xs: 'auto', sm: SLOT_WIDTH }, flexShrink: 0, display: 'flex', justifyContent: align === 'left' ? 'flex-start' : 'flex-end' }}>
      {tooltip ? (
        <Tooltip title={tooltip}>
          <span>{children}</span>
        </Tooltip>
      ) : children}
    </Box>
  );
}

// The single wizard footer used by every tool (DESIGN.md §3). Back is always far left and labelled
// "Back"; Continue is always far right and labelled "Continue". Both are always rendered in
// fixed-width slots so nothing shifts between stages or tools; an unavailable action is disabled
// and the hint says why.
export function WorkflowNav({ back = {}, next = {}, hint }: WorkflowNavProps) {
  const tools = useToolNavigation();
  const backClick = back.onClick ?? tools?.goBack;
  const nextClick = next.onClick ?? tools?.goNext;
  const backTooltip = back.tooltip ?? (!back.onClick && tools?.backLabel ? `Back to ${tools.backLabel}` : undefined);
  const nextTooltip = next.tooltip ?? (!next.onClick && tools?.nextLabel ? `Continue to ${tools.nextLabel}` : undefined);
  const endOfWorkflow = !nextClick;
  const status = hint ?? (endOfWorkflow ? 'You have reached the end of the workflow.' : undefined);
  return (
    <Paper
      component="nav"
      aria-label="Workflow navigation"
      square
      elevation={0}
      sx={{
        position: 'fixed',
        bottom: 0,
        left: { xs: 0, lg: `${DRAWER_WIDTH}px` },
        right: 0,
        zIndex: (t) => t.zIndex.appBar,
        px: { xs: 2, sm: 3 },
        py: 1.5,
        borderTop: 1,
        borderColor: 'divider',
        bgcolor: 'background.paper',
      }}
    >
      <Box sx={{ maxWidth: CONTENT_MAX_WIDTH, mx: 'auto', display: 'flex', flexWrap: { xs: 'wrap', sm: 'nowrap' }, alignItems: 'center', columnGap: 2, rowGap: 1 }}>
        <Slot align="left" tooltip={enabledTooltip(back.disabled, backClick, backTooltip)}>
          <BackButton disabled={Boolean(back.disabled) || !backClick} onClick={backClick}>Back</BackButton>
        </Slot>
        <Typography
          role="status"
          aria-live="polite"
          variant="body2"
          color="text.secondary"
          sx={{ flex: 1, minWidth: 0, textAlign: 'center', order: { xs: 3, sm: 0 }, flexBasis: { xs: '100%', sm: 'auto' }, fontSize: { xs: '0.75rem', sm: '0.8125rem' } }}
        >
          {status}
        </Typography>
        <Box sx={{ flex: { xs: 1, sm: 'none' }, display: { xs: 'block', sm: 'none' } }} />
        <Slot align="right" tooltip={enabledTooltip(next.disabled, nextClick, nextTooltip)}>
          <ContinueButton disabled={Boolean(next.disabled) || endOfWorkflow} onClick={nextClick}>Continue</ContinueButton>
        </Slot>
      </Box>
    </Paper>
  );
}

// Destination tooltips only make sense while the button can be used; the hint covers the reason
// a button is disabled.
function enabledTooltip(disabled: boolean | undefined, click: (() => void) | undefined, tooltip: string | undefined) {
  return disabled || !click ? undefined : tooltip;
}
