import React, { useEffect, useRef } from 'react';
import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Stack from '@mui/material/Stack';
import Step from '@mui/material/Step';
import StepButton from '@mui/material/StepButton';
import Stepper from '@mui/material/Stepper';
import useMediaQuery from '@mui/material/useMediaQuery';
import type { InfoContent } from '../help/InfoButton';
import { WorkflowNav, type WorkflowNavProps } from '../actions/WorkflowNav';
import { StageHeader } from './StageHeader';
import { useShellStatus } from '../../shared/toolNavigation';
import { CONTENT_MAX_WIDTH, NAV_CLEARANCE, SCROLL_MARGIN } from '../../theme/tokens';

export type WizardStep = {
  id: string;
  label: string;
  completed?: boolean;
  // Must match what Continue allows: the stepper never reaches a stage Continue would not.
  disabled?: boolean;
};

type WizardPageProps = {
  steps: WizardStep[];
  activeIndex: number;
  onStepClick?: (index: number, step: WizardStep) => void;
  title: React.ReactNode;
  lead?: React.ReactNode;
  info?: InfoContent;
  actions?: React.ReactNode;
  // Alerts that matter right now (errors, run status, unapplied drafts). Rendered under the header.
  status?: React.ReactNode;
  nav: WorkflowNavProps;
  // Heading focus + scroll-to-top run whenever this changes (defaults to the active stage).
  focusKey?: string | number;
  children: React.ReactNode;
};

// The page skeleton every tool stage uses (DESIGN.md §2): Stepper → stage header → status →
// content → fixed WorkflowNav. Owns focus management so screen-reader users land on the new
// stage heading after every stage change.
export function WizardPage({ steps, activeIndex, onStepClick, title, lead, info, actions, status, nav, focusKey, children }: WizardPageProps) {
  const compact = useMediaQuery('(max-width:599.95px)');
  const heading = useRef<HTMLHeadingElement>(null);
  const shell = useShellStatus();
  const key = focusKey ?? activeIndex;
  useEffect(() => {
    heading.current?.focus({ preventScroll: true });
    window.scrollTo({ top: 0 });
  }, [key]);
  return (
    <Box
      sx={{
        maxWidth: CONTENT_MAX_WIDTH,
        mx: 'auto',
        minWidth: 0,
        pb: NAV_CLEARANCE,
        '& :focus': { scrollMarginTop: `${SCROLL_MARGIN.top}px`, scrollMarginBottom: `${SCROLL_MARGIN.bottom}px` },
      }}
    >
      <Stepper nonLinear alternativeLabel={compact} activeStep={activeIndex} sx={{ mb: 3 }}>
        {steps.map((step, index) => (
          <Step key={step.id} completed={step.completed}>
            <StepButton disabled={step.disabled} onClick={() => onStepClick?.(index, step)}>
              {step.label}
            </StepButton>
          </Step>
        ))}
      </Stepper>
      <StageHeader title={title} lead={lead} info={info} actions={actions} headingRef={heading} />
      {(status || shell?.error) && (
        <Stack spacing={1.5} sx={{ mb: 2.5, '&:empty': { display: 'none' } }}>
          {shell?.error && (
            <Alert severity="error" role="alert" sx={{ whiteSpace: 'pre-wrap' }} onClose={shell.dismissError} action={<Button color="inherit" size="small" onClick={shell.reload}>Reload</Button>}>
              {shell.error}
            </Alert>
          )}
          {status}
        </Stack>
      )}
      {children}
      <WorkflowNav {...nav} />
    </Box>
  );
}
