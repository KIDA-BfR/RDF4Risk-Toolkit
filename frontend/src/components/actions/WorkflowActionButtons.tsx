import React from 'react';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import DownloadIcon from '@mui/icons-material/Download';
import FileUploadIcon from '@mui/icons-material/FileUpload';
import InputIcon from '@mui/icons-material/Input';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import RefreshIcon from '@mui/icons-material/Refresh';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import SaveIcon from '@mui/icons-material/Save';
import SearchIcon from '@mui/icons-material/Search';
import TuneIcon from '@mui/icons-material/Tune';
import Button, { type ButtonProps } from '@mui/material/Button';

type WorkflowButtonProps = ButtonProps & {
  children: React.ReactNode;
};

function withDefaults(
  props: WorkflowButtonProps,
  defaults: Pick<ButtonProps, 'variant' | 'color' | 'startIcon' | 'endIcon'>,
) {
  const { children, variant, color, startIcon, endIcon, ...rest } = props;
  return (
    <Button
      variant={variant ?? defaults.variant}
      color={color ?? defaults.color}
      startIcon={startIcon ?? defaults.startIcon}
      endIcon={endIcon ?? defaults.endIcon}
      {...rest}
    >
      {children}
    </Button>
  );
}

// Back and Continue are the workflow's only navigation buttons (DESIGN.md §3). Their labels,
// variants and icons are fixed here so they can never drift; the destination belongs in the
// WorkflowNav hint/tooltip, not in a changing label. Use them through WorkflowNav.
export function BackButton(props: WorkflowButtonProps) {
  return withDefaults({ ...props, variant: 'outlined', children: 'Back' }, { variant: 'outlined', startIcon: <ArrowBackIcon /> });
}

export function ContinueButton(props: WorkflowButtonProps) {
  return withDefaults({ ...props, variant: 'contained', children: 'Continue' }, { variant: 'contained', endIcon: <ArrowForwardIcon /> });
}

// Action buttons default to outlined: a view has exactly one contained (primary) button, so pass
// variant="contained" only for the stage's main action (DESIGN.md §7).
export function DownloadButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <DownloadIcon /> });
}

export function GenerateButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <PlayArrowIcon /> });
}

export function PrepareButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <TuneIcon /> });
}

export function RefreshActionButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <RefreshIcon /> });
}

export function ResetButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', color: 'warning', startIcon: <RestartAltIcon /> });
}

export function SaveButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <SaveIcon /> });
}

export function SearchActionButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <SearchIcon /> });
}

export function UploadButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <FileUploadIcon /> });
}

// Pull the previous tool's result into this tool ("Use prepared table", "Use generated catalog").
export function UseResultButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <InputIcon /> });
}
