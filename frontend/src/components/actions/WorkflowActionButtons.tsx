import React from 'react';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import DownloadIcon from '@mui/icons-material/Download';
import FileUploadIcon from '@mui/icons-material/FileUpload';
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

export function BackButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <ArrowBackIcon /> });
}

export function ContinueButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'contained', endIcon: <ArrowForwardIcon /> });
}

export function DownloadButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'contained', startIcon: <DownloadIcon /> });
}

export function GenerateButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'contained', startIcon: <PlayArrowIcon /> });
}

export function PrepareButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'contained', startIcon: <TuneIcon /> });
}

export function RefreshActionButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <RefreshIcon /> });
}

export function ResetButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', color: 'warning', startIcon: <RestartAltIcon /> });
}

export function SaveButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'contained', startIcon: <SaveIcon /> });
}

export function SearchActionButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'outlined', startIcon: <SearchIcon /> });
}

export function UploadButton(props: WorkflowButtonProps) {
  return withDefaults(props, { variant: 'contained', startIcon: <FileUploadIcon /> });
}
