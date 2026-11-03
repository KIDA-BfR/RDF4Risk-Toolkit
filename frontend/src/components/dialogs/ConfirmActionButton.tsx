import React, { useId, useState } from 'react';
import Button, { type ButtonProps } from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';

export type ConfirmDialogProps = {
  open: boolean;
  title: string;
  // Names the consequence: what changes, what is lost, what stays.
  message: string;
  // A specific verb + object that repeats the action ("Delete project", "Reset preparation") —
  // never "OK", "Yes", "Confirm" or "Continue".
  confirmLabel: string;
  confirmColor?: ButtonProps['color'];
  onClose: () => void;
  onConfirm: () => void;
};

// The confirmation dialog behind every destructive action. Cancel is autofocused so the
// destructive choice is never the default (rubric §11 / Nielsen #5). Controlled, so triggers that
// live in a closing Menu can keep the dialog mounted outside it.
export function ConfirmDialog({ open, title, message, confirmLabel, confirmColor = 'warning', onClose, onConfirm }: ConfirmDialogProps) {
  const id = useId();
  return (
    <Dialog open={open} onClose={onClose} aria-labelledby={`${id}-title`} aria-describedby={`${id}-message`}>
      <DialogTitle id={`${id}-title`}>{title}</DialogTitle>
      <DialogContent>
        <DialogContentText id={`${id}-message`}>{message}</DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} autoFocus>
          Cancel
        </Button>
        <Button
          variant="contained"
          color={confirmColor}
          onClick={() => {
            onClose();
            onConfirm();
          }}
        >
          {confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

type ConfirmActionButtonProps = Omit<ButtonProps, 'onClick'> & {
  dialogTitle: string;
  dialogMessage: string;
  /** Specific verb + object ("Reset preparation"), never "OK", "Yes", "Confirm" or "Continue". */
  confirmLabel: string;
  confirmColor?: ButtonProps['color'];
  onConfirm: () => void;
  ButtonComponent?: React.ComponentType<ButtonProps & { children: React.ReactNode }>;
};

// Guard for destructive/irreversible actions: a trigger button that opens a ConfirmDialog naming
// what will happen.
export function ConfirmActionButton({
  dialogTitle,
  dialogMessage,
  confirmLabel,
  confirmColor = 'warning',
  onConfirm,
  ButtonComponent = Button,
  children,
  ...buttonProps
}: ConfirmActionButtonProps) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <ButtonComponent {...buttonProps} onClick={() => setOpen(true)}>
        {children}
      </ButtonComponent>
      <ConfirmDialog
        open={open}
        title={dialogTitle}
        message={dialogMessage}
        confirmLabel={confirmLabel}
        confirmColor={confirmColor}
        onClose={() => setOpen(false)}
        onConfirm={onConfirm}
      />
    </>
  );
}
