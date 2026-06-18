import React, { useState } from 'react';
import Button, { type ButtonProps } from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';

type ConfirmActionButtonProps = Omit<ButtonProps, 'onClick'> & {
  dialogTitle: string;
  dialogMessage: string;
  confirmLabel: string;
  confirmColor?: ButtonProps['color'];
  onConfirm: () => void;
  ButtonComponent?: React.ComponentType<ButtonProps & { children: React.ReactNode }>;
};

// Guard for destructive/irreversible actions: opens a confirmation Dialog that names
// what will happen. Cancel is autofocused so the destructive choice is never the
// default (rubric §11 / Nielsen #5).
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
  const close = () => setOpen(false);
  return (
    <>
      <ButtonComponent {...buttonProps} onClick={() => setOpen(true)}>
        {children}
      </ButtonComponent>
      <Dialog open={open} onClose={close} aria-labelledby="confirm-action-title" aria-describedby="confirm-action-message">
        <DialogTitle id="confirm-action-title">{dialogTitle}</DialogTitle>
        <DialogContent>
          <DialogContentText id="confirm-action-message">{dialogMessage}</DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={close} autoFocus>
            Cancel
          </Button>
          <Button
            variant="contained"
            color={confirmColor}
            onClick={() => {
              close();
              onConfirm();
            }}
          >
            {confirmLabel}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
