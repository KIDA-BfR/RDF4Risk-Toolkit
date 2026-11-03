import React, { useState } from 'react';
import IconButton from '@mui/material/IconButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Tooltip from '@mui/material/Tooltip';
import MoreHorizIcon from '@mui/icons-material/MoreHoriz';
import { ConfirmDialog } from '../dialogs/ConfirmActionButton';

export type ToolMenuItem = {
  label: string;
  icon?: React.ReactNode;
  onSelect: () => void;
  disabled?: boolean;
  // Destructive / irreversible items confirm first; Cancel is the default (autofocused).
  confirm?: { title: string; message: string; confirmLabel: string };
};

// Tool-level actions that are rare or destructive (Reset …), always in the same place: the ⋯
// button at the right of the stage header (DESIGN.md §4).
export function ToolMenu({ items, label = 'More actions' }: { items: ToolMenuItem[]; label?: string }) {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  // The item stays set while the dialog closes so its text doesn't blank during the exit transition.
  const [pending, setPending] = useState<ToolMenuItem | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const close = () => setAnchor(null);
  return (
    <>
      <Tooltip title={label}>
        <IconButton aria-label={label} aria-haspopup="menu" aria-expanded={Boolean(anchor)} onClick={(event) => setAnchor(event.currentTarget)} sx={{ border: 1, borderColor: 'divider' }}>
          <MoreHorizIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Menu
        anchorEl={anchor}
        open={Boolean(anchor)}
        onClose={close}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'right' }}
      >
        {items.map((item) => (
          <MenuItem
            key={item.label}
            disabled={item.disabled}
            onClick={() => {
              close();
              if (item.confirm) {
                setPending(item);
                setConfirmOpen(true);
              } else item.onSelect();
            }}
          >
            {item.icon && <ListItemIcon>{item.icon}</ListItemIcon>}
            <ListItemText>{item.label}{item.confirm ? '…' : ''}</ListItemText>
          </MenuItem>
        ))}
      </Menu>
      <ConfirmDialog
        open={confirmOpen}
        title={pending?.confirm?.title ?? ''}
        message={pending?.confirm?.message ?? ''}
        confirmLabel={pending?.confirm?.confirmLabel ?? ''}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => pending?.onSelect()}
      />
    </>
  );
}
