import React, { useId, useRef, useState } from 'react';
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  ListItemIcon,
  ListItemText,
  ListSubheader,
  Menu,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
} from '@mui/material';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import SaveIcon from '@mui/icons-material/Save';
import HistoryIcon from '@mui/icons-material/History';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import AddIcon from '@mui/icons-material/Add';
import DownloadIcon from '@mui/icons-material/Download';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import type { ProjectMeta } from '../../standalone/backendClient';
import { ConfirmDialog } from '../dialogs/ConfirmActionButton';

const savedAt = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' });
const count = new Intl.NumberFormat();

function formatWhen(ts: number | null): string {
  if (!ts) return '';
  try {
    return savedAt.format(new Date(ts * 1000));
  } catch {
    return '';
  }
}

type PendingConfirm = { title: string; message: string; confirmLabel: string; run: () => void };

const menuPaperSx = { width: 280, maxWidth: '90vw' } as const;
// A non-interactive caption row. ListSubheader is skipped by the menu's focus handling.
const menuCaptionSx = { typography: 'caption', color: 'text.secondary', py: 0.5 } as const;

// Sidebar workspace controls: a Projects menu (save current / new / recipe export and import /
// open / delete) and a History button. Persistence is what lets a curator close the laptop and
// resume — the foundation the review explicitly missed ("a refresh destroys an hour of work").
// Open, import, delete and new replace or discard work, so each asks first; the dialog and the
// recipe file input live outside the menus so they survive the menus closing. Delete has its own
// menu so no button sits inside a menu item.
export function WorkspaceControls({
  projects,
  historyCount,
  onSave,
  onOpen,
  onDelete,
  onNew,
  onOpenHistory,
  onExportRecipe,
  onImportRecipe,
  unsavedEditsNote,
}: {
  projects: ProjectMeta[];
  historyCount: number;
  onSave: (name: string) => void;
  onOpen: (id: string) => void;
  onDelete: (id: string) => void;
  onNew: () => void;
  onOpenHistory: () => void;
  onExportRecipe: () => void;
  onImportRecipe: (file: File) => void;
  /** Set while the active tool holds unapplied edits, e.g. "Unapplied edits in RDF generator will be lost." */
  unsavedEditsNote?: string;
}) {
  const [anchor, setAnchor] = useState<null | HTMLElement>(null);
  const [deleteAnchor, setDeleteAnchor] = useState<null | HTMLElement>(null);
  const [saveOpen, setSaveOpen] = useState(false);
  const [name, setName] = useState('');
  // Kept after closing so the dialog text doesn't blank out during its exit transition.
  const [pending, setPending] = useState<PendingConfirm | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const recipeInput = useRef<HTMLInputElement>(null);
  const menuId = useId();
  const deleteMenuId = useId();
  const buttonId = useId();

  // Actions that replace tool state name the active tool's unapplied edits as their own sentence.
  const withNote = (message: string) => (unsavedEditsNote ? `${message} ${unsavedEditsNote}` : message);
  const closeMenus = () => { setAnchor(null); setDeleteAnchor(null); };
  const startSave = () => { closeMenus(); setName(''); setSaveOpen(true); };
  const confirmSave = () => { onSave(name.trim() || 'Untitled project'); setSaveOpen(false); };
  // A normal menu item opens the picker, so Enter and Space work like a click.
  const startImport = () => { closeMenus(); recipeInput.current?.click(); };
  const startDelete = () => { setDeleteAnchor(anchor); setAnchor(null); };
  const ask = (action: PendingConfirm) => { closeMenus(); setPending(action); setConfirmOpen(true); };
  const askNew = () => ask({
    title: 'Start a new project?',
    message: 'This clears the operation history, so earlier actions can no longer be undone or reverted. The tools keep their current data and settings; save them as a project first if you may want to return to this point.',
    confirmLabel: 'Start new project',
    run: onNew,
  });
  const askImport = (file: File) => ask({
    title: `Apply settings from “${file.name}”?`,
    message: withNote('Current settings in every tool are replaced; this is not recorded in History.'),
    confirmLabel: 'Import recipe',
    run: () => onImportRecipe(file),
  });
  const askOpen = (project: ProjectMeta) => ask({
    title: `Open “${project.name}”?`,
    message: withNote('The saved project replaces the data and settings in every tool, and the operation history is cleared. Save the current work as a project first if you still need it.'),
    confirmLabel: 'Open project',
    run: () => onOpen(project.id),
  });
  const askDelete = (project: ProjectMeta) => ask({
    title: `Delete “${project.name}”?`,
    message: 'The saved project is removed from this server and cannot be restored. The work currently open in the tools is not affected.',
    confirmLabel: 'Delete project',
    run: () => onDelete(project.id),
  });

  const historyLabel = `History, ${count.format(historyCount)} ${historyCount === 1 ? 'action' : 'actions'}`;

  return (
    <Box sx={{ px: 2, pb: 1 }}>
      <Stack direction="row" spacing={1}>
        <Button
          id={buttonId}
          fullWidth
          size="small"
          variant="outlined"
          startIcon={<FolderOpenIcon />}
          aria-haspopup="menu"
          aria-expanded={Boolean(anchor)}
          aria-controls={anchor ? menuId : undefined}
          onClick={(e) => setAnchor(e.currentTarget)}
        >
          Projects
        </Button>
        {/* The span keeps the tooltip working while the button is disabled. */}
        <Tooltip describeChild title={historyCount ? 'Operation history' : 'No actions recorded yet'}>
          <Box component="span" sx={{ display: 'inline-flex', flexShrink: 0 }}>
            <Button
              size="small"
              variant="outlined"
              startIcon={<HistoryIcon />}
              onClick={onOpenHistory}
              disabled={!historyCount}
              aria-label={historyLabel}
              sx={{ fontVariantNumeric: 'tabular-nums' }}
            >
              {count.format(historyCount)}
            </Button>
          </Box>
        </Tooltip>
      </Stack>

      <Menu
        id={menuId}
        anchorEl={anchor}
        open={Boolean(anchor)}
        onClose={closeMenus}
        MenuListProps={{ 'aria-labelledby': buttonId }}
        slotProps={{ paper: { sx: menuPaperSx } }}
      >
        <MenuItem onClick={startSave}>
          <ListItemIcon><SaveIcon fontSize="small" /></ListItemIcon>
          <ListItemText primary="Save current as project…" />
        </MenuItem>
        <MenuItem onClick={askNew}>
          <ListItemIcon><AddIcon fontSize="small" /></ListItemIcon>
          <ListItemText primary="New project…" secondary="Clears the operation history" />
        </MenuItem>
        <Divider />
        <Tooltip describeChild placement="right" title="Saves the pipeline settings (not the data tables) as a JSON file you can reuse on new data or attach to a methods section.">
          <MenuItem onClick={() => { closeMenus(); onExportRecipe(); }}>
            <ListItemIcon><DownloadIcon fontSize="small" /></ListItemIcon>
            <ListItemText primary="Export recipe" />
          </MenuItem>
        </Tooltip>
        <Tooltip describeChild placement="right" title="Applies the settings from a recipe file. Data already loaded in the tools stays as it is.">
          <MenuItem onClick={startImport}>
            <ListItemIcon><UploadFileIcon fontSize="small" /></ListItemIcon>
            <ListItemText primary="Import recipe…" />
          </MenuItem>
        </Tooltip>
        <Divider />
        <ListSubheader role="presentation" disableSticky sx={menuCaptionSx}>
          {projects.length ? 'Saved projects' : 'No saved projects yet'}
        </ListSubheader>
        {projects.map((project) => (
          <MenuItem key={project.id} onClick={() => askOpen(project)}>
            <ListItemText
              primary={project.name}
              secondary={formatWhen(project.saved_at) || undefined}
              primaryTypographyProps={{ noWrap: true }}
              secondaryTypographyProps={{ sx: { fontVariantNumeric: 'tabular-nums' } }}
            />
          </MenuItem>
        ))}
        {projects.length > 0 && <Divider />}
        {projects.length > 0 && (
          <MenuItem
            onClick={startDelete}
            aria-haspopup="menu"
            aria-controls={deleteAnchor ? deleteMenuId : undefined}
          >
            <ListItemIcon><DeleteOutlineIcon fontSize="small" /></ListItemIcon>
            <ListItemText primary="Delete project…" />
          </MenuItem>
        )}
      </Menu>

      {/* Opens in place of the Projects menu, anchored to the same button. */}
      <Menu
        id={deleteMenuId}
        anchorEl={deleteAnchor}
        open={Boolean(deleteAnchor)}
        onClose={closeMenus}
        MenuListProps={{ 'aria-label': 'Delete project' }}
        slotProps={{ paper: { sx: menuPaperSx } }}
      >
        <ListSubheader role="presentation" disableSticky sx={menuCaptionSx}>
          Choose the project to delete
        </ListSubheader>
        {projects.map((project) => (
          <MenuItem key={project.id} onClick={() => askDelete(project)}>
            <ListItemIcon><DeleteOutlineIcon fontSize="small" /></ListItemIcon>
            <ListItemText
              primary={project.name}
              secondary={formatWhen(project.saved_at) || undefined}
              primaryTypographyProps={{ noWrap: true }}
              secondaryTypographyProps={{ sx: { fontVariantNumeric: 'tabular-nums' } }}
            />
          </MenuItem>
        ))}
      </Menu>

      {/* Outside the menus so it stays mounted while the file picker is open. */}
      <input
        ref={recipeInput}
        hidden
        type="file"
        accept=".json,application/json"
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = '';
          if (file) askImport(file);
        }}
      />

      <ConfirmDialog
        open={confirmOpen}
        title={pending?.title ?? ''}
        message={pending?.message ?? ''}
        confirmLabel={pending?.confirmLabel ?? ''}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => pending?.run()}
      />

      <Dialog open={saveOpen} onClose={() => setSaveOpen(false)} maxWidth="xs" fullWidth>
        <DialogTitle>Save project</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            margin="dense"
            label="Project name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') confirmSave(); }}
            placeholder="e.g. CHANCE dataset – 2026-06"
            helperText="Saves the data and settings of every tool, including the RDF inspector’s catalog."
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setSaveOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={confirmSave}>Save project</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
