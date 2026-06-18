import React, { useState } from 'react';
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  IconButton,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import SaveIcon from '@mui/icons-material/Save';
import HistoryIcon from '@mui/icons-material/History';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import AddIcon from '@mui/icons-material/Add';
import DownloadIcon from '@mui/icons-material/Download';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import type { ProjectMeta } from '../../standalone/backendClient';

function formatWhen(ts: number | null): string {
  if (!ts) return '';
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return '';
  }
}

// Sidebar workspace controls: a Projects menu (save current / open / delete / new) and a History
// button. Persistence is what lets a curator close the laptop and resume — the foundation the
// review explicitly missed ("a refresh destroys an hour of work").
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
}) {
  const [anchor, setAnchor] = useState<null | HTMLElement>(null);
  const [saveOpen, setSaveOpen] = useState(false);
  const [name, setName] = useState('');

  const closeMenu = () => setAnchor(null);
  const startSave = () => { closeMenu(); setName(''); setSaveOpen(true); };
  const confirmSave = () => { onSave(name.trim() || 'Untitled project'); setSaveOpen(false); };

  return (
    <Box sx={{ px: 2, pb: 1 }}>
      <Stack direction="row" spacing={1}>
        <Button fullWidth size="small" variant="outlined" startIcon={<FolderOpenIcon />} onClick={(e) => setAnchor(e.currentTarget)}>
          Projects
        </Button>
        <Tooltip title="Operation history">
          <Button size="small" variant="outlined" startIcon={<HistoryIcon />} onClick={onOpenHistory} disabled={!historyCount}>
            {historyCount}
          </Button>
        </Tooltip>
      </Stack>

      <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={closeMenu} slotProps={{ paper: { sx: { width: 280, maxWidth: '90vw' } } }}>
        <MenuItem onClick={startSave}>
          <ListItemIcon><SaveIcon fontSize="small" /></ListItemIcon>
          <ListItemText primary="Save current as project…" />
        </MenuItem>
        <MenuItem onClick={() => { closeMenu(); onNew(); }}>
          <ListItemIcon><AddIcon fontSize="small" /></ListItemIcon>
          <ListItemText primary="New project (clear history)" />
        </MenuItem>
        <Divider />
        <MenuItem onClick={() => { closeMenu(); onExportRecipe(); }}>
          <ListItemIcon><DownloadIcon fontSize="small" /></ListItemIcon>
          <ListItemText primary="Export recipe (config)" secondary="Reproducible methods-as-a-file" />
        </MenuItem>
        <MenuItem component="label">
          <ListItemIcon><UploadFileIcon fontSize="small" /></ListItemIcon>
          <ListItemText primary="Import recipe…" />
          <input
            hidden
            type="file"
            accept=".json,application/json"
            onChange={(e) => {
              const file = e.target.files?.[0];
              e.target.value = '';
              closeMenu();
              if (file) onImportRecipe(file);
            }}
          />
        </MenuItem>
        <Divider />
        <Typography variant="caption" color="text.secondary" sx={{ px: 2, py: 0.5, display: 'block' }}>
          {projects.length ? 'Saved projects' : 'No saved projects yet'}
        </Typography>
        {projects.map((project) => (
          <MenuItem key={project.id} onClick={() => { closeMenu(); onOpen(project.id); }} sx={{ pr: 1 }}>
            <ListItemText primary={project.name} secondary={formatWhen(project.saved_at)} primaryTypographyProps={{ noWrap: true }} />
            <IconButton
              size="small"
              aria-label={`Delete project ${project.name}`}
              onClick={(e) => { e.stopPropagation(); onDelete(project.id); }}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </MenuItem>
        ))}
      </Menu>

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
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setSaveOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={confirmSave}>Save</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
