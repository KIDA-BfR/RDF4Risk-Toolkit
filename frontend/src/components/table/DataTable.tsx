import React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Tooltip from '@mui/material/Tooltip';
import {
  DataGrid,
  GridToolbar,
  type GridColDef,
  type GridRenderCellParams,
} from '@mui/x-data-grid';

export type DataTableRowValue = Record<string, unknown>;

export type DataTableProps = {
  rows?: DataTableRowValue[];
  empty: string;
  maxColumns?: number;
  /** Soft cap kept for API compatibility; the grid virtualizes, so all rows are shown. */
  maxRows?: number;
  minWidth?: number;
  /** Optional custom cell renderer (e.g. markdown links in RDF to Table). */
  renderCell?: (value: unknown, column: string) => React.ReactNode;
};

const ROW_HEIGHT = 36;
const HEADER_HEIGHT = 42;
const TOOLBAR_HEIGHT = 48;
const FOOTER_HEIGHT = 40;

// Shared preview table for all five service views. MUI X Data Grid gives column sorting,
// multi-column filtering, a quick-search toolbar, column hide/resize/reorder, CSV export, and
// row virtualization for free — replacing the previous hand-rolled <table> and its 100-row cap.
// Synthetic field names (c0, c1, …) + valueGetter sidestep the Data Grid's dotted-field path
// lookup, so columns like "rdf:type" or "a.b" never break.
export function DataTable({ rows, empty, maxColumns = 8, minWidth = 720, renderCell }: DataTableProps) {
  const safeRows = rows ?? [];
  if (!safeRows.length) {
    return (
      <Typography variant="body2" color="text.secondary">
        {empty}
      </Typography>
    );
  }

  const keys = Object.keys(safeRows[0]).slice(0, maxColumns);
  const gridRows = safeRows.map((row, idx) => ({ __id: idx, __row: row }));

  const columns: GridColDef[] = keys.map((key, i) => ({
    field: `c${i}`,
    headerName: key,
    flex: 1,
    minWidth: Math.max(120, Math.round(minWidth / Math.max(keys.length, 1))),
    sortable: true,
    valueGetter: (_value, row: { __row: DataTableRowValue }) => {
      const v = row.__row[key];
      return v == null ? '' : String(v);
    },
    renderCell: (params: GridRenderCellParams) => {
      const raw = (params.row as { __row: DataTableRowValue }).__row[key];
      const text = raw == null ? '' : String(raw);
      const content = renderCell ? renderCell(raw, key) : text;
      return (
        <Tooltip title={text} disableInteractive>
          <Box component="span" sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {content}
          </Box>
        </Tooltip>
      );
    },
  }));

  const pageSize = 25;
  const visible = Math.min(safeRows.length, pageSize);
  const height = Math.min(
    HEADER_HEIGHT + TOOLBAR_HEIGHT + FOOTER_HEIGHT + visible * ROW_HEIGHT + 4,
    520,
  );

  return (
    <Box sx={{ height, width: '100%' }}>
      <DataGrid
        rows={gridRows}
        columns={columns}
        getRowId={(r) => (r as { __id: number }).__id}
        density="compact"
        columnHeaderHeight={HEADER_HEIGHT}
        rowHeight={ROW_HEIGHT}
        disableRowSelectionOnClick
        pageSizeOptions={[25, 50, 100]}
        initialState={{ pagination: { paginationModel: { pageSize } } }}
        slots={{ toolbar: GridToolbar }}
        slotProps={{ toolbar: { showQuickFilter: true, printOptions: { disableToolbarButton: true } } }}
        sx={{
          borderColor: 'divider',
          borderRadius: 2,
          '& .MuiDataGrid-columnHeaders': { bgcolor: 'background.default' },
          '& .MuiDataGrid-columnHeaderTitle': { fontWeight: 700 },
        }}
      />
    </Box>
  );
}
