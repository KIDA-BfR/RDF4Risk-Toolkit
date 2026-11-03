import React, { useMemo, useState } from 'react';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import Tooltip from '@mui/material/Tooltip';
import TableRowsOutlinedIcon from '@mui/icons-material/TableRowsOutlined';
import {
  DataGrid,
  GridToolbar,
  type GridColDef,
  type GridColumnVisibilityModel,
  type GridComparatorFn,
  type GridRenderCellParams,
} from '@mui/x-data-grid';
import { EmptyState } from '../states/EmptyState';

export type DataTableRowValue = Record<string, unknown>;

export type DataTableProps = {
  rows?: DataTableRowValue[];
  empty: string;
  /** Optional action in the empty state, e.g. the upload button that fills this table. */
  emptyAction?: React.ReactNode;
  /** Columns shown at first; the rest stay available under Columns in the toolbar. */
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

const count = new Intl.NumberFormat();
const isMissing = (value: unknown) => value === null || value === undefined || value === '';

// A column is numeric when every present value is a finite number; it then right-aligns with
// tabular figures and sorts by value instead of as text. Strings that look numeric (IDs, codes)
// stay text.
function isNumericColumn(rows: DataTableRowValue[], key: string): boolean {
  let seen = false;
  for (const row of rows) {
    const value = row[key];
    if (isMissing(value)) continue;
    if (typeof value !== 'number' || !Number.isFinite(value)) return false;
    seen = true;
  }
  return seen;
}

// valueGetter yields strings ('' when missing), so compare them as numbers; missing sorts first.
const compareNumericText: GridComparatorFn<string> = (a, b) => {
  const x = a === '' ? Number.NEGATIVE_INFINITY : Number(a);
  const y = b === '' ? Number.NEGATIVE_INFINITY : Number(b);
  return x === y ? 0 : x < y ? -1 : 1;
};

// Shared preview table for all five service views. MUI X Data Grid gives column sorting,
// multi-column filtering, a quick-search toolbar, column hide/resize/reorder, CSV export, and
// row virtualization for free — replacing the previous hand-rolled <table> and its 100-row cap.
// Synthetic field names (c0, c1, …) + valueGetter sidestep the Data Grid's dotted-field path
// lookup, so columns like "rdf:type" or "a.b" never break. Columns beyond maxColumns start hidden
// (not dropped), so nothing is silently truncated; quick filter and CSV export follow visibility.
export function DataTable({ rows, empty, emptyAction, maxColumns = 8, minWidth = 720, renderCell }: DataTableProps) {
  const keys = useMemo(() => (rows?.length ? Object.keys(rows[0]) : []), [rows]);
  const signature = `${maxColumns}\u0000${keys.join('\u0000')}`;
  const initialVisibility = useMemo<GridColumnVisibilityModel>(
    () => Object.fromEntries(keys.slice(maxColumns).map((_, i) => [`c${i + maxColumns}`, false])),
    [keys, maxColumns],
  );
  // The user's column choices hold for this table only; a table with other columns starts fresh.
  const [visibilityState, setVisibilityState] = useState<{ signature: string; model: GridColumnVisibilityModel } | null>(null);
  const visibilityModel = visibilityState?.signature === signature ? visibilityState.model : initialVisibility;

  const columns = useMemo<GridColDef[]>(() => {
    const safeRows = rows ?? [];
    const shown = Math.max(Math.min(keys.length, maxColumns), 1);
    return keys.map((key, i) => {
      const numeric = isNumericColumn(safeRows, key);
      return {
        field: `c${i}`,
        headerName: key,
        flex: 1,
        minWidth: Math.max(120, Math.round(minWidth / shown)),
        sortable: true,
        ...(numeric ? { align: 'right' as const, headerAlign: 'right' as const, sortComparator: compareNumericText } : {}),
        valueGetter: (_value, row: { __row: DataTableRowValue }) => {
          const v = row.__row[key];
          return v == null ? '' : String(v);
        },
        renderCell: (params: GridRenderCellParams) => {
          const raw = (params.row as { __row: DataTableRowValue }).__row[key];
          if (isMissing(raw)) {
            return <Box component="span" sx={{ color: 'text.secondary' }}>—</Box>;
          }
          const text = String(raw);
          const content = renderCell ? renderCell(raw, key) : text;
          return (
            <Tooltip title={text} disableInteractive>
              <Box component="span" sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {content}
              </Box>
            </Tooltip>
          );
        },
      };
    });
  }, [keys, maxColumns, minWidth, renderCell, rows]);

  if (!rows?.length) {
    return <EmptyState dense icon={<TableRowsOutlinedIcon />} title={empty} primaryAction={emptyAction} />;
  }

  const gridRows = rows.map((row, idx) => ({ __id: idx, __row: row }));
  const visibleColumns = keys.filter((_, i) => visibilityModel[`c${i}`] !== false).length;

  const pageSize = 25;
  const visible = Math.min(rows.length, pageSize);
  const height = Math.min(
    HEADER_HEIGHT + TOOLBAR_HEIGHT + FOOTER_HEIGHT + visible * ROW_HEIGHT + 4,
    520,
  );

  return (
    <Stack spacing={1} sx={{ width: '100%', minWidth: 0 }}>
      {visibleColumns < keys.length && (
        <Typography variant="caption" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums' }}>
          Showing {count.format(visibleColumns)} of {count.format(keys.length)} columns. Choose more under Columns in the toolbar.
        </Typography>
      )}
      <Box sx={{ height, width: '100%' }}>
        <DataGrid
          rows={gridRows}
          columns={columns}
          getRowId={(r) => (r as { __id: number }).__id}
          columnVisibilityModel={visibilityModel}
          onColumnVisibilityModelChange={(model) => setVisibilityState({ signature, model })}
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
            // Header type matches the theme's table head (caption size, 600, secondary ink).
            '& .MuiDataGrid-columnHeader': { color: 'text.secondary' },
            '& .MuiDataGrid-columnHeaderTitle': { fontSize: 'caption.fontSize', fontWeight: 'fontWeightBold' },
            '& .MuiDataGrid-cell--textRight': { fontVariantNumeric: 'tabular-nums' },
          }}
        />
      </Box>
    </Stack>
  );
}
