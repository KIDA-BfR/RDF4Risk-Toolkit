import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, CircularProgress, Dialog, DialogContent, DialogTitle, FormControlLabel, IconButton, Link, Paper, Stack, Switch, Table, TableBody, TableCell, TableHead, TableRow, Tooltip, Typography, useMediaQuery, useTheme } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import TableViewOutlinedIcon from '@mui/icons-material/TableViewOutlined';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { fonts } from '../../theme/tokens';
import { DataTable } from '../../components/table/DataTable';
import { fetchRdfDataTable, type RdfDataTable } from '../../standalone/backendClient';

// Shape sent by RDF_Generator/graph_overview.py (snapshot.outputs.overview).
export type OverviewCell = { text: string; iri: string | null; iris?: string[] };
type RowCell = OverviewCell & { label: string };
type SimpleLayer = { present: boolean; rows: RowCell[]; triples: number; iri?: string | null };
type DataLayer = { present: boolean; iri?: string | null; triples: number; subjects?: number; classes?: OverviewCell[]; columns?: OverviewCell[]; rows?: { subject: OverviewCell; cells: OverviewCell[] }[] };
type VocabularyLayer = { present: boolean; iri?: string | null; triples: number; concepts?: number; rows?: { concept: OverviewCell; match: OverviewCell | null; match_type: string }[] };
export type OverviewLink = { id: string; from: string; to: string; property: string; status: 'ok' | 'missing' | 'off'; detail: string };
export type Overview = { error?: string; layers?: { metadata: SimpleLayer; publication: SimpleLayer; dataset: DataLayer; vocabulary: VocabularyLayer }; links?: OverviewLink[] };

const numberFormat = new Intl.NumberFormat();
const statusColor = { ok: 'success.main', missing: 'warning.main', off: 'text.disabled' } as const;

// A value with its IRI: the readable label by default (IRI on hover), the IRI itself when toggled.
function Value({ cell, showIris }: { cell: OverviewCell; showIris: boolean }) {
  if (cell.iris && showIris) return <Stack spacing={0.25}>{cell.iris.map((iri) => <IriLink key={iri} iri={iri} />)}</Stack>;
  if (!cell.iri) return <>{cell.text || '—'}</>;
  if (showIris) return <IriLink iri={cell.iri} />;
  return <Tooltip title={cell.iri}><Box component="span" sx={{ textDecoration: 'underline dotted', textDecorationColor: 'currentColor', textUnderlineOffset: 3 }}>{cell.text || cell.iri}</Box></Tooltip>;
}

function IriLink({ iri }: { iri: string }) {
  const external = /^https?:\/\//.test(iri);
  return <Link href={external ? iri : undefined} target="_blank" rel="noopener noreferrer" underline="hover" sx={{ fontFamily: fonts.mono, fontSize: '0.75rem', overflowWrap: 'anywhere' }}>{iri}</Link>;
}

const cellSx = { py: 0.5, px: 1, verticalAlign: 'top', fontSize: '0.8125rem' } as const;

function LayerCard({ area, title, kind, layer, counts, showIris, empty, children }: { area: string; title: string; kind: string; layer: { present: boolean; iri?: string | null }; counts?: string; showIris: boolean; empty: string; children?: React.ReactNode }) {
  return <Paper variant="outlined" sx={{ gridArea: area, minWidth: 0, p: 1.5, opacity: layer.present ? 1 : 0.7, borderStyle: layer.present ? 'solid' : 'dashed' }} aria-label={title}>
    <Stack direction="row" alignItems="baseline" justifyContent="space-between" flexWrap="wrap" useFlexGap gap={1}>
      <Box sx={{ minWidth: 0 }}>
        <Typography variant="subtitle2" component="h3">{title}</Typography>
        <Typography variant="caption" color="text.secondary">{kind}</Typography>
      </Box>
      {counts && <Typography variant="caption" color="text.secondary" sx={{ fontVariantNumeric: 'tabular-nums' }}>{counts}</Typography>}
    </Stack>
    {layer.present && layer.iri && showIris && <Box sx={{ mt: 0.5 }}><IriLink iri={layer.iri} /></Box>}
    <Box sx={{ mt: 1, overflowX: 'auto' }}>
      {layer.present ? children : <Typography variant="body2" color="text.secondary">{empty}</Typography>}
    </Box>
  </Paper>;
}

function KeyValueTable({ rows, showIris }: { rows: RowCell[]; showIris: boolean }) {
  return <Table size="small"><TableBody>{rows.map((row) => <TableRow key={row.label}>
    <TableCell component="th" scope="row" sx={{ ...cellSx, color: 'text.secondary', whiteSpace: 'nowrap', width: '1%' }}>{row.label}</TableCell>
    <TableCell sx={cellSx}><Value cell={row} showIris={showIris} /></TableCell>
  </TableRow>)}</TableBody></Table>;
}

// A line between two cards, labelled with the RDF property it stands for. Vertical on narrow
// screens; `horizontalOnWide` lays it out horizontally from md up.
function Connector({ area, link, horizontalOnWide = false }: { area: string; link?: OverviewLink; horizontalOnWide?: boolean }) {
  if (!link) return null;
  const color = statusColor[link.status];
  const lineStyle = link.status === 'ok' ? 'solid' : 'dashed';
  const segment = horizontalOnWide
    ? { flex: 1, alignSelf: 'center', borderColor: color, borderStyle: lineStyle, borderWidth: 0, borderLeftWidth: { xs: 2, md: 0 }, borderTopWidth: { xs: 0, md: 2 }, minHeight: { xs: 12, md: 0 }, minWidth: { xs: 0, md: 12 } }
    : { flex: 1, alignSelf: 'center', borderColor: color, borderStyle: lineStyle, borderWidth: 0, borderLeftWidth: 2, minHeight: 12 };
  const Icon = link.status === 'ok' ? CheckCircleOutlineIcon : link.status === 'missing' ? WarningAmberIcon : RemoveCircleOutlineIcon;
  return <Box sx={{ gridArea: area, display: 'flex', flexDirection: horizontalOnWide ? { xs: 'column', md: 'row' } : 'column', alignItems: 'stretch', minWidth: 0, minHeight: 56 }}>
    <Box sx={segment} />
    <Tooltip title={link.detail}>
      <Stack direction="row" spacing={0.5} alignItems="center" sx={{ alignSelf: 'center', px: 1, py: 0.25, my: 0.5, mx: horizontalOnWide ? { md: 0.5 } : 0, border: 1, borderColor: color, borderRadius: 1, bgcolor: 'background.paper', maxWidth: '100%' }} tabIndex={0} aria-label={`${link.property}: ${link.detail}`}>
        <Icon sx={{ fontSize: 16, color }} />
        <Typography variant="caption" sx={{ fontFamily: fonts.mono, overflowWrap: 'anywhere' }}>{link.property}</Typography>
      </Stack>
    </Tooltip>
    <Box sx={segment} />
  </Box>;
}

// "Open full table": every subject with every property, fetched from the backend when opened.
function FullDataTableButton({ subjects, showIris: initialShowIris, version }: { subjects: number; showIris: boolean; version: string }) {
  const theme = useTheme();
  const narrow = useMediaQuery(theme.breakpoints.down('md'));
  const [open, setOpen] = useState(false);
  const [showIris, setShowIris] = useState(initialShowIris);
  const [state, setState] = useState<{ loading: boolean; error?: string; table?: RdfDataTable; version?: string }>({ loading: false });
  useEffect(() => {
    if (!open || state.version === version) return;
    let cancelled = false;
    setState({ loading: true });
    fetchRdfDataTable().then(
      (table) => { if (!cancelled) setState({ loading: false, table, version }); },
      (error) => { if (!cancelled) setState({ loading: false, error: error instanceof Error ? error.message : String(error) }); },
    );
    return () => { cancelled = true; };
  }, [open, version]);
  // Rows keyed by column header; values follow the label/IRI switch, so search and filter match what is shown.
  const rows = useMemo(() => {
    const table = state.table;
    if (!table) return [];
    const seen = new Map<string, number>();
    const headers = table.columns.map((column) => { const n = (seen.get(column.text) ?? 0) + 1; seen.set(column.text, n); return n > 1 ? `${column.text} (${n})` : column.text; });
    return table.rows.map((row) => Object.fromEntries(table.columns.map((column, index) => {
      const cell = row[column.key];
      return [headers[index], cell ? (showIris && cell.iri ? cell.iri : cell.text) : ''];
    })));
  }, [state.table, showIris]);
  const renderCell = (value: unknown) => {
    const text = String(value ?? '');
    if (showIris && /^https?:\/\//.test(text)) return <Stack spacing={0.25} sx={{ py: 0.5 }}>{text.split('; ').map((iri) => <IriLink key={iri} iri={iri} />)}</Stack>;
    return text;
  };
  return <>
    <Button size="small" startIcon={<TableViewOutlinedIcon />} onClick={() => { setShowIris(initialShowIris); setOpen(true); }}>Open full table</Button>
    <Dialog open={open} onClose={() => setOpen(false)} fullScreen={narrow} fullWidth maxWidth="xl" aria-labelledby="rdf-full-table-title">
      <DialogTitle id="rdf-full-table-title" sx={{ pr: 7 }}>
        <Stack direction="row" alignItems="center" justifyContent="space-between" flexWrap="wrap" useFlexGap gap={1}>
          <Box>
            <Typography variant="h6" component="span">Data graph</Typography>
            <Typography variant="body2" color="text.secondary">{numberFormat.format(subjects)} subjects, one row each; one column per property.</Typography>
          </Box>
          <FormControlLabel sx={{ mr: 0 }} control={<Switch size="small" checked={showIris} onChange={(event) => setShowIris(event.target.checked)} />} label="Show IRIs" />
        </Stack>
        <IconButton aria-label="Close" onClick={() => setOpen(false)} sx={{ position: 'absolute', right: 8, top: 8 }}><CloseIcon /></IconButton>
      </DialogTitle>
      <DialogContent dividers>
        {state.loading && <Stack direction="row" spacing={1} alignItems="center" sx={{ py: 4, justifyContent: 'center' }}><CircularProgress size={20} /><Typography variant="body2">Loading the data graph…</Typography></Stack>}
        {state.error && <Alert severity="error">Could not load the table: {state.error}</Alert>}
        {state.table && !state.loading && <Stack spacing={1.5}>
          {state.table.truncated && <Alert severity="info">Showing the first {numberFormat.format(state.table.rows.length)} of {numberFormat.format(state.table.total)} subjects. Download the data graph on the Export step for all of them.</Alert>}
          <DataTable rows={rows} empty="The data graph has no subjects." maxColumns={12} renderCell={renderCell} />
        </Stack>}
      </DialogContent>
    </Dialog>
  </>;
}

export function ResultOverview({ overview, showIris }: { overview: Overview; showIris: boolean }) {
  const layers = overview.layers;
  if (!layers) return null;
  const link = (id: string) => overview.links?.find((item) => item.id === id);
  const { metadata, publication, dataset, vocabulary } = layers;
  const problems = (overview.links ?? []).filter((item) => item.status === 'missing');
  return <Stack spacing={2}>
    <Box sx={{
      display: 'grid',
      gridTemplateColumns: { xs: 'minmax(0, 1fr)', md: 'minmax(0, 1fr) minmax(140px, 200px) minmax(0, 1fr)' },
      gridTemplateAreas: {
        // Narrow: one column, publication → metadata → data → vocabulary.
        xs: '"pub" "v1" "meta" "h" "data" "v2" "voc"',
        // Wide: metadata ─ data on top, publication and vocabulary below their partner.
        md: '"meta h data" "v1 . v2" "pub . voc"',
      },
      alignItems: 'stretch',
    }}>
      <LayerCard area="meta" title="Dataset metadata" kind="DCAT catalog" layer={metadata} counts={metadata.present ? `${numberFormat.format(metadata.triples)} triples` : undefined} showIris={showIris} empty="No catalog yet. Complete Describe dataset, then generate.">
        <KeyValueTable rows={metadata.rows} showIris={showIris} />
      </LayerCard>
      <Connector area="h" link={link('metadata-dataset')} horizontalOnWide />
      <LayerCard area="data" title="Data graph" kind="Named graph" layer={dataset} counts={dataset.present ? `${numberFormat.format(dataset.subjects ?? 0)} subjects · ${numberFormat.format(dataset.triples)} triples` : undefined} showIris={showIris} empty="Not generated yet.">
        {(dataset.classes?.length ?? 0) > 0 && <Stack direction="row" spacing={0.5} alignItems="center" sx={{ mb: 1 }}><Typography variant="caption" color="text.secondary">Type</Typography>{dataset.classes!.map((cls) => <Chip key={cls.iri ?? cls.text} size="small" variant="outlined" label={<Value cell={cls} showIris={showIris} />} />)}</Stack>}
        <Table size="small">
          <TableHead><TableRow>
            <TableCell sx={{ ...cellSx, fontWeight: 600 }}>Subject</TableCell>
            {(dataset.columns ?? []).map((column) => <TableCell key={column.iri ?? column.text} sx={{ ...cellSx, fontWeight: 600 }}><Value cell={column} showIris={showIris} /></TableCell>)}
          </TableRow></TableHead>
          <TableBody>{(dataset.rows ?? []).map((row) => <TableRow key={row.subject.iri ?? row.subject.text}>
            <TableCell sx={cellSx}><Value cell={row.subject} showIris={showIris} /></TableCell>
            {row.cells.map((cell, index) => <TableCell key={index} sx={cellSx}><Value cell={cell} showIris={showIris} /></TableCell>)}
          </TableRow>)}</TableBody>
        </Table>
        <Stack direction="row" alignItems="center" justifyContent="space-between" flexWrap="wrap" useFlexGap gap={1} sx={{ mt: 1 }}>
          <Typography variant="caption" color="text.secondary">{(dataset.subjects ?? 0) > (dataset.rows?.length ?? 0) ? `First ${dataset.rows?.length} of ${numberFormat.format(dataset.subjects ?? 0)} subjects.` : ''}</Typography>
          <FullDataTableButton subjects={dataset.subjects ?? 0} showIris={showIris} version={`${dataset.iri}|${dataset.triples}`} />
        </Stack>
      </LayerCard>
      <Connector area="v1" link={link('metadata-publication')} />
      <Connector area="v2" link={link('dataset-vocabulary')} />
      <LayerCard area="pub" title="Publication" kind="SemOpenAlex work" layer={publication} counts={publication.present ? `${numberFormat.format(publication.triples)} triples` : undefined} showIris={showIris} empty="No publication attached.">
        <KeyValueTable rows={publication.rows} showIris={showIris} />
      </LayerCard>
      <LayerCard area="voc" title="Vocabulary" kind="SKOS concepts" layer={vocabulary} counts={vocabulary.present ? `${numberFormat.format(vocabulary.concepts ?? 0)} concepts · ${numberFormat.format(vocabulary.triples)} triples` : undefined} showIris={showIris} empty="Not generated yet.">
        <Table size="small">
          <TableHead><TableRow><TableCell sx={{ ...cellSx, fontWeight: 600 }}>Concept</TableCell><TableCell sx={{ ...cellSx, fontWeight: 600 }}>Mapped to</TableCell></TableRow></TableHead>
          <TableBody>{(vocabulary.rows ?? []).map((row) => <TableRow key={row.concept.iri ?? row.concept.text}>
            <TableCell sx={cellSx}><Value cell={row.concept} showIris={showIris} /></TableCell>
            <TableCell sx={cellSx}>{row.match ? <Stack spacing={0.25}><Value cell={row.match} showIris={showIris} /><Typography variant="caption" color="text.secondary" sx={{ fontFamily: fonts.mono }}>skos:{row.match_type}</Typography></Stack> : '—'}</TableCell>
          </TableRow>)}</TableBody>
        </Table>
        {(vocabulary.concepts ?? 0) > (vocabulary.rows?.length ?? 0) && <Typography variant="caption" color="text.secondary">First {vocabulary.rows?.length} of {numberFormat.format(vocabulary.concepts ?? 0)} concepts.</Typography>}
      </LayerCard>
    </Box>
    {problems.length > 0 && <Box component="ul" sx={{ m: 0, pl: 2.5, color: 'warning.main' }}>{problems.map((item) => <li key={item.id}><Typography variant="body2" component="span">{item.detail}</Typography></li>)}</Box>}
  </Stack>;
}
