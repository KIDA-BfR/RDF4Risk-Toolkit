import React from 'react';
import { Box, Card, Chip, Container, Paper, Stack, Typography, alpha } from '@mui/material';

type Tool = {
  step: string;
  title: string;
  caption: string;
  description: string;
  page: string;
  accent: string;
  status: string;
  icon: string;
};

const tools: Tool[] = [
  {
    step: '01',
    title: 'Matching Table Service',
    caption: 'Prepare structured mappings',
    description:
      'Generate matching tables from your data sources by comparing and aligning datasets based on selected criteria. This prepares clean mappings for reconciliation and RDF generation.',
    page: 'Matching Table Generator Page',
    accent: '#2563eb',
    status: 'Prepare',
    icon: '≋',
  },
  {
    step: '02',
    title: 'Reconciliation Service',
    caption: 'Link terms to authorities',
    description:
      'Reconcile terms against external vocabularies and knowledge bases such as Wikidata and NCBI, enriching your data with authoritative Linked Data URIs.',
    page: 'Reconciliation Page',
    accent: '#0891b2',
    status: 'Reconcile',
    icon: '⌁',
  },
  {
    step: '03',
    title: 'Agent-Based Reconciliation Service',
    caption: 'AI-assisted ontology matching',
    description:
      'Use automated, agent-supported reconciliation for external ontologies and knowledge bases, including SKOS match suggestions and review-ready outputs for single files or batches.',
    page: 'Agent Based Reconciliation Page',
    accent: '#7c3aed',
    status: 'Automate',
    icon: '✦',
  },
  {
    step: '04',
    title: 'RDF Generator Service',
    caption: 'Transform tables into RDF',
    description:
      'Generate RDF from tabular data and mappings, turning structured information into an interoperable, machine-readable Linked Data representation.',
    page: 'RDF Generator Page',
    accent: '#14b8a6',
    status: 'Generate',
    icon: '◎',
  },
  {
    step: '05',
    title: 'RDF to Table Service',
    caption: 'Review and export RDF',
    description:
      'Explore TriG/RDF data in tabular form, extract metadata, and export readable documentation formats such as Excel, CSV, and Markdown.',
    page: 'RDF to Table Page',
    accent: '#f59e0b',
    status: 'Export',
    icon: '↧',
  },
];

function ToolCard({ tool }: { tool: Tool }) {
  return (
    <Card
      variant="outlined"
      sx={{
        position: 'relative',
        overflow: 'hidden',
        display: 'flex',
        flexDirection: 'column',
        minHeight: 268,
        p: { xs: 2.25, md: 2.75 },
        borderRadius: 4,
        borderColor: alpha('#94a3b8', 0.32),
        boxShadow: '0 16px 36px rgba(15, 23, 42, 0.07)',
        transition: 'transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease',
        '&::before': {
          content: '""',
          position: 'absolute',
          inset: '0 0 auto 0',
          height: 5,
          background: `linear-gradient(90deg, ${tool.accent}, ${alpha('#14b8a6', 0.38)})`,
        },
        '&:hover': {
          transform: 'translateY(-3px)',
          boxShadow: '0 22px 52px rgba(15, 23, 42, 0.12)',
          borderColor: alpha(tool.accent, 0.35),
        },
      }}
    >
      <Stack direction="row" alignItems="flex-start" justifyContent="space-between" spacing={2}>
        <Box
          sx={{
            display: 'inline-flex',
            width: 54,
            height: 54,
            alignItems: 'center',
            justifyContent: 'center',
            borderRadius: 3,
            color: tool.accent,
            bgcolor: alpha(tool.accent, 0.08),
            fontSize: '1.7rem',
            fontWeight: 900,
            boxShadow: `inset 0 0 0 1px ${alpha(tool.accent, 0.2)}`,
          }}
        >
          {tool.icon}
        </Box>
        <Chip
          label={`Step ${tool.step} · ${tool.status}`}
          size="small"
          sx={{
            color: tool.accent,
            bgcolor: alpha(tool.accent, 0.08),
            border: `1px solid ${alpha(tool.accent, 0.2)}`,
            fontWeight: 850,
          }}
        />
      </Stack>

      <Typography component="h3" sx={{ mt: 2.25, mb: 0.75, color: '#0f172a', fontSize: '1.32rem', lineHeight: 1.25, fontWeight: 900, letterSpacing: '-0.025em' }}>
        {tool.title}
      </Typography>
      <Typography sx={{ mb: 1.75, color: tool.accent, fontSize: '.84rem', fontWeight: 850, textTransform: 'uppercase', letterSpacing: '.07em' }}>
        {tool.caption}
      </Typography>
      <Typography color="text.secondary" sx={{ lineHeight: 1.65, fontSize: '.98rem' }}>
        {tool.description}
      </Typography>

      <Stack direction="row" alignItems="center" justifyContent="space-between" spacing={1.5} sx={{ mt: 'auto', pt: 2.5, borderTop: '1px solid #e2e8f0' }}>
        <Typography color="text.secondary" sx={{ fontSize: '.9rem' }}>
          Open via <Box component="strong" sx={{ color: '#334155' }}>{tool.page}</Box>
        </Typography>
        <Box
          sx={{
            display: 'inline-flex',
            width: 34,
            height: 34,
            alignItems: 'center',
            justifyContent: 'center',
            borderRadius: '50%',
            color: '#fff',
            bgcolor: tool.accent,
            boxShadow: `0 8px 18px ${alpha(tool.accent, 0.25)}`,
            fontWeight: 900,
            flex: '0 0 auto',
          }}
        >
          →
        </Box>
      </Stack>
    </Card>
  );
}

export function HomeApp() {

  return (
    <Box sx={{ bgcolor: '#eef7fb', minHeight: '100vh', color: 'text.primary', py: { xs: 2, md: 3 } }}>
      <Container maxWidth="xl">
        <Stack spacing={3.5}>
          <Paper
            variant="outlined"
            sx={{
              position: 'relative',
              overflow: 'hidden',
              borderRadius: 5,
              p: { xs: 3, md: 6 },
              background: 'linear-gradient(135deg, #ffffff 0%, #f0fdfa 48%, #eff6ff 100%)',
              borderColor: alpha('#94a3b8', 0.28),
              boxShadow: '0 18px 48px rgba(15, 23, 42, 0.08)',
              '&::after': {
                content: '""',
                position: 'absolute',
                width: 360,
                height: 360,
                right: -120,
                top: -140,
                borderRadius: 999,
                background: 'radial-gradient(circle, rgba(20, 184, 166, 0.24) 0%, rgba(37, 99, 235, 0.08) 56%, transparent 72%)',
                pointerEvents: 'none',
              },
            }}
          >
            <Stack spacing={2.5} sx={{ position: 'relative', zIndex: 1 }}>
              <Chip
                label="Linked Data Workflow · RDF4Risk"
                sx={{
                  alignSelf: 'flex-start',
                  px: 1,
                  color: '#0369a1',
                  bgcolor: alpha('#0ea5e9', 0.1),
                  border: `1px solid ${alpha('#0ea5e9', 0.22)}`,
                  fontSize: '.78rem',
                  fontWeight: 800,
                  letterSpacing: '.08em',
                  textTransform: 'uppercase',
                }}
              />
              <Typography
                variant="h1"
                sx={{
                  maxWidth: 920,
                  fontSize: 'clamp(2.5rem, 6vw, 5rem)',
                  lineHeight: 0.96,
                  letterSpacing: '-0.055em',
                  fontWeight: 900,
                }}
              >
                Welcome to the{' '}
                <Box component="span" sx={{ background: 'linear-gradient(90deg, #2563eb, #0891b2, #14b8a6)', WebkitBackgroundClip: 'text', backgroundClip: 'text', color: 'transparent' }}>
                  RDF4Risk Toolkit
                </Box>
              </Typography>
              <Typography color="text.secondary" sx={{ maxWidth: 1050, fontSize: 'clamp(1rem, 1.6vw, 1.22rem)', lineHeight: 1.75 }}>
                RDF4Risk brings together practical tools for turning tabular research data into FAIR Linked Data for risk assessment and life sciences. Prepare matching tables, reconcile terms with trusted vocabularies, generate RDF, and review or export results through one guided workflow workspace.
              </Typography>
              <Stack direction="row" flexWrap="wrap" gap={1.5} sx={{ pt: 1 }}>
                <Chip label={<><Box component="strong" sx={{ color: '#0f766e', mr: 1 }}>5</Box> workflow services</>} sx={{ bgcolor: '#fff', border: `1px solid ${alpha('#94a3b8', 0.35)}`, boxShadow: '0 8px 22px rgba(15, 23, 42, 0.06)', color: '#334155' }} />
                <Chip label="Guided workflow workspace" sx={{ bgcolor: '#fff', border: `1px solid ${alpha('#94a3b8', 0.35)}`, boxShadow: '0 8px 22px rgba(15, 23, 42, 0.06)', color: '#334155' }} />
                <Chip label="Linked Data generation pipeline" sx={{ bgcolor: '#fff', border: `1px solid ${alpha('#94a3b8', 0.35)}`, boxShadow: '0 8px 22px rgba(15, 23, 42, 0.06)', color: '#334155' }} />
              </Stack>
            </Stack>
          </Paper>

          <Box>
            <Typography variant="h2" sx={{ fontSize: 'clamp(1.75rem, 3vw, 2.45rem)', letterSpacing: '-0.04em', fontWeight: 900, mb: 0.75 }}>
              Available Tools
            </Typography>
            <Typography color="text.secondary" sx={{ maxWidth: 720, lineHeight: 1.6, fontSize: '1.02rem' }}>
              Choose the service that matches your current workflow step, from table preparation through reconciliation, RDF generation, and export.
            </Typography>
          </Box>

          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: 'repeat(2, minmax(0, 1fr))' }, gap: 2.25 }}>
            {tools.map((tool) => <ToolCard key={tool.step} tool={tool} />)}
          </Box>

          <Paper variant="outlined" sx={{ p: { xs: 2.25, md: 2.75 }, borderRadius: 4, borderColor: alpha('#2563eb', 0.18), boxShadow: '0 12px 28px rgba(15, 23, 42, 0.06)' }}>
            <Stack direction={{ xs: 'column', md: 'row' }} alignItems={{ xs: 'flex-start', md: 'center' }} justifyContent="space-between" spacing={2}>
              <Box>
                <Typography sx={{ fontSize: '1.05rem', fontWeight: 900, mb: 0.5 }}>Ready to begin?</Typography>
                <Typography color="text.secondary">Select the first workflow step in the sidebar, or jump directly to the service you need.</Typography>
              </Box>
              <Chip label="Use the sidebar navigation →" sx={{ color: '#fff', bgcolor: '#2563eb', backgroundImage: 'linear-gradient(90deg, #2563eb, #0891b2)', fontWeight: 850, px: 1, boxShadow: '0 10px 24px rgba(37, 99, 235, 0.25)' }} />
            </Stack>
          </Paper>
        </Stack>
      </Container>
    </Box>
  );
}
