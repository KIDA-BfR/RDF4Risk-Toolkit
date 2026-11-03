export type ServiceId =
  | 'home'
  | 'matching_table_generator'
  | 'semi_automatic_reconciliation'
  | 'agent_reconciliation'
  | 'rdf_generator'
  | 'rdf_to_table';

export type ToolId = Exclude<ServiceId, 'home'>;

export type ServiceMeta = {
  id: ServiceId;
  // The one name used everywhere: drawer, page title, overview, tooltips, history.
  title: string;
  // One line: what the tool is for. Shown on the overview.
  summary: string;
  // The ⓘ popover next to the page title: what happens here and what you get.
  details: string[];
};

export const services: ServiceMeta[] = [
  {
    id: 'home',
    title: 'Overview',
    summary: 'From research tables to connected, reusable Linked Data.',
    details: ['Start with a table in Prepare, or open any tool directly if you already have prepared data.'],
  },
  {
    id: 'matching_table_generator',
    title: 'Matching table',
    summary: 'Pick the values that need ontology terms and clean them.',
    details: [
      'Load a CSV or Excel table, choose which columns and values to link to vocabularies, and clean them (split, expand, merge near-duplicates).',
      'The result is an SSSOM-ready matching table that the reconciliation tools pick up.',
    ],
  },
  {
    id: 'semi_automatic_reconciliation',
    title: 'Manual curation',
    summary: 'Search vocabularies yourself and confirm each match.',
    details: [
      'Query providers such as Wikidata, BioPortal or your own ontology files, compare the candidates side by side and choose the best match for every term.',
      'Best when you want full control or the term list is short.',
    ],
  },
  {
    id: 'agent_reconciliation',
    title: 'AI-assisted matching',
    summary: 'Let LLM agents propose matches, then review them.',
    details: [
      'Agents retrieve candidates from BioPortal and Wikidata, rank them and propose a SKOS match type with a confidence score.',
      'You review, accept or reject every suggestion; nothing is used without your decision. Exports SSSOM.',
    ],
  },
  {
    id: 'rdf_generator',
    title: 'RDF generator',
    summary: 'Combine data and mappings into RDF, SKOS and DCAT.',
    details: [
      'Bring your data table and the reconciled mapping table together, choose how subjects, graphs and properties are built, and generate RDF.',
      'Optional: URI enrichment, schema templates, a publication reference and a DCAT catalog.',
    ],
  },
  {
    id: 'rdf_to_table',
    title: 'RDF inspector',
    summary: 'Explore the generated graph and export tables.',
    details: [
      'Load a TriG catalog (for example the one from the RDF generator), browse subjects, metadata and statistics.',
      'Export the result as Excel, CSV or Markdown documentation.',
    ],
  },
];

export function serviceMeta(id: ServiceId): ServiceMeta {
  return services.find((service) => service.id === id) ?? services[0];
}

// The pipeline, in order. Reconcile has two alternative tools, not two consecutive steps.
export const workflowPhases: { title: string; summary: string; services: ToolId[] }[] = [
  { title: 'Prepare', summary: 'Choose and clean the terms in your table.', services: ['matching_table_generator'] },
  { title: 'Reconcile', summary: 'Map every term to a trusted vocabulary.', services: ['semi_automatic_reconciliation', 'agent_reconciliation'] },
  { title: 'Generate', summary: 'Turn data and mappings into RDF.', services: ['rdf_generator'] },
  { title: 'Inspect & export', summary: 'Explore the graph and export tables.', services: ['rdf_to_table'] },
];

export type ReconciliationTool = 'semi_automatic_reconciliation' | 'agent_reconciliation';

// Where Back (from a tool's first stage) and Continue (from its last stage) lead, so the two
// buttons keep one meaning across the whole pipeline. Continue out of Prepare and Reconcile is a
// backend hand-off owned by those tools (their WorkflowNav passes its own onClick), so nextTool
// lists plain navigation only — a missing hand-off can never be skipped by a default.
export function previousTool(id: ServiceId, lastReconciliation: ReconciliationTool): ServiceId | null {
  switch (id) {
    case 'matching_table_generator': return 'home';
    case 'semi_automatic_reconciliation':
    case 'agent_reconciliation': return 'matching_table_generator';
    case 'rdf_generator': return lastReconciliation;
    case 'rdf_to_table': return 'rdf_generator';
    default: return null;
  }
}

export function nextTool(id: ServiceId): ServiceId | null {
  return id === 'rdf_generator' ? 'rdf_to_table' : null;
}

export function serviceFromHash(hash = window.location.hash): ServiceId {
  const raw = hash.replace(/^#\/?/, '') as ServiceId;
  return services.some((service) => service.id === raw) ? raw : 'home';
}
