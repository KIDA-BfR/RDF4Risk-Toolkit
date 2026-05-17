export type ServiceId =
  | 'home'
  | 'matching_table_generator'
  | 'semi_automatic_reconciliation'
  | 'agent_reconciliation'
  | 'rdf_generator'
  | 'rdf_to_table';

export type ServiceMeta = {
  id: ServiceId;
  step: string;
  title: string;
  short: string;
  description: string;
  accent: string;
};

export const services: ServiceMeta[] = [
  { id: 'home', step: '00', title: 'Home', short: 'Toolkit overview', description: 'Start page with direct links to all RDF4Risk workflow services.', accent: '#0f172a' },
  { id: 'matching_table_generator', step: '01', title: 'Matching Table Service', short: 'Prepare mappings', description: 'Load tabular data, preprocess values, consolidate terms, and generate SSSOM-ready matching tables.', accent: '#2563eb' },
  { id: 'semi_automatic_reconciliation', step: '02', title: 'Reconciliation Service', short: 'Manual curation', description: 'Use provider queues, ontology filters, and custom searches to reconcile terms against external authorities.', accent: '#0891b2' },
  { id: 'agent_reconciliation', step: '03', title: 'Agent-Based Reconciliation', short: 'AI-assisted matching', description: 'Run LLM-assisted semantic reconciliation with review, telemetry, SSSOM export, and ChatGPT subscription auth status.', accent: '#7c3aed' },
  { id: 'rdf_generator', step: '04', title: 'RDF Generator Service', short: 'Generate RDF', description: 'Combine data and mapping tables, enrich URI context, apply schema templates, and generate RDF/SKOS/DCAT outputs.', accent: '#14b8a6' },
  { id: 'rdf_to_table', step: '05', title: 'RDF to Table Service', short: 'Inspect RDF', description: 'Load TriG catalogs, inspect named graphs and statistics, then export CSV, Excel, and Markdown documentation.', accent: '#f59e0b' },
];

export function serviceFromHash(hash = window.location.hash): ServiceId {
  const raw = hash.replace(/^#\/?/, '') as ServiceId;
  return services.some((service) => service.id === raw) ? raw : 'home';
}
