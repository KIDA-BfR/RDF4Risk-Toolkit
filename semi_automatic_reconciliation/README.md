# Semi-Automatic Reconciliation Service

The active workflow supports:

* loading a matching table from the Matching Table Generator handoff or direct CSV/XLSX/XLS upload,
* selecting reconciliation providers and ontology filters,
* indexing local ontology/thesaurus files,
* configuring a custom SPARQL endpoint,
* processing candidate suggestions,
* reviewing and curating mappings with optional SKOS match types,
* exporting SSSOM and candidate-review CSV files,
* publishing the finalized `shared_reconciled_matching_table` handoff for the RDF Generator.

## Data contract

The active workflow expects the canonical SSSOM matching-table columns:

* `subject_id`
* `subject_label`
* `predicate_id`
* `object_id`
* `object_label`
* `mapping_justification`

Legacy columns such as `Term`, `URI`, `RDF Role`, and `Match Type` are still normalized through `shared_table_io.py` where supported.

## Configuration

Provider credentials and endpoints are read from a local `config.yaml`. On first setup, copy the tracked placeholder file and then fill in local values as needed:

```bash
cp config.example.yaml config.yaml
```

The local `config.yaml` is intentionally ignored by Git so API keys, endpoints, and machine-specific provider choices are not committed. Environment-variable overrides are also supported for sensitive values such as:

* `NCBI_API_KEY`
* `BIOPORTAL_API_KEY`
* `AGROPORTAL_API_KEY`
* `EARTHPORTAL_API_KEY`

Do not commit real API keys or secrets.
