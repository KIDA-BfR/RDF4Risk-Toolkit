# CSV Reconciliation Tool (PoC)

## Overview

This application is a Proof-of-Concept (PoC) tool built with Streamlit designed to help users reconcile terms from a CSV file against various external knowledge bases and controlled vocabularies. Users can upload a CSV, select data providers (like Wikidata, NCBI, BioPortal, etc., including a custom SPARQL endpoint), fetch potential matches (suggestions) for terms lacking URIs, review these suggestions using ranking strategies, and select the appropriate URI to enrich their data. The final reconciled data can then be downloaded.

The toolkit now also includes a separate **Agent-Based Reconciliation** page in the main sidebar. This new workflow is designed for AI-assisted, review-first reconciliation that preserves the same matching-table schema (`Term`, `URI`, `RDF Role`, `Match Type`) while adding optional definition preparation, agent-supported candidate selection, and SKOS match suggestions before accepted results are handed off to the RDF Generator.

## Features

*   **CSV Upload:** Upload a CSV file containing terms to be reconciled.
*   **Required Columns:** Expects specific columns: `Term`, `URI`, `RDF Role`, `Match Type`.
*   **Multiple Data Providers:** Supports reconciliation against:
    *   Wikidata
    *   NCBI (Taxonomy, BioProject, Gene, etc.)
    *   BioPortal
    *   OLS (Ontology Lookup Service @ EBI)
    *   NCI Thesaurus
    *   GeoNames
    *   AGROVOC
    *   Custom SPARQL Endpoint
*   **Processing Queue:** Add selected providers to a queue for sequential processing.
*   **Suggestion Fetching:** Automatically fetches potential URI matches for terms with an empty 'URI' field.
*   **Matching/Ranking Strategies:** View and sort suggestions based on:
    *   **API Ranking:** The default order returned by the provider's API.
    *   **Levenshtein Similarity:** Sorts suggestions based on string edit distance between the input term and the suggestion label.
*   **Interactive Selection:** Review suggestions term-by-term for each provider and select the correct URI or "No Match".
*   **Dynamic Updates:** The main data table updates immediately upon selection.
*   **Progress Monitoring:** View the status and progress of the processing queue and term lookups in the sidebar.
*   **Custom SPARQL Configuration:** Define your own SPARQL endpoint and query template for custom data sources.
*   **Configuration File:** Manage API keys, usernames, and service endpoints via `config.yaml`.
*   **Download Results:** Download the updated CSV file with the reconciled URIs.

## Agent-Based Reconciliation

The new **Agent-Based Reconciliation** workflow complements the existing manual page rather than replacing it.

### Current capabilities

*   Loads matching tables from the Matching Table Generator session handoff or from direct CSV/XLSX uploads.
*   Preserves the existing downstream handoff contract through `shared_reconciled_matching_table` and `all_terms_for_reconciliation`.
*   Supports multiple input tables in one run, with a source selector for reviewing results per file.
*   Supports several definition strategies:
    *   use an existing definition column
    *   upload a separate `Term` / `Definition` sheet
    *   generate definitions from user-provided context text
*   Provides two workflow modes:
    *   **Wikidata Deep Agent**
    *   **BioPortal + Wikidata Multi-Agent**
*   Provides **provider dropdowns** and **model dropdowns** for major LLM vendors, with live model fetching where API credentials are available.
*   Generates suggested URI, provider, label, description, and SKOS match type columns that can be accepted or rejected before final publication.

### Additional dependencies and configuration

The agent-based workflow requires the LLM/agent dependencies listed in the project `requirements.txt`, especially:

*   `openai`
*   `pydantic>=2`
*   `langchain`
*   `langchain-openai`
*   `langgraph`
*   `deepagents`

It expects an API key to be present in the environment variable configured in `agentic_reconciliation/config.yaml` under `agent_reconciliation.model_api_key_env` (and `agent_reconciliation.definition_model_api_key_env` for definition generation). A legacy alias (`openai_api_key_env`) is still accepted for backward compatibility.

Supported provider dropdowns currently include:

*   **OpenAI**
*   **OpenAI Compatible (OpenWebUI / LM Studio)**
*   **ChatGPT Subscription (OpenAI Codex)**
*   **Anthropic**
*   **Google Gemini**

For **OpenAI Compatible**, the Agent UI exposes:

*   Base URL (for example `http://localhost:1234` or your OpenWebUI endpoint)
*   Optional API key

When saved via **Save current provider/model as preferred defaults**, the values are persisted under `agent_reconciliation` in `agentic_reconciliation/config.yaml` as:

*   `openai_compatible_base_url`
*   `provider_api_keys.openai_compatible` (if provided)

The app calls `<base_url>/v1/models` for model discovery and `<base_url>/v1/chat/completions` for inference (it also accepts base URLs already ending in `/v1`).

The page attempts to fetch current model lists from each provider when credentials (or, for OpenAI Compatible, endpoint settings) are available. If live fetching is not possible, it falls back to curated default model lists so the UI remains usable.

### ChatGPT Subscription (OpenAI Codex) sign-in

The agent page now supports Cline-style ChatGPT subscription usage for Codex models via OAuth (no separate per-request API billing in this app flow, subject to OpenAI account policy).

How it works in this project:

*   Select **ChatGPT Subscription (OpenAI Codex)** as provider in agent settings.
*   Open **ChatGPT Subscription Authentication** and click **Sign in with ChatGPT**.
*   Complete browser login and return to Streamlit.
*   Tokens are stored locally at `~/.rdf4risk/openai_codex_oauth.json`.
*   Agent requests are sent to `https://chatgpt.com/backend-api/codex/responses` using refreshed OAuth access tokens.

Important constraints:

*   This path relies on private/undocumented OpenAI endpoints and may break if upstream behavior changes.
*   It should be treated as **experimental** and monitored closely in production environments.

### Notes

*   The manual reconciliation page remains the primary provider-driven workflow.
*   The agent-based page is additive and uses the same canonical matching-table contract so downstream RDF generation continues to work.
*   Heavy RAG-based notebook workflows from the research prototype are intentionally not part of the initial in-app implementation.

## Prerequisites

*   Python 3.10.16
*   pip (Python package installer)

## Setup Instructions

1.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

2.  **Install Dependencies:** Place the file named `requirements.txt` in the same directory with the following content:

    Then, install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure `config.yaml`:** Place the file named `config.yaml` in the same directory and **fill in your own API keys, usernames, and email address** where indicated (`YourAPIKey`, `YourUsername`, `your-email@example.com`).

    **Warning:** Do not commit your API keys or sensitive information to public repositories. Use environment variables or other secure methods for production deployments.

## Running the Application

1.  Navigate to the application directory in your terminal (where `reconciliation_app.py` is located).
2.  Make sure your (virtual) environment is activated.
3.  Run the Streamlit application:
    ```bash
    streamlit run reconciliation_app.py
    ```
4.  The application should open automatically in your web browser.

## Usage Guide

1.  **Upload CSV:**
    *   Click the "Browse files" button under "1. Upload CSV File".
    *   Select your CSV file. It **must** contain columns named `Term`, `URI`, `RDF Role`, and `Match Type`. UTF-8 encoding is recommended.
    *   The application will parse the CSV and display the data in the "Current Data" section. It identifies terms where the `URI` column is empty – these are the terms that need reconciliation.

2.  **Configure in Sidebar:** Use the sidebar on the left for configuration and control:
    *   **Reconciliation Sources:** Check the boxes next to the data providers (Wikidata, NCBI, etc.) you want to use for finding matches. Tooltips provide a brief description of each provider.
    *   **Custom SPARQL Provider:**
        *   If you want to use your own SPARQL endpoint, check "Enable Custom SPARQL Provider".
        *   Enter the full SPARQL Endpoint URL.
        *   Provide the SPARQL Query Template. Use `{term}` as a placeholder for the search term and `{limit}` for the maximum number of results. Ensure your query selects variables for URI, Label, and optionally Description.
        *   Specify the exact variable names used in your query (defaults are `uri`, `label`, `description`).
        *   If enabled, select "Custom SPARQL" from the Reconciliation Sources list.
    *   **Add to Queue:** Click the "Add / Re-queue Selected" button to add the checked providers to the processing queue. The queue status will update below. Re-adding a provider will clear its previous results and queue it again.
    *   **Matching Strategy:** Select how suggestions should be ranked/sorted when displayed:
        *   `API Ranking`: Default order from the source.
        *   `Levenshtein Similarity`: Best string match first.
    *   **Query Settings:** Adjust the "Max Suggestions per Term" slider to control how many potential matches are requested from each provider for each term.
    *   **Process Control:**
        *   Once providers are added to the queue and configuration is complete (including required API keys/usernames in `config.yaml`), click "Start Processing Queue".
        *   The button will be disabled if prerequisites are missing (e.g., empty queue, missing config for a selected provider).
        *   Progress bars for provider processing and term lookup will update in the sidebar. Processing happens sequentially through the queue.

3.  **View Provider Results:**
    *   After processing starts/completes/errors, buttons representing each processed provider will appear under the "View Provider Results" section in the main area.
    *   Icons indicate status (✅ Completed, ❌ Error, ⚙️ Running, 🛑 Stopped).
    *   Click a provider button to load its suggestions into the "Select Suggestions" area below.

4.  **Select Suggestions:**
    *   This section appears after you click a provider button. It lists each term needing reconciliation.
    *   For each term, a dropdown menu shows the suggestions found by the selected provider, formatted and ranked according to the chosen "Matching Strategy".
    *   The score is shown in brackets `[...]` if applicable for the strategy.
    *   Select the most appropriate URI from the dropdown.
    *   Choose `--- No Match ---` if none of the suggestions are correct.
    *   The description of the selected suggestion (if available) is shown below the dropdown.
    *   Your selection immediately updates the `URI` column in the main "Current Data" table.

5.  **Download Reconciled Data:**
    *   Once you have finished reviewing and selecting URIs, scroll down to the "Download Reconciled Data" section.
    *   Click the "Download 'filename_reconciled.csv'" button to save the updated CSV file. `No Match` selections will be saved as empty strings in the 'URI' column.

## Input CSV Format

Your input CSV file **must** contain the following columns:

*   `Term`: The term/label/string you want to reconcile.
*   `URI`: The corresponding URI for the term. Leave this **empty** for terms that need reconciliation. The tool will fill this column based on your selections.
*   `RDF Role`: (Contextual) An RDF property or role associated with the term (e.g., `predicate`, `object`). This column is currently informational for the user but not directly used in the matching logic.
*   `Match Type`: (Contextual) The type of match expected or desired (e.g., `Exact`, `Close`). This column is currently informational for the user but not directly used in the matching logic.

Troubleshooting / Notes

    API Keys/Usernames: Ensure you have correctly entered valid API keys/usernames in config.yaml for the providers you intend to use (NCBI, BioPortal, GeoNames). Also ensure the email for NCBI is provided.

    Missing Modules: If you get ImportError for processing_service, make sure the .py files are present in the same directory as app.py.

    Dependencies: Ensure all packages in requirements.txt are installed in your active Python environment.

    Rate Limits: Be mindful of API rate limits imposed by external providers. Processing large files quickly might hit these limits, causing errors. The tool processes providers and terms sequentially.

    Custom SPARQL: Double-check your endpoint URL and SPARQL query syntax. Ensure the query template correctly uses {term} and {limit} and that the variable names match those specified in the sidebar settings.

    CSV Parsing: If the CSV fails to load, check its encoding (UTF-8 recommended) and delimiter (the app tries common ones like comma, semicolon, tab).