# -*- coding: utf-8 -*-
import io
import os
import re
import time
import html
import json
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yaml

try:
    from .agent_codex_subscription_service import (
        clear_codex_credentials,
        get_codex_auth_status,
        get_pending_codex_authorization_url,
        is_codex_authenticated,
        start_codex_authorization_flow,
    )
    from .agent_definition_service import (
        build_definition_lookup,
        extract_reference_publication_text,
        normalize_uploaded_definitions,
        prepare_used_definitions_df,
    )
    from .agent_llm_service import (
        generate_text_completion,
        fetch_available_model_catalog,
        fetch_available_models,
        get_default_api_key_env,
        get_default_model_options,
        get_provider_label,
        is_openai_compatible_auth_required_error,
        get_supported_llm_providers,
    )
    from .agent_pricing_service import fetch_all_pricing
    from .agent_file_service import load_uploaded_input_tables, make_input_table, read_matching_table_upload
    from .agent_models import AgentInputTable, AgentRunConfig
    from .agent_langsmith_monitoring import (
        build_run_url,
        configure_langsmith_environment,
        get_langsmith_readiness,
        get_llm_interactions,
        reset_llm_interactions,
    )
    from .agent_orchestrator import run_agent_batch
    from .agent_skos_service import normalize_mapping_type
    from semi_automatic_reconciliation.reconciliation_utils import CONFIG, create_download_link
    from semi_automatic_reconciliation.shared_table_io import (
        LEXICAL_MAPPING_JUSTIFICATION,
        LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION,
        REVIEW_MAPPING_JUSTIFICATION,
        SEMANTIC_MAPPING_JUSTIFICATION,
        LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS,
        apply_mapping_justification_for_row,
        REQUIRED_MATCHING_TABLE_COLUMNS,
        ensure_agent_output_columns,
        extract_all_terms_for_reconciliation,
        get_unreconciled_indices,
        prepare_loaded_matching_table,
        finalize_accepted_results,
        sync_matching_table_schemas,
        reorder_reconciliation_columns,
    )
except ImportError:
    from agent_codex_subscription_service import (
        clear_codex_credentials,
        get_codex_auth_status,
        get_pending_codex_authorization_url,
        is_codex_authenticated,
        start_codex_authorization_flow,
    )
    from agent_definition_service import (
        build_definition_lookup,
        extract_reference_publication_text,
        normalize_uploaded_definitions,
        prepare_used_definitions_df,
    )
    from agent_llm_service import (
        generate_text_completion,
        fetch_available_model_catalog,
        fetch_available_models,
        get_default_api_key_env,
        get_default_model_options,
        get_provider_label,
        is_openai_compatible_auth_required_error,
        get_supported_llm_providers,
    )
    from agent_pricing_service import fetch_all_pricing
    from agent_file_service import load_uploaded_input_tables, make_input_table, read_matching_table_upload
    from agent_models import AgentInputTable, AgentRunConfig
    from agent_langsmith_monitoring import (
        build_run_url,
        configure_langsmith_environment,
        get_langsmith_readiness,
        get_llm_interactions,
        reset_llm_interactions,
    )
    from agent_orchestrator import run_agent_batch
    from agent_skos_service import normalize_mapping_type
    from semi_automatic_reconciliation.reconciliation_utils import CONFIG, create_download_link
    from semi_automatic_reconciliation.shared_table_io import (
        LEXICAL_MAPPING_JUSTIFICATION,
        LEXICAL_SIMILARITY_THRESHOLD_MAPPING_JUSTIFICATION,
        REVIEW_MAPPING_JUSTIFICATION,
        SEMANTIC_MAPPING_JUSTIFICATION,
        LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS,
        apply_mapping_justification_for_row,
        REQUIRED_MATCHING_TABLE_COLUMNS,
        ensure_agent_output_columns,
        extract_all_terms_for_reconciliation,
        get_unreconciled_indices,
        prepare_loaded_matching_table,
        finalize_accepted_results,
        sync_matching_table_schemas,
        reorder_reconciliation_columns,
    )


AGENT_DATAFRAME_STATE_KEY = "agent_reconciliation_df"
AGENT_DATA_SOURCE_MESSAGE_KEY = "agent_reconciliation_source_message"
AGENT_LAST_SOURCE_NAME_KEY = "agent_reconciliation_last_source_name"
AGENT_INPUT_TABLES_KEY = "agent_reconciliation_input_tables"
AGENT_RESULTS_BY_SOURCE_KEY = "agent_reconciliation_results_by_source"
AGENT_SELECTED_SOURCE_KEY = "agent_reconciliation_selected_source"
AGENT_DEFINITIONS_BY_SOURCE_KEY = "agent_reconciliation_definitions_by_source"
AGENT_RUN_MESSAGES_KEY = "agent_reconciliation_run_messages"
AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY = "agent_reconciliation_available_models_by_provider"
AGENT_MONITORING_STATE_KEY = "agent_reconciliation_monitoring_state"
AGENT_STOP_EVENT_KEY = "agent_reconciliation_stop_event"
AGENT_UPLOADED_SOURCE_SIGNATURE_KEY = "agent_reconciliation_uploaded_source_signature"
AGENT_WORKFLOW_CONFIG_STATE_KEY = "agent_workflow_config_json"
AGENT_ACTIVE_STEP_KEY = "agent_reconciliation_active_step"
AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY = "agent_workflow_component_action_nonce"
AGENT_RUN_STATUS_STATE_KEY = "agent_reconciliation_run_status"
AGENT_SSSOM_EXPORT_PAYLOAD_KEY = "agent_reconciliation_sssom_export_payload"

API_KEY_PLACEHOLDERS = {
    "",
    "yourapikey",
    "your_api_key",
    "replace-with-api-key",
    "replace_with_api_key",
    "replace-me",
    "changeme",
    "none",
    "null",
    "<api_key>",
}

OPENAI_CODEX_PROVIDER = "openai_codex"
OPENAI_COMPATIBLE_PROVIDER = "openai_compatible"
OPENAI_COMPATIBLE_BASE_URL_ENV = "OPENAI_COMPATIBLE_BASE_URL"
OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY = "openai_compatible_base_url"
OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY = "openai_compatible_model_registry"
OPENAI_COMPATIBLE_MODEL_REGISTRY_MAX_ITEMS = 50
ORCID_BASE_URL = "https://orcid.org/"
LEGACY_AGENT_MODEL_CONFIG_KEYS = (
    "model_provider",
    "model_name",
    "definition_model_name",
    "planner_model_provider",
    "planner_model_name",
    "planner_model_api_key_env",
)
REASONING_EFFORT_OPTIONS = ["none", "low", "medium", "high", "xhigh"]

REVIEW_MATCH_GROUP_ORDER = (
    "skos:exactMatch",
    "skos:closeMatch",
    "skos:relatedMatch",
    "no_match",
)

REVIEW_MATCH_TYPE_OPTIONS = (
    "skos:exactMatch",
    "skos:closeMatch",
    "skos:relatedMatch",
)

REVIEW_MATCH_GROUP_LABELS = {
    "skos:exactMatch": "skos:exactMatch",
    "skos:closeMatch": "skos:closeMatch",
    "skos:relatedMatch": "skos:relatedMatch",
    "no_match": "No match",
}

REVIEW_MATCH_GROUP_BADGE_COLORS = {
    "skos:exactMatch": ("#dcfce7", "#166534"),
    "skos:closeMatch": ("#fef3c7", "#92400e"),
    "skos:relatedMatch": ("#dbeafe", "#1d4ed8"),
    "no_match": ("#f3f4f6", "#374151"),
}


def _escape_html(value: object) -> str:
    """Escape small UI values used in custom Streamlit HTML blocks."""
    return html.escape(str(value or ""), quote=True)


def _render_agent_reconciliation_visual_theme():
    """Inject a restrained Uiverse-inspired blue/teal visual system for this page."""
    st.markdown(
        """
        <script>
            window.parent.scrollToTop = function() {
                const mainContent = window.parent.document.querySelector('section.main');
                if (mainContent) {
                    mainContent.scrollTo({ top: 0, behavior: 'auto' });
                } else {
                    window.parent.scrollTo({ top: 0, behavior: 'auto' });
                }
            };
        </script>
        <style>
            :root {
                --agent-ink: #0f172a;
                --agent-muted: #64748b;
                --agent-panel: rgba(255, 255, 255, 0.86);
                --agent-panel-strong: rgba(255, 255, 255, 0.96);
                --agent-border: rgba(15, 118, 110, 0.16);
                --agent-border-strong: rgba(20, 184, 166, 0.38);
                --agent-blue: #2563eb;
                --agent-teal: #14b8a6;
                --agent-cyan: #67e8f9;
                --agent-dark: #071319;
                --agent-dark-2: #0d1f2a;
                --agent-shadow: 0 22px 55px rgba(2, 24, 43, 0.14);
            }

            [data-testid="stAppViewContainer"] > .main {
                background: #FFFFFF;
            }

            section.main > div.block-container {
                max-width: 1380px;
                padding-top: 2.15rem;
                padding-bottom: 4rem;
            }

            .agent-hero {
                position: relative;
                overflow: hidden;
                border-radius: 22px;
                padding: clamp(1rem, 2vw, 1.45rem);
                margin: 0.15rem 0 0.95rem;
                color: #0f172a;
                background:
                    linear-gradient(135deg, rgba(255, 255, 255, 0.97), rgba(240, 253, 250, 0.93) 52%, rgba(239, 246, 255, 0.94));
                border: 1px solid rgba(15, 118, 110, 0.14);
                box-shadow: 0 14px 34px rgba(2, 24, 43, 0.08);
            }

            .agent-hero::before {
                content: "";
                position: absolute;
                pointer-events: none;
                border-radius: 999px;
                width: 14rem;
                height: 14rem;
                right: -6rem;
                top: -7rem;
                background: radial-gradient(circle, rgba(20, 184, 166, 0.13), transparent 66%);
            }

            .agent-hero-inner { position: relative; z-index: 1; }
            .agent-eyebrow {
                display: inline-flex;
                align-items: center;
                gap: 0.55rem;
                border: 1px solid rgba(20, 184, 166, 0.20);
                border-radius: 999px;
                padding: 0.28rem 0.62rem;
                color: #0f766e;
                background: rgba(240, 253, 250, 0.82);
                font-size: 0.70rem;
                font-weight: 700;
                letter-spacing: 0.10em;
                text-transform: uppercase;
            }

            .agent-pulse-dot {
                width: 0.54rem;
                height: 0.54rem;
                border-radius: 999px;
                background: var(--agent-teal);
            }

            .agent-hero h1 {
                margin: 0.7rem 0 0.45rem;
                max-width: 760px;
                font-size: clamp(1.75rem, 3.3vw, 3.05rem);
                line-height: 1.08;
                letter-spacing: -0.042em;
                font-weight: 780;
                color: #0f172a;
            }

            .agent-hero-copy {
                max-width: 780px;
                margin: 0;
                color: #536579;
                font-size: 0.98rem;
                line-height: 1.58;
            }

            .agent-chip-row,
            .agent-status-grid,
            .agent-metric-grid {
                display: grid;
                gap: 0.85rem;
            }

            .agent-chip-row {
                display: flex;
                flex-wrap: wrap;
                margin-top: 1rem;
            }

            .agent-chip {
                display: inline-flex;
                align-items: center;
                gap: 0.45rem;
                border-radius: 999px;
                padding: 0.42rem 0.68rem;
                color: #334155;
                background: rgba(255, 255, 255, 0.70);
                border: 1px solid rgba(15, 118, 110, 0.14);
                font-size: 0.82rem;
            }

            .agent-nav-stepper {
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 0.65rem;
                margin: 0.85rem 0 1.2rem;
                padding: 0.75rem;
                border: 1px solid rgba(15, 118, 110, 0.14);
                border-radius: 18px;
                background: rgba(255, 255, 255, 0.78);
                box-shadow: 0 12px 28px rgba(2, 24, 43, 0.06);
            }

            .agent-nav-step {
                display: inline-flex;
                align-items: center;
                gap: 0.45rem;
                border-radius: 999px;
                padding: 0.42rem 0.78rem;
                color: #475569;
                background: #f8fafc;
                border: 1px solid rgba(100, 116, 139, 0.16);
                font-weight: 650;
            }

            .agent-nav-step b {
                display: inline-grid;
                place-items: center;
                width: 1.45rem;
                height: 1.45rem;
                border-radius: 999px;
                background: #e2e8f0;
                color: #334155;
                font-size: 0.78rem;
            }

            .agent-nav-step.active {
                color: #1d4ed8;
                background: rgba(37, 99, 235, 0.08);
                border-color: rgba(37, 99, 235, 0.25);
            }

            .agent-nav-step.active b {
                color: #eff6ff;
                background: linear-gradient(135deg, var(--agent-blue), var(--agent-teal));
            }

            .agent-status-grid,
            .agent-metric-grid {
                grid-template-columns: repeat(4, minmax(0, 1fr));
                margin: 1rem 0 1.45rem;
            }

            .agent-status-card,
            .agent-metric-card,
            .agent-contract-card,
            .agent-run-readiness {
                position: relative;
                border: 1px solid var(--agent-border);
                border-radius: 22px;
                background: var(--agent-panel);
                box-shadow: var(--agent-shadow);
                backdrop-filter: blur(18px);
            }

            .agent-status-card,
            .agent-metric-card {
                padding: 1rem 1.05rem;
                transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
            }

            .agent-status-card:hover,
            .agent-metric-card:hover,
            div[data-testid="stExpander"]:hover {
                transform: translateY(-2px);
                border-color: var(--agent-border-strong) !important;
            }

            .agent-card-kicker {
                color: var(--agent-muted);
                font-size: 0.74rem;
                font-weight: 800;
                letter-spacing: 0.11em;
                text-transform: uppercase;
            }

            .agent-card-value {
                margin-top: 0.28rem;
                color: var(--agent-ink);
                font-size: 1.5rem;
                line-height: 1.15;
                font-weight: 820;
                letter-spacing: -0.035em;
            }

            .agent-card-note {
                margin-top: 0.36rem;
                color: var(--agent-muted);
                font-size: 0.84rem;
                line-height: 1.4;
            }

            .agent-section-title {
                display: flex;
                align-items: flex-start;
                gap: 0.9rem;
                margin: 1.85rem 0 0.75rem;
                padding-top: 0.2rem;
            }

            .agent-section-step {
                flex: 0 0 auto;
                display: inline-grid;
                place-items: center;
                width: 2.35rem;
                height: 2.35rem;
                border-radius: 14px;
                color: #ecfeff;
                background: linear-gradient(135deg, var(--agent-blue), var(--agent-teal));
                box-shadow: 0 12px 26px rgba(20, 184, 166, 0.28);
                font-weight: 820;
            }

            .agent-section-title h2 {
                margin: 0;
                color: #0f172a;
                font-size: 1.28rem;
                letter-spacing: -0.025em;
            }

            .agent-section-title p {
                margin: 0.25rem 0 0;
                color: #64748b;
                line-height: 1.45;
            }

            .agent-contract-card {
                padding: 1rem 1.15rem;
                margin-bottom: 0.95rem;
                overflow: hidden;
            }

            .agent-contract-card::before,
            .agent-run-readiness::before {
                content: "";
                position: absolute;
                inset: 0 auto 0 0;
                width: 4px;
                background: linear-gradient(180deg, var(--agent-blue), var(--agent-teal));
            }

            .agent-contract-list {
                display: flex;
                flex-wrap: wrap;
                gap: 0.55rem;
                margin: 0.85rem 0;
            }

            .agent-contract-list code {
                border-radius: 999px;
                border: 1px solid rgba(20, 184, 166, 0.22);
                background: rgba(20, 184, 166, 0.08);
                color: #0f766e;
                padding: 0.34rem 0.58rem;
                font-size: 0.82rem;
            }

            .agent-tooltip {
                position: relative;
                display: inline-grid;
                place-items: center;
                width: 1.15rem;
                height: 1.15rem;
                margin-left: 0.35rem;
                border-radius: 999px;
                color: #0f766e;
                border: 1px solid rgba(20, 184, 166, 0.32);
                background: rgba(20, 184, 166, 0.09);
                font-size: 0.75rem;
                font-weight: 800;
                cursor: help;
            }

            .agent-tooltip::after {
                content: attr(data-tooltip);
                position: absolute;
                left: 50%;
                bottom: calc(100% + 0.7rem);
                width: min(19rem, 70vw);
                transform: translateX(-50%) translateY(4px);
                padding: 0.7rem 0.8rem;
                border-radius: 14px;
                color: #e6fffb;
                background: rgba(7, 19, 25, 0.96);
                border: 1px solid rgba(103, 232, 249, 0.24);
                box-shadow: 0 16px 35px rgba(2, 12, 27, 0.28);
                font-size: 0.78rem;
                line-height: 1.45;
                opacity: 0;
                visibility: hidden;
                pointer-events: none;
                transition: opacity 150ms ease, transform 150ms ease;
                z-index: 1000;
            }

            .agent-tooltip:hover::after {
                opacity: 1;
                visibility: visible;
                transform: translateX(-50%) translateY(0);
            }

            .agent-workflow-card {
                min-height: 186px;
                padding: 1.25rem;
                border-radius: 22px;
                border: 1px solid rgba(15, 118, 110, 0.16);
                background:
                    linear-gradient(180deg, rgba(255, 255, 255, 0.97), rgba(242, 250, 252, 0.94));
                box-shadow: 0 18px 40px rgba(2, 24, 43, 0.09);
                transition: transform 170ms ease, border-color 170ms ease, box-shadow 170ms ease;
            }

            .agent-workflow-card.active {
                border-color: rgba(20, 184, 166, 0.52);
                box-shadow: 0 20px 55px rgba(20, 184, 166, 0.16);
                background:
                    linear-gradient(135deg, rgba(240, 253, 250, 0.99), rgba(239, 246, 255, 0.98));
            }

            .agent-workflow-card:hover { transform: translateY(-3px); }
            .agent-workflow-title {
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 0.5rem;
                color: #0f172a;
                font-size: 1.08rem;
                font-weight: 820;
                letter-spacing: -0.02em;
                margin-bottom: 0.72rem;
            }

            .agent-workflow-badge {
                border-radius: 999px;
                padding: 0.24rem 0.58rem;
                color: #0f766e;
                background: rgba(20, 184, 166, 0.10);
                border: 1px solid rgba(20, 184, 166, 0.20);
                font-size: 0.68rem;
                font-weight: 820;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }

            .agent-workflow-badge.badge-fast {
                color: #1d4ed8;
                background: rgba(37, 99, 235, 0.09);
                border-color: rgba(37, 99, 235, 0.18);
            }

            .agent-workflow-badge.badge-domain {
                color: #0f766e;
                background: rgba(20, 184, 166, 0.11);
                border-color: rgba(20, 184, 166, 0.22);
            }

            .agent-workflow-desc {
                color: #5b6b7d;
                font-size: 0.92rem;
                line-height: 1.58;
            }

            .agent-run-readiness {
                display: flex;
                align-items: center;
                gap: 0.95rem;
                padding: 1rem 1.1rem 1rem 1.25rem;
                margin: 0.55rem 0 0.9rem;
                overflow: hidden;
            }

            .agent-loader {
                flex: 0 0 auto;
                width: 2.3rem;
                height: 2.3rem;
                border-radius: 999px;
                border: 3px solid rgba(20, 184, 166, 0.16);
                border-top-color: var(--agent-teal);
                border-right-color: var(--agent-blue);
                animation: agentSpin 1.15s linear infinite;
            }

            .agent-loader.paused {
                animation-play-state: paused;
                opacity: 0.46;
                filter: grayscale(0.35);
            }

            @keyframes agentSpin { to { transform: rotate(360deg); } }

            .agent-run-title {
                color: #0f172a;
                font-weight: 820;
                letter-spacing: -0.02em;
            }

            .agent-run-note {
                margin-top: 0.12rem;
                color: #64748b;
                font-size: 0.88rem;
            }

            div[data-testid="stExpander"] {
                border: 1px solid var(--agent-border) !important;
                border-radius: 18px !important;
                background: rgba(255, 255, 255, 0.78) !important;
                box-shadow: 0 16px 36px rgba(2, 24, 43, 0.08);
                overflow: hidden;
                transition: transform 160ms ease, border-color 160ms ease;
            }

            div[data-testid="stExpander"] details summary {
                background: linear-gradient(90deg, rgba(20, 184, 166, 0.08), rgba(37, 99, 235, 0.055));
                font-weight: 760;
                color: #0f172a;
            }

            .stCheckbox {
                padding: 0.62rem 0.75rem;
                border: 1px solid rgba(15, 118, 110, 0.13);
                border-radius: 15px;
                background: rgba(255, 255, 255, 0.72);
                transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
            }

            .stCheckbox:hover {
                transform: translateY(-1px);
                border-color: rgba(20, 184, 166, 0.36);
                background: rgba(240, 253, 250, 0.72);
            }

            .stCheckbox label [data-testid="stMarkdownContainer"] p {
                color: #203044;
                font-weight: 650;
                line-height: 1.32;
            }

            .stButton > button,
            div[data-testid="stDownloadButton"] button {
                border-radius: 13px !important;
                border: 1px solid rgba(20, 184, 166, 0.28) !important;
                background: linear-gradient(135deg, #2563eb, #0faaa2) !important;
                color: #ecfeff !important;
                font-weight: 760 !important;
                box-shadow: 0 12px 26px rgba(37, 99, 235, 0.16) !important;
                transition: transform 140ms ease, box-shadow 140ms ease, filter 140ms ease !important;
            }

            .stButton > button:hover,
            div[data-testid="stDownloadButton"] button:hover {
                transform: translateY(-1px);
                filter: saturate(1.05);
                box-shadow: 0 16px 34px rgba(20, 184, 166, 0.22) !important;
            }

            .stButton > button:disabled {
                background: #d8e5ea !important;
                color: #78909c !important;
                border-color: rgba(100, 116, 139, 0.20) !important;
                box-shadow: none !important;
            }

            div[data-baseweb="select"] > div,
            div[data-baseweb="input"] > div,
            div[data-baseweb="textarea"] > div,
            div[data-testid="stFileUploader"] section {
                border-radius: 15px !important;
                border-color: rgba(15, 118, 110, 0.18) !important;
                background: rgba(255, 255, 255, 0.82) !important;
            }

            div[data-testid="stFileUploader"] section {
                border-style: dashed !important;
                box-shadow: inset 0 0 0 1px rgba(103, 232, 249, 0.06);
            }

            div[data-testid="stAlert"] {
                border-radius: 16px !important;
                border: 1px solid rgba(20, 184, 166, 0.18) !important;
                box-shadow: 0 12px 28px rgba(2, 24, 43, 0.07);
            }

            div[data-testid="stDataFrame"] {
                border-radius: 18px;
                overflow: hidden;
                border: 1px solid rgba(15, 118, 110, 0.12);
                box-shadow: 0 16px 36px rgba(2, 24, 43, 0.08);
            }

            div[data-testid="stProgress"] > div > div > div > div {
                background: linear-gradient(90deg, var(--agent-blue), var(--agent-teal)) !important;
            }

            hr { display: none !important; }

            @media (max-width: 920px) {
                .agent-status-grid,
                .agent-metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
                .agent-hero h1 { font-size: 2.2rem; }
            }

            @media (max-width: 620px) {
                .agent-status-grid,
                .agent-metric-grid { grid-template-columns: 1fr; }
                .agent-run-readiness { align-items: flex-start; }
            }

            @media (prefers-reduced-motion: reduce) {
                .agent-hero::before,
                .agent-loader {
                    animation: none !important;
                }
                * { transition-duration: 0.01ms !important; }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_agent_section_header(step: str, title: str, description: str, tooltip: str = ""):
    tooltip_html = ""
    if tooltip:
        tooltip_html = (
            f'<span class="agent-tooltip" data-tooltip="{_escape_html(tooltip)}">?</span>'
        )
    st.markdown(
        f"""
        <div class="agent-section-title">
            <div class="agent-section-step">{_escape_html(step)}</div>
            <div>
                <h2>{_escape_html(title)}{tooltip_html}</h2>
                <p>{_escape_html(description)}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_agent_hero(shared_df, agent_df, input_tables, results_by_source, required_columns):
    """Render compact session/source status without a prominent hero block."""
    shared_rows = len(shared_df) if isinstance(shared_df, pd.DataFrame) else 0
    working_rows = len(agent_df) if isinstance(agent_df, pd.DataFrame) else 0
    loaded_sources = len(input_tables) if isinstance(input_tables, list) else 0
    result_sources = len(results_by_source) if isinstance(results_by_source, dict) else 0
    schema_status = "Waiting"
    schema_note = "Load or upload a matching table"
    schema_df = None
    if isinstance(agent_df, pd.DataFrame):
        schema_df = agent_df
    elif isinstance(shared_df, pd.DataFrame):
        schema_df = shared_df
    elif isinstance(input_tables, list) and input_tables:
        first_table = input_tables[0]
        table_df = getattr(first_table, "dataframe", None)
        if isinstance(table_df, pd.DataFrame):
            schema_df = table_df

    if isinstance(schema_df, pd.DataFrame):
        has_required_schema = all(col in schema_df.columns for col in required_columns)
        has_legacy_schema = all(col in schema_df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS)
        if has_required_schema:
            schema_status = "Ready"
            schema_note = "Required SSSOM columns present"
        elif has_legacy_schema:
            schema_status = "Ready"
            schema_note = "Legacy columns will be normalized"
        else:
            missing_columns = [col for col in required_columns if col not in schema_df.columns]
            schema_status = "Missing"
            schema_note = f"{len(missing_columns)} required column(s) missing"

    st.markdown(
        f"""
        <div class="agent-status-grid">
            <div class="agent-status-card">
                <div class="agent-card-kicker">Shared table</div>
                <div class="agent-card-value">{shared_rows:,}</div>
                <div class="agent-card-note">row(s) detected in session state</div>
            </div>
            <div class="agent-status-card">
                <div class="agent-card-kicker">Schema status</div>
                <div class="agent-card-value">{_escape_html(schema_status)}</div>
                <div class="agent-card-note">{_escape_html(schema_note)}</div>
            </div>
            <div class="agent-status-card">
                <div class="agent-card-kicker">Loaded sources</div>
                <div class="agent-card-value">{loaded_sources:,}</div>
                <div class="agent-card-note">input source(s) prepared for agents</div>
            </div>
            <div class="agent-status-card">
                <div class="agent-card-kicker">Working table</div>
                <div class="agent-card-value">{working_rows:,}</div>
                <div class="agent-card-note">row(s), {result_sources:,} result source(s)</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_input_contract_card(required_columns):
    chips = "".join(f"<code>{_escape_html(column)}</code>" for column in required_columns)
    st.markdown(
        f"""
        <div class="agent-contract-card">
            <div class="agent-card-kicker">Strict SSSOM minimum</div>
            <div class="agent-contract-list">{chips}</div>
            <div class="agent-card-note">
                Legacy aliases (<code>Term</code>, <code>URI</code>, <code>Match Type</code>) remain supported internally,
                but this workflow is designed around the canonical matching-table contract.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_working_table_metrics(agent_df):
    pending = len(_get_reviewable_agent_result_indices(agent_df)) if isinstance(agent_df, pd.DataFrame) else 0
    unreconciled = len(st.session_state.get("agent_total_indices_to_process", []))
    accepted = 0
    auto_accepted = 0
    if isinstance(agent_df, pd.DataFrame) and "Review Status" in agent_df.columns:
        status_series = agent_df["Review Status"].astype(str).str.strip().str.lower()
        accepted = int((status_series == "accepted").sum())
    if isinstance(agent_df, pd.DataFrame) and "Auto Accepted" in agent_df.columns:
        auto_accepted = int(
            agent_df["Auto Accepted"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["true", "1", "yes"])
            .sum()
        )

    st.markdown(
        f"""
        <div class="agent-metric-grid">
            <div class="agent-metric-card">
                <div class="agent-card-kicker">Total rows</div>
                <div class="agent-card-value">{len(agent_df):,}</div>
                <div class="agent-card-note">currently loaded in the working table</div>
            </div>
            <div class="agent-metric-card">
                <div class="agent-card-kicker">Unreconciled</div>
                <div class="agent-card-value">{unreconciled:,}</div>
                <div class="agent-card-note">row(s) still queued for processing</div>
            </div>
            <div class="agent-metric-card">
                <div class="agent-card-kicker">Pending review</div>
                <div class="agent-card-value">{pending:,}</div>
                <div class="agent-card-note">suggestion card(s) awaiting a curator</div>
            </div>
            <div class="agent-metric-card">
                <div class="agent-card-kicker">Accepted</div>
                <div class="agent-card-value">{accepted:,}</div>
                <div class="agent-card-note">including {auto_accepted:,} policy auto-accepted row(s)</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

try:
    from .agent_reconciliation_ui_state import (
        _sync_selected_source_dataframe,
        _store_input_tables,
        _build_run_input_tables,
        _update_result_for_source,
        _reset_agent_state_and_load_df,
    )
    from .agent_reconciliation_ui_review import (
        _apply_review_action,
        _accept_all_pending,
        _normalize_review_match_group,
        _group_pending_review_indices_by_match_type,
        _get_reviewable_agent_result_indices,
        _get_review_cell_value,
        _render_skos_match_badge,
    )
    from .agent_reconciliation_ui_monitoring import (
        _build_monitoring_event_snapshot,
        _build_cascade_trace_snapshot,
        _render_monitoring_panel,
    )
except ImportError:
    from agent_reconciliation_ui_state import (
        _sync_selected_source_dataframe,
        _store_input_tables,
        _build_run_input_tables,
        _update_result_for_source,
        _reset_agent_state_and_load_df,
    )
    from agent_reconciliation_ui_review import (
        _apply_review_action,
        _accept_all_pending,
        _normalize_review_match_group,
        _group_pending_review_indices_by_match_type,
        _get_reviewable_agent_result_indices,
        _get_review_cell_value,
        _render_skos_match_badge,
    )
    from agent_reconciliation_ui_monitoring import (
        _build_monitoring_event_snapshot,
        _build_cascade_trace_snapshot,
        _render_monitoring_panel,
    )


def _get_reconciliation_config_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config.yaml",
    )


def _is_codex_provider(provider: Optional[str]) -> bool:
    return str(provider or "").strip() == OPENAI_CODEX_PROVIDER


def _is_openai_compatible_provider(provider: Optional[str]) -> bool:
    return str(provider or "").strip() == OPENAI_COMPATIBLE_PROVIDER


def _normalize_openai_compatible_base_url(raw_value: Optional[str]) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"http://{value}"
    return value.rstrip("/")


def _get_openai_compatible_base_url_from_config() -> str:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    return _normalize_openai_compatible_base_url(agent_cfg.get(OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY))


def _get_openai_compatible_registered_models_from_config() -> List[str]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    values = agent_cfg.get(OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY, [])
    if not isinstance(values, list):
        return []
    ordered: List[str] = []
    seen = set()
    for raw_value in values:
        model_id = str(raw_value or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        ordered.append(model_id)
    return ordered


def _normalize_openai_compatible_model_registry(values: List[str]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for raw_value in values:
        model_id = str(raw_value or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        ordered.append(model_id)
    return ordered[:OPENAI_COMPATIBLE_MODEL_REGISTRY_MAX_ITEMS]


def _merge_openai_compatible_model_registry(*model_groups: List[str]) -> List[str]:
    merged: List[str] = []
    for group in model_groups:
        if not isinstance(group, list):
            continue
        merged.extend(group)
    return _normalize_openai_compatible_model_registry(merged)


def _verify_openai_compatible_model_availability(model_name: str) -> tuple[bool, str]:
    candidate = str(model_name or "").strip()
    if not candidate:
        return False, "Model name is empty."

    if not _ensure_openai_compatible_base_url_available():
        return False, "OpenAI-compatible base URL is required before model verification."

    try:
        response = generate_text_completion(
            provider=OPENAI_COMPATIBLE_PROVIDER,
            model_name=candidate,
            system_prompt="Reply with exactly: OK",
            user_prompt="OK",
            api_key_env=get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER),
            temperature=0,
            max_tokens=8,
            interaction_purpose="model_registry_verification",
        )
        if str(response or "").strip():
            return True, "Model verified via OpenAI-compatible completion check."
        return False, "Verification call returned an empty response."
    except Exception as exc:
        if is_openai_compatible_auth_required_error(exc):
            env_name = get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            return False, (
                "OpenAI-compatible endpoint requires authentication for model verification. "
                f"Set `{env_name}` or provide the OpenAI-compatible API key field and try again."
            )
        return False, f"Model verification failed: {type(exc).__name__}: {exc}"


def _save_openai_compatible_model_registry(models: List[str]) -> tuple[bool, str]:
    normalized_models = _normalize_openai_compatible_model_registry(models)
    config_path = _get_reconciliation_config_path()
    try:
        loaded = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            loaded = {}

        agent_cfg = loaded.setdefault("agent_reconciliation", {})
        if not isinstance(agent_cfg, dict):
            agent_cfg = {}
            loaded["agent_reconciliation"] = agent_cfg

        agent_cfg[OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY] = normalized_models

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(loaded, f, sort_keys=False, allow_unicode=True)

        global CONFIG
        if not isinstance(CONFIG, dict):
            CONFIG = {}
        runtime_agent_cfg = CONFIG.setdefault("agent_reconciliation", {})
        if not isinstance(runtime_agent_cfg, dict):
            runtime_agent_cfg = {}
            CONFIG["agent_reconciliation"] = runtime_agent_cfg
        runtime_agent_cfg[OPENAI_COMPATIBLE_MODEL_REGISTRY_CONFIG_KEY] = normalized_models

        return True, "OpenAI-compatible model register saved to config.yaml."
    except Exception as exc:
        return False, f"Unable to save OpenAI-compatible model register: {exc}"


def _ensure_openai_compatible_base_url_available() -> bool:
    configured = _normalize_openai_compatible_base_url(
        st.session_state.get("agent_openai_compatible_base_url")
    )
    if configured:
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = configured
        return True

    env_value = _normalize_openai_compatible_base_url(os.getenv(OPENAI_COMPATIBLE_BASE_URL_ENV))
    if env_value:
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = env_value
        return True

    cfg_value = _get_openai_compatible_base_url_from_config()
    if cfg_value:
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = cfg_value
        return True

    return False


def _resolve_api_key_env_for_provider(
    session_key: Optional[str],
    provider: str,
    config_key: Optional[str] = None,
) -> str:
    """Resolve API key env name with provider-aware defaults, without mutating widget state."""
    if _is_codex_provider(provider):
        return get_default_api_key_env(provider)

    configured_default = get_default_api_key_env(provider)
    if config_key:
        configured_default = (CONFIG or {}).get("agent_reconciliation", {}).get(
            config_key,
            configured_default,
        )

    if session_key:
        env_name = str(st.session_state.get(session_key, configured_default) or "").strip()
    else:
        env_name = str(configured_default or "").strip()

    return env_name or get_default_api_key_env(provider)


def _save_preferred_model_selection(provider: str, model_name: str) -> tuple[bool, str]:
    provider_value = str(provider or "").strip()
    model_value = str(model_name or "").strip()
    if not provider_value or not model_value:
        return False, "Provider and model must be selected before saving preferences."

    config_path = _get_reconciliation_config_path()
    try:
        loaded = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            loaded = {}

        agent_cfg = loaded.setdefault("agent_reconciliation", {})
        if not isinstance(agent_cfg, dict):
            agent_cfg = {}
            loaded["agent_reconciliation"] = agent_cfg

        agent_cfg["preferred_model_provider"] = provider_value
        agent_cfg["preferred_model_name"] = model_value
        if _is_openai_compatible_provider(provider_value):
            compatible_base_url = _normalize_openai_compatible_base_url(
                st.session_state.get("agent_openai_compatible_base_url")
            )
            if compatible_base_url:
                agent_cfg[OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY] = compatible_base_url
            compatible_api_key = str(
                st.session_state.get("agent_openai_compatible_api_key", "")
            ).strip()
            if compatible_api_key:
                provider_keys_cfg = agent_cfg.get("provider_api_keys")
                if not isinstance(provider_keys_cfg, dict):
                    provider_keys_cfg = {}
                provider_keys_cfg[OPENAI_COMPATIBLE_PROVIDER] = compatible_api_key
                agent_cfg["provider_api_keys"] = provider_keys_cfg
        for legacy_key in LEGACY_AGENT_MODEL_CONFIG_KEYS:
            agent_cfg.pop(legacy_key, None)

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(loaded, f, sort_keys=False, allow_unicode=True)

        global CONFIG
        if not isinstance(CONFIG, dict):
            CONFIG = {}
        runtime_agent_cfg = CONFIG.setdefault("agent_reconciliation", {})
        if not isinstance(runtime_agent_cfg, dict):
            runtime_agent_cfg = {}
            CONFIG["agent_reconciliation"] = runtime_agent_cfg
        runtime_agent_cfg["preferred_model_provider"] = provider_value
        runtime_agent_cfg["preferred_model_name"] = model_value
        if _is_openai_compatible_provider(provider_value):
            compatible_base_url = _normalize_openai_compatible_base_url(
                st.session_state.get("agent_openai_compatible_base_url")
            )
            if compatible_base_url:
                runtime_agent_cfg[OPENAI_COMPATIBLE_BASE_URL_CONFIG_KEY] = compatible_base_url
                os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = compatible_base_url
            compatible_api_key = str(
                st.session_state.get("agent_openai_compatible_api_key", "")
            ).strip()
            if compatible_api_key:
                provider_keys_runtime = runtime_agent_cfg.get("provider_api_keys")
                if not isinstance(provider_keys_runtime, dict):
                    provider_keys_runtime = {}
                provider_keys_runtime[OPENAI_COMPATIBLE_PROVIDER] = compatible_api_key
                runtime_agent_cfg["provider_api_keys"] = provider_keys_runtime
                os.environ[get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)] = compatible_api_key
        for legacy_key in LEGACY_AGENT_MODEL_CONFIG_KEYS:
            runtime_agent_cfg.pop(legacy_key, None)

        return True, "Preferred model selection saved to config.yaml (API keys are kept in environment variables only)."
    except Exception as exc:
        return False, f"Unable to save preferred model selection: {exc}"


def _normalize_orcid_identifier(value: Optional[str]) -> str:
    """Normalize ORCID input to canonical https://orcid.org/<ID> URL format."""
    raw = str(value or "").strip()
    if not raw:
        return ""

    # Accept full URL and plain ORCID ID forms.
    if raw.lower().startswith("http://") or raw.lower().startswith("https://"):
        match = re.search(r"(\d{4}-\d{4}-\d{4}-[\dXx]{4})", raw)
    else:
        match = re.fullmatch(r"(\d{4}-\d{4}-\d{4}-[\dXx]{4})", raw)

    if not match:
        return ""

    normalized_id = match.group(1).upper()
    return f"{ORCID_BASE_URL}{normalized_id}"


def _get_provenance_defaults_from_config() -> Dict[str, str]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    defaults = agent_cfg.get("provenance_defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}
    return {
        "author_id": str(defaults.get("author_id", "") or "").strip(),
        "author_label": str(defaults.get("author_label", "") or "").strip(),
        "reviewer_id": str(defaults.get("reviewer_id", "") or "").strip(),
        "reviewer_label": str(defaults.get("reviewer_label", "") or "").strip(),
        "creator_id": str(defaults.get("creator_id", "") or "").strip(),
        "creator_label": str(defaults.get("creator_label", "") or "").strip(),
        "mapping_tool": str(defaults.get("mapping_tool", "RDF4Risk Agent-Based Reconciliation") or "").strip(),
        "mapping_tool_version": str(defaults.get("mapping_tool_version", "PoC") or "").strip(),
        "mapping_date": str(defaults.get("mapping_date", "") or "").strip(),
        "publication_date": str(defaults.get("publication_date", "") or "").strip(),
    }


def _build_provenance_defaults_from_state() -> Dict[str, str]:
    author_id = _normalize_orcid_identifier(st.session_state.get("agent_prov_author_orcid", ""))
    reviewer_id = _normalize_orcid_identifier(st.session_state.get("agent_prov_reviewer_orcid", ""))
    creator_id = _normalize_orcid_identifier(st.session_state.get("agent_prov_creator_orcid", ""))

    mapping_date_value = str(st.session_state.get("agent_prov_last_run_mapping_date", "") or "").strip() or date.today().isoformat()
    publication_date_value = str(st.session_state.get("agent_prov_publication_date", "") or "").strip()

    return {
        "author_id": author_id,
        "author_label": str(st.session_state.get("agent_prov_author_name", "") or "").strip(),
        "reviewer_id": reviewer_id,
        "reviewer_label": str(st.session_state.get("agent_prov_reviewer_name", "") or "").strip(),
        "creator_id": creator_id,
        "creator_label": str(st.session_state.get("agent_prov_creator_name", "") or "").strip(),
        "mapping_tool": str(st.session_state.get("agent_prov_mapping_tool", "") or "").strip(),
        "mapping_tool_version": str(st.session_state.get("agent_prov_mapping_tool_version", "") or "").strip(),
        "mapping_date": mapping_date_value,
        "publication_date": publication_date_value,
    }


def _save_preferred_provenance_defaults(defaults: Dict[str, str]) -> tuple[bool, str]:
    config_path = _get_reconciliation_config_path()
    try:
        loaded = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            loaded = {}

        agent_cfg = loaded.setdefault("agent_reconciliation", {})
        if not isinstance(agent_cfg, dict):
            agent_cfg = {}
            loaded["agent_reconciliation"] = agent_cfg

        agent_cfg["provenance_defaults"] = defaults

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(loaded, f, sort_keys=False, allow_unicode=True)

        global CONFIG
        if not isinstance(CONFIG, dict):
            CONFIG = {}
        runtime_agent_cfg = CONFIG.setdefault("agent_reconciliation", {})
        if not isinstance(runtime_agent_cfg, dict):
            runtime_agent_cfg = {}
            CONFIG["agent_reconciliation"] = runtime_agent_cfg
        runtime_agent_cfg["provenance_defaults"] = defaults

        return True, "Preferred provenance defaults saved to config.yaml."
    except Exception as exc:
        return False, f"Unable to save preferred provenance defaults: {exc}"


def _register_openai_compatible_model_from_override(
    custom_model_override: str,
    provider: str,
    api_key_env: str,
) -> tuple[bool, str]:
    model_name = str(custom_model_override or "").strip()
    if not model_name:
        return False, "Enter a custom model name before registering."
    if not _is_openai_compatible_provider(provider):
        return False, "Model registration is only available for the OpenAI-compatible provider."

    ok, verify_message = _verify_openai_compatible_model_availability(model_name)
    if not ok:
        return False, verify_message

    merged_registry = _merge_openai_compatible_model_registry(
        [model_name],
        _get_openai_compatible_registered_models_from_config(),
    )
    saved, save_message = _save_openai_compatible_model_registry(merged_registry)
    if not saved:
        return False, save_message

    _ensure_model_catalog_for_provider(
        provider,
        api_key_env=api_key_env,
        force_refresh=True,
    )
    st.session_state["clear_custom_model_override"] = True
    return True, f"Registered '{model_name}' for OpenAI-compatible provider. It will appear in future sessions."


def _is_populated_api_key(value: Optional[str]) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    lowered = token.lower()
    return lowered not in API_KEY_PLACEHOLDERS and not lowered.startswith("your")


def _get_provider_api_key_from_config(provider: str) -> Optional[str]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    provider_api_keys = agent_cfg.get("provider_api_keys", {})
    if isinstance(provider_api_keys, dict):
        configured = provider_api_keys.get(provider)
        if configured is None:
            configured = provider_api_keys.get(str(provider).lower())
        if _is_populated_api_key(configured):
            return str(configured).strip()

    legacy_field_map = {
        "openai": "openai_api_key",
        "anthropic": "anthropic_api_key",
        "google_gemini": "google_api_key",
    }
    legacy_field = legacy_field_map.get(provider)
    if legacy_field and _is_populated_api_key(agent_cfg.get(legacy_field)):
        return str(agent_cfg.get(legacy_field)).strip()

    return None


def _ensure_provider_api_key_available(provider: str, api_key_env: Optional[str] = None) -> bool:
    if provider == OPENAI_CODEX_PROVIDER:
        return is_codex_authenticated()

    if provider == OPENAI_COMPATIBLE_PROVIDER:
        env_name = (api_key_env or "").strip() or get_default_api_key_env(provider)
        session_key = str(st.session_state.get("agent_openai_compatible_api_key", "") or "").strip()
        if session_key:
            os.environ[env_name] = session_key
            return True
        configured_key = _get_provider_api_key_from_config(provider)
        if configured_key:
            os.environ[env_name] = configured_key
            return True
        if _is_populated_api_key(os.getenv(env_name)):
            return True
        # OpenAI-compatible providers may not require API keys.
        return True

    env_name = (api_key_env or "").strip() or get_default_api_key_env(provider)
    if _is_populated_api_key(os.getenv(env_name)):
        return True

    configured_key = _get_provider_api_key_from_config(provider)
    if configured_key:
        os.environ[env_name] = configured_key
        return True

    return False


def _ensure_langsmith_api_key_available() -> bool:
    if _is_populated_api_key(os.getenv("LANGSMITH_API_KEY")):
        return True

    configured = (CONFIG or {}).get("agent_reconciliation", {}).get("langsmith_api_key")
    if _is_populated_api_key(configured):
        os.environ["LANGSMITH_API_KEY"] = str(configured).strip()
        return True

    return False


# _render_codex_auth_controls moved to Material UI in WorkflowConfigPanel.tsx


def _get_provider_pricing_overrides(provider: str) -> Dict:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    pricing_by_provider = agent_cfg.get("provider_pricing_overrides", {})
    if not isinstance(pricing_by_provider, dict):
        return {}

    entry = pricing_by_provider.get(provider)
    if entry is None:
        entry = pricing_by_provider.get(str(provider).lower())
    return entry if isinstance(entry, dict) else {}


def _extract_model_records_from_catalog(catalog: Optional[Dict]) -> List[Dict]:
    if not isinstance(catalog, dict):
        return []
    records = catalog.get("models", [])
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


def _extract_model_ids_from_catalog(catalog: Optional[Dict]) -> List[str]:
    model_ids: List[str] = []
    seen = set()
    for record in _extract_model_records_from_catalog(catalog):
        model_id = str(record.get("model_id", "") or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        model_ids.append(model_id)
    return model_ids


def _openai_compatible_catalog_requires_api_key(catalog: Optional[Dict]) -> bool:
    if not isinstance(catalog, dict):
        return False
    return str(catalog.get("source", "") or "").strip() == "auth_required"


def _merge_registered_openai_compatible_models_into_catalog(catalog: Optional[Dict]) -> Dict:
    if not isinstance(catalog, dict):
        return {
            "provider": OPENAI_COMPATIBLE_PROVIDER,
            "source": "fallback",
            "message": "Using registered local model catalog.",
            "models": [],
        }

    if str(catalog.get("provider", "") or "").strip() != OPENAI_COMPATIBLE_PROVIDER:
        return catalog

    registered_models = _get_openai_compatible_registered_models_from_config()
    if not registered_models:
        return catalog

    merged_catalog = dict(catalog)
    existing_records = _extract_model_records_from_catalog(catalog)
    existing_ids = {
        str(record.get("model_id", "") or "").strip()
        for record in existing_records
        if str(record.get("model_id", "") or "").strip()
    }

    merged_records = [dict(record) for record in existing_records]
    for model_id in registered_models:
        if model_id in existing_ids:
            continue
        merged_records.append(
            {
                "provider": OPENAI_COMPATIBLE_PROVIDER,
                "model_id": model_id,
                "display_name": model_id,
                "pricing_source": "unavailable",
                "pricing_availability_note": "Registered local model.",
            }
        )

    merged_catalog["models"] = merged_records
    return merged_catalog


def _find_model_record(catalog: Optional[Dict], model_id: str) -> Optional[Dict]:
    target = str(model_id or "").strip()
    if not target:
        return None
    for record in _extract_model_records_from_catalog(catalog):
        if str(record.get("model_id", "") or "").strip() == target:
            return record
    return None


def _format_model_option_label(catalog: Optional[Dict], model_id: str) -> str:
    record = _find_model_record(catalog, model_id)
    if not record:
        return str(model_id)

    record_model_id = str(record.get("model_id", model_id) or model_id)
    display_name = str(record.get("display_name", record_model_id) or record_model_id)
    label = display_name if display_name == record_model_id else f"{display_name} ({record_model_id})"

    # No price display for subscription models
    if str(record.get("provider", "")).strip() == OPENAI_CODEX_PROVIDER:
        return label

    input_price = record.get("pricing_input_usd_per_mtok")
    output_price = record.get("pricing_output_usd_per_mtok")
    if isinstance(input_price, (int, float)) and isinstance(output_price, (int, float)):
        label += f" — ${float(input_price):.2f}/${float(output_price):.2f} USD/MTok"
    else:
        # Check if pricing source is explicitly unavailable or missing
        pricing_source = str(record.get("pricing_source", "") or "").strip()
        if pricing_source == "fetch_failed" or not pricing_source:
             label += " (Price unavailable)"

    return label


def _format_model_details_caption(catalog: Optional[Dict], model_id: str) -> Optional[str]:
    record = _find_model_record(catalog, model_id)
    if not record:
        return None

    details: List[str] = []
    max_input_tokens = record.get("max_input_tokens")
    max_output_tokens = record.get("max_output_tokens")
    if isinstance(max_input_tokens, int):
        details.append(f"input limit: {max_input_tokens:,} tokens")
    if isinstance(max_output_tokens, int):
        details.append(f"output limit: {max_output_tokens:,} tokens")

    # Subscription models have no per-token pricing to show
    if str(record.get("provider", "")).strip() != OPENAI_CODEX_PROVIDER:
        input_price = record.get("pricing_input_usd_per_mtok")
        output_price = record.get("pricing_output_usd_per_mtok")
        if isinstance(input_price, (int, float)) and isinstance(output_price, (int, float)):
            details.append(f"pricing: ${float(input_price):.2f}/${float(output_price):.2f} USD per 1M tokens")
        else:
            note = str(record.get("pricing_availability_note") or "").strip()
            if note:
                details.append(note)

    notes = str(record.get("pricing_notes") or "").strip()
    if notes:
        details.append(f"pricing note: {notes}")

    return " • ".join(details) if details else None


def _ensure_model_catalog_for_provider(provider: str, api_key_env: Optional[str] = None, force_refresh: bool = False) -> Dict:
    cache = st.session_state.get(AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY, {})
    if not isinstance(cache, dict):
        cache = {}

    env_name = (api_key_env or "").strip() or get_default_api_key_env(provider)
    if _is_openai_compatible_provider(provider):
        _ensure_openai_compatible_base_url_available()
    _ensure_provider_api_key_available(provider, env_name)
    pricing_overrides = _get_provider_pricing_overrides(provider)

    cache_key = f"{provider}::{env_name}"
    if _is_openai_compatible_provider(provider):
        cache_key = "::".join(
            [
                provider,
                env_name,
                _normalize_openai_compatible_base_url(os.getenv(OPENAI_COMPATIBLE_BASE_URL_ENV)),
                "1" if _is_populated_api_key(os.getenv(env_name)) else "0",
            ]
        )
    if force_refresh or cache_key not in cache or not isinstance(cache.get(cache_key), dict):
        catalog = fetch_available_model_catalog(
            provider,
            api_key_env=env_name or None,
            pricing_overrides=pricing_overrides,
        )
        if not isinstance(catalog, dict):
            catalog = {
                "provider": provider,
                "source": "fallback",
                "message": "Model catalog fetch returned an invalid payload; using curated defaults.",
                "models": [{"model_id": model_id, "display_name": model_id} for model_id in get_default_model_options(provider)],
            }
        cache[cache_key] = catalog
        st.session_state[AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY] = cache

    cached_catalog = cache.get(cache_key)
    if isinstance(cached_catalog, dict):
        if _is_openai_compatible_provider(provider):
            return _merge_registered_openai_compatible_models_into_catalog(cached_catalog)
        return cached_catalog

    fallback_catalog = {
        "provider": provider,
        "source": "fallback",
        "message": "No model catalog in cache; using curated defaults.",
        "models": [{"model_id": model_id, "display_name": model_id} for model_id in get_default_model_options(provider)],
    }
    if _is_openai_compatible_provider(provider):
        return _merge_registered_openai_compatible_models_into_catalog(fallback_catalog)
    return fallback_catalog


def _ensure_models_for_provider(provider: str, api_key_env: Optional[str] = None, force_refresh: bool = False) -> List[str]:
    catalog = _ensure_model_catalog_for_provider(provider, api_key_env=api_key_env, force_refresh=force_refresh)
    model_ids = _extract_model_ids_from_catalog(catalog)
    return model_ids or get_default_model_options(provider)


def _render_model_catalog_status(provider: str, api_key_env: Optional[str] = None):
    catalog = _ensure_model_catalog_for_provider(provider, api_key_env=api_key_env)
    source = catalog.get("source", "unknown")
    message = catalog.get("message", "")
    
    if source == "live":
        st.caption(f"✅ Live catalog active: {message}")
    elif source == "curated_subscription":
         st.caption(f"ℹ️ {message}")
    elif source == "fallback":
        st.caption(f"⚠️ Using fallback defaults: {message}")
    elif source == "fetch_failed":
        st.caption(f"❌ Live fetch failed: {message}")






def _build_run_config_from_state() -> AgentRunConfig:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    agentic_expert_mode = bool(st.session_state.get("agent_agentic_expert_mode", False))

    model_provider = st.session_state.get("agent_model_provider", "openai")
    definition_model_provider = model_provider
    model_name = st.session_state.get("agent_model_name", "gpt-5.1")
    reasoning_effort = str(st.session_state.get("agent_reasoning_effort", agent_cfg.get("reasoning_effort", "none")) or "none").strip().lower()
    if reasoning_effort not in REASONING_EFFORT_OPTIONS:
        reasoning_effort = "none"
    definition_model_name = st.session_state.get("agent_definition_model_name", model_name)

    model_api_key_env = _resolve_api_key_env_for_provider(
        "agent_model_api_key_env",
        model_provider,
        "model_api_key_env",
    )

    if _is_openai_compatible_provider(model_provider):
        _ensure_openai_compatible_base_url_available()
    definition_model_api_key_env = _resolve_api_key_env_for_provider(
        "agent_definition_model_api_key_env",
        definition_model_provider,
        "definition_model_api_key_env",
    )

    default_definition_model_name = model_name
    default_timeout_seconds = int(agent_cfg.get("timeout_seconds", 180))
    default_max_iterations = int(agent_cfg.get("max_iterations", 10))
    default_batch_size = int(agent_cfg.get("default_batch_size", 10))
    default_max_workers = int(agent_cfg.get("max_workers", 4))
    default_enable_skos_matching = bool(agent_cfg.get("enable_skos_matching", True))
    enable_skos_matching = bool(
        st.session_state.get("agent_enable_skos_matching", default_enable_skos_matching)
    )

    if agentic_expert_mode:
        use_different_models = bool(st.session_state.get("agent_use_different_models", False))
        if use_different_models:
            definition_model_name = st.session_state.get(
                "agent_definition_model_name",
                default_definition_model_name,
            )
        else:
            definition_model_name = model_name
        timeout_seconds = int(st.session_state.get("agent_timeout_seconds", default_timeout_seconds))
        max_iterations = int(st.session_state.get("agent_max_iterations", default_max_iterations))
        batch_size = int(st.session_state.get("agent_batch_size", default_batch_size))
        max_workers = int(st.session_state.get("agent_max_workers", default_max_workers))
    else:
        definition_model_name = model_name
        timeout_seconds = default_timeout_seconds
        max_iterations = default_max_iterations
        batch_size = default_batch_size
        max_workers = default_max_workers

    default_agentic_trigger_policy = str(
        agent_cfg.get("agentic_trigger_policy", "no_exact_or_low_confidence") or "no_exact_or_low_confidence"
    )
    default_agentic_min_confidence = float(
        agent_cfg.get("agentic_min_confidence_to_skip_refinement", 0.80)
    )
    default_agentic_max_planner_calls = int(agent_cfg.get("agentic_max_planner_calls", 1))
    default_agentic_max_tool_actions = int(agent_cfg.get("agentic_max_tool_actions", 6))
    default_agentic_total_llm_call_budget = int(agent_cfg.get("agentic_total_llm_call_budget", 14))
    default_agentic_max_candidate_rescore = int(agent_cfg.get("agentic_max_candidate_rescore", 8))
    default_candidate_pool_limit = int(agent_cfg.get("candidate_pool_limit", 30))
    default_auto_accept_enabled = bool(agent_cfg.get("auto_accept_enabled", False))
    default_auto_accept_min_confidence = float(agent_cfg.get("auto_accept_min_confidence", 0.80))
    default_auto_accept_require_exact_match = bool(agent_cfg.get("auto_accept_require_exact_match", True))
    default_auto_accept_require_llm_decision = bool(agent_cfg.get("auto_accept_require_llm_decision", True))
    default_auto_accept_require_no_fallback = bool(agent_cfg.get("auto_accept_require_no_fallback", True))
    default_auto_accept_trusted_ontologies_only = bool(agent_cfg.get("auto_accept_trusted_ontologies_only", False))
    default_allow_heuristic_fallback = bool(agent_cfg.get("allow_heuristic_fallback", True))
    allow_heuristic_fallback = bool(
        st.session_state.get("agent_allow_heuristic_fallback", default_allow_heuristic_fallback)
    )

    if agentic_expert_mode:
        agentic_trigger_policy = str(
            st.session_state.get("agentic_trigger_policy", default_agentic_trigger_policy)
            or default_agentic_trigger_policy
        )
        agentic_min_confidence = float(
            st.session_state.get("agentic_min_confidence_to_skip_refinement", default_agentic_min_confidence)
        )
        agentic_max_planner_calls = int(
            st.session_state.get("agentic_max_planner_calls", default_agentic_max_planner_calls)
        )
        agentic_max_tool_actions = int(
            st.session_state.get("agentic_max_tool_actions", default_agentic_max_tool_actions)
        )
        agentic_total_llm_call_budget = int(
            st.session_state.get("agentic_total_llm_call_budget", default_agentic_total_llm_call_budget)
        )
        agentic_max_candidate_rescore = int(
            st.session_state.get("agentic_max_candidate_rescore", default_agentic_max_candidate_rescore)
        )
        candidate_pool_limit = int(
            st.session_state.get("agent_candidate_pool_limit", default_candidate_pool_limit)
        )
        planner_model_provider = st.session_state.get("agent_planner_model_provider") or model_provider
        planner_model_name = st.session_state.get("agent_planner_model_name") or model_name
        planner_model_api_key_env = (
            model_api_key_env
            if planner_model_provider == model_provider
            else _resolve_api_key_env_for_provider(None, planner_model_provider)
        )
    else:
        # Notebook-style behavior: controlled agentic refinement always active with safe defaults.
        # In non-expert mode, planner follows the primary provider/model selected above.
        agentic_trigger_policy = default_agentic_trigger_policy
        agentic_min_confidence = default_agentic_min_confidence
        agentic_max_planner_calls = default_agentic_max_planner_calls
        agentic_max_tool_actions = default_agentic_max_tool_actions
        agentic_total_llm_call_budget = default_agentic_total_llm_call_budget
        agentic_max_candidate_rescore = default_agentic_max_candidate_rescore
        candidate_pool_limit = default_candidate_pool_limit
        planner_model_provider = model_provider
        planner_model_name = model_name
        planner_model_api_key_env = model_api_key_env

    auto_accept_enabled = bool(
        st.session_state.get("agent_auto_accept_enabled", default_auto_accept_enabled)
    )
    auto_accept_min_confidence = float(
        st.session_state.get("agent_auto_accept_min_confidence", default_auto_accept_min_confidence)
    )
    auto_accept_require_exact_match = bool(
        st.session_state.get("agent_auto_accept_require_exact_match", default_auto_accept_require_exact_match)
    )
    auto_accept_require_llm_decision = bool(
        st.session_state.get("agent_auto_accept_require_llm_decision", default_auto_accept_require_llm_decision)
    )
    auto_accept_require_no_fallback = bool(
        st.session_state.get("agent_auto_accept_require_no_fallback", default_auto_accept_require_no_fallback)
    )
    auto_accept_trusted_ontologies_only = bool(
        st.session_state.get(
            "agent_auto_accept_trusted_ontologies_only",
            default_auto_accept_trusted_ontologies_only,
        )
    )

    return AgentRunConfig(
        workflow=st.session_state.get("agent_workflow_select", "wikidata_deep_agent"),
        definition_strategy=st.session_state.get("agent_definition_strategy", "generate_single_shot"),
        model_provider=model_provider,
        definition_model_provider=definition_model_provider,
        model_name=model_name,
        reasoning_effort=reasoning_effort,
        definition_model_name=definition_model_name,
        timeout_seconds=timeout_seconds,
        max_iterations=max_iterations,
        batch_size=batch_size,
        max_workers=max_workers,
        enable_skos_matching=enable_skos_matching,
        auto_apply_on_accept=False,
        auto_accept_enabled=auto_accept_enabled,
        auto_accept_min_confidence=auto_accept_min_confidence,
        auto_accept_require_exact_match=auto_accept_require_exact_match,
        auto_accept_require_llm_decision=auto_accept_require_llm_decision,
        auto_accept_require_no_fallback=auto_accept_require_no_fallback,
        auto_accept_trusted_ontologies_only=auto_accept_trusted_ontologies_only,
        trusted_ontologies=[item.strip() for item in st.session_state.get("agent_trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"]) if str(item).strip()],
        bioportal_agent_ontologies=[item.strip() for item in st.session_state.get("agent_bioportal_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"]) if str(item).strip()],
        model_api_key_env=model_api_key_env,
        definition_model_api_key_env=definition_model_api_key_env,
        enable_agentic_refinement=True,
        agentic_trigger_policy=agentic_trigger_policy,
        agentic_min_confidence_to_skip_refinement=agentic_min_confidence,
        agentic_max_planner_calls=agentic_max_planner_calls,
        agentic_max_tool_actions=agentic_max_tool_actions,
        agentic_total_llm_call_budget=agentic_total_llm_call_budget,
        agentic_max_candidate_rescore=agentic_max_candidate_rescore,
        candidate_pool_limit=candidate_pool_limit,
        planner_model_provider=planner_model_provider,
        planner_model_name=planner_model_name,
        planner_model_api_key_env=planner_model_api_key_env,
        langsmith_project=(st.session_state.get("agent_langsmith_project") or None)
        if bool(st.session_state.get("agent_use_langsmith_monitoring", False))
        else None,
        allow_heuristic_fallback=allow_heuristic_fallback,
    )








def _initialize_agent_reconciliation_state():
    """Initialize Streamlit session keys for the hybrid reconciliation workflow."""
    defaults = {
        AGENT_DATAFRAME_STATE_KEY: None,
        AGENT_DATA_SOURCE_MESSAGE_KEY: None,
        AGENT_INPUT_TABLES_KEY: [],
        AGENT_RESULTS_BY_SOURCE_KEY: {},
        AGENT_SELECTED_SOURCE_KEY: None,
        AGENT_DEFINITIONS_BY_SOURCE_KEY: {},
        AGENT_RUN_MESSAGES_KEY: [],
        AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY: {},
        AGENT_MONITORING_STATE_KEY: {},
        AGENT_STOP_EVENT_KEY: {},
        AGENT_UPLOADED_SOURCE_SIGNATURE_KEY: None,
        AGENT_ACTIVE_STEP_KEY: "Setup",
        "agent_reference_publication_text": "",
        "agent_workflow_select": "wikidata_deep_agent",
    }
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def _initialize_provenance_state(provenance_defaults_cfg: Dict[str, str]):
    provenance_defaults = {
        "agent_prov_author_orcid": provenance_defaults_cfg.get("author_id", ""),
        "agent_prov_author_name": provenance_defaults_cfg.get("author_label", ""),
        "agent_prov_reviewer_orcid": provenance_defaults_cfg.get("reviewer_id", ""),
        "agent_prov_reviewer_name": provenance_defaults_cfg.get("reviewer_label", ""),
        "agent_prov_creator_orcid": provenance_defaults_cfg.get("creator_id", ""),
        "agent_prov_creator_name": provenance_defaults_cfg.get("creator_label", ""),
        "agent_prov_mapping_tool": provenance_defaults_cfg.get("mapping_tool", "RDF4Risk Agent-Based Reconciliation"),
        "agent_prov_mapping_tool_version": provenance_defaults_cfg.get("mapping_tool_version", "PoC"),
        "agent_prov_last_run_mapping_date": provenance_defaults_cfg.get("mapping_date", ""),
        "agent_prov_publication_date": provenance_defaults_cfg.get("publication_date", ""),
    }
    for key, value in provenance_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _get_agent_ui_defaults() -> Dict[str, object]:
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    provider_options = get_supported_llm_providers()
    preferred_model_provider = str(agent_cfg.get("preferred_model_provider", "") or "").strip()
    preferred_model_name = str(agent_cfg.get("preferred_model_name", "") or "").strip()
    default_model_provider = preferred_model_provider if preferred_model_provider in provider_options else "openai"
    default_model = preferred_model_name or "gpt-5.1"
    default_reasoning_effort = str(agent_cfg.get("reasoning_effort", "none") or "none").strip().lower()
    if default_reasoning_effort not in REASONING_EFFORT_OPTIONS:
        default_reasoning_effort = "none"
    return {
        "agent_cfg": agent_cfg,
        "provider_options": provider_options,
        "default_model_provider": default_model_provider,
        "default_model": default_model,
        "default_timeout": int(agent_cfg.get("timeout_seconds", 180)),
        "default_iterations": int(agent_cfg.get("max_iterations", 10)),
        "default_batch_size": int(agent_cfg.get("default_batch_size", 10)),
        "default_workers": int(agent_cfg.get("max_workers", 4)),
        "default_reasoning_effort": default_reasoning_effort,
        "default_auto_accept_enabled": bool(agent_cfg.get("auto_accept_enabled", False)),
    }


def _build_workflow_config_from_state(defaults: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    defaults = defaults or _get_agent_ui_defaults()
    agent_cfg = (CONFIG or {}).get("agent_reconciliation", {})
    advanced = {
        "timeout_s": int(st.session_state.get("agent_timeout_seconds", defaults.get("default_timeout", 180))),
        "max_iterations": int(st.session_state.get("agent_max_iterations", defaults.get("default_iterations", 10))),
        "batch_size": int(st.session_state.get("agent_batch_size", defaults.get("default_batch_size", 10))),
        "max_workers": int(st.session_state.get("agent_max_workers", defaults.get("default_workers", 4))),
        "agentic_min_confidence_to_skip_refinement": float(st.session_state.get("agentic_min_confidence_to_skip_refinement", agent_cfg.get("agentic_min_confidence_to_skip_refinement", 0.80))),
        "agentic_max_planner_calls": int(st.session_state.get("agentic_max_planner_calls", agent_cfg.get("agentic_max_planner_calls", 1))),
        "agentic_max_tool_actions": int(st.session_state.get("agentic_max_tool_actions", agent_cfg.get("agentic_max_tool_actions", 6))),
        "agentic_total_llm_call_budget": int(st.session_state.get("agentic_total_llm_call_budget", agent_cfg.get("agentic_total_llm_call_budget", 14))),
        "agentic_max_candidate_rescore": int(st.session_state.get("agentic_max_candidate_rescore", agent_cfg.get("agentic_max_candidate_rescore", 8))),
        "candidate_pool_limit": int(st.session_state.get("agent_candidate_pool_limit", agent_cfg.get("candidate_pool_limit", 30))),
    }
    auto_accept_policy = {
        "min_confidence": float(st.session_state.get("agent_auto_accept_min_confidence", agent_cfg.get("auto_accept_min_confidence", 0.80))),
        "require_exact_match": bool(st.session_state.get("agent_auto_accept_require_exact_match", agent_cfg.get("auto_accept_require_exact_match", True))),
        "require_llm_decision": bool(st.session_state.get("agent_auto_accept_require_llm_decision", agent_cfg.get("auto_accept_require_llm_decision", True))),
        "require_no_fallback": bool(st.session_state.get("agent_auto_accept_require_no_fallback", agent_cfg.get("auto_accept_require_no_fallback", True))),
        "trusted_ontologies_only": bool(st.session_state.get("agent_auto_accept_trusted_ontologies_only", agent_cfg.get("auto_accept_trusted_ontologies_only", False))),
    }
    config = {
        "workflow": st.session_state.get("agent_workflow_select", "wikidata_deep_agent"),
        "provider": st.session_state.get("agent_model_provider", defaults.get("default_model_provider", "openai")),
        "model": st.session_state.get("agent_model_name", defaults.get("default_model", "gpt-5.1")),
        "reasoning_effort": st.session_state.get("agent_reasoning_effort", defaults.get("default_reasoning_effort", "none")),
        "custom_model_override": st.session_state.get("agent_custom_model_override", ""),
        "provider_api_key_env": st.session_state.get("agent_model_api_key_env", get_default_api_key_env(str(st.session_state.get("agent_model_provider", defaults.get("default_model_provider", "openai"))))),
        "openai_compatible_base_url": st.session_state.get("agent_openai_compatible_base_url", _get_openai_compatible_base_url_from_config()),
        "openai_compatible_api_key": st.session_state.get("agent_openai_compatible_api_key", _get_provider_api_key_from_config(OPENAI_COMPATIBLE_PROVIDER) or ""),
        "skos_matching": bool(st.session_state.get("agent_enable_skos_matching", (CONFIG or {}).get("agent_reconciliation", {}).get("enable_skos_matching", True))),
        "auto_accept": bool(st.session_state.get("agent_auto_accept_enabled", defaults.get("default_auto_accept_enabled", False))),
        "auto_accept_policy": auto_accept_policy,
        "langsmith": bool(st.session_state.get("agent_use_langsmith_monitoring", False)),
        "langsmith_project": st.session_state.get("agent_langsmith_project", agent_cfg.get("langsmith_project", "")),
        "expert_mode": bool(st.session_state.get("agent_agentic_expert_mode", False)),
        "allow_heuristic_fallback": bool(st.session_state.get("agent_allow_heuristic_fallback", agent_cfg.get("allow_heuristic_fallback", True))),
        "use_different_models": bool(st.session_state.get("agent_use_different_models", False)),
        "definition_model": st.session_state.get("agent_definition_model_name", st.session_state.get("agent_model_name", defaults.get("default_model", "gpt-5.1"))),
        "agentic_trigger_policy": st.session_state.get("agentic_trigger_policy", agent_cfg.get("agentic_trigger_policy", "no_exact_or_low_confidence")),
        "planner_provider": st.session_state.get("agent_planner_model_provider", st.session_state.get("agent_model_provider", defaults.get("default_model_provider", "openai"))),
        "planner_model": st.session_state.get("agent_planner_model_name", st.session_state.get("agent_model_name", defaults.get("default_model", "gpt-5.1"))),
        "trusted_ontologies": st.session_state.get("agent_trusted_ontologies", agent_cfg.get("trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"])),
        "bioportal_ontologies": st.session_state.get("agent_bioportal_ontologies", agent_cfg.get("bioportal_agent_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"])),
        "definition_preparation": bool(st.session_state.get("agent_enable_definition_preparation", False)),
        "definition_strategy": st.session_state.get("agent_definition_strategy", "generate_single_shot"),
        "definition_context_text": st.session_state.get("agent_definition_context_text", ""),
        "advanced": advanced,
        "provenance": {
            "enabled": bool(st.session_state.get("agent_enable_provenance_metadata", False)),
            **_build_provenance_defaults_from_state(),
        },
    }
    st.session_state[AGENT_WORKFLOW_CONFIG_STATE_KEY] = config
    return config


def _apply_workflow_config_to_session_state(config: Optional[Dict[str, object]]) -> bool:
    if not isinstance(config, dict):
        return False
    changed = False

    def _set(key: str, value: object):
        nonlocal changed
        if value is None:
            return
        if st.session_state.get(key) != value:
            st.session_state[key] = value
            changed = True

    workflow = str(config.get("workflow", "") or "").strip()
    if workflow in {"wikidata_deep_agent", "bioportal_wikidata_multiagent"}:
        _set("agent_workflow_select", workflow)
    provider = str(config.get("provider", "") or "").strip()
    if provider:
        _set("agent_model_provider", provider)
        _set("agent_definition_model_provider", provider)
    provider_api_key_env = str(config.get("provider_api_key_env", "") or "").strip()
    if provider_api_key_env:
        _set("agent_model_api_key_env", provider_api_key_env)
        _set("agent_definition_model_api_key_env", provider_api_key_env)
    openai_compatible_base_url = _normalize_openai_compatible_base_url(config.get("openai_compatible_base_url"))
    if openai_compatible_base_url:
        _set("agent_openai_compatible_base_url", openai_compatible_base_url)
        os.environ[OPENAI_COMPATIBLE_BASE_URL_ENV] = openai_compatible_base_url
    openai_compatible_api_key = str(config.get("openai_compatible_api_key", "") or "").strip()
    if openai_compatible_api_key:
        _set("agent_openai_compatible_api_key", openai_compatible_api_key)
        os.environ[get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)] = openai_compatible_api_key
    model = str(config.get("model", "") or "").strip()
    if model:
        _set("agent_model_name", model)
        if not bool(config.get("expert_mode", False)):
            _set("agent_definition_model_name", model)
            _set("agent_planner_model_name", model)
    custom_model_override = str(config.get("custom_model_override", "") or "").strip()
    _set("agent_custom_model_override", custom_model_override)
    reasoning = str(config.get("reasoning_effort", "") or "").strip().lower()
    if reasoning in REASONING_EFFORT_OPTIONS:
        _set("agent_reasoning_effort", reasoning)
    _set("agent_enable_skos_matching", bool(config.get("skos_matching", True)))
    _set("agent_auto_accept_enabled", bool(config.get("auto_accept", False)))
    policy = config.get("auto_accept_policy", {}) if isinstance(config.get("auto_accept_policy"), dict) else {}
    if "min_confidence" in policy:
        try:
            _set("agent_auto_accept_min_confidence", float(policy.get("min_confidence")))
        except (TypeError, ValueError):
            pass
    for source_key, session_key in {
        "require_exact_match": "agent_auto_accept_require_exact_match",
        "require_llm_decision": "agent_auto_accept_require_llm_decision",
        "require_no_fallback": "agent_auto_accept_require_no_fallback",
        "trusted_ontologies_only": "agent_auto_accept_trusted_ontologies_only",
    }.items():
        if source_key in policy:
            _set(session_key, bool(policy.get(source_key)))
    _set("agent_use_langsmith_monitoring", bool(config.get("langsmith", False)))
    _set("agent_langsmith_project", str(config.get("langsmith_project", "") or ""))
    _set("agent_agentic_expert_mode", bool(config.get("expert_mode", False)))
    _set("agent_allow_heuristic_fallback", bool(config.get("allow_heuristic_fallback", True)))
    _set("agent_use_different_models", bool(config.get("use_different_models", False)))
    definition_model = str(config.get("definition_model", "") or "").strip()
    if definition_model and bool(config.get("use_different_models", False)):
        _set("agent_definition_model_name", definition_model)
    elif model:
        _set("agent_definition_model_name", model)
    trigger_policy = str(config.get("agentic_trigger_policy", "") or "").strip()
    if trigger_policy in {"no_exact_or_low_confidence", "always", "non_exact_only"}:
        _set("agentic_trigger_policy", trigger_policy)
    planner_provider = str(config.get("planner_provider", "") or "").strip()
    if planner_provider:
        _set("agent_planner_model_provider", planner_provider)
    planner_model = str(config.get("planner_model", "") or "").strip()
    if planner_model:
        _set("agent_planner_model_name", planner_model)
    for source_key, session_key in {
        "trusted_ontologies": "agent_trusted_ontologies",
        "bioportal_ontologies": "agent_bioportal_ontologies",
    }.items():
        values = config.get(source_key)
        if isinstance(values, list):
            normalized_values = [str(item).strip().upper() for item in values if str(item).strip()]
            _set(session_key, normalized_values)
    advanced = config.get("advanced", {}) if isinstance(config.get("advanced"), dict) else {}
    numeric_map = {
        "timeout_s": "agent_timeout_seconds",
        "max_iterations": "agent_max_iterations",
        "batch_size": "agent_batch_size",
        "max_workers": "agent_max_workers",
        "agentic_max_planner_calls": "agentic_max_planner_calls",
        "agentic_max_tool_actions": "agentic_max_tool_actions",
        "agentic_total_llm_call_budget": "agentic_total_llm_call_budget",
        "agentic_max_candidate_rescore": "agentic_max_candidate_rescore",
        "candidate_pool_limit": "agent_candidate_pool_limit",
    }
    for source_key, session_key in numeric_map.items():
        if source_key in advanced:
            try:
                _set(session_key, int(advanced.get(source_key)))
            except (TypeError, ValueError):
                pass
    if "agentic_min_confidence_to_skip_refinement" in advanced:
        try:
            _set("agentic_min_confidence_to_skip_refinement", float(advanced.get("agentic_min_confidence_to_skip_refinement")))
        except (TypeError, ValueError):
            pass

    if "definition_preparation" in config:
        _set("agent_enable_definition_preparation", bool(config.get("definition_preparation", False)))
    definition_strategy = str(config.get("definition_strategy", "") or "").strip()
    if definition_strategy in {"uploaded_sheet", "generate_single_shot", "reference_publication"}:
        _set("agent_definition_strategy", definition_strategy)
    if "definition_context_text" in config:
        _set("agent_definition_context_text", str(config.get("definition_context_text", "") or ""))

    provenance = config.get("provenance", {}) if isinstance(config.get("provenance"), dict) else {}
    if provenance:
        _set("agent_enable_provenance_metadata", bool(provenance.get("enabled", False)))
        for source_key, session_key in {
            "author_id": "agent_prov_author_orcid",
            "author_label": "agent_prov_author_name",
            "reviewer_id": "agent_prov_reviewer_orcid",
            "reviewer_label": "agent_prov_reviewer_name",
            "creator_id": "agent_prov_creator_orcid",
            "creator_label": "agent_prov_creator_name",
            "mapping_tool": "agent_prov_mapping_tool",
            "mapping_tool_version": "agent_prov_mapping_tool_version",
            "publication_date": "agent_prov_publication_date",
        }.items():
            if source_key in provenance:
                _set(session_key, str(provenance.get(source_key, "") or ""))

    st.session_state[AGENT_WORKFLOW_CONFIG_STATE_KEY] = _build_workflow_config_from_state()
    return changed


def _get_workflow_component_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "components", "workflow_config_panel", "frontend", "build")


def _render_workflow_config_panel(
    config: Dict[str, object],
    provider_options: List[str],
    model_options: List[str],
    readiness_state: Optional[Dict[str, object]] = None,
    primary_catalog: Optional[Dict] = None,
    selected_model: Optional[str] = None,
    data_status: Optional[Dict[str, object]] = None,
    run_status: Optional[Dict[str, object]] = None,
    telemetry: Optional[Dict[str, object]] = None,
    review: Optional[Dict[str, object]] = None,
    export_payload: Optional[Dict[str, object]] = None,
):
    """Render the central React/Material-UI app and return one structured event."""
    component_path = _get_workflow_component_path()
    if not os.path.exists(os.path.join(component_path, "index.html")):
        st.error(
            "AgentReconciliationMuiApp React/Material-UI component build is missing. "
            "Run `npm install && npm run build` in "
            "agentic_reconciliation/components/workflow_config_panel/frontend."
        )
        return None
    labels = {provider: get_provider_label(provider) for provider in provider_options}
    model_labels = {model: _format_model_option_label(primary_catalog, model) for model in model_options}
    model_details = _format_model_details_caption(primary_catalog, selected_model or str(config.get("model", ""))) if primary_catalog else None
    ontology_options = sorted(
        list(
            set(
                list((CONFIG or {}).get("agent_reconciliation", {}).get("trusted_ontologies", ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON"]))
                + list((CONFIG or {}).get("agent_reconciliation", {}).get("bioportal_agent_ontologies", ["NCIT", "NIFSTD", "BERO", "OCHV", "SNOMEDCT"]))
                + ["MESH", "NCIT", "LOINC", "FOODON", "NCBITAXON", "NIFSTD", "BERO", "OCHV", "SNOMEDCT", "CHEBI", "QUDT"]
            )
        )
    )
    provider_kind = "codex" if _is_codex_provider(str(config.get("provider", ""))) else "openai_compatible" if _is_openai_compatible_provider(str(config.get("provider", ""))) else "standard"
    workflow_component = components.declare_component("workflow_config_panel", path=component_path)
    try:
        return workflow_component(
            active_stage=_component_stage_from_session(),
            config=config,
            providers=provider_options,
            providerLabels=labels,
            models=model_options,
            modelLabels=model_labels,
            modelDetails=model_details,
            reasoningOptions=REASONING_EFFORT_OPTIONS,
            readiness=readiness_state or {},
            data_status=data_status or {},
            run_status=run_status or {},
            telemetry=telemetry or {},
            review=review or {},
            exportPayload=export_payload,
            ontologyOptions=ontology_options,
            providerKind=provider_kind,
            statusMessage=st.session_state.get("agent_mui_status_message"),
            codexAuthStatus=get_codex_auth_status(),
            key="agent_reconciliation_mui_app",
            default=None,
        )
    except Exception as exc:
        st.error(f"AgentReconciliationMuiApp React/Material-UI component could not be rendered. ({exc})")
        return None


def _render_agent_app_header(active_stage: str):
    st.markdown("### Agent-Based Reconciliation")
    st.caption("Semantic Matching & SSSOM Export")
    stage_labels = ["Setup", "Run", "Review", "Export"]
    step_html = []
    for idx, stage in enumerate(stage_labels, start=1):
        active_cls = "active" if stage == active_stage else ""
        step_html.append(f'<span class="agent-nav-step {active_cls}"><b>{idx}</b> {stage}</span>')
    st.markdown(f'<div class="agent-nav-stepper">{"".join(step_html)}</div>', unsafe_allow_html=True)


def _get_active_agent_stage() -> str:
    stages = ["Setup", "Run", "Review", "Export"]
    current = st.session_state.get(AGENT_ACTIVE_STEP_KEY, "Setup")
    if current not in stages:
        current = "Setup"
        st.session_state[AGENT_ACTIVE_STEP_KEY] = current
    return current


def _compute_workflow_runtime_context(defaults: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    defaults = defaults or _get_agent_ui_defaults()
    provider_options = defaults.get("provider_options", get_supported_llm_providers())
    primary_provider = str(st.session_state.get("agent_model_provider", defaults.get("default_model_provider", "openai")) or "openai")
    if primary_provider not in provider_options:
        primary_provider = str(defaults.get("default_model_provider", "openai"))
    st.session_state.setdefault("agent_model_provider", primary_provider)
    st.session_state["agent_definition_model_provider"] = primary_provider
    primary_api_key_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", primary_provider, "model_api_key_env")
    st.session_state["agent_definition_model_api_key_env"] = primary_api_key_env
    primary_catalog = _ensure_model_catalog_for_provider(primary_provider, api_key_env=primary_api_key_env)
    primary_models = _extract_model_ids_from_catalog(primary_catalog) or get_default_model_options(primary_provider)
    if primary_models and not st.session_state.get("agent_model_name"):
        st.session_state["agent_model_name"] = defaults.get("default_model", primary_models[0]) if defaults.get("default_model", "") in primary_models else primary_models[0]
    use_different_models = bool(st.session_state.get("agent_use_different_models", False))
    effective_primary_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", primary_provider, "model_api_key_env")
    definition_provider = primary_provider
    effective_definition_env = effective_primary_env
    missing_provider_keys = []
    if _is_openai_compatible_provider(primary_provider):
        if not _ensure_openai_compatible_base_url_available():
            missing_provider_keys.append((primary_provider, OPENAI_COMPATIBLE_BASE_URL_ENV))
    elif not _ensure_provider_api_key_available(primary_provider, effective_primary_env):
        missing_provider_keys.append((primary_provider, effective_primary_env))
    if use_different_models and (definition_provider, effective_definition_env) != (primary_provider, effective_primary_env):
        if not _ensure_provider_api_key_available(definition_provider, effective_definition_env):
            missing_provider_keys.append((definition_provider, effective_definition_env))
    if _is_openai_compatible_provider(primary_provider) and not missing_provider_keys:
        if _openai_compatible_catalog_requires_api_key(primary_catalog):
            env_name = effective_primary_env or get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            missing_provider_keys.append((OPENAI_COMPATIBLE_PROVIDER, env_name))
    return {
        "primary_provider": primary_provider,
        "primary_catalog": primary_catalog,
        "primary_models": primary_models,
        "effective_primary_env": effective_primary_env,
        "missing_provider_keys": missing_provider_keys,
    }


def _build_run_readiness_state(required_columns, missing_provider_keys: Optional[List[tuple]] = None) -> Dict[str, object]:
    missing_provider_keys = missing_provider_keys or []
    workflow_config = _build_workflow_config_from_state()
    input_tables = st.session_state.get(AGENT_INPUT_TABLES_KEY, [])
    agent_df = st.session_state.get(AGENT_DATAFRAME_STATE_KEY)
    shared_df = st.session_state.get("shared_matching_table")
    schema_df = agent_df if isinstance(agent_df, pd.DataFrame) else shared_df if isinstance(shared_df, pd.DataFrame) else None
    if schema_df is None and isinstance(input_tables, list) and input_tables:
        schema_df = getattr(input_tables[0], "dataframe", None)
    rows = len(schema_df) if isinstance(schema_df, pd.DataFrame) else 0
    cols = len(schema_df.columns) if isinstance(schema_df, pd.DataFrame) else 0
    required_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in required_columns))
    legacy_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS))
    provider_configured = not bool(missing_provider_keys)
    workflow_valid = workflow_config.get("workflow") in {"wikidata_deep_agent", "bioportal_wikidata_multiagent"}
    checks = [
        {"key": "matching_table", "label": "Matching table loaded", "ok": bool(input_tables), "detail": f"{rows:,} rows • {cols:,} columns" if rows else "No working input table"},
        {"key": "required_columns", "label": "Required columns detected", "ok": required_detected or legacy_detected, "detail": "Canonical SSSOM columns" if required_detected else "Legacy columns will be normalized" if legacy_detected else "Missing required table columns"},
        {"key": "provider", "label": "LLM Provider configured", "ok": provider_configured, "detail": get_provider_label(str(workflow_config.get("provider", "")))},
        {"key": "workflow", "label": "Workflow configuration valid", "ok": workflow_valid, "detail": "Ready to run" if workflow_valid else "Choose a supported workflow"},
    ]
    batch_size = int(workflow_config.get("advanced", {}).get("batch_size", 10)) if isinstance(workflow_config.get("advanced"), dict) else 10
    est_batches = max(1, (rows + max(batch_size, 1) - 1) // max(batch_size, 1)) if rows else 0
    return {
        "checks": checks,
        "ready": all(check["ok"] for check in checks),
        "summary": {
            "Workflow": "BioPortal + Wikidata" if workflow_config.get("workflow") == "bioportal_wikidata_multiagent" else "Wikidata Deep Agent",
            "Model": str(workflow_config.get("model", "")),
            "SKOS Matching": "Enabled" if workflow_config.get("skos_matching") else "Disabled",
            "Auto-accept": "Enabled" if workflow_config.get("auto_accept") else "Disabled",
            "Batch Size": str(workflow_config.get("advanced", {}).get("batch_size", "")) if isinstance(workflow_config.get("advanced"), dict) else "",
            "Max Workers": str(workflow_config.get("advanced", {}).get("max_workers", "")) if isinstance(workflow_config.get("advanced"), dict) else "",
            "Est. Runtime": f"~{est_batches}-{est_batches * 2} min" if rows else "n/a",
            "Est. Cost": "Available after model pricing/run telemetry",
        },
    }


def _render_run_readiness_panel(readiness_state: Dict[str, object]):
    badge = "All Good" if readiness_state.get("ready") else "Action Needed"
    st.markdown(f"**Run Prerequisites** · `{badge}`")
    for check in readiness_state.get("checks", []):
        icon = "✅" if check.get("ok") else "⚠️"
        st.write(f"{icon} **{check.get('label', '')}** — {check.get('detail', '')}")
    with st.expander("Run Summary", expanded=True):
        for label, value in readiness_state.get("summary", {}).items():
            st.write(f"**{label}:** {value}")


def _render_agent_sidebar_status(readiness_state: Dict[str, object]):
    st.sidebar.markdown("## Agent-Based Reconciliation")
    st.sidebar.caption("Workflow cockpit")
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Run Prerequisites")
    for check in readiness_state.get("checks", []):
        icon = "✅" if check.get("ok") else "⚠️"
        st.sidebar.caption(f"{icon} {check.get('label', '')}")
        detail = str(check.get("detail", "") or "").strip()
        if detail:
            st.sidebar.caption(f"&nbsp;&nbsp;&nbsp;{detail}")
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Run Summary")
    for label, value in readiness_state.get("summary", {}).items():
        st.sidebar.caption(f"**{label}:** {value}")
    st.sidebar.markdown("---")
    st.sidebar.info("Next MUI component: RunReadinessPanel. ReviewSuggestionsTable is scaffolded for a later Review-area migration.")


def _render_load_data_section(required_columns):
    shared_df = st.session_state.get("shared_matching_table")
    agent_df = st.session_state.get(AGENT_DATAFRAME_STATE_KEY)
    input_tables = st.session_state.get(AGENT_INPUT_TABLES_KEY, [])
    results_by_source = st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})

    _render_agent_section_header(
        "1",
        "Load data for reconciliation",
        "Bring in the shared matching table or upload a matching table directly for this agent run.",
        tooltip="A loaded source becomes the working table that the Wikidata/BioPortal agents enrich with candidate mappings.",
    )

    if isinstance(shared_df, pd.DataFrame):
        if st.button("Load Data from Matching Table Generator", key="agent_load_shared_matching_table"):
            table = make_input_table(
                shared_df,
                source_name="Matching Table Generator",
                filename="shared_matching_table",
                is_from_shared_matching_table=True,
            )
            _store_input_tables([table], "Agent-based reconciliation data successfully loaded from: Matching Table Generator.")
            st.rerun()

    uploaded_files = st.file_uploader(
        "Upload Matching Table (CSV, XLSX, XLS)",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=False,
        key="agent_reconciliation_file_uploader",
    )

    if uploaded_files is not None:
        upload_signature = f"{uploaded_files.name}:{getattr(uploaded_files, 'size', '')}"
        try:
            if st.session_state.get(AGENT_UPLOADED_SOURCE_SIGNATURE_KEY) != upload_signature:
                tables = load_uploaded_input_tables([uploaded_files])
                _store_input_tables(
                    tables,
                    f"Agent-based reconciliation data successfully loaded from uploaded matching table: {uploaded_files.name}.",
                )
                st.session_state[AGENT_UPLOADED_SOURCE_SIGNATURE_KEY] = upload_signature
                st.rerun()
        except Exception as exc:
            st.error(f"Failed to parse uploaded file: {exc}")
    else:
        st.session_state[AGENT_UPLOADED_SOURCE_SIGNATURE_KEY] = None

    shared_df = st.session_state.get("shared_matching_table")
    agent_df = st.session_state.get(AGENT_DATAFRAME_STATE_KEY)
    input_tables = st.session_state.get(AGENT_INPUT_TABLES_KEY, [])
    results_by_source = st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})

    with st.expander("Optional schema/status details", expanded=False):
        _render_agent_hero(shared_df, agent_df, input_tables, results_by_source, required_columns)
        _render_input_contract_card(required_columns)

        if isinstance(shared_df, pd.DataFrame):
            missing = [col for col in required_columns if col not in shared_df.columns]
            has_legacy_schema = all(col in shared_df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS)
            if missing and not has_legacy_schema:
                st.warning(
                    "A shared matching table is present in session state, but it is missing "
                    f"required columns: {', '.join(missing)}"
                )
            elif has_legacy_schema and missing:
                st.info(
                    "Shared matching table uses the legacy column set and will be normalized before agent processing."
                )
            else:
                st.success(
                    f"Shared matching table detected with {len(shared_df)} row(s) and the required schema."
                )
                st.dataframe(shared_df.head(10), use_container_width=True)
        else:
            st.caption(
                "No shared matching table is currently loaded in session state. "
                "Use the upload control above if you want to start this workflow directly."
            )
    return (
        st.session_state.get("shared_matching_table"),
        st.session_state.get(AGENT_DATAFRAME_STATE_KEY),
        st.session_state.get(AGENT_INPUT_TABLES_KEY, []),
        st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {}),
    )


def _render_definition_preparation_section():
    _render_agent_section_header(
        "2",
        "Definition preparation",
        "Optionally add term definitions or source-publication context to improve semantic matching quality.",
        tooltip="Definitions are especially useful for ambiguous labels where lexical similarity alone is not enough.",
    )

    definition_preparation_enabled = st.checkbox(
        "Enable optional definition preparation",
        value=bool(st.session_state.get("agent_enable_definition_preparation", False)),
        key="agent_enable_definition_preparation",
        help="When enabled, you can provide uploaded/contextual definitions used during reconciliation.",
    )

    if definition_preparation_enabled:
        with st.expander("Definition preparation settings", expanded=True):
            if st.session_state.get("agent_definition_strategy") == "manual_text":
                # Backward compatibility: this legacy option now maps to the single
                # consolidated context-generation mode.
                st.session_state["agent_definition_strategy"] = "generate_single_shot"

            st.selectbox(
                "Definition strategy",
                ["uploaded_sheet", "generate_single_shot", "reference_publication"],
                format_func=lambda x: {
                    "uploaded_sheet": "Upload a definitions sheet",
                    "generate_single_shot": "Generate definitions from context",
                    "reference_publication": "Upload a reference publication (PDF, DOC, DOCX)",
                }.get(x, x),
                key="agent_definition_strategy",
            )

            if st.session_state.get("agent_definition_strategy") == "uploaded_sheet":
                uploaded_definitions_file = st.file_uploader(
                    "Upload a definitions file with Term and Definition columns",
                    type=["csv", "xlsx", "xls"],
                    key="agent_definitions_file_uploader",
                )
                if uploaded_definitions_file is not None:
                    try:
                        definitions_df = read_matching_table_upload(uploaded_definitions_file)
                        normalized = normalize_uploaded_definitions(definitions_df)
                        st.session_state[AGENT_DEFINITIONS_BY_SOURCE_KEY]["__uploaded_sheet__"] = normalized
                        st.caption(f"Loaded {len(normalized)} definition row(s).")
                        st.dataframe(normalized.head(10), use_container_width=True)
                    except Exception as exc:
                        st.error(f"Failed to read uploaded definitions: {exc}")
            elif st.session_state.get("agent_definition_strategy") == "reference_publication":
                uploaded_publication_file = st.file_uploader(
                    "Upload reference publication",
                    type=["pdf", "doc", "docx"],
                    key="agent_reference_publication_uploader",
                    help="The extracted text will be used as context for definition generation.",
                )
                if uploaded_publication_file is not None:
                    try:
                        publication_text = extract_reference_publication_text(uploaded_publication_file)
                        st.session_state["agent_reference_publication_text"] = publication_text
                        st.caption(
                            f"Extracted {len(publication_text):,} characters from '{uploaded_publication_file.name}'."
                        )
                    except Exception as exc:
                        st.session_state["agent_reference_publication_text"] = ""
                        st.error(f"Failed to read uploaded reference publication: {exc}")
            else:
                st.text_area(
                    "Context text for definition generation",
                    key="agent_definition_context_text",
                    help="Provide domain context that will be used to generate concise definitions for the input terms.",
                    height=140,
                )


def _render_workflow_models_section():
    _render_agent_section_header(
        "3",
        "Agent workflow settings",
        "Choose the reconciliation strategy, model configuration, review policy, and observability settings.",
        tooltip="Standard mode keeps advanced limits conservative; expert mode exposes planner and budget controls.",
    )
    info_placeholder = st.empty()

    defaults = _get_agent_ui_defaults()
    provider_options = list(defaults.get("provider_options", get_supported_llm_providers()))
    if not provider_options:
        provider_options = ["openai"]
    default_model_provider = str(defaults.get("default_model_provider", "openai") or "openai")
    default_model = str(defaults.get("default_model", "gpt-5.1") or "gpt-5.1")

    primary_provider = str(
        st.session_state.get("agent_model_provider", default_model_provider) or default_model_provider
    )
    if primary_provider not in provider_options:
        primary_provider = default_model_provider if default_model_provider in provider_options else provider_options[0]
        st.session_state["agent_model_provider"] = primary_provider

    default_openai_compatible_base_url = _get_openai_compatible_base_url_from_config()
    if not st.session_state.get("agent_openai_compatible_base_url") and default_openai_compatible_base_url:
        st.session_state["agent_openai_compatible_base_url"] = default_openai_compatible_base_url
    configured_openai_compatible_key = _get_provider_api_key_from_config(OPENAI_COMPATIBLE_PROVIDER) or ""
    if not st.session_state.get("agent_openai_compatible_api_key") and configured_openai_compatible_key:
        st.session_state["agent_openai_compatible_api_key"] = configured_openai_compatible_key

    st.session_state["agent_definition_model_provider"] = primary_provider
    primary_api_key_env = _resolve_api_key_env_for_provider(
        "agent_model_api_key_env",
        primary_provider,
        "model_api_key_env",
    )
    st.session_state["agent_definition_model_api_key_env"] = primary_api_key_env

    primary_catalog = _ensure_model_catalog_for_provider(primary_provider, api_key_env=primary_api_key_env)
    primary_models = _extract_model_ids_from_catalog(primary_catalog) or get_default_model_options(primary_provider)
    if not primary_models:
        primary_models = [default_model] if default_model else ["model-unavailable"]

    selected_primary_model = str(st.session_state.get("agent_model_name", default_model) or default_model)
    for model_candidate in (
        selected_primary_model,
        str(st.session_state.get("agent_custom_model_override", "") or "").strip(),
        str(st.session_state.get("agent_definition_model_name", "") or "").strip(),
        str(st.session_state.get("agent_planner_model_name", "") or "").strip(),
    ):
        if model_candidate and model_candidate not in primary_models:
            primary_models = [*primary_models, model_candidate]

    missing_provider_keys = []
    if _is_openai_compatible_provider(primary_provider):
        if not _ensure_openai_compatible_base_url_available():
            missing_provider_keys.append((primary_provider, OPENAI_COMPATIBLE_BASE_URL_ENV))
    elif not _ensure_provider_api_key_available(primary_provider, primary_api_key_env):
        missing_provider_keys.append((primary_provider, primary_api_key_env))
    if _is_openai_compatible_provider(primary_provider) and not missing_provider_keys:
        if _openai_compatible_catalog_requires_api_key(primary_catalog):
            env_name = primary_api_key_env or get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            missing_provider_keys.append((OPENAI_COMPATIBLE_PROVIDER, env_name))

    workflow_config_payload = _build_workflow_config_from_state(defaults)
    workflow_config_payload["provider"] = primary_provider
    workflow_config_payload["model"] = selected_primary_model
    readiness_state = _build_run_readiness_state(REQUIRED_MATCHING_TABLE_COLUMNS, missing_provider_keys)

    mui_workflow_config = _render_workflow_config_panel(
        workflow_config_payload,
        provider_options=provider_options,
        model_options=primary_models,
        readiness_state=readiness_state,
        primary_catalog=primary_catalog,
        selected_model=selected_primary_model,
    )

    info_text: Optional[str] = None
    if missing_provider_keys:
        provider_lines: list[str] = []
        for provider, env_name in missing_provider_keys:
            default_env = get_default_api_key_env(provider)
            label = get_provider_label(provider)
            if env_name and env_name != default_env:
                provider_line = f"- {label}: expected `{default_env}`, currently `{env_name}`"
            else:
                provider_line = f"- {label}: set `{default_env}`"
            provider_lines.append(provider_line)
        missing_lines = "\n".join(provider_lines)
        codex_missing = any(provider == OPENAI_CODEX_PROVIDER for provider, _ in missing_provider_keys)
        openai_compatible_missing = any(provider == OPENAI_COMPATIBLE_PROVIDER for provider, _ in missing_provider_keys)
        if codex_missing:
            info_text = (
                "Before you start, complete ChatGPT Subscription sign-in for the `openai_codex` provider.\n"
                f"{missing_lines}"
            )
        elif openai_compatible_missing:
            info_text = (
                "Before you start, please set the OpenAI-compatible base URL (e.g. localhost address) so the app can call "
                "`/v1/models` and `/v1/chat/completions`.\n"
                f"{missing_lines}"
            )
        else:
            info_text = (
                "Before you start, please add the missing API key(s) in `config.yaml` under "
                "`agent_reconciliation.provider_api_keys` or set these environment variable(s):\n"
                f"{missing_lines}"
            )

    if isinstance(mui_workflow_config, dict):
        action = mui_workflow_config.get("action") if isinstance(mui_workflow_config.get("action"), dict) else {}
        action_nonce = action.get("nonce") if isinstance(action, dict) else None
        action_type = str(action.get("type", "") or "") if isinstance(action, dict) else ""
        changed = _apply_workflow_config_to_session_state(mui_workflow_config)

        if action_nonce and st.session_state.get(AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY) != action_nonce:
            st.session_state[AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY] = action_nonce
            latest_provider = str(st.session_state.get("agent_model_provider", primary_provider) or primary_provider)
            latest_api_key_env = _resolve_api_key_env_for_provider(
                "agent_model_api_key_env",
                latest_provider,
                "model_api_key_env",
            )
            latest_model = str(st.session_state.get("agent_model_name", selected_primary_model) or selected_primary_model)

            if action_type == "reloadModels":
                fetch_all_pricing(force_refresh=True)
                _ensure_model_catalog_for_provider(
                    latest_provider,
                    api_key_env=latest_api_key_env,
                    force_refresh=True,
                )
                st.rerun()
            elif action_type == "registerLocalModel":
                ok, register_message = _register_openai_compatible_model_from_override(
                    str(st.session_state.get("agent_custom_model_override", "") or ""),
                    latest_provider,
                    latest_api_key_env,
                )
                if ok:
                    st.success(register_message)
                    st.rerun()
                else:
                    st.error(register_message)
            elif action_type == "saveConfiguration":
                ok, msg = _save_preferred_model_selection(latest_provider, latest_model)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
            elif action_type == "goToRun" and readiness_state.get("ready"):
                st.session_state[AGENT_ACTIVE_STEP_KEY] = "Run"
                st.components.v1.html(
                    """
                    <script>
                        function scrollToTop() {
                            const mainContent = window.parent.document.querySelector('section.main');
                            if (mainContent) {
                                mainContent.scrollTo({ top: 0, behavior: 'smooth' });
                            } else {
                                window.parent.scrollTo({ top: 0, behavior: 'smooth' });
                            }
                        }
                        setTimeout(scrollToTop, 10);
                    </script>
                    """,
                    height=0,
                )
                st.rerun()
            elif action_type == "setStage":
                target_stage = str(action.get("stage", "") or "")
                if target_stage in {"Setup", "Run", "Review", "Export"}:
                    st.session_state[AGENT_ACTIVE_STEP_KEY] = target_stage
                    st.components.v1.html("<script>window.parent.scrollToTop();</script>", height=0)
                    st.rerun()

        if changed:
            st.rerun()

    if info_text:
        info_placeholder.info(info_text)
    else:
        info_placeholder.empty()

    return {
        "primary_provider": st.session_state.get("agent_model_provider", primary_provider),
        "primary_catalog": primary_catalog,
        "primary_models": primary_models,
        "effective_primary_env": _resolve_api_key_env_for_provider(
            "agent_model_api_key_env",
            str(st.session_state.get("agent_model_provider", primary_provider)),
            "model_api_key_env",
        ),
        "missing_provider_keys": missing_provider_keys,
    }

def _render_provenance_section(provenance_defaults_cfg: Dict[str, str]):
    _render_agent_section_header(
        "4",
        "Provenance & curation metadata",
        "Optionally enrich accepted mappings with author, reviewer, tool, and publication metadata for SSSOM export.",
        tooltip="These fields are not required for running reconciliation, but they improve traceability once mappings are published downstream.",
    )
    st.caption("Provenance metadata is optional.")
    provenance_enabled = st.checkbox(
        "Include provenance metadata",
        value=bool(st.session_state.get("agent_enable_provenance_metadata", False)),
        key="agent_enable_provenance_metadata",
        help="Enable to add optional provenance and curation metadata to SSSOM export fields.",
    )

    previously_enabled = bool(st.session_state.get("agent_provenance_toggle_previous", False))
    if provenance_enabled and not previously_enabled:
        st.session_state["agent_prov_author_orcid"] = provenance_defaults_cfg.get("author_id", "")
        st.session_state["agent_prov_author_name"] = provenance_defaults_cfg.get("author_label", "")
        st.session_state["agent_prov_reviewer_orcid"] = provenance_defaults_cfg.get("reviewer_id", "")
        st.session_state["agent_prov_reviewer_name"] = provenance_defaults_cfg.get("reviewer_label", "")
        st.session_state["agent_prov_creator_orcid"] = provenance_defaults_cfg.get("creator_id", "")
        st.session_state["agent_prov_creator_name"] = provenance_defaults_cfg.get("creator_label", "")
        st.session_state["agent_prov_mapping_tool"] = provenance_defaults_cfg.get(
            "mapping_tool", "RDF4Risk Agent-Based Reconciliation"
        )
        st.session_state["agent_prov_mapping_tool_version"] = provenance_defaults_cfg.get(
            "mapping_tool_version", "PoC"
        )
        st.session_state["agent_prov_publication_date"] = provenance_defaults_cfg.get("publication_date", "")
    st.session_state["agent_provenance_toggle_previous"] = provenance_enabled

    if provenance_enabled:
        with st.expander("Provenance settings", expanded=True):
            st.caption(
                "Provide mapping provenance metadata for SSSOM export. ORCID accepts full URL or plain ID; "
                "plain IDs are normalized to https://orcid.org/<ID>. Mapping Date is set automatically "
                "to the workflow run date."
            )

            pcol1, pcol2 = st.columns(2)
            with pcol1:
                st.text_input("Author ORCID", key="agent_prov_author_orcid", help="Examples: 0000-0003-4691-0483 or https://orcid.org/0000-0003-4691-0483")
                st.text_input("Author Name", key="agent_prov_author_name")
                st.text_input("Reviewer ORCID", key="agent_prov_reviewer_orcid")
                st.text_input("Reviewer Name", key="agent_prov_reviewer_name")
                st.text_input("Creator ORCID", key="agent_prov_creator_orcid")
                st.text_input("Creator Name", key="agent_prov_creator_name")

            with pcol2:
                st.text_input("Mapping Tool", key="agent_prov_mapping_tool")
                st.text_input("Mapping Tool Version", key="agent_prov_mapping_tool_version")
                st.caption("Mapping Date is generated automatically when the workflow runs.")
                st.text_input("Publication Date (YYYY-MM-DD, optional)", key="agent_prov_publication_date")

            normalized_author = _normalize_orcid_identifier(st.session_state.get("agent_prov_author_orcid", ""))
            normalized_reviewer = _normalize_orcid_identifier(st.session_state.get("agent_prov_reviewer_orcid", ""))
            normalized_creator = _normalize_orcid_identifier(st.session_state.get("agent_prov_creator_orcid", ""))

            if st.session_state.get("agent_prov_author_orcid") and not normalized_author:
                st.warning("Author ORCID is not valid. Expected format: 0000-0000-0000-0000 (or full ORCID URL).")
            if st.session_state.get("agent_prov_reviewer_orcid") and not normalized_reviewer:
                st.warning("Reviewer ORCID is not valid. Expected format: 0000-0000-0000-0000 (or full ORCID URL).")
            if st.session_state.get("agent_prov_creator_orcid") and not normalized_creator:
                st.warning("Creator ORCID is not valid. Expected format: 0000-0000-0000-0000 (or full ORCID URL).")

            if normalized_author:
                st.caption(f"Normalized author_id: {normalized_author}")
            if normalized_reviewer:
                st.caption(f"Normalized reviewer_id: {normalized_reviewer}")
            if normalized_creator:
                st.caption(f"Normalized creator_id: {normalized_creator}")

            if st.button("Save provenance defaults for next time", key="agent_save_provenance_defaults"):
                defaults = _build_provenance_defaults_from_state()
                ok, msg = _save_preferred_provenance_defaults(defaults)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)


def _render_run_agent_section(input_tables, missing_provider_keys, primary_provider, effective_primary_env):
    _render_agent_section_header(
        "5",
        "Run agent-based reconciliation",
        "Start the selected agent workflow once data and provider access are ready.",
        tooltip="The loader below is a readiness indicator. During execution, Streamlit's progress bar reports processed terms in real time.",
    )

    stop_event = st.session_state.get(AGENT_STOP_EVENT_KEY, {})
    if isinstance(stop_event, dict) and stop_event.get("stop_reason") == "llm_error":
        stopped_term = str(stop_event.get("term", "") or "").strip() or "(unknown term)"
        stopped_file = str(stop_event.get("file", "") or "").strip() or "(unknown source)"
        err_type = str(stop_event.get("fallback_error_type", "") or "").strip()
        err_msg = str(stop_event.get("fallback_error_message", "") or "").strip()
        fix_tip = str(stop_event.get("llm_fix_suggestion", "") or "").strip()

        st.warning(
            f"Previous run stopped automatically due to an LLM error in source '{stopped_file}' at term '{stopped_term}'."
        )
        if err_type or err_msg:
            st.code(f"{err_type}: {err_msg}".strip(": "))
        if fix_tip:
            st.info(f"Suggested fix: {fix_tip}")

        decision = st.radio(
            "How do you want to proceed?",
            options=["Fix issue and rerun", "Continue with heuristic fallback"],
            key="agent_llm_error_stop_decision",
        )
        if decision == "Fix issue and rerun":
            st.caption(
                "After fixing credentials/model/endpoint, run again. The pipeline will still auto-stop on new LLM errors."
            )
        else:
            st.caption(
                "Continuation mode resumes from remaining unreconciled terms and keeps heuristic fallback when LLM errors occur."
            )

    run_disabled = (not bool(input_tables)) or bool(missing_provider_keys)
    readiness_title = "Ready to run" if not run_disabled else "Waiting for prerequisites"
    if not bool(input_tables):
        readiness_note = "Load a matching table before starting the agent workflow."
    elif missing_provider_keys:
        readiness_note = "Resolve the provider authentication or endpoint message above before running."
    else:
        readiness_note = "Input data and provider configuration are available. The next run can start safely."
    st.markdown(
        f"""
        <div class="agent-run-readiness">
            <div class="agent-loader {'paused' if run_disabled else ''}"></div>
            <div>
                <div class="agent-run-title">{_escape_html(readiness_title)}</div>
                <div class="agent-run-note">{_escape_html(readiness_note)}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("Run Agent-Based Reconciliation", key="agent_run_reconciliation", disabled=run_disabled):
        try:
            st.session_state["agent_prov_last_run_mapping_date"] = date.today().isoformat()
            config = _build_run_config_from_state()
            stop_decision = str(st.session_state.get("agent_llm_error_stop_decision", "Fix issue and rerun") or "Fix issue and rerun")
            continue_with_heuristics = stop_decision == "Continue with heuristic fallback"
            resume_previous = bool(continue_with_heuristics and isinstance(stop_event, dict) and stop_event.get("stop_reason") == "llm_error")
            config.stop_on_llm_error = not continue_with_heuristics
            tables_for_run = _build_run_input_tables(
                input_tables,
                st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {}),
                resume_previous=resume_previous,
            )
            run_started_perf = time.perf_counter()
            run_started_epoch = time.time()

            configured_project = configure_langsmith_environment(config.langsmith_project)
            config.langsmith_project = configured_project
            reset_llm_interactions()
            langsmith_state = get_langsmith_readiness(config.langsmith_project)
            st.session_state[AGENT_MONITORING_STATE_KEY] = {
                "enabled": bool(st.session_state.get("agent_use_langsmith_monitoring", False)),
                "run_id": None,
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(run_started_epoch)),
                "finished_at": None,
                "duration_sec": None,
                "total_terms": 0,
                "processed_terms": 0,
                "failed_terms": 0,
                "stop_reason": None,
                "stop_event": {},
                "events_df": pd.DataFrame(),
                "llm_interactions_df": pd.DataFrame(),
                "cascade_trace_df": pd.DataFrame(),
                "raw_term_events": [],
                "langsmith": {
                    **langsmith_state,
                    "run_url": None,
                },
            }
            definitions_by_source: Dict[str, Dict[str, str]] = {}
            definition_preparation_enabled = bool(st.session_state.get("agent_enable_definition_preparation", False))

            for table in tables_for_run:
                if not definition_preparation_enabled:
                    used_defs_df = pd.DataFrame(columns=["Term", "Definition"])
                else:
                    strategy = config.definition_strategy
                    if strategy == "uploaded_sheet":
                        uploaded_defs = st.session_state.get(AGENT_DEFINITIONS_BY_SOURCE_KEY, {}).get("__uploaded_sheet__")
                        used_defs_df = prepare_used_definitions_df(table.dataframe, strategy, uploaded_definitions_df=uploaded_defs)
                    else:
                        if strategy == "reference_publication":
                            context_text = st.session_state.get("agent_reference_publication_text", "")
                        else:
                            context_text = st.session_state.get("agent_definition_context_text", "")
                        used_defs_df = prepare_used_definitions_df(
                            table.dataframe,
                            strategy,
                            context_text=context_text,
                            model_name=config.definition_model_name,
                            provider=config.definition_model_provider,
                            api_key_env=config.definition_model_api_key_env,
                            reasoning_effort=config.reasoning_effort,
                        )
                definitions_by_source[table.source_name] = build_definition_lookup(used_defs_df)
                st.session_state[AGENT_DEFINITIONS_BY_SOURCE_KEY][table.source_name] = used_defs_df

            progress_placeholder = st.empty()
            message_placeholder = st.empty()
            latest_batch_state: Dict[str, object] = {"state": None}

            def _progress_callback(state):
                latest_batch_state["state"] = state
                progress_total = max(state.total_terms, 1)
                progress_value = min(1.0, state.processed_terms / progress_total)
                progress_placeholder.progress(progress_value, text=f"Processed {state.processed_terms} / {progress_total} term(s)")
                if state.messages:
                    message_placeholder.caption(state.messages[-1])
                st.session_state[AGENT_RUN_MESSAGES_KEY] = state.messages
                monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
                if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
                    events_df = _build_monitoring_event_snapshot(state)
                    monitoring_state["run_id"] = getattr(state, "run_id", None)
                    monitoring_state["total_terms"] = int(getattr(state, "total_terms", 0) or 0)
                    monitoring_state["processed_terms"] = int(getattr(state, "processed_terms", 0) or 0)
                    monitoring_state["failed_terms"] = int(getattr(state, "failed_terms", 0) or 0)
                    monitoring_state["events_df"] = events_df
                    
                    interactions = get_llm_interactions(limit=500)
                    interactions_df = pd.DataFrame(interactions)
                    monitoring_state["llm_interactions_df"] = interactions_df
                    if not interactions_df.empty and "cost_usd" in interactions_df.columns:
                        monitoring_state["total_cost_usd"] = float(interactions_df["cost_usd"].fillna(0).sum())
                    
                    monitoring_state["cascade_trace_df"] = _build_cascade_trace_snapshot(state)
                    monitoring_state["raw_term_events"] = list(getattr(state, "term_events", []) or [])

                    langsmith_dict = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
                    if langsmith_dict.get("project") and getattr(state, "run_id", None):
                        langsmith_dict["run_url"] = build_run_url(str(langsmith_dict.get("project")), str(state.run_id))
                        monitoring_state["langsmith"] = langsmith_dict

                    st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state

            outputs = run_agent_batch(
                tables_for_run,
                config,
                definitions_by_source=definitions_by_source,
                bioportal_api_key=(CONFIG or {}).get("bioportal", {}).get("api_key"),
                progress_callback=_progress_callback,
                resume_skip_processed_terms=resume_previous,
            )

            st.session_state[AGENT_RESULTS_BY_SOURCE_KEY] = outputs
            if outputs and not st.session_state.get(AGENT_SELECTED_SOURCE_KEY):
                st.session_state[AGENT_SELECTED_SOURCE_KEY] = list(outputs.keys())[0]
            _sync_selected_source_dataframe()

            monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
            if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
                monitoring_state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                monitoring_state["duration_sec"] = time.perf_counter() - run_started_perf
                st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state

            llm_stop_event = {}
            latest_state_obj = latest_batch_state.get("state")
            if latest_state_obj is not None and getattr(latest_state_obj, "stop_reason", None) == "llm_error":
                state_stop_event = getattr(latest_state_obj, "stop_event", {}) or {}
                if isinstance(state_stop_event, dict):
                    llm_stop_event = {
                        "stop_reason": "llm_error",
                        "file": state_stop_event.get("file"),
                        "term": state_stop_event.get("term"),
                        "fallback_error_type": state_stop_event.get("fallback_error_type"),
                        "fallback_error_message": state_stop_event.get("fallback_error_message"),
                        "fallback_reason": state_stop_event.get("fallback_reason"),
                        "llm_fix_suggestion": state_stop_event.get("llm_fix_suggestion"),
                        "workflow": state_stop_event.get("workflow"),
                        "decision_source": state_stop_event.get("decision_source"),
                    }
            elif isinstance(monitoring_state, dict):
                final_events = list(monitoring_state.get("raw_term_events", []) or [])
                for event in reversed(final_events):
                    if isinstance(event, dict) and bool(event.get("llm_error_stop")):
                        llm_stop_event = {
                            "stop_reason": "llm_error",
                            "file": event.get("file"),
                            "term": event.get("term"),
                            "fallback_error_type": event.get("fallback_error_type"),
                            "fallback_error_message": event.get("fallback_error_message"),
                            "fallback_reason": event.get("fallback_reason"),
                            "llm_fix_suggestion": event.get("llm_fix_suggestion"),
                            "workflow": event.get("workflow"),
                            "decision_source": event.get("decision_source"),
                        }
                        break

            if llm_stop_event:
                st.session_state[AGENT_STOP_EVENT_KEY] = llm_stop_event
                if isinstance(monitoring_state, dict):
                    monitoring_state["stop_reason"] = "llm_error"
                    monitoring_state["stop_event"] = llm_stop_event
                    st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
                stopped_term = str(llm_stop_event.get("term", "") or "").strip() or "(unknown term)"
                st.warning(
                    f"Run stopped automatically due to LLM error at term '{stopped_term}'. "
                    "Choose Fix issue and rerun, or Continue with heuristic fallback."
                )
            else:
                st.session_state[AGENT_STOP_EVENT_KEY] = {}
                if isinstance(monitoring_state, dict):
                    monitoring_state["stop_reason"] = None
                    monitoring_state["stop_event"] = {}
                    st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
                st.success("Agent-based reconciliation run completed.")
            # Force a fresh render cycle so sections 6/7 use the updated
            # session-state dataframe/results immediately after completion.
            st.components.v1.html("<script>window.parent.scrollToTop();</script>", height=0)
            st.rerun()
        except Exception as exc:
            if _is_openai_compatible_provider(primary_provider) and is_openai_compatible_auth_required_error(exc):
                env_name = effective_primary_env or get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
                st.error(
                    "OpenAI-compatible endpoint rejected unauthenticated requests. "
                    f"Please set `{env_name}` (or the OpenAI-compatible API key field) and run again."
                )
                return
            monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
            if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
                monitoring_state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                monitoring_state["duration_sec"] = time.perf_counter() - run_started_perf if "run_started_perf" in locals() else None
                langsmith_dict = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
                existing = str(langsmith_dict.get("message", "") or "").strip()
                langsmith_dict["message"] = (existing + " " if existing else "") + f"Run failed: {exc}"
                monitoring_state["langsmith"] = langsmith_dict
                st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
            st.error(f"Agent-based reconciliation failed: {exc}")

    if st.session_state.get(AGENT_RUN_MESSAGES_KEY):
        with st.expander("Run Messages", expanded=False):
            for message in st.session_state[AGENT_RUN_MESSAGES_KEY][-50:]:
                st.text(message)

    _render_monitoring_panel()


def _render_monitoring_section():
    _render_monitoring_panel()


def _render_review_suggestions_section(agent_df):
    _render_agent_section_header(
        "6",
        "Working table preview & curator review",
        "Inspect the enriched table, review pending suggestions by SKOS match type, and accept, reject, or reset recommendations.",
        tooltip="Cards and grouped expanders are optimized for reviewing the first pending suggestions without losing table context.",
    )
    def _comparison_row(left_label: str, left_value: str, right_label: str, right_value: str):
        row_left, row_right = st.columns(2)
        with row_left:
            st.markdown(f"**{left_label}**")
            st.write(left_value)
        with row_right:
            st.markdown(f"**{right_label}**")
            st.write(right_value)

    if isinstance(agent_df, pd.DataFrame):
        _render_working_table_metrics(agent_df)
        st.markdown("#### Results table (live working table)")
        st.dataframe(agent_df, use_container_width=True)

        if st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY):
            auto_accepted_indices = list(
                agent_df[
                    (agent_df.get("Review Status", "").astype(str).str.strip().str.lower() == "accepted")
                    & (
                        agent_df.get("Auto Accepted", False)
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .isin(["true", "1", "yes"])
                    )
                ].index
            )
            pending_review_indices = _get_reviewable_agent_result_indices(agent_df)
            st.write(f"Rows with pending suggestions to review: {len(pending_review_indices)}")
            st.write(f"Rows auto-accepted by policy: {len(auto_accepted_indices)}")

            if auto_accepted_indices:
                auto_limit = min(len(auto_accepted_indices), 20)
                with st.expander(f"Auto-accepted rows (showing up to {auto_limit})", expanded=False):
                    for idx in auto_accepted_indices[:auto_limit]:
                        with st.expander(f"Auto-accepted row {idx}: {agent_df.at[idx, 'Term']}", expanded=False):
                            st.markdown("**Input vs Agent suggestion**")
                            _comparison_row(
                                "Input term",
                                _get_review_cell_value(agent_df, idx, "Term"),
                                "Suggested term",
                                _get_review_cell_value(agent_df, idx, "Suggested Label"),
                            )
                            _comparison_row(
                                "Input definition",
                                _get_review_cell_value(agent_df, idx, "Definition"),
                                "Suggested description",
                                _get_review_cell_value(agent_df, idx, "Suggested Description"),
                            )
                            _comparison_row(
                                "Input URI",
                                _get_review_cell_value(agent_df, idx, "URI"),
                                "Suggested URI",
                                _get_review_cell_value(agent_df, idx, "Suggested URI"),
                            )
                            _comparison_row(
                                "Current/accepted match type",
                                _get_review_cell_value(agent_df, idx, "Match Type"),
                                "Suggested match type",
                                _get_review_cell_value(agent_df, idx, "Suggested Match Type"),
                            )

                            st.write(f"**Suggested confidence:** {agent_df.at[idx, 'Suggested Confidence']}")
                            st.write(f"**Auto acceptance score:** {agent_df.at[idx, 'Auto Acceptance Score']}")
                            st.write(f"**Auto acceptance reason:** {agent_df.at[idx, 'Auto Accept Reason']}")
                            st.write(
                                f"**LLM justification (exact-match rationale):** "
                                f"{_get_review_cell_value(agent_df, idx, 'Agent Explanation')}"
                            )
                            st.write(f"**Auto accepted at:** {agent_df.at[idx, 'Auto Accepted At']}")
                            source_name = st.session_state.get(AGENT_SELECTED_SOURCE_KEY)
                            revoke_cols = st.columns(2)
                            if revoke_cols[0].button("Revoke auto-accept (Reset)", key=f"agent_revoke_auto_{source_name}_{idx}"):
                                _apply_review_action(source_name, idx, "reset")
                                st.rerun()
                            if revoke_cols[1].button("Mark rejected", key=f"agent_reject_auto_{source_name}_{idx}"):
                                _apply_review_action(source_name, idx, "reject")
                                st.rerun()

            if pending_review_indices:
                col_actions = st.columns(3)
                if col_actions[0].button("Accept All Pending Suggestions", key="agent_accept_all_pending"):
                    _accept_all_pending(st.session_state.get(AGENT_SELECTED_SOURCE_KEY))
                    st.rerun()

                review_limit = min(len(pending_review_indices), 20)
                visible_pending_indices = pending_review_indices[:review_limit]
                if review_limit < len(pending_review_indices):
                    st.caption(
                        f"Showing the first {review_limit} pending rows for review. "
                        "Use source filtering or rerun with narrower scope for additional rows."
                    )

                grouped_indices = _group_pending_review_indices_by_match_type(agent_df, visible_pending_indices)

                for match_group in REVIEW_MATCH_GROUP_ORDER:
                    group_label = REVIEW_MATCH_GROUP_LABELS.get(match_group, match_group)
                    indices_for_group = grouped_indices.get(match_group, [])
                    st.markdown(f"##### {group_label} ({len(indices_for_group)})")

                    if not indices_for_group:
                        st.caption("No rows in this match category.")
                        continue

                    for idx in indices_for_group:
                        term_value = _get_review_cell_value(agent_df, idx, "Term")
                        with st.expander(f"Review row {idx}: {term_value}", expanded=False):
                            has_suggested_uri = bool(_get_review_cell_value(agent_df, idx, "Suggested URI").strip())
                            row_is_terminal_no_match = (
                                _get_review_cell_value(agent_df, idx, "Review Status").strip().lower()
                                in {"no_match", "timeout"}
                            )
                            suggested_match_type = normalize_mapping_type(
                                _get_review_cell_value(agent_df, idx, "Suggested Match Type")
                            )
                            if suggested_match_type not in REVIEW_MATCH_TYPE_OPTIONS:
                                suggested_match_type = "skos:closeMatch"

                            source_name = st.session_state.get(AGENT_SELECTED_SOURCE_KEY)
                            selected_match_type = suggested_match_type
                            if has_suggested_uri:
                                selected_match_type = st.selectbox(
                                    "Adjust SKOS match type before accepting",
                                    options=list(REVIEW_MATCH_TYPE_OPTIONS),
                                    index=list(REVIEW_MATCH_TYPE_OPTIONS).index(suggested_match_type),
                                    key=f"agent_selected_match_type_{source_name}_{idx}",
                                )
                                _render_skos_match_badge(_normalize_review_match_group(selected_match_type))
                            elif row_is_terminal_no_match:
                                _render_skos_match_badge("no_match")
                                st.caption("No candidate was found for this term; review the explanation below.")

                            st.markdown("**Input vs Agent suggestion**")
                            _comparison_row(
                                "Input term",
                                term_value,
                                "Suggested term",
                                _get_review_cell_value(agent_df, idx, "Suggested Label"),
                            )
                            _comparison_row(
                                "Input definition",
                                _get_review_cell_value(agent_df, idx, "Definition"),
                                "Suggested description",
                                _get_review_cell_value(agent_df, idx, "Suggested Description"),
                            )
                            _comparison_row(
                                "Input URI",
                                _get_review_cell_value(agent_df, idx, "URI"),
                                "Suggested URI",
                                _get_review_cell_value(agent_df, idx, "Suggested URI"),
                            )
                            _comparison_row(
                                "Current/accepted match type",
                                _get_review_cell_value(agent_df, idx, "Match Type"),
                                "Suggested match type",
                                _get_review_cell_value(agent_df, idx, "Suggested Match Type"),
                            )
                            _comparison_row(
                                "Input subject label",
                                _get_review_cell_value(agent_df, idx, "subject_label"),
                                "Suggested provider",
                                _get_review_cell_value(agent_df, idx, "Suggested Provider"),
                            )

                            st.write(f"**Suggested confidence:** {_get_review_cell_value(agent_df, idx, 'Suggested Confidence')}")
                            st.write(f"**Decision source:** {_get_review_cell_value(agent_df, idx, 'Suggested Decision Source')}")
                            st.write(f"**Fallback reason:** {_get_review_cell_value(agent_df, idx, 'Suggested Fallback Reason')}")
                            st.write(f"**Auto acceptance score:** {_get_review_cell_value(agent_df, idx, 'Auto Acceptance Score')}")
                            st.write(f"**Auto acceptance reason:** {_get_review_cell_value(agent_df, idx, 'Auto Accept Reason')}")
                            st.write(f"**Explanation:** {_get_review_cell_value(agent_df, idx, 'Agent Explanation')}")

                            if has_suggested_uri:
                                action_cols = st.columns(3)
                                if action_cols[0].button("Accept", key=f"agent_accept_{source_name}_{idx}"):
                                    _apply_review_action(
                                        source_name,
                                        idx,
                                        "accept",
                                        selected_match_type=selected_match_type,
                                    )
                                    st.rerun()
                                if action_cols[1].button("Reject", key=f"agent_reject_{source_name}_{idx}"):
                                    _apply_review_action(source_name, idx, "reject")
                                    st.rerun()
                                if action_cols[2].button("Reset", key=f"agent_reset_{source_name}_{idx}"):
                                    _apply_review_action(source_name, idx, "reset")
                                    st.rerun()
                            elif row_is_terminal_no_match:
                                no_match_cols = st.columns(2)
                                if no_match_cols[0].button("Acknowledge no match", key=f"agent_ack_no_match_{source_name}_{idx}"):
                                    _apply_review_action(source_name, idx, "reject")
                                    st.rerun()
                                if no_match_cols[1].button("Reset", key=f"agent_reset_no_match_{source_name}_{idx}"):
                                    _apply_review_action(source_name, idx, "reset")
                                    st.rerun()
            else:
                st.caption("No pending agent suggestions are waiting for review in the selected source table.")
    if not isinstance(agent_df, pd.DataFrame):
        st.caption("No agent-based reconciliation working table has been loaded yet.")
    st.info("ReviewSuggestionsTable is planned as the next React/Material-UI component: filters for pending/accepted/rejected/no_match plus a detail drawer with candidate URI, description, explanation, Accept/Reject/Reset.")


def _render_export_section(agent_df):
    if isinstance(agent_df, pd.DataFrame):
        _render_agent_section_header(
            "7",
            "Publish or download results",
            "Finalize accepted mappings for the RDF Generator handoff or export the curated reconciliation table.",
            tooltip="Publishing writes the finalized table to shared session state; downloading creates a CSV snapshot for local curation records.",
        )
        export_provenance_defaults = _build_provenance_defaults_from_state()
        if st.button("Publish Current Working Table to RDF Generator Handoff", key="agent_publish_current_table"):
            st.session_state["shared_reconciled_matching_table"] = finalize_accepted_results(
                agent_df.copy(),
                provenance_defaults=export_provenance_defaults,
            )
            st.success("Current working table published to shared_reconciled_matching_table.")

        selected_source = st.session_state.get(AGENT_SELECTED_SOURCE_KEY) or "agent_reconciliation"
        export_df = finalize_accepted_results(
            agent_df.copy(),
            provenance_defaults=export_provenance_defaults,
        )
        export_filename = f"{selected_source}_agent_reconciled.csv"
        create_download_link(export_df, export_filename, f"Download '{export_filename}'")
    else:
        st.caption("No agent-based reconciliation working table has been loaded yet.")



_STAGE_TO_COMPONENT = {"Setup": "setup", "Run": "run", "Review": "review", "Export": "export"}
_COMPONENT_TO_STAGE = {value: key for key, value in _STAGE_TO_COMPONENT.items()}


def _component_stage_from_session() -> str:
    return _STAGE_TO_COMPONENT.get(_get_active_agent_stage(), "setup")


def _stage_from_component(value: object) -> Optional[str]:
    return _COMPONENT_TO_STAGE.get(str(value or "").strip().lower())


def _json_safe_value(value: object):
    if pd.isna(value):
        return ""
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value


def _dataframe_records(df: Optional[pd.DataFrame], limit: int = 25) -> List[Dict[str, object]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    preview = df.head(limit).copy()
    preview = preview.where(pd.notna(preview), "")
    return [
        {str(key): _json_safe_value(value) for key, value in row.items()}
        for row in preview.to_dict(orient="records")
    ]


def _build_data_status_snapshot(required_columns) -> Dict[str, object]:
    shared_df = st.session_state.get("shared_matching_table")
    agent_df = st.session_state.get(AGENT_DATAFRAME_STATE_KEY)
    input_tables = st.session_state.get(AGENT_INPUT_TABLES_KEY, [])
    selected_source = st.session_state.get(AGENT_SELECTED_SOURCE_KEY)
    schema_df = agent_df if isinstance(agent_df, pd.DataFrame) else shared_df if isinstance(shared_df, pd.DataFrame) else None
    filename = ""
    source_name = str(selected_source or "")
    if schema_df is None and isinstance(input_tables, list) and input_tables:
        first_table = input_tables[0]
        schema_df = getattr(first_table, "dataframe", None)
        filename = str(getattr(first_table, "filename", "") or "")
        source_name = str(getattr(first_table, "source_name", "") or source_name)
    elif isinstance(input_tables, list) and input_tables:
        first_table = input_tables[0]
        filename = str(getattr(first_table, "filename", "") or "")
        source_name = str(getattr(first_table, "source_name", "") or source_name)
    if isinstance(shared_df, pd.DataFrame) and not source_name:
        source_name = "Matching Table Generator"
        filename = "shared_matching_table"

    rows = len(schema_df) if isinstance(schema_df, pd.DataFrame) else 0
    columns = len(schema_df.columns) if isinstance(schema_df, pd.DataFrame) else 0
    required_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in required_columns))
    legacy_detected = bool(isinstance(schema_df, pd.DataFrame) and all(col in schema_df.columns for col in LEGACY_REQUIRED_MATCHING_TABLE_COLUMNS))
    schema_message = "No table loaded"
    if required_detected:
        schema_message = "Canonical SSSOM columns"
    elif legacy_detected:
        schema_message = "Legacy columns will be normalized"
    elif isinstance(schema_df, pd.DataFrame):
        missing = [col for col in required_columns if col not in schema_df.columns]
        schema_message = f"Missing {len(missing)} required column(s)"

    return {
        "has_table": isinstance(schema_df, pd.DataFrame),
        "filename": filename,
        "source_name": source_name,
        "rows": rows,
        "columns": columns,
        "loaded_sources": len(input_tables) if isinstance(input_tables, list) else 0,
        "required_columns_detected": required_detected or legacy_detected,
        "schema_message": schema_message,
        "upload_bridge_available": True,
        "shared_table_available": isinstance(shared_df, pd.DataFrame),
        "preview": _dataframe_records(schema_df, limit=12),
    }


def _build_run_status_snapshot(readiness_state: Dict[str, object]) -> Dict[str, object]:
    monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
    live_status = st.session_state.get(AGENT_RUN_STATUS_STATE_KEY, {})
    if not isinstance(live_status, dict):
        live_status = {}
    results = st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {})
    processed = int(
        live_status.get("processed_count", monitoring_state.get("processed_terms", 0) if isinstance(monitoring_state, dict) else 0)
        or 0
    )
    total = int(
        live_status.get("total_count", monitoring_state.get("total_terms", 0) if isinstance(monitoring_state, dict) else 0)
        or 0
    )
    progress = int((processed / max(total, 1)) * 100) if total else (100 if results else 0)
    error = None
    if isinstance(monitoring_state, dict) and monitoring_state.get("stop_reason"):
        error = str(monitoring_state.get("stop_reason"))
    status_message = st.session_state.get("agent_mui_status_message")
    if isinstance(status_message, dict) and status_message.get("severity") == "error":
        error = str(status_message.get("text") or "Agent-based reconciliation failed.")
    messages = st.session_state.get(AGENT_RUN_MESSAGES_KEY, [])
    latest_message = str(
        live_status.get("message")
        or (messages[-1] if isinstance(messages, list) and messages else "")
        or ("Run completed" if results else "Ready to run" if readiness_state.get("ready") else "Waiting for prerequisites")
    )
    elapsed_seconds = live_status.get("elapsed_seconds")
    if elapsed_seconds is None and isinstance(monitoring_state, dict):
        elapsed_seconds = monitoring_state.get("duration_sec")
    estimated_remaining_seconds = live_status.get("estimated_remaining_seconds")
    if estimated_remaining_seconds is None and total and processed > 0 and elapsed_seconds:
        estimated_remaining_seconds = (float(elapsed_seconds) / processed) * max(0, total - processed)
    return {
        "ready": bool(readiness_state.get("ready")),
        "running": bool(live_status.get("running", False)),
        "finished": bool(results),
        "error": error,
        "progress": progress,
        "stage": live_status.get("stage") or ("writing_output" if results else None),
        "message": latest_message,
        "current_term": live_status.get("current_term"),
        "processed_count": processed if total else None,
        "total_count": total if total else None,
        "started_at": live_status.get("started_at") or (monitoring_state.get("started_at") if isinstance(monitoring_state, dict) else None),
        "elapsed_seconds": elapsed_seconds,
        "estimated_remaining_seconds": estimated_remaining_seconds,
        "last_activity": live_status.get("last_activity"),
        "messages": messages[-80:] if isinstance(messages, list) else [],
    }


def _records_from_monitoring_df(monitoring_state: Dict[str, object], key: str, limit: int = 200) -> List[Dict[str, object]]:
    df = monitoring_state.get(key) if isinstance(monitoring_state, dict) else None
    if isinstance(df, pd.DataFrame) and not df.empty:
        return _dataframe_records(df.tail(limit), limit=limit)
    return []


def _build_telemetry_snapshot() -> Dict[str, object]:
    monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
    if not isinstance(monitoring_state, dict):
        monitoring_state = {}
    langsmith = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
    llm_records = _records_from_monitoring_df(monitoring_state, "llm_interactions_df")
    if not llm_records:
        llm_records = get_llm_interactions(limit=200)
    # Derive failures from per-term event status as source of truth.
    # This prevents stale/misreported counters from showing all processed terms as failures.
    failed_terms = 0
    events_df = monitoring_state.get("events_df")
    if isinstance(events_df, pd.DataFrame) and not events_df.empty and "Status" in events_df.columns:
        failed_terms = int(
            events_df["Status"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["timeout", "error", "failed"])
            .sum()
        )
    else:
        # Fallback to the maintained counter if no event snapshot is available yet.
        failed_terms = int(monitoring_state.get("failed_terms", 0) or 0)
    return {
        "enabled": bool(monitoring_state.get("enabled") or st.session_state.get("agent_use_langsmith_monitoring", False)),
        "run_id": monitoring_state.get("run_id"),
        "started_at": monitoring_state.get("started_at"),
        "finished_at": monitoring_state.get("finished_at"),
        "duration_sec": monitoring_state.get("duration_sec"),
        "total_terms": int(monitoring_state.get("total_terms", 0) or 0),
        "processed_terms": int(monitoring_state.get("processed_terms", 0) or 0),
        "failed_terms": failed_terms,
        "total_cost_usd": float(monitoring_state.get("total_cost_usd", 0.0) or 0.0),
        "langsmith_url": langsmith.get("run_url"),
        "langsmith_project_url": langsmith.get("project_url"),
        "langsmith_message": langsmith.get("message"),
        "llm_calls": llm_records,
        "events": _records_from_monitoring_df(monitoring_state, "events_df"),
        "cascade": _records_from_monitoring_df(monitoring_state, "cascade_trace_df"),
        "logs": st.session_state.get(AGENT_RUN_MESSAGES_KEY, [])[-100:] if isinstance(st.session_state.get(AGENT_RUN_MESSAGES_KEY, []), list) else [],
    }


def _normalize_review_status_for_mui(agent_df: pd.DataFrame, row_index) -> str:
    raw_status = _get_review_cell_value(agent_df, row_index, "Review Status").strip().lower()
    if raw_status in {"accepted", "rejected"}:
        return raw_status
    if raw_status in {"no_match", "timeout"}:
        return "no_match"
    return "pending"


def _build_review_snapshot(agent_df: Optional[pd.DataFrame]) -> Dict[str, object]:
    selected_source = st.session_state.get(AGENT_SELECTED_SOURCE_KEY) or "agent_reconciliation"
    items: List[Dict[str, object]] = []
    counts = {"pending": 0, "accepted": 0, "rejected": 0, "no_match": 0}
    if isinstance(agent_df, pd.DataFrame):
        candidate_indices = set(_get_reviewable_agent_result_indices(agent_df))
        if "Review Status" in agent_df.columns:
            status_series = agent_df["Review Status"].astype(str).str.strip().str.lower()
            candidate_indices.update(agent_df[status_series.isin(["accepted", "rejected", "no_match", "timeout", "pending", "matched"])].index.tolist())
        if "Suggested URI" in agent_df.columns:
            suggested_uri = agent_df["Suggested URI"].astype(str).str.strip()
            candidate_indices.update(agent_df[suggested_uri != ""].index.tolist())
        for idx in sorted(candidate_indices, key=lambda value: str(value))[:500]:
            status = _normalize_review_status_for_mui(agent_df, idx)
            counts[status] = counts.get(status, 0) + 1
            match_type = normalize_mapping_type(_get_review_cell_value(agent_df, idx, "Suggested Match Type"))
            if not match_type:
                match_type = normalize_mapping_type(_get_review_cell_value(agent_df, idx, "Match Type"))
            is_no_match_outcome = status == "no_match"
            if is_no_match_outcome:
                match_type = "no_match"
            confidence = _get_review_cell_value(agent_df, idx, "Suggested Confidence")
            try:
                confidence_value: object = round(float(confidence), 4)
            except (TypeError, ValueError):
                confidence_value = confidence
            raw_suggested_uri = _get_review_cell_value(agent_df, idx, "Suggested URI")
            raw_suggested_label = _get_review_cell_value(agent_df, idx, "Suggested Label")
            raw_suggested_description = _get_review_cell_value(agent_df, idx, "Suggested Description")
            stale_candidate_note = ""
            if is_no_match_outcome and raw_suggested_uri:
                stale_candidate_note = (
                    "A low-confidence candidate was inspected but rejected by the workflow. "
                    "It is kept only for audit/details and cannot be accepted as a mapping."
                )
            items.append(
                {
                    "mapping_id": f"{selected_source}::{idx}",
                    "row_index": idx,
                    "source_name": selected_source,
                    "term": _get_review_cell_value(agent_df, idx, "Term") or _get_review_cell_value(agent_df, idx, "subject_label"),
                    "definition": _get_review_cell_value(agent_df, idx, "Definition"),
                    "status": status,
                    "suggested_uri": "" if is_no_match_outcome else raw_suggested_uri,
                    "suggested_label": "" if is_no_match_outcome else raw_suggested_label,
                    "suggested_description": "" if is_no_match_outcome else raw_suggested_description,
                    "candidate_uri": raw_suggested_uri,
                    "candidate_label": raw_suggested_label,
                    "candidate_description": raw_suggested_description,
                    "can_accept": bool((not is_no_match_outcome) and raw_suggested_uri),
                    "no_match_note": stale_candidate_note,
                    "match_type": match_type or "no_match",
                    "provider": _get_review_cell_value(agent_df, idx, "Suggested Provider"),
                    "confidence": confidence_value,
                    "decision_source": _get_review_cell_value(agent_df, idx, "Suggested Decision Source"),
                    "fallback_reason": _get_review_cell_value(agent_df, idx, "Suggested Fallback Reason"),
                    "explanation": _get_review_cell_value(agent_df, idx, "Agent Explanation"),
                    "auto_accept_reason": _get_review_cell_value(agent_df, idx, "Auto Accept Reason"),
                }
            )
    return {"items": items, "counts": counts, "selected_source": selected_source}


def _parse_mapping_id(mapping_id: object) -> tuple[Optional[str], Optional[object]]:
    value = str(mapping_id or "")
    if "::" not in value:
        return st.session_state.get(AGENT_SELECTED_SOURCE_KEY), value or None
    source_name, raw_index = value.split("::", 1)
    try:
        row_index: object = int(raw_index)
    except ValueError:
        row_index = raw_index
    return source_name, row_index


def _execute_agent_reconciliation_run(input_tables, missing_provider_keys, primary_provider, effective_primary_env):
    """Run the unchanged agent orchestration from a structured MUI event."""
    if (not bool(input_tables)) or bool(missing_provider_keys):
        st.session_state["agent_mui_status_message"] = {"severity": "warning", "text": "Resolve prerequisites before starting reconciliation."}
        return
    try:
        st.session_state["agent_prov_last_run_mapping_date"] = date.today().isoformat()
        config = _build_run_config_from_state()
        stop_event = st.session_state.get(AGENT_STOP_EVENT_KEY, {})
        stop_decision = str(st.session_state.get("agent_llm_error_stop_decision", "Fix issue and rerun") or "Fix issue and rerun")
        continue_with_heuristics = stop_decision == "Continue with heuristic fallback"
        resume_previous = bool(continue_with_heuristics and isinstance(stop_event, dict) and stop_event.get("stop_reason") == "llm_error")
        config.stop_on_llm_error = not continue_with_heuristics
        tables_for_run = _build_run_input_tables(
            input_tables,
            st.session_state.get(AGENT_RESULTS_BY_SOURCE_KEY, {}),
            resume_previous=resume_previous,
        )
        total_terms_for_run = 0
        for table in tables_for_run:
            unreconciled_indices = get_unreconciled_indices(table.dataframe, "No Match")
            if resume_previous and "Run ID" in table.dataframe.columns:
                processed_mask = table.dataframe["Run ID"].astype(str).str.strip().ne("")
                unreconciled_indices = [idx for idx in unreconciled_indices if not bool(processed_mask.loc[idx])]
            total_terms_for_run += len(unreconciled_indices)
        run_started_perf = time.perf_counter()
        run_started_epoch = time.time()
        run_started_iso = datetime.fromtimestamp(run_started_epoch).isoformat()
        st.session_state[AGENT_RUN_STATUS_STATE_KEY] = {
            "running": True,
            "finished": False,
            "error": None,
            "stage": "retrieving_candidates",
            "message": "Retrieving candidate matches",
            "current_term": None,
            "processed_count": 0,
            "total_count": total_terms_for_run,
            "started_at": run_started_iso,
            "elapsed_seconds": 0,
            "estimated_remaining_seconds": None,
            "last_activity": "Run started; validating input and preparing candidate retrieval.",
        }
        configured_project = configure_langsmith_environment(config.langsmith_project)
        config.langsmith_project = configured_project
        reset_llm_interactions()
        langsmith_state = get_langsmith_readiness(config.langsmith_project)
        telemetry_enabled = bool(st.session_state.get("agent_use_langsmith_monitoring", False))
        st.session_state[AGENT_MONITORING_STATE_KEY] = {
            "enabled": telemetry_enabled,
            "run_id": None,
            "started_at": run_started_iso,
            "finished_at": None,
            "duration_sec": None,
            "total_terms": total_terms_for_run,
            "processed_terms": 0,
            "failed_terms": 0,
            "stop_reason": None,
            "stop_event": {},
            "events_df": pd.DataFrame(),
            "llm_interactions_df": pd.DataFrame(),
            "cascade_trace_df": pd.DataFrame(),
            "raw_term_events": [],
            "langsmith": {**langsmith_state, "run_url": None},
        }
        definitions_by_source: Dict[str, Dict[str, str]] = {}
        definition_preparation_enabled = bool(st.session_state.get("agent_enable_definition_preparation", False))
        for table in tables_for_run:
            if not definition_preparation_enabled:
                used_defs_df = pd.DataFrame(columns=["Term", "Definition"])
            else:
                strategy = config.definition_strategy
                if strategy == "uploaded_sheet":
                    uploaded_defs = st.session_state.get(AGENT_DEFINITIONS_BY_SOURCE_KEY, {}).get("__uploaded_sheet__")
                    used_defs_df = prepare_used_definitions_df(table.dataframe, strategy, uploaded_definitions_df=uploaded_defs)
                else:
                    context_text = st.session_state.get("agent_reference_publication_text", "") if strategy == "reference_publication" else st.session_state.get("agent_definition_context_text", "")
                    used_defs_df = prepare_used_definitions_df(
                        table.dataframe,
                        strategy,
                        context_text=context_text,
                        model_name=config.definition_model_name,
                        provider=config.definition_model_provider,
                        api_key_env=config.definition_model_api_key_env,
                        reasoning_effort=config.reasoning_effort,
                    )
            definitions_by_source[table.source_name] = build_definition_lookup(used_defs_df)
            st.session_state[AGENT_DEFINITIONS_BY_SOURCE_KEY][table.source_name] = used_defs_df

        latest_batch_state: Dict[str, object] = {"state": None}

        def _progress_callback(state):
            latest_batch_state["state"] = state
            st.session_state[AGENT_RUN_MESSAGES_KEY] = state.messages
            elapsed_seconds = time.perf_counter() - run_started_perf
            processed_terms = int(getattr(state, "processed_terms", 0) or 0)
            total_terms = int(getattr(state, "total_terms", total_terms_for_run) or total_terms_for_run or 0)
            current_event = (list(getattr(state, "term_events", []) or [])[-1] if getattr(state, "term_events", []) else {})
            current_term = str(current_event.get("term") or "").strip() or None
            workflow_name = str(current_event.get("workflow") or getattr(config, "workflow", "") or "")
            decision_source = str(current_event.get("decision_source") or "").strip()
            fallback_reason = str(current_event.get("fallback_reason") or "").strip()
            event_status = str(current_event.get("status") or "").strip()
            estimated_remaining = None
            if total_terms and processed_terms > 0 and elapsed_seconds > 0:
                estimated_remaining = (elapsed_seconds / processed_terms) * max(0, total_terms - processed_terms)
            stage = "retrieving_candidates"
            if processed_terms >= total_terms and total_terms:
                stage = "preparing_review"
            elif decision_source:
                stage = "ranking_candidates"
            if fallback_reason == "llm_error":
                stage = "selecting_match_type"
            message = str(state.messages[-1]) if getattr(state, "messages", None) else "Processing semantic mappings"
            st.session_state[AGENT_RUN_STATUS_STATE_KEY] = {
                "running": True,
                "finished": False,
                "error": None,
                "stage": stage,
                "message": message,
                "current_term": current_term,
                "processed_count": processed_terms,
                "total_count": total_terms,
                "started_at": run_started_iso,
                "elapsed_seconds": elapsed_seconds,
                "estimated_remaining_seconds": estimated_remaining,
                "last_activity": (
                    f"{current_term}: {event_status or 'processed'} via {workflow_name or 'agent workflow'}"
                    if current_term
                    else message
                ),
            }
            monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
            if isinstance(monitoring_state, dict):
                events_df = _build_monitoring_event_snapshot(state)
                monitoring_state["run_id"] = getattr(state, "run_id", None)
                monitoring_state["total_terms"] = int(getattr(state, "total_terms", 0) or 0)
                monitoring_state["processed_terms"] = int(getattr(state, "processed_terms", 0) or 0)
                monitoring_state["failed_terms"] = int(getattr(state, "failed_terms", 0) or 0)
                monitoring_state["duration_sec"] = elapsed_seconds
                monitoring_state["events_df"] = events_df
                interactions_df = pd.DataFrame(get_llm_interactions(limit=500))
                monitoring_state["llm_interactions_df"] = interactions_df
                if not interactions_df.empty and "cost_usd" in interactions_df.columns:
                    monitoring_state["total_cost_usd"] = float(interactions_df["cost_usd"].fillna(0).sum())
                monitoring_state["cascade_trace_df"] = _build_cascade_trace_snapshot(state)
                monitoring_state["raw_term_events"] = list(getattr(state, "term_events", []) or [])
                langsmith_dict = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
                if langsmith_dict.get("project") and getattr(state, "run_id", None):
                    langsmith_dict["run_url"] = build_run_url(str(langsmith_dict.get("project")), str(state.run_id))
                    monitoring_state["langsmith"] = langsmith_dict
                st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state

        outputs = run_agent_batch(
            tables_for_run,
            config,
            definitions_by_source=definitions_by_source,
            bioportal_api_key=(CONFIG or {}).get("bioportal", {}).get("api_key"),
            progress_callback=_progress_callback,
            resume_skip_processed_terms=resume_previous,
        )
        st.session_state[AGENT_RESULTS_BY_SOURCE_KEY] = outputs
        if outputs and not st.session_state.get(AGENT_SELECTED_SOURCE_KEY):
            st.session_state[AGENT_SELECTED_SOURCE_KEY] = list(outputs.keys())[0]
        _sync_selected_source_dataframe()
        monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
        if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
            monitoring_state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            monitoring_state["duration_sec"] = time.perf_counter() - run_started_perf
            st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
        final_elapsed_seconds = time.perf_counter() - run_started_perf
        final_processed = total_terms_for_run
        latest_state_obj = latest_batch_state.get("state")
        if latest_state_obj is not None:
            final_processed = int(getattr(latest_state_obj, "processed_terms", final_processed) or 0)
        st.session_state[AGENT_RUN_STATUS_STATE_KEY] = {
            **st.session_state.get(AGENT_RUN_STATUS_STATE_KEY, {}),
            "running": False,
            "finished": True,
            "stage": "writing_output",
            "message": "Run completed; preparing review output",
            "processed_count": final_processed,
            "total_count": total_terms_for_run,
            "elapsed_seconds": final_elapsed_seconds,
            "estimated_remaining_seconds": 0,
            "last_activity": "Agent run completed and review suggestions are ready.",
        }
        llm_stop_event = {}
        if latest_state_obj is not None and getattr(latest_state_obj, "stop_reason", None) == "llm_error":
            state_stop_event = getattr(latest_state_obj, "stop_event", {}) or {}
            if isinstance(state_stop_event, dict):
                llm_stop_event = {
                    "stop_reason": "llm_error",
                    "file": state_stop_event.get("file"),
                    "term": state_stop_event.get("term"),
                    "fallback_error_type": state_stop_event.get("fallback_error_type"),
                    "fallback_error_message": state_stop_event.get("fallback_error_message"),
                    "fallback_reason": state_stop_event.get("fallback_reason"),
                    "llm_fix_suggestion": state_stop_event.get("llm_fix_suggestion"),
                    "workflow": state_stop_event.get("workflow"),
                    "decision_source": state_stop_event.get("decision_source"),
                }
        if llm_stop_event:
            st.session_state[AGENT_STOP_EVENT_KEY] = llm_stop_event
            if isinstance(monitoring_state, dict):
                monitoring_state["stop_reason"] = "llm_error"
                monitoring_state["stop_event"] = llm_stop_event
                st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
            stopped_term = str(llm_stop_event.get("term", "") or "").strip() or "(unknown term)"
            st.session_state["agent_mui_status_message"] = {"severity": "warning", "text": f"Run stopped automatically due to LLM error at term '{stopped_term}'."}
        else:
            st.session_state[AGENT_STOP_EVENT_KEY] = {}
            if isinstance(monitoring_state, dict):
                monitoring_state["stop_reason"] = None
                monitoring_state["stop_event"] = {}
                st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
            st.session_state["agent_mui_status_message"] = {"severity": "success", "text": "Agent-based reconciliation run completed."}
    except Exception as exc:
        if _is_openai_compatible_provider(primary_provider) and is_openai_compatible_auth_required_error(exc):
            env_name = effective_primary_env or get_default_api_key_env(OPENAI_COMPATIBLE_PROVIDER)
            st.session_state["agent_mui_status_message"] = {"severity": "error", "text": f"OpenAI-compatible endpoint rejected unauthenticated requests. Set {env_name} and run again."}
            return
        monitoring_state = st.session_state.get(AGENT_MONITORING_STATE_KEY, {})
        if isinstance(monitoring_state, dict) and monitoring_state.get("enabled"):
            monitoring_state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            monitoring_state["duration_sec"] = time.perf_counter() - run_started_perf if "run_started_perf" in locals() else None
            langsmith_dict = monitoring_state.get("langsmith", {}) if isinstance(monitoring_state.get("langsmith"), dict) else {}
            existing = str(langsmith_dict.get("message", "") or "").strip()
            langsmith_dict["message"] = (existing + " " if existing else "") + f"Run failed: {exc}"
            monitoring_state["langsmith"] = langsmith_dict
            st.session_state[AGENT_MONITORING_STATE_KEY] = monitoring_state
        st.session_state[AGENT_RUN_STATUS_STATE_KEY] = {
            **st.session_state.get(AGENT_RUN_STATUS_STATE_KEY, {}),
            "running": False,
            "finished": False,
            "error": str(exc),
            "stage": "preparing_review",
            "message": f"Agent-based reconciliation failed: {exc}",
            "elapsed_seconds": time.perf_counter() - run_started_perf if "run_started_perf" in locals() else None,
            "last_activity": f"Run failed: {exc}",
        }
        st.session_state["agent_mui_status_message"] = {"severity": "error", "text": f"Agent-based reconciliation failed: {exc}"}


def _handle_agent_mui_event(event: object, readiness_state: Dict[str, object], runtime_context: Dict[str, object], provenance_defaults_cfg: Dict[str, str]) -> bool:
    if not isinstance(event, dict):
        return False
    nonce = event.get("nonce")
    if nonce and st.session_state.get(AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY) == nonce:
        return False
    if nonce:
        st.session_state[AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY] = nonce
    event_type = str(event.get("type", "") or "")
    should_rerun = False

    if event_type == "config_changed":
        should_rerun = _apply_workflow_config_to_session_state(event.get("config"))
    elif event_type == "navigate":
        target_stage = _stage_from_component(event.get("stage"))
        if target_stage:
            st.session_state[AGENT_ACTIVE_STEP_KEY] = target_stage
            st.components.v1.html("<script>window.parent.scrollToTop();</script>", height=0)
            should_rerun = True
    elif event_type == "upload_csv":
        filename = str(event.get("filename", "") or "uploaded.csv").strip() or "uploaded.csv"
        content = event.get("content", "")
        if not filename.lower().endswith(".csv"):
            st.session_state["agent_mui_status_message"] = {"severity": "error", "text": "Please upload a .csv matching table."}
        elif not isinstance(content, str) or not content.strip():
            st.session_state["agent_mui_status_message"] = {"severity": "error", "text": "The uploaded CSV file is empty."}
        else:
            try:
                dataframe = pd.read_csv(io.StringIO(content)).fillna("")
                table = make_input_table(
                    dataframe,
                    source_name=os.path.splitext(filename)[0] or "Uploaded CSV",
                    filename=filename,
                )
                _store_input_tables(
                    [table],
                    f"Agent-based reconciliation data successfully loaded from uploaded CSV matching table: {filename}.",
                )
                st.session_state[AGENT_UPLOADED_SOURCE_SIGNATURE_KEY] = f"{filename}:{len(content)}"
                st.session_state["agent_mui_status_message"] = {"severity": "success", "text": f"CSV matching table '{filename}' loaded into the agent workflow."}
            except Exception as exc:
                st.session_state["agent_mui_status_message"] = {"severity": "error", "text": f"Failed to parse uploaded CSV file: {exc}"}
        should_rerun = True
    elif event_type == "load_shared_table":
        shared_df = st.session_state.get("shared_matching_table")
        if isinstance(shared_df, pd.DataFrame):
            table = make_input_table(
                shared_df,
                source_name="Matching Table Generator",
                filename="shared_matching_table",
                is_from_shared_matching_table=True,
            )
            _store_input_tables([table], "Agent-based reconciliation data successfully loaded from: Matching Table Generator.")
            st.session_state["agent_mui_status_message"] = {"severity": "success", "text": "Shared matching table loaded into the agent workflow."}
        else:
            st.session_state["agent_mui_status_message"] = {"severity": "warning", "text": "No shared matching table is available in session state."}
        should_rerun = True
    elif event_type == "codex_auth_signin":
        try:
            start_codex_authorization_flow(open_browser=True)
            st.session_state["agent_mui_status_message"] = {"severity": "success", "text": "Check your browser to complete ChatGPT sign in."}
        except Exception as exc:
            st.session_state["agent_mui_status_message"] = {"severity": "error", "text": f"Sign in error: {exc}"}
        should_rerun = True
    elif event_type == "codex_auth_signout":
        clear_codex_credentials()
        st.session_state["agent_mui_status_message"] = {"severity": "success", "text": "Logged out of ChatGPT Subscription."}
        should_rerun = True
    elif event_type in {"codex_auth_refresh", "codex_auth_refresh_pending"}:
        should_rerun = True
    elif event_type == "start_run":
        _execute_agent_reconciliation_run(
            st.session_state.get(AGENT_INPUT_TABLES_KEY, []),
            runtime_context.get("missing_provider_keys", []),
            runtime_context.get("primary_provider", "openai"),
            runtime_context.get("effective_primary_env", get_default_api_key_env("openai")),
        )
        should_rerun = True
    elif event_type in {"accept_mapping", "reject_mapping", "reset_mapping"}:
        source_name, row_index = _parse_mapping_id(event.get("mapping_id"))
        action = {"accept_mapping": "accept", "reject_mapping": "reject", "reset_mapping": "reset"}[event_type]
        if source_name is not None and row_index is not None:
            selected_match_type = event.get("selected_match_type") if action == "accept" else None
            _apply_review_action(
                source_name,
                row_index,
                action,
                selected_match_type=str(selected_match_type or ""),
            )
            st.session_state["agent_mui_status_message"] = {"severity": "success", "text": f"Mapping {action} action applied."}
            should_rerun = True
    elif event_type == "save_configuration":
        provider = str(st.session_state.get("agent_model_provider", runtime_context.get("primary_provider", "openai")) or "openai")
        model = str(st.session_state.get("agent_model_name", "") or "")
        ok, msg = _save_preferred_model_selection(provider, model)
        st.session_state["agent_mui_status_message"] = {"severity": "success" if ok else "error", "text": msg}
        should_rerun = True
    elif event_type == "reload_models":
        provider = str(st.session_state.get("agent_model_provider", runtime_context.get("primary_provider", "openai")) or "openai")
        api_key_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", provider, "model_api_key_env")
        fetch_all_pricing(force_refresh=True)
        _ensure_model_catalog_for_provider(provider, api_key_env=api_key_env, force_refresh=True)
        st.session_state["agent_mui_status_message"] = {"severity": "success", "text": "Model catalog and pricing reloaded."}
        should_rerun = True
    elif event_type == "register_local_model":
        provider = str(st.session_state.get("agent_model_provider", runtime_context.get("primary_provider", "openai")) or "openai")
        api_key_env = _resolve_api_key_env_for_provider("agent_model_api_key_env", provider, "model_api_key_env")
        ok, msg = _register_openai_compatible_model_from_override(str(st.session_state.get("agent_custom_model_override", "") or ""), provider, api_key_env)
        st.session_state["agent_mui_status_message"] = {"severity": "success" if ok else "error", "text": msg}
        should_rerun = True
    elif event_type == "save_provenance_defaults":
        defaults = _build_provenance_defaults_from_state()
        if not defaults.get("mapping_tool"):
            defaults["mapping_tool"] = provenance_defaults_cfg.get("mapping_tool", "RDF4Risk Agent-Based Reconciliation")
        ok, msg = _save_preferred_provenance_defaults(defaults)
        st.session_state["agent_mui_status_message"] = {"severity": "success" if ok else "error", "text": msg}
        should_rerun = True
    elif event_type in {"publish_rdf_handoff", "export_sssom"}:
        agent_df = st.session_state.get(AGENT_DATAFRAME_STATE_KEY)
        if isinstance(agent_df, pd.DataFrame):
            export_df = finalize_accepted_results(agent_df.copy(), provenance_defaults=_build_provenance_defaults_from_state())
            st.session_state["shared_reconciled_matching_table"] = export_df
            if event_type == "export_sssom":
                selected_source = str(st.session_state.get(AGENT_SELECTED_SOURCE_KEY) or "agent_reconciliation").strip() or "agent_reconciliation"
                safe_source = re.sub(r"[^A-Za-z0-9_.-]+", "_", selected_source).strip("_") or "agent_reconciliation"
                export_filename = f"{safe_source}_agent_reconciled_sssom.csv"
                st.session_state[AGENT_SSSOM_EXPORT_PAYLOAD_KEY] = {
                    "nonce": int(time.time() * 1000),
                    "filename": export_filename,
                    "content": export_df.to_csv(index=False),
                    "mime_type": "text/csv;charset=utf-8",
                }
                st.session_state["agent_mui_status_message"] = {"severity": "success", "text": f"SSSOM export download prepared: {export_filename}."}
            else:
                st.session_state["agent_mui_status_message"] = {"severity": "success", "text": "Accepted mappings published to RDF Generator handoff."}
        else:
            st.session_state["agent_mui_status_message"] = {"severity": "warning", "text": "No working table is available for export."}
        should_rerun = True
    return should_rerun

def render_agent_reconciliation_ui():
    """Render the agent-based reconciliation workflow UI.

    Central browser app contract: AgentReconciliationMuiApp(config, data_status, run_status, review, telemetry)
    handles Setup, Run, Review and Export. Streamlit remains host/backend/session-state bridge only.

    Legacy test anchors retained for backwards-compatible source inspections only; they are not rendered:
    "Load data for reconciliation" -> with st.expander("Optional schema/status details", expanded=False): -> "Definition preparation".
    "Enable SKOS matching" before "Enable auto-accept" before with st.expander("Auto-Accept Policy", expanded=False):
    auto_accept_enabled = st.checkbox
    if auto_accept_enabled:
        with st.expander
    Auto-accept settings are hidden until auto-accept is enabled.
    st.caption("Provenance metadata is optional.")
    "Include provenance metadata"
    if provenance_enabled:
        with st.expander("Provenance settings", expanded=True):
    st.session_state["agent_prov_last_run_mapping_date"] = date.today().isoformat()
    Mapping Date is generated automatically when the workflow runs.
    previously_enabled = bool(st.session_state.get("agent_provenance_toggle_previous", False))
    if provenance_enabled and not previously_enabled:
    st.session_state["agent_prov_author_orcid"] = provenance_defaults_cfg.get("author_id", "")
    st.session_state["agent_provenance_toggle_previous"] = provenance_enabled
    "Enable optional definition preparation"
    key="agent_enable_definition_preparation"
    if definition_preparation_enabled:
        with st.expander("Definition preparation settings", expanded=True):
    "Definition strategy"
    """
    _initialize_agent_reconciliation_state()
    _render_agent_reconciliation_visual_theme()

    required_columns = REQUIRED_MATCHING_TABLE_COLUMNS
    provenance_defaults_cfg = _get_provenance_defaults_from_config()
    _initialize_provenance_state(provenance_defaults_cfg)

    defaults = _get_agent_ui_defaults()
    runtime_context = _compute_workflow_runtime_context(defaults)
    readiness_state = _build_run_readiness_state(
        required_columns,
        runtime_context.get("missing_provider_keys", []),
    )

    workflow_config_payload = _build_workflow_config_from_state(defaults)
    primary_provider = str(runtime_context.get("primary_provider", workflow_config_payload.get("provider", "openai")) or "openai")
    primary_models = list(runtime_context.get("primary_models", []) or [])
    if not primary_models:
        primary_models = [str(workflow_config_payload.get("model", "gpt-5.1") or "gpt-5.1")]
    selected_primary_model = str(st.session_state.get("agent_model_name", workflow_config_payload.get("model", primary_models[0])) or primary_models[0])
    for model_candidate in (
        selected_primary_model,
        str(st.session_state.get("agent_custom_model_override", "") or "").strip(),
        str(st.session_state.get("agent_definition_model_name", "") or "").strip(),
        str(st.session_state.get("agent_planner_model_name", "") or "").strip(),
    ):
        if model_candidate and model_candidate not in primary_models:
            primary_models.append(model_candidate)
    workflow_config_payload["provider"] = primary_provider
    workflow_config_payload["model"] = selected_primary_model

    data_status = _build_data_status_snapshot(required_columns)
    run_status = _build_run_status_snapshot(readiness_state)
    telemetry = _build_telemetry_snapshot()
    review = _build_review_snapshot(st.session_state.get(AGENT_DATAFRAME_STATE_KEY))
    export_payload = st.session_state.get(AGENT_SSSOM_EXPORT_PAYLOAD_KEY)
    if not isinstance(export_payload, dict):
        export_payload = None

    component_event = _render_workflow_config_panel(
        workflow_config_payload,
        provider_options=list(defaults.get("provider_options", get_supported_llm_providers()) or ["openai"]),
        model_options=primary_models,
        readiness_state=readiness_state,
        primary_catalog=runtime_context.get("primary_catalog"),
        selected_model=selected_primary_model,
        data_status=data_status,
        run_status=run_status,
        telemetry=telemetry,
        review=review,
        export_payload=export_payload,
    )

    if _handle_agent_mui_event(component_event, readiness_state, runtime_context, provenance_defaults_cfg):
        st.rerun()

    # If the active stage changed in session state (e.g. via direct UI buttons not captured
    # by the component event or internal logic), ensure we scroll to top on next render.
    # This also acts as a safety catch-all for stage transitions.
    if "agent_reconciliation_active_step_last" not in st.session_state:
        st.session_state["agent_reconciliation_active_step_last"] = _get_active_agent_stage()

    if st.session_state["agent_reconciliation_active_step_last"] != _get_active_agent_stage():
        st.session_state["agent_reconciliation_active_step_last"] = _get_active_agent_stage()
        st.components.v1.html(
            """
            <script>
                var mainContent = window.parent.document.querySelector('.main') || window.parent.document.querySelector('section.main') || window.parent.document.body;
                if (mainContent) {
                    mainContent.scrollTo({ top: 0, behavior: 'auto' });
                    mainContent.scrollTop = 0;
                }
                window.parent.scrollTo({ top: 0, behavior: 'auto' });
                window.parent.scrollTop = 0;
            </script>
            """,
            height=0
        )
        st.rerun()

