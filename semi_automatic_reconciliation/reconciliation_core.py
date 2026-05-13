# -*- coding: utf-8 -*-
"""Pure Python reconciliation utilities shared by backend services.

This module intentionally has no UI framework dependency. UI-only helpers
remain in ``reconciliation_utils.py`` for legacy UI code.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Mapping

import Levenshtein
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
    logger.addHandler(stream_handler)

NO_MATCH_URI = "No Match"
NO_MATCH_DISPLAY = f"--- {NO_MATCH_URI} ---"
CUSTOM_SPARQL_PROVIDER_NAME = "Custom SPARQL"
DEFAULT_SPARQL_QUERY_TEMPLATE = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?uri ?label ?description WHERE {{
  ?uri skos:prefLabel|rdfs:label ?label .
  FILTER(CONTAINS(LCASE(STR(?label)), LCASE("{term}")))
  OPTIONAL {{ ?uri skos:definition|rdfs:comment ?description . }}
}}
LIMIT {limit}
"""


def _pure_python_levenshtein_distance(s1: str, s2: str) -> int:
    if s1 == s2:
        return 0
    if len(s1) == 0:
        return len(s2)
    if len(s2) == 0:
        return len(s1)
    v0 = list(range(len(s2) + 1))
    v1 = [0] * (len(s2) + 1)
    for i in range(len(s1)):
        v1[0] = i + 1
        for j in range(len(s2)):
            cost = 0 if s1[i] == s2[j] else 1
            v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
        for j in range(len(v0)):
            v0[j] = v1[j]
    return v1[len(s2)]


def load_config(default_config_filename: str = "config.yaml"):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_file_path = os.path.join(project_root, default_config_filename)
    dotenv_path = os.path.join(project_root, ".env")
    load_dotenv(dotenv_path=dotenv_path, override=False)

    def _env_first(config_value, env_name: str):
        env_value = str(os.getenv(env_name) or "").strip()
        if env_value:
            return env_value
        return config_value

    def _apply_secret_env_overrides(config: dict) -> dict:
        if not isinstance(config, dict):
            return config
        for cfg_key, env_name in (
            ("ncbi", "NCBI_API_KEY"),
            ("bioportal", "BIOPORTAL_API_KEY"),
            ("agroportal", "AGROPORTAL_API_KEY"),
            ("earthportal", "EARTHPORTAL_API_KEY"),
        ):
            provider_cfg = config.get(cfg_key)
            if isinstance(provider_cfg, dict):
                provider_cfg["api_key"] = _env_first(provider_cfg.get("api_key"), env_name)

        agent_cfg = config.get("agent_reconciliation")
        if isinstance(agent_cfg, dict):
            provider_keys = agent_cfg.get("provider_api_keys")
            if not isinstance(provider_keys, dict):
                provider_keys = {}
                agent_cfg["provider_api_keys"] = provider_keys
            provider_keys["openai"] = _env_first(provider_keys.get("openai"), "OPENAI_API_KEY")
            provider_keys["anthropic"] = _env_first(provider_keys.get("anthropic"), "ANTHROPIC_API_KEY")
            provider_keys["google_gemini"] = _env_first(provider_keys.get("google_gemini"), "GOOGLE_API_KEY")
            provider_keys["openai_compatible"] = _env_first(provider_keys.get("openai_compatible"), "OPENAI_COMPATIBLE_API_KEY")
            agent_cfg["langsmith_api_key"] = _env_first(agent_cfg.get("langsmith_api_key"), "LANGSMITH_API_KEY")
        return config

    logger.info("Attempting to load config from: %s", config_file_path)
    if not os.path.exists(config_file_path):
        logger.error("Config file not found: %s", config_file_path)
        return None
    try:
        with open(config_file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        config = _apply_secret_env_overrides(config)
        logger.info("Config loaded successfully from %s", config_file_path)
        return config
    except Exception:
        logger.exception("Error loading/parsing config: %s", config_file_path)
        return None


CONFIG = load_config()
USER_AGENT = "PythonReconciliationService/1.0 (Contact: your-email@example.com)"
if CONFIG:
    try:
        contact_email = CONFIG.get("contact_email") or CONFIG.get("ncbi", {}).get("email", "no-email-provided@example.com")
        app_version_str = CONFIG.get("app_version", "N/A")
        ua_template = CONFIG.get("user_agent_template", "PythonReconciliationService (Contact: {email})")
        USER_AGENT = ua_template.replace("{email}", contact_email).replace("{version}", app_version_str)
        logger.debug("User-Agent set to: %s", USER_AGENT)
    except Exception as exc:
        logger.error("Error creating User-Agent from config: %s", exc)
else:
    logger.warning("CONFIG is None, User-Agent will use default. Check config.yaml loading.")


def calculate_levenshtein_score(s1, s2) -> float:
    if not isinstance(s1, str) or not isinstance(s2, str):
        logger.warning("Levenshtein calculation received non-string input: %r, %r", s1, s2)
        return 0.0
    s1_lower = s1.lower()
    s2_lower = s2.lower()
    try:
        distance = Levenshtein.distance(s1_lower, s2_lower)
        if distance is None:
            distance = _pure_python_levenshtein_distance(s1_lower, s2_lower)
    except Exception:
        logger.exception("Error with Levenshtein.distance; falling back to pure Python implementation.")
        distance = _pure_python_levenshtein_distance(s1_lower, s2_lower)
    max_len = max(len(s1_lower), len(s2_lower))
    if max_len == 0:
        return 1.0 if distance == 0 else 0.0
    return 1.0 - (distance / max_len)


def format_suggestion_display(suggestion, strategy="API Ranking", max_desc_length=100):
    label = suggestion.get("label", "N/A")
    desc = suggestion.get("description", "")
    uri = suggestion.get("uri", "")
    source_provider_display = suggestion.get("source_provider")
    db_info = f" [{source_provider_display}]" if source_provider_display else ""
    score_info = ""
    score_key = None
    score_label_prefix = ""
    include_score = False
    if strategy == "Levenshtein Similarity":
        score_key = "levenshtein_score"
        score_label_prefix = "Lev: "
        include_score = True
    elif strategy == "API Ranking":
        score_key = "score"
        include_score = score_key in suggestion
    if include_score and score_key:
        score = suggestion.get(score_key)
        try:
            score_info = f" [{score_label_prefix}{float(score):.2f}]" if score is not None else f" [{score_label_prefix}N/A]"
        except (ValueError, TypeError):
            score_info = f" [{score_label_prefix}N/A]"
    display_text = f"{label}{db_info}{score_info}"
    if desc:
        desc_str = str(desc)
        display_text += f" ({desc_str[:max_desc_length]}...)" if len(desc_str) > max_desc_length else f" ({desc_str})"
    display_text += f" <{uri}>" if uri else " <No URI>"
    return display_text


def get_combined_and_sorted_suggestions(
    term_main,
    all_suggestions_for_term,
    max_suggestions_to_show,
    strategy,
    selected_ontologies_by_provider: Mapping[str, list[str]] | None = None,
):
    combined_suggestions = []
    lookup_providers = {"BioPortal", "OLS (EBI)", "SemLookP", "AgroPortal", "EarthPortal"}
    selected_ontologies_by_provider = selected_ontologies_by_provider or {}

    for provider_name, suggestions_list in all_suggestions_for_term.items():
        if suggestions_list is None:
            continue
        if provider_name in lookup_providers:
            selected_ontologies = selected_ontologies_by_provider.get(provider_name, [])
            if selected_ontologies:
                selected_upper = {str(o).upper() for o in selected_ontologies}
                suggestions_list = [
                    suggestion
                    for suggestion in suggestions_list
                    if suggestion is not None
                    and (suggestion.get("ontology") or suggestion.get("source_provider"))
                    and str(suggestion.get("ontology") or suggestion.get("source_provider")).upper() in selected_upper
                ]
        for suggestion in suggestions_list:
            if suggestion is None:
                continue
            if "label" in suggestion and "uri" in suggestion:
                sugg_copy = suggestion.copy()
                s_label = sugg_copy.get("label", "")
                if not isinstance(s_label, str):
                    s_label = str(s_label)
                sugg_copy["levenshtein_score"] = calculate_levenshtein_score(term_main, s_label)
                combined_suggestions.append(sugg_copy)
            else:
                logger.warning("Skipping malformed suggestion from %s: %r", provider_name, suggestion)

    sort_key_str = "levenshtein_score" if strategy == "Levenshtein Similarity" else "score"

    def sort_key_func(x):
        val = x.get(sort_key_str)
        if isinstance(val, (int, float)):
            return val
        return x.get("levenshtein_score", -1.0)

    sorted_suggestions = sorted([s for s in combined_suggestions if isinstance(s, dict)], key=sort_key_func, reverse=True)
    return sorted_suggestions[:max_suggestions_to_show]
