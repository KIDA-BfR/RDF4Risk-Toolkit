# -*- coding: utf-8 -*-
"""SKOS training example and classification helpers for agent-based reconciliation."""

from __future__ import annotations

import csv
import json
import os
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from .agent_llm_service import (
    OPENAI_COMPATIBLE_PROVIDER,
    generate_json_completion,
    is_openai_compatible_auth_required_error,
)
from .agent_codex_subscription_service import is_codex_authenticated
from .agent_models import SKOSDecision, SKOSMatch

try:
    import Levenshtein  # type: ignore
except Exception:  # pragma: no cover
    Levenshtein = None


DEFAULT_SKOS_TRAINING_CSV = Path(__file__).resolve().parent / "data" / "agent_skos_training_terms.csv"


def _clamp_confidence(value: Optional[float], default: float = 0.0) -> float:
    try:
        candidate = float(value)
    except Exception:
        candidate = float(default)
    if candidate < 0:
        return 0.0
    if candidate > 1:
        return 1.0
    return candidate


def _heuristic_confidence(term_similarity: float, definition_similarity: float, overlap: float) -> float:
    weighted = (0.45 * term_similarity) + (0.35 * definition_similarity) + (0.20 * overlap)
    return _clamp_confidence(weighted, default=0.0)


def _token_overlap(text_a: str, text_b: str) -> float:
    a = {token for token in text_a.lower().split() if token}
    b = {token for token in text_b.lower().split() if token}
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _string_similarity(text_a: str, text_b: str) -> float:
    a = (text_a or "").strip().lower()
    b = (text_b or "").strip().lower()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if Levenshtein is not None:
        return Levenshtein.ratio(a, b)
    return _token_overlap(a, b)


def load_skos_training_examples(training_path: Optional[str] = None) -> pd.DataFrame:
    path = Path(training_path) if training_path else DEFAULT_SKOS_TRAINING_CSV
    if not path.exists():
        return pd.DataFrame()

    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def build_match_pairs(
    df: pd.DataFrame,
    match_name: str,
    label_col: str,
    desc_col: str,
    term_col: str = "term",
    def_col: str = "definition",
) -> str:
    """Build plain-text training examples for a given SKOS match type."""
    if df is None or df.empty:
        return ""

    buffer = StringIO()
    buffer.write(f"========== {match_name} pairs ==========\n\n")

    subset = df.dropna(subset=[label_col])
    for n, row in enumerate(subset.itertuples(index=False), start=1):
        term_a = getattr(row, term_col)
        def_a = getattr(row, def_col)
        term_b = getattr(row, label_col)
        def_b = getattr(row, desc_col)

        buffer.write(f"{n}) Term A: {term_a}\n")
        buffer.write(f"   Definition A: {def_a}\n")
        buffer.write(f"   Term B: {term_b}\n")
        buffer.write(f"   Definition B: {def_b}\n\n")

    return buffer.getvalue()


def build_skos_example_blocks(training_df: Optional[pd.DataFrame] = None) -> Dict[str, str]:
    df = training_df if training_df is not None else load_skos_training_examples()
    if df is None or df.empty:
        return {"exact": "", "close": "", "related": ""}

    return {
        "exact": build_match_pairs(df, "exactMatch", "exactMatch_label", "exactMatch_description"),
        "close": build_match_pairs(df, "closeMatch", "closeMatch_label", "closeMatch_description"),
        "related": build_match_pairs(df, "relatedMatch", "relatedMatch_label", "relatedMatch_description"),
    }


def normalize_mapping_type(mapping_type: str) -> str:
    normalized = (mapping_type or "").strip().lower()
    mapping = {
        "exact": "skos:exactMatch",
        "close": "skos:closeMatch",
        "related": "skos:relatedMatch",
        "none": "",
        "": "",
        "skos:exactmatch": "skos:exactMatch",
        "skos:closematch": "skos:closeMatch",
        "skos:relatedmatch": "skos:relatedMatch",
    }
    return mapping.get(normalized, mapping_type)


def heuristic_classify_skos_match(term_a: str, gen_def: str, term_b: str, onto_def: str) -> SKOSDecision:
    term_similarity = _string_similarity(term_a, term_b)
    definition_similarity = _string_similarity(gen_def, onto_def)
    overlap = _token_overlap(f"{term_a} {gen_def}", f"{term_b} {onto_def}")
    confidence = _heuristic_confidence(term_similarity, definition_similarity, overlap)

    if term_similarity >= 0.94 or definition_similarity >= 0.92:
        mapping_type = "exact"
        explanation = "The terms or definitions are effectively equivalent based on strong lexical similarity."
    elif max(term_similarity, definition_similarity) >= 0.72 or overlap >= 0.40:
        mapping_type = "close"
        explanation = "The terms are very similar and appear substitutable in many contexts, but not fully equivalent."
    elif overlap >= 0.15:
        mapping_type = "related"
        explanation = "The terms are associated within a similar domain, but they do not denote the same concept."
    else:
        mapping_type = "none"
        explanation = "No sufficiently strong semantic relationship could be inferred from the provided text."

    return SKOSDecision(
        mapping_type=mapping_type,
        explanation=explanation,
        input_term=term_a,
        input_definition=gen_def,
        candidate_term=term_b,
        candidate_definition=onto_def,
        decision_source="heuristic_fallback",
        fallback_reason="heuristic_similarity",
        confidence=confidence,
    )


def classify_skos_match(
    term_a: str,
    gen_def: str,
    term_b: str,
    onto_def: str,
    model_name: str = "gpt-5.1",
    provider: str = "openai",
    training_path: Optional[str] = None,
    api_key_env: str = "OPENAI_API_KEY",
    use_llm: bool = True,
    allow_heuristic_fallback: bool = True,
    reasoning_effort: str = "none",
) -> SKOSDecision:
    """Classify the semantic relation between two concepts into SKOS categories."""
    provider_normalized = str(provider or "").strip()

    if use_llm:
        if provider_normalized == "openai_codex":
            if not is_codex_authenticated():
                if not allow_heuristic_fallback:
                    return SKOSDecision(
                        mapping_type="none",
                        explanation="Codex provider is not authenticated; no heuristic fallback allowed by current run configuration.",
                        input_term=term_a,
                        input_definition=gen_def,
                        candidate_term=term_b,
                        candidate_definition=onto_def,
                        decision_source="error_blocked",
                        fallback_reason="codex_not_authenticated",
                        confidence=0.0,
                    )
                decision = heuristic_classify_skos_match(term_a, gen_def, term_b, onto_def)
                decision.fallback_reason = "codex_not_authenticated"
                return decision

        api_key_present = bool(str(os.getenv(api_key_env) or "").strip())
        if provider_normalized not in {OPENAI_COMPATIBLE_PROVIDER, "openai_codex"} and not api_key_present:
            if not allow_heuristic_fallback:
                return SKOSDecision(
                    mapping_type="none",
                    explanation="Missing API key for provider; no heuristic fallback allowed by current run configuration.",
                    input_term=term_a,
                    input_definition=gen_def,
                    candidate_term=term_b,
                    candidate_definition=onto_def,
                    decision_source="error_blocked",
                    fallback_reason="missing_api_key",
                    confidence=0.0,
                )
            decision = heuristic_classify_skos_match(term_a, gen_def, term_b, onto_def)
            decision.fallback_reason = "missing_api_key"
            return decision

        blocks = build_skos_example_blocks(load_skos_training_examples(training_path))
        user_prompt = f"""
You are comparing semantic similarities between two concepts. Each concept is represented by a term and a definition.
Decide whether the relationship is exact, close, related, or none.

Exact match examples:
{blocks['exact']}

Close match examples:
{blocks['close']}

Related match examples:
{blocks['related']}

Concept A:
Term: {term_a}
Definition: {gen_def}

Concept B:
Term: {term_b}
Definition: {onto_def}

Return JSON with keys exact_match, close_match, related_match, explanation.
"""
        try:
            payload = generate_json_completion(
                provider_normalized,
                model_name,
                system_prompt="You classify semantic relations between concepts and must return JSON only.",
                user_prompt=user_prompt,
                api_key_env=api_key_env,
                temperature=0,
                max_tokens=512,
                reasoning_effort=reasoning_effort,
                interaction_purpose="skos",
            )
            structured = SKOSMatch(**payload)
            if getattr(structured, "exact_match", False):
                mapping_type = "exact"
            elif getattr(structured, "close_match", False):
                mapping_type = "close"
            elif getattr(structured, "related_match", False):
                mapping_type = "related"
            else:
                mapping_type = "none"
                
            llm_conf = payload.get("confidence")
            llm_confidence_val = _clamp_confidence(llm_conf) if llm_conf is not None else None
            
            calibrated = _heuristic_confidence(
                _string_similarity(term_a, term_b),
                _string_similarity(gen_def, onto_def),
                _token_overlap(f"{term_a} {gen_def}", f"{term_b} {onto_def}")
            )
            
            # Boost calibrated score based on mapping prior
            if mapping_type == "exact":
                calibrated = min(1.0, calibrated + 0.15)
            elif mapping_type == "close":
                calibrated = min(1.0, calibrated + 0.05)
                
            return SKOSDecision(
                mapping_type=mapping_type,
                explanation=getattr(structured, "explanation", None) or "",
                input_term=term_a,
                input_definition=gen_def,
                candidate_term=term_b,
                candidate_definition=onto_def,
                decision_source="llm",
                fallback_reason=None,
                confidence=calibrated,
                llm_confidence=llm_confidence_val,
            )
        except Exception as exc:
            if not allow_heuristic_fallback:
                return SKOSDecision(
                    mapping_type="none",
                    explanation="LLM classification failed and heuristic fallback is disabled; manual review required.",
                    input_term=term_a,
                    input_definition=gen_def,
                    candidate_term=term_b,
                    candidate_definition=onto_def,
                    decision_source="error_blocked",
                    fallback_reason="missing_api_key"
                    if provider_normalized == OPENAI_COMPATIBLE_PROVIDER and is_openai_compatible_auth_required_error(exc)
                    else "llm_error",
                    fallback_error_type=type(exc).__name__,
                    fallback_error_message=str(exc)[:300],
                    confidence=0.0,
                )
            decision = heuristic_classify_skos_match(term_a, gen_def, term_b, onto_def)
            if provider_normalized == OPENAI_COMPATIBLE_PROVIDER and is_openai_compatible_auth_required_error(exc):
                decision.fallback_reason = "missing_api_key"
            else:
                decision.fallback_reason = "llm_error"
            decision.fallback_error_type = type(exc).__name__
            decision.fallback_error_message = str(exc)[:300]
            decision.fallback_payload_preview = str(locals().get("payload", ""))[:300]
            return decision

    return heuristic_classify_skos_match(term_a, gen_def, term_b, onto_def)
