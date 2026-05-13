# -*- coding: utf-8 -*-
"""Definition preparation utilities for agent-based reconciliation."""

from __future__ import annotations

import os
import platform
import re
import subprocess
import tempfile
import zipfile
from io import BytesIO
from typing import Dict, Iterable, Optional

import xml.etree.ElementTree as ET

import pandas as pd

from .agent_models import DefinitionRecord
from .agent_llm_service import generate_text_completion
from semi_automatic_reconciliation.shared_table_io import sync_matching_table_schemas


def _extract_pdf_text(file_bytes: bytes) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as exc:
        raise ValueError(
            "PDF parsing requires the optional 'pypdf' package. "
            "Install it or upload a DOCX file instead."
        ) from exc

    reader = PdfReader(BytesIO(file_bytes))
    pages = [page.extract_text() or "" for page in reader.pages]
    return "\n".join(pages).strip()


def _extract_docx_text(file_bytes: bytes) -> str:
    try:
        with zipfile.ZipFile(BytesIO(file_bytes)) as docx_zip:
            xml_data = docx_zip.read("word/document.xml")
    except Exception as exc:
        raise ValueError("Unable to parse DOCX file.") from exc

    root = ET.fromstring(xml_data)
    text_parts = [node.text for node in root.iter() if node.text]
    text = "\n".join(text_parts)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()


def _extract_doc_text_with_textutil(file_bytes: bytes, suffix: str) -> str:
    if platform.system().lower() != "darwin":
        raise ValueError(
            "Legacy .doc extraction is currently supported only on macOS via 'textutil'. "
            "Please convert to DOCX or PDF."
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_in:
        temp_in.write(file_bytes)
        temp_in_path = temp_in.name

    try:
        result = subprocess.run(
            ["textutil", "-convert", "txt", "-stdout", temp_in_path],
            capture_output=True,
            text=True,
            check=True,
        )
        return (result.stdout or "").strip()
    except Exception as exc:
        raise ValueError("Unable to extract text from DOC file.") from exc
    finally:
        try:
            os.unlink(temp_in_path)
        except Exception:
            pass


def extract_reference_publication_text(uploaded_publication_file, max_chars: int = 12000) -> str:
    """Extract plain text from an uploaded publication file (PDF/DOC/DOCX)."""
    if uploaded_publication_file is None:
        return ""

    filename = str(getattr(uploaded_publication_file, "name", "") or "").lower()
    file_bytes = uploaded_publication_file.read()
    uploaded_publication_file.seek(0)

    if not file_bytes:
        raise ValueError("Uploaded publication file is empty.")

    if filename.endswith(".pdf"):
        text = _extract_pdf_text(file_bytes)
    elif filename.endswith(".docx"):
        text = _extract_docx_text(file_bytes)
    elif filename.endswith(".doc"):
        text = _extract_doc_text_with_textutil(file_bytes, ".doc")
    else:
        raise ValueError("Unsupported publication file type. Please upload PDF, DOC, or DOCX.")

    if not text.strip():
        raise ValueError("No readable text could be extracted from the uploaded publication.")

    return text.strip()[:max_chars]


def extract_terms_requiring_reconciliation(df: pd.DataFrame, no_match_uri: str = "No Match") -> list[str]:
    if df is None or df.empty:
        return []

    local = sync_matching_table_schemas(df)

    term_col = "subject_label" if "subject_label" in local.columns else "Term"
    uri_col = "object_id" if "object_id" in local.columns else "URI"

    if term_col not in local.columns:
        return []

    if uri_col not in local.columns:
        return local[term_col].dropna().astype(str).unique().tolist()

    filtered = local[
        (local[uri_col].astype(str).str.strip() == "")
        | (local[uri_col].astype(str).str.strip() == no_match_uri)
    ]
    return filtered[term_col].dropna().astype(str).unique().tolist()


def normalize_uploaded_definitions(uploaded_definitions_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if uploaded_definitions_df is None or uploaded_definitions_df.empty:
        return pd.DataFrame(columns=["Term", "Definition"])

    df = uploaded_definitions_df.copy()
    term_col = next((col for col in df.columns if str(col).strip().lower() == "term"), None)
    definition_col = next((col for col in df.columns if str(col).strip().lower() == "definition"), None)

    if not term_col or not definition_col:
        raise ValueError("Uploaded definitions table must contain 'Term' and 'Definition' columns.")

    normalized = df[[term_col, definition_col]].copy()
    normalized.columns = ["Term", "Definition"]
    normalized["Term"] = normalized["Term"].astype(str).str.strip()
    normalized["Definition"] = normalized["Definition"].astype(str).str.strip()
    normalized = normalized[(normalized["Term"] != "") & (normalized["Definition"] != "")]
    normalized = normalized.drop_duplicates(subset=["Term"], keep="first")
    return normalized.reset_index(drop=True)


def build_definition_lookup(definitions_df: Optional[pd.DataFrame]) -> Dict[str, str]:
    if definitions_df is None or definitions_df.empty:
        return {}
    if "Term" not in definitions_df.columns or "Definition" not in definitions_df.columns:
        return {}
    return {
        str(term).strip(): str(definition).strip()
        for term, definition in zip(definitions_df["Term"], definitions_df["Definition"])
        if str(term).strip() and str(definition).strip()
    }


def records_to_dataframe(records: Iterable[DefinitionRecord]) -> pd.DataFrame:
    rows = [{"Term": rec.term, "Definition": rec.definition, "Source": rec.source, "Context": rec.context or ""} for rec in records]
    return pd.DataFrame(rows)


def generate_concise_definitions(
    terms: Iterable[str],
    context: str,
    model_name: str = "o4-mini",
    provider: str = "openai",
    api_key_env: str = "OPENAI_API_KEY",
    reasoning_effort: str = "none",
) -> Dict[str, str]:
    """Port of the simple single-shot definition generation flow from the notebook."""
    term_list = [str(term).strip() for term in terms if str(term).strip()]
    if not term_list:
        return {}

    definitions: Dict[str, str] = {}
    for term in term_list:
        user_prompt = (
            f"Given the following context:\n\n{context}\n\n"
            f"Provide a concise definition of the term '{term}' as used in this reconciliation workflow."
        )
        definitions[term] = generate_text_completion(
            provider,
            model_name,
            system_prompt="You are a helpful assistant that defines scientific and domain-specific terms succinctly.",
            user_prompt=user_prompt,
            api_key_env=api_key_env,
            temperature=0,
            max_tokens=512,
            reasoning_effort=reasoning_effort,
        )
    return definitions


def prepare_used_definitions_df(
    mappings_df: pd.DataFrame,
    strategy: str,
    context_text: Optional[str] = None,
    uploaded_definitions_df: Optional[pd.DataFrame] = None,
    model_name: str = "o4-mini",
    provider: str = "openai",
    api_key_env: str = "OPENAI_API_KEY",
    reasoning_effort: str = "none",
) -> pd.DataFrame:
    """Bridge the one-sheet toolkit input into the term/definition structure used by the notebook workflows."""
    if mappings_df is None or mappings_df.empty:
        return pd.DataFrame(columns=["Term", "Definition"])

    if strategy == "uploaded_sheet":
        return normalize_uploaded_definitions(uploaded_definitions_df)

    if strategy in {"generate_single_shot", "manual_text", "reference_publication"}:
        if not context_text or not context_text.strip():
            raise ValueError("Context text is required when generating definitions.")
        terms = extract_terms_requiring_reconciliation(mappings_df)
        definition_map = generate_concise_definitions(
            terms,
            context_text.strip(),
            model_name=model_name,
            provider=provider,
            api_key_env=api_key_env,
            reasoning_effort=reasoning_effort,
        )
        return pd.DataFrame({"Term": list(definition_map.keys()), "Definition": list(definition_map.values())})

    raise ValueError(f"Unsupported definition strategy: {strategy}")
