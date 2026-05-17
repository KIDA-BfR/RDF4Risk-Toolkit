# -*- coding: utf-8 -*-
"""Pure model-catalog helpers for agent reconciliation UI state."""

from __future__ import annotations

from typing import Dict, List, Optional

from .agent_provider_config import OPENAI_CODEX_PROVIDER


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

    if str(record.get("provider", "")).strip() == OPENAI_CODEX_PROVIDER:
        return label

    input_price = record.get("pricing_input_usd_per_mtok")
    output_price = record.get("pricing_output_usd_per_mtok")
    if isinstance(input_price, (int, float)) and isinstance(output_price, (int, float)):
        label += f" — ${float(input_price):.2f}/${float(output_price):.2f} USD/MTok"
    else:
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
