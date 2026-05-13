# -*- coding: utf-8 -*-
"""Service for fetching and caching LLM pricing information from a reliable community feed."""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

PRICING_CACHE_FILE = Path(__file__).resolve().parent / "data" / "llm_pricing_cache.json"
CACHE_TTL_SECONDS = 86400  # 24 hours

# Using LiteLLM's public pricing registry as it's comprehensive and actively maintained
COMMUNITY_PRICING_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"

PROVIDER_PREFIX_MAP = {
    "openai": ["openai/"],
    "anthropic": ["anthropic/"],
    "google_gemini": ["gemini/"],
}


def _load_cache() -> Dict[str, Any]:
    if not PRICING_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(PRICING_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    PRICING_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    PRICING_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def _is_cache_valid(entry: Dict[str, Any]) -> bool:
    updated_at = entry.get("updated_at", 0)
    return (time.time() - updated_at) < CACHE_TTL_SECONDS


def fetch_all_pricing(force_refresh: bool = False) -> Dict[str, Any]:
    """Fetch and cache pricing for all supported providers."""
    cache = _load_cache()
    
    if not force_refresh and cache and _is_cache_valid(cache):
        return cache.get("pricing", {})

    pricing_data = _fetch_community_pricing()

    if pricing_data:
        # Explicitly ensure openai_codex is always zero-priced (subscription-based)
        pricing_data["openai_codex"] = {}
        
        _save_cache({
            "updated_at": time.time(),
            "pricing": pricing_data,
            "fetch_method": "community_json"
        })
        return pricing_data
    
    return cache.get("pricing", {})


def _fetch_community_pricing() -> Dict[str, Any]:
    try:
        resp = requests.get(COMMUNITY_PRICING_URL, timeout=20)
        resp.raise_for_status()
        raw_data = resp.json()
        
        structured_pricing = {
            "openai": {},
            "openai_codex": {},
            "anthropic": {},
            "google_gemini": {}
        }
        
        for model_key, model_info in raw_data.items():
            if not isinstance(model_info, dict) or model_key == "sample_spec":
                continue
                
            input_cost = model_info.get("input_cost_per_token")
            output_cost = model_info.get("output_cost_per_token")
            
            if input_cost is None or output_cost is None:
                continue
                
            # Convert per token cost to per 1M tokens cost
            input_usd_per_mtok = float(input_cost) * 1_000_000
            output_usd_per_mtok = float(output_cost) * 1_000_000
            
            price_entry = {
                "input_usd_per_mtok": input_usd_per_mtok,
                "output_usd_per_mtok": output_usd_per_mtok
            }
            
            # Map by known provider prefixes if available in litellm data
            litellm_provider = model_info.get("litellm_provider", "")
            
            # OpenAI
            if litellm_provider == "openai" or model_key.startswith("openai/") or model_key.startswith("gpt-") or model_key.startswith("o1-") or model_key.startswith("o3-"):
                clean_key = model_key.replace("openai/", "")
                structured_pricing["openai"][clean_key] = price_entry
                structured_pricing["openai_codex"][clean_key] = price_entry
                
            # Anthropic
            elif litellm_provider == "anthropic" or model_key.startswith("anthropic/") or model_key.startswith("claude-"):
                clean_key = model_key.replace("anthropic/", "")
                structured_pricing["anthropic"][clean_key] = price_entry
                
            # Google Gemini
            elif litellm_provider in ["gemini", "vertex_ai"] or model_key.startswith("gemini/") or model_key.startswith("gemini-"):
                clean_key = model_key.replace("gemini/", "").replace("vertex_ai/", "")
                structured_pricing["google_gemini"][clean_key] = price_entry

        return structured_pricing
    except Exception as e:
        logger.error(f"Error fetching community pricing: {e}")
        return {}


def get_pricing_for_provider(provider: str) -> Dict[str, Any]:
    """Get pricing overrides for a specific provider."""
    all_pricing = fetch_all_pricing()
    return all_pricing.get(provider, {})
