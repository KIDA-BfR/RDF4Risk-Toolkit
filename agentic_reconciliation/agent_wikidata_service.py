# -*- coding: utf-8 -*-
"""Wikidata lookup and enriched definition helpers for agent-based reconciliation."""

from __future__ import annotations

import datetime
import json
import random
import re
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import requests

from .agent_models import AgentCandidate


DEFAULT_WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
DEFAULT_PROPERTY_LABEL_PATHS = [
    Path(__file__).resolve().parent / "data" / "wikidata_properties.json",
]
DEFAULT_HEADERS = {
    "User-Agent": "RDF4RiskAgentReconciliation/0.1 (contact: https://github.com/KIDA-BfR/RDF4Risk-Toolkit)",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}
DEFAULT_CACHE_TTL_SECONDS = 24 * 60 * 60
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.50
DEFAULT_MAX_RETRIES = 4
DEFAULT_BACKOFF_BASE_SECONDS = 0.6
DEFAULT_MAX_BACKOFF_SECONDS = 8.0
SEARCH_CACHE_TTL_SECONDS = 24 * 60 * 60
SEARCH_MAX_RETRIES = 5
SEARCH_BACKOFF_BASE_SECONDS = 1.0
SEARCH_MAX_BACKOFF_SECONDS = 30.0


class WikidataRateLimitError(requests.HTTPError):
    """Raised when Wikidata rate limiting persists after retries."""


_REQUEST_LOCK = threading.Lock()
_RATE_LIMIT_LOCK = threading.Lock()
_LAST_REQUEST_TS = 0.0
_RATE_LIMIT_UNTIL = 0.0
_SESSION = requests.Session()
_ENTITY_CACHE: Dict[str, tuple[float, Dict[str, Any]]] = {}
_LABEL_CACHE: Dict[str, tuple[float, Dict[str, str]]] = {}
_SEARCH_CACHE: Dict[str, tuple[float, List[AgentCandidate]]] = {}


def _now() -> float:
    return time.monotonic()


def _cache_get(cache: Dict[str, tuple[float, Any]], key: str, ttl_seconds: int) -> Optional[Any]:
    item = cache.get(key)
    if not item:
        return None
    inserted_ts, value = item
    if _now() - inserted_ts > max(1, int(ttl_seconds)):
        cache.pop(key, None)
        return None
    return value


def _cache_put(cache: Dict[str, tuple[float, Any]], key: str, value: Any) -> None:
    cache[key] = (_now(), value)


def _cache_key(ids: Iterable[str], language: str, props: str) -> str:
    normalized = sorted({str(item).strip().upper() for item in ids if str(item or "").strip()})
    return f"{language}|{props}|{'|'.join(normalized)}"


def _search_cache_key(term: str, limit: int, language: str, profile: str) -> str:
    return f"{str(term or '').strip().lower()}|{int(limit)}|{language}|{profile}"


def _coerce_retry_after_seconds(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None
    raw = str(header_value).strip()
    try:
        value = float(raw)
        return max(0.0, value)
    except Exception:
        return None


def _throttled_get(
    api_url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    *,
    timeout: int = 15,
    min_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
) -> requests.Response:
    global _LAST_REQUEST_TS, _RATE_LIMIT_UNTIL

    params = dict(params or {})
    params.setdefault("maxlag", 5)
    merged_headers = dict(DEFAULT_HEADERS)
    merged_headers.update(headers or {})

    last_error: Optional[Exception] = None
    for attempt in range(max(1, int(max_retries)) + 1):
        with _RATE_LIMIT_LOCK:
            wait_until = _RATE_LIMIT_UNTIL
        if wait_until > _now():
            time.sleep(wait_until - _now())

        with _REQUEST_LOCK:
            elapsed = _now() - _LAST_REQUEST_TS
            wait_for = max(0.0, float(min_interval_seconds) - elapsed)
            if wait_for > 0:
                time.sleep(wait_for)
            _LAST_REQUEST_TS = _now()

        response = _SESSION.get(api_url, params=params, headers=merged_headers, timeout=timeout)
        try:
            response.raise_for_status()

            # MediaWiki can signal soft rate limits as HTTP 200 with JSON error= maxlag.
            try:
                payload = response.json()
                if isinstance(payload, dict) and payload.get("error", {}).get("code") == "maxlag":
                    retry_after = _coerce_retry_after_seconds(response.headers.get("Retry-After")) or 5.0
                    with _RATE_LIMIT_LOCK:
                        _RATE_LIMIT_UNTIL = max(_RATE_LIMIT_UNTIL, _now() + retry_after)
                    if attempt >= max(1, int(max_retries)):
                        raise WikidataRateLimitError("Wikidata maxlag persisted", response=response)
                    time.sleep(max(0.0, retry_after + random.uniform(0.0, 0.5)))
                    continue
            except ValueError:
                pass

            return response
        except requests.HTTPError as exc:
            status_code = getattr(response, "status_code", None)
            last_error = exc
            is_rate_limit = status_code == 429
            is_retriable = status_code in {429, 500, 502, 503, 504}

            if not is_retriable:
                raise

            retry_after = _coerce_retry_after_seconds(response.headers.get("Retry-After"))
            if retry_after is not None:
                sleep_seconds = retry_after + random.uniform(0.0, 0.5)
            else:
                backoff = min(backoff_base_seconds * (2**attempt), float(max_backoff_seconds))
                sleep_seconds = backoff + random.uniform(0.0, 0.25)

            if is_rate_limit:
                with _RATE_LIMIT_LOCK:
                    _RATE_LIMIT_UNTIL = max(_RATE_LIMIT_UNTIL, _now() + sleep_seconds)

            if attempt >= max(1, int(max_retries)):
                if is_rate_limit:
                    raise WikidataRateLimitError(str(exc), response=response) from exc
                raise

            time.sleep(max(0.0, sleep_seconds))

    if isinstance(last_error, requests.HTTPError):
        raise last_error
    raise RuntimeError("Unexpected Wikidata request failure")


@lru_cache(maxsize=4)
def load_property_labels_from_file(path: Optional[str] = None) -> Dict[str, str]:
    candidate_paths = [Path(path)] if path else DEFAULT_PROPERTY_LABEL_PATHS
    for candidate in candidate_paths:
        if candidate.exists():
            with open(candidate, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
    return {}


def _extract_time_string(wikidata_time: str) -> str:
    try:
        if wikidata_time.startswith("+") or wikidata_time.startswith("-"):
            wikidata_time = wikidata_time[1:]
        dt = datetime.datetime.fromisoformat(wikidata_time.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return wikidata_time


def _collect_referenced_item_ids(entities: Dict[str, Any], property_labels: Dict[str, str]) -> List[str]:
    referenced_ids = set()
    for entity_id, entity in entities.items():
        claims = entity.get("claims", {})
        for pid in property_labels.keys():
            for claim in claims.get(pid, []):
                mainsnak = claim.get("mainsnak", {})
                datavalue = mainsnak.get("datavalue", {})
                value = datavalue.get("value")
                if isinstance(value, dict) and value.get("entity-type") == "item" and "id" in value:
                    referenced_ids.add(value["id"])
        referenced_ids.discard(entity_id)
    return list(referenced_ids)


def _get_entities(
    ids: Iterable[str],
    language: str = "en",
    api_url: str = DEFAULT_WIKIDATA_API_URL,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    ids_list = list(ids)
    if not ids_list:
        return {}
    cache_key = _cache_key(ids_list, language, "labels|descriptions|claims")
    cached = _cache_get(_ENTITY_CACHE, cache_key, DEFAULT_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    params = {
        "action": "wbgetentities",
        "ids": "|".join(ids_list),
        "format": "json",
        "languages": language,
        "props": "labels|descriptions|claims",
    }
    response = _throttled_get(api_url, params, headers or DEFAULT_HEADERS, timeout=15)
    data = response.json()
    entities = data.get("entities", {})
    _cache_put(_ENTITY_CACHE, cache_key, entities)
    return entities


def _get_entity_labels(
    ids: Iterable[str],
    language: str = "en",
    api_url: str = DEFAULT_WIKIDATA_API_URL,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    ids_list = list(ids)
    if not ids_list:
        return {}
    cache_key = _cache_key(ids_list, language, "labels")
    cached = _cache_get(_LABEL_CACHE, cache_key, DEFAULT_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    params = {
        "action": "wbgetentities",
        "ids": "|".join(ids_list),
        "format": "json",
        "languages": language,
        "props": "labels",
    }
    response = _throttled_get(api_url, params, headers or DEFAULT_HEADERS, timeout=15)
    data = response.json()
    entities = data.get("entities", {})
    labels = {}
    for eid, entity in entities.items():
        label_obj = entity.get("labels", {}).get(language)
        if label_obj:
            labels[eid] = label_obj.get("value")
    _cache_put(_LABEL_CACHE, cache_key, labels)
    return labels


def get_wikidata_definition(
    entity_id: str,
    language: str = "en",
    property_labels_path: Optional[str] = None,
    api_url: str = DEFAULT_WIKIDATA_API_URL,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    if not entity_id:
        return None

    property_labels = load_property_labels_from_file(property_labels_path)
    entities = _get_entities([entity_id], language=language, api_url=api_url, headers=headers)
    if entity_id not in entities:
        return None

    entity = entities[entity_id]
    referenced_item_ids = _collect_referenced_item_ids({entity_id: entity}, property_labels)
    referenced_labels = _get_entity_labels(referenced_item_ids, language=language, api_url=api_url, headers=headers)

    def _value_to_string(datavalue: Dict[str, Any]) -> str:
        if not datavalue:
            return ""
        value = datavalue.get("value")
        vtype = datavalue.get("type")
        if vtype == "wikibase-entityid" and isinstance(value, dict):
            eid = value.get("id")
            return referenced_labels.get(eid, eid or "")
        if vtype == "time" and isinstance(value, dict):
            return _extract_time_string(value.get("time", ""))
        if vtype == "globecoordinate" and isinstance(value, dict):
            lat = value.get("latitude")
            lon = value.get("longitude")
            if lat is not None and lon is not None:
                return f"{lat}, {lon}"
        return str(value)

    labels = entity.get("labels", {})
    descriptions = entity.get("descriptions", {})
    claims = entity.get("claims", {})

    label = labels.get(language, {}).get("value") or entity_id
    description = descriptions.get(language, {}).get("value")

    facts: Dict[str, List[str]] = {}
    for pid, human_label in property_labels.items():
        prop_claims = claims.get(pid, [])
        values: List[str] = []
        for claim in prop_claims:
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue")
            if datavalue:
                s = _value_to_string(datavalue)
                if s:
                    values.append(s)
        if values:
            facts[human_label] = values

    definition_parts: List[str] = []
    if description:
        definition_parts.append(description.rstrip("."))
    if facts:
        fact_strings = [f"{human_label}: {', '.join(values)}" for human_label, values in facts.items()]
        definition_parts.append("Key facts: " + "; ".join(fact_strings))
    if not definition_parts:
        definition_parts.append(f"{label} (no textual description available in Wikidata).")

    definition = ". ".join(definition_parts).strip()
    if not definition.endswith("."):
        definition += "."

    return {
        "id": entity_id,
        "label": label,
        "description": description,
        "definition": definition,
        "url": f"https://www.wikidata.org/wiki/{entity_id}",
        "facts": facts,
    }


def resolve_qids_and_pids_in_definition(
    enriched_result: Dict[str, Any],
    language: str = "en",
    property_labels_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not enriched_result:
        return enriched_result

    property_labels = load_property_labels_from_file(property_labels_path)
    qid_pattern = re.compile(r"\bQ\d+\b")
    pid_pattern = re.compile(r"\bP\d+\b")

    all_qids: Set[str] = set()
    definition = enriched_result.get("definition") or ""
    all_qids.update(qid_pattern.findall(definition))

    facts = enriched_result.get("facts") or {}
    if isinstance(facts, dict):
        for vals in facts.values():
            for v in vals:
                if isinstance(v, str):
                    all_qids.update(qid_pattern.findall(v))

    entity_labels: Dict[str, str] = {}
    if all_qids:
        qid_list = list(all_qids)
        for i in range(0, len(qid_list), 50):
            entity_labels.update(_get_entity_labels(qid_list[i:i+50], language=language))

    def _replace_ids_in_text(text: str) -> str:
        text = qid_pattern.sub(lambda m: entity_labels.get(m.group(0), m.group(0)), text)
        text = pid_pattern.sub(lambda m: property_labels.get(m.group(0), m.group(0)), text)
        return text

    new_item = dict(enriched_result)
    if isinstance(definition, str):
        new_item["definition"] = _replace_ids_in_text(definition)

    if isinstance(facts, dict):
        new_facts: Dict[str, list] = {}
        for key, vals in facts.items():
            new_vals = [_replace_ids_in_text(v) if isinstance(v, str) else v for v in vals]
            new_facts[key] = new_vals
        new_item["facts"] = new_facts
    return new_item


def WikidataEntitySearch(
    search: str,
    entity_type: str = "item",
    url: str = DEFAULT_WIKIDATA_API_URL,
    user_agent_header: Optional[str] = None,
    srqiprofile: Optional[str] = None,
) -> str:
    headers = {"User-Agent": user_agent_header or DEFAULT_HEADERS["User-Agent"]}
    if entity_type == "item":
        srnamespace = 0
        srqiprofile = "classic_noboostlinks" if srqiprofile is None else srqiprofile
    elif entity_type == "property":
        srnamespace = 120
        srqiprofile = "classic" if srqiprofile is None else srqiprofile
    else:
        raise ValueError("entity_type must be either 'property' or 'item'")

    params = {
        "action": "query",
        "list": "search",
        "srsearch": search,
        "srnamespace": srnamespace,
        "srlimit": 1,
        "srqiprofile": srqiprofile,
        "srwhat": "text",
        "format": "json",
    }
    response = _throttled_get(url, params, headers, timeout=15)
    data = response.json()
    results = data.get("query", {}).get("search", [])
    if not results:
        return ""
    title = results[0].get("title", "")
    return title


def WikidataEntityDetails(q: str, language: str = "en", property_labels_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    details = get_wikidata_definition(q, language=language, property_labels_path=property_labels_path)
    if details is None:
        return None
    return resolve_qids_and_pids_in_definition(details, language=language, property_labels_path=property_labels_path)


def search_wikidata_candidates(
    term: str,
    limit: int = 10,
    api_url: str = DEFAULT_WIKIDATA_API_URL,
    user_agent: Optional[str] = None,
) -> List[AgentCandidate]:
    cache_key = _search_cache_key(term, limit, "en", "default")
    cached = _cache_get(_SEARCH_CACHE, cache_key, SEARCH_CACHE_TTL_SECONDS)
    if cached is not None:
        return list(cached)

    params = {
        "action": "wbsearchentities",
        "search": term,
        "language": "en",
        "format": "json",
        "limit": limit,
    }
    headers = {"User-Agent": user_agent or DEFAULT_HEADERS["User-Agent"]}
    response = _throttled_get(
        api_url,
        params,
        headers,
        timeout=15,
        max_retries=SEARCH_MAX_RETRIES,
        backoff_base_seconds=SEARCH_BACKOFF_BASE_SECONDS,
        max_backoff_seconds=SEARCH_MAX_BACKOFF_SECONDS,
    )
    data = response.json()
    results = []
    for entry in data.get("search", []):
        qid = entry.get("id")
        uri = entry.get("concepturi") or (f"https://www.wikidata.org/wiki/{qid}" if qid else "")
        results.append(
            AgentCandidate(
                uri=uri,
                label=entry.get("label", ""),
                description=entry.get("description", "") or "",
                source_provider="Wikidata",
                source_workflow="wikidata_deep_agent",
                raw_identifier=qid,
            )
        )
    _cache_put(_SEARCH_CACHE, cache_key, list(results))
    return results


def search_wikidata_candidates_with_options(
    term: str,
    limit: int = 10,
    profile: Optional[str] = None,
    language: str = "en",
    api_url: str = DEFAULT_WIKIDATA_API_URL,
    user_agent: Optional[str] = None,
) -> List[AgentCandidate]:
    """Search candidates with optional profile-driven query shaping.

    Backward compatible wrapper around `search_wikidata_candidates` that allows
    controlled retrieval diversity for the agentic planner.
    """
    search_term = str(term or "").strip()
    if not search_term:
        return []

    selected_profile = str(profile or "default").strip().lower()
    if selected_profile == "focus_related":
        search_term = search_term
    elif selected_profile == "broaden":
        search_term = search_term
    elif selected_profile == "narrow":
        search_term = f'"{search_term}"'

    cache_key = _search_cache_key(search_term, max(1, int(limit or 1)), language, selected_profile or "default")
    cached = _cache_get(_SEARCH_CACHE, cache_key, SEARCH_CACHE_TTL_SECONDS)
    if cached is not None:
        return list(cached)

    params = {
        "action": "wbsearchentities",
        "search": search_term,
        "language": language,
        "format": "json",
        "limit": max(1, int(limit or 1)),
    }
    headers = {"User-Agent": user_agent or DEFAULT_HEADERS["User-Agent"]}
    response = _throttled_get(
        api_url,
        params,
        headers,
        timeout=15,
        max_retries=SEARCH_MAX_RETRIES,
        backoff_base_seconds=SEARCH_BACKOFF_BASE_SECONDS,
        max_backoff_seconds=SEARCH_MAX_BACKOFF_SECONDS,
    )
    data = response.json()

    results: List[AgentCandidate] = []
    for entry in data.get("search", []):
        qid = entry.get("id")
        uri = entry.get("concepturi") or (f"https://www.wikidata.org/wiki/{qid}" if qid else "")
        results.append(
            AgentCandidate(
                uri=uri,
                label=entry.get("label", ""),
                description=entry.get("description", "") or "",
                source_provider="Wikidata",
                source_workflow="wikidata_deep_agent",
                raw_identifier=qid,
            )
        )
    _cache_put(_SEARCH_CACHE, cache_key, list(results))
    return results


def dedupe_candidates(candidates: Iterable[AgentCandidate]) -> List[AgentCandidate]:
    """Deduplicate candidate lists while preserving first-seen ordering."""
    deduped: List[AgentCandidate] = []
    seen: Set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, AgentCandidate):
            continue
        key = str(candidate.raw_identifier or candidate.uri or candidate.label).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def search_wikidata_candidates_multiquery(
    queries: List[str],
    per_query_limit: int = 5,
    language: str = "en",
    api_url: str = DEFAULT_WIKIDATA_API_URL,
    user_agent: Optional[str] = None,
) -> List[AgentCandidate]:
    """Execute several controlled queries and return a deduplicated merged pool."""
    merged: List[AgentCandidate] = []
    for query in queries or []:
        query_text = str(query or "").strip()
        if not query_text:
            continue
        merged.extend(
            search_wikidata_candidates_with_options(
                query_text,
                limit=per_query_limit,
                profile=None,
                language=language,
                api_url=api_url,
                user_agent=user_agent,
            )
        )
    return dedupe_candidates(merged)


def load_candidate_by_qid(
    qid: str,
    language: str = "en",
) -> Optional[AgentCandidate]:
    """Load a single candidate deterministically by QID."""
    identifier = str(qid or "").strip().upper()
    if not re.fullmatch(r"Q\d+", identifier):
        return None

    details = WikidataEntityDetails(identifier, language=language)
    if not details:
        return None

    uri = str(details.get("url") or f"https://www.wikidata.org/wiki/{identifier}")
    return AgentCandidate(
        uri=uri,
        label=str(details.get("label") or identifier),
        description=str(details.get("description") or details.get("definition") or ""),
        source_provider="Wikidata",
        source_workflow="wikidata_deep_agent",
        raw_identifier=identifier,
    )
