import os
import hashlib
import re
from datetime import datetime
import logging
from collections import defaultdict
import yaml, urllib.parse
import pandas as pd
from typing import Dict, Callable, Optional, Any
import aiohttp
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_hashed_graph_uri(file_path, base_uri=None, config=None):
    """
    Generates a unique, hashed graph URI from a file path and the current timestamp.
    The base URI is determined in the following order of precedence:
    1. `base_uri` argument (if provided)
    2. `config['default_namespace']`
    If no base URI can be determined, a ValueError is raised.
    """
    try:
        # Determine base URI: explicit > config > error
        if base_uri is None:
            if config and 'default_namespace' in config:
                base_uri = config['default_namespace']
            else:
                logging.error("Cannot generate graph URI: 'default_namespace' not found in config and no base_uri provided.")
                raise ValueError("Missing default_namespace in configuration.")
        
        base_uri = base_uri.rstrip('/') + '/graph/'

        # 1. Get the filename and current date
        filename = os.path.basename(file_path)
        current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        # 2. Normalize the string
        normalized_string = f"{filename}-{current_date}"
        normalized_string = re.sub(r'[^a-zA-Z0-9_-]', '', normalized_string) # Remove special chars

        # 3. Hash the string using MD5
        hasher = hashlib.md5()
        hasher.update(normalized_string.encode('utf-8'))
        hashed_string = hasher.hexdigest()

        # 4. Construct the final URI with 'da' prefix and 'ta' suffix around the hash
        graph_uri = f"{base_uri}da{hashed_string}ta"
       
        return graph_uri
        
    except Exception as e:
        logging.error(f"Failed to generate hashed graph URI: {e}", exc_info=True)
        return None

def map_custom_uri_to_standard_uri(custom_uri: str, default_namespace: str) -> str:
    """
    This function was previously used to force a '/concepts/' path for URIs within the default namespace.
    However, this can lead to unintended modifications of external URIs or URIs that should remain canonical.
    For now, it will simply return the custom_uri as is, relying on other parts of the code
    (e.g., clean_string_for_uri) to construct internal concept URIs when needed.
    """
    return custom_uri

# --- from prefix_manager.py ---

def extract_prefixes(file_path):
    """
    Extracts prefixes from a file, allowing for multiple prefixes to be associated with the same namespace URI.
    """
    prefixes = defaultdict(list)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            prefix_regex = re.compile(r'^\s*(?:@prefix|PREFIX)\s+([\w\-]+):\s+<([^>]+)>', re.IGNORECASE)
            for line in f:
                match = prefix_regex.match(line)
                if match:
                    prefix, namespace = match.groups()
                    namespace = namespace.strip()
                    if prefix not in prefixes[namespace]:
                        prefixes[namespace].append(prefix)
    except FileNotFoundError:
        pass # Silently fail if the file doesn't exist
    except Exception:
        pass # Silently fail on other errors
    return dict(prefixes)

def find_relevant_prefixes(all_prefixes, full_uris):
    """
    Finds the best prefix for each URI by finding the longest matching namespace
    from the prefix file and then binding that prefix to the common base of the
    data URIs.
    """
    final_bindings = {}
    
    # Sort namespaces by length, descending, to ensure longest match is found first.
    sorted_ns = sorted(all_prefixes.items(), key=lambda item: len(item[0]), reverse=True)

    handled_uris = set()

    for file_ns, prefixes in sorted_ns:
        # Find all data URIs that match this namespace and haven't been handled yet
        matching_uris = [u for u in full_uris if u.startswith(file_ns) and u not in handled_uris]
        
        if not matching_uris:
            continue
        
        logging.info(f"Processing namespace: {file_ns} with potential prefixes: {prefixes}")
        logging.info(f"  > Found {len(matching_uris)} matching URIs.")
            
        # Mark these URIs as handled so they aren't processed by a shorter, less specific namespace
        for u in matching_uris:
            handled_uris.add(u)
            
        # For this group of URIs, find the best prefix
        chosen_prefix = None
        if len(prefixes) == 1:
            chosen_prefix = prefixes[0]
        else:
            # AMBIGUITY RESOLUTION (e.g., obo vs mondo for the same base)
            prefix_scores = defaultdict(int)
            for uri in matching_uris:
                local_id = uri[len(file_ns):]
                for p in prefixes:
                    if local_id.lower().startswith(p.lower()):
                        prefix_scores[p] += 1
            
            if prefix_scores:
                # Case 1: Direct match found (e.g., URI local part starts with 'foodon').
                # Choose the one with the highest score, using length as a tie-breaker.
                chosen_prefix = max(prefix_scores, key=lambda p: (prefix_scores[p], len(p)))
                logging.info(f"  > Direct match logic selected '{chosen_prefix}' based on scores: {dict(prefix_scores)}")
            else:
                # Case 2: No direct match. Check for aliases in a more constrained way.
                # An alias is only considered if it's part of the domain name, which is a safer heuristic.
                try:
                    domain = re.search(r'https?://([^/]+)', file_ns).group(1)
                    alias_candidates = [p for p in prefixes if p.lower() in domain.lower()]
                    if alias_candidates:
                        chosen_prefix = max(alias_candidates, key=len)
                        logging.info(f"  > No direct match. Domain-based alias logic selected '{chosen_prefix}' from candidates: {alias_candidates}")
                    else:
                        chosen_prefix = None
                        logging.info(f"  > No direct match and no domain-based alias found. Skipping prefix assignment.")
                except Exception:
                    chosen_prefix = None
                    logging.info(f"  > Could not parse domain for alias check. Skipping prefix assignment.")

        # If a prefix could not be determined, skip this namespace group.
        if not chosen_prefix:
            # Un-handle the URIs so they can be processed by another (less specific) namespace if one exists.
            for u in matching_uris:
                if u in handled_uris:
                    handled_uris.remove(u)
            continue
        
        # Determine the namespace to bind. If multiple URIs share a more specific
        # common path than the file_ns, use that. Otherwise, use file_ns.
        if len(matching_uris) > 1:
            common_base = os.path.commonprefix(matching_uris)
            # Only use the common_base if it's more specific than the file_ns
            if len(common_base) > len(file_ns):
                last_sep = max(common_base.rfind('/'), common_base.rfind('#'))
                # Ensure separator is found and is after the file_ns part
                if last_sep > len(file_ns) - 2:
                    final_namespace = common_base[:last_sep + 1]
                else:
                    final_namespace = file_ns # Fallback
            else:
                final_namespace = file_ns
        else:
            # With only one URI, the namespace from the prefix file is the correct one.
            final_namespace = file_ns

        if chosen_prefix and chosen_prefix not in final_bindings:
            final_bindings[chosen_prefix] = final_namespace
            logging.info(f"  > SUCCESS: Binding prefix '{chosen_prefix}' to namespace '{final_namespace}'")

    return final_bindings


def add_prefixes_to_rdf(rdf_content, relevant_prefixes):
    prefix_string = ""
    for prefix, namespace in relevant_prefixes.items():
        prefix_string += f"@prefix {prefix}: <{namespace}>.\\n"
    
    return prefix_string + rdf_content

# --- from clickableIri.py ---

def load_api_specs(path: str) -> Dict[str, Any]:
    """Load the YAML file once and return a dict keyed by service name."""
    with open(path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
        return config.get("api_specs", {})

def make_ui_link(template: str, acronym: str, iri: str) -> str:
    """
    Substitute two placeholders in every ui_template:
      {acronym}     – the ontology short‑name (e.g. BFO, DOID)
      {encoded_iri} – the URL‑encoded IRI
    """
    return template.format(
        acronym     = acronym,
        encoded_iri = urllib.parse.quote_plus(iri),
        iri         = iri,                     # keep raw iri if template uses it
    )

def parse_bioportal(payload: dict, iri: str, spec: dict) -> Optional[dict]:
    for hit in payload.get("collection", []):
        if hit.get("@id") == iri:
            acr_url = hit.get("links", {}).get("ontology", "")
            acr = acr_url.rstrip("/").split("/")[-1].upper()
            return {
                "label":   hit.get("prefLabel"),
                "acronym": acr,
                "ui_link": make_ui_link(spec["ui_template"], acr, iri),
            }

def parse_ols(payload: dict, iri: str, spec: dict) -> Optional[dict]:
    for doc in payload.get("response", {}).get("docs", []):
        if doc.get("iri") == iri:
            acr = (doc.get("ontology_name") or "").upper()
            return {
                "label":   doc.get("label"),
                "acronym": acr,
                "ui_link": make_ui_link(spec["ui_template"], acr, iri),
            }

_ONT_ABBR_RX = re.compile(r"/obo/([A-Za-z0-9]+)[_#/]")
def parse_ontobee(payload: dict, iri: str, spec: dict) -> Optional[dict]:
    if payload.get("boolean"):                              # SPARQL ASK == true
        m   = _ONT_ABBR_RX.search(iri)
        acr = m.group(1).upper() if m else "UNKNOWN"
        return {
            "acronym": acr,
            "ui_link": make_ui_link(spec["ui_template"], acr, iri),
        }

PARSER_MAP: Dict[str, Callable[[Any, str, dict], Optional[dict]]] = {
    "bioportal": parse_bioportal, 
    "ols":       parse_ols,
    "ontobee":   parse_ontobee,
}

async def call_api(
    session: aiohttp.ClientSession, 
    spec: dict, 
    iri: str, 
    api_keys: Optional[Dict[str, str]] = None
) -> Optional[dict]:
    """
    Calls a single external API to look up an IRI.
    It dynamically constructs the request, handles authentication, and parses the response.
    """
    if "base_url" not in spec:
        logging.warning(f"Skipping API call for {spec.get('parser', 'unknown service')} as 'base_url' is not defined.")
        return None
        
    base    = spec["base_url"].rstrip("/")
    url     = f"{base}{spec['lookup_endpoint']}"
    params  = spec.get("params", {}).copy()
    headers = spec.get("headers", {}).copy()
    auth    = spec.get("auth")

    # --- Authentication Handling ---
    if auth:
        # Priority: User-provided key > config key > env variable
        service_name = spec.get("parser", "")
        user_api_keys = api_keys or {}
        
        key = user_api_keys.get(service_name, spec.get("api_key") or os.getenv(auth.get("env_var", "")))

        if not key and auth.get("required"):
            key = "YourAPIKey"  # Default placeholder if required and missing
        
        if key:
            auth_method = auth.get("method", "header")
            if auth_method == "header":
                header_name = auth.get("header_name", "Authorization")
                header_template = auth.get("template", "apikey token={key}")
                headers[header_name] = header_template.format(key=key)
            elif auth_method == "param":
                param_name = auth.get("param_name", "apikey")
                params[param_name] = key

    # --- Request Customization ---
    # Each service can have its own query field(s)
    if "query_template" in spec:
        # More flexible templating for complex queries (e.g., SPARQL)
        params.update({k: v.format(iri=iri) for k, v in spec["query_template"].items()})
    else:
        # Default to a simple 'q' parameter
        params.setdefault("q", iri)

    try:
        async with session.get(url, params=params, headers=headers, timeout=10) as resp:
            if resp.status >= 400:
                logging.warning(
                    f"API call to {spec.get('parser', 'unknown service')} for IRI {iri} "
                    f"failed with status {resp.status}. This service will be skipped."
                )
                return None
            return await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logging.warning(f"API call to {spec.get('parser', 'unknown service')} failed for IRI {iri}: {exc}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during API call for {iri} to {spec.get('parser', 'unknown service')}: {e}")
        return None

async def lookup_iri_in_source(
    session: aiohttp.ClientSession, 
    spec: dict, 
    iri: str, 
    api_keys: Optional[Dict[str, str]] = None
) -> Optional[dict]:
    """Helper that calls the API and then parses the result."""
    raw_response = await call_api(session, spec, iri, api_keys)
    if raw_response is None:
        return None  # Network or HTTP error
    
    parser_func = PARSER_MAP.get(spec["parser"])
    if not parser_func:
        logging.warning(f"No parser found for service: {spec['parser']}")
        return None
        
    hit = parser_func(raw_response, iri, spec)
    if hit:
        hit["source"] = spec["parser"].upper()
        hit["iri"] = iri
    return hit

async def lookup_iri_cascade(
    session: aiohttp.ClientSession, 
    iri: str, 
    specs: Dict[str, dict],
    api_keys: Optional[Dict[str, str]] = None,
    ignore_services: Optional[list] = None
) -> dict:
    """
    Tries to resolve an IRI by querying a series of APIs in a defined order.
    The first successful lookup returns the result.
    """
    # --- Pre-flight Checks ---
    if not isinstance(iri, str) or not iri.startswith("http"):
        return {"source": "Error", "iri": iri, "message": "Invalid IRI provided."}
    
    # --- Hardcoded Quick-Resolutions ---
    if iri.startswith("https://www.wikidata.org/wiki/"):
        return {
            "source": "WIKIDATA", "iri": iri, "ui_link": iri,
            "label": "Wikidata Entity", "acronym": "WIKIDATA"
        }
        
    # --- API Cascade ---
    ignore_list = [s.lower() for s in (ignore_services or [])]
    
    for name, spec in specs.items():
        if name.lower() in ignore_list:
            logging.info(f"Skipping ignored service: {name}")
            continue
            
        logging.info(f"Attempting to resolve {iri} using service: {name}")
        try:
            result = await lookup_iri_in_source(session, spec, iri, api_keys)
            
            # A successful result must have a non-empty ui_link.
            if result and result.get("ui_link"):
                logging.info(f"Successfully resolved {iri} with service: {name}")
                return result
            elif result:
                logging.info(f"Service {name} returned a result for {iri}, but it was incomplete (e.g., no ui_link). Trying next service.")
            else:
                logging.info(f"Service {name} did not find a result for {iri}. Trying next service.")

        except Exception as e:
            logging.error(f"An unexpected error occurred while processing service {name} for IRI {iri}: {e}", exc_info=True)
            # Continue to the next service in the cascade
            logging.info(f"Continuing to next service after error in {name}.")
            
    # --- Fallback ---
    return {"source": "None", "iri": iri, "message": "IRI not found in any source."}

async def process_iris_async(
    df: pd.DataFrame,
    specs: Dict[str, dict],
    concurrency_limit: int = 10,
    api_keys: Optional[Dict[str, str]] = None,
    ignore_services: Optional[list] = None,
    progress_callback: Optional[Callable[[float], None]] = None
) -> pd.DataFrame:
    """
    Asynchronously processes a DataFrame of IRIs, resolving them using the lookup cascade.

    Args:
        df: DataFrame with a 'Mapped ID' column containing IRIs.
        specs: API specifications from the config file.
        concurrency_limit: Max number of concurrent API calls.
        api_keys: A dictionary of {service_name: api_key}.
        ignore_services: A list of service names to skip during lookup.
        progress_callback: An optional function to call with progress updates (0.0 to 1.0).
    """
    semaphore = asyncio.Semaphore(concurrency_limit)
    results = []
    total_iris = len(df)

    async def process_with_semaphore(session, iri, specs, keys, ignored):
        async with semaphore:
            return await lookup_iri_cascade(session, iri, specs, keys, ignored)

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_with_semaphore(session, row["Mapped ID"], specs, api_keys, ignore_services)
            for _, row in df.iterrows()
        ]
        
        for i, f in enumerate(asyncio.as_completed(tasks)):
            results.append(await f)
            if progress_callback:
                progress_callback((i + 1) / total_iris)

    # The order of results from as_completed is not guaranteed.
    # We need to re-order them based on the original IRI order.
    results_map = {res['iri']: res for res in results if 'iri' in res}
    ordered_results = [results_map.get(row["Mapped ID"], {"iri": row["Mapped ID"], "source": "Error", "message": "Processing failed"}) for _, row in df.iterrows()]

    return pd.DataFrame(ordered_results)
