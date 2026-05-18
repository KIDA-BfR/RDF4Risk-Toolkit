# processing_service.py
import logging

import pandas as pd

try:
    from .provider_registry import get_provider
    from .reconciliation_core import CUSTOM_SPARQL_PROVIDER_NAME
except ImportError:  # pragma: no cover - direct script fallback
    from provider_registry import get_provider
    from reconciliation_core import CUSTOM_SPARQL_PROVIDER_NAME


logger = logging.getLogger(__name__)


def query_custom_sparql(term: str, limit: int, config: dict, user_agent: str) -> list:
    """Backward-compatible wrapper for the registry-backed custom SPARQL provider."""
    provider = get_provider(CUSTOM_SPARQL_PROVIDER_NAME)
    kwargs = provider.build_kwargs(config, limit)
    return provider.query(term=term, limit=limit, user_agent=user_agent, **kwargs)


def _provider_kwargs(provider_name: str, config: dict, num_suggestions: int) -> dict:
    provider = get_provider(provider_name)
    return provider.build_kwargs(config, num_suggestions)


def _query_provider(provider_name: str, term: str, config: dict, user_agent: str, num_suggestions: int) -> list:
    provider = get_provider(provider_name)
    kwargs_for_call = provider.build_kwargs(config, num_suggestions)
    logger.debug("Calling %s for term %r with args: %s", provider_name, term, sorted(kwargs_for_call.keys()))
    return provider.query(term=term, limit=num_suggestions, user_agent=user_agent, **kwargs_for_call)


# --- Main Chunk Processing Function ---
def process_chunk_for_provider(
    current_provider_name: str,
    all_indices_to_process: list,
    processed_indices_for_this_provider: set,
    df: pd.DataFrame,
    config: dict,
    user_agent: str,
    num_suggestions: int,
    matching_strategy: str,
    chunk_size: int,
) -> (dict, set, bool, str):
    """
    Processes the next chunk of terms for the specified provider by calling the
    registry-backed provider implementation with provider-built kwargs.
    """
    _ = matching_strategy
    indices_to_process_now = []
    processed_in_this_chunk = set()
    suggestions_for_chunk = {}
    error_message = None
    finished_provider = False

    count = 0
    for index in all_indices_to_process:
        if index not in processed_indices_for_this_provider:
            indices_to_process_now.append(index)
            count += 1
            if count >= chunk_size:
                break

    logger.info("Provider %r: Processing chunk with %d terms.", current_provider_name, len(indices_to_process_now))

    if not indices_to_process_now:
        all_done = all(idx in processed_indices_for_this_provider for idx in all_indices_to_process)
        logger.info("No more indices to process for provider %r. All done: %s", current_provider_name, all_done)
        return {}, set(), all_done, None

    try:
        provider = get_provider(current_provider_name)
        kwargs_for_call = provider.build_kwargs(config, num_suggestions)
    except Exception as exc:
        error_message = f"Error preparing provider '{current_provider_name}': {type(exc).__name__} - {exc}"
        logger.exception(error_message)
        return {}, processed_in_this_chunk, True, error_message

    for index in indices_to_process_now:
        term = str(df.loc[index, "Term"]).strip() if index in df.index else None
        if not term:
            logger.debug("Skipping empty or invalid term at index %s for %s", index, current_provider_name)
            processed_in_this_chunk.add(index)
            continue

        try:
            term_suggestions = provider.query(
                term=term,
                limit=num_suggestions,
                user_agent=user_agent,
                **kwargs_for_call,
            )
            suggestions_for_chunk[index] = {current_provider_name: term_suggestions}
            processed_in_this_chunk.add(index)
        except Exception as exc:
            error_message = f"Error querying {current_provider_name} for term '{term}' (Index {index}): {type(exc).__name__} - {exc}"
            logger.exception(error_message)
            finished_provider = True
            break

    all_processed_indices = processed_indices_for_this_provider.union(processed_in_this_chunk)
    remaining_indices_exist = any(idx not in all_processed_indices for idx in all_indices_to_process)
    if not remaining_indices_exist and not error_message:
        logger.info("All terms (%d) processed for provider %r.", len(all_indices_to_process), current_provider_name)
        finished_provider = True
    elif error_message:
        logger.warning("Provider %r finishing chunk due to error: %s", current_provider_name, error_message)
        finished_provider = True

    logger.debug(
        "Returning from process_chunk for %r: Suggestions=%d, ProcessedNow=%d, Finished=%s, Error=%r",
        current_provider_name,
        len(suggestions_for_chunk),
        len(processed_in_this_chunk),
        finished_provider,
        error_message,
    )
    return suggestions_for_chunk, processed_in_this_chunk, finished_provider, error_message


# --- Function to fetch suggestions for a single term from a specific provider ---
def fetch_suggestions_for_term_from_provider(
    provider_name: str,
    term_to_search: str,
    config: dict,
    user_agent: str,
    num_suggestions: int,
) -> list:
    """
    Fetches suggestions for a single term from a specified provider.
    """
    logger.info(
        "[CustomSearch] Entering fetch_suggestions_for_term_from_provider for term=%r, provider=%r, num_suggestions=%s",
        term_to_search,
        provider_name,
        num_suggestions,
    )
    if not term_to_search or not term_to_search.strip():
        logger.warning("Term to search is empty. Returning no suggestions.")
        return []

    try:
        suggestions = _query_provider(provider_name, term_to_search, config, user_agent, num_suggestions)
    except Exception as exc:
        error_message = f"Error querying {provider_name} for term '{term_to_search}': {type(exc).__name__} - {exc}"
        logger.error("[CustomSearch] %s", error_message, exc_info=True)
        raise

    logger.info(
        "[CustomSearch] Returning %d suggestions from fetch_suggestions_for_term_from_provider for term %r, provider %r.",
        len(suggestions),
        term_to_search,
        provider_name,
    )
    return suggestions
