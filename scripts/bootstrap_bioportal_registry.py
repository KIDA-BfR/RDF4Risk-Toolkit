#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bootstrap / fill the persistent BioPortal ontology-context registry.

Pre-fetches ontology metadata for an EXPLICIT list of ontology acronyms
(configured + optional project set + CLI-passed) so that runtime ontology
routing / domain-aware retrieval has local context available up front.

It never fetches the full BioPortal universe and never re-fetches fresh
entries. Missing entries are fetched lazily; stale entries are refreshed only
with --refresh-stale. --dry-run reports the plan without any network call.

Examples:
  python scripts/bootstrap_bioportal_registry.py --config config.yaml --dry-run
  python scripts/bootstrap_bioportal_registry.py --config config.yaml
  python scripts/bootstrap_bioportal_registry.py --ontologies FOODON CHEBI GENEPIO SNOMEDCT NCIT
  python scripts/bootstrap_bioportal_registry.py --config config.yaml --refresh-stale
  python scripts/bootstrap_bioportal_registry.py --project-set
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from agentic_reconciliation.agent_bioportal_context_service import (  # noqa: E402
    DEFAULT_BIOPORTAL_BASE_URL,
    PROJECT_RELEVANT_BIOPORTAL_ONTOLOGIES,
    bootstrap_bioportal_ontology_registry,
    get_ontology_context,
)
from agentic_reconciliation.agent_ontology_routing import route_ontologies  # noqa: E402
from agentic_reconciliation.agent_registry_jobs import (  # noqa: E402
    get_bioportal_registry_job_status,
    start_bioportal_catalog_refresh,
)


def _load_dotenv(repo_root: Path) -> None:
    env_path = repo_root / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _ontologies_from_config(config_path: Path) -> List[str]:
    try:
        import yaml  # type: ignore
    except Exception as exc:  # pragma: no cover - pyyaml is a project dependency
        raise SystemExit(f"PyYAML is required to read {config_path}: {exc}")
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    agent_cfg = data.get("agent_reconciliation", {}) if isinstance(data, dict) else {}
    acronyms: List[str] = []
    for key in ("bioportal_agent_ontologies", "trusted_ontologies"):
        values = agent_cfg.get(key) or []
        if isinstance(values, str):
            values = [v.strip() for v in values.split(",")]
        if isinstance(values, list):
            acronyms.extend(str(v).strip() for v in values if str(v).strip())
    return acronyms


def _dedupe_upper(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        norm = str(value or "").strip().upper()
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap the BioPortal ontology-context registry.")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml; uses configured BioPortal ontologies.")
    parser.add_argument("--ontologies", nargs="*", default=None, help="Explicit ontology acronyms to bootstrap.")
    parser.add_argument("--project-set", action="store_true", help="Include the curated project-relevant ontology set.")
    parser.add_argument("--refresh-stale", action="store_true", help="Refresh entries older than the registry TTL.")
    parser.add_argument("--dry-run", action="store_true", help="Report the plan; no BioPortal calls, no writes.")
    parser.add_argument("--max-requests", type=int, default=None, help="Cap on live BioPortal fetches (safety valve).")
    parser.add_argument("--api-key-env", type=str, default="BIOPORTAL_API_KEY", help="Env var holding the BioPortal API key.")
    parser.add_argument("--base-url", type=str, default=DEFAULT_BIOPORTAL_BASE_URL, help="BioPortal base URL.")
    parser.add_argument("--quiet", action="store_true", help="Suppress logging output.")
    # Routing-only mode
    parser.add_argument("--rank-ontologies", type=str, default=None, help="Run LLM ontology routing for this term (no fetch).")
    parser.add_argument("--definition", type=str, default=None, help="Definition for --rank-ontologies.")
    parser.add_argument("--project-context", type=str, default=None, help="Project context for --rank-ontologies.")
    parser.add_argument("--rank-model-provider", type=str, default="openai_codex", help="LLM provider for routing.")
    parser.add_argument("--rank-model", type=str, default="gpt-5.4-mini", help="LLM model for routing.")
    parser.add_argument("--no-llm", action="store_true", help="Routing without LLM (deterministic prefilter only).")
    parser.add_argument("--debug-prompts", action="store_true", help="Include the routing LLM prompt + raw response in --rank-ontologies output.")
    # Catalog / full-registry job modes
    parser.add_argument("--refresh-catalog", action="store_true", help="Lightweight: discover+stub all BioPortal ontology acronyms.")
    parser.add_argument("--all-bioportal", action="store_true", help="Deep full-context fetch across all BioPortal ontologies (slow).")
    parser.add_argument("--resume", action="store_true", help="Skip already-present fresh entries (resume a prior fetch).")
    parser.add_argument("--yes", action="store_true", help="Confirm an unbounded deep-fetch-all (otherwise refused; use --dry-run or --max-requests).")
    return parser


def resolve_acronyms(args: argparse.Namespace) -> List[str]:
    acronyms: List[str] = []
    if args.ontologies:
        acronyms.extend(args.ontologies)
    if args.config:
        acronyms.extend(_ontologies_from_config(Path(args.config)))
    if args.project_set:
        acronyms.extend(PROJECT_RELEVANT_BIOPORTAL_ONTOLOGIES)
    return _dedupe_upper(acronyms)


def _run_ranking(args: argparse.Namespace, api_key: Optional[str]) -> int:
    acronyms = resolve_acronyms(args)
    if not acronyms:
        acronyms = list(PROJECT_RELEVANT_BIOPORTAL_ONTOLOGIES)
    selected, result = route_ontologies(
        args.rank_ontologies,
        args.definition,
        acronyms,
        use_llm=not args.no_llm,
        project_context=args.project_context,
        registry_lookup=lambda acr: get_ontology_context(acr, ensure=False),
        model_provider=args.rank_model_provider,
        model_name=args.rank_model,
        api_key_env=args.api_key_env,
        capture_prompts=bool(args.debug_prompts),
    )
    out = {
        "term": args.rank_ontologies,
        "definition": args.definition,
        "project_context": args.project_context,
        "used_llm": result.used_llm,
        "routing_mode": result.routing_mode,
        "routing_confident": result.routing_confident,
        "fallback_to_default": result.fallback_to_default,
        "top_score": result.top_score,
        "margin": result.margin,
        "reason": result.reason,
        "quality_gate_applied": result.quality_gate_applied,
        "excluded_summary": result.excluded_summary,
        "ranked_ontologies": [r.as_dict() for r in result.ranked_ontologies],
        "selected_ontologies_for_retrieval": selected,
    }
    if args.debug_prompts and result.prompt_debug:
        out["prompt_debug"] = result.prompt_debug
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def _run_registry_job(args: argparse.Namespace, api_key: Optional[str], *, mode: str) -> int:
    if not api_key and not args.dry_run:
        print(f"No API key in ${args.api_key_env}; use --dry-run or set the key.", file=sys.stderr)
        return 2
    if mode == "catalog":
        print("MODE: lightweight catalog refresh — discovers all BioPortal ontology acronyms "
              "and stubs missing entries (1 listing call, no per-ontology deep fetch).", file=sys.stderr)
    else:
        print("=" * 72, file=sys.stderr)
        print("MODE: DEEP full-context enrichment for ALL BioPortal ontologies (ADVANCED).", file=sys.stderr)
        print("WARNING: this performs MULTIPLE API requests PER ontology across the entire", file=sys.stderr)
        print("BioPortal universe (potentially THOUSANDS of requests) and can take a LONG time.", file=sys.stderr)
        print("Use --dry-run to discover the total first, --max-requests to cap, --resume to continue.", file=sys.stderr)
        print("=" * 72, file=sys.stderr)
        if not args.dry_run and not args.yes and args.max_requests in (None, ""):
            print("REFUSED: unbounded deep-fetch-all requires explicit --yes (or use --max-requests / --dry-run).",
                  file=sys.stderr)
            return 2
    job_id = start_bioportal_catalog_refresh(
        mode=mode,
        all_bioportal=True,
        api_key=api_key,
        base_url=args.base_url,
        refresh=bool(args.refresh_stale),
        max_requests=args.max_requests,
        dry_run=bool(args.dry_run),
        confirmed=True,  # explicit CLI invocation is the confirmation
        run_async=False,
    )
    status = get_bioportal_registry_job_status(job_id) or {}
    print(json.dumps({k: status.get(k) for k in (
        "job_id", "status", "mode", "all_bioportal", "dry_run", "total", "processed",
        "percent", "fetched", "skipped", "failed", "errors_sample", "error",
    )}, ensure_ascii=False, indent=2))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if not args.quiet:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    _load_dotenv(REPO_ROOT)
    api_key_early = os.environ.get(args.api_key_env) or None

    if args.rank_ontologies:
        return _run_ranking(args, api_key_early)
    if args.refresh_catalog:
        return _run_registry_job(args, api_key_early, mode="catalog")
    if args.all_bioportal:
        return _run_registry_job(args, api_key_early, mode="full_context")

    acronyms = resolve_acronyms(args)
    if not acronyms:
        print("No ontologies resolved. Pass --config, --ontologies, or --project-set.", file=sys.stderr)
        return 2

    api_key = os.environ.get(args.api_key_env) or None
    if not api_key and not args.dry_run:
        print(
            f"No API key in ${args.api_key_env}. Use --dry-run, or set the key to fetch missing entries.",
            file=sys.stderr,
        )

    summary = bootstrap_bioportal_ontology_registry(
        acronyms,
        api_key=api_key,
        base_url=args.base_url,
        refresh=bool(args.refresh_stale),
        dry_run=bool(args.dry_run),
        max_requests=args.max_requests,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
