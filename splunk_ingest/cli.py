"""Command-line entrypoint for scheduled flusso ingestion."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from funnel.splunk_events import SCHEMA_VERSION

from .config import IngestConfig
from . import pipeline

log = logging.getLogger("splunk_ingest")


def _load_config(path: str | None, cache_dir: str | None) -> IngestConfig:
    cfg = IngestConfig.from_toml(path) if path else IngestConfig.from_env()
    if cache_dir:
        cfg.cache_dir = Path(cache_dir)
    return cfg


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        prog="splunk_ingest",
        description=(
            f"Ship lean FlussoP1 events (schema v{SCHEMA_VERSION}, "
            "sourcetype :flusso) to Splunk HEC."
        ),
    )
    p.add_argument("command", choices=["run", "backfill"])
    p.add_argument("--config", help="TOML config file (default: SPLUNK_HEC_* env vars).")
    p.add_argument(
        "--cache-dir",
        help="USQL cache root (default: .cache/usql or [cache] dir in TOML).",
    )
    p.add_argument("--since", help="Start day YYYY-MM-DD (backfill; or first incremental run).")
    p.add_argument("--until", help="End day YYYY-MM-DD (backfill, inclusive).")
    p.add_argument("--force", action="store_true", help="Backfill: resend already-shipped days.")
    p.add_argument("--dry-run", action="store_true", help="Build and count events, do not send.")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cfg = _load_config(args.config, args.cache_dir)

    if args.command == "run":
        results = pipeline.run_incremental(cfg, since=args.since, dry_run=args.dry_run)
    else:
        if not (args.since and args.until):
            p.error("backfill requires --since and --until")
        results = pipeline.run_backfill(
            cfg,
            args.since,
            args.until,
            force=args.force,
            dry_run=args.dry_run,
        )

    total_f = sum(r["flussi"] for r in results)
    log.info(
        "Done: %d day(s), %d flussi (schema v%d).",
        len(results),
        total_f,
        SCHEMA_VERSION,
    )


if __name__ == "__main__":
    main()
