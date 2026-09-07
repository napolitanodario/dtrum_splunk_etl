"""Export lean Splunk flusso events (schema v5) from cached action chunks or Parquet."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from funnel.reconstruct import load_action_chunks, reconstruct_flows
from funnel.splunk_events import (
    SCHEMA_VERSION,
    iter_flusso_events,
    split_events_by_hour,
    write_flusso_jsonl,
    write_hourly_flusso_json,
)

log = logging.getLogger("usat")
OUTPUT_DIR = Path("output")
DEFAULT_CACHE = Path(".cache/usql")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export lean Splunk flusso JSONL (schema v5) from cache or actions Parquet.",
    )
    parser.add_argument("--input", type=Path, help="Consolidated actions Parquet.")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help=f"USQL cache root (default: {DEFAULT_CACHE})",
    )
    parser.add_argument(
        "--day",
        type=str,
        default=None,
        help="Calendar day YYYY-MM-DD under cache root (default: stem prefix before '-')",
    )
    parser.add_argument(
        "--stem",
        default="2026-07-21-partial",
        help="Output file stem (default: 2026-07-21-partial)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--also-json-array",
        action="store_true",
        help="Also write a pretty-printed JSON array (large; optional).",
    )
    parser.add_argument(
        "--split-by-hour",
        action="store_true",
        help="Write 24 JSON array files (h00-h23) bucketed by blockStartTime local hour.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.input is not None:
        import pandas as pd
        raw = pd.read_parquet(args.input)
    else:
        cache_dir = args.cache_dir or DEFAULT_CACHE
        day = args.day or args.stem.split("-partial")[0].split("_")[0]
        raw = load_action_chunks(cache_dir, day=day)
        if raw.empty:
            log.error("No cached actions under %s (day=%s)", cache_dir, day)
            return 1

    log.info("Loaded %d action rows", len(raw))
    result = reconstruct_flows(raw)
    log.info(
        "Reconstructed flussi=%d matched=%d",
        len(result.flows),
        len(result.matched),
    )

    events = list(iter_flusso_events(result))
    day = args.day or args.stem.split("-partial")[0].split("_")[0]

    out_jsonl = args.output_dir / f"splunk_flussi_{args.stem}.jsonl"
    n = write_flusso_jsonl(events, out_jsonl)
    size_mb = out_jsonl.stat().st_size / (1024 * 1024)

    completed = sum(1 for e in events if e.get("completed"))
    summary = {
        "stem": args.stem,
        "day": day,
        "action_rows": len(raw),
        "flussi": n,
        "completed": completed,
        "abandoned": n - completed,
        "jsonl": str(out_jsonl),
        "jsonl_mb": round(size_mb, 2),
        "schema_version": SCHEMA_VERSION,
    }

    if args.split_by_hour:
        hourly_paths = write_hourly_flusso_json(events, args.output_dir, args.stem, day)
        hourly_counts = split_events_by_hour(events, day)
        summary["hourly_json"] = {
            f"h{hour:02d}": {
                "path": str(hourly_paths[hour]),
                "flussi": len(hourly_counts[hour]),
                "mb": round(hourly_paths[hour].stat().st_size / (1024 * 1024), 2),
            }
            for hour in range(24)
        }
        summary["hourly_total"] = sum(len(hourly_counts[h]) for h in range(24))

    if args.also_json_array:
        out_json = args.output_dir / f"splunk_flussi_{args.stem}.json"
        out_json.write_text(
            json.dumps(events, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        summary["json"] = str(out_json)
        summary["json_mb"] = round(out_json.stat().st_size / (1024 * 1024), 2)

    summary_path = args.output_dir / f"splunk_flussi_{args.stem}_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary"] = str(summary_path)

    log.info("Wrote %d flussi -> %s (%.2f MiB)", n, out_jsonl, size_mb)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
