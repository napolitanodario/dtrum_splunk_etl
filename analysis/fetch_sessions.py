"""Fetch actions for a fixed list of session ids and rebuild funnel locally.

Skips prefix discovery (which would scan all matching useraction rows). Over a
90-day range that is not feasible. Instead:

1. Locate each id on ``usersession`` (one row per session) with a large
   adaptive window.
2. Download ``useraction`` rows only in each session's real [start, end]
   interval, grouped by calendar day so empty weeks are never walked.
3. Optionally reconstruct flows to Parquet (no Splunk ingest).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from client import (  # noqa: E402
    ACTIONS_INITIAL_WINDOW_MIN,
    PAGE_SIZE,
    DynatraceUSQLClient,
)
from config import ACTION_COLUMNS, FUNNEL_DAY_TZ, get_credentials  # noqa: E402
from funnel.reconstruct import reconstruct_flows, write_flow_outputs  # noqa: E402
from queries import session_actions_query, session_lookup_query  # noqa: E402
from utils import iso_string_to_timestamp_ms_utc  # noqa: E402

log = logging.getLogger("usat")

ANALYSIS_DIR = Path(__file__).resolve().parent
DEFAULT_IDS = ANALYSIS_DIR / "session_ids.txt"
DEFAULT_OUTPUT = ANALYSIS_DIR / "output"
DEFAULT_LOG_DIR = ANALYSIS_DIR / "logs"
DEFAULT_CHUNK_SIZE = 40
# Try the full requested range first (90d ad-hoc). If Dynatrace samples,
# DynatraceUSQLClient shrinks; IN-list cardinality stays tiny either way.
DEFAULT_LOOKUP_WINDOW_HOURS = 24 * 90
PAD_MS = 60_000
FALLBACK_SESSION_MS = 60 * 60 * 1000


def setup_logging(log_dir: Path) -> Path:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    run_log = log_dir / "analysis.log"

    logger = logging.getLogger("usat")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(run_log, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return run_log


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Fetch Dynatrace actions for a given session-id list and optionally "
            "rebuild funnel aggregations locally (no Splunk)."
        ),
    )
    p.add_argument(
        "--session-ids",
        type=Path,
        default=None,
        help=f"Text file, one userSessionId per line (default: {DEFAULT_IDS}).",
    )
    p.add_argument(
        "--session-id",
        action="append",
        dest="session_id_args",
        default=[],
        help="Single session id (repeatable). Combined with --session-ids.",
    )
    time = p.add_mutually_exclusive_group(required=True)
    time.add_argument("--days", type=int, help="Look back N days from now (FUNNEL_DAY_TZ).")
    time.add_argument(
        "--start",
        help="Range start ISO-8601 (requires --end), e.g. 2026-06-09T00:00:00+02:00.",
    )
    p.add_argument("--end", help="Range end ISO-8601 (required with --start).")
    p.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Session ids per USQL IN list (default: {DEFAULT_CHUNK_SIZE}).",
    )
    p.add_argument(
        "--lookup-window-hours",
        type=int,
        default=DEFAULT_LOOKUP_WINDOW_HOURS,
        help=(
            "Initial adaptive window for usersession lookup in hours "
            f"(default: {DEFAULT_LOOKUP_WINDOW_HOURS} = 90d). "
            "Capped to the requested range; shrinks automatically if sampled."
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT}).",
    )
    p.add_argument(
        "--stem",
        default=None,
        help="Output file stem (default: derived from the time range).",
    )
    p.add_argument(
        "--build-flows",
        action="store_true",
        help="Reconstruct funnel flows from the fetched actions.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Ignore an existing actions Parquet and re-fetch.",
    )
    p.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help=f"Log directory (default: {DEFAULT_LOG_DIR}).",
    )
    args = p.parse_args(argv)
    if args.start and not args.end:
        p.error("--start requires --end")
    if args.end and not args.start:
        p.error("--end requires --start")
    if args.days is not None and args.days < 1:
        p.error("--days must be >= 1")
    if args.chunk_size < 1:
        p.error("--chunk-size must be >= 1")
    if args.lookup_window_hours < 1:
        p.error("--lookup-window-hours must be >= 1")
    return args


def _chunked(values: Sequence[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), size):
        yield list(values[i:i + size])


def load_session_ids(path: Path | None, extra: Sequence[str]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        sid = raw.split(",")[0].strip().strip('"').strip("'")
        if not sid or sid.startswith("#"):
            return
        if sid.lower() in {"sessionid", "session_id", "id", "usersessionid"}:
            return
        if sid not in seen:
            seen.add(sid)
            ids.append(sid)

    if path is not None:
        if not path.is_file():
            example = ANALYSIS_DIR / "session_ids.example.txt"
            raise FileNotFoundError(
                f"Session id file not found: {path}. "
                f"Copy {example} to {DEFAULT_IDS} and add one id per line."
            )
        for line in path.read_text(encoding="utf-8").splitlines():
            _add(line)
    for item in extra:
        _add(item)
    if not ids:
        raise ValueError("No session ids provided")
    return ids


def resolve_time_range(args: argparse.Namespace) -> tuple[int, int, str]:
    tz = ZoneInfo(FUNNEL_DAY_TZ)
    if args.days is not None:
        end_dt = datetime.now(tz)
        start_dt = end_dt - timedelta(days=args.days)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        stem = f"adhoc_{start_dt.strftime('%Y-%m-%d')}_{end_dt.strftime('%Y-%m-%d')}"
        return start_ms, end_ms, stem
    start_ms = iso_string_to_timestamp_ms_utc(args.start)
    end_ms = iso_string_to_timestamp_ms_utc(args.end)
    if end_ms <= start_ms:
        raise ValueError("end must be after start")
    start_label = args.start[:10]
    end_label = args.end[:10]
    return start_ms, end_ms, f"adhoc_{start_label}_{end_label}"


def _ms(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _session_end_ms(start_ms: int, end_value, duration_value) -> int:
    end_ms = _ms(end_value)
    if end_ms is not None and end_ms > start_ms:
        return end_ms
    duration_ms = _ms(duration_value)
    if duration_ms is not None and duration_ms > 0:
        return start_ms + duration_ms
    return start_ms + FALLBACK_SESSION_MS


def lookup_sessions(
        client: DynatraceUSQLClient,
        session_ids: Sequence[str],
        start_ms: int,
        end_ms: int,
        *,
        chunk_size: int,
        lookup_window_hours: int,
) -> pd.DataFrame:
    span_min = max(1, math.ceil((end_ms - start_ms) / 60_000))
    window_min = min(span_min, lookup_window_hours * 60)
    log.info(
        "Lookup %d session ids on usersession window_min=%d range_days=%.1f",
        len(session_ids),
        window_min,
        (end_ms - start_ms) / 86_400_000,
    )
    parts: list[pd.DataFrame] = []
    chunks = list(_chunked(session_ids, chunk_size))
    for index, chunk in enumerate(chunks, start=1):
        query = session_lookup_query(chunk)
        part = client.fetch(
            query,
            start_ms,
            end_ms,
            page_size=PAGE_SIZE,
            initial_window_min=window_min,
        )
        log.info(
            "Lookup chunk %d/%d ids=%d rows=%d",
            index, len(chunks), len(chunk), len(part),
        )
        if not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame()
    found = pd.concat(parts, ignore_index=True)
    if "sessionId" in found.columns:
        found["sessionId"] = found["sessionId"].astype(str)
        found = found.drop_duplicates(subset=["sessionId"]).reset_index(drop=True)
    return found


def _day_groups(sessions: pd.DataFrame) -> list[tuple[str, list[str], int, int]]:
    tz = ZoneInfo(FUNNEL_DAY_TZ)
    rows: list[tuple[str, str, int, int]] = []
    for row in sessions.itertuples(index=False):
        start = _ms(getattr(row, "startTime", None))
        if start is None:
            continue
        end = _session_end_ms(
            start,
            getattr(row, "endTime", None),
            getattr(row, "duration", None),
        )
        day = (
            datetime.fromtimestamp(start / 1000, tz=ZoneInfo("UTC"))
            .astimezone(tz)
            .strftime("%Y-%m-%d")
        )
        rows.append((day, str(row.sessionId), start, end))

    by_day: dict[str, list[tuple[str, int, int]]] = {}
    for day, sid, start, end in rows:
        by_day.setdefault(day, []).append((sid, start, end))

    out: list[tuple[str, list[str], int, int]] = []
    for day in sorted(by_day):
        items = by_day[day]
        win_start = min(start for _, start, _ in items) - PAD_MS
        win_end = max(end for _, _, end in items) + PAD_MS
        if win_end <= win_start:
            win_end = win_start + PAD_MS
        out.append((day, [sid for sid, _, _ in items], win_start, win_end))
    return out


def fetch_actions(
        client: DynatraceUSQLClient,
        sessions: pd.DataFrame,
        *,
        chunk_size: int,
) -> pd.DataFrame:
    groups = _day_groups(sessions)
    log.info("Action fetch covering %d calendar-day group(s)", len(groups))
    parts: list[pd.DataFrame] = []
    for day, ids, win_start, win_end in groups:
        span_min = max(1, math.ceil((win_end - win_start) / 60_000))
        window_min = max(ACTIONS_INITIAL_WINDOW_MIN, span_min)
        chunks = list(_chunked(ids, chunk_size))
        for index, chunk in enumerate(chunks, start=1):
            query = session_actions_query(chunk, ACTION_COLUMNS)
            part = client.fetch(
                query,
                win_start,
                win_end,
                page_size=PAGE_SIZE,
                initial_window_min=window_min,
            )
            log.info(
                "Actions day=%s chunk %d/%d sessions=%d rows=%d window_min=%d",
                day, index, len(chunks), len(chunk), len(part), window_min,
            )
            if not part.empty:
                parts.append(part)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).drop_duplicates().reset_index(drop=True)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_log = setup_logging(args.log_dir)
    log.info("Logging to %s", run_log)

    try:
        ids_path = args.session_ids
        if ids_path is None and not args.session_id_args:
            ids_path = DEFAULT_IDS
        session_ids = load_session_ids(ids_path, args.session_id_args)
        start_ms, end_ms, default_stem = resolve_time_range(args)
        stem = args.stem or default_stem
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        actions_path = output_dir / f"actions_{stem}.parquet"
        reused = actions_path.is_file() and not args.force
        missing: list[str] = []
        found = pd.DataFrame()

        if reused:
            actions = pd.read_parquet(actions_path)
            log.info(
                "Using existing %s rows=%d (pass --force to re-fetch)",
                actions_path, len(actions),
            )
        else:
            env_id, api_token = get_credentials()
            client = DynatraceUSQLClient(env_id, api_token)
            found = lookup_sessions(
                client,
                session_ids,
                start_ms,
                end_ms,
                chunk_size=args.chunk_size,
                lookup_window_hours=args.lookup_window_hours,
            )
            if not found.empty and "userId" in found.columns:
                tagged = found["userId"].notna() & (
                    found["userId"].astype(str).str.strip() != ""
                )
                n_untagged = int((~tagged).sum())
                if n_untagged:
                    dropped = found.loc[~tagged, "sessionId"].astype(str).tolist()
                    log.warning(
                        "Dropped %d session(s) without userId (same as discovery): %s",
                        n_untagged,
                        ", ".join(dropped[:20]) + ("…" if n_untagged > 20 else ""),
                    )
                    found = found.loc[tagged].reset_index(drop=True)

            found_ids = (
                set(found["sessionId"].astype(str))
                if not found.empty and "sessionId" in found.columns
                else set()
            )
            missing = [s for s in session_ids if s not in found_ids]
            if missing:
                log.warning(
                    "Sessions not found in range (%d/%d). "
                    "They may be outside RUM retention or the time window: %s",
                    len(missing), len(session_ids),
                    ", ".join(missing[:20]) + ("…" if len(missing) > 20 else ""),
                )
            if found.empty:
                log.error("No requested sessions found; nothing to fetch")
                return 1

            actions = fetch_actions(client, found, chunk_size=args.chunk_size)
            actions.to_parquet(actions_path, index=False, compression="zstd")
            log.info("Wrote %s rows=%d", actions_path, len(actions))
            found.to_parquet(
                output_dir / f"sessions_{stem}.parquet",
                index=False,
                compression="zstd",
            )

        flow_paths: dict[str, Path] = {}
        flows_summary: dict = {}
        if args.build_flows:
            if actions.empty:
                log.warning("No actions to reconstruct")
            else:
                log.info("Reconstructing flows from %d action rows", len(actions))
                result = reconstruct_flows(actions)
                flow_paths = write_flow_outputs(result, output_dir, stem)
                flows_summary = {
                    "flussi": len(result.flows),
                    "matched_rows": len(result.matched),
                    "completed": (
                        int(result.flows["completed"].sum())
                        if not result.flows.empty
                        else 0
                    ),
                }
                log.info(
                    "Flows done flussi=%d matched_rows=%d completed=%d",
                    flows_summary["flussi"],
                    flows_summary["matched_rows"],
                    flows_summary["completed"],
                )

        if not found.empty and "sessionId" in found.columns:
            found_n = int(found["sessionId"].nunique())
        elif "sessionId" in actions.columns:
            found_n = int(actions["sessionId"].nunique())
        else:
            found_n = 0
        summary = {
            "stem": stem,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "requested_sessions": len(session_ids),
            "found_sessions": found_n,
            "action_rows": len(actions),
            "actions": str(actions_path),
            "reused_actions": reused,
            **flows_summary,
            "flow_files": {k: str(v) for k, v in flow_paths.items()},
        }
        if not reused:
            summary["missing_sessions"] = missing

        summary_path = output_dir / f"summary_{stem}.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"log={run_log}")
        return 0
    except Exception:
        log.exception("Analysis fetch aborted")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
