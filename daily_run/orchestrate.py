"""Core daily pipeline: fetch missing USQL days, ingest, prune old cache."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from cache import day_cache_status, prune_usql_days
from config import FUNNEL_DAY_TZ
from main import run as fetch_usql_day
from splunk_ingest.config import IngestConfig
from splunk_ingest.pipeline import latest_settled_day, run_backfill, run_incremental
from splunk_ingest.state import State
from utils import datetime_to_timestamp_ms_utc, prune_log_files

log = logging.getLogger("daily_run")


def day_window_ms(day: date, tz_name: str = FUNNEL_DAY_TZ) -> tuple[int, int]:
    """Half-open [day 00:00, next 00:00) in FUNNEL_DAY_TZ as UTC epoch ms."""
    tz = ZoneInfo(tz_name)
    start = datetime(day.year, day.month, day.day, tzinfo=tz)
    end = start + timedelta(days=1)
    return datetime_to_timestamp_ms_utc(start), datetime_to_timestamp_ms_utc(end)


def resolve_day_range(
    cfg: IngestConfig,
    *,
    day: str | None = None,
    force_settlement: bool = False,
) -> tuple[date, date]:
    """Return inclusive (start, end) calendar days to process."""
    latest = latest_settled_day(cfg)

    if day:
        target = date.fromisoformat(day)
        if target > latest and not force_settlement:
            raise ValueError(
                f"Day {day} is not settled yet (latest_settled={latest.isoformat()}). "
                "Re-run later or pass --force-settlement."
            )
        return target, target

    state = State(cfg.state_dir)
    wm = state.last_settled_day()
    if wm:
        start = date.fromisoformat(wm) + timedelta(days=1)
    else:
        start = latest
        log.info("No Splunk watermark: targeting only %s", latest.isoformat())

    if start > latest:
        log.info(
            "Nothing pending (watermark=%s, latest_settled=%s)",
            wm,
            latest.isoformat(),
        )
        # Signal empty work with start > end; caller may still prune.
        return start, latest

    return start, latest


def actions_cache_ready(cache_dir: Path, day: date) -> bool:
    """True when the day has consolidated actions (including empty 0-row file)."""
    return day_cache_status(cache_dir, day.isoformat()) == "actions"


def fetch_day_complete(cache_dir: Path, day: date) -> bool:
    """True when fetch finished: actions present, or legacy discovery-only empty day."""
    return day_cache_status(cache_dir, day.isoformat()) in ("actions", "empty")


def fetch_missing_days(
    cfg: IngestConfig,
    start: date,
    end: date,
    *,
    force_fetch: bool = False,
) -> list[str]:
    """Fetch any day in [start, end] missing a completed fetch (or forced).

    Completion means ``actions.parquet`` exists (possibly 0 rows after an empty
    discovery), or a legacy discovery-only empty day. Incomplete cache after
    fetch raises — never treat a failed/partial fetch as empty.
    """
    fetched: list[str] = []
    d = start
    while d <= end:
        need = force_fetch or not fetch_day_complete(cfg.cache_dir, d)
        if not need:
            status = day_cache_status(cfg.cache_dir, d.isoformat())
            log.info("Cache hit for %s (status=%s)", d.isoformat(), status)
        else:
            start_ms, end_ms = day_window_ms(d)
            log.info(
                "Fetching USQL day %s (%s .. %s)",
                d.isoformat(),
                start_ms,
                end_ms,
            )
            fetch_usql_day(
                start_ms,
                end_ms,
                force=force_fetch,
                cache_dir=Path(cfg.cache_dir),
            )
            status = day_cache_status(cfg.cache_dir, d.isoformat())
            if status == "incomplete":
                raise RuntimeError(
                    f"Fetch for {d.isoformat()} left incomplete cache under "
                    f"{cfg.cache_dir / d.isoformat()} "
                    "(no actions.parquet completion marker). "
                    "Refusing to treat as empty day."
                )
            if status == "empty":
                log.info(
                    "Day %s: confirmed empty after fetch (legacy discovery-only)",
                    d.isoformat(),
                )
            fetched.append(d.isoformat())
        d += timedelta(days=1)
    return fetched


def prune_cache_and_logs(
    cfg: IngestConfig,
    *,
    retention_days: int,
    dry_run: bool = False,
) -> dict:
    """Drop USQL day folders older than retention; protect unshipped days."""
    summary: dict = {"pruned_days": [], "bytes_freed": 0, "logs_removed": 0}

    if retention_days <= 0:
        log.info("Cache retention disabled (retention_days=%s)", retention_days)
        return summary

    tz = ZoneInfo(FUNNEL_DAY_TZ)
    today = datetime.now(timezone.utc).astimezone(tz).date()
    keep_before = today - timedelta(days=retention_days)

    state = State(cfg.state_dir)
    wm = state.last_settled_day()
    protect_after = date.fromisoformat(wm) if wm else None

    pruned = prune_usql_days(
        cfg.cache_dir,
        keep_before=keep_before,
        protect_after=protect_after,
        dry_run=dry_run,
    )
    summary["pruned_days"] = [p["day"] for p in pruned]
    summary["bytes_freed"] = sum(p["bytes"] for p in pruned if p.get("deleted"))

    log_dir = Path("logs")
    if log_dir.is_dir() and not dry_run:
        n = prune_log_files(log_dir, retention_days=retention_days)
        summary["logs_removed"] = n
        if n:
            log.info("Removed %d aged log file(s) from %s", n, log_dir)

    return summary


def run_daily(
    cfg: IngestConfig,
    *,
    day: str | None = None,
    force_settlement: bool = False,
    force_fetch: bool = False,
    skip_fetch: bool = False,
    skip_ingest: bool = False,
    skip_prune: bool = False,
    dry_run: bool = False,
    retention_days: int | None = None,
) -> dict:
    """Run fetch -> ingest -> prune. Raises on hard failures."""
    retain = cfg.cache_retention_days if retention_days is None else retention_days
    start, end = resolve_day_range(
        cfg, day=day, force_settlement=force_settlement,
    )

    result: dict = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "fetched": [],
        "ingest": [],
        "prune": {},
    }

    if not skip_fetch:
        if start <= end:
            result["fetched"] = fetch_missing_days(
                cfg, start, end, force_fetch=force_fetch,
            )
        else:
            log.info("No days to fetch (range empty)")
    else:
        log.info("Skipping fetch (--skip-fetch)")

    if not skip_ingest:
        if start <= end:
            # Explicit --day must ship only that day (backfill). Incremental
            # would walk from the watermark and fail on any incomplete gap.
            if day:
                ingest_results = run_backfill(
                    cfg,
                    start.isoformat(),
                    end.isoformat(),
                    force=True,
                    dry_run=dry_run,
                )
            else:
                since = None
                if not State(cfg.state_dir).last_settled_day():
                    since = start.isoformat()
                ingest_results = run_incremental(cfg, since=since, dry_run=dry_run)
            result["ingest"] = ingest_results
            total_flussi = sum(r.get("flussi", 0) for r in ingest_results)
            log.info(
                "Ingest done: %d day(s), %d flussi (dry_run=%s)",
                len(ingest_results),
                total_flussi,
                dry_run,
            )
        else:
            log.info("No days to ingest (range empty)")
    else:
        log.info("Skipping ingest (--skip-ingest)")

    if skip_prune or dry_run:
        log.info("Skipping prune (skip_prune=%s dry_run=%s)", skip_prune, dry_run)
    else:
        result["prune"] = prune_cache_and_logs(cfg, retention_days=retain, dry_run=False)

    return result
