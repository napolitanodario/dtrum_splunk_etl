"""Build lean Splunk HEC flusso events from funnel FlowResult."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from config import FUNNEL_DAY_TZ
from funnel.categories import split_categorie_beni
from funnel.reconstruct import FlowResult

SCHEMA_VERSION = 5
_ENRICH_COLS = ("browserType", "country", "city", "bounce")
_TARGET_METRIC_KEYS = (
    "elapsedSeconds",
    "machineSeconds",
    "userSeconds",
    "frontendSeconds",
    "networkSeconds",
    "serverSeconds",
    "actionCount",
)


def _i(v) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _f(v, ndigits: int = 3) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return round(f, ndigits)


def _s(v) -> Optional[str]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s or None


def _bool_bounce(v) -> Optional[bool]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes")


def _clean(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _target_object(value: Any) -> Optional[dict[str, Any]]:
    """Normalize a target-page metrics dict for HEC; None if missing/empty."""
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    if not isinstance(value, dict):
        return None
    out: dict[str, Any] = {}
    for key in _TARGET_METRIC_KEYS:
        if key not in value or value[key] is None:
            continue
        if key == "actionCount":
            iv = _i(value[key])
            if iv is not None:
                out[key] = iv
        else:
            fv = _f(value[key], 2)
            if fv is not None:
                out[key] = fv
    return out or None


def _day_from_flusso_id(flusso_id: str) -> Optional[str]:
    """Parse YYYYMMDD from flusso_id '{user}_{YYYYMMDD}_{n}' -> 'YYYY-MM-DD'."""
    parts = str(flusso_id).rsplit("_", 2)
    if len(parts) < 3:
        return None
    day = parts[-2]
    if len(day) == 8 and day.isdigit():
        return f"{day[:4]}-{day[4:6]}-{day[6:8]}"
    return None


def _session_enrichment(session: pd.DataFrame) -> dict[str, dict]:
    cols = [c for c in _ENRICH_COLS if c in session.columns]
    if not cols or "sessionId" not in session.columns:
        return {}
    out: dict[str, dict] = {}
    for sid, g in session.groupby("sessionId"):
        rec = {}
        for c in cols:
            vals = g[c].dropna()
            if not len(vals):
                continue
            v = vals.iloc[0]
            if c == "bounce":
                rec[c] = _bool_bounce(v)
            else:
                rec[c] = _s(v)
        out[str(sid)] = _clean(rec)
    return out


def iter_flusso_events(result: FlowResult) -> Iterator[dict[str, Any]]:
    """Yield one lean Splunk flusso event dict per reconstructed flusso.

    Schema v5 adds optional scalar target objects ``salva`` / ``stampa`` /
    ``firma`` (precomputed in ``build_flow_features``) so dashboards avoid
    summing nested ``steps[].actions[]`` at search time.

    Iterates ``matched`` grouped by ``flusso_id`` (one sort + groupby) instead of
    materializing a dict of per-flusso DataFrames.
    """
    flows = result.flows
    matched = result.matched
    breakdown = result.step_breakdown
    if flows is None or flows.empty:
        return
    if matched is None or matched.empty:
        return

    enrich = _session_enrichment(result.session)

    # Light index: one namedtuple per flusso (not a DataFrame slice).
    flow_by_id = {row.flusso_id: row for row in flows.itertuples(index=False)}

    # Light step metadata: flusso_id -> step_index -> {stepLabel, pagina}.
    step_info_by_fid: dict[Any, dict[int, dict]] = {}
    if breakdown is not None and not breakdown.empty:
        for fid, bg in breakdown.groupby("flusso_id", sort=False):
            step_info_by_fid[fid] = {
                int(sr.step_index): {
                    "stepLabel": _s(sr.label),
                    "pagina": _s(getattr(sr, "pagina", None)),
                }
                for sr in bg.itertuples(index=False)
            }

    matched = matched.sort_values(
        ["flusso_id", "step_index", "actionStartTime"],
        kind="mergesort",
    )

    for fid, g in matched.groupby("flusso_id", sort=False):
        r = flow_by_id.get(fid)
        if r is None:
            continue

        # Deduplicate occurrences for root metrics (sessionId + startTime).
        occ_key = g[["sessionId", "actionStartTime"]].drop_duplicates()
        occ = g.drop_duplicates(subset=["sessionId", "actionStartTime"], keep="first")
        n_occ = len(occ_key)
        active_s = float(pd.to_numeric(occ["durationSec"], errors="coerce").fillna(0).sum())
        starts = pd.to_numeric(g["actionStartTime"], errors="coerce")
        ends = pd.to_numeric(g["actionEndTime"], errors="coerce")
        block_start = _i(starts.min())
        block_end = _i(ends.max())
        duration_s = _f(
            (ends.max() - starts.min()) / 1000.0 if pd.notna(starts.min()) and pd.notna(ends.max()) else None,
            2,
        )
        dead_s = _f((duration_s or 0) - active_s, 2) if duration_s is not None else None

        session_ids = sorted({str(s) for s in g["sessionId"].dropna().unique()})
        primary = session_ids[0] if session_ids else None
        e = enrich.get(primary, {}) if primary else {}

        categories = split_categorie_beni(getattr(r, "categorie_beni", None))

        # Global seq by chronological order of first occurrence.
        occ_sorted = occ.sort_values("actionStartTime", kind="mergesort")
        seq_map: dict[tuple, int] = {}
        for seq, row in enumerate(occ_sorted.itertuples(index=False), start=1):
            seq_map[(str(row.sessionId), int(row.actionStartTime))] = seq

        step_info = step_info_by_fid.get(fid, {})

        steps_out: list[dict] = []
        for step_index, sg in g.groupby("step_index", sort=True):
            si = int(step_index)
            info = step_info.get(si, {})
            sg = sg.sort_values("actionStartTime", kind="mergesort")
            # One entry per occurrence within the step.
            seen: set[tuple] = set()
            actions_out: list[dict] = []
            for a in sg.itertuples(index=False):
                sid = _s(a.sessionId)
                st = _i(a.actionStartTime)
                if sid is None or st is None:
                    continue
                key = (sid, st)
                if key in seen:
                    continue
                seen.add(key)
                dur = None
                if hasattr(a, "durationSec") and a.durationSec is not None:
                    try:
                        dur = int(round(float(a.durationSec) * 1000))
                    except (TypeError, ValueError):
                        dur = None
                actions_out.append(_clean({
                    "seq": seq_map.get(key),
                    "sessionId": sid,
                    "startTime": st,
                    "actionKey": _s(getattr(a, "actionKey", None)),
                    "actionType": _s(getattr(a, "actionType", None)),
                    "duration": dur,
                    "frontendTime": _i(getattr(a, "frontendTime", None)),
                    "networkTime": _i(getattr(a, "networkTime", None)),
                    "serverTime": _i(getattr(a, "serverTime", None)),
                }))

            step_starts = pd.to_numeric(sg["actionStartTime"], errors="coerce")
            step_ends = pd.to_numeric(sg["actionEndTime"], errors="coerce")
            label = info.get("stepLabel")
            if label is None and "label" in sg.columns:
                label = _s(sg["label"].iloc[0])
            steps_out.append(_clean({
                "stepIndex": si,
                "stepLabel": label,
                "pagina": info.get("pagina"),
                "actionCount": len(actions_out),
                "firstStartTime": _i(step_starts.min()),
                "lastEndTime": _i(step_ends.max()),
                "actions": actions_out,
            }))

        event = _clean({
            "eventId": f"f:{fid}",
            "version": SCHEMA_VERSION,
            "flussoId": _s(fid),
            "userId": _s(getattr(r, "userId", None) or (g["userId"].iloc[0] if "userId" in g.columns else None)),
            "day": _day_from_flusso_id(fid),
            "blockStartTime": block_start,
            "blockEndTime": block_end,
            "sessionIds": session_ids,
            "categories": categories or None,
            "browserType": e.get("browserType"),
            "country": e.get("country"),
            "city": e.get("city"),
            "bounce": e.get("bounce"),
            "nOccurrences": n_occ,
            "nUniqueActions": _i(getattr(r, "n_unique_actions", None)) or int(g["actionKey"].nunique()),
            "nSteps": len(steps_out),
            "maxStep": _i(getattr(r, "max_step", None)) or (max((s["stepIndex"] for s in steps_out), default=None)),
            "durationSeconds": duration_s,
            "activeSeconds": round(active_s, 2),
            "deadTimeSeconds": dead_s,
            "completed": bool(getattr(r, "completed", False)),
            "abandoned": bool(getattr(r, "abandoned", True)),
            "requestErrors": _i(getattr(r, "request_errors", 0)) or 0,
            "javascriptErrors": _i(getattr(r, "js_errors", 0)) or 0,
            "totalErrors": _i(getattr(r, "total_errors", 0)) or 0,
            "hasError": bool(getattr(r, "has_error", False)),
            "salva": _target_object(getattr(r, "salva", None)),
            "stampa": _target_object(getattr(r, "stampa", None)),
            "firma": _target_object(getattr(r, "firma", None)),
            "steps": steps_out,
        })
        yield event


def write_flusso_jsonl(events: Iterable[dict], path: Path) -> int:
    """Write one JSON object per line. Returns event count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
            n += 1
    return n


def write_flusso_json_array(events: list[dict], path: Path) -> int:
    """Write a single JSON array file. Prefer JSONL for large exports."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(events, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return len(events)


def hour_bucket_for_event(
        event: dict[str, Any],
        day: str,
        tz_name: str | None = None,
) -> int | None:
    """Map a flusso event to local hour 0-23 on calendar day if blockStartTime matches."""
    block_start = event.get("blockStartTime")
    if block_start is None:
        return None
    tz = ZoneInfo(tz_name or FUNNEL_DAY_TZ)
    dt = datetime.fromtimestamp(int(block_start) / 1000, tz=timezone.utc).astimezone(tz)
    if dt.strftime("%Y-%m-%d") != day:
        return None
    return dt.hour


def split_events_by_hour(
        events: Iterable[dict[str, Any]],
        day: str,
        tz_name: str | None = None,
) -> dict[int, list[dict[str, Any]]]:
    """Bucket Splunk v2 flusso events by blockStartTime hour (0-23) in FUNNEL_DAY_TZ."""
    buckets: dict[int, list[dict[str, Any]]] = {h: [] for h in range(24)}
    for event in events:
        hour = hour_bucket_for_event(event, day, tz_name=tz_name)
        if hour is not None:
            buckets[hour].append(event)
    return buckets


def write_hourly_flusso_json(
        events: Iterable[dict[str, Any]],
        output_dir: Path,
        stem: str,
        day: str,
        tz_name: str | None = None,
) -> dict[int, Path]:
    """Write 24 JSON array files (one per hour) for validation / inspection."""
    buckets = split_events_by_hour(events, day, tz_name=tz_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[int, Path] = {}
    for hour in range(24):
        path = output_dir / f"splunk_flussi_{stem}_h{hour:02d}.json"
        write_flusso_json_array(buckets[hour], path)
        paths[hour] = path
    return paths
