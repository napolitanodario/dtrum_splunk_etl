"""Convert analysis flow Parquets to JSON and compute action-level timings.

Reads ``flows_*.parquet``, ``matched_actions_*.parquet`` and
``step_breakdown_*.parquet`` written by ``fetch_sessions.py --build-flows``.
Does not call Dynatrace or Splunk.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

ANALYSIS_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ANALYSIS_DIR / "output"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Read aggregated flow Parquets from analysis/output, write JSON "
            "traces, and compute action timing metrics."
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Directory with flow Parquets (default: {DEFAULT_OUTPUT}).",
    )
    p.add_argument(
        "--stem",
        default=None,
        help="File stem, e.g. adhoc_2026-06-09_2026-09-07 (default: newest flows_*.parquet).",
    )
    return p.parse_args(argv)


def resolve_stem(output_dir: Path, stem: str | None) -> str:
    if stem:
        return stem
    files = sorted(
        output_dir.glob("flows_*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No flows_*.parquet under {output_dir}. Run fetch_sessions with --build-flows first."
        )
    name = files[0].name
    return name[len("flows_"):-len(".parquet")]


def _f(value: Any, ndigits: int = 3) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, ndigits)


def _i(value: Any) -> int | None:
    number = _f(value, 0)
    return None if number is None else int(number)


def _iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.isoformat()


def _jsonable(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return _f(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return _iso(value)
    if hasattr(value, "tolist"):
        return [_jsonable(v) for v in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return str(value)


def _stats(series: pd.Series, *, ndigits: int = 3) -> dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return {"count": 0}
    return {
        "count": int(len(numeric)),
        "min": _f(numeric.min(), ndigits),
        "max": _f(numeric.max(), ndigits),
        "mean": _f(numeric.mean(), ndigits),
        "median": _f(numeric.median(), ndigits),
        "p90": _f(numeric.quantile(0.90), ndigits),
        "p95": _f(numeric.quantile(0.95), ndigits),
        "sum": _f(numeric.sum(), ndigits),
    }


def load_frames(output_dir: Path, stem: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    flows_path = output_dir / f"flows_{stem}.parquet"
    matched_path = output_dir / f"matched_actions_{stem}.parquet"
    breakdown_path = output_dir / f"step_breakdown_{stem}.parquet"
    missing = [str(p) for p in (flows_path, matched_path, breakdown_path) if not p.is_file()]
    if missing:
        raise FileNotFoundError("Missing Parquet files:\n" + "\n".join(missing))
    return (
        pd.read_parquet(flows_path),
        pd.read_parquet(matched_path),
        pd.read_parquet(breakdown_path),
    )


def _pagina_map(breakdown: pd.DataFrame) -> dict[tuple[str, int], str]:
    if breakdown.empty or "flusso_id" not in breakdown.columns:
        return {}
    out: dict[tuple[str, int], str] = {}
    for row in breakdown.itertuples(index=False):
        idx = _i(getattr(row, "step_index", None))
        if idx is None:
            continue
        pagina = getattr(row, "pagina", None)
        if pagina is None or pd.isna(pagina):
            continue
        out[(str(row.flusso_id), idx)] = str(pagina)
    return out


def _action_record(row: Any) -> dict[str, Any]:
    rec = {
        "start": _iso(getattr(row, "actionStartDt", None)),
        "end": _iso(getattr(row, "actionEndDt", None)),
        "duration_s": _f(getattr(row, "durationSec", None)),
        "frontend_ms": _i(getattr(row, "frontendTime", None)),
        "network_ms": _i(getattr(row, "networkTime", None)),
        "server_ms": _i(getattr(row, "serverTime", None)),
        "actionKey": getattr(row, "actionKey", None),
        "actionType": getattr(row, "actionType", None),
        "actionName": getattr(row, "actionName", None),
        "sessionId": getattr(row, "sessionId", None),
        "request_errors": _i(getattr(row, "requestErrorCount", 0)) or 0,
        "js_errors": _i(getattr(row, "javascriptErrorCount", 0)) or 0,
    }
    return {k: v for k, v in rec.items() if v is not None}


def build_traces(
        flows: pd.DataFrame,
        matched: pd.DataFrame,
        breakdown: pd.DataFrame,
) -> list[dict[str, Any]]:
    pages = _pagina_map(breakdown)
    flow_by_id = {str(r.flusso_id): r for r in flows.itertuples(index=False)}
    traces: list[dict[str, Any]] = []
    if matched.empty:
        return traces

    matched = matched.sort_values(
        ["flusso_id", "step_index", "actionStartTime"],
        kind="mergesort",
    )
    for fid, g in matched.groupby("flusso_id", sort=False):
        fid_s = str(fid)
        flow = flow_by_id.get(fid_s)
        steps_out: list[dict[str, Any]] = []
        step_frames = list(g.groupby("step_index", sort=True))
        for i, (step_index, sg) in enumerate(step_frames):
            si = int(step_index)
            start = sg["actionStartDt"].min()
            end = sg["actionEndDt"].max()
            dwell = None
            if pd.notna(start) and pd.notna(end):
                dwell = _f((pd.Timestamp(end) - pd.Timestamp(start)).total_seconds())
            wait_next = None
            if i + 1 < len(step_frames):
                nxt = step_frames[i + 1][1]
                nxt_start = nxt["actionStartDt"].min()
                if pd.notna(end) and pd.notna(nxt_start):
                    wait_next = _f(
                        (pd.Timestamp(nxt_start) - pd.Timestamp(end)).total_seconds()
                    )
            label = sg["label"].iloc[0] if "label" in sg.columns else None
            actions = [_action_record(a) for a in sg.itertuples(index=False)]
            steps_out.append({
                "step_index": si,
                "label": None if label is None or pd.isna(label) else str(label),
                "pagina": pages.get((fid_s, si)),
                "n_actions": len(actions),
                "start": _iso(start),
                "end": _iso(end),
                "dwell_s": dwell,
                "wait_to_next_step_s": wait_next,
                "duration_s": _stats(sg["durationSec"]),
                "frontend_ms": _stats(sg["frontendTime"], ndigits=1),
                "network_ms": _stats(sg["networkTime"], ndigits=1),
                "server_ms": _stats(sg["serverTime"], ndigits=1),
                "actions": actions,
            })

        rec: dict[str, Any] = {
            "flusso_id": fid_s,
            "n_matched_actions": int(len(g)),
            "action_duration_s": _stats(g["durationSec"]),
            "frontend_ms": _stats(g["frontendTime"], ndigits=1),
            "network_ms": _stats(g["networkTime"], ndigits=1),
            "server_ms": _stats(g["serverTime"], ndigits=1),
            "steps": steps_out,
        }
        if flow is not None:
            rec.update({
                "userId": getattr(flow, "userId", None),
                "sessionIds": _jsonable(getattr(flow, "sessionIds", None)),
                "sessionId": getattr(flow, "sessionId", None),
                "completed": bool(getattr(flow, "completed", False)),
                "abandoned": bool(getattr(flow, "abandoned", True)),
                "max_step": _i(getattr(flow, "max_step", None)),
                "n_steps": _i(getattr(flow, "n_steps", None)),
                "duration_s": _f(getattr(flow, "duration_s", None)),
                "active_s": _f(getattr(flow, "active_s", None)),
                "dead_time_s": _f(getattr(flow, "dead_time_s", None)),
                "categorie_beni": getattr(flow, "categorie_beni", None),
                "has_error": bool(getattr(flow, "has_error", False)),
                "request_errors": _i(getattr(flow, "request_errors", 0)) or 0,
                "js_errors": _i(getattr(flow, "js_errors", 0)) or 0,
            })
        traces.append(rec)
    return traces


def _top_keys(matched: pd.DataFrame, n: int = 15) -> list[dict[str, Any]]:
    if matched.empty or "actionKey" not in matched.columns:
        return []
    rows: list[dict[str, Any]] = []
    for key, g in matched.groupby("actionKey", sort=False):
        rows.append({
            "actionKey": str(key),
            "n": int(len(g)),
            "duration_s": _stats(g["durationSec"]),
            "frontend_ms": _stats(g["frontendTime"], ndigits=1),
            "network_ms": _stats(g["networkTime"], ndigits=1),
            "server_ms": _stats(g["serverTime"], ndigits=1),
        })
    rows.sort(key=lambda r: (r["duration_s"].get("mean") or 0), reverse=True)
    return rows[:n]


def _slowest(matched: pd.DataFrame, column: str, n: int = 20) -> list[dict[str, Any]]:
    if matched.empty or column not in matched.columns:
        return []
    ranked = matched.nlargest(n, column)
    out: list[dict[str, Any]] = []
    for row in ranked.itertuples(index=False):
        rec = {c: _jsonable(getattr(row, c, None)) for c in extra}
        rec.update(_action_record(row))
        out.append(rec)
    return out


def compute_metrics(
        flows: pd.DataFrame,
        matched: pd.DataFrame,
        breakdown: pd.DataFrame,
) -> dict[str, Any]:
    n_flussi = int(len(flows))
    completed = int(flows["completed"].sum()) if n_flussi and "completed" in flows.columns else 0
    overview = {
        "n_flussi": n_flussi,
        "completed": completed,
        "abandoned": n_flussi - completed,
        "completion_rate": _f(completed / n_flussi) if n_flussi else None,
        "n_matched_actions": int(len(matched)),
        "n_unique_action_keys": (
            int(matched["actionKey"].nunique()) if not matched.empty else 0
        ),
    }

    by_type: dict[str, Any] = {}
    if not matched.empty and "actionType" in matched.columns:
        for action_type, g in matched.groupby("actionType", sort=False):
            by_type[str(action_type)] = {
                "n": int(len(g)),
                "duration_s": _stats(g["durationSec"]),
                "frontend_ms": _stats(g["frontendTime"], ndigits=1),
                "network_ms": _stats(g["networkTime"], ndigits=1),
                "server_ms": _stats(g["serverTime"], ndigits=1),
            }

    by_step: list[dict[str, Any]] = []
    if not matched.empty:
        dwell_rows: list[dict[str, Any]] = []
        for (fid, step_index), sg in matched.groupby(["flusso_id", "step_index"], sort=True):
            start = sg["actionStartDt"].min()
            end = sg["actionEndDt"].max()
            dwell = None
            if pd.notna(start) and pd.notna(end):
                dwell = (pd.Timestamp(end) - pd.Timestamp(start)).total_seconds()
            label = sg["label"].iloc[0] if "label" in sg.columns else None
            dwell_rows.append({
                "flusso_id": str(fid),
                "step_index": int(step_index),
                "label": None if label is None or pd.isna(label) else str(label),
                "dwell_s": dwell,
            })
        dwell_df = pd.DataFrame(dwell_rows)
        for step_index, sg in matched.groupby("step_index", sort=True):
            si = int(step_index)
            label = sg["label"].iloc[0] if "label" in sg.columns else None
            step_dwell = dwell_df.loc[dwell_df["step_index"] == si, "dwell_s"]
            reached = int(sg["flusso_id"].nunique())
            by_step.append({
                "step_index": si,
                "label": None if label is None or pd.isna(label) else str(label),
                "n_flussi": reached,
                "n_actions": int(len(sg)),
                "reach_rate": _f(reached / n_flussi) if n_flussi else None,
                "dwell_s": _stats(step_dwell),
                "duration_s": _stats(sg["durationSec"]),
                "frontend_ms": _stats(sg["frontendTime"], ndigits=1),
                "network_ms": _stats(sg["networkTime"], ndigits=1),
                "server_ms": _stats(sg["serverTime"], ndigits=1),
            })

    return {
        "overview": overview,
        "flusso_timings": {
            "duration_s": _stats(flows["duration_s"]) if n_flussi else {"count": 0},
            "active_s": _stats(flows["active_s"]) if n_flussi else {"count": 0},
            "dead_time_s": _stats(flows["dead_time_s"]) if n_flussi else {"count": 0},
        },
        "action_timings": {
            "duration_s": _stats(matched["durationSec"]) if not matched.empty else {"count": 0},
            "frontend_ms": (
                _stats(matched["frontendTime"], ndigits=1)
                if not matched.empty else {"count": 0}
            ),
            "network_ms": (
                _stats(matched["networkTime"], ndigits=1)
                if not matched.empty else {"count": 0}
            ),
            "server_ms": (
                _stats(matched["serverTime"], ndigits=1)
                if not matched.empty else {"count": 0}
            ),
        },
        "by_action_type": by_type,
        "by_step": by_step,
        "top_action_keys_by_mean_duration": _top_keys(matched),
        "slowest_actions_by_duration": _slowest(matched, "durationSec"),
        "slowest_actions_by_server": _slowest(matched, "serverTime"),
        "n_step_breakdown_rows": int(len(breakdown)),
    }


def flows_to_json(flows: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in flows.itertuples(index=False):
        rec = {col: _jsonable(getattr(row, col, None)) for col in flows.columns}
        records.append(rec)
    return records


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_jsonable) + "\n",
        encoding="utf-8",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    try:
        stem = resolve_stem(output_dir, args.stem)
        flows, matched, breakdown = load_frames(output_dir, stem)
        traces = build_traces(flows, matched, breakdown)
        metrics = compute_metrics(flows, matched, breakdown)
        metrics["stem"] = stem

        paths = {
            "flows": output_dir / f"flows_{stem}.json",
            "trace": output_dir / f"trace_{stem}.json",
            "metrics": output_dir / f"action_metrics_{stem}.json",
        }
        write_json(paths["flows"], flows_to_json(flows))
        write_json(paths["trace"], traces)
        write_json(paths["metrics"], metrics)

        summary = {
            "stem": stem,
            **metrics["overview"],
            "files": {k: str(v) for k, v in paths.items()},
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"action_metrics failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
