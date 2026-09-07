"""Per-flusso feature rows from matched funnel actions."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from funnel.definitions import STEP_INFO

# Reaching this step index marks a completed emission (override locally if needed).
COMPLETION_STEP = 6

# (event_key, min_step_index) — pagina taken from STEP_INFO[min_step_index].
TARGET_SPECS: tuple[tuple[str, int], ...] = (
    ("salva", 6),
    ("stampa", 7),
    ("firma", 8),
)


def build_flow_features(
        matched: pd.DataFrame,
        step_breakdown: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """One row per flusso: timings, depth, errors, insured-good category flags.

    Also attaches optional target-page timing dicts ``salva`` / ``stampa`` /
    ``firma`` (schema v5) when ``max_step`` reaches the corresponding step.
    """
    if matched is None or not len(matched):
        return pd.DataFrame()
    categorie_by_fid = _categorie_by_flusso(step_breakdown)
    pagina_by_step = _pagina_by_step(step_breakdown)

    rows = []
    for fid, g in matched.groupby("flusso_id"):
        g = g.sort_values("actionStartTime") if "actionStartTime" in g.columns else g
        elapsed = (g["actionEndDt"].max() - g["actionStartDt"].min()).total_seconds()
        active = float(pd.to_numeric(g["durationSec"], errors="coerce").sum())
        max_step = int(g["step_index"].max())
        session_ids = sorted({str(s) for s in g["sessionId"].dropna().unique()})

        rec: dict[str, Any] = {
            "flusso_id": fid,
            "userId": g["userId"].iloc[0] if "userId" in g.columns else None,
            "sessionIds": session_ids,
            "sessionId": session_ids[0] if session_ids else None,
            "n_actions": len(g),
            "n_unique_actions": g["actionKey"].nunique(),
            "duration_s": round(elapsed, 2),
            "active_s": round(active, 2),
            "dead_time_s": round(elapsed - active, 2),
            "max_step": max_step,
            "n_steps": int(g["step_index"].nunique()),
            "completed": bool(max_step >= COMPLETION_STEP),
            "abandoned": bool(max_step < COMPLETION_STEP),
            "request_errors": _int_sum(g, "requestErrorCount"),
            "js_errors": _int_sum(g, "javascriptErrorCount"),
            "categorie_beni": categorie_by_fid.get(fid, "Altro"),
        }
        rec["total_errors"] = rec["request_errors"] + rec["js_errors"]
        rec["has_error"] = rec["total_errors"] > 0

        fid_pages = pagina_by_step.get(str(fid), {})
        for key, min_step in TARGET_SPECS:
            rec[key] = (
                _target_page_metrics(g, min_step, fid_pages)
                if max_step >= min_step
                else None
            )

        rows.append(rec)

    return pd.DataFrame(rows).reset_index(drop=True)


def _target_page_metrics(
        matched: pd.DataFrame,
        min_step: int,
        fid_pages: dict[int, str],
) -> Optional[dict[str, Any]]:
    """Elapsed / machine / user / F+N+S for the pagina of ``min_step``.

    Simplified formula (no 10% overhead, no overlap de-dup, no Index page-load
    exclusion). Dynatrace F/N/S are ms; results are seconds (2 decimals).
    Omits the object when the page group has no matched actions.
    """
    pagina = fid_pages.get(min_step)
    if not pagina:
        info = STEP_INFO.get(min_step)
        pagina = info[0] if info else None
    if not pagina:
        return None

    step_indexes = {
        idx for idx, (pag, _) in STEP_INFO.items() if pag == pagina
    }
    for idx, pag in fid_pages.items():
        if pag == pagina:
            step_indexes.add(idx)
    if not step_indexes:
        return None

    subset = matched[matched["step_index"].isin(step_indexes)]
    if subset.empty:
        return None

    occ = subset.drop_duplicates(
        subset=["sessionId", "actionStartTime"],
        keep="first",
    )
    starts = pd.to_numeric(occ["actionStartTime"], errors="coerce")
    ends = pd.to_numeric(occ["actionEndTime"], errors="coerce")
    if not len(occ) or starts.isna().all() or ends.isna().all():
        return None

    elapsed_s = float((ends.max() - starts.min()) / 1000.0)
    frontend_s = _ms_sum_to_seconds(occ, "frontendTime")
    network_s = _ms_sum_to_seconds(occ, "networkTime")
    server_s = _ms_sum_to_seconds(occ, "serverTime")
    machine_s = frontend_s + network_s + server_s
    user_s = max(0.0, elapsed_s - machine_s)

    return {
        "elapsedSeconds": round(elapsed_s, 2),
        "machineSeconds": round(machine_s, 2),
        "userSeconds": round(user_s, 2),
        "frontendSeconds": round(frontend_s, 2),
        "networkSeconds": round(network_s, 2),
        "serverSeconds": round(server_s, 2),
        "actionCount": int(len(occ)),
    }


def _ms_sum_to_seconds(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum()) / 1000.0


def _int_sum(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _pagina_by_step(
        step_breakdown: pd.DataFrame | None,
) -> dict[str, dict[int, str]]:
    """flusso_id -> step_index -> pagina from breakdown (falls back to STEP_INFO)."""
    out: dict[str, dict[int, str]] = {}
    if step_breakdown is None or step_breakdown.empty:
        return out
    if "flusso_id" not in step_breakdown.columns or "step_index" not in step_breakdown.columns:
        return out
    if "pagina" not in step_breakdown.columns:
        return out
    for row in step_breakdown.itertuples(index=False):
        fid = str(row.flusso_id)
        try:
            idx = int(row.step_index)
        except (TypeError, ValueError):
            continue
        pagina = getattr(row, "pagina", None)
        if pagina is None or (isinstance(pagina, float) and pd.isna(pagina)):
            continue
        pagina_s = str(pagina).strip()
        if not pagina_s:
            continue
        out.setdefault(fid, {})[idx] = pagina_s
    return out


def _categorie_by_flusso(step_breakdown: pd.DataFrame | None) -> dict[str, str]:
    if step_breakdown is None or step_breakdown.empty:
        return {}
    if "flusso_id" not in step_breakdown.columns or "categorie_beni" not in step_breakdown.columns:
        return {}
    rows = (
        step_breakdown[["flusso_id", "categorie_beni"]]
        .dropna(subset=["flusso_id", "categorie_beni"])
        .drop_duplicates(subset=["flusso_id"], keep="first")
    )
    return {str(row.flusso_id): str(row.categorie_beni) for row in rows.itertuples(index=False)}
