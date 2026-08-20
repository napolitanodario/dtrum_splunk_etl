"""Per-flusso feature rows from matched funnel actions."""

import pandas as pd

# Reaching this step index marks a completed emission (override locally if needed).
COMPLETION_STEP = 6


def build_flow_features(
        matched: pd.DataFrame,
        step_breakdown: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """One row per flusso: timings, depth, errors, insured-good category flags."""
    if matched is None or not len(matched):
        return pd.DataFrame()
    categorie_by_fid = _categorie_by_flusso(step_breakdown)

    rows = []
    for fid, g in matched.groupby("flusso_id"):
        g = g.sort_values("actionStartTime") if "actionStartTime" in g.columns else g
        elapsed = (g["actionEndDt"].max() - g["actionStartDt"].min()).total_seconds()
        active = float(pd.to_numeric(g["durationSec"], errors="coerce").sum())
        max_step = int(g["step_index"].max())
        session_ids = sorted({str(s) for s in g["sessionId"].dropna().unique()})

        rec = {
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
        rows.append(rec)

    return pd.DataFrame(rows).reset_index(drop=True)


def _int_sum(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


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
