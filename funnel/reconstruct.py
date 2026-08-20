"""Reconstruct FlussoP1 flows from raw Dynatrace user-action rows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from funnel.aggregate import build_flow_features
from funnel.breakdown import build_breakdown
from funnel.prepare import normalize_actions
from funnel.tagging import matched_actions_frame


@dataclass
class FlowResult:
    """Outputs of funnel reconstruction for one fetch window."""

    session: pd.DataFrame
    step_breakdown: pd.DataFrame
    assignments: pd.DataFrame
    matched: pd.DataFrame
    flows: pd.DataFrame


def reconstruct_flows(raw: pd.DataFrame) -> FlowResult:
    """Normalise raw actions, rebuild flussi, and compute per-flusso metrics."""
    session = normalize_actions(raw)
    step_breakdown, assignments = build_breakdown(session)
    matched = matched_actions_frame(session, assignments)
    flows = build_flow_features(matched, step_breakdown=step_breakdown)
    return FlowResult(
        session=session,
        step_breakdown=step_breakdown,
        assignments=assignments,
        matched=matched,
        flows=flows,
    )


def load_action_chunks(
        cache_dir: Path,
        pattern: str = "actions_*.parquet",
        *,
        day: str | None = None,
) -> pd.DataFrame:
    """Load cached actions (consolidated, staging, or legacy flat layout)."""
    from cache import load_cached_actions

    _ = pattern  # kept for backward-compatible call sites; resolution is in cache.py
    return load_cached_actions(cache_dir, day=day)


def write_flow_outputs(
        result: FlowResult,
        output_dir: Path,
        stem: str,
) -> dict[str, Path]:
    """Write flows, matched actions, and step breakdown to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "flows": output_dir / f"flows_{stem}.parquet",
        "matched": output_dir / f"matched_actions_{stem}.parquet",
        "step_breakdown": output_dir / f"step_breakdown_{stem}.parquet",
    }
    result.flows.to_parquet(paths["flows"], index=False, compression="zstd")
    result.matched.to_parquet(paths["matched"], index=False, compression="zstd")
    result.step_breakdown.to_parquet(paths["step_breakdown"], index=False, compression="zstd")
    return paths
