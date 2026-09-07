"""Splunk HEC ingestion for lean FlussoP1 events (sourcetype :flusso only).

Reads a calendar day from the USQL Parquet cache, reconstructs flussi via
``funnel.reconstruct_flows``, and ships HEC envelopes built from
``funnel.splunk_events.iter_flusso_events``.

    from splunk_ingest import IngestConfig, run_incremental
    cfg = IngestConfig.from_toml("splunk_ingest/prod.toml")
    run_incremental(cfg)
"""

from .config import IngestConfig
from .hec import HECClient
from .pipeline import process_day, run_backfill, run_incremental

__all__ = [
    "IngestConfig",
    "HECClient",
    "run_incremental",
    "run_backfill",
    "process_day",
]
