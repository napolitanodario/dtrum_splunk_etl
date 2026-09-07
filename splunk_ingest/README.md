# splunk_ingest

Ship reconstructed lean funnel events to a Splunk HTTP Event Collector.

- **Sourcetype:** `{prefix}:flusso` only (default `dtrum:funnel:flusso`)
- **Schema:** version **5** (same payload as `export_splunk_flussi.py` / JSONL exports)
- **Source:** USQL day cache (`.cache/usql/{YYYY-MM-DD}/`), not live Dynatrace fetch

## Events produced

| sourcetype | grain | notes |
|---|---|---|
| `dtrum:funnel:flusso` | one per reconstructed emission attempt | nested `steps[]` with actions; `blockStartTime` / `blockEndTime`; enrichment `browserType` / `country` / `city` / `bounce`; optional target objects `salva` / `stampa` / `firma` |

Each entry in `steps[].actions[]` includes `seq`, `sessionId`, `startTime`, `actionKey`, `actionType`, `duration`, plus Dynatrace timing breakdown in ms: `frontendTime`, `networkTime`, `serverTime` (omitted when null).

No `:action` or `:action_dim` events are emitted.

### Target performance objects (schema v5)

Precomputed in ETL (`funnel.aggregate.build_flow_features`) so dashboards can use flat fields without summing nested actions at search time.

| Object | Present when | KPI mapping (Flows Monitoring) |
|---|---|---|
| `salva` | `maxStep >= 6` | Qta / tempi fino al Salva |
| `stampa` | `maxStep >= 7` | Qta / tempi fino a Stampa Documenti |
| `firma` | `maxStep >= 8` | Qta / tempi fino a Firma |

Each object (when present) has:

| Field | Meaning |
|---|---|
| `elapsedSeconds` | Wall-clock: lastEnd − firstStart of the **pagina** group |
| `machineSeconds` | Sum of frontend + network + server (seconds) |
| `userSeconds` | `max(0, elapsed − machine)` |
| `frontendSeconds` / `networkSeconds` / `serverSeconds` | Dynatrace timing sums for the page group |
| `actionCount` | Deduped occurrences (`sessionId` + `startTime`) |

Formula is **simplified** (no 10% overhead, no overlap factor, no Index page-load exclusion). Grouping is by **pagina** (same as the C# batch), not `stepIndex <= N`.

Root `durationSeconds` / `activeSeconds` / `deadTimeSeconds` remain whole-flusso timings (including post-Salva steps) and must not be used for “fino al Salva” KPIs.

**Qta** in Splunk = count of events where the object exists (e.g. `salva.elapsedSeconds` is set). Percentiles and duration buckets are computed at search time on those scalars.

Days already shipped as schema v4 stay as-is until re-backfilled.

## Setup

```bash
cp splunk_ingest/config.example.toml splunk_ingest/prod.toml   # edit url/token/index
# or export SPLUNK_HEC_URL / SPLUNK_HEC_TOKEN / SPLUNK_HEC_INDEX ...
```

Fetch the day first with the ETL, then ingest:

```bash
python main.py --start 2026-06-22T00:00:00+02:00 --end 2026-06-23T00:00:00+02:00
python -m splunk_ingest backfill --since 2026-06-22 --until 2026-06-22 --config splunk_ingest/prod.toml --dry-run
```

## Use

```bash
# Incremental (for cron / Task Scheduler via daily_run): ships every settled day since the watermark.
python -m splunk_ingest run --config splunk_ingest/prod.toml

# Prefer the full unattended chain on Windows:
#   run_daily.bat
# or: python -m daily_run --config splunk_ingest/prod.toml
# See root README “Windows unattended daily run”.

# Historical load from cache.
python -m splunk_ingest backfill --since 2026-06-15 --until 2026-06-22 --config splunk_ingest/prod.toml

# Validate volume / shape without sending.
python -m splunk_ingest backfill --since 2026-06-22 --until 2026-06-22 --config splunk_ingest/prod.toml --dry-run
```

## Idempotency

- Unit of work: funnel calendar day (`FUNNEL_DAY_TZ`, Europe/Rome).
- A day ships only after settlement (`day end + settlement_lag_hours`).
- Ledger under `state_dir` (default `.cache/splunk_state/`): `watermark.json`, `days_ledger.parquet`.
- Deterministic `eventId` (`f:{flussoId}`): on partial re-run use `| dedup eventId` Splunk-side.
- USQL day cache retention is configured via `[cache] retention_days` (default 14) and applied by `daily_run` after a successful ingest; Splunk state is never pruned.
