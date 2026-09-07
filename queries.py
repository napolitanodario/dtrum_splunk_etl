"""USQL query builders for discovery and session-action fetches.

Builders return query strings only; execution is handled by DynatraceUSQLClient.
Discovery comes before session_actions in the ETL pipeline.

Discovery queries may include {start_ms} / {end_ms} placeholders. The client
substitutes them per adaptive time window so action startTime stays aligned
with the API timeframe.
"""

from typing import Iterable, Mapping

# Align with DynatraceUSQLClient.PAGE_SIZE / table API row cap.
_DEFAULT_LIMIT = 5_000


def _quote_list(values: Iterable[str]) -> str:
    """Quote string values for a USQL IN (...) list."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def _select_list(columns: Mapping[str, str]) -> str:
    """Build a SELECT list from alias -> USQL expression."""
    return ",\n  ".join(f"{expr} AS {alias}" for alias, expr in columns.items())


def discovery_query(
        name_prefixes: Iterable[str],
        limit: int = _DEFAULT_LIMIT,
) -> str:
    """Session ids for identified users with a matching action in the window.

    Returns one row per matching useraction (sessionId may repeat). Callers
    should drop_duplicates on sessionId after fetch.

    Filters on useraction.startTime via {start_ms}/{end_ms} placeholders
    (filled by DynatraceUSQLClient per window). No GROUP BY / TOP, so the
    row cap is LIMIT/pageSize (5000) instead of the 1000 aggregation limit.
    """
    clauses = " OR ".join(
        f"useraction.name LIKE '{prefix}%'" for prefix in name_prefixes
    )
    return f"""
SELECT usersession.userSessionId AS sessionId
FROM useraction
WHERE usersession.userId IS NOT NULL
  AND usersession.userId != ''
  AND (
       {clauses}
  )
  AND useraction.startTime >= {{start_ms}}
  AND useraction.startTime < {{end_ms}}
LIMIT {limit}
""".strip()


def discovery_query_test(
        name_prefixes: Iterable[str],
        limit: int = _DEFAULT_LIMIT,
) -> str:
    """Diagnostic twin of discovery_query (same shape; kept for local tests)."""
    return discovery_query(name_prefixes, limit=limit)


def session_lookup_query(
        session_ids: Iterable[str],
        limit: int = _DEFAULT_LIMIT,
) -> str:
    """Locate known sessions via ``usersession`` (one row per id).

    Use this instead of ``discovery_query`` when the session ids are already
    known. Discovery scans ``useraction`` by name prefix and cannot be used
    over multi-week ranges. This query is bounded by the IN list, so the
    adaptive client can start with a large window (days, not minutes).

    Overlap filter on start/end keeps window-shrink aligned with the API
    timeframe (sessions that start before a sub-window but end inside it).

    ``userId`` must be set on the session (same as ``discovery_query``):
    tagged at some point in the session, not necessarily on every action.
    """
    ids = _quote_list(session_ids)
    return f"""
SELECT
  userSessionId AS sessionId,
  startTime,
  endTime,
  userId,
  duration
FROM usersession
WHERE userSessionId IN ({ids})
  AND userId IS NOT NULL
  AND userId != ''
  AND startTime < {{end_ms}}
  AND endTime > {{start_ms}}
LIMIT {limit}
""".strip()


def session_actions_query(
        session_ids: Iterable[str],
        columns: Mapping[str, str],
) -> str:
    """Actions for the given sessions in the current adaptive time window.

    Filters on useraction.startTime via {start_ms}/{end_ms} so the client's
    window shrink actually reduces rows (API timeframe alone is on the
    session and would otherwise return whole-session action histories).

    LIMIT 5000 matches the table API row cap.
    """
    ids = _quote_list(session_ids)
    select_list = _select_list(columns)
    return f"""
SELECT
  {select_list}
FROM useraction
WHERE usersession.userSessionId IN ({ids})
  AND useraction.startTime >= {{start_ms}}
  AND useraction.startTime < {{end_ms}}
ORDER BY startTime ASC
LIMIT 5000
""".strip()
