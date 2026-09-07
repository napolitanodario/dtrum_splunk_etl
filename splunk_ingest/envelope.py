"""Wrap lean flusso v2 event bodies into Splunk HEC envelopes."""

from __future__ import annotations

import copy
import hashlib
from typing import Any, Iterable, Iterator, Optional


def _hash_user_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def wrap_flusso_event(event: dict[str, Any], cfg) -> dict[str, Any]:
    """Build one HEC envelope around a flusso event body.

    ``event`` is the payload produced by ``funnel.splunk_events.iter_flusso_events``
    (already includes ``version`` and ``eventId``).
    """
    body = copy.deepcopy(event)
    if getattr(cfg, "hash_user_id", False) and body.get("userId"):
        body["userId"] = _hash_user_id(str(body["userId"]))

    env: dict[str, Any] = {
        "host": cfg.host,
        "source": cfg.source,
        "sourcetype": cfg.sourcetype("flusso"),
        "index": cfg.index,
        "event": body,
    }
    start_ms = body.get("blockStartTime")
    if start_ms is not None:
        env["time"] = round(int(start_ms) / 1000.0, 3)
    return env


def wrap_flusso_events(events: Iterable[dict[str, Any]], cfg) -> Iterator[dict[str, Any]]:
    for ev in events:
        yield wrap_flusso_event(ev, cfg)
