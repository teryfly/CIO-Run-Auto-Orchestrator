"""
sse_utils.py — SSE helper utilities (Phase 3).
"""

from __future__ import annotations

import json
import logging

from .models import SSEPayload

logger = logging.getLogger(__name__)

TERMINAL_SSE_EVENTS: frozenset[str] = frozenset(
    {
        "workflow_complete",
        "workflow_failed",
    }
)

DONE_SENTINEL = "__DONE__"

KEEPALIVE_FRAME: dict = {"comment": "keepalive"}


def is_terminal_event(event_type: str) -> bool:
    """Return True if event_type signals that the CIO stream has ended."""
    return event_type in TERMINAL_SSE_EVENTS


def build_sse_frame(event: object) -> dict:
    """Convert a CIOEvent (or _EventProxy) into an sse-starlette yield dict."""
    raw_type = getattr(event, "event_type", None)
    event_type: str = getattr(raw_type, "value", str(raw_type)) if raw_type is not None else "info"

    payload = SSEPayload.from_cio_event(event)
    return {
        "event": event_type,
        "data": json.dumps(payload.model_dump()),
    }
