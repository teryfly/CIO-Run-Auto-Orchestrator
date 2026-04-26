"""
event_bus.py — Redis Pub/Sub fan-out bus (Phase 2, hardened in Phase 3).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis

from .redis_client import get_redis

logger = logging.getLogger(__name__)

_CHANNEL_PREFIX = "cio:events:"
_DONE_MESSAGE = "__DONE__"
_POLL_TIMEOUT: float = 0.1


def _channel(task_id: str) -> str:
    return f"{_CHANNEL_PREFIX}{task_id}"


class _EventProxy:
    __slots__ = ("event_type", "message", "metadata", "timestamp")

    def __init__(self, event_type, message, metadata, timestamp):
        self.event_type = event_type
        self.message = message
        self.metadata = metadata
        self.timestamp = timestamp


class EventBus:
    async def publish(self, task_id: str, event: object) -> None:
        r = await get_redis()
        payload = json.dumps(
            {
                "event_type": getattr(
                    getattr(event, "event_type", None),
                    "value",
                    str(getattr(event, "event_type", "info")),
                ),
                "message": getattr(event, "message", ""),
                "metadata": getattr(event, "metadata", {}) or {},
                "timestamp": getattr(event, "timestamp", ""),
            }
        )
        await r.publish(_channel(task_id), payload)

    async def close(self, task_id: str) -> None:
        r = await get_redis()
        await r.publish(_channel(task_id), _DONE_MESSAGE)

    async def subscribe(
        self,
        task_id: str,
        *,
        maxsize: int = 256,
        keepalive_interval: float = 15.0,
    ) -> AsyncGenerator[Optional[object], None]:
        r = await get_redis()
        pubsub: aioredis.client.PubSub = r.pubsub()
        channel = _channel(task_id)
        await pubsub.subscribe(channel)

        loop = asyncio.get_running_loop()
        last_yield_time: float = loop.time()

        try:
            while True:
                try:
                    msg = await asyncio.wait_for(
                        pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=_POLL_TIMEOUT
                        ),
                        timeout=_POLL_TIMEOUT + 0.05,
                    )
                except asyncio.TimeoutError:
                    msg = None

                now = loop.time()

                if msg is None:
                    if now - last_yield_time >= keepalive_interval:
                        last_yield_time = now
                        yield None
                    else:
                        await asyncio.sleep(0)
                    continue

                if msg.get("type") != "message":
                    continue

                data: str = msg.get("data", "")

                if data == _DONE_MESSAGE:
                    return

                try:
                    raw = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    continue

                last_yield_time = now
                yield _EventProxy(
                    event_type=raw.get("event_type", "info"),
                    message=raw.get("message", ""),
                    metadata=raw.get("metadata", {}),
                    timestamp=raw.get("timestamp", ""),
                )

        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception:
                pass
