"""
redis_client.py — Shared async Redis connection manager.
"""

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

_client: Optional[aioredis.Redis] = None


async def init_redis(redis_url: str) -> aioredis.Redis:
    global _client
    _client = aioredis.from_url(
        redis_url,
        encoding="utf-8",
        decode_responses=True,
        max_connections=50,
    )
    await _client.ping()
    logger.info("redis_client: connected to %s", redis_url)
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
        logger.info("redis_client: connection closed")


async def get_redis() -> aioredis.Redis:
    if _client is None:
        raise RuntimeError(
            "Redis client is not initialised. Call `await init_redis(url)` first."
        )
    return _client
