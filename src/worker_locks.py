"""
worker_locks.py — Redis distributed lock helpers for the worker (Phase 4 split).
"""

from __future__ import annotations

import logging

from .redis_client import get_redis

logger = logging.getLogger(__name__)

_LOCK_KEY = "cio:lock:{project_name}"


def _lock_key(project_name: str) -> str:
    return _LOCK_KEY.format(project_name=project_name)


class LockStolenError(RuntimeError):
    """Raised by the heartbeat when the distributed lock has been taken over."""


async def _acquire_lock(project_name: str, token: str, ttl: int) -> bool:
    r = await get_redis()
    result = await r.set(
        _lock_key(project_name),
        token,
        nx=True,
        ex=ttl,
    )
    return result is not None


async def _release_lock(project_name: str, token: str) -> None:
    lua_script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    r = await get_redis()
    await r.eval(lua_script, 1, _lock_key(project_name), token)


async def _refresh_lock(project_name: str, token: str, ttl: int) -> bool:
    lua_script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("expire", KEYS[1], ARGV[2])
    else
        return 0
    end
    """
    r = await get_redis()
    result = await r.eval(lua_script, 1, _lock_key(project_name), token, ttl)
    return bool(result)
