"""
task_queue/queue.py — Redis-backed TaskQueue (Phase 2).
"""

from __future__ import annotations

import logging
from typing import Optional

from ..models import Task
from ..redis_client import get_redis
from .store import TaskStore

logger = logging.getLogger(__name__)

_QUEUE_KEY = "cio:queue:tasks"


class TaskQueue:
    """Redis-backed FIFO task queue."""

    def __init__(self, store: TaskStore) -> None:
        self.store = store

    async def enqueue(self, task: Task) -> None:
        """Serialise task and push it to the tail of the Redis list."""
        r = await get_redis()
        payload = task.model_dump_json()
        await r.lpush(_QUEUE_KEY, payload)
        logger.debug("queue.enqueue: task=%s", task.task_id)

    async def dequeue(self, timeout: float = 0.5) -> Optional[Task]:
        """Block up to `timeout` seconds for the next task."""
        r = await get_redis()
        result = await r.brpop(_QUEUE_KEY, timeout=int(max(1, timeout)))
        if result is None:
            return None
        _key, raw = result
        try:
            task = Task.model_validate_json(raw)
            logger.debug("queue.dequeue: task=%s", task.task_id)
            return task
        except Exception as exc:
            logger.error("queue.dequeue: failed to deserialise task: %s", exc)
            return None

    async def qsize(self) -> int:
        """Return the approximate number of items currently in the queue."""
        r = await get_redis()
        return await r.llen(_QUEUE_KEY)
