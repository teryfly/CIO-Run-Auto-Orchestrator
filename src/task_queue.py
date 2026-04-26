"""
task_queue.py — In-process async task queue and task store (Phase 1).

TaskStore
─────────
An in-process dict that maps task_id → Task.
In Phase 2 this is replaced by Redis HSET / HGET operations with no
changes to the surrounding API.

TaskQueue
─────────
A thin wrapper around asyncio.Queue that enqueues Task objects and exposes
a non-blocking dequeue() with a configurable timeout.

Workers import both via a shared instance created in main.py.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .models import Task, TaskStatus

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# TaskStore — in-process persistence (Phase 1)                                 #
# --------------------------------------------------------------------------- #


class TaskStore:
    """
    Thread-safe in-memory store for Task objects.

    API is intentionally identical to the Redis-backed store that will
    replace it in Phase 2 (get / save / update / list).
    """

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}
        self._lock = asyncio.Lock()

    async def save(self, task: Task) -> None:
        """Persist a new task (raises if task_id already exists)."""
        async with self._lock:
            if task.task_id in self._tasks:
                raise ValueError(f"Task {task.task_id!r} already exists")
            self._tasks[task.task_id] = task
            logger.debug("store.save: task=%s project=%r", task.task_id, task.project_name)

    def update(self, task: Task) -> None:
        """
        Overwrite an existing task record.

        Synchronous on purpose — called from worker threads where
        async context is unavailable.  Safe because dict assignment
        in CPython is atomic for a single key.
        """
        self._tasks[task.task_id] = task
        logger.debug(
            "store.update: task=%s status=%s", task.task_id, task.status.value
        )

    async def get(self, task_id: str) -> Optional[Task]:
        """Return the Task for task_id, or None if not found."""
        async with self._lock:
            return self._tasks.get(task_id)

    async def list_by_project(self, project_name: str) -> list[Task]:
        """Return all tasks for a given project (any status)."""
        async with self._lock:
            return [t for t in self._tasks.values() if t.project_name == project_name]

    async def has_running_task(self, project_name: str) -> bool:
        """Return True if there is already a RUNNING task for the project."""
        tasks = await self.list_by_project(project_name)
        return any(t.status == TaskStatus.RUNNING for t in tasks)

    async def find_incomplete(self, project_name: str) -> Optional[Task]:
        """
        Return a PENDING or RUNNING task for the project, or None.
        Used for dedup: if one already exists, return it rather than
        creating a new one.
        """
        tasks = await self.list_by_project(project_name)
        for task in tasks:
            if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                return task
        return None


# --------------------------------------------------------------------------- #
# TaskQueue — async work queue                                                  #
# --------------------------------------------------------------------------- #


class TaskQueue:
    """
    Async FIFO queue that feeds tasks to the worker pool.

    Wraps asyncio.Queue so that in Phase 2 the implementation can be
    swapped for a Redis list (LPUSH / BRPOP) without changing callers.
    """

    def __init__(self, store: TaskStore, maxsize: int = 0) -> None:
        self._q: asyncio.Queue[Task] = asyncio.Queue(maxsize=maxsize)
        self.store = store  # Workers use this to call store.update()

    async def enqueue(self, task: Task) -> None:
        """Add a task to the queue tail."""
        await self._q.put(task)
        logger.debug("queue.enqueue: task=%s", task.task_id)

    async def dequeue(self, timeout: float = 0.5) -> Optional[Task]:
        """
        Remove and return the next task, or None if the queue is empty
        after `timeout` seconds.
        """
        try:
            task = await asyncio.wait_for(self._q.get(), timeout=timeout)
            logger.debug("queue.dequeue: task=%s", task.task_id)
            return task
        except asyncio.TimeoutError:
            return None

    def qsize(self) -> int:
        return self._q.qsize()
