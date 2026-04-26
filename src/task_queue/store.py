"""
task_queue/store.py — Redis-backed TaskStore (Phase 2).

Key layout
──────────
cio:task:{task_id}             STRING — full serialised Task JSON
cio:project:{name}:tasks       SET    — task_ids belonging to a project
cio:task:digest:{hash}         STRING — task_id of the active dedup record

Bug fixes
─────────
- Bug 1: update() fixed to use asyncio.get_running_loop()
- Bug 3: _async_update() promoted to public async_update()

v0.5 note (auto_run_stream 接口简化)
─────────────────────────────────────
The `if task.requirement:` guards in save() and async_update() correctly skip
digest operations when requirement is empty (""). No code change required.

v0.6 note (dynamic engine config)
───────────────────────────────────
Task now carries work_dir and config_json fields.  Because the store serialises
the full Task via model_dump_json() / model_validate_json(), these new fields
are persisted and restored automatically — no changes to store logic required.

The find_by_digest() dedup key intentionally does NOT incorporate work_dir or
config_json: two requests for the same project+requirement but different engine
configs still deduplicate to the same task.  The first caller's config wins.
If distinct configs require separate executions, callers must use distinct
project_names.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Optional

from ..models import Task, TaskStatus
from ..redis_client import get_redis

logger = logging.getLogger(__name__)

# Redis key helpers
_TASK_KEY = "cio:task:{task_id}"
_PROJECT_TASKS_KEY = "cio:project:{project_name}:tasks"
_DIGEST_KEY = "cio:task:digest:{digest}"


def _task_key(task_id: str) -> str:
    return _TASK_KEY.format(task_id=task_id)


def _project_tasks_key(project_name: str) -> str:
    return _PROJECT_TASKS_KEY.format(project_name=project_name)


def _digest_key(project_name: str, requirement: str) -> str:
    raw = f"{project_name}\x00{requirement}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return _DIGEST_KEY.format(digest=digest)


class TaskStore:
    """Redis-backed task store."""

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    async def save(self, task: Task) -> None:
        """
        Persist a new task.

        Registers the task in the project's task set and (if requirement
        is non-empty) registers a dedup digest key.

        Raises ValueError if task_id already exists.
        """
        r = await get_redis()
        key = _task_key(task.task_id)

        payload = task.model_dump_json()
        set_result = await r.set(key, payload, nx=True)
        if not set_result:
            raise ValueError(f"Task {task.task_id!r} already exists")

        await r.sadd(_project_tasks_key(task.project_name), task.task_id)

        # Skip digest registration for empty requirement (pure resume tasks)
        if task.requirement:
            dkey = _digest_key(task.project_name, task.requirement)
            await r.set(dkey, task.task_id)

        logger.debug(
            "store.save: task=%s project=%r work_dir=%r",
            task.task_id,
            task.project_name,
            task.work_dir or "(env default)",
        )

    def update(self, task: Task) -> None:
        """
        Overwrite an existing task record synchronously.

        Bug 1 fix: uses asyncio.get_running_loop() instead of the
        deprecated asyncio.get_event_loop().
        """
        loop = asyncio.get_running_loop()
        future = asyncio.run_coroutine_threadsafe(self.async_update(task), loop)
        future.result(timeout=5)

    async def async_update(self, task: Task) -> None:
        """
        Async implementation of update() — public for use by recovery.py.

        Bug 3 fix: promoted from private _async_update to public async_update.
        """
        r = await get_redis()
        payload = task.model_dump_json()
        await r.set(_task_key(task.task_id), payload)

        # Clear digest key when terminal (skipped for empty requirement)
        if task.is_terminal() and task.requirement:
            dkey = _digest_key(task.project_name, task.requirement)
            await r.delete(dkey)

        logger.debug(
            "store.update: task=%s status=%s", task.task_id, task.status.value
        )

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    async def get(self, task_id: str) -> Optional[Task]:
        """Return the Task for task_id, or None if not found."""
        r = await get_redis()
        raw = await r.get(_task_key(task_id))
        if raw is None:
            return None
        return Task.model_validate_json(raw)

    async def list_by_project(self, project_name: str) -> list[Task]:
        """Return all tasks for a given project (any status)."""
        r = await get_redis()
        task_ids = await r.smembers(_project_tasks_key(project_name))
        if not task_ids:
            return []

        pipe = r.pipeline()
        for tid in task_ids:
            pipe.get(_task_key(tid))
        raws = await pipe.execute()

        tasks = []
        for raw in raws:
            if raw is not None:
                try:
                    tasks.append(Task.model_validate_json(raw))
                except Exception as exc:
                    logger.warning(
                        "store.list_by_project: failed to parse task: %s", exc
                    )
        return tasks

    async def has_running_task(self, project_name: str) -> bool:
        """Return True if there is already a RUNNING task for the project."""
        tasks = await self.list_by_project(project_name)
        return any(t.status == TaskStatus.RUNNING for t in tasks)

    async def find_incomplete(self, project_name: str) -> Optional[Task]:
        """Return a PENDING or RUNNING task for the project, or None."""
        tasks = await self.list_by_project(project_name)
        for task in tasks:
            if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                return task
        return None

    async def find_by_digest(
        self, project_name: str, requirement: str
    ) -> Optional[Task]:
        """Look up a task by its dedup digest (project_name + requirement)."""
        r = await get_redis()
        dkey = _digest_key(project_name, requirement)
        task_id = await r.get(dkey)
        if task_id is None:
            return None
        return await self.get(task_id)
