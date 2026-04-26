"""
recovery.py — Startup crash-recovery scanner (Phase 4).
"""

from __future__ import annotations

import logging

from .models import TaskStatus
from .redis_client import get_redis
from .task_queue import TaskQueue, TaskStore

logger = logging.getLogger(__name__)

_TASK_KEY_PATTERN = "cio:task:*"


async def recover_stranded_tasks(
    store: TaskStore,
    queue: TaskQueue,
) -> int:
    r = await get_redis()

    task_keys: list[str] = []
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor, match=_TASK_KEY_PATTERN, count=100)
        task_keys.extend(
            k for k in keys
            if k.startswith("cio:task:") and not k.startswith("cio:task:digest:")
        )
        if cursor == 0:
            break

    if not task_keys:
        logger.info("recovery: no task records found in Redis — nothing to recover")
        return 0

    logger.info(
        "recovery: found %d task record(s) in Redis — scanning for stranded tasks",
        len(task_keys),
    )

    recovered = 0
    for key in task_keys:
        raw = await r.get(key)
        if raw is None:
            continue

        from .models import Task

        try:
            task = Task.model_validate_json(raw)
        except Exception as exc:
            logger.warning(
                "recovery: failed to parse task key=%s: %s — skipping", key, exc
            )
            continue

        if task.is_terminal():
            continue

        if task.status == TaskStatus.RUNNING:
            task.transition(
                TaskStatus.INTERRUPTED,
                detail="crash-recovery: worker lost during execution",
            )
            await store.async_update(task)
            logger.warning(
                "recovery: task=%s project=%r was RUNNING — marked INTERRUPTED, re-queuing",
                task.task_id,
                task.project_name,
            )

        elif task.status == TaskStatus.INTERRUPTED:
            logger.info(
                "recovery: task=%s project=%r is INTERRUPTED — re-queuing for RESUME",
                task.task_id,
                task.project_name,
            )

        elif task.status == TaskStatus.PENDING:
            logger.info(
                "recovery: task=%s project=%r is PENDING — re-queuing",
                task.task_id,
                task.project_name,
            )

        await queue.enqueue(task)
        recovered += 1

    logger.info(
        "recovery: re-queued %d stranded task(s) out of %d total",
        recovered,
        len(task_keys),
    )
    return recovered
