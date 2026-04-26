"""
test_recovery.py — Unit tests for src/recovery.py

Coverage targets:
- recover_stranded_tasks: RUNNING → INTERRUPTED + re-queued
- recover_stranded_tasks: INTERRUPTED → re-queued directly
- recover_stranded_tasks: PENDING → re-queued
- recover_stranded_tasks: SUCCESS → skipped
- recover_stranded_tasks: FAILED → skipped
- recover_stranded_tasks: no tasks → returns 0
- recover_stranded_tasks: skips digest keys
- recover_stranded_tasks: handles malformed JSON gracefully
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Task, TaskStatus
from src.recovery import recover_stranded_tasks


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def make_fake_redis_with_tasks(tasks: list[Task], extra_keys: list[str] = None):
    """Build a FakeRedis-like mock pre-populated with given tasks."""
    data = {}
    for task in tasks:
        data[f"cio:task:{task.task_id}"] = task.model_dump_json()

    if extra_keys:
        for k in extra_keys:
            data[k] = "some-value"

    class MockRedis:
        async def scan(self, cursor, match=None, count=100):
            keys = list(data.keys())
            if match:
                import fnmatch
                keys = [k for k in keys if fnmatch.fnmatch(k, match)]
            return (0, keys)

        async def get(self, key):
            return data.get(key)

    return MockRedis()


class TestRecoverStrandedTasks:
    def test_running_task_becomes_interrupted_and_requeued(self):
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.RUNNING)

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks([task])

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 1
        store.async_update.assert_awaited_once()
        queue.enqueue.assert_awaited_once()
        # The task passed to async_update should be INTERRUPTED
        updated_task = store.async_update.call_args[0][0]
        assert updated_task.status == TaskStatus.INTERRUPTED

    def test_interrupted_task_requeued_directly(self):
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.INTERRUPTED)

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks([task])

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 1
        store.async_update.assert_not_awaited()
        queue.enqueue.assert_awaited_once()

    def test_pending_task_requeued(self):
        task = Task(project_name="proj", requirement="req")
        # PENDING is default

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks([task])

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 1
        queue.enqueue.assert_awaited_once()

    def test_success_task_skipped(self):
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.SUCCESS)

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks([task])

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 0
        queue.enqueue.assert_not_awaited()

    def test_failed_task_skipped(self):
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.FAILED, detail="fatal error")

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks([task])

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 0

    def test_no_tasks_returns_zero(self):
        store = MagicMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        class EmptyRedis:
            async def scan(self, cursor, match=None, count=100):
                return (0, [])

        with patch("src.recovery.get_redis", AsyncMock(return_value=EmptyRedis())):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 0

    def test_digest_keys_skipped(self):
        """Keys like cio:task:digest:xxx should not be treated as task records."""
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.RUNNING)

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks(
            [task],
            extra_keys=["cio:task:digest:abc123def456"]
        )

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        # Only the real task should be processed
        assert count == 1

    def test_malformed_json_skipped_gracefully(self):
        """Malformed task records should be logged and skipped, not crash."""
        class MalformedRedis:
            async def scan(self, cursor, match=None, count=100):
                return (0, ["cio:task:badtask"])

            async def get(self, key):
                return "not-valid-json{{{"

        store = MagicMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        with patch("src.recovery.get_redis", AsyncMock(return_value=MalformedRedis())):
            count = run(recover_stranded_tasks(store, queue))

        assert count == 0
        queue.enqueue.assert_not_awaited()

    def test_multiple_tasks_mixed_statuses(self):
        tasks = [
            Task(project_name="p1", requirement="r1"),  # PENDING
            Task(project_name="p2", requirement="r2"),  # RUNNING
            Task(project_name="p3", requirement="r3"),  # SUCCESS
            Task(project_name="p4", requirement="r4"),  # FAILED
            Task(project_name="p5", requirement="r5"),  # INTERRUPTED
        ]
        tasks[1].transition(TaskStatus.RUNNING)
        tasks[2].transition(TaskStatus.SUCCESS)
        tasks[3].transition(TaskStatus.FAILED)
        tasks[4].transition(TaskStatus.INTERRUPTED)

        store = MagicMock()
        store.async_update = AsyncMock()
        queue = MagicMock()
        queue.enqueue = AsyncMock()

        fake_redis = make_fake_redis_with_tasks(tasks)

        with patch("src.recovery.get_redis", AsyncMock(return_value=fake_redis)):
            count = run(recover_stranded_tasks(store, queue))

        # PENDING, RUNNING, INTERRUPTED → 3 re-queued
        assert count == 3
        assert queue.enqueue.await_count == 3
