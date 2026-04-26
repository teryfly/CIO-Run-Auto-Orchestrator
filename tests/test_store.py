"""
test_store.py — Unit tests for src/task_queue/store.py

Coverage targets:
- save(): persists task, registers project set, registers digest (when requirement set)
- save(): skips digest for empty requirement (CTD change)
- save(): raises ValueError on duplicate task_id
- get(): returns task or None
- async_update(): overwrites task, clears digest on terminal
- update() sync wrapper
- list_by_project(): returns all tasks for project
- find_incomplete(): returns PENDING or RUNNING task
- find_by_digest(): returns task by digest or None
- has_running_task()
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.models import Task, TaskStatus
from src.task_queue.store import TaskStore, _digest_key


@pytest.fixture
def store(fake_redis):
    s = TaskStore()
    with patch("src.task_queue.store.get_redis", return_value=AsyncMock(return_value=fake_redis)):
        # Patch get_redis to return our fake redis directly
        pass
    return s, fake_redis


# Helper to run async tests easily
def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.fixture
def patched_store(fake_redis):
    """Returns a TaskStore with get_redis patched to the fake_redis."""
    store = TaskStore()

    async def _fake_get_redis():
        return fake_redis

    return store, fake_redis, _fake_get_redis


class TestTaskStoreSave:
    def test_save_persists_task(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="do it")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.get(task.task_id))

        assert result is not None
        assert result.task_id == task.task_id

    def test_save_registers_digest_when_requirement_set(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="do it")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))

        dkey = _digest_key("proj", "do it")
        assert run(redis.get(dkey)) == task.task_id

    def test_save_skips_digest_when_requirement_empty(self, patched_store):
        """CTD change: empty requirement → no digest registered."""
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))

        dkey = _digest_key("proj", "")
        # Digest key should NOT be set
        assert run(redis.get(dkey)) is None

    def test_save_raises_on_duplicate(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            with pytest.raises(ValueError, match="already exists"):
                run(store.save(task))

    def test_save_registers_project_set(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="myproj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))

        members = run(redis.smembers("cio:project:myproj:tasks"))
        assert task.task_id in members


class TestTaskStoreGet:
    def test_get_returns_task(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            fetched = run(store.get(task.task_id))

        assert fetched.task_id == task.task_id

    def test_get_returns_none_for_missing(self, patched_store):
        store, redis, get_redis = patched_store

        with patch("src.task_queue.store.get_redis", get_redis):
            result = run(store.get("nonexistent"))

        assert result is None


class TestTaskStoreAsyncUpdate:
    def test_async_update_overwrites(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            task.transition(TaskStatus.RUNNING, detail="mode=new")
            run(store.async_update(task))
            updated = run(store.get(task.task_id))

        assert updated.status == TaskStatus.RUNNING

    def test_async_update_clears_digest_on_terminal(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            dkey = _digest_key("proj", "req")
            assert run(redis.get(dkey)) == task.task_id

            task.transition(TaskStatus.SUCCESS)
            run(store.async_update(task))
            assert run(redis.get(dkey)) is None

    def test_async_update_keeps_digest_when_not_terminal(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            task.transition(TaskStatus.RUNNING)
            run(store.async_update(task))
            dkey = _digest_key("proj", "req")
            assert run(redis.get(dkey)) == task.task_id

    def test_async_update_skips_digest_clear_when_empty_requirement(self, patched_store):
        """Empty requirement tasks never registered a digest; clearing is skipped safely."""
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            task.transition(TaskStatus.SUCCESS)
            run(store.async_update(task))  # Should not raise


class TestTaskStoreListAndFind:
    def test_find_incomplete_returns_pending(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.find_incomplete("proj"))

        assert result.task_id == task.task_id

    def test_find_incomplete_returns_none_when_all_terminal(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.SUCCESS)

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.find_incomplete("proj"))

        assert result is None

    def test_find_by_digest_returns_task(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.find_by_digest("proj", "req"))

        assert result.task_id == task.task_id

    def test_find_by_digest_returns_none_for_empty_req(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.find_by_digest("proj", ""))

        # No digest was registered, so None
        assert result is None

    def test_has_running_task_true(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.RUNNING)

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.has_running_task("proj"))

        assert result is True

    def test_has_running_task_false(self, patched_store):
        store, redis, get_redis = patched_store
        task = Task(project_name="proj", requirement="req")

        with patch("src.task_queue.store.get_redis", get_redis):
            run(store.save(task))
            result = run(store.has_running_task("proj"))

        assert result is False
