"""
test_sse_utils.py — Unit tests for src/sse_utils.py, src/worker_locks.py,
                     src/task_queue/queue.py, and src/redis_client.py
                     (all tested through mocked Redis / in-process stubs).

This file is focused on closing the coverage gap identified after the main
test run (total was 71%; target is ≥ 80%).

Modules targeted
────────────────
- sse_utils.py         : build_sse_frame, is_terminal_event, constants
- worker_locks.py      : _acquire_lock, _release_lock, _refresh_lock, LockStolenError
- task_queue/queue.py  : enqueue, dequeue (item present + empty), qsize
- redis_client.py      : get_redis (uninitialised → RuntimeError), init path mock
- api.py (extra)       : stream_task already-terminal path
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import SSEPayload, Task, TaskMode, TaskStatus
from src.sse_utils import (
    DONE_SENTINEL,
    KEEPALIVE_FRAME,
    TERMINAL_SSE_EVENTS,
    build_sse_frame,
    is_terminal_event,
)


# --------------------------------------------------------------------------- #
# sse_utils                                                                     #
# --------------------------------------------------------------------------- #

class TestIsTerminalEvent:
    def test_workflow_complete_is_terminal(self):
        assert is_terminal_event("workflow_complete") is True

    def test_workflow_failed_is_terminal(self):
        assert is_terminal_event("workflow_failed") is True

    def test_step_start_is_not_terminal(self):
        assert is_terminal_event("step_start") is False

    def test_info_is_not_terminal(self):
        assert is_terminal_event("info") is False

    def test_empty_string_is_not_terminal(self):
        assert is_terminal_event("") is False

    def test_terminal_events_set_contents(self):
        assert "workflow_complete" in TERMINAL_SSE_EVENTS
        assert "workflow_failed" in TERMINAL_SSE_EVENTS
        assert "step_start" not in TERMINAL_SSE_EVENTS


class TestBuildSseFrame:
    def test_frame_has_event_and_data_keys(self):
        class FakeEvent:
            event_type = "step_start"
            message = "Starting"
            metadata = {}
            timestamp = "2025-01-01T00:00:00Z"

        frame = build_sse_frame(FakeEvent())
        assert "event" in frame
        assert "data" in frame

    def test_frame_event_equals_event_type(self):
        class FakeEvent:
            event_type = "agent_recv"
            message = "received"
            metadata = {}
            timestamp = ""

        frame = build_sse_frame(FakeEvent())
        assert frame["event"] == "agent_recv"

    def test_frame_data_is_json_encoded_payload(self):
        class FakeEvent:
            event_type = "info"
            message = "hello"
            metadata = {"k": "v"}
            timestamp = "ts"

        frame = build_sse_frame(FakeEvent())
        payload = json.loads(frame["data"])
        assert payload["message"] == "hello"
        assert payload["metadata"] == {"k": "v"}
        assert payload["timestamp"] == "ts"

    def test_frame_handles_enum_event_type(self):
        """event_type with a .value attribute (real CIOEvent enum)."""
        class FakeEnumType:
            value = "step_complete"

        class FakeEvent:
            event_type = FakeEnumType()
            message = "done"
            metadata = {}
            timestamp = ""

        frame = build_sse_frame(FakeEvent())
        assert frame["event"] == "step_complete"

    def test_frame_handles_missing_event_type(self):
        """Falls back to 'info' when event_type is absent."""
        frame = build_sse_frame(object())
        assert frame["event"] == "info"

    def test_keepalive_frame_is_comment(self):
        assert KEEPALIVE_FRAME == {"comment": "keepalive"}

    def test_done_sentinel_value(self):
        assert DONE_SENTINEL == "__DONE__"


# --------------------------------------------------------------------------- #
# worker_locks                                                                  #
# --------------------------------------------------------------------------- #

def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestWorkerLocks:
    def _make_redis(self, set_result=True, eval_result=1):
        r = AsyncMock()
        r.set = AsyncMock(return_value=set_result)
        r.eval = AsyncMock(return_value=eval_result)
        return r

    def test_acquire_lock_success(self):
        from src.worker_locks import _acquire_lock

        fake_r = self._make_redis(set_result=True)
        with patch("src.worker_locks.get_redis", AsyncMock(return_value=fake_r)):
            result = run(_acquire_lock("proj", "tok", ttl=60))
        assert result is True

    def test_acquire_lock_already_held(self):
        from src.worker_locks import _acquire_lock

        fake_r = self._make_redis(set_result=None)  # SET NX returns None when key exists
        with patch("src.worker_locks.get_redis", AsyncMock(return_value=fake_r)):
            result = run(_acquire_lock("proj", "tok", ttl=60))
        assert result is False

    def test_release_lock_calls_eval(self):
        from src.worker_locks import _release_lock

        fake_r = self._make_redis(eval_result=1)
        with patch("src.worker_locks.get_redis", AsyncMock(return_value=fake_r)):
            run(_release_lock("proj", "tok"))
        fake_r.eval.assert_awaited_once()

    def test_refresh_lock_held_returns_true(self):
        from src.worker_locks import _refresh_lock

        fake_r = self._make_redis(eval_result=1)
        with patch("src.worker_locks.get_redis", AsyncMock(return_value=fake_r)):
            result = run(_refresh_lock("proj", "tok", ttl=60))
        assert result is True

    def test_refresh_lock_stolen_returns_false(self):
        from src.worker_locks import _refresh_lock

        fake_r = self._make_redis(eval_result=0)
        with patch("src.worker_locks.get_redis", AsyncMock(return_value=fake_r)):
            result = run(_refresh_lock("proj", "tok", ttl=60))
        assert result is False

    def test_lock_stolen_error_is_runtime_error(self):
        from src.worker_locks import LockStolenError

        err = LockStolenError("stolen")
        assert isinstance(err, RuntimeError)
        assert "stolen" in str(err)


# --------------------------------------------------------------------------- #
# task_queue/queue.py                                                           #
# --------------------------------------------------------------------------- #

class TestTaskQueue:
    def _make_redis(self, brpop_result=None):
        r = AsyncMock()
        r.lpush = AsyncMock(return_value=1)
        r.brpop = AsyncMock(return_value=brpop_result)
        r.llen = AsyncMock(return_value=3)
        return r

    def test_enqueue_calls_lpush(self):
        from src.task_queue.queue import TaskQueue
        from src.task_queue.store import TaskStore

        task = Task(project_name="proj", requirement="req")
        store = MagicMock(spec=TaskStore)
        q = TaskQueue(store=store)

        fake_r = self._make_redis()
        with patch("src.task_queue.queue.get_redis", AsyncMock(return_value=fake_r)):
            run(q.enqueue(task))

        fake_r.lpush.assert_awaited_once()
        args = fake_r.lpush.call_args[0]
        assert args[0] == "cio:queue:tasks"
        assert task.task_id in args[1]  # JSON payload contains task_id

    def test_dequeue_returns_task_when_present(self):
        from src.task_queue.queue import TaskQueue

        task = Task(project_name="proj", requirement="req")
        payload = task.model_dump_json()
        fake_r = self._make_redis(brpop_result=("cio:queue:tasks", payload))

        q = TaskQueue(store=MagicMock())
        with patch("src.task_queue.queue.get_redis", AsyncMock(return_value=fake_r)):
            result = run(q.dequeue(timeout=0.1))

        assert result is not None
        assert result.task_id == task.task_id

    def test_dequeue_returns_none_when_empty(self):
        from src.task_queue.queue import TaskQueue

        fake_r = self._make_redis(brpop_result=None)
        q = TaskQueue(store=MagicMock())
        with patch("src.task_queue.queue.get_redis", AsyncMock(return_value=fake_r)):
            result = run(q.dequeue(timeout=0.1))

        assert result is None

    def test_dequeue_handles_bad_json(self):
        from src.task_queue.queue import TaskQueue

        fake_r = self._make_redis(brpop_result=("cio:queue:tasks", "not-json{{"))
        q = TaskQueue(store=MagicMock())
        with patch("src.task_queue.queue.get_redis", AsyncMock(return_value=fake_r)):
            result = run(q.dequeue(timeout=0.1))

        assert result is None

    def test_qsize_returns_count(self):
        from src.task_queue.queue import TaskQueue

        fake_r = self._make_redis()
        q = TaskQueue(store=MagicMock())
        with patch("src.task_queue.queue.get_redis", AsyncMock(return_value=fake_r)):
            size = run(q.qsize())

        assert size == 3


# --------------------------------------------------------------------------- #
# redis_client.py                                                              #
# --------------------------------------------------------------------------- #

class TestRedisClient:
    def test_get_redis_raises_before_init(self):
        """get_redis() must raise RuntimeError when init_redis has not been called."""
        import src.redis_client as rc

        # Temporarily clear the module-level client
        original = rc._client
        rc._client = None
        try:
            with pytest.raises(RuntimeError, match="not initialised"):
                run(rc.get_redis())
        finally:
            rc._client = original

    def test_close_redis_when_none_is_noop(self):
        """close_redis() with no client should not raise."""
        import src.redis_client as rc

        original = rc._client
        rc._client = None
        try:
            run(rc.close_redis())  # should not raise
        finally:
            rc._client = original


# --------------------------------------------------------------------------- #
# api.py — stream_task already-terminal path                                   #
# --------------------------------------------------------------------------- #

class TestApiStreamAlreadyTerminal:
    def test_stream_returns_empty_for_terminal_task(self):
        """GET /tasks/{id}/stream on a SUCCESS task returns 200 with empty body."""
        from unittest.mock import AsyncMock, MagicMock
        from fastapi.testclient import TestClient
        from src.api import app

        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.SUCCESS)

        store = MagicMock()
        store.get = AsyncMock(return_value=task)

        app.state.task_store = store
        app.state.task_queue = MagicMock()
        app.state.event_bus = MagicMock()

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get(f"/tasks/{task.task_id}/stream")
        assert resp.status_code == 200

    def test_stream_returns_404_for_missing_task(self):
        from fastapi.testclient import TestClient
        from src.api import app

        store = MagicMock()
        store.get = AsyncMock(return_value=None)

        app.state.task_store = store
        app.state.task_queue = MagicMock()
        app.state.event_bus = MagicMock()

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/tasks/nonexistent/stream")
        assert resp.status_code == 404
