"""
test_event_bus.py — Unit tests for src/event_bus.py pure-Python paths
                    and additional src/worker.py coverage.

event_bus.py is 100% Redis pub/sub; we cannot test the full subscribe()
generator without a live broker.  We test:
  - _channel() helper
  - _EventProxy construction and field access
  - EventBus.publish() with a mocked Redis client
  - EventBus.close()  with a mocked Redis client

Additional worker.py lines
──────────────────────────
- heartbeat: refresh returns True (log path)
- heartbeat: refresh returns False → LockStolenError raised
- _run_locked(): LockStolenError from heartbeat propagates up
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Task, TaskMode, TaskStatus


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# --------------------------------------------------------------------------- #
# event_bus internals                                                           #
# --------------------------------------------------------------------------- #

class TestEventBusChannel:
    def test_channel_prefix(self):
        from src.event_bus import _channel
        assert _channel("abc123") == "cio:events:abc123"

    def test_channel_unique_per_task(self):
        from src.event_bus import _channel
        assert _channel("task-1") != _channel("task-2")


class TestEventProxy:
    def test_fields_accessible(self):
        from src.event_bus import _EventProxy
        ep = _EventProxy(
            event_type="step_start",
            message="hello",
            metadata={"k": "v"},
            timestamp="2025-01-01T00:00:00Z",
        )
        assert ep.event_type == "step_start"
        assert ep.message == "hello"
        assert ep.metadata == {"k": "v"}
        assert ep.timestamp == "2025-01-01T00:00:00Z"

    def test_slots_defined(self):
        from src.event_bus import _EventProxy
        assert hasattr(_EventProxy, "__slots__")


class TestEventBusPublish:
    def test_publish_calls_redis_publish(self):
        from src.event_bus import EventBus

        fake_r = AsyncMock()
        fake_r.publish = AsyncMock(return_value=1)

        class FakeEvent:
            event_type = "info"
            message = "test message"
            metadata = {"x": 1}
            timestamp = "ts"

        bus = EventBus()
        with patch("src.event_bus.get_redis", AsyncMock(return_value=fake_r)):
            run(bus.publish("task-123", FakeEvent()))

        fake_r.publish.assert_awaited_once()
        channel_arg, payload_arg = fake_r.publish.call_args[0]
        assert channel_arg == "cio:events:task-123"
        import json
        data = json.loads(payload_arg)
        assert data["message"] == "test message"
        assert data["event_type"] == "info"

    def test_publish_handles_enum_event_type(self):
        """event_type with .value (real CIOEvent enum) must be serialised correctly."""
        from src.event_bus import EventBus

        fake_r = AsyncMock()
        fake_r.publish = AsyncMock(return_value=1)

        class FakeEnumType:
            value = "agent_send"

        class FakeEvent:
            event_type = FakeEnumType()
            message = "sending"
            metadata = {}
            timestamp = ""

        bus = EventBus()
        with patch("src.event_bus.get_redis", AsyncMock(return_value=fake_r)):
            run(bus.publish("task-xyz", FakeEvent()))

        _, payload_arg = fake_r.publish.call_args[0]
        import json
        data = json.loads(payload_arg)
        assert data["event_type"] == "agent_send"

    def test_publish_none_metadata_becomes_empty_dict(self):
        from src.event_bus import EventBus

        fake_r = AsyncMock()
        fake_r.publish = AsyncMock(return_value=1)

        class FakeEvent:
            event_type = "warn"
            message = "careful"
            metadata = None
            timestamp = ""

        bus = EventBus()
        with patch("src.event_bus.get_redis", AsyncMock(return_value=fake_r)):
            run(bus.publish("task-abc", FakeEvent()))

        _, payload_arg = fake_r.publish.call_args[0]
        import json
        data = json.loads(payload_arg)
        assert data["metadata"] == {}


class TestEventBusClose:
    def test_close_publishes_done_sentinel(self):
        from src.event_bus import EventBus, _DONE_MESSAGE

        fake_r = AsyncMock()
        fake_r.publish = AsyncMock(return_value=1)

        bus = EventBus()
        with patch("src.event_bus.get_redis", AsyncMock(return_value=fake_r)):
            run(bus.close("task-999"))

        fake_r.publish.assert_awaited_once()
        channel_arg, message_arg = fake_r.publish.call_args[0]
        assert channel_arg == "cio:events:task-999"
        assert message_arg == _DONE_MESSAGE


# --------------------------------------------------------------------------- #
# worker.py — heartbeat paths                                                   #
# --------------------------------------------------------------------------- #

class TestWorkerHeartbeat:
    def _make_worker(self, bus=None, queue=None):
        from src.worker import Worker

        engine = MagicMock()
        scheduler = MagicMock()
        bus = bus or AsyncMock()
        queue = queue or MagicMock()
        queue.store = MagicMock()

        return Worker(
            engine=engine,
            scheduler=scheduler,
            event_bus=bus,
            task_queue=queue,
            max_retries=3,
            poll_interval=0.1,
        )

    def test_heartbeat_raises_lock_stolen_when_refresh_fails(self):
        from src.worker import Worker
        from src.worker_locks import LockStolenError

        w = self._make_worker()

        async def _test():
            with patch("src.worker._refresh_lock", AsyncMock(return_value=False)):
                with patch("src.worker.settings") as mock_settings:
                    mock_settings.lock_heartbeat_interval = 0.01
                    mock_settings.lock_ttl = 60
                    with pytest.raises(LockStolenError):
                        await w._heartbeat("proj", "tok")

        run(_test())

    def test_heartbeat_logs_refresh_success(self):
        """When refresh succeeds, heartbeat continues without raising."""
        from src.worker import Worker

        w = self._make_worker()
        call_count = 0

        async def _test():
            nonlocal call_count

            async def _refresh_ok(*a, **kw):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    raise asyncio.CancelledError("stop test")
                return True

            with patch("src.worker._refresh_lock", _refresh_ok):
                with patch("src.worker.settings") as mock_settings:
                    mock_settings.lock_heartbeat_interval = 0.01
                    mock_settings.lock_ttl = 60
                    with pytest.raises(asyncio.CancelledError):
                        await w._heartbeat("proj", "tok")

        run(_test())
        assert call_count >= 1

    def test_run_locked_lock_stolen_propagates(self):
        """LockStolenError from inside _run_locked should re-raise."""
        from src.worker import Worker
        from src.worker_locks import LockStolenError

        mock_bus = AsyncMock()
        mock_queue = MagicMock()
        mock_queue.store = MagicMock()

        mock_scheduler = MagicMock()
        mock_scheduler.decide.return_value = TaskMode.NEW

        task = Task(project_name="proj", requirement="req")

        w = Worker(
            engine=MagicMock(),
            scheduler=mock_scheduler,
            event_bus=mock_bus,
            task_queue=mock_queue,
            max_retries=3,
            poll_interval=0.1,
        )

        async def _stream_raises(t, mode):
            raise LockStolenError("stolen mid-stream")

        w._stream = _stream_raises

        with pytest.raises(LockStolenError):
            run(w._run_locked(task, "token"))
