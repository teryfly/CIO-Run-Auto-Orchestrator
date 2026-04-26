"""
conftest.py — Shared pytest fixtures.

Provides:
- mock_redis: in-memory fake redis for store/queue tests
- sample_task: a Task with a non-empty requirement
- resume_task: a Task with empty requirement (pure-resume scenario)
- mock_engine: MagicMock WorkflowEngine
- mock_scheduler: MagicMock Scheduler
- mock_bus: MagicMock EventBus
- mock_queue: MagicMock TaskQueue with a real store
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Task, TaskMode, TaskStatus


# --------------------------------------------------------------------------- #
# In-memory fake Redis                                                          #
# --------------------------------------------------------------------------- #


class FakeRedis:
    """Minimal in-memory Redis stub covering the methods used by TaskStore."""

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._sets: Dict[str, set] = {}

    async def set(self, key: str, value: str, nx: bool = False, ex: int = None) -> Optional[bool]:
        if nx and key in self._data:
            return None
        self._data[key] = value
        return True

    async def get(self, key: str) -> Optional[str]:
        return self._data.get(key)

    async def delete(self, key: str) -> int:
        return int(self._data.pop(key, None) is not None)

    async def sadd(self, key: str, *values) -> int:
        if key not in self._sets:
            self._sets[key] = set()
        before = len(self._sets[key])
        self._sets[key].update(values)
        return len(self._sets[key]) - before

    async def smembers(self, key: str) -> set:
        return self._sets.get(key, set())

    def pipeline(self):
        return FakePipeline(self)

    async def lpush(self, key: str, value: str) -> int:
        if key not in self._sets:
            self._sets[key] = []
        self._sets[key].insert(0, value)
        return len(self._sets[key])

    async def brpop(self, key: str, timeout: int = 0) -> Optional[tuple]:
        lst = self._sets.get(key, [])
        if lst:
            return (key, lst.pop())
        return None

    async def llen(self, key: str) -> int:
        return len(self._sets.get(key, []))

    async def publish(self, channel: str, message: str) -> int:
        return 0

    async def ping(self) -> bool:
        return True

    async def scan(self, cursor: int, match: str = None, count: int = 100):
        keys = list(self._data.keys())
        if match:
            import fnmatch
            keys = [k for k in keys if fnmatch.fnmatch(k, match)]
        return (0, keys)

    async def eval(self, script: str, numkeys: int, *args) -> Any:
        # Simple compare-and-delete / compare-and-expire
        key = args[0]
        token = args[1]
        if self._data.get(key) == token:
            if "del" in script:
                del self._data[key]
                return 1
            elif "expire" in script:
                return 1
        return 0

    async def expire(self, key: str, seconds: int) -> int:
        return 1 if key in self._data else 0

    async def aclose(self) -> None:
        pass


class FakePipeline:
    def __init__(self, redis: FakeRedis):
        self._redis = redis
        self._cmds = []

    def get(self, key: str):
        self._cmds.append(("get", key))
        return self

    async def execute(self):
        results = []
        for cmd, key in self._cmds:
            if cmd == "get":
                results.append(self._redis._data.get(key))
        return results


# --------------------------------------------------------------------------- #
# Fixtures                                                                      #
# --------------------------------------------------------------------------- #


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def sample_task():
    return Task(project_name="test-project", requirement="Build a REST API")


@pytest.fixture
def resume_task():
    """Task with empty requirement → pure RESUME scenario."""
    return Task(project_name="existing-project", requirement="")


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.run_stream.return_value = iter([])
    engine.run_secondary_stream.return_value = iter([])
    engine.resume_stream.return_value = iter([])
    return engine


@pytest.fixture
def mock_scheduler():
    scheduler = MagicMock()
    scheduler.decide.return_value = TaskMode.NEW
    return scheduler


@pytest.fixture
def mock_bus():
    bus = AsyncMock()
    bus.publish = AsyncMock()
    bus.close = AsyncMock()
    return bus


@pytest.fixture
def mock_store():
    store = MagicMock()
    store.update = MagicMock()
    store.save = AsyncMock()
    store.get = AsyncMock(return_value=None)
    store.find_incomplete = AsyncMock(return_value=None)
    store.find_by_digest = AsyncMock(return_value=None)
    store.async_update = AsyncMock()
    return store


@pytest.fixture
def mock_queue(mock_store):
    queue = MagicMock()
    queue.store = mock_store
    queue.enqueue = AsyncMock()
    queue.dequeue = AsyncMock(return_value=None)
    return queue
