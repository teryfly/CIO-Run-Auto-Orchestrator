"""
worker.py — Async worker that executes a single Task via CIO-Agent stream APIs.

v0.6.0 changes
──────────────
- Worker no longer holds a fixed WorkflowEngine + Scheduler.
  It receives an EngineFactory and calls factory.get(work_dir, config_json)
  at execution time to obtain the correct (engine, scheduler) pair for
  each task.  Identical configurations are served from the factory cache,
  so the cost is only paid once per unique (work_dir, config) combination.

All other behaviour (lock heartbeat, retry logic, INTERRUPTED→RESUME routing,
crash-recovery integration, stream timeout) is unchanged from v0.5.0.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING

from cio.errors import FatalError, RetriableError
from cio.workflow_engine import WorkflowEngine

from .config import settings
from .engine_factory import EngineFactory
from .models import Task, TaskMode, TaskStatus
from .worker_locks import (
    LockStolenError,
    _acquire_lock,
    _refresh_lock,
    _release_lock,
)

if TYPE_CHECKING:
    from .event_bus import EventBus
    from .task_queue import TaskQueue

logger = logging.getLogger(__name__)


class Worker:
    """
    Async worker.  One Worker instance = one concurrent execution slot.

    Workers share the same EngineFactory, EventBus, and TaskQueue.
    Each uses a unique token per lock acquisition.
    """

    def __init__(
        self,
        engine_factory: EngineFactory,
        event_bus: "EventBus",
        task_queue: "TaskQueue",
        max_retries: int = 3,
        poll_interval: float = 0.5,
    ) -> None:
        self._factory = engine_factory
        self._bus = event_bus
        self._queue = task_queue
        self._max_retries = max_retries
        self._poll_interval = poll_interval
        self._running = False

    # ------------------------------------------------------------------ #
    # Lifecycle                                                             #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        self._running = True
        while self._running:
            task = await self._queue.dequeue(timeout=self._poll_interval)
            if task is None:
                continue
            await self._execute(task)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------ #
    # Task execution                                                        #
    # ------------------------------------------------------------------ #

    async def _execute(self, task: Task) -> None:
        lock_token = uuid.uuid4().hex

        acquired = await _acquire_lock(
            task.project_name, lock_token, ttl=settings.lock_ttl
        )
        if not acquired:
            logger.warning(
                "worker: project=%r is locked by another worker — re-queuing task=%s",
                task.project_name,
                task.task_id,
            )
            await self._queue.enqueue(task)
            await asyncio.sleep(self._poll_interval)
            return

        heartbeat_task = asyncio.create_task(
            self._heartbeat(task.project_name, lock_token),
            name=f"heartbeat-{task.task_id}",
        )

        try:
            await self._run_locked(task, lock_token)
        except LockStolenError:
            logger.error(
                "worker: lock stolen for project=%r task=%s — re-queuing",
                task.project_name,
                task.task_id,
            )
            await self._handle_retriable(
                task, RetriableError("distributed lock was stolen during execution")
            )
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            await _release_lock(task.project_name, lock_token)

    async def _heartbeat(self, project_name: str, token: str) -> None:
        interval = settings.lock_heartbeat_interval
        while True:
            await asyncio.sleep(interval)
            still_held = await _refresh_lock(project_name, token, settings.lock_ttl)
            if still_held:
                logger.debug(
                    "worker.heartbeat: refreshed lock for project=%r (TTL=%ds)",
                    project_name,
                    settings.lock_ttl,
                )
            else:
                logger.error(
                    "worker.heartbeat: lock for project=%r stolen — raising LockStolenError",
                    project_name,
                )
                raise LockStolenError(f"lock stolen for project {project_name!r}")

    async def _run_locked(self, task: Task, lock_token: str) -> None:
        """Execute the task while the distributed lock is held."""
        # Resolve engine + scheduler for this task's work_dir / config_json.
        try:
            engine, scheduler = await self._factory.get(
                work_dir=task.work_dir or None,
                config_json=task.config_json or None,
            )
        except Exception as exc:
            detail = f"engine initialisation failed: {exc}"
            task.transition(TaskStatus.FAILED, detail=detail)
            self._queue.store.update(task)
            logger.error("worker: task=%s %s -> FAILED", task.task_id, detail)
            await self._bus.close(task.task_id)
            return

        try:
            mode = scheduler.decide(task)
        except ValueError as exc:
            task.transition(TaskStatus.FAILED, detail=str(exc))
            self._queue.store.update(task)
            logger.error(
                "worker: task=%s ValueError -> FAILED: %s", task.task_id, exc
            )
            await self._bus.close(task.task_id)
            return

        task.transition(TaskStatus.RUNNING, detail=f"mode={mode.value}")
        self._queue.store.update(task)
        logger.info(
            "worker: task=%s project=%r mode=%s -> RUNNING",
            task.task_id,
            task.project_name,
            mode.value,
        )

        try:
            await self._stream(task, mode, engine)
            task.transition(TaskStatus.SUCCESS)
            self._queue.store.update(task)
            logger.info("worker: task=%s -> SUCCESS", task.task_id)

        except FatalError as exc:
            task.transition(TaskStatus.FAILED, detail=str(exc))
            self._queue.store.update(task)
            logger.error("worker: task=%s FatalError -> FAILED: %s", task.task_id, exc)

        except RetriableError as exc:
            await self._bus.close(task.task_id)
            await self._handle_retriable(task, exc)
            return

        except LockStolenError:
            raise

        except asyncio.CancelledError:
            task.transition(TaskStatus.INTERRUPTED, detail="worker cancelled")
            self._queue.store.update(task)
            logger.warning("worker: task=%s -> INTERRUPTED", task.task_id)
            await self._bus.close(task.task_id)
            raise

        except Exception as exc:
            task.transition(TaskStatus.FAILED, detail=f"unexpected: {exc}")
            self._queue.store.update(task)
            logger.exception("worker: task=%s unexpected error -> FAILED", task.task_id)

        await self._bus.close(task.task_id)

    # ------------------------------------------------------------------ #
    # Stream dispatcher                                                     #
    # ------------------------------------------------------------------ #

    async def _stream(self, task: Task, mode: TaskMode, engine: WorkflowEngine) -> None:
        """
        Dispatch to the correct CIO stream API and forward every CIOEvent
        to the EventBus.  Runs the synchronous iterator in a thread.
        """
        loop = asyncio.get_running_loop()

        def _iter_events():
            if mode == TaskMode.NEW:
                return engine.run_stream(
                    task.requirement,
                    project_name=task.project_name,
                )
            if mode == TaskMode.RESUME:
                return engine.resume_stream(
                    task.project_name,
                    task.requirement or "",
                )
            return engine.run_secondary_stream(
                task.project_name,
                task.requirement,
            )

        executor_future = loop.run_in_executor(
            None, self._consume_sync, task, _iter_events()
        )
        timeout = settings.stream_timeout
        try:
            if timeout > 0:
                await asyncio.wait_for(executor_future, timeout=timeout)
            else:
                await executor_future
        except asyncio.TimeoutError:
            logger.warning(
                "worker: task=%s stream timed out after %.0fs -> INTERRUPTED",
                task.task_id,
                timeout,
            )
            raise asyncio.CancelledError(f"stream timeout after {timeout}s")

    def _consume_sync(self, task: Task, iterator) -> None:
        loop = asyncio.get_running_loop()
        for cio_event in iterator:
            asyncio.run_coroutine_threadsafe(
                self._bus.publish(task.task_id, cio_event), loop
            ).result(timeout=5)

    # ------------------------------------------------------------------ #
    # Retry logic                                                           #
    # ------------------------------------------------------------------ #

    async def _handle_retriable(self, task: Task, exc: RetriableError) -> None:
        if task.retry_count < self._max_retries:
            object.__setattr__(task, "retry_count", task.retry_count + 1)
            task.transition(
                TaskStatus.INTERRUPTED,
                detail=f"retry {task.retry_count}: {exc}",
            )
            self._queue.store.update(task)
            logger.warning(
                "worker: task=%s RetriableError (attempt %d/%d) -> INTERRUPTED: %s",
                task.task_id,
                task.retry_count,
                self._max_retries,
                exc,
            )
            await self._queue.enqueue(task)
        else:
            task.transition(
                TaskStatus.FAILED,
                detail=f"max retries ({self._max_retries}) exceeded: {exc}",
            )
            self._queue.store.update(task)
            logger.error(
                "worker: task=%s max retries exceeded -> FAILED: %s",
                task.task_id,
                exc,
            )
            await self._bus.close(task.task_id)
