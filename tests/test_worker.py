"""
test_worker.py — Unit tests for src/worker.py

Mocking strategy
────────────────
Worker._stream runs CIO iterators inside run_in_executor, which spawns a
real OS thread that has no asyncio event loop.  To avoid that:
  - TestRunLocked: replaces w._stream with a plain async coroutine so the
    executor path is never entered while all state-machine logic still runs.
  - TestStream: replaces w._consume_sync (the sync body sent to the thread)
    with a no-op lambda, letting the real _stream dispatch logic run and
    make the correct engine API call.

Coverage targets:
- _run_locked(): ValueError from scheduler → task FAILED, bus closed
- _run_locked(): normal NEW flow → SUCCESS
- _run_locked(): FatalError → FAILED
- _run_locked(): RetriableError → INTERRUPTED + re-queued
- _run_locked(): CancelledError → INTERRUPTED
- _run_locked(): unexpected exception → FAILED
- _handle_retriable(): below max_retries → INTERRUPTED + re-queued
- _handle_retriable(): at max_retries → FAILED + bus closed
- _handle_retriable(): retry_count increments correctly
- _stream(): mode NEW calls engine.run_stream with correct args
- _stream(): mode RESUME calls engine.resume_stream with correct args
- _stream(): mode SECONDARY calls run_secondary_stream (no file_content kwarg)
- _stream(): empty requirement in RESUME passes "" to resume_stream
- _execute(): lock not acquired → re-queues task
- stop(): sets _running = False
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Task, TaskMode, TaskStatus
from src.worker import Worker


# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def make_worker(engine=None, scheduler=None, bus=None, queue=None,
                max_retries=3, poll_interval=0.1):
    engine = engine or MagicMock()
    scheduler = scheduler or MagicMock()
    bus = bus or AsyncMock()
    queue = queue or MagicMock()
    w = Worker(
        engine=engine,
        scheduler=scheduler,
        event_bus=bus,
        task_queue=queue,
        max_retries=max_retries,
        poll_interval=poll_interval,
    )
    return w, engine, scheduler, bus, queue


def run(coro):
    """Run a coroutine on a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def noop_stream(side_effect=None):
    """
    Return an async coroutine to replace Worker._stream.
    side_effect=None  → returns normally (task will reach SUCCESS).
    side_effect=<exc> → raises the given exception.
    """
    if side_effect is None:
        async def _ok(task, mode):
            pass
        return _ok
    exc = side_effect
    async def _raise(task, mode):
        raise exc
    return _raise


# --------------------------------------------------------------------------- #
# TestRunLocked                                                                 #
# --------------------------------------------------------------------------- #

class TestRunLocked:
    def test_value_error_from_scheduler_marks_failed(self, sample_task, mock_queue, mock_bus):
        scheduler = MagicMock()
        scheduler.decide.side_effect = ValueError("no checkpoint")

        w, _, _, _, _ = make_worker(scheduler=scheduler, bus=mock_bus, queue=mock_queue)
        run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.FAILED
        assert "no checkpoint" in sample_task.status_detail
        mock_queue.store.update.assert_called()
        mock_bus.close.assert_awaited()

    def test_value_error_task_never_becomes_running(self, sample_task, mock_queue, mock_bus):
        scheduler = MagicMock()
        scheduler.decide.side_effect = ValueError("oops")

        w, _, _, _, _ = make_worker(scheduler=scheduler, bus=mock_bus, queue=mock_queue)
        run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.FAILED

    def test_normal_new_flow_succeeds(self, sample_task, mock_queue, mock_bus, mock_scheduler):
        mock_scheduler.decide.return_value = TaskMode.NEW

        w, _, _, _, _ = make_worker(
            scheduler=mock_scheduler, bus=mock_bus, queue=mock_queue
        )
        w._stream = noop_stream()
        run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.SUCCESS
        mock_bus.close.assert_awaited()

    def test_fatal_error_marks_failed(self, sample_task, mock_queue, mock_bus, mock_scheduler):
        from cio.errors import FatalError

        mock_scheduler.decide.return_value = TaskMode.NEW
        w, _, _, _, _ = make_worker(
            scheduler=mock_scheduler, bus=mock_bus, queue=mock_queue
        )
        w._stream = noop_stream(side_effect=FatalError("bad config"))
        run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.FAILED
        assert "bad config" in sample_task.status_detail

    def test_retriable_error_transitions_to_interrupted(
        self, sample_task, mock_queue, mock_bus, mock_scheduler
    ):
        from cio.errors import RetriableError

        mock_scheduler.decide.return_value = TaskMode.NEW
        w, _, _, _, _ = make_worker(
            scheduler=mock_scheduler, bus=mock_bus, queue=mock_queue, max_retries=3
        )
        w._stream = noop_stream(side_effect=RetriableError("rate limit"))
        run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.INTERRUPTED
        mock_queue.enqueue.assert_awaited()

    def test_unexpected_exception_marks_failed(
        self, sample_task, mock_queue, mock_bus, mock_scheduler
    ):
        mock_scheduler.decide.return_value = TaskMode.NEW
        w, _, _, _, _ = make_worker(
            scheduler=mock_scheduler, bus=mock_bus, queue=mock_queue
        )
        w._stream = noop_stream(side_effect=RuntimeError("unexpected"))
        run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.FAILED
        assert "unexpected" in sample_task.status_detail

    def test_cancelled_error_marks_interrupted(
        self, sample_task, mock_queue, mock_bus, mock_scheduler
    ):
        mock_scheduler.decide.return_value = TaskMode.NEW
        w, _, _, _, _ = make_worker(
            scheduler=mock_scheduler, bus=mock_bus, queue=mock_queue
        )
        w._stream = noop_stream(side_effect=asyncio.CancelledError("timed out"))
        with pytest.raises(asyncio.CancelledError):
            run(w._run_locked(sample_task, "token"))

        assert sample_task.status == TaskStatus.INTERRUPTED


# --------------------------------------------------------------------------- #
# TestHandleRetriable                                                           #
# --------------------------------------------------------------------------- #

class TestHandleRetriable:
    def test_below_max_retries_interrupts_and_requeues(
        self, sample_task, mock_queue, mock_bus
    ):
        from cio.errors import RetriableError

        w, _, _, _, _ = make_worker(bus=mock_bus, queue=mock_queue, max_retries=3)
        run(w._handle_retriable(sample_task, RetriableError("rate limit")))

        assert sample_task.status == TaskStatus.INTERRUPTED
        assert sample_task.retry_count == 1
        mock_queue.enqueue.assert_awaited()

    def test_at_max_retries_marks_failed(self, sample_task, mock_queue, mock_bus):
        from cio.errors import RetriableError

        object.__setattr__(sample_task, "retry_count", 3)
        w, _, _, _, _ = make_worker(bus=mock_bus, queue=mock_queue, max_retries=3)
        run(w._handle_retriable(sample_task, RetriableError("still failing")))

        assert sample_task.status == TaskStatus.FAILED
        assert "max retries" in sample_task.status_detail
        mock_bus.close.assert_awaited()
        mock_queue.enqueue.assert_not_awaited()

    def test_retry_count_increments(self, sample_task, mock_queue, mock_bus):
        from cio.errors import RetriableError

        w, _, _, _, _ = make_worker(bus=mock_bus, queue=mock_queue, max_retries=5)
        run(w._handle_retriable(sample_task, RetriableError("err")))
        assert sample_task.retry_count == 1

        run(w._handle_retriable(sample_task, RetriableError("err")))
        assert sample_task.retry_count == 2


# --------------------------------------------------------------------------- #
# TestStream                                                                    #
# (_consume_sync replaced with no-op so executor thread is harmless;           #
# the real _stream dispatch logic still runs and calls engine APIs)            #
# --------------------------------------------------------------------------- #

class TestStream:
    def test_stream_new_calls_run_stream(self, sample_task, mock_queue, mock_bus):
        engine = MagicMock()
        engine.run_stream.return_value = iter([])

        w, _, _, _, _ = make_worker(engine=engine, bus=mock_bus, queue=mock_queue)
        w._consume_sync = lambda task, iterator: list(iterator)   # no-op in thread

        run(w._stream(sample_task, TaskMode.NEW))

        engine.run_stream.assert_called_once_with(
            sample_task.requirement, project_name=sample_task.project_name
        )

    def test_stream_resume_calls_resume_stream(self, sample_task, mock_queue, mock_bus):
        engine = MagicMock()
        engine.resume_stream.return_value = iter([])

        w, _, _, _, _ = make_worker(engine=engine, bus=mock_bus, queue=mock_queue)
        w._consume_sync = lambda task, iterator: list(iterator)

        run(w._stream(sample_task, TaskMode.RESUME))

        engine.resume_stream.assert_called_once_with(
            sample_task.project_name, sample_task.requirement or ""
        )

    def test_stream_secondary_no_file_content_param(self, sample_task, mock_queue, mock_bus):
        """CTD change: run_secondary_stream must NOT receive requirement_file_content."""
        engine = MagicMock()
        engine.run_secondary_stream.return_value = iter([])

        w, _, _, _, _ = make_worker(engine=engine, bus=mock_bus, queue=mock_queue)
        w._consume_sync = lambda task, iterator: list(iterator)

        run(w._stream(sample_task, TaskMode.SECONDARY))

        engine.run_secondary_stream.assert_called_once_with(
            sample_task.project_name,
            sample_task.requirement,
        )
        _, kwargs = engine.run_secondary_stream.call_args
        assert "requirement_file_content" not in kwargs

    def test_stream_resume_with_empty_requirement(self, resume_task, mock_queue, mock_bus):
        """Empty requirement in RESUME passes '' as override_requirement."""
        engine = MagicMock()
        engine.resume_stream.return_value = iter([])

        w, _, _, _, _ = make_worker(engine=engine, bus=mock_bus, queue=mock_queue)
        w._consume_sync = lambda task, iterator: list(iterator)

        run(w._stream(resume_task, TaskMode.RESUME))

        engine.resume_stream.assert_called_once_with(resume_task.project_name, "")


# --------------------------------------------------------------------------- #
# TestExecute                                                                   #
# --------------------------------------------------------------------------- #

class TestExecute:
    def test_lock_not_acquired_requeues_task(
        self, sample_task, mock_queue, mock_bus, mock_scheduler
    ):
        w, _, _, _, _ = make_worker(
            scheduler=mock_scheduler, bus=mock_bus, queue=mock_queue
        )

        with patch("src.worker._acquire_lock", new=AsyncMock(return_value=False)):
            run(w._execute(sample_task))

        mock_queue.enqueue.assert_awaited_once_with(sample_task)

    def test_stop_sets_running_false(self):
        w, _, _, _, _ = make_worker()
        w._running = True
        w.stop()
        assert w._running is False
