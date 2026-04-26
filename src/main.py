"""
main.py — Application entry point.

v0.6 change (dynamic engine config)
─────────────────────────────────────
- `_build_workflow_engine()` and its single global (engine, work_dir) pair are
  removed.  WorkflowEngine / Scheduler are now created on-demand (and cached)
  by `engine_factory.get_engine_and_scheduler()`.
- Workers no longer hold a fixed engine reference; they call the factory at
  execution time using the task's own work_dir / config_json fields.
- `app.state.engine_factory` is set so that future middleware / health checks
  can reach the factory if needed.

Everything else (uvicorn setup, worker pool, signal handling, Redis init/
teardown, crash recovery) is unchanged from Phase 4.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import uvicorn

from .api import app
from .config import settings
from .event_bus import EventBus
from .recovery import recover_stranded_tasks
from .redis_client import close_redis, init_redis
from .task_queue import TaskQueue, TaskStore
from .worker import Worker

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Worker pool                                                                   #
# --------------------------------------------------------------------------- #


async def _run_worker_pool(
    workers: list[Worker],
    shutdown_event: asyncio.Event,
) -> None:
    """Run all workers concurrently; stop them when shutdown_event is set."""
    tasks = [
        asyncio.create_task(w.run(), name=f"worker-{i}")
        for i, w in enumerate(workers)
    ]

    await shutdown_event.wait()

    logger.info(
        "main: shutdown signal received — stopping %d workers", len(workers)
    )
    for w in workers:
        w.stop()

    done, pending = await asyncio.wait(tasks, timeout=30)
    for t in pending:
        logger.warning(
            "main: worker %s did not stop in time — cancelling", t.get_name()
        )
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("main: all workers stopped")


# --------------------------------------------------------------------------- #
# Main                                                                          #
# --------------------------------------------------------------------------- #


async def _async_main() -> None:
    logger.info("main: connecting to Redis at %s", settings.redis_url)
    await init_redis(settings.redis_url)

    try:
        await _run_app()
    finally:
        logger.info("main: closing Redis connection")
        await close_redis()


async def _run_app() -> None:
    """Inner coroutine that builds and runs the full application stack."""
    # ── Shared infrastructure (Redis-backed) ─────────────────────────── #
    task_store = TaskStore()
    task_queue = TaskQueue(store=task_store)
    event_bus = EventBus()

    # ── Attach to FastAPI app state ──────────────────────────────────── #
    app.state.task_store = task_store
    app.state.task_queue = task_queue
    app.state.event_bus = event_bus
    # engine_factory module is imported lazily by workers; expose for tests
    from . import engine_factory
    app.state.engine_factory = engine_factory

    # ── Crash recovery — re-queue stranded tasks ─────────────────────── #
    recovered = await recover_stranded_tasks(task_store, task_queue)
    if recovered:
        logger.info(
            "main: crash recovery complete — %d task(s) re-queued for auto-resume",
            recovered,
        )

    # ── Build worker pool ────────────────────────────────────────────── #
    shutdown_event = asyncio.Event()

    workers = [
        Worker(
            event_bus=event_bus,
            task_queue=task_queue,
            max_retries=settings.max_retries,
            poll_interval=settings.worker_poll_interval,
        )
        for _ in range(settings.worker_concurrency)
    ]

    # ── OS signal handling ───────────────────────────────────────────── #
    loop = asyncio.get_running_loop()

    def _on_signal():
        logger.info("main: received shutdown signal")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: shutdown_event.set())

    # ── Start uvicorn in background ──────────────────────────────────── #
    uv_config = uvicorn.Config(
        app=app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        loop="none",
    )
    server = uvicorn.Server(uv_config)

    logger.info(
        "main: starting CIO Orchestrator on %s:%d with %d workers",
        settings.api_host,
        settings.api_port,
        settings.worker_concurrency,
    )

    await asyncio.gather(
        server.serve(),
        _run_worker_pool(workers, shutdown_event),
    )


def main() -> None:
    """Console-script entry point (see pyproject.toml [project.scripts])."""
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        logger.info("main: interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
