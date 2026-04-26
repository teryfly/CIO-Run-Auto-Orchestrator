"""
main.py — Application entry point.

v0.6.1 fix
──────────
- Worker now receives an EngineFactory instance (fixes ImportError from
  worker.py expecting EngineFactory class).
- app.state.engine_factory stores the shared EngineFactory instance.
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
from .engine_factory import EngineFactory
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


async def _run_worker_pool(
    workers: list[Worker],
    shutdown_event: asyncio.Event,
) -> None:
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


async def _async_main() -> None:
    logger.info("main: connecting to Redis at %s", settings.redis_url)
    await init_redis(settings.redis_url)

    try:
        await _run_app()
    finally:
        logger.info("main: closing Redis connection")
        await close_redis()


async def _run_app() -> None:
    task_store = TaskStore()
    task_queue = TaskQueue(store=task_store)
    event_bus = EventBus()
    engine_factory = EngineFactory()

    app.state.task_store = task_store
    app.state.task_queue = task_queue
    app.state.event_bus = event_bus
    app.state.engine_factory = engine_factory

    recovered = await recover_stranded_tasks(task_store, task_queue)
    if recovered:
        logger.info(
            "main: crash recovery complete — %d task(s) re-queued for auto-resume",
            recovered,
        )

    shutdown_event = asyncio.Event()

    workers = [
        Worker(
            engine_factory=engine_factory,
            event_bus=event_bus,
            task_queue=task_queue,
            max_retries=settings.max_retries,
            poll_interval=settings.worker_poll_interval,
        )
        for _ in range(settings.worker_concurrency)
    ]

    loop = asyncio.get_running_loop()

    def _on_signal():
        logger.info("main: received shutdown signal")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: shutdown_event.set())

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
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        logger.info("main: interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
