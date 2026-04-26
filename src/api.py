"""
api.py — FastAPI application exposing the three core endpoints.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from .config import settings
from .models import Task, TaskCreateRequest, TaskResponse, TaskStatus
from .sse_utils import KEEPALIVE_FRAME, build_sse_frame, is_terminal_event

logger = logging.getLogger(__name__)

app = FastAPI(
    title="CIO Run Auto Orchestrator",
    version="0.5.0",
    description=(
        "Task scheduling layer over CIO-Agent API.  "
        "Handles NEW / RESUME / SECONDARY routing, concurrency, "
        "Redis persistence, and real-time SSE streaming."
    ),
)


def _store(request: Request):
    return request.app.state.task_store


def _queue(request: Request):
    return request.app.state.task_queue


def _bus(request: Request):
    return request.app.state.event_bus


@app.post("/tasks", status_code=202, response_model=TaskResponse)
async def create_task(body: TaskCreateRequest, request: Request):
    """
    Create a new task (or return an existing incomplete one for the same project).

    Dedup rules:
    1. Status-based: if a PENDING or RUNNING task exists for project_name, return it.
    2. Digest-based: if a task with the same project_name + requirement hash exists.
    """
    store = _store(request)
    queue = _queue(request)

    existing = await store.find_incomplete(body.project_name)

    if existing is None:
        existing = await store.find_by_digest(body.project_name, body.requirement)

    if existing is not None:
        logger.info(
            "api.create_task: dedup hit for project=%r → returning task=%s",
            body.project_name,
            existing.task_id,
        )
        return TaskResponse.from_task(existing)

    task = Task(
        project_name=body.project_name,
        requirement=body.requirement,
    )
    await store.save(task)
    await queue.enqueue(task)

    logger.info(
        "api.create_task: created task=%s project=%r", task.task_id, task.project_name
    )
    return TaskResponse.from_task(task)


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, request: Request):
    """Return the current state of a task."""
    store = _store(request)
    task = await store.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    return TaskResponse.from_task(task)


@app.get("/tasks/{task_id}/stream")
async def stream_task(task_id: str, request: Request):
    """SSE endpoint — streams CIOEvent objects in real time."""
    store = _store(request)
    bus = _bus(request)

    task = await store.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")

    if task.is_terminal():
        async def _empty():
            return
            yield

        return EventSourceResponse(_empty())

    keepalive_interval = settings.sse_keepalive_interval

    async def _event_generator():
        async for cio_event in bus.subscribe(
            task_id, keepalive_interval=keepalive_interval
        ):
            if await request.is_disconnected():
                break

            if cio_event is None:
                yield KEEPALIVE_FRAME
                continue

            frame = build_sse_frame(cio_event)
            yield frame

            if is_terminal_event(frame["event"]):
                await bus.close(task_id)
                break

    return EventSourceResponse(_event_generator())


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})
