"""
api.py — FastAPI application exposing the three core endpoints.

v0.6.1: OpenAPI docs enabled with full metadata from api_contract.md.
        Docs available at /docs (Swagger UI), /redoc, /openapi.json.
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

_DESCRIPTION = """
## CIO Run Auto Orchestrator

A production-ready task scheduling layer over the **CIO-Agent** API.

Handles automatic **NEW / RESUME / SECONDARY** routing, concurrent worker
execution, Redis persistence, distributed locking, crash recovery, and
real-time SSE streaming — so your frontend only needs to POST a task and
open an EventSource.

---

### Task Lifecycle

```
POST /tasks
     │
     ▼
[PENDING] ──► [RUNNING] ──► [SUCCESS]
                        ──► [FAILED]
                        ──► [INTERRUPTED]  → auto-resumed next run
```

### Scheduling Decision

| Condition | Mode |
|---|---|
| `requirement` is empty, checkpoint exists | **RESUME** |
| `requirement` is empty, no checkpoint | **FAILED** (error in status_detail) |
| project does not exist | **NEW** |
| project exists + checkpoint | **RESUME** |
| project exists, no checkpoint | **SECONDARY** |

### SSE Event Stream

Connect to `GET /tasks/{task_id}/stream` with an `EventSource`. Terminal
events (`workflow_complete`, `workflow_failed`) close the stream automatically.

### Dedup Behaviour

Two requests with the same `project_name + requirement` return the same task.
`work_dir` and `config_json` are **not** part of the dedup key.

---

**Base URL:** `http://{host}:1577`  
**Version:** 0.6.1  
**Source:** [GitHub — CIO-Run-Auto-Orchestrator](https://github.com/teryfly/cio-agent)
"""

app = FastAPI(
    title="CIO Run Auto Orchestrator",
    version="0.6.1",
    description=_DESCRIPTION,
    summary="Task scheduling layer over CIO-Agent with SSE streaming.",
    contact={
        "name": "CIO Orchestrator",
        "url": "https://github.com/teryfly/cio-agent",
    },
    license_info={
        "name": "MIT",
    },
    openapi_tags=[
        {
            "name": "tasks",
            "description": "Create, poll, and stream tasks.",
        },
        {
            "name": "health",
            "description": "Liveness probe.",
        },
    ],
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


def _store(request: Request):
    return request.app.state.task_store


def _queue(request: Request):
    return request.app.state.task_queue


def _bus(request: Request):
    return request.app.state.event_bus


@app.post(
    "/tasks",
    status_code=202,
    response_model=TaskResponse,
    tags=["tasks"],
    summary="Create or resume a task",
    response_description="Task accepted (new or existing dedup hit)",
    responses={
        202: {"description": "Task created or existing task returned (dedup)."},
        422: {
            "description": (
                "Validation error — blank project_name, unknown field "
                "(e.g. requirement_file_content), config_json is not valid JSON, "
                "or config_json contains a structural error."
            )
        },
    },
)
async def create_task(body: TaskCreateRequest, request: Request):
    """
    Create a new task, or return an existing incomplete task for the same project (dedup).

    **Dedup rules (in order):**
    1. If a **PENDING** or **RUNNING** task exists for `project_name`, return it.
    2. If a task with the same `project_name + requirement` SHA-256 hash exists in Redis, return it.
    3. Otherwise create a new task.

    **`requirement` empty:** forces the RESUME path. Returns an error (task → FAILED)
    if no checkpoint exists for the project.

    **`config_json`:** optional JSON string overriding the global CIO config.
    Structural errors return 422; missing `model`/`api_key` fall back to
    `CIO_MODEL`/`CIO_API_KEY` env vars and only fail at execution time (task → FAILED).

    **`work_dir`:** always takes precedence over `CIO_WORK_DIR` env var and over
    any `work_dir` key inside `config_json`.
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

    from .models import Task as _Task
    task = _Task(
        project_name=body.project_name,
        requirement=body.requirement,
        work_dir=body.work_dir or "",
        config_json=body.config_json or "",
    )
    await store.save(task)
    await queue.enqueue(task)

    logger.info(
        "api.create_task: created task=%s project=%r", task.task_id, task.project_name
    )
    return TaskResponse.from_task(task)


@app.get(
    "/tasks/{task_id}",
    response_model=TaskResponse,
    tags=["tasks"],
    summary="Get task status",
    responses={
        200: {"description": "Current task state."},
        404: {"description": "Task not found."},
    },
)
async def get_task(task_id: str, request: Request):
    """
    Return the current state of a task.

    Poll this endpoint at a comfortable interval (e.g. every 3 s) or use
    `GET /tasks/{task_id}/stream` for real-time SSE updates instead.
    """
    store = _store(request)
    task = await store.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    return TaskResponse.from_task(task)


@app.get(
    "/tasks/{task_id}/stream",
    tags=["tasks"],
    summary="SSE event stream",
    response_description="text/event-stream — real-time CIO-Agent events",
    responses={
        200: {
            "description": (
                "Server-Sent Events stream. Each frame is `event: <type>\\ndata: <JSON>\\n\\n`. "
                "Terminal events: `workflow_complete`, `workflow_failed`."
            ),
            "content": {"text/event-stream": {}},
        },
        404: {"description": "Task not found."},
    },
)
async def stream_task(task_id: str, request: Request):
    """
    Open a real-time SSE connection to receive CIO-Agent events as they are emitted.

    **SSE frame format:**
    ```
    event: step_start
    data: {"message": "Starting architect phase", "metadata": {}, "timestamp": "..."}

    : keepalive
    ```

    **Terminal events** (`workflow_complete`, `workflow_failed`) cause the server
    to close the stream. Keepalive comments are sent every ~15 s when idle.

    **JavaScript example:**
    ```javascript
    const es = new EventSource(`/tasks/${taskId}/stream`);
    es.addEventListener('workflow_complete', e => { console.log(JSON.parse(e.data)); es.close(); });
    es.addEventListener('workflow_failed',   e => { console.error(JSON.parse(e.data)); es.close(); });
    ```
    """
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


@app.get(
    "/health",
    tags=["health"],
    summary="Liveness check",
    response_description="Service is up",
)
async def health():
    """Returns `{\"status\": \"ok\"}` when the service is running."""
    return JSONResponse({"status": "ok"})
