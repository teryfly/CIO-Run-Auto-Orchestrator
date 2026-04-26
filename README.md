# CIO Run Auto Orchestrator

**Version:** 0.6.1 | **Python:** 3.10+

A production-ready task scheduling layer over the [CIO-Agent](https://github.com/teryfly/cio-agent) API.  
It handles automatic NEW / RESUME / SECONDARY routing, concurrent worker execution, Redis persistence, distributed locking, crash recovery, and real-time SSE streaming — so your frontend only needs to POST a task and open an EventSource.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                  H5 / Web Client                    │
│   POST /tasks  ─►  GET /tasks/{id}/stream (SSE)     │
└───────────────────────┬─────────────────────────────┘
                        │ HTTP / SSE
┌───────────────────────▼─────────────────────────────┐
│            FastAPI Orchestrator (this repo)          │
│  api.py ─► TaskQueue ─► Worker Pool ─► EventBus     │
│             Redis       EngineFactory   Redis Pub/Sub│
└───────────────────────┬─────────────────────────────┘
                        │ Python API
┌───────────────────────▼─────────────────────────────┐
│               CIO-Agent WorkflowEngine               │
│   run_stream / resume_stream / run_secondary_stream  │
└─────────────────────────────────────────────────────┘
```

### Components

| File | Role |
|---|---|
| `api.py` | FastAPI routes: POST /tasks, GET /tasks/{id}, GET /tasks/{id}/stream |
| `worker.py` | Async worker: dequeues tasks, acquires lock, resolves engine via EngineFactory, streams CIO-Agent output |
| `engine_factory.py` | WorkflowEngine + Scheduler factory; caches instances by `(work_dir, config_hash)`; merges `config_json` with env-var defaults |
| `worker_locks.py` | Redis distributed lock helpers (acquire / release / refresh) |
| `scheduler.py` | Decides task mode: NEW / RESUME / SECONDARY |
| `event_bus.py` | Redis Pub/Sub fan-out for SSE clients |
| `task_queue/queue.py` | Redis-backed FIFO task queue (BRPOP) |
| `task_queue/store.py` | Redis-backed task store (GET/SET + project index) |
| `recovery.py` | Startup crash-recovery: re-queues RUNNING/INTERRUPTED tasks |
| `config.py` | All settings via environment variables |
| `models/` | Pydantic models: Task, TaskCreateRequest, TaskResponse, SSEPayload, ConfigJsonSchema |

---

## Quick Start

### 1. Prerequisites

```bash
# Python 3.10+
pip install cio-agent fastapi uvicorn sse-starlette redis pytest pytest-asyncio pytest-cov

# Redis running locally (or set REDIS_URL)
redis-server
```

### 2. Environment Variables

```bash
export CIO_API_KEY="your-poe-api-key"
export CIO_MODEL="GPT-4.1"
export CIO_WORK_DIR="./workspace"
export REDIS_URL="redis://localhost:6379/0"

# Optional tuning
export WORKER_CONCURRENCY=4
export MAX_RETRIES=3
export LOCK_TTL=3600
export LOCK_HEARTBEAT_INTERVAL=1200
export STREAM_TIMEOUT=0
export SSE_KEEPALIVE_INTERVAL=15.0
```

### 3. Run

```bash
cd CIO-Run-Auto-Orchestrator
python -m src.main
# Server starts on http://0.0.0.0:1577
```

### 4. Run Tests

```bash
cd CIO-Run-Auto-Orchestrator
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## API Summary

| Method | Path | Description |
|---|---|---|
| `POST` | `/tasks` | Create a task (or return existing incomplete one) |
| `GET` | `/tasks/{task_id}` | Poll task status |
| `GET` | `/tasks/{task_id}/stream` | Real-time SSE event stream |
| `GET` | `/health` | Liveness check |

Full request/response schemas → **api_contract.md**

---

## Task Lifecycle

```
           POST /tasks
                │
                ▼
           [PENDING] ──────────────────────────────┐
                │ worker dequeues                   │
                ▼                                   │
           [RUNNING] ──► CIO-Agent stream           │
                │                                   │ dedup hit:
           ┌────┴─────────────────┐                 │ return existing
           ▼                     ▼                  │
       [SUCCESS]           [FAILED] ◄───────────────┘
                                 ▲
           [INTERRUPTED] ────────┘
           (RetriableError / timeout / crash)
           Scheduler → RESUME next attempt
```

### Scheduling Decision

```
requirement 为空？  Yes → RESUME（无 checkpoint 则任务转 FAILED）
      │
      No
      │
project_exists?  No  → NEW   (run_stream)
      │
      Yes
      │
checkpoint_exists?  Yes → RESUME     (resume_stream)
      │
      No  → SECONDARY  (run_secondary_stream)
```

---

## POST /tasks — Field Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `project_name` | string | ✅ | Unique project identifier (1–128 chars, slug-style recommended) |
| `requirement` | string | ❌ | Development requirement (default `""`). Empty = auto-resume from checkpoint. |
| `work_dir` | string | ❌ | Override CIO work directory for this task. Takes precedence over `CIO_WORK_DIR` env var and any `work_dir` inside `config_json`. |
| `config_json` | string | ❌ | JSON string representing a CIOConfig dict. Replaces `CIO_CONFIG_PATH` / env-var config when non-empty. Fields absent from the JSON fall back to env vars (`CIO_MODEL`, `CIO_API_KEY`, etc.). Only structural errors (wrong types, invalid enum values) return 422; missing `model`/`api_key` that cannot be filled from env vars cause the task to transition to `FAILED`. |

> **Breaking change from v0.4:** `requirement_file_content` has been **removed**. To pass document content, concatenate it into the `requirement` field. Sending `requirement_file_content` now returns `422`.

---

## Dynamic Engine Configuration (v0.6)

By default the orchestrator uses `CIO_WORK_DIR` and `CIO_CONFIG_PATH` (or individual `CIO_*` env vars) for every task. Starting from v0.6, callers can override these on a per-task basis:

- **`work_dir`** — sets the CIO working directory for just this task.
- **`config_json`** — supplies a full or partial CIOConfig as a JSON string. Fields present in the JSON take precedence over env vars; fields absent fall back to env vars.

The `EngineFactory` caches `(WorkflowEngine, Scheduler)` pairs keyed by `(work_dir, sha256(config_json))`, so repeated calls with the same parameters are cheap.

### Config Merge Precedence (highest → lowest)

1. Request-level `work_dir` param — always wins for the work directory
2. Fields supplied in `config_json`
3. Environment variables (`CIO_MODEL`, `CIO_API_KEY`, `CIO_LLM_URL`, …)
4. Built-in defaults

### config_json Validation

Structural errors are caught at the API boundary and returned as `422 Unprocessable Entity` with field-level detail:

| Rejected at 422 | Not rejected at 422 |
|---|---|
| Invalid JSON syntax | Missing `model` (falls back to `CIO_MODEL`) |
| Wrong field type (e.g. `max_fix_rounds: "three"`) | Missing `api_key` (falls back to `CIO_API_KEY`) |
| Invalid enum value (e.g. `push_strategy: "always"`) | Missing any other optional field |
| `target_coverage` outside 0–100 | — |

If `model` or `api_key` remain empty after env-var fallback, the worker transitions the task to `FAILED` with a descriptive `status_detail` — task creation still returns `202`.

### Dedup Note

The dedup key is `project_name + requirement` only. Two requests with the same project+requirement but different `work_dir` / `config_json` deduplicate to the **first** task created; the first caller's engine config wins. To force a separate execution, use a distinct `project_name`.

---

## SSE Event Stream

Connect to `GET /tasks/{task_id}/stream` with an `EventSource`.

### Event Types

| event | Meaning | Terminal? |
|---|---|---|
| `step_start` | A workflow phase is beginning | No |
| `step_complete` | A phase finished | No |
| `agent_send` | Prompt sent to LLM | No |
| `agent_recv` | LLM response received | No |
| `cio_decision` | Routing/mode decision | No |
| `info` | General progress message | No |
| `warn` | Non-fatal warning | No |
| `error` | Recoverable error | No |
| `workflow_complete` | Task succeeded | **Yes** |
| `workflow_failed` | Task failed | **Yes** |

---

## Dedup Behaviour

Two layers, in order:

1. **Status-based**: if a PENDING or RUNNING task exists for the same `project_name`, return it immediately.
2. **Digest-based**: if a task with the same `project_name + requirement` SHA-256 hash exists in Redis, return it. (Skipped when `requirement` is empty.)

---

## Crash Recovery

On startup, `recovery.py` scans Redis for stranded tasks:

| Status at crash | Recovery action |
|---|---|
| `RUNNING` | → `INTERRUPTED`, re-queued → Scheduler attempts RESUME |
| `INTERRUPTED` | Re-queued directly → Scheduler attempts RESUME |
| `PENDING` | Re-queued (in-memory queue was lost) |
| `SUCCESS` / `FAILED` | Skipped |

---

## Distributed Lock

One lock per `project_name` prevents concurrent modification:

- Key: `cio:lock:{project_name}`
- Acquired with SET NX + TTL (default 3600 s)
- Released with Lua compare-and-delete (safe across replicas)
- Refreshed every `LOCK_HEARTBEAT_INTERVAL` seconds by a background coroutine

---

## Redis Key Layout

```
cio:task:{task_id}              STRING  full Task JSON
cio:project:{name}:tasks        SET     task_ids for a project
cio:task:digest:{sha256}        STRING  task_id for dedup (only when requirement non-empty)
cio:lock:{project_name}         STRING  distributed lock token
cio:queue:tasks                 LIST    FIFO task queue (BRPOP)
cio:events:{task_id}            CHANNEL Redis Pub/Sub SSE fan-out
```

---

## Configuration Reference

| Env Var | Default | Description |
|---|---|---|
| `CIO_API_KEY` | *(required)* | POE / LLM API key; used as fallback when `api_key` is absent from `config_json` |
| `CIO_MODEL` | `GPT-4.1` | Default LLM model; used as fallback when `model` is absent from `config_json` |
| `CIO_LLM_URL` | `https://api.poe.com` | OpenAI-compatible base URL |
| `CIO_WORK_DIR` | `./workspace` | CIO-Agent working directory; overridable per-task via the `work_dir` request field |
| `CIO_CONFIG_PATH` | `` | Path to cio config.yaml; ignored when `config_json` is supplied in the request |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `WORKER_CONCURRENCY` | `4` | Number of parallel workers |
| `WORKER_POLL_INTERVAL` | `0.5` | Seconds between queue polls |
| `MAX_RETRIES` | `3` | Max RetriableError retries per task |
| `LOCK_TTL` | `3600` | Distributed lock TTL in seconds |
| `LOCK_HEARTBEAT_INTERVAL` | `1200` | Lock refresh interval |
| `STREAM_TIMEOUT` | `0` | Max stream duration (0 = unlimited) |
| `SSE_KEEPALIVE_INTERVAL` | `15.0` | SSE keepalive comment interval |
| `API_HOST` | `0.0.0.0` | Uvicorn bind host |
| `API_PORT` | `1577` | Uvicorn bind port |
| `LOG_LEVEL` | `INFO` | Python logging level |