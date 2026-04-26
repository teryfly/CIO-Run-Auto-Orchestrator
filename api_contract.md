# API Contract — CIO Run Auto Orchestrator

**Version:** 0.6.1  
**Base URL:** `http://{host}:1577`  
**Content-Type (requests):** `application/json`  
**Content-Type (SSE):** `text/event-stream`

---

## Table of Contents

1. [POST /tasks — Create Task](#1-post-tasks)
2. [GET /tasks/{task_id} — Get Task Status](#2-get-taskstask_id)
3. [GET /tasks/{task_id}/stream — SSE Event Stream](#3-get-taskstask_idstream)
4. [GET /health — Health Check](#4-get-health)
5. [Data Models](#5-data-models)
6. [SSE Event Reference](#6-sse-event-reference)
7. [Error Responses](#7-error-responses)
8. [Integration Patterns](#8-integration-patterns)

---

## 1. POST /tasks

Create a new task, or return an existing incomplete task for the same project (dedup).

### Request

```
POST /tasks
Content-Type: application/json
```

#### Body

```json
{
  "project_name": "my-flask-app",
  "requirement": "Create a REST API with JWT authentication",
  "work_dir": "/data/projects",
  "config_json": "{\"model\":\"GPT-4.1\",\"api_key\":\"sk-xxx\",\"llm_url\":\"https://api.poe.com\"}"
}
```

| Field | Type | Required | Constraints | Description |
|---|---|---|---|---|
| `project_name` | string | ✅ | 1–128 chars | Unique project identifier. Slug-style recommended (`a-z`, `0-9`, `-`). |
| `requirement` | string | ❌ | default `""` | Natural-language requirement. When empty, automatically resumes from the latest checkpoint; returns an error if no checkpoint exists. When non-empty, used as the development requirement. To include document content, concatenate it into this field. |
| `work_dir` | string | ❌ | default `null` | Override the CIO work directory for this task. When non-empty, takes precedence over the `CIO_WORK_DIR` environment variable **and** over any `work_dir` embedded inside `config_json`. |
| `config_json` | string | ❌ | default `null` | JSON string representing a CIOConfig dict. When non-empty, replaces the `CIO_CONFIG_PATH` / env-var config path. Parsed, merged with env-var defaults, then passed to `CIOConfig.from_dict()`. Returns `422` on invalid JSON or wrong field types. Missing `model` / `api_key` fall back to `CIO_MODEL` / `CIO_API_KEY`; if both are still empty after merging, the task transitions to `FAILED` (not 422). |

> **Removed field:** `requirement_file_content` has been removed. Passing this field results in `422 Unprocessable Entity`.

#### `config_json` Schema

The string value of `config_json` must be a valid JSON **object**. All fields are optional — missing values fall back to environment-variable defaults before the engine is constructed. Only structural errors (wrong types, invalid enum values, out-of-range numbers) are rejected at the API boundary with `422`.

**Top-level fields**

| Field | Type | Env-var fallback | Description |
|---|---|---|---|
| `model` | string | `CIO_MODEL` (default `"GPT-4.1"`) | LLM model identifier. |
| `api_key` | string | `CIO_API_KEY` | API key for the LLM provider. |
| `llm_url` | string | `CIO_LLM_URL` (default `"https://api.poe.com"`) | OpenAI-compatible base URL. |
| `work_dir` | string | `CIO_WORK_DIR` | CIO working directory — **always overridden by the request-level `work_dir` param**. |
| `file_limit` | integer | `30` | Max files routed to Engineer decomposition. |
| `architect_prompt` | string | `"default"` | Architect system prompt identifier. |
| `engineer_prompt` | string | `"default"` | Engineer system prompt identifier. |
| `claude_alias` | string | `""` | Claude Code CLI model alias. |
| `cio_prompts` | object | `{}` | Stage-name → prompt-id mapping. |
| `execution_context_max_turns` | integer | `10` | Max recent events included in CIO context. |
| `execution_context_content_limit` | integer | `500` | Max chars retained per event. |

**`models` section** (all optional, `"default"` inherits global model)

| Field | Type | Description |
|---|---|---|
| `cio_naming_model` | string | Model for project naming. |
| `cio_decision_model` | string | Model for CIO routing decisions. |
| `cio_executor_model` | string | Model for executor phase. |
| `architect_model` | string | Model for architect phase. |
| `engineer_model` | string | Model for engineer phase. |
| `documenter_model` | string | Model for documenter phase. |

**`validation` section** (all optional)

| Field | Type | Constraints | Description |
|---|---|---|---|
| `validate_after_run` | boolean | — | Run validation after workflow completes. |
| `max_fix_rounds` | integer | ≥ 0 | Max auto-fix attempts per validation step. |
| `model` | string | — | Validation model (`"default"` = global model). |
| `step_filter` | string[] | — | Subset of steps to run, e.g. `["V0","V1","V4"]`. |
| `stdout_preview_limit` | integer | ≥ 0 | Max chars of Claude Code output passed to CIO. |
| `target_coverage` | integer | 0–100 | Minimum line coverage % for V3 to pass. |

**`claude_md` section** (all optional)

| Field | Type | Description |
|---|---|---|
| `enabled` | boolean | Enable CLAUDE.md optimisation. |
| `model` | string | Model for ClaudeMD optimiser. |
| `memory_model` | string | Model for memory distillation. |

**`git` section** (all optional)

| Field | Type | Constraints | Description |
|---|---|---|---|
| `enabled` | boolean | — | Enable local git commits. |
| `user.name` | string | — | Commit author name. |
| `user.email` | string | — | Commit author email. |
| `gitlab.token` | string | — | GitLab Personal Access Token. |
| `gitlab.base_url` | string | — | GitLab instance URL. |
| `gitlab.namespace` | string | — | GitLab namespace (auto-resolved if empty). |
| `gitlab.branch` | string | — | Default target branch. |
| `push_strategy` | string | `never`\|`on_complete`\|`on_phase`\|`manual` | When to push to remote. |
| `branch_strategy` | string | `feature_branch`\|`direct_main` | Branch strategy for secondary dev. |
| `feature_branch_prefix` | string | — | Prefix for auto-created feature branches. |
| `init_on_new_project` | boolean | — | Auto-init git repo on new projects. |
| `commit_on_phase` | boolean | — | Commit after each successful phase. |
| `tag_on_validate` | boolean | — | Tag after successful validation. |
| `gitignore_cio_logs` | boolean | — | Add `.cio/logs/` to `.gitignore`. |

> **Priority rule:** The request-level `work_dir` param always overrides any `work_dir` key inside `config_json`.

> **model / api_key fallback:** If either field is absent from `config_json`, the corresponding env var (`CIO_MODEL` / `CIO_API_KEY`) is used. Only if the merged value is still empty does the task transition to `FAILED` (this is not a `422` — task creation succeeds; the error surfaces in `status_detail`).

**422 examples — structural errors caught at API boundary:**

```json
// Wrong type: max_fix_rounds expects integer
{
  "detail": [
    {
      "loc": ["body", "config_json"],
      "msg": "config_json failed schema validation:\n  • validation → max_fix_rounds: Input should be a valid integer",
      "type": "value_error"
    }
  ]
}
```

```json
// Invalid enum: push_strategy must be one of the allowed values
{
  "detail": [
    {
      "loc": ["body", "config_json"],
      "msg": "config_json failed schema validation:\n  • git → push_strategy: push_strategy must be one of ['manual', 'never', 'on_complete', 'on_phase']",
      "type": "value_error"
    }
  ]
}
```

```json
// target_coverage out of range
{
  "detail": [
    {
      "loc": ["body", "config_json"],
      "msg": "config_json failed schema validation:\n  • validation → target_coverage: target_coverage must be between 0 and 100",
      "type": "value_error"
    }
  ]
}
```

### Response — 202 Accepted

```json
{
  "task_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "project_name": "my-flask-app",
  "requirement": "Create a REST API with JWT authentication",
  "work_dir": "/data/projects",
  "config_json": "{\"model\":\"GPT-4.1\",\"api_key\":\"sk-xxx\",\"llm_url\":\"https://api.poe.com\"}",
  "mode": "unknown",
  "status": "pending",
  "created_at": "2025-01-15T10:30:00.000Z",
  "updated_at": "2025-01-15T10:30:00.000Z",
  "retry_count": 0,
  "status_detail": ""
}
```

### Request Examples

```json
// 场景一：新项目（使用环境变量配置）
{
  "project_name": "my-flask-app",
  "requirement": "Create a REST API with JWT authentication"
}

// 场景二：新项目，仅覆盖工作目录
{
  "project_name": "my-flask-app",
  "requirement": "Create a REST API with JWT authentication",
  "work_dir": "/data/workspace/team-a"
}

// 场景三：动态指定完整 CIO 配置（work_dir 优先于 config_json 内部值）
{
  "project_name": "my-flask-app",
  "requirement": "Create a REST API with JWT authentication",
  "work_dir": "/data/workspace/team-a",
  "config_json": "{\"model\":\"GPT-4.1\",\"api_key\":\"sk-xxx\",\"llm_url\":\"https://api.poe.com\",\"file_limit\":30}"
}

// 场景四：二次开发（含附件内容拼入 requirement）
{
  "project_name": "my-flask-app",
  "requirement": "Add rate limiting\n\n---\n[spec.md content here]"
}

// 场景五：恢复中断任务（不加新需求）
{
  "project_name": "my-flask-app"
}
```

### Dedup Behaviour

| Condition | Result |
|---|---|
| PENDING or RUNNING task exists for `project_name` | Return existing task (202) |
| Exact `project_name + requirement` hash exists in Redis | Return existing task (202) |
| None of the above | Create new task (202) |

> **Note:** Dedup is keyed on `project_name + requirement` only — `work_dir` and `config_json` are **not** part of the dedup key. Two requests with the same project+requirement but different engine configs deduplicate to the first task; the first caller's config wins. To force a separate execution, use a distinct `project_name`.

> **Note:** When `requirement` is empty, digest-based dedup is skipped.

### JavaScript Example

```javascript
const res = await fetch('/tasks', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    project_name: 'my-flask-app',
    requirement: 'Create a REST API with JWT auth',
    work_dir: '/data/workspace/team-a',
    config_json: JSON.stringify({
      model: 'GPT-4.1',
      api_key: 'sk-xxx',
      llm_url: 'https://api.poe.com',
    }),
  }),
});
const task = await res.json();
// task.task_id → use for polling and SSE
```

---

## 2. GET /tasks/{task_id}

Poll the current state of a task.

### Request

```
GET /tasks/{task_id}
```

| Parameter | Location | Type | Description |
|---|---|---|---|
| `task_id` | path | string | Task ID returned by POST /tasks |

### Response — 200 OK

```json
{
  "task_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "project_name": "my-flask-app",
  "requirement": "Create a REST API with JWT authentication",
  "work_dir": "/data/workspace/team-a",
  "config_json": "",
  "mode": "new",
  "status": "running",
  "created_at": "2025-01-15T10:30:00.000Z",
  "updated_at": "2025-01-15T10:30:05.123Z",
  "retry_count": 0,
  "status_detail": "mode=new"
}
```

### Response — 404 Not Found

```json
{ "detail": "Task 'abc123' not found" }
```

---

## 3. GET /tasks/{task_id}/stream

Open a real-time SSE connection to receive CIO-Agent events as they are emitted.

### Request

```
GET /tasks/{task_id}/stream
Accept: text/event-stream
```

### Response — 200 OK (SSE)

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

#### SSE Frame Structure

```
event: <event_type>
data: <JSON-encoded SSEPayload>
\n
```

#### SSEPayload JSON Schema

```json
{
  "message": "Starting architect phase for my-flask-app",
  "metadata": { "phase": 1, "step": "architect" },
  "timestamp": "2025-01-15T10:30:06.000Z"
}
```

#### Keepalive Comment

Sent every 15 seconds (configurable) when no event arrives:

```
: keepalive
```

#### Stream Termination

The server closes the stream when a terminal event (`workflow_complete` or `workflow_failed`) is published.

### JavaScript EventSource Example

```javascript
function streamTask(taskId, { onEvent, onComplete, onError } = {}) {
  const es = new EventSource(`/tasks/${taskId}/stream`);
  const eventTypes = [
    'step_start', 'step_complete', 'agent_send', 'agent_recv',
    'cio_decision', 'info', 'warn', 'error',
    'workflow_complete', 'workflow_failed',
  ];
  eventTypes.forEach(type => {
    es.addEventListener(type, (e) => {
      const payload = JSON.parse(e.data);
      onEvent?.({ type, ...payload });
      if (type === 'workflow_complete') { onComplete?.(payload); es.close(); }
      if (type === 'workflow_failed')   { onError?.(payload);   es.close(); }
    });
  });
  es.onerror = (err) => { onError?.({ message: 'SSE connection error', err }); es.close(); };
  return es;
}
```

---

## 4. GET /health

```json
{ "status": "ok" }
```

---

## 5. Data Models

### TaskStatus Enum

| Value | Meaning |
|---|---|
| `pending` | Queued, not yet picked up by a worker |
| `running` | Worker actively executing CIO-Agent stream |
| `success` | Completed successfully — terminal |
| `failed` | Failed (fatal error or max retries exceeded) — terminal |
| `interrupted` | Stopped mid-run; checkpoint may exist → auto-resumed |

### TaskMode Enum

| Value | Meaning |
|---|---|
| `unknown` | Scheduling decision not yet made |
| `new` | New project — `run_stream()` |
| `resume` | Checkpoint found — `resume_stream()` |
| `secondary` | Existing project, no checkpoint — `run_secondary_stream()` |

### TaskResponse (full schema)

```typescript
interface TaskResponse {
  task_id:       string;
  project_name:  string;
  requirement:   string;
  work_dir:      string;   // "" when using env-var default
  config_json:   string;   // "" when using env-var / yaml config
  mode:          'unknown' | 'new' | 'resume' | 'secondary';
  status:        'pending' | 'running' | 'success' | 'failed' | 'interrupted';
  created_at:    string;
  updated_at:    string;
  retry_count:   number;
  status_detail: string;
}
```

### Engine Config Resolution

| `work_dir` param | `config_json` param | Effective work_dir | Effective CIO config |
|---|---|---|---|
| empty / null | empty / null | `CIO_WORK_DIR` env var | `CIO_CONFIG_PATH` yaml, or env vars via `from_dict()` |
| `"/data/ws"` | empty / null | `"/data/ws"` | `CIO_CONFIG_PATH` yaml, or env vars via `from_dict()` |
| empty / null | `"{...}"` | `config_json.work_dir` if present, else `CIO_WORK_DIR` | parsed + merged with env-var defaults → `from_dict()` |
| `"/data/ws"` | `"{...}"` | **`"/data/ws"`** (request param wins) | parsed + merged with env-var defaults → `from_dict()`, `work_dir` overridden |

**Merge precedence (config_json path):** `config_json` field value > env-var default > built-in default. The request-level `work_dir` is always applied last.

---

## 6. SSE Event Reference

| event_type | Terminal | Typical message |
|---|---|---|
| `step_start` | No | `"Starting phase 1: architect"` |
| `step_complete` | No | `"Phase 1 complete"` |
| `agent_send` | No | `"Sending prompt to GPT-4.1"` |
| `agent_recv` | No | `"Response received (1842 tokens)"` |
| `cio_decision` | No | `"Routing decision: SECONDARY"` |
| `info` | No | `"Checking for existing checkpoint..."` |
| `warn` | No | `"Rate limit hit, retrying in 2s"` |
| `error` | No | `"Phase 3 executor error — retrying"` |
| `workflow_complete` | **Yes** | `"Project my-flask-app created successfully"` |
| `workflow_failed` | **Yes** | `"Fatal error: invalid CIO config"` |

---

## 7. Error Responses

```json
{ "detail": "Human-readable error description" }
```

| Status | When |
|---|---|
| `400 Bad Request` | `requirement` is empty and the project has no restorable checkpoint |
| `404 Not Found` | `task_id` does not exist in Redis |
| `422 Unprocessable Entity` | Request body validation failed — blank `project_name`, unknown field `requirement_file_content`, `config_json` is not valid JSON, or `config_json` contains a structural error (wrong field type, out-of-range value, invalid enum such as `push_strategy: "always"`) |
| `500 Internal Server Error` | Unexpected server error |

> **Note:** The error for missing checkpoint surfaces as task `FAILED` status (with `status_detail`), not as HTTP 400. Task creation returns `202`; the error occurs during worker execution.

> **Note:** Missing `model` / `api_key` in `config_json` are **not** a `422`. They fall back to `CIO_MODEL` / `CIO_API_KEY` env vars. Only if the merged value is still empty does the worker transition the task to `FAILED` with a clear `status_detail` message.

---

## 8. Integration Patterns

### Pattern A — Fire and Poll

```javascript
async function runTask(projectName, requirement, { workDir, configJson } = {}) {
  const { task_id } = await fetch('/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      project_name: projectName,
      requirement,
      work_dir: workDir,
      config_json: configJson,
    }),
  }).then(r => r.json());

  while (true) {
    const task = await fetch(`/tasks/${task_id}`).then(r => r.json());
    if (task.status === 'success') return task;
    if (task.status === 'failed') throw new Error(task.status_detail);
    await new Promise(r => setTimeout(r, 3000));
  }
}
```

### Pattern B — SSE Live Progress (recommended)

```javascript
async function runTaskWithProgress(projectName, requirement, onProgress, { workDir, configJson } = {}) {
  const { task_id } = await fetch('/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      project_name: projectName,
      requirement,
      work_dir: workDir,
      config_json: configJson,
    }),
  }).then(r => r.json());

  return new Promise((resolve, reject) => {
    streamTask(task_id, { onEvent: onProgress, onComplete: resolve, onError: reject });
  });
}
```

### Pattern C — Resume Interrupted Task

```javascript
const { task_id } = await fetch('/tasks', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ project_name: 'my-flask-app' }),
}).then(r => r.json());
```

### Pattern D — Multi-tenant / Per-team Config

```javascript
// Each team supplies its own work_dir and CIO config at request time
async function runForTeam(team, projectName, requirement) {
  return runTaskWithProgress(
    projectName,
    requirement,
    console.log,
    {
      workDir: `/data/workspace/${team.id}`,
      configJson: JSON.stringify({
        model: team.model,
        api_key: team.apiKey,
        llm_url: team.llmUrl,
      }),
    }
  );
}
```
