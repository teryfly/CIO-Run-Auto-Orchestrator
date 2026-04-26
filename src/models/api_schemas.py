"""
models/api_schemas.py — FastAPI request / response schemas and SSE envelope.

v0.5: requirement_file_content removed; requirement optional default "".
v0.6: work_dir / config_json optional fields added to TaskCreateRequest
      and TaskResponse.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config_schema import validate_config_json
from .enums import TaskMode, TaskStatus
from .task import Task


class TaskCreateRequest(BaseModel):
    """
    Body for POST /tasks.

    extra="forbid" ensures the removed field requirement_file_content
    returns 422 when sent.

    v0.6 fields
    ───────────
    work_dir    — optional; overrides CIO_WORK_DIR env var when non-empty.
    config_json — optional; when non-empty replaces CIO_CONFIG_PATH / env-var
                  path. Validated against ConfigJsonSchema before task creation;
                  422 is returned with field-level detail on failure.
    """

    model_config = ConfigDict(extra="forbid")

    project_name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Unique CIO project identifier (slug-style recommended).",
    )
    requirement: str = Field(
        default="",
        description=(
            "Natural-language requirement passed to CIO-Agent. "
            "When empty, automatically resumes from the latest checkpoint. "
            "Returns an error if no checkpoint exists for the project."
        ),
    )
    work_dir: Optional[str] = Field(
        default=None,
        description=(
            "Override the CIO work directory for this task. "
            "When non-empty, takes precedence over CIO_WORK_DIR and over "
            "any work_dir embedded inside config_json."
        ),
    )
    config_json: Optional[str] = Field(
        default=None,
        description=(
            "JSON string passed to CIOConfig.from_json(). "
            "When non-empty, replaces the CIO_CONFIG_PATH / env-var config. "
            "Must contain at minimum: model, api_key. "
            "Validated against ConfigJsonSchema; returns 422 on failure."
        ),
    )

    @model_validator(mode="after")
    def _validate_config_json(self) -> "TaskCreateRequest":
        """Run ConfigJsonSchema validation if config_json is provided."""
        raw = (self.config_json or "").strip()
        if raw:
            try:
                validate_config_json(raw)
            except ValueError as exc:
                raise ValueError(str(exc)) from exc
        return self


class TaskResponse(BaseModel):
    """Serialised view of a Task returned by the API."""

    task_id: str
    project_name: str
    requirement: str
    work_dir: str
    config_json: str
    mode: TaskMode
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    retry_count: int
    status_detail: str

    @classmethod
    def from_task(cls, task: Task) -> "TaskResponse":
        return cls(
            task_id=task.task_id,
            project_name=task.project_name,
            requirement=task.requirement,
            work_dir=task.work_dir,
            config_json=task.config_json,
            mode=task.mode,
            status=task.status,
            created_at=task.created_at,
            updated_at=task.updated_at,
            retry_count=task.retry_count,
            status_detail=task.status_detail,
        )


class SSEPayload(BaseModel):
    """
    JSON body carried inside each SSE `data:` field.

    Maps CIOEvent fields:
      event_type → SSE `event:` line  (handled by sse-starlette)
      message    → payload.message
      metadata   → payload.metadata
      timestamp  → payload.timestamp
    """

    message: str
    metadata: dict = Field(default_factory=dict)
    timestamp: str = ""

    @classmethod
    def from_cio_event(cls, event: object) -> "SSEPayload":
        """Build from a CIOEvent — uses getattr to avoid hard cio imports."""
        return cls(
            message=getattr(event, "message", ""),
            metadata=getattr(event, "metadata", {}) or {},
            timestamp=getattr(event, "timestamp", ""),
        )
