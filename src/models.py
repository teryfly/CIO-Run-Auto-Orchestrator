"""
models.py — Core data structures for the CIO Orchestrator.

Defines:
  - TaskMode   : execution path chosen by the scheduler
  - TaskStatus : state machine for a task's lifecycle
  - Task       : the canonical task record (stored in-memory Phase 1, Redis Phase 2)
  - TaskCreateRequest : FastAPI request body schema
  - TaskResponse      : FastAPI response schema

CTD change (auto_run_stream 接口简化)
──────────────────────────────────────
- TaskCreateRequest: 移除 requirement_file_content；requirement 改为可选默认 ""
- Task: 移除 requirement_file_content 字段
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


# --------------------------------------------------------------------------- #
# Enums                                                                        #
# --------------------------------------------------------------------------- #


class TaskMode(str, Enum):
    """Execution path selected by the Scheduler."""

    NEW = "new"
    RESUME = "resume"
    SECONDARY = "secondary"
    UNKNOWN = "unknown"  # set before scheduling decision is made


class TaskStatus(str, Enum):
    """
    State machine:

        PENDING ──► RUNNING ──► SUCCESS
                            ──► FAILED
                            ──► INTERRUPTED   (checkpoint exists → resumable)
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    INTERRUPTED = "interrupted"  # stopped mid-way; CIO checkpoint present


# --------------------------------------------------------------------------- #
# Core task record                                                              #
# --------------------------------------------------------------------------- #


class Task(BaseModel):
    """
    Canonical task record.

    In Phase 1 this lives in an in-process dict.
    In Phase 2 it will be serialised to/from Redis as JSON.
    """

    task_id: str = Field(default_factory=lambda: uuid4().hex)
    project_name: str
    requirement: str = ""

    mode: TaskMode = TaskMode.UNKNOWN
    status: TaskStatus = TaskStatus.PENDING

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # How many automatic retries have been attempted
    retry_count: int = 0

    # Human-readable reason for the last status transition (e.g. error message)
    status_detail: str = ""

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def touch(self) -> None:
        """Update the `updated_at` timestamp in-place."""
        object.__setattr__(self, "updated_at", datetime.now(timezone.utc))

    def transition(self, new_status: TaskStatus, detail: str = "") -> None:
        """Apply a status transition and refresh the timestamp."""
        object.__setattr__(self, "status", new_status)
        object.__setattr__(self, "status_detail", detail)
        self.touch()

    def is_terminal(self) -> bool:
        """Return True if no further status transitions are expected."""
        return self.status in (
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
        )

    def is_resumable(self) -> bool:
        """Return True if CIO may be able to resume via checkpoint."""
        return self.status == TaskStatus.INTERRUPTED


# --------------------------------------------------------------------------- #
# API schemas                                                                  #
# --------------------------------------------------------------------------- #


class TaskCreateRequest(BaseModel):
    """
    Body for POST /tasks.

    CTD change: requirement_file_content 已移除；requirement 现为可选，默认 ""。
    当 requirement 为空时，Scheduler 强制走 RESUME 路径；若无 checkpoint 则任务
    转为 FAILED 并携带明确错误说明。

    extra="forbid" 确保传入已删除的 requirement_file_content 字段时返回 422。
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


class TaskResponse(BaseModel):
    """Serialised view of a Task returned by the API."""

    task_id: str
    project_name: str
    requirement: str
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
            mode=task.mode,
            status=task.status,
            created_at=task.created_at,
            updated_at=task.updated_at,
            retry_count=task.retry_count,
            status_detail=task.status_detail,
        )


# --------------------------------------------------------------------------- #
# SSE event envelope                                                           #
# --------------------------------------------------------------------------- #


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
        """
        Build from a CIOEvent instance.
        Uses getattr so this module has no hard import of cio internals.
        """
        return cls(
            message=getattr(event, "message", ""),
            metadata=getattr(event, "metadata", {}) or {},
            timestamp=getattr(event, "timestamp", ""),
        )
