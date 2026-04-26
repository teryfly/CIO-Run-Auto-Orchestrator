"""
models/task.py — Canonical Task record stored in Redis.

v0.6 additions
──────────────
work_dir   : overrides CIO_WORK_DIR when non-empty
config_json: raw JSON string; when non-empty, CIOConfig is built via
             CIOConfig.from_json() instead of the yaml/env-var path
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field

from .enums import TaskMode, TaskStatus


class Task(BaseModel):
    """
    Canonical task record — serialised to/from Redis as JSON.

    Pydantic's model_dump_json / model_validate_json handle serialisation.
    New fields (work_dir, config_json) default to "" so existing Redis
    records without these keys deserialise correctly (backward-compatible).
    """

    task_id: str = Field(default_factory=lambda: uuid4().hex)
    project_name: str
    requirement: str = ""

    # Dynamic engine configuration (v0.6)
    work_dir: str = ""
    config_json: str = ""

    mode: TaskMode = TaskMode.UNKNOWN
    status: TaskStatus = TaskStatus.PENDING

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    retry_count: int = 0
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
        return self.status in (TaskStatus.SUCCESS, TaskStatus.FAILED)

    def is_resumable(self) -> bool:
        """Return True if CIO may be able to resume via checkpoint."""
        return self.status == TaskStatus.INTERRUPTED
