"""
test_models.py — Unit tests for src/models.py

Coverage targets:
- Task creation defaults
- Task.touch(), transition(), is_terminal(), is_resumable()
- TaskCreateRequest validation (project_name required, requirement optional)
- TaskCreateRequest rejects requirement_file_content (422 equivalent)
- TaskResponse.from_task()
- SSEPayload.from_cio_event()
- TaskMode / TaskStatus enums
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    SSEPayload,
    Task,
    TaskCreateRequest,
    TaskMode,
    TaskResponse,
    TaskStatus,
)


# --------------------------------------------------------------------------- #
# Task                                                                          #
# --------------------------------------------------------------------------- #


class TestTask:
    def test_defaults(self):
        task = Task(project_name="proj")
        assert task.requirement == ""
        assert task.status == TaskStatus.PENDING
        assert task.mode == TaskMode.UNKNOWN
        assert task.retry_count == 0
        assert task.status_detail == ""
        assert len(task.task_id) == 32  # uuid4().hex

    def test_requirement_provided(self):
        task = Task(project_name="proj", requirement="Do something")
        assert task.requirement == "Do something"

    def test_touch_updates_timestamp(self):
        task = Task(project_name="proj")
        old_ts = task.updated_at
        import time; time.sleep(0.01)
        task.touch()
        assert task.updated_at > old_ts

    def test_transition_updates_status_and_detail(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.RUNNING, detail="mode=new")
        assert task.status == TaskStatus.RUNNING
        assert task.status_detail == "mode=new"

    def test_transition_without_detail(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.SUCCESS)
        assert task.status == TaskStatus.SUCCESS
        assert task.status_detail == ""

    def test_is_terminal_success(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.SUCCESS)
        assert task.is_terminal() is True

    def test_is_terminal_failed(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.FAILED, detail="boom")
        assert task.is_terminal() is True

    def test_is_terminal_running(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.RUNNING)
        assert task.is_terminal() is False

    def test_is_terminal_pending(self):
        task = Task(project_name="proj")
        assert task.is_terminal() is False

    def test_is_terminal_interrupted(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.INTERRUPTED)
        assert task.is_terminal() is False

    def test_is_resumable_interrupted(self):
        task = Task(project_name="proj")
        task.transition(TaskStatus.INTERRUPTED)
        assert task.is_resumable() is True

    def test_is_resumable_other_statuses(self):
        for status in (TaskStatus.PENDING, TaskStatus.RUNNING,
                       TaskStatus.SUCCESS, TaskStatus.FAILED):
            task = Task(project_name="proj")
            task.transition(status)
            assert task.is_resumable() is False

    def test_unique_task_ids(self):
        ids = {Task(project_name="p").task_id for _ in range(100)}
        assert len(ids) == 100

    def test_json_round_trip(self):
        task = Task(project_name="proj", requirement="req")
        restored = Task.model_validate_json(task.model_dump_json())
        assert restored.task_id == task.task_id
        assert restored.requirement == task.requirement


# --------------------------------------------------------------------------- #
# TaskCreateRequest                                                             #
# --------------------------------------------------------------------------- #


class TestTaskCreateRequest:
    def test_project_name_required(self):
        with pytest.raises(ValidationError):
            TaskCreateRequest()

    def test_project_name_empty_rejected(self):
        with pytest.raises(ValidationError):
            TaskCreateRequest(project_name="")

    def test_project_name_too_long_rejected(self):
        with pytest.raises(ValidationError):
            TaskCreateRequest(project_name="x" * 129)

    def test_requirement_optional_defaults_empty(self):
        req = TaskCreateRequest(project_name="proj")
        assert req.requirement == ""

    def test_requirement_can_be_set(self):
        req = TaskCreateRequest(project_name="proj", requirement="do it")
        assert req.requirement == "do it"

    def test_requirement_file_content_rejected(self):
        """Passing the removed field should raise ValidationError (extra field)."""
        with pytest.raises(ValidationError):
            TaskCreateRequest(
                project_name="proj",
                requirement="something",
                requirement_file_content="some content",
            )

    def test_valid_minimal(self):
        req = TaskCreateRequest(project_name="my-proj")
        assert req.project_name == "my-proj"


# --------------------------------------------------------------------------- #
# TaskResponse                                                                  #
# --------------------------------------------------------------------------- #


class TestTaskResponse:
    def test_from_task(self, sample_task):
        resp = TaskResponse.from_task(sample_task)
        assert resp.task_id == sample_task.task_id
        assert resp.project_name == sample_task.project_name
        assert resp.requirement == sample_task.requirement
        assert resp.mode == TaskMode.UNKNOWN
        assert resp.status == TaskStatus.PENDING
        assert resp.retry_count == 0

    def test_from_task_after_transition(self, sample_task):
        sample_task.transition(TaskStatus.RUNNING, detail="mode=new")
        object.__setattr__(sample_task, "mode", TaskMode.NEW)
        resp = TaskResponse.from_task(sample_task)
        assert resp.status == TaskStatus.RUNNING
        assert resp.mode == TaskMode.NEW
        assert resp.status_detail == "mode=new"


# --------------------------------------------------------------------------- #
# SSEPayload                                                                    #
# --------------------------------------------------------------------------- #


class TestSSEPayload:
    def test_from_cio_event(self):
        class FakeEvent:
            event_type = "step_start"
            message = "hello"
            metadata = {"phase": 1}
            timestamp = "2025-01-01T00:00:00Z"

        payload = SSEPayload.from_cio_event(FakeEvent())
        assert payload.message == "hello"
        assert payload.metadata == {"phase": 1}
        assert payload.timestamp == "2025-01-01T00:00:00Z"

    def test_from_cio_event_missing_fields(self):
        payload = SSEPayload.from_cio_event(object())
        assert payload.message == ""
        assert payload.metadata == {}
        assert payload.timestamp == ""

    def test_from_cio_event_none_metadata(self):
        class FakeEvent:
            message = "hi"
            metadata = None
            timestamp = ""

        payload = SSEPayload.from_cio_event(FakeEvent())
        assert payload.metadata == {}


# --------------------------------------------------------------------------- #
# Enums                                                                         #
# --------------------------------------------------------------------------- #


class TestEnums:
    def test_task_mode_values(self):
        assert TaskMode.NEW == "new"
        assert TaskMode.RESUME == "resume"
        assert TaskMode.SECONDARY == "secondary"
        assert TaskMode.UNKNOWN == "unknown"

    def test_task_status_values(self):
        assert TaskStatus.PENDING == "pending"
        assert TaskStatus.RUNNING == "running"
        assert TaskStatus.SUCCESS == "success"
        assert TaskStatus.FAILED == "failed"
        assert TaskStatus.INTERRUPTED == "interrupted"
