"""
test_scheduler.py — Unit tests for src/scheduler.py

Coverage targets:
- Empty requirement + checkpoint exists → RESUME
- Empty requirement + no checkpoint → ValueError
- Empty requirement + checkpoint check throws → ValueError (no checkpoint)
- Non-empty, project missing → NEW
- Non-empty, project exists + checkpoint → RESUME
- Non-empty, project exists + no checkpoint → SECONDARY
- project_exists raises → defaults to NEW
- checkpoint_exists raises → defaults to SECONDARY
- _apply() sets task.mode correctly
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.models import Task, TaskMode
from src.scheduler import Scheduler


def make_scheduler(project_exists: bool, checkpoint_exists: bool,
                   project_raises=False, checkpoint_raises=False) -> Scheduler:
    """Helper: build a Scheduler with mocked CIO stores."""
    s = Scheduler.__new__(Scheduler)
    s._work_dir = "/fake/workdir"

    mock_ps = MagicMock()
    if project_raises:
        mock_ps.project_exists.side_effect = RuntimeError("store down")
    else:
        mock_ps.project_exists.return_value = project_exists
    s._project_store = mock_ps

    mock_st = MagicMock()
    if checkpoint_raises:
        mock_st.checkpoint_exists.side_effect = RuntimeError("tracker down")
    else:
        mock_st.checkpoint_exists.return_value = checkpoint_exists
    s._state_tracker = mock_st

    return s


class TestSchedulerEmptyRequirement:
    def test_empty_req_with_checkpoint_returns_resume(self, resume_task):
        s = make_scheduler(project_exists=True, checkpoint_exists=True)
        mode = s.decide(resume_task)
        assert mode == TaskMode.RESUME
        assert resume_task.mode == TaskMode.RESUME

    def test_empty_req_no_checkpoint_raises_value_error(self, resume_task):
        s = make_scheduler(project_exists=True, checkpoint_exists=False)
        with pytest.raises(ValueError, match="checkpoint"):
            s.decide(resume_task)

    def test_empty_req_checkpoint_check_raises_value_error(self, resume_task):
        """If checkpoint check throws, treat as no checkpoint → ValueError."""
        s = make_scheduler(project_exists=True, checkpoint_exists=False,
                           checkpoint_raises=True)
        with pytest.raises(ValueError, match="checkpoint"):
            s.decide(resume_task)

    def test_empty_req_error_message_contains_project_name(self, resume_task):
        s = make_scheduler(project_exists=True, checkpoint_exists=False)
        with pytest.raises(ValueError) as exc_info:
            s.decide(resume_task)
        assert "existing-project" in str(exc_info.value)

    def test_empty_req_skips_project_exists_check(self, resume_task):
        """project_exists should NOT be called for empty-requirement tasks."""
        s = make_scheduler(project_exists=True, checkpoint_exists=True)
        s.decide(resume_task)
        s._project_store.project_exists.assert_not_called()


class TestSchedulerNonEmptyRequirement:
    def test_project_not_exists_returns_new(self, sample_task):
        s = make_scheduler(project_exists=False, checkpoint_exists=False)
        mode = s.decide(sample_task)
        assert mode == TaskMode.NEW
        assert sample_task.mode == TaskMode.NEW

    def test_project_exists_checkpoint_exists_returns_resume(self, sample_task):
        s = make_scheduler(project_exists=True, checkpoint_exists=True)
        mode = s.decide(sample_task)
        assert mode == TaskMode.RESUME
        assert sample_task.mode == TaskMode.RESUME

    def test_project_exists_no_checkpoint_returns_secondary(self, sample_task):
        s = make_scheduler(project_exists=True, checkpoint_exists=False)
        mode = s.decide(sample_task)
        assert mode == TaskMode.SECONDARY
        assert sample_task.mode == TaskMode.SECONDARY

    def test_project_store_raises_defaults_to_new(self, sample_task):
        s = make_scheduler(project_exists=False, checkpoint_exists=False,
                           project_raises=True)
        mode = s.decide(sample_task)
        assert mode == TaskMode.NEW

    def test_checkpoint_raises_defaults_to_secondary(self, sample_task):
        s = make_scheduler(project_exists=True, checkpoint_exists=False,
                           checkpoint_raises=True)
        mode = s.decide(sample_task)
        assert mode == TaskMode.SECONDARY

    def test_decides_mutates_task_mode(self, sample_task):
        s = make_scheduler(project_exists=False, checkpoint_exists=False)
        assert sample_task.mode == TaskMode.UNKNOWN
        s.decide(sample_task)
        assert sample_task.mode == TaskMode.NEW

    def test_decides_updates_task_timestamp(self, sample_task):
        import time
        s = make_scheduler(project_exists=False, checkpoint_exists=False)
        old_ts = sample_task.updated_at
        time.sleep(0.01)
        s.decide(sample_task)
        assert sample_task.updated_at > old_ts


class TestApply:
    def test_apply_sets_mode(self, sample_task):
        Scheduler._apply(sample_task, TaskMode.SECONDARY)
        assert sample_task.mode == TaskMode.SECONDARY

    def test_apply_returns_mode(self, sample_task):
        result = Scheduler._apply(sample_task, TaskMode.RESUME)
        assert result == TaskMode.RESUME
