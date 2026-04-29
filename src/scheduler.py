"""
scheduler.py — Scheduling decision logic.

Implements the core routing rule:

    IF   requirement is empty            → RESUME (no checkpoint → ValueError)
    ELIF project does not exist          → NEW
    ELIF checkpoint exists               → RESUME
    ELSE                                 → SECONDARY

Priority: empty-requirement-RESUME > RESUME > SECONDARY

CTD change (auto_run_stream 接口简化)
──────────────────────────────────────
在 decide() 最开头增加空 requirement 分支：
  - requirement 为空 → 强制 RESUME
  - 若无 checkpoint → 抛 ValueError（由 worker._run_locked 捕获转为 FAILED）

Bug fix (v0.6.2)
──────────────────────────────────────
StateTracker 构造函数第二参数要求一个带 correlation_id 属性的 logger 对象，
传入 None 会在内部访问 logger.correlation_id 时抛出
  AttributeError: 'NoneType' object has no attribute 'correlation_id'
修复方式：将模块级 logger（logging.Logger 实例）传入，替代原来的 None。
"""

from __future__ import annotations

import logging

from cio.project_store import ProjectStore
from cio.state_tracker import StateTracker

from .models import Task, TaskMode

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Determines the execution mode for a task by inspecting CIO-Agent state.

    Parameters
    ----------
    work_dir:
        The CIO work_dir that both ProjectStore and StateTracker use.
        Must match the value in CIOConfig.
    """

    def __init__(self, work_dir: str) -> None:
        self._work_dir = work_dir
        # ProjectStore and StateTracker are lightweight; constructing per
        # Scheduler instance is fine.
        self._project_store = ProjectStore(work_dir)
        # Bug fix: pass the module-level logger instead of None.
        # StateTracker internally accesses logger.correlation_id during
        # initialisation; passing None causes:
        #   AttributeError: 'NoneType' object has no attribute 'correlation_id'
        self._state_tracker = StateTracker(work_dir, logger=logger)  # type: ignore[arg-type]

    # ---------------------------------------------------------------------- #
    # Public API                                                               #
    # ---------------------------------------------------------------------- #

    def decide(self, task: Task) -> TaskMode:
        """
        Inspect CIO state and return the appropriate TaskMode.

        This method is synchronous and cheap (disk reads only).
        It mutates `task.mode` in-place and returns it for convenience.

        Decision table
        ──────────────────────────────────────────────────────────────
        requirement | project_exists | checkpoint_exists | mode
        ────────────┼────────────────┼──────────────────┼──────────
        empty       | —              | True             | RESUME
        empty       | —              | False            | ValueError
        non-empty   | False          | —                | NEW
        non-empty   | True           | True             | RESUME
        non-empty   | True           | False            | SECONDARY
        """
        project_name = task.project_name

        # ── 空 requirement → 强制 RESUME ────────────────────────── #
        if not task.requirement:
            try:
                self._state_tracker.set_project_name(project_name)
                has_checkpoint = self._state_tracker.checkpoint_exists(project_name)
            except Exception as exc:
                logger.warning(
                    "scheduler.decide: checkpoint check failed for %r: %s — treating as no checkpoint",
                    project_name,
                    exc,
                )
                has_checkpoint = False

            if not has_checkpoint:
                raise ValueError(
                    f"requirement 为空，但 project={project_name!r} "
                    "不存在可恢复的 checkpoint，无法执行任何操作"
                )

            logger.info(
                "scheduler.decide: requirement 为空，project=%r 有 checkpoint → mode=RESUME",
                project_name,
            )
            return self._apply(task, TaskMode.RESUME)

        # ── 以下原有逻辑完全不变 ────────────────────────────────── #

        try:
            project_exists = self._project_store.project_exists(project_name)
        except Exception as exc:
            logger.warning(
                "scheduler.decide: project_exists check failed for %r: %s — defaulting to NEW",
                project_name,
                exc,
            )
            project_exists = False

        if not project_exists:
            logger.info(
                "scheduler.decide: project=%r does not exist → mode=NEW", project_name
            )
            return self._apply(task, TaskMode.NEW)

        # Project exists — check for a resumable checkpoint.
        try:
            self._state_tracker.set_project_name(project_name)
            checkpoint_exists = self._state_tracker.checkpoint_exists(project_name)
        except Exception as exc:
            logger.warning(
                "scheduler.decide: checkpoint_exists check failed for %r: %s — defaulting to SECONDARY",
                project_name,
                exc,
            )
            checkpoint_exists = False

        if checkpoint_exists:
            logger.info(
                "scheduler.decide: project=%r has checkpoint → mode=RESUME", project_name
            )
            return self._apply(task, TaskMode.RESUME)

        logger.info(
            "scheduler.decide: project=%r exists, no checkpoint → mode=SECONDARY",
            project_name,
        )
        return self._apply(task, TaskMode.SECONDARY)

    # ---------------------------------------------------------------------- #
    # Internal helpers                                                         #
    # ---------------------------------------------------------------------- #

    @staticmethod
    def _apply(task: Task, mode: TaskMode) -> TaskMode:
        """Write the decided mode onto the task and return it."""
        object.__setattr__(task, "mode", mode)
        task.touch()
        return mode