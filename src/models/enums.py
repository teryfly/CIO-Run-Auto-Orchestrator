"""
models/enums.py — TaskMode and TaskStatus enumerations.
"""

from __future__ import annotations

from enum import Enum


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
