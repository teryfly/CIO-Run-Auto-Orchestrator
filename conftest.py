"""
Root-level conftest.py — stubs external dependencies (cio-agent, redis)
before any test module imports src code.

This file runs before any test collection, so sys.modules stubs are in
place before `from src.scheduler import Scheduler` etc. are executed.

v0.6 additions
──────────────
- CIOConfig stub: added from_json(), to_dict(), to_json() methods so that
  engine_factory tests can exercise the config_json path without a real
  cio-agent install.
"""

from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock


def _make_module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# --------------------------------------------------------------------------- #
# Stub: cio.*                                                                  #
# --------------------------------------------------------------------------- #

# Base cio package
cio_mod = _make_module("cio")

# cio.config — CIOConfig with all construction paths stubbed
class _CIOConfig:
    """Minimal CIOConfig stub supporting all construction paths used in v0.6."""

    def __init__(self, data: dict):
        self._data = data
        # Expose commonly accessed attributes
        self.work_dir = data.get("work_dir", "./workspace/solutions")
        self.api_key = data.get("api_key", "stub-key")
        self.model = data.get("model", "GPT-4.1")
        self.llm_url = data.get("llm_url", "https://api.poe.com")

    @classmethod
    def from_yaml(cls, path: str) -> "_CIOConfig":
        return cls({"work_dir": "./workspace/solutions"})

    @classmethod
    def from_dict(cls, data: dict) -> "_CIOConfig":
        return cls(data)

    @classmethod
    def from_json(cls, json_str: str) -> "_CIOConfig":
        return cls(json.loads(json_str))

    def to_dict(self) -> dict:
        return dict(self._data)

    def to_json(self) -> str:
        return json.dumps(self._data)

    def validate(self) -> None:
        pass  # no-op in tests


_make_module("cio.config", CIOConfig=_CIOConfig)

# cio.logger
_make_module("cio.logger", CIOLogger=MagicMock())

# cio.project_store
class _ProjectStore:
    def __init__(self, *a, **kw): pass
    def project_exists(self, name): return False
    def get_project_dir(self, name): return None
    def init_project(self, name): return None
    def list_projects(self): return []

_make_module("cio.project_store", ProjectStore=_ProjectStore)

# cio.project_namer
_make_module("cio.project_namer", ProjectNamer=MagicMock())

# cio.state_tracker
class _StateTracker:
    def __init__(self, *a, **kw): pass
    def set_project_name(self, name): pass
    def checkpoint_exists(self, name): return False
    def load_checkpoint(self, name): return False

_make_module("cio.state_tracker", StateTracker=_StateTracker)

# cio.workflow_engine
_make_module("cio.workflow_engine", WorkflowEngine=MagicMock())

# cio.errors
class _FatalError(Exception): pass
class _RetriableError(Exception): pass
class _CIOError(Exception): pass

_make_module(
    "cio.errors",
    CIOError=_CIOError,
    FatalError=_FatalError,
    RetriableError=_RetriableError,
)

# Make cio.errors accessible as attributes on the cio package too
cio_mod.errors = sys.modules["cio.errors"]

# --------------------------------------------------------------------------- #
# Stub: redis.asyncio                                                          #
# --------------------------------------------------------------------------- #

redis_mod = _make_module("redis")
redis_asyncio = _make_module("redis.asyncio", Redis=MagicMock(), from_url=MagicMock())
redis_mod.asyncio = redis_asyncio

# --------------------------------------------------------------------------- #
# Stub: sse_starlette                                                          #
# --------------------------------------------------------------------------- #

sse_mod = _make_module("sse_starlette")
_make_module("sse_starlette.sse", EventSourceResponse=MagicMock())
