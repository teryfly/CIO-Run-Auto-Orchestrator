"""
engine_factory.py — Per-request WorkflowEngine + Scheduler factory with caching.

v0.6.1: Added EngineFactory class as a proper injectable object.
        The module-level get_engine_and_scheduler() and clear_cache() remain
        for backward compatibility.

Bug fix (v0.6.2)
──────────────────────────────────────────────────────────────────────────────
Fix: work_dir priority not honoured — config_json internal work_dir ignored.

Bug fix (v0.6.3)
──────────────────────────────────────────────────────────────────────────────
Fix: 'Logger' object has no attribute 'correlation_id'

Root cause (revised, v0.6.4):
  CIOLogger wraps a standard Python logging.Logger instance internally.
  WorkflowEngine and StateTracker access `logger.correlation_id` on whatever
  object is passed to them — which may be the raw inner Logger, not the
  CIOLogger wrapper.  Patching only the outer CIOLogger is insufficient.

Fix (v0.6.4):
  _ensure_correlation_id() now injects correlation_id onto:
    1. The CIOLogger wrapper itself.
    2. Any inner `.logger` attribute (the wrapped logging.Logger), which is
       the object actually accessed by WorkflowEngine / StateTracker internals.
  This covers both current and future CIOLogger implementations.

  Additionally, the module-level logging.Logger used by Scheduler is also
  pre-injected so that StateTracker(work_dir, logger=logger) in scheduler.py
  never raises AttributeError.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from typing import Any, Dict, Optional, Tuple

from .config import settings
from .scheduler import Scheduler

logger = logging.getLogger(__name__)

# Pre-inject correlation_id onto this module's logger so it can be safely
# passed to StateTracker (which accesses logger.correlation_id at init time).
if not hasattr(logger, "correlation_id"):
    try:
        logger.correlation_id = uuid.uuid4().hex  # type: ignore[attr-defined]
    except Exception:
        pass

# Cache key: (work_dir: str, config_hash: str)
# Cache value: (WorkflowEngine, Scheduler)
_cache: dict[Tuple[str, str], Tuple[object, Scheduler]] = {}


def _config_hash(config_json: str) -> str:
    return hashlib.sha256(config_json.encode()).hexdigest()


def _extract_work_dir_from_config(config_json: str) -> str:
    """
    Safely extract work_dir from a config_json string.

    Returns an empty string on any error (invalid JSON, missing key, wrong type).
    Never raises.
    """
    try:
        data = json.loads(config_json)
        return (data.get("work_dir") or "").strip()
    except Exception:
        return ""


def _resolve_work_dir(request_work_dir: Optional[str], config_json: str) -> str:
    """
    Resolve the effective work_dir according to documented priority:

      1. request-level work_dir  (non-empty → wins immediately)
      2. work_dir inside config_json
      3. CIO_WORK_DIR env var    (final fallback)
    """
    # Priority 1 — explicit request param
    candidate = (request_work_dir or "").strip()
    if candidate:
        logger.debug("engine_factory: work_dir from request param: %r", candidate)
        return candidate

    # Priority 2 — work_dir inside config_json
    if config_json:
        candidate = _extract_work_dir_from_config(config_json)
        if candidate:
            logger.debug("engine_factory: work_dir from config_json: %r", candidate)
            return candidate

    # Priority 3 — env var / built-in default
    candidate = settings.cio_work_dir
    logger.debug("engine_factory: work_dir from env/default: %r", candidate)
    return candidate


def _merge_with_env_defaults(
    parsed: Dict[str, Any],
    resolved_work_dir: str,
) -> Dict[str, Any]:
    """
    Merge caller-supplied config with environment-variable defaults.

    The `resolved_work_dir` parameter is the already-prioritised work_dir
    (request param > config_json > env var).  It is injected last so that
    it always wins regardless of what was in `parsed["work_dir"]`.
    """
    baseline: Dict[str, Any] = {
        "model": settings.cio_model,
        "api_key": settings.cio_api_key,
        "llm_url": settings.cio_llm_url,
        "work_dir": resolved_work_dir,
        "file_limit": 20,
    }

    # caller values take precedence over baseline …
    merged = {**baseline, **parsed}
    # … except work_dir, which is always the pre-resolved authoritative value.
    merged["work_dir"] = resolved_work_dir

    merged["model"] = (merged.get("model") or "").strip()
    merged["api_key"] = (merged.get("api_key") or "").strip()

    errors = []
    if not merged["model"]:
        errors.append(
            "model: value is empty in config_json and CIO_MODEL env var is not set"
        )
    if not merged["api_key"]:
        errors.append(
            "api_key: value is empty in config_json and CIO_API_KEY env var is not set"
        )
    if errors:
        raise ValueError(
            "config_json cannot be used — required fields missing after env-var fallback:\n"
            + "\n".join(f"  • {e}" for e in errors)
        )

    return merged


def _inject_correlation_id(obj: object, correlation_id: str) -> None:
    """
    Best-effort injection of `correlation_id` onto `obj`.

    Tries three strategies in order:
      1. Direct attribute assignment (works for most objects).
      2. object.__setattr__ override (for objects with custom __setattr__).
      3. Silently gives up — the attribute may already exist via __slots__.
    """
    if hasattr(obj, "correlation_id"):
        return
    try:
        obj.correlation_id = correlation_id  # type: ignore[union-attr]
        return
    except (AttributeError, TypeError):
        pass
    try:
        object.__setattr__(obj, "correlation_id", correlation_id)
    except (AttributeError, TypeError):
        pass


def _ensure_correlation_id(cio_logger: object) -> object:
    """
    Ensure that *both* the CIOLogger wrapper and any inner logging.Logger it
    wraps have a `correlation_id` attribute.

    Background
    ──────────
    CIO-Agent internals (StateTracker, WorkflowEngine) access
    `logger.correlation_id` at construction time.  Depending on the CIOLogger
    implementation, they may receive either the CIOLogger wrapper or the raw
    `logging.Logger` it contains internally (commonly stored as `.logger`).

    Patching only the outer wrapper was insufficient (v0.6.3 regression).
    This version patches both so the attribute is present regardless of which
    object the internals ultimately receive.
    """
    correlation_id = uuid.uuid4().hex

    # 1. Patch the outer CIOLogger wrapper.
    _inject_correlation_id(cio_logger, correlation_id)

    # 2. Patch any inner logger attribute (the wrapped logging.Logger).
    #    Common attribute names: 'logger', '_logger', 'log', '_log'.
    for attr in ("logger", "_logger", "log", "_log"):
        inner = getattr(cio_logger, attr, None)
        if inner is not None and isinstance(inner, logging.Logger):
            _inject_correlation_id(inner, correlation_id)

    return cio_logger


def _build_engine_and_scheduler(
    resolved_work_dir: str,
    config_json: Optional[str],
) -> Tuple[object, Scheduler]:
    from cio.config import CIOConfig
    from cio.logger import CIOLogger
    from cio.project_namer import ProjectNamer
    from cio.project_store import ProjectStore
    from cio.workflow_engine import WorkflowEngine

    if config_json:
        logger.info(
            "engine_factory: building CIOConfig from config_json (work_dir=%s)",
            resolved_work_dir,
        )
        parsed: Dict[str, Any] = json.loads(config_json)
        merged = _merge_with_env_defaults(parsed, resolved_work_dir)
        config = CIOConfig.from_dict(merged)

    elif settings.cio_config_path:
        logger.info(
            "engine_factory: loading CIOConfig from path=%s (work_dir=%s)",
            settings.cio_config_path,
            resolved_work_dir,
        )
        base = CIOConfig.from_yaml(settings.cio_config_path)
        config = CIOConfig.from_dict({**base.to_dict(), "work_dir": resolved_work_dir})

    else:
        logger.info(
            "engine_factory: building CIOConfig from env vars (work_dir=%s)",
            resolved_work_dir,
        )
        config = CIOConfig.from_dict(
            {
                "model": settings.cio_model,
                "api_key": settings.cio_api_key,
                "llm_url": settings.cio_llm_url,
                "work_dir": resolved_work_dir,
                "file_limit": 20,
            }
        )

    config.validate()

    cio_logger = CIOLogger(config.work_dir)
    # Bug fix (v0.6.4): inject correlation_id onto BOTH the CIOLogger wrapper
    # AND its inner logging.Logger, because CIO-Agent internals may access
    # logger.correlation_id on either object depending on implementation.
    _ensure_correlation_id(cio_logger)

    store = ProjectStore(config.work_dir)
    namer = ProjectNamer(config.api_key)
    engine = WorkflowEngine(config, cio_logger, store, namer)
    scheduler = Scheduler(work_dir=config.work_dir)

    return engine, scheduler


def get_engine_and_scheduler(
    work_dir: Optional[str],
    config_json: Optional[str],
) -> Tuple[object, Scheduler]:
    """
    Module-level helper — returns a cached (WorkflowEngine, Scheduler) pair.
    Kept for backward compatibility; prefer EngineFactory.get() in new code.

    work_dir priority (highest → lowest):
      1. `work_dir` parameter (request-level)
      2. work_dir key inside `config_json`
      3. CIO_WORK_DIR env var / built-in default
    """
    resolved_config_json = (config_json or "").strip()

    # Resolve work_dir with correct three-level priority.
    resolved_work_dir = _resolve_work_dir(work_dir, resolved_config_json)

    cache_key = (resolved_work_dir, _config_hash(resolved_config_json))

    if cache_key not in _cache:
        logger.info(
            "engine_factory: cache miss — constructing new engine for work_dir=%r",
            resolved_work_dir,
        )
        pair = _build_engine_and_scheduler(
            resolved_work_dir=resolved_work_dir,
            config_json=resolved_config_json or None,
        )
        _cache[cache_key] = pair
    else:
        logger.debug("engine_factory: cache hit for work_dir=%r", resolved_work_dir)

    return _cache[cache_key]


def clear_cache() -> None:
    """Evict all cached engines. Useful in tests."""
    _cache.clear()


class EngineFactory:
    """
    Injectable factory object used by Worker instances.

    Wraps the module-level get_engine_and_scheduler() cache so workers
    can call `await self._factory.get(work_dir, config_json)`.
    """

    async def get(
        self,
        work_dir: Optional[str],
        config_json: Optional[str],
    ) -> Tuple[object, Scheduler]:
        """Return a (WorkflowEngine, Scheduler) pair for the given parameters."""
        return get_engine_and_scheduler(work_dir=work_dir, config_json=config_json)