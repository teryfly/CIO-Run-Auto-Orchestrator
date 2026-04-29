"""
engine_factory.py — Per-request WorkflowEngine + Scheduler factory with caching.

v0.6.1: Added EngineFactory class as a proper injectable object.
        The module-level get_engine_and_scheduler() and clear_cache() remain
        for backward compatibility.

Bug fix (v0.6.2)
──────────────────────────────────────────────────────────────────────────────
Fix: work_dir priority not honoured — config_json internal work_dir ignored.

Root cause (two-part):

  1. get_engine_and_scheduler() resolved work_dir as:
         resolved_work_dir = (work_dir or "").strip() or settings.cio_work_dir
     When the request-level work_dir is empty (""), this immediately falls back
     to the CIO_WORK_DIR env var, completely skipping any work_dir embedded
     inside config_json.

  2. _merge_with_env_defaults() contained:
         merged["work_dir"] = work_dir   # last line — always overwrites
     This forced the already-wrong resolved_work_dir onto the merged config,
     discarding whatever work_dir the caller had put inside config_json.

Correct priority order (from api_contract.md / README.md):
  1. Request-level work_dir param  (highest — always wins when non-empty)
  2. work_dir key inside config_json
  3. CIO_WORK_DIR env var          (lowest fallback)

Fixed implementation:
  - get_engine_and_scheduler() now extracts config_json["work_dir"] as an
    intermediate fallback before reaching the env-var default.
  - _merge_with_env_defaults() no longer blindly overwrites merged["work_dir"];
    the final resolved work_dir is already correct when it arrives here and is
    only used to fill the baseline — the merged dict (which may contain a
    caller-supplied work_dir) takes precedence via {**baseline, **parsed},
    after which we apply the single authoritative value passed in.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Optional, Tuple

from .config import settings
from .scheduler import Scheduler

logger = logging.getLogger(__name__)

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

    Parameters
    ----------
    request_work_dir:
        The work_dir supplied at the POST /tasks request level (may be None/"").
    config_json:
        The raw config_json string (may be empty).
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

    Only model and api_key emptiness is validated here — other missing
    fields have sensible built-in defaults inside CIO-Agent.
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

    The `get` method is async-compatible (runs synchronous construction
    in the calling thread — construction is fast after the first build
    because results are cached).
    """

    async def get(
        self,
        work_dir: Optional[str],
        config_json: Optional[str],
    ) -> Tuple[object, Scheduler]:
        """Return a (WorkflowEngine, Scheduler) pair for the given parameters."""
        return get_engine_and_scheduler(work_dir=work_dir, config_json=config_json)