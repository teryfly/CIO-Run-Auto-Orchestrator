"""
engine_factory.py — Per-request WorkflowEngine + Scheduler factory with caching.

v0.6.1: Added EngineFactory class as a proper injectable object.
        The module-level get_engine_and_scheduler() and clear_cache() remain
        for backward compatibility.
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


def _merge_with_env_defaults(
    parsed: Dict[str, Any],
    work_dir: str,
) -> Dict[str, Any]:
    baseline: Dict[str, Any] = {
        "model": settings.cio_model,
        "api_key": settings.cio_api_key,
        "llm_url": settings.cio_llm_url,
        "work_dir": work_dir,
        "file_limit": 20,
    }

    merged = {**baseline, **parsed}
    merged["work_dir"] = work_dir
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
    work_dir: str,
    config_json: Optional[str],
) -> Tuple[object, Scheduler]:
    from cio.config import CIOConfig
    from cio.logger import CIOLogger
    from cio.project_namer import ProjectNamer
    from cio.project_store import ProjectStore
    from cio.workflow_engine import WorkflowEngine

    if config_json:
        logger.info(
            "engine_factory: building CIOConfig from config_json (work_dir=%s)", work_dir
        )
        parsed: Dict[str, Any] = json.loads(config_json)
        merged = _merge_with_env_defaults(parsed, work_dir)
        config = CIOConfig.from_dict(merged)

    elif settings.cio_config_path:
        logger.info(
            "engine_factory: loading CIOConfig from path=%s (work_dir=%s)",
            settings.cio_config_path,
            work_dir,
        )
        base = CIOConfig.from_yaml(settings.cio_config_path)
        config = CIOConfig.from_dict({**base.to_dict(), "work_dir": work_dir})

    else:
        logger.info(
            "engine_factory: building CIOConfig from env vars (work_dir=%s)", work_dir
        )
        config = CIOConfig.from_dict(
            {
                "model": settings.cio_model,
                "api_key": settings.cio_api_key,
                "llm_url": settings.cio_llm_url,
                "work_dir": work_dir,
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
    """
    resolved_work_dir = (work_dir or "").strip() or settings.cio_work_dir
    resolved_config_json = (config_json or "").strip()

    cache_key = (resolved_work_dir, _config_hash(resolved_config_json))

    if cache_key not in _cache:
        logger.info(
            "engine_factory: cache miss — constructing new engine for work_dir=%r",
            resolved_work_dir,
        )
        pair = _build_engine_and_scheduler(
            work_dir=resolved_work_dir,
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
