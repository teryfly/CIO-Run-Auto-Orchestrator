"""
engine_factory.py — Per-request WorkflowEngine + Scheduler factory with caching.

Motivation
──────────
POST /tasks accepts optional `work_dir` and `config_json` fields that allow
callers to override the global CIO_WORK_DIR / CIO_CONFIG_PATH settings on a
per-task basis.  Because WorkflowEngine and Scheduler are both bound to a
specific work_dir at construction time, a single global engine is no longer
sufficient.

Cache strategy
──────────────
Engines are expensive to construct (disk I/O, config validation).  We cache
them keyed by (work_dir, config_hash) where config_hash is the SHA-256 of the
raw config_json string (or "" when env-var config is used).  The cache is
unbounded but in practice the number of distinct (work_dir, config) combos
is small.

Config merging (v0.6.1)
────────────────────────
When config_json is supplied, its parsed dict is merged with environment-variable
defaults before being handed to CIOConfig.from_dict().  This means:

  • Fields absent from config_json are filled from env vars.
  • Fields present in config_json always win over env vars.
  • The request-level work_dir always wins over both config_json and env vars.
  • Only if model or api_key are still empty after merging is a ValueError raised
    (which the worker catches and converts to task FAILED).
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
    """Return a SHA-256 hex digest of the config_json string."""
    return hashlib.sha256(config_json.encode()).hexdigest()


def _merge_with_env_defaults(
    parsed: Dict[str, Any],
    work_dir: str,
) -> Dict[str, Any]:
    """
    Merge a parsed config_json dict with environment-variable defaults.

    Merge rules
    ───────────
    1. Start with the env-var baseline (same keys used when no config_json
       is supplied).
    2. Overlay with everything from parsed (caller-supplied values win).
    3. Always override work_dir with the request-level value (highest prio).
    4. If model or api_key are empty after merge, raise ValueError so the
       worker can transition the task to FAILED with a clear message.

    Returns
    -------
    dict
        Ready to pass to CIOConfig.from_dict().
    """
    baseline: Dict[str, Any] = {
        "model": settings.cio_model,      # CIO_MODEL  (default "GPT-4.1")
        "api_key": settings.cio_api_key,  # CIO_API_KEY (required env var)
        "llm_url": settings.cio_llm_url,  # CIO_LLM_URL
        "work_dir": work_dir,
        "file_limit": 20,
    }

    # Caller values win over baseline for every key they supply
    merged = {**baseline, **parsed}

    # work_dir from the request parameter always has final say
    merged["work_dir"] = work_dir

    # Strip whitespace so blank strings are caught consistently
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
    """
    Construct a WorkflowEngine and a matching Scheduler.

    Parameters
    ----------
    work_dir:
        The CIO work directory to use (already resolved by the caller).
    config_json:
        Raw JSON string from the request.  When provided, it is parsed,
        merged with env-var defaults, and passed to CIOConfig.from_dict().
        When None, the existing CIO_CONFIG_PATH / env-var path is used.
    """
    from cio.config import CIOConfig
    from cio.logger import CIOLogger
    from cio.project_namer import ProjectNamer
    from cio.project_store import ProjectStore
    from cio.workflow_engine import WorkflowEngine

    if config_json:
        logger.info(
            "engine_factory: building CIOConfig from config_json (work_dir=%s)", work_dir
        )
        parsed: Dict[str, Any] = json.loads(config_json)  # already validated; won't fail
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
    Return a cached (WorkflowEngine, Scheduler) pair for the given parameters.

    Resolution order for work_dir:
        1. `work_dir` request param (non-empty string)
        2. CIO_WORK_DIR environment variable (settings.cio_work_dir)

    Resolution order for config:
        1. `config_json` request param (non-empty)
               → parsed, merged with env-var defaults, CIOConfig.from_dict()
        2. CIO_CONFIG_PATH env var
               → CIOConfig.from_yaml(), then work_dir overlaid
        3. Individual CIO_* env vars
               → CIOConfig.from_dict()

    In all cases the resolved work_dir is overlaid last so the request-level
    value always takes final precedence.
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
        logger.debug(
            "engine_factory: cache hit for work_dir=%r", resolved_work_dir
        )

    return _cache[cache_key]


def clear_cache() -> None:
    """Evict all cached engines. Useful in tests."""
    _cache.clear()
