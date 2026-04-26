"""
config.py — Centralised settings for CIO Orchestrator.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    cio_api_key: str = field(default_factory=lambda: os.environ["CIO_API_KEY"])
    cio_model: str = field(default_factory=lambda: os.getenv("CIO_MODEL", "GPT-4.1"))
    cio_llm_url: str = field(
        default_factory=lambda: os.getenv("CIO_LLM_URL", "https://api.poe.com")
    )
    cio_work_dir: str = field(
        default_factory=lambda: os.getenv("CIO_WORK_DIR", "./workspace/solutions")
    )
    cio_config_path: str = field(
        default_factory=lambda: os.getenv("CIO_CONFIG_PATH", "")
    )
    worker_concurrency: int = field(
        default_factory=lambda: int(os.getenv("WORKER_CONCURRENCY", "4"))
    )
    worker_poll_interval: float = field(
        default_factory=lambda: float(os.getenv("WORKER_POLL_INTERVAL", "0.5"))
    )
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("MAX_RETRIES", "3"))
    )
    api_host: str = field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    api_port: int = field(
        default_factory=lambda: int(os.getenv("API_PORT", "1577"))
    )
    redis_url: str = field(
        default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    lock_ttl: int = field(
        default_factory=lambda: int(os.getenv("LOCK_TTL", "3600"))
    )
    lock_heartbeat_interval: float = field(
        default_factory=lambda: float(
            os.getenv(
                "LOCK_HEARTBEAT_INTERVAL",
                str(int(os.getenv("LOCK_TTL", "3600")) / 3),
            )
        )
    )
    stream_timeout: float = field(
        default_factory=lambda: float(os.getenv("STREAM_TIMEOUT", "0"))
    )
    sse_keepalive_interval: float = field(
        default_factory=lambda: float(os.getenv("SSE_KEEPALIVE_INTERVAL", "20.0"))
    )
    sse_max_duration: float = field(
        default_factory=lambda: float(os.getenv("SSE_MAX_DURATION", "0"))
    )


settings = Settings()
