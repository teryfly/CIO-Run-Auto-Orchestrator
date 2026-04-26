"""
models/config_schema.py — Structural validator for the config_json request field.

Design principles (v0.6.1)
──────────────────────────
1. Structure validation only
   Validates field *types* and *nesting* against the CIO-Agent default config
   schema (see cio-agent default_config.yaml).  Unknown extra keys are allowed
   so callers can include any CIOConfig field without being rejected.

2. Missing values are fine — env-vars are the fallback
   No field is required in config_json.  If `model` or `api_key` are absent
   (or blank), engine_factory fills them from CIO_MODEL / CIO_API_KEY before
   constructing the engine.

3. Only truly empty final values are fatal
   validate_config_json() itself does NOT raise on missing model/api_key.
   That check lives in engine_factory._merge_with_env_defaults(), which runs
   after the merge and raises ValueError (→ task FAILED) if the combined
   value is still empty.

4. Type errors are caught early at the API boundary (→ 422)
   Wrong types (e.g. validation.max_fix_rounds: "three") are rejected here
   with field-level detail so the caller gets immediate feedback.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# --------------------------------------------------------------------------- #
# Nested sub-schemas (structural only — all fields optional)                   #
# --------------------------------------------------------------------------- #

class _ModelsSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    cio_naming_model: Optional[str] = None
    cio_decision_model: Optional[str] = None
    cio_executor_model: Optional[str] = None
    architect_model: Optional[str] = None
    engineer_model: Optional[str] = None
    documenter_model: Optional[str] = None


class _ValidationSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    validate_after_run: Optional[bool] = None
    max_fix_rounds: Optional[int] = None
    model: Optional[str] = None
    step_filter: Optional[List[str]] = None
    stdout_preview_limit: Optional[int] = None
    target_coverage: Optional[int] = None

    @field_validator("target_coverage", mode="before")
    @classmethod
    def _coverage_range(cls, v: Any) -> Any:
        if v is not None and not (0 <= int(v) <= 100):
            raise ValueError("target_coverage must be between 0 and 100")
        return v


class _ClaudeMdSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: Optional[bool] = None
    model: Optional[str] = None
    memory_model: Optional[str] = None


class _GitUserSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: Optional[str] = None
    email: Optional[str] = None


class _GitLabSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    token: Optional[str] = None
    base_url: Optional[str] = None
    namespace: Optional[str] = None
    branch: Optional[str] = None


_VALID_PUSH_STRATEGIES = {"never", "on_complete", "on_phase", "manual"}
_VALID_BRANCH_STRATEGIES = {"feature_branch", "direct_main"}


class _GitSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: Optional[bool] = None
    user: Optional[_GitUserSchema] = None
    gitlab: Optional[_GitLabSchema] = None
    push_strategy: Optional[str] = None
    branch_strategy: Optional[str] = None
    feature_branch_prefix: Optional[str] = None
    init_on_new_project: Optional[bool] = None
    commit_on_phase: Optional[bool] = None
    tag_on_validate: Optional[bool] = None
    gitignore_cio_logs: Optional[bool] = None

    @field_validator("push_strategy", mode="before")
    @classmethod
    def _push_strategy_enum(cls, v: Any) -> Any:
        if v is not None and v not in _VALID_PUSH_STRATEGIES:
            raise ValueError(
                f"push_strategy must be one of {sorted(_VALID_PUSH_STRATEGIES)}"
            )
        return v

    @field_validator("branch_strategy", mode="before")
    @classmethod
    def _branch_strategy_enum(cls, v: Any) -> Any:
        if v is not None and v not in _VALID_BRANCH_STRATEGIES:
            raise ValueError(
                f"branch_strategy must be one of {sorted(_VALID_BRANCH_STRATEGIES)}"
            )
        return v


# --------------------------------------------------------------------------- #
# Top-level schema                                                              #
# --------------------------------------------------------------------------- #

class ConfigJsonSchema(BaseModel):
    """
    Structural schema for the `config_json` field in POST /tasks.

    All fields are optional here.  Missing model / api_key are filled from
    environment variables by engine_factory before engine construction.
    Only type / enum / range errors are rejected at this layer (→ 422).
    """

    model_config = ConfigDict(extra="allow")

    # Core LLM settings — optional; env-var fallback applied in engine_factory
    model: Optional[str] = None
    api_key: Optional[str] = None
    llm_url: Optional[str] = None

    # CIO settings
    work_dir: Optional[str] = None
    file_limit: Optional[int] = None
    architect_prompt: Optional[str] = None
    engineer_prompt: Optional[str] = None
    claude_alias: Optional[str] = None
    cio_prompts: Optional[Dict[str, Any]] = None
    execution_context_max_turns: Optional[int] = None
    execution_context_content_limit: Optional[int] = None

    # Nested sections
    models: Optional[_ModelsSchema] = None
    validation: Optional[_ValidationSchema] = None
    claude_md: Optional[_ClaudeMdSchema] = None
    git: Optional[_GitSchema] = None


# --------------------------------------------------------------------------- #
# Public validator called by TaskCreateRequest                                  #
# --------------------------------------------------------------------------- #

def validate_config_json(config_json: str) -> None:
    """
    Parse `config_json` as JSON and validate its structure against
    ConfigJsonSchema.

    Only raises ValueError for:
      - invalid JSON syntax
      - wrong field types / out-of-range values / unknown enum values

    Missing model / api_key are NOT an error here; engine_factory merges
    them from environment variables and raises only if the final merged
    value is still empty.

    Raises
    ------
    ValueError
        Human-readable message with field-level detail, suitable for a
        422 Unprocessable Entity response.
    """
    try:
        data: Dict[str, Any] = json.loads(config_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"config_json is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("config_json must be a JSON object (dict), not a list or scalar")

    from pydantic import ValidationError

    try:
        ConfigJsonSchema.model_validate(data)
    except ValidationError as exc:
        lines = []
        for err in exc.errors():
            loc = " → ".join(str(p) for p in err["loc"]) if err["loc"] else "(root)"
            lines.append(f"  • {loc}: {err['msg']}")
        raise ValueError(
            "config_json failed schema validation:\n" + "\n".join(lines)
        ) from exc
