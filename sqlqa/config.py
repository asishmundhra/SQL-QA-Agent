from pathlib import Path
from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator


class Targets(BaseModel):
    include_paths: List[str] = Field(default_factory=lambda: ["**/*.py", "**/*.sql"])
    exclude_paths: List[str] = Field(default_factory=lambda: ["tests/**", "migrations/**"])


class StaticRuleSettings(BaseModel):
    forbid_select_star: bool = True
    forbid_update_delete_without_where: bool = True
    forbid_leading_wildcard_like: bool = True
    max_in_list: int = 100
    require_explicit_columns_in_joins: bool = True


SeverityLevel = Literal["info", "warning", "error"]


class SeverityConfig(BaseModel):
    select_star: SeverityLevel = "warning"
    update_delete_without_where: SeverityLevel = "error"
    leading_wildcard_like: SeverityLevel = "warning"
    non_sargable_predicate: SeverityLevel = "warning"
    long_in_list: SeverityLevel = "info"


class PolicyConfig(BaseModel):
    targets: Targets = Field(default_factory=Targets)
    static_rules: StaticRuleSettings = Field(default_factory=StaticRuleSettings)
    severity: SeverityConfig = Field(default_factory=SeverityConfig)

    @model_validator(mode="after")
    def validate_max_in_list(self) -> "PolicyConfig":
        if self.static_rules.max_in_list < 1:
            raise ValueError("static_rules.max_in_list must be >= 1")
        return self


def load_config(policy_path: str) -> PolicyConfig:
    """Load and validate YAML policy config."""
    path = Path(policy_path)
    if not path.exists():
        raise FileNotFoundError(f"Policy file not found: {policy_path}")
    raw = yaml.safe_load(path.read_text()) or {}
    try:
        return PolicyConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid policy config: {exc}") from exc


__all__ = [
    "PolicyConfig",
    "Targets",
    "StaticRuleSettings",
    "SeverityConfig",
    "load_config",
]
