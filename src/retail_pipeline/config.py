"""Non-secret configuration with strict identifier and path validation."""

import json
import re
from dataclasses import dataclass, fields
from datetime import date
from pathlib import Path


def validate_run_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}", value):
        raise ValueError("run_id must be 1-128 letters, digits, underscores or hyphens")
    return value


def validate_batch_date(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("batch_date must be YYYY-MM-DD")
    date.fromisoformat(value)
    return value


def validate_reject_ratio(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not 0 <= value <= 1:
        raise ValueError("max_reject_ratio must be between 0 and 1")
    return value


@dataclass(frozen=True)
class Config:
    region: str
    stack_name: str
    project_name: str = "retail-pipeline"
    environment: str = "dev"
    max_reject_ratio: float = 0.25
    poll_seconds: int = 15
    timeout_seconds: int = 3600
    deploy_redshift: bool = False
    redshift_database: str = "analytics"
    redshift_workgroup: str = ""
    redshift_secret_arn: str = ""
    redshift_subnet_ids: str = ""
    redshift_security_group_ids: str = ""

    def __post_init__(self):
        for field in fields(self):
            if field.type is str and not isinstance(getattr(self, field.name), str):
                raise ValueError(f"{field.name} must be a string")
        for name in ("stack_name", "project_name", "environment"):
            if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", getattr(self, name)):
                raise ValueError(f"{name} must be a lowercase AWS resource name (max 40 chars)")
        if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-\d", self.region):
            raise ValueError("Invalid AWS region")
        if self.environment not in {"dev", "test", "prod"}:
            raise ValueError("environment must be dev, test or prod")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.redshift_database):
            raise ValueError("Invalid Redshift database identifier")
        if self.redshift_workgroup and not re.fullmatch(
            r"[a-z][a-z0-9-]{0,63}", self.redshift_workgroup
        ):
            raise ValueError("Invalid Redshift workgroup name")
        if self.redshift_secret_arn and not re.fullmatch(
            r"arn:aws(?:-us-gov|-cn)?:secretsmanager:[a-z0-9-]+:\d{12}:secret:[A-Za-z0-9/_+=.@-]+",
            self.redshift_secret_arn,
        ):
            raise ValueError("Invalid Secrets Manager ARN")
        validate_reject_ratio(self.max_reject_ratio)
        for name in ("poll_seconds", "timeout_seconds"):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"{name} must be an integer")
        if self.poll_seconds < 1 or self.timeout_seconds < self.poll_seconds:
            raise ValueError("Polling and timeout must be positive, timeout >= polling")
        if not isinstance(self.deploy_redshift, bool):
            raise ValueError("deploy_redshift must be a JSON boolean")
        if self.deploy_redshift and not (
            self.redshift_subnet_ids and self.redshift_security_group_ids
        ):
            raise ValueError("Redshift deployment requires subnet and security group IDs")
        if self.redshift_subnet_ids and not re.fullmatch(
            r"subnet-[0-9a-f]+(,subnet-[0-9a-f]+){2,}", self.redshift_subnet_ids
        ):
            raise ValueError("redshift_subnet_ids must contain at least three comma-separated IDs")
        if self.redshift_security_group_ids and not re.fullmatch(
            r"sg-[0-9a-f]+(,sg-[0-9a-f]+)*", self.redshift_security_group_ids
        ):
            raise ValueError("redshift_security_group_ids must contain comma-separated IDs")

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        values = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(values, dict):
            raise ValueError("Configuration must be a JSON object")
        unknown = set(values) - {field.name for field in fields(cls)}
        if unknown:
            raise ValueError(
                f"Unknown config keys (credentials are not accepted): {sorted(unknown)}"
            )
        return cls(**values)
