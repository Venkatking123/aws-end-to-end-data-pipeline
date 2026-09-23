"""Configuration failures are caught before creating AWS resources or jobs."""

import json

import pytest

from retail_pipeline.config import (
    Config,
    validate_batch_date,
    validate_reject_ratio,
    validate_run_id,
)


@pytest.mark.parametrize("value", ["", "../run", "run/a", "run'", "a" * 129, None, 123])
def test_run_id_rejects_unsafe_or_invalid_values(value):
    with pytest.raises(ValueError, match="run_id"):
        validate_run_id(value)


def test_run_id_accepts_bounded_identifier():
    assert validate_run_id("A-2026_01") == "A-2026_01"
    assert validate_run_id("a" * 128) == "a" * 128


@pytest.mark.parametrize("value", ["2026-1-01", "2026-02-30", "2026-01-01/", None])
def test_batch_date_must_be_a_real_iso_date(value):
    with pytest.raises(ValueError):
        validate_batch_date(value)
    assert validate_batch_date("2024-02-29") == "2024-02-29"


@pytest.mark.parametrize("value", [-0.1, 1.1, float("nan"), float("inf"), True, "0.4", None])
def test_reject_ratio_requires_bounded_number(value):
    with pytest.raises(ValueError, match="max_reject_ratio"):
        validate_reject_ratio(value)


@pytest.mark.parametrize("value", [0, 0.4, 1])
def test_reject_ratio_accepts_inclusive_bounds(value):
    assert validate_reject_ratio(value) == value


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"region": "invalid"}, "region"),
        ({"region": None}, "string"),
        ({"project_name": "Uppercase"}, "project_name"),
        ({"environment": "preview"}, "environment"),
        ({"redshift_database": "db'; DROP TABLE x"}, "database"),
        ({"redshift_workgroup": "name/invalid"}, "workgroup"),
        ({"redshift_secret_arn": "password"}, "ARN"),
        ({"poll_seconds": 0}, "Polling"),
        ({"poll_seconds": True}, "integer"),
        ({"poll_seconds": 1.5}, "integer"),
        ({"timeout_seconds": "60"}, "integer"),
        ({"poll_seconds": 30, "timeout_seconds": 15}, "Polling"),
        ({"deploy_redshift": "false"}, "boolean"),
        ({"deploy_redshift": True}, "subnet"),
        ({"redshift_subnet_ids": "subnet-ab,subnet-cd"}, "subnet"),
        ({"redshift_security_group_ids": "sg-ab,garbage"}, "security_group"),
    ],
)
def test_config_rejects_invalid_fields(overrides, message):
    with pytest.raises(ValueError, match=message):
        Config(**{"region": "us-east-1", "stack_name": "retail-dev", **overrides})


def test_config_load_and_private_redshift_network(tmp_path):
    path = tmp_path / "config.json"
    values = {
        "region": "us-east-1",
        "stack_name": "retail-dev",
        "deploy_redshift": True,
        "redshift_subnet_ids": "subnet-ab,subnet-cd,subnet-ef",
        "redshift_security_group_ids": "sg-ab,sg-cd",
    }
    path.write_text(json.dumps(values), encoding="utf-8")
    config = Config.load(path)
    assert config.deploy_redshift is True
    assert config.max_reject_ratio == 0.25
    assert config.redshift_subnet_ids == values["redshift_subnet_ids"]


@pytest.mark.parametrize(
    "values, message",
    [([], "JSON object"), (None, "JSON object"), ({"aws_secret_access_key": "x"}, "Unknown")],
)
def test_config_load_rejects_nonobjects_and_credentials(tmp_path, values, message):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        Config.load(path)
