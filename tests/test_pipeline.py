"""Publication gates and persisted outputs from the shared Spark runner."""

import json
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from retail_pipeline.pipeline import QualityError, check_quality, execute
from retail_pipeline.schemas import GOLD_SCHEMA, SILVER_SCHEMA

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_INPUT = PROJECT_ROOT / "data/raw/orders.csv"


@pytest.mark.parametrize(
    ("input_rows", "rejected_rows", "valid_rows", "maximum", "expected"),
    [(20, 8, 10, 0.4, 0.4), (1, 0, 1, 0.0, 0.0), (5, 4, 1, 1.0, 0.8)],
)
def test_quality_gate_accepts_inclusive_boundary(
    input_rows, rejected_rows, valid_rows, maximum, expected
):
    assert check_quality(input_rows, rejected_rows, valid_rows, maximum) == expected


@pytest.mark.parametrize(
    ("input_rows", "rejected_rows", "valid_rows", "maximum", "message"),
    [
        (0, 0, 0, 1.0, "Empty input snapshot"),
        (20, 8, 10, 0.39, "Quality gate failed"),
        (2, 2, 0, 1.0, "Quality gate failed"),
    ],
)
def test_quality_gate_rejects_unpublishable_snapshot(
    input_rows, rejected_rows, valid_rows, maximum, message
):
    with pytest.raises(QualityError, match=message):
        check_quality(input_rows, rejected_rows, valid_rows, maximum)


@pytest.mark.parametrize("maximum", [-0.01, 1.01, float("nan"), float("inf"), True])
def test_quality_gate_rejects_invalid_threshold(maximum):
    with pytest.raises(ValueError, match="max_reject_ratio"):
        check_quality(1, 0, 1, maximum)


@pytest.mark.spark
def test_execute_persists_sample_datasets_and_exact_metrics(spark, tmp_path):
    expected = json.loads((PROJECT_ROOT / "data/expected/sample_metrics.json").read_text())
    run_id = "sample-publication"
    metrics = execute(spark, SAMPLE_INPUT.as_uri(), tmp_path.as_uri(), run_id, 0.4)

    assert metrics.as_dict() == {
        "run_id": run_id,
        "input_rows": expected["input_count"],
        "rejected_rows": expected["rejected_count"],
        "duplicate_rows": expected["duplicate_count"],
        "valid_rows": expected["valid_count"],
        "gold_rows": expected["gold_count"],
        "reject_ratio": 0.4,
        "net_amount": expected["net_amount"],
    }
    paths = {
        "silver": tmp_path / "silver/order_lines" / f"run_id={run_id}" / "data",
        "gold": tmp_path / "gold/daily_sales" / f"run_id={run_id}" / "data",
        "rejected": tmp_path / "quarantine" / f"run_id={run_id}" / "data",
        "duplicates": tmp_path / "audit/duplicates" / f"run_id={run_id}" / "data",
    }
    frames = {name: spark.read.parquet(path.as_uri()) for name, path in paths.items()}
    assert frames["silver"].count() == expected["valid_count"]
    assert frames["gold"].count() == expected["gold_count"]
    assert frames["rejected"].count() == expected["rejected_count"]
    assert frames["duplicates"].count() == expected["duplicate_count"]
    for name, schema in (("silver", SILVER_SCHEMA), ("gold", GOLD_SCHEMA)):
        # Parquet restores nullable fields and moves the partition column last.
        assert {field.name: field.dataType for field in frames[name].schema} == {
            field.name: field.dataType for field in schema
        }
    assert {path.name for path in paths["silver"].glob("order_date=*")} == {
        "order_date=2026-01-15",
        "order_date=2026-01-16",
    }
    assert frames["gold"].agg(F.sum("net_amount")).first()[0] == Decimal("560.50")
    assert {
        (row.order_id, row.quantity)
        for row in frames["silver"].filter(F.col("order_id").isin("O1002", "O1003")).collect()
    } == {("O1002", 3), ("O1003", 2)}
    assert "malformed_csv" in (
        frames["rejected"].filter(F.col("order_id") == "BAD07").first().validation_errors
    )
    # Only the entry point is allowed to publish a success marker.
    assert not (tmp_path / "manifests").exists()


@pytest.mark.spark
def test_failed_quality_gate_preserves_audit_without_publishing_sales(spark, tmp_path):
    run_id = "rejected-publication"
    with pytest.raises(QualityError, match="Quality gate failed"):
        execute(spark, SAMPLE_INPUT.as_uri(), tmp_path.as_uri(), run_id, 0.39)

    quarantine = tmp_path / "quarantine" / f"run_id={run_id}" / "data"
    duplicates = tmp_path / "audit/duplicates" / f"run_id={run_id}" / "data"
    assert spark.read.parquet(quarantine.as_uri()).count() == 8
    assert spark.read.parquet(duplicates.as_uri()).count() == 2
    assert not (tmp_path / "silver").exists()
    assert not (tmp_path / "gold").exists()
    assert not (tmp_path / "manifests").exists()
