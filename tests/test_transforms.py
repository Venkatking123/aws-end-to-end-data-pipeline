"""Transformation correctness is exercised by a real Spark execution engine."""

import csv
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from retail_pipeline.schemas import GOLD_SCHEMA, RAW_COLUMNS, RAW_SCHEMA, SILVER_SCHEMA
from retail_pipeline.transforms import _parse_csv_line, aggregate_daily, read_raw, transform

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_RECORD = {
    "order_id": "O1",
    "line_id": "1",
    "customer_id": "C1",
    "product_id": "P1",
    "category": "Books",
    "country": "US",
    "quantity": "2",
    "unit_price": "12.50",
    "discount_amount": "1.00",
    "order_timestamp": "2026-01-15T10:00:00",
    "updated_at": "2026-01-15T10:00:00",
    "status": "COMPLETED",
}


def raw_frame(spark, records):
    return spark.createDataFrame(
        [tuple({**BASE_RECORD, **record}[name] for name in RAW_COLUMNS) for record in records],
        RAW_SCHEMA,
    )


def column_types(schema):
    return [(field.name, field.dataType) for field in schema.fields]


def test_parser_validates_record_width_and_quoting():
    row = ",".join(BASE_RECORD[name] for name in RAW_COLUMNS)
    assert _parse_csv_line(row)[-2:] == (None, False)
    assert _parse_csv_line(",".join(RAW_COLUMNS))[-1] is True
    assert _parse_csv_line(row + ",")[-2] == row + ","
    assert _parse_csv_line(row.rsplit(",", 1)[0])[-2] is not None
    assert _parse_csv_line('"unterminated')[-2] == '"unterminated'


@pytest.mark.spark
def test_sample_reconciles_all_rows_and_exact_money(spark):
    expected = json.loads((PROJECT_ROOT / "data/expected/sample_metrics.json").read_text())
    raw = read_raw(spark, str(PROJECT_ROOT / "data/raw/orders.csv"))
    assert all(field.dataType.simpleString() == "string" for field in raw.schema.fields)
    result = transform(raw)
    gold = aggregate_daily(result.valid)
    assert raw.count() == expected["input_count"]
    assert result.valid.count() == expected["valid_count"]
    assert result.rejected.count() == expected["rejected_count"]
    assert result.duplicates.count() == expected["duplicate_count"]
    assert gold.count() == expected["gold_count"]
    totals = gold.agg(
        *[
            F.sum(name).alias(name)
            for name in ("units", "gross_amount", "discount_amount", "net_amount")
        ]
    ).first()
    assert totals.units == expected["units"]
    for name in ("gross_amount", "discount_amount", "net_amount"):
        assert totals[name] == Decimal(expected[name])
    assert column_types(result.valid.schema) == column_types(SILVER_SCHEMA)
    assert column_types(gold.schema) == column_types(GOLD_SCHEMA)
    errors = {row.order_id: row.validation_errors for row in result.rejected.collect()}
    assert "malformed_csv" in errors["BAD07"]
    assert "invalid_quantity" in errors["O1002"]


@pytest.mark.spark
def test_normalization_and_fixed_decimal_arithmetic(spark):
    result = transform(
        raw_frame(
            spark,
            [
                {
                    "order_id": " O1 ",
                    "country": " us ",
                    "status": " completed ",
                    "quantity": "3",
                    "unit_price": "0.10",
                    "discount_amount": "0.01",
                }
            ],
        )
    )
    row = result.valid.first()
    assert row.order_id == "O1"
    assert row.country == "US"
    assert row.status == "COMPLETED"
    assert row.gross_amount == Decimal("0.30")
    assert row.net_amount == Decimal("0.29")
    assert row.order_date == date(2026, 1, 15)
    # Spark timestamps are UTC internally; Python's naive collected datetime is
    # rendered in the host timezone, so verify in the Spark UTC session instead.
    timestamp = result.valid.select(
        F.date_format("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp")
    ).first()
    assert timestamp.timestamp == "2026-01-15T10:00:00"
    assert result.rejected.count() == 0


@pytest.mark.spark
def test_invalid_values_are_quarantined_without_ansi_cast_failures(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    cases = [
        ({"quantity": "1.5"}, "invalid_quantity"),
        ({"quantity": "0"}, "invalid_quantity"),
        ({"quantity": "2147483648"}, "invalid_quantity"),
        ({"quantity": None}, "invalid_quantity"),
        ({"unit_price": "-1.00"}, "invalid_unit_price"),
        ({"unit_price": "1.001"}, "invalid_unit_price"),
        ({"unit_price": "1e2"}, "invalid_unit_price"),
        ({"unit_price": "NaN"}, "invalid_unit_price"),
        ({"unit_price": "10000000000000000"}, "invalid_unit_price"),
        ({"discount_amount": "-0.01"}, "invalid_discount_amount"),
        ({"discount_amount": "26.00"}, "discount_exceeds_gross"),
        ({"customer_id": " "}, "missing_customer_id"),
        ({"category": "é" * 51}, "category_too_long"),
        ({"country": "USA"}, "invalid_country"),
        ({"status": "PENDING"}, "invalid_status"),
        ({"order_timestamp": "2026-02-30T10:00:00"}, "invalid_order_timestamp"),
        ({"order_timestamp": "2026-01-15T10:00:00Z"}, "invalid_order_timestamp"),
        ({"order_timestamp": "2026-1-15T10:00:00"}, "invalid_order_timestamp"),
        ({"updated_at": "yesterday"}, "invalid_updated_at"),
        ({"order_timestamp": "2019-12-31T23:59:59"}, "order_date_out_of_range"),
        (
            {"order_timestamp": "2036-01-01T00:00:00", "updated_at": "2036-01-01T00:00:00"},
            "order_date_out_of_range",
        ),
        ({"updated_at": "2026-01-14T10:00:00"}, "updated_before_order"),
        ({"quantity": "2", "unit_price": "9999999999999999.99"}, "amount_overflow"),
    ]
    source = raw_frame(
        spark, [{**values, "order_id": f"BAD{index}"} for index, (values, _) in enumerate(cases)]
    )
    result = transform(source)
    rejected = {row.order_id: row.validation_errors for row in result.rejected.collect()}
    assert len(rejected) == len(cases)
    assert result.valid.count() == 0
    for index, (_, reason) in enumerate(cases):
        assert reason in rejected[f"BAD{index}"]


@pytest.mark.spark
def test_latest_valid_correction_wins_and_cancelled_winner_is_not_sales(spark):
    source = raw_frame(
        spark,
        [
            {},
            {"updated_at": "2026-01-15T11:00:00", "quantity": "3"},
            {"updated_at": "2026-01-15T12:00:00", "quantity": "-1"},
            {"order_id": "O2"},
            {"order_id": "O2", "updated_at": "2026-01-15T11:00:00", "status": "CANCELLED"},
        ],
    )
    result = transform(source)
    rows = {row.order_id: row for row in result.valid.collect()}
    assert rows["O1"].quantity == 3
    assert rows["O1"].net_amount == Decimal("36.50")
    assert rows["O2"].status == "CANCELLED"
    assert rows["O2"].gross_amount == rows["O2"].net_amount == Decimal("0.00")
    assert result.rejected.count() == 1
    assert result.duplicates.count() == 2
    gold = aggregate_daily(result.valid).first()
    assert gold.order_count == gold.line_count == 1
    assert gold.units == 3
    assert gold.net_amount == Decimal("36.50")


@pytest.mark.spark
def test_equal_timestamp_deduplication_is_stable_across_partitioning(spark):
    source = raw_frame(spark, [{"quantity": "1"}, {"quantity": "2"}, {"quantity": "2"}])
    first = transform(source.repartition(1))
    second = transform(source.repartition(3))
    first_row = first.valid.first()
    second_row = second.valid.first()
    assert first_row._record_hash == second_row._record_hash
    assert first_row.quantity == second_row.quantity
    all_hashes = [row._record_hash for row in first.duplicates.collect()] + [first_row._record_hash]
    assert first_row._record_hash == max(all_hashes)
    assert first.duplicates.count() == 2


@pytest.mark.spark
def test_order_count_is_distinct_within_date_country_category(spark):
    source = raw_frame(
        spark,
        [
            {},
            {"line_id": "2"},
            {"order_id": "O2"},
            {"line_id": "3", "category": "Home"},
        ],
    )
    groups = {row.category: row for row in aggregate_daily(transform(source).valid).collect()}
    assert groups["Books"].order_count == 2
    assert groups["Books"].line_count == 3
    assert groups["Books"].units == 6
    assert groups["Home"].order_count == 1


@pytest.mark.spark
def test_empty_input_and_all_cancelled_have_stable_output_schemas(spark):
    empty = transform(raw_frame(spark, []))
    assert empty.valid.count() == empty.rejected.count() == empty.duplicates.count() == 0
    assert column_types(empty.valid.schema) == column_types(SILVER_SCHEMA)
    empty_gold = aggregate_daily(empty.valid)
    assert empty_gold.count() == 0
    assert column_types(empty_gold.schema) == column_types(GOLD_SCHEMA)
    cancelled = transform(raw_frame(spark, [{"status": "CANCELLED"}]))
    assert aggregate_daily(cancelled.valid).count() == 0


@pytest.mark.spark
def test_group_decimal_overflow_remains_visible_to_publication_quality_gate(spark):
    source = raw_frame(
        spark,
        [
            {
                "order_id": "BIG1",
                "quantity": "1",
                "unit_price": "6000000000000000.00",
                "discount_amount": "0",
            },
            {
                "order_id": "BIG2",
                "quantity": "1",
                "unit_price": "6000000000000000.00",
                "discount_amount": "0",
            },
        ],
    )
    result = transform(source)
    assert result.valid.count() == 2
    row = aggregate_daily(result.valid).first()
    assert row.gross_amount is None
    assert row.net_amount is None


@pytest.mark.spark
def test_csv_reader_preserves_quoted_commas_and_quarantines_bad_width(spark, tmp_path):
    path = tmp_path / "records = 100%.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(RAW_COLUMNS)
        record = {**BASE_RECORD, "category": "Books, illustrated"}
        writer.writerow([record[name] for name in RAW_COLUMNS])
        writer.writerow([BASE_RECORD[name] for name in RAW_COLUMNS] + [""])
        writer.writerow([BASE_RECORD[name] for name in RAW_COLUMNS[:-1]])
        handle.write('"unterminated\n')
    source = read_raw(spark, str(path))
    result = transform(source)
    assert source.count() == 4
    assert result.valid.first().category == "Books, illustrated"
    assert result.rejected.count() == 3
    assert all("malformed_csv" in row.validation_errors for row in result.rejected.collect())


@pytest.mark.spark
def test_missing_columns_fail_with_an_actionable_error(spark):
    with pytest.raises(ValueError, match="Missing required source columns: quantity"):
        transform(raw_frame(spark, [{}]).drop("quantity"))


@pytest.mark.spark
def test_every_file_header_is_checked_before_accepting_rows(spark, tmp_path):
    good = tmp_path / "good.csv"
    good.write_text(",".join(RAW_COLUMNS) + "\n", encoding="utf-8")
    bad = tmp_path / "bad.csv"
    swapped = RAW_COLUMNS.copy()
    swapped[0], swapped[1] = swapped[1], swapped[0]
    bad.write_text(",".join(swapped) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="CSV header does not match required column order"):
        read_raw(spark, str(tmp_path))
