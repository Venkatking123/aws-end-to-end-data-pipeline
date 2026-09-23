"""Pure Spark transformations shared by local execution and AWS Glue.

Input is a complete order-line snapshot. Validate before deduplication: an invalid
correction is quarantined, so the most recent *valid* version remains active.
Ties use a canonical SHA-256 row hash, then the source filename. Amounts are
Decimal(18,2); cancelled lines are retained with zero recognized gross/net sales.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from retail_pipeline.schemas import MONEY_TYPE, RAW_COLUMNS, SILVER_SCHEMA

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
MONEY_PATTERN = r"^\d+(?:\.\d{1,2})?$"


@dataclass(frozen=True)
class TransformResult:
    """DataFrames are lazy; the runner owns caching, actions and persistence."""

    valid: DataFrame
    rejected: DataFrame
    duplicates: DataFrame


def _parse_csv_line(line: str) -> tuple:
    """Keep malformed records visible, including extra or missing CSV fields.

    The source contract intentionally excludes embedded newlines. Spark's native
    CSV parser silently drops extra tokens, so exact field-count validation uses
    Python's strict CSV parser on each physical line before the Spark projection.
    """
    try:
        values = next(csv.reader([line.lstrip("\ufeff")], strict=True))
        if values == RAW_COLUMNS:
            return (*([None] * len(RAW_COLUMNS)), None, True)
        corrupt = line if len(values) != len(RAW_COLUMNS) else None
        fields = (values + [None] * len(RAW_COLUMNS))[: len(RAW_COLUMNS)]
        return (*fields, corrupt, False)
    except (csv.Error, StopIteration):
        return (*([None] * len(RAW_COLUMNS)), line, False)


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read UTF-8 CSV files from local storage or S3 without type inference.

    Every file must use the documented header and one physical line per record.
    Blank lines and exact header lines are ignored. Every resolved file's first
    line is checked before processing; bad/missing headers fail the whole read.
    Files must be plain, uncompressed CSV. Header checks read one line per file
    through Hadoop, so the same behavior applies to local paths and S3.
    """
    parser_schema = StructType(
        [
            *[StructField(name, StringType(), True) for name in RAW_COLUMNS],
            StructField("_corrupt_record", StringType(), True),
            StructField("_is_header", BooleanType(), False),
        ]
    )
    parse_line = F.udf(_parse_csv_line, parser_schema)
    lines = spark.read.text(path).withColumn("_source_file", F.input_file_name())
    for filename in lines.inputFiles():
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(filename)
        filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
        stream = filesystem.open(hadoop_path)
        try:
            reader = spark._jvm.java.io.BufferedReader(
                spark._jvm.java.io.InputStreamReader(stream, "UTF-8")
            )
            header = reader.readLine()
            try:
                fields = next(csv.reader([header.lstrip("\ufeff")], strict=True)) if header else []
            except csv.Error as exc:
                raise ValueError(f"Malformed CSV header in {filename}") from exc
            if fields != RAW_COLUMNS:
                raise ValueError(f"CSV header does not match required column order in {filename}")
        finally:
            stream.close()
    parsed = lines.filter(F.length(F.trim("value")) > 0).withColumn("_parsed", parse_line("value"))
    return (
        parsed.filter(~F.col("_parsed._is_header"))
        .select("_parsed.*", "_source_file")
        .drop("_is_header")
    )


def transform(raw: DataFrame) -> TransformResult:
    """Normalize, classify, and choose one valid version of each order line."""
    missing = sorted(set(RAW_COLUMNS) - set(raw.columns))
    if missing:
        raise ValueError(f"Missing required source columns: {', '.join(missing)}")
    raw.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    raw.sparkSession.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    normalized = (
        raw.select(
            *[F.trim(F.col(name).cast("string")).alias(name) for name in RAW_COLUMNS],
            *[
                (F.col(name) if name in raw.columns else F.lit(None).cast("string")).alias(name)
                for name in ("_corrupt_record", "_source_file")
            ],
        )
        .withColumn("country", F.upper("country"))
        .withColumn("status", F.upper("status"))
    )
    normalized = normalized.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(name) for name in RAW_COLUMNS]), {"ignoreNullFields": "false"}
            ),
            256,
        ),
    )
    parsed = (
        normalized.withColumn("_quantity", F.expr("try_cast(quantity as int)"))
        .withColumn("_unit_price", F.expr("try_cast(unit_price as decimal(18,2))"))
        .withColumn("_discount_amount", F.expr("try_cast(discount_amount as decimal(18,2))"))
        .withColumn(
            "_order_timestamp", F.try_to_timestamp("order_timestamp", F.lit(TIMESTAMP_FORMAT))
        )
        .withColumn("_updated_at", F.try_to_timestamp("updated_at", F.lit(TIMESTAMP_FORMAT)))
        .withColumn("_gross_amount", F.expr("try_cast(_quantity * _unit_price as decimal(18,2))"))
        .withColumn(
            "_net_amount", F.expr("try_cast(_gross_amount - _discount_amount as decimal(18,2))")
        )
    )

    def invalid(condition: F.Column, reason: str) -> F.Column:
        # Null predicates are invalid as well (e.g. a missing required value).
        return F.when(~F.coalesce(condition, F.lit(False)), F.lit(reason))

    errors = [F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed_csv"))]
    errors.extend(
        invalid(F.length(F.col(name)) > 0, f"missing_{name}")
        for name in ("order_id", "line_id", "customer_id", "product_id", "category")
    )
    errors.extend(
        [
            invalid(F.length(F.encode("category", "UTF-8")) <= 100, "category_too_long"),
            invalid(F.col("country").rlike(r"^[A-Z]{2}$"), "invalid_country"),
            invalid(
                F.col("quantity").rlike(r"^\d+$") & (F.col("_quantity") > 0), "invalid_quantity"
            ),
            invalid(
                F.col("unit_price").rlike(MONEY_PATTERN) & F.col("_unit_price").isNotNull(),
                "invalid_unit_price",
            ),
            invalid(
                F.col("discount_amount").rlike(MONEY_PATTERN)
                & F.col("_discount_amount").isNotNull(),
                "invalid_discount_amount",
            ),
            invalid(
                F.col("order_timestamp").rlike(TIMESTAMP_PATTERN)
                & F.col("_order_timestamp").isNotNull(),
                "invalid_order_timestamp",
            ),
            invalid(
                F.col("updated_at").rlike(TIMESTAMP_PATTERN) & F.col("_updated_at").isNotNull(),
                "invalid_updated_at",
            ),
            invalid(F.col("status").isin("COMPLETED", "CANCELLED"), "invalid_status"),
            F.when(
                F.col("_order_timestamp").isNotNull()
                & ~F.to_date("_order_timestamp").between(
                    F.lit("2020-01-01").cast("date"), F.lit("2035-12-31").cast("date")
                ),
                F.lit("order_date_out_of_range"),
            ),
            F.when(F.col("_updated_at") < F.col("_order_timestamp"), F.lit("updated_before_order")),
            F.when(
                F.col("_discount_amount") > F.col("_gross_amount"), F.lit("discount_exceeds_gross")
            ),
            F.when(
                F.col("_quantity").isNotNull()
                & F.col("_unit_price").isNotNull()
                & (F.col("_gross_amount").isNull() | F.col("_net_amount").isNull()),
                F.lit("amount_overflow"),
            ),
        ]
    )
    classified = parsed.withColumn(
        "validation_errors", F.filter(F.array(*errors), lambda value: value.isNotNull())
    )
    rejected = classified.filter(F.size("validation_errors") > 0).select(
        *RAW_COLUMNS,
        "_corrupt_record",
        "_source_file",
        "_record_hash",
        "validation_errors",
    )
    accepted = classified.filter(F.size("validation_errors") == 0)
    money_zero = F.lit(0).cast(MONEY_TYPE)
    typed = accepted.select(
        *[F.col(name) for name in RAW_COLUMNS[:6]],
        F.col("_quantity").alias("quantity"),
        F.col("_unit_price").alias("unit_price"),
        F.col("_discount_amount").alias("discount_amount"),
        F.col("_order_timestamp").alias("order_timestamp"),
        F.col("_updated_at").alias("updated_at"),
        "status",
        F.when(F.col("status") == "COMPLETED", F.col("_gross_amount"))
        .otherwise(money_zero)
        .alias("gross_amount"),
        F.when(F.col("status") == "COMPLETED", F.col("_net_amount"))
        .otherwise(money_zero)
        .alias("net_amount"),
        F.to_date("_order_timestamp").alias("order_date"),
        "_record_hash",
        "_source_file",
    )
    ordering = Window.partitionBy("order_id", "line_id").orderBy(
        F.col("updated_at").desc(),
        F.col("_record_hash").desc(),
        F.col("_source_file").asc_nulls_last(),
    )
    ranked = typed.withColumn("dedup_rank", F.row_number().over(ordering))
    valid = ranked.filter(F.col("dedup_rank") == 1).select(*SILVER_SCHEMA.fieldNames())
    duplicates = ranked.filter(F.col("dedup_rank") > 1)
    return TransformResult(valid=valid, rejected=rejected, duplicates=duplicates)


def aggregate_daily(valid: DataFrame) -> DataFrame:
    """Aggregate completed order lines at UTC date/country/category grain.

    Decimal sums are evaluated in Spark's wider accumulator then converted with
    try_cast. The runner must reject null totals (overflow) before publishing.
    Order counts are distinct within each group and must not be summed across
    categories to obtain a global distinct-order count.
    """
    return (
        valid.filter(F.col("status") == "COMPLETED")
        .groupBy("order_date", "country", "category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count(F.lit(1)).alias("line_count"),
            F.sum("quantity").cast("long").alias("units"),
            F.sum("gross_amount").alias("_gross_amount"),
            F.sum("discount_amount").alias("_discount_amount"),
            F.sum("net_amount").alias("_net_amount"),
        )
        .select(
            "order_date",
            "country",
            "category",
            "order_count",
            "line_count",
            "units",
            *[
                F.expr(f"try_cast(_{name} as decimal(18,2))").alias(name)
                for name in ("gross_amount", "discount_amount", "net_amount")
            ],
        )
    )
