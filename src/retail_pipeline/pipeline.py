"""Common Spark orchestration. A caller publishes success only after this returns."""

from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_pipeline.config import validate_reject_ratio, validate_run_id
from retail_pipeline.logging import configure_logging
from retail_pipeline.transforms import aggregate_daily, read_raw, transform


class QualityError(ValueError):
    """Input failed the publication gate; quarantine remains available."""


@dataclass(frozen=True)
class Metrics:
    run_id: str
    input_rows: int
    rejected_rows: int
    duplicate_rows: int
    valid_rows: int
    gold_rows: int
    reject_ratio: float
    net_amount: str

    def as_dict(self) -> dict:
        return asdict(self)


def check_quality(input_rows: int, rejected_rows: int, valid_rows: int, maximum: float) -> float:
    validate_reject_ratio(maximum)
    if input_rows == 0:
        raise QualityError("Empty input snapshot is not publishable")
    ratio = rejected_rows / input_rows
    if valid_rows == 0 or ratio > maximum:
        raise QualityError(
            f"Quality gate failed: input={input_rows}, rejected={rejected_rows}, "
            f"valid={valid_rows}, ratio={ratio:.4f}, maximum={maximum}"
        )
    return ratio


def execute(
    spark: SparkSession,
    input_path: str,
    output_root: str,
    run_id: str,
    max_reject_ratio: float = 0.25,
) -> Metrics:
    validate_run_id(run_id)
    validate_reject_ratio(max_reject_ratio)
    logger = configure_logging()
    logger.info("etl_started", extra={"fields": {"run_id": run_id}})
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    root = output_root.rstrip("/")
    raw = read_raw(spark, input_path).cache()
    result = transform(raw)
    cached = [raw, result.valid.cache(), result.rejected.cache(), result.duplicates.cache()]
    try:
        input_rows = raw.count()
        rejected_rows = result.rejected.count()
        valid_rows = result.valid.count()
        duplicate_rows = result.duplicates.count()
        if input_rows != rejected_rows + valid_rows + duplicate_rows:
            raise QualityError("Row reconciliation failed")
        result.rejected.write.mode("errorifexists").parquet(
            f"{root}/quarantine/run_id={run_id}/data"
        )
        result.duplicates.write.mode("errorifexists").parquet(
            f"{root}/audit/duplicates/run_id={run_id}/data"
        )
        ratio = check_quality(input_rows, rejected_rows, valid_rows, max_reject_ratio)
        result.valid.write.mode("errorifexists").partitionBy("order_date").parquet(
            f"{root}/silver/order_lines/run_id={run_id}/data"
        )
        gold = aggregate_daily(result.valid).cache()
        cached.append(gold)
        gold_rows = gold.count()
        if (
            gold.filter(
                F.col("gross_amount").isNull()
                | F.col("discount_amount").isNull()
                | F.col("net_amount").isNull()
            )
            .limit(1)
            .count()
        ):
            raise QualityError("Gold decimal aggregation overflow; snapshot is not publishable")
        total = gold.agg(F.sum("net_amount").alias("total")).first()["total"]
        gold.write.mode("errorifexists").parquet(f"{root}/gold/daily_sales/run_id={run_id}/data")
        metrics = Metrics(
            run_id,
            input_rows,
            rejected_rows,
            duplicate_rows,
            valid_rows,
            gold_rows,
            ratio,
            str(total if total is not None else "0.00"),
        )
        logger.info("etl_validated", extra={"fields": metrics.as_dict()})
        return metrics
    finally:
        for frame in cached:
            frame.unpersist()
