"""AWS Glue entry point; shared transformations are shipped with --extra-py-files."""

import sys
from datetime import UTC, datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from retail_pipeline.aws import build_manifest, ensure_empty_prefix, put_json
from retail_pipeline.config import validate_batch_date, validate_reject_ratio, validate_run_id
from retail_pipeline.logging import configure_logging
from retail_pipeline.pipeline import execute


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET",
            "RAW_BATCH_DATE",
            "RUN_ID",
            "MAX_REJECT_RATIO",
        ],
    )
    run_id = validate_run_id(args["RUN_ID"])
    batch_date = validate_batch_date(args["RAW_BATCH_DATE"])
    maximum = validate_reject_ratio(float(args["MAX_REJECT_RATIO"]))
    bucket = args["DATA_BUCKET"]
    s3 = boto3.client("s3")
    # CloudFormation serializes this job (MaxConcurrentRuns=1). Never reuse a run ID.
    for prefix in (
        f"manifests/{run_id}/",
        f"silver/order_lines/run_id={run_id}/",
        f"gold/daily_sales/run_id={run_id}/",
        f"quarantine/run_id={run_id}/",
        f"audit/duplicates/run_id={run_id}/",
    ):
        ensure_empty_prefix(s3, bucket, prefix)

    def publish(name, payload):
        put_json(s3, bucket, f"manifests/{run_id}/{name}.json", payload)

    publish(
        "started",
        {"run_id": run_id, "batch_date": batch_date, "started_at": datetime.now(UTC).isoformat()},
    )
    logger = configure_logging()
    try:
        context = GlueContext(SparkContext.getOrCreate())
        job = Job(context)
        job.init(args["JOB_NAME"], args)
        metrics = execute(
            context.spark_session,
            f"s3://{bucket}/raw/batch_date={batch_date}/",
            f"s3://{bucket}",
            run_id,
            maximum,
        )
        # A validated all-cancelled snapshot has no gold rows. The warehouse
        # runner clears the mart without submitting an empty COPY manifest.
        manifest = build_manifest(s3, bucket, run_id) if metrics.gold_rows else {"entries": []}
        publish("redshift", manifest)
        job.commit()
        publish(
            "success",
            {
                **metrics.as_dict(),
                "batch_date": batch_date,
                "completed_at": datetime.now(UTC).isoformat(),
            },
        )
        logger.info("snapshot_published", extra={"fields": metrics.as_dict()})
    except Exception:
        logger.exception("etl_failed", extra={"fields": {"run_id": run_id}})
        publish("failure", {"run_id": run_id, "failed_at": datetime.now(UTC).isoformat()})
        raise


if __name__ == "__main__":
    main()
