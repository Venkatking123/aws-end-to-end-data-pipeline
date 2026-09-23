"""Run a raw snapshot through Glue, Athena reconciliation and optional Redshift."""

import argparse
import json
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.config import Config as BotoConfig

from retail_pipeline.aws import (
    ensure_empty_prefix,
    execute_redshift,
    query_athena,
    reconcile_athena,
    render_sql,
    stack_outputs,
    wait_glue,
)
from retail_pipeline.config import Config, validate_batch_date, validate_run_id
from retail_pipeline.logging import configure_logging

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--input", help="Upload one CSV only when the raw batch prefix is empty")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--load-redshift", action="store_true")
    parser.add_argument("--resume-run", help="Reconcile/load an already successful Glue snapshot")
    args = parser.parse_args()
    config = Config.load(args.config)
    batch_date = validate_batch_date(args.batch_date)
    run_id = validate_run_id(args.resume_run or args.run_id or uuid4().hex)
    if args.resume_run and (args.input or args.run_id):
        parser.error("--resume-run cannot be combined with --input or --run-id")
    if args.input and not Path(args.input).is_file():
        parser.error("--input must be an existing CSV file")
    logger = configure_logging()
    session = boto3.Session(region_name=config.region)
    sdk_config = BotoConfig(retries={"mode": "standard", "max_attempts": 5})

    def client(service):
        return session.client(service, config=sdk_config)

    outputs = stack_outputs(client("cloudformation"), config.stack_name)
    bucket = outputs["DataBucketName"]
    workgroup = config.redshift_workgroup or outputs.get("RedshiftWorkgroupName", "")
    secret = config.redshift_secret_arn
    if args.load_redshift:
        if not workgroup:
            parser.error("Redshift load requires a deployed or configured workgroup")
        if not secret and outputs.get("RedshiftNamespaceName") and not config.redshift_workgroup:
            secret = client("redshift-serverless").get_namespace(
                namespaceName=outputs["RedshiftNamespaceName"]
            )["namespace"]["adminPasswordSecretArn"]
    s3 = client("s3")
    glue = client("glue")
    logger.info("pipeline_started", extra={"fields": {"run_id": run_id, "batch_date": batch_date}})
    if not args.resume_run:
        ensure_empty_prefix(s3, bucket, f"manifests/{run_id}/")
        if args.input:
            prefix = f"raw/batch_date={batch_date}/"
            ensure_empty_prefix(s3, bucket, prefix)
            s3.upload_file(
                args.input,
                bucket,
                f"{prefix}orders.csv",
                ExtraArgs={"ServerSideEncryption": "AES256"},
            )
        job_run = glue.start_job_run(
            JobName=outputs["GlueJobName"],
            Arguments={
                "--RAW_BATCH_DATE": batch_date,
                "--RUN_ID": run_id,
                "--MAX_REJECT_RATIO": str(config.max_reject_ratio),
            },
        )["JobRunId"]
        logger.info("glue_submitted", extra={"fields": {"run_id": run_id, "job_run_id": job_run}})
        wait_glue(
            glue,
            outputs["GlueJobName"],
            job_run,
            interval=config.poll_seconds,
            timeout=config.timeout_seconds,
        )
    body = s3.get_object(Bucket=bucket, Key=f"manifests/{run_id}/success.json")["Body"]
    try:
        metrics = json.loads(body.read())
    finally:
        body.close()
    if metrics["run_id"] != run_id or metrics["batch_date"] != batch_date:
        raise ValueError("Success marker identity does not match the requested snapshot")
    athena = client("athena")
    query_args = {
        "database": outputs["GlueDatabaseName"],
        "workgroup": outputs["AthenaWorkGroupName"],
        "interval": config.poll_seconds,
        "timeout": config.timeout_seconds,
    }
    for sql in render_sql(
        ROOT / "sql/athena/001_tables.sql", database=outputs["GlueDatabaseName"], bucket=bucket
    ):
        query_athena(athena, sql, **query_args)
    validation = render_sql(
        ROOT / "sql/athena/002_validation.sql", database=outputs["GlueDatabaseName"], run_id=run_id
    )
    query_id = query_athena(athena, validation[0], **query_args)
    summary = reconcile_athena(athena, query_id, metrics)
    logger.info("athena_reconciled", extra={"fields": {"run_id": run_id, **summary}})
    if args.load_redshift:
        redshift = client("redshift-data")
        redshift_args = {
            "workgroup": workgroup,
            "database": config.redshift_database,
            "secret_arn": secret,
            "interval": config.poll_seconds,
            "timeout": config.timeout_seconds,
        }
        execute_redshift(
            redshift, render_sql(ROOT / "sql/redshift/001_schema.sql"), **redshift_args
        )
        load_sql = render_sql(
            ROOT / "sql/redshift/002_load_snapshot.sql",
            run_id=run_id,
            manifest_uri=f"s3://{bucket}/manifests/{run_id}/redshift.json",
            copy_role_arn=outputs["RedshiftCopyRoleArn"],
        )
        if metrics["gold_rows"] == 0:
            # A fully cancelled snapshot still replaces the mart with an empty snapshot.
            # Redshift does not accept an empty COPY manifest.
            load_sql = [sql for sql in load_sql if not sql.lstrip().upper().startswith("COPY ")]
        statement_id = execute_redshift(redshift, load_sql, **redshift_args)
        logger.info(
            "warehouse_loaded", extra={"fields": {"run_id": run_id, "statement_id": statement_id}}
        )
    print(
        json.dumps(
            {
                "run_id": run_id,
                "metrics": metrics,
                "athena": summary,
                "redshift_loaded": args.load_redshift,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
