"""Small, testable AWS adapters. No credentials or account IDs are hard-coded."""

import json
import re
import time
from decimal import Decimal
from pathlib import Path
from string import Template

from retail_pipeline.config import validate_run_id


def stack_outputs(client, stack_name: str) -> dict[str, str]:
    stack = client.describe_stacks(StackName=stack_name)["Stacks"][0]
    if stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise RuntimeError(f"Stack is not ready: {stack['StackStatus']}")
    return {entry["OutputKey"]: entry["OutputValue"] for entry in stack.get("Outputs", [])}


def ensure_empty_prefix(s3, bucket: str, prefix: str) -> None:
    if s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get("KeyCount", 0):
        raise FileExistsError(f"Immutable prefix already exists: s3://{bucket}/{prefix}")


def put_json(s3, bucket: str, key: str, value: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(value, indent=2) + "\n").encode(),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def build_manifest(s3, bucket: str, run_id: str) -> dict:
    validate_run_id(run_id)
    prefix = f"gold/daily_sales/run_id={run_id}/data/"
    entries = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") and obj["Size"] > 0:
                entries.append(
                    {
                        "url": f"s3://{bucket}/{obj['Key']}",
                        "mandatory": True,
                        "meta": {"content_length": obj["Size"]},
                    }
                )
    if not entries:
        raise ValueError("No Parquet files found; refusing to publish an empty manifest")
    return {"entries": sorted(entries, key=lambda entry: entry["url"])}


def render_sql(path: Path, **values: str) -> list[str]:
    """Templates use validated identifiers, never arbitrary user SQL fragments."""
    import sqlparse

    patterns = {
        "database": r"[a-z][a-z0-9_]{0,254}",
        "bucket": r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]",
        "run_id": r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}",
        "copy_role_arn": r"arn:aws(?:-us-gov|-cn)?:iam::\d{12}:role/[A-Za-z0-9/+=,.@_-]+",
        "manifest_uri": r"s3://[a-z0-9][a-z0-9.-]+/manifests/[A-Za-z0-9_-]+/redshift.json",
    }
    for key, value in values.items():
        if key not in patterns or not re.fullmatch(patterns[key], value):
            raise ValueError(f"Unsafe SQL template parameter: {key}")
    rendered = Template(path.read_text(encoding="utf-8")).substitute(values)
    return [statement.strip() for statement in sqlparse.split(rendered) if statement.strip()]


def wait_glue(client, job_name: str, job_run_id: str, *, interval=15, timeout=3600) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = client.get_job_run(JobName=job_name, RunId=job_run_id)["JobRun"]
        state = run["JobRunState"]
        if state == "SUCCEEDED":
            return run
        if state in {"FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"}:
            raise RuntimeError(f"Glue {state}: {run.get('ErrorMessage', job_run_id)}")
        time.sleep(interval)
    client.batch_stop_job_run(JobName=job_name, JobRunIds=[job_run_id])
    raise TimeoutError(f"Glue timed out; stop requested for {job_run_id}")


def query_athena(client, sql: str, database: str, workgroup: str, *, interval=5, timeout=600):
    query_id = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )["QueryExecutionId"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            return query_id
        if status["State"] in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Athena {status['State']}: {status.get('StateChangeReason')}")
        time.sleep(interval)
    client.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError(f"Athena timed out; cancellation requested for {query_id}")


def reconcile_athena(client, query_id: str, metrics: dict) -> dict:
    result = client.get_query_results(QueryExecutionId=query_id)["ResultSet"]
    rows = result["Rows"]
    if len(rows) != 2:
        raise ValueError("Expected exactly one Athena reconciliation row")
    names = [col["Name"] for col in result["ResultSetMetadata"]["ColumnInfo"]]
    values = dict(
        zip(names, [cell.get("VarCharValue", "") for cell in rows[1]["Data"]], strict=True)
    )
    if (
        int(values["gold_rows"]) != metrics["gold_rows"]
        or int(values["invalid_rows"]) != 0
        or Decimal(values["net_revenue"]) != Decimal(metrics["net_amount"])
    ):
        raise ValueError(f"Athena reconciliation failed: {values}")
    return values


def execute_redshift(
    client,
    statements: list[str],
    *,
    workgroup: str,
    database: str,
    secret_arn: str = "",
    interval=5,
    timeout=900,
) -> str:
    args = {"WorkgroupName": workgroup, "Database": database, "Sqls": statements}
    if secret_arn:
        args["SecretArn"] = secret_arn
    statement_id = client.batch_execute_statement(**args)["Id"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = client.describe_statement(Id=statement_id)
        if status["Status"] == "FINISHED":
            return statement_id
        if status["Status"] in {"FAILED", "ABORTED"}:
            raise RuntimeError(f"Redshift {status['Status']}: {status.get('Error')}")
        time.sleep(interval)
    client.cancel_statement(Id=statement_id)
    raise TimeoutError(f"Redshift timed out; cancellation requested for {statement_id}")
