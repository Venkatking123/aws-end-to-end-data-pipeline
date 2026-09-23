"""AWS adapter contracts use local mocks and never resolve AWS credentials."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from retail_pipeline import aws

ROOT = Path(__file__).resolve().parents[1]


def test_stack_outputs_requires_a_ready_stack():
    client = Mock()
    client.describe_stacks.return_value = {
        "Stacks": [{"StackStatus": "CREATE_IN_PROGRESS", "Outputs": []}]
    }
    with pytest.raises(RuntimeError, match="not ready"):
        aws.stack_outputs(client, "retail-dev")
    client.describe_stacks.return_value["Stacks"][0] = {
        "StackStatus": "UPDATE_COMPLETE",
        "Outputs": [{"OutputKey": "DataBucketName", "OutputValue": "data-bucket"}],
    }
    assert aws.stack_outputs(client, "retail-dev") == {"DataBucketName": "data-bucket"}


def test_immutable_prefix_and_encrypted_json():
    s3 = Mock()
    s3.list_objects_v2.return_value = {"KeyCount": 1}
    with pytest.raises(FileExistsError, match="Immutable prefix"):
        aws.ensure_empty_prefix(s3, "data-bucket", "raw/batch_date=2026-01-15/")
    assert s3.list_objects_v2.call_args.kwargs["MaxKeys"] == 1
    s3.list_objects_v2.return_value = {"KeyCount": 0}
    aws.ensure_empty_prefix(s3, "data-bucket", "manifests/run-1/")
    aws.put_json(s3, "data-bucket", "manifests/run-1/success.json", {"gold_rows": 7})
    upload = s3.put_object.call_args.kwargs
    assert json.loads(upload["Body"]) == {"gold_rows": 7}
    assert upload["ContentType"] == "application/json"
    assert upload["ServerSideEncryption"] == "AES256"


def test_manifest_lists_all_pages_with_lengths_in_stable_order():
    s3 = Mock()
    prefix = "gold/daily_sales/run_id=run-1/data/"
    s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": prefix + "b.parquet", "Size": 23}]},
        {
            "Contents": [
                {"Key": prefix + "_SUCCESS", "Size": 0},
                {"Key": prefix + "empty.parquet", "Size": 0},
                {"Key": prefix + "a.parquet", "Size": 12},
            ]
        },
    ]
    assert aws.build_manifest(s3, "data-bucket", "run-1") == {
        "entries": [
            {
                "url": f"s3://data-bucket/{prefix}a.parquet",
                "mandatory": True,
                "meta": {"content_length": 12},
            },
            {
                "url": f"s3://data-bucket/{prefix}b.parquet",
                "mandatory": True,
                "meta": {"content_length": 23},
            },
        ]
    }
    s3.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="data-bucket", Prefix=prefix
    )


def test_manifest_refuses_an_empty_snapshot():
    s3 = Mock()
    s3.get_paginator.return_value.paginate.return_value = [{}]
    with pytest.raises(ValueError, match="empty manifest"):
        aws.build_manifest(s3, "data-bucket", "run-1")


def test_sql_renderer_preserves_projection_placeholders_and_statement_boundaries(tmp_path):
    statements = aws.render_sql(
        ROOT / "sql/athena/001_tables.sql", database="retail_dev", bucket="data-bucket"
    )
    assert len(statements) == 2
    assert "${run_id}" in statements[0]
    assert "${order_date}" in statements[0]
    assert "s3://data-bucket/" in statements[0]
    template = tmp_path / "queries.sql"
    template.write_text("SELECT 'semi;colon'; SELECT '${run_id}';", encoding="utf-8")
    assert aws.render_sql(template, run_id="run-1") == ["SELECT 'semi;colon';", "SELECT 'run-1';"]


@pytest.mark.parametrize(
    "values",
    [
        {"database": "db; DROP TABLE x"},
        {"run_id": "../../other"},
        {"bucket": "bucket'"},
        {"unknown": "safe"},
        {"copy_role_arn": "arn:aws:iam::123456789012:role/copy'"},
        {"manifest_uri": "s3://data-bucket/other/run-1.json"},
    ],
)
def test_sql_renderer_rejects_unsafe_parameters(values):
    with pytest.raises(ValueError, match="Unsafe SQL"):
        aws.render_sql(ROOT / "sql/redshift/001_schema.sql", **values)


@pytest.fixture
def clock(monkeypatch):
    fake = Mock()
    fake.monotonic.return_value = 0
    monkeypatch.setattr(aws, "time", fake)
    return fake


def test_glue_waits_for_success_and_reports_service_failure(clock):
    glue = Mock()
    glue.get_job_run.side_effect = [
        {"JobRun": {"JobRunState": "RUNNING"}},
        {"JobRun": {"JobRunState": "SUCCEEDED"}},
    ]
    assert aws.wait_glue(glue, "etl", "jr-1", interval=2)["JobRunState"] == "SUCCEEDED"
    clock.sleep.assert_called_once_with(2)
    glue.get_job_run.side_effect = None
    glue.get_job_run.return_value = {"JobRun": {"JobRunState": "FAILED", "ErrorMessage": "bad"}}
    with pytest.raises(RuntimeError, match="Glue FAILED: bad"):
        aws.wait_glue(glue, "etl", "jr-1")


def test_glue_timeout_requests_stop(clock):
    glue = Mock()
    clock.monotonic.side_effect = [0, 10]
    with pytest.raises(TimeoutError, match="stop requested"):
        aws.wait_glue(glue, "etl", "jr-1", timeout=10)
    glue.batch_stop_job_run.assert_called_once_with(JobName="etl", JobRunIds=["jr-1"])


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "CANCELLED"])
def test_athena_waits_and_surfaces_terminal_status(clock, state):
    athena = Mock()
    athena.start_query_execution.return_value = {"QueryExecutionId": "q-1"}
    athena.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "QUEUED"}}},
        {"QueryExecution": {"Status": {"State": state, "StateChangeReason": "detail"}}},
    ]
    if state == "SUCCEEDED":
        assert aws.query_athena(athena, "SELECT 1", "retail_dev", "analytics") == "q-1"
    else:
        with pytest.raises(RuntimeError, match=f"Athena {state}: detail"):
            aws.query_athena(athena, "SELECT 1", "retail_dev", "analytics")
    athena.start_query_execution.assert_called_once_with(
        QueryString="SELECT 1",
        QueryExecutionContext={"Database": "retail_dev"},
        WorkGroup="analytics",
    )


def test_athena_timeout_requests_cancellation(clock):
    athena = Mock()
    athena.start_query_execution.return_value = {"QueryExecutionId": "q-1"}
    clock.monotonic.side_effect = [0, 10]
    with pytest.raises(TimeoutError, match="cancellation requested"):
        aws.query_athena(athena, "SELECT 1", "retail_dev", "analytics", timeout=10)
    athena.stop_query_execution.assert_called_once_with(QueryExecutionId="q-1")


def reconciliation_result(**overrides):
    values = {"gold_rows": "7", "invalid_rows": "0", "net_revenue": "560.50", **overrides}
    return {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Name": name} for name in values]},
            "Rows": [
                {"Data": [{"VarCharValue": name} for name in values]},
                {"Data": [{"VarCharValue": value} for value in values.values()]},
            ],
        }
    }


def test_athena_reconciliation_compares_exact_decimal_and_row_counts():
    athena = Mock()
    athena.get_query_results.return_value = reconciliation_result()
    assert aws.reconcile_athena(athena, "q-1", {"gold_rows": 7, "net_amount": "560.500"}) == {
        "gold_rows": "7",
        "invalid_rows": "0",
        "net_revenue": "560.50",
    }


@pytest.mark.parametrize(
    "overrides", [{"gold_rows": "6"}, {"invalid_rows": "1"}, {"net_revenue": "560.51"}]
)
def test_athena_reconciliation_rejects_mismatches(overrides):
    athena = Mock()
    athena.get_query_results.return_value = reconciliation_result(**overrides)
    with pytest.raises(ValueError, match="reconciliation failed"):
        aws.reconcile_athena(athena, "q-1", {"gold_rows": 7, "net_amount": "560.50"})


@pytest.mark.parametrize("state", ["FINISHED", "FAILED", "ABORTED"])
def test_redshift_submits_atomic_batch_and_reports_terminal_status(clock, state):
    redshift = Mock()
    redshift.batch_execute_statement.return_value = {"Id": "s-1"}
    redshift.describe_statement.side_effect = [
        {"Status": "STARTED"},
        {"Status": state, "Error": "detail"},
    ]
    statements = ["CREATE TEMP TABLE stage (id INT)", "INSERT INTO stage VALUES (1)"]
    kwargs = {"workgroup": "warehouse", "database": "analytics", "secret_arn": "secret-arn"}
    if state == "FINISHED":
        assert aws.execute_redshift(redshift, statements, **kwargs) == "s-1"
    else:
        with pytest.raises(RuntimeError, match=f"Redshift {state}: detail"):
            aws.execute_redshift(redshift, statements, **kwargs)
    redshift.batch_execute_statement.assert_called_once_with(
        WorkgroupName="warehouse", Database="analytics", Sqls=statements, SecretArn="secret-arn"
    )


def test_redshift_timeout_requests_cancellation_without_secret(clock):
    redshift = Mock()
    redshift.batch_execute_statement.return_value = {"Id": "s-1"}
    clock.monotonic.side_effect = [0, 10]
    with pytest.raises(TimeoutError, match="cancellation requested"):
        aws.execute_redshift(redshift, ["SELECT 1"], workgroup="w", database="db", timeout=10)
    redshift.cancel_statement.assert_called_once_with(Id="s-1")
    assert "SecretArn" not in redshift.batch_execute_statement.call_args.kwargs
