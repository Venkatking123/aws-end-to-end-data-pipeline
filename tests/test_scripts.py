"""Deployment and resume orchestration are exercised without an AWS account."""

import importlib.util
import io
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = {
    "DataBucketName": "data-bucket",
    "GlueJobName": "retail-dev-etl",
    "GlueDatabaseName": "retail_dev",
    "AthenaWorkGroupName": "retail-dev-analytics",
    "RedshiftCopyRoleArn": "arn:aws:iam::123456789012:role/retail-copy",
}


def load_script(name, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    script = load_script("run_pipeline", monkeypatch)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"region": "us-east-1", "stack_name": "retail-dev"}), encoding="utf-8"
    )
    metrics = {
        "run_id": "run-1",
        "batch_date": "2026-01-15",
        "gold_rows": 7,
        "net_amount": "560.50",
    }
    clients = {name: Mock() for name in ("s3", "glue", "athena", "cloudformation", "redshift-data")}
    clients["s3"].list_objects_v2.return_value = {"KeyCount": 0}
    clients["s3"].get_object.return_value = {"Body": io.BytesIO(json.dumps(metrics).encode())}
    clients["glue"].start_job_run.return_value = {"JobRunId": "jr-1"}
    session = Mock()
    session.client.side_effect = lambda service, **kwargs: clients[service]
    monkeypatch.setattr(script.boto3, "Session", Mock(return_value=session))
    monkeypatch.setattr(script, "stack_outputs", Mock(return_value=OUTPUTS.copy()))
    monkeypatch.setattr(script, "configure_logging", Mock(return_value=Mock()))
    monkeypatch.setattr(script, "wait_glue", Mock())
    monkeypatch.setattr(script, "query_athena", Mock(return_value="q-1"))
    monkeypatch.setattr(script, "reconcile_athena", Mock(return_value={"gold_rows": "7"}))
    monkeypatch.setattr(script, "execute_redshift", Mock(return_value="s-1"))
    arguments = ["run_pipeline.py", "--config", str(config), "--batch-date", "2026-01-15"]
    return script, clients, arguments, metrics


def test_new_run_uploads_once_waits_and_reconciles(runtime, monkeypatch, tmp_path, capsys):
    script, clients, arguments, metrics = runtime
    source = tmp_path / "orders.csv"
    source.write_text("synthetic sample", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [*arguments, "--run-id", "run-1", "--input", str(source)])
    script.main()
    clients["s3"].upload_file.assert_called_once_with(
        str(source),
        "data-bucket",
        "raw/batch_date=2026-01-15/orders.csv",
        ExtraArgs={"ServerSideEncryption": "AES256"},
    )
    assert [call.kwargs["Prefix"] for call in clients["s3"].list_objects_v2.call_args_list] == [
        "manifests/run-1/",
        "raw/batch_date=2026-01-15/",
    ]
    clients["glue"].start_job_run.assert_called_once_with(
        JobName="retail-dev-etl",
        Arguments={
            "--RAW_BATCH_DATE": "2026-01-15",
            "--RUN_ID": "run-1",
            "--MAX_REJECT_RATIO": "0.25",
        },
    )
    script.wait_glue.assert_called_once_with(
        clients["glue"], "retail-dev-etl", "jr-1", interval=15, timeout=3600
    )
    assert script.query_athena.call_count == 3
    script.reconcile_athena.assert_called_once_with(clients["athena"], "q-1", metrics)
    assert clients["s3"].get_object.return_value["Body"].closed
    assert json.loads(capsys.readouterr().out)["redshift_loaded"] is False


def test_existing_raw_batch_cannot_be_overwritten(runtime, monkeypatch, tmp_path):
    script, clients, arguments, _ = runtime
    clients["s3"].list_objects_v2.side_effect = [{"KeyCount": 0}, {"KeyCount": 1}]
    source = tmp_path / "orders.csv"
    source.write_text("synthetic sample", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [*arguments, "--run-id", "run-1", "--input", str(source)])
    with pytest.raises(FileExistsError, match="Immutable prefix"):
        script.main()
    clients["s3"].upload_file.assert_not_called()
    clients["glue"].start_job_run.assert_not_called()


@pytest.mark.parametrize("gold_rows", [0, 7])
def test_resume_skips_glue_and_atomically_loads_full_or_empty_snapshot(
    runtime, monkeypatch, gold_rows
):
    script, clients, arguments, metrics = runtime
    script.stack_outputs.return_value["RedshiftWorkgroupName"] = "retail-warehouse"
    metrics["gold_rows"] = gold_rows
    clients["s3"].get_object.return_value = {"Body": io.BytesIO(json.dumps(metrics).encode())}
    monkeypatch.setattr(sys, "argv", [*arguments, "--resume-run", "run-1", "--load-redshift"])
    script.main()
    clients["glue"].start_job_run.assert_not_called()
    clients["s3"].upload_file.assert_not_called()
    clients["s3"].list_objects_v2.assert_not_called()
    assert script.execute_redshift.call_count == 2
    statements = script.execute_redshift.call_args.args[1]
    copy_statements = [sql for sql in statements if sql.upper().startswith("COPY ")]
    assert len(copy_statements) == (1 if gold_rows else 0)
    assert "LOCK TABLE analytics.pipeline_runs" in statements[0]
    assert "LOCK TABLE analytics.daily_sales" in statements[1]
    assert any("DELETE FROM analytics.daily_sales" in sql for sql in statements)
    assert any("INSERT INTO analytics.pipeline_runs" in sql for sql in statements)


def test_resume_rejects_success_marker_for_another_snapshot(runtime, monkeypatch):
    script, clients, arguments, metrics = runtime
    metrics["batch_date"] = "2026-01-14"
    clients["s3"].get_object.return_value = {"Body": io.BytesIO(json.dumps(metrics).encode())}
    monkeypatch.setattr(sys, "argv", [*arguments, "--resume-run", "run-1"])
    with pytest.raises(ValueError, match="identity"):
        script.main()
    assert clients["s3"].get_object.return_value["Body"].closed
    script.query_athena.assert_not_called()
    script.execute_redshift.assert_not_called()


def test_athena_failure_prevents_redshift_promotion(runtime, monkeypatch):
    script, _, arguments, _ = runtime
    script.stack_outputs.return_value["RedshiftWorkgroupName"] = "retail-warehouse"
    script.reconcile_athena.side_effect = ValueError("Athena reconciliation failed")
    monkeypatch.setattr(sys, "argv", [*arguments, "--resume-run", "run-1", "--load-redshift"])
    with pytest.raises(ValueError, match="reconciliation"):
        script.main()
    script.execute_redshift.assert_not_called()


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    script = load_script("deploy", monkeypatch)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"region": "us-east-1", "stack_name": "retail-dev"}), encoding="utf-8"
    )
    clients = {name: Mock() for name in ("s3", "glue", "cloudformation")}
    clients["glue"].get_paginator.return_value.paginate.return_value = [{"JobRuns": []}]
    session = Mock()
    session.client.side_effect = lambda service, **kwargs: clients[service]
    monkeypatch.setattr(script.boto3, "Session", Mock(return_value=session))
    monkeypatch.setattr(script, "stack_outputs", Mock(return_value=OUTPUTS.copy()))
    monkeypatch.setattr(script, "configure_logging", Mock(return_value=Mock()))
    monkeypatch.setattr(script, "package", Mock(return_value=tmp_path / "retail_pipeline.zip"))
    monkeypatch.setattr(sys, "argv", ["deploy.py", "--config", str(config)])
    return script, clients


def client_error(message, code="ValidationError"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "TestOperation")


@pytest.mark.parametrize("operation", ["create", "update", "unchanged"])
def test_deploy_creates_updates_or_reuses_stack_then_uploads_artifacts(deployment, operation):
    script, clients = deployment
    cf = clients["cloudformation"]
    if operation == "create":
        cf.describe_stacks.side_effect = client_error("Stack does not exist")
    if operation == "unchanged":
        cf.update_stack.side_effect = client_error("No updates are to be performed")
    script.main()
    if operation == "create":
        cf.create_stack.assert_called_once()
        assert cf.create_stack.call_args.kwargs["EnableTerminationProtection"] is True
        cf.update_stack.assert_not_called()
    else:
        cf.update_stack.assert_called_once()
        cf.create_stack.assert_not_called()
    if operation == "unchanged":
        cf.get_waiter.assert_not_called()
    else:
        cf.get_waiter.assert_called_once_with(f"stack_{operation}_complete")
        cf.get_waiter.return_value.wait.assert_called_once()
    assert clients["s3"].upload_file.call_count == 2
    assert [call.args[2] for call in clients["s3"].upload_file.call_args_list] == [
        "artifacts/retail_pipeline.zip",
        "artifacts/glue/retail_etl.py",
    ]


def test_deploy_does_not_replace_artifacts_while_a_job_is_active(deployment):
    script, clients = deployment
    clients["glue"].get_paginator.return_value.paginate.return_value = [
        {"JobRuns": [{"JobRunState": "SUCCEEDED"}]},
        {"JobRuns": [{"JobRunState": "RUNNING"}]},
    ]
    with pytest.raises(RuntimeError, match="Glue job is active"):
        script.main()
    script.package.assert_not_called()
    clients["s3"].upload_file.assert_not_called()


def test_deploy_propagates_access_denied_instead_of_creating_another_stack(deployment):
    script, clients = deployment
    clients["cloudformation"].describe_stacks.side_effect = client_error("Denied", "AccessDenied")
    with pytest.raises(ClientError, match="AccessDenied"):
        script.main()
    clients["cloudformation"].create_stack.assert_not_called()
    clients["cloudformation"].update_stack.assert_not_called()
    clients["s3"].upload_file.assert_not_called()
