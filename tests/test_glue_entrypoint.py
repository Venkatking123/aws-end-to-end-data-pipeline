"""Exercise Glue publication ordering without the managed Glue runtime or AWS."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def glue_job(monkeypatch):
    args = {
        "JOB_NAME": "retail-dev-etl",
        "DATA_BUCKET": "retail-test-bucket",
        "RAW_BATCH_DATE": "2026-01-15",
        "RUN_ID": "test-run",
        "MAX_REJECT_RATIO": "0.4",
    }
    context = SimpleNamespace(spark_session=object())
    job = Mock()
    context_module = ModuleType("awsglue.context")
    context_module.GlueContext = Mock(return_value=context)
    job_module = ModuleType("awsglue.job")
    job_module.Job = Mock(return_value=job)
    utils_module = ModuleType("awsglue.utils")
    utils_module.getResolvedOptions = Mock(return_value=args)
    for name, module in {
        "awsglue": ModuleType("awsglue"),
        "awsglue.context": context_module,
        "awsglue.job": job_module,
        "awsglue.utils": utils_module,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).resolve().parents[1] / "glue/retail_etl.py"
    spec = importlib.util.spec_from_file_location("retail_glue_entrypoint", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    s3 = Mock()
    monkeypatch.setattr(module.boto3, "client", Mock(return_value=s3))
    monkeypatch.setattr(module, "SparkContext", Mock())
    monkeypatch.setattr(module, "ensure_empty_prefix", Mock())
    monkeypatch.setattr(module, "configure_logging", Mock(return_value=Mock()))
    monkeypatch.setattr(module, "execute", Mock())
    monkeypatch.setattr(module, "build_manifest", Mock(return_value={"entries": ["gold"]}))
    publications = {}
    order = []

    def publish(_client, _bucket, key, payload):
        publications[key.rsplit("/", 1)[-1]] = payload
        order.append(key.rsplit("/", 1)[-1])

    monkeypatch.setattr(module, "put_json", Mock(side_effect=publish))
    job.commit.side_effect = lambda: order.append("commit")
    return SimpleNamespace(
        module=module,
        job=job,
        s3=s3,
        context=context,
        publications=publications,
        order=order,
    )


@pytest.mark.parametrize("gold_rows", [0, 7])
def test_success_publishes_only_after_manifest_and_job_commit(glue_job, gold_rows):
    state = glue_job
    metrics = {"run_id": "test-run", "gold_rows": gold_rows, "net_amount": "0.00"}
    state.module.execute.return_value = SimpleNamespace(
        gold_rows=gold_rows, as_dict=lambda: metrics
    )

    state.module.main()

    assert state.order == ["started.json", "redshift.json", "commit", "success.json"]
    assert state.publications["success.json"]["batch_date"] == "2026-01-15"
    assert state.publications["success.json"]["gold_rows"] == gold_rows
    state.module.execute.assert_called_once_with(
        state.context.spark_session,
        "s3://retail-test-bucket/raw/batch_date=2026-01-15/",
        "s3://retail-test-bucket",
        "test-run",
        0.4,
    )
    if gold_rows:
        state.module.build_manifest.assert_called_once_with(
            state.s3, "retail-test-bucket", "test-run"
        )
        assert state.publications["redshift.json"] == {"entries": ["gold"]}
    else:
        state.module.build_manifest.assert_not_called()
        assert state.publications["redshift.json"] == {"entries": []}


@pytest.mark.parametrize("failure_point", ["GlueContext", "execute", "build_manifest"])
def test_failures_publish_diagnostics_without_success_or_commit(glue_job, failure_point):
    state = glue_job
    state.module.execute.return_value = SimpleNamespace(gold_rows=1)
    getattr(state.module, failure_point).side_effect = RuntimeError("injected failure")

    with pytest.raises(RuntimeError, match="injected failure"):
        state.module.main()

    assert state.order == ["started.json", "failure.json"]
    assert state.publications["failure.json"]["run_id"] == "test-run"
    state.job.commit.assert_not_called()


def test_existing_run_is_rejected_before_any_publication(glue_job):
    state = glue_job
    state.module.ensure_empty_prefix.side_effect = FileExistsError("already published")

    with pytest.raises(FileExistsError, match="already published"):
        state.module.main()

    assert state.publications == {}
    state.module.execute.assert_not_called()
