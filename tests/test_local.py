"""The local CLI publishes success only after execution and refuses reused runs."""

import json
import sys
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from retail_pipeline import local, pipeline
from retail_pipeline.pipeline import Metrics, QualityError


@pytest.fixture
def local_runtime(monkeypatch, tmp_path):
    session = MagicMock(name="local_spark")
    builder = MagicMock(name="spark_builder")
    builder.master.return_value = builder
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = session
    monkeypatch.setattr(SparkSession, "builder", builder)
    runner = MagicMock(name="execute")
    monkeypatch.setattr(pipeline, "execute", runner)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "retail_pipeline.local",
            "--input",
            str(tmp_path / "input snapshot.csv"),
            "--output-dir",
            str(tmp_path / "output"),
            "--run-id",
            "local-run",
            "--max-reject-ratio",
            "0.4",
        ],
    )
    return session, builder, runner


def test_local_publishes_metrics_after_execution(local_runtime, tmp_path, capsys):
    session, _, runner = local_runtime
    manifest = tmp_path / "output/manifests/local-run/success.json"
    expected = Metrics("local-run", 20, 8, 2, 10, 7, 0.4, "560.50")

    def complete(*args):
        assert manifest.parent.is_dir()
        assert not manifest.exists()
        return expected

    runner.side_effect = complete
    local.main()

    runner.assert_called_once_with(
        session,
        (tmp_path / "input snapshot.csv").as_posix(),
        (tmp_path / "output").as_posix(),
        "local-run",
        0.4,
    )
    assert json.loads(manifest.read_text(encoding="utf-8")) == expected.as_dict()
    assert json.loads(capsys.readouterr().out) == expected.as_dict()
    session.stop.assert_called_once_with()


@pytest.mark.parametrize("failure", [QualityError("quality failed"), OSError("write failed")])
def test_local_failure_leaves_no_success_and_reserves_run_id(local_runtime, tmp_path, failure):
    session, builder, runner = local_runtime
    runner.side_effect = failure
    manifest_dir = tmp_path / "output/manifests/local-run"

    with pytest.raises(type(failure), match=str(failure)):
        local.main()

    assert manifest_dir.is_dir()
    assert not (manifest_dir / "success.json").exists()
    session.stop.assert_called_once_with()
    with pytest.raises(FileExistsError):
        local.main()
    runner.assert_called_once()
    builder.getOrCreate.assert_called_once()


def test_local_refuses_existing_success_before_starting_spark(local_runtime, tmp_path):
    session, builder, runner = local_runtime
    manifest = tmp_path / "output/manifests/local-run/success.json"
    manifest.parent.mkdir(parents=True)
    original = '{"run_id": "local-run", "net_amount": "123.45"}\n'
    manifest.write_text(original, encoding="utf-8")

    with pytest.raises(FileExistsError):
        local.main()

    assert manifest.read_text(encoding="utf-8") == original
    builder.getOrCreate.assert_not_called()
    runner.assert_not_called()
    session.stop.assert_not_called()


def test_local_refuses_unsafe_run_id_before_creating_output(local_runtime, tmp_path, monkeypatch):
    _, builder, runner = local_runtime
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "retail_pipeline.local",
            "--output-dir",
            str(tmp_path / "output"),
            "--run-id",
            "../escape",
        ],
    )

    with pytest.raises(ValueError, match="run_id"):
        local.main()

    assert not (tmp_path / "output").exists()
    builder.getOrCreate.assert_not_called()
    runner.assert_not_called()
