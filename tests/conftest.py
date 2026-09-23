"""Real local Spark fixtures; integration tests share one small JVM."""

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    source_path = str(Path(__file__).resolve().parents[1] / "src")
    os.environ["PYTHONPATH"] = os.pathsep.join(
        part for part in (source_path, os.environ.get("PYTHONPATH")) if part
    )
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", warehouse.as_uri())
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
