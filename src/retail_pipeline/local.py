"""Run the exact Glue transformation code against local sample files."""

import argparse
import json
import os
import sys
from pathlib import Path

from retail_pipeline.config import validate_run_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/raw/orders.csv")
    parser.add_argument("--output-dir", default="build/local")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-reject-ratio", type=float, default=0.4)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    root = Path(args.output_dir).resolve()
    manifest_dir = root / "manifests" / args.run_id
    manifest_dir.mkdir(parents=True, exist_ok=False)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    from retail_pipeline.pipeline import execute

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("retail-local")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        metrics = execute(
            spark,
            Path(args.input).resolve().as_posix(),
            root.as_posix(),
            args.run_id,
            args.max_reject_ratio,
        )
        payload = json.dumps(metrics.as_dict(), indent=2)
        (manifest_dir / "success.json").write_text(payload + "\n", encoding="utf-8")
        print(payload)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
