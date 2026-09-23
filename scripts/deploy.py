"""Provision AWS infrastructure and upload the versioned ETL artifacts."""

import argparse
import json
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from package_glue import package

from retail_pipeline.aws import stack_outputs
from retail_pipeline.config import Config
from retail_pipeline.logging import configure_logging

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = Config.load(args.config)
    logger = configure_logging()
    session = boto3.Session(region_name=config.region)
    sdk_config = BotoConfig(retries={"mode": "standard", "max_attempts": 5})
    cf = session.client("cloudformation", config=sdk_config)
    parameters = {
        "ProjectName": config.project_name,
        "Environment": config.environment,
        "DeployRedshift": str(config.deploy_redshift).lower(),
        "RedshiftDatabaseName": config.redshift_database,
        "RedshiftSubnetIds": config.redshift_subnet_ids,
        "RedshiftSecurityGroupIds": config.redshift_security_group_ids,
    }
    request = {
        "StackName": config.stack_name,
        "TemplateBody": (ROOT / "infrastructure" / "cloudformation.yaml").read_text(),
        "Parameters": [
            {"ParameterKey": key, "ParameterValue": value} for key, value in parameters.items()
        ],
        "Capabilities": ["CAPABILITY_IAM"],
        "Tags": [
            {"Key": "Project", "Value": config.project_name},
            {"Key": "Environment", "Value": config.environment},
        ],
    }
    exists = True
    try:
        cf.describe_stacks(StackName=config.stack_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ValidationError" and "does not exist" in str(exc):
            exists = False
        else:
            raise
    waiter = None
    if exists:
        try:
            cf.update_stack(**request)
            waiter = "stack_update_complete"
        except ClientError as exc:
            if "No updates are to be performed" not in str(exc):
                raise
    else:
        cf.create_stack(**request, EnableTerminationProtection=True)
        waiter = "stack_create_complete"
    if waiter:
        logger.info("stack_deploying", extra={"fields": {"stack": config.stack_name}})
        cf.get_waiter(waiter).wait(
            StackName=config.stack_name, WaiterConfig={"Delay": 15, "MaxAttempts": 240}
        )
    outputs = stack_outputs(cf, config.stack_name)
    glue = session.client("glue", config=sdk_config)
    active = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
    for page in glue.get_paginator("get_job_runs").paginate(JobName=outputs["GlueJobName"]):
        if any(run["JobRunState"] in active for run in page["JobRuns"]):
            raise RuntimeError("Cannot replace artifacts while a Glue job is active")
    s3 = session.client("s3", config=sdk_config)
    for source, key in (
        (package(), "artifacts/retail_pipeline.zip"),
        (ROOT / "glue" / "retail_etl.py", "artifacts/glue/retail_etl.py"),
    ):
        s3.upload_file(
            str(source),
            outputs["DataBucketName"],
            key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
    logger.info("deployment_ready", extra={"fields": outputs})
    print(json.dumps(outputs, indent=2))


if __name__ == "__main__":
    main()
