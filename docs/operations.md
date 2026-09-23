# Operations and recovery

## Before the first AWS run

Follow the [README setup and deployment steps](../README.md) and review [the infrastructure guide](infrastructure.md). Use a nonproduction AWS account or a dedicated development environment for the first acceptance run.

Confirm the configured Region, resource names, credential profile, raw source URI, and output prefixes. The S3 data bucket and Redshift database must be in the same Region. Verify that the job package and entry-point script are uploaded before starting Glue. Never put AWS keys, database passwords, or exported session tokens in configuration files or command examples.

The checked-in sample is synthetic and deliberately contains bad records. Its demonstration reject threshold accepts those records into quarantine while publishing the valid subset. Set a business-approved threshold for real data; allowing rejects is an explicit data-quality decision.

## Routine run checklist

1. Receive and preserve a complete source snapshot. Record its origin, object key, ingestion date, and expected row count outside the dataset if needed for reconciliation.
2. Create a fresh run identifier and start the Glue job against that snapshot.
3. Wait for the Glue execution to finish. Inspect the driver/error logs when it fails; a service status alone does not reconcile data quality.
4. Confirm the run's success marker exists, then inspect input, rejected, duplicate, silver, and gold counts in its metrics.
5. Run the Athena validation and reporting SQL for the same identifier.
6. Load the gold manifest into Redshift and verify the load audit plus aggregate counts and totals.
7. Record the successful run identifier in the consumer handoff. Retain the previous run for rollback according to the retention policy.

## Monitoring

Glue sends runtime output to CloudWatch. Application events include run context and quality statistics so operators can correlate a source batch with its outputs. Investigate failures by job run identifier and pipeline run identifier together. Raw row values may be available in quarantine; restrict access and retention according to the source's sensitivity.

Track these signals in the operating environment:

| Signal | Why it matters | Suggested response |
| --- | --- | --- |
| Failed or timed-out Glue execution | The snapshot was not published successfully. | Read error logs, check source and IAM access, and retry with a new run identifier after correction. |
| Missing success marker | The output is incomplete or failed its quality gate. | Keep consumers on the prior successful snapshot. |
| Increased rejected-row ratio | Source quality or schema may have changed. | Inspect quarantine reasons and involve the source owner. |
| Unexpected input or gold count | A partial source snapshot can silently shrink the mart. | Reconcile against the source's expected population before loading Redshift. |
| Large duplicate count | Repeated ingestion or conflicting updates may be present. | Review source versioning and selected-record precedence. |
| Athena or Redshift reconciliation mismatch | Consumers may be using different snapshots or schema versions. | Check run identifiers, manifest contents, decimal types, and SQL filters. |
| Increased runtime or bytes scanned | Data growth or query filters may be affecting cost. | Inspect partition filters, file sizes, worker settings, and query plans. |

Log collection and a Glue failure alarm are implemented. The alarm has no notification action by default; attach your alert destination and establish an on-call rotation and service-level objectives. A successful local test does not validate IAM, regional service availability, quota capacity, or account policies.

## Failure handling

### Quality threshold exceeded

Read the error log and quarantine for the failed attempt. A quality failure preserves quarantine and superseded-version output; a success metrics file is only published on success. Correct the source or obtain an explicit business decision to change the threshold. Submit a new complete snapshot with a fresh run identifier. Do not manually create a success marker or load Parquet from the failed attempt.

### Glue failed after writing some objects

Treat the entire run as unpublished. Partial S3 artifacts are useful for diagnosis and do not need to be deleted to rerun safely. Start a new attempt under a new identifier, then remove or expire failed-run artifacts according to retention policy. Do not overwrite a completed run.

### Athena reports a partition-projection error or no rows

Supply an equality predicate for the injected `run_id`. Confirm that the identifier refers to a successful run, the table location uses the correct bucket/prefix, and the selected date lies inside the projection range. An existing table is not evidence that a requested S3 partition exists.

### Redshift COPY fails

Check the manifest URI, its listed object sizes, same-Region placement, the Redshift IAM role association, S3 access policies, and Parquet column order/type compatibility. Keep the existing mart in place while correcting the failure. Use the warehouse load audit to distinguish a committed load from an uncommitted or repeated attempt.

When Glue already succeeded, retry downstream work with `--resume-run` rather than rerunning ETL. Supply the successful identifier and its original batch date:

```bash
python scripts/run_pipeline.py --config build/dev.json --batch-date 2026-01-15 --resume-run RUN_ID_FROM_SUCCESSFUL_RUN --load-redshift
```

Replace the example identifier and date with the saved run identity. The runner verifies that they match the success marker. Omit `--load-redshift` to retry only Athena setup and reconciliation. A warehouse run already recorded as committed is protected against replay.

### A published snapshot is wrong

Stop downstream promotion of the incorrect identifier. Reconcile the source, generate a corrected full snapshot, and publish a fresh run. For a rollback, reprocess the retained prior raw snapshot with a new run identifier, then load that newly published run through the normal warehouse procedure. Replaying an already audited warehouse run is intentionally a no-op and will not restore an older snapshot. Lake consumers can select the prior successful identifier directly. Retain the relevant audit records for traceability.

## Concurrency and retries

Use unique run identifiers and serialize promotion/loading of the current warehouse snapshot. Multiple completed lake runs may coexist, but loading an older snapshot after a newer one can intentionally roll the mart back. The operator controls which snapshot is current; wall-clock completion order is not a source-version policy.

Use a single writer for raw uploads and deployments. The empty-prefix check and raw upload are separate requests, so concurrent uploads to the same batch date can overwrite one another. Deployments replace the shared job artifacts; finish deployment before starting jobs and do not replace artifacts while a job is running. The Glue job's one-run concurrency limit does not serialize these operator commands.

Do not assume a retry is equivalent to incremental processing. Every ETL run computes a complete snapshot. Warehouse retry behavior and the load audit protect the intended replacement workflow, but cross-service exactly-once delivery is not promised.

## Retention, costs, and cleanup

S3 storage, Glue compute, Athena scanning, and Redshift usage incur AWS charges. The local example requires no AWS services. Keep development Glue runs small, use date and run predicates in Athena, and stop or remove unneeded warehouse resources after testing.

The deployment script enables CloudFormation termination protection when creating a stack. Disable that protection intentionally before deleting the stack. The infrastructure retains data-bearing resources where configured to avoid accidental deletion of project data. Review the template's deletion policies before deleting a stack. Plan cleanup explicitly: preserve any required audit evidence, remove obsolete run outputs and query results, empty all bucket versions when retiring a retained bucket, and remove separately created Redshift resources or IAM associations. Do not run blanket recursive deletion commands against an unverified bucket or prefix.

## Production-readiness work

Before adopting this starter as an operating service, set source completeness checks, tested volume limits, retry/promotion ownership, access review, backup/retention policies, incident routing, and restore objectives. Consider a managed orchestrator and catalog-level governance when the number of sources or consumers grows. An AWS acceptance run must verify deployment, execution, Athena queries, Redshift loading, and reconciliation in the intended account.
