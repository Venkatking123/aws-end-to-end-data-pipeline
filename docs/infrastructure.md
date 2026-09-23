# Infrastructure and deployment reference

`infrastructure/cloudformation.yaml` provisions the lake, runtime roles, Glue job, catalog database, Athena workgroup, and monitoring. Redshift Serverless is an explicit opt-in because it requires an existing VPC and incurs additional cost. Run the commands in the [README](../README.md) for the complete deployment, artifact upload, sample ingestion, and pipeline execution sequence.

## Resources and defaults

| Resource | Default configuration |
| --- | --- |
| S3 | Generated bucket name; SSE-S3 encryption; versioning; ACLs disabled; public access blocked; TLS required |
| Glue | `retail-pipeline-dev-etl`; Glue 5.0; two G.1X workers; 60-minute timeout; no automatic retries; one concurrent run |
| Catalog | `retail_pipeline_dev`; tables created from `sql/athena/001_tables.sql` |
| Athena | `retail-pipeline-dev-analytics`; engine 3; encrypted results; enforced workgroup settings; 1 GiB per-query scan cutoff |
| CloudWatch | Custom Glue error/output/insights log groups with 30-day retention; failed-run alarm |
| Redshift, when enabled | Private workgroup `retail-pipeline-dev-redshift`; namespace `retail-pipeline-dev-warehouse`; database `analytics`; 8 base RPUs and 32 maximum RPUs |

Project names can contain lowercase letters, digits, and hyphens. The catalog database replaces hyphens with underscores. Choose a different project/environment pair for an independent deployment in the same account and Region.

The S3 bucket, bucket policy, and optional Redshift namespace have `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain`. Raw inputs and published runs have no automatic expiry. Scratch data expires after 7 days; Athena results and Spark events after 30 days, including older object versions. Incomplete multipart uploads are removed after 7 days. Retention protects recovery data but continues to incur storage costs.

## Deploy the stack directly

Use a current AWS CLI v2 and an authenticated AWS IAM Identity Center profile or an assumed deployment role. Keep AWS credentials outside the repository. Confirm the intended account with `aws sts get-caller-identity` before deployment. Glue, Athena, S3, and Redshift must use the same Region; Parquet COPY requires the bucket and warehouse to be colocated. See [AWS columnar COPY requirements](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html).

The following single-line command works in PowerShell and Bash:

```text
aws cloudformation deploy --template-file infrastructure/cloudformation.yaml --stack-name retail-pipeline-dev --region us-east-1 --capabilities CAPABILITY_IAM --parameter-overrides ProjectName=retail-pipeline Environment=dev DeployRedshift=false RedshiftDatabaseName=analytics
```

Review the created identifiers:

```text
aws cloudformation describe-stacks --stack-name retail-pipeline-dev --region us-east-1 --query "Stacks[0].Outputs" --output table
```

The job initially references artifact locations that do not yet contain files. Before starting it, package and upload `dist/retail_pipeline.zip` to `artifacts/retail_pipeline.zip` and `glue/retail_etl.py` to `artifacts/glue/retail_etl.py` in `DataBucketName`. The repository deployment script performs this step. The role cannot overwrite raw inputs or artifacts.

Start each Glue attempt with a new `--RUN_ID`, the intended `--RAW_BATCH_DATE`, and an explicit quality threshold. The stack defaults `--MAX_REJECT_RATIO` to `0.4` for the deliberately imperfect demonstration data; select a stricter threshold for real feeds. Job bookmarks are disabled because each batch is a full snapshot. Automatic Glue retries are disabled because reusing an output run ID is forbidden.

## Enable Redshift Serverless

Set `deploy_redshift` to `true` in the local configuration and provide `redshift_subnet_ids` and `redshift_security_group_ids`, or pass the equivalent CloudFormation parameters. The template validates that these lists are nonempty but cannot verify routing, AZ coverage, or available IP space.

Supply three existing private subnets in one VPC, spanning three supported Availability Zones. This template enables enhanced VPC routing. At 8 base RPUs AWS requires at least nine free addresses in each subnet; reserve additional room for scaling. Check the [current Serverless network requirements](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-usage-considerations.html) for the selected Region and capacity.

Enable VPC DNS resolution and DNS hostnames. Associate an S3 gateway endpoint with the selected subnet route tables, and allow its endpoint policy to read this bucket's `gold/` and `manifests/` prefixes. Security groups and network ACLs must permit the required outbound HTTPS traffic and return traffic. A NAT path is an alternative, with additional cost. Data API access uses AWS service endpoints and does not require public database ingress. Direct SQL clients require an authorized network path to the private workgroup. See [enhanced VPC routing configuration](https://docs.aws.amazon.com/redshift/latest/mgmt/enhanced-vpc-enabling-cluster.html).

For example, replace the placeholder IDs with existing network resources:

```text
aws cloudformation deploy --template-file infrastructure/cloudformation.yaml --stack-name retail-pipeline-dev --region us-east-1 --capabilities CAPABILITY_IAM --parameter-overrides ProjectName=retail-pipeline Environment=dev DeployRedshift=true RedshiftDatabaseName=analytics RedshiftSubnetIds=subnet-11111111,subnet-22222222,subnet-33333333 RedshiftSecurityGroupIds=sg-11111111
```

`ManageAdminPassword: true` lets Redshift generate and manage the bootstrap administrator credentials in Secrets Manager; no password is accepted by this project. The runner obtains the managed secret ARN through `GetNamespace` and passes that reference to the Data API. It does not print or persist the password. Managed admin credentials are suitable for initial setup; use a scoped database loader identity for an established production environment. [AWS documents managed namespace passwords](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-redshiftserverless-namespace.html).

To use an existing Serverless warehouse, leave `deploy_redshift` false, configure its workgroup/database and optional `redshift_secret_arn`, and associate the stack's `RedshiftCopyRoleArn` with its namespace. Preserve all existing namespace roles when adding the COPY role. An ARN is an identifier, not a secret. Without a secret ARN, Data API uses the caller's IAM identity; an administrator must grant the resulting database user the required privileges. IAM role `loader` maps to `IAMR:loader`, while IAM user `loader` maps to `IAM:loader`. See [Data API authentication and permissions](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-iam.html).

## Permission boundaries

The two runtime roles are separate from the human/CI deployment and orchestration identity:

| Identity | Required access |
| --- | --- |
| Glue execution role, created by stack | Read raw/artifact objects; read and write derived outputs, audit, manifests, scratch, and Spark events; CloudWatch logs under this stack; metrics in the Glue namespace |
| Redshift COPY role, created by stack | Read only `gold/` and `manifests/` objects in this bucket and scoped bucket metadata/list access |
| Infrastructure deployment identity | CloudFormation stack lifecycle and creation/configuration of the resource types in the template; scoped IAM role creation/pass-role; optional Redshift and managed-secret provisioning |
| Pipeline orchestration identity | Describe this stack; upload artifacts/raw snapshots; read completion/manifest objects; start/get/stop this Glue job; Athena query/start/status/result/stop for this workgroup; S3 query-result reads/writes; Glue catalog read/create-table rights in this database |
| Warehouse orchestration identity | Redshift Data API execute/batch/status/result/cancel; `GetNamespace` for the created namespace; scoped secret access when using Secrets Manager, or `GetCredentials` for IAM authentication |

Use resource-scoped policies for the account's deployment convention instead of granting these permissions broadly. Data API statement/result access should be limited to the executing principal where supported. With a customer-managed KMS key on an existing secret, the caller also needs the corresponding decrypt permission. The generated stack uses service-managed encryption by default.

The warehouse loader needs database `CREATE` to initialize the schema, schema `USAGE`/`CREATE`, `SELECT`/`INSERT`/`DELETE` and lock privileges on the two tables, temporary table creation, and permission to assume the COPY role. An administrator can initialize the schema once and grant a dedicated loader only the runtime subset. Redshift constraints are not used as uniqueness enforcement; the loader serializes promotion with explicit table locks and an audit check.

The catalog assumes ordinary IAM access. If the account is governed by Lake Formation, grant the orchestration/query principal the appropriate database, table, and data-location permissions according to that account's policy before running Athena DDL or queries. This template does not change account-wide Lake Formation defaults.

## Catalog and publication

Athena uses partition projection, so no crawler or `MSCK REPAIR TABLE` is required. Silver projects `run_id` plus ISO `order_date` strings in the inclusive 2020–2035 range. Gold projects `run_id` and keeps `order_date` as a physical Parquet date. The double-dollar placeholders in the SQL source are intentional: the Python renderer turns `$${run_id}` into the literal `${run_id}` that Athena needs in its location template.

Every analytical query must filter a completed run ID. Injected projection requires an equality filter, but it does not check publication status; the orchestration runner checks the success manifest before reading the run. S3 versioning is recovery protection, not Object Lock. Immutability is enforced by the pipeline's unique-run guards and operating permissions.

The Redshift loader submits all locking, staging, COPY, DELETE, INSERT, and audit statements as one Data API batch. Locks come first, before a table creation or COPY establishes the transaction snapshot, so concurrent loaders see the latest committed audit. See [Redshift isolation behavior](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html). With an interactive SQL client, wrap the complete load script in `BEGIN`/`COMMIT`. A zero-row, validated snapshot publishes an empty manifest, omits COPY, and atomically clears the current warehouse snapshot. Repeating a committed run ID leaves the current warehouse unchanged. Rebuild an older raw snapshot under a new run ID to restore it intentionally.

## Monitoring, cost, and removal

The CloudWatch alarm watches `glue.error.ALL` with the documented job/run/type/observability dimensions. It has no notification action by default; attach an existing SNS topic or connect it to the organization's alerting route. Logs and explicit runner errors remain the primary diagnostics. The metrics and Glue 5 log prefixes follow [Glue observability](https://docs.aws.amazon.com/glue/latest/dg/monitor-observability.html) and [Glue logging](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging.html). The separate [job-insights streams](https://docs.aws.amazon.com/glue/latest/dg/monitor-job-insights.html) use an explicit continuous-logging group under the same stack prefix, matching the execution role's log permissions.

Set account budgets before sustained operation. The Athena cutoff limits individual query scans; it is not an account spending cap. Glue worker/timeout limits and Redshift maximum capacity bound a single workload's resources, but repeated runs still incur charges. Redshift storage, retained S3 data/versions, logs, secrets, and optional network resources may continue billing between runs.

Deleting the stack removes the job, workgroup, catalog, runtime roles, and logs while retaining the data bucket/policy and optional Redshift namespace. Save stack outputs before deletion. Retained namespaces still have data and managed-secret/storage costs; IAM role associations may reference roles removed with the stack and must be replaced before reuse. Review and remove retained resources separately only after confirming backups and retention obligations. Athena's workgroup refuses deletion while it still has saved named queries because `RecursiveDeleteOption` is false; remove those queries explicitly if required. This project never empties retained storage automatically.
