-- Submit ALL statements in one BatchExecuteStatement request, which is atomic.
-- With a SQL client, wrap this entire file in BEGIN and COMMIT.
-- Copy only files listed by a successful run's manifest; Parquet column order is fixed.
-- The runner omits COPY for a validated zero-row snapshot (empty manifests are invalid).
-- Acquire locks before CREATE/COPY establishes the transaction snapshot, so
-- concurrent promotion/replay sees the latest committed audit and mart.
-- Every loader takes these locks in this order.
LOCK TABLE analytics.pipeline_runs;
LOCK TABLE analytics.daily_sales;

CREATE TEMP TABLE stage_daily_sales (
    order_date DATE NOT NULL,
    country VARCHAR(2) NOT NULL,
    category VARCHAR(100) NOT NULL,
    order_count BIGINT NOT NULL,
    line_count BIGINT NOT NULL,
    units BIGINT NOT NULL,
    gross_amount DECIMAL(18, 2) NOT NULL,
    discount_amount DECIMAL(18, 2) NOT NULL,
    net_amount DECIMAL(18, 2) NOT NULL
);

COPY stage_daily_sales (
    order_date, country, category, order_count, line_count, units,
    gross_amount, discount_amount, net_amount
)
FROM '${manifest_uri}'
IAM_ROLE '${copy_role_arn}'
MANIFEST
FORMAT AS PARQUET;

-- Each raw batch is a COMPLETE snapshot. Delete+insert rolls back on any error.
DELETE FROM analytics.daily_sales
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.pipeline_runs WHERE run_id = '${run_id}'
);

INSERT INTO analytics.daily_sales (
    order_date, country, category, order_count, line_count, units,
    gross_amount, discount_amount, net_amount, source_run_id, loaded_at
)
SELECT
    order_date, country, category, order_count, line_count, units,
    gross_amount, discount_amount, net_amount, '${run_id}', GETDATE()
FROM stage_daily_sales
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.pipeline_runs WHERE run_id = '${run_id}'
);

INSERT INTO analytics.pipeline_runs (run_id, loaded_at, row_count)
SELECT '${run_id}', GETDATE(), COUNT(*)
FROM stage_daily_sales
HAVING NOT EXISTS (
    SELECT 1 FROM analytics.pipeline_runs WHERE run_id = '${run_id}'
);
