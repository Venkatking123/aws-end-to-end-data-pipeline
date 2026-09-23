-- Run once through the Redshift Data API or a SQL client in the configured database.
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.daily_sales (
    order_date DATE NOT NULL,
    country VARCHAR(2) NOT NULL,
    category VARCHAR(100) NOT NULL,
    order_count BIGINT NOT NULL,
    line_count BIGINT NOT NULL,
    units BIGINT NOT NULL,
    gross_amount DECIMAL(18, 2) NOT NULL,
    discount_amount DECIMAL(18, 2) NOT NULL,
    net_amount DECIMAL(18, 2) NOT NULL,
    source_run_id VARCHAR(128) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (order_date, country, category)
ENCODE AUTO;

-- Deliberately no primary key: Redshift uniqueness constraints are informational.
-- The loader uses explicit locks and NOT EXISTS to enforce replay protection.
CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    run_id VARCHAR(128) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT GETDATE(),
    row_count BIGINT NOT NULL
)
DISTSTYLE ALL
SORTKEY (loaded_at)
ENCODE AUTO;
