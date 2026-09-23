-- The database is created by CloudFormation. These external tables are idempotent.
-- $$ escapes are resolved by Python string.Template and preserve Athena placeholders.
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.order_lines (
    order_id STRING,
    line_id STRING,
    customer_id STRING,
    product_id STRING,
    category STRING,
    country STRING,
    quantity INT,
    unit_price DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    order_timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    status STRING,
    gross_amount DECIMAL(18, 2),
    net_amount DECIMAL(18, 2),
    _record_hash STRING,
    _source_file STRING
)
PARTITIONED BY (run_id STRING, order_date STRING)
STORED AS PARQUET
LOCATION 's3://${bucket}/silver/order_lines/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'projection.enabled' = 'true',
    'projection.run_id.type' = 'injected',
    'projection.order_date.type' = 'date',
    'projection.order_date.range' = '2020-01-01,2035-12-31',
    'projection.order_date.format' = 'yyyy-MM-dd',
    'projection.order_date.interval' = '1',
    'projection.order_date.interval.unit' = 'DAYS',
    'storage.location.template' = 's3://${bucket}/silver/order_lines/run_id=$${run_id}/data/order_date=$${order_date}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.daily_sales (
    order_date DATE,
    country STRING,
    category STRING,
    order_count BIGINT,
    line_count BIGINT,
    units BIGINT,
    gross_amount DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    net_amount DECIMAL(18, 2)
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 's3://${bucket}/gold/daily_sales/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'projection.enabled' = 'true',
    'projection.run_id.type' = 'injected',
    'storage.location.template' = 's3://${bucket}/gold/daily_sales/run_id=$${run_id}/data/'
);
