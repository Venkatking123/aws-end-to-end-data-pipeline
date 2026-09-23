-- Sales by day. Category-level distinct order counts are not additive across categories.
SELECT
    order_date,
    SUM(line_count) AS line_count,
    SUM(units) AS units,
    SUM(net_amount) AS net_revenue
FROM analytics.daily_sales
GROUP BY order_date
ORDER BY order_date;

-- Category contribution to current snapshot revenue.
SELECT
    category,
    SUM(net_amount) AS net_revenue,
    ROUND(100.0 * SUM(net_amount) / NULLIF(SUM(SUM(net_amount)) OVER (), 0), 2)
        AS revenue_share_percent
FROM analytics.daily_sales
GROUP BY category
ORDER BY net_revenue DESC;

-- The table must contain exactly one promoted snapshot when nonempty.
SELECT
    COUNT(*) AS gold_rows,
    COUNT(DISTINCT source_run_id) AS snapshot_count,
    SUM(net_amount) AS net_revenue,
    SUM(CASE WHEN net_amount <> gross_amount - discount_amount THEN 1 ELSE 0 END)
        AS inconsistent_rows
FROM analytics.daily_sales;

SELECT run_id, loaded_at, row_count
FROM analytics.pipeline_runs
ORDER BY loaded_at DESC;
