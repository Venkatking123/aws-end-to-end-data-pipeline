-- Execute only after manifests/<run_id>/success.json exists and Glue succeeded.
-- Fail promotion when invalid_rows>0 or counts/totals differ from the manifest.
-- An all-cancelled snapshot legitimately has zero gold rows and zero totals.
SELECT
    COUNT(*) AS gold_rows,
    COALESCE(SUM(line_count), 0) AS line_count,
    COALESCE(SUM(units), 0) AS units,
    COALESCE(SUM(net_amount), CAST(0 AS DECIMAL(18, 2))) AS net_revenue,
    COUNT_IF(
        order_date IS NULL OR country IS NULL OR category IS NULL
        OR order_count IS NULL OR line_count IS NULL OR units IS NULL
        OR gross_amount IS NULL OR discount_amount IS NULL OR net_amount IS NULL
        OR order_count <= 0 OR line_count <= 0 OR units <= 0
        OR order_count > line_count OR line_count > units
        OR gross_amount < 0 OR discount_amount < 0 OR net_amount < 0
        OR net_amount <> gross_amount - discount_amount
    ) AS invalid_rows
FROM ${database}.daily_sales
WHERE run_id = '${run_id}';
