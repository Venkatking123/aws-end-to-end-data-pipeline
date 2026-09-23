-- Always select one completed immutable run. Injected projection requires this filter.
SELECT
    order_date,
    country,
    SUM(net_amount) AS net_revenue,
    SUM(units) AS units
FROM ${database}.daily_sales
WHERE run_id = '${run_id}'
GROUP BY order_date, country
ORDER BY order_date, country;

-- Count orders at the line grain when aggregating across categories.
-- Silver order_date is an ISO date STRING partition for efficient projection/pruning.
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(*) AS order_lines,
    SUM(net_amount) AS net_revenue
FROM ${database}.order_lines
WHERE run_id = '${run_id}'
    AND order_date BETWEEN '2020-01-01' AND '2035-12-31'
GROUP BY order_date
ORDER BY order_date;

-- Category performance within each country.
SELECT
    country,
    category,
    SUM(net_amount) AS net_revenue,
    SUM(discount_amount) AS discounts,
    ROUND(100.0 * SUM(discount_amount) / NULLIF(SUM(gross_amount), 0), 2)
        AS discount_percent
FROM ${database}.daily_sales
WHERE run_id = '${run_id}'
GROUP BY country, category
ORDER BY country, net_revenue DESC;
