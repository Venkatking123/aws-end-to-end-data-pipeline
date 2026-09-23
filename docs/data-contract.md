# Retail data contract

## Source and grain

The source is a plain, uncompressed UTF-8 CSV containing the exact header below and a complete snapshot of retail order lines. Each logical line is identified by `(order_id, line_id)`. Input rows can include multiple versions of a line; the transformation validates rows and then selects one current version per key. Each record occupies one physical line. Standard quoted commas are supported; embedded newlines are not. The first line of every resolved input file must match the required header, including column order; a missing or mismatched header fails the read. Blank data lines and exact repeated header lines are ignored.

Required header:

```csv
order_id,line_id,customer_id,product_id,category,country,quantity,unit_price,discount_amount,order_timestamp,updated_at,status
```

| Field | Accepted values and meaning |
| --- | --- |
| `order_id` | Required, trimmed, case-sensitive order identifier. |
| `line_id` | Required, trimmed, case-sensitive line identifier within an order. |
| `customer_id` | Required, trimmed, case-sensitive identifier; no customer names are required. |
| `product_id` | Required, trimmed, case-sensitive product identifier. |
| `category` | Required, trimmed, case-sensitive sales category, at most 100 UTF-8 bytes to match the warehouse column. |
| `country` | Trimmed and uppercased; exactly two letters. This is a shape check, not validation against an ISO registry. |
| `quantity` | Digits representing a positive 32-bit integer. |
| `unit_price` | Nonnegative fixed-point amount with at most two fractional digits, fitting `DECIMAL(18,2)`. |
| `discount_amount` | Nonnegative fixed-point discount for the entire line, fitting `DECIMAL(18,2)` and no greater than quantity times unit price. |
| `order_timestamp` | UTC time in `yyyy-MM-dd'T'HH:mm:ss`, for example `2026-09-01T10:30:00`; UTC order date must lie between `2020-01-01` and `2035-12-31`, inclusive, matching Athena projection. |
| `updated_at` | UTC source revision time in the same format, greater than or equal to `order_timestamp`. |
| `status` | Trimmed and uppercased; `COMPLETED` or `CANCELLED`. |

Use a single currency for a snapshot. The supplied sample uses USD across all countries; country identifies the market. The schema has no currency column or exchange-rate source, so combining different currencies would produce invalid financial totals. The sample amounts are illustrative retail measures, not accounting or payment records. Scientific notation, implicit rounding of extra decimal places, timezone suffixes, and offset timestamps are outside this contract.

## Business calculations

```text
gross_amount = quantity * unit_price
net_amount   = gross_amount - discount_amount
```

Discounts cannot exceed gross line value. Decimal types preserve currency precision; binary floating-point arithmetic is not the contract for monetary calculations. Both calculated per-line amounts must fit `DECIMAL(18,2)`. The formulas apply to completed lines; cancelled winners have recognized `gross_amount` and `net_amount` set to zero while preserving their original unit price and discount for audit.

Silver represents the valid current order lines, including cancelled lines for traceability. Gold represents sales grouped by `(order_date, country, category)` after excluding cancelled lines. Distinct order counts are meaningful within each group; summing group-level distinct counts across categories can count an order more than once.

## Validation and quarantine

The transformation applies an explicit schema and rejects values that cannot satisfy the contract. Invalid rows are retained separately with rejection reasons so the source can be corrected. Rejected input is not silently coerced into a valid business row. Incorrect field counts and malformed quoting are classified as `malformed_csv`; other reasons identify the invalid field, missing identifier, update-before-order timestamps, excessive discount, or amount overflow. A row can carry more than one reason.

The rejected-row ratio is calculated against the input row count. The configured maximum ratio is a publication gate; values above the threshold fail the run. Duplicate valid versions are tracked separately from invalid rows. A business-approved threshold is required before loading a real source, particularly where incomplete data is unacceptable.

## Snapshot and version rules

Every batch is the complete current population intended for analytics. For multiple valid versions of a key, the latest `updated_at` wins. Equal timestamps select the lexicographically greatest SHA-256 hash of the normalized source business row, then the first source filename. Exact duplicate business values are equivalent for reporting. Superseded valid rows are preserved in a separate duplicates dataset. Replacing a line in a later snapshot changes its entire current contribution; this project does not retain a slowly changing dimension or an event-sourced history inside the current mart.

Validation occurs before duplicate resolution. If a newer version is rejected and an older valid version is present in the same file, the older valid version can remain in silver. Operators must review rejects rather than assume they cannot affect the meaning of the published snapshot. Quarantined source corrections should be resubmitted in a new complete snapshot.

## Published schemas

Silver contains the 12 typed source fields plus `gross_amount DECIMAL(18,2)`, `net_amount DECIMAL(18,2)`, `order_date DATE`, `_record_hash STRING`, and `_source_file STRING`. The order date is stored as a partition directory in silver Parquet. The hash supports deterministic selection and the source filename supplies record provenance.

Gold Parquet has the following exact column order. Athena's run identifier is a projected partition column and is not an extra physical Parquet field.

| Column | Type | Meaning |
| --- | --- | --- |
| `order_date` | `DATE` | UTC order date. |
| `country` | `STRING` | Market country code. |
| `category` | `STRING` | Product category. |
| `order_count` | `BIGINT` | Distinct completed orders within the group. |
| `line_count` | `BIGINT` | Completed order lines within the group. |
| `units` | `BIGINT` | Sum of completed quantity. |
| `gross_amount` | `DECIMAL(18,2)` | Gross completed sales. |
| `discount_amount` | `DECIMAL(18,2)` | Discounts on completed lines. |
| `net_amount` | `DECIMAL(18,2)` | Gross sales minus discounts. |

## Sample reconciliation

The checked-in [sample](../data/raw/orders.csv) has 20 input records: 8 are rejected, 2 are superseded valid versions, and 10 remain in silver. Of the 10 current lines, 9 are completed and 1 is cancelled. Gold contains 7 groups, 20 units, USD 616.50 gross sales, USD 56.00 discounts, and USD 560.50 net sales. The reject ratio is `0.40`; the demonstration run must allow at least this ratio.

The invalid latest correction for order `O1002` is quarantined while its prior valid version survives, illustrating why rejected corrections need review. Machine-readable expectations are stored in [sample_metrics.json](../data/expected/sample_metrics.json).

## Evolution

Changes to required columns, accepted domains, timestamp interpretation, decimal precision, aggregation grain, or output column order are contract changes. Coordinate source updates, PySpark code, fixtures, Athena DDL, Redshift DDL/COPY, and consumer queries in the same change. Run the tests and a new AWS acceptance snapshot before promoting the change.

See [schemas.py](../src/retail_pipeline/schemas.py), [transforms.py](../src/retail_pipeline/transforms.py), and [the sample notes](../data/README.md) for the implementation, and [the README](../README.md) for the executable local example.
