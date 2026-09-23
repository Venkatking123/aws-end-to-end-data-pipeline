# Sample data contract

`raw/orders.csv` contains synthetic order-line data, including intentional defects.
It has 20 data records: 10 surviving valid lines, 2 superseded valid versions, and
8 rejected records. Nine surviving lines are completed and one is cancelled.
The seven daily sales groups total 20 units, 616.50 gross, 56.00 discounts, and
560.50 net. Machine-readable expectations are in `expected/sample_metrics.json`.

CSV files must be uncompressed, have the exact header shown in the sample, use
UTF-8, and have one
physical line per record. Standard quoted commas are supported; embedded newlines
are not. Extra/missing fields and malformed quoting are quarantined. Empty lines
and repeated exact header lines are ignored. Each file's first line is checked
before processing; a missing or mismatched header fails the entire read. Values are trimmed; country and
status are uppercased. Countries use a two-letter shape check, not an ISO registry.
Identifiers and category are required but remain case-sensitive. Category is at
most 100 UTF-8 bytes to match the warehouse. All prices use
one reporting currency, USD; country describes the market, not the currency.

Quantity must be a positive 32-bit integer. Unit price and per-line discount must
be nonnegative fixed-point amounts with at most two fractional digits, with
discount no greater than quantity times unit price. Exponents and implicit
rounding are rejected. Money and calculated per-line amounts must fit
`DECIMAL(18,2)`. Timestamps must be UTC in `yyyy-MM-dd'T'HH:mm:ss` format, with
`updated_at >= order_timestamp`. Status must be `COMPLETED` or `CANCELLED`.
Order dates must be between 2020-01-01 and 2035-12-31, inclusive, matching the
Athena partition projection range.

Validation precedes deduplication on `(order_id, line_id)`. The latest valid
`updated_at` wins; equal timestamps select the lexicographically greatest SHA-256
hash of the normalized business row, followed by the first source filename. The
sample's invalid latest correction for O1002 is quarantined and its prior valid
version survives. Superseded valid versions are preserved in the duplicates
dataset. Cancelled winners remain in silver with zero recognized gross/net and
are excluded entirely from gold; their original unit price and discount remain
available for audit.

Gold groups by UTC order date, country and category. `order_count` is the distinct
order count within that group; summing it across categories overcounts orders
that contain products in more than one category. Each run is a full snapshot:
input must contain all versions needed to reconstruct the desired snapshot.
