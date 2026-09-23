"""Explicit input and published table schemas; no inference is used."""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_COLUMNS = [
    "order_id",
    "line_id",
    "customer_id",
    "product_id",
    "category",
    "country",
    "quantity",
    "unit_price",
    "discount_amount",
    "order_timestamp",
    "updated_at",
    "status",
]

# All source fields remain strings until validation has classified them.
RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in RAW_COLUMNS])
MONEY_TYPE = DecimalType(18, 2)
SILVER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", MONEY_TYPE, True),
        StructField("discount_amount", MONEY_TYPE, True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("gross_amount", MONEY_TYPE, True),
        StructField("net_amount", MONEY_TYPE, True),
        StructField("order_date", DateType(), True),
        StructField("_record_hash", StringType(), True),
        StructField("_source_file", StringType(), True),
    ]
)
GOLD_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), True),
        StructField("country", StringType(), True),
        StructField("category", StringType(), True),
        StructField("order_count", LongType(), False),
        StructField("line_count", LongType(), False),
        StructField("units", LongType(), True),
        StructField("gross_amount", MONEY_TYPE, True),
        StructField("discount_amount", MONEY_TYPE, True),
        StructField("net_amount", MONEY_TYPE, True),
    ]
)
