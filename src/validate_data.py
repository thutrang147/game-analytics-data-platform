from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

spark = (
    SparkSession.builder
    .appName("GameAnalyticsValidation")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Explicit schema cho game events
# StructType: Định nghĩa schema của DataFrame
event_schema = StructType([
    StructField("event_id", StringType(), True),    # StructField: định nghĩa 1 cột và kiểu dữ liệu
    StructField("player_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_at", StringType(), True),
    StructField("source_version", StringType(), True),
    StructField("level_id", StringType(), True),
])

# 2. Explicit schema cho payments
payment_schema = StructType([
    StructField("payment_event_id", StringType(), True),
    StructField("purchase_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_at", StringType(), True),
    StructField("amount_cents", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("source_version", StringType(), True),
])

# 3. Đọc dữ liệu
events_path = (
    RAW_DIR / "game_events"
    / "arrival_date=2026-11-01"
    / "events_001.jsonl"
)

payments_path = (
    RAW_DIR / "payments"
    / "arrival_date=2026-11-01"
    / "payments_001.csv"
)

events_raw = spark.read.schema(event_schema).json(
    str(events_path)
)

payments_raw = (
    spark.read
    .option("header", True)
    .schema(payment_schema)
    .csv(str(payments_path))
)

# 4. Chuẩn hóa kiểu dữ liệu
timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

events = (
    events_raw
    .withColumn(    # withColumn: tạo hoặc thay thế cột
        "event_at_ts",
        F.to_timestamp("event_at", timestamp_format)    # to_timestamp: chuyển string thành timestamp
    )
    .withColumn(
        "version_num",
        F.col("source_version").cast("long")    # ép kiểu thành số nguyên 64-bit
    )
    .withColumn("event_date", F.to_date("event_at_ts"))
)

payments = (
    payments_raw
    .withColumn(
        "event_at_ts",
        F.to_timestamp("event_at", timestamp_format)
    )
    .withColumn(
        "version_num",
        F.col("source_version").cast("long")
    )
    .withColumn(
        "amount_num",
        F.col("amount_cents").cast("long")
    )
    .withColumn("event_date", F.to_date("event_at_ts"))
)

# 5. Validation game events
events = events.withColumn(
    "error_reason",
    F.when( # case when
        F.col("event_id").isNull()
        | (F.trim("event_id") == "")
        | F.col("player_id").isNull()
        | (F.trim("player_id") == "")
        | F.col("session_id").isNull()
        | (F.trim("session_id") == ""),
        "MISSING_REQUIRED_FIELD"
    )
    .when(
        F.col("event_at_ts").isNull(),
        "INVALID_TIMESTAMP"
    )
    .when(
        F.col("version_num").isNull()
        | (F.col("version_num") < 1),
        "INVALID_VERSION"
    )
    .when(
        ~F.col("event_type").isin(
            "session_start",
            "level_start",
            "level_complete"
        ) | F.col("event_type").isNull(),
        "INVALID_EVENT_TYPE"
    )
    .when(
        F.col("event_type").isin(
            "level_start", "level_complete"
        )
        & (
            F.col("level_id").isNull()
            | (F.trim("level_id") == "")
        ),
        "MISSING_LEVEL_ID"
    )
)

# 6. Validation payments
payments = payments.withColumn(
    "error_reason",
    F.when(
        F.col("payment_event_id").isNull()
        | (F.trim("payment_event_id") == "")
        | F.col("purchase_id").isNull()
        | (F.trim("purchase_id") == "")
        | F.col("player_id").isNull()
        | (F.trim("player_id") == ""),
        "MISSING_REQUIRED_FIELD"
    )
    .when(
        F.col("event_at_ts").isNull(),
        "INVALID_TIMESTAMP"
    )
    .when(
        F.col("version_num").isNull()
        | (F.col("version_num") < 1),
        "INVALID_VERSION"
    )
    .when(
        F.col("amount_num").isNull()
        | (F.col("amount_num") <= 0),
        "INVALID_AMOUNT"
    )
    .when(
        F.col("currency").isNull()
        | (F.col("currency") != "USD"),
        "INVALID_CURRENCY"
    )
    .when(
        F.col("event_type").isNull()
        | ~F.col("event_type").isin(
            "purchase", "refund"
        ),
        "INVALID_PAYMENT_TYPE"
    )
)

# 7. Tách valid và invalid records
valid_events = events.filter(
    F.col("error_reason").isNull()
)
invalid_events = events.filter(
    F.col("error_reason").isNotNull()
)

valid_payments = payments.filter(
    F.col("error_reason").isNull()
)
invalid_payments = payments.filter(
    F.col("error_reason").isNotNull()
)

# 8. Kiểm tra kết quả
print("\n=== GAME EVENTS ===")
print("Valid:", valid_events.count())
print("Invalid:", invalid_events.count())

print("\n=== PAYMENTS ===")
print("Valid:", valid_payments.count())
print("Invalid:", invalid_payments.count())

print("\n=== VALID GAME EVENTS ===")
valid_events.select(
    "event_id", "player_id", "event_type", "event_date"
).show(truncate=False)

print("\n=== INVALID GAME EVENTS ===")
invalid_events.select(
    "event_id", "error_reason"
).show(truncate=False)

spark.stop()
