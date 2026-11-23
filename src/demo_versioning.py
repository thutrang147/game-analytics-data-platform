import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from transforms import resolve_event_versions


BASE_DIR = Path(__file__).resolve().parents[1]

spark = (
    SparkSession.builder
    .appName("GameAnalyticsVersioning")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField(name, StringType(), True)
    for name in [
        "event_id", "player_id", "session_id",
        "event_type", "event_at", "source_version",
        "level_id"
    ]
])

path = (
    BASE_DIR / "data/raw/game_events"
    / "arrival_date=2026-11-01/events_001.jsonl"
)

try:
    raw = spark.read.schema(schema).json(str(path))

    # Fixture chuẩn của bài 2 phải có đúng hai dòng
    assert raw.count() == 2

    # Gửi lại E001 với nội dung giống hệt
    duplicate = raw.filter(F.col("event_id") == "E001")

    # E001 được sửa ở phiên bản 2
    corrected = (
        duplicate
        .withColumn("source_version", F.lit("2"))
        .withColumn("session_id", F.lit("S001_FIX"))
    )

    incoming = (
        raw
        .unionByName(duplicate)
        .unionByName(corrected)
    )

    # Bài thử xung đột: E002 version 1 có nội dung khác
    if "--conflict" in sys.argv:
        conflict = (
            raw.filter(F.col("event_id") == "E002")
            .withColumn("session_id", F.lit("S999"))
        )
        incoming = incoming.unionByName(conflict)

    print("\n=== INPUT ===")
    incoming.select(
        "event_id", "session_id", "source_version"
    ).show()

    latest, conflicts = resolve_event_versions(incoming)

    if latest is None:
        quarantine_path = (
            BASE_DIR
            / "data/quarantine/run_id=lesson4_conflict"
        )

        conflicts.write.mode("overwrite").parquet(
            str(quarantine_path)
        )

        print("VERSION_CONFLICT detected!")
        print("Quarantine:", quarantine_path)
        conflicts.select(
            "event_id", "session_id", "error_reason"
        ).show()

        raise RuntimeError("Batch stopped before MERGE")

    print("\n=== OUTPUT AFTER DEDUP ===")
    latest.select(
        "event_id",
        "session_id",
        "version_num"
    ).show()

    # Kiểm tra kết quả nghiệp vụ
    assert latest.count() == 2

    e001_version = (
        latest
        .filter(F.col("event_id") == "E001")
        .select("version_num")
        .first()[0]
    )

    assert e001_version == 2

    print("PASS: Deduplication and versioning")

finally:
    spark.stop()
