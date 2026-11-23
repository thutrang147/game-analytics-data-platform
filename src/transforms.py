from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, StructType


def resolve_event_versions(df):
    """
    Input: DataFrame đã vượt qua bước validation.
    Output:
      - latest: bản ghi có version cao nhất
      - conflicts: các dòng xung đột phiên bản
    """

    # Bước 1: Chuẩn hóa dữ liệu
    normalized = (
        df
        .withColumn("player_id", F.trim("player_id"))
        .withColumn("session_id", F.trim("session_id"))
        .withColumn("event_type", F.trim("event_type"))
        .withColumn("level_id", F.trim("level_id"))
        .withColumn(
            "event_at_ts",
            F.to_timestamp(
                "event_at",
                "yyyy-MM-dd'T'HH:mm:ss.SSSX"
            )
        )
        .withColumn(
            "version_num",
            F.col("source_version").cast("long")
        )
    )

    # Bước 2: Hash các cột nghiệp vụ đã chuẩn hóa
    normalized = normalized.withColumn(
        "payload_hash",
        F.sha2( # tạo hash SHA-256
            F.to_json(  # biểu diễn nhất quán các cột dùng để tính hash
                F.struct(
                    "player_id",
                    "session_id",
                    "event_type",
                    "event_at_ts",
                    "level_id"
                ),
                options={"ignoreNullFields": "false"}
            ),
            256
        )
    )

    # Bước 3: Phát hiện cùng key/version, khác hash
    conflict_keys = (
        normalized
        .groupBy("event_id", "version_num") # gom dữ liệu theo khóa và version
        .agg(
            F.countDistinct("payload_hash") # đếm số hash khác nhau
            .alias("hash_count")
        )
        .filter(F.col("hash_count") > 1)
        .select("event_id", "version_num")
    )

    conflicts = (
        normalized
        .join(
            conflict_keys,
            ["event_id", "version_num"],
            "inner"
        )
        .withColumn(
            "error_reason",
            F.lit("VERSION_CONFLICT")
        )
    )

    # Batch có conflict thì không được chọn kết quả
    if conflicts.limit(1).count() > 0:
        return None, conflicts

    # Bước 4: Bỏ các bản ghi trùng hoàn toàn
    unique_versions = normalized.dropDuplicates([   # loại bản ghi trùng
        "event_id",
        "version_num",
        "payload_hash"
    ])

    # Bước 5: Chọn version lớn nhất của từng event
    window_spec = (
        Window
        .partitionBy("event_id")    # tạo nhóm sự kiện cùng khóa
        .orderBy(F.col("version_num").desc())
    )

    latest = (
        unique_versions
        .withColumn(
            "row_num",
            F.row_number().over(window_spec)    # đánh số từng bản ghi trong nhóm 
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return latest, conflicts


# Shared validation/versioning: reusable from local batch and a future Glue job.

def string_schema(columns, corrupt=False):
    # Explicit strings preserve invalid values for quarantine instead of silently inferring types.
    result = [StructField(c, StringType(), True) for c in columns]
    if corrupt:
        result.append(StructField("_corrupt_record", StringType(), True))
    return StructType(result)


def required_missing(columns):
    condition = F.lit(False)
    for name in columns:
        condition = condition | F.col(name).isNull() | (F.col(name) == "")
    return condition


def validate_rows(df, source):
    """Return typed, valid records and rejected raw records with reasons."""
    trim_cols = (
        ["event_id", "player_id", "session_id", "event_type", "level_id"]
        if source == "game_events"
        else ["payment_event_id", "purchase_id", "player_id", "event_type", "currency"]
    )
    for name in trim_cols:
        df = df.withColumn(name, F.when(F.trim(F.col(name)) != "", F.trim(F.col(name))))
    if source == "payments":
        df = df.withColumn("currency", F.upper("currency"))

    # Explicit casts tolerate bad input even when Glue enables ANSI mode.
    # Require an offset: a local timestamp alone cannot identify a UTC instant.
    df = (
        df.withColumn("parsed_at", F.when(
            F.col("event_at").rlike(r"T.*(?:Z|[+-][0-9]{2}:[0-9]{2})$"),
            F.try_to_timestamp("event_at")))
          .withColumn("version_num", F.when(
              F.col("source_version").rlike(r"^[0-9]+$"),
              F.expr("try_cast(source_version as bigint)")))
          .withColumn("event_date", F.to_date("parsed_at"))
    )

    if source == "game_events":
        bad = (
            F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_JSON")
             .when(required_missing(["event_id", "player_id", "session_id"]),
                   "MISSING_REQUIRED_FIELD")
             .when(F.col("parsed_at").isNull(), "INVALID_TIMESTAMP")
             .when(F.col("version_num").isNull() | (F.col("version_num") < 1),
                   "INVALID_VERSION")
             .when(F.col("event_type").isNull() |
                   ~F.col("event_type").isin("session_start", "level_start", "level_complete"),
                   "INVALID_EVENT_TYPE")
             .when(F.col("event_type").isin("level_start", "level_complete") &
                   (F.col("level_id").isNull() | (F.col("level_id") == "")),
                   "MISSING_LEVEL_ID")
        )
    else:
        df = df.withColumn("amount_num", F.when(F.col("amount_cents").rlike(r"^[0-9]+$"), F.expr("try_cast(amount_cents as bigint)")))
        bad = (
            F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_CSV")
             .when(required_missing(["payment_event_id", "purchase_id", "player_id"]),
                   "MISSING_REQUIRED_FIELD")
             .when(F.col("parsed_at").isNull(), "INVALID_TIMESTAMP")
             .when(F.col("version_num").isNull() | (F.col("version_num") < 1),
                   "INVALID_VERSION")
             .when(F.col("amount_num").isNull() | (F.col("amount_num") <= 0),
                   "INVALID_AMOUNT")
             .when(F.col("currency").isNull() | (F.col("currency") != "USD"),
                   "INVALID_CURRENCY")
             .when(F.col("event_type").isNull() |
                   ~F.col("event_type").isin("purchase", "refund"),
                   "INVALID_PAYMENT_TYPE")
        )

    checked = df.withColumn("error_reason", bad)
    rejected = checked.filter(F.col("error_reason").isNotNull())
    valid = checked.filter(F.col("error_reason").isNull())

    # Keep provenance outside the payload hash; replay may arrive in a new file.
    provenance = [c for c in ("source_file", "source_line", "raw_payload", "arrival_date") if c in valid.columns]
    if source == "game_events":
        ready = valid.select(*provenance,
            "event_id", "player_id", "session_id", "event_type", "level_id",
            F.col("parsed_at").alias("event_at"), "event_date",
            F.col("version_num").alias("source_version"),
        )
    else:
        ready = valid.select(*provenance,
            "payment_event_id", "purchase_id", "player_id", "event_type",
            F.col("parsed_at").alias("event_at"), "event_date",
            F.col("amount_num").alias("amount_cents"), "currency",
            F.col("version_num").alias("source_version"),
        )
    return ready, rejected


def resolve_versions(df, source):
    """Return latest candidates and conflicts; caller MUST stop if conflicts exist."""
    key = "event_id" if source == "game_events" else "payment_event_id"
    fields = (
        ["player_id", "session_id", "event_type", "level_id", "event_at"]
        if source == "game_events"
        else ["purchase_id", "player_id", "event_type", "event_at", "amount_cents", "currency"]
    )
    # A stable business payload hash distinguishes retries from contradictory updates.
    with_hash = df.withColumn(
        "payload_hash",
        F.sha2(F.to_json(F.struct(*[F.col(c) for c in fields]),
                         options={"ignoreNullFields": "false"}), 256),
    )
    conflicts_by_key = (
        with_hash.groupBy(key, "source_version")
                 .agg(F.countDistinct("payload_hash").alias("hash_count"))
                 .filter(F.col("hash_count") > 1)
                 .select(key, "source_version")
    )
    conflicts = with_hash.join(conflicts_by_key, [key, "source_version"], "inner")

    # Gộp retry trước, rồi giữ source_version cao nhất; không sắp theo ngày nhận file.
    unique = with_hash.dropDuplicates([key, "source_version", "payload_hash"])
    window = Window.partitionBy(key).orderBy(F.col("source_version").desc())
    latest = (
        unique.withColumn("rn", F.row_number().over(window))
              .filter(F.col("rn") == 1)
              .drop("rn")
    )
    return latest, conflicts
