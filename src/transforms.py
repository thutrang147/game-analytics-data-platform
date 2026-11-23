from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
