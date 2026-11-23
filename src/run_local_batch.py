import argparse
import csv
import io
import json
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

from verify_manifest import MANIFEST_PATH, verify_manifest
from transforms import string_schema, validate_rows, resolve_versions

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

EVENT_COLS = [
    "event_id", "player_id", "session_id", "event_type",
    "event_at", "source_version", "level_id",
]
PAYMENT_COLS = [
    "payment_event_id", "purchase_id", "player_id", "event_type",
    "event_at", "amount_cents", "currency", "source_version",
]


def read_source(spark, manifest, source, data_dir):
    """Local adapter: strict file envelope, permissive rows with original evidence.

    Only listed files are opened. Glue can replace this adapter with a distributed
    reader while keeping validation and version resolution unchanged.
    """
    columns = EVENT_COLS if source == "game_events" else PAYMENT_COLS
    records = []
    for entry in manifest["files"]:
        if entry["source"] != source:
            continue
        path = Path(data_dir) / entry["key"]
        text = path.read_text(encoding="utf-8")  # undecodable file fails the batch
        arrival = re.search(r"arrival_date=(\d{4}-\d{2}-\d{2})(?:/|$)", entry["key"])
        arrival_date = arrival.group(1) if arrival else manifest["batch_date"]
        if source == "game_events":
            if path.suffix != ".jsonl" or text.lstrip().startswith("["):
                raise ValueError(f"INVALID_FILE_STRUCTURE: {path}")
            inputs = enumerate(text.splitlines(), 1)
            for line_no, line in inputs:
                corrupt = None
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict) or any(
                        isinstance(obj.get(c), (list, dict, bool)) for c in columns
                    ):
                        raise ValueError("Expected scalar fields in JSON object")
                except (ValueError, TypeError):
                    obj, corrupt = {}, line
                values = [None if obj.get(c) is None else str(obj[c]) for c in columns]
                records.append(values + [corrupt, str(path.resolve()), str(line_no), line, arrival_date])
        else:
            if path.suffix != ".csv":
                raise ValueError(f"INVALID_FILE_STRUCTURE: {path}")
            reader = csv.reader(io.StringIO(text, newline=""), strict=True)
            if next(reader, None) != columns:
                raise ValueError(f"INVALID_CSV_HEADER: {path}")
            previous = reader.line_num
            lines = text.splitlines(keepends=True)
            for values in reader:
                raw = "".join(lines[previous:reader.line_num])
                start_line = previous + 1
                previous = reader.line_num
                corrupt = raw if len(values) != len(columns) else None
                values = (values + [None] * len(columns))[:len(columns)]
                records.append(values + [corrupt, str(path.resolve()), str(start_line), raw, arrival_date])
    schema = string_schema(columns, corrupt=True)
    for name in ["source_file", "source_line", "raw_payload", "arrival_date"]:
        schema.add(name, StringType(), True)
    return spark.createDataFrame(records, schema)


def run_batch(spark, manifest_path=MANIFEST_PATH, run_id=None, data_dir=DATA_DIR):
    data_dir = Path(data_dir)
    # Re-use the manifest verification completed in Lesson 5A.
    # Checksum catches changed bytes before parsing; it does not replace validation.
    manifest = verify_manifest(Path(manifest_path), data_dir)
    run_id = run_id or ("run_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                        + "_" + uuid.uuid4().hex[:8])
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run_id")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    run_dir = data_dir / "runs" / run_id
    destinations = {source: data_dir / "staging" / source / f"run_id={run_id}"
                    for source in ("game_events", "payments")}
    if any(p.exists() for p in destinations.values()) or (data_dir / "quarantine" / f"run_id={run_id}").exists():
        raise FileExistsError(f"Run already exists: {run_id}")
    # Atomic reservation prevents two local executions from sharing a run_id.
    run_dir.mkdir(parents=True, exist_ok=False)
    pending = run_dir / "pending"
    report = {"run_id": run_id, "manifest_id": manifest["manifest_id"],
              "batch_date": manifest["batch_date"], "status": "RUNNING", "sources": {}}
    cached, published = [], []
    def save_report():
        temporary = run_dir / "status.tmp"
        temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
        temporary.replace(run_dir / "status.json")
    try:
        save_report()
        results = {}
        any_conflict = False
        for source in destinations:
            raw = read_source(spark, manifest, source, data_dir).cache()
            cached.append(raw)
            valid, rejected = validate_rows(raw, source)
            valid = valid.cache()
            cached.append(valid)
            latest, conflicts = resolve_versions(valid, source)
            conflicts = conflicts.cache()
            cached.append(conflicts)
            raw_count, rejected_count = raw.count(), rejected.count()
            valid_count, conflict_count = valid.count(), conflicts.count()
            # Conflict rows form a separate, exclusive bucket in reconciliation.
            key = "event_id" if source == "game_events" else "payment_event_id"
            clean = valid.join(conflicts.select(key, "source_version").distinct(),
                               [key, "source_version"], "left_anti")
            clean_latest, _ = resolve_versions(clean, source)
            selected_count = clean_latest.count()
            removed_count = valid_count - conflict_count - selected_count
            stats = dict(raw=raw_count, rejected=rejected_count,
                         duplicate_or_older=removed_count, staging=0,
                         selected=selected_count, conflict=conflict_count)
            if raw_count != rejected_count + removed_count + selected_count + conflict_count:
                raise RuntimeError("ROW_RECONCILIATION_FAILED")
            report["sources"][source] = stats
            results[source] = latest
            if rejected_count:
                destination = data_dir / "quarantine" / f"run_id={run_id}" / source
                # coalesce(1): Gom về một partition đầu ra, tiện kiểm tra fixture nhỏ
                # errorifexists: Báo lỗi nếu đường dẫn đầu ra đã tồn tại
                rejected.coalesce(1).write.mode("errorifexists").parquet(str(destination))
            if conflict_count:
                any_conflict = True
                destination = data_dir / "quarantine" / f"run_id={run_id}" / (source + "_conflict")
                conflicts.withColumn("error_reason", F.lit("VERSION_CONFLICT")).coalesce(1).write.mode(
                    "errorifexists").parquet(str(destination))
        if any_conflict:
            raise RuntimeError("VERSION_CONFLICT: batch stopped before staging/MERGE")
        # Each execution owns a run_id. Replay compares business columns across runs;
        # warehouse idempotency requires a later Iceberg MERGE and is not proven here.
        for source, latest in results.items():
            # Empty Parquet still carries an explicit schema and a success marker.
            latest.coalesce(1).write.mode("errorifexists").parquet(str(pending / source))
        for source, destination in destinations.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            (pending / source).rename(destination)
            published.append(destination)
        for stats in report["sources"].values():
            stats["staging"] = stats["selected"]
        report["status"] = "SUCCEEDED"
        save_report()
        print(json.dumps(report, indent=2))
        return report
    except Exception as exc:
        # Roll back only paths published by this execution, preserving existing runs.
        for destination in published:
            shutil.rmtree(destination)
        report["status"] = "FAILED"
        report["error"] = str(exc)
        for stats in report["sources"].values():
            stats["staging"] = 0
        save_report()
        raise
    finally:
        for frame in cached:
            frame.unpersist()
        if pending.exists():
            shutil.rmtree(pending)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--run-id")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("GameAnalyticsLocalBatch").master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.ansi.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run_batch(spark, args.manifest, args.run_id, args.data_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
