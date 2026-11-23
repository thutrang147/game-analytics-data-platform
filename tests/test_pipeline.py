import json
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from create_manifest import create_manifest
from generate_data import generate_data, events, payments
from run_local_batch import EVENT_COLS, PAYMENT_COLS, run_batch
from transforms import resolve_versions, string_schema, validate_rows
from verify_manifest import verify_manifest


DAY = "2026-11-01"


def batch(root, day=DAY):
    return create_manifest(day, data_dir=root)


def raw_frame(spark, source, rows):
    columns = EVENT_COLS if source == "game_events" else PAYMENT_COLS
    values = [[None if row.get(c) is None else str(row[c]) for c in columns]
              + [row.get("_corrupt_record")] for row in rows]
    return spark.createDataFrame(values, string_schema(columns, corrupt=True))


def business(frame):
    excluded = {"source_file", "source_line", "raw_payload", "arrival_date"}
    cols = sorted(set(frame.columns) - excluded)
    return sorted(frame.select(*cols).toJSON().collect())


def test_fixture_immutable_and_manifest_checksums(tmp_path):
    generate_data(tmp_path)
    path = batch(tmp_path)
    before = path.read_bytes()
    assert len(verify_manifest(path, tmp_path)["files"]) == 2
    generate_data(tmp_path)
    assert batch(tmp_path).read_bytes() == before
    with pytest.raises(FileExistsError):
        generate_data(tmp_path, "quality")
    extra = tmp_path / "raw/game_events/extra.jsonl"
    extra.write_text("not included")
    assert batch(tmp_path).read_bytes() == before
    raw = tmp_path / "raw/game_events/arrival_date=2026-11-01/events_001.jsonl"
    raw.write_text(raw.read_text() + "\n")
    with pytest.raises(ValueError, match="CHECKSUM_MISMATCH"):
        verify_manifest(path, tmp_path)
    with pytest.raises(RuntimeError, match="different content"):
        batch(tmp_path)
    raw.unlink()
    with pytest.raises(FileNotFoundError):
        verify_manifest(path, tmp_path)


@pytest.mark.parametrize("mutation", ["duplicate", "source", "path", "checksum", "empty"])
def test_invalid_manifest(tmp_path, mutation):
    generate_data(tmp_path)
    path = batch(tmp_path)
    obj = json.loads(path.read_text())
    if mutation == "duplicate":
        obj["files"].append(obj["files"][0])
    elif mutation == "source":
        obj["files"][0]["source"] = "unknown"
    elif mutation == "path":
        obj["files"][0]["key"] = "../outside.jsonl"
    elif mutation == "checksum":
        obj["files"][0]["checksum_sha256"] = "bad"
    else:
        obj["files"] = []
    path.write_text(json.dumps(obj))
    with pytest.raises(ValueError):
        verify_manifest(path, tmp_path)


@pytest.mark.parametrize("source,patch,reason", [
    ("game_events", {"player_id": " "}, "MISSING_REQUIRED_FIELD"),
    ("game_events", {"event_at": "garbage"}, "INVALID_TIMESTAMP"),
    ("game_events", {"event_at": "2026-11-01T09:00:00"}, "INVALID_TIMESTAMP"),
    ("game_events", {"source_version": "1.5"}, "INVALID_VERSION"),
    ("game_events", {"source_version": "9999999999999999999999999"}, "INVALID_VERSION"),
    ("game_events", {"event_type": "unknown"}, "INVALID_EVENT_TYPE"),
    ("game_events", {"event_type": "level_start"}, "MISSING_LEVEL_ID"),
    ("game_events", {"_corrupt_record": "{"}, "MALFORMED_JSON"),
    ("payments", {"amount_cents": "1.5"}, "INVALID_AMOUNT"),
    ("payments", {"amount_cents": "0"}, "INVALID_AMOUNT"),
    ("payments", {"currency": "EUR"}, "INVALID_CURRENCY"),
    ("payments", {"event_type": "unknown"}, "INVALID_PAYMENT_TYPE"),
    ("payments", {"source_version": "0"}, "INVALID_VERSION"),
    ("payments", {"purchase_id": None}, "MISSING_REQUIRED_FIELD"),
])
def test_validation(spark, source, patch, reason):
    base = events[0] if source == "game_events" else payments[0]
    valid, rejected = validate_rows(raw_frame(spark, source, [dict(base, **patch)]), source)
    assert valid.count() == 0
    assert rejected.select("error_reason").first()[0] == reason


@pytest.mark.parametrize("source", ["game_events", "payments"])
def test_versions_and_normalized_hash(spark, source):
    base = events[0] if source == "game_events" else payments[0]
    # Equivalent UTC instants and trimmed fields must hash identically.
    retry = dict(base, player_id=" P01 ", event_at="2026-11-01T16:00:00+07:00" if source == "game_events"
                 else "2026-11-01T16:15:00+07:00")
    rows = [base, retry, dict(base, source_version=2, player_id="CORRECTED"), base]
    valid, rejected = validate_rows(raw_frame(spark, source, rows), source)
    assert rejected.count() == 0
    latest, conflicts = resolve_versions(valid, source)
    assert conflicts.count() == 0
    selected = latest.collect()
    assert len(selected) == 1
    assert selected[0].source_version == 2
    assert selected[0].player_id == "CORRECTED"
    normalized, _ = resolve_versions(valid.filter(F.col("source_version") == 1), source)
    assert normalized.count() == 1
    assert len(normalized.first().payload_hash) == 64


@pytest.mark.parametrize("source", ["game_events", "payments"])
def test_conflict_even_on_older_version(spark, source):
    base = events[0] if source == "game_events" else payments[0]
    valid, _ = validate_rows(raw_frame(spark, source, [base, dict(base, player_id="OTHER"),
                                                     dict(base, source_version=3)]), source)
    _, conflicts = resolve_versions(valid, source)
    assert conflicts.count() == 2


def test_replay_late_refund_and_business_metrics(spark, tmp_path):
    generate_data(tmp_path)
    paths = {day: batch(tmp_path, day) for day in [DAY, "2026-11-02", "2026-11-03"]}
    reports = []
    for run, day in [("first", DAY), ("replay", DAY), ("day2", "2026-11-02"), ("day3", "2026-11-03")]:
        reports.append(run_batch(spark, paths[day], run, tmp_path))
    assert all(r["status"] == "SUCCEEDED" for r in reports)
    for source in ["game_events", "payments"]:
        def read(run):
            return spark.read.parquet(str(tmp_path / f"staging/{source}/run_id={run}"))
        assert business(read("first")) == business(read("replay"))
    with pytest.raises(FileExistsError):
        run_batch(spark, paths[DAY], "first", tmp_path)
    frames = {}
    for source in ["game_events", "payments"]:
        frames[source] = [row.asDict() for run in ["first", "day2", "day3"]
                         for row in spark.read.parquet(str(tmp_path / f"staging/{source}/run_id={run}")).collect()]
    games = frames["game_events"]
    cohort = {r["player_id"] for r in games if r["event_date"] == date(2026, 11, 1)}
    returning = {r["player_id"] for r in games if r["event_date"] == date(2026, 11, 2)}
    assert cohort == {"P01", "P02"}
    assert len(cohort & returning) / len(cohort) == 0.5
    starts = {(r["player_id"], r["session_id"], r["level_id"]) for r in games if r["event_type"] == "level_start"}
    finishes = {(r["player_id"], r["session_id"], r["level_id"]) for r in games if r["event_type"] == "level_complete"}
    assert starts == {("P01", "S003", "L01")}
    assert len(finishes & starts) / len(starts) == 1
    revenue = {}
    for row in frames["payments"]:
        revenue[row["event_date"]] = revenue.get(row["event_date"], 0) + row["amount_cents"] * (
            -1 if row["event_type"] == "refund" else 1)
    assert revenue == {date(2026, 11, 1): 1000, date(2026, 11, 2): -300}
    refund = next(r for r in frames["payments"] if r["event_type"] == "refund")
    assert refund["arrival_date"] == "2026-11-03"
    assert reports[2]["sources"]["payments"]["staging"] == 0
    assert reports[3]["sources"]["game_events"]["staging"] == 0


@pytest.mark.parametrize("scenario", ["quality", "conflict"])
def test_quality_counts_and_conflict_stops_staging(spark, tmp_path, scenario):
    generate_data(tmp_path, scenario)
    manifest = batch(tmp_path)
    if scenario == "conflict":
        with pytest.raises(RuntimeError, match="VERSION_CONFLICT"):
            run_batch(spark, manifest, scenario, tmp_path)
        assert not (tmp_path / "staging").exists()
        report = json.loads((tmp_path / f"runs/{scenario}/status.json").read_text())
        assert report["status"] == "FAILED"
    else:
        report = run_batch(spark, manifest, scenario, tmp_path)
    for source, stats in report["sources"].items():
        assert stats["raw"] == stats["rejected"] + stats["duplicate_or_older"] + stats["selected"] + stats["conflict"]
        assert stats["rejected"] == 1
        if scenario == "quality":
            assert stats["duplicate_or_older"] == 3
        else:
            assert stats["conflict"] >= 2
            assert stats["staging"] == 0
        rejected = spark.read.parquet(str(tmp_path / f"quarantine/run_id={scenario}/{source}"))
        row = rejected.first()
        assert row.source_file and row.source_line and row.raw_payload
        assert row.error_reason == "INVALID_VERSION"


@pytest.mark.parametrize("source,content", [
    ("payments", "wrong,header\n"), ("game_events", "[{\"event_id\": \"E\"}]"),
    ("payments", 'payment_event_id,purchase_id,player_id,event_type,event_at,amount_cents,currency,source_version\n"unterminated'),
])
def test_file_structure_failure(spark, tmp_path, source, content):
    generate_data(tmp_path)
    key = (f"raw/{source}/arrival_date={DAY}/" + ("payments_001.csv" if source == "payments" else "events_001.jsonl"))
    (tmp_path / key).write_text(content)
    path = batch(tmp_path)
    with pytest.raises((ValueError, __import__("csv").Error)):
        run_batch(spark, path, "badfile", tmp_path)
    assert not (tmp_path / "staging").exists()
    assert json.loads((tmp_path / "runs/badfile/status.json").read_text())["status"] == "FAILED"


def test_malformed_rows_empty_and_only_manifest_inputs(spark, tmp_path):
    generate_data(tmp_path)
    game = tmp_path / f"raw/game_events/arrival_date={DAY}/events_001.jsonl"
    game.write_text("{bad json\n")
    pay = tmp_path / f"raw/payments/arrival_date={DAY}/payments_001.csv"
    pay.write_text(pay.read_text().splitlines()[0] + "\nshort,row\n")
    path = batch(tmp_path)
    # An unlisted file must never enter the batch.
    game.with_name("unlisted.jsonl").write_text(json.dumps(events[0]))
    report = run_batch(spark, path, "invalid", tmp_path)
    for source, stats in report["sources"].items():
        assert stats["raw"] == stats["rejected"] == 1
        assert stats["staging"] == 0
        frame = spark.read.parquet(str(tmp_path / f"staging/{source}/run_id=invalid"))
        assert frame.count() == 0
        assert "payload_hash" in frame.columns


def test_write_failure_never_succeeds(spark, tmp_path, monkeypatch):
    from pyspark.sql.readwriter import DataFrameWriter
    generate_data(tmp_path)
    path = batch(tmp_path)
    original = DataFrameWriter.parquet
    def fail_second(self, path, *args, **kwargs):
        if str(path).endswith("pending/payments"):
            raise OSError("simulated disk failure")
        return original(self, path, *args, **kwargs)
    monkeypatch.setattr(DataFrameWriter, "parquet", fail_second)
    with pytest.raises(OSError, match="disk failure"):
        run_batch(spark, path, "diskfail", tmp_path)
    report = json.loads((tmp_path / "runs/diskfail/status.json").read_text())
    assert report["status"] == "FAILED"
    assert not (tmp_path / "staging").exists()
    assert not (tmp_path / "runs/diskfail/pending").exists()


def test_multiple_files_and_absent_source(spark, tmp_path):
    generate_data(tmp_path)
    key1 = f"raw/game_events/arrival_date={DAY}/events_001.jsonl"
    key2 = f"raw/game_events/arrival_date={DAY}/events_002.jsonl"
    (tmp_path / key2).write_text(json.dumps(dict(events[0], source_version=2)) + "\n")
    path = create_manifest(DAY, "multiple", tmp_path, [("game_events", key1), ("game_events", key2)])
    report = run_batch(spark, path, "multiple", tmp_path)
    assert report["sources"]["game_events"]["raw"] == 3
    assert report["sources"]["game_events"]["duplicate_or_older"] == 1
    assert report["sources"]["payments"]["raw"] == 0
    frame = spark.read.parquet(str(tmp_path / "staging/game_events/run_id=multiple"))
    assert dict(frame.dtypes)["source_version"] == "bigint"
    assert dict(frame.dtypes)["event_at"] == "timestamp"
    assert dict(frame.dtypes)["event_date"] == "date"
    empty = spark.read.parquet(str(tmp_path / "staging/payments/run_id=multiple"))
    assert dict(empty.dtypes)["amount_cents"] == "bigint"
    assert empty.count() == 0


@pytest.mark.parametrize("source,key", [("unknown", "raw/file"), ("game_events", "../outside.jsonl")])
def test_create_manifest_rejects_invalid_entries(tmp_path, source, key):
    with pytest.raises(ValueError):
        create_manifest(data_dir=tmp_path, sources=[(source, key)])


def test_publication_failure_preserves_existing_results(spark, tmp_path, monkeypatch):
    generate_data(tmp_path)
    path = batch(tmp_path)
    existing = tmp_path / "staging/game_events/run_id=previous"
    existing.mkdir(parents=True)
    sentinel = existing / "keep.txt"
    sentinel.write_text("existing user result")
    original = Path.rename
    def fail_second(self, target):
        if str(self).endswith("pending/payments"):
            raise OSError("publication failed")
        return original(self, target)
    monkeypatch.setattr(Path, "rename", fail_second)
    with pytest.raises(OSError, match="publication failed"):
        run_batch(spark, path, "publishfail", tmp_path)
    assert sentinel.read_text() == "existing user result"
    assert not (tmp_path / "staging/game_events/run_id=publishfail").exists()
    assert not (tmp_path / "staging/payments/run_id=publishfail").exists()
    report = json.loads((tmp_path / "runs/publishfail/status.json").read_text())
    assert report["status"] == "FAILED"
    assert all(s["staging"] == 0 for s in report["sources"].values())
