import hashlib
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

BATCH_DATE = "2026-11-01"
MANIFEST_ID = "manifest_001"


def sha256_file(path: Path) -> str:
    """Tính SHA-256 từ nội dung file."""
    hasher = hashlib.sha256()   # kiểm tra tính toàn vẹn file

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):    # Đọc file theo chunk 1 MiB, tránh tải toàn bộ file vào RAM
            hasher.update(chunk)

    return hasher.hexdigest()


def create_manifest(batch_date=BATCH_DATE, manifest_id=MANIFEST_ID, data_dir=DATA_DIR, sources=None):
    from datetime import date
    import re
    date.fromisoformat(batch_date)
    if not re.fullmatch(r"[A-Za-z0-9_-]+", manifest_id):
        raise ValueError("Invalid manifest_id")
    # Freeze an explicit list. Existing manifests are never expanded by discovery.
    sources = sources if sources is not None else [
        ("game_events", f"raw/game_events/arrival_date={batch_date}/events_001.jsonl"),
        ("payments", f"raw/payments/arrival_date={batch_date}/payments_001.csv"),
    ]
    files = []
    seen = set()
    if not sources:
        raise ValueError("Manifest requires a nonempty file list")

    for source, key in sources:
        if source not in {"game_events", "payments"}:
            raise ValueError("Unexpected source")
        file_path = (Path(data_dir) / key).resolve()
        if Path(key).is_absolute() or not file_path.is_relative_to(Path(data_dir).resolve()):
            raise ValueError(f"Invalid path: {key}")
        if key in seen:
            raise ValueError("Duplicate manifest key")
        seen.add(key)

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        files.append({
            "source": source,
            "key": key,
            "checksum_sha256": sha256_file(file_path),
        })

    manifest = {
        "batch_date": batch_date,   # Ngày batch được tổ chức để xử lý
        "manifest_id": manifest_id, # Định danh danh sách file đầu vào của batch
        "files": files,
    }

    output = (
        Path(data_dir)
        / "manifests"
        / f"batch_date={batch_date}"
        / f"{manifest_id}.json"
    )

    output.parent.mkdir(parents=True, exist_ok=True)

    # Manifest đã chốt không được âm thầm sửa.
    if output.exists():
        existing = json.loads(output.read_text(encoding="utf-8"))

        if existing != manifest:
            raise RuntimeError(
                "Manifest already exists with different content"
            )

        print("Manifest already exists and matches.")
    else:
        with output.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(manifest, indent=2))
        print("Created manifest:", output)

    return output


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Freeze checksums for an explicit batch file list")
    parser.add_argument("--batch-date", default=BATCH_DATE)
    parser.add_argument("--manifest-id", default=MANIFEST_ID)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--file", action="append", help="SOURCE=KEY; repeat for each input file")
    args = parser.parse_args()
    sources = [item.split("=", 1) for item in args.file] if args.file else None
    print(create_manifest(args.batch_date, args.manifest_id, args.data_dir, sources))


if __name__ == "__main__":
    main()
