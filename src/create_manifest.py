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


def main():
    sources = [
        (
            "game_events",
            "raw/game_events/arrival_date=2026-11-01/events_001.jsonl"
        ),
        (
            "payments",
            "raw/payments/arrival_date=2026-11-01/payments_001.csv"
        ),
    ]

    files = []

    for source, key in sources:
        file_path = DATA_DIR / key

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        files.append({
            "source": source,
            "key": key,
            "checksum_sha256": sha256_file(file_path),
        })

    manifest = {
        "batch_date": BATCH_DATE,   # Ngày batch được tổ chức để xử lý
        "manifest_id": MANIFEST_ID, # Định danh danh sách file đầu vào của batch
        "files": files,
    }

    output = (
        DATA_DIR
        / "manifests"
        / f"batch_date={BATCH_DATE}"
        / f"{MANIFEST_ID}.json"
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
        output.write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8"
        )
        print("Created manifest:", output)

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
