import json
from pathlib import Path

from create_manifest import sha256_file

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

MANIFEST_PATH = (
    DATA_DIR
    / "manifests/batch_date=2026-11-01/manifest_001.json"
)


def verify_manifest(manifest_path: Path):
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    print("Manifest:", manifest["manifest_id"])

    files = manifest["files"]

    if len(files) != 2:
        raise ValueError("Expected exactly 2 files")

    sources = {item["source"] for item in files}

    if sources != {"game_events", "payments"}:
        raise ValueError("Unexpected sources")

    for item in files:
        key = item["key"]
        file_path = (DATA_DIR / key).resolve()

        # Chỉ cho phép đọc file trong thư mục data.
        if not file_path.is_relative_to(DATA_DIR.resolve()):
            raise ValueError(f"Invalid path: {key}")

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        actual_hash = sha256_file(file_path)
        expected_hash = item["checksum_sha256"]

        if actual_hash != expected_hash:
            raise ValueError(f"CHECKSUM_MISMATCH: {key}")

        print(f"PASS: {item['source']}")

    print("All manifest files verified!")


if __name__ == "__main__":
    verify_manifest(MANIFEST_PATH)
