import json
from pathlib import Path

from create_manifest import sha256_file

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

MANIFEST_PATH = (
    DATA_DIR
    / "manifests/batch_date=2026-11-01/manifest_001.json"
)


def verify_manifest(manifest_path: Path, data_dir=DATA_DIR):
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    from datetime import date
    import re
    if not isinstance(manifest, dict):
        raise ValueError("Manifest must be an object")
    date.fromisoformat(manifest["batch_date"])
    if not re.fullmatch(r"[A-Za-z0-9_-]+", manifest["manifest_id"]):
        raise ValueError("Invalid manifest_id")
    files = manifest["files"]
    if not isinstance(files, list) or not files:
        raise ValueError("Manifest requires a nonempty file list")
    seen = set()
    for item in files:
        if item["source"] not in {"game_events", "payments"}:
            raise ValueError("Unexpected source")
        key = item["key"]
        if key in seen:
            raise ValueError("Duplicate manifest key")
        seen.add(key)
        if Path(key).is_absolute():
            raise ValueError("Absolute keys are forbidden")
        if not re.fullmatch(r"[0-9a-f]{64}", item["checksum_sha256"]):
            raise ValueError("Invalid checksum")
        file_path = (Path(data_dir) / key).resolve()

        # Chỉ cho phép đọc file trong thư mục data.
        if not file_path.is_relative_to(Path(data_dir).resolve()):
            raise ValueError(f"Invalid path: {key}")

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        actual_hash = sha256_file(file_path)
        expected_hash = item["checksum_sha256"]

        if actual_hash != expected_hash:
            raise ValueError(f"CHECKSUM_MISMATCH: {key}")

    return manifest


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    verify_manifest(args.manifest, args.data_dir)
    print("All manifest files verified!")
