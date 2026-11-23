import csv
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

# 1. Game events: hai người chơi bắt đầu phiên
events = [
    {
        "event_id": "E001",
        "player_id": "P01",
        "session_id": "S001",
        "event_type": "session_start",
        "event_at": "2026-11-01T09:00:00.000Z",
        "source_version": 1,
        "level_id": None,
    },
    {
        "event_id": "E002",
        "player_id": "P02",
        "session_id": "S002",
        "event_type": "session_start",
        "event_at": "2026-11-01T10:00:00.000Z",
        "source_version": 1,
        "level_id": None,
    },
]

events_path = (
    RAW_DIR / "game_events"
    / "arrival_date=2026-11-01"
    / "events_001.jsonl"
)

events_path.parent.mkdir(parents=True, exist_ok=True)

with events_path.open("w", encoding="utf-8") as f:
    for event in events:
        f.write(json.dumps(event) + "\n")

# 2. Payments: P01 mua vật phẩm trị giá 1000 cents
payments = [
    {
        "payment_event_id": "PAY001",
        "purchase_id": "PUR001",
        "player_id": "P01",
        "event_type": "purchase",
        "event_at": "2026-11-01T09:15:00.000Z",
        "amount_cents": 1000,
        "currency": "USD",
        "source_version": 1,
    }
]

payments_path = (
    RAW_DIR / "payments"
    / "arrival_date=2026-11-01"
    / "payments_001.csv"
)

payments_path.parent.mkdir(parents=True, exist_ok=True)

with payments_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=payments[0].keys())
    writer.writeheader()
    writer.writerows(payments)

print("Generated game events:", len(events))
print("Generated payments:", len(payments))
