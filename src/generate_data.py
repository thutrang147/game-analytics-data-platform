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



def fixture_days():
    """Deterministic three-day fixture; event time and delivery date are distinct."""
    day2 = [dict(events[0], event_id="E003", session_id="S003", event_at="2026-11-02T09:00:00.000Z")]
    for key, kind, minute in [("E004", "level_start", "01"), ("E005", "level_complete", "05")]:
        day2.append(dict(day2[0], event_id=key, event_type=kind, level_id="L01",
                         event_at=f"2026-11-02T09:{minute}:00.000Z"))
    # Refund arrives on Nov 3, but belongs to the Nov 2 business date.
    refund = dict(payments[0], payment_event_id="PAY002", event_type="refund",
                  event_at="2026-11-02T16:00:00.000Z", amount_cents=300)
    return {"2026-11-01": (events, payments), "2026-11-02": (day2, []),
            "2026-11-03": ([], [refund])}


def generate_data(data_dir=BASE_DIR / "data", scenario="base"):
    import io
    from copy import deepcopy
    days = deepcopy(fixture_days())
    if scenario in {"quality", "conflict"}:
        for rows, key in [(days["2026-11-01"][0], "event_id"),
                          (days["2026-11-01"][1], "payment_event_id")]:
            original = dict(rows[0])
            rows.extend([dict(original), dict(original, source_version=2), dict(original)])
            rows.append(dict(original, **{key: "INVALID"}, source_version=0))
            if scenario == "conflict":
                rows.append(dict(original, player_id="CONFLICT"))
    elif scenario != "base":
        raise ValueError("Unknown fixture scenario")

    outputs = {}
    for day, (event_rows, payment_rows) in days.items():
        outputs[Path(data_dir) / f"raw/game_events/arrival_date={day}/events_001.jsonl"] = "".join(
            json.dumps(row) + "\n" for row in event_rows)
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=list(payments[0]))
        writer.writeheader()
        writer.writerows(payment_rows)
        outputs[Path(data_dir) / f"raw/payments/arrival_date={day}/payments_001.csv"] = buffer.getvalue()
    # Check every destination before writing: never overwrite frozen raw inputs.
    for path, content in outputs.items():
        if path.exists() and path.read_bytes() != content.encode("utf-8"):
            raise FileExistsError(f"Raw differs; choose a separate --data-dir: {path}")
    for path, content in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            with path.open("x", encoding="utf-8", newline="") as handle:
                handle.write(content)
    return list(outputs)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=BASE_DIR / "data")
    parser.add_argument("--scenario", choices=["base", "quality", "conflict"], default="base")
    args = parser.parse_args()
    for path in generate_data(args.data_dir, args.scenario):
        print(path)


if __name__ == "__main__":
    main()
