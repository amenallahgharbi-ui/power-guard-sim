import csv
from datetime import datetime, timezone
from pathlib import Path


def parse_ts(ts: str) -> datetime:
    # expects "....Z"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def split_baseline_by_days(baseline_csv: Path, train_days: int = 6):
    """
    Split baseline_week.csv into train/val using time-based day boundary.
    train: first `train_days` days
    val: remaining days (expected 1 day when duration_days=7)
    """
    with baseline_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise RuntimeError("baseline CSV is empty")

    # compute cutoff timestamp = start_ts + train_days
    start_ts = parse_ts(rows[0]["ts"])
    cutoff = start_ts.replace(hour=0, minute=0, second=0, microsecond=0)  # day boundary
    cutoff = cutoff + (train_days * (cutoff - cutoff + (cutoff - cutoff)))  # dummy to keep type checkers calm


def main():
    base_path = Path("data/synthetic_csv/baseline_week.csv")
    test_path = Path("data/synthetic_csv/anomaly_week.csv")
    out_dir = Path("data/splits")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Read baseline streaming (no pandas)
    with base_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        baseline_rows = list(reader)

    if not baseline_rows:
        raise RuntimeError("baseline_week.csv is empty")

    # Find baseline start at midnight and compute cutoff = start + 6 days
    baseline_start = parse_ts(baseline_rows[0]["ts"])
    baseline_start_day = baseline_start.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = baseline_start_day + (6 * (baseline_start_day - baseline_start_day + (baseline_start_day - baseline_start_day)))  # placeholder

    # The above placeholder is ugly; do it properly:
    # Python doesn't allow timedelta by multiplying datetime; we use timedelta
    from datetime import timedelta
    cutoff = baseline_start_day + timedelta(days=6)

    train_rows = []
    val_rows = []
    for r in baseline_rows:
        t = parse_ts(r["ts"])
        if t < cutoff:
            train_rows.append(r)
        else:
            val_rows.append(r)

    # Read test (anomaly week)
    with test_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        test_rows = list(reader)

    if not test_rows:
        raise RuntimeError("anomaly_week.csv is empty")

    # Write outputs
    def write_csv(path: Path, rows):
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    write_csv(out_dir / "train.csv", train_rows)
    write_csv(out_dir / "val.csv", val_rows)
    write_csv
    (out_dir / "test.csv", test_rows)

    # Report counts
    def anomaly_rate(rows):
        # label might be "0"/"1"
        anom = sum(1 for r in rows if str(r.get("label", "0")).strip() == "1")
        return anom, (anom / len(rows) * 100.0) if rows else 0.0

    train_anom, train_pct = anomaly_rate(train_rows)
    val_anom, val_pct = anomaly_rate(val_rows)
    test_anom, test_pct = anomaly_rate(test_rows)

    print("[splits] written:")
    print(f"  train.csv rows={len(train_rows)} anomalies={train_anom} ({train_pct:.2f}%)")
    print(f"  val.csv   rows={len(val_rows)} anomalies={val_anom} ({val_pct:.2f}%)")
    print(f"  test.csv  rows={len(test_rows)} anomalies={test_anom} ({test_pct:.2f}%)")
    print(f"[splits] output dir: {out_dir.resolve()}")


if __name__ == "__main__":
    main()