import argparse
import json
import os
from pathlib import Path

import requests


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True, help="Path to .jsonl file")
    p.add_argument("--endpoint", default="http://localhost:8000/ingest")
    p.add_argument("--batch-size", type=int, default=200)
    args = p.parse_args()

    token = os.environ.get("INGEST_API_TOKEN", "")
    if not token:
        raise SystemExit("INGEST_API_TOKEN is not set in environment")

    path = Path(args.file)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    batch = []
    sent = 0

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch.append(json.loads(line))
            if len(batch) >= args.batch_size:
                r = requests.post(
                    args.endpoint,
                    headers={"X-API-Token": token, "Content-Type": "application/json"},
                    data=json.dumps(batch),
                    timeout=60,
                )
                r.raise_for_status()
                sent += len(batch)
                print(f"[replay] sent={sent}")
                batch = []

    if batch:
        r = requests.post(
            args.endpoint,
            headers={"X-API-Token": token, "Content-Type": "application/json"},
            data=json.dumps(batch),
            timeout=60,
        )
        r.raise_for_status()
        sent += len(batch)
        print(f"[replay] sent={sent}")

    print("[replay] done")


if __name__ == "__main__":
    main()