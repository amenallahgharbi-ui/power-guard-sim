import csv
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def daily_sine(i: int, points_per_day: int, amplitude: float) -> float:
    return amplitude * math.sin(2.0 * math.pi * (i / points_per_day))


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def main():
    cfg_path = Path("generators/configs/baseline_week.yml")
    schema_path = Path("generators/schemas/telemetry_schema.json")

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    seed = int(cfg["meta"]["seed"])
    random.seed(seed)

    sampling_seconds = int(cfg["meta"]["sampling_seconds"])
    duration_days = int(cfg["meta"]["duration_days"])
    start_ts = parse_ts(cfg["meta"]["start_ts_utc"])

    total_points = int((duration_days * 24 * 3600) / sampling_seconds)
    points_per_day = int((24 * 3600) / sampling_seconds)

    prof = cfg["profile_nominal"]
    noise = prof["noise_sigma"]
    rw_sigma = prof["random_walk_sigma"]

    out_dir = Path(cfg["output"]["dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / cfg["output"]["jsonl_file"]
    csv_path = out_dir / cfg["output"]["csv_file"]

    bounds = {
        "voltage_v": (0.0, 400.0),
        "current_a": (0.0, 2000.0),
        "temperature_c": (-40.0, 85.0),
        "soc_pct": (0.0, 100.0),
    }

    csv_fields = [
        "ts",
        "asset_id",
        "voltage_v",
        "current_a",
        "temperature_c",
        "soc_pct",
        "profile",
        "label",
        "event_type",
        "event_id",
    ]

    errors_count = 0
    written = 0

    with jsonl_path.open("w", encoding="utf-8") as jf, csv_path.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.DictWriter(cf, fieldnames=csv_fields)
        writer.writeheader()

        for asset in cfg["assets"]:
            asset_id = asset["asset_id"]
            nominal = asset["nominal"]

            soc = float(nominal["soc_start_pct"])

            v_rw = 0.0
            i_rw = 0.0
            t_rw = 0.0

            for i in range(total_points):
                ts = start_ts + timedelta(seconds=i * sampling_seconds)

                v_base = float(nominal["voltage_base_v"])
                i_base = float(nominal["current_base_a"])
                t_base = float(nominal["temperature_base_c"])

                voltage = (
                    v_base
                    + daily_sine(i, points_per_day, float(prof["voltage_daily_amp_v"]))
                    + v_rw
                    + random.gauss(0.0, float(noise["voltage_v"]))
                )
                current = (
                    i_base
                    + daily_sine(i, points_per_day, float(prof["current_daily_amp_a"]))
                    + i_rw
                    + random.gauss(0.0, float(noise["current_a"]))
                )
                temp = (
                    t_base
                    + daily_sine(i, points_per_day, float(prof["temperature_daily_amp_c"]))
                    + t_rw
                    + random.gauss(0.0, float(noise["temperature_c"]))
                )

                v_rw += random.gauss(0.0, float(rw_sigma["voltage_v"]))
                i_rw += random.gauss(0.0, float(rw_sigma["current_a"]))
                t_rw += random.gauss(0.0, float(rw_sigma["temperature_c"]))

                # baseline: slow discharge
                soc -= (max(current, 0.0) * sampling_seconds) / (24 * 3600) * 1.5
                soc += random.gauss(0.0, float(noise["soc_pct"]))
                soc = clamp(soc, *bounds["soc_pct"])

                msg = {
                    "ts": iso_z(ts),
                    "asset_id": asset_id,
                    "voltage_v": round(clamp(voltage, *bounds["voltage_v"]), 3),
                    "current_a": round(clamp(current, *bounds["current_a"]), 3),
                    "temperature_c": round(clamp(temp, *bounds["temperature_c"]), 3),
                    "soc_pct": round(soc, 3),
                    "profile": "nominal",
                }

                errs = list(validator.iter_errors(msg))
                if errs:
                    errors_count += 1
                    continue

                jf.write(json.dumps(msg, ensure_ascii=False) + "\n")

                row = dict(msg)
                row.update({"label": 0, "event_type": "none", "event_id": ""})
                writer.writerow(row)
                written += 1

    print(f"[baseline] wrote {written} rows")
    print(f"[baseline] schema validation errors skipped: {errors_count}")
    print(f"[baseline] JSONL: {jsonl_path}")
    print(f"[baseline] CSV : {csv_path}")


if __name__ == "__main__":
    main()