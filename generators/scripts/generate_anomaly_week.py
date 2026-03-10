import csv
import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, List, Tuple

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


@dataclass
class Event:
    event_type: str
    start_idx: int
    end_idx: int  # inclusive
    magnitude: float
    target: Optional[str] = None


def choose_window(total_points: int, window_len: int) -> Tuple[int, int]:
    window_len = max(1, min(window_len, total_points))
    start = random.randint(0, total_points - window_len)
    end = start + window_len - 1
    return start, end


def add_coverage(covered: set, start: int, end: int) -> int:
    before = len(covered)
    covered.update(range(start, end + 1))
    return len(covered) - before


def main():
    cfg_path = Path("generators/configs/anomaly_week.yml")
    schema_path = Path("generators/schemas/telemetry_schema.json")

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    random.seed(int(cfg["meta"]["seed"]))

    sampling_seconds = int(cfg["meta"]["sampling_seconds"])
    duration_days = int(cfg["meta"]["duration_days"])
    start_ts = parse_ts(cfg["meta"]["start_ts_utc"])

    total_points = int((duration_days * 24 * 3600) / sampling_seconds)
    points_per_day = int((24 * 3600) / sampling_seconds)

    prof = cfg["profile_nominal"]
    noise = prof["noise_sigma"]
    rw_sigma = prof["random_walk_sigma"]

    an = cfg["anomalies"]
    min_pct = float(an["anomaly_rate_per_asset"]["min_pct"])
    max_pct = float(an["anomaly_rate_per_asset"]["max_pct"])

    drift_cfg = an["drift_slow"]
    spike_cfg = an["thermal_spike"]
    step_cfg = an["step_change"]

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

    per_asset_report = {}

    with jsonl_path.open("w", encoding="utf-8") as jf, csv_path.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.DictWriter(cf, fieldnames=csv_fields)
        writer.writeheader()

        for asset in cfg["assets"]:
            asset_id = asset["asset_id"]
            nominal = asset["nominal"]

            # budget anomalous points for this asset
            target_pct = random.uniform(min_pct, max_pct)
            target_points = int(round(total_points * (target_pct / 100.0)))
            covered = set()
            events: List[Event] = []

            # Helper to only add events if we still need coverage
            def remaining() -> int:
                return max(0, target_points - len(covered))

            # 1) Drift (0 or 1 window), but capped to not exceed budget
            if drift_cfg["enabled"] and remaining() > 0:
                dur_h = random.randint(int(drift_cfg["duration_hours_min"]), int(drift_cfg["duration_hours_max"]))
                win = int((dur_h * 3600) / sampling_seconds)
                # cap drift window to at most 60% of budget (so spikes/step still exist)
                win = min(win, max(1, int(0.6 * target_points)))
                s, e = choose_window(total_points, win)
                mag = random.uniform(float(drift_cfg["pct_increase_min"]), float(drift_cfg["pct_increase_max"]))
                tgt = drift_cfg.get("target", "current_a")
                events.append(Event("drift_slow", s, e, mag, tgt))
                add_coverage(covered, s, e)

            # 2) Step change (instant start) but LIMITED duration
            if step_cfg["enabled"] and remaining() > 0:
                dur_m = random.randint(int(step_cfg["duration_minutes_min"]), int(step_cfg["duration_minutes_max"]))
                win = int((dur_m * 60) / sampling_seconds)
                # cap step to at most 30% of budget
                win = min(win, max(1, int(0.3 * target_points)))
                s, e = choose_window(total_points, win)
                mag = random.uniform(float(step_cfg["pct_increase_min"]), float(step_cfg["pct_increase_max"]))
                tgt = step_cfg.get("target", "current_a")
                events.append(Event("step_change", s, e, mag, tgt))
                add_coverage(covered, s, e)

            # 3) Thermal spikes fill the remainder
            spike_id = 0
            while spike_cfg["enabled"] and remaining() > 0 and spike_id < 200:
                dur_m = random.randint(int(spike_cfg["duration_minutes_min"]), int(spike_cfg["duration_minutes_max"]))
                win = int((dur_m * 60) / sampling_seconds)
                # don't add spikes larger than what's remaining (to stay in range)
                win = min(win, remaining())
                s, e = choose_window(total_points, win)
                delta = random.uniform(float(spike_cfg["temp_increase_c_min"]), float(spike_cfg["temp_increase_c_max"]))
                events.append(Event("thermal_spike", s, e, delta))
                add_coverage(covered, s, e)
                spike_id += 1

            # Build active lookup
            active_by_idx: Dict[int, List[Tuple[int, Event]]] = {}
            for ev_i, ev in enumerate(events, start=1):
                for k in range(ev.start_idx, ev.end_idx + 1):
                    active_by_idx.setdefault(k, []).append((ev_i, ev))

            # Generate series
            soc = float(nominal["soc_start_pct"])
            v_rw = i_rw = t_rw = 0.0

            schema_errors = 0
            rows_written = 0
            anomalous_rows = 0

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

                label = 0
                event_type = "none"
                event_id = ""

                if i in active_by_idx:
                    label = 1
                    evs = active_by_idx[i]

                    # apply all effects
                    for _, ev in evs:
                        if ev.event_type == "thermal_spike":
                            temp += ev.magnitude
                        elif ev.event_type in ("drift_slow", "step_change"):
                            tgt = ev.target or "current_a"
                            factor = 1.0 + (ev.magnitude / 100.0)
                            if tgt == "current_a":
                                current *= factor
                            elif tgt == "voltage_v":
                                voltage *= factor

                    # choose primary label (priority)
                    types = [ev.event_type for _, ev in evs]
                    if "thermal_spike" in types:
                        event_type = "thermal_spike"
                    elif "step_change" in types:
                        event_type = "step_change"
                    else:
                        event_type = "drift_slow"

                    for ev_i, ev in evs:
                        if ev.event_type == event_type:
                            event_id = f"{asset_id}_{event_type}_{ev_i}"
                            break

                # update random walks
                v_rw += random.gauss(0.0, float(rw_sigma["voltage_v"]))
                i_rw += random.gauss(0.0, float(rw_sigma["current_a"]))
                t_rw += random.gauss(0.0, float(rw_sigma["temperature_c"]))

                # SoC evolution
                soc -= (max(current, 0.0) * sampling_seconds) / (24 * 3600) * 1.5
                soc += random.gauss(0.0, float(noise["soc_pct"]))
                soc = clamp(soc, *bounds["soc_pct"])

                voltage = clamp(voltage, *bounds["voltage_v"])
                current = clamp(current, *bounds["current_a"])
                temp = clamp(temp, *bounds["temperature_c"])

                msg_profile = "nominal" if label == 0 else event_type

                msg = {
                    "ts": iso_z(ts),
                    "asset_id": asset_id,
                    "voltage_v": round(voltage, 3),
                    "current_a": round(current, 3),
                    "temperature_c": round(temp, 3),
                    "soc_pct": round(soc, 3),
                    "profile": msg_profile,
                }

                if list(validator.iter_errors(msg)):
                    schema_errors += 1
                    continue

                jf.write(json.dumps(msg, ensure_ascii=False) + "\n")

                row = dict(msg)
                row.update({"label": label, "event_type": event_type, "event_id": event_id})
                writer.writerow(row)

                rows_written += 1
                anomalous_rows += label

            per_asset_report[asset_id] = {
                "rows": rows_written,
                "anomalous": anomalous_rows,
                "anomaly_pct": (anomalous_rows / rows_written * 100.0) if rows_written else 0.0,
                "schema_errors": schema_errors,
                "target_points": target_points,
            }

    print("[anomaly] generation done")
    for asset_id, r in per_asset_report.items():
        print(
            f"  {asset_id}: rows={r['rows']}, anomalous={r['anomalous']} "
            f"({r['anomaly_pct']:.2f}%), target={r['target_points']}, schema_errors={r['schema_errors']}"
        )
    print(f"[anomaly] JSONL: {jsonl_path}")
    print(f"[anomaly] CSV : {csv_path}")


if __name__ == "__main__":
    main()