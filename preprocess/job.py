import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple

import requests
print("[preprocess] VERSION=2026-02-26-windowed-v1")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

SLEEP_SECONDS = int(os.environ.get("PREPROCESS_SLEEP_SECONDS", "60"))
LOOKBACK_HOURS = int(os.environ.get("PREPROCESS_LOOKBACK_HOURS", "48"))


def must_env():
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL is not set")
    if not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is not set")


def sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


def parse_ts(ts: str) -> datetime:
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    if "." in s:
        head, tail = s.split(".", 1)
        tz_pos = max(tail.rfind("+"), tail.rfind("-"))
        if tz_pos > 0:
            frac = tail[:tz_pos]
            tz = tail[tz_pos:]
        else:
            frac = tail
            tz = ""
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}{tz}"

    return datetime.fromisoformat(s).astimezone(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def postgrest_get(table: str, params: str) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    r = requests.get(url, headers=sb_headers(), timeout=60)
    if not r.ok:
        raise RuntimeError(f"GET failed url={url} status={r.status_code} body={r.text}")
    return r.json()


def postgrest_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    r = requests.post(url, headers=sb_headers(), data=json.dumps(rows), timeout=120)
    if not r.ok:
        raise RuntimeError(f"UPSERT failed url={url} status={r.status_code} body={r.text}")


def minute_bucket(ts_iso: str) -> str:
    dt = parse_ts(ts_iso).replace(second=0, microsecond=0)
    return iso_z(dt)


def percentile(values: List[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    k = (len(xs) - 1) * p
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def preprocess_once() -> None:
    since_dt = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    since_dt = since_dt.replace(minute=0, second=0, microsecond=0)
    now_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    total_raw = 0
    total_buckets = 0
    total_dq = 0

    cursor = since_dt
    while cursor < now_dt:
        next_cursor = cursor + timedelta(hours=1)
        start_iso = iso_z(cursor)
        end_iso = iso_z(next_cursor)

        raw = postgrest_get(
            "telemetry",
            "select=asset_id,ts,voltage_v,current_a,temperature_c,soc_pct,inserted_at"
            f"&ts=gte.{start_iso}"
            f"&ts=lt.{end_iso}"
            "&order=ts.asc"
            "&limit=1000",
        )

        total_raw += len(raw)

        buckets: Dict[Tuple[str, str], Dict[str, Any]] = {}
        lag_by_bucket: Dict[Tuple[str, str], List[float]] = {}

        for m in raw:
            asset_id = m["asset_id"]
            ts_min = minute_bucket(m["ts"])
            key = (asset_id, ts_min)

            b = buckets.get(key)
            if b is None:
                b = {
                    "asset_id": asset_id,
                    "ts_min": ts_min,
                    "voltage_sum": 0.0,
                    "current_sum": 0.0,
                    "temp_sum": 0.0,
                    "soc_sum": 0.0,
                    "n": 0,
                }
                buckets[key] = b
                lag_by_bucket[key] = []

            b["voltage_sum"] += float(m["voltage_v"])
            b["current_sum"] += float(m["current_a"])
            b["temp_sum"] += float(m["temperature_c"])
            b["soc_sum"] += float(m["soc_pct"])
            b["n"] += 1

            ts_dt = parse_ts(m["ts"])
            ins_dt = parse_ts(m["inserted_at"])
            lag_by_bucket[key].append((ins_dt - ts_dt).total_seconds())

        upsert_rows: List[Dict[str, Any]] = []
        for (asset_id, ts_min), b in buckets.items():
            n = b["n"]
            lags = lag_by_bucket[(asset_id, ts_min)]
            upsert_rows.append(
                {
                    "asset_id": asset_id,
                    "ts_min": ts_min,
                    "voltage_v_avg": b["voltage_sum"] / n,
                    "current_a_avg": b["current_sum"] / n,
                    "temperature_c_avg": b["temp_sum"] / n,
                    "soc_pct_avg": b["soc_sum"] / n,
                    "n_samples": n,
                    "lag_avg_s": (sum(lags) / len(lags)) if lags else None,
                    "lag_p95_s": percentile(lags, 0.95),
                }
            )

        postgrest_upsert("telemetry_1m", upsert_rows, on_conflict="asset_id,ts_min")
        total_buckets += len(upsert_rows)

        cursor = next_cursor

    # After backfill/upsert, compute DQ over the SAME lookback window using telemetry_1m
    since_iso = iso_z(since_dt)

    mins = postgrest_get(
        "telemetry_1m",
        "select=asset_id,ts_min"
        f"&ts_min=gte.{since_iso}"
        "&order=ts_min.asc"
        "&limit=200000",
    )

    minutes_by_day: Dict[Tuple[str, str], int] = {}
    for r in mins:
        day = parse_ts(r["ts_min"]).date().isoformat()
        k = (r["asset_id"], day)
        minutes_by_day[k] = minutes_by_day.get(k, 0) + 1

    # For DQ duplicates + lag stats, we compute from raw again, but day-by-day windowed
    # (Optional: could be optimized later)
    # We'll fetch raw per day in smaller hourly chunks again
    raw_rows_by_day: Dict[Tuple[str, str], int] = {}
    seen: Dict[Tuple[str, str], int] = {}
    lag_by_day: Dict[Tuple[str, str], List[float]] = {}

    cursor = since_dt
    while cursor < now_dt:
        next_cursor = cursor + timedelta(hours=1)
        start_iso = iso_z(cursor)
        end_iso = iso_z(next_cursor)

        raw = postgrest_get(
            "telemetry",
            "select=asset_id,ts,inserted_at"
            f"&ts=gte.{start_iso}"
            f"&ts=lt.{end_iso}"
            "&order=ts.asc"
            "&limit=1000",
        )

        for m in raw:
            asset_id = m["asset_id"]
            ts = m["ts"]
            day = parse_ts(ts).date().isoformat()
            kday = (asset_id, day)

            raw_rows_by_day[kday] = raw_rows_by_day.get(kday, 0) + 1
            seen[(asset_id, ts)] = seen.get((asset_id, ts), 0) + 1

            ts_dt = parse_ts(ts)
            ins_dt = parse_ts(m["inserted_at"])
            lag_by_day.setdefault(kday, []).append((ins_dt - ts_dt).total_seconds())

        cursor = next_cursor

    dup_by_day: Dict[Tuple[str, str], int] = {}
    for (asset_id, ts), c in seen.items():
        if c > 1:
            day = parse_ts(ts).date().isoformat()
            dup_by_day[(asset_id, day)] = dup_by_day.get((asset_id, day), 0) + (c - 1)

    dq_rows: List[Dict[str, Any]] = []
    for (asset_id, day), received in minutes_by_day.items():
        expected = 1440
        lags = lag_by_day.get((asset_id, day), [])
        dq_rows.append(
            {
                "asset_id": asset_id,
                "day": day,
                "expected_samples": expected,
                "received_samples": received,
                "completeness_pct": (received / expected) * 100.0,
                "raw_rows": raw_rows_by_day.get((asset_id, day), 0),
                "duplicates_raw": dup_by_day.get((asset_id, day), 0),
                "lag_p50_s": percentile(lags, 0.50),
                "lag_p95_s": percentile(lags, 0.95),
                "updated_at": iso_z(datetime.now(timezone.utc)),
            }
        )

    postgrest_upsert("dq_metrics_1d", dq_rows, on_conflict="asset_id,day")
    total_dq = len(dq_rows)

    print(
        f"[preprocess] backfill_hours={LOOKBACK_HOURS} raw_rows={total_raw} "
        f"telemetry_1m_upserts={total_buckets} dq_rows={total_dq} since={since_iso}"
    )

