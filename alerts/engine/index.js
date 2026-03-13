import fetch from "node-fetch";
import { journalAlertsCreated } from "./journal.js";

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const SLEEP_SECONDS = parseInt(process.env.ALERTS_SLEEP_SECONDS || "60", 10);
const LOOKBACK_MINUTES = parseInt(process.env.ALERTS_LOOKBACK_MINUTES || "60", 10);
const N = parseInt(process.env.ALERTS_HYSTERESIS_N || "3", 10);
const ALPHA = parseFloat(process.env.ALERTS_EWMA_ALPHA || "0.2");

const J0 = process.env.CALIB_J0 || "2026-02-01";
const J7 = process.env.CALIB_J7 || "2026-02-08";

const ANCHOR_TO_LATEST = process.env.ALERTS_ANCHOR_TO_LATEST_TELEMETRY === "1";
const RUN_ONCE = process.env.ALERTS_RUN_ONCE === "1";

// Completeness rule knobs
const COMPLETENESS_MIN_RATIO = parseFloat(process.env.ALERTS_COMPLETENESS_MIN_RATIO || "0.98");
const COMPLETENESS_MIN_MISSING = parseInt(process.env.ALERTS_COMPLETENESS_MIN_MISSING || "3", 10);

function mustEnv() {
  if (!SUPABASE_URL) throw new Error("SUPABASE_URL is not set");
  if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is not set");
}

function headers(prefer = "resolution=merge-duplicates,return=minimal") {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    Prefer: prefer,
  };
}

async function sbGet(table, query) {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, { headers: headers(), method: "GET" });
  if (!r.ok) throw new Error(`GET ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

async function sbUpsert(table, rows, onConflict) {
  if (!rows.length) return [];
  const url = `${SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(onConflict)}`;
  const r = await fetch(url, {
    headers: headers("resolution=merge-duplicates,return=representation"),
    method: "POST",
    body: JSON.stringify(rows),
  });
  if (!r.ok) throw new Error(`UPSERT ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

function isoZ(d) {
  return new Date(d).toISOString().replace(/\.\d{3}Z$/, "Z");
}

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function normId(x) {
  return String(x ?? "").trim();
}

function mkAlert(asset_id, ts_min, alert_type, severity, metric_value, threshold, message) {
  return {
    asset_id,
    ts_min,
    alert_type,
    severity,
    metric_value,
    threshold,
    rule_window_n: N,
    message,
  };
}

function hysteresisTrip(series, isBadFn) {
  const trips = [];
  for (let i = 0; i < series.length; i++) {
    let ok = true;
    for (let k = 0; k < N; k++) {
      const j = i - k;
      if (j < 0 || !isBadFn(series[j])) {
        ok = false;
        break;
      }
    }
    if (ok) trips.push(i);
  }
  return trips;
}

async function getLatestTelemetryTs() {
  const rows = await sbGet("telemetry_1m", "select=ts_min&order=ts_min.desc&limit=1");
  if (!rows.length) return null;
  return rows[0].ts_min;
}

async function fetchTelemetryForAsset(assetId, since, anchor) {
  const id = normId(assetId);
  return sbGet(
    "telemetry_1m",
    `select=asset_id,ts_min,voltage_v_avg,current_a_avg,temperature_c_avg&asset_id=eq.${encodeURIComponent(
      id
    )}&ts_min=gte.${isoZ(since)}&ts_min=lte.${isoZ(anchor)}&order=ts_min.asc&limit=5000`
  );
}

async function runOnce() {
  // 1) Load baselines and pick latest per asset
  const baselineAll = await sbGet(
    "calibration_baseline",
    "select=asset_id,j0,j7,voltage_low,current_high,temp_high,voltage_mean,current_mean,temp_median&order=asset_id.asc,j0.desc&limit=20000"
  );

  const baseByAsset = new Map();
  for (const b of baselineAll) {
    const id = normId(b.asset_id);
    if (!id) continue;
    if (!baseByAsset.has(id)) baseByAsset.set(id, { ...b, asset_id: id });
  }

  if (process.env.CALIB_FORCE_GLOBAL === "1") {
    const forced = await sbGet(
      "calibration_baseline",
      `select=asset_id,j0,j7,voltage_low,current_high,temp_high,voltage_mean,current_mean,temp_median&j0=eq.${J0}&j7=eq.${J7}`
    );
    baseByAsset.clear();
    for (const b of forced) {
      const id = normId(b.asset_id);
      if (!id) continue;
      baseByAsset.set(id, { ...b, asset_id: id });
    }
  }

  // 2) Choose lookback window
  let anchor = new Date();
  if (ANCHOR_TO_LATEST) {
    const latest = await getLatestTelemetryTs();
    if (latest) anchor = new Date(latest);
  }
  const since = new Date(anchor.getTime() - LOOKBACK_MINUTES * 60 * 1000);

  // 3) Fetch telemetry PER ASSET (avoids PostgREST max-rows truncation)
  const byAsset = new Map();
  for (const assetId of baseByAsset.keys()) {
    const rows = await fetchTelemetryForAsset(assetId, since, anchor);
    if (rows.length) byAsset.set(assetId, rows.map((r) => ({ ...r, asset_id: normId(r.asset_id) })));
  }

  console.log(
    `[alert-engine] window since=${isoZ(since)} anchor=${isoZ(anchor)} assets_with_rows=${byAsset.size}/${baseByAsset.size}`
  );

  // 4) Evaluate trips + completeness and insert alerts
  const alerts = [];
  const expected = LOOKBACK_MINUTES + 1; // inclusive since..anchor

  for (const [assetId, rows] of byAsset.entries()) {
    const b = baseByAsset.get(assetId);
    if (!rows.length) continue;

    // completeness_low (1 alert per asset per run)
    const missing = Math.max(0, expected - rows.length);
    const ratio = expected > 0 ? rows.length / expected : 1;

    if (missing >= COMPLETENESS_MIN_MISSING && ratio < COMPLETENESS_MIN_RATIO) {
      alerts.push(
        mkAlert(
          assetId,
          isoZ(anchor),
          "completeness_low",
          "warning",
          rows.length,
          expected,
          `Telemetry completeness low: got=${rows.length} expected≈${expected} missing=${missing} (${Math.round(
            ratio * 100
          )}%)`
        )
      );
    }

    // threshold trips (N-minute persistence)
    const vTrips = hysteresisTrip(rows, (x) => Number(x.voltage_v_avg) < Number(b.voltage_low));
    for (const idx of vTrips) {
      const x = rows[idx];
      alerts.push(
        mkAlert(
          assetId,
          x.ts_min,
          "voltage_low",
          "warning",
          Number(x.voltage_v_avg),
          Number(b.voltage_low),
          `Voltage bas persisté ${N} minutes (avg=${x.voltage_v_avg}, thr=${b.voltage_low})`
        )
      );
    }

    const iTrips = hysteresisTrip(rows, (x) => Number(x.current_a_avg) > Number(b.current_high));
    for (const idx of iTrips) {
      const x = rows[idx];
      alerts.push(
        mkAlert(
          assetId,
          x.ts_min,
          "current_high",
          "warning",
          Number(x.current_a_avg),
          Number(b.current_high),
          `Courant haut persisté ${N} minutes (avg=${x.current_a_avg}, thr=${b.current_high})`
        )
      );
    }

    const tTrips = hysteresisTrip(rows, (x) => Number(x.temperature_c_avg) > Number(b.temp_high));
    for (const idx of tTrips) {
      const x = rows[idx];
      alerts.push(
        mkAlert(
          assetId,
          x.ts_min,
          "temp_high",
          "warning",
          Number(x.temperature_c_avg),
          Number(b.temp_high),
          `Température haute persistée ${N} minutes (avg=${x.temperature_c_avg}, thr=${b.temp_high})`
        )
      );
    }
  }

  await sbUpsert("alerts", alerts, "asset_id,ts_min,alert_type");

  // Automatic signed journal (Ed25519): record alert_created events
  try {
    await journalAlertsCreated(alerts);
  } catch (e) {
    console.error("[alert-engine] journal write failed:", e?.message || e);
  }

  // 5) Update EWMA state using last point per asset
  const ewmaRows = [];
  for (const [assetId, rows] of byAsset.entries()) {
    const last = rows[rows.length - 1];
    ewmaRows.push({
      asset_id: assetId,
      metric: "voltage_v",
      ts_min: last.ts_min,
      ewma: Number(last.voltage_v_avg),
      alpha: ALPHA,
    });
    ewmaRows.push({
      asset_id: assetId,
      metric: "current_a",
      ts_min: last.ts_min,
      ewma: Number(last.current_a_avg),
      alpha: ALPHA,
    });
    ewmaRows.push({
      asset_id: assetId,
      metric: "temperature_c",
      ts_min: last.ts_min,
      ewma: Number(last.temperature_c_avg),
      alpha: ALPHA,
    });
  }
  await sbUpsert("ewma_state", ewmaRows, "asset_id,metric");

  return {
    alertsInserted: alerts.length,
    ewmaUpserts: ewmaRows.length,
    assets: byAsset.size,
    anchor: isoZ(anchor),
    since: isoZ(since),
    completeness: { minRatio: COMPLETENESS_MIN_RATIO, minMissing: COMPLETENESS_MIN_MISSING, expected },
  };
}

async function main() {
  mustEnv();
  console.log(`[alert-engine] VERSION=2026-03-11 v5`);
  console.log(
    `[alert-engine] N=${N} alpha=${ALPHA} lookback_minutes=${LOOKBACK_MINUTES} sleep_s=${SLEEP_SECONDS}`
  );
  console.log(
    `[alert-engine] anchor_to_latest=${ANCHOR_TO_LATEST} run_once=${RUN_ONCE} J0=${J0} J7=${J7} (force_global=${
      process.env.CALIB_FORCE_GLOBAL === "1"
    })`
  );
  console.log(
    `[alert-engine] completeness_min_ratio=${COMPLETENESS_MIN_RATIO} completeness_min_missing=${COMPLETENESS_MIN_MISSING}`
  );

  if (RUN_ONCE) {
    const t0 = Date.now();
    const r = await runOnce();
    console.log(
      `[alert-engine] ok alerts=${r.alertsInserted} ewma_upserts=${r.ewmaUpserts} assets=${r.assets} anchor=${r.anchor} since=${r.since} dt_ms=${
        Date.now() - t0
      }`
    );
    process.exit(0);
  }

  while (true) {
    const t0 = Date.now();
    try {
      const r = await runOnce();
      console.log(
        `[alert-engine] ok alerts=${r.alertsInserted} ewma_upserts=${r.ewmaUpserts} assets=${r.assets} anchor=${r.anchor} since=${r.since} dt_ms=${
          Date.now() - t0
        }`
      );
    } catch (e) {
      console.error(`[alert-engine] error dt_ms=${Date.now() - t0}`, e);
    }
    await sleep(SLEEP_SECONDS * 1000);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});