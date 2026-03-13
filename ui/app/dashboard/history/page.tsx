"use client";

import { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  ReferenceDot,
  Legend,
} from "recharts";

type TelemetryRow = {
  asset_id: string;
  ts_min: string;
  voltage_v_avg: number | null;
  current_a_avg: number | null;
  temperature_c_avg: number | null;
  soc_pct_avg: number | null;
};

type AlertRow = {
  asset_id: string;
  ts_min: string;
  alert_type: string;
  severity: string;
  metric_value: number | null;
  threshold: number | null;
  status: string | null;
  message: string | null;
};

type HistoryPayload = { telemetry: TelemetryRow[]; alerts: AlertRow[] };

function isoNowMinusHours(h: number) {
  const d = new Date(Date.now() - h * 3600 * 1000);
  return d.toISOString();
}

export default function HistoryPage() {
  const [assets, setAssets] = useState<string[]>([]);
  const [asset, setAsset] = useState("");
  const [start, setStart] = useState(isoNowMinusHours(24));
  const [end, setEnd] = useState(new Date().toISOString());
  const [data, setData] = useState<HistoryPayload | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function loadAssets() {
      try {
        // quick way: use existing alerts endpoint to discover assets
        const res = await fetch("/api/alerts", { cache: "no-store" });
        if (!res.ok) throw new Error(`GET /api/alerts failed: ${res.status}`);
        const rows = (await res.json()) as AlertRow[];
        const ids = Array.from(new Set(rows.map((r) => r.asset_id))).sort();
        if (!cancelled) setAssets(ids);
        if (!cancelled && ids.length && !asset) setAsset(ids[0]);
      } catch (e: any) {
        if (!cancelled) setErr(e?.message ?? String(e));
      }
    }

    loadAssets();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const canFetch = useMemo(() => asset && start && end, [asset, start, end]);

  async function load() {
    if (!canFetch) return;
    setLoading(true);
    setErr(null);

    try {
      const url = `/api/history?asset_id=${encodeURIComponent(asset)}&start=${encodeURIComponent(
        start
      )}&end=${encodeURIComponent(end)}`;
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) throw new Error(`GET ${url} failed: ${res.status}`);
      const payload = (await res.json()) as HistoryPayload;
      setData(payload);
    } catch (e: any) {
      setErr(e?.message ?? String(e));
      setData(null);
    } finally {
      setLoading(false);
    }
  }

  // Auto-load when asset changes once
  useEffect(() => {
    if (asset) load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [asset]);

  const chartData = useMemo(() => {
    const telemetry = data?.telemetry ?? [];
    return telemetry.map((r) => ({
      ts: r.ts_min,
      t: new Date(r.ts_min).getTime(),
      voltage: r.voltage_v_avg,
      current: r.current_a_avg,
      temp: r.temperature_c_avg,
      soc: r.soc_pct_avg,
    }));
  }, [data]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">History</h1>
        <button
          onClick={load}
          disabled={!canFetch || loading}
          className="rounded bg-gray-900 text-white px-3 py-2 text-sm disabled:opacity-50"
        >
          {loading ? "Loading..." : "Reload"}
        </button>
      </div>

      <div className="rounded border bg-white p-4 grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
        <div>
          <div className="text-xs text-gray-600 mb-1">Asset</div>
          <select
            className="w-full rounded border px-2 py-2"
            value={asset}
            onChange={(e) => setAsset(e.target.value)}
          >
            {assets.map((a) => (
              <option key={a} value={a}>
                {a}
              </option>
            ))}
          </select>
        </div>

        <div>
          <div className="text-xs text-gray-600 mb-1">Start (ISO)</div>
          <input className="w-full rounded border px-2 py-2" value={start} onChange={(e) => setStart(e.target.value)} />
        </div>

        <div>
          <div className="text-xs text-gray-600 mb-1">End (ISO)</div>
          <input className="w-full rounded border px-2 py-2" value={end} onChange={(e) => setEnd(e.target.value)} />
        </div>

        
      </div>

      {err && <div className="text-sm text-red-600">Error: {err}</div>}

      {data && (
        <div className="grid grid-cols-1 gap-4">
          <section className="rounded border bg-white p-3">
            <div className="font-medium mb-2">
              Voltage / Current / Temperature / SoC (alerts as red dots)
            </div>

            <div style={{ width: "100%", height: 380 }}>
              <ResponsiveContainer>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="t"
                    type="number"
                    domain={["dataMin", "dataMax"]}
                    tickFormatter={(v) => new Date(v).toLocaleString()}
                  />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip
                    labelFormatter={(v) => new Date(Number(v)).toISOString()}
                    formatter={(value: any, name: any) => [value, name]}
                  />
                  <Legend />

                  <Line
                    yAxisId="left"
                    type="monotone"
                    dataKey="voltage"
                    stroke="#2563eb"
                    dot={false}
                    name="Voltage (V)"
                  />
                  <Line
                    yAxisId="left"
                    type="monotone"
                    dataKey="current"
                    stroke="#16a34a"
                    dot={false}
                    name="Current (A)"
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="temp"
                    stroke="#f97316"
                    dot={false}
                    name="Temp (°C)"
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="soc"
                    stroke="#7c3aed"
                    dot={false}
                    name="SoC (%)"
                  />

                  {/* Alerts markers (use voltage y position for a consistent marker placement) */}
                  {(data.alerts ?? []).map((a, idx) => {
                    const t = new Date(a.ts_min).getTime();
                    const nearest = chartData.find((p) => p.t === t);
                    const y = nearest?.voltage ?? null;
                    if (y == null) return null;

                    return (
                      <ReferenceDot
                        key={`${a.ts_min}-${a.alert_type}-${idx}`}
                        x={t}
                        y={y}
                        yAxisId="left"
                        r={4}
                        fill="#dc2626"
                        stroke="none"
                      />
                    );
                  })}
                </LineChart>
              </ResponsiveContainer>
            </div>

            <div className="mt-2 text-xs text-gray-600">
              Telemetry points: <span className="font-semibold">{data.telemetry.length}</span> — Alerts in range:{" "}
              <span className="font-semibold">{data.alerts.length}</span>
            </div>
          </section>

          <section className="rounded border bg-white overflow-auto">
            <div className="border-b p-3 font-medium">Alerts (details)</div>
            <table className="min-w-full text-sm">
              <thead className="bg-gray-100 text-left">
                <tr>
                  <th className="p-2">ts</th>
                  <th className="p-2">type</th>
                  <th className="p-2">severity</th>
                  <th className="p-2">message</th>
                </tr>
              </thead>
              <tbody>
                {(data.alerts ?? []).slice(0, 200).map((a, i) => (
                  <tr key={`${a.ts_min}-${a.alert_type}-${i}`} className="border-t">
                    <td className="p-2 whitespace-nowrap">{new Date(a.ts_min).toISOString()}</td>
                    <td className="p-2">{a.alert_type}</td>
                    <td className="p-2">{a.severity}</td>
                    <td className="p-2 text-gray-700">{a.message ?? "-"}</td>
                  </tr>
                ))}
                {(data.alerts ?? []).length === 0 && (
                  <tr>
                    <td className="p-4 text-gray-600" colSpan={4}>
                      No alerts in range.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
        </div>
      )}
    </div>
  );
}