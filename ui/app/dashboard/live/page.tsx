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
  Legend,
} from "recharts";

type Telemetry1mRow = {
  asset_id: string;
  ts_min: string;
  voltage_v_avg: number | null;
  current_a_avg: number | null;
  temperature_c_avg: number | null;
  soc_pct_avg: number | null;
  n_samples: number | null;
};

type AlertRow = {
  asset_id: string;
  ts_min: string;
  alert_type: string;
  severity: string;
};

type LivePayload = {
  telemetry: Telemetry1mRow[];
  alerts: AlertRow[];
};

export default function LivePage() {
  const [data, setData] = useState<LivePayload | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const [asset, setAsset] = useState<string>("");
  const [assetAutoSelected, setAssetAutoSelected] = useState(false);

  async function load() {
    try {
      setErr(null);
      const res = await fetch("/api/live", { cache: "no-store" });
      if (!res.ok) throw new Error(`GET /api/live failed: ${res.status}`);
      const payload = (await res.json()) as LivePayload;

      setData(payload);

      // Auto-select an asset only once (do not override user selection)
      if (!assetAutoSelected) {
        const assets = Array.from(new Set((payload.telemetry ?? []).map((t) => t.asset_id))).sort();
        if (assets.length) {
          setAsset((prev) => prev || assets[0]);
          setAssetAutoSelected(true);
        }
      }
    } catch (e: any) {
      setErr(e?.message ?? String(e));
    }
  }

  useEffect(() => {
    let cancelled = false;
    let t: any;

    async function loop() {
      if (cancelled) return;
      await load();
      if (cancelled) return;
      t = setTimeout(loop, 5000);
    }

    loop();
    return () => {
      cancelled = true;
      if (t) clearTimeout(t);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const assets = useMemo(() => {
    const ids = Array.from(new Set((data?.telemetry ?? []).map((t) => t.asset_id))).sort();
    return ids;
  }, [data]);

  const telemetry = useMemo(() => {
    const all = data?.telemetry ?? [];
    const filtered = asset ? all.filter((r) => r.asset_id === asset) : all;
    const last = filtered.slice(0, 120).reverse(); // API sorted desc -> reverse for chart
    return last.map((r) => ({
      t: new Date(r.ts_min).getTime(),
      ts: r.ts_min,
      voltage: r.voltage_v_avg,
      current: r.current_a_avg,
      temp: r.temperature_c_avg,
      soc: r.soc_pct_avg,
    }));
  }, [data, asset]);

  const latest = telemetry.length ? telemetry[telemetry.length - 1] : null;

  const alerts = useMemo(() => {
    const all = data?.alerts ?? [];
    return asset ? all.filter((a) => a.asset_id === asset).slice(0, 50) : all.slice(0, 50);
  }, [data, asset]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">Live</h1>

        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-600">Asset</span>
          <select
            className="rounded border bg-white px-2 py-1"
            value={asset}
            onChange={(e) => setAsset(e.target.value)}
          >
            {assets.map((a) => (
              <option key={a} value={a}>
                {a}
              </option>
            ))}
          </select>

          <button
            onClick={load}
            className="rounded border bg-white px-3 py-2 text-sm hover:bg-gray-50"
          >
            Refresh
          </button>
        </div>
      </div>

      {err && <div className="text-sm text-red-600">Error: {err}</div>}

      <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
        <div className="rounded border bg-white p-3">
          <div className="text-xs text-gray-500">Voltage (V)</div>
          <div className="text-lg font-semibold">{latest?.voltage ?? "-"}</div>
        </div>
        <div className="rounded border bg-white p-3">
          <div className="text-xs text-gray-500">Current (A)</div>
          <div className="text-lg font-semibold">{latest?.current ?? "-"}</div>
        </div>
        <div className="rounded border bg-white p-3">
          <div className="text-xs text-gray-500">Temp (°C)</div>
          <div className="text-lg font-semibold">{latest?.temp ?? "-"}</div>
        </div>
        <div className="rounded border bg-white p-3">
          <div className="text-xs text-gray-500">SoC (%)</div>
          <div className="text-lg font-semibold">{latest?.soc ?? "-"}</div>
        </div>
      </div>

      <section className="rounded border bg-white p-3">
        <div className="font-medium mb-2">Last ~120 minutes</div>
        <div style={{ width: "100%", height: 360 }}>
          <ResponsiveContainer>
            <LineChart data={telemetry}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="t"
                type="number"
                domain={["dataMin", "dataMax"]}
                tickFormatter={(v) => new Date(v).toLocaleTimeString()}
              />
              <YAxis yAxisId="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip labelFormatter={(v) => new Date(Number(v)).toISOString()} />
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
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="rounded border bg-white overflow-auto">
        <div className="border-b p-3 font-medium">Latest alerts (this asset)</div>
        <table className="min-w-full text-sm">
          <thead className="bg-gray-100 text-left">
            <tr>
              <th className="p-2">ts</th>
              <th className="p-2">type</th>
              <th className="p-2">severity</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={`${a.asset_id}-${a.ts_min}-${a.alert_type}-${i}`} className="border-t">
                <td className="p-2 whitespace-nowrap">{new Date(a.ts_min).toISOString()}</td>
                <td className="p-2">{a.alert_type}</td>
                <td className="p-2">{a.severity}</td>
              </tr>
            ))}
            {alerts.length === 0 && (
              <tr>
                <td className="p-4 text-gray-600" colSpan={3}>
                  No alerts.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}