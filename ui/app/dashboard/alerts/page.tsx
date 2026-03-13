"use client";

import { useEffect, useState } from "react";

type AlertRow = {
  asset_id: string;
  ts_min: string;
  alert_type: string;
  severity: string;
  metric_value: number;
  threshold: number;
  status: string;
  created_at: string;
  message: string;
};

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<AlertRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const res = await fetch("/api/alerts", { cache: "no-store" });
        if (!res.ok) throw new Error(`GET /api/alerts failed: ${res.status}`);
        const data = (await res.json()) as AlertRow[];
        if (!cancelled) setAlerts(data);
      } catch (e: any) {
        if (!cancelled) setErr(e?.message ?? String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Alerts</h1>

      {loading && <div className="text-sm text-gray-600">Loading...</div>}
      {err && <div className="text-sm text-red-600">Error: {err}</div>}

      {!loading && !err && (
        <div className="overflow-auto rounded border bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-100 text-left">
              <tr>
                <th className="p-2">ts</th>
                <th className="p-2">asset</th>
                <th className="p-2">type</th>
                <th className="p-2">severity</th>
                <th className="p-2">value</th>
                <th className="p-2">thr</th>
                <th className="p-2">status</th>
              </tr>
            </thead>
            <tbody>
              {alerts.map((a, i) => (
                <tr key={`${a.asset_id}-${a.ts_min}-${a.alert_type}-${i}`} className="border-t">
                  <td className="p-2 whitespace-nowrap">{new Date(a.ts_min).toISOString()}</td>
                  <td className="p-2">{a.asset_id}</td>
                  <td className="p-2">{a.alert_type}</td>
                  <td className="p-2">{a.severity}</td>
                  <td className="p-2">{a.metric_value}</td>
                  <td className="p-2">{a.threshold}</td>
                  <td className="p-2">{a.status}</td>
                </tr>
              ))}

              {alerts.length === 0 && (
                <tr>
                  <td className="p-4 text-gray-600" colSpan={7}>
                    No alerts.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
