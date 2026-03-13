"use client";

import { useEffect, useMemo, useState } from "react";

type ModelMetricRow = {
  asset_id: string;
  model_name: string;
  target: string;
  horizon_min: number;
  rmse: number;
  mae: number;
  trained_at: string;
};

export default function ModelsPage() {
  const [rows, setRows] = useState<ModelMetricRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [asset, setAsset] = useState<string>("");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const res = await fetch("/api/models", { cache: "no-store" });
        if (!res.ok) throw new Error(`GET /api/models failed: ${res.status}`);
        const data = (await res.json()) as ModelMetricRow[];
        if (!cancelled) setRows(data);
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

  const assetIds = useMemo(() => Array.from(new Set(rows.map((r) => r.asset_id))).sort(), [rows]);
  const filtered = useMemo(() => (asset ? rows.filter((r) => r.asset_id === asset) : rows), [rows, asset]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">Models</h1>

        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-600">Asset</span>
          <select className="rounded border bg-white px-2 py-1" value={asset} onChange={(e) => setAsset(e.target.value)}>
            <option value="">All</option>
            {assetIds.map((a) => (
              <option key={a} value={a}>
                {a}
              </option>
            ))}
          </select>
        </div>
      </div>

      {loading && <div className="text-sm text-gray-600">Loading...</div>}
      {err && <div className="text-sm text-red-600">Error: {err}</div>}

      {!loading && !err && (
        <div className="overflow-auto rounded border bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-100 text-left">
              <tr>
                <th className="p-2">trained_at</th>
                <th className="p-2">asset</th>
                <th className="p-2">model</th>
                <th className="p-2">target</th>
                <th className="p-2">horizon</th>
                <th className="p-2">RMSE</th>
                <th className="p-2">MAE</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={`${r.asset_id}-${r.model_name}-${r.trained_at}-${i}`} className="border-t">
                  <td className="p-2 whitespace-nowrap">{new Date(r.trained_at).toISOString()}</td>
                  <td className="p-2">{r.asset_id}</td>
                  <td className="p-2">{r.model_name}</td>
                  <td className="p-2">{r.target}</td>
                  <td className="p-2">{r.horizon_min} min</td>
                  <td className="p-2">{Number(r.rmse).toFixed(4)}</td>
                  <td className="p-2">{Number(r.mae).toFixed(4)}</td>
                </tr>
              ))}

              {filtered.length === 0 && (
                <tr>
                  <td className="p-4 text-gray-600" colSpan={7}>
                    No model metrics found. Run: <code>python models/train_regression.py</code>
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