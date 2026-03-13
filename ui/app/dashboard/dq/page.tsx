"use client";

import { useEffect, useState } from "react";

type DqRow = {
  asset_id: string;
  day: string;
  expected_samples: number | null;
  received_samples: number | null;
  completeness_pct: number | null;
  raw_rows: number | null;
  duplicates_raw: number | null;
  lag_p50_s: number | null;
  lag_p95_s: number | null;
  updated_at: string | null;
};

export default function DqPage() {
  const [rows, setRows] = useState<DqRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const res = await fetch("/api/dq", { cache: "no-store" });
        if (!res.ok) throw new Error(`GET /api/dq failed: ${res.status}`);
        const data = (await res.json()) as DqRow[];
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

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Data Quality</h1>

      {loading && <div className="text-sm text-gray-600">Loading...</div>}
      {err && <div className="text-sm text-red-600">Error: {err}</div>}

      {!loading && !err && (
        <div className="overflow-auto rounded border bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-100 text-left">
              <tr>
                <th className="p-2">day</th>
                <th className="p-2">asset</th>
                <th className="p-2">completeness %</th>
                <th className="p-2">received</th>
                <th className="p-2">expected</th>
                <th className="p-2">duplicates</th>
                <th className="p-2">lag p50 (s)</th>
                <th className="p-2">lag p95 (s)</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={`${r.asset_id}-${r.day}-${i}`} className="border-t">
                  <td className="p-2 whitespace-nowrap">{r.day}</td>
                  <td className="p-2">{r.asset_id}</td>
                  <td className="p-2">
                    {r.completeness_pct == null ? "-" : r.completeness_pct.toFixed(2)}
                  </td>
                  <td className="p-2">{r.received_samples ?? "-"}</td>
                  <td className="p-2">{r.expected_samples ?? "-"}</td>
                  <td className="p-2">{r.duplicates_raw ?? "-"}</td>
                  <td className="p-2">{r.lag_p50_s ?? "-"}</td>
                  <td className="p-2">{r.lag_p95_s ?? "-"}</td>
                </tr>
              ))}

              {rows.length === 0 && (
                <tr>
                  <td className="p-4 text-gray-600" colSpan={8}>
                    No DQ rows found. (Did you run preprocess/job.py ?)
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