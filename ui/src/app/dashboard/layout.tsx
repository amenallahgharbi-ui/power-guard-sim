export default function DashboardHome() {
  const cards = [
    { label: "Assets", value: "2", hint: "asset_A1, asset_A2" },
    { label: "Alerts (last 200)", value: "—", hint: "from /api/alerts" },
    { label: "Last refresh", value: "Live", hint: "dev mode" },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Overview</h1>
        <p className="text-sm text-slate-600">
          A modern monitoring UI for telemetry, anomaly detection, and data quality.
        </p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {cards.map((c) => (
          <div key={c.label} className="rounded-xl border bg-white p-4 shadow-sm">
            <div className="text-xs font-medium text-slate-500">{c.label}</div>
            <div className="mt-1 text-2xl font-semibold">{c.value}</div>
            <div className="mt-2 text-xs text-slate-500">{c.hint}</div>
          </div>
        ))}
      </div>

      <div className="rounded-xl border bg-white p-4 shadow-sm">
        <div className="text-sm font-semibold">Next steps</div>
        <ul className="mt-2 text-sm text-slate-600 list-disc pl-5 space-y-1">
          <li>Live view (latest telemetry_1m per asset)</li>
          <li>Models view (baseline + EWMA state)</li>
          <li>Data Quality view (dq_metrics_1d + completeness alerts)</li>
        </ul>
      </div>
    </div>
  );
}