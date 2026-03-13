export default function DashboardHome() {
  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Dashboard</h1>
      <p className="text-gray-600">
        Welcome to Power-Guard UI.
      </p>

      <ul className="list-disc pl-5 text-sm text-gray-700 space-y-1">
        <li>Overview</li>
        <li>Alerts</li>
        <li>Live</li>
        <li>Models</li>
        <li>Data Quality</li>
      </ul>
    </div>
  );
}
