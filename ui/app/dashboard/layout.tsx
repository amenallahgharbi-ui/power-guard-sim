import Link from "next/link";

const nav = [
  { href: "/dashboard", label: "Overview" },
  { href: "/dashboard/history", label: "History" },
  { href: "/dashboard/alerts", label: "Alerts" },
  { href: "/dashboard/live", label: "Live" },
  { href: "/dashboard/models", label: "Models" },
  { href: "/dashboard/dq", label: "Data Quality" },
  { href: "/dashboard/journal", label: "Journal" }, // <-- NEW
];

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <div className="flex">
        <aside className="w-64 bg-white border-r min-h-screen p-4">
          <div className="mb-6">
            <div className="text-xs text-gray-500">Power-Guard</div>
            <div className="font-semibold text-lg">Dashboard</div>
          </div>

          <nav className="flex flex-col gap-1 text-sm">
            {nav.map((i) => (
              <Link
                key={i.href}
                href={i.href}
                className="rounded-lg px-3 py-2 text-gray-700 hover:bg-gray-100 hover:text-gray-900 transition"
              >
                {i.label}
              </Link>
            ))}
          </nav>

          <div className="mt-6 rounded-lg border bg-gray-50 p-3 text-xs text-gray-600">
            Status: <span className="font-semibold text-gray-900">Dev</span>
          </div>
        </aside>

        <div className="flex-1">
          <header className="sticky top-0 bg-red-500 text-white border-b">
            <div className="px-6 py-4 flex items-center justify-between">
              <div className="text-sm text-gray-600">Power-Guard</div>
              <div className="text-xs text-gray-500">Dev server</div>
            </div>
          </header>

          <main className="p-6">{children}</main>
        </div>
      </div>
    </div>
  );
}