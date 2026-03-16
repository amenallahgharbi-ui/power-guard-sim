"use client";

import { useEffect, useMemo, useState } from "react";

type JournalRow = {
  ts: string;
  actor: string;
  event_type: string;
  entity_type: string | null;
  entity_id: string | null;
  payload: any;
  payload_hash: string;
  verified: boolean;
  verified_runtime?: boolean;
};

function asText(v: any) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function rowKey(r: JournalRow) {
  return `${r.ts}::${r.payload_hash}`;
}

export default function JournalPage() {
  const [rows, setRows] = useState<JournalRow[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [q, setQ] = useState("");
  const [selected, setSelected] = useState<JournalRow | null>(null);

  // email export UI state
  const [email, setEmail] = useState("");
  const [selectedKeys, setSelectedKeys] = useState<Record<string, boolean>>({});
  const [sending, setSending] = useState(false);
  const [sendMsg, setSendMsg] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setErr(null);
    try {
      const res = await fetch("/api/journal?limit=300", { cache: "no-store" });
      if (!res.ok) throw new Error(`GET /api/journal failed: ${res.status}`);
      const data = (await res.json()) as JournalRow[];
      setRows(data);

      // keep selection if possible
      if (selected) {
        const still = data.find(
          (r) => r.payload_hash === selected.payload_hash && r.ts === selected.ts,
        );
        setSelected(still ?? null);
      }

      // prune selectedKeys when data refreshes
      setSelectedKeys((prev) => {
        const next: Record<string, boolean> = {};
        for (const r of data) {
          const k = rowKey(r);
          if (prev[k]) next[k] = true;
        }
        return next;
      });
    } catch (e: any) {
      setErr(e?.message ?? String(e));
      setRows([]);
      setSelected(null);
      setSelectedKeys({});
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter((r) => {
      const hay = [
        r.ts,
        r.actor,
        r.event_type,
        r.entity_type ?? "",
        r.entity_id ?? "",
        r.payload_hash,
        asText(r.payload?.payload?.message ?? r.payload?.message ?? ""),
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(needle);
    });
  }, [rows, q]);

  const selectedRows = useMemo(() => {
    const ks = selectedKeys;
    return filtered.filter((r) => ks[rowKey(r)]);
  }, [filtered, selectedKeys]);

  const allFilteredSelected =
    filtered.length > 0 && selectedRows.length === filtered.length;

  function toggleRow(r: JournalRow) {
    const k = rowKey(r);
    setSelectedKeys((prev) => ({ ...prev, [k]: !prev[k] }));
  }

  function toggleSelectAllFiltered() {
    if (filtered.length === 0) return;
    setSelectedKeys((prev) => {
      const next = { ...prev };
      const shouldSelect = !allFilteredSelected;
      for (const r of filtered) next[rowKey(r)] = shouldSelect;

      // if unselecting, delete keys to keep object small
      if (!shouldSelect) {
        for (const r of filtered) delete next[rowKey(r)];
      }
      return next;
    });
  }

  async function sendCsv() {
    setSendMsg(null);
    setErr(null);

    const rowsToSend = selectedRows.length > 0 ? selectedRows : filtered;

    if (!email.trim()) {
      setSendMsg("Please enter an email.");
      return;
    }
    if (rowsToSend.length === 0) {
      setSendMsg("No rows to export.");
      return;
    }

    setSending(true);
    try {
      const res = await fetch("/api/journal/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: email.trim(),
          filename: "signed_journal.csv",
          rows: rowsToSend,
        }),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        const e =
          typeof data?.error === "string"
            ? data.error
            : data?.error
              ? JSON.stringify(data.error)
              : `Send failed: ${res.status}`;
        throw new Error(e);
      }

      const idInfo =
        typeof data?.id === "string" && data.id ? ` (id: ${data.id})` : "";

      setSendMsg(`Sent ${rowsToSend.length} rows to ${email.trim()}.${idInfo}`);
    } catch (e: any) {
      setSendMsg(`Error: ${e?.message ?? String(e)}`);
    } finally {
      setSending(false);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Signed journal</h1>
          <div className="text-xs text-gray-500">
            Showing latest events written to <code>public.signed_journal</code>
          </div>
        </div>

        <button
          onClick={load}
          className="rounded border bg-white px-3 py-2 text-sm hover:bg-gray-50"
        >
          Refresh
        </button>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search (type, actor, entity, hash, message...)"
          className="w-full md:w-[520px] rounded border bg-white px-3 py-2 text-sm"
        />
        <div className="text-xs text-gray-500">
          Rows: <span className="font-medium text-gray-900">{filtered.length}</span>
          {rows.length !== filtered.length ? (
            <span className="text-gray-400"> / {rows.length}</span>
          ) : null}
        </div>
      </div>

      {/* email export bar */}
      <div className="flex flex-wrap items-center gap-2 rounded border bg-white p-3">
        <input
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="Email (e.g. you@example.com)"
          className="w-full md:w-[360px] rounded border bg-white px-3 py-2 text-sm"
        />

        <button
          onClick={toggleSelectAllFiltered}
          className="rounded border bg-white px-3 py-2 text-sm hover:bg-gray-50"
          disabled={filtered.length === 0}
          title="Select or unselect all filtered rows"
        >
          {allFilteredSelected ? "Unselect all filtered" : "Select all filtered"}
        </button>

        <button
          onClick={sendCsv}
          disabled={sending || filtered.length === 0}
          className="rounded bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-60"
          title="Send selected rows (or all filtered if nothing selected) as CSV to the provided email"
        >
          {sending ? "Sending..." : "Envoyer CSV"}
        </button>

        <div className="text-xs text-gray-500">
          Selected: <span className="font-medium text-gray-900">{selectedRows.length}</span>
          {selectedRows.length === 0 ? (
            <span className="text-gray-400"> (will send all filtered)</span>
          ) : null}
        </div>

        {sendMsg ? (
          <div className="w-full text-sm text-gray-700">{sendMsg}</div>
        ) : null}
      </div>

      {err && <div className="text-sm text-red-600">Error: {err}</div>}
      {loading && <div className="text-sm text-gray-600">Loading…</div>}

      <div className="grid grid-cols-1 lg:grid-cols-[1fr_420px] gap-4">
        <div className="overflow-auto rounded border bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-100 text-left">
              <tr>
                <th className="p-2 w-[42px]">
                  <input
                    type="checkbox"
                    checked={allFilteredSelected}
                    onChange={toggleSelectAllFiltered}
                    aria-label="Select all filtered"
                  />
                </th>
                <th className="p-2">ts</th>
                <th className="p-2">actor</th>
                <th className="p-2">event</th>
                <th className="p-2">entity</th>
                <th className="p-2">message</th>
                <th className="p-2">hash</th>
                <th className="p-2">sig</th>
              </tr>
            </thead>

            <tbody>
              {filtered.map((r, i) => {
                const ok = r.verified_runtime !== false;
                const msg =
                  r.payload?.payload?.message ??
                  r.payload?.payload?.payload?.message ??
                  r.payload?.message ??
                  "";

                const isInspectSelected =
                  selected?.payload_hash === r.payload_hash && selected?.ts === r.ts;

                const checked = !!selectedKeys[rowKey(r)];

                return (
                  <tr
                    key={`${r.ts}-${r.payload_hash}-${i}`}
                    className={`border-t ${isInspectSelected ? "bg-blue-50" : ""}`}
                    title="Click to view payload"
                    onClick={() => setSelected(r)}
                  >
                    <td className="p-2" onClick={(e) => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleRow(r)}
                        aria-label="Select row"
                      />
                    </td>
                    <td className="p-2 whitespace-nowrap">{new Date(r.ts).toISOString()}</td>
                    <td className="p-2">{r.actor}</td>
                    <td className="p-2">{r.event_type}</td>
                    <td className="p-2">
                      {r.entity_type ?? "-"}:{r.entity_id ?? "-"}
                    </td>
                    <td className="p-2 max-w-[340px] truncate text-gray-700">
                      {String(msg || "-")}
                    </td>
                    <td className="p-2 font-mono text-xs">{r.payload_hash.slice(0, 10)}…</td>
                    <td className="p-2">
                      {ok ? (
                        <span className="rounded bg-green-100 text-green-800 px-2 py-1 text-xs">
                          valid
                        </span>
                      ) : (
                        <span className="rounded bg-red-100 text-red-800 px-2 py-1 text-xs">
                          invalid
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}

              {filtered.length === 0 && !loading && (
                <tr>
                  <td className="p-4 text-gray-600" colSpan={8}>
                    No matching events.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="rounded border bg-white">
          <div className="border-b p-3 font-medium">Event payload</div>
          <div className="p-3">
            {!selected ? (
              <div className="text-sm text-gray-600">
                Click a row to inspect details.
              </div>
            ) : (
              <pre className="text-xs whitespace-pre-wrap break-words bg-gray-50 border rounded p-3 overflow-auto max-h-[70vh]">
                {JSON.stringify(selected.payload, null, 2)}
              </pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}