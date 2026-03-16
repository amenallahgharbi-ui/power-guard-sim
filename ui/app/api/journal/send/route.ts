import { NextResponse } from "next/server";
import { Resend } from "resend";

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

function mustEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is not set`);
  return v;
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function escapeCsv(value: any): string {
  const s =
    value == null
      ? ""
      : typeof value === "string"
        ? value
        : (() => {
            try {
              return JSON.stringify(value);
            } catch {
              return String(value);
            }
          })();

  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function rowsToCsv(rows: JournalRow[]): string {
  const header = [
    "ts",
    "actor",
    "event_type",
    "entity_type",
    "entity_id",
    "payload_hash",
    "verified",
    "verified_runtime",
    "payload_json",
  ];
  const lines = [header.join(",")];
  for (const r of rows) {
    lines.push(
      [
        escapeCsv(r.ts),
        escapeCsv(r.actor),
        escapeCsv(r.event_type),
        escapeCsv(r.entity_type),
        escapeCsv(r.entity_id),
        escapeCsv(r.payload_hash),
        escapeCsv(r.verified),
        escapeCsv(r.verified_runtime ?? ""),
        escapeCsv(r.payload),
      ].join(","),
    );
  }
  return lines.join("\n") + "\n";
}

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const email = String(body?.email ?? "").trim();
    const rows = body?.rows as JournalRow[] | undefined;
    const filename = String(body?.filename ?? "journal.csv");

    if (!email || !isValidEmail(email)) {
      return NextResponse.json({ ok: false, error: "Invalid email" }, { status: 400 });
    }
    if (!Array.isArray(rows) || rows.length === 0) {
      return NextResponse.json(
        { ok: false, error: "rows must be a non-empty array" },
        { status: 400 },
      );
    }
    if (rows.length > 2000) {
      return NextResponse.json(
        { ok: false, error: "Too many rows (max 2000)" },
        { status: 400 },
      );
    }

    const resend = new Resend(mustEnv("RESEND_API_KEY"));
    const from = mustEnv("RESEND_FROM");

    const csv = rowsToCsv(rows);
    const base64 = Buffer.from(csv, "utf8").toString("base64");

    const resp = await resend.emails.send({
      from,
      to: [email],
      subject: `Power-Guard: signed journal export (${rows.length} rows)`,
      text: `Attached: ${filename}\nRows: ${rows.length}\nGenerated at: ${new Date().toISOString()}`,
      attachments: [{ filename, content: base64 }],
    });

    if (resp.error) {
      const message =
        typeof (resp.error as any)?.message === "string"
          ? (resp.error as any).message
          : typeof resp.error === "string"
            ? resp.error
            : "Resend error";

      return NextResponse.json(
        {
          ok: false,
          provider: "resend",
          error: message,
          error_raw: resp.error,
          from,
          to: email,
        },
        { status: 502 },
      );
    }

    return NextResponse.json({
      ok: true,
      provider: "resend",
      sent: rows.length,
      id: resp.data?.id ?? null,
    });
  } catch (e: any) {
    return NextResponse.json(
      { ok: false, error: e?.message ?? String(e) },
      { status: 500 },
    );
  }
}