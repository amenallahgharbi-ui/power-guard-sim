import { NextResponse } from "next/server";
import { sbHeaders, sbUrl } from "@/lib/supabaseRest";

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

async function sbGet<T = any>(table: string, query: string): Promise<T[]> {
  const url = `${sbUrl(table)}?${query}`;
  const r = await fetch(url, { headers: sbHeaders(), cache: "no-store" });
  if (!r.ok) throw new Error(`GET ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

export async function GET() {
  try {
    // latest days first
    const q =
      "select=asset_id,day,expected_samples,received_samples,completeness_pct,raw_rows,duplicates_raw,lag_p50_s,lag_p95_s,updated_at" +
      "&order=day.desc,asset_id.asc" +
      "&limit=5000";

    const rows = await sbGet<DqRow>("dq_metrics_1d", q);
    return NextResponse.json(rows);
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? String(e) }, { status: 500 });
  }
}