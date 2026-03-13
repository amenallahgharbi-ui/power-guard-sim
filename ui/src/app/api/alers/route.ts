import { NextResponse } from "next/server";
import { sbHeaders, sbUrl } from "@/lib/supabaseRest";

type AlertRow = {
  asset_id: string;
  ts_min: string;
  alert_type: string;
  severity: string;
  metric_value: number | null;
  threshold: number | null;
  status: string | null;
  created_at: string | null;
  message: string | null;
};

async function sbGet<T = any>(table: string, query: string): Promise<T[]> {
  const url = `${sbUrl(table)}?${query}`;
  const r = await fetch(url, { headers: sbHeaders(), cache: "no-store" });
  if (!r.ok) throw new Error(`GET ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

export async function GET() {
  try {
    // Get asset list from baselines (most reliable in this repo)
    const baselines = await sbGet<{ asset_id: string }>(
      "calibration_baseline",
      "select=asset_id&order=asset_id.asc&limit=20000"
    );

    const assetIds = Array.from(
      new Set(baselines.map((b) => String(b.asset_id).trim()).filter(Boolean))
    ).sort();

    // Fetch last N alerts per asset (avoids PostgREST max-rows truncation)
    const PER_ASSET = 500;

    const results = await Promise.all(
      assetIds.map(async (assetId) => {
        const q =
          "select=asset_id,ts_min,alert_type,severity,metric_value,threshold,status,created_at,message" +
          `&asset_id=eq.${encodeURIComponent(assetId)}` +
          "&order=ts_min.desc" +
          `&limit=${PER_ASSET}`;

        return sbGet<AlertRow>("alerts", q);
      })
    );

    const alerts = results
      .flat()
      .sort((a, b) => (a.ts_min < b.ts_min ? 1 : a.ts_min > b.ts_min ? -1 : 0));

    return NextResponse.json(alerts);
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? String(e) }, { status: 500 });
  }
}