import { NextResponse } from "next/server";
import { sbHeaders, sbUrl } from "@/lib/supabaseRest";

async function sbGet<T = any>(table: string, query: string): Promise<T[]> {
  const url = `${sbUrl(table)}?${query}`;
  const r = await fetch(url, { headers: sbHeaders(), cache: "no-store" });
  if (!r.ok) throw new Error(`GET ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const assetId = (searchParams.get("asset_id") ?? "").trim();
    const start = (searchParams.get("start") ?? "").trim(); // ISO
    const end = (searchParams.get("end") ?? "").trim(); // ISO

    if (!assetId || !start || !end) {
      return NextResponse.json(
        { error: "Missing asset_id/start/end query params" },
        { status: 400 }
      );
    }

    const telemetryQ =
      "select=asset_id,ts_min,voltage_v_avg,current_a_avg,temperature_c_avg,soc_pct_avg" +
      `&asset_id=eq.${encodeURIComponent(assetId)}` +
      `&ts_min=gte.${encodeURIComponent(start)}` +
      `&ts_min=lte.${encodeURIComponent(end)}` +
      "&order=ts_min.asc" +
      "&limit=20000";

    const alertsQ =
      "select=asset_id,ts_min,alert_type,severity,metric_value,threshold,status,message" +
      `&asset_id=eq.${encodeURIComponent(assetId)}` +
      `&ts_min=gte.${encodeURIComponent(start)}` +
      `&ts_min=lte.${encodeURIComponent(end)}` +
      "&order=ts_min.asc" +
      "&limit=20000";

    const [telemetry, alerts] = await Promise.all([
      sbGet("telemetry_1m", telemetryQ),
      sbGet("alerts", alertsQ),
    ]);

    return NextResponse.json({ telemetry, alerts });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? String(e) }, { status: 500 });
  }
}