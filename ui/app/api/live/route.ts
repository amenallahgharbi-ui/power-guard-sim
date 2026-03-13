import { NextResponse } from "next/server";
import { sbHeaders, sbUrl } from "@/lib/supabaseRest";

type Telemetry1mRow = {
  asset_id: string;
  ts_min: string;
  voltage_v_avg: number | null;
  current_a_avg: number | null;
  temperature_c_avg: number | null;
  soc_pct_avg: number | null;
  n_samples: number | null;
};

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
    const telemetry = await sbGet<Telemetry1mRow>(
      "telemetry_1m",
      "select=asset_id,ts_min,voltage_v_avg,current_a_avg,temperature_c_avg,soc_pct_avg,n_samples&order=ts_min.desc&limit=2000"
    );

    const alerts = await sbGet<AlertRow>(
      "alerts",
      "select=asset_id,ts_min,alert_type,severity,metric_value,threshold,status,created_at,message&order=ts_min.desc&limit=2000"
    );

    return NextResponse.json({ telemetry, alerts });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? String(e) }, { status: 500 });
  }
}