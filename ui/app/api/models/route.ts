import { NextResponse } from "next/server";
import { sbHeaders, sbUrl } from "@/lib/supabaseRest";

type ModelMetricRow = {
  asset_id: string;
  model_name: string;
  target: string;
  horizon_min: number;
  rmse: number;
  mae: number;
  trained_at: string;
};

async function sbGet<T = any>(table: string, query: string): Promise<T[]> {
  const url = `${sbUrl(table)}?${query}`;
  const r = await fetch(url, { headers: sbHeaders(), cache: "no-store" });
  if (!r.ok) throw new Error(`GET ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

export async function GET() {
  try {
    const q =
      "select=asset_id,model_name,target,horizon_min,rmse,mae,trained_at" +
      "&order=trained_at.desc,asset_id.asc,model_name.asc" +
      "&limit=5000";
    const rows = await sbGet<ModelMetricRow>("model_metrics", q);
    return NextResponse.json(rows);
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? String(e) }, { status: 500 });
  }
}