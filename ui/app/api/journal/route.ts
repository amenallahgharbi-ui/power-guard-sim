import { NextResponse } from "next/server";
import crypto from "crypto";
import { sbHeaders, sbUrl } from "@/lib/supabaseRest";

type JournalRow = {
  ts: string;
  actor: string;
  event_type: string;
  entity_type: string | null;
  entity_id: string | null;
  payload: any;
  payload_hash: string;
  public_key: string;
  signature: string;
  verified: boolean;
};

function verifyEd25519(hashHex: string, signatureB64: string, publicKeyPem: string) {
  return crypto.verify(
    null,
    Buffer.from(hashHex, "utf8"),
    publicKeyPem,
    Buffer.from(signatureB64, "base64")
  );
}

async function sbGet<T = any>(table: string, query: string): Promise<T[]> {
  const url = `${sbUrl(table)}?${query}`;
  const r = await fetch(url, { headers: sbHeaders(), cache: "no-store" });
  if (!r.ok) throw new Error(`GET ${url} -> ${r.status} ${await r.text()}`);
  return r.json();
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const limit = Number(searchParams.get("limit") ?? "200");

    const q =
      "select=ts,actor,event_type,entity_type,entity_id,payload,payload_hash,public_key,signature,verified" +
      "&order=ts.desc" +
      `&limit=${Math.min(1000, Math.max(1, limit))}`;

    const rows = await sbGet<JournalRow>("signed_journal", q);

    const enriched = rows.map((r) => ({
      ...r,
      verified_runtime:
        r.public_key === "db-chain-only"
          ? true
          : verifyEd25519(r.payload_hash, r.signature, r.public_key),
    }));

    return NextResponse.json(enriched);
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? String(e) }, { status: 500 });
  }
}