import crypto from "crypto";
import fs from "fs";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

function readKey(kind) {
  const pathVar =
    kind === "private" ? "JOURNAL_ED25519_PRIVATE_KEY_PATH" : "JOURNAL_ED25519_PUBLIC_KEY_PATH";
  const pemVar =
    kind === "private" ? "JOURNAL_ED25519_PRIVATE_KEY_PEM" : "JOURNAL_ED25519_PUBLIC_KEY_PEM";

  const p = process.env[pathVar];
  if (p) return fs.readFileSync(p, "utf8");

  return mustEnv(pemVar);
}

function sha256Hex(input) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

function signEd25519(payload) {
  const privateKeyPem = readKey("private");
  const publicKeyPem = readKey("public");

  const canonical = JSON.stringify(payload);
  const hash = sha256Hex(canonical);

  const signature = crypto.sign(null, Buffer.from(hash, "utf8"), privateKeyPem).toString("base64");
  const verified = crypto.verify(
    null,
    Buffer.from(hash, "utf8"),
    publicKeyPem,
    Buffer.from(signature, "base64")
  );

  return { hash, signature, publicKeyPem, verified };
}

async function sbInsert(table, rows) {
  const SUPABASE_URL = mustEnv("SUPABASE_URL").replace(/\/$/, "");
  const KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");

  const url = `${SUPABASE_URL}/rest/v1/${table}`;
  const r = await fetch(url, {
    method: "POST",
    headers: {
      apikey: KEY,
      Authorization: `Bearer ${KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(rows),
  });

  if (!r.ok) throw new Error(`POST ${url} -> ${r.status} ${await r.text()}`);
}

export async function journalAlertsCreated(alertRows, max = 2000) {
  const rows = alertRows.slice(0, max).map((a) => {
    const event = {
      ts: new Date().toISOString(),
      actor: "alert-engine",
      event_type: "alert_created",
      entity_type: "alert",
      entity_id: `${a.asset_id}:${a.ts_min}:${a.alert_type}`,
      payload: a,
    };

    const { hash, signature, publicKeyPem, verified } = signEd25519(event);

    return {
      actor: event.actor,
      event_type: event.event_type,
      entity_type: event.entity_type,
      entity_id: event.entity_id,
      payload: event,
      payload_hash: hash,
      public_key: publicKeyPem,
      signature,
      verified,
    };
  });

  if (rows.length) await sbInsert("signed_journal", rows);
}