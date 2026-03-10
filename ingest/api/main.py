import json
import os
from pathlib import Path
from typing import Any, Dict, List, Union

import requests
from fastapi import FastAPI, Header, HTTPException, Request
from jsonschema import Draft202012Validator

app = FastAPI(title="power-guard-sim ingest api", version="0.1.0")

# Load schema once
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "generators" / "schemas" / "telemetry_schema.json"
_schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
_validator = Draft202012Validator(_schema)


def require_token(x_api_token: str | None) -> None:
    expected = os.environ.get("INGEST_API_TOKEN", "")
    if not expected:
        raise RuntimeError("INGEST_API_TOKEN is not set")
    if not x_api_token or x_api_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def validate_msg(msg: Dict[str, Any]) -> None:
    errors = list(_validator.iter_errors(msg))
    if errors:
        # Return first error for simplicity
        e0 = errors[0]
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Schema validation failed",
                "error": e0.message,
                "path": list(e0.path),
            },
        )


def supabase_insert(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    supabase_anon_key = os.environ.get("SUPABASE_ANON_KEY", "")
    if not supabase_url or not supabase_anon_key:
        raise RuntimeError("https://jwecytazbhavfdpefdxo.supabase.co / eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp3ZWN5dGF6YmhhdmZkcGVmZHhvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE1MDI3NzQsImV4cCI6MjA4NzA3ODc3NH0.anxeKYJyVG6uEPiT_sfsxq3fPppLd87IhprdVUvsL3I")

    endpoint = f"{supabase_url}/rest/v1/telemetry"
    headers = {
        "apikey": supabase_anon_key,
        "Authorization": f"Bearer {supabase_anon_key}",
        "Content-Type": "application/json",
        # Ask PostgREST to return minimal response
        "Prefer": "return=minimal",
    }

    # Map incoming schema fields to DB columns
    payload = []
    for m in rows:
        payload.append(
            {
                "ts": m["ts"],
                "asset_id": m["asset_id"],
                "voltage_v": m["voltage_v"],
                "current_a": m["current_a"],
                "temperature_c": m["temperature_c"],
                "soc_pct": m["soc_pct"],
                "profile": m["profile"],
                "sig": m.get("sig"),
            }
        )

    r = requests.post(endpoint, headers=headers, data=json.dumps(payload), timeout=30)
    if r.status_code not in (200, 201, 204):
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Supabase insert failed",
                "status_code": r.status_code,
                "response": r.text,
            },
        )

    return {"inserted": len(payload)}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
async def ingest(
    request: Request,
    x_api_token: Union[str, None] = Header(default=None, alias="X-API-Token"),
):
    require_token(x_api_token)

    body = await request.json()

    # Accept either a single object or a list
    if isinstance(body, dict):
        rows = [body]
    elif isinstance(body, list):
        rows = body
    else:
        raise HTTPException(status_code=400, detail="Body must be an object or list")

    if not rows:
        raise HTTPException(status_code=400, detail="Empty payload")

    # Validate each
    for m in rows:
        if not isinstance(m, dict):
            raise HTTPException(status_code=400, detail="Each item must be an object")
        validate_msg(m)

    # Insert into Supabase
    return supabase_insert(rows)