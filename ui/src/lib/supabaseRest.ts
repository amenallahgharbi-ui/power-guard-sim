export function mustEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

export function sbHeaders() {
  const key = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
  return {
    apikey: key,
    Authorization: `Bearer ${key}`,
    "Content-Type": "application/json",
  };
}

export function sbUrl(path: string) {
  const base = mustEnv("SUPABASE_URL").replace(/\/$/, "");
  return `${base}/rest/v1/${path}`;
}