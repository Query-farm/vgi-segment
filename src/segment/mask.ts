/**
 * Mask secret-ish values in Segment configuration objects before they reach SQL.
 *
 * Destination, warehouse, source and subscription `settings` are open key/value
 * maps that hold API keys, passwords, service-account JSON and connection strings.
 * Every value whose key looks secret is replaced with `REDACTED` (nulls and empty
 * strings stay as they are, so "is it configured?" is still answerable), URLs lose
 * their userinfo, and any known secret (a source's write keys) is scrubbed wherever
 * it appears. Source write keys themselves are never returned by any column.
 */

export const REDACTED = "<redacted>";

/** Key names whose values are treated as secrets (case-insensitive, matched anywhere in the key). */
const SECRET_KEY =
  /(pass(word|phrase)?|secret|token|api_?key|apikey|private_?key|privatekey|access_?key|credential|auth|signing|cert|pem|jwt|dsn|connection_?string|write_?key|writekey|sas|hmac|salt|^key$|_key$|Key$)/i;

/** Does this key name hold a secret? */
export function isSecretKey(key: string): boolean {
  return SECRET_KEY.test(key);
}

/** Strip `user:password@` from URL-shaped strings (JDBC/Postgres/Redis DSNs, webhooks). */
function scrubUrl(s: string): string {
  return s.replace(/\b([a-z][a-z0-9+.-]*:\/\/)[^\s/@:]+(:[^\s/@]*)?@/gi, `$1${REDACTED}@`);
}

/** Return a deep copy of `value` with secret-ish entries masked; `known` values are scrubbed anywhere. */
export function maskSecrets(value: unknown, known: string[] = []): unknown {
  const secrets = known.filter((k) => typeof k === "string" && k.length >= 6);
  const walk = (v: unknown, keyIsSecret: boolean): unknown => {
    if (v == null) return v;
    if (typeof v === "string") {
      if (v === "") return v;
      if (keyIsSecret) return REDACTED;
      let out = scrubUrl(v);
      for (const s of secrets) out = out.split(s).join(REDACTED);
      return out;
    }
    if (typeof v === "number" || typeof v === "boolean") return keyIsSecret ? REDACTED : v;
    if (Array.isArray(v)) return v.map((x) => walk(x, keyIsSecret));
    if (typeof v === "object") {
      const out: Record<string, unknown> = {};
      for (const [k, x] of Object.entries(v as Record<string, unknown>))
        out[k] = walk(x, keyIsSecret || isSecretKey(k));
      return out;
    }
    return v;
  };
  return walk(value, false);
}

/** The dotted paths of the keys that were masked (so an audit can list them without values). */
export function maskedPaths(value: unknown, prefix = ""): string[] {
  const out: string[] = [];
  if (Array.isArray(value)) {
    for (const x of value) for (const p of maskedPaths(x, `${prefix}[]`)) if (!out.includes(p)) out.push(p);
  } else if (value && typeof value === "object") {
    for (const [k, x] of Object.entries(value as Record<string, unknown>)) {
      const p = prefix ? `${prefix}.${k}` : k;
      if (isSecretKey(k) && x != null && x !== "") out.push(p);
      else out.push(...maskedPaths(x, p));
    }
  }
  return out;
}
