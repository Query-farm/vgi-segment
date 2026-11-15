/**
 * Segment Public API client: authentication, regions, rate limits and cursor paging.
 *
 * - **Auth.** A `segment` DuckDB secret carries a Public API token (workspace
 *   Settings > Access Management > Tokens), sent as `Authorization: Bearer <token>`.
 *   The token never enters table-function state or an error message.
 * - **Region.** `REGION 'us'` (default) → https://api.segmentapis.com, `'eu'` →
 *   https://eu1.api.segmentapis.com. `BASE_URL` overrides both (a proxy, or the
 *   offline replay server in the tests); http:// is accepted only for loopback.
 * - **Rate limits.** The Public API answers 429 with `Retry-After` (seconds) and/or
 *   `X-RateLimit-Reset`; the client waits (bounded) and retries. 5xx and network
 *   errors retry with exponential backoff.
 * - **Paging.** List endpoints take `pagination.count` + `pagination.cursor` and
 *   answer `data.pagination.next`. Callers keep that opaque cursor in their scan
 *   state, so a long scan resumes on any process.
 */

import { secretOfType } from "@query-farm/vgi";
import { log } from "../log.ts";
import { VERSION } from "../meta.ts";

export const SECRET_TYPE = "segment";
export const TIMEOUT_MS = 30_000;
export const MAX_ATTEMPTS = 5;
/** Longest single wait for a rate-limit reset. */
export const MAX_RATE_WAIT_MS = 65_000;
const USER_AGENT = `vgi-segment-worker/${VERSION}`;
const ACCEPT =
  "application/vnd.segment.v1+json, application/vnd.segment.v1beta+json, application/vnd.segment.v1alpha+json";

export const REGIONS: Record<string, string> = {
  us: "https://api.segmentapis.com",
  eu: "https://eu1.api.segmentapis.com",
};

export class SegmentError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
  }
}

export interface SegmentConfig {
  baseUrl: string;
  token: string;
}

const LOOPBACK = new Set(["127.0.0.1", "localhost", "[::1]", "::1"]);

/** Normalize and check a base URL: https, or http only for a loopback fixture server. */
export function normalizeBaseUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new SegmentError(
      `segment secret: base_url '${raw}' is not a URL (expected e.g. https://api.segmentapis.com)`,
    );
  }
  if (url.protocol !== "https:" && !(url.protocol === "http:" && LOOPBACK.has(url.hostname))) {
    throw new SegmentError(
      "segment secret: base_url must use https:// (http:// is accepted only for 127.0.0.1/localhost)",
    );
  }
  return url.origin + url.pathname.replace(/\/+$/, "");
}

function str(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : undefined;
}

/** Build the client config from the `segment` secret DuckDB resolved for this call. */
export function configFromSecrets(secrets: Record<string, Record<string, unknown>>): SegmentConfig {
  const s = secretOfType(secrets as never, SECRET_TYPE) as Record<string, unknown> | undefined;
  if (!s) {
    throw new SegmentError(
      "no segment secret: run CREATE SECRET (TYPE segment, TOKEN '<public api token>') " +
        "(add REGION 'eu' for an EU workspace)",
    );
  }
  const token = str(s.token);
  if (!token) throw new SegmentError("segment secret: TOKEN (a Segment Public API token) is required");
  const region = (str(s.region) ?? "us").toLowerCase();
  const base = str(s.base_url);
  if (!base && !REGIONS[region]) {
    throw new SegmentError(`segment secret: REGION must be 'us' or 'eu' (got '${region}')`);
  }
  return { token, baseUrl: normalizeBaseUrl(base ?? (REGIONS[region] as string)) };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * How long to wait after a 429: `Retry-After` (seconds or an HTTP date), else
 * `X-RateLimit-Reset` / `RateLimit-Reset` (epoch seconds when large, else seconds to wait).
 */
export function retryDelayMs(headers: Headers, now = Date.now()): number | undefined {
  const retry = headers.get("Retry-After");
  if (retry) {
    if (/^\d+(\.\d+)?$/.test(retry)) return Math.ceil(Number(retry) * 1000);
    const at = Date.parse(retry);
    if (!Number.isNaN(at)) return Math.max(0, at - now);
  }
  const reset = headers.get("X-RateLimit-Reset") ?? headers.get("RateLimit-Reset");
  if (reset && /^\d+$/.test(reset)) {
    const n = Number(reset);
    return n > 1_000_000_000 ? Math.max(0, n * 1000 - now) + 250 : n * 1000;
  }
  return undefined;
}

export type Query = Record<string, string | string[] | undefined>;

/** Encode a query: arrays become Segment's indexed form (`groupBy.0=…&groupBy.1=…`). */
export function encodeQuery(query: Query | undefined): string {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v === undefined) continue;
    if (Array.isArray(v)) {
      for (const [i, x] of v.entries()) qs.append(`${k}.${i}`, x);
    } else if (v !== "") qs.set(k, v);
  }
  return qs.toString();
}

/** What a plan-gated feature needs, for 403/404 messages (set per endpoint). */
export interface GetOptions {
  feature?: string;
}

/**
 * GET one Segment Public API resource and return its `data` object.
 * `path` is an API path (`/sources`); the token is never put in a URL.
 */
export async function segmentGet(
  cfg: SegmentConfig,
  path: string,
  query?: Query,
  opts: GetOptions = {},
): Promise<Record<string, unknown>> {
  const q = encodeQuery(query);
  const url = `${cfg.baseUrl}${path}${q ? `?${q}` : ""}`;
  for (let attempt = 1; ; attempt++) {
    let res: Response;
    try {
      res = await fetch(url, {
        headers: { Accept: ACCEPT, Authorization: `Bearer ${cfg.token}`, "User-Agent": USER_AGENT },
        signal: AbortSignal.timeout(TIMEOUT_MS),
      });
    } catch (err) {
      if (attempt >= MAX_ATTEMPTS) throw new SegmentError(`GET ${path} failed: ${(err as Error).message}`);
      await sleep(250 * 2 ** attempt);
      continue;
    }
    if (res.ok) {
      const text = await res.text();
      const body = text ? (JSON.parse(text) as { data?: Record<string, unknown> }) : {};
      return body.data ?? {};
    }
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt >= MAX_ATTEMPTS) {
      const detail = await errorSummary(res);
      let hint = "";
      if (res.status === 401) hint = " (check the segment secret's TOKEN and REGION)";
      else if ((res.status === 403 || res.status === 404) && opts.feature) {
        hint = ` (this endpoint needs ${opts.feature} on the Segment plan, and a token whose role can read it)`;
      } else if (res.status === 403) hint = " (the token's role cannot read this resource)";
      throw new SegmentError(
        `Segment GET ${path}${q ? `?${q}` : ""} failed with HTTP ${res.status}: ${detail}${hint}`,
        res.status,
      );
    }
    await res.body?.cancel();
    const delay =
      res.status === 429 ? Math.min(retryDelayMs(res.headers) ?? 1000 * attempt, MAX_RATE_WAIT_MS) : 250 * 2 ** attempt;
    log.debug(`segment: HTTP ${res.status} for ${path}; retry ${attempt} in ${delay} ms`);
    await sleep(delay);
  }
}

/** Segment errors are `{errors: [{type, message, field?}]}`; keep the message short. */
async function errorSummary(res: Response): Promise<string> {
  const text = await res.text();
  try {
    const j = JSON.parse(text) as { errors?: { type?: string; message?: string; field?: string }[] };
    const parts = (j.errors ?? []).map((e) => [e.type, e.field, e.message].filter(Boolean).join(": "));
    return parts.join("; ") || text.slice(0, 300);
  } catch {
    return text.slice(0, 300);
  }
}
