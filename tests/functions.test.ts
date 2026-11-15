// Unit tests. The SQL-level tests live in test/sql and run through DuckDB.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { join } from "node:path";
import {
  configFromSecrets,
  encodeQuery,
  normalizeBaseUrl,
  REGIONS,
  retryDelayMs,
  type SegmentConfig,
  segmentGet,
} from "../src/segment/client.ts";
import { flattenRuleSchema, typesOf } from "../src/segment/jsonschema.ts";
import { isSecretKey, maskedPaths, maskSecrets, REDACTED } from "../src/segment/mask.ts";
import { chunkWindow, resolveWindow } from "../src/segment/metrics.ts";
import { constraintsOf, toEpochMs } from "../src/segment/pushdown.ts";
import { getAll } from "../src/segment/table.ts";
import { runStatus } from "../src/segment/unified.ts";

const ROOT = join(import.meta.dir, "..");
const HOUR = 3_600_000;
const DAY = 24 * HOUR;

// ------------------------------------------------------------------ secrets

test("secret: token required; region picks the base URL; base_url wins", () => {
  expect(() => configFromSecrets({})).toThrow(/no segment secret/);
  expect(() => configFromSecrets({ s: { type: "segment" } })).toThrow(/TOKEN/);
  expect(configFromSecrets({ s: { type: "segment", token: "t" } }).baseUrl).toBe(REGIONS.us as string);
  expect(configFromSecrets({ s: { type: "segment", token: "t", region: "EU" } }).baseUrl).toBe(
    "https://eu1.api.segmentapis.com",
  );
  expect(() => configFromSecrets({ s: { type: "segment", token: "t", region: "apac" } })).toThrow(/REGION/);
  expect(configFromSecrets({ s: { type: "segment", token: "t", base_url: "http://127.0.0.1:9/" } }).baseUrl).toBe(
    "http://127.0.0.1:9",
  );
});

test("base_url: https only, except loopback", () => {
  expect(() => normalizeBaseUrl("http://api.segmentapis.com")).toThrow(/https/);
  expect(() => normalizeBaseUrl("not a url")).toThrow(/not a URL/);
  expect(normalizeBaseUrl("https://proxy.example.com/segment/")).toBe("https://proxy.example.com/segment");
});

// ------------------------------------------------------------- query + retries

test("encodeQuery: arrays use Segment's indexed form, empty values are dropped", () => {
  expect(encodeQuery({ groupBy: "eventName,source", sourceId: ["a", "b"], x: undefined, y: "" })).toBe(
    "groupBy=eventName%2Csource&sourceId.0=a&sourceId.1=b",
  );
});

test("retryDelayMs: Retry-After seconds or date, then X-RateLimit-Reset", () => {
  const now = 1_000_000_000_000;
  expect(retryDelayMs(new Headers({ "Retry-After": "2" }), now)).toBe(2000);
  expect(retryDelayMs(new Headers({ "Retry-After": new Date(now + 5000).toUTCString() }), now)).toBe(5000);
  expect(retryDelayMs(new Headers({ "X-RateLimit-Reset": "3" }), now)).toBe(3000);
  expect(retryDelayMs(new Headers({ "X-RateLimit-Reset": String(now / 1000 + 10) }), now)).toBe(10_250);
  expect(retryDelayMs(new Headers(), now)).toBeUndefined();
});

// --------------------------------------------------------------------- masking

test("mask: secret-looking keys, URL userinfo and known values are redacted", () => {
  const settings = {
    apiKey: "k1",
    endpoint: "north_america",
    nested: { password: "p", user: "u", privateKey: null, token: "" },
    webhook: "https://user:pw@hooks.example.com/x",
    note: "old key WRITEKEY123 rotated",
    headers: [{ Authorization: "Bearer x" }],
  };
  const masked = maskSecrets(settings, ["WRITEKEY123"]) as Record<string, unknown>;
  expect(masked).toEqual({
    apiKey: REDACTED,
    endpoint: "north_america",
    nested: { password: REDACTED, user: "u", privateKey: null, token: "" },
    webhook: `https://${REDACTED}@hooks.example.com/x`,
    note: `old key ${REDACTED} rotated`,
    headers: [{ Authorization: REDACTED }],
  });
  expect(maskedPaths(settings)).toEqual(["apiKey", "nested.password", "headers[].Authorization"]);
  expect(JSON.stringify(masked)).not.toContain("k1");
  for (const k of ["secretKey", "credentials", "client_secret", "serviceAccountKey", "sharedSecret"]) {
    expect(isSecretKey(k)).toBe(true);
  }
  for (const k of ["endpoint", "name", "measurementId", "database"]) expect(isSecretKey(k)).toBe(false);
});

// ----------------------------------------------------------------- JSON Schema

test("flattenRuleSchema: sections, nesting, arrays, required, enums", () => {
  const props = flattenRuleSchema({
    type: "object",
    properties: {
      properties: {
        type: "object",
        required: ["id"],
        properties: {
          id: { type: ["string"] },
          kind: { enum: ["a", 1, null] },
          items: {
            type: "array",
            items: { type: "object", required: ["sku"], properties: { sku: { type: "string" } } },
          },
          meta: { type: ["object", "null"], properties: { n: { type: "integer", minimum: 0 } } },
        },
      },
      traits: { type: "object", properties: { email: { type: "string", format: "email" } } },
    },
  });
  const by = Object.fromEntries(props.map((p) => [`${p.section}:${p.property_name}`, p]));
  expect(Object.keys(by).sort()).toEqual([
    "properties:id",
    "properties:items",
    "properties:items[].sku",
    "properties:kind",
    "properties:meta",
    "properties:meta.n",
    "traits:email",
  ]);
  expect(by["properties:id"]?.is_required).toBe(true);
  expect(by["properties:kind"]?.enum_values).toEqual(["a", "1"]);
  expect(by["properties:kind"]?.is_nullable).toBe(true);
  expect(by["properties:items"]?.item_type).toBe("object");
  expect(by["properties:items[].sku"]?.is_required).toBe(true);
  expect(by["properties:meta"]?.data_type).toBe("object");
  expect(by["properties:meta.n"]?.minimum).toBe(0);
  expect(by["traits:email"]?.format).toBe("email");
  expect(flattenRuleSchema(null)).toEqual([]);
  expect(typesOf({ anyOf: [{ type: "string" }, { type: "null" }] })).toEqual(["string", "null"]);
});

// --------------------------------------------------------------------- windows

test("windows: arguments, pushed bounds, defaults, retention clamp, chunking", () => {
  const anchor = Date.UTC(2026, 10, 15, 12, 30);
  const scan = (args: Record<string, unknown>, constraints = constraintsOf(undefined)) => ({
    args: { start_time: null, end_time: null, granularity: "DAY", ...args },
    constraints,
    anchorMs: anchor,
    projected: new Set<string>(),
  });
  // Explicit arguments win and are not aligned (TIMESTAMPTZ arrives in microseconds).
  const w = resolveWindow(scan({ start_time: BigInt(Date.UTC(2026, 10, 10, 5)) * 1000n, end_time: anchor * 1000 }));
  expect(w.start).toBe(Date.UTC(2026, 10, 10, 5));
  expect(w.end).toBe(anchor);
  // Default: 7 days back from now - 1 min, aligned down to a day.
  const d = resolveWindow(scan({}));
  expect(d.start).toBe(Date.UTC(2026, 10, 8));
  expect(d.end).toBe(anchor - 60_000);
  // A pushed lower bound older than retention is clamped.
  const c = resolveWindow(
    scan({}, [{ kind: "cmp", column: "time", op: "ge", value: BigInt(Date.UTC(2026, 6, 1)) * 1000n }]),
  );
  expect(c.start).toBeGreaterThanOrEqual(anchor - 30 * DAY);
  expect(c.start % DAY).toBe(0);
  // MINUTE windows split at 4 hours.
  expect(chunkWindow(0, 6 * HOUR, "MINUTE")).toEqual([
    [0, 4 * HOUR],
    [4 * HOUR, 6 * HOUR],
  ]);
  expect(chunkWindow(0, 45 * DAY, "DAY").length).toBe(2);
  expect(toEpochMs(1_700_000_000_000)).toBe(1_700_000_000_000);
  expect(toEpochMs(1_700_000_000_000_000)).toBe(1_700_000_000_000);
});

test("runStatus: Segment statuses map onto the shared run vocabulary", () => {
  expect(
    ["SUCCESS", "PARTIAL_SUCCESS", "FAIL", "FAILED", "IN_PROGRESS", "RUNNING", "PENDING", "CANCELLED"].map(runStatus),
  ).toEqual(["success", "failed", "failed", "failed", "running", "running", "queued", "cancelled"]);
});

// --------------------------------------------------------- against the replay server

describe("against the Segment replay server", () => {
  let mock: ReturnType<typeof Bun.spawn>;
  let url: string;
  const cfg = (token = "test-token"): SegmentConfig => ({ baseUrl: url, token });

  beforeAll(async () => {
    mock = Bun.spawn([process.env.PYTHON ?? "python3", join(ROOT, "test", "mock_api", "server.py")], {
      stdout: "pipe",
      stderr: "ignore",
    });
    const reader = (mock.stdout as ReadableStream<Uint8Array>).getReader();
    let text = "";
    while (!text.includes("\n")) {
      const { value, done } = await reader.read();
      if (done) break;
      text += new TextDecoder().decode(value);
    }
    reader.releaseLock();
    const port = text.match(/^PORT:(\d+)/)?.[1];
    if (!port) throw new Error(`replay server did not report a port: ${text}`);
    url = `http://127.0.0.1:${port}`;
  });

  afterAll(() => {
    mock?.kill();
  });

  test("getAll follows pagination.next cursors", async () => {
    const rows = await getAll(cfg(), { path: "/sources", items: "sources" });
    expect(rows.map((r) => r.id)).toEqual(["src_web", "src_ios", "src_server", "src_stripe", "src_retl"]);
    const log = (await (await fetch(`${url}/__requests?path=/sources`)).json()) as { query: string }[];
    expect(log.length).toBe(3);
  });

  test("a 429 is retried after Retry-After", async () => {
    const t0 = Date.now();
    const rows = await getAll(cfg(), { path: "/tracking-plans", items: "trackingPlans" });
    expect(rows.length).toBe(2);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(900);
  });

  test("errors carry the status, Segment's message and a hint", async () => {
    await expect(segmentGet(cfg("nope"), "/")).rejects.toThrow(/HTTP 401: unauthorized.*TOKEN and REGION/);
    await expect(
      segmentGet(cfg("starter-token"), "/spaces", { "pagination.count": "1" }, { feature: "Engage" }),
    ).rejects.toThrow(/HTTP 403: forbidden.*needs Engage/);
    await expect(segmentGet(cfg(), "/sources", { bogus: "1", "pagination.count": "1" })).rejects.toThrow(
      /HTTP 422.*unknown parameter/,
    );
  });
});
