/**
 * One factory for every Segment Public API list endpoint.
 *
 * A spec names its columns (with a JSON extractor each) and the requests a scan
 * makes. A request either yields rows, or — when it names a `fan` — turns each of
 * its rows into further requests (sources → their connected destinations,
 * destinations → subscriptions → Reverse ETL syncs). The factory turns that into a
 * VGI table function that:
 *
 * - streams one Segment page per Arrow batch;
 * - keeps its work queue and the opaque `pagination.next` cursor in serializable
 *   state, so a scan resumes on any process over HTTP and survives rate-limit pauses;
 * - reads the `segment` secret on every call and never stores it in state;
 * - pushes filters down (narrowing only) and lets the SDK re-apply them exactly.
 */

import {
  batchFromColumns,
  bool,
  constraintRecord,
  defineTableFunction,
  field,
  float64,
  formatPushedFilters,
  int32,
  int64,
  list,
  schema,
  type TableDescriptor,
  timestampMicros,
  utf8,
  type VgiDataType,
  type VgiFunction,
} from "@query-farm/vgi";
import { log } from "../log.ts";
import { type Category, column, objectTags } from "../meta.ts";
import { configFromSecrets, type Query, SECRET_TYPE, type SegmentConfig, SegmentError, segmentGet } from "./client.ts";
import { type Constraint, constraintsOf } from "./pushdown.ts";

// ------------------------------------------------------------------ column helpers

export interface Col {
  name: string;
  type: VgiDataType;
  /** DuckDB spelling for vgi.result_columns_schema. */
  sqlType: string;
  doc: string;
  nullable: boolean;
  get: (row: Row) => unknown;
}

// biome-ignore lint/suspicious/noExplicitAny: Segment JSON is untyped.
export type Row = Record<string, any> & { __ctx?: Record<string, string> };

/** Walk a dotted path through untyped JSON. */
export function at(row: unknown, path: string): unknown {
  let cur: unknown = row;
  for (const p of path.split(".")) {
    if (cur == null || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[p];
  }
  return cur;
}

export const asStr = (v: unknown): string | null =>
  v == null ? null : typeof v === "string" ? v : typeof v === "object" ? JSON.stringify(v) : String(v);
export const asBool = (v: unknown): boolean | null => (typeof v === "boolean" ? v : null);
export const asInt = (v: unknown): number | null =>
  typeof v === "number" && Number.isFinite(v) ? Math.trunc(v) : null;
export const asBig = (v: unknown): bigint | null => {
  if (typeof v === "bigint") return v;
  if (typeof v === "number" && Number.isFinite(v)) return BigInt(Math.trunc(v));
  if (typeof v === "string" && /^-?\d+$/.test(v.trim())) return BigInt(v.trim());
  return null;
};
export const asNum = (v: unknown): number | null => {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "" && Number.isFinite(Number(v))) return Number(v);
  return null;
};
/** ISO 8601 → TIMESTAMPTZ microseconds. */
export function asTime(v: unknown): bigint | null {
  if (typeof v !== "string" || v === "") return null;
  const ms = Date.parse(v);
  return Number.isNaN(ms) ? null : BigInt(ms) * 1000n;
}
export const asStrList = (v: unknown): string[] | null =>
  Array.isArray(v) ? v.filter((x) => x != null).map((x) => (typeof x === "string" ? x : JSON.stringify(x))) : null;
export const asJson = (v: unknown): string | null => (v == null ? null : JSON.stringify(v));

const JSON_META: [string, string] = ["ARROW:extension:name", "arrow.json"];

type Getter = string | ((r: Row) => unknown);
const getter = (g: Getter) => (typeof g === "string" ? (r: Row) => at(r, g) : g);

function mk(name: string, type: VgiDataType, sqlType: string, doc: string, get: (r: Row) => unknown, nullable = true) {
  return { name, type, sqlType, doc, get, nullable } as Col;
}

export const C = {
  str: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, utf8(), "VARCHAR", doc, (r) => asStr(getter(g)(r)), nullable),
  time: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, timestampMicros("UTC"), "TIMESTAMP WITH TIME ZONE", doc, (r) => asTime(getter(g)(r)), nullable),
  bool: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, bool, "BOOLEAN", doc, (r) => asBool(getter(g)(r)), nullable),
  int: (name: string, doc: string, g: Getter) => mk(name, int32, "INTEGER", doc, (r) => asInt(getter(g)(r))),
  big: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, int64(), "BIGINT", doc, (r) => asBig(getter(g)(r)), nullable),
  num: (name: string, doc: string, g: Getter) => mk(name, float64(), "DOUBLE", doc, (r) => asNum(getter(g)(r))),
  strList: (name: string, doc: string, g: Getter) =>
    mk(name, list(utf8()), "VARCHAR[]", doc, (r) => asStrList(getter(g)(r))),
  json: (name: string, doc: string, g: Getter) => mk(name, utf8(), "JSON", doc, (r) => asJson(getter(g)(r))),
  /** A constant value (e.g. `tool = 'segment'`). */
  lit: (name: string, doc: string, value: string) => mk(name, utf8(), "VARCHAR", doc, () => value, false),
  /** A value from the request context (e.g. the parent's id). */
  ctx: (name: string, doc: string, key: string, nullable = false) =>
    mk(name, utf8(), "VARCHAR", doc, (r) => r.__ctx?.[key] ?? null, nullable),
  custom: mk,
};

// --------------------------------------------------------------------- the factory

/** One request of a scan (serialized into the scan state, so plain data only). */
export interface Req {
  path: string;
  query?: Query;
  /** Key of `data` holding the array (or the single object) of items. */
  items: string;
  /** `pagination` = pagination.count/cursor (default), `plain` = count/cursor, `none` = one response. */
  paging?: "pagination" | "plain" | "none";
  pageSize?: number;
  /** Values exposed to columns (`C.ctx`) and to fan-out children. */
  ctx?: Record<string, string>;
  /** Name of a spec `fans` entry: turn this request's rows into child requests instead of emitting them. */
  fan?: string;
  /** A 404 answers "no such object": zero rows instead of an error. */
  notFoundEmpty?: boolean;
  /** Plan-gated feature named in 403/404 errors. */
  feature?: string;
}

export interface ScanContext<A> {
  args: A;
  constraints: Constraint[];
  /** Wall-clock anchor fixed when the scan started (epoch ms), stable across resumes. */
  anchorMs: number;
  /** Output columns the query actually reads. */
  projected: Set<string>;
}

export interface FnExample {
  sql: string;
  description: string;
}

export interface SegmentSpec<A> {
  name: string;
  description: string;
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
  pushdown?: string;
  args?: Record<string, VgiDataType>;
  argDocs?: Record<string, string>;
  argDefaults?: Record<string, unknown>;
  argConstraints?: Record<string, Record<string, unknown>>;
  columns: Col[];
  /** The first requests of a scan, computed once from the arguments and pushed filters. */
  requests: (scan: ScanContext<A>) => Req[];
  /** Fan-out handlers: rows of a parent request → child requests. */
  fans?: Record<string, (row: Row, ctx: Record<string, string>, scan: ScanContext<A>) => Req[]>;
  /** Reshape a page's items into output rows (flatten series, schema properties …). */
  rows?: (items: Row[], req: Req, scan: ScanContext<A>) => Row[];
  /** Per-row enrichment with extra calls (only for columns the query reads). */
  expand?: (cfg: SegmentConfig, rows: Row[], scan: ScanContext<A>) => Promise<Row[]>;
  /** Default plan-gated feature for this spec's requests. */
  feature?: string;
  examples: FnExample[];
}

interface ScanState {
  /** JSON array of pending requests; the head is being read. */
  queue: string;
  /** Opaque Segment cursor for the head request ("" = first page). */
  cursor: string;
  started: boolean;
  anchorMs: number;
}

export const DEFAULT_PAGE_SIZE = 200;

function itemsOf(data: Record<string, unknown>, key: string): Row[] {
  const v = data[key];
  if (Array.isArray(v)) return v.filter((x) => x && typeof x === "object") as Row[];
  if (v && typeof v === "object") return [v as Row];
  return [];
}

/** The Arrow output schema of a column list (column comments ride along as field metadata). */
export function schemaOf(columns: Col[]) {
  return schema(
    columns.map((c) =>
      c.sqlType === "JSON"
        ? field(c.name, c.type, c.nullable, new Map([["comment", c.doc], JSON_META]))
        : column(c.name, c.type, c.doc, c.nullable),
    ),
  );
}

/** Discovery tags shared by a spec's function or table. */
function tagsOf<A>(spec: SegmentSpec<A>): Record<string, string> {
  return {
    domain: "analytics",
    ...objectTags({
      title: spec.title,
      category: spec.category,
      keywords: spec.keywords,
      docLlm: spec.docLlm,
      docMd: spec.docMd + (spec.pushdown ? `\n\n**Pushdown:** ${spec.pushdown}` : ""),
    }),
    "vgi.result_columns_schema": JSON.stringify(
      spec.columns.map((c) => ({ name: c.name, type: c.sqlType, description: c.doc })),
    ),
  };
}

/** Fetch one page of a request: its items and the next cursor (or null when done). */
export async function fetchPage(
  cfg: SegmentConfig,
  req: Req,
  cursor: string,
  defaultFeature?: string,
): Promise<{ items: Row[]; next: string | null }> {
  const paging = req.paging ?? "pagination";
  const size = String(req.pageSize ?? DEFAULT_PAGE_SIZE);
  const query: Query = { ...req.query };
  if (paging === "pagination") {
    query["pagination.count"] = size;
    if (cursor) query["pagination.cursor"] = cursor;
  } else if (paging === "plain") {
    query.count = size;
    if (cursor) query.cursor = cursor;
  }
  let data: Record<string, unknown>;
  try {
    data = await segmentGet(cfg, req.path, query, { feature: req.feature ?? defaultFeature });
  } catch (err) {
    if (req.notFoundEmpty && err instanceof SegmentError && err.status === 404) return { items: [], next: null };
    throw err;
  }
  const next = paging === "none" ? null : asStr(at(data, "pagination.next"));
  return { items: itemsOf(data, req.items), next: next && next !== cursor ? next : null };
}

/** Read every page of a request (used by expansions; bounded). */
export async function getAll(cfg: SegmentConfig, req: Req): Promise<Row[]> {
  const rows: Row[] = [];
  let cursor = "";
  for (let guard = 0; guard < 1000; guard++) {
    const page = await fetchPage(cfg, req, cursor);
    rows.push(...page.items);
    if (!page.next) break;
    cursor = page.next;
  }
  return rows;
}

export function defineSegmentTable<A extends Record<string, unknown>>(spec: SegmentSpec<A>): VgiFunction {
  const outSchema = schemaOf(spec.columns);
  const byName = new Map(spec.columns.map((c) => [c.name, c]));

  return defineTableFunction<A, ScanState>({
    name: spec.name,
    description: spec.description,
    args: spec.args ?? {},
    argDocs: spec.argDocs,
    argDefaults: spec.argDefaults,
    argConstraints: spec.argConstraints as never,
    filterPushdown: true,
    projectionPushdown: true,
    // DuckDB drops pushed predicates from its plan; the SDK re-applies them exactly,
    // so translating a predicate only ever narrows what Segment returns.
    autoApplyFilters: true,
    onBind: (params) =>
      params.resolvedSecretsProvided
        ? { outputSchema: outSchema }
        : { outputSchema: outSchema, lookupSecretTypes: [SECRET_TYPE] },
    initialState: () => ({ queue: "[]", cursor: "", started: false, anchorMs: Date.now() }),
    process: async (params, state, out) => {
      const cfg = configFromSecrets(params.secrets);
      const scan: ScanContext<A> = {
        args: params.args,
        constraints: constraintsOf(params.pushdownFilters),
        anchorMs: state.anchorMs,
        projected: new Set(params.outputSchema.fields.map((f) => f.name)),
      };
      log.debug(`${spec.name}: pushed filters ${formatPushedFilters(params.pushdownFilters)}`);
      let queue: Req[] = JSON.parse(state.queue);
      if (!state.started) {
        queue = spec.requests(scan);
        state.started = true;
      }

      // Loop until a page yields rows or the queue drains, so no empty batches go out.
      for (;;) {
        let head = queue[0];
        if (!head) {
          state.queue = "[]";
          out.finish();
          return;
        }
        if (head.ctx?.__cursor !== undefined) {
          // A parent parked while its children ran resumes from its saved cursor.
          const { __cursor, ...clean } = head.ctx;
          state.cursor = __cursor;
          head = { ...head, ctx: clean };
          queue[0] = head;
        }
        const page = await fetchPage(cfg, head, state.cursor, spec.feature);
        const ctx = head.ctx ?? {};
        const rest = queue.slice(1);
        const more = page.next !== null;
        state.cursor = page.next ?? "";

        if (head.fan) {
          const fan = spec.fans?.[head.fan];
          if (!fan) throw new SegmentError(`internal: unknown fan-out '${head.fan}'`);
          const children = page.items.flatMap((row) => fan(row, ctx, scan));
          if (!more) queue = [...children, ...rest];
          else if (!children.length) queue = [head, ...rest];
          else {
            // Depth-first: this page's children run first; the parent waits with its cursor.
            queue = [...children, { ...head, ctx: { ...ctx, __cursor: state.cursor } }, ...rest];
            state.cursor = "";
          }
          state.queue = JSON.stringify(queue);
          continue;
        }

        if (!more) queue = rest;
        state.queue = JSON.stringify(queue);

        let rows = spec.rows ? spec.rows(page.items, head, scan) : page.items;
        rows = rows.map((r) => ({ ...r, __ctx: { ...ctx, ...(r.__ctx ?? {}) } }));
        if (spec.expand && rows.length) rows = await spec.expand(cfg, rows, scan);
        if (rows.length === 0) continue;
        const cols: Record<string, unknown[]> = {};
        for (const f of params.outputSchema.fields) {
          const c = byName.get(f.name);
          cols[f.name] = c ? rows.map((r) => c.get(r)) : rows.map(() => null);
        }
        out.emit(batchFromColumns(cols, params.outputSchema));
        return;
      }
    },
    tags: tagsOf(spec),
    examples: spec.examples,
  });
}

/**
 * A spec with no arguments exposed as a catalog table (`SELECT … FROM segment.main.sources`).
 * The scan keeps every capability (pushdown, paging, secrets) and is table-only.
 */
export function defineSegmentCatalogTable(
  spec: SegmentSpec<Record<string, never>>,
  schemaPath: string[],
  primaryKey: string[],
): TableDescriptor {
  const fn = defineSegmentTable({ ...spec, name: `${schemaPath.join("_")}_${spec.name}_scan` });
  const record = (id: string, kind: "NOT_NULL" | "PRIMARY_KEY", columns: string[]) =>
    constraintRecord({
      schemaPath,
      table: spec.name,
      id,
      kind,
      columns: columns.map((c) => [c]),
      enforcement: "ENFORCED",
      validation: "ALL_ROWS",
    });
  const notNull = spec.columns.filter((c) => !c.nullable).map((c) => c.name);
  return {
    name: spec.name,
    comment: spec.description,
    columns: schemaOf(spec.columns),
    function: fn,
    constraints: [
      ...notNull.map((c) => record(`${c}_not_null`, "NOT_NULL", [c])),
      ...(primaryKey.length ? [record("pk", "PRIMARY_KEY", primaryKey)] : []),
    ],
    tags: { ...tagsOf(spec), "vgi.example_queries": JSON.stringify(spec.examples) },
  };
}
