/**
 * The `unified` schema: Segment rows in the shapes other workers share.
 *
 * - `runs`: warehouse syncs and Reverse ETL syncs in the orchestrator `unified.runs`
 *   shape (airflow, dagster, prefect, dbt …), orchestrator = 'segment'.
 * - `event_definitions`, `property_definitions`, `cohorts`: the product-analytics
 *   vocabulary shared with the Amplitude, Mixpanel and PostHog workers, built from
 *   LIVE tracking plans and Engage audiences.
 */

import type { TableDescriptor } from "@query-farm/vgi";
import { flattenRules, planRequests, retlFans, rulesReq, warehouseSyncChildReqs } from "./resources.ts";
import { asNum, asTime, C, type Col, defineSegmentCatalogTable, type Req, type Row } from "./table.ts";

const U = "segment.unified";
const UNIFIED = ["unified"];
const enc = encodeURIComponent;

/** Map a Segment sync status onto the shared run status set. */
export function runStatus(raw: unknown): string {
  const s = String(raw ?? "").toUpperCase();
  if (/SUCCESS|SUCCEEDED|COMPLETED|^OK$/.test(s) && !/PARTIAL/.test(s)) return "success";
  if (/RUNNING|IN_PROGRESS|STARTED|SYNCING|PROCESSING/.test(s)) return "running";
  if (/QUEUED|PENDING|SCHEDULED|WAITING/.test(s)) return "queued";
  if (/CANCEL|ABORT/.test(s)) return "cancelled";
  if (/SKIP/.test(s)) return "skipped";
  return "failed";
}

const isWarehouse = (r: Row) => r.__ctx?.kind === "warehouse";

const runColumns: Col[] = [
  C.lit("orchestrator", "Always 'segment'.", "segment"),
  C.str(
    "run_id",
    "Reverse ETL sync id; for warehouse syncs (which have no id) 'warehouse:<warehouse_id>:<source_id>:<start>'.",
    (r) => (isWarehouse(r) ? `warehouse:${r.__ctx?.warehouse_id}:${r.sourceId}:${r.start}` : r.syncId),
    false,
  ),
  C.str(
    "pipeline",
    "'warehouse/<warehouse_id>/<source_id>' for warehouse syncs, 'reverse_etl/<model_id>/<subscription_id>' for Reverse ETL.",
    (r) =>
      isWarehouse(r)
        ? `warehouse/${r.__ctx?.warehouse_id}/${r.sourceId}`
        : `reverse_etl/${r.modelId ?? r.__ctx?.model_id}/${r.__ctx?.subscription_id}`,
  ),
  C.str(
    "status",
    "queued, running, success, failed, cancelled or skipped.",
    (r) => runStatus(isWarehouse(r) ? r.status : r.syncStatus),
    false,
  ),
  C.str("raw_status", "Segment's own status string.", (r) => (isWarehouse(r) ? r.status : r.syncStatus)),
  C.str(
    "trigger",
    "'schedule' for warehouse syncs; 'schedule' or 'manual' for Reverse ETL (from the mapping's schedule).",
    (r) => (isWarehouse(r) ? "schedule" : (r.__ctx?.trigger ?? null)),
  ),
  C.time("queued_at", "Not reported by Segment (NULL).", () => null),
  C.time("started_at", "When the run started.", (r) => (isWarehouse(r) ? r.start : r.startedAt)),
  C.time("ended_at", "When the run finished (NULL while running).", (r) => (isWarehouse(r) ? r.end : r.finishedAt)),
  C.num("duration_seconds", "Run duration in seconds.", (r) => {
    if (isWarehouse(r)) return asNum(r.duration);
    const s = asTime(r.startedAt);
    const e = asTime(r.finishedAt);
    return s !== null && e !== null ? Number(e - s) / 1e6 : null;
  }),
  C.str("url", "Not provided (NULL).", () => null),
];

export const runs: TableDescriptor = defineSegmentCatalogTable(
  {
    name: "runs",
    description: "Warehouse syncs and Reverse ETL syncs in the cross-orchestrator unified.runs shape",
    title: "Runs (Unified)",
    category: "unified",
    keywords: "segment, runs, unified, orchestrator, warehouse syncs, reverse etl syncs, pipeline runs",
    docLlm:
      "Segment's data movement runs in the shared orchestrator shape (UNION ALL with airflow/dagster/prefect/" +
      "dbt workers' unified.runs): orchestrator = 'segment', run_id, pipeline ('warehouse/<warehouse_id>/" +
      "<source_id>' for warehouse syncs, 'reverse_etl/<model_id>/<subscription_id>' for Reverse ETL), status " +
      "(queued, running, success, failed, cancelled, skipped), raw_status, trigger, queued_at (NULL), " +
      "started_at, ended_at, duration_seconds, url (NULL). Reads every warehouse's syncs and every Reverse ETL " +
      "mapping's syncs; native detail is in main.warehouse_syncs and main.reverse_etl_syncs.",
    docMd: "## unified.runs\n\nWarehouse and Reverse ETL syncs in the cross-orchestrator runs shape.",
    columns: runColumns,
    requests: () => [
      { path: "/warehouses", items: "warehouses", fan: "wh" },
      { path: "/destinations", items: "destinations", fan: "retl_dest" },
    ],
    fans: {
      wh: (row, _ctx, scan) =>
        warehouseSyncChildReqs(scan, row.id, "").map((r) => ({ ...r, ctx: { ...r.ctx, kind: "warehouse" } })),
      ...retlFans(),
    },
    examples: [
      {
        sql: `SELECT orchestrator, status, count(*) AS runs FROM ${U}.runs GROUP BY ALL ORDER BY status`,
        description: "Segment run outcomes in the shared shape",
      },
    ],
  },
  UNIFIED,
  [],
);

const tool = C.lit("tool", "The analytics tool the row came from: 'segment'.", "segment");

export const eventDefinitions: TableDescriptor = defineSegmentCatalogTable(
  {
    name: "event_definitions",
    description: "Planned events from LIVE tracking plans in the shared product-analytics shape",
    title: "Event Definitions (Shared Shape)",
    category: "unified",
    keywords: "product analytics, event definitions, tracking plan, unified, segment, protocols",
    docLlm:
      "Every TRACK, PAGE and SCREEN rule of the LIVE tracking plans in the shared product-analytics shape " +
      "(UNION ALL with amplitude/mixpanel/posthog unified.event_definitions): tool = 'segment', event_name, " +
      "display_name (NULL), description (the rule schema's description), category (the rule type in lower case: " +
      "'track', 'page' or 'screen'), is_hidden (NULL), is_active (false when the rule is deprecated), volume " +
      "(NULL; see main.event_volume) and tags (the tracking plan name). One row per plan that defines the event.",
    docMd: "## unified.event_definitions\n\nPlanned events in the shared product-analytics shape.",
    columns: [
      tool,
      C.str("event_name", "Event name.", (r) => r.key, false),
      C.str("display_name", "Display name (Segment has none; NULL).", () => null),
      C.str("description", "Description from the rule's JSON Schema.", (r) => r.jsonSchema?.description ?? null),
      C.str("category", "Rule type in lower case: track, page or screen.", (r) => String(r.type).toLowerCase()),
      C.bool("is_hidden", "Not tracked by Segment (NULL).", () => null),
      C.bool("is_active", "False when the rule is deprecated.", (r) => !r.deprecatedAt),
      C.big("volume", "Recent volume (NULL; use main.event_volume).", () => null),
      C.strList("tags", "The tracking plan(s) defining the event.", (r) =>
        r.__ctx?.tracking_plan_name ? [r.__ctx.tracking_plan_name] : null,
      ),
    ],
    requests: (scan) => planRequests(scan, "tracking_plan_id", "rules", "LIVE"),
    fans: { rules: (row) => [rulesReq(row)] },
    rows: (items) => items.filter((r) => ["TRACK", "PAGE", "SCREEN"].includes(r.type) && r.key),
    examples: [
      {
        sql: `SELECT event_name, description FROM ${U}.event_definitions WHERE is_active ORDER BY event_name`,
        description: "Active planned events",
      },
    ],
  },
  UNIFIED,
  [],
);

const SCOPE: Record<string, string> = {
  TRACK: "event",
  PAGE: "event",
  SCREEN: "event",
  IDENTIFY: "user",
  GROUP: "group",
};

export const propertyDefinitions: TableDescriptor = defineSegmentCatalogTable(
  {
    name: "property_definitions",
    description: "Planned event properties and user/group traits in the shared product-analytics shape",
    title: "Property Definitions (Shared Shape)",
    category: "unified",
    keywords: "product analytics, property definitions, traits, tracking plan, unified, segment, protocols",
    docLlm:
      "Every property of the LIVE tracking plans in the shared product-analytics shape (UNION ALL with " +
      "amplitude/mixpanel/posthog unified.property_definitions): tool = 'segment', property_name (dotted path " +
      "for nested), scope ('event' for track/page/screen properties, 'user' for identify traits, 'group' for " +
      "group traits), event_name (event scope only), data_type, description, is_array, is_required, is_hidden " +
      "(NULL), enum_values. Context fields and COMMON rules are left out. WHERE scope = 'event' skips nothing " +
      "upstream (all rules are read per plan); tracking.properties has the full detail.",
    docMd: "## unified.property_definitions\n\nPlanned properties and traits in the shared shape.",
    columns: [
      tool,
      C.str("property_name", "Property or trait name (dotted path for nested).", "property_name", false),
      C.str("scope", "'event', 'user' or 'group'.", (r) => SCOPE[r.rule_type] ?? null, false),
      C.str("event_name", "Event the property belongs to (event scope).", (r) =>
        SCOPE[r.rule_type] === "event" ? r.event_name : null,
      ),
      C.str("data_type", "JSON Schema type(s), joined by '|'.", "data_type"),
      C.str("description", "Description.", "description"),
      C.bool("is_array", "Array-valued.", "is_array"),
      C.bool("is_required", "Required.", "is_required"),
      C.bool("is_hidden", "Not tracked by Segment (NULL).", () => null),
      C.strList("enum_values", "Allowed values.", "enum_values"),
    ],
    requests: (scan) => planRequests(scan, "tracking_plan_id", "rules", "LIVE"),
    fans: { rules: (row) => [rulesReq(row)] },
    rows: (items) =>
      flattenRules(items).filter((p) => SCOPE[p.rule_type as string] && ["properties", "traits"].includes(p.section)),
    examples: [
      {
        sql: `SELECT scope, count(*) AS properties FROM ${U}.property_definitions GROUP BY scope ORDER BY scope`,
        description: "Planned properties per scope",
      },
    ],
  },
  UNIFIED,
  [],
);

export const cohorts: TableDescriptor = defineSegmentCatalogTable(
  {
    name: "cohorts",
    description: "Engage audiences in the shared product-analytics cohorts shape",
    title: "Cohorts (Shared Shape)",
    category: "unified",
    keywords: "product analytics, cohorts, audiences, engage, unified, segment",
    docLlm:
      "Engage audiences in the shared product-analytics cohorts shape (UNION ALL with the other analytics " +
      "workers' unified.cohorts): tool = 'segment', id, name, description, size, created_at, updated_at, " +
      "is_archived (NULL: Segment audiences are enabled or disabled, not archived) and definition (JSON: " +
      "{query, targetEntity, audienceType, computeCadence}). Reads every space; space-level detail is " +
      "in engage.audiences. Needs Engage / Unify.",
    docMd: "## unified.cohorts\n\nEngage audiences as cohorts.",
    feature: "Engage / Unify (Profiles spaces)",
    columns: [
      tool,
      C.str("id", "Audience id.", "id", false),
      C.str("name", "Audience name.", "name"),
      C.str("description", "Description.", "description"),
      C.big("size", "Profiles in the audience (count).", "size.count"),
      C.time("created_at", "When created.", "createdAt"),
      C.time("updated_at", "When last changed.", "updatedAt"),
      C.bool("is_archived", "Not applicable to Segment (NULL).", () => null),
      C.json("definition", "{query, targetEntity, audienceType, computeCadence}.", (r) => ({
        ...(r.definition ?? {}),
        audienceType: r.audienceType ?? null,
        computeCadence: r.computeCadence?.type ?? null,
      })),
    ],
    requests: () => [{ path: "/spaces", items: "spaces", fan: "aud" }],
    fans: {
      aud: (row): Req[] => [
        {
          path: `/spaces/${enc(row.id)}/audiences`,
          items: "audiences",
          ctx: { space_id: row.id },
          notFoundEmpty: true,
        },
      ],
    },
    examples: [
      { sql: `SELECT name, size FROM ${U}.cohorts ORDER BY size DESC NULLS LAST`, description: "Audiences by size" },
    ],
  },
  UNIFIED,
  ["id"],
);

export const UNIFIED_TABLES: TableDescriptor[] = [runs, eventDefinitions, propertyDefinitions, cohorts];
