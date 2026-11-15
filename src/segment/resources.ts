/**
 * The Segment control-plane tables: one spec per Public API list endpoint.
 *
 * Schemas: `main` (workspace, sources, destinations, connections, warehouses and
 * their syncs, functions, Reverse ETL, labels, audit trail), `tracking` (tracking
 * plans, rules, flattened rule properties), `engage` (spaces, audiences, computed
 * traits) and `iam` (users, groups, roles). Write keys are never returned and every
 * `settings` map passes through `maskSecrets`.
 */

import { bool, int32, type TableDescriptor, utf8, type VgiFunction } from "@query-farm/vgi";
import { segmentGet } from "./client.ts";
import { flattenRuleSchema, ruleDescription } from "./jsonschema.ts";
import { maskedPaths, maskSecrets } from "./mask.ts";
import { eqValues, timeBounds } from "./pushdown.ts";
import {
  asStr,
  C,
  type Col,
  defineSegmentCatalogTable,
  defineSegmentTable,
  type Req,
  type Row,
  type ScanContext,
} from "./table.ts";

const enc = encodeURIComponent;
const MAIN = ["main"];
const TRACKING = ["tracking"];
const ENGAGE = ["engage"];
const IAM = ["iam"];
const M = "segment.main";
const T = "segment.tracking";
const E = "segment.engage";
const I = "segment.iam";

const FEATURE_RETL = "Reverse ETL";
const FEATURE_ENGAGE = "Engage / Unify (Profiles spaces)";
const FEATURE_FUNCTIONS = "Functions";
const FEATURE_AUDIT = "the Audit Trail (Business tier)";
const FEATURE_PROTOCOLS = "Protocols (tracking plans)";

/** `key:value` strings for Segment labels. */
const labelList = (v: unknown): string[] | null => (Array.isArray(v) ? v.map((l: Row) => `${l.key}:${l.value}`) : null);

/** Masked settings + the list of masked keys, shared by sources/destinations/warehouses. */
function settingsColumns(knownSecrets: (r: Row) => string[] = () => []): Col[] {
  return [
    C.json(
      "settings",
      "Configuration as JSON with every secret-looking value (API keys, passwords, tokens, credentials, private keys, DSN passwords) replaced by '<redacted>'.",
      (r) => (r.settings == null ? null : maskSecrets(r.settings, knownSecrets(r))),
    ),
    C.strList("masked_settings", "Dotted paths of the settings keys whose values were redacted.", (r) =>
      maskedPaths(r.settings ?? {}),
    ),
  ];
}

/** Ids pushed as `col = 'x'` / `col IN (…)`, or a named argument. */
function idsFor<A>(scan: ScanContext<A>, column: string, arg?: string): string[] | undefined {
  const a = arg ? (scan.args as Record<string, unknown>)[arg] : undefined;
  if (typeof a === "string" && a !== "") return [a];
  return eqValues(scan.constraints, column);
}

// ===================================================================== workspace

const workspace = defineSegmentCatalogTable(
  {
    name: "workspaces",
    description: "The Segment workspace the token belongs to (id, slug, name)",
    title: "Segment Workspace",
    category: "workspace",
    keywords: "segment, workspace, slug, tenant, account",
    docLlm:
      "One row for the workspace the segment secret's Public API token belongs to (GET /): id, slug and name. " +
      "Use it to confirm which workspace you are connected to; the slug forms app URLs " +
      "(https://app.segment.com/<slug>/...).",
    docMd: "## workspaces\n\nThe workspace the token belongs to: `id`, `slug`, `name`.",
    columns: [
      C.str("id", "Workspace id.", "id", false),
      C.str("slug", "Workspace slug (used in app.segment.com URLs).", "slug"),
      C.str("name", "Workspace display name.", "name"),
    ],
    requests: () => [{ path: "/", items: "workspace", paging: "none" }],
    examples: [{ sql: `SELECT slug, name FROM ${M}.workspaces`, description: "Which workspace the token reads" }],
  },
  MAIN,
  ["id"],
);

// ======================================================================= sources

function sourceColumns(): Col[] {
  return [
    C.str("id", "Source id.", "id", false),
    C.str("slug", "Source slug (unique within the workspace).", "slug"),
    C.str("name", "Source display name.", "name"),
    C.str("workspace_id", "Id of the workspace the source belongs to (joins workspaces.id).", "workspaceId"),
    C.bool("enabled", "True when the source accepts data.", "enabled"),
    C.str(
      "source_type",
      "Catalog slug of the source type, e.g. 'javascript', 'ios', 'http-api', 'salesforce'.",
      "metadata.slug",
    ),
    C.str("source_type_name", "Catalog name of the source type, e.g. 'Javascript'.", "metadata.name"),
    C.strList("categories", "Catalog categories of the source type, e.g. ['Website'].", "metadata.categories"),
    C.bool(
      "is_cloud_event_source",
      "True for cloud-app event sources (e.g. Stripe webhooks) rather than libraries.",
      "metadata.isCloudEventSource",
    ),
    C.custom(
      "write_key_count",
      int32,
      "INTEGER",
      "Number of write keys the source has (count). The write keys themselves are never returned.",
      (r) => (Array.isArray(r.writeKeys) ? r.writeKeys.length : null),
    ),
    C.strList("labels", "Labels as 'key:value' strings, e.g. ['environment:prod'].", (r) => labelList(r.labels)),
    ...settingsColumns((r) => (Array.isArray(r.writeKeys) ? r.writeKeys : [])),
  ];
}

const sources = defineSegmentCatalogTable(
  {
    name: "sources",
    description: "Every source in the workspace: type, enabled, labels and masked settings (never write keys)",
    title: "Segment Sources",
    category: "pipeline",
    keywords: "segment, sources, write keys, javascript, http api, cloud sources, labels, data collection",
    docLlm:
      "Every Segment source (GET /sources): id, slug, name, enabled, source_type (catalog slug such as " +
      "'javascript', 'http-api', 'ios'), source_type_name, categories, is_cloud_event_source, labels as " +
      "'key:value' strings, write_key_count and settings JSON with secret-looking values redacted. Write keys " +
      "are never returned. WHERE id = '…' (or IN) reads only those sources. Join to connections, " +
      "destinations (source_id) or tracking.plan_sources.",
    docMd: "## sources\n\nEvery source with its type, labels and masked settings. Write keys are never returned.",
    pushdown: "`id` (=, IN) → GET /sources/{id}.",
    columns: sourceColumns(),
    requests: (scan) => {
      const ids = idsFor(scan, "id");
      return ids
        ? ids.map((id) => ({ path: `/sources/${enc(id)}`, items: "source", paging: "none", notFoundEmpty: true }))
        : [{ path: "/sources", items: "sources" }];
    },
    examples: [
      {
        sql: `SELECT source_type, count(*) AS sources, count(*) FILTER (WHERE enabled) AS enabled FROM ${M}.sources GROUP BY source_type ORDER BY sources DESC`,
        description: "Sources per type, and how many are enabled",
      },
      {
        sql: `SELECT name, labels FROM ${M}.sources WHERE list_contains(labels, 'environment:prod') ORDER BY name`,
        description: "Production-labelled sources",
      },
    ],
  },
  MAIN,
  ["id"],
);

const sourceSchemaSettings = defineSegmentCatalogTable(
  {
    name: "source_schema_settings",
    description: "Per-source Protocols schema settings: unplanned events/traits, violation handling, forwarding",
    title: "Protocols Schema Controls",
    category: "pipeline",
    keywords: "segment, protocols, schema settings, unplanned events, violations, blocking, forwarding",
    docLlm:
      "One row per source with its Protocols schema controls (GET /sources/{id}/settings): whether unplanned " +
      "track events, unplanned event properties and unplanned identify/group traits are allowed, whether events " +
      "or properties with violations are allowed, the common-event violation policy, and the source ids that " +
      "violations and blocked events are forwarded to. WHERE source_id = '…' reads one source; otherwise every " +
      "source is listed first (one call each).",
    docMd: "## source_schema_settings\n\nProtocols schema controls per source (blocking and forwarding).",
    pushdown: "`source_id` (=, IN) reads only those sources.",
    feature: FEATURE_PROTOCOLS,
    columns: [
      C.ctx("source_id", "Source whose schema controls these are (joins sources.id).", "source_id"),
      C.ctx("source_name", "Source name (NULL when the source id was given directly).", "source_name", true),
      C.bool(
        "track_allow_unplanned_events",
        "Allow track events not in the tracking plan.",
        "track.allowUnplannedEvents",
      ),
      C.bool(
        "track_allow_unplanned_event_properties",
        "Allow track event properties not in the plan.",
        "track.allowUnplannedEventProperties",
      ),
      C.bool(
        "track_allow_event_on_violations",
        "Allow track events that have violations.",
        "track.allowEventOnViolations",
      ),
      C.bool(
        "track_allow_properties_on_violations",
        "Keep properties that have violations.",
        "track.allowPropertiesOnViolations",
      ),
      C.str(
        "track_common_event_on_violations",
        "Common track event policy on violations: ALLOW, BLOCK or OMIT_PROPERTIES.",
        "track.commonEventOnViolations",
      ),
      C.bool(
        "identify_allow_unplanned_traits",
        "Allow identify traits not in the plan.",
        "identify.allowUnplannedTraits",
      ),
      C.bool(
        "identify_allow_traits_on_violations",
        "Keep identify traits that have violations.",
        "identify.allowTraitsOnViolations",
      ),
      C.str(
        "identify_common_event_on_violations",
        "Identify policy on violations: ALLOW, BLOCK or OMIT_TRAITS.",
        "identify.commonEventOnViolations",
      ),
      C.bool("group_allow_unplanned_traits", "Allow group traits not in the plan.", "group.allowUnplannedTraits"),
      C.bool(
        "group_allow_traits_on_violations",
        "Keep group traits that have violations.",
        "group.allowTraitsOnViolations",
      ),
      C.str(
        "group_common_event_on_violations",
        "Group policy on violations: ALLOW, BLOCK or OMIT_TRAITS.",
        "group.commonEventOnViolations",
      ),
      C.str("forwarding_violations_to", "Source id that receives violation events.", "forwardingViolationsTo"),
      C.str("forwarding_blocked_events_to", "Source id that receives blocked events.", "forwardingBlockedEventsTo"),
    ],
    requests: (scan) => {
      const ids = idsFor(scan, "source_id");
      return ids ? ids.map((id) => settingsReq(id, {})) : [{ path: "/sources", items: "sources", fan: "settings" }];
    },
    fans: { settings: (row) => [settingsReq(row.id, { source_name: asStr(row.name) ?? "" })] },
    examples: [
      {
        sql: `SELECT source_id, source_name FROM ${M}.source_schema_settings WHERE NOT track_allow_unplanned_events ORDER BY source_id`,
        description: "Sources that block unplanned track events",
      },
    ],
  },
  MAIN,
  ["source_id"],
);

function settingsReq(id: string, ctx: Record<string, string>): Req {
  const c: Record<string, string> = { source_id: id, ...ctx };
  if (!c.source_name) delete c.source_name;
  return { path: `/sources/${enc(id)}/settings`, items: "settings", paging: "none", ctx: c, notFoundEmpty: true };
}

// ================================================================== destinations

function destinationColumns(): Col[] {
  return [
    C.str("id", "Destination instance id.", "id", false),
    C.str("name", "Destination instance name.", "name"),
    C.bool("enabled", "True when the destination receives data.", "enabled"),
    C.str("source_id", "Id of the source that feeds this destination instance.", "sourceId"),
    C.str(
      "destination_type",
      "Catalog slug of the destination, e.g. 'google-analytics-4', 'amplitude', 'webhooks'.",
      "metadata.slug",
    ),
    C.str("destination_type_name", "Catalog name of the destination.", "metadata.name"),
    C.str("destination_metadata_id", "Catalog id of the destination type.", "metadata.id"),
    C.strList("categories", "Catalog categories, e.g. ['Analytics'].", "metadata.categories"),
    C.bool(
      "is_actions",
      "True for an Actions destination (has action subscriptions; see destination_subscriptions).",
      (r) => (Array.isArray(r.metadata?.actions) ? r.metadata.actions.length > 0 : null),
    ),
    ...settingsColumns(),
  ];
}

const destinations = defineSegmentCatalogTable(
  {
    name: "destinations",
    description: "Every destination instance: type, enabled, feeding source and masked settings",
    title: "Segment Destinations",
    category: "pipeline",
    keywords: "segment, destinations, integrations, actions, webhooks, amplitude, google analytics, settings",
    docLlm:
      "Every destination instance (GET /destinations): id, name, enabled, source_id (the source feeding it), " +
      "destination_type (catalog slug such as 'amplitude', 'webhooks', 'google-analytics-4'), " +
      "destination_type_name, categories, is_actions and settings JSON with API keys, secrets and passwords " +
      "redacted (masked_settings lists which). WHERE id = '…' reads one destination; WHERE source_id = '…' " +
      "reads that source's connected destinations only.",
    docMd: "## destinations\n\nEvery destination instance with its type and masked settings.",
    pushdown: "`id` (=, IN) → GET /destinations/{id}; `source_id` (=, IN) → GET /sources/{id}/connected-destinations.",
    columns: destinationColumns(),
    requests: (scan) => {
      const ids = idsFor(scan, "id");
      if (ids) {
        return ids.map((id) => ({
          path: `/destinations/${enc(id)}`,
          items: "destination",
          paging: "none",
          notFoundEmpty: true,
        }));
      }
      const srcs = idsFor(scan, "source_id");
      if (srcs) {
        return srcs.map((id) => ({
          path: `/sources/${enc(id)}/connected-destinations`,
          items: "destinations",
          notFoundEmpty: true,
        }));
      }
      return [{ path: "/destinations", items: "destinations" }];
    },
    examples: [
      {
        sql: `SELECT destination_type, count(*) AS instances FROM ${M}.destinations WHERE enabled GROUP BY destination_type ORDER BY instances DESC`,
        description: "Enabled destination instances per type",
      },
      {
        sql: `SELECT name, masked_settings FROM ${M}.destinations ORDER BY name`,
        description: "Which settings of each destination hold secrets (values are redacted)",
      },
    ],
  },
  MAIN,
  ["id"],
);

const destinationSubscriptions = defineSegmentCatalogTable(
  {
    name: "destination_subscriptions",
    description: "Actions destination subscriptions (mappings): action, trigger, enabled, Reverse ETL model",
    title: "Actions Mappings",
    category: "pipeline",
    keywords: "segment, actions, subscriptions, mappings, trigger, reverse etl, destination actions",
    docLlm:
      "Every Actions destination subscription (mapping), GET /destinations/{id}/subscriptions: destination_id, " +
      "id, name, action_slug, action_id, enabled, trigger (the FQL condition such as 'type = \"track\"'), " +
      "model_id (set for Reverse ETL mappings), schedule_strategy and schedule_config (Reverse ETL), and settings " +
      "(the field mapping) with secret-looking values redacted. WHERE destination_id = '…' reads one destination; " +
      "otherwise every destination is listed (classic destinations have no subscriptions).",
    docMd: "## destination_subscriptions\n\nActions mappings per destination, including Reverse ETL schedules.",
    pushdown: "`destination_id` (=, IN) reads only those destinations.",
    columns: [
      C.ctx("destination_id", "Destination instance id.", "destination_id"),
      C.str("id", "Subscription id.", "id", false),
      C.str("name", "Subscription (mapping) name.", "name"),
      C.str("action_slug", "Action slug, e.g. 'trackEvent'.", "actionSlug"),
      C.str("action_id", "Catalog id of the destination action the mapping runs.", "actionId"),
      C.bool("enabled", "True when the mapping is active.", "enabled"),
      C.str("trigger", "FQL trigger condition, e.g. 'type = \"track\"'.", "trigger"),
      C.str("model_id", "Reverse ETL model id for Reverse ETL mappings, else NULL.", "modelId"),
      C.str(
        "schedule_strategy",
        "Reverse ETL schedule: CRON, DBT_CLOUD, MANUAL, PERIODIC or SPECIFIC_DAYS.",
        "reverseETLSchedule.strategy",
      ),
      C.json(
        "schedule_config",
        "Reverse ETL schedule configuration (interval, days, hours, cron spec).",
        "reverseETLSchedule.config",
      ),
      C.json("settings", "The field mapping as JSON, secret-looking values redacted.", (r) =>
        r.settings == null ? null : maskSecrets(r.settings),
      ),
    ],
    requests: (scan) => {
      const ids = idsFor(scan, "destination_id");
      return ids
        ? ids.map((id) => subscriptionsReq(id))
        : [{ path: "/destinations", items: "destinations", fan: "subs" }];
    },
    fans: { subs: (row) => [subscriptionsReq(row.id)] },
    examples: [
      {
        sql: `SELECT destination_id, name, trigger FROM ${M}.destination_subscriptions WHERE enabled ORDER BY destination_id, name`,
        description: "Active Actions mappings and their triggers",
      },
    ],
  },
  MAIN,
  ["destination_id", "id"],
);

function subscriptionsReq(destinationId: string): Req {
  return {
    path: `/destinations/${enc(destinationId)}/subscriptions`,
    items: "subscriptions",
    ctx: { destination_id: destinationId },
    notFoundEmpty: true,
  };
}

// =================================================================== connections

const connections = defineSegmentCatalogTable(
  {
    name: "connections",
    description: "The pipeline graph: one edge per source → destination and source → warehouse connection",
    title: "Pipeline Graph",
    category: "pipeline",
    keywords: "segment, connections, pipeline graph, lineage, source to destination, source to warehouse, edges",
    docLlm:
      "The Segment pipeline as an edge list: one row per source → destination and source → warehouse " +
      "connection (GET /sources/{id}/connected-destinations and /connected-warehouses): source_id, source_name, " +
      "source_enabled, target_type ('destination' or 'warehouse'), target_id, target_name, target_slug (catalog " +
      "slug) and target_enabled. WHERE source_id = '…' reads one source's edges; WHERE target_type = " +
      "'warehouse' skips the destination calls. Use it to draw lineage or find sources with no outputs.",
    docMd: "## connections\n\nThe source → destination / warehouse edge list.",
    pushdown: "`source_id` (=, IN) reads only those sources; `target_type` (=, IN) skips the other edge kind.",
    columns: [
      C.ctx("source_id", "Source the edge starts from (joins sources.id).", "source_id"),
      C.ctx("source_name", "Source name (NULL when the source id was given directly).", "source_name", true),
      C.custom(
        "source_enabled",
        bool,
        "BOOLEAN",
        "Whether the source is enabled (NULL when the source id was given directly).",
        (r) => (r.__ctx?.source_enabled === undefined ? null : r.__ctx.source_enabled === "true"),
      ),
      C.ctx("target_type", "'destination' or 'warehouse'.", "target_type"),
      C.str("target_id", "Destination instance id or warehouse id.", "id", false),
      C.str("target_name", "Destination name, or the warehouse's settings name.", (r) =>
        r.__ctx?.target_type === "warehouse" ? (r.settings?.name ?? r.metadata?.name) : r.name,
      ),
      C.str("target_slug", "Catalog slug of the destination or warehouse type.", "metadata.slug"),
      C.bool("target_enabled", "Whether the destination or warehouse is enabled.", "enabled"),
    ],
    requests: (scan) => {
      const ids = idsFor(scan, "source_id");
      return ids ? ids.flatMap((id) => edgeReqs(scan, id, {})) : [{ path: "/sources", items: "sources", fan: "edges" }];
    },
    fans: {
      edges: (row, _ctx, scan) =>
        edgeReqs(scan, row.id, { source_name: asStr(row.name) ?? "", source_enabled: String(row.enabled === true) }),
    },
    examples: [
      {
        sql: `SELECT source_name, target_type, target_name FROM ${M}.connections ORDER BY source_name, target_type, target_name`,
        description: "The whole pipeline graph",
      },
      {
        sql: `SELECT s.name FROM ${M}.sources s ANTI JOIN ${M}.connections c ON c.source_id = s.id ORDER BY s.name`,
        description: "Sources that feed nothing",
      },
    ],
  },
  MAIN,
  ["source_id", "target_type", "target_id"],
);

function edgeReqs<A>(scan: ScanContext<A>, sourceId: string, ctx: Record<string, string>): Req[] {
  const kinds = eqValues(scan.constraints, "target_type") ?? ["destination", "warehouse"];
  const reqs: Req[] = [];
  const base = { source_id: sourceId, ...ctx };
  if (kinds.includes("destination")) {
    reqs.push({
      path: `/sources/${enc(sourceId)}/connected-destinations`,
      items: "destinations",
      ctx: { ...base, target_type: "destination" },
      notFoundEmpty: true,
    });
  }
  if (kinds.includes("warehouse")) {
    reqs.push({
      path: `/sources/${enc(sourceId)}/connected-warehouses`,
      items: "warehouses",
      ctx: { ...base, target_type: "warehouse" },
      notFoundEmpty: true,
    });
  }
  return reqs;
}

// ==================================================================== warehouses

const warehouses = defineSegmentCatalogTable(
  {
    name: "warehouses",
    description: "Every warehouse connection: type, enabled, masked credentials and live connection state",
    title: "Segment Warehouses",
    category: "warehouses",
    keywords: "segment, warehouses, snowflake, bigquery, redshift, postgres, databricks, connection state",
    docLlm:
      "Every warehouse (GET /warehouses): id, name (from settings), enabled, warehouse_type (catalog slug: " +
      "'snowflake', 'bigquery', 'redshift', 'postgres', 'databricks' …), warehouse_type_name, settings JSON with " +
      "passwords, private keys and service-account credentials redacted (masked_settings lists them), and " +
      "connection_state ('CONNECTED' or 'FAILED' from GET /warehouses/{id}/connection-state — fetched only when " +
      "the query reads that column, one call per warehouse). WHERE id = '…' reads one warehouse.",
    docMd: "## warehouses\n\nWarehouse connections with masked credentials and (on demand) their connection state.",
    pushdown: "`id` (=, IN) → GET /warehouses/{id}. `connection_state` costs one call per warehouse, only when read.",
    columns: [
      C.str("id", "Warehouse id.", "id", false),
      C.str("name", "Warehouse name (settings.name).", "settings.name"),
      C.str("workspace_id", "Id of the workspace the warehouse belongs to (joins workspaces.id).", "workspaceId"),
      C.bool("enabled", "True when syncs are enabled.", "enabled"),
      C.str("warehouse_type", "Catalog slug: snowflake, bigquery, redshift, postgres, databricks …", "metadata.slug"),
      C.str("warehouse_type_name", "Catalog name of the warehouse type.", "metadata.name"),
      C.str(
        "connection_state",
        "CONNECTED or FAILED (GET /warehouses/{id}/connection-state; read only when selected).",
        "__connectionState",
      ),
      ...settingsColumns(),
    ],
    requests: (scan) => {
      const ids = idsFor(scan, "id");
      return ids
        ? ids.map((id) => ({ path: `/warehouses/${enc(id)}`, items: "warehouse", paging: "none", notFoundEmpty: true }))
        : [{ path: "/warehouses", items: "warehouses" }];
    },
    expand: async (cfg, rows, scan) => {
      if (!scan.projected.has("connection_state")) return rows;
      for (const r of rows) {
        const data = await segmentGet(cfg, `/warehouses/${enc(r.id)}/connection-state`).catch(() => ({}));
        r.__connectionState = (data as Row).connectionState ?? null;
      }
      return rows;
    },
    examples: [
      {
        sql: `SELECT name, warehouse_type, connection_state FROM ${M}.warehouses WHERE enabled ORDER BY name`,
        description: "Enabled warehouses and whether Segment can reach them",
      },
    ],
  },
  MAIN,
  ["id"],
);

/** Warehouse sync rows (SyncV1 has no id: the run is identified by warehouse + source + start). */
export const SYNC_STATUS_DOC = "Segment's sync status as reported, e.g. SUCCESS, FAILED, PARTIAL_SUCCESS, RUNNING.";

const warehouseSyncs = defineSegmentCatalogTable(
  {
    name: "warehouse_syncs",
    description: "Warehouse sync history: one row per sync of a source into a warehouse (status, rows, duration)",
    title: "Warehouse Sync History",
    category: "warehouses",
    keywords: "segment, warehouse syncs, sync history, sync reports, loads, failures, snowflake, bigquery",
    docLlm:
      "Warehouse sync reports (GET /warehouses/{id}/syncs, or /warehouses/{id}/connected-sources/{source}/syncs): " +
      "warehouse_id, source_id, started_at, ended_at (NULL while running), status (e.g. SUCCESS, FAILED, " +
      "PARTIAL_SUCCESS), duration_seconds, human_duration, rows_synced, notices (JSON list of {level, message, " +
      "createdAt}) and error_message (the first error-level notice). WHERE warehouse_id = '…' reads one warehouse " +
      "and WHERE source_id = '…' uses the per-source endpoint; otherwise every warehouse is listed first. " +
      "The same rows appear in unified.runs with orchestrator = 'segment'.",
    docMd: "## warehouse_syncs\n\nSync history per warehouse and source.",
    pushdown: "`warehouse_id` (=, IN) reads only those warehouses; `source_id` (=) → /connected-sources/{id}/syncs.",
    columns: warehouseSyncColumns(),
    requests: (scan) => warehouseSyncRequests(scan),
    fans: { syncs: (row, _ctx, scan) => warehouseSyncChildReqs(scan, row.id, asStr(row.settings?.name) ?? "") },
    examples: [
      {
        sql: `SELECT warehouse_id, status, count(*) AS syncs, sum(rows_synced) AS rows FROM ${M}.warehouse_syncs GROUP BY ALL ORDER BY warehouse_id, status`,
        description: "Sync outcomes and rows loaded per warehouse",
      },
      {
        sql: `SELECT warehouse_id, source_id, started_at, error_message FROM ${M}.warehouse_syncs WHERE status = 'FAILED' ORDER BY started_at DESC`,
        description: "Failed syncs and why",
      },
    ],
  },
  MAIN,
  ["warehouse_id", "source_id", "started_at"],
);

export function warehouseSyncColumns(): Col[] {
  return [
    C.ctx("warehouse_id", "Warehouse the sync loaded into (joins warehouses.id).", "warehouse_id"),
    C.ctx("warehouse_name", "Warehouse name (NULL when the warehouse id was given directly).", "warehouse_name", true),
    C.str("source_id", "Source loaded by the sync.", "sourceId", false),
    C.time("started_at", "When the sync started.", "start", false),
    C.time("ended_at", "When the sync finished (NULL while it runs).", "end"),
    C.str("status", SYNC_STATUS_DOC, "status"),
    C.num("duration_seconds", "Duration in seconds (partial while running).", "duration"),
    C.str("human_duration", "Human-readable duration, e.g. '3m 12s'.", "humanDuration"),
    C.big("rows_synced", "Rows loaded into the warehouse (count).", "count"),
    C.json("notices", "Notices raised during the sync: [{level, message, createdAt}].", "notices"),
    C.str("error_message", "Message of the first error-level notice, else NULL.", (r) => {
      const n = Array.isArray(r.notices) ? r.notices.find((x: Row) => /error|fatal/i.test(String(x.level))) : null;
      return n?.message ?? null;
    }),
  ];
}

export function warehouseSyncRequests<A>(scan: ScanContext<A>): Req[] {
  const ids = idsFor(scan, "warehouse_id");
  return ids
    ? ids.flatMap((id) => warehouseSyncChildReqs(scan, id, ""))
    : [{ path: "/warehouses", items: "warehouses", fan: "syncs" }];
}

export function warehouseSyncChildReqs<A>(scan: ScanContext<A>, warehouseId: string, name: string): Req[] {
  const ctx: Record<string, string> = { warehouse_id: warehouseId };
  if (name) ctx.warehouse_name = name;
  const srcs = eqValues(scan.constraints, "source_id");
  if (srcs) {
    return srcs.map((s) => ({
      path: `/warehouses/${enc(warehouseId)}/connected-sources/${enc(s)}/syncs`,
      items: "reports",
      ctx,
      notFoundEmpty: true,
    }));
  }
  return [{ path: `/warehouses/${enc(warehouseId)}/syncs`, items: "reports", ctx, notFoundEmpty: true }];
}

// ===================================================================== functions

const FUNCTION_TYPES = ["SOURCE", "DESTINATION", "INSERT_SOURCE", "INSERT_DESTINATION", "INSERT_TRANSFORMATION"];

const functions = defineSegmentCatalogTable(
  {
    name: "functions",
    description: "Source, destination and insert functions (metadata only; see function_code for the code)",
    title: "Segment Functions",
    category: "functions",
    keywords: "segment, functions, source functions, destination functions, insert functions, custom code",
    docLlm:
      "Every Segment Function (GET /functions?resourceType=…, read once per type): id, resource_type (SOURCE, " +
      "DESTINATION, INSERT_SOURCE, INSERT_DESTINATION or INSERT_TRANSFORMATION), display_name, description, " +
      "created_at, created_by, logo_url and catalog_id. The code is not included: call " +
      "segment.main.function_code('<id>') to read one function's source and setting definitions. " +
      "WHERE resource_type = '…' reads only that type.",
    docMd: "## functions\n\nFunction metadata. The code is opt-in through `function_code(id)`.",
    pushdown: "`resource_type` (=, IN) → resourceType (one request per type otherwise).",
    feature: FEATURE_FUNCTIONS,
    columns: [
      C.str("id", "Function id.", "id", false),
      C.str(
        "resource_type",
        "SOURCE, DESTINATION, INSERT_SOURCE, INSERT_DESTINATION or INSERT_TRANSFORMATION.",
        "resourceType",
      ),
      C.str("display_name", "Function name.", "displayName"),
      C.str("description", "What the function does, as written by its author.", "description"),
      C.time("created_at", "When the function was created.", "createdAt"),
      C.str("created_by", "Id of the user who created it.", "createdBy"),
      C.str("logo_url", "URL of the function's logo image, if one was uploaded.", "logoUrl"),
      C.str("catalog_id", "Catalog id of the function (used when connecting it).", "catalogId"),
    ],
    requests: (scan) =>
      (eqValues(scan.constraints, "resource_type") ?? FUNCTION_TYPES)
        .filter((t) => FUNCTION_TYPES.includes(t))
        .map((t) => ({ path: "/functions", query: { resourceType: t }, items: "functions" })),
    examples: [
      {
        sql: `SELECT resource_type, count(*) AS functions FROM ${M}.functions GROUP BY resource_type ORDER BY resource_type`,
        description: "Functions per type",
      },
    ],
  },
  MAIN,
  ["id"],
);

const functionCode = defineSegmentTable<{ function_id: string }>({
  name: "function_code",
  description: "One function's source code and setting definitions (opt-in: code is never in the functions table)",
  title: "Function Code",
  category: "functions",
  keywords: "segment, function code, javascript, source code, settings, deployed",
  docLlm:
    "Read one Segment Function in full (GET /functions/{id}): id, resource_type, display_name, code (the " +
    "JavaScript source), deployed_at, is_latest_version, batch_max_count and settings (JSON list of the " +
    "function's setting definitions {name, label, type, required, sensitive}; definitions carry no values). " +
    "Argument: function_id (from segment.main.functions). Code is returned only through this function, never " +
    "by the functions table.",
  docMd: "## function_code(function_id)\n\nOne function's code and setting definitions.",
  feature: FEATURE_FUNCTIONS,
  args: { function_id: utf8() },
  argDocs: { function_id: "Function id, from segment.main.functions." },
  columns: [
    C.str("id", "Function id.", "id", false),
    C.str("resource_type", "Function type.", "resourceType"),
    C.str("display_name", "Function name.", "displayName"),
    C.str("code", "The function's JavaScript source.", "code"),
    C.time("deployed_at", "When the code was last deployed.", "deployedAt"),
    C.bool("is_latest_version", "True when the deployed version is the latest.", "isLatestVersion"),
    C.big("batch_max_count", "Maximum batch size for batched handlers.", "batchMaxCount"),
    C.json("settings", "Setting definitions: [{name, label, description, type, required, sensitive}].", (r) =>
      Array.isArray(r.settings) ? r.settings.map((s: Row) => maskSecrets(s)) : null,
    ),
  ],
  requests: ({ args }) => [
    { path: `/functions/${enc(args.function_id)}`, items: "function", paging: "none", notFoundEmpty: true },
  ],
  examples: [
    {
      sql: `SELECT display_name, length(code) AS code_chars FROM ${M}.function_code('dfn_enrich')`,
      description: "Read one function's code",
    },
  ],
});

// ====================================================================== labels

const labels = defineSegmentCatalogTable(
  {
    name: "labels",
    description: "Workspace labels (key:value pairs used to scope sources and access)",
    title: "Workspace Labels",
    category: "workspace",
    keywords: "segment, labels, tags, environment, access scoping",
    docLlm:
      "Every label defined in the workspace (GET /labels): key, value and description. Sources carry labels " +
      "(sources.labels holds 'key:value' strings) and IAM permissions can be scoped by them.",
    docMd: "## labels\n\nWorkspace labels.",
    columns: [
      C.str("key", "Label key, e.g. 'environment'.", "key", false),
      C.str("value", "Label value, e.g. 'prod'.", "value", false),
      C.str("description", "What the label is for (free text set in the workspace).", "description"),
    ],
    requests: () => [{ path: "/labels", items: "labels", paging: "none" }],
    examples: [{ sql: `SELECT key, value FROM ${M}.labels ORDER BY key, value`, description: "All labels" }],
  },
  MAIN,
  ["key", "value"],
);

// ================================================================= audit trail

const auditEvents = defineSegmentCatalogTable(
  {
    name: "audit_events",
    description: "Audit trail: who changed what in the workspace, with time and resource filters pushed down",
    title: "Audit Trail",
    category: "audit",
    keywords: "segment, audit trail, audit log, changes, who changed, compliance, security",
    docLlm:
      "The workspace audit trail (GET /audit-events): id, timestamp, type (e.g. 'Source Created', " +
      "'Destination Modified'), actor (user or token id), actor_email, resource_id, resource_type and " +
      "resource_name. WHERE timestamp >= / < bounds become startTime/endTime, and WHERE resource_id = '…' or " +
      "resource_type = '…' are sent to Segment, so filter early. Needs the Audit Trail feature (Business tier).",
    docMd: "## audit_events\n\nWho changed what, with time and resource filters pushed down.",
    pushdown:
      "`timestamp` ranges → startTime/endTime; `resource_id` (=) → resourceId; `resource_type` (=) → resourceType.",
    feature: FEATURE_AUDIT,
    columns: [
      C.str("id", "Audit event id.", "id", false),
      C.time("timestamp", "When it happened.", "timestamp"),
      C.str("type", "Event type, e.g. 'Destination Modified'.", "type"),
      C.str("actor", "Who did it (user or token id).", "actor"),
      C.str("actor_email", "The actor's email when it is a user.", "actorEmail"),
      C.str("resource_id", "Id of the changed resource.", "resourceId"),
      C.str("resource_type", "Type of the changed resource, e.g. 'source', 'destination'.", "resourceType"),
      C.str("resource_name", "Name of the changed resource.", "resourceName"),
    ],
    requests: (scan) => {
      const { lower, upper } = timeBounds(scan.constraints, "timestamp");
      const one = (col: string) => {
        const v = eqValues(scan.constraints, col);
        return v && v.length === 1 ? v[0] : undefined;
      };
      return [
        {
          path: "/audit-events",
          items: "events",
          query: {
            startTime: lower === undefined ? undefined : new Date(lower).toISOString(),
            endTime: upper === undefined ? undefined : new Date(upper).toISOString(),
            resourceId: one("resource_id"),
            resourceType: one("resource_type"),
          },
        },
      ];
    },
    examples: [
      {
        sql: `SELECT timestamp, actor_email, type, resource_name FROM ${M}.audit_events WHERE timestamp >= TIMESTAMPTZ '2026-11-01' AND resource_type = 'destination' ORDER BY timestamp DESC`,
        description: "Destination changes since November 1 (time and type pushed down)",
      },
    ],
  },
  MAIN,
  ["id"],
);

// =================================================================== Reverse ETL

const reverseEtlModels = defineSegmentCatalogTable(
  {
    name: "reverse_etl_models",
    description: "Reverse ETL models: the warehouse SQL that feeds Reverse ETL mappings",
    title: "Segment Reverse ETL Models",
    category: "reverse_etl",
    keywords: "segment, reverse etl, models, warehouse query, activation",
    docLlm:
      "Every Reverse ETL model (GET /reverse-etl-models): id, source_id (the Reverse ETL warehouse source), name, " +
      "description, enabled, query (the SQL run in the warehouse) and query_identifier_column. Mappings that use " +
      "a model are in destination_subscriptions (model_id); their runs are in reverse_etl_syncs. WHERE id = '…' " +
      "reads one model.",
    docMd: "## reverse_etl_models\n\nReverse ETL models and their SQL.",
    pushdown: "`id` (=, IN) → GET /reverse-etl-models/{id}.",
    feature: FEATURE_RETL,
    columns: [
      C.str("id", "Model id.", "id", false),
      C.str("source_id", "Reverse ETL source (warehouse connection) id.", "sourceId"),
      C.str("name", "Model name.", "name"),
      C.str("description", "What the model computes, as written by its author.", "description"),
      C.bool("enabled", "True when the model is enabled.", "enabled"),
      C.str("query", "The SQL the model runs in the warehouse.", "query"),
      C.str("query_identifier_column", "Column that uniquely identifies a record.", "queryIdentifierColumn"),
    ],
    requests: (scan) => {
      const ids = idsFor(scan, "id");
      return ids
        ? ids.map((id) => ({
            path: `/reverse-etl-models/${enc(id)}`,
            items: "reverseEtlModel",
            paging: "none",
            notFoundEmpty: true,
          }))
        : [{ path: "/reverse-etl-models", items: "models" }];
    },
    examples: [
      {
        sql: `SELECT name, query_identifier_column FROM ${M}.reverse_etl_models WHERE enabled ORDER BY name`,
        description: "Enabled Reverse ETL models",
      },
    ],
  },
  MAIN,
  ["id"],
);

export function reverseEtlSyncColumns(): Col[] {
  return [
    C.ctx("destination_id", "Destination whose mapping ran the sync.", "destination_id", true),
    C.ctx("subscription_id", "Mapping (subscription) id.", "subscription_id"),
    C.str("model_id", "Reverse ETL model id.", (r) => r.modelId ?? r.__ctx?.model_id, false),
    C.str("sync_id", "Segment's id for this sync run.", "syncId", false),
    C.str("source_id", "Reverse ETL source id.", "sourceId"),
    C.str("status", "Sync status: IN_PROGRESS, SUCCESS, PARTIAL_SUCCESS or FAIL.", "syncStatus"),
    C.time("started_at", "When the sync started.", "startedAt"),
    C.time("finished_at", "When the sync finished (NULL while running).", "finishedAt"),
    C.str("duration", "Duration as Segment reports it.", "duration"),
    C.big("extract_count", "Records extracted from the warehouse (count).", "extractPhase.extractCount"),
    C.big("added_count", "New records found (count).", "extractPhase.addedCount"),
    C.big("updated_count", "Changed records found (count).", "extractPhase.updatedCount"),
    C.big("deleted_count", "Deleted records found (count).", "extractPhase.deletedCount"),
    C.big("delivered_count", "Records delivered successfully (count).", "loadPhase.deliverSuccessCount"),
    C.big("failed_count", "Records that failed delivery (count).", "loadPhase.deliverFailureCount"),
    C.str("error", "Error message, if any.", "error"),
    C.str("error_code", "Fatal error code, if any.", "errorCode"),
  ];
}

/** Requests for Reverse ETL syncs: direct when model + subscription are pushed, else via destinations. */
export function reverseEtlSyncRequests<A>(scan: ScanContext<A>): Req[] {
  const models = eqValues(scan.constraints, "model_id");
  const subs = eqValues(scan.constraints, "subscription_id");
  if (models && subs && models.length === 1) {
    return subs.map((s) => retlSyncReq(models[0] as string, s, {}));
  }
  const dests = eqValues(scan.constraints, "destination_id");
  if (dests) return dests.map((d) => ({ ...subscriptionsReq(d), fan: "retl", feature: FEATURE_RETL }));
  return [{ path: "/destinations", items: "destinations", fan: "retl_dest" }];
}

export function retlSyncReq(modelId: string, subscriptionId: string, ctx: Record<string, string>): Req {
  return {
    path: `/reverse-etl-models/${enc(modelId)}/subscriptionId/${enc(subscriptionId)}/syncs`,
    items: "syncStatuses",
    paging: "plain",
    pageSize: 100,
    ctx: { ...ctx, model_id: modelId, subscription_id: subscriptionId },
    notFoundEmpty: true,
    feature: FEATURE_RETL,
  };
}

/** Fan-out handlers shared by reverse_etl_syncs and unified.runs. */
export function retlFans<A>() {
  return {
    retl_dest: (row: Row) => [{ ...subscriptionsReq(row.id), fan: "retl" }],
    retl: (row: Row, ctx: Record<string, string>, scan: ScanContext<A>) => {
      if (!row.modelId) return [];
      const models = eqValues(scan.constraints, "model_id");
      if (models && !models.includes(row.modelId)) return [];
      const trigger = row.reverseETLSchedule?.strategy === "MANUAL" ? "manual" : "schedule";
      return [retlSyncReq(row.modelId, row.id, { destination_id: ctx.destination_id ?? "", trigger })];
    },
  };
}

const reverseEtlSyncs = defineSegmentCatalogTable(
  {
    name: "reverse_etl_syncs",
    description: "Reverse ETL sync runs per mapping: status, extract and delivery counts, errors",
    title: "Reverse ETL Sync Runs",
    category: "reverse_etl",
    keywords: "segment, reverse etl, syncs, runs, activation, delivery failures, sync status",
    docLlm:
      "Reverse ETL sync runs (GET /reverse-etl-models/{model}/subscriptionId/{subscription}/syncs): " +
      "destination_id, subscription_id, model_id, sync_id, source_id, status (IN_PROGRESS, SUCCESS, " +
      "PARTIAL_SUCCESS, FAIL), started_at, finished_at, duration, extract counts (extract/added/updated/deleted), " +
      "delivered_count, failed_count, error and error_code. WHERE model_id = '…' AND subscription_id = '…' reads " +
      "one mapping directly; WHERE destination_id = '…' reads that destination's mappings; otherwise every " +
      "destination's subscriptions are listed to find the Reverse ETL mappings. Also in unified.runs.",
    docMd: "## reverse_etl_syncs\n\nReverse ETL runs per mapping.",
    pushdown:
      "`model_id` + `subscription_id` (=) → that mapping's syncs; `destination_id` (=, IN) → those destinations only; `model_id` alone skips other models' mappings.",
    feature: FEATURE_RETL,
    columns: reverseEtlSyncColumns(),
    requests: (scan) => reverseEtlSyncRequests(scan),
    fans: retlFans(),
    examples: [
      {
        sql: `SELECT model_id, status, count(*) AS runs, sum(failed_count) AS failed_records FROM ${M}.reverse_etl_syncs GROUP BY ALL ORDER BY model_id, status`,
        description: "Reverse ETL run outcomes per model",
      },
    ],
  },
  MAIN,
  ["subscription_id", "sync_id"],
);

// ================================================================ tracking plans

const trackingPlans = defineSegmentCatalogTable(
  {
    name: "plans",
    description: "Protocols tracking plans: name, type (LIVE, TEMPLATE, RULE_LIBRARY …), timestamps",
    title: "Tracking Plans",
    category: "tracking_plans",
    keywords: "segment, protocols, tracking plans, schema, data governance, event specification",
    docLlm:
      "Every Protocols tracking plan (GET /tracking-plans): id, name, slug, description, type (LIVE, TEMPLATE, " +
      "RULE_LIBRARY, PROPERTY_LIBRARY or ENGAGE), created_at and updated_at. LIVE plans are the ones connected " +
      "to sources (see plan_sources). WHERE type = 'LIVE' is sent as the type filter; WHERE id = '…' reads one plan.",
    docMd: "## tracking.plans\n\nTracking plans.",
    pushdown: "`id` (=, IN) → GET /tracking-plans/{id}; `type` (=) → type.",
    feature: FEATURE_PROTOCOLS,
    columns: [
      C.str("id", "Tracking plan id.", "id", false),
      C.str("name", "Plan name.", "name"),
      C.str("slug", "Plan slug.", "slug"),
      C.str("description", "What the plan covers, as written by its owner.", "description"),
      C.str("type", "LIVE, TEMPLATE, RULE_LIBRARY, PROPERTY_LIBRARY or ENGAGE.", "type"),
      C.time("created_at", "When the plan was created.", "createdAt"),
      C.time("updated_at", "When the plan last changed.", "updatedAt"),
    ],
    requests: (scan) => planRequests(scan, "id"),
    examples: [
      {
        sql: `SELECT name, type, updated_at FROM ${T}.plans ORDER BY updated_at DESC`,
        description: "Tracking plans by last change",
      },
    ],
  },
  TRACKING,
  ["id"],
);

/** Tracking-plan list (or per-id) requests, honouring a pushed `type`. */
function planRequests<A>(scan: ScanContext<A>, idCol: string, fan?: string, type?: string): Req[] {
  const ids = idsFor(scan, idCol);
  if (ids) {
    return ids.map((id) => ({
      path: `/tracking-plans/${enc(id)}`,
      items: "trackingPlan",
      paging: "none",
      notFoundEmpty: true,
      fan,
    }));
  }
  const types = eqValues(scan.constraints, idCol === "id" ? "type" : "plan_type");
  const t = type ?? (types && types.length === 1 ? types[0] : undefined);
  return [{ path: "/tracking-plans", items: "trackingPlans", query: { type: t }, fan }];
}

function rulesReq(plan: Row | string): Req {
  const id = typeof plan === "string" ? plan : plan.id;
  const ctx: Record<string, string> = { tracking_plan_id: id };
  if (typeof plan !== "string" && plan.name) ctx.tracking_plan_name = String(plan.name);
  if (typeof plan !== "string" && plan.type) ctx.plan_type = String(plan.type);
  return {
    path: `/tracking-plans/${enc(id)}/rules`,
    items: "rules",
    ctx,
    notFoundEmpty: true,
    feature: FEATURE_PROTOCOLS,
  };
}

const planCtxCols = (): Col[] => [
  C.ctx("tracking_plan_id", "Tracking plan the row belongs to (joins tracking.plans.id).", "tracking_plan_id"),
  C.ctx("tracking_plan_name", "Tracking plan name (from the plan list).", "tracking_plan_name", true),
  C.ctx("plan_type", "Tracking plan type (LIVE, TEMPLATE …).", "plan_type", true),
];

const ruleFans = { rules: (row: Row) => [rulesReq(row)] };

const rules = defineSegmentCatalogTable(
  {
    name: "rules",
    description: "Tracking plan rules: one row per event/identify/group/page rule with its JSON Schema",
    title: "Tracking Plan Rules",
    category: "tracking_plans",
    keywords: "segment, protocols, tracking plan rules, events, json schema, event specification, deprecated",
    docLlm:
      "Every rule of every tracking plan (GET /tracking-plans/{id}/rules): tracking_plan_id, tracking_plan_name, " +
      "plan_type, rule_type (TRACK, IDENTIFY, GROUP, PAGE, SCREEN or COMMON), event_name (the rule key: the " +
      "event name for TRACK rules, NULL for identify/group), version, created_at, updated_at, deprecated_at, " +
      "is_deprecated, description, property_count, required_properties and json_schema (the full JSON Schema). " +
      "Use tracking.properties for one row per property. WHERE tracking_plan_id = '…' reads one plan; WHERE " +
      "plan_type = 'LIVE' lists only live plans.",
    docMd: "## tracking.rules\n\nTracking plan rules with their JSON Schema.",
    pushdown: "`tracking_plan_id` (=, IN) reads only those plans; `plan_type` (=) → the plan list's type filter.",
    feature: FEATURE_PROTOCOLS,
    columns: [
      ...planCtxCols(),
      C.str(
        "rule_id",
        "Rule identity within the plan, '<rule_type>:<key>' (Segment rules carry no id), e.g. 'TRACK:Order Completed', 'IDENTIFY:'.",
        (r) => `${r.type}:${r.key ?? ""}`,
        false,
      ),
      C.str("rule_type", "TRACK, IDENTIFY, GROUP, PAGE, SCREEN or COMMON.", "type", false),
      C.str(
        "event_name",
        "Rule key: the event name for TRACK rules (page/screen name for PAGE/SCREEN).",
        (r) => r.key || null,
      ),
      C.big("version", "Rule version number, incremented by Segment on every edit (1 = first).", "version"),
      C.time("created_at", "When the rule was created.", "createdAt"),
      C.time("updated_at", "When the rule last changed.", "updatedAt"),
      C.time("deprecated_at", "When the rule was deprecated, else NULL.", "deprecatedAt"),
      C.bool("is_deprecated", "True when deprecated_at is set.", (r) => !!r.deprecatedAt),
      C.str("description", "The schema's description.", (r) => ruleDescription(r.jsonSchema)),
      C.custom(
        "property_count",
        int32,
        "INTEGER",
        "Number of properties/traits defined (all sections, nested included).",
        (r) => flattenRuleSchema(r.jsonSchema).filter((p) => p.section !== "context").length,
      ),
      C.strList("required_properties", "Required top-level properties/traits.", (r) =>
        flattenRuleSchema(r.jsonSchema)
          .filter((p) => p.section !== "context" && p.depth === 0 && p.is_required)
          .map((p) => p.property_name),
      ),
      C.json("json_schema", "The rule's full JSON Schema.", "jsonSchema"),
    ],
    requests: (scan) => planRequests(scan, "tracking_plan_id", "rules"),
    fans: ruleFans,
    examples: [
      {
        sql: `SELECT tracking_plan_name, event_name, property_count FROM ${T}.rules WHERE rule_type = 'TRACK' AND NOT is_deprecated ORDER BY tracking_plan_name, event_name`,
        description: "Planned track events per plan",
      },
    ],
  },
  TRACKING,
  ["tracking_plan_id", "rule_id"],
);

const properties = defineSegmentCatalogTable(
  {
    name: "properties",
    description:
      "Tracking plan properties flattened: one row per property/trait with type, required and allowed values",
    title: "Tracking Plan Properties",
    category: "tracking_plans",
    keywords: "segment, protocols, properties, traits, json schema, enum, required, data types, tracking plan",
    docLlm:
      "Every property of every tracking-plan rule, flattened from the rule's JSON Schema: tracking_plan_id, " +
      "tracking_plan_name, plan_type, rule_type, event_name, section ('properties' for track/page/screen " +
      "properties, 'traits' for identify/group traits, 'context'), property_name (dotted path for nested " +
      "objects, '[]' for array items: 'products[].sku'), depth, data_type ('string', 'number', 'integer', " +
      "'boolean', 'object', 'array', or several joined by '|'), is_required, is_nullable, is_array, item_type, " +
      "enum_values (allowed values as strings), pattern, format, minimum, maximum, min_length, max_length, " +
      "description and json_schema. WHERE tracking_plan_id = '…' reads one plan.",
    docMd: "## tracking.properties\n\nOne row per property/trait with type, required flag and allowed values.",
    pushdown: "`tracking_plan_id` (=, IN) reads only those plans; `plan_type` (=) → the plan list's type filter.",
    feature: FEATURE_PROTOCOLS,
    columns: [
      ...planCtxCols(),
      C.str("rule_id", "The owning rule, '<rule_type>:<key>' (joins tracking.rules.rule_id).", "rule_id", false),
      C.str("rule_type", "TRACK, IDENTIFY, GROUP, PAGE, SCREEN or COMMON.", "rule_type", false),
      C.str("event_name", "Event name for TRACK rules (rule key), else NULL.", "event_name"),
      C.str("section", "'properties', 'traits' or 'context'.", "section", false),
      C.str(
        "property_name",
        "Property path, e.g. 'order_id', 'address.city', 'products[].sku'.",
        "property_name",
        false,
      ),
      C.custom("depth", int32, "INTEGER", "Nesting depth (0 = top level).", (r) => r.depth, false),
      C.str("data_type", "JSON Schema type(s) excluding null, joined by '|'.", "data_type"),
      C.bool("is_required", "Listed in the parent object's required list.", "is_required", false),
      C.bool("is_nullable", "null is allowed (or no type is declared).", "is_nullable", false),
      C.bool("is_array", "The property is an array.", "is_array", false),
      C.str("item_type", "Type of the array items.", "item_type"),
      C.strList("enum_values", "Allowed values (enum/const) as strings; NULL when unrestricted.", "enum_values"),
      C.str("pattern", "Regex the value must match.", "pattern"),
      C.str("format", "JSON Schema format, e.g. 'date-time', 'email'.", "format"),
      C.num("minimum", "Smallest allowed numeric value, in the property's own units (JSON Schema minimum).", "minimum"),
      C.num("maximum", "Largest allowed numeric value, in the property's own units (JSON Schema maximum).", "maximum"),
      C.big("min_length", "Minimum string length in characters (JSON Schema minLength).", "min_length"),
      C.big("max_length", "Maximum string length in characters (JSON Schema maxLength).", "max_length"),
      C.str("description", "Property description.", "description"),
      C.json("json_schema", "The property's own JSON Schema.", "json_schema"),
    ],
    requests: (scan) => planRequests(scan, "tracking_plan_id", "rules"),
    fans: ruleFans,
    rows: (items) => flattenRules(items),
    examples: [
      {
        sql: `SELECT event_name, property_name, data_type, enum_values FROM ${T}.properties WHERE is_required AND section = 'properties' ORDER BY event_name, property_name`,
        description: "Required event properties and their allowed values",
      },
      {
        sql: `SELECT property_name, list(DISTINCT data_type) AS types FROM ${T}.properties GROUP BY property_name HAVING count(DISTINCT data_type) > 1`,
        description: "Properties whose type differs between events",
      },
    ],
  },
  TRACKING,
  ["tracking_plan_id", "rule_id", "section", "property_name"],
);

/** Rules → one row per flattened property. */
export function flattenRules(items: Row[]): Row[] {
  return items.flatMap((rule) =>
    flattenRuleSchema(rule.jsonSchema).map((p) => ({
      ...p,
      rule_type: rule.type,
      rule_id: `${rule.type}:${rule.key ?? ""}`,
      event_name: rule.key || null,
      deprecated_at: rule.deprecatedAt ?? null,
    })),
  );
}

const planSources = defineSegmentCatalogTable(
  {
    name: "plan_sources",
    description: "Which sources each tracking plan is connected to",
    title: "Tracking Plan Sources",
    category: "tracking_plans",
    keywords: "segment, protocols, tracking plan, connected sources, enforcement",
    docLlm:
      "The sources connected to each tracking plan (GET /tracking-plans/{id}/sources): tracking_plan_id, " +
      "tracking_plan_name, plan_type, source_id, source_name, source_slug, source_type and enabled. A source " +
      "is validated against the plan it is connected to; combine with main.source_schema_settings for its " +
      "blocking policy. WHERE tracking_plan_id = '…' reads one plan.",
    docMd: "## tracking.plan_sources\n\nTracking plan → source connections.",
    pushdown: "`tracking_plan_id` (=, IN) reads only those plans; `plan_type` (=) → type.",
    feature: FEATURE_PROTOCOLS,
    columns: [
      ...planCtxCols(),
      C.str("source_id", "Connected source id.", "id", false),
      C.str("source_name", "Display name of the connected source.", "name"),
      C.str("source_slug", "URL slug of the connected source.", "slug"),
      C.str("source_type", "Catalog slug of the source type.", "metadata.slug"),
      C.bool("enabled", "Whether the source is enabled.", "enabled"),
    ],
    requests: (scan) => planRequests(scan, "tracking_plan_id", "plan_sources"),
    fans: {
      plan_sources: (row) => {
        const r = rulesReq(row);
        return [{ ...r, path: `/tracking-plans/${enc(row.id)}/sources`, items: "sources" }];
      },
    },
    examples: [
      {
        sql: `SELECT tracking_plan_name, source_name FROM ${T}.plan_sources ORDER BY tracking_plan_name, source_name`,
        description: "Which sources each plan validates",
      },
    ],
  },
  TRACKING,
  ["tracking_plan_id", "source_id"],
);

// ======================================================================= engage

const spaces = defineSegmentCatalogTable(
  {
    name: "spaces",
    description: "Engage / Unify spaces (Profiles workspaces)",
    title: "Engage Spaces",
    category: "engage",
    keywords: "segment, engage, unify, profiles, spaces, personas",
    docLlm:
      "Every Engage / Unify space (GET /spaces): id, slug and name. Audiences and computed traits live in a " +
      "space. Needs Engage or Unify on the Segment plan; otherwise the query fails with a 403 that says so.",
    docMd: "## engage.spaces\n\nEngage / Unify spaces.",
    feature: FEATURE_ENGAGE,
    columns: [
      C.str("id", "Space id.", "id", false),
      C.str("slug", "Space slug.", "slug"),
      C.str("name", "Space name.", "name"),
    ],
    requests: () => [{ path: "/spaces", items: "spaces" }],
    examples: [{ sql: `SELECT slug, name FROM ${E}.spaces ORDER BY name`, description: "All spaces" }],
  },
  ENGAGE,
  ["id"],
);

const spaceFan = (kind: string, items: string) => (row: Row) => [
  { path: `/spaces/${enc(row.id)}/${kind}`, items, ctx: { space_id: row.id }, notFoundEmpty: true },
];

function perSpace<A>(scan: ScanContext<A>, kind: string, items: string, fan: string): Req[] {
  const ids = idsFor(scan, "space_id");
  return ids
    ? ids.map((id) => ({ path: `/spaces/${enc(id)}/${kind}`, items, ctx: { space_id: id }, notFoundEmpty: true }))
    : [{ path: "/spaces", items: "spaces", fan }];
}

const audiences = defineSegmentCatalogTable(
  {
    name: "audiences",
    description: "Engage audiences per space: definition, size, compute cadence, status",
    title: "Engage Audiences",
    category: "engage",
    keywords: "segment, engage, audiences, segments, cohorts, audience size, realtime, batch",
    docLlm:
      "Every Engage audience (GET /spaces/{id}/audiences): space_id, id, name, key, description, enabled, status, " +
      "audience_type (USERS, ACCOUNTS or LINKED), compute_cadence (REALTIME or BATCH), size (profiles in the " +
      "audience), size_type, definition_query (the audience query language), target_entity, created_by, " +
      "updated_by, created_at, updated_at. WHERE space_id = '…' reads one space.",
    docMd: "## engage.audiences\n\nAudiences with their size and definition.",
    pushdown: "`space_id` (=, IN) reads only those spaces.",
    feature: FEATURE_ENGAGE,
    columns: [
      C.ctx("space_id", "Engage space the object lives in (joins spaces.id).", "space_id"),
      C.str("id", "Audience id.", "id", false),
      C.str("name", "Audience name.", "name"),
      C.str("key", "Audience key (the trait name written to profiles).", "key"),
      C.str("description", "What the audience selects, as written by its author.", "description"),
      C.bool("enabled", "True when the audience computes.", "enabled"),
      C.str("status", "Computation status.", "status"),
      C.str("audience_type", "USERS, ACCOUNTS or LINKED.", "audienceType"),
      C.str("compute_cadence", "REALTIME or BATCH.", "computeCadence.type"),
      C.big("size", "Profiles (or accounts) in the audience at the last computation (count).", "size.count"),
      C.str("size_type", "What size counts: USERS, ACCOUNTS or ENTITIES.", "size.type"),
      C.str("definition_query", "The audience definition query.", "definition.query"),
      C.str("target_entity", "Target entity for linked audiences.", "definition.targetEntity"),
      C.str("created_by", "Creator id.", "createdBy"),
      C.str("updated_by", "Last editor id.", "updatedBy"),
      C.time("created_at", "When created.", "createdAt"),
      C.time("updated_at", "When last changed.", "updatedAt"),
    ],
    requests: (scan) => perSpace(scan, "audiences", "audiences", "aud"),
    fans: { aud: spaceFan("audiences", "audiences") },
    examples: [
      {
        sql: `SELECT name, compute_cadence, size FROM ${E}.audiences WHERE enabled ORDER BY size DESC NULLS LAST`,
        description: "Largest enabled audiences",
      },
    ],
  },
  ENGAGE,
  ["space_id", "id"],
);

const computedTraits = defineSegmentCatalogTable(
  {
    name: "computed_traits",
    description: "Engage computed traits per space: definition, status, options",
    title: "Engage Computed Traits",
    category: "engage",
    keywords: "segment, engage, computed traits, profile traits, aggregations",
    docLlm:
      "Every computed trait (GET /spaces/{id}/computed-traits): space_id, id, name, key, description, enabled, " +
      "status, definition_type (USERS or ACCOUNTS), definition_query, include_historical_data, " +
      "include_anonymous_users, created_by, updated_by, created_at, updated_at. WHERE space_id = '…' reads one space.",
    docMd: "## engage.computed_traits\n\nComputed traits and their definitions.",
    pushdown: "`space_id` (=, IN) reads only those spaces.",
    feature: FEATURE_ENGAGE,
    columns: [
      C.ctx("space_id", "Engage space the object lives in (joins spaces.id).", "space_id"),
      C.str("id", "Computed trait id.", "id", false),
      C.str("name", "Trait name.", "name"),
      C.str("key", "Trait key written to profiles.", "key"),
      C.str("description", "What the trait computes, as written by its author.", "description"),
      C.bool("enabled", "True when the trait computes.", "enabled"),
      C.str("status", "Computation status.", "status"),
      C.str("definition_type", "USERS or ACCOUNTS.", "definition.type"),
      C.str("definition_query", "The trait definition query.", "definition.query"),
      C.bool("include_historical_data", "Backfills from historical data.", "options.includeHistoricalData"),
      C.bool("include_anonymous_users", "Computes for anonymous profiles too.", "options.includeAnonymousUsers"),
      C.str("created_by", "Creator id.", "createdBy"),
      C.str("updated_by", "Last editor id.", "updatedBy"),
      C.time("created_at", "When created.", "createdAt"),
      C.time("updated_at", "When last changed.", "updatedAt"),
    ],
    requests: (scan) => perSpace(scan, "computed-traits", "computedTraits", "ct"),
    fans: { ct: spaceFan("computed-traits", "computedTraits") },
    examples: [
      { sql: `SELECT name, key, status FROM ${E}.computed_traits ORDER BY name`, description: "Computed traits" },
    ],
  },
  ENGAGE,
  ["space_id", "id"],
);

// ========================================================================== iam

const roleNames = (r: Row): string[] | null =>
  Array.isArray(r.permissions) ? [...new Set(r.permissions.map((p: Row) => String(p.roleName)))] : null;

const permissionsCol = () =>
  C.json("permissions", "Role grants: [{roleId, roleName, resources: [{id, type, labels}], labels}].", "permissions");

const users = defineSegmentCatalogTable(
  {
    name: "users",
    description: "Workspace users and their roles (IAM)",
    title: "Workspace Users",
    category: "access",
    keywords: "segment, iam, users, members, roles, permissions, access review",
    docLlm:
      "Every workspace user (GET /users): id, name, email (as Segment returns it), role_names (distinct roles " +
      "granted) and permissions (JSON grants with the resources and labels they are scoped to). Use for access " +
      "reviews, e.g. who holds 'Workspace Owner'.",
    docMd: "## iam.users\n\nWorkspace users and their role grants.",
    columns: [
      C.str("id", "User id.", "id", false),
      C.str("name", "User name.", "name"),
      C.str("email", "User email.", "email"),
      C.strList("role_names", "Distinct role names granted.", roleNames),
      permissionsCol(),
    ],
    requests: () => [{ path: "/users", items: "users" }],
    examples: [
      {
        sql: `SELECT email FROM ${I}.users WHERE list_contains(role_names, 'Workspace Owner') ORDER BY email`,
        description: "Workspace owners",
      },
    ],
  },
  IAM,
  ["id"],
);

const userGroups = defineSegmentCatalogTable(
  {
    name: "user_groups",
    description: "IAM user groups: member count and roles",
    title: "IAM User Groups",
    category: "access",
    keywords: "segment, iam, groups, user groups, roles, permissions",
    docLlm: "Every IAM user group (GET /groups): id, name, member_count, role_names and permissions (JSON grants).",
    docMd: "## iam.user_groups\n\nUser groups and their role grants.",
    columns: [
      C.str("id", "Group id.", "id", false),
      C.str("name", "Group name.", "name"),
      C.big("member_count", "Members (count).", "memberCount"),
      C.strList("role_names", "Distinct role names granted.", roleNames),
      permissionsCol(),
    ],
    requests: () => [{ path: "/groups", items: "userGroups" }],
    examples: [
      {
        sql: `SELECT name, member_count FROM ${I}.user_groups ORDER BY member_count DESC`,
        description: "Largest groups",
      },
    ],
  },
  IAM,
  ["id"],
);

const roles = defineSegmentCatalogTable(
  {
    name: "roles",
    description: "IAM roles available in the workspace",
    title: "IAM Roles",
    category: "access",
    keywords: "segment, iam, roles, permissions",
    docLlm: "Every IAM role (GET /roles): id, name and description.",
    docMd: "## iam.roles\n\nRoles.",
    columns: [
      C.str("id", "Role id.", "id", false),
      C.str("name", "Role name, e.g. 'Workspace Owner'.", "name"),
      C.str("description", "What the role allows.", "description"),
    ],
    requests: () => [{ path: "/roles", items: "roles" }],
    examples: [{ sql: `SELECT name, description FROM ${I}.roles ORDER BY name`, description: "All roles" }],
  },
  IAM,
  ["id"],
);

export const MAIN_TABLES: TableDescriptor[] = [
  workspace,
  sources,
  sourceSchemaSettings,
  destinations,
  destinationSubscriptions,
  connections,
  warehouses,
  warehouseSyncs,
  functions,
  labels,
  auditEvents,
  reverseEtlModels,
  reverseEtlSyncs,
];
export const MAIN_FUNCTIONS: VgiFunction[] = [functionCode];
export const TRACKING_TABLES: TableDescriptor[] = [trackingPlans, rules, properties, planSources];
export const ENGAGE_TABLES: TableDescriptor[] = [spaces, audiences, computedTraits];
export const IAM_TABLES: TableDescriptor[] = [users, userGroups, roles];
export { planRequests, rulesReq };
