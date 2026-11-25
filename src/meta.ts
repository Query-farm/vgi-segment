/**
 * Discovery metadata: the one file to edit when you rename or extend the worker.
 *
 * DuckDB, coding agents and the `vgi-lint` metadata checker all read these
 * tags. Keep every object documented and `make lint-explain` stays at 100.
 *
 * - Catalog-level tags (title, license, author, links, long description) live in
 *   `CATALOG_TAGS` below.
 * - Every function carries `objectTags(...)`: a title, a category from
 *   `SCHEMA_CATEGORIES`, search keywords, and LLM- and human-facing docs.
 */

import { field, type VgiDataType, type VgiField } from "@query-farm/vgi";

export const CATALOG_NAME = "segment";
export const VERSION = "0.2.0";

export const SOURCE_URL = "https://github.com/Query-farm/vgi-segment";

/**
 * Categories group objects in docs and agent tools. Every object's `vgi.category`
 * must name one of its schema's categories.
 */
export const MAIN_CATEGORIES = [
  { name: "workspace", title: "Workspace", description: "The workspace itself and its labels." },
  {
    name: "pipeline",
    title: "Pipeline",
    description: "Sources, destinations, Actions mappings, schema settings and the source → output graph.",
  },
  { name: "warehouses", title: "Warehouses", description: "Warehouse connections and their sync history." },
  { name: "functions", title: "Functions", description: "Source, destination and insert functions." },
  { name: "reverse_etl", title: "Reverse ETL", description: "Reverse ETL models and their sync runs." },
  { name: "observability", title: "Observability", description: "Event volume and Delivery Overview time series." },
  { name: "audit", title: "Audit Trail", description: "Who changed what in the workspace." },
] as const;
export const TRACKING_CATEGORIES = [
  {
    name: "tracking_plans",
    title: "Tracking Plans",
    description: "Protocols tracking plans, their rules, flattened properties and connected sources.",
  },
] as const;
export const ENGAGE_CATEGORIES = [
  { name: "engage", title: "Engage", description: "Engage / Unify spaces, audiences and computed traits." },
] as const;
export const IAM_CATEGORIES = [
  { name: "access", title: "Access Management", description: "Workspace users, user groups and roles." },
] as const;
export const UNIFIED_CATEGORIES = [
  {
    name: "unified",
    title: "Shared Shapes",
    description: "Segment rows in the shapes shared with the orchestrator and product-analytics workers.",
  },
] as const;

export type Category =
  | (typeof MAIN_CATEGORIES)[number]["name"]
  | (typeof TRACKING_CATEGORIES)[number]["name"]
  | (typeof ENGAGE_CATEGORIES)[number]["name"]
  | (typeof IAM_CATEGORIES)[number]["name"]
  | (typeof UNIFIED_CATEGORIES)[number]["name"];

/** Turn `"a, b, c"` into the JSON array string `vgi.keywords` requires. */
export function keywordsArray(keywords: string): string {
  return JSON.stringify(
    keywords
      .split(",")
      .map((k) => k.trim())
      .filter(Boolean),
  );
}

/** Build the standard per-object discovery tags. */
export function objectTags(opts: {
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
}): Record<string, string> {
  return {
    "vgi.title": opts.title,
    "vgi.category": opts.category,
    "vgi.keywords": keywordsArray(opts.keywords),
    "vgi.doc_llm": opts.docLlm,
    "vgi.doc_md": opts.docMd,
  };
}

/** Build an Arrow field whose `comment` shows up in DuckDB's `DESCRIBE` and agent tools. */
export function column(name: string, type: VgiDataType, comment: string, nullable = true): VgiField {
  return field(name, type, nullable, new Map([["comment", comment]]));
}

const SOURCES_MD =
  "### Sources and references\n\n" +
  "- [Segment Public API](https://docs.segmentapis.com/) — the endpoints this worker reads\n" +
  "- [Public API authentication and regions](https://segment.com/docs/api/public-api/) — tokens, `api.segmentapis.com` / `eu1.api.segmentapis.com`\n" +
  "- [Protocols tracking plans](https://segment.com/docs/protocols/tracking-plan/create/) and [schema controls](https://segment.com/docs/protocols/enforce/schema-configuration/)\n" +
  "- [Delivery Overview](https://segment.com/docs/connections/delivery-overview/) and [warehouse syncs](https://segment.com/docs/connections/storage/warehouses/warehouse-syncs/)\n" +
  "- [Rate limits](https://docs.segmentapis.com/tag/Rate-Limits/)";

export const CATALOG_COMMENT =
  "Twilio Segment's control plane as SQL: sources, destinations, the pipeline graph, warehouses and their syncs, tracking plans, event volume, Reverse ETL, Engage and IAM, read-only from the Segment Public API.";

export const CATALOG_DOC_LLM =
  "Read-only SQL access to a Twilio Segment workspace through the Segment Public API (api.segmentapis.com). " +
  "Segment has no API for reading raw events (they land in warehouses and destinations), so this worker " +
  "covers the control plane and its metrics: sources (never their write keys), destinations and Actions " +
  "mappings with secret settings redacted, the source → destination/warehouse connection graph, warehouses " +
  "with masked credentials and connection state, warehouse sync history, Functions (code only through " +
  "a per-id function), Reverse ETL models and syncs, labels, the audit trail, event volume and Delivery Overview " +
  "time series (schema main); Protocols tracking plans, rules and one row per planned property (schema " +
  "tracking); Engage spaces, audiences and computed traits (schema engage); users, groups and roles (schema " +
  "iam); and shared shapes in schema unified: warehouse and Reverse ETL syncs in the cross-orchestrator " +
  "shape, plus planned events, properties and audiences in the product-analytics vocabulary shared with " +
  "the Amplitude, Mixpanel and PostHog workers. Every object needs a `segment` DuckDB secret: TOKEN (a " +
  "Public API token) and optionally REGION ('us' default or 'eu') or BASE_URL. Ids and time ranges in WHERE " +
  "push down into API paths and parameters; long scans page with opaque cursors and honour 429 rate limits. " +
  "Plan-gated features (Engage, Reverse ETL, Functions, Audit Trail, Protocols) fail with an error naming " +
  "the feature when the workspace lacks it. Upstream docs: https://docs.segmentapis.com/ .";

export const CATALOG_DOC_MD =
  "# Segment\n\n" +
  "Query a Twilio Segment workspace's control plane from DuckDB: sources, destinations, the pipeline graph, " +
  "warehouses and syncs, tracking plans, event volume, Delivery Overview, Reverse ETL, Engage and IAM. " +
  "Read-only.\n\n" +
  "Create a `segment` secret with a Public API token (`TOKEN`, plus `REGION 'eu'` for EU workspaces). Write " +
  "keys are never returned and secret-looking settings are redacted. Ids and time windows in `WHERE` are " +
  "pushed into the API; paging and rate limits are handled for you.\n\n" +
  SOURCES_MD;

export const SCHEMA_COMMENT =
  "Segment control-plane objects and metrics: what collects data, where it goes and how it is loaded.";
export const TRACKING_COMMENT = "Segment Protocols tracking plans, with each rule's JSON Schema flattened to rows.";
export const ENGAGE_COMMENT = "Segment Engage / Unify metadata per Profiles space (no profiles).";
export const IAM_COMMENT = "Segment workspace users, groups and role grants for access reviews.";
export const UNIFIED_COMMENT = "Segment data in cross-worker shapes for UNION ALL with other Query.Farm workers.";

export const SCHEMA_DOC_MD =
  "## main\n\n" +
  "The Segment control plane: what collects data, where it goes and how it is loaded. Sources feed " +
  "destinations and warehouses; the connection graph, Actions mappings and Protocols schema controls " +
  "describe that pipeline, and sync histories show whether loads succeeded.\n\n" +
  "Event volume and Delivery Overview are time series with a granularity and a window; read dimension " +
  "columns become the API's groupBy. Write keys are never returned and secret settings are redacted.\n\n" +
  "Every object needs a `segment` secret. Ids and time ranges in `WHERE` are pushed into the API.";
export const SCHEMA_DOC_LLM =
  "The Segment control plane as tables (pushdown on ids and time ranges) plus table functions for time " +
  "series (event volume and Delivery Overview with start_time :=, end_time :=, granularity :=) and for " +
  "reading one Function's code by id. Per-parent tables (schema settings, mappings, connections, " +
  "warehouse and Reverse ETL syncs) take the parent id as a WHERE = / IN filter and otherwise read every " +
  "parent, one call each. Secret settings are redacted; source write keys are never returned.";

export const TRACKING_DOC_MD =
  "## tracking\n\n" +
  "Segment Protocols tracking plans: the agreed specification of which events, properties and traits a " +
  "source may send.\n\n" +
  "Each rule's JSON Schema is also flattened into one row per property or trait, with its type, required " +
  "flag, allowed values, pattern and bounds, so a plan can be audited or compared with plain SQL.\n\n" +
  "Filter on `tracking_plan_id` to read one plan only.";
export const TRACKING_DOC_LLM =
  "Segment Protocols tracking plans. Plans have a type (LIVE plans are connected to sources); each plan " +
  "has rules (TRACK, PAGE, SCREEN, IDENTIFY, GROUP) carrying a JSON Schema, and the flattened table has one " +
  "row per property or trait (dotted paths for nested objects, [] for array items) with data_type, " +
  "is_required, enum_values, pattern, minimum/maximum. WHERE tracking_plan_id = '…' reads one plan only.";
export const ENGAGE_DOC_MD =
  "## engage\n\n" +
  "Segment Engage / Unify metadata: Profiles spaces, the audiences defined in them (definition, size, " +
  "compute cadence) and computed traits.\n\n" +
  "Only metadata is read; profiles themselves are not listed. Needs Engage or Unify on the Segment " +
  "plan: without it the query fails with a 403 that names the feature.";
export const ENGAGE_DOC_LLM =
  "Engage / Unify metadata: spaces, audiences (definition query, size, REALTIME or BATCH cadence, type) and " +
  "computed traits per space. WHERE space_id = '…' reads one space; otherwise every space is listed first. " +
  "Needs Engage or Unify on the Segment plan (403 otherwise).";
export const IAM_DOC_MD =
  "## iam\n\n" +
  "Segment access management: who can sign in to the workspace, which user groups exist and which roles " +
  "they hold.\n\n" +
  "Role grants are listed with the resources (workspace, sources, spaces) and labels they are scoped to, " +
  "for access reviews.";
export const IAM_DOC_LLM =
  "Segment access management for access reviews: workspace users (name, email as Segment returns it, role " +
  "names and JSON permission grants scoped to resources or labels), user groups with member counts and " +
  "roles, and the role catalog. Use list_contains(role_names, 'Workspace Owner') to find owners.";
export const UNIFIED_DOC_MD =
  "## unified\n\n" +
  "Segment data in shapes other Query.Farm workers share, so one `UNION ALL` covers several tools.\n\n" +
  "Warehouse and Reverse ETL syncs appear as orchestrator runs next to Airflow, Dagster, Prefect or dbt; " +
  "planned events, properties and Engage audiences appear in the product-analytics vocabulary shared with " +
  "Amplitude, Mixpanel and PostHog (`tool = 'segment'`).";
export const UNIFIED_DOC_LLM =
  "Shared shapes for UNION ALL across workers. Runs: warehouse syncs and Reverse ETL syncs with " +
  "orchestrator = 'segment' and the same columns as the orchestrator workers' unified runs. Definitions: " +
  "planned events and properties from LIVE tracking plans in the product-analytics vocabulary " +
  "(tool = 'segment'); cohorts: Engage audiences. Native detail lives in schemas main, tracking and engage.";

export const AGENT_TEST_TASKS = [
  { name: "workspace_slug", prompt: "What is the slug of the Segment workspace?" },
  { name: "enabled_sources", prompt: "List the names of the enabled Segment sources." },
  {
    name: "destinations_per_type",
    prompt: "How many Segment destination instances are there of each destination type?",
  },
  { name: "web_outputs", prompt: "Which destinations and warehouses does the 'Website' source send data to?" },
  { name: "failed_warehouse_syncs", prompt: "Which Segment warehouse syncs failed, and what was the error?" },
  { name: "rows_synced", prompt: "How many rows were synced into each Segment warehouse in total?" },
  { name: "unplanned_blocked", prompt: "Which Segment sources block unplanned track events?" },
  {
    name: "order_completed_props",
    prompt: "Which properties are required on the 'Order Completed' event in the tracking plans?",
  },
  { name: "plan_enums", prompt: "What values are allowed for the 'plan' trait in the Segment tracking plans?" },
  { name: "workspace_owners", prompt: "Who are the Segment workspace owners?" },
  { name: "audit_changes", prompt: "Who modified destinations according to the Segment audit trail?" },
  { name: "retl_failures", prompt: "How many Segment Reverse ETL syncs failed?" },
  { name: "largest_audience", prompt: "Which Segment Engage audience is the largest?" },
  { name: "transform_functions", prompt: "How many Segment functions are there of each type?" },
  { name: "engage_inventory", prompt: "How many audiences and computed traits does each Segment Engage space have?" },
  { name: "group_roles", prompt: "Which roles does each Segment user group hold, and how many roles exist in total?" },
  {
    name: "delivery_failures",
    prompt: "Over the last week, why did events from source src_web fail to deliver to destination dst_amplitude?",
  },
  { name: "active_mappings", prompt: "List the enabled Segment Actions mappings with their triggers." },
  { name: "weekly_volume", prompt: "How many events did each Segment source receive in the last 7 days?" },
  { name: "function_code_size", prompt: "How long (in characters) is the code of each Segment destination function?" },
  { name: "prod_labels", prompt: "Which Segment labels exist, and which sources carry the environment:prod label?" },
  {
    name: "retl_models_by_mapping",
    prompt: "Which Segment Reverse ETL models are used by which destination mappings?",
  },
  { name: "warehouse_health", prompt: "Which Segment warehouses are failing their connection check?" },
  {
    name: "plan_coverage",
    prompt: "For each Segment tracking plan, how many rules does it have and which sources is it connected to?",
  },
  {
    name: "unified_runs_status",
    prompt: "Across all Segment syncs in the shared runs shape, how many runs ended in each status?",
  },
  {
    name: "shared_definitions",
    prompt:
      "In the shared product-analytics shape, how many planned events and how many planned user traits does Segment define, and what is the largest cohort?",
  },
];

/** Bind-only probes: each binds the object and shows its columns without fetching (LIMIT 0). */
export const EXECUTABLE_EXAMPLES = [
  {
    name: "sources_columns",
    description: "The sources table columns (binds only; create a segment secret and drop LIMIT 0 for rows)",
    sql: "SELECT id, name, enabled, source_type FROM segment.main.sources LIMIT 0",
  },
  {
    name: "properties_columns",
    description: "The tracking.properties columns (binds only; create a segment secret and drop LIMIT 0 for rows)",
    sql: "SELECT event_name, property_name, data_type, is_required FROM segment.tracking.properties LIMIT 0",
  },
];

export const CATALOG_TAGS: Record<string, string> = {
  "vgi.title": "Twilio Segment",
  "vgi.subtitle":
    "Segment sources, destinations, pipeline graph, warehouses, tracking plans, event volume and Reverse ETL as SQL tables.",
  "vgi.doc_llm": CATALOG_DOC_LLM,
  "vgi.doc_md": CATALOG_DOC_MD,
  "vgi.source_url": SOURCE_URL,
  "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
  "vgi.license": "0BSD",
  "vgi.author": "Query Farm LLC",
  "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
  "vgi.keywords": keywordsArray(
    "segment, twilio segment, cdp, customer data platform, sources, destinations, warehouses, tracking plans, protocols, reverse etl, engage, event volume, delivery overview",
  ),
  "vgi.support_contact": `${SOURCE_URL}/issues`,
  "vgi.support_policy_url": `${SOURCE_URL}#support`,
  // Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
  "vgi.agent_test_tasks": JSON.stringify(AGENT_TEST_TASKS),
  "vgi.executable_examples": JSON.stringify(EXECUTABLE_EXAMPLES),
};

const schemaTags = (title: string, keywords: string, llm: string, md: string, categories: unknown) => ({
  "vgi.title": title,
  domain: "analytics",
  "vgi.keywords": keywordsArray(keywords),
  "vgi.doc_llm": llm,
  "vgi.doc_md": md,
  "vgi.categories": JSON.stringify(categories),
});

export const SCHEMA_TAGS = schemaTags(
  "Segment Control Plane",
  "segment, sources, destinations, connections, warehouses, syncs, functions, reverse etl, audit, event volume",
  SCHEMA_DOC_LLM,
  SCHEMA_DOC_MD,
  MAIN_CATEGORIES,
);
export const TRACKING_SCHEMA_TAGS = schemaTags(
  "Segment Protocols Tracking Plans",
  "segment, protocols, tracking plans, rules, properties, json schema",
  TRACKING_DOC_LLM,
  TRACKING_DOC_MD,
  TRACKING_CATEGORIES,
);
export const ENGAGE_SCHEMA_TAGS = schemaTags(
  "Segment Engage",
  "segment, engage, unify, spaces, audiences, computed traits",
  ENGAGE_DOC_LLM,
  ENGAGE_DOC_MD,
  ENGAGE_CATEGORIES,
);
export const IAM_SCHEMA_TAGS = schemaTags(
  "Segment Access Management",
  "segment, iam, users, groups, roles, permissions",
  IAM_DOC_LLM,
  IAM_DOC_MD,
  IAM_CATEGORIES,
);
export const UNIFIED_SCHEMA_TAGS = schemaTags(
  "Shared Shapes",
  "segment, unified, runs, event definitions, property definitions, cohorts, product analytics",
  UNIFIED_DOC_LLM,
  UNIFIED_DOC_MD,
  UNIFIED_CATEGORIES,
);
