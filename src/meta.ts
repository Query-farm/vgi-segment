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
export const VERSION = "0.1.0";

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
  "function_code), Reverse ETL models and syncs, labels, the audit trail, event volume and Delivery Overview " +
  "time series (schema main); Protocols tracking plans, rules and one row per planned property (schema " +
  "tracking); Engage spaces, audiences and computed traits (schema engage); users, groups and roles (schema " +
  "iam); and shared shapes in schema unified: runs (warehouse + Reverse ETL syncs in the cross-orchestrator " +
  "shape), event_definitions, property_definitions and cohorts (the product-analytics vocabulary shared with " +
  "the Amplitude, Mixpanel and PostHog workers). Every object needs a `segment` DuckDB secret: TOKEN (a " +
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

export const SCHEMA_DOC_MD =
  "## main\n\n" +
  "The Segment control plane: workspace, sources, destinations, Actions mappings, connections, warehouses " +
  "and syncs, functions, Reverse ETL, labels, the audit trail, and the event-volume / Delivery Overview " +
  "time series. All objects need a `segment` secret.";
export const SCHEMA_DOC_LLM =
  "Segment control-plane objects as tables (pushdown on ids and time ranges) plus three table functions: " +
  "event_volume(start_time :=, end_time :=, granularity :=) and delivery_overview(step, source_id, " +
  "destination_id :=, …) for time series, and function_code(function_id) for a function's code. " +
  "Per-parent tables (source_schema_settings, destination_subscriptions, connections, warehouse_syncs, " +
  "reverse_etl_syncs) take the parent id as a WHERE = / IN filter and otherwise read every parent.";

export const TRACKING_DOC_MD =
  "## tracking\n\nProtocols tracking plans: plans, rules (JSON Schema), one row per planned property, and connected sources.";
export const TRACKING_DOC_LLM =
  "Segment Protocols tracking plans. plans lists them; rules has one row per event/identify/group rule with " +
  "its JSON Schema; properties flattens every rule's schema into one row per property or trait (type, " +
  "required, enum values, pattern); plan_sources links plans to sources. WHERE tracking_plan_id = '…' reads " +
  "one plan only.";
export const ENGAGE_DOC_MD = "## engage\n\nEngage / Unify spaces, audiences and computed traits (metadata).";
export const ENGAGE_DOC_LLM =
  "Engage / Unify metadata: spaces, audiences (definition, size, cadence) and computed traits per space. " +
  "WHERE space_id = '…' reads one space. Needs Engage or Unify on the Segment plan.";
export const IAM_DOC_MD = "## iam\n\nWorkspace users, user groups and roles.";
export const IAM_DOC_LLM =
  "Segment access management: users (name, email, role names, permission grants), user groups and roles. " +
  "Use for access reviews.";
export const UNIFIED_DOC_MD =
  "## unified\n\nSegment rows in shared shapes: `runs` (cross-orchestrator) and the product-analytics " +
  "vocabulary (`event_definitions`, `property_definitions`, `cohorts`).";
export const UNIFIED_DOC_LLM =
  "Shared shapes for UNION ALL across workers. runs: warehouse syncs and Reverse ETL syncs as " +
  "orchestrator = 'segment' runs (same columns as airflow/dagster/prefect/dbt unified.runs). " +
  "event_definitions / property_definitions: planned events and properties from LIVE tracking plans in the " +
  "vocabulary shared with amplitude/mixpanel/posthog (tool = 'segment'). cohorts: Engage audiences.";

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
