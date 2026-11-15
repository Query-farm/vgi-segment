/**
 * Time-series observability endpoints: event volume and Delivery Overview.
 *
 * Both take a granularity and a [startTime, endTime) window with a per-granularity
 * maximum range, so a long window is split into several requests (each one a step
 * of the resumable scan). The dimensions sent as `groupBy` are exactly the
 * dimension columns the query reads or filters on, so unread dimensions are never
 * fetched and a filtered dimension is always present for the SDK to re-check.
 */

import { timestampMicros, utf8, type VgiFunction } from "@query-farm/vgi";
import { eqValues, timeBounds, toEpochMs } from "./pushdown.ts";
import { C, defineSegmentTable, type Req, type Row, type ScanContext } from "./table.ts";

const M = "segment.main";
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

export const GRANULARITIES = ["DAY", "HOUR", "MINUTE"] as const;
/** Longest window one request may cover. */
export const MAX_RANGE_MS: Record<string, number> = { MINUTE: 4 * HOUR, HOUR: 14 * DAY, DAY: 30 * DAY };
/** Window used when neither arguments nor WHERE bounds give one. */
export const DEFAULT_LOOKBACK_MS: Record<string, number> = { MINUTE: HOUR, HOUR: DAY, DAY: 7 * DAY };
const BUCKET_MS: Record<string, number> = { MINUTE, HOUR, DAY };
/** How far back Segment keeps each granularity. */
export const RETENTION_MS: Record<string, number> = { MINUTE: 48 * HOUR, HOUR: 30 * DAY, DAY: 30 * DAY };

interface WindowArgs {
  start_time: unknown;
  end_time: unknown;
  granularity: string;
}

/** Resolve the [start, end) window: arguments first, then pushed `time` bounds, then the default lookback. */
export function resolveWindow<A extends WindowArgs>(scan: ScanContext<A>): { start: number; end: number; g: string } {
  const g = String(scan.args.granularity ?? "DAY").toUpperCase();
  const pushed = timeBounds(scan.constraints, "time");
  // Segment recommends lagging queries one minute behind the clock.
  const now = Math.floor((scan.anchorMs - MINUTE) / MINUTE) * MINUTE;
  const end = toEpochMs(scan.args.end_time) ?? pushed.upper ?? now;
  let start = toEpochMs(scan.args.start_time) ?? pushed.lower ?? end - (DEFAULT_LOOKBACK_MS[g] ?? 7 * DAY);
  if (scan.args.start_time == null) {
    // Buckets start on granularity boundaries: align a derived start down so its bucket is included,
    // but never before Segment's retention (older buckets do not exist; asking for them is a 422).
    const b = BUCKET_MS[g] ?? DAY;
    start = Math.floor(start / b) * b;
    const oldest = scan.anchorMs - (RETENTION_MS[g] ?? 30 * DAY) + MINUTE;
    if (start < oldest) start = Math.ceil(oldest / b) * b;
  }
  return { start, end, g };
}

/** Split [start, end) into chunks no longer than the granularity's maximum range. */
export function chunkWindow(start: number, end: number, g: string): [number, number][] {
  const max = MAX_RANGE_MS[g] ?? 30 * DAY;
  const out: [number, number][] = [];
  for (let s = start; s < end; s += max) out.push([s, Math.min(end, s + max)]);
  return out;
}

const iso = (ms: number) => new Date(ms).toISOString().replace(/\.000Z$/, "Z");

/** The dimensions to group by: the dimension columns the query reads or filters. */
function dims<A>(scan: ScanContext<A>, map: Record<string, string>): string[] {
  const out = new Set<string>();
  const filtered = new Set(scan.constraints.map((c) => c.column));
  for (const [col, dim] of Object.entries(map)) if (scan.projected.has(col) || filtered.has(col)) out.add(dim);
  return [...out];
}

/** Series → one row per (dimension values, bucket). */
function seriesRows(items: Row[], seriesKey: string): Row[] {
  return items.flatMap((it) =>
    (Array.isArray(it[seriesKey]) ? it[seriesKey] : []).map((p: Row) => ({
      ...it,
      __time: p.time,
      __count: p.count,
      __retry: p.retryCount,
    })),
  );
}

const windowArgs = {
  start_time: timestampMicros("UTC"),
  end_time: timestampMicros("UTC"),
  granularity: utf8(),
};
const windowDocs = {
  start_time:
    "Window start, inclusive (named). Default: the WHERE time >= bound, else end_time minus 7 days (DAY), 1 day (HOUR) or 1 hour (MINUTE).",
  end_time: "Window end, exclusive (named). Default: the WHERE time < bound, else now minus one minute.",
  granularity:
    "Bucket size: 'DAY' (default), 'HOUR' or 'MINUTE'. Longer windows than 30 days / 14 days / 4 hours are split into several requests.",
};

// ================================================================== event volume

type VolumeArgs = WindowArgs & Record<string, unknown>;

const VOLUME_DIMS = {
  source_id: "source",
  source_name: "source",
  source_slug: "source",
  event_name: "eventName",
  event_type: "eventType",
};

export const eventVolume: VgiFunction = defineSegmentTable<VolumeArgs>({
  name: "event_volume",
  description: "Event volume over time from GET /events/volume, grouped by the dimensions you select",
  title: "Event Volume",
  category: "observability",
  keywords: "segment, event volume, throughput, events per day, mtu, observability, ingestion, time series",
  docLlm:
    "Events received by the workspace over time (GET /events/volume): one row per bucket and dimension " +
    "combination with time (bucket start, TIMESTAMPTZ), granularity, source_id, source_name, source_slug, " +
    "event_name, event_type ('track', 'identify', 'page', 'screen', 'group', 'alias') and count. Only the " +
    "dimension columns the query reads are requested as groupBy, so `SELECT time, sum(count)` is a single " +
    "workspace total and adding event_name splits by event. Named arguments: start_time, end_time (an " +
    "inclusive/exclusive window), granularity ('DAY' default, 'HOUR', 'MINUTE'). Without arguments the window " +
    "comes from WHERE time >= … AND time < …, else the last 7 days (DAY). WHERE source_id / event_name / " +
    "event_type = or IN are sent as API filters. Segment keeps 30 days of DAY/HOUR buckets and 48 hours of MINUTE buckets; a WHERE bound older than that is clamped to the retention, an explicit start_time is not.",
  docMd:
    "## event_volume(start_time := NULL, end_time := NULL, granularity := 'DAY')\n\n" +
    "Event volume per time bucket; read dimension columns become `groupBy`.",
  pushdown:
    "`time` ranges → startTime/endTime (when no arguments); `source_id`, `event_name`, `event_type` (=, IN) → " +
    "sourceId/eventName/eventType; read dimension columns → groupBy; long windows split per granularity limit.",
  args: windowArgs,
  argDefaults: { start_time: null, end_time: null, granularity: "DAY" },
  argDocs: windowDocs,
  argConstraints: { granularity: { choices: [...GRANULARITIES] } },
  columns: [
    C.time("time", "Bucket start (UTC).", "__time", false),
    C.ctx("granularity", "Bucket size: DAY, HOUR or MINUTE.", "granularity"),
    C.str("source_id", "Source id (when grouped by source).", "source.id"),
    C.str("source_name", "Source name (when grouped by source).", "source.name"),
    C.str("source_slug", "Source slug (when grouped by source).", "source.slug"),
    C.str("event_name", "Event name (when grouped by event name).", "eventName"),
    C.str("event_type", "Event type: track, identify, page, screen, group, alias (when grouped).", "eventType"),
    C.big("count", "Events received in the bucket (count).", "__count", false),
  ],
  requests: (scan) => {
    const { start, end, g } = resolveWindow(scan);
    const groupBy = dims(scan, VOLUME_DIMS);
    const query = {
      granularity: g,
      groupBy: groupBy.length ? groupBy.join(",") : undefined,
      sourceId: eqValues(scan.constraints, "source_id"),
      eventName: eqValues(scan.constraints, "event_name"),
      eventType: eqValues(scan.constraints, "event_type"),
    };
    return chunkWindow(start, end, g).map(
      ([s, e]): Req => ({
        path: "/events/volume",
        items: "result",
        query: { ...query, startTime: iso(s), endTime: iso(e) },
        ctx: { granularity: g },
      }),
    );
  },
  rows: (items) => seriesRows(items, "series"),
  examples: [
    {
      sql: `SELECT event_name, sum(count) AS events FROM ${M}.event_volume(start_time := TIMESTAMPTZ '2026-11-01', end_time := TIMESTAMPTZ '2026-11-08') GROUP BY event_name ORDER BY events DESC LIMIT 10`,
      description: "Top events in the first week of November",
    },
    {
      sql: `SELECT time, sum(count) AS events FROM ${M}.event_volume(granularity := 'HOUR') WHERE source_id = 'src_web' GROUP BY time ORDER BY time`,
      description: "Hourly volume of one source over the last day (source filter pushed down)",
    },
  ],
});

// ============================================================= delivery overview

export const DELIVERY_STEPS: Record<string, { path: string; destination: boolean }> = {
  successfully_received: { path: "/delivery-overview/successfully-received", destination: false },
  failed_on_ingest: { path: "/delivery-overview/failed-on-ingest", destination: false },
  filtered_at_source: { path: "/delivery-overview/filtered-at-source", destination: false },
  filtered_at_destination: { path: "/delivery-overview/filtered-at-destination", destination: true },
  failed_delivery: { path: "/delivery-overview/failed-delivery", destination: true },
  successful_delivery: { path: "/delivery-overview/successful-delivery", destination: true },
};

type DeliveryArgs = WindowArgs &
  Record<string, unknown> & { step: string; source_id: string; destination_id: string | null };

const DELIVERY_DIMS = {
  event_name: "eventName",
  event_type: "eventType",
  discard_reason: "discardReason",
  app_version: "appVersion",
};

export const deliveryOverview: VgiFunction = defineSegmentTable<DeliveryArgs>({
  name: "delivery_overview",
  description: "Delivery Overview metrics for one pipeline step of a source (and destination) over time",
  title: "Delivery Overview",
  category: "observability",
  keywords: "segment, delivery overview, failed delivery, filtered events, discard reason, observability, retries",
  docLlm:
    "Segment Delivery Overview (GET /delivery-overview/<step>): event counts per time bucket for one step of " +
    "a source's pipeline. Arguments: step ('successfully_received', 'failed_on_ingest', 'filtered_at_source', " +
    "'filtered_at_destination', 'failed_delivery', 'successful_delivery'), source_id, destination_id (the " +
    "destination instance id; required for the last three steps), plus named start_time, end_time and " +
    "granularity. Rows: step, time, granularity, event_name, event_type, discard_reason, app_version, count " +
    "and retry_count. Only the dimension columns the query reads are requested as groupBy. Use it to find why " +
    "events were dropped (discard_reason) or failed to deliver.",
  docMd:
    "## delivery_overview(step, source_id, destination_id := NULL, start_time := NULL, end_time := NULL, granularity := 'DAY')\n\n" +
    "Delivery Overview counts for one pipeline step.",
  pushdown: "`time` ranges → startTime/endTime (when no arguments); read dimension columns → groupBy.",
  args: { step: utf8(), source_id: utf8(), destination_id: utf8(), ...windowArgs },
  argDefaults: { destination_id: null, start_time: null, end_time: null, granularity: "DAY" },
  argDocs: {
    step: "Pipeline step: successfully_received, failed_on_ingest, filtered_at_source, filtered_at_destination, failed_delivery or successful_delivery.",
    source_id: "Source id (from segment.main.sources).",
    destination_id: "Destination instance id (named); required for the destination steps.",
    ...windowDocs,
  },
  argConstraints: {
    step: { choices: Object.keys(DELIVERY_STEPS) },
    granularity: { choices: [...GRANULARITIES] },
  },
  columns: [
    C.ctx("step", "The pipeline step queried.", "step"),
    C.time("time", "Bucket start (UTC).", "__time", false),
    C.ctx("granularity", "Bucket size: DAY, HOUR or MINUTE.", "granularity"),
    C.str("event_name", "Event name (when grouped).", "eventName"),
    C.str("event_type", "Event type (when grouped).", "eventType"),
    C.str("discard_reason", "Why events were filtered or failed (when grouped).", "discardReason"),
    C.str("app_version", "App version (when grouped).", "appVersion"),
    C.big("count", "Events in the bucket (count).", "__count", false),
    C.big("retry_count", "Delivery retries in the bucket (count).", "__retry"),
  ],
  requests: (scan) => {
    const step = DELIVERY_STEPS[scan.args.step];
    if (!step) throw new Error(`unknown step '${scan.args.step}'`);
    if (step.destination && !scan.args.destination_id) {
      throw new Error(`delivery_overview: step '${scan.args.step}' needs destination_id := '<destination id>'`);
    }
    const { start, end, g } = resolveWindow(scan);
    const groupBy = dims(scan, DELIVERY_DIMS);
    return chunkWindow(start, end, g).map(
      ([s, e]): Req => ({
        path: step.path,
        items: "dataset",
        query: {
          sourceId: scan.args.source_id,
          destinationConfigId: step.destination ? (scan.args.destination_id ?? undefined) : undefined,
          granularity: g,
          startTime: iso(s),
          endTime: iso(e),
          groupBy: groupBy.length ? groupBy.join(",") : undefined,
        },
        ctx: { granularity: g, step: scan.args.step },
        feature: "Delivery Overview",
      }),
    );
  },
  rows: (items) => seriesRows(items, "series"),
  examples: [
    {
      sql: `SELECT discard_reason, sum(count) AS events FROM ${M}.delivery_overview('failed_delivery', 'src_web', destination_id := 'dst_amplitude') GROUP BY discard_reason ORDER BY events DESC`,
      description: "Why deliveries to a destination failed over the last week",
    },
  ],
});
