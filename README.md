<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A Query.Farm VGI worker for DuckDB.</em></p>

# Query Segment Sources, Destinations, Tracking Plans and Event Volume in DuckDB

> **vgi-segment** · a [Query.Farm](https://query.farm) VGI worker · written in TypeScript

[![CI](https://github.com/Query-farm/vgi-segment/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-segment/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

The Twilio Segment control plane as SQL, read-only, through the
[Segment Public API](https://docs.segmentapis.com/): which sources collect data and where
it goes, how warehouses and Reverse ETL syncs are doing, what the Protocols tracking plans
allow, how much traffic each source and event gets, and who changed what.

Segment has no API for reading raw events (they land in your warehouse and destinations),
so this worker covers the configuration, metrics and run history around them.

```sql
CREATE SECRET segment (TYPE segment, TOKEN 'sgp_…');            -- add REGION 'eu' for EU workspaces

-- The pipeline graph: every source → destination / warehouse edge.
SELECT source_name, target_type, target_name, target_enabled
FROM segment.main.connections ORDER BY ALL;

-- Required properties of every planned event (tracking-plan JSON Schema, one row per property).
SELECT event_name, property_name, data_type, enum_values
FROM segment.tracking.properties
WHERE section = 'properties' AND is_required ORDER BY ALL;

-- Top events this week (groupBy = eventName only, because that is the dimension read).
SELECT event_name, sum(count) AS events
FROM segment.main.event_volume() GROUP BY event_name ORDER BY events DESC LIMIT 10;

-- Failed warehouse syncs and why.
SELECT warehouse_name, source_id, started_at, error_message
FROM segment.main.warehouse_syncs WHERE status = 'FAILED' ORDER BY started_at DESC;
```

## Tables and functions

All objects need a `segment` secret. Ids and time ranges in `WHERE` are pushed into the API
(see *Pushdown*); everything else is filtered by DuckDB.

**`segment.main`**: the control plane

| Object | What it returns |
|---|---|
| `workspaces` | The workspace the token belongs to (id, slug, name) |
| `sources` | Sources: type, enabled, labels (`'key:value'`), `write_key_count`, masked settings. **Write keys are never returned.** |
| `source_schema_settings` | Per-source Protocols controls: unplanned events/traits allowed, violation policy, forwarding |
| `destinations` | Destination instances: type, feeding source, `is_actions`, masked settings |
| `destination_subscriptions` | Actions mappings: action, FQL trigger, Reverse ETL model and schedule |
| `connections` | The source → destination / warehouse edge list |
| `warehouses` | Warehouses with masked credentials and (when read) `connection_state` |
| `warehouse_syncs` | Warehouse sync history: status, rows, duration, notices, first error |
| `functions` | Source/destination/insert functions (metadata only) |
| `function_code(function_id)` | One function's code and setting definitions (code is opt-in) |
| `reverse_etl_models` | Reverse ETL models and their SQL |
| `reverse_etl_syncs` | Reverse ETL runs per mapping: status, extract/delivery counts, errors |
| `labels` | Workspace labels |
| `audit_events` | Audit trail (who changed what) |
| `event_volume(start_time :=, end_time :=, granularity :=)` | `/events/volume` time series by source, event name, event type |
| `delivery_overview(step, source_id, destination_id :=, …)` | Delivery Overview counts for one pipeline step, by event / discard reason / app version |

**`segment.tracking`**: Protocols tracking plans: `plans`, `rules` (one per event/identify/group rule,
with its JSON Schema), `properties` (each rule's schema flattened: one row per property or trait with
`data_type`, `is_required`, `is_nullable`, `enum_values`, `pattern`, `format`, `minimum`/`maximum`, …;
nested paths like `address.city`, array items like `products[].sku`) and `plan_sources`.

**`segment.engage`**: `spaces`, `audiences`, `computed_traits` (metadata).

**`segment.iam`**: `users` (name, email as Segment returns it, `role_names`, permission grants), `user_groups`, `roles`.

**`segment.unified`**: shared shapes for `UNION ALL` across Query.Farm workers:

- `runs`: warehouse syncs and Reverse ETL syncs in the cross-orchestrator `unified.runs` shape
  (`orchestrator = 'segment'`, `pipeline` = `warehouse/<warehouse>/<source>` or
  `reverse_etl/<model>/<mapping>`, status in queued/running/success/failed/cancelled/skipped).
- `event_definitions`, `property_definitions`, `cohorts`: the product-analytics vocabulary shared
  with the Amplitude, Mixpanel and PostHog workers (`tool = 'segment'`), from LIVE tracking plans
  and Engage audiences.

Not exposed because the Public API has no endpoint for them: raw events, Protocols violation
counts and per-profile data.

## Secrets never leak

- Source write keys are never returned; `write_key_count` says how many exist, and a write key that
  appears inside some other setting is scrubbed too.
- Every `settings` map (sources, destinations, warehouses, Actions mappings) is deep-copied with
  secret-looking keys (`apiKey`, `secret`, `password`, `token`, `privateKey`, `credentials`,
  `Authorization`, …) replaced by `<redacted>` and URL user-info stripped
  (`https://<redacted>@host/…`). `masked_settings` lists which keys were redacted, so you can audit
  what is configured without seeing values.
- The Public API token lives only in the DuckDB secret: never in scan state, logs or error messages.

The SQL tests plant fake write keys, API keys, passwords and private keys in the fixture and check
that none of them reaches any column.

## Authentication

```sql
CREATE SECRET segment (TYPE segment, TOKEN '<public api token>');               -- US workspace
CREATE SECRET segment_eu (TYPE segment, TOKEN '<token>', REGION 'eu');          -- eu1.api.segmentapis.com
CREATE SECRET segment_proxy (TYPE segment, TOKEN '<token>', BASE_URL 'https://segment-proxy.internal');
```

Create the token under *Workspace settings > Access Management > Tokens*; a read-only role is
enough. Plan-gated features (Engage, Reverse ETL, Functions, the Audit Trail, Protocols,
Delivery Overview) fail with Segment's 403/404 plus a hint naming the feature, so a missing
feature is easy to tell from a bad token.

## Pushdown

| Object | Pushed into the API |
|---|---|
| `sources`, `destinations`, `warehouses`, `reverse_etl_models`, `tracking.plans` | `id = / IN` → `GET /<collection>/{id}` |
| `destinations` | `source_id = / IN` → `/sources/{id}/connected-destinations` |
| `source_schema_settings`, `connections` | `source_id = / IN` (no source listing); `connections.target_type` skips the other edge kind |
| `destination_subscriptions` | `destination_id = / IN` |
| `warehouse_syncs` | `warehouse_id = / IN`; `source_id =` → `/connected-sources/{id}/syncs` |
| `reverse_etl_syncs` | `model_id` + `subscription_id =` → that mapping only; `destination_id`; `model_id` skips other mappings |
| `functions` | `resource_type = / IN` → `resourceType` |
| `audit_events` | `timestamp` range → `startTime`/`endTime`; `resource_id =`, `resource_type =` |
| `tracking.*` | `tracking_plan_id = / IN`; `plans.type` / `plan_type =` → `type` |
| `engage.audiences`, `computed_traits` | `space_id = / IN` |
| `event_volume`, `delivery_overview` | `time` range → window; `source_id`/`event_name`/`event_type` → filters; read dimension columns → `groupBy` |
| `warehouses.connection_state` | fetched (one call per warehouse) only when the column is read |

Time series windows: explicit `start_time`/`end_time` win; otherwise the `WHERE time` bounds;
otherwise the last 7 days (DAY), 1 day (HOUR) or 1 hour (MINUTE). Windows longer than Segment's
per-request limit (30 days / 14 days / 4 hours) are split into several requests. A derived start
older than Segment's retention (30 days; 48 hours for MINUTE) is clamped.

## Paging and rate limits

List endpoints page with `pagination.count` / `pagination.cursor` (`count` / `cursor` for Reverse
ETL syncs). The scan keeps its work queue and the opaque cursor in serializable state, so a long
scan resumes on any worker process over HTTP. 429 responses wait for `Retry-After` (or
`X-RateLimit-Reset`) and retry; 5xx and network errors retry with exponential backoff.

## Install

**Prerequisites:** [Bun](https://bun.sh) 1.3+ (for a source checkout).

```bash
git clone https://github.com/Query-farm/vgi-segment.git && cd vgi-segment
bun install --frozen-lockfile
```

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
ATTACH 'segment' (TYPE vgi, LOCATION '/abs/path/to/vgi-segment/bin/vgi-segment-worker');
```

**Haybarn 2.0** (`uvx haybarn-cli`):

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'segment' (TYPE vgi, LOCATION '/abs/path/to/vgi-segment/bin/vgi-segment-worker');
```

`LOCATION` is the command DuckDB runs, so use an absolute path. Tagged releases ship signed
standalone executables (no Bun needed); point `LOCATION` at the downloaded binary. The same
binary serves HTTP with `--http --port 8000` (see the `Dockerfile`).

## Testing

```bash
make test-unit                 # bun unit tests (masking, JSON Schema flattening, windows, retries)
make test-sql                  # test/sql/*.test through Haybarn 2.0 (stdio)
make test-sql TRANSPORT=http   # …over HTTP (also: launch)
make check                     # everything CI runs
```

No test touches the network or a real workspace. `test/mock_api/server.py` is a stdlib-only
replay server over a hand-built workspace (`test/fixtures/workspace.json`, shaped after the
Public API's documented models). It enforces the bearer token (and a second token whose plan
lacks Engage, Reverse ETL, Functions and the Audit Trail), rejects unknown or missing query
parameters, pages two items at a time with opaque cursors, enforces the event-volume window and
retention limits, answers one 429 and one 503, and keeps a `/__requests` log the SQL tests read to
check exactly what was pushed down.

The metadata check runs with `execute: false` in CI: every object needs a `segment` secret for a
live workspace.

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm
