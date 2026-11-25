# Changelog

## 0.2.0 (2026-11-25)

- VGI protocol 3.4: bump @query-farm/vgi to 0.52.3 and @query-farm/vgi-rpc to 0.30.3. **Requires the
  3.4 vgi extension**: DuckDB 2.0 (`INSTALL vgi FROM query_farm`) or Haybarn 2.0.1+; Haybarn 2.0.0
  still serves the 3.3 extension, which cannot attach this worker.
- The Docker image supports Orchard hosted mode (`VGI_ORCHARD_HOSTED=1`): HTTP only, Orchard Pass
  authentication, large values through the Orchard blob vendor; the HEALTHCHECK uses `HEAD /health`.
- CI and `make` run the SQL tests on haybarn-unittest 2.0.1rc3.

## 0.1.1 (2026-11-16)

- Bump @query-farm/vgi to 0.50.1.
- CI tooling (haybarn-unittest, lint helpers) runs on Python 3.14.

## 0.1.0 (unreleased)

First release: read-only Segment Public API access from DuckDB.

- `main`: workspaces, sources (write keys never returned), source_schema_settings, destinations,
  destination_subscriptions, connections, warehouses (+ connection_state), warehouse_syncs,
  functions, function_code(id), labels, audit_events, reverse_etl_models, reverse_etl_syncs,
  event_volume(), delivery_overview().
- `tracking`: plans, rules, properties (JSON Schema flattened per property), plan_sources.
- `engage`: spaces, audiences, computed_traits. `iam`: users, user_groups, roles.
- `unified`: runs (cross-orchestrator shape), event_definitions, property_definitions, cohorts
  (shared product-analytics vocabulary).
- `segment` secret: TOKEN plus REGION ('us' / 'eu') or BASE_URL.
- Secret-looking settings redacted everywhere; pushdown of ids, time windows and groupBy;
  cursor paging in serializable scan state; 429/5xx backoff; plan-gated feature errors.

Targets: @query-farm/vgi 0.50 · VGI protocol 3.3 · Haybarn 2.0.1rc3 · vgi-lint-check 2.x (unpinned)
