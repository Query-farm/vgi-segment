# Changelog

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

Targets: @query-farm/vgi 0.50 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
