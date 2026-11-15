#!/usr/bin/env python3
"""An offline Segment Public API replay server for the tests (stdlib only).

It serves the hand-built workspace in ``test/fixtures/workspace.json`` (shapes taken
from the documented Public API models) and behaves like api.segmentapis.com where the
worker depends on it:

* **Auth.** ``Authorization: Bearer test-token`` sees everything. ``Bearer
  starter-token`` is a workspace without Engage, Reverse ETL, Functions or the Audit
  Trail: those endpoints answer 403 ``forbidden``, as a plan-gated feature does.
  Anything else is a 401 ``unauthorized``.
* **Strict parameters.** Each route accepts only its documented query parameters; an
  unknown parameter, a missing required one or a bad value is a 422 ``validation``
  error. List routes require ``pagination.count`` (1..1000; Reverse ETL syncs use
  ``count`` 1..100 + ``cursor``) and return at most 2 items per page, so paging always
  runs. Cursors are opaque.
* **Metrics.** ``/events/volume`` and ``/delivery-overview/<step>`` build time buckets
  from per-minute rates in the fixture, enforce the per-granularity window limits
  (MINUTE 4 h / 48 h retention, HOUR 14 d, DAY 30 d; 30 d retention) and group by the
  ``groupBy`` dimensions (comma-delimited); array filters use the indexed form
  ``sourceId.0=…``.
* **Rate limits.** The first ``/tracking-plans`` list request answers 429 with
  ``Retry-After: 1``; the first ``/labels`` request answers 503.
* **Introspection.** ``GET /__requests?path=/sources`` returns every recorded request
  to that path (``seq``, ``path``, ``query`` as a JSON object string, ``status``), so SQL tests can
  assert exactly what was pushed down.

Undocumented fixture-only keys (warehouse ``connectionState``/``sources``, plan
``rules``/``sources``) never appear in responses. Source ``writeKeys`` and secret
settings DO appear, exactly as the real API returns them: the worker must drop them.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

HERE = Path(__file__).resolve().parent
WS = json.loads((HERE.parent / "fixtures" / "workspace.json").read_text())
FULL_TOKEN = "test-token"
LIMITED_TOKEN = "starter-token"
PAGE_CAP = 2
LOCK = threading.Lock()
REQUESTS: list[dict] = []
ONCE: set[str] = set()

MINUTE = dt.timedelta(minutes=1)
BUCKET = {"MINUTE": MINUTE, "HOUR": dt.timedelta(hours=1), "DAY": dt.timedelta(days=1)}
MAX_RANGE = {"MINUTE": dt.timedelta(hours=4), "HOUR": dt.timedelta(days=14), "DAY": dt.timedelta(days=30)}
RETENTION = {"MINUTE": dt.timedelta(hours=48), "HOUR": dt.timedelta(days=30), "DAY": dt.timedelta(days=30)}
DELIVERY_STEPS = {
    "successfully-received": ("successfully_received", False),
    "failed-on-ingest": ("failed_on_ingest", False),
    "filtered-at-source": ("filtered_at_source", False),
    "filtered-at-destination": ("filtered_at_destination", True),
    "failed-delivery": ("failed_delivery", True),
    "successful-delivery": ("successful_delivery", True),
}
GATED = (r"^/spaces", r"^/reverse-etl-models", r"^/audit-events", r"^/functions")
HIDDEN_KEYS = {"connectionState", "sources", "rules"}


class ApiError(Exception):
    def __init__(self, status: int, kind: str, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status, self.kind, self.message, self.headers = status, kind, message, headers or {}


def clean(obj: dict) -> dict:
    return {k: v for k, v in obj.items() if k not in HIDDEN_KEYS}


def by_id(items: list[dict], id_: str, what: str) -> dict:
    for it in items:
        if it["id"] == id_:
            return it
    raise ApiError(404, "not-found", f"{what} {id_} not found")


def parse_time(v: str, name: str) -> dt.datetime:
    try:
        t = dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        raise ApiError(422, "validation", f"{name} must be an ISO8601 timestamp") from None
    if t.tzinfo is None:
        raise ApiError(422, "validation", f"{name} must include a time zone")
    return t


def iso(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Params:
    """Query parameters with strict, per-route validation."""

    def __init__(self, raw: list[tuple[str, str]]):
        self.raw = raw
        self.seen: set[str] = set()
        self.values: dict[str, str] = {}
        for k, v in raw:
            if k in self.values:
                raise ApiError(422, "validation", f"duplicate parameter {k}")
            self.values[k] = v

    def get(self, name: str, required: bool = False) -> str | None:
        self.seen.add(name)
        v = self.values.get(name)
        if required and v is None:
            raise ApiError(422, "validation", f"{name} is required")
        return v

    def array(self, name: str) -> list[str] | None:
        """Indexed array form: name.0=a&name.1=b."""
        out: dict[int, str] = {}
        for k, v in self.values.items():
            m = re.fullmatch(re.escape(name) + r"\.(\d+)", k)
            if m:
                self.seen.add(k)
                out[int(m.group(1))] = v
        if not out:
            return None
        if sorted(out) != list(range(len(out))):
            raise ApiError(422, "validation", f"{name} indexes must be 0..n-1")
        return [out[i] for i in range(len(out))]

    def done(self) -> None:
        extra = sorted(set(self.values) - self.seen)
        if extra:
            raise ApiError(422, "validation", f"unknown parameter(s): {', '.join(extra)}")


def paginate(p: Params, items: list, key: str, style: str = "pagination", max_count: int = 1000) -> dict:
    if style == "pagination":
        count_s, cursor = p.get("pagination.count", required=True), p.get("pagination.cursor")
    else:
        count_s, cursor = p.get("count", required=True), p.get("cursor")
    p.done()
    if not re.fullmatch(r"\d+", count_s or "") or not 1 <= int(count_s) <= max_count:
        raise ApiError(422, "validation", f"count must be between 1 and {max_count}")
    start = 0
    if cursor:
        try:
            start = int(base64.b64decode(cursor).decode())
        except Exception:
            raise ApiError(422, "validation", "invalid pagination cursor") from None
    n = min(int(count_s), PAGE_CAP)
    page = items[start : start + n]
    enc = lambda i: base64.b64encode(str(i).encode()).decode()  # noqa: E731
    nxt = enc(start + n) if start + n < len(items) else None
    return {
        key: page,
        "pagination": {
            "current": enc(start),
            "next": nxt,
            "previous": enc(max(0, start - n)) if start else None,
            "totalEntries": len(items),
        },
    }


def one(p: Params, key: str, obj: dict) -> dict:
    p.done()
    return {key: obj}


def source_brief(sid: str) -> dict:
    s = by_id(WS["sources"], sid, "source")
    return {"id": s["id"], "name": s["name"], "slug": s["slug"]}


def metric_window(p: Params) -> tuple[str, dt.datetime, dt.datetime]:
    g = p.get("granularity", required=True)
    if g not in BUCKET:
        raise ApiError(422, "validation", "granularity must be DAY, HOUR or MINUTE")
    start = parse_time(p.get("startTime", required=True) or "", "startTime")
    end = parse_time(p.get("endTime", required=True) or "", "endTime")
    now = dt.datetime.now(dt.timezone.utc)
    if end <= start:
        raise ApiError(422, "validation", "endTime must be after startTime")
    if end - start > MAX_RANGE[g]:
        raise ApiError(422, "validation", f"time range too large for granularity {g}")
    if start < now - RETENTION[g] - dt.timedelta(minutes=5):
        raise ApiError(422, "validation", f"startTime is older than the retention for granularity {g}")
    return g, start, end


def buckets(g: str, start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    step = BUCKET[g]
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    t = epoch + ((start - epoch) // step) * step
    out = []
    while t < end:
        out.append(t)
        t += step
    return out


def group_by(p: Params, allowed: set[str]) -> list[str]:
    raw = p.get("groupBy")
    dims = [d for d in (raw or "").split(",") if d]
    bad = [d for d in dims if d not in allowed]
    if bad:
        raise ApiError(422, "validation", f"invalid groupBy value(s): {', '.join(bad)}")
    return dims


def series_items(rows: list[dict], dims: list[str], g: str, bs: list[dt.datetime], dim_value) -> list[dict]:
    per_bucket = BUCKET[g] // MINUTE
    groups: dict[tuple, dict] = {}
    for r in rows:
        key = tuple(dim_value(r, d) for d in dims)
        grp = groups.setdefault(key, {"rate": 0, "retry": 0, "row": r})
        grp["rate"] += r["rate"]
        grp["retry"] += r.get("retry", 0)
    items = []
    for key in sorted(groups, key=lambda k: tuple("" if x is None else str(x) for x in k)):
        grp = groups[key]
        item: dict = {}
        for d, v in zip(dims, key):
            item[d] = source_brief(v) if d == "source" else v
        series = [{"time": iso(t), "count": grp["rate"] * per_bucket, "retryCount": grp["retry"] * per_bucket} for t in bs]
        item["total"] = sum(s["count"] for s in series)
        item["series"] = series
        item["_totalRetry"] = sum(s["retryCount"] for s in series)
        items.append(item)
    return items


def events_volume(p: Params) -> dict:
    g, start, end = metric_window(p)
    dims = group_by(p, {"eventName", "eventType", "source"})
    filters = {"source": p.array("sourceId"), "eventName": p.array("eventName"), "eventType": p.array("eventType")}
    p.array("appVersion")
    rows = [r for r in WS["event_volume_per_minute"] if all(f is None or r[k] in f for k, f in filters.items())]
    items = series_items(rows, dims, g, buckets(g, start, end), lambda r, d: r[d])
    for it in items:
        it.pop("_totalRetry")
        for s in it["series"]:
            s.pop("retryCount")
    out = paginate(p, items, "result")
    out["path"] = "/observability/event_volume/v2.json"
    out["query"] = {"workspaceId": WS["workspace"]["id"], "granularity": g, "startTime": iso(start), "endTime": iso(end)}
    return out


def delivery(p: Params, slug: str) -> dict:
    step, needs_dest = DELIVERY_STEPS[slug]
    source = p.get("sourceId", required=True)
    by_id(WS["sources"], source or "", "source")
    dest = p.get("destinationConfigId", required=needs_dest)
    if dest and not needs_dest:
        raise ApiError(422, "validation", "destinationConfigId is not accepted for a source step")
    g, start, end = metric_window(p)
    dims = group_by(p, {"eventName", "eventType", "discardReason", "appVersion"})
    rows = [
        r
        for r in WS["delivery_per_minute"]
        if r["step"] == step and r["source"] == source and (not needs_dest or r.get("destination") == dest)
    ]
    items = series_items(rows, dims, g, buckets(g, start, end), lambda r, d: r.get(d))
    for it in items:
        it["totalRetryCount"] = it.pop("_totalRetry")
    out = paginate(p, items, "dataset")
    out["total"] = sum(i["total"] for i in items)
    return out


def route(path: str, p: Params) -> dict:  # noqa: C901 - one table of routes
    seg = [s for s in path.split("/") if s]
    if path == "/":
        return one(p, "workspace", WS["workspace"])
    if seg == ["labels"]:
        if "labels" not in ONCE:
            ONCE.add("labels")
            raise ApiError(503, "unavailable", "try again")
        p.done()
        return {"labels": WS["labels"]}
    if seg == ["sources"]:
        return paginate(p, WS["sources"], "sources")
    if len(seg) >= 2 and seg[0] == "sources":
        src = by_id(WS["sources"], seg[1], "source")
        if len(seg) == 2:
            return one(p, "source", src)
        if seg[2:] == ["settings"]:
            settings = WS["source_settings"].get(src["id"], {})
            return one(p, "settings", settings) | {"sourceId": src["id"]}
        if seg[2:] == ["connected-destinations"]:
            return paginate(p, [d for d in WS["destinations"] if d["sourceId"] == src["id"]], "destinations")
        if seg[2:] == ["connected-warehouses"]:
            whs = [clean(w) for w in WS["warehouses"] if src["id"] in w["sources"]]
            return paginate(p, whs, "warehouses")
    if seg == ["destinations"]:
        return paginate(p, WS["destinations"], "destinations")
    if len(seg) >= 2 and seg[0] == "destinations":
        d = by_id(WS["destinations"], seg[1], "destination")
        if len(seg) == 2:
            return one(p, "destination", d)
        if seg[2:] == ["subscriptions"]:
            return paginate(p, WS["subscriptions"].get(d["id"], []), "subscriptions")
    if seg == ["warehouses"]:
        return paginate(p, [clean(w) for w in WS["warehouses"]], "warehouses")
    if len(seg) >= 2 and seg[0] == "warehouses":
        w = by_id(WS["warehouses"], seg[1], "warehouse")
        if len(seg) == 2:
            return one(p, "warehouse", clean(w))
        if seg[2:] == ["connection-state"]:
            p.done()
            return {"connectionState": w["connectionState"]}
        if seg[2:] == ["syncs"]:
            return paginate(p, WS["syncs"].get(w["id"], []), "reports")
        if len(seg) == 5 and seg[2] == "connected-sources" and seg[4] == "syncs":
            if seg[3] not in w["sources"]:
                raise ApiError(404, "not-found", f"source {seg[3]} is not connected to {w['id']}")
            return paginate(p, [s for s in WS["syncs"].get(w["id"], []) if s["sourceId"] == seg[3]], "reports")
    if seg == ["functions"]:
        rt = p.get("resourceType", required=True)
        if rt not in {"SOURCE", "DESTINATION", "INSERT_SOURCE", "INSERT_DESTINATION", "INSERT_TRANSFORMATION"}:
            raise ApiError(422, "validation", "invalid resourceType")
        keep = ("id", "resourceType", "createdAt", "createdBy", "displayName", "description", "logoUrl", "catalogId")
        fns = [{k: f.get(k) for k in keep} for f in WS["functions"] if f["resourceType"] == rt]
        return paginate(p, fns, "functions")
    if len(seg) == 2 and seg[0] == "functions":
        return one(p, "function", by_id(WS["functions"], seg[1], "function"))
    if seg == ["audit-events"]:
        evs = WS["audit_events"]
        st, en = p.get("startTime"), p.get("endTime")
        rid, rtype = p.get("resourceId"), p.get("resourceType")
        if st:
            evs = [e for e in evs if parse_time(e["timestamp"], "t") >= parse_time(st, "startTime")]
        if en:
            evs = [e for e in evs if parse_time(e["timestamp"], "t") < parse_time(en, "endTime")]
        if rid:
            evs = [e for e in evs if e["resourceId"] == rid]
        if rtype:
            evs = [e for e in evs if e["resourceType"] == rtype]
        return paginate(p, sorted(evs, key=lambda e: e["timestamp"], reverse=True), "events")
    if seg == ["reverse-etl-models"]:
        return paginate(p, WS["reverse_etl_models"], "models")
    if len(seg) == 2 and seg[0] == "reverse-etl-models":
        return one(p, "reverseEtlModel", by_id(WS["reverse_etl_models"], seg[1], "model"))
    if len(seg) == 5 and seg[0] == "reverse-etl-models" and seg[2] == "subscriptionId" and seg[4] == "syncs":
        by_id(WS["reverse_etl_models"], seg[1], "model")
        key = f"{seg[1]}/{seg[3]}"
        if key not in WS["reverse_etl_syncs"]:
            raise ApiError(404, "not-found", f"subscription {seg[3]} of model {seg[1]} not found")
        return paginate(p, WS["reverse_etl_syncs"][key], "syncStatuses", style="plain", max_count=100)
    if seg == ["tracking-plans"]:
        if "tracking-plans" not in ONCE:
            ONCE.add("tracking-plans")
            raise ApiError(429, "too-many-requests", "rate limit exceeded", {"Retry-After": "1"})
        t = p.get("type")
        if t is not None and t not in {"ENGAGE", "LIVE", "PROPERTY_LIBRARY", "RULE_LIBRARY", "TEMPLATE"}:
            raise ApiError(422, "validation", "invalid type")
        plans = [clean(tp) for tp in WS["tracking_plans"] if t is None or tp["type"] == t]
        return paginate(p, plans, "trackingPlans")
    if len(seg) >= 2 and seg[0] == "tracking-plans":
        tp = by_id(WS["tracking_plans"], seg[1], "tracking plan")
        if len(seg) == 2:
            return one(p, "trackingPlan", clean(tp))
        if seg[2:] == ["rules"]:
            return paginate(p, tp["rules"], "rules")
        if seg[2:] == ["sources"]:
            return paginate(p, [by_id(WS["sources"], s, "source") for s in tp["sources"]], "sources")
    if seg == ["spaces"]:
        return paginate(p, WS["spaces"], "spaces")
    if len(seg) == 3 and seg[0] == "spaces":
        sp = by_id(WS["spaces"], seg[1], "space")
        if seg[2] == "audiences":
            p.get("include")
            p.get("search")
            return paginate(p, WS["audiences"].get(sp["id"], []), "audiences")
        if seg[2] == "computed-traits":
            return paginate(p, WS["computed_traits"].get(sp["id"], []), "computedTraits")
    if seg == ["users"]:
        return paginate(p, WS["users"], "users")
    if seg == ["groups"]:
        return paginate(p, WS["groups"], "userGroups")
    if seg == ["roles"]:
        return paginate(p, WS["roles"], "roles")
    if seg == ["events", "volume"]:
        return events_volume(p)
    if len(seg) == 2 and seg[0] == "delivery-overview" and seg[1] in DELIVERY_STEPS:
        return delivery(p, seg[1])
    raise ApiError(404, "not-found", f"no route for {path}")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep stdout clean (it carries PORT:<n>)
        sys.stderr.write("mock: " + fmt % args + "\n")

    def send(self, status: int, body: object, headers: dict | None = None) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/vnd.segment.v1+json" if status < 400 else "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-RateLimit-Limit", "10")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        raw = parse_qsl(u.query, keep_blank_values=True)
        if u.path == "/__requests":
            want = dict(raw).get("path")
            with LOCK:
                rows = [r for r in REQUESTS if want is None or r["path"] == want]
            self.send(200, rows)
            return
        entry = {"seq": 0, "path": u.path, "query": json.dumps(dict(raw), sort_keys=True), "status": 0}
        with LOCK:
            entry["seq"] = len(REQUESTS) + 1
            REQUESTS.append(entry)
        try:
            auth = self.headers.get("Authorization", "")
            if auth not in (f"Bearer {FULL_TOKEN}", f"Bearer {LIMITED_TOKEN}"):
                raise ApiError(401, "unauthorized", "The token is invalid or expired")
            if auth == f"Bearer {LIMITED_TOKEN}" and any(re.match(g, u.path) for g in GATED):
                raise ApiError(403, "forbidden", "This feature is not enabled for your workspace")
            data = route(u.path, Params(raw))
            entry["status"] = 200
            self.send(200, {"data": data})
        except ApiError as e:
            entry["status"] = e.status
            self.send(e.status, {"errors": [{"type": e.kind, "message": e.message}]}, e.headers)

    def do_POST(self):  # noqa: N802
        self.send(405, {"errors": [{"type": "method-not-allowed", "message": "the worker is read-only"}]})

    do_PUT = do_PATCH = do_DELETE = do_POST


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
