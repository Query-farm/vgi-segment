/**
 * Assemble the `segment` catalog: schemas `main`, `tracking`, `engage`, `iam` and
 * `unified`, plus the `segment` secret type.
 */

import { type CatalogDescriptor, field, schema, type TableDescriptor, utf8, type VgiFunction } from "@query-farm/vgi";
import {
  CATALOG_COMMENT,
  CATALOG_NAME,
  CATALOG_TAGS,
  ENGAGE_DOC_MD,
  ENGAGE_SCHEMA_TAGS,
  IAM_DOC_MD,
  IAM_SCHEMA_TAGS,
  SCHEMA_DOC_MD,
  SCHEMA_TAGS,
  SOURCE_URL,
  TRACKING_DOC_MD,
  TRACKING_SCHEMA_TAGS,
  UNIFIED_DOC_MD,
  UNIFIED_SCHEMA_TAGS,
} from "./meta.ts";
import { SECRET_TYPE } from "./segment/client.ts";
import { deliveryOverview, eventVolume } from "./segment/metrics.ts";
import { ENGAGE_TABLES, IAM_TABLES, MAIN_FUNCTIONS, MAIN_TABLES, TRACKING_TABLES } from "./segment/resources.ts";
import { UNIFIED_TABLES } from "./segment/unified.ts";

export const FUNCTIONS: VgiFunction[] = [eventVolume, deliveryOverview, ...MAIN_FUNCTIONS];

const doc = (text: string) => new Map([["vgi_doc", text]]);

/** CREATE SECRET (TYPE segment, TOKEN '…', REGION 'us' | 'eu', BASE_URL '…'). */
export const SEGMENT_SECRET_TYPE = {
  name: SECRET_TYPE,
  description: "A Segment Public API token (Bearer) and the workspace's region",
  schema: schema(
    [
      field(
        "token",
        utf8(),
        true,
        new Map([
          ["redact", "true"],
          [
            "vgi_doc",
            "Public API token (Workspace settings > Access Management > Tokens); a read-only role is enough.",
          ],
        ]),
      ),
      field(
        "region",
        utf8(),
        true,
        doc("Workspace region: 'us' (default, https://api.segmentapis.com) or 'eu' (https://eu1.api.segmentapis.com)."),
      ),
      field(
        "base_url",
        utf8(),
        true,
        doc(
          "Override the API origin (a proxy or test server); wins over REGION. http:// only for 127.0.0.1/localhost.",
        ),
      ),
    ],
    new Map([["vgi_doc_url", "https://segment.com/docs/api/public-api/"]]),
  ),
};

interface Example {
  description: string;
  sql: string;
}

/** Publish each function's examples as `vgi.example_queries`; return them all. */
function stampExampleQueries(functions: VgiFunction[]): Example[] {
  const all: Example[] = [];
  for (const fn of functions) {
    const ex = (fn.meta.examples ?? []).map((e) => ({ description: e.description, sql: e.sql }));
    fn.meta.tags = { ...fn.meta.tags, "vgi.example_queries": JSON.stringify(ex) };
    all.push(...ex);
  }
  return all;
}

const tableExamples = (tables: TableDescriptor[]) =>
  tables.flatMap((t) => JSON.parse(t.tags?.["vgi.example_queries"] ?? "[]") as Example[]);

const schemaEntry = (
  name: string,
  comment: string,
  tags: Record<string, string>,
  tables: TableDescriptor[],
  functions: VgiFunction[] = [],
) => {
  const examples = [...stampExampleQueries(functions), ...tableExamples(tables)];
  return {
    path: [name],
    comment,
    functions,
    tables,
    tags: { ...tags, "vgi.example_queries": JSON.stringify(examples.slice(0, 12)) },
  };
};

export const CATALOG: CatalogDescriptor = {
  name: CATALOG_NAME,
  comment: CATALOG_COMMENT,
  sourceUrl: SOURCE_URL,
  tags: CATALOG_TAGS,
  secretTypes: [SEGMENT_SECRET_TYPE],
  schemas: [
    schemaEntry("main", SCHEMA_DOC_MD, SCHEMA_TAGS, MAIN_TABLES, FUNCTIONS),
    schemaEntry("tracking", TRACKING_DOC_MD, TRACKING_SCHEMA_TAGS, TRACKING_TABLES),
    schemaEntry("engage", ENGAGE_DOC_MD, ENGAGE_SCHEMA_TAGS, ENGAGE_TABLES),
    schemaEntry("iam", IAM_DOC_MD, IAM_SCHEMA_TAGS, IAM_TABLES),
    schemaEntry("unified", UNIFIED_DOC_MD, UNIFIED_SCHEMA_TAGS, UNIFIED_TABLES),
  ],
};
