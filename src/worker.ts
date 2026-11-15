/**
 * The worker entry point.
 *
 *   bin/vgi-segment-worker                      # stdio: what DuckDB launches by default
 *   bin/vgi-segment-worker --http --port 8000   # HTTP: for Docker / hosted deploys
 *   bin/vgi-segment-worker --help
 *
 * Attach it from DuckDB:
 *
 *   ATTACH 'segment' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-segment-worker');
 */

import { CATALOG } from "./catalog.ts";
import { serve } from "./cli.ts";
import { CATALOG_DOC_LLM, SOURCE_URL, VERSION } from "./meta.ts";

if (import.meta.main) {
  serve(CATALOG, { version: VERSION, doc: CATALOG_DOC_LLM, repositoryUrl: SOURCE_URL });
}
