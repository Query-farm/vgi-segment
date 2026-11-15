/**
 * Flatten a Segment tracking-plan rule's JSON Schema into one row per property.
 *
 * A rule's `jsonSchema` describes the whole event: its top-level `properties`
 * object holds sections — `properties` (track/page/screen properties), `traits`
 * (identify/group traits) and `context` — each an object schema of its own. Every
 * leaf and nested object property becomes a row with a dotted path
 * (`order.items.sku`; array items add `[]`, e.g. `products[].sku`).
 */

// biome-ignore lint/suspicious/noExplicitAny: JSON Schema is untyped.
type Schema = Record<string, any>;

export interface FlatProperty {
  section: string;
  property_name: string;
  depth: number;
  data_type: string | null;
  types: string[];
  is_required: boolean;
  is_nullable: boolean;
  is_array: boolean;
  item_type: string | null;
  enum_values: string[] | null;
  pattern: string | null;
  format: string | null;
  minimum: number | null;
  maximum: number | null;
  min_length: number | null;
  max_length: number | null;
  description: string | null;
  json_schema: Schema;
}

const asObj = (v: unknown): Schema | undefined =>
  v && typeof v === "object" && !Array.isArray(v) ? (v as Schema) : undefined;

/** The JSON types a schema allows, from `type` (string or list) or `anyOf`/`oneOf` branches. */
export function typesOf(s: Schema): string[] {
  const out: string[] = [];
  const t = s.type;
  if (typeof t === "string") out.push(t);
  else if (Array.isArray(t)) out.push(...t.filter((x): x is string => typeof x === "string"));
  for (const k of ["anyOf", "oneOf"]) {
    if (Array.isArray(s[k])) for (const b of s[k]) if (asObj(b)) out.push(...typesOf(b));
  }
  if (out.length === 0 && Array.isArray(s.enum)) {
    for (const v of s.enum) out.push(v === null ? "null" : Array.isArray(v) ? "array" : typeof v);
  }
  return [...new Set(out)];
}

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const strOrNull = (v: unknown): string | null => (typeof v === "string" && v !== "" ? v : null);

function enumOf(s: Schema): string[] | null {
  const e = Array.isArray(s.enum) ? s.enum : s.const !== undefined ? [s.const] : null;
  if (!e) return null;
  return e.filter((v) => v !== null).map((v) => (typeof v === "string" ? v : JSON.stringify(v)));
}

function walk(section: string, obj: Schema, prefix: string, depth: number, out: FlatProperty[]): void {
  const props = asObj(obj.properties);
  if (!props) return;
  const required = new Set(Array.isArray(obj.required) ? (obj.required as string[]) : []);
  for (const [name, raw] of Object.entries(props)) {
    const s = asObj(raw) ?? {};
    const path = prefix ? `${prefix}.${name}` : name;
    const types = typesOf(s);
    const nonNull = types.filter((t) => t !== "null");
    const items = asObj(s.items);
    const isArray = nonNull.includes("array");
    out.push({
      section,
      property_name: path,
      depth,
      data_type: nonNull.length ? nonNull.join("|") : types.length ? "null" : null,
      types,
      is_required: required.has(name),
      is_nullable: types.length === 0 || types.includes("null"),
      is_array: isArray,
      item_type: items ? typesOf(items).join("|") || null : null,
      enum_values: enumOf(s),
      pattern: strOrNull(s.pattern),
      format: strOrNull(s.format),
      minimum: num(s.minimum ?? s.exclusiveMinimum),
      maximum: num(s.maximum ?? s.exclusiveMaximum),
      min_length: num(s.minLength),
      max_length: num(s.maxLength),
      description: strOrNull(s.description),
      json_schema: s,
    });
    if (asObj(s.properties)) walk(section, s, path, depth + 1, out);
    if (isArray && items && asObj(items.properties)) walk(section, items, `${path}[]`, depth + 1, out);
  }
}

/** Every property of a rule's schema, section by section (`properties`, `traits`, `context`, …). */
export function flattenRuleSchema(jsonSchema: unknown): FlatProperty[] {
  const root = asObj(jsonSchema);
  if (!root) return [];
  const sections = asObj(root.properties);
  if (!sections) return [];
  const out: FlatProperty[] = [];
  for (const [section, raw] of Object.entries(sections)) {
    const s = asObj(raw);
    if (s) walk(section, s, "", 0, out);
  }
  return out;
}

/** The rule-level description (root `description`), if any. */
export function ruleDescription(jsonSchema: unknown): string | null {
  return strOrNull(asObj(jsonSchema)?.description);
}
