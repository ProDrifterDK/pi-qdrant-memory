import { canonicalStringify } from "../domain/canonical.js";
import { redactAndScan, type SecretScanner } from "../security/redaction.js";
import { types as nodeTypes } from "node:util";

export const CURATION_CATEGORIES = ["preference", "correction", "convention", "fact", "failure", "learning"] as const;
export const CURATION_SCOPES = ["project", "session", "host", "global"] as const;
export type CurationCategory = typeof CURATION_CATEGORIES[number];
export type CurationScope = typeof CURATION_SCOPES[number];
const MAX_ITEMS = 32;
const MAX_EVIDENCE = 16;
const MAX_FIELD = 512;
const MAX_VALUE_CHARS = 4_096;
const MAX_TEXT_CHARS = 16_000;
const SECRETISH = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;
const ID = /^[A-Za-z0-9._:/-]{1,512}$/u;

export interface CurationItem {
  readonly category: CurationCategory;
  readonly scope: CurationScope;
  readonly subject: string;
  readonly predicate: string;
  value?: unknown;
  text?: string;
  readonly evidence: readonly string[];
  confidence?: number;
}
export interface CurationResult {
  readonly items: readonly CurationItem[];
}
export interface CurationValidationContext {
  /** Episodes whose eventKind is a direct user event; only they may evidence preferences/corrections. */
  readonly directUserEpisodeIds: ReadonlySet<string>;
  /** Every episode id known to exist in the explicit membership. */
  readonly knownEpisodeIds: ReadonlySet<string>;
  readonly maxItems?: number;
  readonly maxEvidence?: number;
}

function fail(message: string): never { throw new TypeError(`Curation result is invalid: ${message}`); }
function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}
function bounded(name: string, value: unknown, max = MAX_FIELD): string {
  if (typeof value !== "string" || value.length === 0 || value.length > max) fail(`${name} must be a bounded non-empty string`);
  if (SECRETISH.test(value)) fail(`${name} must be redacted`);
  return value;
}
function itemId(name: string, value: unknown): string {
  const id = bounded(name, value, 512);
  if (!ID.test(id)) fail(`${name} must be an exact episode id`);
  return id;
}
function boundedList(name: string, value: unknown, max: number): unknown[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > max) fail(`${name} must be a bounded non-empty list`);
  return value;
}

/** Owned canonical clone constructed exclusively from own data descriptors. */
function ownedPlainClone(value: unknown): unknown {
  const active = new Set<object>();
  const clone = (candidate: unknown): unknown => {
    if (candidate === null || typeof candidate === "string" || typeof candidate === "boolean") return candidate;
    if (typeof candidate === "number") { if (!Number.isFinite(candidate)) fail("input value contains a non-finite number"); return candidate; }
    if (typeof candidate !== "object" || nodeTypes.isProxy(candidate) || active.has(candidate)) fail("input is not a plain acyclic JSON graph");
    active.add(candidate);
    try {
      if (Object.getOwnPropertySymbols(candidate).length > 0) fail("input contains symbol keys");
      if (Array.isArray(candidate)) {
        if (Object.getPrototypeOf(candidate) !== Array.prototype) fail("input array prototype is invalid");
        const lengthDescriptor = Object.getOwnPropertyDescriptor(candidate, "length");
        if (lengthDescriptor === undefined || !("value" in lengthDescriptor) || !Number.isSafeInteger(lengthDescriptor.value) || lengthDescriptor.value < 0 || lengthDescriptor.value > 4096 || Object.getOwnPropertyNames(candidate).length !== lengthDescriptor.value + 1) fail("input array is sparse or unbounded");
        const result: unknown[] = [];
        for (let index = 0; index < lengthDescriptor.value; index += 1) {
          const descriptor = Object.getOwnPropertyDescriptor(candidate, String(index));
          if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) fail("input array contains an accessor or hole");
          result.push(clone(descriptor.value));
        }
        return result;
      }
      const prototype = Object.getPrototypeOf(candidate);
      if (prototype !== Object.prototype && prototype !== null) fail("input object prototype is invalid");
      const result: Record<string, unknown> = {};
      for (const name of Object.getOwnPropertyNames(candidate)) {
        const descriptor = Object.getOwnPropertyDescriptor(candidate, name);
        if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) fail("input contains an accessor or hidden field");
        Object.defineProperty(result, name, { value: clone(descriptor.value), enumerable: true, writable: true, configurable: true });
      }
      return result;
    } finally { active.delete(candidate); }
  };
  try { return JSON.parse(canonicalStringify(clone(value))) as unknown; }
  catch (error) { if (error instanceof TypeError && error.message.startsWith("Curation result is invalid:")) throw error; fail("input is not canonical JSON"); }
}

function validateItem(value: unknown, ctx: CurationValidationContext, maxEvidence: number): CurationItem {
  if (!isPlainRecord(value)) fail("items must be plain objects");
  const allowed = new Set(["category", "scope", "subject", "predicate", "value", "text", "evidence", "confidence"]);
  for (const key of Object.keys(value)) if (!allowed.has(key)) fail(`unknown item field ${key}`);
  if (Object.getOwnPropertySymbols(value).length > 0) fail("item fields must not be symbols");
  const category = value.category;
  const scope = value.scope;
  const subject = value.subject;
  const predicate = value.predicate;
  const evidence = value.evidence;
  const confidence = value.confidence;
  const itemValue = value.value;
  const text = value.text;
  if (typeof category !== "string" || !(CURATION_CATEGORIES as readonly string[]).includes(category)) fail("unknown category");
  if (typeof scope !== "string" || !(CURATION_SCOPES as readonly string[]).includes(scope)) fail("unknown scope");
  bounded("subject", subject);
  bounded("predicate", predicate);
  if (itemValue !== undefined) {
    let serialized: string;
    try { serialized = canonicalStringify(itemValue); } catch { fail("value is not canonical JSON"); }
    if (serialized.length > MAX_VALUE_CHARS) fail("value is unbounded");
  }
  if (text !== undefined) bounded("text", text, MAX_TEXT_CHARS);
  if (itemValue === undefined && text === undefined) fail("item requires a value or text");
  if (confidence !== undefined) {
    if (typeof confidence !== "number" || !Number.isFinite(confidence) || confidence < 0 || confidence > 1) fail("confidence must be finite in [0,1]");
  }
  const evidenceIds = boundedList("evidence", evidence, maxEvidence).map((id, index) => itemId(`evidence[${index}]`, id));
  if (new Set(evidenceIds).size !== evidenceIds.length) fail("evidence must not repeat");
  for (const id of evidenceIds) if (!ctx.knownEpisodeIds.has(id)) fail(`evidence episode ${id} is not in the explicit membership`);
  // Tool output can never supply direct evidence for a standing instruction.
  if (category === "preference" || category === "correction") {
    if (!evidenceIds.some((id) => ctx.directUserEpisodeIds.has(id))) fail("direct user evidence is required for preferences and corrections");
  }
  const subjectText = subject as string;
  const predicateText = predicate as string;
  const item: CurationItem = {
    category: category as CurationCategory,
    scope: scope as CurationScope,
    subject: subjectText,
    predicate: predicateText,
    evidence: Object.freeze([...evidenceIds]),
  };
  if (itemValue !== undefined) item.value = Object.freeze(ownedPlainClone(itemValue));
  if (text !== undefined) item.text = text as string;
  if (confidence !== undefined) item.confidence = confidence;
  return Object.freeze(item);
}

/**
 * Strict curation-result validation over an OWNED canonical clone. Unknown
 * fields/categories/scopes, non-plain or accessor-bearing input, unbounded
 * lists/strings/values and duplicate/foreign evidence ids are rejected.
 * Standing preferences/corrections require at least one direct-user episode
 * in evidence; a tool output that invents a standing instruction is rejected.
 */
export function validateCurationResult(input: unknown, ctx: CurationValidationContext): CurationResult {
  const directUserEpisodeIds = ctx.directUserEpisodeIds;
  const knownEpisodeIds = ctx.knownEpisodeIds;
  if (directUserEpisodeIds === null || typeof directUserEpisodeIds !== "object" || knownEpisodeIds === null || typeof knownEpisodeIds !== "object") fail("validation context is invalid");
  const maxItems = ctx.maxItems ?? MAX_ITEMS;
  const maxEvidence = ctx.maxEvidence ?? MAX_EVIDENCE;
  if (!Number.isSafeInteger(maxItems) || maxItems < 1 || maxItems > MAX_ITEMS || !Number.isSafeInteger(maxEvidence) || maxEvidence < 1 || maxEvidence > MAX_EVIDENCE) fail("validation bounds are invalid");
  if (!isPlainRecord(input)) fail("input must be a plain object");
  const allowed = new Set(["items"]);
  for (const key of Object.keys(input)) if (!allowed.has(key)) fail(`unknown top-level field ${key}`);
  if (Object.getOwnPropertySymbols(input).length > 0) fail("top-level fields must not be symbols");
  const ownedInput = ownedPlainClone(input);
  if (!isPlainRecord(ownedInput)) fail("input must be canonical JSON");
  const rawItems = ownedInput.items;
  if (!Array.isArray(rawItems) || rawItems.length > maxItems) fail("items must be a bounded list");
  // Accepted empty results are meaningful: coverage still records that the
  // explicit membership was examined.  Snapshot a dense array so sparse or
  // accessor-bearing output cannot silently disappear during validation.
  let items: unknown[];
  try { items = Array.from(rawItems); canonicalStringify(items); } catch { fail("items must be canonical JSON"); }
  const validated = items.map((item) => validateItem(ownedPlainClone(item), ctx, maxEvidence));
  const identities = validated.map((item) => canonicalStringify(item));
  if (new Set(identities).size !== identities.length) fail("items must not repeat");
  return Object.freeze({ items: Object.freeze(validated) });
}

/**
 * Validate that a validated result is safe to persist in an accepted proposal.
 * The entire canonical item (including nested value objects and their keys) is
 * structurally redacted and passed through the mandatory built-in scanner. Any
 * required transformation is rejected rather than persisting the raw result;
 * this keeps proposal/content identity and later materialization deterministic.
 */
export function assertPersistableCurationResult(result: CurationResult, scan?: SecretScanner): CurationResult {
  if (!isPlainRecord(result) || !Array.isArray(result.items)) fail("persistable result is invalid");
  for (const item of result.items) {
    let serialized: string;
    try { serialized = canonicalStringify(item); } catch { fail("persistable item is not canonical JSON"); }
    const checked = redactAndScan({ text: serialized, maxChars: 65_536, homeDir: "/", ...(scan === undefined ? {} : { scan }) });
    if (checked.dropped || checked.secretScan !== "passed" || checked.redactionStatus !== "unchanged" || checked.text !== serialized) fail("result contains unsafe persistable data");
  }
  return result;
}

/**
 * JSON-only parsing with fences/prefix/suffix rejection, prototype-key
 * rejection and duplicate-key rejection. The returned value is a plain
 * caller-independent clone.
 */
export function parseStrictCurationJson(text: string): unknown {
  if (typeof text !== "string" || text.length === 0 || text.length > 65_536) throw new TypeError("Curation output must be a bounded non-empty string");
  if (!/^\s*[{[]/u.test(text) || !/[}\]]\s*$/u.test(text)) throw new TypeError("Curation output must be a bare JSON object/array without fences or prose");
  assertNoDuplicateKeys(text);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text, (key, value) => {
      if (key === "__proto__" || key === "constructor" || key === "prototype") throw new TypeError("prototype key");
      return value;
    });
  } catch { throw new TypeError("Curation output is not strict JSON"); }
  return ownedPlainClone(parsed);
}

/** Reject duplicate object keys at every nesting level (JSON.parse keeps only the last). */
function assertNoDuplicateKeys(text: string): void {
  const stack: Set<string>[] = [];
  let index = 0;
  const length = text.length;
  while (index < length) {
    const ch = text[index]!;
    if (ch === "{") { stack.push(new Set<string>()); index += 1; }
    else if (ch === "}") { stack.pop(); index += 1; }
    else if (ch === '"') {
      const start = index;
      index += 1;
      while (index < length && text[index] !== '"') { if (text[index] === "\\") index += 1; index += 1; }
      index += 1;
      let cursor = index;
      while (cursor < length && /\s/u.test(text[cursor]!)) cursor += 1;
      if (text[cursor] === ":" && stack.length > 0) {
        const raw = text.slice(start + 1, index - 1);
        const key = decodeJsonString(raw);
        const top = stack[stack.length - 1]!;
        if (top.has(key)) throw new TypeError("duplicate key");
        top.add(key);
      }
      index = cursor;
    } else { index += 1; }
  }
}
function decodeJsonString(raw: string): string {
  let result = "";
  for (let i = 0; i < raw.length; i += 1) {
    const ch = raw[i]!;
    if (ch !== "\\") { result += ch; continue; }
    const next = raw[i + 1];
    if (next === undefined) break;
    if (next === "u") {
      const hex = raw.slice(i + 2, i + 6);
      if (hex.length === 4) { result += String.fromCodePoint(Number.parseInt(hex, 16)); i += 5; continue; }
      i += 1; continue;
    }
    const map: Record<string, string> = { '"': '"', "\\": "\\", "/": "/", b: "\b", f: "\f", n: "\n", r: "\r", t: "\t" };
    result += map[next] ?? next;
    i += 1;
  }
  return result;
}
