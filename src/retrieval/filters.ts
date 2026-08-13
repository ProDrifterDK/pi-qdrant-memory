import type { HostId } from "../types.js";

export type RetrievalLane = "current" | "historical" | "episodes" | "curated" | "raptor" | "exact";
export type FilterScalar = string | number | boolean;
export type FilterCondition =
  | { key: string; match: { value: FilterScalar } }
  | { key: string; match: { any: FilterScalar[] } }
  | { key: string; range: { gt?: string; gte?: string; lte?: string } }
  | { is_null: { key: string } };
const FILTER_ISSUER = Symbol("pi-qdrant-memory-v2.guarded-lane-filter");
export class GuardedLaneFilter {
  readonly must: FilterCondition[]; readonly must_not: FilterCondition[]; readonly should: FilterCondition[]; readonly #issuer: symbol;
  constructor(input: { must: FilterCondition[]; mustNot: FilterCondition[]; should: FilterCondition[] }, issuer: symbol) {
    if (issuer !== FILTER_ISSUER) throw new TypeError("Guarded filter requires the module issuer");
    this.#issuer = issuer; this.must = Object.freeze([...input.must]) as unknown as FilterCondition[]; this.must_not = Object.freeze([...input.mustNot]) as unknown as FilterCondition[]; this.should = Object.freeze([...input.should]) as unknown as FilterCondition[]; Object.freeze(this);
  }
  static isValid(value: unknown): value is GuardedLaneFilter { return typeof value === "object" && value !== null && #issuer in value && value instanceof GuardedLaneFilter && value.#issuer === FILTER_ISSUER; }
}

export interface GuardedLaneFilterInput {
  ownerHost: HostId;
  lane: RetrievalLane;
  projectId: string;
  global: boolean;
  now: number;
  maxClockSkewMs: number;
  privacyEpoch: number;
  coordinationPolicyEpoch: number;
  activeGeneration?: string;
  exactRecordTypes?: readonly ("episode" | "curated_memory" | "curated_current")[];
  after?: string;
  before?: string;
}

function match(key: string, value: FilterScalar): FilterCondition { return { key, match: { value } }; }
function range(key: string, value: { gt?: string; gte?: string; lte?: string }): FilterCondition { return { key, range: value }; }
function freezeCondition(condition: FilterCondition): FilterCondition {
  const record = condition as unknown as Record<string, unknown>;
  for (const value of Object.values(record)) {
    if (typeof value === "object" && value !== null) {
      const nested = value as Record<string, unknown>;
      for (const child of Object.values(nested)) if (Array.isArray(child)) Object.freeze(child);
      Object.freeze(value);
    }
  }
  return Object.freeze(condition);
}
function recordTypes(lane: RetrievalLane, exactRecordTypes?: readonly ("episode" | "curated_memory" | "curated_current")[]): string[] {
  switch (lane) {
    case "current": return ["curated_current"];
    case "historical": return ["curated_memory"];
    case "episodes": return ["episode"];
    case "curated": return ["curated_current", "curated_memory"];
    case "raptor": return ["raptor_summary"];
    case "exact": {
      const values = exactRecordTypes ?? ["episode", "curated_memory", "curated_current"];
      if (!Array.isArray(values) || values.length < 1 || values.length > 3 || new Set(values).size !== values.length || values.some((value) => value !== "episode" && value !== "curated_memory" && value !== "curated_current")) throw new TypeError("Invalid exact retrieval types");
      return [...values];
    }
  }
}
function iso(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const time = Date.parse(value);
  if (!Number.isFinite(time) || new Date(time).toISOString() !== value) throw new TypeError("Invalid retrieval time bound");
  return value;
}

/** Build the immutable mandatory Qdrant filter for one internal lane. No model argument reaches this function. */
export function laneFilter(input: GuardedLaneFilterInput): GuardedLaneFilter {
  if ((input.ownerHost !== "pi" && input.ownerHost !== "prime") || typeof input.projectId !== "string" || input.projectId.length === 0 || input.projectId.length > 512) throw new TypeError("Invalid retrieval scope");
  if (!Number.isFinite(input.now) || !Number.isFinite(input.maxClockSkewMs) || input.maxClockSkewMs < 0 || !Number.isSafeInteger(input.privacyEpoch) || input.privacyEpoch < 0 || !Number.isSafeInteger(input.coordinationPolicyEpoch) || input.coordinationPolicyEpoch < 0) throw new TypeError("Invalid retrieval epoch");
  const after = iso(input.after); const before = iso(input.before);
  if (after !== undefined && before !== undefined && after > before) throw new TypeError("Invalid retrieval time window");
  const types = recordTypes(input.lane, input.exactRecordTypes);
  const must: FilterCondition[] = [
    match("owner_host", input.ownerHost),
    types.length === 1 ? match("record_type", types[0]!) : { key: "record_type", match: { any: types } },
    match("status", "active"),
    match("secret_scan", "passed"),
    match("privacy_epoch", input.privacyEpoch),
  ];
  if (input.lane === "current" || input.lane === "historical" || input.lane === "curated" || input.lane === "raptor") must.push(match("coordination_policy_epoch", input.coordinationPolicyEpoch));
  if (input.global) {
    if (input.lane !== "current" && input.lane !== "historical" && input.lane !== "curated" && input.lane !== "exact") throw new TypeError("Global scope is unsupported for this lane");
    must.push(match("scope", "global"));
  } else if (input.lane === "episodes" || input.lane === "exact") must.push(match("project_id", input.projectId));
  else if (input.lane === "current" || input.lane === "historical" || input.lane === "curated") must.push(match("scope", "project"), match("project_id", input.projectId));
  else if (input.lane === "raptor") must.push(match("covered_projects", input.projectId));
  if (input.lane === "raptor") {
    if (typeof input.activeGeneration !== "string" || input.activeGeneration.length === 0 || input.activeGeneration.length > 512) throw new TypeError("Active generation is required");
    must.push(match("generation_id", input.activeGeneration));
  }
  const temporalKey = input.lane === "episodes" ? "event_at" : input.lane === "historical" ? "effective_at" : input.lane === "raptor" ? "temporal_to" : "created_at";
  if (after !== undefined || before !== undefined) must.push(range(temporalKey, { ...(after === undefined ? {} : { gte: after }), ...(before === undefined ? {} : { lte: before }) }));
  const should: FilterCondition[] = [
    { is_null: { key: "expires_at" } },
    range("expires_at", { gt: new Date(input.now + input.maxClockSkewMs).toISOString() }),
  ];
  must.forEach(freezeCondition); should.forEach(freezeCondition);
  const mustNot: FilterCondition[] = [];
  return new GuardedLaneFilter({ must, mustNot, should }, FILTER_ISSUER);
}
Object.freeze(GuardedLaneFilter); Object.freeze(GuardedLaneFilter.prototype);
