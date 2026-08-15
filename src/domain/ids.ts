import type { HostId } from "../types.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "./canonical.js";

const SCHEMA_NAMESPACE = "pi-qdrant-memory-v2";
const MAX_ID_LENGTH = 512;
const MAX_MEMBERS = 1024;
/** Shared persisted causal-sequence bound (also enforced by capture selector). */
export const MAX_SESSION_SEQUENCE = 4_294_967_295;
export const SESSION_SEQUENCE_STRIDE = 65_536;
const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

export type TombstoneScope = "occurrence" | "content" | "state";

export interface StateKeyInput {
  host: HostId;
  scope: string;
  projectId?: string | null;
  category: string;
  subject: string;
  predicate: string;
}

function hashParts(value: unknown): string {
  return sha256Hex(canonicalStringify(value));
}

/** Domain-tagged identity: `tag:hex` keeps occurrence/content/state targets runtime-verifiable. */
function tagged(domain: "state" | "content" | "occurrence" | "current" | "conflict", value: unknown): string {
  return `${domain}:${sha256Hex(canonicalStringify(value))}`;
}

function requireText(name: string, value: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_ID_LENGTH) throw new TypeError(`${name} must be a bounded non-empty string`);
  return value;
}
function requireId(name: string, value: string): string {
  requireText(name, value);
  if (/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value)) throw new TypeError(`${name} must be redacted`);
  return value;
}
function requireEpoch(name: string, value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError(`${name} must be a non-negative integer`);
  return value as number;
}
function requireHex64(name: string, value: string): string {
  requireText(name, value);
  if (!HEX64.test(value)) throw new TypeError(`${name} must be a SHA-256 hex digest`);
  return value;
}

/**
 * Logical state identity; it is independent of the current value and owner.
 * The `state:` tag makes the target domain verifiable at runtime.
 */
export function stateKey(input: StateKeyInput): string {
  if (input.host !== "pi" && input.host !== "prime") throw new TypeError("stateKey.host must be pi or prime");
  requireId("stateKey.scope", input.scope);
  requireId("stateKey.category", input.category);
  requireId("stateKey.subject", input.subject);
  requireId("stateKey.predicate", input.predicate);
  if (input.projectId !== undefined && input.projectId !== null) requireId("stateKey.projectId", input.projectId);
  const identity: Record<string, unknown> = { category: input.category, domain: "state", host: input.host, predicate: input.predicate, projectId: input.projectId ?? null, schema: SCHEMA_NAMESPACE, scope: input.scope, subject: input.subject };
  return tagged("state", identity);
}

/** Reusable value identity under one coordination policy. */
export function contentId(policyHash: string, logicalStateKey: string, canonicalValue: unknown): string {
  requireText("contentId.policyHash", policyHash);
  requireId("contentId.stateKey", logicalStateKey);
  return tagged("content", { canonicalValue, domain: "content", policyHash, stateKey: logicalStateKey });
}

export interface SessionEffectiveOrder {
  readonly kind: "session";
  /** Durable session identity; sequence is meaningful only within this exact session. */
  readonly sessionId: string;
  readonly sequence: number;
  /** Fallback tuple carried with every session order for cross-session comparison. */
  readonly eventAt: string;
  readonly episodeId: string;
  readonly contentId: string;
}
/** Legacy `session:N` values remain parseable for old records but are never treated as cross-session causal evidence. */
export type EffectiveOrder = SessionEffectiveOrder | `session:${number}` | readonly [string, string, string];

/** Validate durable session orders plus the legacy/cross-session encodings. */
export function validateEffectiveOrder(value: unknown): asserts value is EffectiveOrder {
  if (typeof value === "string") {
    if (value.length > MAX_ID_LENGTH || !/^session:(?:0|[1-9]\d*)$/u.test(value)) throw new TypeError("effectiveOrder causal sequence is invalid");
    const legacySequence = Number(value.slice("session:".length));
    if (!Number.isSafeInteger(legacySequence) || legacySequence > MAX_SESSION_SEQUENCE) throw new TypeError("effectiveOrder causal sequence is invalid");
    return;
  }
  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype || Object.getOwnPropertySymbols(value).length > 0 || Object.getOwnPropertyNames(value).length !== 4 || value.length !== 3 || value.some((item) => typeof item !== "string")) throw new TypeError("effectiveOrder tuple is invalid");
    const [eventAt, primaryEpisodeId, contentIdValue] = value as [string, string, string];
    const dateMatch = ISO_DATE.exec(eventAt); const parsed = Date.parse(eventAt);
    if (dateMatch === null || Number(dateMatch[1]) < 1970 || Number(dateMatch[1]) > 2100 || !Number.isFinite(parsed) || new Date(parsed).toISOString() !== eventAt) throw new TypeError("effectiveOrder event timestamp is invalid");
    requireId("effectiveOrder.primaryEpisodeId", primaryEpisodeId);
    requireId("effectiveOrder.contentId", contentIdValue);
    return;
  }
  if (typeof value !== "object" || value === null || Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError("effectiveOrder session object is invalid");
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  if (JSON.stringify(keys) !== JSON.stringify(["contentId", "episodeId", "eventAt", "kind", "sequence", "sessionId"]) || record.kind !== "session") throw new TypeError("effectiveOrder session object is invalid");
  requireId("effectiveOrder.sessionId", record.sessionId as string);
  requireId("effectiveOrder.episodeId", record.episodeId as string);
  requireId("effectiveOrder.contentId", record.contentId as string);
  if (!Number.isSafeInteger(record.sequence) || (record.sequence as number) < 0 || (record.sequence as number) > MAX_SESSION_SEQUENCE) throw new TypeError("effectiveOrder sequence is invalid");
  const eventAt = record.eventAt;
  const dateMatch = typeof eventAt === "string" ? ISO_DATE.exec(eventAt) : null; const parsed = typeof eventAt === "string" ? Date.parse(eventAt) : Number.NaN;
  if (dateMatch === null || Number(dateMatch[1]) < 1970 || Number(dateMatch[1]) > 2100 || !Number.isFinite(parsed) || new Date(parsed).toISOString() !== eventAt) throw new TypeError("effectiveOrder event timestamp is invalid");
}

/** Insert-only occurrence identity. effectiveOrder may be a causal tuple. */
export function observationId(policyEpoch: number, logicalContentId: string, primaryEvidenceEpisodeId: string, effectiveOrder: unknown): string {
  requireEpoch("observationId.policyEpoch", policyEpoch);
  requireId("observationId.contentId", logicalContentId);
  requireId("observationId.primaryEvidenceEpisodeId", primaryEvidenceEpisodeId);
  validateEffectiveOrder(effectiveOrder);
  return tagged("occurrence", { contentId: logicalContentId, domain: "occurrence", effectiveOrder, policyEpoch, primaryEvidenceEpisodeId });
}

/**
 * Per-policy-epoch mutable current-point identity: one `curated_current`
 * point per logical state key AND coordination policy epoch. A policy
 * migration creates a NEW current identity (the old point is never mutated);
 * active-epoch readers select only the point whose epoch matches the active
 * control, so the old view is hidden without touching old records.
 */
export function curatedCurrentId(host: HostId, stateKeyValue: string, coordinationPolicyEpoch: number): string {
  if (host !== "pi" && host !== "prime") throw new TypeError("curatedCurrentId.host must be pi or prime");
  requireId("curatedCurrentId.stateKey", stateKeyValue);
  requireEpoch("curatedCurrentId.coordinationPolicyEpoch", coordinationPolicyEpoch);
  return tagged("current", { domain: "current", host, policyEpoch: coordinationPolicyEpoch, stateKey: stateKeyValue });
}

/**
 * Content-addressed immutable conflict-manifest identity: the manifest id
 * binds the coordination policy hash, the logical state key and the sorted
 * conflicting observation members, so concurrent identical conflicts converge
 * to one manifest and no winner is ever chosen.
 */
export function conflictManifestId(policyHash: string, stateKeyValue: string, members: readonly string[]): string {
  requireId("conflictManifestId.policyHash", policyHash);
  requireId("conflictManifestId.stateKey", stateKeyValue);
  // Bounded conflict manifests use the same 1024-member envelope as other
  // immutable membership records. The materializer fails closed at the cap;
  // it never emits partial conflict state or false coverage.
  if (!Array.isArray(members) || members.length < 2 || members.length > MAX_MEMBERS) throw new TypeError(`conflictManifestId.members must contain 2..${MAX_MEMBERS} observations`);
  const sorted = [...members];
  for (let index = 0; index < sorted.length; index += 1) {
    requireId(`conflictManifestId.members[${index}]`, sorted[index]!);
    if (index > 0 && sorted[index - 1]! >= sorted[index]!) throw new TypeError("conflictManifestId.members must be strictly sorted and unique");
  }
  return tagged("conflict", { domain: "conflict", members: sorted, policyHash, stateKey: stateKeyValue });
}

export function evidenceLinkId(observation: string, episode: string, extractorRevision: string | number): string {
  requireId("evidenceLinkId.observationId", observation);
  requireId("evidenceLinkId.episodeId", episode);
  if (typeof extractorRevision === "string") requireId("evidenceLinkId.extractorRevision", extractorRevision);
  else if (!Number.isSafeInteger(extractorRevision) || extractorRevision < 0 || extractorRevision > MAX_ID_LENGTH) throw new TypeError("evidenceLinkId.extractorRevision is invalid");
  return deterministicUuid(`${SCHEMA_NAMESPACE}:evidence-link`, observation, episode, extractorRevision);
}

export interface EpisodeIdentityInput {
  host: HostId;
  sessionId: string;
  messageId: string;
  part?: string | number;
}

export function episodeId(input: EpisodeIdentityInput): string;
export function episodeId(host: HostId, sessionId: string, messageId: string, part?: string | number): string;
export function episodeId(inputOrHost: EpisodeIdentityInput | HostId, sessionId?: string, messageId?: string, part?: string | number): string {
  const input: EpisodeIdentityInput = typeof inputOrHost === "string"
    ? (part === undefined
      ? { host: inputOrHost, sessionId: sessionId ?? "", messageId: messageId ?? "" }
      : { host: inputOrHost, sessionId: sessionId ?? "", messageId: messageId ?? "", part })
    : inputOrHost;
  if (input.host !== "pi" && input.host !== "prime") throw new TypeError("episodeId.host must be pi or prime");
  requireId("episodeId.sessionId", input.sessionId);
  requireId("episodeId.messageId", input.messageId);
  if (input.part !== undefined && (typeof input.part === "string" ? (input.part.length === 0 || input.part.length > MAX_ID_LENGTH) : (!Number.isSafeInteger(input.part) || input.part < 0 || input.part > MAX_ID_LENGTH))) throw new TypeError("episodeId.part is invalid");
  return deterministicUuid(`${SCHEMA_NAMESPACE}:episode`, input.host, input.sessionId, input.messageId, input.part ?? null);
}

export interface JobIdentityInput {
  ownerHost: HostId;
  membership: readonly string[];
  policyHash: string;
  extractorRevision: string;
  coordinationPolicyEpoch: number;
  policyIntersectionId: string;
  privacyEpoch: number;
}
export function jobId(ownerHost: HostId, membership: readonly string[], policyHash: string, extractorRevision: string, coordinationPolicyEpoch: number, policyIntersectionId: string, privacyEpoch: number): string;
export function jobId(input: JobIdentityInput): string;
export function jobId(
  ownerOrInput: HostId | JobIdentityInput,
  membership?: readonly string[], policyHash?: string, extractorRevision?: string, coordinationPolicyEpoch?: number, policyIntersectionId?: string, privacyEpoch?: number,
): string {
  const input: JobIdentityInput = typeof ownerOrInput === "string"
    ? { ownerHost: ownerOrInput, membership: membership ?? [], policyHash: policyHash ?? "", extractorRevision: extractorRevision ?? "", coordinationPolicyEpoch: coordinationPolicyEpoch ?? 0, policyIntersectionId: policyIntersectionId ?? "", privacyEpoch: privacyEpoch ?? 0 }
    : ownerOrInput;
  if (input.ownerHost !== "pi" && input.ownerHost !== "prime") throw new TypeError("jobId.ownerHost must be pi or prime");
  requireId("jobId.policyHash", input.policyHash);
  requireId("jobId.extractorRevision", input.extractorRevision);
  requireEpoch("jobId.coordinationPolicyEpoch", input.coordinationPolicyEpoch);
  requireId("jobId.policyIntersectionId", input.policyIntersectionId);
  requireEpoch("jobId.privacyEpoch", input.privacyEpoch);
  if (!Array.isArray(input.membership) || input.membership.length === 0 || input.membership.length > 65_536) throw new TypeError("jobId.membership must contain bounded IDs");
  input.membership.forEach((id, index) => requireId(`jobId.membership[${index}]`, id));
  return deterministicUuid(`${SCHEMA_NAMESPACE}:job`, input.ownerHost, [...input.membership], input.policyHash, input.extractorRevision, input.coordinationPolicyEpoch, input.policyIntersectionId, input.privacyEpoch);
}

export function manifestHash(memberIds: readonly string[]): string {
  if (!Array.isArray(memberIds) || memberIds.length === 0 || memberIds.length > MAX_MEMBERS) throw new TypeError("manifest member IDs are invalid or unbounded");
  memberIds.forEach((id, index) => requireId(`manifest member ID ${index}`, id));
  return hashParts([...memberIds]);
}

/**
 * Tombstone point identity: the exact `H(owner_host,"tombstone",target_id)`
 * formula (spec §13.6.1) so targets and provenance source IDs are directly
 * batch-retrievable. Targets are domain-tagged and verified before insertion.
 */
export function tombstoneId(ownerHost: HostId, targetId: string): string {
  if (ownerHost !== "pi" && ownerHost !== "prime") throw new TypeError("tombstoneId.ownerHost must be pi or prime");
  requireId("tombstoneId.targetId", targetId);
  return deterministicUuid(SCHEMA_NAMESPACE, ownerHost, "tombstone", targetId);
}

export interface CoverageIdentityInput {
  ownerHost: HostId;
  episodeId: string;
  extractorRevision: string;
  coordinationPolicyHash: string;
  coordinationPolicyEpoch: number;
  policyIntersectionId: string;
  privacyEpoch: number;
}
/**
 * Deterministic coverage identity: owner + episode + extractor + active
 * coordination hash/epoch + processing-policy intersection + privacy epoch,
 * so coverage is policy-specific and pre-forget coverage can never suppress
 * post-forget work.
 */
export function coverageId(input: CoverageIdentityInput): string {
  if (input.ownerHost !== "pi" && input.ownerHost !== "prime") throw new TypeError("coverageId.ownerHost must be pi or prime");
  requireId("coverageId.episodeId", input.episodeId);
  requireId("coverageId.extractorRevision", input.extractorRevision);
  requireId("coverageId.coordinationPolicyHash", input.coordinationPolicyHash);
  requireEpoch("coverageId.coordinationPolicyEpoch", input.coordinationPolicyEpoch);
  requireId("coverageId.policyIntersectionId", input.policyIntersectionId);
  requireEpoch("coverageId.privacyEpoch", input.privacyEpoch);
  return deterministicUuid(`${SCHEMA_NAMESPACE}:coverage`, input.ownerHost, input.episodeId, input.extractorRevision, input.coordinationPolicyHash, input.coordinationPolicyEpoch, input.policyIntersectionId, input.privacyEpoch);
}

/** Mutable lease point for a job: the lease is separate from immutable job/proposal identity. */
export function leasePointId(jobIdValue: string): string {
  requireId("leasePointId.jobId", jobIdValue);
  return deterministicUuid(`${SCHEMA_NAMESPACE}:lease`, jobIdValue);
}

/** Immutable proposal point identity bound to the job, content hash, epoch and fencing token. */
export function proposalIdFor(jobIdValue: string, proposalHash: string, coordinationPolicyEpoch: number, fencingToken: number): string {
  requireId("proposalIdFor.jobId", jobIdValue);
  requireHex64("proposalIdFor.proposalHash", proposalHash);
  requireEpoch("proposalIdFor.coordinationPolicyEpoch", coordinationPolicyEpoch);
  if (!Number.isSafeInteger(fencingToken) || fencingToken < 0) throw new TypeError("proposalIdFor.fencingToken must be a non-negative integer");
  return deterministicUuid(`${SCHEMA_NAMESPACE}:proposal`, jobIdValue, proposalHash, coordinationPolicyEpoch, fencingToken);
}

/** Runtime domain verification for tombstone targets. */
export function isStateTarget(value: unknown): value is string { return typeof value === "string" && value.length <= MAX_ID_LENGTH && /^state:[0-9a-f]{64}$/u.test(value); }
export function isContentTarget(value: unknown): value is string { return typeof value === "string" && value.length <= MAX_ID_LENGTH && /^content:[0-9a-f]{64}$/u.test(value); }
/** Occurrence targets are tagged observations or episode point IDs. */
export function isOccurrenceTarget(value: unknown): value is string { return typeof value === "string" && value.length <= MAX_ID_LENGTH && (/^occurrence:[0-9a-f]{64}$/u.test(value) || UUID.test(value)); }

/** Content-addressed proposal hash: binds owner/job/membership/output/epochs/hash/fence. */
export function proposalContentHash(input: { ownerHost: HostId; jobId: string; ownerId: string; membership: readonly string[]; content: unknown; policyHash: string; policyEpoch: number; fencingToken: number; privacyEpoch: number; policyIntersectionId: string }): string {
  if (input.ownerHost !== "pi" && input.ownerHost !== "prime") throw new TypeError("proposalContentHash.ownerHost must be pi or prime");
  requireId("proposalContentHash.jobId", input.jobId);
  requireId("proposalContentHash.ownerId", input.ownerId);
  requireId("proposalContentHash.policyHash", input.policyHash);
  requireEpoch("proposalContentHash.policyEpoch", input.policyEpoch);
  if (!Number.isSafeInteger(input.fencingToken) || input.fencingToken < 0) throw new TypeError("proposalContentHash.fencingToken must be a non-negative integer");
  requireEpoch("proposalContentHash.privacyEpoch", input.privacyEpoch);
  requireId("proposalContentHash.policyIntersectionId", input.policyIntersectionId);
  if (!Array.isArray(input.membership) || input.membership.length === 0 || input.membership.length > MAX_MEMBERS) throw new TypeError("proposalContentHash.membership must contain bounded IDs");
  input.membership.forEach((id, index) => requireId(`proposalContentHash.membership[${index}]`, id));
  const serialized = canonicalStringify(input.content);
  if (serialized.length > 16_000) throw new TypeError("proposalContentHash.content is unbounded");
  return sha256Hex(canonicalStringify({ content: input.content, coordinationPolicyEpoch: input.policyEpoch, coordinationPolicyHash: input.policyHash, fencingToken: input.fencingToken, jobId: input.jobId, membership: [...input.membership], ownerHost: input.ownerHost, ownerId: input.ownerId, policyIntersectionId: input.policyIntersectionId, privacyEpoch: input.privacyEpoch }));
}

export function isTombstoneTarget(scope: TombstoneScope, value: unknown): value is string {
  if (scope === "occurrence") return isOccurrenceTarget(value);
  if (scope === "content") return isContentTarget(value);
  if (scope === "state") return isStateTarget(value);
  return false;
}
