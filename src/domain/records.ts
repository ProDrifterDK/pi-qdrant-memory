import type { HostId } from "../types.js";
import { canonicalStringify, sha256Hex } from "./canonical.js";
import { coverageId, jobId, leasePointId, manifestHash as canonicalManifestHash, proposalContentHash, proposalIdFor, tombstoneId, validateEffectiveOrder, type EffectiveOrder } from "./ids.js";
import { processingPolicyHash, type ProcessingPolicy } from "./policy.js";

export const RECORD_SCHEMA_REVISION = 1;
const MAX_TEXT_CHARS = 16_000;
const MAX_ID_CHARS = 512;
const MAX_ARRAY = 1024;
const SECRET_ID = /(api[-_]?key|access[-_]?token|auth(?:orization|entication)?|bearer|credential|password|secret|token)/iu;
const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
type PlainRecord = Record<string, unknown>;

export interface RecordEnvelope {
  ownerHost: HostId;
  schemaRevision: 1;
  createdAt: string;
  privacyEpoch: number;
  processingPolicyId: string;
  expiresAt: string | null;
  contentHash: string;
}
export interface DerivedEnvelope extends RecordEnvelope {
  coordinationPolicyHash: string;
  coordinationPolicyEpoch: number;
}

export interface EpisodeRecord extends RecordEnvelope {
  recordType: "episode";
  id: string;
  contentHash: string;
  sourceEntryId: string;
  host: HostId;
  projectId: string;
  projectIdentityKind: "registered" | "local_only";
  sessionId: string;
  turnId: string;
  agentRole: "root" | "child";
  depth: number;
  eventKind: "user" | "assistant" | "tool_call" | "tool_result" | "tool_error";
  eventAt: string;
  modelId: string;
  embeddingDimension: number;
  originProvider: string;
  destinationId: string;
  status: "active";
  /** Structural redaction of the persisted text/tool excerpts; final scan remains passed. */
  redactionStatus: "unchanged" | "redacted";
  secretScan: "passed";
  text?: string;
  toolName?: string;
  toolArgs?: string;
  errorFingerprint?: string;
  vector?: number[];
  producerId?: string;
  nodeId?: string;
}
/**
 * The sole semantic projection that crosses Task 7 document egress.  It is
 * deterministic, scanner-safe by construction, and never includes the
 * high-entropy error fingerprint itself (only its safe presence marker).
 */
export function episodeSemanticProjection(episode: Pick<EpisodeRecord, "eventKind" | "text" | "toolName" | "toolArgs" | "errorFingerprint">): string {
  const parts = [`event:${episode.eventKind}`];
  if (episode.toolName !== undefined) parts.push(`tool:${episode.toolName}`);
  if (episode.text !== undefined) parts.push(`text:${episode.text}`);
  if (episode.toolArgs !== undefined) parts.push(`tool_args:${episode.toolArgs}`);
  if (episode.errorFingerprint !== undefined) parts.push("error_fingerprint:present");
  return parts.join("\n");
}

export interface CuratedMemoryRecord extends DerivedEnvelope {
  recordType: "curated_memory";
  id: string;
  contentId: string;
  observationId: string;
  eventAt: string;
  effectiveAt: string;
  sourceEpisodeIds?: string[];
  manifestHash?: string;
  primaryEvidenceEpisodeId?: string;
  effectiveOrder: EffectiveOrder;
  stateKey?: string;
  category?: string;
  scope?: string;
  subject?: string;
  predicate?: string;
  value?: unknown;
  text?: string;
  provenance?: string[];
  confidence?: number;
  vector?: number[];
}
interface CuratedCurrentBase extends DerivedEnvelope {
  recordType: "curated_current";
  id: string;
  version: number;
  stateKey: string;
  effectiveOrder: EffectiveOrder;
  sourceEpisodeIds?: string[];
  text?: string;
  vector?: number[];
}
export interface CuratedCurrentResolvedRecord extends CuratedCurrentBase {
  resolution: "resolved";
  contentId: string;
  observationId: string;
  conflictManifestHash?: never;
}
export interface CuratedCurrentConflictRecord extends CuratedCurrentBase {
  resolution: "conflict";
  conflictManifestHash: string;
  contentId?: never;
  observationId?: never;
}
export type CuratedCurrentRecord = CuratedCurrentResolvedRecord | CuratedCurrentConflictRecord;
export interface RaptorSummaryRecord extends DerivedEnvelope {
  recordType: "raptor_summary";
  id: string;
  generationId: string;
  clusterId: string;
  membershipHash: string;
  level: number;
  memberIds?: string[];
  manifestHash?: string;
  summary: string;
  vector?: number[];
  modelId: string;
  embeddingDimension: number;
  promptRevision: string;
  algorithm: string;
  seed: number;
  jobId: string;
  fencingToken: number;
  temporalFrom: string;
  temporalTo: string;
  coveredProjects: string[];
  algorithmParameters: unknown;
}
export interface ControlRecord extends RecordEnvelope {
  recordType: "collection_control";
  id: string;
  version: number;
  activeGeneration: string | null;
  activeBaseGeneration: string | null;
  privacyEpoch: number;
  coordinationPolicyEpoch: number;
  coordinationPolicyHash: string;
  state: "active" | "draining" | "retired";
  scanCursor: string | null;
  lastForgetBarrier: string | null;
  /** Bounded, exact, redacted, monotonic collection-wide destination revocations. */
  revokedDestinationIds: string[];
}
export interface ProcessingPolicyRecord extends RecordEnvelope {
  recordType: "processing_policy";
  id: string;
  policy: ProcessingPolicy;
  canonicalHash: string;
  expiresAt: string | null;
}
/**
 * Immutable explicit-membership job identity. `policyId` is the serialized
 * producer-policy intersection. Mutable claim/acceptance state lives only on
 * the per-job `lease` point; proposals are immutable outputs.
 */
export interface JobRecord extends DerivedEnvelope {
  recordType: "job";
  id: string;
  policyId: string;
  policyHash: string;
  policyEpoch: number;
  membership: string[];
  extractorRevision: string;
}
/**
 * Mutable per-job lease/claim point: version and fencing token are monotonic;
 * acceptance is the single authority recorded here (exactly one proposal).
 */
export interface LeaseRecord extends DerivedEnvelope {
  recordType: "lease";
  id: string;
  jobId: string;
  ownerId: string;
  version: number;
  fencingToken: number;
  expiresAt: string;
  state: "leased" | "accepted" | "released";
  acceptedProposalId: string | null;
  acceptedManifestHash: string | null;
}
/** Immutable proposal output: bounded canonical membership + validated content. */
export interface ProposalRecord extends DerivedEnvelope {
  recordType: "proposal";
  id: string;
  jobId: string;
  /** Nominal producing root identity (derived from the LeaseAuthority nodeId); immutable. */
  ownerId: string;
  proposalHash: string;
  manifestHash: string;
  fencingToken: number;
  membership: string[];
  content: unknown;
}
export interface CoverageRecord extends DerivedEnvelope {
  recordType: "coverage";
  id: string;
  episodeId: string;
  extractorRevision: string;
}
export interface EvidenceLinkRecord extends DerivedEnvelope {
  recordType: "evidence_link";
  id: string;
  sourceId: string;
  targetId: string;
  jobId: string;
  extractorRevision: string;
}
export interface TombstoneRecord extends RecordEnvelope {
  recordType: "tombstone";
  id: string;
  scope: "occurrence" | "content" | "state";
  targetId: string;
  provenanceId?: string;
}
export type MemoryRecord = EpisodeRecord | CuratedMemoryRecord | CuratedCurrentRecord | RaptorSummaryRecord | ControlRecord | ProcessingPolicyRecord | JobRecord | LeaseRecord | ProposalRecord | CoverageRecord | EvidenceLinkRecord | TombstoneRecord;

export interface RecordValidationContext {
  ownerHost?: HostId;
  schemaRevision?: number;
  privacyEpoch?: number;
  policyEpoch?: number;
  coordinationPolicyEpoch?: number;
  maxTextChars?: number;
  vectorDimension?: number;
}

const COMMON_KEYS = new Set(["recordType", "id", "ownerHost", "schemaRevision", "createdAt", "privacyEpoch", "processingPolicyId", "expiresAt", "contentHash"]);
const DERIVED_KEYS = new Set(["coordinationPolicyHash", "coordinationPolicyEpoch"]);
const RECORD_KEYS: Record<string, ReadonlySet<string>> = {
  episode: new Set([...COMMON_KEYS, "sourceEntryId", "host", "projectId", "projectIdentityKind", "sessionId", "turnId", "agentRole", "depth", "eventKind", "eventAt", "modelId", "embeddingDimension", "originProvider", "destinationId", "status", "redactionStatus", "secretScan", "text", "toolName", "toolArgs", "errorFingerprint", "vector", "producerId", "nodeId"]),
  curated_memory: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "contentId", "observationId", "eventAt", "effectiveAt", "sourceEpisodeIds", "manifestHash", "primaryEvidenceEpisodeId", "effectiveOrder", "stateKey", "category", "scope", "subject", "predicate", "value", "text", "provenance", "confidence", "vector"]),
  curated_current: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "contentId", "observationId", "version", "stateKey", "resolution", "conflictManifestHash", "effectiveOrder", "sourceEpisodeIds", "text", "vector"]),
  raptor_summary: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "generationId", "clusterId", "membershipHash", "level", "memberIds", "manifestHash", "summary", "vector", "modelId", "embeddingDimension", "promptRevision", "algorithm", "seed", "jobId", "fencingToken", "temporalFrom", "temporalTo", "coveredProjects", "algorithmParameters"]),
  collection_control: new Set([...COMMON_KEYS, "version", "activeGeneration", "activeBaseGeneration", "privacyEpoch", "coordinationPolicyEpoch", "coordinationPolicyHash", "state", "scanCursor", "lastForgetBarrier", "revokedDestinationIds"]),
  processing_policy: new Set([...COMMON_KEYS, "policy", "canonicalHash"]),
  job: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "policyId", "policyHash", "policyEpoch", "membership", "extractorRevision"]),
  lease: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "jobId", "ownerId", "version", "fencingToken", "state", "acceptedProposalId", "acceptedManifestHash"]),
  proposal: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "jobId", "ownerId", "proposalHash", "manifestHash", "fencingToken", "membership", "content"]),
  coverage: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "episodeId", "extractorRevision"]),
  evidence_link: new Set([...COMMON_KEYS, ...DERIVED_KEYS, "sourceId", "targetId", "jobId", "extractorRevision"]),
  tombstone: new Set([...COMMON_KEYS, "scope", "targetId", "provenanceId"]),
};
function isRecord(value: unknown): value is PlainRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function fail(message: string): never { throw new TypeError(`Invalid memory record: ${message}`); }
function text(name: string, value: unknown, max = MAX_ID_CHARS, redact = name.toLowerCase().includes("id")): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > max || (redact && SECRET_ID.test(value))) fail(`${name} must be bounded and redacted`);
}
function finite(name: string, value: unknown): asserts value is number { if (typeof value !== "number" || !Number.isFinite(value)) fail(`${name} must be finite`); }
function integer(name: string, value: unknown, min = 0, max = 4294967295): asserts value is number { finite(name, value); if (!Number.isSafeInteger(value) || value < min || value > max) fail(`${name} must be a bounded integer`); }
function isoDate(name: string, value: unknown): asserts value is string {
  text(name, value, 24, false); const match = ISO_DATE.exec(value); const year = match === null ? -1 : Number(match[1]);
  const parsed = match === null ? Number.NaN : Date.parse(value); const instant = new Date(parsed);
  if (match === null || year < 1970 || year > 2100 || !Number.isFinite(parsed) || instant.toISOString() !== value) fail(`${name} must be a bounded ISO timestamp`);
}
function expiry(value: unknown): void { if (value !== null) isoDate("expiresAt", value); }
function host(name: string, value: unknown): asserts value is HostId { if (value !== "pi" && value !== "prime") fail(`${name} is invalid`); }
function ids(name: string, value: unknown): asserts value is string[] { if (!Array.isArray(value) || value.length === 0 || value.length > MAX_ARRAY) fail(`${name} must be bounded IDs`); value.forEach((item) => text(name, item, MAX_ID_CHARS, true)); }
function strictlySorted(name: string, value: string[]): void { for (let index = 1; index < value.length; index += 1) if (value[index - 1]! >= value[index]!) fail(`${name} must be strictly sorted and unique`); }
function vector(value: unknown, expected?: number): void { if (!Array.isArray(value) || (expected !== undefined && value.length !== expected)) fail("vector dimension is invalid"); value.forEach((item) => finite("vector element", item)); }
function optionalText(name: string, value: unknown, max = MAX_ID_CHARS, redact = name.toLowerCase().includes("id")): void { if (value !== undefined && value !== null) text(name, value, max, redact); }
function derived(value: PlainRecord, context: RecordValidationContext): void {
  text("coordinationPolicyHash", value.coordinationPolicyHash, MAX_ID_CHARS, false); integer("coordinationPolicyEpoch", value.coordinationPolicyEpoch);
  if (context.coordinationPolicyEpoch !== undefined && value.coordinationPolicyEpoch !== context.coordinationPolicyEpoch) fail("coordination policy epoch mismatch");
}
function common(value: PlainRecord, context: RecordValidationContext): void {
  text("id", value.id); host("ownerHost", value.ownerHost); if (value.schemaRevision !== RECORD_SCHEMA_REVISION) fail("schema revision mismatch");
  isoDate("createdAt", value.createdAt); integer("privacyEpoch", value.privacyEpoch); text("processingPolicyId", value.processingPolicyId); expiry(value.expiresAt); text("contentHash", value.contentHash, MAX_ID_CHARS, false);
  if (context.ownerHost !== undefined && value.ownerHost !== context.ownerHost) fail("owner host mismatch");
  if (context.schemaRevision !== undefined && value.schemaRevision !== context.schemaRevision) fail("schema revision mismatch");
  if (context.privacyEpoch !== undefined && value.privacyEpoch !== context.privacyEpoch) fail("privacy epoch mismatch");
}
function validatePolicy(value: unknown): asserts value is ProcessingPolicy {
  if (!isRecord(value)) fail("policy must be an object");
  const keys = new Set(["id", "ownerHost", "destinationIds", "originProvider", "allowCrossProviderReplay", "expiresAt", "residency", "dataUse", "policyRevision"]);
  for (const key of Object.keys(value)) if (!keys.has(key)) fail(`unknown policy field ${key}`);
  try {
    if (value.ownerHost !== "pi" && value.ownerHost !== "prime") throw new Error("owner");
    const hash = processingPolicyHash(value as unknown as ProcessingPolicy);
    if (value.id !== hash) throw new Error("policy id");
  } catch { fail("invalid processing policy"); }
}
function validate(value: PlainRecord, context: RecordValidationContext): MemoryRecord {
  const recordType = value.recordType;
  if (typeof recordType !== "string" || !(recordType in RECORD_KEYS)) fail("unknown record type");
  for (const key of Object.keys(value)) if (!RECORD_KEYS[recordType]!.has(key)) fail(`unknown field ${key}`);
  common(value, context);
  const isDerived = ["curated_memory", "curated_current", "raptor_summary", "job", "lease", "proposal", "coverage", "evidence_link"].includes(recordType);
  if (isDerived) derived(value, context);
  switch (recordType) {
    case "episode":
      text("sourceEntryId", value.sourceEntryId); host("host", value.host); if (value.host !== value.ownerHost) fail("episode host mismatch"); text("projectId", value.projectId); if (value.projectIdentityKind !== "registered" && value.projectIdentityKind !== "local_only") fail("project identity kind invalid"); text("sessionId", value.sessionId); text("turnId", value.turnId); if (value.agentRole !== "root" && value.agentRole !== "child") fail("agent role invalid"); integer("depth", value.depth); if (!["user", "assistant", "tool_call", "tool_result", "tool_error"].includes(String(value.eventKind))) fail("event kind invalid"); isoDate("eventAt", value.eventAt); text("modelId", value.modelId); integer("embeddingDimension", value.embeddingDimension, 1, 65536); if (value.embeddingDimension !== (context.vectorDimension ?? 1024)) fail("embedding dimension mismatch"); text("originProvider", value.originProvider, MAX_ID_CHARS, false); text("destinationId", value.destinationId); if (value.status !== "active") fail("episode status invalid"); if (value.redactionStatus !== "unchanged" && value.redactionStatus !== "redacted") fail("redaction status invalid"); if (value.secretScan !== "passed") fail("secret scan invalid");
      if (value.text !== undefined) text("text", value.text, context.maxTextChars ?? MAX_TEXT_CHARS, false); if (value.toolName !== undefined) text("toolName", value.toolName, MAX_ID_CHARS, false); if (value.toolArgs !== undefined) text("toolArgs", value.toolArgs, context.maxTextChars ?? MAX_TEXT_CHARS, false); if (value.errorFingerprint !== undefined) text("errorFingerprint", value.errorFingerprint, MAX_ID_CHARS, false); if (value.producerId !== undefined) text("producerId", value.producerId); if (value.nodeId !== undefined) text("nodeId", value.nodeId); if (value.vector !== undefined) vector(value.vector, context.vectorDimension ?? 1024); return value as unknown as EpisodeRecord;
    case "curated_memory":
      text("contentId", value.contentId); text("observationId", value.observationId); isoDate("eventAt", value.eventAt); isoDate("effectiveAt", value.effectiveAt); try { validateEffectiveOrder(value.effectiveOrder); } catch { fail("effectiveOrder is invalid"); } if (value.sourceEpisodeIds !== undefined) ids("sourceEpisodeIds", value.sourceEpisodeIds); if (value.manifestHash !== undefined) text("manifestHash", value.manifestHash, MAX_ID_CHARS, false); if (value.primaryEvidenceEpisodeId !== undefined) text("primaryEvidenceEpisodeId", value.primaryEvidenceEpisodeId); if (value.sourceEpisodeIds === undefined && value.manifestHash === undefined && value.primaryEvidenceEpisodeId === undefined) fail("derived source/manifest closure missing"); if (value.provenance !== undefined) ids("provenance", value.provenance); optionalText("stateKey", value.stateKey); optionalText("category", value.category, MAX_ID_CHARS, false); optionalText("scope", value.scope, MAX_ID_CHARS, false); optionalText("subject", value.subject, MAX_ID_CHARS, false); optionalText("predicate", value.predicate, MAX_ID_CHARS, false); if (value.value !== undefined) { try { const serialized = canonicalStringify(value.value); if (serialized.length > (context.maxTextChars ?? MAX_TEXT_CHARS)) fail("value is unbounded"); } catch { fail("value is not canonical JSON"); } } if (value.text !== undefined) text("text", value.text, context.maxTextChars ?? MAX_TEXT_CHARS, false); if (value.confidence !== undefined) { finite("confidence", value.confidence); if (value.confidence < 0 || value.confidence > 1) fail("confidence invalid"); } if (value.vector !== undefined) vector(value.vector, context.vectorDimension ?? 1024); return value as unknown as CuratedMemoryRecord;
    case "curated_current":
      integer("version", value.version, 1); text("stateKey", value.stateKey); try { validateEffectiveOrder(value.effectiveOrder); } catch { fail("effectiveOrder is invalid"); } if (value.resolution === "resolved") { text("contentId", value.contentId); text("observationId", value.observationId); if (value.conflictManifestHash !== undefined) fail("resolved current cannot carry a conflict manifest"); } else if (value.resolution === "conflict") { if (Object.prototype.hasOwnProperty.call(value, "contentId") || Object.prototype.hasOwnProperty.call(value, "observationId")) fail("conflict current cannot select content or observation"); text("conflictManifestHash", value.conflictManifestHash, MAX_ID_CHARS, false); } else fail("resolution invalid"); if (value.sourceEpisodeIds !== undefined) ids("sourceEpisodeIds", value.sourceEpisodeIds); if (value.text !== undefined) text("text", value.text, context.maxTextChars ?? MAX_TEXT_CHARS, false); if (value.vector !== undefined) vector(value.vector, context.vectorDimension ?? 1024); return value as unknown as CuratedCurrentRecord;
    case "raptor_summary":
      text("generationId", value.generationId); text("clusterId", value.clusterId); text("membershipHash", value.membershipHash, MAX_ID_CHARS, false); integer("level", value.level); if (value.memberIds !== undefined) ids("memberIds", value.memberIds); if (value.manifestHash === undefined && value.memberIds === undefined) fail("summary source/manifest closure missing"); if (value.manifestHash !== undefined) text("manifestHash", value.manifestHash, MAX_ID_CHARS, false); if (value.memberIds !== undefined && value.membershipHash !== canonicalManifestHash(value.memberIds)) fail("summary membership hash mismatch"); text("summary", value.summary, context.maxTextChars ?? MAX_TEXT_CHARS, false); text("modelId", value.modelId); integer("embeddingDimension", value.embeddingDimension, 1, 65536); if (value.embeddingDimension !== (context.vectorDimension ?? 1024)) fail("embedding dimension mismatch"); text("promptRevision", value.promptRevision); text("algorithm", value.algorithm, MAX_ID_CHARS, false); integer("seed", value.seed); text("jobId", value.jobId); integer("fencingToken", value.fencingToken); isoDate("temporalFrom", value.temporalFrom); isoDate("temporalTo", value.temporalTo); if (Date.parse(value.temporalFrom) > Date.parse(value.temporalTo)) fail("summary temporal range is inverted"); ids("coveredProjects", value.coveredProjects); try { const parameters = canonicalStringify(value.algorithmParameters); if (parameters.length > 4096) fail("algorithm parameters are unbounded"); } catch { fail("algorithm parameters are not canonical"); } if (value.vector !== undefined) vector(value.vector, context.vectorDimension ?? 1024); return value as unknown as RaptorSummaryRecord;
    case "collection_control":
      integer("version", value.version, 1); if (value.activeGeneration !== null) text("activeGeneration", value.activeGeneration); if (value.activeBaseGeneration !== null) text("activeBaseGeneration", value.activeBaseGeneration); integer("privacyEpoch", value.privacyEpoch); integer("coordinationPolicyEpoch", value.coordinationPolicyEpoch); text("coordinationPolicyHash", value.coordinationPolicyHash, MAX_ID_CHARS, false); if (!["active", "draining", "retired"].includes(String(value.state))) fail("control state invalid"); if (value.scanCursor !== null) text("scanCursor", value.scanCursor); if (value.lastForgetBarrier !== null) isoDate("lastForgetBarrier", value.lastForgetBarrier);
      if (!Array.isArray(value.revokedDestinationIds) || value.revokedDestinationIds.length > 1024) fail("revokedDestinationIds must be bounded"); value.revokedDestinationIds.forEach((destination, index) => { if (typeof destination !== "string" || destination.length === 0 || destination.length > 256 || !/^[A-Za-z0-9._:/-]+$/u.test(destination) || SECRET_ID.test(destination)) fail(`revokedDestinationIds[${index}] must be an exact redacted destination ID`); }); if (new Set(value.revokedDestinationIds).size !== value.revokedDestinationIds.length) fail("revokedDestinationIds must not repeat"); return value as unknown as ControlRecord;
    case "processing_policy":
      validatePolicy(value.policy); if (value.policy.ownerHost !== value.ownerHost) fail("policy owner mismatch"); if (value.expiresAt !== value.policy.expiresAt || value.processingPolicyId !== value.policy.id) fail("processing policy envelope mismatch"); if (value.canonicalHash !== processingPolicyHash(value.policy) || value.id !== value.canonicalHash || value.canonicalHash !== value.policy.id) fail("processing policy canonical hash mismatch"); return value as unknown as ProcessingPolicyRecord;
    case "job": {
      text("policyId", value.policyId); if (value.policyId !== value.processingPolicyId) fail("job policy ID mismatch"); text("policyHash", value.policyHash, MAX_ID_CHARS, false); if (value.policyHash !== value.coordinationPolicyHash) fail("job policy hash mismatch"); integer("policyEpoch", value.policyEpoch); if (value.policyEpoch !== value.coordinationPolicyEpoch) fail("job policy epoch mismatch"); ids("membership", value.membership); strictlySorted("job membership", value.membership); text("extractorRevision", value.extractorRevision, MAX_ID_CHARS, true); const jobOwner = value.ownerHost; host("job.ownerHost", jobOwner); integer("job.privacyEpoch", value.privacyEpoch); try { if (value.id !== jobId(jobOwner, value.membership, value.policyHash, value.extractorRevision, value.policyEpoch, value.policyId, value.privacyEpoch)) fail("job ID formula mismatch"); } catch { fail("job ID formula mismatch"); } if (context.policyEpoch !== undefined && value.policyEpoch !== context.policyEpoch) fail("policy epoch mismatch"); return value as unknown as JobRecord;
    }
    case "lease":
      text("jobId", value.jobId); text("ownerId", value.ownerId); integer("version", value.version, 1); integer("fencingToken", value.fencingToken); if (value.expiresAt === null) fail("lease requires an expiry"); isoDate("expiresAt", value.expiresAt); if (value.state !== "leased" && value.state !== "accepted" && value.state !== "released") fail("lease state invalid"); if (value.acceptedProposalId !== null) text("acceptedProposalId", value.acceptedProposalId); if (value.acceptedManifestHash !== null) text("acceptedManifestHash", value.acceptedManifestHash, MAX_ID_CHARS, false); if ((value.acceptedProposalId === null) !== (value.acceptedManifestHash === null)) fail("lease acceptance fields must move together"); if (value.state === "leased" && value.acceptedProposalId !== null) fail("leased claim cannot carry acceptance"); if (value.state === "accepted" && value.acceptedProposalId === null) fail("accepted claim requires proposal and manifest"); try { if (value.id !== leasePointId(value.jobId)) fail("lease ID formula mismatch"); } catch { fail("lease ID formula mismatch"); } return value as unknown as LeaseRecord;
    case "proposal": {
      text("jobId", value.jobId); text("ownerId", value.ownerId); if (!/^[0-9a-f]{64}$/u.test(String(value.proposalHash))) fail("proposal hash must be a SHA-256 hex digest"); text("manifestHash", value.manifestHash, MAX_ID_CHARS, false); integer("fencingToken", value.fencingToken); ids("membership", value.membership); strictlySorted("proposal membership", value.membership); if (value.manifestHash !== canonicalManifestHash(value.membership)) fail("proposal manifest hash mismatch"); const proposalOwner = value.ownerHost; host("proposal.ownerHost", proposalOwner); text("proposal.coordinationPolicyHash", value.coordinationPolicyHash, MAX_ID_CHARS, false); integer("proposal.coordinationPolicyEpoch", value.coordinationPolicyEpoch); integer("proposal.privacyEpoch", value.privacyEpoch); text("proposal.processingPolicyId", value.processingPolicyId); try { const content = canonicalStringify(value.content); if (content.length > (context.maxTextChars ?? MAX_TEXT_CHARS)) fail("proposal content is unbounded"); const recomputed = proposalContentHash({ ownerHost: proposalOwner, jobId: value.jobId, ownerId: value.ownerId, membership: value.membership, content: value.content, policyHash: value.coordinationPolicyHash, policyEpoch: value.coordinationPolicyEpoch, fencingToken: value.fencingToken, privacyEpoch: value.privacyEpoch, policyIntersectionId: value.processingPolicyId }); if (value.proposalHash !== recomputed) fail("proposal content hash mismatch"); } catch { fail("proposal content is not canonical"); } try { if (value.id !== proposalIdFor(value.jobId, value.proposalHash, value.coordinationPolicyEpoch, value.fencingToken)) fail("proposal ID formula mismatch"); } catch { fail("proposal ID formula mismatch"); } return value as unknown as ProposalRecord;
    }
    case "coverage": {
      text("episodeId", value.episodeId); text("extractorRevision", value.extractorRevision, MAX_ID_CHARS, true); integer("coverage.coordinationPolicyEpoch", value.coordinationPolicyEpoch); text("coverage.coordinationPolicyHash", value.coordinationPolicyHash, MAX_ID_CHARS, false); text("coverage.processingPolicyId", value.processingPolicyId); integer("coverage.privacyEpoch", value.privacyEpoch); const coverageOwner = value.ownerHost; host("coverage.ownerHost", coverageOwner); try { if (value.id !== coverageId({ ownerHost: coverageOwner, episodeId: value.episodeId, extractorRevision: value.extractorRevision, coordinationPolicyHash: value.coordinationPolicyHash, coordinationPolicyEpoch: value.coordinationPolicyEpoch, policyIntersectionId: value.processingPolicyId, privacyEpoch: value.privacyEpoch })) fail("coverage ID formula mismatch"); } catch { fail("coverage ID formula mismatch"); } return value as unknown as CoverageRecord;
    }
    case "evidence_link": text("sourceId", value.sourceId); text("targetId", value.targetId); text("jobId", value.jobId); text("extractorRevision", value.extractorRevision, MAX_ID_CHARS, true); return value as unknown as EvidenceLinkRecord;
    case "tombstone": {
      if (value.scope !== "occurrence" && value.scope !== "content" && value.scope !== "state") fail("tombstone scope invalid"); text("targetId", value.targetId); if (value.provenanceId !== undefined) text("provenanceId", value.provenanceId); const tombOwner = value.ownerHost; host("tombstone.ownerHost", tombOwner); if (!(value.scope === "occurrence" ? /^(?:occurrence:[0-9a-f]{64}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/iu.test(String(value.targetId)) : value.scope === "content" ? /^content:[0-9a-f]{64}$/u.test(String(value.targetId)) : /^state:[0-9a-f]{64}$/u.test(String(value.targetId)))) fail("tombstone target does not match its scope"); try { if (value.id !== tombstoneId(tombOwner, value.targetId)) fail("tombstone ID formula mismatch"); } catch { fail("tombstone ID formula mismatch"); } return value as unknown as TombstoneRecord;
    }
  }
  return fail("unknown record type");
}
export function parseMemoryRecord(value: unknown, context: RecordValidationContext = {}): MemoryRecord { if (!isRecord(value)) fail("record must be an object"); return validate(value, context); }
export function assertMemoryRecord(value: unknown, context: RecordValidationContext = {}): asserts value is MemoryRecord { parseMemoryRecord(value, context); }
export function isMemoryRecord(value: unknown, context: RecordValidationContext = {}): value is MemoryRecord { try { parseMemoryRecord(value, context); return true; } catch { return false; } }
export function canonicalRecordHash(record: MemoryRecord): string {
  const validated = parseMemoryRecord(record); const copy: PlainRecord = { ...(validated as unknown as PlainRecord) };
  delete copy.contentHash; delete copy.createdAt; delete copy.producerId; delete copy.nodeId;
  // The episode content hash COMMITS the exact embedded vector (1024 floats):
  // a changed/missing vector changes the hash, so a persisted point's hash
  // cryptographically binds the vector readback. Other record types keep the
  // contractual exclusion (vectors are query artifacts, not identity).
  if (record.recordType !== "episode") delete copy.vector;
  // The processing-policy point identity is content-addressed by the policy
  // itself; the observed envelope privacy epoch is not part of that identity,
  // so reusing an unchanged policy after a privacy-epoch increment converges
  // (insert-only "existing") instead of colliding on a same-ID/different-hash.
  if (record.recordType === "processing_policy") delete copy.privacyEpoch;
  // Tombstone identity is target/scope-stable under the fixed
  // H(owner,"tombstone",target) formula: the envelope privacy epoch and
  // processing-policy intersection are informational (occurrence visibility is
  // permanent), so repeated same-target forget across privacy AND
  // processing-policy changes converges deterministically even under
  // concurrency instead of content-hash colliding.
  if (record.recordType === "tombstone") { delete copy.privacyEpoch; delete copy.processingPolicyId; }
  return sha256Hex(canonicalStringify(copy));
}
export function assertCanonicalRecordHash(record: MemoryRecord): void { if (record.contentHash !== canonicalRecordHash(record)) throw new TypeError("Memory record canonical hash mismatch"); }

export function parsePersistedMemoryRecord(value: unknown, context: RecordValidationContext = {}): MemoryRecord {
  const record = parseMemoryRecord(value, context);
  assertCanonicalRecordHash(record);
  return record;
}

export function isPersistedMemoryRecord(value: unknown, context: RecordValidationContext = {}): value is MemoryRecord {
  try { parsePersistedMemoryRecord(value, context); return true; } catch { return false; }
}
