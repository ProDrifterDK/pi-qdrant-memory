import { types as nodeTypes } from "node:util";
import { canonicalRecordHash, parseMemoryRecord, type ConflictManifestRecord, type ControlRecord, type CoverageRecord, type CuratedCurrentRecord, type CuratedMemoryRecord, type EpisodeRecord, type EvidenceLinkRecord, type JobRecord, type LeaseRecord, type MemoryRecord, type ProcessingPolicyRecord, type RaptorSummaryRecord, type ProposalRecord, type TombstoneRecord } from "../domain/records.js";
import { canonicalStringify } from "../domain/canonical.js";
import { bindConfiguredDestination, canonicalEgressEndpoint, gateCuratedEgressText } from "../security/egress.js";
import type { AuthorizedDestination, HostId, RuntimeConfig } from "../types.js";
import { physicalPointId, COLLECTION_CONTROL_ID, assertBootstrapControl, controlPayload, controlRecordFromPayload, isPhysicalPointId, type PointRecordType } from "./schema.js";
import { expectedQdrantCollection, readPolicy, validatePurpose, type ControlCasPrecondition, type CuratedCurrentCasPrecondition, type HostScopedQdrantCollection, type LeaseCasPrecondition, type PointId, type PreparedPoint, type QdrantClientOptions, type QdrantCollectionInfo, type QdrantPoint, type QdrantReadClient, type QdrantReadPolicy, type QdrantScrollResult, type QdrantSearchHit, type ReadOptions, type TypedUpdatePrecondition } from "./client.js";
import { fetchJson, fetchOk, MemoryClientError } from "../clients/http.js";
import { contentId, curatedCurrentId, evidenceLinkId, jobId, leasePointId, manifestHash, observationId, proposalContentHash, proposalIdFor, stateKey, tombstoneId, coverageId, isContentTarget, isOccurrenceTarget, isStateTarget, isTombstoneTarget, conflictManifestId, type TombstoneScope } from "../domain/ids.js";
import { assertPersistableCurationResult, validateCurationResult, type CurationItem, type CurationResult } from "../curation/validate.js";
import { jobExpired } from "../coordination/deadline.js";
import { RootWorkerContext } from "../coordination/root.js";
import { QdrantContentHashCollisionError, QdrantLegacyEpisodeHashError } from "../domain/qdrant-errors.js";
export { QdrantContentHashCollisionError, QdrantLegacyEpisodeHashError, QDRANT_CONTENT_HASH_COLLISION, QDRANT_LEGACY_EPISODE_HASH } from "../domain/qdrant-errors.js";
import type { ClaimLeaseInput } from "../coordination/leases.js";
import type { CreateJobInput, ProposalContent, WriteProposalInput } from "../coordination/jobs.js";
import type { CreateTombstoneInput } from "../coordination/tombstones.js";
import type { MarkCoverageInput } from "../coordination/reconcile.js";
import { parseCurationProposalEnvelope, provenanceMatches } from "../curation/provenance.js";
import { CURATION_PROMPT_REVISION } from "../curation/prompt.js";
import { projectCurationItem as sharedProjectCurationItem, projectConflictAggregate, compareProjectionOrders } from "../curation/projection.js";
type Payload = Record<string, unknown>;
type Consistency = number | "majority" | "quorum" | "all";
type JsonRecord = Record<string, unknown>;
// Strict curation bounds permit 32 items, 1,024 conflict members per item,
// and 16 source episodes per member. Keep every legitimate tombstone closure
// completable while retaining a finite fail-closed ceiling.
const MAX_COMPLETION_DERIVED_TARGETS = 600_000;
/** Lexical (non-exported) raw write-capability type; never appears in the public d.ts. */
interface QdrantSessionWriter extends QdrantReadClient { upsertPoints(points: readonly PreparedPoint[], mode: "insert_only" | "update_only", precondition?: TypedUpdatePrecondition): Promise<void>; }
/** Truthful minimum capability required by insert/readback verification (lexical). */
type QdrantWriteVerificationClient = Pick<QdrantSessionWriter, "endpoint" | "ownerHost" | "collection" | "maxClockSkewMs" | "retrieve" | "upsertPoints">;
/** Lexical raw-protocol seam used ONLY by the #private store protocol; never exported. */
interface CoordinationStore {
  readonly ownerHost: "pi" | "prime";
  readControl(): Promise<ControlRecord>;
  compareAndSwapControl(expectedVersion: number, next: ControlRecord): Promise<boolean>;
  readLease(jobId: string): Promise<LeaseRecord | null>;
  insertLease(lease: LeaseRecord): Promise<"inserted" | "existing">;
  casLease(input: { jobId: string; expectedVersion: number; expectedFencingToken: number; expectedPolicyEpoch: number; expectedPolicyHash: string; expectedPrivacyEpoch: number; expectedState: "leased" | "accepted" | "released" | "completed"; expectedOwner: string; expectedAcceptedProposalId: string | null; expectedAcceptedManifestHash: string | null; expectedProcessingPolicyId: string; expectedCreatedAt: string; expectedContentHash: string; expiresBefore?: number; expiresAfter?: number; next: LeaseRecord }): Promise<boolean>;
  readJob(jobId: string): Promise<JobRecord | null>;
  insertJob(job: JobRecord): Promise<"inserted" | "existing">;
  readProposal(proposalId: string): Promise<ProposalRecord | null>;
  insertProposal(proposal: ProposalRecord): Promise<"inserted" | "existing">;
  readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]>;
  insertTombstone(tombstone: TombstoneRecord): Promise<"inserted" | "existing">;
  readCoverage(coverageIds: readonly string[]): Promise<CoverageRecord[]>;
  insertCoverage(coverage: CoverageRecord): Promise<"inserted" | "existing">;
  scrollLeases(offset?: string, limit?: number): Promise<{ leases: LeaseRecord[]; nextOffset?: string }>;
  scrollJobs(offset?: string, limit?: number): Promise<{ jobs: JobRecord[]; nextOffset?: string }>;
  readEpisode(episodeId: string): Promise<EpisodeRecord | null>;
  readEpisodes(episodeIds: readonly string[], expectedPrivacyEpoch?: number): Promise<EpisodeRecord[]>;
  readCurated(recordType: "curated_memory" | "curated_current" | "conflict_manifest" | "evidence_link", id: string): Promise<CuratedMemoryRecord | CuratedCurrentRecord | ConflictManifestRecord | EvidenceLinkRecord | null>;
  insertCurated(record: CuratedMemoryRecord | CuratedCurrentRecord | EvidenceLinkRecord | ConflictManifestRecord): Promise<"inserted" | "existing">;
  casCuratedCurrent(input: { id: string; precondition: CuratedCurrentCasPrecondition; next: CuratedCurrentRecord }): Promise<boolean>;
  readRaptor(id: string): Promise<RaptorSummaryRecord | null>;
  insertRaptor(record: RaptorSummaryRecord): Promise<"inserted" | "existing">;
  publishGenerationControl(expectedVersion: number, expectedBaseGeneration: string | null, next: ControlRecord): Promise<boolean>;
}

/**
 * Package-internal safe-owner module.
 * THE single safe owner module for Qdrant write + Task 8 coordination
 * authority. The raw REST transport, the raw insert/CAS primitives, the
 * private write engine, `ProductionCoordinationStore` (with its true #-private
 * protocol), and EVERY lease/job/tombstone/reconcile/control protocol
 * implementation + the LeaseAuthority/QuiescenceProof capabilities are ALL
 * lexical in this module. Nothing raw, no *OnProtocol function, no issuer or
 * registrar is exported; the only exports are the safe creation APIs, safe
 * object types and the pure payload helpers. Public coordination modules are
 * thin Production-brand-only wrappers over the store's named safe methods.
 * No dynamic imports anywhere; the module graph is cycle-free (type-only
 * imports of the public coordination modules).
 */
/** Lexical (non-exported) raw write-capability type; never appears in the public d.ts. */

/**
 * @internal
 * THE safe owner module for Qdrant write authority. The raw REST transport
 * (read/session classes), the raw insert/CAS primitives and the private write
 * engine are ALL lexical in this module — none of them are exported, and no
 * other dist module recreates them under another name. The only production
 * consumer, `ProductionCoordinationStore`, lives HERE too: it builds its true
 * #-private raw protocol from the lexical engine over ONE lexical session.
 * Public creation APIs accept validated OPTIONS only and return only safe
 * objects (ProductionCoordinationStore, BoundQdrantDestination, the safe
 * bundle); they never accept or return a raw session/writer.
 */

function isRecord(value: unknown): value is JsonRecord { return typeof value === "object" && value !== null && !Array.isArray(value) && !nodeTypes.isProxy(value); }
/** Clone canonical JSON exclusively from own data descriptors. The caller graph
 * is inspected exactly once: proxies, accessors, symbols, sparse/extra array
 * keys, cycles and non-JSON values fail before any discriminator is read. */
function ownedCanonicalJsonSnapshot(value: unknown, label: string): unknown {
  const active = new Set<object>();
  const clone = (candidate: unknown): unknown => {
    if (candidate === null || typeof candidate === "string" || typeof candidate === "boolean") return candidate;
    if (typeof candidate === "number") {
      if (!Number.isFinite(candidate)) throw new TypeError(`${label} contains a non-finite number`);
      return candidate;
    }
    if (typeof candidate !== "object" || nodeTypes.isProxy(candidate)) throw new TypeError(`${label} contains a non-JSON value`);
    if (active.has(candidate)) throw new TypeError(`${label} is cyclic`);
    active.add(candidate);
    try {
      if (Object.getOwnPropertySymbols(candidate).length > 0) throw new TypeError(`${label} contains symbol keys`);
      const prototype = Object.getPrototypeOf(candidate);
      if (Array.isArray(candidate)) {
        if (prototype !== Array.prototype) throw new TypeError(`${label} array is invalid`);
        const lengthDescriptor = Object.getOwnPropertyDescriptor(candidate, "length");
        if (lengthDescriptor === undefined || !("value" in lengthDescriptor) || !Number.isSafeInteger(lengthDescriptor.value) || lengthDescriptor.value < 0 || lengthDescriptor.value > 16_384) throw new TypeError(`${label} array length is invalid`);
        const length = lengthDescriptor.value as number;
        const names = Object.getOwnPropertyNames(candidate);
        if (names.length !== length + 1 || !names.includes("length")) throw new TypeError(`${label} array is sparse or has extra keys`);
        const result: unknown[] = [];
        for (let index = 0; index < length; index += 1) {
          const descriptor = Object.getOwnPropertyDescriptor(candidate, String(index));
          if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) throw new TypeError(`${label} array contains an accessor or hole`);
          result.push(clone(descriptor.value));
        }
        return result;
      }
      if (prototype !== Object.prototype && prototype !== null) throw new TypeError(`${label} object is invalid`);
      const result: Record<string, unknown> = {};
      for (const name of Object.getOwnPropertyNames(candidate)) {
        const descriptor = Object.getOwnPropertyDescriptor(candidate, name);
        if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) throw new TypeError(`${label} contains an accessor or hidden field`);
        Object.defineProperty(result, name, { value: clone(descriptor.value), enumerable: true, writable: true, configurable: true });
      }
      return result;
    } finally { active.delete(candidate); }
  };
  const owned = clone(value);
  try { return JSON.parse(canonicalStringify(owned)) as unknown; }
  catch { throw new TypeError(`${label} is not canonical JSON`); }
}
/** Own a backend point before inspecting discriminators. */
function ownedPointSnapshot(value: unknown): QdrantPoint {
  const clone = ownedCanonicalJsonSnapshot(value, "Qdrant point");
  if (!isRecord(clone)) throw new TypeError("Qdrant point is invalid");
  return clone as unknown as QdrantPoint;
}

function ownedPointsSnapshot(values: readonly unknown[]): QdrantPoint[] {
  if (!Array.isArray(values)) throw new TypeError("Qdrant point list is invalid");
  return values.map((value) => ownedPointSnapshot(value));
}
function failInput(message: string): never { throw new MemoryClientError("configuration", message); }
function failResponse(message: string): never { throw new MemoryClientError("invalid-response", message); }
function validId(value: unknown): value is PointId { return isPhysicalPointId(value); }
function validateId(value: unknown): asserts value is PointId { if (!validId(value)) failInput("Point ID must be a UUID"); }
function validateResponseId(value: unknown): asserts value is PointId { if (!validId(value)) failResponse("Qdrant point ID is invalid"); }
function validateCollection(value: unknown): asserts value is string { if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$/u.test(value)) failInput("Collection name is invalid"); }
function baseUrl(value: string): string { let parsed: URL; try { parsed = new URL(value); } catch { return failInput("Qdrant endpoint is invalid"); } if (!["http:", "https:"].includes(parsed.protocol) || parsed.username !== "" || parsed.password !== "" || parsed.search !== "" || parsed.hash !== "") failInput("Qdrant endpoint is invalid"); return parsed.toString().replace(/\/+$/u, ""); }
function validateTimeout(value: unknown): void { if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) failInput("Request timeout is invalid"); }
function headers(key: string | undefined, json = false): Record<string, string> { const result: Record<string, string> = {}; if (json) result["content-type"] = "application/json"; if (key !== undefined) result["api-key"] = key; return result; }
function requestOptions(options: QdrantClientOptions, fetchImpl: typeof fetch): { timeoutMs: number; signal?: AbortSignal; fetchImpl: typeof fetch } { const result: { timeoutMs: number; signal?: AbortSignal; fetchImpl: typeof fetch } = { timeoutMs: options.timeoutMs, fetchImpl }; if (options.signal !== undefined) result.signal = options.signal; return result; }
function collectionPath(options: QdrantClientOptions, suffix = ""): string { return `${options.baseUrl}/collections/${encodeURIComponent(options.collection)}${suffix}`; }
function consistency(url: string, value: Consistency | undefined): string { if (value === undefined) return url; const parsed = new URL(url); parsed.searchParams.set("consistency", String(value)); return parsed.toString(); }
function validQdrantVersion(value: string): boolean { const match = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/u.exec(value); if (match === null) return false; const [major, minor, patch] = value.split("+")[0]!.split(".").map((part) => Number(part)); return [major, minor, patch].every((part) => Number.isSafeInteger(part)); }
function pointWriteUrl(options: QdrantClientOptions): string { const parsed = new URL(collectionPath(options, "/points")); parsed.searchParams.set("wait", "true"); parsed.searchParams.set("ordering", "strong"); return parsed.toString(); }
function isFiniteVector(value: unknown): value is number[] { return Array.isArray(value) && value.length === 1024 && value.every((part) => typeof part === "number" && Number.isFinite(part)); }
function validatePayload(value: unknown, response = false): JsonRecord { if (!isRecord(value)) { if (response) failResponse("Qdrant payload must be an object"); failInput("Qdrant payload must be an object"); } try { canonicalStringify(value); } catch { if (response) failResponse("Qdrant payload must be finite canonical JSON"); failInput("Qdrant payload must be finite canonical JSON"); } return value; }
function normalizePoint(value: PreparedPoint, payloadOnly = false): PreparedPoint { if (!isRecord(value) || !validId(value.id)) failInput("Prepared point ID must be a UUID"); const payload = validatePayload(value.payload); const point: PreparedPoint = { id: value.id, payload }; if (value.vector !== undefined) { if (!isRecord(value.vector)) failInput("Prepared point vector is invalid"); const keys = Object.keys(value.vector); if (keys.length === 0) point.vector = {}; else if (keys.length === 1 && keys[0] === "semantic" && isFiniteVector(value.vector.semantic)) point.vector = { semantic: [...value.vector.semantic] }; else failInput("Prepared point must contain one finite semantic vector or an empty payload-only map"); } else if (payloadOnly) point.vector = {}; return point; }
function envelope(value: unknown): unknown { if (!isRecord(value) || !("result" in value)) failResponse("Qdrant JSON envelope is invalid"); if (value.status !== undefined && value.status !== "ok") failResponse("Qdrant envelope status is invalid"); return value.result; }
function updateEnvelope(value: unknown): void { const result = envelope(value); if (result === true) return; if (!isRecord(result) || !["acknowledged", "completed", "ok"].includes(String(result.status))) failResponse("Qdrant update did not complete"); if (result.operation_id !== undefined && result.operation_id !== null && (!Number.isSafeInteger(result.operation_id) || Number(result.operation_id) < 0)) failResponse("Qdrant operation ID is invalid"); }
function validatePolicy(policy: QdrantReadPolicy, configuredOwner: HostId): void { if (!isRecord(policy) || policy.ownerHost !== configuredOwner || (policy.ownerHost !== "pi" && policy.ownerHost !== "prime") || policy.requireStatus !== "active" || policy.requireSecretScan !== "passed" || !Number.isFinite(policy.now) || !Number.isFinite(policy.maxClockSkewMs) || policy.maxClockSkewMs < 0 || !Array.isArray(policy.recordTypes) || policy.recordTypes.length === 0 || policy.recordTypes.some((type) => !["episode", "curated_memory", "curated_current", "conflict_manifest", "raptor_summary", "collection_control", "processing_policy", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone", "collection_metadata"].includes(type))) failInput("Read policy is invalid"); try { validatePurpose(policy.purpose, policy.recordTypes); } catch { failInput("Read policy purpose is invalid"); } if (policy.projectId !== undefined && (typeof policy.projectId !== "string" || policy.projectId.length === 0) || policy.processingPolicyId !== undefined && (typeof policy.processingPolicyId !== "string" || policy.processingPolicyId.length === 0) || policy.privacyEpoch !== undefined && (!Number.isSafeInteger(policy.privacyEpoch) || policy.privacyEpoch < 0)) failInput("Read policy scope is invalid"); }
/** Coordination points are control-state, not memory; their envelope expiry is lease/claim state. */
const COORDINATION_POINT_TYPES = new Set(["collection_control", "processing_policy", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone", "collection_metadata"]);
function validatePayloadForPolicy(payload: JsonRecord, policy: QdrantReadPolicy): void {
  if (payload.owner_host !== policy.ownerHost) failResponse("Qdrant point owner is missing or foreign");
  if (typeof payload.record_type !== "string" || !policy.recordTypes.includes(payload.record_type as PointRecordType)) failResponse("Qdrant point record type is outside the read policy");
  if (payload.status !== policy.requireStatus || payload.secret_scan !== policy.requireSecretScan) failResponse("Qdrant point status/secret policy is invalid");
  const expiry = payload.expires_at; if (policy.purpose !== "metadata" && !COORDINATION_POINT_TYPES.has(payload.record_type) && expiry !== null && (typeof expiry !== "string" || !Number.isFinite(Date.parse(expiry)) || Date.parse(expiry) <= policy.now + policy.maxClockSkewMs)) failResponse("Qdrant point is expired or has an invalid expiry");
  if (payload.record_type === "tombstone" && policy.purpose !== "internal" && policy.purpose !== "write_verification") failResponse("Qdrant tombstones are not readable memory points");
  if (policy.projectId !== undefined && payload.project_id !== policy.projectId) failResponse("Qdrant point project policy mismatch");
  if (policy.processingPolicyId !== undefined && payload.processing_policy_id !== policy.processingPolicyId) failResponse("Qdrant processing policy mismatch");
  if (policy.privacyEpoch !== undefined && payload.privacy_epoch !== policy.privacyEpoch) failResponse("Qdrant privacy epoch mismatch");
}
function point(value: unknown, policy: QdrantReadPolicy, includeVector: boolean): QdrantPoint { if (!isRecord(value) || !validId(value.id)) failResponse("Qdrant point ID is invalid"); const payload = validatePayload(value.payload, true); validatePayloadForPolicy(payload, policy); let vector: { semantic: number[] } | undefined; if (value.vector !== undefined) { if (!isRecord(value.vector)) failResponse("Qdrant named vector is invalid"); const keys = Object.keys(value.vector); if (keys.length === 0) return { id: value.id, payload }; if (keys.length !== 1 || keys[0] !== "semantic" || !isFiniteVector(value.vector.semantic)) failResponse("Qdrant named vector is invalid"); if (includeVector) vector = { semantic: value.vector.semantic.map(component => Math.fround(component)) }; } return vector === undefined ? { id: value.id, payload } : { id: value.id, payload, vector }; }
function responsePoints(value: unknown, policy: QdrantReadPolicy, includeVector: boolean): QdrantPoint[] { if (!Array.isArray(value)) failResponse("Qdrant points result is invalid"); return value.map((item) => point(item, policy, includeVector)); }
type WireKey = "owner_host" | "record_type" | "status" | "secret_scan" | "expires_at" | "project_id" | "processing_policy_id" | "version" | "fencing_token" | "coordination_policy_epoch" | "coordination_policy_hash" | "privacy_epoch" | "state" | "active_base_generation" | "job_id" | "owner_id" | "id" | "accepted_proposal_id" | "accepted_manifest_hash" | "created_at" | "content_hash" | "resolution" | "content_id" | "conflict_manifest_hash";
type WireCondition = { key: WireKey; match?: { value?: string | number | boolean; any?: string[] }; range?: { gt?: string; lte?: string } } | { is_null: { key: WireKey } };
type WireFilter = { must: WireCondition[]; must_not: WireCondition[]; should: WireCondition[] };
function serverFilter(policy: QdrantReadPolicy): WireFilter { validatePolicy(policy, policy.ownerHost); const must: WireCondition[] = [{ key: "owner_host", match: { value: policy.ownerHost } }, { key: "status", match: { value: "active" } }, { key: "secret_scan", match: { value: "passed" } }]; if (policy.recordTypes.length === 1) must.push({ key: "record_type", match: { value: policy.recordTypes[0]! } }); else must.push({ key: "record_type", match: { any: [...policy.recordTypes] } }); if (policy.projectId !== undefined) must.push({ key: "project_id", match: { value: policy.projectId } }); if (policy.processingPolicyId !== undefined) must.push({ key: "processing_policy_id", match: { value: policy.processingPolicyId } }); if (policy.privacyEpoch !== undefined) must.push({ key: "privacy_epoch", match: { value: policy.privacyEpoch } }); return { must, must_not: policy.purpose === "internal" || policy.purpose === "write_verification" ? [] : [{ key: "record_type", match: { value: "tombstone" } }], should: policy.purpose === "internal" && policy.recordTypes.every((type) => COORDINATION_POINT_TYPES.has(type)) ? [] : [{ is_null: { key: "expires_at" } }, { key: "expires_at", range: { gt: new Date(policy.now + policy.maxClockSkewMs).toISOString() } }] }; }
function responseCollection(value: unknown): QdrantCollectionInfo { const result = envelope(value); if (!isRecord(result) || !isRecord(result.config) || !isRecord(result.config.params) || !isRecord(result.config.params.vectors)) failResponse("Collection configuration is invalid"); const vectors = result.config.params.vectors; if (!isRecord(vectors) || Object.keys(vectors).length !== 1 || !isRecord(vectors.semantic) || vectors.semantic.size !== 1024 || vectors.semantic.distance !== "Dot") failResponse("Collection must have exactly semantic 1024/Dot vector"); let pointsCount: number | null = null; if (result.points_count !== undefined && result.points_count !== null) { if (!Number.isSafeInteger(result.points_count) || Number(result.points_count) < 0) failResponse("Collection point count is invalid"); pointsCount = result.points_count as number; } let payloadSchema: JsonRecord | undefined; if (result.payload_schema !== undefined) { if (!isRecord(result.payload_schema)) failResponse("Collection payload schema is invalid"); payloadSchema = {}; for (const [field, value] of Object.entries(result.payload_schema)) { if (!isRecord(value) || typeof value.data_type !== "string" || !["keyword", "integer", "datetime", "text"].includes(value.data_type)) failResponse("Collection payload schema entry is invalid"); payloadSchema[field] = value; } } const status = typeof result.status === "string" ? result.status : undefined; return { ...(status === undefined ? {} : { status }), dimension: 1024, distance: "Dot", vectors: { semantic: { size: 1024, distance: "Dot" } }, pointsCount, ...(payloadSchema === undefined ? {} : { payloadSchema }), raw: value }; }
/**
 * GLOBAL RULE: snapshot every untrusted QdrantClientOptions field EXACTLY ONCE
 * into a plain frozen object. An accessor-bearing caller object is read once;
 * every later validation/construction uses ONLY the snapshot. Never spread the
 * caller object and never re-read a getter.
 */
function snapshotQdrantOptions(input: QdrantClientOptions): QdrantClientOptions {
  const baseUrl = input.baseUrl;
  const collection = input.collection;
  const ownerHost = input.ownerHost;
  const apiKey = input.apiKey;
  const timeoutMs = input.timeoutMs;
  const fetchImpl = input.fetchImpl;
  const readConsistency = input.readConsistency;
  const maxClockSkewMs = input.maxClockSkewMs;
  const signal = input.signal;
  const replicationFactor = input.replicationFactor;
  const writeConsistencyFactor = input.writeConsistencyFactor;
  const snapshot: QdrantClientOptions = { baseUrl, collection, ownerHost, timeoutMs };
  if (apiKey !== undefined) snapshot.apiKey = apiKey;
  if (fetchImpl !== undefined) snapshot.fetchImpl = fetchImpl;
  if (readConsistency !== undefined) snapshot.readConsistency = readConsistency;
  if (maxClockSkewMs !== undefined) snapshot.maxClockSkewMs = maxClockSkewMs;
  if (signal !== undefined) snapshot.signal = signal;
  if (replicationFactor !== undefined) snapshot.replicationFactor = replicationFactor;
  if (writeConsistencyFactor !== undefined) snapshot.writeConsistencyFactor = writeConsistencyFactor;
  return Object.freeze(snapshot);
}
/** Snapshot an untrusted AuthorizedDestination EXACTLY ONCE into a plain frozen object. */
function snapshotAuthorizedDestination(input: AuthorizedDestination): AuthorizedDestination {
  const id = input.id;
  const residency = input.residency;
  const dataUse = input.dataUse;
  return Object.freeze({ id, residency, dataUse });
}
function freezeOptions(input: QdrantClientOptions): QdrantClientOptions {
  // The snapshot is the ONLY trusted source: validate and canonicalize it.
  const snapshot = snapshotQdrantOptions(input);
  const endpoint = baseUrl(snapshot.baseUrl);
  validateCollection(snapshot.collection);
  if (snapshot.ownerHost !== "pi" && snapshot.ownerHost !== "prime") failInput("Owner host is invalid");
  if (snapshot.collection !== expectedQdrantCollection(snapshot.ownerHost)) failInput("Qdrant collection does not match owner host");
  if (snapshot.apiKey !== undefined && (typeof snapshot.apiKey !== "string" || snapshot.apiKey.trim() === "")) failInput("Qdrant API key is invalid");
  validateTimeout(snapshot.timeoutMs);
  if (snapshot.maxClockSkewMs !== undefined && (!Number.isFinite(snapshot.maxClockSkewMs) || snapshot.maxClockSkewMs < 0)) failInput("Clock skew is invalid");
  return Object.freeze({ ...snapshot, baseUrl: endpoint, maxClockSkewMs: snapshot.maxClockSkewMs ?? 0 });
}

/**
 * Module-private per-instance state: frozen options, the transport function
 * CAPTURED at construction (never the dynamic global fetch), and the injected
 * flag. No `.options` property is ever emitted; WeakMaps keep apiKey/fetchImpl
 * undiscoverable via Object.keys/property names/symbols/inspection.
 */
interface RestClientState { options: QdrantClientOptions; fetchImpl: typeof fetch; injectedFetch: boolean; }
const REST_STATE = new WeakMap<object, RestClientState>();
/** Opaque per-instance frozen transport token: an empty identity object, never {options}. */
const REST_TOKEN = new WeakMap<object, object>();

function hasTransportToken(value: unknown): value is RestQdrantReadClient { return typeof value === "object" && value !== null && REST_STATE.has(value as object); }
function transportTokenOf(value: RestQdrantReadClient): object | undefined { return REST_TOKEN.get(value); }
function restState(value: RestQdrantReadClient): RestClientState { const state = REST_STATE.get(value); if (state === undefined) throw new TypeError("REST client state is missing"); return state; }

class RestQdrantReadClient implements QdrantReadClient {
  readonly endpoint: string; readonly ownerHost: HostId; readonly collection: HostScopedQdrantCollection; readonly maxClockSkewMs: number;
  constructor(options: QdrantClientOptions) {
    const frozen = freezeOptions(options);
    const capturedFetch = frozen.fetchImpl !== undefined ? frozen.fetchImpl : (typeof globalThis !== "undefined" ? globalThis.fetch : fetch).bind(globalThis);
    REST_STATE.set(this, { options: frozen, fetchImpl: capturedFetch, injectedFetch: frozen.fetchImpl !== undefined });
    REST_TOKEN.set(this, Object.freeze({}));
    this.endpoint = frozen.baseUrl; this.ownerHost = frozen.ownerHost; this.collection = expectedQdrantCollection(this.ownerHost); this.maxClockSkewMs = frozen.maxClockSkewMs ?? 0; Object.freeze(this);
  }

  async health(): Promise<unknown> { const response = await fetchOk(`${restState(this).options.baseUrl}/healthz`, { method: "GET", headers: headers(restState(this).options.apiKey) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const text = await response.text(); if (text.trim() === "healthz check passed") return text; let parsed: unknown; try { parsed = JSON.parse(text) as unknown; } catch { throw new MemoryClientError("invalid-json", "Health response was not valid JSON"); } if (!isRecord(parsed) || !("result" in parsed)) failResponse("Health response is invalid"); const result = envelope(parsed); if (!isRecord(result) || result.status !== "ok") failResponse("Health response is invalid"); return parsed; }
  async collectionInfo(): Promise<QdrantCollectionInfo> { return responseCollection(await fetchJson<unknown>(consistency(collectionPath(restState(this).options), restState(this).options.readConsistency), { method: "GET", headers: headers(restState(this).options.apiKey) }, requestOptions(restState(this).options, restState(this).fetchImpl))); }
  async retrieve(ids: readonly PointId[], policy: QdrantReadPolicy, options: ReadOptions = {}): Promise<QdrantPoint[]> { validatePolicy(policy, this.ownerHost); if (!Array.isArray(ids) || ids.length === 0 || ids.length > 1024 || ids.some((id) => !validId(id))) failInput("Retrieve IDs are invalid"); const init: RequestInit = { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ ids, with_payload: true, with_vector: options.includeVector === true }) }; const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points"), restState(this).options.readConsistency), init, requestOptions(restState(this).options, restState(this).fetchImpl)); return responsePoints(envelope(response), policy, options.includeVector === true); }
  async scroll(input: { policy: QdrantReadPolicy; offset?: PointId; limit?: number }): Promise<QdrantScrollResult> { validatePolicy(input.policy, this.ownerHost); if (input.offset !== undefined && !validId(input.offset)) failInput("Scroll offset is invalid"); const limit = input.limit ?? 256; if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1024) failInput("Scroll limit is invalid"); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/scroll"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ offset: input.offset ?? null, limit, with_payload: true, with_vector: false, filter: serverFilter(input.policy) }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (!isRecord(result) || !Array.isArray(result.points)) failResponse("Scroll response is invalid"); const next = result.next_page_offset; if (next !== undefined && next !== null) validateResponseId(next); return next === undefined || next === null ? { points: responsePoints(result.points, input.policy, false) } : { points: responsePoints(result.points, input.policy, false), nextOffset: next }; }
  async search(input: { vector: readonly number[]; limit: number; policy: QdrantReadPolicy }): Promise<QdrantSearchHit[]> { validatePolicy(input.policy, this.ownerHost); if (!isFiniteVector(input.vector)) failInput("Search vector must contain finite 1024-dimensional numbers"); if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 1024) failInput("Search limit is invalid"); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/search"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ vector: { name: "semantic", vector: [...input.vector] }, limit: input.limit, filter: serverFilter(input.policy), with_payload: true, with_vector: false }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (!Array.isArray(result)) failResponse("Search response is invalid"); return result.map((value) => { if (!isRecord(value) || !validId(value.id) || typeof value.score !== "number" || !Number.isFinite(value.score)) failResponse("Search hit is invalid"); const payload = validatePayload(value.payload, true); validatePayloadForPolicy(payload, input.policy); return { id: value.id, score: value.score, payload }; }); }
  async count(policy: QdrantReadPolicy): Promise<number> { validatePolicy(policy, this.ownerHost); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/count"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ exact: true, filter: serverFilter(policy) }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (!isRecord(result) || !Number.isSafeInteger(result.count) || Number(result.count) < 0) failResponse("Count response is invalid"); return result.count as number; }
}
/** Module-private unexported issuer: real REST writers are branded at construction. */
const REST_WRITER_ISSUER = Symbol("pi-qdrant-memory-v2.rest-writer-issuer");

class RestQdrantSessionWriter extends RestQdrantReadClient {
  readonly #issuer: symbol;
  constructor(options: QdrantClientOptions) {
    super(options);
    this.#issuer = REST_WRITER_ISSUER;
    Object.freeze(this);
  }
  /** Real-brand check: only genuine REST writers pass — exact prototype (subclasses fail), private issuer and transport token. */
  static isValid(value: unknown): value is RestQdrantSessionWriter {
    if (typeof value !== "object" || value === null || !(#issuer in value) || !hasTransportToken(value)) return false;
    return Object.getPrototypeOf(value) === RestQdrantSessionWriter.prototype && value instanceof RestQdrantSessionWriter && value.#issuer === REST_WRITER_ISSUER;
  }
  async upsertPoints(points: readonly PreparedPoint[], mode: "insert_only" | "update_only", precondition?: TypedUpdatePrecondition): Promise<void> { if (!Array.isArray(points) || points.length === 0 || points.length > 1024 || points.some((point) => !isRecord(point) || !isPhysicalPointId(point.id))) failInput("Prepared points are invalid"); if (mode !== "insert_only" && mode !== "update_only") failInput("Upsert mode is invalid"); if (mode === "update_only" && precondition === undefined) failInput("Update-only precondition is required"); if (precondition !== undefined) validatePrecondition(precondition, this.ownerHost); const normalized = points.map(point => normalizePoint(point, true)); for (const point of normalized) if (point.payload.owner_host !== this.ownerHost) failInput("Point owner does not match configured owner"); const body: JsonRecord = { points: normalized, update_mode: mode }; if (precondition !== undefined) body.update_filter = wirePrecondition(precondition); const response = await fetchJson<unknown>(pointWriteUrl(restState(this).options), { method: "PUT", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify(body) }, requestOptions(restState(this).options, restState(this).fetchImpl)); updateEnvelope(response); }
}
function validBoundedText(value: unknown, max = 512): value is string { return typeof value === "string" && value.length > 0 && value.length <= max; }
function validatePrecondition(value: TypedUpdatePrecondition, owner: HostId): void {
  if (!isRecord(value) || value.ownerHost !== owner) failInput("Closed update precondition is invalid");
  if (value.kind === "collection-control-cas") {
    if (value.recordType !== "collection_control" || !["active", "draining", "retired"].includes(value.expectedState) || !Number.isSafeInteger(value.expectedVersion) || value.expectedVersion < 0 || !Number.isSafeInteger(value.expectedEpoch) || value.expectedEpoch < 0 || !Number.isSafeInteger(value.expectedPrivacyEpoch) || value.expectedPrivacyEpoch < 0 || (value.expectedBaseGeneration !== undefined && value.expectedBaseGeneration !== null && !validBoundedText(value.expectedBaseGeneration))) failInput("Closed control precondition is invalid");
    return;
  }
  if (value.kind === "current-cas") {
    if (value.recordType !== "curated_current" || !validBoundedText(value.id) || !Number.isSafeInteger(value.expectedVersion) || value.expectedVersion < 0 || !Number.isSafeInteger(value.expectedEpoch) || value.expectedEpoch < 0 || !validBoundedText(value.expectedPolicyHash) || !validBoundedText(value.expectedProcessingPolicyId) || (value.expectedExpiresAt !== null && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value.expectedExpiresAt)) || !Number.isSafeInteger(value.expectedPrivacyEpoch) || value.expectedPrivacyEpoch < 0 || (value.expectedResolution !== "resolved" && value.expectedResolution !== "conflict") || (value.expectedContentId !== null && !validBoundedText(value.expectedContentId)) || (value.expectedConflictManifestHash !== null && !validBoundedText(value.expectedConflictManifestHash)) || (value.expectedResolution === "resolved") === (value.expectedContentId === null) || (value.expectedResolution === "conflict") !== (value.expectedConflictManifestHash !== null) || !/^[0-9a-f]{64}$/u.test(value.expectedContentHash)) failInput("Closed curated-current precondition is invalid");
    return;
  }
  if (value.kind === "lease-cas") {
    if (value.recordType !== "lease" || !validBoundedText(value.jobId) || !Number.isSafeInteger(value.expectedVersion) || value.expectedVersion < 0 || !Number.isSafeInteger(value.expectedFencingToken) || value.expectedFencingToken < 0 || !Number.isSafeInteger(value.expectedPolicyEpoch) || value.expectedPolicyEpoch < 0 || !validBoundedText(value.expectedPolicyHash) || !Number.isSafeInteger(value.expectedPrivacyEpoch) || value.expectedPrivacyEpoch < 0 || (value.expectedState !== "leased" && value.expectedState !== "accepted" && value.expectedState !== "released" && value.expectedState !== "completed") || !validBoundedText(value.expectedOwner) || (value.expectedAcceptedProposalId !== null && !validBoundedText(value.expectedAcceptedProposalId)) || (value.expectedAcceptedManifestHash !== null && !validBoundedText(value.expectedAcceptedManifestHash)) || (value.expectedAcceptedProposalId === null) !== (value.expectedAcceptedManifestHash === null) || !validBoundedText(value.expectedProcessingPolicyId) || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value.expectedCreatedAt) || !/^[0-9a-f]{64}$/u.test(value.expectedContentHash) || (value.expiresBefore !== undefined && (!Number.isSafeInteger(value.expiresBefore) || value.expiresBefore < 0)) || (value.expiresAfter !== undefined && (!Number.isSafeInteger(value.expiresAfter) || value.expiresAfter < 0))) failInput("Closed lease precondition is invalid");
    return;
  }
  failInput("Closed update precondition is invalid");
}
function wirePrecondition(value: TypedUpdatePrecondition): WireFilter {
  if (value.kind === "current-cas") {
    const must: WireCondition[] = [{ key: "owner_host", match: { value: value.ownerHost } }, { key: "record_type", match: { value: "curated_current" } }, { key: "version", match: { value: value.expectedVersion } }, { key: "coordination_policy_epoch", match: { value: value.expectedEpoch } }, { key: "coordination_policy_hash", match: { value: value.expectedPolicyHash } }, { key: "processing_policy_id", match: { value: value.expectedProcessingPolicyId } }, ...(value.expectedExpiresAt === null ? [{ is_null: { key: "expires_at" } } as WireCondition] : [{ key: "expires_at", match: { value: value.expectedExpiresAt } } as WireCondition]), { key: "privacy_epoch", match: { value: value.expectedPrivacyEpoch } }, { key: "resolution", match: { value: value.expectedResolution } }, { key: "content_hash", match: { value: value.expectedContentHash } } as WireCondition]; if (value.expectedResolution === "resolved") must.push({ key: "content_id", match: { value: value.expectedContentId as string } }); else must.push({ key: "conflict_manifest_hash", match: { value: value.expectedConflictManifestHash as string } }); return { must, must_not: [], should: [] };
  }
  if (value.kind === "collection-control-cas") {
    const must: WireCondition[] = [{ key: "owner_host", match: { value: value.ownerHost } }, { key: "record_type", match: { value: "collection_control" } }, { key: "version", match: { value: value.expectedVersion } }, { key: "privacy_epoch", match: { value: value.expectedPrivacyEpoch } }, { key: "coordination_policy_epoch", match: { value: value.expectedEpoch } }, { key: "state", match: { value: value.expectedState } } as WireCondition]; if (value.expectedBaseGeneration === null) must.push({ is_null: { key: "active_base_generation" } }); else if (value.expectedBaseGeneration !== undefined) must.push({ key: "active_base_generation", match: { value: value.expectedBaseGeneration } }); return { must, must_not: [], should: [] };
  }
  const must: WireCondition[] = [{ key: "owner_host", match: { value: value.ownerHost } }, { key: "record_type", match: { value: "lease" } }, { key: "job_id", match: { value: value.jobId } }, { key: "version", match: { value: value.expectedVersion } }, { key: "fencing_token", match: { value: value.expectedFencingToken } }, { key: "coordination_policy_epoch", match: { value: value.expectedPolicyEpoch } }, { key: "coordination_policy_hash", match: { value: value.expectedPolicyHash } }, { key: "privacy_epoch", match: { value: value.expectedPrivacyEpoch } }, { key: "state", match: { value: value.expectedState } }, { key: "owner_id", match: { value: value.expectedOwner } }, { key: "processing_policy_id", match: { value: value.expectedProcessingPolicyId } }, { key: "created_at", match: { value: value.expectedCreatedAt } }, { key: "content_hash", match: { value: value.expectedContentHash } }]; if (value.expectedAcceptedProposalId === null) { must.push({ is_null: { key: "accepted_proposal_id" } }, { is_null: { key: "accepted_manifest_hash" } }); } else { must.push({ key: "accepted_proposal_id", match: { value: value.expectedAcceptedProposalId } }, { key: "accepted_manifest_hash", match: { value: value.expectedAcceptedManifestHash as string } }); } if (value.expiresBefore !== undefined) must.push({ key: "expires_at", range: { lte: new Date(value.expiresBefore).toISOString() } }); if (value.expiresAfter !== undefined) must.push({ key: "expires_at", range: { gt: new Date(value.expiresAfter).toISOString() } }); return { must, must_not: [], should: [] };
}

Object.freeze(RestQdrantReadClient);
Object.freeze(RestQdrantReadClient.prototype);
Object.freeze(RestQdrantSessionWriter);
Object.freeze(RestQdrantSessionWriter.prototype);
/** Real-brand check for the REST writer class (avoids the value/interface name clash). */
function isRestQdrantSessionWriter(value: unknown): value is RestQdrantSessionWriter { return RestQdrantSessionWriter.isValid(value); }
/** Unpatchable per-instance transport accessor: returns the frozen token or undefined for non-genuine writers. */
function restTransportOf(value: unknown): object | undefined {
  return RestQdrantSessionWriter.isValid(value) ? transportTokenOf(value) : undefined;
}
/**
 * Production-bound writer: the exact real brand AND no injected transport.
 * The injected flag is private per-instance state, never inferred from exposed
 * options. Writers constructed with `fetchImpl` (test seams) are NOT
 * production-bound; production paths stub global fetch instead.
 */
function isProductionRestQdrantSessionWriter(value: unknown): value is RestQdrantSessionWriter {
  if (!RestQdrantSessionWriter.isValid(value)) return false;
  const state = REST_STATE.get(value as object);
  return state !== undefined && state.injectedFetch === false;
}
/** @internal */


/** Truthful minimum capability required by insert/readback verification (lexical). */
const CONTROL_PATCH_KEYS = new Set(["version", "processingPolicyId", "activeGeneration", "activeBaseGeneration", "privacyEpoch", "coordinationPolicyEpoch", "coordinationPolicyHash", "state", "scanCursor", "lastForgetBarrier", "revokedDestinationIds", "contentHash"]);
function fail(message: string): never { throw new TypeError(message); }

const FIELD_NAMES: Readonly<Record<string, string>> = { recordType: "record_type", ownerHost: "owner_host", schemaRevision: "schema_revision", createdAt: "created_at", privacyEpoch: "privacy_epoch", processingPolicyId: "processing_policy_id", expiresAt: "expires_at", contentHash: "content_hash", sourceEntryId: "source_entry_id", projectId: "project_id", projectIdentityKind: "project_identity_kind", sessionId: "session_id", turnId: "turn_id", agentRole: "agent_role", eventKind: "event_kind", eventAt: "event_at", modelId: "model_id", embeddingDimension: "embedding_dimension", originProvider: "origin_provider", destinationId: "destination_id", redactionStatus: "redaction_status", secretScan: "secret_scan", toolName: "tool_name", toolArgs: "tool_args", errorFingerprint: "error_fingerprint", producerId: "producer_id", nodeId: "node_id", sessionSequence: "session_sequence", coordinationPolicyHash: "coordination_policy_hash", coordinationPolicyEpoch: "coordination_policy_epoch", contentId: "content_id", observationId: "observation_id", effectiveAt: "effective_at", sourceEpisodeIds: "source_episode_ids", manifestHash: "manifest_hash", primaryEvidenceEpisodeId: "primary_evidence_episode_id", effectiveOrder: "effective_order", stateKey: "state_key", category: "category", scope: "scope", subject: "subject", predicate: "predicate", confidence: "confidence", generationId: "generation_id", clusterId: "cluster_id", membershipHash: "membership_hash", level: "level", memberIds: "member_ids", summary: "summary", promptRevision: "prompt_revision", algorithm: "algorithm", seed: "seed", jobId: "job_id", fencingToken: "fencing_token", temporalFrom: "temporal_from", temporalTo: "temporal_to", coveredProjects: "covered_projects", algorithmParameters: "algorithm_parameters", activeGeneration: "active_generation", activeBaseGeneration: "active_base_generation", state: "state", scanCursor: "scan_cursor", lastForgetBarrier: "last_forget_barrier", policy: "policy", canonicalHash: "canonical_hash", policyId: "policy_id", policyHash: "policy_hash", policyEpoch: "policy_epoch", membership: "membership", leaseExpiresAt: "lease_expires_at", leaseOwner: "lease_owner", acceptedProposalId: "accepted_proposal_id", acceptedManifestHash: "accepted_manifest_hash", terminalOperation: "terminal_operation", episodeId: "episode_id", extractorRevision: "extractor_revision", sourceId: "source_id", targetId: "target_id", provenanceId: "provenance_id", resolution: "resolution", conflictManifestHash: "conflict_manifest_hash", value: "value", revokedDestinationIds: "revoked_destination_ids", ownerId: "owner_id", proposalHash: "proposal_hash", content: "content" };
function mapKey(key: string): string { return FIELD_NAMES[key] ?? key; }
/** Serialize an already-owned parsed record. Never call parseMemoryRecord here:
 * callers that snapshot a getter-bearing record must not re-read it. */
function payloadFromParsed(parsed: MemoryRecord): Payload {
  const payload: Payload = {};
  for (const [key, value] of Object.entries(parsed as unknown as Payload)) {
    if (key === "vector") continue;
    const mapped = mapKey(key);
    if (mapped === key && /[A-Z]/u.test(key)) fail(`Unmapped record field: ${key}`);
    payload[mapped] = value;
  }
  payload.status = payload.status ?? "active";
  payload.secret_scan = payload.secret_scan ?? "passed";
  return payload;
}
export function recordPayload(record: MemoryRecord): Payload {
  // parseMemoryRecord owns/canonicalizes the input exactly once. The returned
  // plain snapshot, not the caller object, is the only value serialized.
  return payloadFromParsed(parseMemoryRecord(record));
}
function recordPointFromParsed(parsed: MemoryRecord): PreparedPoint {
  const payload = parsed.recordType === "collection_control" ? controlPayload(parsed) : payloadFromParsed(parsed);
  const point: PreparedPoint = { id: physicalPointId(parsed.recordType, parsed.id), payload };
  if ("vector" in parsed && parsed.vector !== undefined) point.vector = { semantic: [...parsed.vector] };
  return point;
}
function recordPoint(record: MemoryRecord): PreparedPoint {
  return recordPointFromParsed(parseMemoryRecord(record));
}
function policyFor(client: QdrantWriteVerificationClient, recordType: "episode" | "curated_memory" | "curated_current" | "conflict_manifest" | "raptor_summary" | "collection_control" | "processing_policy" | "job" | "lease" | "proposal" | "coverage" | "evidence_link" | "tombstone" | "collection_metadata", purpose: "write_verification" | "control" | "internal" = "write_verification") { return readPolicy({ ownerHost: client.ownerHost, purpose, recordTypes: [recordType], maxClockSkewMs: client.maxClockSkewMs }); }
function contentHash(payload: Payload): unknown { return payload.content_hash; }
function collision(expected: string, actual: unknown): never { throw new Error(`content hash collision for ${expected}: ${String(actual)}`); }
function checkHash(payload: Payload, expected: string): void { if (contentHash(payload) !== expected) collision(expected, contentHash(payload)); }
/**
 * EXACT authoritative read/cas reread primitive: retrieve zero points ->
 * undefined; exactly ONE point whose OUTER id equals the requested id ->
 * returned. ANY extra, duplicate, or unrequested/alias point THROWS (fail
 * closed) — an ambiguous response never yields inserted/existing, a true CAS
 * or any authority. This covers insertOnly preflight/postflight, updateOnlyCas
 * and publishControlCas current/final rereads and the typed lease CAS final
 * reread; callers must never map ambiguity to success.
 */
async function retrieveOne(client: QdrantWriteVerificationClient, id: string, policy: ReturnType<typeof policyFor>, includeVector = false): Promise<QdrantPoint | undefined> {
  const points = await client.retrieve([id], policy, { includeVector });
  const matches = points.filter((point) => point.id === id);
  if (points.length !== matches.length || matches.length > 1) throw new Error("Qdrant readback is ambiguous");
  return matches[0];
}

/**
 * @internal
 * The private write engine: the ONLY production consumer of the raw
 * insert/cas primitives (insertOnly/updateOnlyCas/publishControlCas/casPoint)
 * inside this module. The raw primitives are NOT module exports; control.ts
 * receives them as an opaque engine object bound to the exact branded session
 * it constructed. There is no exported raw function, writer constructor or
 * registrar anywhere on the package-visible surface.
 */
interface PrivateWriteEngine {
  insertOnly<T extends MemoryRecord>(record: T): Promise<"inserted" | "existing">;
  updateOnlyCas(input: { id: string; expectedVersion: number; expectedEpoch: number; patch: Record<string, unknown> }): Promise<boolean>;
  publishControlCas(input: { expectedVersion: number; expectedBaseGeneration: string | null; next: ControlRecord }): Promise<boolean>;
  casPoint(input: { recordType: "lease"; id: string; precondition: LeaseCasPrecondition; next: LeaseRecord }): Promise<boolean>;
  casCuratedCurrent(input: { id: string; precondition: CuratedCurrentCasPrecondition; next: CuratedCurrentRecord }): Promise<boolean>;
}
/** @internal */
function privateWriteEngine(client: QdrantWriteVerificationClient): PrivateWriteEngine {
  return Object.freeze({
    insertOnly: <T extends MemoryRecord>(record: T) => insertOnly(client, record),
    updateOnlyCas: (input: { id: string; expectedVersion: number; expectedEpoch: number; patch: Record<string, unknown> }) => updateOnlyCas(client as QdrantSessionWriter, input),
    publishControlCas: (input: { expectedVersion: number; expectedBaseGeneration: string | null; next: ControlRecord }) => publishControlCas(client as QdrantSessionWriter, input),
    casPoint: (input: { recordType: "lease"; id: string; precondition: LeaseCasPrecondition; next: LeaseRecord }) => casPoint(client as QdrantSessionWriter, input),
    casCuratedCurrent: (input: { id: string; precondition: CuratedCurrentCasPrecondition; next: CuratedCurrentRecord }) => casCuratedCurrent(client as QdrantSessionWriter, input),
  });
}

/** @internal */
/** Insert-only is at-least-once: preflight and postflight reads classify observed state; a concurrent race is inherently ambiguous. */
async function insertOnly<T extends MemoryRecord>(client: QdrantWriteVerificationClient, record: T): Promise<"inserted" | "existing"> {
  // Own/parse ONCE before touching any record getter or network. Every later
  // decision (owner/type/version/hash/point/policy) uses this immutable dense
  // snapshot, so getter swaps cannot split validation from persistence.
  const parsed = parseMemoryRecord(record);
  // Owner-domain write safety BEFORE any retrieve/upsert: a writer can never
  // physically insert a canonical record owned by a different host (a pi
  // writer cannot pollute the collection with a prime job/coverage/proposal/
  // tombstone that only its pi-filtered reread would reject).
  if (parsed.ownerHost !== client.ownerHost) fail("Memory record owner does not match the client owner");
  if (parsed.recordType === "collection_control" && parsed.version === 0) assertBootstrapControl(parsed, client.ownerHost);
  else if (parsed.contentHash !== canonicalRecordHash(parsed)) fail("Memory record canonical hash mismatch");
  const point = recordPointFromParsed(parsed); const policy = policyFor(client, parsed.recordType === "collection_control" ? "collection_control" : parsed.recordType, parsed.recordType === "collection_control" ? "control" : "write_verification");
  const includeVector = point.vector !== undefined;
  const exactPoint = (candidate: QdrantPoint): boolean => {
    try {
      let payloadMatches = canonicalStringify(candidate.payload) === canonicalStringify(point.payload);
      // Evidence-link identity intentionally excludes jobId so overlapping
      // accepted jobs converge on the first immutable link. The higher-level
      // curated kernel validates the preserved job against its exact job.
      if (!payloadMatches && parsed.recordType === "evidence_link") {
        const normalized = { ...candidate.payload, job_id: point.payload.job_id };
        payloadMatches = canonicalStringify(normalized) === canonicalStringify(point.payload);
      }
      if (!payloadMatches) return false;
      if (point.vector === undefined) return candidate.vector === undefined;
      return candidate.vector !== undefined && canonicalStringify(candidate.vector.semantic) === canonicalStringify(point.vector.semantic);
    } catch { return false; }
  };
  const before = await retrieveOne(client, point.id, policy, includeVector); let existing = false;
  if (before !== undefined) { checkHash(before.payload, parsed.contentHash); if (!exactPoint(before)) throw new Error(`insert-only existing point did not match exactly ${point.id}`); existing = true; }
  await client.upsertPoints([point], "insert_only");
  const after = await retrieveOne(client, point.id, policy, includeVector); if (after === undefined) throw new Error(`insert-only write did not read back point ${point.id}`); checkHash(after.payload, parsed.contentHash); if (!exactPoint(after)) throw new Error(`insert-only write did not read back exactly ${point.id}`);
  return existing ? "existing" : "inserted";
}
function patchPayload(patch: Record<string, unknown>): Payload { const result: Payload = {}; for (const key of Object.keys(patch)) { if (!CONTROL_PATCH_KEYS.has(key)) fail(`CAS patch key is not mutable: ${key}`); const value = patch[key]; if (key === "version" || key === "privacyEpoch" || key === "coordinationPolicyEpoch") { if (!Number.isSafeInteger(value) || (value as number) < 0) fail(`CAS patch field is invalid: ${key}`); } else if (key === "state") { if (!["active", "draining", "retired"].includes(String(value))) fail("CAS patch state is invalid"); } else if (["activeGeneration", "activeBaseGeneration", "scanCursor", "lastForgetBarrier"].includes(key)) { if (value !== null && (typeof value !== "string" || value.length === 0)) fail(`CAS patch field is invalid: ${key}`); } else if (key === "processingPolicyId") { if (typeof value !== "string" || value.length === 0 || value.length > 512) fail("CAS patch processing policy is invalid"); } else if (key === "coordinationPolicyHash") { if (typeof value !== "string" || value.length === 0 || value.length > 512) fail("CAS patch policy hash is invalid"); } else if (key === "revokedDestinationIds") { if (!Array.isArray(value) || value.length > 1024 || value.some((id) => typeof id !== "string" || id.length === 0 || id.length > 256 || !/^[A-Za-z0-9._:/-]+$/u.test(id)) || new Set(value).size !== value.length) fail("CAS patch revocations are invalid"); } else if (key === "contentHash") { if (typeof value !== "string" || !/^[a-f0-9]{64}$/u.test(value)) fail("CAS patch content hash is invalid"); } result[mapKey(key)] = value; } return result; }
function ownerPrecondition(client: QdrantSessionWriter, expectedVersion: number, expectedPrivacyEpoch: number, expectedEpoch: number, expectedState: "active" | "draining" | "retired", expectedBaseGeneration?: string | null): ControlCasPrecondition { return { kind: "collection-control-cas", ownerHost: client.ownerHost, recordType: "collection_control", expectedVersion, expectedPrivacyEpoch, expectedEpoch, expectedState, ...(expectedBaseGeneration === undefined ? {} : { expectedBaseGeneration }) }; }
function mergePayload(existing: Payload, patch: Payload): Payload { return { ...existing, ...patch }; }
function deepEqual(left: unknown, right: unknown): boolean { try { return canonicalStringify(left) === canonicalStringify(right); } catch { return false; } }

/** @internal */
async function updateOnlyCas(client: QdrantSessionWriter, input: { id: string; expectedVersion: number; expectedEpoch: number; patch: Record<string, unknown> }): Promise<boolean> {
  if (input.id !== COLLECTION_CONTROL_ID) fail("CAS control ID is invalid"); if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0 || !Number.isSafeInteger(input.expectedEpoch) || input.expectedEpoch < 0) fail("CAS version/epoch is invalid"); if (!isRecord(input.patch) || input.patch.version !== input.expectedVersion + 1) fail("CAS patch must advance version exactly once");
  const patch = patchPayload(input.patch); const policy = policyFor(client, "collection_control", "control"); const current = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true); if (current === undefined) return false;
  const currentRecord = controlRecordFromPayload(current.payload, client.ownerHost); if (currentRecord.state === "retired" || currentRecord.version !== input.expectedVersion || currentRecord.coordinationPolicyEpoch !== input.expectedEpoch) return false;
  const suppliedHash = input.patch.contentHash; const candidate = { ...currentRecord, ...input.patch, contentHash: "pending" } as ControlRecord; parseMemoryRecord(candidate);
  const privacyDelta = candidate.privacyEpoch - currentRecord.privacyEpoch; const coordinationDelta = candidate.coordinationPolicyEpoch - currentRecord.coordinationPolicyEpoch;
  if (privacyDelta < 0 || privacyDelta > 1 || coordinationDelta < 0 || coordinationDelta > 1) return false;
  if (coordinationDelta === 0 && (candidate.coordinationPolicyHash !== currentRecord.coordinationPolicyHash || candidate.processingPolicyId !== currentRecord.processingPolicyId)) return false;
  if (candidate.revokedDestinationIds.length < currentRecord.revokedDestinationIds.length || !currentRecord.revokedDestinationIds.every((id) => candidate.revokedDestinationIds.includes(id))) return false;
  const computedHash = canonicalRecordHash(candidate); if (suppliedHash !== undefined && suppliedHash !== computedHash) fail("CAS patch content hash is inconsistent"); candidate.contentHash = computedHash;
  const point = recordPoint(candidate); if (current.vector !== undefined) point.vector = current.vector;
  await client.upsertPoints([point], "update_only", ownerPrecondition(client, input.expectedVersion, currentRecord.privacyEpoch, input.expectedEpoch, currentRecord.state));
  const reread = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true); if (reread === undefined) return false; const rereadRecord = controlRecordFromPayload(reread.payload, client.ownerHost); return deepEqual(controlPayload(rereadRecord), controlPayload(candidate));
}
/** @internal */
async function publishControlCas(client: QdrantSessionWriter, input: { expectedVersion: number; expectedBaseGeneration: string | null; next: ControlRecord }): Promise<boolean> {
  if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0 || input.next.id !== COLLECTION_CONTROL_ID || input.next.version !== input.expectedVersion + 1 || input.next.ownerHost !== client.ownerHost) fail("Control CAS input is invalid"); parseMemoryRecord(input.next); if (input.next.contentHash !== canonicalRecordHash(input.next)) fail("Next control canonical hash mismatch");
  const policy = policyFor(client, "collection_control", "control"); const current = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true); if (current === undefined) return false; const currentRecord = controlRecordFromPayload(current.payload, client.ownerHost);
  if (currentRecord.state !== "active" || input.next.state !== "active") return false;
  if (input.next.privacyEpoch !== currentRecord.privacyEpoch || input.next.coordinationPolicyEpoch !== currentRecord.coordinationPolicyEpoch || input.next.coordinationPolicyHash !== currentRecord.coordinationPolicyHash || input.next.processingPolicyId !== currentRecord.processingPolicyId) return false;
  const expectedPayload = controlPayload(input.next); if (currentRecord.version === input.next.version && deepEqual(controlPayload(currentRecord), expectedPayload)) return true; if (currentRecord.version !== input.expectedVersion || currentRecord.activeBaseGeneration !== input.expectedBaseGeneration) return false;
  const point = recordPoint(input.next); if (current.vector !== undefined) point.vector = current.vector;
  await client.upsertPoints([point], "update_only", ownerPrecondition(client, input.expectedVersion, currentRecord.privacyEpoch, currentRecord.coordinationPolicyEpoch, currentRecord.state, input.expectedBaseGeneration));
  const reread = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true); if (reread === undefined) return false; const rereadRecord = controlRecordFromPayload(reread.payload, client.ownerHost); return deepEqual(controlPayload(rereadRecord), expectedPayload);
}

/** Coordination point types whose wire payloads round-trip through recordPayload. */
const COORDINATION_RECORD_TYPES = new Set(["lease", "job", "proposal", "coverage", "tombstone"] as const);
const SNAKE_TO_CAMEL: ReadonlyMap<string, string> = new Map(Object.entries(FIELD_NAMES).map(([camel, snake]) => [snake, camel]));
/** Strict inverse of recordPayload; status/secret_scan are wire-only defaults except for episode. */
export function recordFromPayload(value: unknown, ownerHost: "pi" | "prime", semanticVector?: readonly number[]): MemoryRecord {
  let snapshot: unknown;
  try { snapshot = ownedCanonicalJsonSnapshot(value, "Coordination point payload"); }
  catch { throw new TypeError("Coordination point payload is invalid"); }
  if (!isRecord(snapshot) || typeof snapshot.record_type !== "string") throw new TypeError("Coordination point payload is invalid");
  const owned = snapshot as Payload;
  // Named vectors are the only legal vector transport. Snapshot the semantic
  // vector once; payload-level vectors and unsupported record pairings fail.
  if (Object.prototype.hasOwnProperty.call(owned, "vector")) throw new TypeError("Coordination point vector must be a named vector");
  const record: Payload = {};
  const recordType = owned.record_type;
  for (const [wire, raw] of Object.entries(owned)) {
    if ((wire === "status" || wire === "secret_scan") && recordType !== "episode") continue;
    record[SNAKE_TO_CAMEL.get(wire) ?? wire] = raw;
  }
  const supportsNamedVector = recordType === "episode" || recordType === "curated_memory" || recordType === "curated_current" && owned.resolution === "resolved" || recordType === "raptor_summary";
  let vectorSnapshot: number[] | undefined;
  if (semanticVector !== undefined) {
    let ownedVector: unknown;
    try { ownedVector = ownedCanonicalJsonSnapshot(semanticVector, "Coordination semantic vector"); }
    catch { throw new TypeError("Coordination semantic vector is invalid"); }
    if (!Array.isArray(ownedVector) || ownedVector.length !== 1024 || !ownedVector.every((component) => typeof component === "number" && Number.isFinite(component))) throw new TypeError("Coordination semantic vector is invalid");
    vectorSnapshot = ownedVector as number[];
    if (!supportsNamedVector) throw new TypeError("Record type does not permit a named semantic vector");
  }
  let parsed: MemoryRecord;
  try { parsed = vectorSnapshot === undefined ? parseMemoryRecord(record, { ownerHost }) : parseMemoryRecord({ ...record, vector: vectorSnapshot }, { ownerHost }); }
  catch { throw new TypeError("Coordination point record is invalid"); }
  if (parsed.contentHash !== canonicalRecordHash(parsed)) throw new TypeError("Coordination point canonical hash mismatch");
  try { if (canonicalStringify(owned) !== canonicalStringify(payloadFromParsed(parsed))) throw new TypeError("Coordination point payload mismatch"); } catch { throw new TypeError("Coordination point payload mismatch"); }
  return parsed;
}
/** Strict inverse of recordPayload for coordination points; status/secret_scan are wire-only defaults. */
export function coordinationRecordFromPayload(value: unknown, ownerHost: "pi" | "prime"): JobRecord | LeaseRecord | ProposalRecord | CoverageRecord | TombstoneRecord {
  // recordFromPayload owns the raw wire object exactly once. Do not pre-read
  // record_type from a caller accessor before that canonical snapshot.
  const parsed = recordFromPayload(value, ownerHost);
  if (!COORDINATION_RECORD_TYPES.has(parsed.recordType as never)) throw new TypeError("Coordination point payload is invalid");
  return parsed as JobRecord | LeaseRecord | ProposalRecord | CoverageRecord | TombstoneRecord;
}
/** Strict episode readback parser for internal coordination reads (tombstone target verification). */
export function episodeRecordFromPayload(value: unknown, ownerHost: "pi" | "prime"): EpisodeRecord {
  const parsed = recordFromPayload(value, ownerHost);
  if (parsed.recordType !== "episode") throw new TypeError("Episode payload is invalid");
  return parsed as EpisodeRecord;
}

/**
 * Typed single-point CAS for lease/job points: update_only + typed
 * update_filter, strong ordering/wait, then reread and exact payload compare.
 * A successful HTTP response is never treated as a transaction.
 */
/**
 * Typed single-point CAS for the lease/claim point. The precondition pins
 * owner/version/fencing/policy/privacy/state/acceptance and expiry bounds; the
 * next transition is validated (version exactly +1, fence preserve-or-increment,
 * acceptance moves only as one unit), then update_only + update_filter with
 * strong ordering/wait is applied and the point is reread and compared EXACTLY
 * to `next`. A Qdrant-acknowledged zero-match or delayed concurrent write
 * therefore returns false; callers return only the exact reread.
 */
/** @internal */
async function casPoint(client: QdrantSessionWriter, input: { recordType: "lease"; id: string; precondition: LeaseCasPrecondition; next: LeaseRecord }): Promise<boolean> {
  if (input.precondition.kind !== "lease-cas" || input.precondition.recordType !== "lease" || input.precondition.jobId !== input.next.jobId || input.id !== input.next.id) fail("Typed CAS precondition does not match the point");
  const next = input.next;
  if (next.recordType !== "lease" || next.ownerHost !== client.ownerHost || next.contentHash !== canonicalRecordHash(next)) fail("Typed CAS record is invalid");
  const p = input.precondition;
  // Explicit transition table, validated before any network call.
  if (next.version !== p.expectedVersion + 1) fail("Typed CAS must advance version exactly once");
  if (next.coordinationPolicyEpoch !== p.expectedPolicyEpoch || next.coordinationPolicyHash !== p.expectedPolicyHash || next.privacyEpoch !== p.expectedPrivacyEpoch || next.jobId !== p.jobId) fail("Typed CAS must preserve the pinned policy/privacy identity");
  // Immutable-current binding: createdAt, job and processing-policy intersection cannot mutate.
  if (next.createdAt !== p.expectedCreatedAt) fail("Typed CAS cannot mutate the claim createdAt");
  if (next.processingPolicyId !== p.expectedProcessingPolicyId) fail("Typed CAS cannot mutate the claim processing-policy intersection");
  if (next.acceptedProposalId === null && next.acceptedManifestHash !== null) fail("Typed CAS acceptance fields must move together");
  // Acceptance pair rules:
  // - only leased + accepted=null may introduce acceptance (owner/fence unchanged, live-expiry floor);
  // - an existing acceptance pair may be preserved unchanged through renew/steal/release/reacquire;
  // - it may never be introduced otherwise or altered.
  if (p.expectedAcceptedProposalId === null && next.acceptedProposalId !== null) {
    if (p.expectedState !== "leased" || next.ownerId !== p.expectedOwner || next.fencingToken !== p.expectedFencingToken || next.state !== "accepted" || p.expiresAfter === undefined) fail("Typed CAS acceptance transition is invalid");
  } else if (p.expectedAcceptedProposalId !== null) {
    if (next.acceptedProposalId !== p.expectedAcceptedProposalId || next.acceptedManifestHash !== p.expectedAcceptedManifestHash) fail("Typed CAS cannot alter an existing acceptance");
  }
  // State/owner/fence transitions. Curation completion is the accepted->completed
  // transition. RAPTOR has no proposal: its named completion operation may
  // perform one leased->completed transition carrying the terminal marker.
  const raptorTerminal = next.terminalOperation === "raptor";
  if (raptorTerminal && (next.state !== "completed" || p.expectedState !== "leased" || p.expectedAcceptedProposalId !== null || p.expectedAcceptedManifestHash !== null || next.ownerId !== p.expectedOwner || next.fencingToken !== p.expectedFencingToken || next.acceptedProposalId !== null || next.acceptedManifestHash !== null)) fail("Typed CAS RAPTOR completion transition is invalid");
  if (!raptorTerminal && next.state === "completed" && (p.expectedState !== "accepted" || p.expectedAcceptedProposalId === null || p.expectedAcceptedManifestHash === null || next.ownerId !== p.expectedOwner || next.fencingToken !== p.expectedFencingToken || next.acceptedProposalId !== p.expectedAcceptedProposalId || next.acceptedManifestHash !== p.expectedAcceptedManifestHash)) fail("Typed CAS completion transition is invalid");
  if (p.expectedState === "completed") fail("Typed CAS cannot transition a completed lease");
  const ownerChanged = next.ownerId !== p.expectedOwner;
  const fenceChanged = next.fencingToken !== p.expectedFencingToken;
  if (next.state === "released") {
    if (ownerChanged || fenceChanged) fail("Typed CAS release must preserve owner and fencing token");
    // A release consumes a genuinely LIVE owner authority: the CAS must pin an
    // exact-owner expiry floor (expiresAt > freshNow), so locally expired or
    // skew-grace claims can never be released.
    if (p.expiresAfter === undefined) fail("Typed CAS release requires the exact-owner live expiry floor at CAS time");
  } else if (fenceChanged) {
    if (next.fencingToken !== p.expectedFencingToken + 1) fail("Typed CAS fencing token must be preserved or increment exactly once");
    if (p.expectedState === "released") {
      // released reacquire always increments the fence; no expiry cut needed
    } else if (ownerChanged) {
      // transfer/steal of a leased or accepted claim requires the conservative expiry cut
      if (p.expiresBefore === undefined) fail("Typed CAS owner change requires the conservative expiry cutoff");
    } else {
      // same-owner fence invalidation (crashed same-ID worker) requires the expiry cut
      if (p.expiresBefore === undefined) fail("Typed CAS same-owner fence increment requires the conservative expiry cutoff");
    }
  } else {
    if (ownerChanged) fail("Typed CAS cannot change owner while preserving the fencing token");
    if (p.expectedState === "released") fail("Typed CAS released reacquire must increment the fencing token");
    // preserve-fence leased->leased / accepted->accepted (renew) requires proof the
    // current claim is live for its exact owner at the CAS instant (the
    // expiresAfter floor is the fresh owner now, never now+skew); an expired claim must go through the
    // fenced reacquire (fence+1 + expiresBefore).
    if (p.expiresAfter === undefined) fail("Typed CAS preserve-fence renewal requires the exact-owner live expiry floor at CAS time");
  }
  const point = recordPoint(next);
  await client.upsertPoints([point], "update_only", p);
  const policy = policyFor(client, "lease", "internal");
  const reread = await retrieveOne(client, point.id, policy);
  if (reread === undefined) return false;
  // Exact wire equality to the intended next payload; never compare to itself.
  return canonicalStringify(reread.payload) === canonicalStringify(payloadFromParsed(next));
}




export interface QdrantDestinationFactoryInput {
  options: QdrantClientOptions;
  destination: AuthorizedDestination;
  egressMode: RuntimeConfig["privacy"]["egressMode"];
  nodeId?: string;
  coordinationPolicyHash: string;
  coordinationPolicyEpoch: number;
}
export interface QdrantDestinationFactory { bind(destination: AuthorizedDestination): BoundQdrantDestination; }
function validCoordinationBinding(hash: unknown, epoch: unknown): asserts hash is string {
  if (typeof hash !== "string" || hash.length === 0 || hash.length > 512 || !/^[A-Za-z0-9._:-]+$/u.test(hash) || !Number.isSafeInteger(epoch) || (epoch as number) < 0) throw new TypeError("Qdrant coordination binding is invalid");
}
function payloadValue(payload: Payload, camel: string, snake: string): unknown { return Object.prototype.hasOwnProperty.call(payload, snake) ? payload[snake] : payload[camel]; }
function optionalPayload(payload: Payload, camel: string, snake: string): unknown { return Object.prototype.hasOwnProperty.call(payload, snake) ? payload[snake] : payload[camel]; }
function sameCanonicalWirePayload(point: QdrantPoint, parsed: ProcessingPolicyRecord | EpisodeRecord): boolean {
  try { return canonicalStringify(point.payload) === canonicalStringify(payloadFromParsed(parsed)); } catch { return false; }
}
function sameCanonicalWireVector(point: QdrantPoint, parsed: ProcessingPolicyRecord | EpisodeRecord): boolean {
  const expected = parsed.recordType === "episode" && parsed.vector !== undefined ? { semantic: parsed.vector } : null;
  try { return canonicalStringify(point.vector ?? null) === canonicalStringify(expected); } catch { return false; }
}
/**
 * EXACT full EpisodeRecord readback equality: canonicalStringify of the whole
 * record (every field, including createdAt/producerId/nodeId which the
 * canonical hash intentionally excludes) plus the exact vector. A backend that
 * substitutes excluded fields while retaining hash+vector is never accepted.
 */
function sameEpisodeRecordExact(readback: ProcessingPolicyRecord | EpisodeRecord, record: ProcessingPolicyRecord | EpisodeRecord): boolean {
  if (readback.recordType !== "episode" || record.recordType !== "episode") return false;
  try { return canonicalStringify(readback) === canonicalStringify(record); } catch { return false; }
}
/**
 * Shared exact vector-aware Episode point reconstruction (payload + named
 * semantic vector), WITHOUT hash verification: used by the verified parser and
 * by the narrow legacy-hash classifier below.
 */
function reconstructEpisodePoint(point: QdrantPoint): unknown {
  // `point` is already an owned plain snapshot when this helper is called.
  const payload = point.payload;
  const id = typeof payload.id === "string" ? payload.id : "";
  const common = {
    recordType: "episode", id, ownerHost: payloadValue(payload, "ownerHost", "owner_host"), schemaRevision: payloadValue(payload, "schemaRevision", "schema_revision"),
    createdAt: payloadValue(payload, "createdAt", "created_at"), privacyEpoch: payloadValue(payload, "privacyEpoch", "privacy_epoch"),
    processingPolicyId: payloadValue(payload, "processingPolicyId", "processing_policy_id"), expiresAt: payloadValue(payload, "expiresAt", "expires_at"), contentHash: payloadValue(payload, "contentHash", "content_hash"),
  };
  return {
    ...common, sourceEntryId: payloadValue(payload, "sourceEntryId", "source_entry_id"), host: payload.host,
    projectId: payloadValue(payload, "projectId", "project_id"), projectIdentityKind: payloadValue(payload, "projectIdentityKind", "project_identity_kind"),
    sessionId: payloadValue(payload, "sessionId", "session_id"), turnId: payloadValue(payload, "turnId", "turn_id"), agentRole: payloadValue(payload, "agentRole", "agent_role"),
    depth: payload.depth, eventKind: payloadValue(payload, "eventKind", "event_kind"), eventAt: payloadValue(payload, "eventAt", "event_at"),
    modelId: payloadValue(payload, "modelId", "model_id"), embeddingDimension: payloadValue(payload, "embeddingDimension", "embedding_dimension"),
    originProvider: payloadValue(payload, "originProvider", "origin_provider"), destinationId: payloadValue(payload, "destinationId", "destination_id"),
    status: payload.status, redactionStatus: payloadValue(payload, "redactionStatus", "redaction_status"), secretScan: payloadValue(payload, "secretScan", "secret_scan"),
    ...(optionalPayload(payload, "text", "text") === undefined ? {} : { text: optionalPayload(payload, "text", "text") }),
    ...(optionalPayload(payload, "toolName", "tool_name") === undefined ? {} : { toolName: optionalPayload(payload, "toolName", "tool_name") }),
    ...(optionalPayload(payload, "toolArgs", "tool_args") === undefined ? {} : { toolArgs: optionalPayload(payload, "toolArgs", "tool_args") }),
    ...(optionalPayload(payload, "errorFingerprint", "error_fingerprint") === undefined ? {} : { errorFingerprint: optionalPayload(payload, "errorFingerprint", "error_fingerprint") }),
    ...(optionalPayload(payload, "producerId", "producer_id") === undefined ? {} : { producerId: optionalPayload(payload, "producerId", "producer_id") }),
    ...(optionalPayload(payload, "nodeId", "node_id") === undefined ? {} : { nodeId: optionalPayload(payload, "nodeId", "node_id") }),
    ...(optionalPayload(payload, "sessionSequence", "session_sequence") === undefined ? {} : { sessionSequence: optionalPayload(payload, "sessionSequence", "session_sequence") }),
    ...(point.vector?.semantic === undefined ? {} : { vector: point.vector.semantic }),
  };
}
/**
 * ONE shared exact vector-aware Episode point parser (payload + named semantic
 * vector): exact physical/logical ID, owner, record shape, exact canonical
 * wire payload, exact named vector and vector-bound contentHash must ALL
 * verify. Missing vector, malformed payload, hash mismatch or any structural
 * deviation returns null (fail closed). Reused by BoundQdrantDestination and
 * by ProductionCoordinationStore so episode reads never diverge.
 */
export function parseBoundEpisodePoint(point: QdrantPoint, ownerHost: "pi" | "prime"): EpisodeRecord | null {
  let owned: QdrantPoint;
  try { owned = ownedPointSnapshot(point); } catch { return null; }
  const payload = owned.payload;
  if (payload.record_type !== "episode") return null;
  const id = payload.id;
  if (typeof id !== "string" || id.length === 0 || physicalPointId("episode", id) !== owned.id) return null;
  try {
    const semantic = owned.vector?.semantic;
    const parsed = recordFromPayload(payload, ownerHost, semantic);
    if (parsed.recordType !== "episode" || parsed.id !== id || parsed.ownerHost !== ownerHost || parsed.vector === undefined) return null;
    if (parsed.contentHash !== canonicalRecordHash(parsed) || !sameCanonicalWirePayload(owned, parsed) || !sameCanonicalWireVector(owned, parsed)) return null;
    return parsed;
  } catch { return null; }
}

/**
 * Narrow internal legacy-hash classifier: a present exact-ID Episode point
 * whose stored contentHash is valid ONLY under the former vector-excluding
 * formula (vector present, hash computed without it). Distinct from both a
 * verified current point and an arbitrary malformed hash.
 */
export function isLegacyEpisodePoint(point: QdrantPoint, ownerHost: "pi" | "prime"): boolean {
  let owned: QdrantPoint;
  try { owned = ownedPointSnapshot(point); } catch { return false; }
  const payload = owned.payload;
  if (payload.record_type !== "episode" || owned.vector?.semantic === undefined || typeof payload.content_hash !== "string") return false;
  const id = payload.id;
  if (typeof id !== "string" || id.length === 0 || physicalPointId("episode", id) !== owned.id) return false;
  try {
    const value = reconstructEpisodePoint(owned);
    const parsed = parseMemoryRecord(value);
    if (parsed.recordType !== "episode" || parsed.id !== id || parsed.ownerHost !== ownerHost || parsed.vector === undefined) return false;
    const stored = payload.content_hash;
    if (canonicalRecordHash(parsed) === stored) return false;
    if (!sameCanonicalWirePayload(owned, parsed) || !sameCanonicalWireVector(owned, parsed)) return false;
    const { vector: _vector, ...noVector } = parsed as EpisodeRecord;
    return canonicalRecordHash(noVector as EpisodeRecord) === stored;
  } catch { return false; }
}

async function boundRetrieve(client: QdrantWriteVerificationClient, recordType: "processing_policy" | "episode", id: string): Promise<ProcessingPolicyRecord | EpisodeRecord | null> {
  const policy = policyFor(client, recordType);
  const requestedId = physicalPointId(recordType, id);
  const points = await client.retrieve([requestedId], policy, { includeVector: true });
  // Exact physical response cardinality: zero or exactly one requested point;
  // extras, duplicates or unrequested aliases fail closed (never `.find()` and
  // ignore extras).
  const matches = points.filter((candidate) => candidate.id === requestedId);
  if (points.length !== matches.length || matches.length > 1) return null;
  const point = matches[0];
  if (point === undefined) return null;
  let owned: QdrantPoint;
  try { owned = ownedPointSnapshot(point); } catch { return null; }
  const payload = owned.payload;
  if (payload.record_type !== recordType || payload.content_hash === undefined) return null;
  if (recordType === "episode") {
    const parsed = parseBoundEpisodePoint(owned, client.ownerHost);
    if (parsed !== null) return parsed;
    // A present same-ID episode whose hash verifies ONLY under the legacy
    // vector-excluding formula is a verified terminal collision, never an
    // ambiguous null that would loop pending forever.
    if (isLegacyEpisodePoint(owned, client.ownerHost)) throw new QdrantLegacyEpisodeHashError();
    return null;
  }
  const value: unknown = {
    recordType, id, ownerHost: payloadValue(payload, "ownerHost", "owner_host"), schemaRevision: payloadValue(payload, "schemaRevision", "schema_revision"),
    createdAt: payloadValue(payload, "createdAt", "created_at"), privacyEpoch: payloadValue(payload, "privacyEpoch", "privacy_epoch"),
    processingPolicyId: payloadValue(payload, "processingPolicyId", "processing_policy_id"), expiresAt: payloadValue(payload, "expiresAt", "expires_at"), contentHash: payloadValue(payload, "contentHash", "content_hash"),
    policy: payload.policy, canonicalHash: payloadValue(payload, "canonicalHash", "canonical_hash"),
  };
  try {
    const parsed = parseMemoryRecord(value);
    if (parsed.contentHash !== canonicalRecordHash(parsed) || !sameCanonicalWirePayload(owned, parsed as ProcessingPolicyRecord | EpisodeRecord) || !sameCanonicalWireVector(owned, parsed as ProcessingPolicyRecord | EpisodeRecord)) return null;
    return parsed as ProcessingPolicyRecord | EpisodeRecord;
  } catch { return null; }
}
function isVerifiedBoundCollision(readback: ProcessingPolicyRecord | EpisodeRecord | null, record: ProcessingPolicyRecord | EpisodeRecord): boolean {
  return readback !== null && readback.recordType === record.recordType && readback.id === record.id && readback.contentHash !== record.contentHash;
}
/** Module-private unexported issuer: bound destinations are constructed only through the factory. */
const BOUND_QDRANT_DESTINATION_ISSUER = Symbol("pi-qdrant-memory-v2.bound-qdrant-destination-issuer");

/**
 * Nominal, frozen, privately branded bound Qdrant destination. It snapshots
 * the endpoint/owner/collection/coordination identity and binds insert/readback
 * once; forged prototypes and monkeypatched statics fail the brand check.
 */
export class BoundQdrantDestination {
  readonly #issuer: symbol;
  readonly #transportToken: object;
  readonly endpoint: string;
  readonly destination: AuthorizedDestination;
  readonly ownerHost: "pi" | "prime";
  readonly collection: "pi_memory" | "prime_memory";
  readonly coordination: { readonly policyHash: string; readonly policyEpoch: number };
  readonly #insertAndReadback: (record: ProcessingPolicyRecord | EpisodeRecord) => Promise<"inserted" | "existing">;
  readonly #retrieve: <T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string) => Promise<T | null>;
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(input: { endpoint: string; destination: AuthorizedDestination; ownerHost: "pi" | "prime"; collection: "pi_memory" | "prime_memory"; coordination: { policyHash: string; policyEpoch: number }; transportToken: object; insertAndReadback: (record: ProcessingPolicyRecord | EpisodeRecord) => Promise<"inserted" | "existing">; retrieve: <T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string) => Promise<T | null>; }, issuer: symbol) {
    if (issuer !== BOUND_QDRANT_DESTINATION_ISSUER) throw new TypeError("Qdrant destination requires the module issuer");
    this.#issuer = issuer;
    this.#transportToken = input.transportToken;
    this.endpoint = input.endpoint;
    this.destination = Object.freeze({ ...input.destination });
    this.ownerHost = input.ownerHost;
    this.collection = input.collection;
    this.coordination = Object.freeze({ policyHash: input.coordination.policyHash, policyEpoch: input.coordination.policyEpoch });
    this.#insertAndReadback = input.insertAndReadback;
    this.#retrieve = input.retrieve;
    Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is BoundQdrantDestination {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof BoundQdrantDestination && value.#issuer === BOUND_QDRANT_DESTINATION_ISSUER;
  }
  insertAndReadback(record: ProcessingPolicyRecord | EpisodeRecord): Promise<"inserted" | "existing"> { return this.#insertAndReadback(record); }
  retrieve<T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string): Promise<T | null> { return this.#retrieve(recordType, id); }
  /** Opaque per-instance transport identity; compared by `===` in the ingest bundle. */
  get transport(): object { return this.#transportToken; }
}
Object.freeze(BoundQdrantDestination);
Object.freeze(BoundQdrantDestination.prototype);

/**
 * Module-private: build the factory over an EXISTING lexical session (used by
 * the safe bundle so store + destination share the exact transport).
 */
function createQdrantDestinationFactoryFromSession(session: RestQdrantSessionWriter, input: Omit<QdrantDestinationFactoryInput, "options">): QdrantDestinationFactory {
  if (!isProductionRestQdrantSessionWriter(session)) throw new TypeError("Qdrant destination requires a production-bound REST session (no injected transport)");
  // Snapshot every untrusted input field EXACTLY ONCE (plain frozen objects).
  const destination = snapshotAuthorizedDestination(input.destination);
  const egressMode = input.egressMode;
  const nodeId = input.nodeId;
  const coordinationPolicyHash = input.coordinationPolicyHash;
  const coordinationPolicyEpoch = input.coordinationPolicyEpoch;
  validCoordinationBinding(coordinationPolicyHash, coordinationPolicyEpoch);
  const endpoint = canonicalEgressEndpoint(session.endpoint);
  if (session.collection !== expectedQdrantCollection(session.ownerHost)) throw new TypeError("Qdrant writer endpoint/host/collection pairing is invalid");
  const client = Object.freeze({ endpoint: session.endpoint, ownerHost: session.ownerHost, collection: session.collection, maxClockSkewMs: session.maxClockSkewMs, retrieve: session.retrieve.bind(session), upsertPoints: session.upsertPoints.bind(session) }) as QdrantWriteVerificationClient;
  const ownerHost = session.ownerHost; const collection = session.collection; const transportToken = transportTokenOf(session) ?? Object.freeze({});
  if (client.endpoint !== endpoint || client.ownerHost !== ownerHost || client.collection !== collection || client.collection !== expectedQdrantCollection(client.ownerHost)) throw new TypeError("Qdrant writer endpoint/host/collection pairing is invalid");
  const configuredIdentity = Object.freeze({ ...destination });
  const configured = bindConfiguredDestination({ endpoint, configuredDestination: configuredIdentity, requestedDestination: configuredIdentity, egressMode, ...(nodeId === undefined ? {} : { nodeId }) });
  const coordination = Object.freeze({ policyHash: coordinationPolicyHash, policyEpoch: coordinationPolicyEpoch });
  return Object.freeze({ bind: (requested: AuthorizedDestination): BoundQdrantDestination => {
    const destination = Object.freeze({ ...bindConfiguredDestination({ endpoint, configuredDestination: configured, requestedDestination: requested, egressMode, ...(nodeId === undefined ? {} : { nodeId }) }) });
    return new BoundQdrantDestination({
      endpoint, destination, ownerHost, collection, coordination, transportToken,
      insertAndReadback: async (record: ProcessingPolicyRecord | EpisodeRecord) => {
        let result: "inserted" | "existing";
        try { result = await insertOnly(client, record); }
        catch {
          let observed: ProcessingPolicyRecord | EpisodeRecord | null = null;
          try { observed = await boundRetrieve(client, record.recordType, record.id); }
          catch (readError) {
            // A present same-ID legacy-hash episode is a verified terminal
            // collision, never an ambiguous pending loop.
            if (readError instanceof QdrantLegacyEpisodeHashError) throw new QdrantContentHashCollisionError();
            /* ambiguous readback remains retryable */
          }
          if (isVerifiedBoundCollision(observed, record)) throw new QdrantContentHashCollisionError();
          throw new Error("Qdrant insertion failed");
        }
        let readback: ProcessingPolicyRecord | EpisodeRecord | null;
        try { readback = await boundRetrieve(client, record.recordType, record.id); }
        catch (readError) {
          // The post-success readback also maps the legacy classifier error to
          // the terminal collision category (a race that lands a legacy point
          // between insert and readback must never become a pending loop).
          if (readError instanceof QdrantLegacyEpisodeHashError) throw new QdrantContentHashCollisionError();
          throw readError;
        }
        if (readback === null) throw new Error("Qdrant insert/readback is unavailable");
        if (isVerifiedBoundCollision(readback, record)) throw new QdrantContentHashCollisionError();
        // For episodes neither the vector-bound hash nor the vector alone is
        // enough: the FULL record (including hash-excluded fields such as
        // createdAt/producerId/nodeId) must equal the record exactly.
        if (record.recordType === "episode" ? !sameEpisodeRecordExact(readback, record) : readback.contentHash !== record.contentHash) throw new Error("Qdrant insert/readback is unavailable");
        return result;
      },
      retrieve: async <T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string): Promise<T | null> => {
        try { return await boundRetrieve(client, recordType, id) as T | null; }
        catch (error) {
          // Legacy-hash episodes surface as the existing verified
          // content-hash collision terminal category.
          if (error instanceof QdrantLegacyEpisodeHashError) throw new QdrantContentHashCollisionError();
          throw error;
        }
      },
    }, BOUND_QDRANT_DESTINATION_ISSUER);
  } });
}

/** Create a closure that snapshots one canonical endpoint/client/destination pairing. The raw session is constructed LEXICALLY from validated options. */
export function createQdrantDestinationFactory(input: QdrantDestinationFactoryInput): QdrantDestinationFactory {
  // Snapshot every untrusted field EXACTLY ONCE; the session is built from the
  // plain options snapshot and the factory input is a plain frozen object.
  const options = snapshotQdrantOptions(input.options);
  const destination = snapshotAuthorizedDestination(input.destination);
  const egressMode = input.egressMode;
  const nodeId = input.nodeId;
  const coordinationPolicyHash = input.coordinationPolicyHash;
  const coordinationPolicyEpoch = input.coordinationPolicyEpoch;
  const rest: Omit<QdrantDestinationFactoryInput, "options"> = { destination, egressMode, ...(nodeId === undefined ? {} : { nodeId }), coordinationPolicyHash, coordinationPolicyEpoch };
  return createQdrantDestinationFactoryFromSession(new RestQdrantSessionWriter(options), rest);
}

/** Bind an exact expected identity; callers cannot pass an independent allowlist. */
export function bindQdrantDestination(factory: QdrantDestinationFactory, destination: AuthorizedDestination): BoundQdrantDestination {
  // Snapshot the bound function and the destination EXACTLY ONCE: a getter
  // swap between reads can neither relabel the destination nor retarget bind.
  const bindFn = factory?.bind;
  if (typeof bindFn !== "function") throw new TypeError("Qdrant destination factory is invalid");
  const dest = snapshotAuthorizedDestination(destination);
  return bindFn(dest);
}




/** @internal */
/**
 * OCC single-point CAS for the mutable curated-current point: update_only +
 * typed update_filter pins owner/record/version/epoch/hash/privacy/resolution
 * and the exact resolved content or conflict manifest; strong ordering/wait,
 * then reread and exact payload compare. A Qdrant-acknowledged zero-match or
 * delayed concurrent write returns false; callers return only the exact reread.
 */
async function casCuratedCurrent(client: QdrantSessionWriter, input: { id: string; precondition: CuratedCurrentCasPrecondition; next: CuratedCurrentRecord }): Promise<boolean> {
  if (input.precondition.kind !== "current-cas" || input.precondition.recordType !== "curated_current" || input.precondition.id !== input.next.id || input.id !== physicalPointId("curated_current", input.next.id)) fail("Curated-current CAS precondition does not match the point");
  const next = input.next;
  if (next.recordType !== "curated_current" || next.ownerHost !== client.ownerHost || next.contentHash !== canonicalRecordHash(next)) fail("Curated-current CAS record is invalid");
  const p = input.precondition;
  if (next.version !== p.expectedVersion + 1) fail("Curated-current CAS must advance version exactly once");
  if (next.coordinationPolicyEpoch !== p.expectedEpoch || next.coordinationPolicyHash !== p.expectedPolicyHash || next.privacyEpoch !== p.expectedPrivacyEpoch) fail("Curated-current CAS must preserve the pinned coordination/privacy identity");
  // The old processing-policy intersection and expiry are pinned by the
  // update filter. A causally later accepted job may intentionally publish its
  // own intersection envelope; the materializer has already bound `next` to
  // that accepted job before reaching this OCC seam.
  if (next.resolution === "resolved") {
    // A resolved transition is allowed from resolved (a later effective value)
    // or from conflict (a later-dated observation resolves the conflicted view).
    if (p.expectedResolution !== "resolved" && p.expectedResolution !== "conflict") fail("Curated-current CAS resolution transition is invalid");
  } else {
    // A conflict transition is allowed from a resolved current (a within-skew
    // different value) or from another conflict; conflict->conflict may only
    // GROW the immutable manifest member set (the same manifest would mean
    // nothing changed and must not re-CAS).
    if (next.conflictManifestHash === undefined) fail("Curated-current CAS conflict transition is invalid");
    if (p.expectedResolution === "conflict" && (p.expectedConflictManifestHash === null || next.conflictManifestHash === p.expectedConflictManifestHash)) fail("Curated-current CAS conflict transition is invalid");
  }
  const point = recordPoint(next);
  await client.upsertPoints([point], "update_only", p);
  const policy = policyFor(client, "curated_current", "internal");
  const reread = await retrieveOne(client, point.id, policy, true);
  if (reread === undefined) return false;
  if (next.resolution === "resolved") {
    const intended = next.vector;
    const actual = reread.vector?.semantic;
    if (!Array.isArray(intended) || intended.length !== 1024 || !Array.isArray(actual) || actual.length !== 1024 || intended.some((value, index) => actual[index] !== value)) return false;
  } else if (reread.vector !== undefined) return false;
  return canonicalStringify(reread.payload) === canonicalStringify(payloadFromParsed(next));
}

/**
 * The PRODUCTION coordination store exposes NO raw mutators (no control
 * compare-and-swap, lease/job/proposal/coverage/tombstone inserts, generic
 * upsert, session, writer or client escape). Its public
 * surface is validated READS plus the opaque transport token; every mutation
 * flows through the NAMED SAFE high-level methods on this class
 * (claim/renew/release/accept/createJob/writeProposal/markCoverage/
 * createTombstone + named control transitions), which delegate to
 * package-internal `...OnProtocol` implementations over the TRUE #-private
 * `#protocol` field.
 * There is NO exported registry, facade, register/resolve function or raw
 * protocol escape anywhere in the package: `#protocol` is unreachable from
 * outside this class.
 */
export class ProductionCoordinationStore {
  readonly #issuer: symbol;
  readonly #bound: Pick<QdrantSessionWriter, "retrieve" | "scroll" | "upsertPoints">;
  /** TRUE #-private raw protocol: the ONLY way this class touches raw inserts/CAS. Never exposed, returned or registered. */
  readonly #protocol: CoordinationStore;
  /** Module-private per-store authority scope: LeaseAuthority/QuiescenceProof bind THIS object; no caller can obtain or forge it. */
  readonly #authorityScope: object;
  readonly #transportToken: object;
  readonly ownerHost: "pi" | "prime";
  readonly endpoint: string;
  readonly collection: "pi_memory" | "prime_memory";
  readonly maxClockSkewMs: number;
  /** Public constructor is unusable without the module-private issuer symbol; the raw session is constructed LEXICALLY from validated options. */
  constructor(options: QdrantClientOptions, issuer: symbol, sharedSession?: object) {
    if (issuer !== PRODUCTION_STORE_ISSUER) throw new TypeError("Production store requires the module issuer");
    if (options.fetchImpl !== undefined) throw new TypeError("Production store requires a production-bound session: injected fetchImpl transports are rejected");
    this.#issuer = issuer;
    const session = (sharedSession ?? new RestQdrantSessionWriter(options)) as RestQdrantSessionWriter;
    // Blocker A: only a GENUINE lexical session with NO injected transport may
    // carry Production authority; a fabricated fetch can never mint it.
    if (!isProductionRestQdrantSessionWriter(session)) throw new TypeError("Production store requires a production-bound REST session (no injected transport)");
    this.#authorityScope = Object.freeze({});
    this.#transportToken = transportTokenOf(session) ?? Object.freeze({});
    this.ownerHost = session.ownerHost;
    this.endpoint = session.endpoint;
    this.collection = session.collection;
    this.maxClockSkewMs = session.maxClockSkewMs;
    const bound = Object.freeze({ retrieve: session.retrieve.bind(session), scroll: session.scroll.bind(session), upsertPoints: session.upsertPoints.bind(session) });
    this.#bound = bound;
    const frozenSession = Object.freeze({ ...bound, endpoint: this.endpoint, ownerHost: this.ownerHost, collection: this.collection, maxClockSkewMs: this.maxClockSkewMs }) as QdrantSessionWriter;
    // The raw write primitives are NON-exported functions in this module; the
    // engine binds them to this exact session and is used to build the TRUE
    // #-private raw protocol; nothing is registered, exported or reachable by
    // callers.
    const engine = privateWriteEngine(frozenSession);
    const facade: CoordinationStore = {
      ownerHost: this.ownerHost,
      readControl: async () => this.readControl(),
      readJob: async (id: string) => this.readJob(id),
      insertJob: async (job: JobRecord) => engine.insertOnly(job),
      readProposal: async (id: string) => this.readProposal(id),
      insertProposal: async (proposal: ProposalRecord) => engine.insertOnly(proposal),
      readTombstones: async (targetIds: readonly string[]) => this.readTombstones(targetIds),
      insertTombstone: async (tombstone: TombstoneRecord) => engine.insertOnly(tombstone),
      readCoverage: async (coverageIds: readonly string[]) => this.readCoverage(coverageIds),
      insertCoverage: async (coverage: CoverageRecord) => engine.insertOnly(coverage),
      readLease: async (jobIdValue: string) => this.readLease(jobIdValue),
      insertLease: async (lease: LeaseRecord) => engine.insertOnly(lease),
      casLease: async (input: Parameters<CoordinationStore["casLease"]>[0]) => {
        const precondition: LeaseCasPrecondition = { kind: "lease-cas", ownerHost: this.ownerHost, recordType: "lease", jobId: input.jobId, expectedVersion: input.expectedVersion, expectedFencingToken: input.expectedFencingToken, expectedPolicyEpoch: input.expectedPolicyEpoch, expectedPolicyHash: input.expectedPolicyHash, expectedPrivacyEpoch: input.expectedPrivacyEpoch, expectedState: input.expectedState, expectedOwner: input.expectedOwner, expectedAcceptedProposalId: input.expectedAcceptedProposalId, expectedAcceptedManifestHash: input.expectedAcceptedManifestHash, expectedProcessingPolicyId: input.expectedProcessingPolicyId, expectedCreatedAt: input.expectedCreatedAt, expectedContentHash: input.expectedContentHash, ...(input.expiresBefore === undefined ? {} : { expiresBefore: input.expiresBefore }), ...(input.expiresAfter === undefined ? {} : { expiresAfter: input.expiresAfter }) };
        return engine.casPoint({ recordType: "lease", id: leasePointId(input.jobId), precondition, next: input.next });
      },
      compareAndSwapControl: async (expectedVersion: number, next: ControlRecord) => {
        if (next.id !== COLLECTION_CONTROL_ID || next.ownerHost !== this.ownerHost || next.version !== expectedVersion + 1 || next.contentHash !== canonicalRecordHash(next)) return false;
        let current: ControlRecord;
        try { current = await this.readControl(); } catch { return false; }
        if (current.version !== expectedVersion || current.state === "retired") return false;
        const patch: Record<string, unknown> = { version: next.version, processingPolicyId: next.processingPolicyId, activeGeneration: next.activeGeneration, activeBaseGeneration: next.activeBaseGeneration, privacyEpoch: next.privacyEpoch, coordinationPolicyEpoch: next.coordinationPolicyEpoch, coordinationPolicyHash: next.coordinationPolicyHash, state: next.state, scanCursor: next.scanCursor, lastForgetBarrier: next.lastForgetBarrier, revokedDestinationIds: [...next.revokedDestinationIds], contentHash: next.contentHash };
        return engine.updateOnlyCas({ id: COLLECTION_CONTROL_ID, expectedVersion, expectedEpoch: current.coordinationPolicyEpoch, patch });
      },
      scrollLeases: async (offset?: string, limit = 256) => this.scrollLeases(offset, limit),
      scrollJobs: async (offset?: string, limit = 256) => this.scrollJobs(offset, limit),
      readEpisode: async (episodeIdValue: string) => this.readEpisode(episodeIdValue),
      readEpisodes: async (episodeIds: readonly string[], expectedPrivacyEpoch?: number) => this.readEpisodes(episodeIds, expectedPrivacyEpoch),
      readCurated: async (recordType, id) => this.#readCurated(recordType, id),
      insertCurated: async (record: CuratedMemoryRecord | CuratedCurrentRecord | EvidenceLinkRecord | ConflictManifestRecord) => engine.insertOnly(record),
      casCuratedCurrent: async (input: { id: string; precondition: CuratedCurrentCasPrecondition; next: CuratedCurrentRecord }) => engine.casCuratedCurrent(input),
      readRaptor: async (id: string) => this.readRaptorSummary(id),
      insertRaptor: async (record: RaptorSummaryRecord) => engine.insertOnly(record),
      publishGenerationControl: async (expectedVersion: number, expectedBaseGeneration: string | null, next: ControlRecord) => engine.publishControlCas({ expectedVersion, expectedBaseGeneration, next }),
    };
    this.#protocol = Object.freeze(facade);
    Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is ProductionCoordinationStore {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof ProductionCoordinationStore && value.#issuer === PRODUCTION_STORE_ISSUER;
  }
  /** Opaque per-instance transport identity; compared by `===` in the ingest bundle. */
  get transport(): object { return this.#transportToken; }
  private internalPolicy(recordType: "lease" | "job" | "proposal" | "coverage" | "tombstone" | "episode" | "curated_memory" | "curated_current" | "conflict_manifest" | "evidence_link" | "raptor_summary" | "processing_policy", privacyEpoch?: number) {
    return readPolicy({ ownerHost: this.ownerHost, purpose: "internal", recordTypes: [recordType], maxClockSkewMs: this.maxClockSkewMs, ...(privacyEpoch === undefined ? {} : { privacyEpoch }) });
  }
  async readOne<T extends JobRecord | LeaseRecord | ProposalRecord | CoverageRecord | TombstoneRecord>(recordType: T["recordType"], id: string): Promise<T | null> {
    try {
      const points = ownedPointsSnapshot(await this.#bound.retrieve([id], this.internalPolicy(recordType)));
      // Exact physical response cardinality: zero or exactly one requested
      // point; extras, duplicates or unrequested aliases fail closed.
      const matches = points.filter((candidate) => candidate.id === id);
      if (points.length !== matches.length || matches.length > 1) return null;
      const point = matches[0];
      if (point === undefined) return null;
      // BIND the parsed payload identity to the outer point identity and the
      // requested id: a canonical payload for a DIFFERENT logical id served at
      // this physical point is a cross-id alias and fails closed.
      const parsed = coordinationRecordFromPayload(point.payload, this.ownerHost) as T;
      return parsed.recordType === recordType && parsed.id === point.id && parsed.id === id ? parsed : null;
    } catch { return null; }
  }
  async readControl(): Promise<ControlRecord> {
    const points = ownedPointsSnapshot(await this.#bound.retrieve([COLLECTION_CONTROL_ID], readPolicy({ ownerHost: this.ownerHost, purpose: "control", recordTypes: ["collection_control"], maxClockSkewMs: this.maxClockSkewMs })));
    // Exact cardinality: extras/duplicates/aliases are ambiguous and fail closed.
    const matches = points.filter((candidate) => candidate.id === COLLECTION_CONTROL_ID);
    if (points.length !== matches.length || matches.length > 1) throw new TypeError("Collection control readback is ambiguous");
    const point = matches[0];
    if (point === undefined) throw new TypeError("Collection control point is missing");
    // Bind the parsed identity to the outer point identity.
    const parsed = controlRecordFromPayload(point.payload, this.ownerHost);
    if (parsed.id !== COLLECTION_CONTROL_ID || parsed.id !== point.id) throw new TypeError("Collection control identity mismatch");
    return parsed;
  }
  async readLease(jobIdValue: string): Promise<LeaseRecord | null> { return this.readOne("lease", leasePointId(jobIdValue)); }
  async readJob(jobIdValue: string): Promise<JobRecord | null> { return this.readOne("job", jobIdValue); }
  async readProposal(id: string): Promise<ProposalRecord | null> { return this.readOne("proposal", id); }
  async readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]> {
    if (!Array.isArray(targetIds) || targetIds.length === 0 || targetIds.length > 1024 || targetIds.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512) || new Set(targetIds).size !== targetIds.length) throw new TypeError("Tombstone target IDs are invalid");
    const ids = targetIds.map((id) => tombstoneId(this.ownerHost, id));
    const points = ownedPointsSnapshot(await this.#bound.retrieve(ids, this.internalPolicy("tombstone")));
    // Batch cardinality: every returned point must be a requested physical ID
    // exactly once; extras, duplicates or aliases fail closed (missing
    // requested IDs remain meaningful).
    const requested = new Set(ids);
    if (points.some((point) => !requested.has(point.id)) || new Set(points.map((point) => point.id)).size !== points.length) throw new TypeError("Tombstone readback contains unrequested or duplicate points");
    const found = new Map(points.map((point) => [point.id, point]));
    return ids.map((id) => found.get(id)).filter((point) => point !== undefined).map((point) => {
      const parsed = coordinationRecordFromPayload(point!.payload, this.ownerHost);
      // Bind every parsed identity to its outer point identity; a canonical
      // payload for a different logical id is a cross-id alias and rejects the
      // whole batch.
      if (parsed.recordType !== "tombstone" || parsed.id !== point!.id) throw new TypeError("Tombstone readback identity mismatch");
      return parsed as TombstoneRecord;
    });
  }
  async readCoverage(coverageIds: readonly string[]): Promise<CoverageRecord[]> {
    if (!Array.isArray(coverageIds) || coverageIds.length === 0 || coverageIds.length > 1024 || coverageIds.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512) || new Set(coverageIds).size !== coverageIds.length) throw new TypeError("Coverage IDs are invalid");
    const points = ownedPointsSnapshot(await this.#bound.retrieve([...coverageIds], this.internalPolicy("coverage")));
    // Batch cardinality: every returned point must be a requested ID exactly
    // once; extras, duplicates or aliases fail closed.
    const requested = new Set(coverageIds);
    if (points.some((point) => !requested.has(point.id)) || new Set(points.map((point) => point.id)).size !== points.length) throw new TypeError("Coverage readback contains unrequested or duplicate points");
    const found = new Map(points.map((point) => [point.id, point]));
    return [...coverageIds].map((id) => found.get(id)).filter((point) => point !== undefined).map((point) => {
      const parsed = coordinationRecordFromPayload(point!.payload, this.ownerHost);
      if (parsed.recordType !== "coverage" || parsed.id !== point!.id) throw new TypeError("Coverage readback identity mismatch");
      return parsed as CoverageRecord;
    });
  }
  async scrollEpisodeIds(offset: string | undefined, limit = 256, expectedPrivacyEpoch?: number): Promise<{ episodeIds: string[]; nextOffset?: string }> {
    if (expectedPrivacyEpoch !== undefined && (!Number.isSafeInteger(expectedPrivacyEpoch) || expectedPrivacyEpoch < 0)) throw new TypeError("Episode privacy epoch is invalid");
    const result = await this.#bound.scroll({ policy: this.internalPolicy("episode", expectedPrivacyEpoch), ...(offset === undefined ? {} : { offset }), limit });
    const points = ownedPointsSnapshot(result.points); const episodeIds: string[] = []; const seen = new Set<string>();
    for (const point of points) {
      const payload = point.payload;
      if (point.vector !== undefined || seen.has(point.id) || payload.record_type !== "episode" || payload.id !== point.id || payload.owner_host !== this.ownerHost || expectedPrivacyEpoch !== undefined && payload.privacy_epoch !== expectedPrivacyEpoch || typeof payload.content_hash !== "string" || !/^[a-f0-9]{64}$/u.test(payload.content_hash)) throw new TypeError("Episode scroll readback is malformed or foreign");
      seen.add(point.id); episodeIds.push(point.id);
    }
    return { episodeIds, ...(result.nextOffset === undefined ? {} : { nextOffset: result.nextOffset }) };
  }
  async scrollLeases(offset?: string, limit = 256): Promise<{ leases: LeaseRecord[]; nextOffset?: string }> {
    const result = await this.#bound.scroll({ policy: this.internalPolicy("lease"), ...(offset === undefined ? {} : { offset }), limit });
    const ownedPoints = ownedPointsSnapshot(result.points);
    // NEVER ignore malformed/foreign/alias points in the lease scroll: an
    // unparseable or cross-id point could falsely prove quiescence, so every
    // returned point must parse as a lease with parsed.id === point.id.
    const leases: LeaseRecord[] = [];
    for (const point of ownedPoints) {
      const parsed = coordinationRecordFromPayload(point.payload, this.ownerHost);
      if (parsed.recordType !== "lease" || parsed.id !== point.id) throw new TypeError("Lease scroll readback is malformed or foreign");
      leases.push(parsed as LeaseRecord);
    }
    return { leases, ...(result.nextOffset === undefined ? {} : { nextOffset: result.nextOffset }) };
  }
  /** Bounded authoritative job discovery for crash-resume selection. */
  async scrollJobs(offset?: string, limit = 256): Promise<{ jobs: JobRecord[]; nextOffset?: string }> {
    const result = await this.#bound.scroll({ policy: this.internalPolicy("job"), ...(offset === undefined ? {} : { offset }), limit });
    const ownedPoints = ownedPointsSnapshot(result.points);
    const jobs: JobRecord[] = [];
    for (const point of ownedPoints) {
      const parsed = coordinationRecordFromPayload(point.payload, this.ownerHost);
      if (parsed.recordType !== "job" || parsed.id !== point.id) throw new TypeError("Job scroll readback is malformed or foreign");
      jobs.push(parsed as JobRecord);
    }
    return { jobs, ...(result.nextOffset === undefined ? {} : { nextOffset: result.nextOffset }) };
  }
  async readEpisode(episodeIdValue: string, expectedPrivacyEpoch?: number): Promise<EpisodeRecord | null> {
    try {
      // Vector-aware reads: the shared exact parser requires the named semantic
      // vector, exact wire payload and vector-bound contentHash.
      const points = ownedPointsSnapshot(await this.#bound.retrieve([episodeIdValue], this.internalPolicy("episode", expectedPrivacyEpoch), { includeVector: true }));
      // Exact cardinality: extras/duplicates/aliases fail closed.
      const matches = points.filter((candidate) => candidate.id === episodeIdValue);
      if (points.length !== matches.length || matches.length > 1) return null;
      const point = matches[0];
      if (point === undefined) return null;
      const parsed = parseBoundEpisodePoint(point, this.ownerHost);
      return parsed !== null && parsed.ownerHost === this.ownerHost && (expectedPrivacyEpoch === undefined || parsed.privacyEpoch === expectedPrivacyEpoch) ? parsed : null;
    } catch { return null; }
  }
  async #readCurated(recordType: "curated_memory" | "curated_current" | "conflict_manifest" | "evidence_link", id: string): Promise<CuratedMemoryRecord | CuratedCurrentRecord | ConflictManifestRecord | EvidenceLinkRecord | null> {
      // The curated logical ids are tagged strings (occurrence:/current:/
      // conflict:), so the exact physical point id is the deterministic UUID
      // derived from (recordType, logical id).
      const physicalId = physicalPointId(recordType, id);
      // Vector-aware reads: curated observations/currents carry the derived
      // BGE-M3 vector on their points (payload-only reads would lose it).
      const points = ownedPointsSnapshot(await this.#bound.retrieve([physicalId], this.internalPolicy(recordType), { includeVector: true }));
      // Exact cardinality: extras/duplicates/aliases fail closed.
      const matches = points.filter((candidate) => candidate.id === physicalId);
      if (points.length !== matches.length || matches.length > 1) return null;
      const point = matches[0];
      if (point === undefined || point.payload.record_type !== recordType) return null;
      // Bind the parsed identity to the outer point identity and the requested
      // logical id: a canonical payload for a DIFFERENT logical id is a
      // cross-id alias.
      const semantic = point.vector?.semantic;
      const parsed = recordFromPayload(point.payload, this.ownerHost, semantic);
      if (parsed.recordType !== recordType || parsed.id !== id || physicalPointId(recordType, parsed.id) !== point.id) return null;
      // Bind the point's named semantic vector (curated records carry the
      // derived BGE-M3 vector as a query artifact; exactly 1024 finite values).
      if (recordType === "curated_memory") {
        if (!Array.isArray(semantic) || semantic.length !== 1024 || !semantic.every((value) => typeof value === "number" && Number.isFinite(value))) return null;
      } else if (recordType === "curated_current") {
        const typed = parsed as CuratedCurrentRecord;
        if (typed.resolution === "resolved") {
          if (!Array.isArray(semantic) || semantic.length !== 1024 || !semantic.every((value) => typeof value === "number" && Number.isFinite(value))) return null;
        } else if (point.vector !== undefined) return null;
      } else if (point.vector !== undefined) return null;
      return parsed as CuratedMemoryRecord | CuratedCurrentRecord | ConflictManifestRecord | EvidenceLinkRecord;
  }
  /** Page immutable RAPTOR records for one prior generation with exact vector-aware readback. */
  async scrollRaptorSummaries(generationId: string, offset?: string, limit = 256): Promise<{ summaries: RaptorSummaryRecord[]; nextOffset?: string }> {
    if (!validBoundedText(generationId) || (offset !== undefined && !validBoundedText(offset)) || !Number.isSafeInteger(limit) || limit < 1 || limit > 256) throw new TypeError("RAPTOR scroll input is invalid");
    const result = await this.#bound.scroll({ policy: this.internalPolicy("raptor_summary"), ...(offset === undefined ? {} : { offset }), limit });
    const page = ownedPointsSnapshot(result.points);
    if (new Set(page.map((point) => point.id)).size !== page.length) throw new TypeError("RAPTOR scroll contains duplicate points");
    if (page.length === 0) return { summaries: [], ...(result.nextOffset === undefined ? {} : { nextOffset: result.nextOffset }) };
    const full = ownedPointsSnapshot(await this.#bound.retrieve(page.map((point) => point.id), this.internalPolicy("raptor_summary"), { includeVector: true }));
    if (full.length !== page.length || new Set(full.map((point) => point.id)).size !== full.length) throw new TypeError("RAPTOR scroll readback is incomplete or duplicated");
    const byId = new Map(full.map((point) => [point.id, point])); const summaries: RaptorSummaryRecord[] = [];
    for (const outer of page) {
      const point = byId.get(outer.id); if (point === undefined || point.payload.record_type !== "raptor_summary") throw new TypeError("RAPTOR scroll readback identity mismatch");
      const semantic = point.vector?.semantic; const parsed = recordFromPayload(point.payload, this.ownerHost, semantic);
      if (parsed.recordType !== "raptor_summary" || physicalPointId("raptor_summary", parsed.id) !== point.id || parsed.contentHash !== canonicalRecordHash(parsed)) throw new TypeError("RAPTOR scroll record is malformed");
      if (parsed.vector !== undefined && (!Array.isArray(semantic) || semantic.length !== 1024 || !semantic.every((value) => typeof value === "number" && Number.isFinite(value)))) throw new TypeError("RAPTOR scroll vector is malformed");
      if (parsed.vector === undefined && point.vector !== undefined) throw new TypeError("RAPTOR scroll vector is unexpected");
      if (parsed.generationId === generationId) summaries.push(parsed);
    }
    return { summaries, ...(result.nextOffset === undefined ? {} : { nextOffset: result.nextOffset }) };
  }
  async readRaptorSummary(id: string): Promise<RaptorSummaryRecord | null> {
    try {
      if (!validBoundedText(id)) return null;
      const physicalId = physicalPointId("raptor_summary", id);
      const points = ownedPointsSnapshot(await this.#bound.retrieve([physicalId], this.internalPolicy("raptor_summary"), { includeVector: true }));
      const matches = points.filter((candidate) => candidate.id === physicalId);
      if (points.length !== matches.length || matches.length > 1) return null;
      const point = matches[0]; if (point === undefined || point.payload.record_type !== "raptor_summary") return null;
      const semantic = point.vector?.semantic;
      const parsed = recordFromPayload(point.payload, this.ownerHost, semantic);
      if (parsed.recordType !== "raptor_summary" || parsed.id !== id || physicalPointId("raptor_summary", parsed.id) !== point.id) return null;
      if (parsed.vector !== undefined && (!Array.isArray(semantic) || semantic.length !== 1024 || !semantic.every((value) => typeof value === "number" && Number.isFinite(value)))) return null;
      if (parsed.vector === undefined && point.vector !== undefined) return null;
      return parsed;
    } catch { return null; }
  }
  async readEpisodes(episodeIds: readonly string[], expectedPrivacyEpoch?: number): Promise<EpisodeRecord[]> {
    if (expectedPrivacyEpoch !== undefined && (!Number.isSafeInteger(expectedPrivacyEpoch) || expectedPrivacyEpoch < 0)) throw new TypeError("Episode privacy epoch is invalid");
    if (!Array.isArray(episodeIds) || episodeIds.length === 0 || episodeIds.length > 1024 || episodeIds.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512) || new Set(episodeIds).size !== episodeIds.length) throw new TypeError("Episode IDs are invalid");
    const points = ownedPointsSnapshot(await this.#bound.retrieve([...episodeIds], this.internalPolicy("episode", expectedPrivacyEpoch), { includeVector: true }));
    // Exact mapping: every returned episode must be one of the requested point
    // IDs; extras, duplicates, missing vectors, malformed payloads or hash
    // mismatches fail closed through the shared vector-aware parser.
    const requested = new Set(episodeIds);
    const episodes: EpisodeRecord[] = [];
    const seen = new Set<string>();
    for (const point of points) {
      if (!requested.has(point.id) || seen.has(point.id)) throw new TypeError("Episode readback contains extras or duplicates");
      const parsed = parseBoundEpisodePoint(point, this.ownerHost);
      if (parsed === null || parsed.ownerHost !== this.ownerHost || parsed.id !== point.id || expectedPrivacyEpoch !== undefined && parsed.privacyEpoch !== expectedPrivacyEpoch) throw new TypeError("Episode readback point is malformed or identity mismatched");
      seen.add(point.id);
      episodes.push(parsed);
    }
    return episodes;
  }
  async readProcessingPolicies(policyIds: readonly string[]): Promise<ProcessingPolicyRecord[]> {
    if (!Array.isArray(policyIds) || policyIds.length === 0 || policyIds.length > 1024 || policyIds.some((id) => typeof id !== "string" || !/^[a-f0-9]{64}$/u.test(id)) || new Set(policyIds).size !== policyIds.length) throw new TypeError("Processing policy IDs are invalid");
    const physicalIds = policyIds.map((id) => physicalPointId("processing_policy", id));
    const points = ownedPointsSnapshot(await this.#bound.retrieve(physicalIds, this.internalPolicy("processing_policy")));
    const requested = new Map(physicalIds.map((id, index) => [id, policyIds[index]!]));
    const policies: ProcessingPolicyRecord[] = []; const seen = new Set<string>();
    for (const point of points) {
      const logicalId = requested.get(point.id);
      if (logicalId === undefined || seen.has(point.id) || point.payload.record_type !== "processing_policy" || point.vector !== undefined) throw new TypeError("Processing policy readback contains extras or duplicates");
      const parsed = recordFromPayload(point.payload, this.ownerHost);
      if (parsed.recordType !== "processing_policy" || parsed.id !== logicalId || physicalPointId("processing_policy", parsed.id) !== point.id || parsed.contentHash !== canonicalRecordHash(parsed)) throw new TypeError("Processing policy readback point is malformed or identity mismatched");
      seen.add(point.id); policies.push(parsed);
    }
    return policies;
  }

  private async assertAcceptedAuthorityBase(authority: LeaseAuthority): Promise<JobRecord> {
    if (!LeaseAuthority.isValid(authority) || !authority.matchesStore(this) || !authority.matchesScope(this.#authorityScope) || authority.state !== "accepted") throw new TypeError("Curated write requires a genuine accepted lease authority");
    const job = await this.readJob(authority.jobId);
    const claim = await this.readLease(authority.jobId);
    if (job === null || claim === null || !authority.matchesClaim(claim) || claim.state !== "accepted") throw new TypeError("Curated write authority claim is stale");
    const now = authority.now();
    if (jobExpired(job, now, authority.maxClockSkewMs) || Date.parse(claim.expiresAt) <= now) throw new TypeError("Curated write authority is expired");
    return job;
  }

  private async assertRaptorAuthority(authority: LeaseAuthority, destinationIds: readonly string[], targetIds: readonly string[]): Promise<{ job: JobRecord; control: ControlRecord; claim: LeaseRecord; digest: string }> {
    if (!LeaseAuthority.isValid(authority) || !authority.matchesStore(this) || !authority.matchesScope(this.#authorityScope) || (authority.state !== "leased" && authority.state !== "accepted")) throw new TypeError("RAPTOR operation requires a genuine live lease authority");
    if (!Array.isArray(destinationIds) || destinationIds.length < 2 || destinationIds.length > 3 || destinationIds.some((id) => !validBoundedText(id, 256)) || new Set(destinationIds).size !== destinationIds.length) throw new TypeError("RAPTOR destination identity is invalid");
    if (!Array.isArray(targetIds) || targetIds.length === 0 || targetIds.length > 65_536 || targetIds.some((id) => !validBoundedText(id)) || new Set(targetIds).size !== targetIds.length) throw new TypeError("RAPTOR evidence targets are invalid");
    const control = await this.readControl(); const job = await this.readJob(authority.jobId); const claim = await this.readLease(authority.jobId);
    if (job === null || claim === null || !authority.matchesClaim(claim) || claim.state !== authority.state || !claimIdentityMatchesJob(claim, job)) throw new TypeError("RAPTOR authority claim is stale");
    const now = authority.now();
    if (jobExpired(job, now, authority.maxClockSkewMs) || Date.parse(claim.expiresAt) <= now || control.state !== "active" || control.privacyEpoch !== authority.privacyEpoch || control.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || control.coordinationPolicyHash !== authority.coordinationPolicyHash || destinationIds.some((id) => control.revokedDestinationIds.includes(id))) throw new TypeError("RAPTOR authority is expired or revoked");
    if (job.ownerHost !== this.ownerHost || job.id !== authority.jobId || job.policyId !== authority.processingPolicyId || job.policyHash !== authority.coordinationPolicyHash || job.policyEpoch !== authority.coordinationPolicyEpoch || job.privacyEpoch !== authority.privacyEpoch || job.membership.length !== targetIds.length || targetIds.some((id, index) => job.membership[index] !== id)) throw new TypeError("RAPTOR authority is not bound to the exact job membership");
    const tombstones = [];
    for (let index = 0; index < targetIds.length; index += 1024) tombstones.push(...await this.readTombstones(targetIds.slice(index, index + 1024)));
    if (tombstones.length !== 0) throw new TypeError("RAPTOR evidence is tombstoned");
    const digest = canonicalStringify({ control, job, claim, destinationIds: [...destinationIds].sort(), targetIds: [...targetIds], tombstones });
    return { job, control, claim, digest };
  }

  private async assertCuratedRecordAgainstJob(record: CuratedMemoryRecord | CuratedCurrentRecord | EvidenceLinkRecord | ConflictManifestRecord | CoverageRecord, job: JobRecord): Promise<void> {
    if (record.ownerHost !== this.ownerHost || record.processingPolicyId !== job.policyId || record.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || record.coordinationPolicyHash !== job.coordinationPolicyHash || record.privacyEpoch !== job.privacyEpoch || record.expiresAt !== job.expiresAt) throw new TypeError("Curated write record policy/owner binding is invalid");
    const members = new Set(job.membership);
    const priorJobMatches = (prior: JobRecord | null, episodeId: string): prior is JobRecord => prior !== null && prior.ownerHost === this.ownerHost && prior.policyId === job.policyId && prior.policyHash === job.policyHash && prior.policyEpoch === job.policyEpoch && prior.privacyEpoch === job.privacyEpoch && prior.extractorRevision === job.extractorRevision && prior.expiresAt === job.expiresAt && prior.membership.includes(episodeId);
    const acceptedLeaseMatches = (prior: JobRecord, lease: LeaseRecord | null): boolean => lease !== null && (lease.state === "accepted" || lease.state === "released" || lease.state === "completed") && lease.acceptedProposalId !== null && lease.acceptedManifestHash !== null && claimIdentityMatchesJob(lease, prior);
    const assertObservationEvidenceClosure = async (observation: CuratedMemoryRecord): Promise<boolean> => {
      const sourceIds = observation.sourceEpisodeIds ?? [];
      if (sourceIds.length === 0 || observation.provenance === undefined || canonicalStringify(sourceIds) !== canonicalStringify(observation.provenance)) throw new TypeError("Conflict member evidence closure is invalid");
      let currentJobEvidence = false;
      for (const episodeId of sourceIds) {
        const linkId = evidenceLinkId(observation.id, episodeId, job.extractorRevision);
        const linkValue = await this.#readCurated("evidence_link", linkId);
        if (linkValue === null || linkValue.recordType !== "evidence_link") throw new TypeError("Conflict member evidence link is missing");
        const link = linkValue as EvidenceLinkRecord;
        if (link.id !== linkId || link.ownerHost !== this.ownerHost || link.processingPolicyId !== job.policyId || link.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || link.coordinationPolicyHash !== job.coordinationPolicyHash || link.privacyEpoch !== job.privacyEpoch || link.expiresAt !== job.expiresAt || link.createdAt !== observation.createdAt || link.sourceId !== observation.id || link.targetId !== episodeId || link.extractorRevision !== job.extractorRevision || link.contentHash !== canonicalRecordHash(link)) throw new TypeError("Conflict member evidence link is invalid");
        const linkJob = link.jobId === job.id ? job : await this.readJob(link.jobId);
        if (!priorJobMatches(linkJob, episodeId)) throw new TypeError("Conflict member prior job is not bound to the accepted policy");
        const linkLease = await this.readLease(link.jobId);
        if (!acceptedLeaseMatches(linkJob, linkLease)) throw new TypeError("Conflict member prior job has no durable acceptance");
        if (members.has(episodeId)) currentJobEvidence = true;
      }
      return currentJobEvidence;
    };
    const validateManifest = async (manifest: ConflictManifestRecord): Promise<{ aggregate: ReturnType<typeof projectConflictAggregate>; boundMember: boolean }> => {
      if (manifest.ownerHost !== this.ownerHost || manifest.processingPolicyId !== job.policyId || manifest.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || manifest.coordinationPolicyHash !== job.coordinationPolicyHash || manifest.privacyEpoch !== job.privacyEpoch || manifest.expiresAt !== job.expiresAt || manifest.members.length < 2 || new Set(manifest.members).size !== manifest.members.length || manifest.id !== conflictManifestId(manifest.coordinationPolicyHash, manifest.stateKey, manifest.members) || manifest.contentHash !== canonicalRecordHash(manifest)) throw new TypeError("Conflict manifest members are invalid");
      const observations: CuratedMemoryRecord[] = [];
      let boundMember = false;
      for (const memberId of manifest.members) {
        const value = await this.#readCurated("curated_memory", memberId);
        if (value === null || value.recordType !== "curated_memory") throw new TypeError("Conflict member is missing");
        const member = value as CuratedMemoryRecord;
        if (member.id !== memberId || member.id !== member.observationId || member.ownerHost !== this.ownerHost || member.processingPolicyId !== job.policyId || member.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || member.coordinationPolicyHash !== job.coordinationPolicyHash || member.privacyEpoch !== job.privacyEpoch || member.expiresAt !== job.expiresAt || member.stateKey !== manifest.stateKey || member.createdAt !== member.eventAt || member.effectiveAt !== member.eventAt || member.contentHash !== canonicalRecordHash(member)) throw new TypeError("Conflict member is not bound to the accepted policy");
        if (await assertObservationEvidenceClosure(member)) boundMember = true;
        observations.push(member);
      }
      const aggregate = projectConflictAggregate(observations);
      if (canonicalStringify(manifest.members) !== canonicalStringify(aggregate.members) || manifest.createdAt !== aggregate.createdAt) throw new TypeError("Conflict manifest aggregate is invalid");
      return { aggregate, boundMember };
    };
    if (record.recordType === "evidence_link") {
      if (!members.has(record.targetId)) throw new TypeError("Evidence link is not bound to the accepted job membership");
      const source = await this.#readCurated("curated_memory", record.sourceId);
      if (source === null || source.ownerHost !== this.ownerHost || source.processingPolicyId !== job.policyId || source.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || source.coordinationPolicyHash !== job.coordinationPolicyHash || source.privacyEpoch !== job.privacyEpoch) throw new TypeError("Evidence link source is not bound to the accepted job");
      if (record.jobId !== job.id) {
        const priorJob = await this.readJob(record.jobId);
        if (!priorJobMatches(priorJob, record.targetId)) throw new TypeError("Evidence link prior job is not bound to the accepted policy");
        const priorLease = await this.readLease(record.jobId);
        if (!acceptedLeaseMatches(priorJob, priorLease)) throw new TypeError("Evidence link prior job has no durable acceptance");
      }
    } else if (record.recordType === "curated_memory") {
      const sourceIds = record.sourceEpisodeIds ?? [];
      if (sourceIds.length === 0 || sourceIds.some((id) => !members.has(id))) throw new TypeError("Curated record source evidence is not bound to the accepted job membership");
    } else if (record.recordType === "curated_current") {
      const sourceIds = record.sourceEpisodeIds ?? [];
      if (sourceIds.length === 0) throw new TypeError("Curated current source evidence is empty");
      if (record.resolution === "resolved") {
        if (sourceIds.some((id) => !members.has(id))) throw new TypeError("Resolved current source evidence is not bound to the accepted job membership");
      } else {
        const manifestValue = await this.#readCurated("conflict_manifest", record.conflictManifestHash);
        if (manifestValue === null || manifestValue.recordType !== "conflict_manifest") throw new TypeError("Conflict current manifest is missing");
        const { aggregate, boundMember } = await validateManifest(manifestValue as ConflictManifestRecord);
        if (!boundMember || record.createdAt !== aggregate.createdAt || canonicalStringify(record.sourceEpisodeIds) !== canonicalStringify(aggregate.sourceEpisodeIds) || canonicalStringify(record.effectiveOrder) !== canonicalStringify(aggregate.effectiveOrder)) throw new TypeError("Conflict current aggregate is not bound to the accepted job");
      }
    } else if (record.recordType === "coverage") {
      if (!members.has(record.episodeId) || record.id !== coverageId({ ownerHost: this.ownerHost, episodeId: record.episodeId, extractorRevision: record.extractorRevision, coordinationPolicyHash: record.coordinationPolicyHash, coordinationPolicyEpoch: record.coordinationPolicyEpoch, policyIntersectionId: job.policyId, privacyEpoch: record.privacyEpoch })) throw new TypeError("Coverage is not bound to the accepted job membership");
    } else {
      const { boundMember } = await validateManifest(record);
      if (!boundMember) throw new TypeError("Conflict manifest has no evidence from the accepted job");
    }
  }

  // ---------------------------------------------------------------------------
  // Named SAFE high-level methods. The production class accepts ONLY the same
  // safe high-level inputs/capabilities as the public wrappers — never raw
  // next/CAS/cutoff/callback/facade payloads. Each delegates to a
  // package-internal `...OnProtocol` implementation over the TRUE #-private
  // protocol; raw
  // mutation never leaves the module boundary, and nobody can obtain
  // `#protocol` from outside (it is not exported, returned or registered).
  // ---------------------------------------------------------------------------
  async claimLease(worker: RootWorkerContext, input: ClaimLeaseInput): Promise<LeaseAuthority | null> {
    return claimLeaseOnProtocol(this.#protocol, this, this.#authorityScope, worker, input);
  }
  async renewLease(authority: LeaseAuthority): Promise<LeaseAuthority | null> {
    return renewLeaseOnProtocol(this.#protocol, this, this.#authorityScope, authority);
  }
  async releaseLease(authority: LeaseAuthority): Promise<boolean> {
    return releaseLeaseOnProtocol(this.#protocol, this, this.#authorityScope, authority);
  }
  async acceptLease(authority: LeaseAuthority, proposalId: string): Promise<LeaseAuthority | null> {
    return acceptLeaseAuthorityOnProtocol(this.#protocol, this, this.#authorityScope, authority, proposalId);
  }
  async acceptProposal(authority: LeaseAuthority, input: { proposalId: string }): Promise<LeaseAuthority | null> {
    return acceptProposalOnProtocol(this.#protocol, this, this.#authorityScope, authority, input);
  }
  async createJob(input: CreateJobInput): Promise<JobRecord> {
    return createJobOnProtocol(this.#protocol, input);
  }
  async completeJob(authority: LeaseAuthority): Promise<boolean> {
    return completeJobOnProtocol(this.#protocol, this, this.#authorityScope, authority);
  }
  /** Complete a published RAPTOR job without manufacturing a curation
   * proposal. The named operation proves the active generation and exact
   * evidence barrier before a fenced lease may become terminal. */
  async completeRaptorJob(authority: LeaseAuthority, input: { generationId: string; evidenceIds: readonly string[]; destinationIds: readonly string[] }): Promise<boolean> {
    return completeRaptorJobOnProtocol(this.#protocol, this, this.#authorityScope, authority, input);
  }
  async writeProposal(authority: LeaseAuthority, input: WriteProposalInput): Promise<ProposalRecord> {
    return writeProposalOnProtocol(this.#protocol, this, this.#authorityScope, authority, input);
  }
  async markCoverage(authority: LeaseAuthority, input: MarkCoverageInput): Promise<CoverageRecord> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const normalized = snapshotMarkCoverageInput(input);
    const candidate = coverageRecordForInput(this, normalized);
    await assertCoverageRecordBoundToJob(candidate, job, this.ownerHost);
    return markCoverageOnProtocol(this.#protocol, this, this.#authorityScope, authority, normalized);
  }
  async readObservation(authority: LeaseAuthority, id: string): Promise<CuratedMemoryRecord | null> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const record = await this.#readCurated("curated_memory", id);
    return record !== null && record.recordType === "curated_memory" && record.ownerHost === this.ownerHost && record.coordinationPolicyEpoch === job.coordinationPolicyEpoch && record.coordinationPolicyHash === job.coordinationPolicyHash && record.privacyEpoch === job.privacyEpoch ? record : null;
  }
  async readCurrent(authority: LeaseAuthority, id: string): Promise<CuratedCurrentRecord | null> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const record = await this.#readCurated("curated_current", id);
    return record !== null && record.recordType === "curated_current" && record.ownerHost === this.ownerHost && record.coordinationPolicyEpoch === job.coordinationPolicyEpoch && record.coordinationPolicyHash === job.coordinationPolicyHash && record.privacyEpoch === job.privacyEpoch ? record : null;
  }
  async readConflictManifest(authority: LeaseAuthority, id: string): Promise<ConflictManifestRecord | null> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const record = await this.#readCurated("conflict_manifest", id);
    return record !== null && record.recordType === "conflict_manifest" && record.ownerHost === this.ownerHost && record.coordinationPolicyEpoch === job.coordinationPolicyEpoch && record.coordinationPolicyHash === job.coordinationPolicyHash && record.privacyEpoch === job.privacyEpoch ? record : null;
  }
  async insertObservation(authority: LeaseAuthority, input: { record: CuratedMemoryRecord }): Promise<CuratedMemoryRecord> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const record = ownedCuratedRecordSnapshot(input.record) as CuratedMemoryRecord;
    await this.assertCuratedRecordAgainstJob(record, job);
    return insertCuratedOnProtocol(this.#protocol, this, this.#authorityScope, authority, { record }) as Promise<CuratedMemoryRecord>;
  }
  async insertEvidenceLink(authority: LeaseAuthority, input: { record: EvidenceLinkRecord }): Promise<EvidenceLinkRecord> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const record = ownedCuratedRecordSnapshot(input.record) as EvidenceLinkRecord;
    await this.assertCuratedRecordAgainstJob(record, job);
    return insertCuratedOnProtocol(this.#protocol, this, this.#authorityScope, authority, { record }) as Promise<EvidenceLinkRecord>;
  }
  async insertConflictManifest(authority: LeaseAuthority, input: { record: ConflictManifestRecord }): Promise<ConflictManifestRecord> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const record = ownedCuratedRecordSnapshot(input.record) as ConflictManifestRecord;
    await this.assertCuratedRecordAgainstJob(record, job);
    return insertCuratedOnProtocol(this.#protocol, this, this.#authorityScope, authority, { record }) as Promise<ConflictManifestRecord>;
  }
  async upsertCuratedCurrent(authority: LeaseAuthority, input: { record: CuratedCurrentRecord; expectedVersion: number | null }): Promise<CuratedCurrentRecord | null> {
    const job = await this.assertAcceptedAuthorityBase(authority);
    const expectedVersion = input.expectedVersion;
    const record = ownedCuratedRecordSnapshot(input.record) as CuratedCurrentRecord;
    await this.assertCuratedRecordAgainstJob(record, job);
    return upsertCuratedCurrentOnProtocol(this.#protocol, this, this.#authorityScope, authority, { record, expectedVersion });
  }
  /** Stable, capability-gated RAPTOR authority checkpoint around every visible operation. */
  async readRaptorBarrier(authority: LeaseAuthority, input: { destinationIds: readonly string[]; evidenceIds: readonly string[] }): Promise<string> {
    return (await this.assertRaptorAuthority(authority, input.destinationIds, input.evidenceIds)).digest;
  }
  /** Insert one immutable summary/manifest node and require exact vector-aware readback. */
  async writeRaptorSummary(authority: LeaseAuthority, input: { record: RaptorSummaryRecord; destinationIds: readonly string[]; evidenceIds: readonly string[] }): Promise<RaptorSummaryRecord> {
    const before = await this.assertRaptorAuthority(authority, input.destinationIds, input.evidenceIds);
    const record = parseMemoryRecord(input.record, { ownerHost: this.ownerHost, privacyEpoch: authority.privacyEpoch, coordinationPolicyEpoch: authority.coordinationPolicyEpoch, vectorDimension: 1024 }) as RaptorSummaryRecord;
    if (record.recordType !== "raptor_summary" || record.ownerHost !== this.ownerHost || record.jobId !== before.job.id || record.fencingToken !== authority.fencingToken || record.processingPolicyId !== before.job.policyId || record.coordinationPolicyHash !== before.job.policyHash || record.coordinationPolicyEpoch !== before.job.policyEpoch || record.privacyEpoch !== before.job.privacyEpoch || record.expiresAt !== before.job.expiresAt || record.contentHash !== canonicalRecordHash(record)) throw new TypeError("RAPTOR summary is not bound to the live job");
    await this.#protocol.insertRaptor(record);
    const after = await this.assertRaptorAuthority(authority, input.destinationIds, input.evidenceIds);
    if (after.digest !== before.digest) throw new TypeError("RAPTOR authority changed during summary write");
    const readback = await this.readRaptorSummary(record.id);
    if (readback === null || canonicalStringify(readback) !== canonicalStringify(record)) throw new TypeError("RAPTOR summary readback is not exact");
    return readback;
  }
  /** Single fenced publication CAS; losing immutable nodes remain unreachable. */
  async publishRaptorGeneration(authority: LeaseAuthority, input: { expected: ControlRecord; generationId: string; destinationIds: readonly string[]; evidenceIds: readonly string[] }): Promise<boolean> {
    const generationId = input.generationId; if (!validBoundedText(generationId)) throw new TypeError("RAPTOR generation ID is invalid");
    const before = await this.assertRaptorAuthority(authority, input.destinationIds, input.evidenceIds);
    const expected = parseMemoryRecord(input.expected, { ownerHost: this.ownerHost }) as ControlRecord;
    if (expected.recordType !== "collection_control" || canonicalStringify(expected) !== canonicalStringify(before.control) || expected.state !== "active") return false;
    const pending = { ...expected, version: expected.version + 1, activeGeneration: generationId, activeBaseGeneration: expected.activeGeneration, contentHash: "pending" } as ControlRecord;
    const next = { ...pending, contentHash: canonicalRecordHash(pending) } as ControlRecord;
    const second = await this.assertRaptorAuthority(authority, input.destinationIds, input.evidenceIds);
    if (second.digest !== before.digest) return false;
    if (!await this.#protocol.publishGenerationControl(expected.version, expected.activeBaseGeneration, next)) return false;
    const final = await this.readControl();
    return canonicalStringify(final) === canonicalStringify(next);
  }
  async createTombstone(input: CreateTombstoneInput): Promise<TombstoneRecord[]> {
    return createTombstoneOnProtocol(this.#protocol, this, this.#authorityScope, input);
  }
  async initializeControl(initial: ControlRecord): Promise<ControlRecord> {
    return initializeControlOnProtocol(this.#protocol, this.#authorityScope, initial);
  }
  async beginPolicyDrain(input: { now: number }): Promise<ControlRecord> {
    return beginPolicyDrainOnProtocol(this.#protocol, this.#authorityScope, input);
  }
  async waitForOldLeasesToQuiesce(input: { retiredEpoch: number; maxLeaseMs: number; maxClockSkewMs: number; timeoutMs?: number; pollIntervalMs?: number; now?: () => number; signal?: AbortSignal }): Promise<QuiescenceProof> {
    return waitForOldLeasesToQuiesceOnProtocol(this.#protocol, this.#authorityScope, input);
  }
  async activatePolicyEpoch(input: { proof: QuiescenceProof; nextPolicyHash: string; memoryModelTimeoutMs: number; signal?: AbortSignal }): Promise<ControlRecord> {
    return activatePolicyEpochOnProtocol(this.#protocol, this.#authorityScope, input);
  }
  async rotateCoordinationPolicy(input: { nextPolicyHash: string; maxLeaseMs: number; maxClockSkewMs: number; memoryModelTimeoutMs: number; quiesceTimeoutMs?: number; now: number; signal?: AbortSignal }): Promise<ControlRecord> {
    return rotateCoordinationPolicyOnProtocol(this.#protocol, this.#authorityScope, input);
  }
  async beginForgetBarrier(input: { now: number; revokedDestinationIds?: readonly string[] }): Promise<ControlRecord> {
    return beginForgetBarrierOnProtocol(this.#protocol, this.#authorityScope, input);
  }
}




Object.freeze(ProductionCoordinationStore);
Object.freeze(ProductionCoordinationStore.prototype);

/** Module-private unexported issuer: production stores are constructed only inside this module. */
const PRODUCTION_STORE_ISSUER = Symbol("pi-qdrant-memory-v2.production-store-issuer");

/** Production seam: validated OPTIONS in, ONLY the safe store out (never accepts/returns a raw writer/session). */
export function createQdrantCoordinationStore(options: QdrantClientOptions): ProductionCoordinationStore {
  // Snapshot the untrusted options EXACTLY ONCE; the constructor only ever sees
  // the plain frozen snapshot (no accessor can be re-read or swapped later).
  const snapshot = snapshotQdrantOptions(options);
  return new ProductionCoordinationStore(snapshot, PRODUCTION_STORE_ISSUER);
}

export interface QdrantSafeBundleInput {
  options: QdrantClientOptions;
  destination: AuthorizedDestination;
  egressMode: RuntimeConfig["privacy"]["egressMode"];
  nodeId?: string;
  coordinationPolicyHash: string;
  coordinationPolicyEpoch: number;
}
export interface QdrantSafeBundle {
  store: ProductionCoordinationStore;
  qdrant: QdrantDestinationFactory;
  transport: object;
}
/**
 * ONE safe construction for the ingest bundle: a single LEXICAL session
 * serves BOTH the production store and the destination factory, so the exact
 * transport-object binding (store.transport === bound-destination.transport)
 * holds without any raw session being exported, returned or accepted.
 */
export function createQdrantSafeBundle(input: QdrantSafeBundleInput): QdrantSafeBundle {
  // Snapshot every untrusted field EXACTLY ONCE; no getter is ever re-read and
  // no spread of the caller object happens anywhere in the chain.
  const options = snapshotQdrantOptions(input.options);
  const destination = snapshotAuthorizedDestination(input.destination);
  const egressMode = input.egressMode;
  const nodeId = input.nodeId;
  const coordinationPolicyHash = input.coordinationPolicyHash;
  const coordinationPolicyEpoch = input.coordinationPolicyEpoch;
  if (options.fetchImpl !== undefined) throw new TypeError("Qdrant bundle requires a production-bound session: injected fetchImpl transports are rejected");
  const session = new RestQdrantSessionWriter(options);
  if (!isProductionRestQdrantSessionWriter(session)) throw new TypeError("Qdrant bundle requires a production-bound REST session (no injected transport)");
  const transport = transportTokenOf(session) ?? Object.freeze({});
  const store = new ProductionCoordinationStore(options, PRODUCTION_STORE_ISSUER, session);
  const qdrant = createQdrantDestinationFactoryFromSession(session, { destination, egressMode, ...(nodeId === undefined ? {} : { nodeId }), coordinationPolicyHash, coordinationPolicyEpoch });
  return { store, qdrant, transport };
}

// ---------------------------------------------------------------------------
// Authority kernel: leases (lexical; NOT exported)
// ---------------------------------------------------------------------------

const LEASE_AUTHORITY_ISSUER = Symbol("pi-qdrant-memory-v2.lease-authority");

interface LeaseAuthorityState {
  store: ProductionCoordinationStore;
  /** Module-private per-store authority scope: minted ONLY from the owning store's lexical methods. */
  scope: object;
  worker: RootWorkerContext;
  ownerHost: HostId;
  nodeId: string;
  jobId: string;
  leasePointIdValue: string;
  ownerId: string;
  version: number;
  fencingToken: number;
  state: "leased" | "accepted" | "released";
  acceptedProposalId: string | null;
  acceptedManifestHash: string | null;
  contentHash: string;
  processingPolicyId: string;
  coordinationPolicyHash: string;
  coordinationPolicyEpoch: number;
  privacyEpoch: number;
  expiresAt: string;
  leaseMs: number;
  jobDeadline: string | null;
  maxClockSkewMs: number;
}
export class LeaseAuthority {
  readonly #issuer: symbol;
  readonly #state: Readonly<LeaseAuthorityState>;
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(state: LeaseAuthorityState, issuer: symbol) {
    if (issuer !== LEASE_AUTHORITY_ISSUER) throw new TypeError("Lease authority requires the module issuer");
    this.#issuer = issuer;
    this.#state = Object.freeze({ ...state });
    Object.freeze(this);
  }
  /** Brand check: only genuine in-module authorities pass; forged prototypes and structural objects fail. */
  static isValid(value: unknown): value is LeaseAuthority {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof LeaseAuthority && value.#issuer === LEASE_AUTHORITY_ISSUER;
  }
  /** Exact store binding (object identity); never leaks the live store. */
  matchesStore(store: ProductionCoordinationStore): boolean { return this.#state.store === store; }
  /** Private per-store scope binding; only the owning store's lexical methods can mint or consume. */
  matchesScope(scope: object): boolean { return this.#state.scope === scope; }
  /** EXACT current-claim match: every identity/liveness field must equal the persisted claim. */
  matchesClaim(claim: LeaseRecord): boolean {
    return claim.recordType === "lease" && claim.id === this.#state.leasePointIdValue && claim.jobId === this.#state.jobId && claim.ownerHost === this.#state.ownerHost && claim.ownerId === this.#state.ownerId && claim.version === this.#state.version && claim.fencingToken === this.#state.fencingToken && claim.state === this.#state.state && claim.acceptedProposalId === this.#state.acceptedProposalId && claim.acceptedManifestHash === this.#state.acceptedManifestHash && claim.contentHash === this.#state.contentHash && claim.processingPolicyId === this.#state.processingPolicyId && claim.coordinationPolicyHash === this.#state.coordinationPolicyHash && claim.coordinationPolicyEpoch === this.#state.coordinationPolicyEpoch && claim.privacyEpoch === this.#state.privacyEpoch && claim.expiresAt === this.#state.expiresAt;
  }
  /** Trusted fresh clock: delegates to the private bound worker; validates every call. */
  now(): number { return this.#state.worker.now(); }
  get ownerHost(): HostId { return this.#state.ownerHost; }
  get nodeId(): string { return this.#state.nodeId; }
  get jobId(): string { return this.#state.jobId; }
  get ownerId(): string { return this.#state.ownerId; }
  get version(): number { return this.#state.version; }
  get fencingToken(): number { return this.#state.fencingToken; }
  get state(): "leased" | "accepted" | "released" { return this.#state.state; }
  get acceptedProposalId(): string | null { return this.#state.acceptedProposalId; }
  get acceptedManifestHash(): string | null { return this.#state.acceptedManifestHash; }
  get contentHash(): string { return this.#state.contentHash; }
  get processingPolicyId(): string { return this.#state.processingPolicyId; }
  get coordinationPolicyHash(): string { return this.#state.coordinationPolicyHash; }
  get coordinationPolicyEpoch(): number { return this.#state.coordinationPolicyEpoch; }
  get privacyEpoch(): number { return this.#state.privacyEpoch; }
  get expiresAt(): string { return this.#state.expiresAt; }
  get leaseMs(): number { return this.#state.leaseMs; }
  get jobDeadline(): string | null { return this.#state.jobDeadline; }
  get maxClockSkewMs(): number { return this.#state.maxClockSkewMs; }
}
Object.freeze(LeaseAuthority);
Object.freeze(LeaseAuthority.prototype);

interface LeaseClaim { jobId: string; ownerId: string; version: number; fencingToken: number; expiresAt: string; state: "leased" | "accepted" | "released" | "completed"; acceptedProposalId: string | null; acceptedManifestHash: string | null; }
function claimFrom(record: LeaseRecord): LeaseClaim {
  return { jobId: record.jobId, ownerId: record.ownerId, version: record.version, fencingToken: record.fencingToken, expiresAt: record.expiresAt, state: record.state, acceptedProposalId: record.acceptedProposalId, acceptedManifestHash: record.acceptedManifestHash };
}
function iso(ms: number): string { return new Date(ms).toISOString(); }
/** FULL exact claim equality (every field), never merely owner/version/fence/state. */
function claimEquals(left: LeaseRecord, right: LeaseRecord): boolean {
  return left.recordType === "lease" && left.id === right.id && left.jobId === right.jobId && left.ownerHost === right.ownerHost && left.ownerId === right.ownerId && left.version === right.version && left.fencingToken === right.fencingToken && left.state === right.state && left.acceptedProposalId === right.acceptedProposalId && left.acceptedManifestHash === right.acceptedManifestHash && left.contentHash === right.contentHash && left.processingPolicyId === right.processingPolicyId && left.coordinationPolicyHash === right.coordinationPolicyHash && left.coordinationPolicyEpoch === right.coordinationPolicyEpoch && left.privacyEpoch === right.privacyEpoch && left.expiresAt === right.expiresAt && left.createdAt === right.createdAt;
}
const MAX_TIME = Date.parse("2100-12-31T23:59:59.999Z");
function validLeaseClock(name: string, value: unknown, plusMs = 0): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) + plusMs > MAX_TIME) throw new TypeError(`${name} must be a bounded clock value`);
  return value as number;
}
function hashed(record: LeaseRecord): LeaseRecord { return { ...record, contentHash: canonicalRecordHash(record) } as LeaseRecord; }
interface LeaseCasInput {
  jobId: string;
  expectedVersion: number;
  expectedFencingToken: number;
  expectedPolicyEpoch: number;
  expectedPolicyHash: string;
  expectedPrivacyEpoch: number;
  expectedState: "leased" | "accepted" | "released" | "completed";
  expectedOwner: string;
  expectedAcceptedProposalId: string | null;
  expectedAcceptedManifestHash: string | null;
  expectedProcessingPolicyId: string;
  expectedCreatedAt: string;
  expectedContentHash: string;
  expiresBefore?: number;
  expiresAfter?: number;
  next: LeaseRecord;
}
function leaseCasInput(current: LeaseRecord, next: LeaseRecord, extra: { expiresBefore?: number; expiresAfter?: number } = {}): LeaseCasInput {
  return { jobId: current.jobId, expectedVersion: current.version, expectedFencingToken: current.fencingToken, expectedPolicyEpoch: current.coordinationPolicyEpoch, expectedPolicyHash: current.coordinationPolicyHash, expectedPrivacyEpoch: current.privacyEpoch, expectedState: current.state, expectedOwner: current.ownerId, expectedAcceptedProposalId: current.acceptedProposalId, expectedAcceptedManifestHash: current.acceptedManifestHash, expectedProcessingPolicyId: current.processingPolicyId, expectedCreatedAt: current.createdAt, expectedContentHash: current.contentHash, ...(extra.expiresBefore === undefined ? {} : { expiresBefore: extra.expiresBefore }), ...(extra.expiresAfter === undefined ? {} : { expiresAfter: extra.expiresAfter }), next };
}
const SECRET = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;
function boundedRedacted(name: string, value: unknown, max = 512): string {
  if (typeof value !== "string" || value.length === 0 || value.length > max || SECRET.test(value)) throw new TypeError(`${name} must be bounded and redacted`);
  return value;
}

/** Conservative lease expiry: skew may duplicate work but never authorizes stale publication. */
export function isLeaseExpired(lease: Pick<LeaseRecord, "expiresAt">, now: number, maxClockSkewMs: number): boolean {
  if (!Number.isFinite(now) || !Number.isFinite(maxClockSkewMs) || maxClockSkewMs < 0) throw new TypeError("Lease expiry inputs are invalid");
  return Date.parse(lease.expiresAt) + maxClockSkewMs <= now;
}

interface LeaseActorInput {
  ownerHost: HostId;
  jobId: string; ownerId: string; now: number; leaseMs: number; policyEpoch: number; policyHash: string; privacyEpoch: number; maxClockSkewMs: number;
}
function validateActorInput(input: LeaseActorInput): void {
  if (input.ownerHost !== "pi" && input.ownerHost !== "prime") throw new TypeError("Lease owner host is invalid");
  boundedRedacted("Lease jobId", input.jobId);
  boundedRedacted("Lease ownerId", input.ownerId);
  if (!Number.isSafeInteger(input.leaseMs) || input.leaseMs < 1 || input.leaseMs > 86_400_000 || !Number.isSafeInteger(input.policyEpoch) || input.policyEpoch < 0 || !Number.isSafeInteger(input.maxClockSkewMs) || input.maxClockSkewMs < 0 || input.maxClockSkewMs > 3_600_000 || !Number.isSafeInteger(input.privacyEpoch) || input.privacyEpoch < 0) throw new TypeError("Lease claim inputs are invalid");
  if (typeof input.policyHash !== "string" || input.policyHash.length === 0 || input.policyHash.length > 512) throw new TypeError("Lease policy hash is invalid");
  validLeaseClock("Lease clock", input.now, input.leaseMs);
}
/** Read the immutable job and verify host/privacy/coord hash+epoch; claims are bound to the job's intersection policy. */
async function readJobIdentity(store: CoordinationStore, input: Pick<LeaseActorInput, "jobId" | "policyEpoch" | "policyHash" | "privacyEpoch">): Promise<Awaited<ReturnType<CoordinationStore["readJob"]>> & { policyId: string } | null> {
  const job = await store.readJob(input.jobId);
  if (job === null) return null;
  if (job.id !== input.jobId || job.coordinationPolicyEpoch !== input.policyEpoch || job.coordinationPolicyHash !== input.policyHash || job.privacyEpoch !== input.privacyEpoch) return null;
  return { ...job, policyId: job.policyId };
}
/** Lease TTL never outlives a finite job deadline: expiresAt = min(now+leaseMs, job.expiresAt). */
function leaseExpiry(now: number, leaseMs: number, jobDeadline: string | null): string {
  if (jobDeadline === null) return iso(now + leaseMs);
  return iso(Math.min(now + leaseMs, Date.parse(jobDeadline)));
}
function freshLease(store: CoordinationStore, input: LeaseActorInput, control: Awaited<ReturnType<CoordinationStore["readControl"]>>, jobPolicyId: string, jobDeadline: string | null): LeaseRecord {
  const record: LeaseRecord = {
    ownerHost: input.ownerHost, schemaRevision: 1, createdAt: iso(input.now), privacyEpoch: control.privacyEpoch,
    processingPolicyId: jobPolicyId, expiresAt: leaseExpiry(input.now, input.leaseMs, jobDeadline), recordType: "lease",
    id: leasePointId(input.jobId), jobId: input.jobId, ownerId: input.ownerId, version: 1, fencingToken: 1, state: "leased",
    acceptedProposalId: null, acceptedManifestHash: null,
    coordinationPolicyHash: input.policyHash, coordinationPolicyEpoch: input.policyEpoch, contentHash: "pending",
  };
  return hashed(record);
}
function mintLeaseAuthority(target: ProductionCoordinationStore, scope: object, worker: RootWorkerContext, claim: LeaseRecord, leaseMs: number, jobDeadline: string | null, maxClockSkewMs: number): LeaseAuthority {
  if (claim.state === "completed") throw new TypeError("Completed jobs cannot mint lease authority");
  const authority = new LeaseAuthority({ store: target, scope, worker, ownerHost: worker.host, nodeId: worker.nodeId, jobId: claim.jobId, leasePointIdValue: claim.id, ownerId: claim.ownerId, version: claim.version, fencingToken: claim.fencingToken, state: claim.state, acceptedProposalId: claim.acceptedProposalId, acceptedManifestHash: claim.acceptedManifestHash, contentHash: claim.contentHash, processingPolicyId: claim.processingPolicyId, coordinationPolicyHash: claim.coordinationPolicyHash, coordinationPolicyEpoch: claim.coordinationPolicyEpoch, privacyEpoch: claim.privacyEpoch, expiresAt: claim.expiresAt, leaseMs, jobDeadline, maxClockSkewMs }, LEASE_AUTHORITY_ISSUER);
  AUTHORITY_WORKERS.set(authority, worker);
  return authority;
}
/** Post-CAS exact reread: FULL identity/content/deadline match with the intended transition (never merely owner/version/fence/state). */
function exactReread(store: CoordinationStore, jobId: string, next: LeaseRecord): Promise<LeaseRecord | null> {
  return (async () => {
    const reread = await store.readLease(jobId);
    if (reread === null || reread.recordType !== "lease" || reread.id !== next.id || reread.jobId !== next.jobId || reread.ownerHost !== next.ownerHost || reread.ownerId !== next.ownerId || reread.version !== next.version || reread.fencingToken !== next.fencingToken || reread.state !== next.state || reread.acceptedProposalId !== next.acceptedProposalId || reread.acceptedManifestHash !== next.acceptedManifestHash || reread.contentHash !== next.contentHash || reread.processingPolicyId !== next.processingPolicyId || reread.coordinationPolicyHash !== next.coordinationPolicyHash || reread.coordinationPolicyEpoch !== next.coordinationPolicyEpoch || reread.privacyEpoch !== next.privacyEpoch || reread.expiresAt !== next.expiresAt) return null;
    return reread;
  })();
}

/**
 * Claim a job lease with a genuine RootWorkerContext: the owner identity is
 * DERIVED from worker.nodeId (no caller-selected identity), the worker clock
 * is sampled FRESH after the slow reads, and a successful exact claim mints a
 * LeaseAuthority. A worker for node A can NEVER obtain authority for a live
 * node B by passing/reading B strings. Two expiry concepts stay separate:
 * conservative expiry (expiresAt + skew <= contenderNow) governs steal;
 * exact-owner active liveness is simply expiresAt > now.
 */
/** @internal Raw-protocol claim; the production class routes through its named safe method. */
/** @internal */
/** BLOCKER GUARD: the external claim input may ONLY carry the four allowed
 * fields. Any other enumerable own key (string or symbol) is rejected BEFORE
 * any field is read, so a smuggled ownerId/ownerHost/now/leaseMs/
 * maxClockSkewMs can never override the trusted worker identity/config.
 */
function assertLeaseClaimInputShape(input: ClaimLeaseInput): void {
  if (input === null || typeof input !== "object" || Array.isArray(input)) throw new TypeError("Lease claim inputs are invalid");
  for (const key of Object.keys(input)) {
    if (key !== "jobId" && key !== "policyEpoch" && key !== "policyHash" && key !== "privacyEpoch") throw new TypeError("Lease claim inputs contain an unknown key");
  }
  if (Object.getOwnPropertySymbols(input).length > 0) throw new TypeError("Lease claim inputs contain a symbol key");
}

async function claimLeaseOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, worker: RootWorkerContext, input: ClaimLeaseInput): Promise<LeaseAuthority | null> {
  if (!RootWorkerContext.isValid(worker)) throw new TypeError("Lease claim requires a verified root worker");
  // GLOBAL RULE + BLOCKER: NEVER spread ClaimLeaseInput. Reject unknown own
  // keys first, then snapshot the four allowed fields EXACTLY ONCE; every
  // later read uses ONLY the locals. The lease TTL and clock skew derive ONLY
  // from the genuine worker's bound configuration.
  assertLeaseClaimInputShape(input);
  const jobId = input.jobId;
  const policyEpoch = input.policyEpoch;
  const policyHash = input.policyHash;
  const privacyEpoch = input.privacyEpoch;
  const actorInput: LeaseActorInput = { ownerHost: worker.host, ownerId: worker.nodeId, now: 0, jobId, policyEpoch, policyHash, privacyEpoch, leaseMs: worker.leaseMs, maxClockSkewMs: worker.maxClockSkewMs };
  validateActorInput({ ...actorInput, now: 0 });
  let control;
  try { control = await protocol.readControl(); } catch { return null; }
  if (control.state !== "active" || control.coordinationPolicyEpoch !== policyEpoch || control.coordinationPolicyHash !== policyHash || control.privacyEpoch !== privacyEpoch) return null;
  const job = await readJobIdentity(protocol, { jobId, policyEpoch, policyHash, privacyEpoch });
  if (job === null || job.ownerHost !== worker.host) return null;
  // Read the CURRENT claim BEFORE the fresh clock sample; the sample used for
  // job expiry/steal/CAS decisions is taken AFTER the claim read.
  const current = await protocol.readLease(jobId);
  const now = validLeaseClock("Lease claim clock", worker.now(), worker.leaseMs);
  actorInput.now = now;
  if (jobExpired(job, now, worker.maxClockSkewMs)) return null;
  let claim: LeaseRecord | null = null;
  if (current === null) {
    try {
      const intended = freshLease(protocol, actorInput, control, job.policyId, job.expiresAt);
      await protocol.insertLease(intended);
      claim = await exactReread(protocol, jobId, intended);
      if (claim === null) return null;
    } catch {
      // Insert-race: reread the CURRENT claim and pass THAT to claimOrSteal (never the captured null).
      const raced = await protocol.readLease(jobId);
      claim = await claimOrSteal(protocol, worker, actorInput, job, raced);
    }
  } else {
    claim = await claimOrSteal(protocol, worker, actorInput, job, current);
  }
  if (claim === null) return null;
  // Before minting: EXACT claim reread + active control + FINAL fresh clock,
  // proving the lease live, the finite job live with the bound skew, the claim
  // within the job deadline and full identity. Never return authority crossed
  // during the awaits.
  const finalClaim = await protocol.readLease(jobId);
  if (finalClaim === null || !claimEquals(finalClaim, claim)) return null;
  const finalNow = validLeaseClock("Lease claim final clock", worker.now(), 0);
  if (Date.parse(finalClaim.expiresAt) <= finalNow) return null;
  if (jobExpired(job, finalNow, worker.maxClockSkewMs)) return null;
  if (job.expiresAt !== null && Date.parse(finalClaim.expiresAt) > Date.parse(job.expiresAt)) return null;
  let after;
  try { after = await protocol.readControl(); } catch { return null; }
  if (after.state !== "active" || after.coordinationPolicyEpoch !== policyEpoch || after.coordinationPolicyHash !== policyHash || after.privacyEpoch !== privacyEpoch) return null;
  return mintLeaseAuthority(target, scope, worker, finalClaim, worker.leaseMs, job.expiresAt, worker.maxClockSkewMs);
}

async function claimOrSteal(store: CoordinationStore, worker: RootWorkerContext, input: LeaseActorInput, job: { policyId: string; expiresAt: string | null }, current: LeaseRecord | null): Promise<LeaseRecord | null> {
  if (current === null || current.state === "completed") return null;
  if (current.ownerHost !== input.ownerHost || current.processingPolicyId !== job.policyId || current.coordinationPolicyEpoch !== input.policyEpoch || current.coordinationPolicyHash !== input.policyHash || current.privacyEpoch !== input.privacyEpoch) return null;
  if (jobExpired({ expiresAt: job.expiresAt }, input.now, input.maxClockSkewMs)) return null;
  const conservativeCut = input.now - input.maxClockSkewMs;
  if (current.ownerId === input.ownerId && current.state !== "released") {
    // Exact-owner ACTIVE liveness: expiresAt strictly in the future; a
    // preexisting claim that outlives a finite job is never returned.
    if (Date.parse(current.expiresAt) > input.now) {
      if (job.expiresAt !== null && Date.parse(current.expiresAt) > Date.parse(job.expiresAt)) return null;
      return current;
    }
    if (!isLeaseExpired(current, input.now, input.maxClockSkewMs)) return null;
    const refreshed = hashed({ ...current, version: current.version + 1, fencingToken: current.fencingToken + 1, expiresAt: leaseExpiry(input.now, input.leaseMs, job.expiresAt) });
    const won = await store.casLease(leaseCasInput(current, refreshed, { expiresBefore: conservativeCut }));
    return won ? await exactReread(store, input.jobId, refreshed) : null;
  }
  if (current.state !== "released" && !isLeaseExpired(current, input.now, input.maxClockSkewMs)) return null;
  const nextState: "leased" | "accepted" = current.state === "accepted" || (current.state === "released" && current.acceptedProposalId !== null) ? "accepted" : "leased";
  const next = hashed({ ...current, ownerId: input.ownerId, version: current.version + 1, fencingToken: current.fencingToken + 1, expiresAt: leaseExpiry(input.now, input.leaseMs, job.expiresAt), state: nextState });
  const won = await store.casLease(leaseCasInput(current, next, current.state === "released" ? {} : { expiresBefore: conservativeCut }));
  if (won) return await exactReread(store, input.jobId, next);
  return null;
}

/** Renew an owned live claim using the genuine authority; returns a NEW authority (the old one is stale by version/content hash). */
/** @internal Raw-protocol renewal; the production class routes through its named safe method. */
/** @internal */
async function renewLeaseOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority): Promise<LeaseAuthority | null> {
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Lease renewal requires a genuine lease authority");
  if (!authority.matchesStore(target)) throw new TypeError("Lease authority does not match the store");
  if (!authority.matchesScope(scope)) throw new TypeError("Lease authority does not match the store authority scope");
  const input = { jobId: authority.jobId, policyEpoch: authority.coordinationPolicyEpoch, policyHash: authority.coordinationPolicyHash, privacyEpoch: authority.privacyEpoch, maxClockSkewMs: authority.maxClockSkewMs };
  let control;
  try { control = await protocol.readControl(); } catch { return null; }
  if (control.state !== "active" || control.coordinationPolicyEpoch !== input.policyEpoch || control.coordinationPolicyHash !== input.policyHash || control.privacyEpoch !== input.privacyEpoch) return null;
  const job = await readJobIdentity(protocol, input);
  if (job === null || job.ownerHost !== authority.ownerHost) return null;
  const current = await protocol.readLease(input.jobId);
  if (current === null || !authority.matchesClaim(current)) return null;
  // Fresh worker clock after the slow reads: exact-owner liveness + job deadline
  // with the BOUND skew.
  const now = validLeaseClock("Lease renewal clock", authority.now(), authority.leaseMs);
  if (Date.parse(current.expiresAt) <= now) return null;
  if (jobExpired(job, now, authority.maxClockSkewMs)) return null;
  const next = hashed({ ...current, version: current.version + 1, expiresAt: leaseExpiry(now, authority.leaseMs, job.expiresAt) });
  const won = await protocol.casLease(leaseCasInput(current, next, { expiresAfter: now }));
  if (!won) return null;
  const reread = await exactReread(protocol, input.jobId, next);
  if (reread === null) return null;
  let after;
  try { after = await protocol.readControl(); } catch { return null; }
  if (after.state !== "active" || after.coordinationPolicyEpoch !== input.policyEpoch || after.coordinationPolicyHash !== input.policyHash || after.privacyEpoch !== input.privacyEpoch) return null;
  // EXACT final claim reread FIRST (the last awaited operation), THEN the
  // FINAL fresh clock — the clock is sampled strictly after the last await.
  const finalClaim = await protocol.readLease(input.jobId);
  if (finalClaim === null || !claimEquals(finalClaim, reread)) return null;
  const finalNow = validLeaseClock("Lease renewal final clock", authority.now(), 0);
  if (Date.parse(reread.expiresAt) <= finalNow) return null;
  if (jobExpired(job, finalNow, authority.maxClockSkewMs)) return null;
  return mintLeaseAuthority(target, scope, authorityWorker(authority), finalClaim, authority.leaseMs, job.expiresAt, authority.maxClockSkewMs);
}
/** Module-private worker binding (a lease capability NEVER leaks the master RootWorkerContext). */
const AUTHORITY_WORKERS = new WeakMap<LeaseAuthority, RootWorkerContext>();
function authorityWorker(authority: LeaseAuthority): RootWorkerContext {
  const worker = AUTHORITY_WORKERS.get(authority);
  if (worker === undefined) throw new TypeError("Lease authority worker binding is missing");
  return worker;
}

/** Release a genuinely live owned claim; consumes the authority (no successor). Locally expired or skew-grace claims can NEVER release. */
/** @internal Raw-protocol release; the production class routes through its named safe method. */
/** @internal */
async function releaseLeaseOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority): Promise<boolean> {
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Lease release requires a genuine lease authority");
  if (!authority.matchesStore(target)) throw new TypeError("Lease authority does not match the store");
  if (!authority.matchesScope(scope)) throw new TypeError("Lease authority does not match the store authority scope");
  const input = { jobId: authority.jobId, policyEpoch: authority.coordinationPolicyEpoch, policyHash: authority.coordinationPolicyHash, privacyEpoch: authority.privacyEpoch, maxClockSkewMs: authority.maxClockSkewMs };
  let control;
  try { control = await protocol.readControl(); } catch { return false; }
  // Safe release must remain possible during POLICY DRAIN (otherwise quiescence
  // always waits lease+skew expiry): accept `draining` ONLY when the authority's
  // coord hash+epoch/privacy exactly match that draining control. Retired or
  // changed epoch/hash/privacy is rejected; claim/renew/write/accept/
  // materialize still require active.
  const controlMatches = control.coordinationPolicyEpoch === input.policyEpoch && control.coordinationPolicyHash === input.policyHash && control.privacyEpoch === input.privacyEpoch;
  if (control.state !== "active" && control.state !== "draining") return false;
  if (!controlMatches) return false;
  const job = await readJobIdentity(protocol, input);
  if (job === null || job.ownerHost !== authority.ownerHost) return false;
  const current = await protocol.readLease(input.jobId);
  if (current === null || !authority.matchesClaim(current)) return false;
  const now = validLeaseClock("Lease release clock", authority.now(), 0);
  if (Date.parse(current.expiresAt) <= now) return false;
  if (jobExpired(job, now, authority.maxClockSkewMs)) return false;
  const next = hashed({ ...current, version: current.version + 1, state: "released" });
  const won = await protocol.casLease(leaseCasInput(current, next, { expiresAfter: now }));
  if (!won) return false;
  // Exact FULL successor reread (not state-only).
  return (await exactReread(protocol, input.jobId, next)) !== null;
}

/**
 * MODULE-PRIVATE one-step rotation: requires the genuine prior authority bound
 * to the same store, exact-matches the current claim, performs the typed CAS
 * with a FRESH worker-clock expiry floor and mints a NEW authority ONLY on the
 * exact full reread of the intended transition. There is NO exported rotation
 * and no public mint; the only public acceptance entry is the complete safe
 * `acceptLeaseAuthority` below.
 */
async function rotateLeaseAuthority(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, next: LeaseRecord): Promise<LeaseAuthority | null> {
  if (!LeaseAuthority.isValid(authority)) return null;
  if (!authority.matchesStore(target)) return null;
  if (!authority.matchesScope(scope)) return null;
  const current = await protocol.readLease(authority.jobId);
  if (current === null || !authority.matchesClaim(current)) return null;
  // Fresh worker clock immediately before the CAS floor.
  const now = validLeaseClock("Lease rotation clock", authority.now(), 0);
  if (Date.parse(current.expiresAt) <= now) return null;
  if (jobExpired({ expiresAt: authority.jobDeadline }, now, authority.maxClockSkewMs)) return null;
  const won = await protocol.casLease(leaseCasInput(current, next, { expiresAfter: now }));
  if (!won) return null;
  const reread = await exactReread(protocol, authority.jobId, next);
  if (reread === null) return null;
  return mintLeaseAuthority(target, scope, authorityWorker(authority), reread, authority.leaseMs, authority.jobDeadline, authority.maxClockSkewMs);
}

/**
 * The ONE safe acceptance operation (name: acceptLeaseAuthority). Everything
 * acceptProposal could require is validated HERE, so calling this function
 * directly can never bypass a job/proposal/tombstone/control invariant:
 * nominal store-bound LEASED authority; exact active control/job/current
 * claim; finite deadline + lease freshness with the BOUND skew; exact
 * persisted proposal (original ownerId/fence/job/membership/policy/privacy/
 * deadline); bounded membership tombstones; fresh clock after every await;
 * the ONLY allowed leased->accepted transition via the private rotation;
 * post-CAS exact claim + tombstones + active control + fresh liveness.
 * Returns the NEW accepted authority or null (never a bare boolean).
 */
/** @internal Raw-protocol acceptance (the ONE safe acceptance operation); the production class routes through its named safe method. */
/** @internal */
async function acceptLeaseAuthorityOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, proposalId: string): Promise<LeaseAuthority | null> {
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Accept proposal requires a genuine lease authority");
  if (!authority.matchesStore(target)) throw new TypeError("Accept proposal authority does not match the store");
  if (!authority.matchesScope(scope)) throw new TypeError("Accept proposal authority does not match the store authority scope");
  if (authority.state !== "leased") return null;
  if (typeof proposalId !== "string" || proposalId.length === 0 || proposalId.length > 512) throw new TypeError("Accept proposal inputs are invalid");
  const input = { policyEpoch: authority.coordinationPolicyEpoch, policyHash: authority.coordinationPolicyHash, privacyEpoch: authority.privacyEpoch, maxClockSkewMs: authority.maxClockSkewMs, jobId: authority.jobId };
  const workerNow = (): number => authority.now();
  const control = await protocol.readControl();
  if (authority.ownerHost !== control.ownerHost) throw new TypeError("Accept proposal authority host does not match the collection owner");
  if (control.state !== "active" || control.coordinationPolicyEpoch !== input.policyEpoch || control.coordinationPolicyHash !== input.policyHash || control.privacyEpoch !== input.privacyEpoch) throw new TypeError("Accept proposal failed: control policy does not match");
  const job = await protocol.readJob(input.jobId);
  if (job === null || job.id !== input.jobId) throw new TypeError("Accept proposal failed: job is missing");
  if (job.policyEpoch !== input.policyEpoch || job.policyHash !== input.policyHash || job.privacyEpoch !== input.privacyEpoch) throw new TypeError("Accept proposal failed: job policy does not match");
  const claim = await protocol.readLease(input.jobId);
  if (claim === null || !authority.matchesClaim(claim) || claim.state !== "leased") throw new TypeError("Accept proposal failed: stale lease authority");
  const claimNow = workerNow();
  if (Date.parse(claim.expiresAt) <= claimNow) throw new TypeError("Accept proposal failed: lease is expired");
  if (jobExpired(job, claimNow, input.maxClockSkewMs)) throw new TypeError("Accept proposal failed: job is expired");
  if (!claimIdentityMatchesJob(claim, job)) throw new TypeError("Accept proposal failed: claim does not match the job policy");
  const proposal = await protocol.readProposal(proposalId);
  if (proposal === null || proposal.id !== proposalId || proposal.jobId !== authority.jobId || proposal.ownerId !== authority.ownerId || proposal.fencingToken !== authority.fencingToken || proposal.coordinationPolicyEpoch !== input.policyEpoch || proposal.coordinationPolicyHash !== input.policyHash || proposal.privacyEpoch !== input.privacyEpoch || proposal.expiresAt !== job.expiresAt || proposal.membership.length !== job.membership.length || proposal.membership.some((id, index) => id !== job.membership[index]) || proposal.ownerHost !== control.ownerHost || proposal.processingPolicyId !== job.policyId) throw new TypeError("Accept proposal failed: proposal is not bound to the current authority, fencing, membership and policy");
  const tombstones = await protocol.readTombstones(job.membership);
  if (tombstones.length > 0) throw new TypeError("Accept proposal failed: job membership is tombstoned");
  // Fresh clock sampled immediately AFTER the slow tombstone read.
  const freshNow = workerNow();
  if (Date.parse(claim.expiresAt) <= freshNow) throw new TypeError("Accept proposal failed: lease is expired");
  if (jobExpired(job, freshNow, input.maxClockSkewMs)) throw new TypeError("Accept proposal failed: job is expired");
  const next = { ...claim, version: claim.version + 1, state: "accepted" as const, acceptedProposalId: proposal.id, acceptedManifestHash: proposal.manifestHash, contentHash: "pending" };
  const finalNext = { ...next, contentHash: canonicalRecordHash(next) } as typeof next;
  const accepted = await rotateLeaseAuthority(protocol, target, scope, authority, finalNext);
  if (accepted === null) return null;
  // Post-CAS authority checks: revalidate tombstones + active control, EXACT-
  // reread the accepted claim matching the NEW authority, then take a FINAL
  // fresh clock and validate lease/job/control. A capability stolen or
  // released during the post-CAS barrier is never returned.
  const tombstonesAfter = await protocol.readTombstones(job.membership);
  if (tombstonesAfter.length > 0) return null;
  const finalJob = await protocol.readJob(input.jobId);
  const finalProposal = await protocol.readProposal(proposal.id);
  const finalTombstones = await protocol.readTombstones(job.membership);
  if (finalJob === null || finalJob.contentHash !== job.contentHash || finalProposal === null || finalProposal.contentHash !== proposal.contentHash || finalTombstones.length > 0) return null;
  const after = await protocol.readControl();
  if (after.state !== "active" || after.coordinationPolicyEpoch !== input.policyEpoch || after.coordinationPolicyHash !== input.policyHash || after.privacyEpoch !== input.privacyEpoch) return null;
  const acceptedClaim = await protocol.readLease(input.jobId);
  if (acceptedClaim === null || !accepted.matchesClaim(acceptedClaim)) return null;
  let finalNow: number;
  try { finalNow = workerNow(); } catch { return null; }
  if (Date.parse(acceptedClaim.expiresAt) <= finalNow) return null;
  if (jobExpired(finalJob, finalNow, input.maxClockSkewMs)) return null;
  return accepted;
}

function claimIdentityMatchesJob(claim: LeaseRecord, job: { id: string; ownerHost: HostId; policyId: string; coordinationPolicyEpoch: number; coordinationPolicyHash: string; privacyEpoch: number; expiresAt: string | null }): boolean {
  if (job.expiresAt !== null && Date.parse(claim.expiresAt) > Date.parse(job.expiresAt)) return false;
  return claim.ownerHost === job.ownerHost && claim.processingPolicyId === job.policyId && claim.coordinationPolicyEpoch === job.coordinationPolicyEpoch && claim.coordinationPolicyHash === job.coordinationPolicyHash && claim.privacyEpoch === job.privacyEpoch && claim.jobId === job.id && claim.id === leasePointId(job.id);
}

/** Raw claim state (including released) for diagnostics and quiescence checks. */


// ---------------------------------------------------------------------------
// Authority kernel: jobs (lexical; NOT exported)
// ---------------------------------------------------------------------------
export function validateSortedMembership(membership: readonly string[]): void {
  if (!Array.isArray(membership) || membership.length === 0) throw new TypeError("Job membership must be non-empty");
  for (let index = 1; index < membership.length; index += 1) {
    if (membership[index - 1]! >= membership[index]!) throw new TypeError("Job membership must be strictly sorted and unique");
  }
}
export function jobIdFor(input: Pick<CreateJobInput, "ownerHost" | "membership" | "policyHash" | "policyEpoch" | "extractorRevision" | "policyIntersectionId" | "privacyEpoch">): string {
  // GLOBAL RULE: snapshot every accessor-bearing field EXACTLY ONCE; the
  // membership is normalized to an owned dense frozen copy.
  const ownerHost = input.ownerHost;
  const membership = ownedMembershipSnapshot(input.membership);
  const policyHash = input.policyHash;
  const policyEpoch = input.policyEpoch;
  const extractorRevision = input.extractorRevision;
  const policyIntersectionId = input.policyIntersectionId;
  const privacyEpoch = input.privacyEpoch;
  validateSortedMembership(membership);
  return jobId(ownerHost, membership, policyHash, extractorRevision, policyEpoch, policyIntersectionId, privacyEpoch);
}
/** Immutable explicit-membership job point; identity is enforced by the record parser. */


/** @internal Raw-protocol job creation; the production class routes through its named safe method. */
/** @internal */
async function createJobOnProtocol(protocol: CoordinationStore, input: CreateJobInput): Promise<JobRecord> {
  // OWNED SNAPSHOTS at entry: every field exactly once; the membership is a
  // dense-once frozen copy; the job ID and the record are computed from the
  // locals only (never jobIdFor(input) on the original).
  const ownerHost = input.ownerHost;
  const membership = ownedMembershipSnapshot(input.membership);
  const policyIntersectionId = input.policyIntersectionId;
  const policyHash = input.policyHash;
  const policyEpoch = input.policyEpoch;
  const extractorRevision = input.extractorRevision;
  const privacyEpoch = input.privacyEpoch;
  const createdAt = input.createdAt;
  const expiresAt = input.expiresAt;
  if (protocol.ownerHost !== ownerHost) throw new TypeError("Job store owner does not match the job owner");
  validateSortedMembership(membership);
  const normalizedInput: CreateJobInput = { ownerHost, membership, policyIntersectionId, policyHash, policyEpoch, extractorRevision, privacyEpoch, createdAt, expiresAt };
  const id = jobIdFor(normalizedInput);
  const record: JobRecord = {
    ownerHost, schemaRevision: 1, createdAt, privacyEpoch,
    processingPolicyId: policyIntersectionId, expiresAt: expiresAt ?? null, recordType: "job", id,
    policyId: policyIntersectionId, policyHash, policyEpoch,
    membership: [...membership], extractorRevision,
    coordinationPolicyHash: policyHash, coordinationPolicyEpoch: policyEpoch, contentHash: "pending",
  };
  const final = { ...record, contentHash: canonicalRecordHash(record) } as JobRecord;
  await protocol.insertJob(final);
  const reread = await protocol.readJob(id);
  if (reread === null || reread.id !== id) throw new TypeError("Job insert did not read back");
  return reread;
}
/** Content-addressed proposal hash: binds owner/job/membership/output/epochs/hash/fence/privacy. */
export function proposalHashFor(input: { ownerHost: HostId; jobId: string; ownerId: string; membership: readonly string[]; content: ProposalContent; policyHash: string; policyEpoch: number; fencingToken: number; privacyEpoch: number; policyIntersectionId: string }): string {
  // GLOBAL RULE: snapshot every field EXACTLY ONCE (owned dense membership +
  // canonical content clone); the hash covers ONLY the snapshots.
  const ownerHost = input.ownerHost;
  const jobIdValue = input.jobId;
  const ownerId = input.ownerId;
  const membership = ownedMembershipSnapshot(input.membership);
  const content = ownedCanonicalContentSnapshot(input.content) as ProposalContent;
  const policyHash = input.policyHash;
  const policyEpoch = input.policyEpoch;
  const fencingToken = input.fencingToken;
  const privacyEpoch = input.privacyEpoch;
  const policyIntersectionId = input.policyIntersectionId;
  return proposalContentHash({ ownerHost, jobId: jobIdValue, ownerId, membership, content, policyHash, policyEpoch, fencingToken, privacyEpoch, policyIntersectionId });
}
/** @internal Raw-protocol terminal job completion; all derived effects are
 * authoritatively derived from the accepted proposal and job membership. */
async function completeRaptorJobOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, input: { generationId: string; evidenceIds: readonly string[]; destinationIds: readonly string[] }): Promise<boolean> {
  if (!LeaseAuthority.isValid(authority) || !authority.matchesStore(target) || !authority.matchesScope(scope) || authority.state !== "leased") throw new TypeError("RAPTOR completion requires a genuine leased authority");
  if (input === null || typeof input !== "object" || !validBoundedText(input.generationId) || !Array.isArray(input.evidenceIds) || input.evidenceIds.length === 0 || input.evidenceIds.length > 65_536 || input.evidenceIds.some(id => !validBoundedText(id)) || new Set(input.evidenceIds).size !== input.evidenceIds.length || !Array.isArray(input.destinationIds) || input.destinationIds.length < 2 || input.destinationIds.length > 3 || input.destinationIds.some(id => !validBoundedText(id, 256)) || new Set(input.destinationIds).size !== input.destinationIds.length) throw new TypeError("RAPTOR completion proof is invalid");
  const job = await protocol.readJob(authority.jobId); const claim = await protocol.readLease(authority.jobId); const control = await protocol.readControl();
  if (job === null || claim === null || !authority.matchesClaim(claim) || claim.state !== "leased" || job.ownerHost !== target.ownerHost || job.policyId !== authority.processingPolicyId || job.policyHash !== authority.coordinationPolicyHash || job.policyEpoch !== authority.coordinationPolicyEpoch || job.privacyEpoch !== authority.privacyEpoch || job.membership.length !== input.evidenceIds.length || job.membership.some((id, index) => id !== input.evidenceIds[index]) || control.state !== "active" || control.activeGeneration !== input.generationId || control.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || control.coordinationPolicyHash !== authority.coordinationPolicyHash || control.privacyEpoch !== authority.privacyEpoch || input.destinationIds.some(id => control.revokedDestinationIds.includes(id))) return false;
  const now = authority.now(); if (jobExpired(job, now, authority.maxClockSkewMs) || Date.parse(claim.expiresAt) <= now) return false;
  // The barrier read is the authoritative pre-CAS evidence check; the lease
  // CAS below necessarily changes the digest, so only the exact lease
  // readback can prove terminal completion here.
  await target.readRaptorBarrier(authority, { destinationIds: input.destinationIds, evidenceIds: input.evidenceIds });
  const finalControl = await protocol.readControl(); const finalClaim = await protocol.readLease(authority.jobId); const finalNow = authority.now();
  if (finalControl.activeGeneration !== input.generationId || finalControl.coordinationPolicyEpoch !== control.coordinationPolicyEpoch || finalControl.coordinationPolicyHash !== control.coordinationPolicyHash || finalControl.privacyEpoch !== control.privacyEpoch || finalClaim === null || !authority.matchesClaim(finalClaim) || finalClaim.state !== "leased" || Date.parse(finalClaim.expiresAt) <= finalNow || jobExpired(job, finalNow, authority.maxClockSkewMs)) return false;
  const next = hashed({ ...finalClaim, version: finalClaim.version + 1, state: "completed", terminalOperation: "raptor" });
  const won = await protocol.casLease({ jobId: job.id, expectedVersion: finalClaim.version, expectedFencingToken: finalClaim.fencingToken, expectedPolicyEpoch: finalClaim.coordinationPolicyEpoch, expectedPolicyHash: finalClaim.coordinationPolicyHash, expectedPrivacyEpoch: finalClaim.privacyEpoch, expectedState: "leased", expectedOwner: finalClaim.ownerId, expectedAcceptedProposalId: null, expectedAcceptedManifestHash: null, expectedProcessingPolicyId: finalClaim.processingPolicyId, expectedCreatedAt: finalClaim.createdAt, expectedContentHash: finalClaim.contentHash, expiresAfter: finalNow, next });
  if (!won) return false;
  const reread = await protocol.readLease(job.id); return reread !== null && canonicalStringify(reread) === canonicalStringify(next);
}
/** @internal Raw-protocol terminal job completion; all derived effects are
 * authoritatively derived from the accepted proposal and job membership. */
async function completeJobOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority): Promise<boolean> {
  const no = (_reason: string): false => false;
  if (!LeaseAuthority.isValid(authority) || !authority.matchesStore(target) || !authority.matchesScope(scope) || authority.state !== "accepted") throw new TypeError("Job completion requires a genuine accepted lease authority");
  const job = await protocol.readJob(authority.jobId);
  const initialClaim = await protocol.readLease(authority.jobId);
  if (job === null || initialClaim === null || !authority.matchesClaim(initialClaim) || initialClaim.state !== "accepted" || initialClaim.acceptedProposalId === null || initialClaim.acceptedManifestHash === null) return no("check-2");
  const now = authority.now();
  if (jobExpired(job, now, authority.maxClockSkewMs) || Date.parse(initialClaim.expiresAt) <= now || job.id !== authority.jobId || job.policyId !== authority.processingPolicyId || job.expiresAt !== null && Date.parse(initialClaim.expiresAt) > Date.parse(job.expiresAt)) return no("check-3");
  const proposal = await protocol.readProposal(initialClaim.acceptedProposalId);
  if (proposal === null || proposal.id !== initialClaim.acceptedProposalId || proposal.jobId !== job.id || proposal.ownerHost !== job.ownerHost || proposal.processingPolicyId !== job.policyId || proposal.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || proposal.coordinationPolicyHash !== job.coordinationPolicyHash || proposal.privacyEpoch !== job.privacyEpoch || proposal.manifestHash !== initialClaim.acceptedManifestHash || proposal.expiresAt !== job.expiresAt || proposal.membership.length !== job.membership.length || proposal.membership.some((id, index) => id !== job.membership[index])) return no("check-4");
  const initialControl = await protocol.readControl();
  const initialTombstones = await protocol.readTombstones(job.membership);
  if (initialControl.ownerHost !== target.ownerHost || initialControl.state !== "active" || initialControl.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || initialControl.coordinationPolicyHash !== authority.coordinationPolicyHash || initialControl.privacyEpoch !== authority.privacyEpoch || initialTombstones.length > 0) return no("check-4-barrier");
  const episodes = await protocol.readEpisodes(job.membership, job.privacyEpoch);
  if (episodes.length !== job.membership.length) return no("check-5");
  const episodeMap = new Map(episodes.map((episode) => [episode.id, episode]));
  if (job.membership.length === 0 || episodeMap.get(job.membership[0]!)?.createdAt !== job.createdAt) return no("check-5-created-at");
  let result: CurationResult;
  try {
    const proposalEnvelope = parseCurationProposalEnvelope(proposal.content);
    if (proposalEnvelope === null || !provenanceMatches(proposalEnvelope.provenance, { host: target.ownerHost, policyId: job.policyId, policyHash: job.policyId, policyEpoch: job.coordinationPolicyEpoch, promptRevision: CURATION_PROMPT_REVISION }) || proposal.createdAt !== proposalEnvelope.provenance.invokedAt) return no("check-6");
    result = assertPersistableCurationResult(validateCurationResult({ items: proposalEnvelope.items }, { directUserEpisodeIds: new Set(episodes.filter((episode) => episode.eventKind === "user").map((episode) => episode.id)), knownEpisodeIds: new Set(episodes.map((episode) => episode.id)) }));
  } catch { return no("check-6"); }
  const projections = result.items.map((item) => sharedProjectCurationItem(target.ownerHost, job.coordinationPolicyHash, job.coordinationPolicyEpoch, item, episodeMap));
  if (new Set(projections.map((projection) => projection.observationId)).size !== projections.length) return no("check-7");
  const sameEnvelope = (record: { ownerHost: HostId; processingPolicyId: string; coordinationPolicyEpoch: number; coordinationPolicyHash: string; privacyEpoch: number; expiresAt: string | null; contentHash: string }): boolean => record.ownerHost === target.ownerHost && record.processingPolicyId === job.policyId && record.coordinationPolicyEpoch === job.coordinationPolicyEpoch && record.coordinationPolicyHash === job.coordinationPolicyHash && record.privacyEpoch === job.privacyEpoch && record.expiresAt === job.expiresAt && record.contentHash === canonicalRecordHash(record as never);
  const memberSet = new Set(job.membership);
  const observations = new Map<string, CuratedMemoryRecord>();
  const links = new Map<string, EvidenceLinkRecord>();
  const currents = new Map<string, CuratedCurrentRecord>();
  const conflicts = new Map<string, ConflictManifestRecord>();
  const derivedTargets: string[] = [];
  for (let projectionIndex = 0; projectionIndex < projections.length; projectionIndex += 1) {
    const projection = projections[projectionIndex]!;
    const item = result.items[projectionIndex]!;
    const record = await protocol.readCurated("curated_memory", projection.observationId);
    if (record === null || record.recordType !== "curated_memory" || record.id !== projection.observationId || !sameEnvelope(record) || record.createdAt !== projection.primary.eventAt || record.contentId !== projection.contentId || record.observationId !== projection.observationId || record.stateKey !== projection.stateKey || record.eventAt !== projection.primary.eventAt || record.effectiveAt !== projection.primary.eventAt || record.primaryEvidenceEpisodeId !== projection.primary.id || canonicalStringify(record.effectiveOrder) !== canonicalStringify(projection.effectiveOrder) || record.category !== item.category || record.scope !== item.scope || record.projectId !== projection.projectId || record.subject !== item.subject || record.predicate !== item.predicate || record.text !== projection.text || canonicalStringify(record.sourceEpisodeIds ?? []) !== canonicalStringify(projection.evidence.map((episode) => episode.id).sort()) || canonicalStringify(record.provenance ?? []) !== canonicalStringify(projection.evidence.map((episode) => episode.id).sort()) || !Array.isArray(record.vector) || record.vector.length !== 1024 || !record.vector.every((value) => typeof value === "number" && Number.isFinite(value))) return no("observation");
    if (Object.prototype.hasOwnProperty.call(record, "value") !== (item.value !== undefined) || item.value !== undefined && canonicalStringify(record.value) !== canonicalStringify(item.value) || record.confidence !== item.confidence) return no("observation-fields");
    observations.set(record.id, record);
    derivedTargets.push(record.id, record.contentId, projection.stateKey);
    for (const episode of projection.evidence) {
      const linkId = evidenceLinkId(record.id, episode.id, job.extractorRevision);
      const link = await protocol.readCurated("evidence_link", linkId);
      if (link === null || link.recordType !== "evidence_link" || link.id !== linkId || !sameEnvelope(link) || link.createdAt !== record.createdAt || link.sourceId !== record.id || link.targetId !== episode.id || link.extractorRevision !== job.extractorRevision) return no("evidence-link");
      if (link.jobId !== job.id) {
        const prior = await protocol.readJob(link.jobId);
        const priorLease = prior === null ? null : await protocol.readLease(prior.id);
        if (prior === null || priorLease === null || (priorLease.state !== "accepted" && priorLease.state !== "released" && priorLease.state !== "completed") || priorLease.acceptedProposalId === null || priorLease.acceptedManifestHash === null || !claimIdentityMatchesJob(priorLease, prior) || prior.ownerHost !== target.ownerHost || prior.policyId !== job.policyId || prior.policyHash !== job.policyHash || prior.policyEpoch !== job.policyEpoch || prior.privacyEpoch !== job.privacyEpoch || prior.extractorRevision !== job.extractorRevision || prior.expiresAt !== job.expiresAt || !prior.membership.includes(link.targetId)) return no("evidence-prior");
      }
      links.set(link.id, link);
      derivedTargets.push(link.sourceId, link.targetId);
    }
    const current = await protocol.readCurated("curated_current", projection.currentId);
    if (current === null || current.recordType !== "curated_current" || current.id !== projection.currentId || current.ownerHost !== target.ownerHost || current.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || current.coordinationPolicyHash !== job.coordinationPolicyHash || current.privacyEpoch !== job.privacyEpoch || current.stateKey !== projection.stateKey || current.scope !== projection.scope || current.projectId !== projection.projectId || (current.sourceEpisodeIds ?? []).length === 0 || current.contentHash !== canonicalRecordHash(current)) return no("current-envelope");
    derivedTargets.push(current.stateKey);
    if (current.resolution === "resolved") {
      derivedTargets.push(current.contentId, current.observationId);
      if (!current.contentId || !current.observationId || !Array.isArray(current.vector) || current.vector.length !== 1024 || !current.vector.every((value) => typeof value === "number" && Number.isFinite(value))) return no("current-vector");
      const pointedValue = await protocol.readCurated("curated_memory", current.observationId);
      if (pointedValue === null || pointedValue.recordType !== "curated_memory") return no("current-observation");
      const pointed = pointedValue as CuratedMemoryRecord;
      if (pointed.ownerHost !== target.ownerHost || pointed.processingPolicyId !== current.processingPolicyId || pointed.expiresAt !== current.expiresAt || current.createdAt !== pointed.eventAt || pointed.contentHash !== canonicalRecordHash(pointed) || pointed.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || pointed.coordinationPolicyHash !== job.coordinationPolicyHash || pointed.privacyEpoch !== job.privacyEpoch || pointed.stateKey !== projection.stateKey || pointed.scope !== current.scope || pointed.projectId !== current.projectId || pointed.contentId !== current.contentId || current.effectiveOrder === undefined || canonicalStringify(current.effectiveOrder) !== canonicalStringify(pointed.effectiveOrder) || current.text !== pointed.text || canonicalStringify(current.sourceEpisodeIds ?? []) !== canonicalStringify(pointed.sourceEpisodeIds ?? []) || pointed.provenance === undefined || canonicalStringify(pointed.provenance) !== canonicalStringify(pointed.sourceEpisodeIds ?? []) || current.createdAt !== pointed.eventAt || !Array.isArray(pointed.vector) || pointed.vector.length !== 1024 || pointed.vector.some((value, index) => current.vector![index] !== value)) return no("current-pointed");
      for (const episodeId of pointed.sourceEpisodeIds ?? []) {
        const linkId = evidenceLinkId(pointed.id, episodeId, job.extractorRevision);
        const linkValue = await protocol.readCurated("evidence_link", linkId);
        if (linkValue === null || linkValue.recordType !== "evidence_link") return no("current-pointed-link");
        const link = linkValue as EvidenceLinkRecord;
        if (link.id !== linkId || link.ownerHost !== target.ownerHost || link.processingPolicyId !== pointed.processingPolicyId || link.expiresAt !== pointed.expiresAt || link.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || link.coordinationPolicyHash !== job.coordinationPolicyHash || link.privacyEpoch !== job.privacyEpoch || link.createdAt !== pointed.createdAt || link.sourceId !== pointed.id || link.targetId !== episodeId || link.extractorRevision !== job.extractorRevision || link.contentHash !== canonicalRecordHash(link)) return no("current-pointed-link-envelope");
        const linkJob = link.jobId === job.id ? job : await protocol.readJob(link.jobId);
        const linkLease = linkJob === null ? null : link.jobId === job.id ? initialClaim : await protocol.readLease(linkJob.id);
        if (linkJob === null || linkLease === null || (linkLease.state !== "accepted" && linkLease.state !== "released" && linkLease.state !== "completed") || linkLease.acceptedProposalId === null || linkLease.acceptedManifestHash === null || !claimIdentityMatchesJob(linkLease, linkJob) || linkJob.ownerHost !== target.ownerHost || linkJob.policyId !== pointed.processingPolicyId || linkJob.policyHash !== job.policyHash || linkJob.policyEpoch !== job.policyEpoch || linkJob.privacyEpoch !== job.privacyEpoch || linkJob.extractorRevision !== job.extractorRevision || linkJob.expiresAt !== pointed.expiresAt || !linkJob.membership.includes(episodeId)) return no("current-pointed-link-job");
        derivedTargets.push(link.sourceId, link.targetId);
      }
      // A cross-processing-policy current is admissible only when it proves a
      // strictly later canonical observation. Equal/within-skew cannot silently
      // migrate the envelope, and an older job can never rewind the winner.
      const orderComparison = compareProjectionOrders(pointed.effectiveOrder, projection.effectiveOrder, authority.maxClockSkewMs);
      const crossPolicyCurrent = current.processingPolicyId !== job.policyId || current.expiresAt !== job.expiresAt;
      if (crossPolicyCurrent) {
        if (orderComparison !== "after") return no("current-cross-policy");
      } else if (current.observationId !== projection.observationId) {
        if (pointed.contentId === projection.contentId) {
          if (orderComparison === "before") return no("current-rewind");
        } else if (orderComparison !== "after") return no("current-rewind");
      }
    } else {
      if (current.processingPolicyId !== job.policyId || current.expiresAt !== job.expiresAt || current.conflictManifestHash === undefined || current.vector !== undefined) return no("conflict-current");
      const manifestValue = await protocol.readCurated("conflict_manifest", current.conflictManifestHash);
      if (manifestValue === null || manifestValue.recordType !== "conflict_manifest") return no("conflict-manifest");
      const manifest = manifestValue as ConflictManifestRecord;
      if (manifest.ownerHost !== target.ownerHost || manifest.contentHash !== canonicalRecordHash(manifest) || manifest.processingPolicyId !== job.policyId || manifest.expiresAt !== job.expiresAt || manifest.coordinationPolicyEpoch !== current.coordinationPolicyEpoch || manifest.coordinationPolicyHash !== current.coordinationPolicyHash || manifest.privacyEpoch !== current.privacyEpoch || manifest.stateKey !== projection.stateKey || manifest.members.length < 2 || current.conflictManifestHash !== manifest.id) return no("conflict-membership");
      const conflictMembers: CuratedMemoryRecord[] = [];
      derivedTargets.push(manifest.stateKey, ...manifest.members);
      for (const memberId of manifest.members) {
        const memberValue = await protocol.readCurated("curated_memory", memberId);
        if (memberValue === null || memberValue.recordType !== "curated_memory") return no("conflict-member");
        const member = memberValue as CuratedMemoryRecord;
        if (member.id !== member.observationId || member.createdAt !== member.eventAt || member.effectiveAt !== member.eventAt || member.contentHash !== canonicalRecordHash(member) || member.sourceEpisodeIds === undefined || member.provenance === undefined || canonicalStringify(member.sourceEpisodeIds) !== canonicalStringify([...member.sourceEpisodeIds].sort()) || canonicalStringify(member.provenance) !== canonicalStringify(member.sourceEpisodeIds) || member.ownerHost !== target.ownerHost || member.processingPolicyId !== job.policyId || member.expiresAt !== job.expiresAt || member.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || member.coordinationPolicyHash !== job.coordinationPolicyHash || member.privacyEpoch !== job.privacyEpoch || member.stateKey !== projection.stateKey || member.sourceEpisodeIds.length === 0 || !Array.isArray(member.vector) || member.vector.length !== 1024 || !member.vector.every((value) => typeof value === "number" && Number.isFinite(value))) return no("conflict-member-envelope");
        for (const episodeId of member.sourceEpisodeIds) {
          const linkId = evidenceLinkId(member.id, episodeId, job.extractorRevision);
          const linkValue = await protocol.readCurated("evidence_link", linkId);
          if (linkValue === null || linkValue.recordType !== "evidence_link") return no("conflict-member-link");
          const link = linkValue as EvidenceLinkRecord;
          if (link.id !== linkId || link.ownerHost !== target.ownerHost || link.processingPolicyId !== job.policyId || link.expiresAt !== job.expiresAt || link.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || link.coordinationPolicyHash !== job.coordinationPolicyHash || link.privacyEpoch !== job.privacyEpoch || link.createdAt !== member.createdAt || link.sourceId !== member.id || link.targetId !== episodeId || link.extractorRevision !== job.extractorRevision || link.contentHash !== canonicalRecordHash(link)) return no("conflict-member-link-envelope");
          const linkJob = link.jobId === job.id ? job : await protocol.readJob(link.jobId);
          const linkLease = linkJob === null ? null : link.jobId === job.id ? initialClaim : await protocol.readLease(linkJob.id);
          if (linkJob === null || linkLease === null || (linkLease.state !== "accepted" && linkLease.state !== "released" && linkLease.state !== "completed") || linkLease.acceptedProposalId === null || linkLease.acceptedManifestHash === null || !claimIdentityMatchesJob(linkLease, linkJob) || linkJob.ownerHost !== target.ownerHost || linkJob.policyId !== job.policyId || linkJob.policyHash !== job.policyHash || linkJob.policyEpoch !== job.policyEpoch || linkJob.privacyEpoch !== job.privacyEpoch || linkJob.extractorRevision !== job.extractorRevision || linkJob.expiresAt !== job.expiresAt || !linkJob.membership.includes(episodeId)) return no("conflict-member-link-job");
          derivedTargets.push(link.sourceId, link.targetId);
        }
        conflictMembers.push(member);
      }
      let aggregate: ReturnType<typeof projectConflictAggregate>;
      try { aggregate = projectConflictAggregate(conflictMembers); } catch { return no("conflict-aggregate"); }
      if (canonicalStringify(manifest.members) !== canonicalStringify(aggregate.members) || manifest.id !== conflictManifestId(manifest.coordinationPolicyHash, manifest.stateKey, aggregate.members) || manifest.createdAt !== aggregate.createdAt || canonicalStringify(current.sourceEpisodeIds ?? []) !== canonicalStringify(aggregate.sourceEpisodeIds) || canonicalStringify(current.effectiveOrder) !== canonicalStringify(aggregate.effectiveOrder) || current.createdAt !== aggregate.createdAt || !aggregate.representatives.some((member) => member.contentId === projection.contentId)) return no("conflict-projection");
      conflicts.set(manifest.id, manifest);
    }
    currents.set(current.id, current);
  }
  const expectedCoverage = job.membership.map((episodeId) => coverageId({ ownerHost: target.ownerHost, episodeId, extractorRevision: job.extractorRevision, coordinationPolicyHash: job.coordinationPolicyHash, coordinationPolicyEpoch: job.coordinationPolicyEpoch, policyIntersectionId: job.policyId, privacyEpoch: job.privacyEpoch }));
  const coverage = await protocol.readCoverage(expectedCoverage);
  if (coverage.length !== expectedCoverage.length || new Set(coverage.map((record) => record.id)).size !== coverage.length || coverage.some((record) => record.ownerHost !== target.ownerHost || !expectedCoverage.includes(record.id) || !memberSet.has(record.episodeId) || record.processingPolicyId !== job.policyId || record.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || record.coordinationPolicyHash !== job.coordinationPolicyHash || record.privacyEpoch !== job.privacyEpoch || record.expiresAt !== null || record.extractorRevision !== job.extractorRevision || record.contentHash !== canonicalRecordHash(record))) return no("check-19");
  const coverageEpisodeMap = new Map(episodes.map((episode) => [episode.id, episode]));
  if (coverage.some((record) => {
    const episode = coverageEpisodeMap.get(record.episodeId);
    return episode === undefined || episode.createdAt !== record.createdAt;
  })) return no("check-19-created-at");
  const membershipSet = new Set(job.membership);
  const mutationTargets = [...new Set(derivedTargets)].filter((target) => !membershipSet.has(target)).sort();
  if (mutationTargets.length > MAX_COMPLETION_DERIVED_TARGETS) return no("check-19-derived-target-cap");
  const finalJob = await protocol.readJob(job.id);
  const finalProposal = await protocol.readProposal(proposal.id);
  const finalTombstones = await protocol.readTombstones(job.membership);
  const finalDerivedTombstones = mutationTargets.length === 0 ? [] : await readTombstoneChunks(protocol, mutationTargets);
  const finalControl = await protocol.readControl();
  const finalClaim = await protocol.readLease(job.id);
  const finalNow = authority.now();
  if (finalJob === null || finalProposal === null || canonicalStringify(finalJob) !== canonicalStringify(job) || canonicalStringify(finalProposal) !== canonicalStringify(proposal) || canonicalStringify(finalControl) !== canonicalStringify(initialControl) || canonicalStringify(finalTombstones) !== canonicalStringify(initialTombstones) || finalTombstones.length > 0 || finalDerivedTombstones.length > 0 || finalControl.state !== "active" || finalControl.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || finalControl.coordinationPolicyHash !== authority.coordinationPolicyHash || finalControl.privacyEpoch !== authority.privacyEpoch || finalClaim === null || !authority.matchesClaim(finalClaim) || canonicalStringify(finalClaim) !== canonicalStringify(initialClaim) || finalClaim.state !== "accepted" || Date.parse(finalClaim.expiresAt) <= finalNow || jobExpired(finalJob, finalNow, authority.maxClockSkewMs)) return no("check-20");
  const next = hashed({ ...finalClaim, version: finalClaim.version + 1, state: "completed" });
  const won = await protocol.casLease({ jobId: job.id, expectedVersion: finalClaim.version, expectedFencingToken: finalClaim.fencingToken, expectedPolicyEpoch: finalClaim.coordinationPolicyEpoch, expectedPolicyHash: finalClaim.coordinationPolicyHash, expectedPrivacyEpoch: finalClaim.privacyEpoch, expectedState: "accepted", expectedOwner: finalClaim.ownerId, expectedAcceptedProposalId: finalClaim.acceptedProposalId, expectedAcceptedManifestHash: finalClaim.acceptedManifestHash, expectedProcessingPolicyId: finalClaim.processingPolicyId, expectedCreatedAt: finalClaim.createdAt, expectedContentHash: finalClaim.contentHash, expiresAfter: finalNow, next });
  if (!won) return no("check-21");
  const reread = await protocol.readLease(job.id);
  if (reread === null || canonicalStringify(reread) !== canonicalStringify(next)) return no("check-22");
  const afterJob = await protocol.readJob(job.id);
  const afterProposal = await protocol.readProposal(proposal.id);
  const afterCoverage = await protocol.readCoverage(expectedCoverage);
  const afterControl = await protocol.readControl();
  const afterTombstones = await protocol.readTombstones(job.membership);
  const afterDerivedTombstones = mutationTargets.length === 0 ? [] : await readTombstoneChunks(protocol, mutationTargets);
  const afterClaim = await protocol.readLease(job.id);
  const afterNow = authority.now();
  return afterJob !== null && canonicalStringify(afterJob) === canonicalStringify(finalJob) && afterProposal !== null && canonicalStringify(afterProposal) === canonicalStringify(finalProposal) && canonicalStringify(afterCoverage) === canonicalStringify(coverage) && afterClaim !== null && canonicalStringify(afterClaim) === canonicalStringify(next) && canonicalStringify(afterControl) === canonicalStringify(finalControl) && afterControl.state === "active" && afterTombstones.length === 0 && afterDerivedTombstones.length === 0 && Date.parse(afterClaim.expiresAt) > afterNow && !jobExpired(afterJob, afterNow, authority.maxClockSkewMs);
}
/** @internal Raw-protocol proposal write; the production class routes through its named safe method. */
/** @internal */
async function writeProposalOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, input: WriteProposalInput): Promise<ProposalRecord> {
  // BRAND FIRST: a structural fake with forged matchesScope/matchesClaim/
  // getters is rejected by the private brand check BEFORE any caller method
  // or getter is invoked; then the exact store object identity and the
  // private per-store scope must both hold.
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Proposal requires a genuine lease authority");
  if (!authority.matchesStore(target)) throw new TypeError("Proposal authority does not match the store");
  if (!authority.matchesScope(scope)) throw new TypeError("Proposal authority does not match the store authority scope");
  // The genuine LeaseAuthority is the ONLY source of nominal identity AND
  // configuration: host/job/owner/fence/policy hash+epoch/privacy/intersection
  // and the BOUND skew all derive from it. The caller only supplies the
  // content, membership and createdAt; there is no mismatched caller policy
  // field and no ownerId/fencingToken/now tuple to impersonate with.
  if (authority.state !== "leased") throw new TypeError("Proposal requires a LEASED authority");
  if (target.ownerHost !== authority.ownerHost) throw new TypeError("Proposal store owner does not match the authority owner");
  // OWNED SNAPSHOTS (before any await): dense-once membership, canonical
  // content clone, createdAt once — the caller input is never read again.
  const membership = ownedMembershipSnapshot(input.membership);
  const content = ownedCanonicalContentSnapshot(input.content);
  const createdAt = input.createdAt;
  validateSortedMembership(membership);
  if (typeof createdAt !== "string" || createdAt.length === 0 || createdAt.length > 512) throw new TypeError("Proposal createdAt is invalid");
  // Validate authority-store binding BEFORE reads, then read the ACTIVE
  // control, the EXACT current claim and the exact Job (deadline included).
  let control;
  try { control = await protocol.readControl(); } catch { throw new TypeError("Proposal control is unavailable"); }
  if (control.state !== "active" || control.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || control.coordinationPolicyHash !== authority.coordinationPolicyHash || control.privacyEpoch !== authority.privacyEpoch) throw new TypeError("Proposal control does not match the authority");
  const claim = await protocol.readLease(authority.jobId);
  if (claim === null || !authority.matchesClaim(claim) || claim.state !== "leased") throw new TypeError("Proposal authority claim is stale");
  const job = await protocol.readJob(authority.jobId);
  if (job === null || job.id !== authority.jobId || job.ownerHost !== authority.ownerHost || job.policyEpoch !== authority.coordinationPolicyEpoch || job.policyHash !== authority.coordinationPolicyHash || job.privacyEpoch !== authority.privacyEpoch || job.policyId !== authority.processingPolicyId || job.membership.length !== membership.length || job.membership.some((id, index) => id !== membership[index])) throw new TypeError("Proposal job does not match the authority");
  // Fresh worker clock AFTER the reads; the claim must be within the job
  // deadline and live for its exact owner, the finite job live with the BOUND
  // skew, and the membership tombstone-free BEFORE any insert.
  const proposalNow = authority.now();
  if (Date.parse(claim.expiresAt) <= proposalNow) throw new TypeError("Proposal lease is expired");
  if (job.expiresAt !== null && Date.parse(claim.expiresAt) > Date.parse(job.expiresAt)) throw new TypeError("Proposal claim outlives the job deadline");
  if (jobExpired(job, proposalNow, authority.maxClockSkewMs)) throw new TypeError("Proposal job is expired");
  const tombstones = await protocol.readTombstones([...membership]);
  if (tombstones.length > 0) throw new TypeError("Proposal membership is tombstoned");
  // The tombstone read is slow: immediately AFTER it, exact-reread the current
  // leased claim + active control and take a FRESH clock before the insert.
  const claimBeforeInsert = await protocol.readLease(authority.jobId);
  if (claimBeforeInsert === null || !authority.matchesClaim(claimBeforeInsert) || claimBeforeInsert.state !== "leased") throw new TypeError("Proposal authority claim changed before insert");
  let controlBeforeInsert;
  try { controlBeforeInsert = await protocol.readControl(); } catch { throw new TypeError("Proposal control is unavailable before insert"); }
  if (controlBeforeInsert.state !== "active" || controlBeforeInsert.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || controlBeforeInsert.coordinationPolicyHash !== authority.coordinationPolicyHash || controlBeforeInsert.privacyEpoch !== authority.privacyEpoch) throw new TypeError("Proposal control changed before insert");
  const insertNow = authority.now();
  if (Date.parse(claimBeforeInsert.expiresAt) <= insertNow) throw new TypeError("Proposal lease is expired before insert");
  if (job.expiresAt !== null && Date.parse(claimBeforeInsert.expiresAt) > Date.parse(job.expiresAt)) throw new TypeError("Proposal claim outlives the job deadline");
  if (jobExpired(job, insertNow, authority.maxClockSkewMs)) throw new TypeError("Proposal job is expired before insert");
  const ownerHost = authority.ownerHost;
  const policyHash = authority.coordinationPolicyHash;
  const policyEpoch = authority.coordinationPolicyEpoch;
  const privacyEpoch = authority.privacyEpoch;
  const policyIntersectionId = authority.processingPolicyId;
  const proposalHash = proposalHashFor({ ownerHost, jobId: authority.jobId, ownerId: authority.ownerId, membership, content: content as ProposalContent, policyHash, policyEpoch, fencingToken: authority.fencingToken, privacyEpoch, policyIntersectionId });
  const id = proposalIdFor(authority.jobId, proposalHash, policyEpoch, authority.fencingToken);
  const record: ProposalRecord = {
    ownerHost, schemaRevision: 1, createdAt, privacyEpoch,
    processingPolicyId: policyIntersectionId, expiresAt: job.expiresAt, recordType: "proposal", id,
    jobId: authority.jobId, ownerId: authority.ownerId, proposalHash, manifestHash: manifestHash(membership), fencingToken: authority.fencingToken,
    membership: [...membership], content,
    coordinationPolicyHash: policyHash, coordinationPolicyEpoch: policyEpoch, contentHash: "pending",
  };
  const final = { ...record, contentHash: canonicalRecordHash(record) } as ProposalRecord;
  await protocol.insertProposal(final);
  // Reread the exact FULL proposal (exact equality, not just the id) AND
  // revalidate tombstones/claim/control and take a FINAL clock after the last
  // await before returning.
  const reread = await protocol.readProposal(id);
  if (reread === null || reread.id !== id || reread.contentHash !== final.contentHash || reread.ownerId !== final.ownerId || reread.fencingToken !== final.fencingToken || reread.expiresAt !== final.expiresAt || reread.proposalHash !== final.proposalHash || reread.membership.length !== membership.length || reread.membership.some((memberId, index) => memberId !== membership[index]) || canonicalStringify(reread.content) !== canonicalStringify(content)) throw new TypeError("Proposal insert did not read back exactly");
  const tombstonesAfter = await protocol.readTombstones([...membership]);
  if (tombstonesAfter.length > 0) throw new TypeError("Proposal membership became tombstoned after insert");
  if (reread === null || reread.id !== id) throw new TypeError("Proposal insert did not read back");
  const claimAfter = await protocol.readLease(authority.jobId);
  if (claimAfter === null || !authority.matchesClaim(claimAfter)) throw new TypeError("Proposal authority claim changed during insert");
  let controlAfter;
  try { controlAfter = await protocol.readControl(); } catch { throw new TypeError("Proposal control is unavailable after insert"); }
  if (controlAfter.state !== "active" || controlAfter.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || controlAfter.coordinationPolicyHash !== authority.coordinationPolicyHash || controlAfter.privacyEpoch !== authority.privacyEpoch) throw new TypeError("Proposal control changed during insert");
  const finalNow = authority.now();
  if (Date.parse(claimAfter.expiresAt) <= finalNow) throw new TypeError("Proposal lease is expired after insert");
  if (jobExpired(job, finalNow, authority.maxClockSkewMs)) throw new TypeError("Proposal job is expired after insert");
  return reread;
}



/** Owned canonical clone of a caller-supplied curated record: canonicalStringify
 * rejects accessors/sparse arrays/symbols/non-plain/non-finite values; the
 * JSON.parse clone is the ONLY object validated and persisted (a malicious
 * accessor can never swap validated and persisted values). */
function ownedCuratedRecordSnapshot<T extends CuratedMemoryRecord | CuratedCurrentRecord | EvidenceLinkRecord | ConflictManifestRecord>(value: T): T {
  let serialized: string;
  try { serialized = canonicalStringify(value); } catch { throw new TypeError("Curated record is not canonical JSON"); }
  let clone: unknown;
  try { clone = JSON.parse(serialized) as unknown; } catch { throw new TypeError("Curated record is invalid"); }
  const record = parseMemoryRecord(clone) as T;
  if (record.contentHash !== canonicalRecordHash(record)) throw new TypeError("Curated record canonical hash mismatch");
  return record;
}
/** Evidence links are immutable by deterministic (observation, episode,
 * extractor) identity. An overlapping accepted job may observe an existing
 * valid link whose provenance jobId differs; all other fields must remain
 * byte-identical, and the prior job must be independently validated. */
function equivalentEvidenceLinkExceptJob(left: EvidenceLinkRecord, right: EvidenceLinkRecord): boolean {
  if (left.recordType !== "evidence_link" || right.recordType !== "evidence_link") return false;
  const normalizedLeft = { ...left, jobId: right.jobId };
  return deepEqual(normalizedLeft, right);
}


/** Derived tombstone targets that can invalidate a curated mutation without
 * tombstoning one of the source episodes. This is computed only from an owned,
 * validated record snapshot; callers cannot swap a target between validation
 * and the barrier read. */
function curatedMutationTombstoneTargets(record: CuratedMemoryRecord | CuratedCurrentRecord | EvidenceLinkRecord | ConflictManifestRecord): readonly string[] {
  const targets: string[] = [];
  switch (record.recordType) {
    case "curated_memory":
      targets.push(record.id, record.contentId);
      if (record.stateKey !== undefined) targets.push(record.stateKey);
      break;
    case "evidence_link":
      // targetId is normally a source episode in the accepted membership; the
      // source observation is the additional derived target that can be
      // forgotten independently of episode membership.
      targets.push(record.sourceId, record.targetId);
      break;
    case "conflict_manifest":
      targets.push(record.stateKey, ...record.members);
      break;
    case "curated_current":
      targets.push(record.stateKey);
      if (record.resolution === "resolved") targets.push(record.contentId, record.observationId);
      else targets.push(record.conflictManifestHash);
      break;
  }
  const unique = [...new Set(targets)].sort();
  if (unique.length > 4096 || unique.some((target) => typeof target !== "string" || target.length === 0 || target.length > 512)) throw new TypeError("Curated derived tombstone targets are invalid");
  return Object.freeze(unique);
}

async function readTombstoneChunks(protocol: CoordinationStore, ids: readonly string[]): Promise<readonly TombstoneRecord[]> {
  const found: TombstoneRecord[] = [];
  for (let index = 0; index < ids.length; index += 1024) found.push(...await protocol.readTombstones(ids.slice(index, index + 1024)));
  return found;
}

async function readMutationTombstones(protocol: CoordinationStore, membership: readonly string[], derivedTargets: readonly string[]): Promise<{ membership: readonly TombstoneRecord[]; derived: readonly TombstoneRecord[] }> {
  const membershipTombstones = await protocol.readTombstones(membership);
  const membershipSet = new Set(membership);
  const disjointDerived = [...new Set(derivedTargets)].filter((target) => !membershipSet.has(target)).sort();
  const derivedTombstones = disjointDerived.length === 0 ? [] : await readTombstoneChunks(protocol, disjointDerived);
  return { membership: membershipTombstones, derived: derivedTombstones };
}

/** Fresh accepted authority barrier used immediately before and after every
 * curated mutation. It deliberately rereads all liveness inputs in-kernel.
 * Both source membership and mutation-specific derived targets are checked
 * before the kernel write and reread after the slowest liveness reads. */
async function assertFreshAcceptedBarrier(protocol: CoordinationStore, authority: LeaseAuthority, membership: readonly string[], derivedTargets: readonly string[] = []): Promise<void> {
  const derived = [...new Set(derivedTargets)].sort();
  if (derived.length > 4096 || derived.some((target) => typeof target !== "string" || target.length === 0 || target.length > 512)) throw new TypeError("Curated derived tombstone targets are invalid");
  const jobMatchesAuthority = (candidate: JobRecord | null): candidate is JobRecord => candidate !== null && candidate.id === authority.jobId && candidate.ownerHost === authority.ownerHost && candidate.policyId === authority.processingPolicyId && candidate.policyHash === authority.coordinationPolicyHash && candidate.policyEpoch === authority.coordinationPolicyEpoch && candidate.coordinationPolicyHash === authority.coordinationPolicyHash && candidate.coordinationPolicyEpoch === authority.coordinationPolicyEpoch && candidate.privacyEpoch === authority.privacyEpoch && candidate.membership.length === membership.length && candidate.membership.every((id, index) => id === membership[index]);
  const controlMatches = (control: ControlRecord): boolean => control.ownerHost === authority.ownerHost && control.state === "active" && control.coordinationPolicyEpoch === authority.coordinationPolicyEpoch && control.coordinationPolicyHash === authority.coordinationPolicyHash && control.privacyEpoch === authority.privacyEpoch;
  const proposalMatches = (proposal: ProposalRecord | null, job: JobRecord, claim: LeaseRecord): proposal is ProposalRecord => proposal !== null && claim.acceptedProposalId !== null && claim.acceptedManifestHash !== null && proposal.id === claim.acceptedProposalId && proposal.jobId === job.id && proposal.ownerHost === job.ownerHost && proposal.processingPolicyId === job.policyId && proposal.coordinationPolicyEpoch === job.coordinationPolicyEpoch && proposal.coordinationPolicyHash === job.coordinationPolicyHash && proposal.privacyEpoch === job.privacyEpoch && proposal.expiresAt === job.expiresAt && proposal.manifestHash === claim.acceptedManifestHash && proposal.membership.length === job.membership.length && proposal.membership.every((id, index) => id === job.membership[index]);
  const control = await protocol.readControl();
  const job = await protocol.readJob(authority.jobId);
  const claim = await protocol.readLease(authority.jobId);
  if (!jobMatchesAuthority(job) || claim === null || claim.acceptedProposalId === null) throw new TypeError("Curated write claim barrier failed");
  const proposal = await protocol.readProposal(claim.acceptedProposalId);
  const firstTombstones = await readMutationTombstones(protocol, membership, derived);
  const now = authority.now();
  if (!controlMatches(control) || !proposalMatches(proposal, job, claim) || !authority.matchesClaim(claim) || claim.state !== "accepted" || Date.parse(claim.expiresAt) <= now || jobExpired(job, now, authority.maxClockSkewMs) || firstTombstones.membership.length > 0 || firstTombstones.derived.length > 0) throw new TypeError("Curated write first barrier failed");
  const finalControl = await protocol.readControl();
  const finalJob = await protocol.readJob(authority.jobId);
  const finalProposal = await protocol.readProposal(claim.acceptedProposalId);
  const finalClaim = await protocol.readLease(authority.jobId);
  const finalTombstones = await readMutationTombstones(protocol, membership, derived);
  const finalNow = authority.now();
  if (!jobMatchesAuthority(finalJob) || finalClaim === null || !controlMatches(finalControl) || !proposalMatches(finalProposal, finalJob, finalClaim) || !authority.matchesClaim(finalClaim) || finalClaim.state !== "accepted" || Date.parse(finalClaim.expiresAt) <= finalNow || jobExpired(finalJob, finalNow, authority.maxClockSkewMs) || finalTombstones.membership.length > 0 || finalTombstones.derived.length > 0) throw new TypeError("Curated write final barrier failed");
  if (canonicalStringify(finalControl) !== canonicalStringify(control) || canonicalStringify(finalJob) !== canonicalStringify(job) || canonicalStringify(finalProposal) !== canonicalStringify(proposal) || canonicalStringify(finalClaim) !== canonicalStringify(claim) || canonicalStringify(finalTombstones) !== canonicalStringify(firstTombstones)) throw new TypeError("Curated write authority changed during barrier");
}


/** @internal Raw-protocol immutable curated write; the production class routes through its named safe method. */
async function insertCuratedOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, input: { record: CuratedMemoryRecord | EvidenceLinkRecord | ConflictManifestRecord }): Promise<CuratedMemoryRecord | EvidenceLinkRecord | ConflictManifestRecord> {
  // GLOBAL RULE: snapshot the caller record EXACTLY ONCE into an owned
  // canonical clone; only the clone is validated and persisted.
  const record = ownedCuratedRecordSnapshot(input.record);
  const derivedTargets = curatedMutationTombstoneTargets(record);
  const job = await protocol.readJob(authority.jobId);
  if (job === null) throw new TypeError("Curated write job is missing");
  await assertFreshAcceptedBarrier(protocol, authority, job.membership, derivedTargets);
  await protocol.insertCurated(record);
  const reread = await protocol.readCurated(record.recordType, record.id);
  let exactReadback = reread !== null && reread.id === record.id && reread.contentHash === record.contentHash && canonicalRecordHash(reread as never) === reread.contentHash && deepEqual(reread, record);
  if (!exactReadback && reread !== null && record.recordType === "evidence_link" && reread.recordType === "evidence_link" && equivalentEvidenceLinkExceptJob(reread, record)) {
    const priorJob = await protocol.readJob(reread.jobId);
    const priorLease = priorJob === null ? null : await protocol.readLease(priorJob.id);
    exactReadback = priorJob !== null && priorLease !== null && (priorLease.state === "accepted" || priorLease.state === "released" || priorLease.state === "completed") && priorLease.acceptedProposalId !== null && priorLease.acceptedManifestHash !== null && claimIdentityMatchesJob(priorLease, priorJob) && priorJob.ownerHost === target.ownerHost && priorJob.policyId === record.processingPolicyId && priorJob.policyHash === record.coordinationPolicyHash && priorJob.policyEpoch === record.coordinationPolicyEpoch && priorJob.privacyEpoch === record.privacyEpoch && priorJob.extractorRevision === record.extractorRevision && priorJob.expiresAt === record.expiresAt && priorJob.membership.includes(record.targetId);
  }
  if (!exactReadback) throw new TypeError("Curated record did not read back exactly");
  if (record.recordType === "curated_memory") {
    const intendedVector = (record as CuratedMemoryRecord | CuratedCurrentRecord).vector;
    const actualVector = (reread as CuratedMemoryRecord | CuratedCurrentRecord).vector;
    if (!Array.isArray(intendedVector) || intendedVector.length !== 1024 || !Array.isArray(actualVector) || actualVector.length !== 1024 || actualVector.some((value, index) => value !== intendedVector[index])) throw new TypeError("Curated vector did not read back exactly");
  } else if ((reread as CuratedMemoryRecord | CuratedCurrentRecord).vector !== undefined) throw new TypeError("Unexpected curated vector read back");
  await assertFreshAcceptedBarrier(protocol, authority, job.membership, derivedTargets);
  return reread as CuratedMemoryRecord | EvidenceLinkRecord | ConflictManifestRecord;
}

/** @internal Raw-protocol OCC current upsert; the production class routes through its named safe method. */
async function upsertCuratedCurrentOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, input: { record: CuratedCurrentRecord; expectedVersion: number | null }): Promise<CuratedCurrentRecord | null> {
  // GLOBAL RULE: snapshot the caller input EXACTLY ONCE; only the owned
  // canonical clone is validated, CASed and persisted.
  const expectedVersion = input.expectedVersion;
  const record = ownedCuratedRecordSnapshot(input.record) as CuratedCurrentRecord;
  if (record.recordType !== "curated_current") throw new TypeError("Curated current record is invalid");
  let derivedTargets = [...curatedMutationTombstoneTargets(record)];
  if (record.resolution === "conflict") {
    const manifestValue = await protocol.readCurated("conflict_manifest", record.conflictManifestHash);
    if (manifestValue === null || manifestValue.recordType !== "conflict_manifest" || manifestValue.id !== record.conflictManifestHash || manifestValue.stateKey !== record.stateKey || manifestValue.contentHash !== canonicalRecordHash(manifestValue)) throw new TypeError("Curated conflict current manifest is invalid");
    derivedTargets = [...new Set([...derivedTargets, ...manifestValue.members])].sort();
    if (derivedTargets.length > 4096) throw new TypeError("Curated derived tombstone targets are invalid");
  }
  const job = await protocol.readJob(authority.jobId);
  if (job === null) throw new TypeError("Curated current job is missing");
  await assertFreshAcceptedBarrier(protocol, authority, job.membership, derivedTargets);
  if (expectedVersion === null) {
    await protocol.insertCurated(record);
    const reread = await protocol.readCurated("curated_current", record.id);
    if (reread === null || reread.id !== record.id || reread.contentHash !== record.contentHash || canonicalRecordHash(reread as never) !== reread.contentHash || !deepEqual(reread, record)) throw new TypeError("Curated current did not read back exactly");
    if (record.resolution === "resolved" && (!Array.isArray(record.vector) || !Array.isArray((reread as CuratedCurrentRecord).vector) || (reread as CuratedCurrentRecord).vector!.some((value, index) => value !== record.vector![index]))) throw new TypeError("Curated current vector did not read back exactly");
    if (record.resolution === "conflict" && (reread as CuratedCurrentRecord).vector !== undefined) throw new TypeError("Unexpected conflict current vector");
    await assertFreshAcceptedBarrier(protocol, authority, job.membership, derivedTargets);
    return reread as CuratedCurrentRecord;
  }
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1) throw new TypeError("Curated current expected version is invalid");
  const current = await protocol.readCurated("curated_current", record.id) as CuratedCurrentRecord | null;
  if (current === null || current.recordType !== "curated_current" || current.version !== expectedVersion) return null;
  const precondition: CuratedCurrentCasPrecondition = {
    kind: "current-cas", ownerHost: target.ownerHost, recordType: "curated_current", id: record.id,
    expectedVersion, expectedEpoch: current.coordinationPolicyEpoch, expectedPolicyHash: current.coordinationPolicyHash, expectedProcessingPolicyId: current.processingPolicyId, expectedExpiresAt: current.expiresAt,
    expectedPrivacyEpoch: current.privacyEpoch, expectedResolution: current.resolution,
    expectedContentId: current.resolution === "resolved" ? current.contentId ?? null : null,
    expectedConflictManifestHash: current.resolution === "conflict" ? current.conflictManifestHash ?? null : null,
    expectedContentHash: current.contentHash,
  };
  const applied = await protocol.casCuratedCurrent({ id: physicalPointId("curated_current", record.id), precondition, next: record });
  if (!applied) return null;
  const reread = await protocol.readCurated("curated_current", record.id) as CuratedCurrentRecord | null;
  if (reread === null || reread.id !== record.id || reread.contentHash !== record.contentHash || canonicalRecordHash(reread as never) !== reread.contentHash || !deepEqual(reread, record)) return null;
  if (record.resolution === "resolved" && (!Array.isArray(record.vector) || !Array.isArray((reread as CuratedCurrentRecord).vector) || (reread as CuratedCurrentRecord).vector!.some((value, index) => value !== record.vector![index]))) return null;
  if (record.resolution === "conflict" && (reread as CuratedCurrentRecord).vector !== undefined) return null;
  await assertFreshAcceptedBarrier(protocol, authority, job.membership, derivedTargets);
  return reread;
}

/** @internal Raw-protocol acceptance wrapper; the production class routes through its named safe method. */
/** @internal */
async function acceptProposalOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, input: { proposalId: string }): Promise<LeaseAuthority | null> {
  // SECURITY AUTHORITY FIRST: brand + exact-store + private-scope BEFORE any
  // caller input field/getter is accessed (the proposalId getter on a forged
  // input object can never fire for a fake authority).
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Accept proposal requires a genuine lease authority");
  if (!authority.matchesStore(target)) throw new TypeError("Accept proposal authority does not match the store");
  if (!authority.matchesScope(scope)) throw new TypeError("Accept proposal authority does not match the store authority scope");
  // Reject malformed/proxy options before any property access, then snapshot
  // the one contractual field from an own enumerable data descriptor. Unknown
  // fields (including explosive getters) are intentionally untouched.
  if (input === null || typeof input !== "object" || Array.isArray(input) || nodeTypes.isProxy(input)) throw new TypeError("Accept proposal inputs are invalid");
  const proposalDescriptor = Object.getOwnPropertyDescriptor(input, "proposalId");
  if (proposalDescriptor === undefined || !("value" in proposalDescriptor) || proposalDescriptor.enumerable !== true) throw new TypeError("Accept proposal inputs are invalid");
  const proposalId = proposalDescriptor.value;
  if (typeof proposalId !== "string" || proposalId.length === 0 || proposalId.length > 512) throw new TypeError("Accept proposal inputs are invalid");
  return acceptLeaseAuthorityOnProtocol(protocol, target, scope, authority, proposalId);
}



// ---------------------------------------------------------------------------
// Authority kernel: tombstones (lexical; NOT exported)
// ---------------------------------------------------------------------------
function tombstoneRecord(input: CreateTombstoneInput, targetId: string, scope: TombstoneScope): TombstoneRecord {
  const record: TombstoneRecord = {
    ownerHost: input.ownerHost, schemaRevision: 1, createdAt: input.createdAt, privacyEpoch: input.privacyEpoch,
    processingPolicyId: input.processingPolicyId, expiresAt: null, recordType: "tombstone",
    id: tombstoneId(input.ownerHost, targetId), scope, targetId, contentHash: "pending",
  };
  return { ...record, contentHash: canonicalRecordHash(record) } as TombstoneRecord;
}
/**
 * Insert immutable tombstones (strong ordering/wait, reread) for a target and
 * its provenance source IDs. Mismatched scope/target types fail closed; bare
 * UUID occurrence targets are verified as episodes through an explicit
 * selector and an exact store lookup. Provenance source tombstones are
 * provenance-independent, so the same source converges across forgotten
 * targets (no provenanceId-dependent collision).
 */

/** @internal Raw-protocol tombstone creation; the production class routes through its named safe method. */
/** @internal */
async function createTombstoneOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, input: CreateTombstoneInput): Promise<TombstoneRecord[]> {
  // OWNED SNAPSHOTS at entry: every field is read EXACTLY ONCE into locals and
  // a plain frozen normalized input; the caller object is never read again and
  // never passed to tombstoneRecord. A getter swap valid->invalid can never
  // make a different target get persisted than the one validated first.
  const ownerHost = input.ownerHost;
  const scopeValue = input.scope;
  const targetId = input.targetId;
  const targetKind = input.targetKind;
  const provenanceInput = input.provenanceIds;
  const createdAt = input.createdAt;
  const privacyEpoch = input.privacyEpoch;
  const processingPolicyId = input.processingPolicyId;
  if (target.ownerHost !== ownerHost) throw new TypeError("Tombstone store owner does not match the target owner");
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(createdAt) || !Number.isFinite(Date.parse(createdAt))) throw new TypeError("Tombstone createdAt is invalid");
  if (!Number.isSafeInteger(privacyEpoch) || privacyEpoch < 0) throw new TypeError("Tombstone privacy epoch is invalid");
  if (typeof processingPolicyId !== "string" || processingPolicyId.length === 0 || processingPolicyId.length > 512 || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(processingPolicyId)) throw new TypeError("Tombstone processing policy is invalid");
  if (typeof targetId !== "string" || targetId.length === 0 || targetId.length > 512) throw new TypeError("Tombstone target is invalid");
  if (!isTombstoneTarget(scopeValue, targetId)) throw new TypeError("Tombstone target does not match its scope");
  if (scopeValue === "occurrence" && !isOccurrenceTarget(targetId)) throw new TypeError("Tombstone occurrence target is not verifiable");
  const isBareEpisode = scopeValue === "occurrence" && !String(targetId).startsWith("occurrence:") && isOccurrenceTarget(targetId);
  const provenanceIds = provenanceInput === undefined ? Object.freeze([]) : ownedStringArraySnapshot(provenanceInput, "Tombstone provenance", 512, (id) => isOccurrenceTarget(id));
  if (provenanceIds.length > 1024 || new Set(provenanceIds).size !== provenanceIds.length) throw new TypeError("Tombstone provenance IDs are invalid");
  const normalizedInput: CreateTombstoneInput = { ownerHost, scope: scopeValue, targetId, createdAt, privacyEpoch, processingPolicyId, provenanceIds };
  if (targetKind !== undefined) normalizedInput.targetKind = targetKind;
  Object.freeze(normalizedInput);
  if (isBareEpisode) {
    if (targetKind !== "episode") throw new TypeError("Tombstone episode targets require an explicit episode selector");
    const episode = await protocol.readEpisode(targetId);
    if (episode === null || episode.ownerHost !== ownerHost) throw new TypeError("Tombstone episode target is not an existing episode of this host");
  }
  const records: TombstoneRecord[] = [tombstoneRecord(normalizedInput, targetId, scopeValue)];
  // Bare-UUID provenance episodes require the same explicit selector + exact store verification as the primary target.
  for (const sourceId of provenanceIds) {
    if (String(sourceId).startsWith("occurrence:")) continue;
    if (targetKind !== "episode") throw new TypeError("Tombstone provenance episode IDs require the explicit episode selector");
    const episode = await protocol.readEpisode(sourceId);
    if (episode === null || episode.ownerHost !== ownerHost) throw new TypeError("Tombstone provenance episode is not an existing episode of this host");
  }
  for (const sourceId of provenanceIds) records.push(tombstoneRecord(normalizedInput, sourceId, "occurrence"));
  const inserted: TombstoneRecord[] = [];
  for (const record of records) {
    // Tombstone identity is target/scope-stable under fixed H(owner,"tombstone",target);
    // repeated same-target forget across privacy/policy epochs converges idempotently
    // (exact-read existing before insert) instead of content-hash colliding.
    const existing = await protocol.readTombstones([record.targetId]);
    if (!existing.some((entry) => entry.id === record.id)) await protocol.insertTombstone(record);
    const reread = await protocol.readTombstones([record.targetId]);
    if (!reread.some((entry) => entry.id === record.id)) throw new TypeError("Tombstone insert did not read back");
    inserted.push(reread.find((entry) => entry.id === record.id)!);
  }
  return inserted;
}

// ---------------------------------------------------------------------------
// Authority kernel: reconcile (lexical; NOT exported)
// ---------------------------------------------------------------------------
function snapshotMarkCoverageInput(input: MarkCoverageInput): MarkCoverageInput {
  const ownerHost = input.ownerHost; const episodeId = input.episodeId; const extractorRevision = input.extractorRevision; const policyHash = input.policyHash; const policyEpoch = input.policyEpoch; const privacyEpoch = input.privacyEpoch; const createdAt = input.createdAt; const processingPolicyId = input.processingPolicyId;
  return Object.freeze({ ownerHost, episodeId, extractorRevision, policyHash, policyEpoch, privacyEpoch, createdAt, processingPolicyId });
}
function coverageRecordForInput(target: ProductionCoordinationStore, input: MarkCoverageInput): CoverageRecord {
  const record: CoverageRecord = { ownerHost: input.ownerHost, schemaRevision: 1, createdAt: input.createdAt, privacyEpoch: input.privacyEpoch, processingPolicyId: input.processingPolicyId, expiresAt: null, recordType: "coverage", id: coverageId({ ownerHost: input.ownerHost, episodeId: input.episodeId, extractorRevision: input.extractorRevision, coordinationPolicyHash: input.policyHash, coordinationPolicyEpoch: input.policyEpoch, policyIntersectionId: input.processingPolicyId, privacyEpoch: input.privacyEpoch }), episodeId: input.episodeId, extractorRevision: input.extractorRevision, coordinationPolicyHash: input.policyHash, coordinationPolicyEpoch: input.policyEpoch, contentHash: "pending" };
  if (target.ownerHost !== input.ownerHost) throw new TypeError("Coverage store owner does not match the target owner");
  return { ...record, contentHash: canonicalRecordHash(record) } as CoverageRecord;
}
async function assertCoverageRecordBoundToJob(record: CoverageRecord, job: JobRecord, ownerHost: HostId): Promise<void> {
  if (record.ownerHost !== ownerHost || record.ownerHost !== job.ownerHost || !job.membership.includes(record.episodeId) || record.processingPolicyId !== job.policyId || record.coordinationPolicyHash !== job.coordinationPolicyHash || record.coordinationPolicyEpoch !== job.coordinationPolicyEpoch || record.privacyEpoch !== job.privacyEpoch || record.extractorRevision !== job.extractorRevision || record.expiresAt !== null || record.contentHash !== canonicalRecordHash(record) || record.id !== coverageId({ ownerHost, episodeId: record.episodeId, extractorRevision: record.extractorRevision, coordinationPolicyHash: record.coordinationPolicyHash, coordinationPolicyEpoch: record.coordinationPolicyEpoch, policyIntersectionId: job.policyId, privacyEpoch: record.privacyEpoch })) throw new TypeError("Coverage is not bound to the accepted job membership");
}

async function markCoverageOnProtocol(protocol: CoordinationStore, target: ProductionCoordinationStore, scope: object, authority: LeaseAuthority, input: MarkCoverageInput): Promise<CoverageRecord> {
  const normalized = snapshotMarkCoverageInput(input);
  const final = coverageRecordForInput(target, normalized);
  const job = await protocol.readJob(authority.jobId);
  if (job === null) throw new TypeError("Coverage job is missing");
  await assertCoverageRecordBoundToJob(final, job, target.ownerHost);
  const episodes = await protocol.readEpisodes([final.episodeId], job.privacyEpoch);
  if (episodes.length === 1) {
    if (episodes[0]!.id !== final.episodeId || episodes[0]!.ownerHost !== target.ownerHost || episodes[0]!.createdAt !== final.createdAt) throw new TypeError("Coverage timestamp is not bound to its canonical episode");
  } else if (final.createdAt !== job.createdAt) throw new TypeError("Coverage timestamp requires its canonical episode");
  await assertFreshAcceptedBarrier(protocol, authority, job.membership);
  await protocol.insertCoverage(final);
  const reread = await protocol.readCoverage([final.id]);
  if (reread.length !== 1 || reread[0]!.id !== final.id || reread[0]!.contentHash !== final.contentHash || canonicalRecordHash(reread[0]!) !== reread[0]!.contentHash || !deepEqual(reread[0], final)) throw new TypeError("Coverage insert did not read back exactly");
  await assertFreshAcceptedBarrier(protocol, authority, job.membership);
  return reread[0]!;
}


// ---------------------------------------------------------------------------
// Authority kernel: control (lexical; NOT exported)
// ---------------------------------------------------------------------------
const QUIESCENCE_PROOF_ISSUER = Symbol("pi-qdrant-memory-v2.quiescence-proof-issuer");

/** Unforgeable quiescence proof: branded, tied to the exact draining control identity. */
export class QuiescenceProof {
  readonly #issuer: symbol;
  /** Module-private per-store scope: minted ONLY from the owning store's lexical quiescence. */
  readonly #scope: object;
  readonly #controlVersion: number;
  readonly #coordinationPolicyEpoch: number;
  readonly #coordinationPolicyHash: string;
  readonly #privacyEpoch: number;
  readonly #completedAt: number;
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(control: ControlRecord, completedAt: number, scope: object, issuer: symbol) {
    if (issuer !== QUIESCENCE_PROOF_ISSUER) throw new TypeError("Quiescence proof requires the module issuer");
    this.#issuer = issuer;
    this.#scope = scope;
    this.#controlVersion = control.version;
    this.#coordinationPolicyEpoch = control.coordinationPolicyEpoch;
    this.#coordinationPolicyHash = control.coordinationPolicyHash;
    this.#privacyEpoch = control.privacyEpoch;
    this.#completedAt = completedAt;
    Object.freeze(this);
  }
  /** Private per-store scope binding; only the owning store's lexical methods can mint or consume. */
  matchesScope(scope: object): boolean { return this.#scope === scope; }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is QuiescenceProof {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof QuiescenceProof && value.#issuer === QUIESCENCE_PROOF_ISSUER;
  }
  matches(control: ControlRecord): boolean {
    return this.#controlVersion === control.version && this.#coordinationPolicyEpoch === control.coordinationPolicyEpoch && this.#coordinationPolicyHash === control.coordinationPolicyHash && this.#privacyEpoch === control.privacyEpoch;
  }
}

Object.freeze(QuiescenceProof);
Object.freeze(QuiescenceProof.prototype);

function validClock(name: string, value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > MAX_TIME) throw new TypeError(`${name} must be a finite bounded clock value`);
  return value as number;
}
function validDelay(name: string, value: unknown, max = 86_400_000): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > max) throw new TypeError(`${name} must be a bounded non-negative duration`);
  return value as number;
}
/** Owned dense string-array snapshot: every element is read EXACTLY ONCE;
 * sparse arrays, accessor-bearing arrays and symbol-carrying arrays are
 * rejected. The returned array is a caller-independent frozen copy. */
function ownedStringArraySnapshot(input: readonly string[], name: string, maxLength: number, validate?: (value: string) => boolean): readonly string[] {
  if (input === null || typeof input !== "object" || !Array.isArray(input)) throw new TypeError(`${name} is invalid`);
  const snapshot: string[] = [];
  for (let i = 0; i < input.length; i += 1) {
    if (!(i in input)) throw new TypeError(`${name} must be dense`);
    const value = input[i];
    if (typeof value !== "string" || value.length === 0 || value.length > maxLength || (validate !== undefined && !validate(value))) throw new TypeError(`${name} element is invalid`);
    snapshot.push(value);
  }
  if (Object.getOwnPropertySymbols(input).length > 0) throw new TypeError(`${name} contains a symbol key`);
  return Object.freeze(snapshot);
}
/** Owned membership snapshot: dense-once string array + non-empty. */
function ownedMembershipSnapshot(input: readonly string[]): readonly string[] {
  const snapshot = ownedStringArraySnapshot(input, "Membership", 512);
  if (snapshot.length === 0) throw new TypeError("Membership must be non-empty");
  return snapshot;
}
/** Owned canonical content clone: canonicalStringify (rejects accessors/non-plain/
 * symbols/cycles/non-finite) then JSON.parse into a caller-independent plain
 * value; the clone (not the caller object) is used for hashing and persistence. */
function ownedCanonicalContentSnapshot(input: unknown): unknown {
  let serialized: string;
  try { serialized = canonicalStringify(input); } catch { throw new TypeError("Content is not canonical JSON"); }
  let clone: unknown;
  try { clone = JSON.parse(serialized) as unknown; } catch { throw new TypeError("Content is invalid"); }
  return Object.freeze(clone as object);
}

/**
 * Normalized, frozen live-abort adapter. Native AbortSignal intrinsics are
 * captured LEXICALLY at module init (the native `aborted` descriptor getter
 * and the native prototype add/remove methods); the instance's own
 * `aborted`/addEventListener/removeEventListener properties are NEVER trusted
 * or read. A genuine signal is recognized by invoking the captured NATIVE
 * getter (its internal slot rejects Object.create(AbortSignal.prototype)
 * imposters, proxies, structural fakes and cross-realm imposters — attacker
 * getters/methods are never invoked). The adapter is frozen and branded in a
 * module-private WeakSet (no symbol-property read on untrusted objects).
 */
interface FrozenAbortSignal {
  get aborted(): boolean;
  addEventListener(type: "abort", listener: () => void, options?: { once?: boolean }): void;
  removeEventListener(type: "abort", listener: () => void): void;
}
const NATIVE_ABORTED_GETTER: ((this: AbortSignal) => boolean) | undefined = Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")?.get;
const NATIVE_SIGNAL_ADD: ((type: string, listener: EventListenerOrEventListenerObject, options?: AddEventListenerOptions | boolean) => void) | undefined = AbortSignal.prototype.addEventListener;
const NATIVE_SIGNAL_REMOVE: ((type: string, listener: EventListenerOrEventListenerObject, options?: EventListenerOptions | boolean) => void) | undefined = AbortSignal.prototype.removeEventListener;
/** Module-private adapter brand: only adapters created HERE are in this WeakSet. */
const NORMALIZED_SIGNALS = new WeakSet<object>();
function normalizeSignal(signal: AbortSignal | FrozenAbortSignal | undefined): FrozenAbortSignal | undefined {
  // Type/null FIRST; the adapter brand is a WeakSet membership check (no
  // property access or trap on arbitrary objects).
  if (signal === undefined) return undefined;
  if (signal !== null && typeof signal === "object" && NORMALIZED_SIGNALS.has(signal)) return signal as FrozenAbortSignal;
  if (signal === null || typeof signal !== "object") throw new TypeError("Signal is invalid");
  if (typeof NATIVE_ABORTED_GETTER !== "function" || typeof NATIVE_SIGNAL_ADD !== "function" || typeof NATIVE_SIGNAL_REMOVE !== "function") throw new TypeError("Signal validation is unavailable");
  // Genuine native internal-slot check: calling the captured NATIVE getter on
  // the object throws for any imposter (Object.create(AbortSignal.prototype),
  // Proxy, structural, cross-realm) WITHOUT reading the instance's own
  // aborted/add/remove properties.
  let aborted: unknown;
  try {
    aborted = NATIVE_ABORTED_GETTER.call(signal as AbortSignal);
  } catch {
    throw new TypeError("Signal must be a genuine AbortSignal");
  }
  if (typeof aborted !== "boolean") throw new TypeError("Signal aborted state is invalid");
  // add/remove closures call the captured NATIVE prototype methods with the
  // genuine signal; attacker method overrides are never invoked.
  const add = (type: "abort", listener: () => void, options?: { once?: boolean }): void => NATIVE_SIGNAL_ADD.call(signal as AbortSignal, type, listener, options);
  const remove = (type: "abort", listener: () => void): void => NATIVE_SIGNAL_REMOVE.call(signal as AbortSignal, type, listener);
  const adapter: FrozenAbortSignal = Object.freeze({
    get aborted(): boolean { return NATIVE_ABORTED_GETTER.call(signal as AbortSignal) as boolean; },
    addEventListener: add,
    removeEventListener: remove,
  });
  NORMALIZED_SIGNALS.add(adapter);
  return adapter;
}

async function sleep(ms: number, signal?: FrozenAbortSignal): Promise<void> {
  if (signal?.aborted) throw new TypeError("Operation aborted");
  if (ms <= 0) return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => { signal?.removeEventListener("abort", onAbort); resolve(); }, ms);
    const onAbort = (): void => { clearTimeout(timer); reject(new TypeError("Operation aborted")); };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
function nextControl(current: ControlRecord, patch: Partial<Omit<ControlRecord, "recordType" | "id" | "ownerHost" | "schemaRevision" | "createdAt" | "contentHash">>): ControlRecord {
  const value: ControlRecord = { ...current, ...patch, version: current.version + 1, createdAt: current.createdAt, contentHash: "pending" };
  return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord;
}
async function casOrReread(store: CoordinationStore, expectedVersion: number, next: ControlRecord, message: string): Promise<ControlRecord> {
  if (await store.compareAndSwapControl(expectedVersion, next)) return store.readControl();
  throw new TypeError(message);
}
/** @internal */
async function initializeControlOnProtocol(protocol: CoordinationStore, scope: object, initial: ControlRecord): Promise<ControlRecord> {
  // Snapshot the owner EXACTLY ONCE (no post-read caller re-reads).
  const ownerHost = initial.ownerHost;
  assertBootstrapControl(initial, ownerHost);
  const stored = await protocol.readControl();
  if (stored.version !== 0) throw new TypeError("Collection control is already initialized");
  if (stored.id !== COLLECTION_CONTROL_ID || stored.ownerHost !== ownerHost || stored.privacyEpoch !== 0 || stored.coordinationPolicyEpoch !== 0 || stored.state !== "active" || stored.revokedDestinationIds.length !== 0) throw new TypeError("Bootstrap control does not match the stored point");
  return stored;
}
/** @internal */
async function beginPolicyDrainOnProtocol(protocol: CoordinationStore, scope: object, input: { now: number }): Promise<ControlRecord> {
  validClock("Drain clock", input.now);
  const current = await protocol.readControl();
  if (current.state !== "active") throw new TypeError("Collection control is not active and cannot drain");
  return casOrReread(protocol, current.version, nextControl(current, { state: "draining", activeGeneration: null }), "Concurrent policy drain lost the control CAS");
}
/** @internal */
async function waitForOldLeasesToQuiesceOnProtocol(protocol: CoordinationStore, scope: object, input: { retiredEpoch: number; maxLeaseMs: number; maxClockSkewMs: number; timeoutMs?: number; pollIntervalMs?: number; now?: () => number; signal?: AbortSignal | FrozenAbortSignal }): Promise<QuiescenceProof> {
  // Normalize the signal BEFORE any protocol read: a malformed signal can
  // never throw after a mutation has started.
  const signal = normalizeSignal(input.signal);
  const maxLeaseMs = input.maxLeaseMs;
  const maxClockSkewMs = input.maxClockSkewMs;
  const retiredEpoch = input.retiredEpoch;
  const now = input.now ?? (() => Date.now());
  const pollIntervalMs = input.pollIntervalMs ?? 25;
  const timeoutMs = input.timeoutMs ?? maxLeaseMs + maxClockSkewMs;
  validDelay("Quiescence lease window", maxLeaseMs, 86_400_000);
  validDelay("Quiescence clock skew", maxClockSkewMs, 3_600_000);
  if (!Number.isSafeInteger(retiredEpoch) || retiredEpoch < 0) throw new TypeError("Retired policy epoch is invalid");
  validDelay("Quiescence poll interval", pollIntervalMs, 60_000);
  const started = validClock("Quiescence start clock", now());
  validDelay("Quiescence timeout", timeoutMs, MAX_QUIESCENCE_TIMEOUT_MS);
  const deadline = started + timeoutMs;
  const pinned = await protocol.readControl();
  if (pinned.state !== "draining" || pinned.coordinationPolicyEpoch !== retiredEpoch) throw new TypeError("Collection control is not draining the retired epoch");
  for (;;) {
    if (signal?.aborted) throw new TypeError("Lease quiescence aborted");
    const current = validClock("Quiescence poll clock", now());
    // SCAN FIRST at the fresh `current` — including current === deadline. A
    // worst-case old lease becomes conservatively expired EXACTLY at the
    // deadline (expiresAt + skew <= current), so the boundary scan must be
    // allowed to quiesce. Only a NON-quiesced scan at/after the deadline
    // times out.
    let quiesced = true;
    let cursor: string | undefined;
    do {
      const slice = await protocol.scrollLeases(cursor, 256);
      for (const lease of slice.leases) {
        if (lease.coordinationPolicyEpoch !== retiredEpoch || (lease.state !== "leased" && lease.state !== "accepted")) continue;
        // Exact conservative equivalence: expiresAt + skew <= current is
        // quiesced; the lease is expired AT the boundary, never a false timeout.
        if (Date.parse(lease.expiresAt) + maxClockSkewMs > current) { quiesced = false; break; }
      }
      cursor = slice.nextOffset;
    } while (cursor !== undefined && quiesced);
    if (quiesced) break;
    if (current >= deadline) throw new TypeError("Lease quiescence deadline exceeded");
    // Never sleep beyond the deadline: clamp the next delay to the remaining
    // budget and rescan at the boundary.
    const remaining = deadline - current;
    const delay = Math.min(pollIntervalMs, remaining);
    await sleep(delay, signal);
  }
  const after = await protocol.readControl();
  if (after.state !== "draining" || after.version !== pinned.version || after.coordinationPolicyEpoch !== pinned.coordinationPolicyEpoch || after.coordinationPolicyHash !== pinned.coordinationPolicyHash || after.privacyEpoch !== pinned.privacyEpoch) throw new TypeError("Collection control changed during lease quiescence");
  return new QuiescenceProof(after, validClock("Quiescence completion clock", now()), scope, QUIESCENCE_PROOF_ISSUER);
}
/** @internal */
async function activatePolicyEpochOnProtocol(protocol: CoordinationStore, scope: object, input: { proof: QuiescenceProof; nextPolicyHash: string; memoryModelTimeoutMs: number; signal?: AbortSignal | FrozenAbortSignal }): Promise<ControlRecord> {
  // SECURITY AUTHORITY FIRST: snapshot ONLY the capability reference, then
  // brand + private-scope it immediately — BEFORE any other caller input field
  // or getter (signal/hash/timeout) is accessed. A forged proof can never
  // trigger unrelated malicious getters on the input object.
  const proof = input.proof;
  if (!QuiescenceProof.isValid(proof)) throw new TypeError("Policy activation requires a genuine quiescence proof");
  if (!proof.matchesScope(scope)) throw new TypeError("Quiescence proof does not match this store's authority scope");
  // Only after the genuine scope-bound proof: snapshot the remaining fields
  // EXACTLY ONCE, normalize/validate, then read/CAS. All later reads use ONLY
  // the locals.
  const signal = normalizeSignal(input.signal);
  const nextPolicyHash = input.nextPolicyHash;
  const memoryModelTimeoutMs = input.memoryModelTimeoutMs;
  validDelay("LLM timeout", memoryModelTimeoutMs, 3_600_000);
  if (typeof nextPolicyHash !== "string" || nextPolicyHash.length === 0 || nextPolicyHash.length > 512 || !/^[A-Za-z0-9._:-]+$/u.test(nextPolicyHash)) throw new TypeError("Next coordination policy hash is invalid");
  const before = await protocol.readControl();
  if (before.state !== "draining" || !proof.matches(before)) throw new TypeError("Quiescence proof does not match the current draining control");
  await sleep(memoryModelTimeoutMs, signal);
  const reread = await protocol.readControl();
  if (reread.state !== "draining" || !proof.matches(reread)) throw new TypeError("Collection control changed while awaiting the LLM timeout");
  const next = nextControl(reread, { state: "active", coordinationPolicyEpoch: reread.coordinationPolicyEpoch + 1, coordinationPolicyHash: nextPolicyHash });
  if (await protocol.compareAndSwapControl(reread.version, next)) return protocol.readControl();
  const winner = await protocol.readControl();
  if (winner.state === "active" && winner.coordinationPolicyEpoch === reread.coordinationPolicyEpoch + 1 && winner.coordinationPolicyHash === nextPolicyHash) return winner;
  throw new TypeError("Concurrent policy activation lost the control CAS");
}
/** @internal */
/** Coherent upper bound for the quiescence timeout: the internal max lease
 * window (86_400_000) plus the max skew (3_600_000) is exactly 90_000_000, so
 * every valid individual duration pair yields an accepted default timeout.
 */
const MAX_QUIESCENCE_TIMEOUT_MS = 90_000_000;

async function rotateCoordinationPolicyOnProtocol(protocol: CoordinationStore, scope: object, input: { nextPolicyHash: string; maxLeaseMs: number; maxClockSkewMs: number; memoryModelTimeoutMs: number; quiesceTimeoutMs?: number; now: number; signal?: AbortSignal }): Promise<ControlRecord> {
  // PURE PREFLIGHT: snapshot + validate the ENTIRE input BEFORE any protocol
  // read, CAS or mutation. A bad argument can never leave the collection
  // drained/stuck: every failure below happens before the first write.
  const now = input.now;
  const nextPolicyHash = input.nextPolicyHash;
  const maxLeaseMs = input.maxLeaseMs;
  const maxClockSkewMs = input.maxClockSkewMs;
  const memoryModelTimeoutMs = input.memoryModelTimeoutMs;
  const quiesceTimeoutMs = input.quiesceTimeoutMs;
  // Normalize the signal (genuine AbortSignal brand + bound methods) BEFORE
  // any read or mutation; the normalized adapter is the ONLY signal passed
  // downstream — a malformed {aborted:false}-style fake can never throw after
  // the drain.
  const signal = normalizeSignal(input.signal);
  validClock("Rotation clock", now);
  validDelay("Quiescence lease window", maxLeaseMs, 86_400_000);
  validDelay("Quiescence clock skew", maxClockSkewMs, 3_600_000);
  validDelay("LLM timeout", memoryModelTimeoutMs, 3_600_000);
  if (typeof nextPolicyHash !== "string" || nextPolicyHash.length === 0 || nextPolicyHash.length > 512 || !/^[A-Za-z0-9._:-]+$/u.test(nextPolicyHash)) throw new TypeError("Next coordination policy hash is invalid");
  const timeoutMs = quiesceTimeoutMs === undefined ? maxLeaseMs + maxClockSkewMs : quiesceTimeoutMs;
  validDelay("Quiescence timeout", timeoutMs, MAX_QUIESCENCE_TIMEOUT_MS);
  if (signal?.aborted) throw new TypeError("Operation aborted");
  // Mutation ONLY after the full preflight: drain, pin the returned draining
  // control/epoch directly (no loose extra read), quiesce, activate.
  const drained = await beginPolicyDrainOnProtocol(protocol, scope, { now });
  const retiredEpoch = drained.coordinationPolicyEpoch;
  const proof = await waitForOldLeasesToQuiesceOnProtocol(protocol, scope, { retiredEpoch, maxLeaseMs, maxClockSkewMs, timeoutMs, ...(signal === undefined ? {} : { signal }) });
  return activatePolicyEpochOnProtocol(protocol, scope, { proof, nextPolicyHash, memoryModelTimeoutMs, ...(signal === undefined ? {} : { signal }) });
}
/** @internal */
async function beginForgetBarrierOnProtocol(protocol: CoordinationStore, scope: object, input: { now: number; revokedDestinationIds?: readonly string[] }): Promise<ControlRecord> {
  const now = validClock("Forget barrier clock", input.now);
  const requestedRevocations = input.revokedDestinationIds === undefined ? [] : [...input.revokedDestinationIds];
  if (requestedRevocations.length > 256 || requestedRevocations.some((id) => !validBoundedText(id, 256) || SECRET.test(id)) || new Set(requestedRevocations).size !== requestedRevocations.length) throw new TypeError("Forget barrier destination revocations are invalid");
  const current = await protocol.readControl();
  const revokedDestinationIds = [...new Set([...current.revokedDestinationIds, ...requestedRevocations])].sort();
  const barrier = new Date(now).toISOString();
  return casOrReread(protocol, current.version, nextControl(current, { privacyEpoch: current.privacyEpoch + 1, activeGeneration: null, lastForgetBarrier: barrier, revokedDestinationIds }), "Concurrent forget barrier lost the control CAS");
}
