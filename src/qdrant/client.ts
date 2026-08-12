import { fetchJson, fetchOk, MemoryClientError } from "../clients/http.js";
import { canonicalStringify, deterministicUuid } from "../domain/canonical.js";
import { assertBootstrapControl, collectionControlPoint, collectionMetadataPoint, collectionVectors, isPhysicalPointId, type PayloadIndexSchema, type PointRecordType } from "./schema.js";
import type { ControlRecord } from "../domain/records.js";
import type { HostId } from "../types.js";

type JsonRecord = Record<string, unknown>;
type Consistency = number | "majority" | "quorum" | "all";
export type PointId = string;
export type HostScopedQdrantCollection = "pi_memory" | "prime_memory";
/** The only collection an owner may write or read through this client family. */
export function expectedQdrantCollection(ownerHost: HostId): HostScopedQdrantCollection { return ownerHost === "pi" ? "pi_memory" : "prime_memory"; }
export interface QdrantClientOptions { baseUrl: string; collection: string; ownerHost: HostId; apiKey?: string; timeoutMs: number; fetchImpl?: typeof fetch; readConsistency?: Consistency; maxClockSkewMs?: number; signal?: AbortSignal; replicationFactor?: number; writeConsistencyFactor?: number; }
export type ReadPurpose = "memory" | "control" | "metadata" | "query" | "internal" | "write_verification";
export interface QdrantReadPolicy { ownerHost: HostId; purpose: ReadPurpose; recordTypes: readonly PointRecordType[]; now: number; maxClockSkewMs: number; requireStatus: "active"; requireSecretScan: "passed"; projectId?: string; processingPolicyId?: string; }
export interface ReadOptions { includeVector?: boolean; }
export interface QdrantPoint { id: PointId; payload: JsonRecord; vector?: { semantic: number[] }; }
export interface QdrantCollectionInfo { status?: string; dimension: 1024; distance: "Cosine"; vectors: { semantic: { size: 1024; distance: "Cosine" } }; pointsCount: number | null; payloadSchema?: JsonRecord; raw: unknown; }
export interface QdrantScrollResult { points: QdrantPoint[]; nextOffset?: PointId; }
export interface QdrantSearchHit { id: PointId; score: number; payload: JsonRecord; }
export interface PreparedPoint { id: PointId; payload: JsonRecord; vector?: { semantic: readonly number[] }; }
export type ControlCasPrecondition = { kind: "collection-control-cas"; ownerHost: HostId; recordType: "collection_control"; expectedVersion: number; expectedEpoch: number; expectedPrivacyEpoch: number; expectedState: "active" | "draining" | "retired"; expectedBaseGeneration?: string | null };
/**
 * Typed lease/claim CAS: pins owner, version, fencing token, policy hash+epoch,
 * privacy epoch, state, exact current acceptance, and optional conservative
 * expiry cut (steal/reacquire) or live-expiry floor (acceptance). The claim
 * point is the single acceptance authority.
 */
export type LeaseCasPrecondition = {
  kind: "lease-cas";
  ownerHost: HostId;
  recordType: "lease";
  jobId: string;
  expectedVersion: number;
  expectedFencingToken: number;
  expectedPolicyEpoch: number;
  expectedPolicyHash: string;
  expectedPrivacyEpoch: number;
  expectedState: "leased" | "accepted" | "released";
  /** Every lease CAS pins the exact current owner (never null). */
  expectedOwner: string;
  expectedAcceptedProposalId: string | null;
  expectedAcceptedManifestHash: string | null;
  /** Immutable-current binding: exact processing-policy intersection, createdAt and canonical content hash. */
  expectedProcessingPolicyId: string;
  expectedCreatedAt: string;
  expectedContentHash: string;
  expiresBefore?: number;
  expiresAfter?: number;
};
export type TypedUpdatePrecondition = ControlCasPrecondition | LeaseCasPrecondition;
export interface QdrantReadClient { readonly endpoint: string; readonly ownerHost: HostId; readonly collection: HostScopedQdrantCollection; readonly maxClockSkewMs: number; health(): Promise<unknown>; collectionInfo(): Promise<QdrantCollectionInfo>; retrieve(ids: readonly PointId[], policy: QdrantReadPolicy, options?: ReadOptions): Promise<QdrantPoint[]>; scroll(input: { policy: QdrantReadPolicy; offset?: PointId; limit?: number }): Promise<QdrantScrollResult>; search(input: { vector: readonly number[]; limit: number; policy: QdrantReadPolicy }): Promise<QdrantSearchHit[]>; count(policy: QdrantReadPolicy): Promise<number>; }
export function validatePurpose(purpose: ReadPurpose, recordTypes: readonly PointRecordType[]): void { if (!["memory", "control", "metadata", "query", "internal", "write_verification"].includes(purpose)) throw new TypeError("Read purpose is invalid"); if ((purpose === "metadata" && (recordTypes.length !== 1 || recordTypes[0] !== "collection_metadata")) || (purpose === "control" && (recordTypes.length !== 1 || recordTypes[0] !== "collection_control")) || ((purpose === "memory" || purpose === "query") && (recordTypes.length === 0 || recordTypes.some((type) => !["episode", "curated_memory", "curated_current", "raptor_summary"].includes(type)))) || ((purpose === "internal" || purpose === "write_verification") && recordTypes.length === 0)) throw new TypeError("Read purpose and record types do not match"); }
export function readPolicy(input: { ownerHost: HostId; purpose: ReadPurpose; recordTypes: readonly PointRecordType[]; now?: number; maxClockSkewMs?: number; projectId?: string; processingPolicyId?: string }): QdrantReadPolicy {
  // GLOBAL RULE: snapshot every field EXACTLY ONCE; the returned policy is
  // built EXPLICITLY (no caller spread — smuggled keys are never carried).
  const ownerHost = input.ownerHost;
  const purpose = input.purpose;
  const recordTypes = [...input.recordTypes];
  const projectId = input.projectId;
  const processingPolicyId = input.processingPolicyId;
  const now = input.now ?? Date.now();
  const skew = input.maxClockSkewMs ?? 0;
  if (ownerHost !== "pi" && ownerHost !== "prime") throw new TypeError("Read owner is invalid");
  if (recordTypes.length === 0 || recordTypes.some((type) => !["episode", "curated_memory", "curated_current", "raptor_summary", "collection_control", "processing_policy", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone", "collection_metadata"].includes(type))) throw new TypeError("Read record type policy is invalid");
  if (!Number.isFinite(now) || !Number.isFinite(skew) || skew < 0) throw new TypeError("Read expiry policy is invalid");
  if (projectId !== undefined && (typeof projectId !== "string" || projectId.length === 0) || processingPolicyId !== undefined && (typeof processingPolicyId !== "string" || processingPolicyId.length === 0)) throw new TypeError("Read scope policy is invalid");
  validatePurpose(purpose, recordTypes);
  const policy: QdrantReadPolicy = { ownerHost, purpose, recordTypes, now, maxClockSkewMs: skew, requireStatus: "active", requireSecretScan: "passed" };
  if (projectId !== undefined) policy.projectId = projectId;
  if (processingPolicyId !== undefined) policy.processingPolicyId = processingPolicyId;
  return policy;
}

export type QdrantReadCapabilities = QdrantReadClient;
export function physicalPointIdFor(recordType: string, logicalId: string): string {
  return isPhysicalPointId(logicalId) ? logicalId : deterministicUuid("pi-qdrant-memory-v2:point", recordType, logicalId);
}
