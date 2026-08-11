import type { HostId } from "../types.js";
import { type EffectiveOrder } from "./ids.js";
import { type ProcessingPolicy } from "./policy.js";
export declare const RECORD_SCHEMA_REVISION = 1;
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
export declare function episodeSemanticProjection(episode: Pick<EpisodeRecord, "eventKind" | "text" | "toolName" | "toolArgs" | "errorFingerprint">): string;
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
}
export interface ProcessingPolicyRecord extends RecordEnvelope {
    recordType: "processing_policy";
    id: string;
    policy: ProcessingPolicy;
    canonicalHash: string;
    expiresAt: string | null;
}
export interface JobRecord extends DerivedEnvelope {
    recordType: "job";
    id: string;
    policyId: string;
    policyHash: string;
    policyEpoch: number;
    membership: string[];
    state: "pending" | "leased" | "accepted" | "completed" | "failed" | "retired";
    leaseExpiresAt: string | null;
    fencingToken: number;
    leaseOwner: string | null;
    acceptedProposalId: string | null;
    acceptedManifestHash: string | null;
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
export type MemoryRecord = EpisodeRecord | CuratedMemoryRecord | CuratedCurrentRecord | RaptorSummaryRecord | ControlRecord | ProcessingPolicyRecord | JobRecord | CoverageRecord | EvidenceLinkRecord | TombstoneRecord;
export interface RecordValidationContext {
    ownerHost?: HostId;
    schemaRevision?: number;
    privacyEpoch?: number;
    policyEpoch?: number;
    coordinationPolicyEpoch?: number;
    maxTextChars?: number;
    vectorDimension?: number;
}
export declare function parseMemoryRecord(value: unknown, context?: RecordValidationContext): MemoryRecord;
export declare function assertMemoryRecord(value: unknown, context?: RecordValidationContext): asserts value is MemoryRecord;
export declare function isMemoryRecord(value: unknown, context?: RecordValidationContext): value is MemoryRecord;
export declare function canonicalRecordHash(record: MemoryRecord): string;
export declare function assertCanonicalRecordHash(record: MemoryRecord): void;
export declare function parsePersistedMemoryRecord(value: unknown, context?: RecordValidationContext): MemoryRecord;
export declare function isPersistedMemoryRecord(value: unknown, context?: RecordValidationContext): value is MemoryRecord;
export {};
