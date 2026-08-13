import { type ControlRecord, type MemoryRecord, type ProcessingPolicyRecord, type TombstoneRecord } from "../domain/records.js";
import type { AuthorizedDestination, HostId, RetrievalConfig, RuntimeConfig } from "../types.js";
import type { ProjectIdentity } from "../project.js";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
import { GuardedLaneFilter, type RetrievalLane } from "./filters.js";
import type { MemorySearchMode } from "../tool.js";
import { type QdrantClientOptions } from "../qdrant/client.js";
export type CandidateLane = "current" | "historical" | "episodes" | "curated" | "raptor" | "exact";
export interface MemoryCandidate {
    id: string;
    text: string;
    rawScore: number;
    adjustedScore: number;
    lane: CandidateLane;
    recordType: "episode" | "curated_memory" | "curated_current";
    projectId?: string;
    scope?: string;
    projectLabel?: string;
    sourceType: string;
    sourceSystem: string;
    createdAt?: string;
    expiresAt?: string;
    validFrom?: string;
    validTo?: string;
    policyEpoch?: number;
    evidenceIds: string[];
    contentId?: string;
    observationId?: string;
    stateKey?: string;
    processingPolicyId: string;
    authorizationPolicyIds?: string[];
    privacyEpoch: number;
}
export interface MemorySearchResult {
    query: string;
    hits: MemoryCandidate[];
}
export interface RankedMemoryRecord {
    record: MemoryRecord;
    score: number;
}
export interface MemoryRecordRef {
    recordType: MemoryRecord["recordType"];
    id: string;
}
export interface MemoryReadRequest {
    lane: RetrievalLane;
    filter: GuardedLaneFilter;
    limit: number;
    vector?: readonly number[];
    signal?: AbortSignal;
}
export interface ExactReadRequest {
    query: string;
    filter: GuardedLaneFilter;
    limit: number;
    signal?: AbortSignal;
}
export interface MemoryReadStore {
    readonly destination: AuthorizedDestination;
    readControl(): Promise<ControlRecord>;
    search(input: MemoryReadRequest): Promise<RankedMemoryRecord[]>;
    exact(input: ExactReadRequest): Promise<RankedMemoryRecord[]>;
    retrieve(refs: readonly MemoryRecordRef[]): Promise<MemoryRecord[]>;
    retrieveEvidence(ids: readonly string[]): Promise<MemoryRecord[]>;
    readPolicies(ids: readonly string[]): Promise<ProcessingPolicyRecord[]>;
    readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]>;
    health(signal?: AbortSignal): Promise<void>;
    collectionInfo(signal?: AbortSignal): Promise<{
        dimension: number;
        distance: string;
    }>;
}
export interface MemoryRetrieverInput {
    query: string;
    host: HostId;
    project: ProjectIdentity;
    isChild: boolean;
    modelDestination: AuthorizedDestination;
    mode?: MemorySearchMode;
    after?: string;
    before?: string;
    limit?: number;
    signal?: AbortSignal;
}
/** Chronological historical intervals within one policy/state stream. Only adjacent equal content is collapsed, so A→B→A remains visible. */
export declare function historicalIntervals(input: readonly MemoryCandidate[]): MemoryCandidate[];
/** Guarded hybrid retrieval. Every result passes a final stable-control, policy and tombstone barrier. */
export declare class MemoryRetriever {
    private readonly dependencies;
    constructor(dependencies: {
        reader: MemoryReadStore;
        config: RetrievalConfig;
        embedding?: BoundEmbeddingDestination;
        embeddingDestination?: AuthorizedDestination;
        resolveEmbedding?: (control: ControlRecord, signal?: AbortSignal) => Promise<{
            embedding: BoundEmbeddingDestination;
            destination: AuthorizedDestination;
        } | undefined>;
        queryPrefix?: string;
        maxClockSkewMs?: number;
        now?: () => number;
    });
    search(input: MemoryRetrieverInput): Promise<MemorySearchResult>;
}
export interface GuardedMemoryReadStoreOptions extends QdrantClientOptions {
    destination: AuthorizedDestination;
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    nodeId?: string;
}
export declare class GuardedMemoryReadStore implements MemoryReadStore {
    #private;
    readonly destination: AuthorizedDestination;
    constructor(options: GuardedMemoryReadStoreOptions, issuer: symbol);
    static isValid(value: unknown): value is GuardedMemoryReadStore;
    health(signal?: AbortSignal): Promise<void>;
    collectionInfo(signal?: AbortSignal): Promise<{
        dimension: number;
        distance: string;
    }>;
    search(input: MemoryReadRequest): Promise<RankedMemoryRecord[]>;
    exact(input: ExactReadRequest): Promise<RankedMemoryRecord[]>;
    retrieve(refs: readonly MemoryRecordRef[]): Promise<MemoryRecord[]>;
    retrieveEvidence(ids: readonly string[]): Promise<MemoryRecord[]>;
    readControl(): Promise<ControlRecord>;
    readPolicies(ids: readonly string[]): Promise<ProcessingPolicyRecord[]>;
    readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]>;
}
export declare function createGuardedMemoryReadStore(options: GuardedMemoryReadStoreOptions): GuardedMemoryReadStore;
