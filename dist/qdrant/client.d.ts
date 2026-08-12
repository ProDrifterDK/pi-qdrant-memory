import { type PointRecordType } from "./schema.js";
import type { HostId } from "../types.js";
type JsonRecord = Record<string, unknown>;
type Consistency = number | "majority" | "quorum" | "all";
export type PointId = string;
export type HostScopedQdrantCollection = "pi_memory" | "prime_memory";
/** The only collection an owner may write or read through this client family. */
export declare function expectedQdrantCollection(ownerHost: HostId): HostScopedQdrantCollection;
export interface QdrantClientOptions {
    baseUrl: string;
    collection: string;
    ownerHost: HostId;
    apiKey?: string;
    timeoutMs: number;
    fetchImpl?: typeof fetch;
    readConsistency?: Consistency;
    maxClockSkewMs?: number;
    signal?: AbortSignal;
    replicationFactor?: number;
    writeConsistencyFactor?: number;
}
export type ReadPurpose = "memory" | "control" | "metadata" | "query" | "internal" | "write_verification";
export interface QdrantReadPolicy {
    ownerHost: HostId;
    purpose: ReadPurpose;
    recordTypes: readonly PointRecordType[];
    now: number;
    maxClockSkewMs: number;
    requireStatus: "active";
    requireSecretScan: "passed";
    projectId?: string;
    processingPolicyId?: string;
}
export interface ReadOptions {
    includeVector?: boolean;
}
export interface QdrantPoint {
    id: PointId;
    payload: JsonRecord;
    vector?: {
        semantic: number[];
    };
}
export interface QdrantCollectionInfo {
    status?: string;
    dimension: 1024;
    distance: "Cosine";
    vectors: {
        semantic: {
            size: 1024;
            distance: "Cosine";
        };
    };
    pointsCount: number | null;
    payloadSchema?: JsonRecord;
    raw: unknown;
}
export interface QdrantScrollResult {
    points: QdrantPoint[];
    nextOffset?: PointId;
}
export interface QdrantSearchHit {
    id: PointId;
    score: number;
    payload: JsonRecord;
}
export interface PreparedPoint {
    id: PointId;
    payload: JsonRecord;
    vector?: {
        semantic: readonly number[];
    };
}
export type ControlCasPrecondition = {
    kind: "collection-control-cas";
    ownerHost: HostId;
    recordType: "collection_control";
    expectedVersion: number;
    expectedEpoch: number;
    expectedPrivacyEpoch: number;
    expectedState: "active" | "draining" | "retired";
    expectedBaseGeneration?: string | null;
};
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
export interface QdrantReadClient {
    readonly endpoint: string;
    readonly ownerHost: HostId;
    readonly collection: HostScopedQdrantCollection;
    readonly maxClockSkewMs: number;
    health(): Promise<unknown>;
    collectionInfo(): Promise<QdrantCollectionInfo>;
    retrieve(ids: readonly PointId[], policy: QdrantReadPolicy, options?: ReadOptions): Promise<QdrantPoint[]>;
    scroll(input: {
        policy: QdrantReadPolicy;
        offset?: PointId;
        limit?: number;
    }): Promise<QdrantScrollResult>;
    search(input: {
        vector: readonly number[];
        limit: number;
        policy: QdrantReadPolicy;
    }): Promise<QdrantSearchHit[]>;
    count(policy: QdrantReadPolicy): Promise<number>;
}
export declare function validatePurpose(purpose: ReadPurpose, recordTypes: readonly PointRecordType[]): void;
export declare function readPolicy(input: {
    ownerHost: HostId;
    purpose: ReadPurpose;
    recordTypes: readonly PointRecordType[];
    now?: number;
    maxClockSkewMs?: number;
    projectId?: string;
    processingPolicyId?: string;
}): QdrantReadPolicy;
export type QdrantReadCapabilities = QdrantReadClient;
export declare function physicalPointIdFor(recordType: string, logicalId: string): string;
export {};
