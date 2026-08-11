import { type PayloadIndexSchema, type PointRecordType } from "./schema.js";
import type { ControlRecord } from "../domain/records.js";
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
export interface ControlUpdatePrecondition {
    kind: "collection-control-cas";
    ownerHost: HostId;
    recordType: "collection_control";
    expectedVersion: number;
    expectedEpoch: number;
    expectedPrivacyEpoch: number;
    expectedState: "active" | "draining" | "retired";
    expectedBaseGeneration?: string | null;
}
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
export interface QdrantSessionWriter extends QdrantReadClient {
    upsertPoints(points: readonly PreparedPoint[], mode: "insert_only" | "update_only", precondition?: ControlUpdatePrecondition): Promise<void>;
}
export interface QdrantAdminClient extends QdrantReadClient {
    createCollection(): Promise<void>;
    createPayloadIndex(field: string, schema: PayloadIndexSchema): Promise<void>;
    deletePoints(ids: readonly PointId[]): Promise<void>;
}
export declare function readPolicy(input: {
    ownerHost: HostId;
    purpose: ReadPurpose;
    recordTypes: readonly PointRecordType[];
    now?: number;
    maxClockSkewMs?: number;
    projectId?: string;
    processingPolicyId?: string;
}): QdrantReadPolicy;
declare class RestQdrantReadClient implements QdrantReadClient {
    readonly endpoint: string;
    readonly ownerHost: HostId;
    readonly collection: HostScopedQdrantCollection;
    readonly maxClockSkewMs: number;
    protected readonly options: QdrantClientOptions;
    constructor(options: QdrantClientOptions);
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
declare class RestQdrantSessionWriter extends RestQdrantReadClient implements QdrantSessionWriter {
    upsertPoints(points: readonly PreparedPoint[], mode: "insert_only" | "update_only", precondition?: ControlUpdatePrecondition): Promise<void>;
}
declare class RestQdrantAdminClient extends RestQdrantReadClient implements QdrantAdminClient {
    constructor(options: QdrantClientOptions & {
        apiKey: string;
    });
    createCollection(): Promise<void>;
    createPayloadIndex(field: string, schema: PayloadIndexSchema): Promise<void>;
    deletePoints(ids: readonly PointId[]): Promise<void>;
    insertMetadataPoint(owner: HostId): Promise<void>;
    insertInitialControlPoint(control: ControlRecord): Promise<void>;
    serverInfo(): Promise<{
        version: string;
    }>;
}
export type QdrantReadCapabilities = QdrantReadClient;
export type QdrantSessionWriteCapabilities = QdrantSessionWriter;
export declare const QdrantReadClient: typeof RestQdrantReadClient;
export declare const QdrantSessionWriter: typeof RestQdrantSessionWriter;
export declare const QdrantAdminClient: typeof RestQdrantAdminClient;
export declare function createQdrantReadClient(options: QdrantClientOptions): QdrantReadClient;
export declare function createQdrantSessionWriter(options: QdrantClientOptions): QdrantSessionWriter;
export declare function createQdrantAdminClient(options: QdrantClientOptions & {
    apiKey: string;
}): QdrantAdminClient;
export declare const qdrantReadClient: typeof createQdrantReadClient;
export declare const sessionWriter: typeof createQdrantSessionWriter;
export declare const adminClient: typeof createQdrantAdminClient;
export {};
