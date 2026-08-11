import { type EpisodeRecord, type ProcessingPolicyRecord } from "../domain/records.js";
import { type ProcessingPolicy } from "../domain/policy.js";
import type { AuthorizedDestination } from "../types.js";
import type { OutboxFileSystem, OutboxJob } from "./store.js";
export interface OutboxJobProcessor {
    process(job: OutboxJob, input: {
        signal?: AbortSignal;
    }): Promise<{
        status: "delivered" | "pending" | "quarantined";
        category?: string;
    }>;
}
export interface DeliveryResult {
    delivered: number;
    pending: number;
    quarantined: number;
}
export interface DeliveryInput {
    outboxRoot: string;
    producerPath: string;
    processor: OutboxJobProcessor;
    now: () => number;
    maxClockSkewMs: number;
    retryBaseMs?: number;
    retryMaxMs?: number;
    heartbeatTimeoutMs?: number;
    attemptTimeoutMs?: number;
    fs?: Partial<OutboxFileSystem>;
}
export interface OutboxDelivery {
    deliver(input: {
        signal?: AbortSignal;
        maxJobs?: number;
    }): Promise<DeliveryResult>;
    adopt(producerPath: string): Promise<void>;
    shutdown(input?: {
        signal?: AbortSignal;
        maxJobs?: number;
    }): Promise<DeliveryResult>;
}
export declare function createOutboxDelivery(input: DeliveryInput): OutboxDelivery;
/** The only control surface Task 7 needs; Task 8/13 must provide its bounded revocation snapshot. */
export interface IngestControlReader {
    read(): Promise<{
        state: "active" | "draining" | "retired";
        privacyEpoch: number;
        coordinationPolicyEpoch: number;
        policyHash: string;
        revokedDestinationIds: readonly string[];
    }>;
}
/** Opaque Qdrant capability created only by the validated destination factory. */
export interface BoundQdrantDestination {
    readonly destination: AuthorizedDestination;
    /** Immutable host/physical collection pairing of the opaque writer. */
    readonly ownerHost: "pi" | "prime";
    readonly collection: "pi_memory" | "prime_memory";
    /** Independently pinned control policy; never an episode processing-policy ID. */
    readonly coordination: {
        readonly policyHash: string;
        readonly policyEpoch: number;
    };
    insertAndReadback(record: ProcessingPolicyRecord | EpisodeRecord): Promise<"inserted" | "existing">;
    retrieve<T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string): Promise<T | null>;
}
/** Opaque BGE-M3-only capability created only by the validated destination factory. */
export interface BoundEmbeddingDestination {
    readonly destination: AuthorizedDestination;
    embed(input: {
        model: string;
        text: string;
        signal?: AbortSignal;
    }): Promise<readonly number[]>;
}
export interface IngestInput {
    job: OutboxJob;
    now: number;
    localPolicy: ProcessingPolicy;
    qdrant: BoundQdrantDestination;
    embedding: BoundEmbeddingDestination;
    control: IngestControlReader;
    maxClockSkewMs: number;
}
/**
 * Ingest a durable job without throwing into a host turn.  Its public `now`
 * value is fixed for deterministic direct callers; the production processor
 * below supplies its live clock so expiry is checked again after embedding.
 */
export declare function ingestPendingJobs(input: IngestInput): Promise<DeliveryResult>;
/** The sole production OutboxJobProcessor; Task 5 remains scheduling-only. */
export declare function createIngestProcessor(input: Omit<IngestInput, "job" | "now"> & {
    now: () => number;
}): OutboxJobProcessor;
