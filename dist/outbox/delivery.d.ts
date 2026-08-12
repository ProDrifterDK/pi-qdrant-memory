import { type ProcessingPolicy } from "../domain/policy.js";
import { BoundIngestRuntime } from "../coordination/ingest.js";
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
/**
 * Task 8 tombstone barrier: returns the subset of episode IDs that are
 * logically tombstoned at read time (batch-read with configured consistency).
 */
export interface IngestTombstoneReader {
    readTombstoned(episodeIds: readonly string[]): Promise<readonly string[]>;
}
export interface IngestInput {
    job: OutboxJob;
    now: number;
    localPolicy: ProcessingPolicy;
    /** ONE production ingest bundle (store + qdrant + embedding + control + tombstones), brand-checked. */
    runtime: BoundIngestRuntime;
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
