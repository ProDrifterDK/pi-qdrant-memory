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
