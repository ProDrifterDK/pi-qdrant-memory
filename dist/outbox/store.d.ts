import * as nodeFs from "node:fs/promises";
import type { HostId } from "../types.js";
import type { EpisodeRecord } from "../domain/records.js";
import type { ProcessingPolicy } from "../domain/policy.js";
export type OutboxFileSystem = Pick<typeof nodeFs, "chmod" | "link" | "lstat" | "mkdir" | "open" | "readFile" | "readdir" | "realpath" | "rename" | "rm" | "stat">;
export interface OutboxJob {
    readonly version: 1;
    readonly id: string;
    readonly ownerHost: HostId;
    readonly nodeId: string;
    readonly producerUuid: string;
    readonly createdAt: string;
    readonly deadline: string | null;
    readonly policyId: string;
    readonly policy: ProcessingPolicy;
    readonly episodeIds: readonly string[];
    readonly episodes: readonly EpisodeRecord[];
    readonly auditHash: string;
}
export interface StoredOutboxJob extends OutboxJob {
    readonly file: string;
}
export interface EnqueueInput {
    episodes: readonly EpisodeRecord[];
    policy: ProcessingPolicy;
}
export interface OutboxStatus {
    readonly state: "active" | "closed";
    readonly nodeId: string;
    readonly producerUuid: string;
    readonly jobs: number;
    readonly bytes: number;
    readonly oldestCreatedAt: string | null;
    readonly failedAttempts: number;
    readonly heartbeatAt: number;
    readonly captureAllowed: boolean;
}
export interface Outbox {
    readonly root: string;
    readonly producerPath: string;
    readonly nodeId: string;
    readonly producerUuid: string;
    enqueue(input: EnqueueInput): Promise<StoredOutboxJob>;
    listPending(): Promise<StoredOutboxJob[]>;
    quarantine(job: StoredOutboxJob | OutboxJob | string, category: string): Promise<void>;
    heartbeat(): Promise<void>;
    closeProducer(): Promise<void>;
    outboxStatus(): Promise<OutboxStatus>;
}
export interface CreateOutboxInput {
    host: HostId;
    homeDir: string;
    env?: Record<string, string | undefined>;
    nodeId?: string;
    producerUuid?: string;
    machineId?: string;
    sharedFilesystem?: boolean;
    maxJobs?: number;
    maxBytes?: number;
    now?: () => number;
    randomBytes?: (size: number) => Uint8Array;
    fs?: Partial<OutboxFileSystem>;
    notifyFull?: (status: Readonly<Pick<OutboxStatus, "jobs" | "bytes" | "captureAllowed">>) => void;
}
export declare function parseOutboxJob(value: unknown, expected?: {
    host?: HostId;
    nodeId?: string;
    producerUuid?: string;
    homeDir?: string;
}): OutboxJob;
/** Resolve and persist the pseudonymous installation node identity without creating a producer. */
export declare function resolveOutboxNodeId(input: Omit<CreateOutboxInput, "producerUuid" | "maxJobs" | "maxBytes" | "notifyFull">): Promise<string>;
export declare class OutboxCapacityError extends Error {
    constructor();
}
export declare function createOutbox(input: CreateOutboxInput): Promise<Outbox>;
