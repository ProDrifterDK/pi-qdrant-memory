import { type ControlRecord, type MemoryRecord } from "../domain/records.js";
import type { AuthorizedDestination, RuntimeConfig } from "../types.js";
import type { BoundQdrantDestination } from "../outbox/delivery.js";
import { type QdrantSessionWriter } from "./client.js";
export { QdrantContentHashCollisionError, QDRANT_CONTENT_HASH_COLLISION } from "../domain/qdrant-errors.js";
/** Truthful minimum capability required by insert/readback verification. */
export type QdrantWriteVerificationClient = Pick<QdrantSessionWriter, "endpoint" | "ownerHost" | "collection" | "maxClockSkewMs" | "retrieve" | "upsertPoints">;
/** Insert-only is at-least-once: preflight and postflight reads classify observed state; a concurrent race is inherently ambiguous. */
export declare function insertOnly<T extends MemoryRecord>(client: QdrantWriteVerificationClient, record: T): Promise<"inserted" | "existing">;
export declare function insertInitialControl(client: QdrantWriteVerificationClient, control: ControlRecord): Promise<"inserted" | "existing">;
export declare function updateOnlyCas(client: QdrantSessionWriter, input: {
    id: string;
    expectedVersion: number;
    expectedEpoch: number;
    patch: Record<string, unknown>;
}): Promise<boolean>;
export declare function publishControlCas(client: QdrantSessionWriter, input: {
    expectedVersion: number;
    expectedBaseGeneration: string | null;
    next: ControlRecord;
}): Promise<boolean>;
export type SessionWriter = QdrantSessionWriter;
/** A nominal, endpoint-pinned writer capability; factories never accept a raw structural client. */
export declare class ValidatedQdrantSessionWriter {
    #private;
    readonly endpoint: string;
    readonly ownerHost: "pi" | "prime";
    readonly collection: "pi_memory" | "prime_memory";
    private constructor();
    writer(): QdrantWriteVerificationClient;
    static bind(input: {
        endpoint: string;
        client: QdrantWriteVerificationClient;
    }): ValidatedQdrantSessionWriter;
}
/** Explicit factory seam for endpoint-bound production writers and test fakes. */
export declare function bindQdrantSessionWriter(input: {
    endpoint: string;
    client: QdrantWriteVerificationClient;
}): ValidatedQdrantSessionWriter;
/** Factory-only configuration for the opaque Task 7 Qdrant egress capability. */
export interface QdrantDestinationFactoryInput {
    endpoint: string;
    destination: AuthorizedDestination;
    client: ValidatedQdrantSessionWriter;
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    nodeId?: string;
    coordinationPolicyHash: string;
    coordinationPolicyEpoch: number;
}
export interface QdrantDestinationFactory {
    bind(destination: AuthorizedDestination): BoundQdrantDestination;
}
/** Create a closure that snapshots one canonical endpoint/client/destination pairing. */
export declare function createQdrantDestinationFactory(input: QdrantDestinationFactoryInput): QdrantDestinationFactory;
/** Bind an exact expected identity; callers cannot pass an independent allowlist. */
export declare function bindQdrantDestination(factory: QdrantDestinationFactory, destination: AuthorizedDestination): BoundQdrantDestination;
