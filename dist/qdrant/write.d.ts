import { type ControlRecord, type MemoryRecord } from "../domain/records.js";
import { type QdrantSessionWriter } from "./client.js";
/** Insert-only is at-least-once: preflight and postflight reads classify observed state; a concurrent race is inherently ambiguous. */
export declare function insertOnly<T extends MemoryRecord>(client: QdrantSessionWriter, record: T): Promise<"inserted" | "existing">;
export declare function insertInitialControl(client: QdrantSessionWriter, control: ControlRecord): Promise<"inserted" | "existing">;
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
