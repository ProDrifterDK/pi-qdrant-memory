import type { RuntimeConfig } from "../types.js";
export interface InspectOptions {
    ids?: readonly string[];
    recordTypes?: readonly string[];
    limit?: number;
}
export interface InspectRecordSource {
    read?(input: InspectOptions): Promise<readonly unknown[]>;
    records?: readonly unknown[];
}
export interface SafeInspectRecord {
    readonly id?: string;
    readonly recordType?: string;
    readonly ownerHost?: string;
    readonly [key: string]: unknown;
}
export interface InspectResult {
    readonly ok: true;
    readonly records: readonly SafeInspectRecord[];
    readonly count: number;
    readonly truncated: boolean;
    readonly contentHash: string;
}
/** Project metadata only; raw text, vectors, payloads, credentials and tool
 * material never cross the operator inspection boundary. */
export declare function redactInspectRecord(value: unknown): SafeInspectRecord;
/** Bounded deterministic operator inspection. */
export declare function inspectRecords(options?: InspectOptions, source?: InspectRecordSource): Promise<InspectResult>;
/** Bounded production read path. It uses the named read-only transport and
 * projects wire payloads before the generic allowlist redactor sees them. */
export declare function inspectQdrantRecords(config: RuntimeConfig, options?: InspectOptions, fetchImpl?: typeof fetch): Promise<InspectResult>;
export declare const inspect: typeof inspectRecords;
export declare const boundedInspect: typeof inspectRecords;
