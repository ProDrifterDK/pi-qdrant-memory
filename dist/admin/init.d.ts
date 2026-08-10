import { type ControlRecord } from "../domain/records.js";
import type { RuntimeConfig } from "../types.js";
export interface InitializeDestinationResult {
    host: RuntimeConfig["host"];
    collection: string;
    ownerHost: RuntimeConfig["host"];
    schema: "pi-qdrant-memory-v2";
    schemaRevision: 1;
    vector: {
        name: "semantic";
        model: "bge-m3";
        dimension: 1024;
        distance: "Cosine";
    };
    capture: {
        enabled: boolean;
        episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
    };
    initialized: boolean;
    collectionCreated: boolean;
    qdrantVersion?: string;
}
export interface InitializeDestinationDependencies {
    signal?: AbortSignal;
    fetchImpl?: typeof fetch;
    adminApiKey?: string;
    now?: () => number;
    initialControl?: ControlRecord;
    retryAttempts?: number;
    retryDelayMs?: number;
}
/** Destination initialization never consults ambient process credentials. */
export declare function initializeDestination(config: RuntimeConfig, deps?: InitializeDestinationDependencies): Promise<InitializeDestinationResult>;
