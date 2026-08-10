import type { RuntimeConfig } from "../types.js";
export interface InitializeDestinationResult {
    host: RuntimeConfig["host"];
    collection: string;
    ownerHost: RuntimeConfig["host"];
    schema: "pi-qdrant-memory-v2";
    schemaRevision: 1;
    vector: {
        name: "semantic";
        model: string;
        dimension: 1024;
        distance: "Cosine";
    };
    capture: {
        enabled: boolean;
        episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
    };
    initialized: false;
}
export interface InitializeDestinationDependencies {
    signal?: AbortSignal;
}
/**
 * Task 1 deliberately exposes a destination-only contract shell. Network
 * initialization and collection mutation are owned by the Qdrant task.
 */
export declare function initializeDestination(config: RuntimeConfig, _deps?: InitializeDestinationDependencies): Promise<InitializeDestinationResult>;
