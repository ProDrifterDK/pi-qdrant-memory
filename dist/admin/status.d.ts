import type { RuntimeConfig } from "../types.js";
export interface MemoryStatus {
    host: RuntimeConfig["host"];
    configPath: string;
    enabled: boolean;
    autoRecall: boolean;
    destination: {
        endpoint: string;
        collection: string;
        ownerHost: RuntimeConfig["host"];
        schema: "pi-qdrant-memory-v2";
        dimension: 1024;
        distance: "Cosine";
        exists: boolean;
        healthy: boolean;
        keyConfigured: boolean;
    };
    embeddings: {
        endpoint: string;
        model: string;
        dimension: 1024;
        healthy: boolean;
        keyConfigured: boolean;
    };
    capture: {
        enabled: boolean;
        episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
    };
    privacy: {
        egressMode: RuntimeConfig["privacy"]["egressMode"];
        qdrantDestinations: number;
        embeddingDestinations: number;
        llmDestinations: number;
    };
    qdrant: {
        healthy: boolean;
        destinationHealthy: boolean;
        probed: false;
    };
}
export interface MemoryStatusDependencies {
    signal?: AbortSignal;
}
/** Return the destination-only v2 status contract without touching services. */
export declare function memoryStatus(config: RuntimeConfig, _deps?: MemoryStatusDependencies): Promise<MemoryStatus>;
