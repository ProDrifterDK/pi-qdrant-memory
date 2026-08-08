import type { RuntimeConfig } from "../types.js";
interface Dependencies {
    fetchImpl?: typeof fetch;
    signal?: AbortSignal;
}
export interface MemoryStatus {
    destinationExists: boolean;
    destination: {
        endpoint: string;
        collection: string;
        exists: boolean;
        dimension: number | null;
        distance: string | null;
        pointCount: number | null;
        healthy: boolean;
        keyConfigured: boolean;
    };
    source: {
        endpoint: string;
        collection: string;
        exists: boolean;
        dimension: number | null;
        distance: string | null;
        pointCount: number | null;
        healthy: boolean;
        keyConfigured: boolean;
    };
    embeddings: {
        endpoint: string;
        model: string;
        dimension: number;
        healthy: boolean;
        keyConfigured: boolean;
    };
    qdrant: {
        healthy: boolean;
        destinationHealthy: boolean;
        sourceHealthy: boolean;
    };
}
export declare function memoryStatus(config: RuntimeConfig, deps?: Dependencies): Promise<MemoryStatus>;
export {};
