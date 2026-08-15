import type { RuntimeConfig } from "../types.js";
export interface MemoryStatusAudit {
    metadata?: {
        ownerHost?: RuntimeConfig["host"];
        schemaRevision?: number;
        vector?: {
            name: string;
            dimension: number;
            distance: string;
        };
        pointCount?: number | null;
    };
    policy?: {
        hash?: string;
        mismatch?: boolean;
    };
    outbox?: {
        jobs?: number;
        bytes?: number;
        oldestAt?: string | null;
        failures?: number;
    };
    coverage?: {
        missing?: number;
        oldestAt?: string | null;
        lastReconcileAt?: string | null;
    };
    jobs?: {
        queued?: number;
        leased?: number;
        failed?: number;
    };
    generation?: {
        active?: string | null;
        manifestHash?: string | null;
        levels?: number;
        orphans?: number;
    };
    privacy?: {
        epoch?: number;
        revokedDestinationIds?: readonly string[];
    };
    records?: Record<string, number>;
    embeddingHealthy?: boolean;
    dedicatedLlmAvailable?: boolean;
    fallbackLlmAvailable?: boolean;
    lastErrorCategory?: string | null;
}
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
        distance: "Dot";
        exists: boolean;
        healthy: boolean;
        keyConfigured: boolean;
        authMode?: "configured" | "not_configured";
        pointCount?: number | null;
    };
    embeddings: {
        endpoint: string;
        model: string;
        dimension: 1024;
        healthy: boolean;
        keyConfigured: boolean;
        authMode?: "configured" | "not_configured";
    };
    capture: {
        enabled: boolean;
        episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
        explicitRetention?: boolean;
        explicitEgress?: boolean;
    };
    privacy: {
        egressMode: RuntimeConfig["privacy"]["egressMode"];
        qdrantDestinations: number;
        embeddingDestinations: number;
        llmDestinations: number;
        epoch?: number;
        revokedDestinationIds?: readonly string[];
    };
    qdrant: {
        healthy: boolean;
        destinationHealthy: boolean;
        probed: boolean;
    };
    /** Redacted operational audit fields. No API keys, record payloads or text. */
    metadata?: {
        ownerHost: RuntimeConfig["host"];
        schemaRevision: number;
        vector: {
            name: "semantic";
            dimension: 1024;
            distance: "Dot";
        };
        pointCount: number | null;
    };
    policy?: {
        hash: string | null;
        mismatch: boolean;
    };
    projects?: {
        registered: number;
        aliases: readonly string[];
    };
    scopes?: {
        root: RuntimeConfig["retrieval"]["rootScope"];
        childSearch: boolean;
    };
    outbox?: {
        jobs: number;
        bytes: number;
        oldestAt: string | null;
        failures: number;
    };
    coverage?: {
        missing: number;
        oldestAt: string | null;
        lastReconcileAt: string | null;
    };
    jobs?: {
        queued: number;
        leased: number;
        failed: number;
    };
    generation?: {
        active: string | null;
        manifestHash: string | null;
        levels: number;
        orphans: number;
    };
    recordCounts?: Readonly<Record<string, number>>;
    embeddingHealth?: boolean;
    dedicatedLlmAvailable?: boolean;
    fallbackLlmAvailable?: boolean;
    lastErrorCategory?: string | null;
}
export interface MemoryStatusDependencies {
    signal?: AbortSignal;
    fetchImpl?: typeof fetch;
    audit?(): Promise<MemoryStatusAudit> | MemoryStatusAudit;
}
/** Probe only the configured host collection with its collection-scoped key. */
export declare function memoryStatus(config: RuntimeConfig, deps?: MemoryStatusDependencies): Promise<MemoryStatus>;
