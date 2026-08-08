export type HostId = "prime" | "pi";
export interface RetrievalConfig {
    topK: number;
    candidatesPerLane: number;
    minScore: number;
    projectBoost: number;
    contextBudgetChars: number;
    toolResultBudgetChars: number;
    hardContextCharBudget: 16000;
    timeoutMs: number;
}
export interface RuntimeConfig {
    host: HostId;
    enabled: boolean;
    autoRecall: boolean;
    configPath: string;
    qdrant: {
        url: string;
        collection: string;
        apiKey?: string;
    };
    embeddings: {
        baseUrl: string;
        model: string;
        dimension: number;
        queryPrefix: string;
        apiKey?: string;
    };
    retrieval: RetrievalConfig;
    admin: {
        destinationApiKey?: string;
        source: {
            url: string;
            collection: string;
            schema: "hermes-qdrant-memory-v0.9-compatible";
            apiKey?: string;
        };
    };
}
export interface ConfigLoadDependencies {
    env: Record<string, string | undefined>;
    homeDir: string;
    xdgConfigHome?: string;
    readTextFile(path: string): Promise<string>;
}
