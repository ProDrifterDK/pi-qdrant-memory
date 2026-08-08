export interface QdrantFilter {
    must: Array<{
        key: string;
        match: {
            value: string;
        };
    }>;
    must_not?: Array<{
        key: string;
        match: {
            value: string;
        };
    }>;
}
export interface QdrantSearchHit {
    id: string | number;
    score: number;
    payload: Record<string, unknown>;
}
interface ReadonlyQdrantClientOptions {
    baseUrl: string;
    collection: string;
    apiKey?: string;
    timeoutMs: number;
    fetchImpl?: typeof fetch;
}
export declare class ReadonlyQdrantClient {
    private readonly options;
    constructor(options: ReadonlyQdrantClientOptions);
    health(signal?: AbortSignal): Promise<void>;
    collectionInfo(signal?: AbortSignal): Promise<{
        dimension: number;
        distance: string;
    }>;
    search(input: {
        vector: number[];
        limit: number;
        filter: QdrantFilter;
        signal?: AbortSignal;
    }): Promise<QdrantSearchHit[]>;
}
export {};
