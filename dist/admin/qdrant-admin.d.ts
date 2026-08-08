export interface AdminPoint {
    id: string | number;
    vector: number[];
    payload: Record<string, unknown>;
}
export interface AdminCollectionInfo {
    dimension: number;
    distance: string;
    pointCount: number | null;
    payloadSchema?: Record<string, unknown>;
}
export interface AdminQdrantClientOptions {
    baseUrl: string;
    apiKey?: string;
    timeoutMs: number;
    fetchImpl?: typeof fetch;
}
export declare class AdminQdrantClient {
    private readonly options;
    constructor(options: AdminQdrantClientOptions);
    collectionInfo(collection: string, signal?: AbortSignal): Promise<AdminCollectionInfo>;
    createCollection(collection: string, dimension: number, distance: "Cosine", signal?: AbortSignal): Promise<void>;
    createKeywordIndex(collection: string, field: string, signal?: AbortSignal): Promise<void>;
    scroll(collection: string, offset?: string | number, limit?: number, signal?: AbortSignal): Promise<{
        points: AdminPoint[];
        nextOffset?: string | number;
    }>;
    upsert(collection: string, points: readonly AdminPoint[], signal?: AbortSignal): Promise<void>;
}
