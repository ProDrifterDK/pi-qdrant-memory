export interface EmbeddingsClientOptions {
    baseUrl: string;
    model: string;
    dimension: number;
    queryPrefix: string;
    apiKey?: string;
    timeoutMs: number;
    fetchImpl?: typeof fetch;
}
export declare class EmbeddingsClient {
    private readonly options;
    constructor(options: EmbeddingsClientOptions);
    embedQuery(query: string, signal?: AbortSignal): Promise<number[]>;
}
