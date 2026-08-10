export type MemoryErrorCategory = "timeout" | "cancelled" | "network" | "http" | "invalid-json" | "invalid-response" | "configuration";
export declare class MemoryClientError extends Error {
    readonly category: MemoryErrorCategory;
    readonly status?: number | undefined;
    constructor(category: MemoryErrorCategory, message: string, status?: number | undefined);
}
export interface FetchOptions {
    timeoutMs: number;
    signal?: AbortSignal;
    fetchImpl?: typeof fetch;
}
export declare function fetchOk(url: string, init: RequestInit, options: FetchOptions): Promise<Response>;
export declare function fetchJson<T>(url: string, init: RequestInit, options: FetchOptions): Promise<T>;
