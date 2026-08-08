export interface RecallCacheOptions {
    maxEntries: number;
    ttlMs: number;
    now?: () => number;
}
/** A bounded, expiring LRU of shared retrieval promises. */
export declare class RecallCache<T> {
    private readonly options;
    private readonly entries;
    private readonly now;
    private useSequence;
    constructor(options: RecallCacheOptions);
    get size(): number;
    getOrCreate(key: string, factory: () => Promise<T>): Promise<T>;
    delete(key: string): void;
    clear(): void;
    private nextUse;
    private evictExpired;
    private evictOverCapacity;
}
