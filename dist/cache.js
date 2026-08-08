/** A bounded, expiring LRU of shared retrieval promises. */
export class RecallCache {
    options;
    entries = new Map();
    now;
    useSequence = 0;
    constructor(options) {
        this.options = options;
        if (!Number.isSafeInteger(options.maxEntries) || options.maxEntries < 1) {
            throw new Error("Recall cache maxEntries must be a positive integer");
        }
        if (!Number.isFinite(options.ttlMs) || options.ttlMs < 0) {
            throw new Error("Recall cache ttlMs must be a non-negative finite number");
        }
        this.now = options.now ?? Date.now;
    }
    get size() {
        this.evictExpired();
        return this.entries.size;
    }
    getOrCreate(key, factory) {
        this.evictExpired();
        const existing = this.entries.get(key);
        if (existing !== undefined) {
            existing.lastUsed = this.nextUse();
            return existing.promise;
        }
        const promise = Promise.resolve().then(factory);
        const entry = {
            promise,
            expiresAt: this.now() + this.options.ttlMs,
            lastUsed: this.nextUse(),
        };
        this.entries.set(key, entry);
        this.evictOverCapacity();
        // Attach a handler immediately so a fire-and-forget prefetch can never
        // create an unhandled rejection. A superseded value must survive an older
        // request rejecting later.
        void promise.catch(() => {
            if (this.entries.get(key)?.promise === promise)
                this.entries.delete(key);
        });
        return promise;
    }
    delete(key) {
        this.evictExpired();
        this.entries.delete(key);
    }
    clear() {
        this.entries.clear();
    }
    nextUse() {
        this.useSequence += 1;
        return this.useSequence;
    }
    evictExpired() {
        const now = this.now();
        for (const [key, entry] of this.entries) {
            if (entry.expiresAt <= now)
                this.entries.delete(key);
        }
    }
    evictOverCapacity() {
        while (this.entries.size > this.options.maxEntries) {
            let lruKey;
            let lruUse = Number.POSITIVE_INFINITY;
            for (const [key, entry] of this.entries) {
                if (entry.lastUsed < lruUse) {
                    lruKey = key;
                    lruUse = entry.lastUsed;
                }
            }
            if (lruKey === undefined)
                return;
            this.entries.delete(lruKey);
        }
    }
}
//# sourceMappingURL=cache.js.map