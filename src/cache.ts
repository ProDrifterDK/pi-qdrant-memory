interface CacheEntry<T> {
  promise: Promise<T>;
  expiresAt: number;
  lastUsed: number;
}

export interface RecallCacheOptions {
  maxEntries: number;
  ttlMs: number;
  now?: () => number;
}

/** A bounded, expiring LRU of shared retrieval promises. */
export class RecallCache<T> {
  private readonly entries = new Map<string, CacheEntry<T>>();
  private readonly now: () => number;
  private useSequence = 0;

  constructor(private readonly options: RecallCacheOptions) {
    if (!Number.isSafeInteger(options.maxEntries) || options.maxEntries < 1) {
      throw new Error("Recall cache maxEntries must be a positive integer");
    }
    if (!Number.isFinite(options.ttlMs) || options.ttlMs < 0) {
      throw new Error("Recall cache ttlMs must be a non-negative finite number");
    }
    this.now = options.now ?? Date.now;
  }

  get size(): number {
    this.evictExpired();
    return this.entries.size;
  }

  getOrCreate(key: string, factory: () => Promise<T>): Promise<T> {
    this.evictExpired();
    const existing = this.entries.get(key);
    if (existing !== undefined) {
      existing.lastUsed = this.nextUse();
      return existing.promise;
    }

    const promise = Promise.resolve().then(factory);
    const entry: CacheEntry<T> = {
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
      if (this.entries.get(key)?.promise === promise) this.entries.delete(key);
    });
    return promise;
  }

  delete(key: string): void {
    this.evictExpired();
    this.entries.delete(key);
  }

  clear(): void {
    this.entries.clear();
  }

  private nextUse(): number {
    this.useSequence += 1;
    return this.useSequence;
  }

  private evictExpired(): void {
    const now = this.now();
    for (const [key, entry] of this.entries) {
      if (entry.expiresAt <= now) this.entries.delete(key);
    }
  }

  private evictOverCapacity(): void {
    while (this.entries.size > this.options.maxEntries) {
      let lruKey: string | undefined;
      let lruUse = Number.POSITIVE_INFINITY;
      for (const [key, entry] of this.entries) {
        if (entry.lastUsed < lruUse) {
          lruKey = key;
          lruUse = entry.lastUsed;
        }
      }
      if (lruKey === undefined) return;
      this.entries.delete(lruKey);
    }
  }
}
