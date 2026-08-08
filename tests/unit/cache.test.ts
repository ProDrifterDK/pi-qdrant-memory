import { describe, expect, it, vi } from "vitest";
import { RecallCache } from "../../src/cache.js";

describe("RecallCache", () => {
  it("reuses the identical in-flight promise and clears session state", async () => {
    let now = 10;
    const factory = vi.fn(async () => "one");
    const cache = new RecallCache<string>({ maxEntries: 2, ttlMs: 300_000, now: () => now });

    const first = cache.getOrCreate("session|project|query|rev1", factory);
    const second = cache.getOrCreate("session|project|query|rev1", async () => "two");

    expect(first).toBe(second);
    await expect(second).resolves.toBe("one");
    expect(factory).toHaveBeenCalledTimes(1);
    cache.clear();
    expect(cache.size).toBe(0);
  });

  it("expires entries after five minutes", async () => {
    let now = 0;
    const cache = new RecallCache<string>({ maxEntries: 32, ttlMs: 300_000, now: () => now });
    await cache.getOrCreate("key", async () => "first");

    now = 299_999;
    await expect(cache.getOrCreate("key", async () => "early")).resolves.toBe("first");
    now = 300_000;
    await expect(cache.getOrCreate("key", async () => "expired")).resolves.toBe("expired");
  });

  it("evicts the strict least-recently-used entry even when clock timestamps tie", async () => {
    const cache = new RecallCache<string>({ maxEntries: 2, ttlMs: 300_000, now: () => 0 });
    await cache.getOrCreate("a", async () => "a1");
    await cache.getOrCreate("b", async () => "b1");
    await cache.getOrCreate("a", async () => "a2");
    await cache.getOrCreate("c", async () => "c1");

    await expect(cache.getOrCreate("a", async () => "a3")).resolves.toBe("a1");
    await expect(cache.getOrCreate("b", async () => "b2")).resolves.toBe("b2");
  });

  it("keeps no more than 32 entries", async () => {
    const cache = new RecallCache<number>({ maxEntries: 32, ttlMs: 300_000, now: () => 0 });
    for (let index = 0; index < 33; index += 1) {
      await cache.getOrCreate(`key-${index}`, async () => index);
    }
    expect(cache.size).toBe(32);
    await expect(cache.getOrCreate("key-0", async () => 99)).resolves.toBe(99);
  });

  it("evicts a rejection without an unhandled rejection", async () => {
    const cache = new RecallCache<string>({ maxEntries: 2, ttlMs: 300_000 });
    const rejected = cache.getOrCreate("key", async () => { throw new Error("nope"); });
    await expect(rejected).rejects.toThrow("nope");
    await Promise.resolve();
    expect(cache.size).toBe(0);
    await expect(cache.getOrCreate("key", async () => "retry")).resolves.toBe("retry");
  });

  it("does not let an older rejection delete a newer value for the same key", async () => {
    let rejectOld: ((error: Error) => void) | undefined;
    const cache = new RecallCache<string>({ maxEntries: 2, ttlMs: 300_000 });
    const old = cache.getOrCreate("key", () => new Promise((_resolve, reject) => { rejectOld = reject; }));
    await Promise.resolve();
    cache.delete("key");
    const current = cache.getOrCreate("key", async () => "current");
    rejectOld?.(new Error("old"));
    await expect(old).rejects.toThrow("old");
    await expect(current).resolves.toBe("current");
    await expect(cache.getOrCreate("key", async () => "wrong")).resolves.toBe("current");
  });

  it("separates otherwise identical requests by configuration revision", async () => {
    const cache = new RecallCache<string>({ maxEntries: 32, ttlMs: 300_000 });
    await expect(cache.getOrCreate("session|project|same|rev1", async () => "one")).resolves.toBe("one");
    await expect(cache.getOrCreate("session|project|same|rev2", async () => "two")).resolves.toBe("two");
  });
});
