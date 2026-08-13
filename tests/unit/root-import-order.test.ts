import { describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";

// These tests are import-ORDER sensitive: the root module snapshots the
// SessionManager prototype methods at module load, so every test builds its
// prototype state BEFORE a fresh dynamic import (vi.resetModules) and restores
// it afterwards. A manager created before the import must stay genuine.
const options = (extra: Record<string, unknown> = {}): Record<string, unknown> => ({
  host: "pi", env: {}, membership: [], nodeId: "node-a", leaseMs: 30_000, maxClockSkewMs: 1_000,
  workerPolicy: {}, extractorRevision: "x", producerPolicies: [], embedding: {}, llm: {}, ...extra,
});
interface ManagerMethods { getHeader: (...args: unknown[]) => unknown; getEntries: (...args: unknown[]) => unknown; getSessionId: (...args: unknown[]) => unknown; getBranch: (...args: unknown[]) => unknown; }
type ManagerMethod = keyof ManagerMethods;
type ManagerCounts = Record<ManagerMethod, number>;
const MANAGER_METHODS: readonly ManagerMethod[] = ["getHeader", "getEntries", "getSessionId", "getBranch"] as const;
function zeroCounts(): ManagerCounts { return { getHeader: 0, getEntries: 0, getSessionId: 0, getBranch: 0 }; }
function wrapManagerMethods(counts: ManagerCounts): ManagerMethods {
  const proto = SessionManager.prototype as unknown as ManagerMethods;
  const originals = {} as ManagerMethods;
  for (const method of MANAGER_METHODS) {
    const original = proto[method];
    originals[method] = original;
    proto[method] = function (this: unknown, ...args: unknown[]) { counts[method] += 1; return original.apply(this, args); };
  }
  return originals;
}
function setManagerMethod(method: ManagerMethod, replacement: (...args: unknown[]) => unknown): void {
  (SessionManager.prototype as unknown as ManagerMethods)[method] = replacement;
}
function restoreManagerMethods(originals: ManagerMethods): void {
  for (const method of MANAGER_METHODS) (SessionManager.prototype as unknown as ManagerMethods)[method] = originals[method];
}

describe("Task 9 root import order: pre-import managers and module-cache independence", () => {
  it("accepts a manager created before the module import: genuine root reads header once, entries/sessionId once", async () => {
    vi.resetModules();
    // The manager exists BEFORE the root module is ever imported.
    const manager = SessionManager.inMemory();
    const counts = zeroCounts();
    const originals = wrapManagerMethods(counts);
    try {
      const root = await import("../../src/coordination/root.js");
      // Store is absent -> fail closed AFTER the genuine root lifecycle digest:
      // header (marker) once, entries/sessionId (root state proof) once each.
      await expect(root.runCurationFromLifecycle(manager, options() as never)).resolves.toEqual({ state: "child" });
      expect(counts).toEqual({ getHeader: 1, getEntries: 1, getSessionId: 1, getBranch: 0 });
      expect(root.RootWorkerContext.isValid({})).toBe(false);
    } finally { restoreManagerMethods(originals); }
  });

  it("resolves a genuine child before entries/sessionId: header once, entries/sessionId zero", async () => {
    for (const env of [{ PI_SUBAGENT_CHILD: "1" }, { PI_SUBAGENT_DEPTH: "2" }]) {
      vi.resetModules();
      const manager = SessionManager.inMemory();
      const counts = zeroCounts();
      const originals = wrapManagerMethods(counts);
      try {
        const root = await import("../../src/coordination/root.js");
        await expect(root.runCurationFromLifecycle(manager, options({ env }) as never)).resolves.toEqual({ state: "child" });
        // Child resolution reads ONLY the marker header; the internal session
        // state (entries/sessionId) stays untouched for child lifecycles.
        expect(counts).toEqual({ getHeader: 1, getEntries: 0, getSessionId: 0, getBranch: 0 });
      } finally { restoreManagerMethods(originals); }
    }
  });

  it("fails closed on header marker accessors without invoking them", async () => {
    for (const key of ["rlmDepth", "parentSession"] as const) {
      vi.resetModules();
      let fired = 0;
      const hostileHeader: Record<string, unknown> = {};
      Object.defineProperty(hostileHeader, key, { configurable: true, enumerable: true, get() { fired += 1; throw new Error(`${key} getter fired`); } });
      const originals = wrapManagerMethods(zeroCounts());
      // The wrapper is installed BEFORE import, so the fresh module treats it
      // as the genuine host method and reads through it.
      setManagerMethod("getHeader", () => hostileHeader);
      try {
        const root = await import("../../src/coordination/root.js");
        const manager = SessionManager.inMemory();
        await expect(root.runCurationFromLifecycle(manager, options() as never)).resolves.toEqual({ state: "child" });
        // The accessor marker is rejected by descriptor inspection; its getter
        // is never invoked.
        expect(fired).toBe(0);
      } finally { restoreManagerMethods(originals); }
    }
  });

  it("fails closed on nested header proxies without invoking traps", async () => {
    for (const key of ["parentSession", "rlmDepth"] as const) {
      vi.resetModules();
      let traps = 0;
      const proxied = new Proxy({}, { get() { traps += 1; throw new Error("trap fired"); } });
      const originals = wrapManagerMethods(zeroCounts());
      setManagerMethod("getHeader", () => ({ [key]: proxied }));
      try {
        const root = await import("../../src/coordination/root.js");
        const manager = SessionManager.inMemory();
        await expect(root.runCurationFromLifecycle(manager, options() as never)).resolves.toEqual({ state: "child" });
        expect(traps).toBe(0);
      } finally { restoreManagerMethods(originals); }
    }
  });

  it("rejects the manager when any lifecycle prototype method is replaced after import", async () => {
    for (const method of ["getEntries", "getSessionId", "getBranch"] as const) {
      vi.resetModules();
      const root = await import("../../src/coordination/root.js");
      const manager = SessionManager.inMemory();
      const proto = SessionManager.prototype as unknown as ManagerMethods;
      const original = proto[method];
      let touched = 0;
      const input = options({ env: {} });
      for (const key of ["store", "membership", "llm"] as const) {
        Object.defineProperty(input, key, { configurable: true, get() { touched += 1; throw new Error(`${key} getter fired`); } });
      }
      try {
        // Post-import replacement of ANY snapshotted method breaks the exact
        // prototype binding; the manager fails closed before options are read.
        setManagerMethod(method, () => { throw new Error(`${method} hooked`); });
        await expect(root.runCurationFromLifecycle(manager, input as never)).resolves.toEqual({ state: "child" });
        expect(touched).toBe(0);
      } finally { setManagerMethod(method, original); }
    }
  });
});
