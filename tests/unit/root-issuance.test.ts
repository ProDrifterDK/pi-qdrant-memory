import { describe, expect, it } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { RootWorkerContext, runCurationFromLifecycle } from "../../src/coordination/root.js";
const options = (extra: Record<string, unknown> = {}): Record<string, unknown> => ({ host: "pi", env: {}, membership: [], nodeId: "node-a", leaseMs: 30_000, maxClockSkewMs: 1_000, workerPolicy: {}, extractorRevision: "x", producerPolicies: [], embedding: {}, llm: {}, createdAt: () => "2026-08-10T00:00:00.000Z", ...extra });
describe("Task 9 root capability issuance boundary", () => {
  it("exports only nominal context and lifecycle operation", async () => { const real = await import("../../src/coordination/root.js"); expect(Object.keys(real).sort()).toEqual(["RootWorkerContext", "runCurationFromLifecycle"]); expect(Object.getOwnPropertyNames(RootWorkerContext).sort()).toEqual(["isValid", "length", "name", "prototype"]); });
  it("rejects fake/proxy/subclass-like managers before touching options", async () => {
    const manager = SessionManager.inMemory(); const explosive = new Proxy({}, { get() { throw new Error("options getter fired"); } });
    expect(await runCurationFromLifecycle({} as SessionManager, explosive as never)).toEqual({ state: "child" });
    expect(await runCurationFromLifecycle(Object.create(SessionManager.prototype) as SessionManager, explosive as never)).toEqual({ state: "child" });
    const fakeSubclass = Object.create(SessionManager.prototype) as SessionManager; Object.setPrototypeOf(fakeSubclass, {});
    expect(await runCurationFromLifecycle(fakeSubclass, explosive as never)).toEqual({ state: "child" });
    expect(await runCurationFromLifecycle(new Proxy(manager, {}), explosive as never)).toEqual({ state: "child" });
    const before = Object.getOwnPropertyDescriptor(SessionManager, "inMemory"); await runCurationFromLifecycle(manager, explosive as never); const after = Object.getOwnPropertyDescriptor(SessionManager, "inMemory"); expect(after?.value).toBe(before?.value);
  });
  it("fails closed at the store gate for a genuine root manager", async () => {
    const input = options();
    let membershipReads = 0;
    Object.defineProperty(input, "membership", { configurable: true, get() { membershipReads += 1; throw new Error("membership touched"); } });
    await expect(runCurationFromLifecycle(SessionManager.inMemory(), input as never)).resolves.toEqual({ state: "child" });
    expect(membershipReads).toBe(0);
    expect(RootWorkerContext.isValid({})).toBe(false);
  });
  it("uses host markers fail-closed", async () => { const manager = SessionManager.inMemory(); await expect(runCurationFromLifecycle(manager, options({ env: { PI_SUBAGENT_CHILD: "1" } }) as never)).resolves.toEqual({ state: "child" }); await expect(runCurationFromLifecycle(manager, options({ env: { PI_SUBAGENT_DEPTH: "2" } }) as never)).resolves.toEqual({ state: "child" }); });
  it("rejects a post-import prototype hook before touching options", async () => {
    const manager = SessionManager.inMemory();
    const descriptor = Object.getOwnPropertyDescriptor(SessionManager.prototype, "getHeader");
    const original = descriptor?.value;
    if (typeof original !== "function" || descriptor === undefined) throw new Error("missing manager descriptor");
    let touched = 0;
    const input = options({ env: {} });
    for (const key of ["store", "membership", "llm"] as const) Object.defineProperty(input, key, { configurable: true, get() { touched += 1; throw new Error("hook options touched"); } });
    try {
      Object.defineProperty(SessionManager.prototype, "getHeader", { ...descriptor, value: () => ({ parentSession: null, root: true }) });
      await expect(runCurationFromLifecycle(manager, input as never)).resolves.toEqual({ state: "child" });
      expect(touched).toBe(0);
    } finally { Object.defineProperty(SessionManager.prototype, "getHeader", descriptor); }
  });
  it("resolves genuine child before touching remaining lifecycle getters", async () => {
    const manager = SessionManager.inMemory();
    const input = options({ env: { PI_SUBAGENT_CHILD: "1" } });
    let touched = 0;
    for (const key of ["store", "membership", "workerPolicy", "producerPolicies", "embedding", "llm", "createdAt"] as const) Object.defineProperty(input, key, { configurable: true, get() { touched += 1; throw new Error(`${key} getter fired`); } });
    await expect(runCurationFromLifecycle(manager, input as never)).resolves.toEqual({ state: "child" });
    expect(touched).toBe(0);
  });
});
