import { describe, expect, it } from "vitest";
import { RootWorkerContext } from "../../src/coordination/root.js";

describe("Task 8 root capability issuance boundary", () => {
  it("exports ONLY the capability class — no mint, factory, test hook or raw runtime issuer", async () => {
    expect(Object.getOwnPropertyNames(RootWorkerContext).sort()).toEqual(["isValid", "length", "name", "prototype"]);
    const real = await import("../../src/coordination/root.js");
    const exported = Object.keys(real).sort();
    expect(exported).toEqual(["RootWorkerContext"]);
    for (const name of ["issueRootWorker", "verifyRootWorker", "mint", "mintRootWorker", "createRootWorker", "RootSessionRuntime", "RootWorkerEvidence"]) {
      expect(exported).not.toContain(name);
    }
  });
  it("cannot be constructed by any exported or structural means (fail closed until Task 9)", () => {
    // Emitted-JS constructor attempts with any symbol fail the module-private issuer check.
    expect(() => new RootWorkerContext("pi", "h", Symbol("x"))).toThrow(/issuer/i);
    // Prototype-forged objects fail the brand check.
    const forged = Object.create(RootWorkerContext.prototype) as RootWorkerContext;
    expect(RootWorkerContext.isValid(forged)).toBe(false);
    expect(RootWorkerContext.isValid({})).toBe(false);
    expect(RootWorkerContext.isValid(null)).toBe(false);
    expect(RootWorkerContext.isValid(undefined)).toBe(false);
    // No exported function returns a valid context: the only export is the class itself.
    const statics = Object.getOwnPropertyNames(RootWorkerContext);
    expect(statics.filter((key) => typeof (RootWorkerContext as unknown as Record<string, unknown>)[key] === "function").sort()).toEqual(["isValid"]);
    // The trusted clock is exposed ONLY as the validating instance method `now`
    // (safe nonnegative integer on every call); no clock issuer or structural
    // clock is exported, so Task 8 still fails closed until Task 9 issuance.
    expect(Object.getOwnPropertyNames(RootWorkerContext.prototype)).toContain("now");
    expect(typeof (RootWorkerContext.prototype as unknown as Record<string, unknown>).now).toBe("function");
    // The class object and prototype are frozen: isValid cannot be monkeypatched.
    expect(Object.isFrozen(RootWorkerContext)).toBe(true);
    expect(Object.isFrozen(RootWorkerContext.prototype)).toBe(true);
    expect(() => { (RootWorkerContext as unknown as Record<string, unknown>).isValid = () => true; }).toThrow();
    expect(() => { Object.defineProperty(RootWorkerContext, "isValid", { value: () => true }); }).toThrow();
    expect(() => { (RootWorkerContext.prototype as unknown as Record<string, unknown>).hack = 1; }).toThrow();
    // The class object and prototype are frozen: isValid cannot be monkeypatched.
    expect(Object.isFrozen(RootWorkerContext)).toBe(true);
    expect(Object.isFrozen(RootWorkerContext.prototype)).toBe(true);
    expect(() => { (RootWorkerContext as unknown as Record<string, unknown>).isValid = () => true; }).toThrow();
    expect(() => { Object.defineProperty(RootWorkerContext, "isValid", { value: () => true }); }).toThrow();
    expect(() => { (RootWorkerContext.prototype as unknown as Record<string, unknown>).hack = 1; }).toThrow();
  });
});
