import { describe, expect, it } from "vitest";
import { validateHermesPoint } from "../../src/admin/hermes-contract.js";
import type { AdminPoint } from "../../src/admin/qdrant-admin.js";

function point(
  payload: Record<string, unknown> = { text: "safe" },
  id: string | number = "source-1",
  vector: number[] = [0.1, -0.2],
): AdminPoint {
  return { id, vector, payload };
}

describe("validateHermesPoint", () => {
  it.each([
    { text: "safe", model: "bge-m3" },
    { text: "safe", model: "" },
    { text: "safe", fact_status: "" },
    { text: "safe", fact_status: "active" },
    { text: "safe", project_path: "" },
    { text: "safe", project_path: "/workspace/project" },
    { text: "safe", source_type: "session", created_at: "2026-08-08T10:11:12.123Z", tags: ["one", "two"] },
  ])("accepts the fixed legacy contract: %j", (payload) => {
    expect(validateHermesPoint(point(payload))).toMatchObject({ eligible: true });
  });

  it("treats the verified empty source model as absent", () => {
    expect(validateHermesPoint(point({ text: "safe", model: "" }))).toEqual({
      eligible: true,
      point: point({ text: "safe", model: "" }),
    });
  });

  it.each([
    [{ text: "safe", fact_status: "deprecated" }, "fact-status"],
    [{ text: "safe", stale: true }, "stale"],
    [{ text: "safe", requires_review: true }, "review-required"],
    [{ text: "safe", consolidation_quarantined: true }, "quarantined"],
    [{ text: "safe", raptor_excluded: true }, "raptor-excluded"],
    [{ text: "safe", raptor_forgotten: true }, "raptor-forgotten"],
    [{ text: "safe", project_path: "relative/path" }, "project-path"],
    [{ text: "safe", created_at: "2026-02-30T12:00:00Z" }, "created-at"],
    [{ text: "safe", tags: ["ok", 3] }, "tags"],
    [{ text: "safe", model: 42 }, "model"],
    [{ text: "safe", source_type: false }, "source-type"],
    [{ text: "   " }, "text"],
  ] as const)("rejects an ineligible payload with its fixed reason", (payload, reason) => {
    expect(validateHermesPoint(point(payload as Record<string, unknown>))).toEqual({ eligible: false, reason });
  });

  it.each([
    ["stale", "false", "stale"],
    ["requires_review", 0, "review-required"],
    ["consolidation_quarantined", null, "quarantined"],
    ["raptor_excluded", undefined, "raptor-excluded"],
    ["raptor_forgotten", "", "raptor-forgotten"],
  ])("fails closed when present safety flag %s is malformed", (flag, value, reason) => {
    expect(validateHermesPoint(point({ text: "safe", [flag]: value }))).toEqual({ eligible: false, reason });
  });

  it.each([
    [{ id: "", vector: [1], payload: { text: "safe" } }, "id"],
    [{ id: -1, vector: [1], payload: { text: "safe" } }, "id"],
    [{ id: 1.5, vector: [1], payload: { text: "safe" } }, "id"],
    [{ id: Number.MAX_SAFE_INTEGER + 1, vector: [1], payload: { text: "safe" } }, "id"],
    [{ id: "ok", vector: [], payload: { text: "safe" } }, "vector"],
    [{ id: "ok", vector: [1, Number.NaN], payload: { text: "safe" } }, "vector"],
    [{ id: "ok", vector: [1, Number.POSITIVE_INFINITY], payload: { text: "safe" } }, "vector"],
    [{ id: "ok", vector: ["1"], payload: { text: "safe" } }, "vector"],
  ])("strictly rejects malformed point structure", (candidate, reason) => {
    expect(validateHermesPoint(candidate as AdminPoint)).toEqual({ eligible: false, reason });
  });

  it("rejects sparse, accessor-backed, and subclass vectors and tags", () => {
    const sparseVector = new Array<number>(2);
    sparseVector[1] = 0.2;
    const sparseTags = new Array<string>(2);
    sparseTags[1] = "safe";
    class VectorSubclass extends Array<number> {}
    class TagSubclass extends Array<string> {}
    const accessorVector = [0.1, 0.2];
    Object.defineProperty(accessorVector, "0", { enumerable: true, get: () => 0.1 });
    const accessorTags = ["safe"];
    Object.defineProperty(accessorTags, "0", { enumerable: true, get: () => "safe" });

    expect(validateHermesPoint(point({ text: "safe" }, "source", sparseVector))).toEqual({ eligible: false, reason: "vector" });
    expect(validateHermesPoint(point({ text: "safe" }, "source", new VectorSubclass(0.1, 0.2)))).toEqual({ eligible: false, reason: "vector" });
    expect(validateHermesPoint(point({ text: "safe" }, "source", accessorVector))).toEqual({ eligible: false, reason: "vector" });
    expect(validateHermesPoint(point({ text: "safe", tags: sparseTags }))).toEqual({ eligible: false, reason: "tags" });
    expect(validateHermesPoint(point({ text: "safe", tags: new TagSubclass("safe") }))).toEqual({ eligible: false, reason: "tags" });
    expect(validateHermesPoint(point({ text: "safe", tags: accessorTags }))).toEqual({ eligible: false, reason: "tags" });
  });

  it("does not mutate caller-owned input", () => {
    const input = point({ text: "safe", model: "", tags: ["tag"], stale: false });
    const before = structuredClone(input);
    validateHermesPoint(input);
    expect(input).toEqual(before);
  });
});
