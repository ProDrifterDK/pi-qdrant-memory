import { describe, expect, it } from "vitest";
import { canonicalStringify, deterministicPointId } from "../../src/admin/canonical.js";
import {
  MAX_PROJECT_LABEL_LENGTH,
  MAX_SOURCE_POINT_ID_LENGTH,
  MAX_SOURCE_TYPE_LENGTH,
  MAX_TAG_COUNT,
  MAX_TAG_LENGTH,
  buildImportPlan,
  normalizeHermesPoint,
} from "../../src/admin/import-plan.js";
import type { AdminPoint } from "../../src/admin/qdrant-admin.js";

function point(
  payload: Record<string, unknown> = { text: "safe memory", model: "bge-m3", source_type: "note" },
  id: string | number = "source-1",
  vector: number[] = [0.1, 0.2],
): AdminPoint {
  return { id, vector, payload };
}

function planInput(points: readonly AdminPoint[], extra: Record<string, unknown> = {}) {
  return {
    points,
    targetHost: "prime" as const,
    sourceIdentity: "qdrant:http://source",
    sourceCollection: "hermes_memory",
    sourceDimension: 2,
    sourceDistance: "Cosine",
    destinationCollection: "prime_memory",
    destinationDimension: 2,
    destinationDistance: "Cosine",
    configuredModel: "bge-m3",
    ...extra,
  };
}

describe("canonicalStringify", () => {
  it("sorts object keys recursively, preserves array order, and normalizes negative zero", () => {
    expect(canonicalStringify({ z: -0, a: [{ y: 2, x: 1 }, 3] })).toBe('{"a":[{"x":1,"y":2},3],"z":0}');
    expect(canonicalStringify({ b: 2, a: 1 })).toBe(canonicalStringify({ a: 1, b: 2 }));
    expect(canonicalStringify([1, 2])).not.toBe(canonicalStringify([2, 1]));
  });

  it("supports shared acyclic references deterministically", () => {
    const shared = { b: 2, a: 1 };
    expect(canonicalStringify({ left: shared, right: shared })).toBe(
      '{"left":{"a":1,"b":2},"right":{"a":1,"b":2}}',
    );
  });

  it("ignores inherited toJSON hooks", () => {
    const original = Object.getOwnPropertyDescriptor(Array.prototype, "toJSON");
    Object.defineProperty(Array.prototype, "toJSON", {
      configurable: true,
      value: () => ["tampered"],
    });
    try {
      expect(canonicalStringify([1, 2])).toBe("[1,2]");
    } finally {
      if (original === undefined) delete (Array.prototype as { toJSON?: unknown }).toJSON;
      else Object.defineProperty(Array.prototype, "toJSON", original);
    }
  });

  it.each([
    undefined,
    1n,
    Symbol("x"),
    () => 1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    { value: undefined },
    [1, undefined],
    new Date("2026-01-01T00:00:00Z"),
    { toJSON: () => "surprise" },
    new (class Example { value = 1; })(),
  ])("rejects ambiguous or non-JSON value %#", (value) => {
    expect(() => canonicalStringify(value)).toThrow();
  });

  it("rejects cycles rather than invoking JSON coercion", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => canonicalStringify(cyclic)).toThrow(/cyclic/i);
  });

  it("rejects sparse arrays, accessors, and symbol-keyed properties", () => {
    const sparse = [1, , 3];
    const accessor = Object.defineProperty({}, "value", { enumerable: true, get: () => 1 });
    const symbolKeyed = { safe: true, [Symbol("hidden")]: "value" };
    class SpecialArray<T> extends Array<T> {}
    expect(() => canonicalStringify(sparse)).toThrow();
    expect(() => canonicalStringify(accessor)).toThrow();
    expect(() => canonicalStringify(symbolKeyed)).toThrow();
    expect(() => canonicalStringify(new SpecialArray(1, 2))).toThrow();
  });
});

describe("normalizeHermesPoint", () => {
  it("maps only the destination allowlist and derives project identity lexically", () => {
    const input = point({
      text: "safe memory",
      model: "bge-m3",
      project_path: "/private/work/project",
      source_type: "session",
      created_at: "2026-08-08T10:11:12Z",
      tags: ["one", "two"],
      fact_status: "active",
      stale: false,
      unknown_secret_field: "must-not-carry",
    }, 7);
    const result = normalizeHermesPoint({
      point: input,
      targetHost: "prime",
      sourceCollection: "hermes_memory",
      configuredModel: "bge-m3",
    });
    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error("expected accepted point");
    expect(result.projectLabel).toBe("project");
    expect(result.sourceType).toBe("session");
    expect(result.point.id).toBe(deterministicPointId("prime", "hermes_memory", 7));
    expect(result.point.vector).toEqual([0.1, 0.2]);
    expect(result.point.payload).toMatchObject({
      text: "safe memory",
      host: "prime",
      project_id: expect.stringMatching(/^[a-f0-9]{64}$/),
      project_label: "project",
      source_type: "session",
      source_system: "hermes",
      source_collection: "hermes_memory",
      source_point_id: 7,
      created_at: "2026-08-08T10:11:12Z",
      tags: ["one", "two"],
      status: "active",
      secret_scan: "passed",
    });
    expect(result.point.payload).not.toHaveProperty("project_path");
    expect(result.point.payload).not.toHaveProperty("unknown_secret_field");
    expect(result.point.payload).not.toHaveProperty("fact_status");
    expect(result.point.payload).not.toHaveProperty("import_run_id");
    expect(JSON.stringify(result)).not.toContain("/private/work");
  });

  it("normalizes absent or empty project and source type to a global unknown record", () => {
    for (const projectPath of [undefined, ""]) {
      const payload: Record<string, unknown> = { text: "safe memory", model: "bge-m3", source_type: "" };
      if (projectPath !== undefined) payload.project_path = projectPath;
      const result = normalizeHermesPoint({
        point: point(payload),
        targetHost: "pi",
        sourceCollection: "hermes_memory",
        configuredModel: "bge-m3",
      });
      expect(result).toMatchObject({ accepted: true, sourceType: "unknown" });
      if (!result.accepted) throw new Error("expected accepted point");
      expect(result.point.payload).not.toHaveProperty("project_id");
      expect(result.point.payload).not.toHaveProperty("project_label");
    }
  });

  it.each([
    [{ text: "safe", model: "bge-m3", tags: Array(MAX_TAG_COUNT + 1).fill("tag") }, "tag-bounds"],
    [{ text: "safe", model: "bge-m3", tags: ["x".repeat(MAX_TAG_LENGTH + 1)] }, "tag-bounds"],
    [{ text: "safe", model: "bge-m3", source_type: "x".repeat(MAX_SOURCE_TYPE_LENGTH + 1) }, "source-type-bounds"],
    [{ text: "safe", model: "bge-m3", project_path: `/root/${"x".repeat(MAX_PROJECT_LABEL_LENGTH + 1)}` }, "project-label-bounds"],
  ])("rejects over-bound metadata deterministically", (payload, reason) => {
    expect(normalizeHermesPoint({
      point: point(payload),
      targetHost: "prime",
      sourceCollection: "hermes_memory",
      configuredModel: "bge-m3",
    })).toEqual({ accepted: false, reason });
  });

  it("bounds copied string source point IDs", () => {
    expect(normalizeHermesPoint({
      point: point({ text: "safe", model: "bge-m3" }, "x".repeat(MAX_SOURCE_POINT_ID_LENGTH + 1)),
      targetHost: "prime",
      sourceCollection: "hermes_memory",
      configuredModel: "bge-m3",
    })).toEqual({ accepted: false, reason: "source-point-id-bounds" });
  });

  it.each([
    [{ text: "password=hunter2long", model: "bge-m3" }, "secret"],
    [{ text: "safe", model: "bge-m3", tags: ["ghp_abcdefghijklmnopqrstuvwxyz123456"] }, "secret"],
    [{ text: "safe", model: "bge-m3", source_type: "token=abcdefghijklmnop" }, "secret"],
    [{ text: "safe", model: "bge-m3", project_path: "/root/password=hunter2long" }, "secret"],
  ])("scans full text and all mapped tag/provenance values", (payload, reason) => {
    expect(normalizeHermesPoint({
      point: point(payload),
      targetHost: "prime",
      sourceCollection: "hermes_memory",
      configuredModel: "bge-m3",
    })).toEqual({ accepted: false, reason });
  });

  it("does not mutate or retain mutable caller-owned arrays", () => {
    const input = point({ text: "safe", model: "bge-m3", tags: ["one"] });
    const before = structuredClone(input);
    const result = normalizeHermesPoint({
      point: input,
      targetHost: "prime",
      sourceCollection: "hermes_memory",
      configuredModel: "bge-m3",
    });
    expect(input).toEqual(before);
    if (!result.accepted) throw new Error("expected accepted point");
    expect(result.point.vector).not.toBe(input.vector);
    expect(result.point.payload.tags).not.toBe(input.payload.tags);
  });
});

describe("buildImportPlan", () => {
  it("hashes every selected vector component and relevant mapped payload value", () => {
    const baseline = buildImportPlan(planInput([point()]));
    const vectorChanged = buildImportPlan(planInput([point(undefined, "source-1", [0.1, 0.2000001])]));
    const payloadChanged = buildImportPlan(planInput([point({ text: "safe memory", model: "bge-m3", source_type: "event" })]));
    expect(vectorChanged.planId).not.toBe(baseline.planId);
    expect(payloadChanged.planId).not.toBe(baseline.planId);
  });

  it("is stable under payload object-key and input point reordering", () => {
    const first = point({ text: "safe memory", model: "bge-m3", source_type: "note", tags: ["a", "b"] }, "b");
    const reordered = point({ tags: ["a", "b"], source_type: "note", model: "bge-m3", text: "safe memory" }, "b");
    const other = point({ text: "other safe memory", model: "bge-m3", source_type: "note" }, "a", [0.3, 0.4]);
    expect(buildImportPlan(planInput([first, other])).planId).toBe(
      buildImportPlan(planInput([other, reordered])).planId,
    );
  });

  it("does not hash or carry unknown source payload fields", () => {
    const one = buildImportPlan(planInput([point({ text: "safe", model: "bge-m3", unknown: "one" })]));
    const two = buildImportPlan(planInput([point({ text: "safe", model: "bge-m3", unknown: "two" })]));
    expect(two.planId).toBe(one.planId);
    expect(one.accepted[0]?.payload).not.toHaveProperty("unknown");
  });

  it("hashes relevant safety-field presence even when the normalized destination is identical", () => {
    const absent = buildImportPlan(planInput([point({ text: "safe", model: "bge-m3" })]));
    const explicitFalse = buildImportPlan(planInput([point({ text: "safe", model: "bge-m3", stale: false })]));
    expect(explicitFalse.planId).not.toBe(absent.planId);
  });

  it("assigns import_run_id only after hashing pre-run records", () => {
    const plan = buildImportPlan(planInput([point()]));
    expect(plan.planId).toMatch(/^[a-f0-9]{64}$/);
    expect(plan.accepted).toHaveLength(1);
    expect(plan.accepted[0]?.payload.import_run_id).toBe(plan.planId);
    expect(plan.report).toEqual({
      accepted: 1,
      rejected: 0,
      bySourceType: { note: 1 },
      byProjectLabel: { global: 1 },
    });
    expect(() => JSON.stringify(plan)).not.toThrow();
  });

  it("reports deterministic counts only and never rejected content or paths", () => {
    const secret = "password=hunter2long";
    const rawPath = "/private/sensitive/project";
    const plan = buildImportPlan(planInput([
      point({ text: secret, model: "bge-m3" }, "secret"),
      point({ text: "safe", model: "bge-m3", project_path: "relative/path" }, "path"),
      point({ text: "safe", model: "bge-m3", project_path: rawPath, source_type: "note" }, "ok"),
    ]));
    expect(plan.rejected).toEqual({ "project-path": 1, secret: 1 });
    expect(plan.report).toEqual({
      accepted: 1,
      rejected: 2,
      bySourceType: { note: 1 },
      byProjectLabel: { project: 1 },
    });
    const serialized = JSON.stringify({ rejected: plan.rejected, report: plan.report });
    expect(serialized).not.toContain(secret);
    expect(serialized).not.toContain(rawPath);
    expect(serialized).not.toContain("relative/path");
  });

  it("applies collection-level model rules and treats empty source model as absent", () => {
    expect(() => buildImportPlan(planInput([point({ text: "safe", model: "other" })]))).toThrow(/model/i);
    expect(() => buildImportPlan(planInput([point({ text: "safe" })]))).toThrow(/declared source model/i);
    expect(() => buildImportPlan(planInput([point({ text: "safe", model: "" })]))).toThrow(/declared source model/i);
    expect(() => buildImportPlan(planInput([point({ text: "safe" })], { declaredSourceModel: "other" }))).toThrow(/model/i);
    expect(buildImportPlan(planInput([
      point({ text: "with model", model: "bge-m3" }, "a"),
      point({ text: "without model" }, "b"),
    ])).accepted).toHaveLength(2);
    expect(buildImportPlan(planInput([point({ text: "safe" })], { declaredSourceModel: "bge-m3" })).accepted).toHaveLength(1);
  });

  it("aborts rather than counting model and collection incompatibilities", () => {
    expect(() => buildImportPlan(planInput([
      point({ text: "safe", model: "bge-m3" }, "a"),
      point({ text: "safe", model: "wrong" }, "b"),
    ]))).toThrow(/model/i);
    expect(() => buildImportPlan(planInput([point()], { destinationDimension: 3 }))).toThrow(/dimension/i);
    expect(() => buildImportPlan(planInput([point()], { destinationDistance: "Euclid" }))).toThrow(/distance/i);
    expect(() => buildImportPlan(planInput([point(undefined, "source-1", [0.1])] as AdminPoint[]))).toThrow(/dimension/i);
  });

  it("includes all transform and source/destination contract inputs in the manifest hash", () => {
    const baseline = buildImportPlan(planInput([point()]));
    const variants = [
      { sourceIdentity: "qdrant:http://other" },
      { sourceCollection: "other_source" },
      { sourceDistance: "cosine" },
      { destinationCollection: "other_destination" },
      { destinationDistance: "cosine" },
      { targetHost: "pi" },
      { configuredModel: "other", points: [point({ text: "safe", model: "other" })] },
      { declaredSourceModel: "bge-m3" },
    ];
    for (const variant of variants) {
      const { points = [point()], ...extra } = variant as { points?: AdminPoint[] } & Record<string, unknown>;
      expect(buildImportPlan(planInput(points, extra)).planId).not.toBe(baseline.planId);
    }
  });

  it("does not mutate caller input while adding run IDs to output copies", () => {
    const input = [point({ text: "safe", model: "bge-m3", tags: ["tag"] })];
    const before = structuredClone(input);
    const plan = buildImportPlan(planInput(input));
    expect(input).toEqual(before);
    expect(plan.accepted[0]?.vector).not.toBe(input[0]?.vector);
    expect(plan.accepted[0]?.payload).not.toBe(input[0]?.payload);
    expect(input[0]?.payload).not.toHaveProperty("import_run_id");
  });
});
