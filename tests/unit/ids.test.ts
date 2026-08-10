import { describe, expect, it } from "vitest";
import { deterministicUuid } from "../../src/domain/canonical.js";
import { contentId, episodeId, evidenceLinkId, jobId, manifestHash, observationId, stateKey, tombstoneId } from "../../src/domain/ids.js";

describe("v2 canonical identities", () => {
  it("separates content, observation, and evidence identity", () => {
    const s = stateKey({ host: "pi", scope: "project", projectId: "p", category: "preference", subject: "editor", predicate: "uses" });
    expect(s).toBe("798e710818b455f5b79f37351cd7063d19335da1afe57d9fe3973d758326020e");
    const c = contentId("policy-hash", s, "vim");
    const o = observationId(3, c, "00000000-0000-0000-0000-000000000001", "session:7");
    expect(o).not.toBe(c);
    expect(c).toBe("820db3b862ec6bfc69d43d3d6d97c534bdcf1b55aed9774e2e86b05e306da8d1");
    expect(o).toBe("ae93cd1fe2df732b782ad421de6f3d854d26a5d3a890af063791823c9347f5f9");
    expect(evidenceLinkId(o, "00000000-0000-0000-0000-000000000001", 1)).toBe("2a05e51c-2c9c-5f09-af19-2a954c7d59b1");
    expect(episodeId("pi", "session", "message", 0)).toBe("44da9599-49cb-5784-81f6-47babfbfe2bc");
    expect(jobId("pi", ["episode-1"], "policy-hash", "extractor-1")).toBe("ebb713f3-5dd2-556f-b7a7-eefde844d99d");
    expect(manifestHash(["episode-1", "episode-2"])).toBe("c66d126b3b0eb3908da158b051ca0b6682d1ccfe9897b6ef6c3d1b57302e64e8");
    expect(tombstoneId("content", "content-1")).toBe("08e85e1f-eedc-5be8-a240-cd0e9a76c423");
    expect(deterministicUuid("pi-qdrant-memory-v2", "pi", "session", "message")).toBe("f4058bea-7025-52ff-b0b8-7fbfc756d360");
  });
  it("is stable, order-sensitive only where the identity domain is order-sensitive", () => {
    const input = { host: "pi" as const, scope: "project", projectId: "p", category: "a", subject: "b", predicate: "c" };
    expect(stateKey(input)).toBe(stateKey({ predicate: "c", subject: "b", category: "a", projectId: "p", scope: "project", host: "pi" }));
    expect(episodeId("pi", "session", "message", 0)).toBe(episodeId("pi", "session", "message", 0));
    expect(episodeId("pi", "session", "message", 0)).not.toBe(episodeId("prime", "session", "message", 0));
    expect(jobId("pi", ["b", "a"], "policy", "extractor")).not.toBe(jobId("pi", ["a", "b"], "policy", "extractor"));
    expect(manifestHash(["a", "b"])).not.toBe(manifestHash(["b", "a"]));
    expect(tombstoneId("content", "content-id")).not.toBe(tombstoneId("state", "content-id"));
  });
});


describe("identity input validation", () => {
  it("rejects empty extractor revisions and unordered/empty membership", () => {
    expect(() => evidenceLinkId("observation", "episode", "")).toThrow(/revision/i);
    expect(() => jobId("pi", [], "policy", "extractor")).toThrow(/membership/i);
    expect(() => manifestHash([])).toThrow(/member/i);
    expect(() => manifestHash(Array.from({ length: 1025 }, (_, index) => `episode-${index}`))).toThrow(/member|bounded/i);
    expect(() => jobId("pi", ["api-token"] , "policy", "extractor")).toThrow(/membership|redacted/i);
    expect(() => evidenceLinkId("observation", "episode", "api-token")).toThrow(/revision|redacted/i);
    expect(() => tombstoneId("content", "content-id", "api-token")).toThrow(/provenance|redacted/i);
    expect(() => observationId(0, "content", "episode", "session:-1")).toThrow(/effective|sequence/i);
    expect(() => observationId(0, "content", "episode", ["2026-08-10T00:00:00.000Z", "episode"])).toThrow(/effective|tuple/i);
  });
});
