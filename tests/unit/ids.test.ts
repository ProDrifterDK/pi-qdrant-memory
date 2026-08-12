import { describe, expect, it } from "vitest";
import { deterministicUuid } from "../../src/domain/canonical.js";
import { contentId, coverageId, episodeId, evidenceLinkId, isContentTarget, isOccurrenceTarget, isStateTarget, isTombstoneTarget, jobId, leasePointId, manifestHash, observationId, proposalIdFor, stateKey, tombstoneId } from "../../src/domain/ids.js";

describe("v2 canonical identities", () => {
  it("separates domain-tagged content, observation, and state identity", () => {
    const s = stateKey({ host: "pi", scope: "project", projectId: "p", category: "preference", subject: "editor", predicate: "uses" });
    expect(s).toBe("state:118a3673232e2372bd5a734748b942e4321e394965cbb7b4ae9963107c565096");
    const c = contentId("policy-hash", s, "vim");
    const o = observationId(3, c, "00000000-0000-0000-0000-000000000001", "session:7");
    expect(o).not.toBe(c);
    expect(c).toBe("content:7f04943b1072d5e8df33a37973e42e9d75ecef23db68ddc743c5b45f9d5a1101");
    expect(o).toBe("occurrence:16997fb02575cc2349ddab18462bdf31f300ad5f4a04177c44d0bac5db3e1ff0");
    expect(isStateTarget(s)).toBe(true);
    expect(isContentTarget(c)).toBe(true);
    expect(isOccurrenceTarget(o)).toBe(true);
    expect(isOccurrenceTarget("00000000-0000-0000-0000-000000000001")).toBe(true);
    expect(isContentTarget(s)).toBe(false);
    expect(isStateTarget(c)).toBe(false);
    expect(isTombstoneTarget("state", s)).toBe(true);
    expect(isTombstoneTarget("content", s)).toBe(false);
    expect(evidenceLinkId(o, "00000000-0000-0000-0000-000000000001", 1)).toBe("9ea71bc8-a769-56de-815b-c70cc759f3bd");
    expect(episodeId("pi", "session", "message", 0)).toBe("44da9599-49cb-5784-81f6-47babfbfe2bc");
    expect(jobId("pi", ["episode-1"], "policy-hash", "extractor-1", 1, "intersection-1", 0)).toBe("6a7aa8fd-065c-516a-98d2-755cc4d7e92f");
    expect(jobId({ ownerHost: "pi", membership: ["episode-1"], policyHash: "policy-hash", extractorRevision: "extractor-1", coordinationPolicyEpoch: 1, policyIntersectionId: "intersection-1", privacyEpoch: 0 })).toBe("6a7aa8fd-065c-516a-98d2-755cc4d7e92f");
    expect(manifestHash(["episode-1", "episode-2"])).toBe("c66d126b3b0eb3908da158b051ca0b6682d1ccfe9897b6ef6c3d1b57302e64e8");
    expect(tombstoneId("pi", "content-1")).toBe("f9d30453-fff5-5454-adc5-d4d10f4992ab");
    expect(tombstoneId("prime", "content-1")).not.toBe(tombstoneId("pi", "content-1"));
    expect(coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "extractor-1", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 3, policyIntersectionId: "inter-1", privacyEpoch: 0 })).toBe("15de7458-5b07-5e9d-987d-e8fff6641b08");
    expect(leasePointId("job-1")).toBe("a753361e-2c74-550d-9d52-6f5762a37a4b");
    expect(proposalIdFor("job-1", "a".repeat(64), 3, 2)).toBe("6301d96a-1c3a-5fab-b22f-848e4361e687");
    expect(deterministicUuid("pi-qdrant-memory-v2", "pi", "session", "message")).toBe("f4058bea-7025-52ff-b0b8-7fbfc756d360");
  });
  it("is stable, order-sensitive only where the identity domain is order-sensitive", () => {
    const input = { host: "pi" as const, scope: "project", projectId: "p", category: "a", subject: "b", predicate: "c" };
    expect(stateKey(input)).toBe(stateKey({ predicate: "c", subject: "b", category: "a", projectId: "p", scope: "project", host: "pi" }));
    expect(episodeId("pi", "session", "message", 0)).toBe(episodeId("pi", "session", "message", 0));
    expect(episodeId("pi", "session", "message", 0)).not.toBe(episodeId("prime", "session", "message", 0));
    expect(jobId("pi", ["b", "a"], "policy", "extractor", 1, "i", 0)).not.toBe(jobId("pi", ["a", "b"], "policy", "extractor", 1, "i", 0));
    expect(jobId("pi", ["a"], "policy", "extractor", 1, "i", 0)).not.toBe(jobId("pi", ["a"], "policy", "extractor", 2, "i", 0));
    expect(jobId("pi", ["a"], "policy", "extractor", 1, "i", 0)).not.toBe(jobId("pi", ["a"], "policy", "extractor", 1, "other", 0));
    expect(jobId("pi", ["a"], "policy", "extractor", 1, "i", 0)).not.toBe(jobId("pi", ["a"], "policy", "extractor", 1, "i", 2));
    expect(manifestHash(["a", "b"])).not.toBe(manifestHash(["b", "a"]));
    expect(tombstoneId("pi", "content-id")).not.toBe(tombstoneId("pi", "state-id"));
    expect(coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "extractor-1", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 1, policyIntersectionId: "inter-1", privacyEpoch: 0 })).not.toBe(coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "extractor-2", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 1, policyIntersectionId: "inter-1", privacyEpoch: 0 }));
    expect(coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "extractor-1", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 1, policyIntersectionId: "inter-1", privacyEpoch: 0 })).not.toBe(coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "extractor-1", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 2, policyIntersectionId: "inter-1", privacyEpoch: 0 }));
    expect(leasePointId("job-1")).not.toBe(leasePointId("job-2"));
    expect(proposalIdFor("job-1", "a".repeat(64), 1, 1)).not.toBe(proposalIdFor("job-1", "b".repeat(64), 1, 1));
  });
});

describe("identity input validation", () => {
  it("rejects empty extractor revisions and unordered/empty membership", () => {
    expect(() => evidenceLinkId("observation", "episode", "")).toThrow(/revision/i);
    expect(() => jobId("pi", [], "policy", "extractor", 1, "i", 0)).toThrow(/membership/i);
    expect(() => manifestHash([])).toThrow(/member/i);
    expect(() => manifestHash(Array.from({ length: 1025 }, (_, index) => `episode-${index}`))).toThrow(/member|bounded/i);
    expect(() => jobId("pi", ["api-token"], "policy", "extractor", 1, "i", 0)).toThrow(/membership|redacted/i);
    expect(() => evidenceLinkId("observation", "episode", "api-token")).toThrow(/revision|redacted/i);
    expect(() => tombstoneId("pi", "api-token")).toThrow(/target|redacted/i);
    expect(() => tombstoneId("pi", "")).toThrow(/target/i);
    expect(() => coverageId({ ownerHost: "pi", episodeId: "", extractorRevision: "extractor-1", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 1, policyIntersectionId: "inter-1", privacyEpoch: 0 })).toThrow(/episode/i);
    expect(() => coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 1, policyIntersectionId: "inter-1", privacyEpoch: 0 })).toThrow(/extractor/i);
    expect(() => coverageId({ ownerHost: "pi", episodeId: "ep-1", extractorRevision: "extractor-1", coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: -1, policyIntersectionId: "inter-1", privacyEpoch: 0 })).toThrow(/epoch/i);
    expect(() => leasePointId("")).toThrow(/job/i);
    expect(() => proposalIdFor("job-1", "short", 1, 1)).toThrow(/hash/i);
    expect(() => proposalIdFor("job-1", "a".repeat(64), 1, -1)).toThrow(/fencing/i);
    expect(() => observationId(0, "content", "episode", "session:-1")).toThrow(/effective|sequence/i);
    expect(() => observationId(0, "content", "episode", ["2026-08-10T00:00:00.000Z", "episode"])).toThrow(/effective|tuple/i);
  });
});
