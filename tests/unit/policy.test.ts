import { describe, expect, it } from "vitest";
import { intersectPolicies, isPolicyExpired, processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { assertEgressAllowed, destinationForEndpoint, isDestinationAllowed } from "../../src/security/egress.js";

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const value = { id: "pending", ownerHost: "pi" as const, destinationIds: { qdrant: "q", embedding: "e", llm: "l" }, originProvider: "provider-a", allowCrossProviderReplay: false, expiresAt: "2026-08-20T00:00:00.000Z", residency: "eu", dataUse: "memory", policyRevision: "r1", ...overrides };
  return { ...value, id: processingPolicyHash(value) };
}

describe("producer processing policy", () => {
  it("intersects exact common capabilities and carries earliest expiry", () => {
    const result = intersectPolicies([policy({ expiresAt: "2026-08-12T00:00:00.000Z" })], policy({ expiresAt: "2026-08-20T00:00:00.000Z", policyRevision: "worker-r" }));
    expect(result).not.toBeNull();
    expect(result?.destinationIds).toEqual({ qdrant: "q", embedding: "e", llm: "l" });
    expect(result?.expiresAt).toBe("2026-08-12T00:00:00.000Z");
    expect(result?.policyRevision).toBe("worker-r");
    expect(result?.id).toBe(processingPolicyHash(result!));
  });
  it("fails closed on owner, capability, residency/data-use, and provider mismatches", () => {
    expect(intersectPolicies([policy({ ownerHost: "prime" })], policy())).toBeNull();
    expect(intersectPolicies([policy({ destinationIds: { qdrant: "other", embedding: "e", llm: "l" } })], policy())).toBeNull();
    expect(intersectPolicies([policy({ residency: "us" })], policy())).toBeNull();
    expect(intersectPolicies([policy({ originProvider: "provider-b" })], policy())).toBeNull();
  });
  it("rejects producer snapshots whose IDs are not content addressed", () => {
    expect(() => intersectPolicies([{ ...policy(), id: "arbitrary" }], policy())).toThrow(/content addressed/i);
  });
  it("never widens replay permissions even when providers match", () => {
    const source = policy({ allowCrossProviderReplay: false });
    const worker = policy({ allowCrossProviderReplay: true });
    expect(intersectPolicies([source], worker)?.allowCrossProviderReplay).toBe(false);
  });
  it("requires explicit cross-provider replay and preserves revision while hashing it", () => {
    const result = intersectPolicies([policy({ originProvider: "provider-b", allowCrossProviderReplay: true })], policy({ allowCrossProviderReplay: true, policyRevision: "worker-r" }));
    expect(result?.allowCrossProviderReplay).toBe(true);
    expect(result?.policyRevision).toBe("worker-r");
    expect(processingPolicyHash(policy({ policyRevision: "r1" }))).not.toBe(processingPolicyHash(policy({ policyRevision: "r2" })));
  });
  it("keeps local-only destinations node-bound and rejects forged or unauthorized egress", () => {
    const local = destinationForEndpoint("http://127.0.0.1:6333", "node-a");
    expect(local.id).toMatch(/^local:/);
    expect(isDestinationAllowed("local_only", local, [], { nodeId: "node-a" })).toBe(true);
    expect(isDestinationAllowed("local_only", { id: local.id, residency: "local", dataUse: "memory" }, [], { nodeId: "node-b" })).toBe(false);
    expect(isDestinationAllowed("local_only", { ...local, endpoint: "https://external.example", nodeId: "node-a" }, [], { nodeId: "node-a" })).toBe(false);
    expect(isDestinationAllowed("local_only", { ...local, nodeId: "node-b" }, [], { nodeId: "node-a" })).toBe(false);
    expect(() => destinationForEndpoint("https://external.example", "node-a")).toThrow(/loopback/i);
    expect(() => destinationForEndpoint("http://127.0.0.1:6333")).toThrow(/node/i);
    expect(() => assertEgressAllowed({ mode: "allowlist", destination: local, allowlist: [] })).toThrow(/authorized/i);
  });
  it("rejects encoded secret material in URL and Unix endpoint paths", () => {
    expect(() => destinationForEndpoint("http://127.0.0.1:6333/%41%4b%49%41ABCDEFGHIJKLMNOP", "node-redacted")).toThrow();
    expect(() => destinationForEndpoint("http://127.0.0.1:6333/%70assword=hunter2long", "node-redacted")).toThrow();
    expect(() => destinationForEndpoint("unix:/tmp/%70assword=hunter2long", "node-redacted")).toThrow();
    expect(() => destinationForEndpoint("http://127.0.0.1:6333/%ZZ", "node-redacted")).toThrow();
    expect(destinationForEndpoint("http://127.0.0.1:6333/%63ollections", "node-redacted").endpoint).toContain("%63ollections");
  });

  it("rejects secret node IDs and endpoint path material before destination construction", () => {
    const high = "opaque 0123456789abcdef0123456789abcdef0123456789abcdef";
    for (const nodeId of ["sk-abcdefghijklmnopqrstuvwxyz123456", high]) expect(() => destinationForEndpoint("http://127.0.0.1:6333", nodeId)).toThrow(/node/i);
    expect(() => destinationForEndpoint("http://127.0.0.1:6333/sk-abcdefghijklmnopqrstuvwxyz123456", "node-redacted")).toThrow();
    expect(() => destinationForEndpoint(`http://127.0.0.1:6333/${high}`, "node-redacted")).toThrow();
    expect(destinationForEndpoint("http://127.0.0.1:6333/collections", "node-redacted").nodeId).toBe("node-redacted");
  });
  it("handles expiry conservatively", () => expect(isPolicyExpired(policy(), Date.parse("2026-08-20T00:00:00.000Z"))).toBe(true));
});
