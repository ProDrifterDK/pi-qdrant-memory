import { describe, expect, it } from "vitest";
import { buildManifest, verifyManifest } from "../../src/raptor/manifest.js";
import { generationIsVisible, publicationIdentity, publishGeneration, type Generation } from "../../src/raptor/publication.js";
import { buildRaptorGeneration } from "../../src/raptor/builder.js";
import { LeaseAuthority, ProductionCoordinationStore } from "../../src/qdrant/write.js";

function generation(id: string, baseGeneration: string | null = "old"): Generation {
  return Object.freeze({ id, manifestRoot: `root-${id}`, membershipHash: `members-${id}`, baseGeneration, privacyEpoch: 2, coordinationPolicyEpoch: 4, coordinationPolicyHash: "policy-hash", jobId: "job-1", fencingToken: 3, status: "building" });
}
describe("Task 10 manifests and publication", () => {
  it("builds bounded sorted content-addressed chunks and a deterministic Merkle root", () => {
    const ids = Array.from({ length: 17 }, (_, index) => `leaf-${String(index).padStart(3, "0")}`).reverse();
    const input = { ownerHost: "pi" as const, leafIds: ids, chunkSize: 4, policyId: "policy", policyHash: "coord", policyEpoch: 4, privacyEpoch: 2, algorithm: "raptor-v1", promptRevision: "raptor-summary-v1", modelId: "model", seed: "seed-v1" };
    const first = buildManifest(input);
    const second = buildManifest({ ...input, leafIds: [...ids].sort() });
    expect(first).toEqual(second);
    expect(first.chunks.every((chunk) => chunk.memberIds.length <= 4)).toBe(true);
    expect(first.chunks.flatMap((chunk) => chunk.memberIds)).toEqual([...ids].sort());
    expect(verifyManifest(first)).toBe(true);
    expect(first.root.membershipHash).toMatch(/^[a-f0-9]{64}$/u);
    expect(first.root.merkleRoot).toMatch(/^[a-f0-9]{64}$/u);
  });

  it("binds generation identity to manifest, policy, algorithm, prompt, model and seed", () => {
    const a = publicationIdentity({ manifestRoot: "m", membershipHash: "mh", baseGeneration: null, privacyEpoch: 1, coordinationPolicyEpoch: 2, coordinationPolicyHash: "p", policyId: "i", algorithm: "a", promptRevision: "r", modelId: "model", seed: "s" });
    const b = publicationIdentity({ manifestRoot: "m", membershipHash: "mh", baseGeneration: null, privacyEpoch: 1, coordinationPolicyEpoch: 2, coordinationPolicyHash: "p", policyId: "i", algorithm: "a", promptRevision: "r", modelId: "model", seed: "different" });
    expect(a).not.toBe(b);
    expect(a).toMatch(/^[a-f0-9]{64}$/u);
  });

  it("keeps builder and publication capability-first before touching caller accessors", async () => {
    let reads = 0; const input = Object.create(Object.prototype) as Record<string, unknown>; Object.defineProperty(input, "host", { enumerable: true, get() { reads += 1; throw new Error("invoked"); } });
    expect(await buildRaptorGeneration({} as ProductionCoordinationStore, {} as LeaseAuthority, input as never)).toEqual({ state: "pending", reason: "invalid_input" }); expect(reads).toBe(0);
  });

  it("keeps publication nominal and visibility reachable only from active control", async () => {
    const built = generation("a");
    expect(generationIsVisible({ state: "active", activeGeneration: "a", privacyEpoch: 2, coordinationPolicyEpoch: 4, coordinationPolicyHash: "policy-hash" }, built)).toBe(true);
    expect(generationIsVisible({ state: "active", activeGeneration: "old", privacyEpoch: 2, coordinationPolicyEpoch: 4, coordinationPolicyHash: "policy-hash" }, built)).toBe(false);
    await expect(publishGeneration({} as never, {} as never, { control: {} as never, generation: built, tombstoneTargets: ["leaf-a"], destinationIds: ["q", "e", "l"] })).rejects.toThrow(/genuine production capabilities/i);
  });
});
