import { describe, expect, it } from "vitest";
import { isMemoryRecord, isPersistedMemoryRecord, parseMemoryRecord, parsePersistedMemoryRecord, assertCanonicalRecordHash, canonicalRecordHash, type MemoryRecord } from "../../src/domain/records.js";
import { processingPolicyHash } from "../../src/domain/policy.js";
import { manifestHash } from "../../src/domain/ids.js";

const envelope = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 2, processingPolicyId: "policy-1", expiresAt: null, contentHash: "hash" };
const derivedEnvelope = { ...envelope, coordinationPolicyHash: "coordination-hash", coordinationPolicyEpoch: 3 };
const policyBase = { id: "pending", ownerHost: "pi" as const, destinationIds: { qdrant: "qdrant-local", embedding: "embedding-local", llm: "llm-local" }, originProvider: "provider-a", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "revision-1" };
const policy = { ...policyBase, id: processingPolicyHash(policyBase) };
const episode = (): MemoryRecord => ({ ...envelope, recordType: "episode", id: "episode-1", contentHash: "hash", sourceEntryId: "source-1", host: "pi", projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2026-08-10T00:00:00.000Z", modelId: "provider-model", embeddingDimension: 1024, originProvider: "provider-a", destinationId: "local-node-a", status: "active", secretScan: "passed", text: "safe" });

describe("v2 record schemas", () => {
  it("accepts every discriminated record type with provenance closure", () => {
    const records: MemoryRecord[] = [
      episode(),
      { ...derivedEnvelope, recordType: "curated_memory", id: "curated-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] },
      { ...derivedEnvelope, recordType: "curated_current", id: "current-1", contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "resolved", effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1"] },
      { ...derivedEnvelope, recordType: "curated_current", id: "conflict-1", version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", effectiveOrder: "session:1" },
      { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-1", generationId: "generation-1", clusterId: "cluster-1", membershipHash: manifestHash(["episode-1"]), level: 0, memberIds: ["episode-1"], summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } },
      { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-manifest-1", generationId: "generation-1", clusterId: "cluster-2", membershipHash: "manifest-hash", level: 1, manifestHash: "manifest-hash", summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } },
      { ...envelope, recordType: "collection_control", id: "control-1", version: 1, activeGeneration: null, activeBaseGeneration: null, privacyEpoch: 2, coordinationPolicyEpoch: 3, coordinationPolicyHash: "coordination-hash", state: "active", scanCursor: null, lastForgetBarrier: null },
      { ...envelope, processingPolicyId: policy.id, recordType: "processing_policy", id: policy.id, policy, canonicalHash: policy.id, expiresAt: policy.expiresAt },
      { ...derivedEnvelope, recordType: "job", id: "job-1", policyId: "policy-1", policyHash: "policy-hash", policyEpoch: 3, membership: ["episode-1"], state: "accepted", leaseExpiresAt: null, fencingToken: 1, leaseOwner: "worker-1", acceptedProposalId: "proposal-1", acceptedManifestHash: "manifest-1" },
      { ...derivedEnvelope, recordType: "coverage", id: "coverage-1", episodeId: "episode-1", extractorRevision: "extractor-1" },
      { ...derivedEnvelope, recordType: "evidence_link", id: "link-1", sourceId: "curated-1", targetId: "episode-1", jobId: "job-1", extractorRevision: "extractor-1" },
      { ...envelope, recordType: "tombstone", id: "tombstone-1", scope: "occurrence", targetId: "episode-1" },
    ];
    for (const [index, record] of records.entries()) expect(isMemoryRecord(record), `record ${index} ${record.recordType}`).toBe(true);
  });
  it("rejects unknown fields/types, missing closure, wrong epochs, unbounded text, vectors, and secret IDs", () => {
    expect(isMemoryRecord({ ...episode(), recordType: "unknown" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), extra: true })).toBe(false);
    expect(isMemoryRecord({ ...episode(), id: "api-key=secret" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), text: "x".repeat(16001) })).toBe(false);
    expect(isMemoryRecord({ ...episode(), vector: [0, Number.NaN] })).toBe(false);
    expect(isMemoryRecord({ ...episode(), ownerHost: "prime" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), schemaRevision: 2 })).toBe(false);
    expect(isMemoryRecord({ ...episode(), privacyEpoch: 3 }, { privacyEpoch: 2 })).toBe(false);
    expect(isMemoryRecord({ ...episode(), sourceEntryId: "" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), eventKind: "bogus" })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "coverage", id: "coverage-1", episodeId: "episode-1", extractorRevision: "api-token" })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "evidence_link", id: "link-1", sourceId: "source-1", targetId: "target-1", jobId: "job-1", extractorRevision: "x".repeat(513) })).toBe(false);
    expect(isMemoryRecord({ ...episode(), status: "stale" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), projectIdentityKind: "spoofed" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), modelId: "" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), embeddingDimension: 1536 })).toBe(false);
    expect(isMemoryRecord({ ...episode(), recordType: "curated_memory", contentId: "content", observationId: "observation", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt })).toBe(false);
    expect(isMemoryRecord({ ...episode(), recordType: "raptor_summary", generationId: "g", membershipHash: "m", level: 0 })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "raptor_summary", id: "summary-1", generationId: "generation-1", clusterId: "cluster-1", membershipHash: "wrong", level: 0, memberIds: ["episode-1"], summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: {} })).toBe(false);
    expect(isMemoryRecord({ ...episode(), coordinationPolicyEpoch: 4 }, { coordinationPolicyEpoch: 3 })).toBe(false);
    expect(isMemoryRecord({ ...episode(), createdAt: "not-a-date" })).toBe(false);
    expect(isMemoryRecord({ ...episode(), eventAt: "2026-02-30T00:00:00.000Z" })).toBe(false);
  });
  it("checks processing policy owner, shape, and canonical hash contract", () => {
    const persistedPolicy = { ...envelope, processingPolicyId: policy.id, recordType: "processing_policy", id: policy.id, policy, canonicalHash: policy.id, expiresAt: policy.expiresAt };
    expect(isMemoryRecord(persistedPolicy)).toBe(true);
    expect(isMemoryRecord({ ...persistedPolicy, processingPolicyId: "policy-1" })).toBe(false);
    expect(isMemoryRecord({ ...persistedPolicy, expiresAt: "2026-08-11T00:00:00.000Z" })).toBe(false);
    expect(isPersistedMemoryRecord({ ...persistedPolicy, processingPolicyId: "policy-1" })).toBe(false);
    expect(isPersistedMemoryRecord({ ...persistedPolicy, expiresAt: "2026-08-11T00:00:00.000Z" })).toBe(false);
    expect(isMemoryRecord({ ...persistedPolicy, policy: { ...policy, ownerHost: "prime" } })).toBe(false);
    expect(isMemoryRecord({ ...persistedPolicy, policy: { ...policy, destinationIds: { ...policy.destinationIds, qdrant: "" } } })).toBe(false);
    expect(isMemoryRecord({ ...persistedPolicy, policy: { ...policy, id: "not-content-addressed" } })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "curated_current", id: "current-1", contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1"] })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "curated_current", id: "current-1", version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", contentId: undefined, effectiveOrder: "session:1" })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "curated_current", id: "current-1", contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "resolved", conflictManifestHash: "manifest-1", effectiveOrder: "session:1" })).toBe(false);
  });
  it("hashes content invariant to delivery timestamps and vector floats", () => {
    const first = episode(); const second = { ...first, createdAt: "2026-08-11T00:00:00.000Z", vector: Array.from({ length: 1024 }, (_, index) => index / 1024) };
    expect(canonicalRecordHash(first)).toBe(canonicalRecordHash(second));
    expect(canonicalRecordHash({ ...first, text: "changed" })).not.toBe(canonicalRecordHash(first));
    const hashed = { ...first, contentHash: canonicalRecordHash(first) };
    expect(hashed.contentHash).toBe(canonicalRecordHash(hashed));
    expect(() => assertCanonicalRecordHash(hashed)).not.toThrow();
    expect(() => assertCanonicalRecordHash(first)).toThrow(/hash/i);
  });
  it("applies policy epoch context only to job records", () => {
    expect(isMemoryRecord(episode(), { policyEpoch: 3 })).toBe(true);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "job", id: "job-1", policyId: "other-policy", policyHash: "policy-hash", policyEpoch: 3, membership: ["episode-1"], state: "pending", leaseExpiresAt: null, fencingToken: 1, leaseOwner: null, acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "job", id: "job-1", policyId: "policy-1", policyHash: "policy-hash", policyEpoch: 3, membership: ["episode-1"], state: "pending", leaseExpiresAt: null, fencingToken: 1, leaseOwner: null, acceptedProposalId: null, acceptedManifestHash: null }, { policyEpoch: 2 })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "job", id: "job-1", policyId: "policy-1", policyHash: "policy-hash", policyEpoch: 3, membership: ["episode-1"], state: "leased", leaseExpiresAt: null, fencingToken: 1, leaseOwner: null, acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "job", id: "job-1", policyId: "policy-1", policyHash: "policy-hash", policyEpoch: 3, membership: ["episode-1"], state: "accepted", leaseExpiresAt: null, fencingToken: 1, leaseOwner: null, acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
  });
  it("separates structural and persisted canonical-hash validation", () => {
    expect(isMemoryRecord({ ...episode(), contentHash: "not-canonical" })).toBe(true);
    expect(() => parsePersistedMemoryRecord({ ...episode(), contentHash: "not-canonical" })).toThrow(/canonical|hash/i);
    expect(isPersistedMemoryRecord({ ...episode(), contentHash: "not-canonical" })).toBe(false);
  });
  it("rejects malformed and secret effective orders in structural and persisted guards", () => {
    const resolved = { ...derivedEnvelope, recordType: "curated_memory", id: "curated-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] };
    expect(isMemoryRecord({ ...resolved, effectiveOrder: { sequence: 1 } })).toBe(false);
    expect(isMemoryRecord({ ...resolved, effectiveOrder: ["2026-08-10T00:00:00.000Z", "api-token", "content-1"] })).toBe(false);
    expect(isPersistedMemoryRecord({ ...resolved, effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1"] })).toBe(false);
    expect(() => canonicalRecordHash({ ...resolved, effectiveOrder: { sequence: 1 } } as MemoryRecord)).toThrow(/effective/i);
    expect(isMemoryRecord({ ...resolved, effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1", "extra"] })).toBe(false);
  });
  it("throws a typed error from the explicit guard", () => expect(() => parseMemoryRecord({ ...episode(), ownerHost: "prime" })).toThrow(/host/i));
});
