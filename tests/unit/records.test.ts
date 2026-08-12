import { describe, expect, it } from "vitest";
import { isMemoryRecord, isPersistedMemoryRecord, parseMemoryRecord, parsePersistedMemoryRecord, assertCanonicalRecordHash, canonicalRecordHash, type MemoryRecord } from "../../src/domain/records.js";
import { processingPolicyHash } from "../../src/domain/policy.js";
import { manifestHash, proposalContentHash, proposalIdFor } from "../../src/domain/ids.js";

const envelope = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 2, processingPolicyId: "policy-1", expiresAt: null, contentHash: "hash" };
const derivedEnvelope = { ...envelope, coordinationPolicyHash: "coordination-hash", coordinationPolicyEpoch: 3 };
const policyBase = { id: "pending", ownerHost: "pi" as const, destinationIds: { qdrant: "qdrant-local", embedding: "embedding-local", llm: "llm-local" }, originProvider: "provider-a", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "revision-1" };
const policy = { ...policyBase, id: processingPolicyHash(policyBase) };
const episode = (): MemoryRecord => ({ ...envelope, recordType: "episode", id: "episode-1", contentHash: "hash", sourceEntryId: "source-1", host: "pi", projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2026-08-10T00:00:00.000Z", modelId: "provider-model", embeddingDimension: 1024, originProvider: "provider-a", destinationId: "local-node-a", status: "active", redactionStatus: "unchanged", secretScan: "passed", text: "safe" });

describe("v2 record schemas", () => {
  it("accepts every discriminated record type with provenance closure", () => {
    const records: MemoryRecord[] = [
      episode(),
      { ...derivedEnvelope, recordType: "curated_memory", id: "curated-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] },
      { ...derivedEnvelope, recordType: "curated_current", id: "current-1", contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "resolved", effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1"] },
      { ...derivedEnvelope, recordType: "curated_current", id: "conflict-1", version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", effectiveOrder: "session:1" },
      { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-1", generationId: "generation-1", clusterId: "cluster-1", membershipHash: manifestHash(["episode-1"]), level: 0, memberIds: ["episode-1"], summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } },
      { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-manifest-1", generationId: "generation-1", clusterId: "cluster-2", membershipHash: "manifest-hash", level: 1, manifestHash: "manifest-hash", summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } },
      { ...envelope, recordType: "collection_control", id: "control-1", version: 1, activeGeneration: null, activeBaseGeneration: null, privacyEpoch: 2, coordinationPolicyEpoch: 3, coordinationPolicyHash: "coordination-hash", state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [] },
      { ...envelope, processingPolicyId: policy.id, recordType: "processing_policy", id: policy.id, policy, canonicalHash: policy.id, expiresAt: policy.expiresAt },
      { ...derivedEnvelope, recordType: "job", id: "50e7c5c9-bbfb-501e-9d7c-9179abd4e901", policyId: "policy-1", policyHash: "coordination-hash", policyEpoch: 3, membership: ["episode-1"], extractorRevision: "extractor-1" },
      { ...derivedEnvelope, recordType: "coverage", id: "4a780177-f166-5aa9-a98c-f75cb0e55ba1", episodeId: "episode-1", extractorRevision: "extractor-1" },
      { ...derivedEnvelope, recordType: "evidence_link", id: "link-1", sourceId: "curated-1", targetId: "episode-1", jobId: "job-1", extractorRevision: "extractor-1" },
      { ...envelope, recordType: "tombstone", id: "d6b6113a-fb37-546c-9807-b3f8ee692694", scope: "occurrence", targetId: "00000000-0000-5000-8000-000000000001" },
      { ...derivedEnvelope, recordType: "lease", id: "a753361e-2c74-550d-9d52-6f5762a37a4b", jobId: "job-1", ownerId: "worker-1", version: 2, fencingToken: 3, expiresAt: "2026-08-11T00:00:00.000Z", state: "leased", acceptedProposalId: null, acceptedManifestHash: null },
      { ...derivedEnvelope, recordType: "lease", id: "a753361e-2c74-550d-9d52-6f5762a37a4b", jobId: "job-1", ownerId: "worker-1", version: 3, fencingToken: 3, expiresAt: "2026-08-11T00:00:00.000Z", state: "accepted", acceptedProposalId: "fd26720c-f8ab-5c49-8c81-3466669e575f", acceptedManifestHash: "42a341584d40e9d1e6228d5ca7bd003104fc3ab282f5aa53316bd9c6b1d89061" },
      { ...derivedEnvelope, recordType: "proposal", id: proposalIdFor("job-1", proposalContentHash({ ownerHost: "pi", jobId: "job-1", ownerId: "worker-1", membership: ["episode-1"], content: { summary: "safe summary" }, policyHash: "coordination-hash", policyEpoch: 3, fencingToken: 3, privacyEpoch: 2, policyIntersectionId: "policy-1" }), 3, 3), jobId: "job-1", ownerId: "worker-1", proposalHash: proposalContentHash({ ownerHost: "pi", jobId: "job-1", ownerId: "worker-1", membership: ["episode-1"], content: { summary: "safe summary" }, policyHash: "coordination-hash", policyEpoch: 3, fencingToken: 3, privacyEpoch: 2, policyIntersectionId: "policy-1" }), manifestHash: "42a341584d40e9d1e6228d5ca7bd003104fc3ab282f5aa53316bd9c6b1d89061", fencingToken: 3, membership: ["episode-1"], content: { summary: "safe summary" } },
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
  it("binds the episode hash to the exact vector while keeping timestamps excluded and other records vector-excluded", () => {
    const first = episode();
    // Delivery timestamps stay excluded from identity.
    expect(canonicalRecordHash(first)).toBe(canonicalRecordHash({ ...first, createdAt: "2026-08-11T00:00:00.000Z" }));
    // An episode WITH a vector hashes DIFFERENTLY from the no-vector source...
    const withVector = { ...first, vector: Array.from({ length: 1024 }, (_, index) => index / 1024) };
    expect(canonicalRecordHash(withVector)).not.toBe(canonicalRecordHash(first));
    // ...and a single changed coordinate changes the hash (cryptographic binding).
    const changedCoordinate = { ...withVector, vector: withVector.vector!.map((value, index) => (index === 7 ? value + 1 : value)) };
    expect(canonicalRecordHash(changedCoordinate)).not.toBe(canonicalRecordHash(withVector));
    // Identical vectors hash identically.
    expect(canonicalRecordHash(withVector)).toBe(canonicalRecordHash({ ...withVector, vector: [...withVector.vector!] }));
    // Non-episode records keep the contractual vector exclusion.
    const curated: MemoryRecord = { ...derivedEnvelope, recordType: "curated_memory", id: "curated-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] };
    expect(canonicalRecordHash(curated)).toBe(canonicalRecordHash({ ...curated, vector: Array.from({ length: 1024 }, () => 0.25) }));
    const raptor: MemoryRecord = { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-1", generationId: "generation-1", clusterId: "cluster-1", membershipHash: manifestHash(["episode-1"]), level: 0, memberIds: ["episode-1"], summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } };
    expect(canonicalRecordHash(raptor)).toBe(canonicalRecordHash({ ...raptor, vector: Array.from({ length: 1024 }, () => 0.25) }));
    expect(canonicalRecordHash({ ...first, text: "changed" })).not.toBe(canonicalRecordHash(first));
    const hashed = { ...first, contentHash: canonicalRecordHash(first) };
    expect(hashed.contentHash).toBe(canonicalRecordHash(hashed));
    expect(() => assertCanonicalRecordHash(hashed)).not.toThrow();
    expect(() => assertCanonicalRecordHash(first)).toThrow(/hash/i);
  });
  it("applies policy epoch context only to job records and separates lease/acceptance state", () => {
    const pendingJob = { ...derivedEnvelope, recordType: "job", id: "50e7c5c9-bbfb-501e-9d7c-9179abd4e901", policyId: "policy-1", policyHash: "coordination-hash", policyEpoch: 3, membership: ["episode-1"], extractorRevision: "extractor-1" };
    expect(isMemoryRecord(episode(), { policyEpoch: 3 })).toBe(true);
    expect(isMemoryRecord({ ...pendingJob, policyId: "other-policy" })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob, policyHash: "policy-hash" })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob, policyEpoch: 2 })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob }, { policyEpoch: 2 })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob, id: "forged-id" })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob, membership: ["episode-2"] })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob, extractorRevision: "" })).toBe(false);
    expect(isMemoryRecord({ ...pendingJob, state: "leased" })).toBe(false);
    const leaseBase = { ...derivedEnvelope, recordType: "lease", id: "a753361e-2c74-550d-9d52-6f5762a37a4b", jobId: "job-1", ownerId: "worker-1", expiresAt: "2026-08-11T00:00:00.000Z" };
    expect(isMemoryRecord({ ...leaseBase, version: 0, fencingToken: 1, state: "leased", acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
    expect(isMemoryRecord({ ...leaseBase, version: 1, fencingToken: -1, state: "leased", acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
    expect(isMemoryRecord({ ...leaseBase, version: 1, fencingToken: 1, state: "expired", acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
    expect(isMemoryRecord({ ...leaseBase, version: 1, fencingToken: 1, state: "released", acceptedProposalId: null, acceptedManifestHash: null })).toBe(true);
    expect(isMemoryRecord({ ...leaseBase, version: 1, fencingToken: 1, state: "leased", acceptedProposalId: "proposal-1", acceptedManifestHash: null })).toBe(false);
    expect(isMemoryRecord({ ...leaseBase, version: 1, fencingToken: 1, state: "accepted", acceptedProposalId: "fd26720c-f8ab-5c49-8c81-3466669e575f", acceptedManifestHash: "42a341584d40e9d1e6228d5ca7bd003104fc3ab282f5aa53316bd9c6b1d89061" })).toBe(true);
    expect(isMemoryRecord({ ...leaseBase, version: 1, fencingToken: 1, state: "leased", acceptedProposalId: "proposal-1", acceptedManifestHash: "manifest-1" })).toBe(false);
    expect(isMemoryRecord({ ...leaseBase, id: "forged-lease-id", version: 1, fencingToken: 1, state: "leased", acceptedProposalId: null, acceptedManifestHash: null })).toBe(false);
    const proposalHash = proposalContentHash({ ownerHost: "pi", jobId: "job-1", ownerId: "worker-1", membership: ["episode-1"], content: { summary: "safe summary" }, policyHash: "coordination-hash", policyEpoch: 3, fencingToken: 3, privacyEpoch: 2, policyIntersectionId: "policy-1" });
    const proposalBase = { ...derivedEnvelope, recordType: "proposal", id: proposalIdFor("job-1", proposalHash, 3, 3), jobId: "job-1", ownerId: "worker-1", proposalHash, manifestHash: "42a341584d40e9d1e6228d5ca7bd003104fc3ab282f5aa53316bd9c6b1d89061", fencingToken: 3, membership: ["episode-1"], content: { summary: "safe summary" } };
    expect(isMemoryRecord(proposalBase)).toBe(true);
    expect(isMemoryRecord({ ...proposalBase, proposalHash: "short" })).toBe(false);
    expect(isMemoryRecord({ ...proposalBase, manifestHash: "manifest-1" })).toBe(false);
    expect(isMemoryRecord({ ...proposalBase, membership: ["episode-2"] })).toBe(false);
    expect(isMemoryRecord({ ...proposalBase, content: { summary: "x".repeat(20000) } })).toBe(false);
    expect(isMemoryRecord({ ...proposalBase, id: "proposal-1" })).toBe(false);
  });
  it("enforces derived ID formulas for coverage and tombstone records", () => {
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "coverage", id: "4a780177-f166-5aa9-a98c-f75cb0e55ba1", episodeId: "episode-1", extractorRevision: "extractor-1" })).toBe(true);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "coverage", id: "forged-coverage", episodeId: "episode-1", extractorRevision: "extractor-1" })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "coverage", id: "4a780177-f166-5aa9-a98c-f75cb0e55ba1", episodeId: "episode-2", extractorRevision: "extractor-1" })).toBe(false);
    expect(isMemoryRecord({ ...envelope, recordType: "tombstone", id: "d6b6113a-fb37-546c-9807-b3f8ee692694", scope: "occurrence", targetId: "00000000-0000-5000-8000-000000000001" })).toBe(true);
    expect(isMemoryRecord({ ...envelope, recordType: "tombstone", id: "tombstone-1", scope: "occurrence", targetId: "00000000-0000-5000-8000-000000000001" })).toBe(false);
    expect(isMemoryRecord({ ...envelope, recordType: "tombstone", id: "d6b6113a-fb37-546c-9807-b3f8ee692694", scope: "content", targetId: "00000000-0000-5000-8000-000000000001" })).toBe(false);
    expect(isMemoryRecord({ ...envelope, recordType: "tombstone", id: "9b85c5fd-a7af-5461-bf4e-dcdf04aad6fe", scope: "content", targetId: "content:7f04943b1072d5e8df33a37973e42e9d75ecef23db68ddc743c5b45f9d5a1101" })).toBe(true);
  });
  it("keeps processing policy identity invariant to the observed privacy epoch (no same-ID/different-hash collision)", () => {
    const persistedPolicy = { ...envelope, processingPolicyId: policy.id, recordType: "processing_policy", id: policy.id, policy, canonicalHash: policy.id, expiresAt: policy.expiresAt };
    const epoch0 = { ...persistedPolicy, privacyEpoch: 0, contentHash: "pending" };
    const epoch2 = { ...persistedPolicy, privacyEpoch: 2, contentHash: "pending" };
    expect(canonicalRecordHash(epoch0)).toBe(canonicalRecordHash(epoch2));
    expect(canonicalRecordHash(epoch0)).toBe(canonicalRecordHash({ ...epoch2, contentHash: canonicalRecordHash(epoch2) }));
    const hashed = { ...epoch2, contentHash: canonicalRecordHash(epoch2) };
    expect(isPersistedMemoryRecord(hashed)).toBe(true);
    expect(isPersistedMemoryRecord({ ...hashed, contentHash: canonicalRecordHash(epoch0) })).toBe(true);
    // A changed policy is a different point entirely.
    const changedPolicy = { ...policy, policyRevision: "r2", id: "pending" };
    const finalChangedPolicy = { ...changedPolicy, id: processingPolicyHash(changedPolicy) };
    const changedRecord = { ...epoch0, policy: finalChangedPolicy, id: finalChangedPolicy.id, canonicalHash: finalChangedPolicy.id, processingPolicyId: finalChangedPolicy.id, contentHash: "pending" };
    expect(canonicalRecordHash(epoch0)).not.toBe(canonicalRecordHash(changedRecord));
  });
  it("rejects revoked destination IDs that are unbounded, duplicated, or secret-shaped", () => {
    const base = { ...envelope, recordType: "collection_control", id: "control-1", version: 1, activeGeneration: null, activeBaseGeneration: null, privacyEpoch: 2, coordinationPolicyEpoch: 3, coordinationPolicyHash: "coordination-hash", state: "active", scanCursor: null, lastForgetBarrier: null };
    expect(isMemoryRecord({ ...base, revokedDestinationIds: ["qdrant:pi"] })).toBe(true);
    expect(isMemoryRecord({ ...base, revokedDestinationIds: ["qdrant:pi", "qdrant:pi"] })).toBe(false);
    expect(isMemoryRecord({ ...base, revokedDestinationIds: ["api-key=secret"] })).toBe(false);
    expect(isMemoryRecord({ ...base, revokedDestinationIds: ["bad id"] })).toBe(false);
    expect(isMemoryRecord({ ...base, revokedDestinationIds: Array.from({ length: 1025 }, (_, index) => `dest-${index}`) })).toBe(false);
    expect(isMemoryRecord({ ...base, revokedDestinationIds: ["x".repeat(257)] })).toBe(false);
    expect(isMemoryRecord(base)).toBe(false);
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
