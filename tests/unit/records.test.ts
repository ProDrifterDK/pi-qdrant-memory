import { describe, expect, it } from "vitest";
import { isMemoryRecord, isPersistedMemoryRecord, parseMemoryRecord, parsePersistedMemoryRecord, assertCanonicalRecordHash, canonicalRecordHash, type MemoryRecord } from "../../src/domain/records.js";
import { processingPolicyHash } from "../../src/domain/policy.js";
import { curatedCurrentId, evidenceLinkId, manifestHash, proposalContentHash, proposalIdFor } from "../../src/domain/ids.js";
import { canonicalStringify } from "../../src/domain/canonical.js";
import { recordFromPayload, recordPayload } from "../../src/qdrant/write.js";

const envelope = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 2, processingPolicyId: "policy-1", expiresAt: null, contentHash: "hash" };
const derivedEnvelope = { ...envelope, coordinationPolicyHash: "coordination-hash", coordinationPolicyEpoch: 3 };
const policyBase = { id: "pending", ownerHost: "pi" as const, destinationIds: { qdrant: "qdrant-local", embedding: "embedding-local", llm: "llm-local" }, originProvider: "provider-a", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "revision-1" };
const policy = { ...policyBase, id: processingPolicyHash(policyBase) };
const episode = (): MemoryRecord => ({ ...envelope, recordType: "episode", id: "episode-1", contentHash: "hash", sourceEntryId: "source-1", host: "pi", projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2026-08-10T00:00:00.000Z", modelId: "provider-model", embeddingDimension: 1024, originProvider: "provider-a", destinationId: "local-node-a", status: "active", redactionStatus: "unchanged", secretScan: "passed", text: "safe" });

describe("v2 record schemas", () => {
  it("accepts every discriminated record type with provenance closure", () => {
    const records: MemoryRecord[] = [
      episode(),
      { ...derivedEnvelope, recordType: "curated_memory", id: "observation-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] },
      { ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "resolved", text: "safe", vector: Array.from({ length: 1024 }, () => 0), effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1"] },
      { ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", effectiveOrder: "session:1" },
      { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-1", generationId: "generation-1", clusterId: "cluster-1", membershipHash: manifestHash(["episode-1"]), level: 0, memberIds: ["episode-1"], summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } },
      { ...derivedEnvelope, recordType: "raptor_summary", id: "summary-manifest-1", generationId: "generation-1", clusterId: "cluster-2", membershipHash: "manifest-hash", level: 1, manifestHash: "manifest-hash", summary: "summary", modelId: "provider-model", embeddingDimension: 1024, promptRevision: "prompt-1", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: "2026-08-01T00:00:00.000Z", temporalTo: "2026-08-10T00:00:00.000Z", coveredProjects: ["project-1"], algorithmParameters: { clusters: 1 } },
      { ...envelope, recordType: "collection_control", id: "control-1", version: 1, activeGeneration: null, activeBaseGeneration: null, privacyEpoch: 2, coordinationPolicyEpoch: 3, coordinationPolicyHash: "coordination-hash", state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [] },
      { ...envelope, processingPolicyId: policy.id, recordType: "processing_policy", id: policy.id, policy, canonicalHash: policy.id, expiresAt: policy.expiresAt },
      { ...derivedEnvelope, recordType: "job", id: "50e7c5c9-bbfb-501e-9d7c-9179abd4e901", policyId: "policy-1", policyHash: "coordination-hash", policyEpoch: 3, membership: ["episode-1"], extractorRevision: "extractor-1" },
      { ...derivedEnvelope, recordType: "coverage", id: "4a780177-f166-5aa9-a98c-f75cb0e55ba1", episodeId: "episode-1", extractorRevision: "extractor-1" },
      { ...derivedEnvelope, recordType: "evidence_link", id: evidenceLinkId("curated-1", "episode-1", "extractor-1"), sourceId: "curated-1", targetId: "episode-1", jobId: "job-1", extractorRevision: "extractor-1" },
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
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1"] })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", contentId: undefined, effectiveOrder: "session:1" })).toBe(false);
    expect(isMemoryRecord({ ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "resolved", conflictManifestHash: "manifest-1", effectiveOrder: "session:1" })).toBe(false);
  });
  it("binds episode and immutable curated hashes to exact vectors while query vectors remain excluded", () => {
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
    // Immutable curated observations commit their named vector too.
    const curated: MemoryRecord = { ...derivedEnvelope, recordType: "curated_memory", id: "observation-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] };
    const curatedVector = Array.from({ length: 1024 }, () => 0.25);
    expect(canonicalRecordHash(curated)).not.toBe(canonicalRecordHash({ ...curated, vector: curatedVector }));
    expect(canonicalRecordHash({ ...curated, vector: curatedVector })).not.toBe(canonicalRecordHash({ ...curated, vector: curatedVector.map((value, index) => index === 3 ? value + 1 : value) }));
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
    const resolved = { ...derivedEnvelope, recordType: "curated_memory", id: "observation-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1"] };
    expect(isMemoryRecord({ ...resolved, effectiveOrder: { sequence: 1 } })).toBe(false);
    expect(isMemoryRecord({ ...resolved, effectiveOrder: ["2026-08-10T00:00:00.000Z", "api-token", "content-1"] })).toBe(false);
    expect(isPersistedMemoryRecord({ ...resolved, effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1"] })).toBe(false);
    expect(() => canonicalRecordHash({ ...resolved, effectiveOrder: { sequence: 1 } } as MemoryRecord)).toThrow(/effective/i);
    expect(isMemoryRecord({ ...resolved, effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1", "extra"] })).toBe(false);
  });
  it("throws a typed error from the explicit guard", () => expect(() => parseMemoryRecord({ ...episode(), ownerHost: "prime" })).toThrow(/host/i));
});

describe("curated current resolution, sorted provenance, and exact-once payload parsing", () => {
  const resolvedCurrent = (): MemoryRecord => ({ ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), contentId: "content-1", observationId: "observation-1", version: 1, stateKey: "state-1", resolution: "resolved", text: "safe", vector: Array.from({ length: 1024 }, (_, index) => index / 1024), sourceEpisodeIds: ["episode-1", "episode-2"], effectiveOrder: ["2026-08-10T00:00:00.000Z", "episode-1", "content-1"] });
  const conflictCurrent = (): MemoryRecord => ({ ...derivedEnvelope, recordType: "curated_current", id: curatedCurrentId("pi", "state-1", 3), version: 1, stateKey: "state-1", resolution: "conflict", conflictManifestHash: "manifest-1", effectiveOrder: "session:1" });
  const canonical = (record: MemoryRecord): MemoryRecord => ({ ...record, contentHash: canonicalRecordHash(record) });
  const jobRecord = (): MemoryRecord => canonical({ ...derivedEnvelope, recordType: "job", id: "50e7c5c9-bbfb-501e-9d7c-9179abd4e901", policyId: "policy-1", policyHash: "coordination-hash", policyEpoch: 3, membership: ["episode-1"], extractorRevision: "extractor-1" });

  it("requires resolved current text plus a finite 1024-dimensional vector and forbids conflict text/vector", () => {
    expect(isMemoryRecord(resolvedCurrent())).toBe(true);
    expect(isMemoryRecord(conflictCurrent())).toBe(true);
    // A resolved current MUST carry both text and the full named vector.
    expect(isMemoryRecord({ ...resolvedCurrent(), text: undefined })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), text: "" })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), vector: undefined })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), vector: [] })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), vector: [0.1, 0.2] })).toBe(false);
    // The vector must be exactly 1024 FINITE components.
    const vector = Array.from({ length: 1024 }, (_, index) => index / 1024);
    expect(isMemoryRecord({ ...resolvedCurrent(), vector: vector.map((value, index) => (index === 7 ? Number.NaN : value)) })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), vector: vector.map((value, index) => (index === 9 ? Number.POSITIVE_INFINITY : value)) })).toBe(false);
    // A conflict current must NOT select content, observation, text or vector.
    expect(isMemoryRecord({ ...conflictCurrent(), text: "safe" })).toBe(false);
    expect(isMemoryRecord({ ...conflictCurrent(), vector: Array.from({ length: 1024 }, () => 0.25) })).toBe(false);
    expect(isMemoryRecord({ ...conflictCurrent(), text: "safe", vector: Array.from({ length: 1024 }, () => 0.25) })).toBe(false);
    expect(isMemoryRecord({ ...conflictCurrent(), contentId: "content-1" })).toBe(false);
    expect(isMemoryRecord({ ...conflictCurrent(), observationId: "observation-1" })).toBe(false);
  });

  it("requires sourceEpisodeIds and provenance to be sorted and unique", () => {
    const curated = (): MemoryRecord => ({ ...derivedEnvelope, recordType: "curated_memory", id: "observation-1", contentId: "content-1", observationId: "observation-1", eventAt: envelope.createdAt, effectiveAt: envelope.createdAt, effectiveOrder: "session:0", sourceEpisodeIds: ["episode-1", "episode-2"], provenance: ["evidence-1", "evidence-2"] });
    expect(isMemoryRecord(curated())).toBe(true);
    expect(isMemoryRecord({ ...curated(), sourceEpisodeIds: ["episode-2", "episode-1"] })).toBe(false);
    expect(isMemoryRecord({ ...curated(), sourceEpisodeIds: ["episode-1", "episode-1"] })).toBe(false);
    expect(isMemoryRecord({ ...curated(), sourceEpisodeIds: [] })).toBe(false);
    expect(isMemoryRecord({ ...curated(), provenance: ["evidence-2", "evidence-1"] })).toBe(false);
    expect(isMemoryRecord({ ...curated(), provenance: ["evidence-1", "evidence-1"] })).toBe(false);
    expect(isMemoryRecord({ ...curated(), provenance: [] })).toBe(false);
    // The same invariant applies to the mutable current point.
    expect(isMemoryRecord({ ...resolvedCurrent(), sourceEpisodeIds: ["episode-2", "episode-1"] })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), sourceEpisodeIds: ["episode-1", "episode-1"] })).toBe(false);
    expect(isMemoryRecord({ ...resolvedCurrent(), sourceEpisodeIds: [] })).toBe(false);
  });

  it("recordPayload snapshots inputs exactly once: accessors are never invoked and nested getters are never re-read", () => {
    // Own accessor properties fail closed WITHOUT ever invoking the getter.
    let textGets = 0;
    const accessorRecord = episode();
    Object.defineProperty(accessorRecord, "text", { enumerable: true, configurable: true, get() { textGets += 1; return "safe"; } });
    expect(() => recordPayload(accessorRecord)).toThrow(/canonical|JSON/i);
    expect(textGets).toBe(0);
    // Unknown-field accessors are never invoked either.
    let unknownGets = 0;
    const unknownRecord = episode();
    Object.defineProperty(unknownRecord, "evil", { enumerable: true, configurable: true, get() { unknownGets += 1; return 1; } });
    expect(() => recordPayload(unknownRecord)).toThrow(/canonical|JSON/i);
    expect(unknownGets).toBe(0);
    // A nested Proxy-wrapped vector observes ZERO get-trap reads: the input is
    // snapshotted once via descriptors and the payload stays vector-free.
    let vectorGets = 0;
    const vector = new Proxy(Array.from({ length: 1024 }, (_, index) => index / 1024), { get(target, prop) { if (typeof prop === "string" && /^\d+$/u.test(prop)) vectorGets += 1; return Reflect.get(target, prop); } });
    const payload = recordPayload({ ...episode(), vector });
    expect(vectorGets).toBe(0);
    expect(payload).not.toHaveProperty("vector");
    expect(recordPayload({ ...episode(), vector: [...vector] })).toEqual(payload);
  });

  it("recordFromPayload reads the wire input exactly once: no unknown getters, no input re-reads, no aliasing", () => {
    // Wire payloads carrying own accessor properties fail closed WITHOUT invocation.
    let wireGets = 0;
    const accessorWire = recordPayload(jobRecord());
    Object.defineProperty(accessorWire, "membership", { enumerable: true, configurable: true, get() { wireGets += 1; return ["episode-1"]; } });
    expect(() => recordFromPayload(accessorWire, "pi")).toThrow(/payload is invalid/i);
    expect(wireGets).toBe(0);
    // Unknown wire accessors are never invoked either.
    let unknownWireGets = 0;
    const unknownWire = recordPayload(jobRecord());
    Object.defineProperty(unknownWire, "evil_field", { enumerable: true, configurable: true, get() { unknownWireGets += 1; return "x"; } });
    expect(() => recordFromPayload(unknownWire, "pi")).toThrow(/payload is invalid/i);
    expect(unknownWireGets).toBe(0);
    // Proxy-wrapped payloads and nested arrays fail closed without invoking
    // their traps. Authoritative wire input must be a dense plain data graph.
    let payloadGets = 0;
    const proxied = new Proxy(recordPayload(jobRecord()), { get(target, prop) { payloadGets += 1; return Reflect.get(target, prop); } });
    expect(() => recordFromPayload(proxied, "pi")).toThrow(/payload is invalid/i);
    expect(payloadGets).toBe(0);
    let nestedGets = 0;
    const nestedWire = recordPayload(jobRecord());
    nestedWire.membership = new Proxy(["episode-1"], { get(target, prop) { if (typeof prop === "string" && /^\d+$/u.test(prop)) nestedGets += 1; return Reflect.get(target, prop); } });
    expect(() => recordFromPayload(nestedWire, "pi")).toThrow(/payload is invalid/i);
    expect(nestedGets).toBe(0);
    // Mutating the input AFTER parsing cannot leak into the returned record and
    // the exact original wire is never re-read.
    const wire = recordPayload(jobRecord());
    const originalWire = canonicalStringify(wire);
    const fromWire = recordFromPayload(wire, "pi");
    wire.membership = [...(wire.membership as string[]), "episode-9"];
    expect(fromWire).toMatchObject({ membership: ["episode-1"] });
    expect(canonicalStringify(recordPayload(fromWire))).toBe(originalWire);
  });

  it("recordFromPayload snapshots only dense plain semantic vectors", () => {
    const record = canonical(resolvedCurrent());
    const wire = recordPayload(record);
    let indexGets = 0;
    const proxyVector = new Proxy(Array.from({ length: 1024 }, (_, index) => index / 1024), { get(target, prop) { if (typeof prop === "string" && /^\d+$/u.test(prop)) indexGets += 1; return Reflect.get(target, prop); } });
    expect(() => recordFromPayload(wire, "pi", proxyVector)).toThrow(/semantic vector is invalid/i);
    expect(indexGets).toBe(0);
    let accessorGets = 0;
    const accessorVector = Array.from({ length: 1024 }, (_, index) => index / 1024);
    Object.defineProperty(accessorVector, "17", { enumerable: true, configurable: true, get() { accessorGets += 1; return 17 / 1024; } });
    expect(() => recordFromPayload(wire, "pi", accessorVector)).toThrow(/semantic vector is invalid/i);
    expect(accessorGets).toBe(0);
    const sparseVector = Array.from({ length: 1024 }, (_, index) => index / 1024);
    delete sparseVector[17];
    expect(() => recordFromPayload(wire, "pi", sparseVector)).toThrow(/semantic vector is invalid/i);
    const denseVector = Array.from({ length: 1024 }, (_, index) => index / 1024);
    expect(recordFromPayload(wire, "pi", denseVector)).toEqual(record);
    // A conflict current rejects the named vector transport.
    expect(() => recordFromPayload(recordPayload(canonical(conflictCurrent())), "pi", denseVector)).toThrow(/vector/i);
    // A resolved current REQUIRES the named vector transport on readback.
    expect(() => recordFromPayload(wire, "pi")).toThrow(/vector|invalid/i);
  });
});
