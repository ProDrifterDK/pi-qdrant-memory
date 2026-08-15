import { describe, expect, it, vi } from "vitest";
import { laneFilter } from "../../src/retrieval/filters.js";
import { mergeCandidates } from "../../src/retrieval/merge.js";
import { createGuardedMemoryReadStore, MemoryRetriever, historicalIntervals, type MemoryCandidate, type MemoryReadStore } from "../../src/retrieval/search.js";
import { canonicalRecordHash, type ControlRecord, type CuratedMemoryRecord, type CuratedCurrentRecord, type EpisodeRecord, type MemoryRecord, type ProcessingPolicyRecord, type RaptorSummaryRecord } from "../../src/domain/records.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { EmbeddingsClient, bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory } from "../../src/clients/embeddings.js";
import { projectCurationItem } from "../../src/curation/projection.js";
import { manifestHash } from "../../src/domain/ids.js";
import { physicalPointIdFor } from "../../src/qdrant/client.js";
import { recordPayload } from "../../src/qdrant/write.js";

describe("guarded retrieval lane filters", () => {
  it("pins every episode lane to owner, project, active privacy, expiry, status, scan and time", () => {
    const filter = laneFilter({
      ownerHost: "prime", lane: "episodes", projectId: "project-1", global: false,
      now: Date.parse("2026-08-13T15:00:00.000Z"), maxClockSkewMs: 300_000,
      privacyEpoch: 4, coordinationPolicyEpoch: 9, after: "2026-08-01T00:00:00.000Z", before: "2026-08-31T23:59:59.000Z",
    });
    expect(filter.must).toEqual(expect.arrayContaining([
      { key: "owner_host", match: { value: "prime" } },
      { key: "record_type", match: { value: "episode" } },
      { key: "project_id", match: { value: "project-1" } },
      { key: "privacy_epoch", match: { value: 4 } },
      { key: "status", match: { value: "active" } },
      { key: "secret_scan", match: { value: "passed" } },
      { key: "event_at", range: { gte: "2026-08-01T00:00:00.000Z", lte: "2026-08-31T23:59:59.000Z" } },
    ]));
    expect(filter.should).toEqual([
      { is_null: { key: "expires_at" } },
      { key: "expires_at", range: { gt: "2026-08-13T15:05:00.000Z" } },
    ]);
    expect(Object.isFrozen(filter)).toBe(true);
  });
  it("pins derived project lanes by both scope and project and permits explicit global scope without a project", () => {
    const common = { ownerHost: "prime" as const, projectId: "project-1", now: Date.parse(NOW), maxClockSkewMs: 300_000, privacyEpoch: 4, coordinationPolicyEpoch: 9 };
    for (const lane of ["current", "historical", "curated"] as const) {
      const project = laneFilter({ ...common, lane, global: false });
      expect(project.must).toEqual(expect.arrayContaining([{ key: "scope", match: { value: "project" } }, { key: "project_id", match: { value: "project-1" } }]));
      const global = laneFilter({ ...common, lane, global: true });
      expect(global.must).toContainEqual({ key: "scope", match: { value: "global" } });
      expect(global.must.some((condition) => "key" in condition && condition.key === "project_id")).toBe(false);
    }
  });

});


describe("reciprocal rank fusion", () => {
  const hit = (id: string, lane: MemoryCandidate["lane"], evidenceIds: string[], text = id): MemoryCandidate => ({
    id, text, rawScore: 1, adjustedScore: 0, lane, projectId: "project-1", sourceType: "episode", sourceSystem: "prime", evidenceIds, recordType: "episode", processingPolicyId: "policy-1", privacyEpoch: 4,
  });
  it("fuses ranked lanes, deduplicates concrete evidence, and ties deterministically", () => {
    const result = mergeCandidates({
      lanes: [
        [hit("a", "episodes", ["e-a"]), hit("b", "episodes", ["e-b"])],
        [hit("b", "exact", ["e-b"]), hit("c", "exact", ["e-c"])],
        [hit("alias-a", "curated", ["e-a"], "same evidence")],
      ],
      limit: 10, projectBoost: 0,
    });
    expect(result.map((item) => item.id)).toEqual(["b", "a", "c"]);
    expect(result[0]?.adjustedScore).toBeCloseTo(1 / 61 + 1 / 62, 12);
    expect(result.filter((item) => item.evidenceIds.includes("e-a"))).toHaveLength(1);
  });
  it("collapses only consecutive historical repeats and derives deterministic validity intervals", () => {
    const base: MemoryCandidate = { id: "obs-a1", text: "A", rawScore: 0.8, adjustedScore: 0.8, lane: "historical", recordType: "curated_memory", scope: "project", projectId: "p1", sourceType: "curated_memory", sourceSystem: "prime", validFrom: "2026-01-01T00:00:00.000Z", policyEpoch: 9, evidenceIds: ["ep-a1"], contentId: "content-a", observationId: "obs-a1", stateKey: "state-1", processingPolicyId: "policy", privacyEpoch: 4 };
    const values = historicalIntervals([base, { ...base, id: "obs-a2", observationId: "obs-a2", evidenceIds: ["ep-a2"], validFrom: "2026-02-01T00:00:00.000Z", rawScore: 0.9 }, { ...base, id: "obs-b", observationId: "obs-b", evidenceIds: ["ep-b"], contentId: "content-b", text: "B", validFrom: "2026-03-01T00:00:00.000Z" }, { ...base, id: "obs-a3", observationId: "obs-a3", evidenceIds: ["ep-a3"], validFrom: "2026-04-01T00:00:00.000Z" }]);
    expect(values.map((value) => ({ id: value.id, from: value.validFrom, to: value.validTo }))).toEqual([
      { id: "obs-a1", from: "2026-01-01T00:00:00.000Z", to: "2026-03-01T00:00:00.000Z" },
      { id: "obs-b", from: "2026-03-01T00:00:00.000Z", to: "2026-04-01T00:00:00.000Z" },
      { id: "obs-a3", from: "2026-04-01T00:00:00.000Z", to: undefined },
    ]);
  });

});


const NOW = "2026-08-13T15:00:00.000Z";
const VECTOR = Array.from({ length: 1024 }, () => Math.fround(0.01));
const NORMALIZED_VECTOR = Array.from({ length: 1024 }, () => 0.03125);
function canonical<T extends MemoryRecord>(record: T): T { return { ...record, contentHash: canonicalRecordHash(record) }; }
function policy(): ProcessingPolicy {
  const base = { id: "pending", ownerHost: "prime" as const, destinationIds: { qdrant: "q-local", embedding: "embed:local", llm: "provider/model" }, originProvider: "provider", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "r1" };
  return { ...base, id: processingPolicyHash(base) };
}
function fixtures() {
  const activePolicy = policy();
  const control = canonical<ControlRecord>({ recordType: "collection_control", id: "27587d0b-724e-5de9-a9a6-ae8127a15f2a", ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: activePolicy.id, expiresAt: null, version: 7, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 9, coordinationPolicyHash: "coord-hash", state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" });
  const episode = canonical<EpisodeRecord>({ recordType: "episode", id: "11111111-1111-5111-8111-111111111111", ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: activePolicy.id, expiresAt: null, sourceEntryId: "entry-1", host: "prime", projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: NOW, modelId: "provider/model", embeddingDimension: 1024, originProvider: "provider", destinationId: "q-local", status: "active", redactionStatus: "unchanged", secretScan: "passed", text: "alpha exact memory", vector: VECTOR, contentHash: "pending" });
  const policyRecord = canonical<ProcessingPolicyRecord>({ recordType: "processing_policy", id: activePolicy.id, ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: activePolicy.id, expiresAt: null, policy: activePolicy, canonicalHash: activePolicy.id, contentHash: "pending" });
  const projection = projectCurationItem("prime", "coord-hash", 9, { category: "fact", scope: "project", projectId: "project-1", subject: "alpha", predicate: "is", evidence: [episode.id], value: "safe" }, new Map([[episode.id, episode]]));
  const curated = canonical<CuratedMemoryRecord>({ recordType: "curated_memory", id: projection.observationId, ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: activePolicy.id, expiresAt: null, coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 9, contentId: projection.contentId, observationId: projection.observationId, eventAt: NOW, effectiveAt: NOW, sourceEpisodeIds: [episode.id], primaryEvidenceEpisodeId: episode.id, effectiveOrder: projection.effectiveOrder, stateKey: projection.stateKey, category: "fact", scope: "project", projectId: "project-1", subject: "alpha", predicate: "is", value: "safe", text: projection.text, confidence: 0.9, vector: VECTOR, contentHash: "pending" });
  const current = canonical<CuratedCurrentRecord>({ recordType: "curated_current", id: projection.currentId, ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: activePolicy.id, expiresAt: null, coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 9, contentId: projection.contentId, observationId: projection.observationId, version: 1, stateKey: projection.stateKey, scope: "project", projectId: "project-1", resolution: "resolved", effectiveOrder: projection.effectiveOrder, sourceEpisodeIds: [episode.id], text: projection.text, vector: VECTOR, contentHash: "pending" });
  return { activePolicy, control, episode, policyRecord, curated, current };
}
function readerFixture(): { reader: MemoryReadStore; calls: { exact: ReturnType<typeof vi.fn>; tombstones: ReturnType<typeof vi.fn> } } {
  const value = fixtures();
  const exact = vi.fn(async () => [{ record: value.episode, score: 1 }]);
  const tombstones = vi.fn(async () => []);
  const reader: MemoryReadStore = {
    destination: { id: "q-local", residency: "local", dataUse: "memory" },
    readControl: vi.fn(async () => value.control),
    search: vi.fn(async () => []),
    exact,
    retrieve: vi.fn(async () => []),
    retrieveEvidence: vi.fn(async () => []),
    readPolicies: vi.fn(async () => [value.policyRecord]),
    readTombstones: tombstones,
    health: vi.fn(async () => undefined),
    collectionInfo: vi.fn(async () => ({ dimension: 1024, distance: "Dot" })),
  };
  return { reader, calls: { exact, tombstones } };
}

describe("MemoryRetriever guarded exact lane", () => {
  it("returns concrete project evidence only after stable control, policy and tombstone checks", async () => {
    const { reader, calls } = readerFixture();
    const retriever = new MemoryRetriever({ reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
    expect(result.hits).toEqual([expect.objectContaining({ id: "11111111-1111-5111-8111-111111111111", text: "alpha exact memory", lane: "exact", evidenceIds: ["11111111-1111-5111-8111-111111111111"] })]);
    expect(calls.exact).toHaveBeenCalledWith(expect.objectContaining({ query: "alpha", filter: expect.objectContaining({ must: expect.arrayContaining([{ key: "owner_host", match: { value: "prime" } }, { key: "project_id", match: { value: "project-1" } }]) }) }));
    expect(calls.tombstones).toHaveBeenCalledWith(expect.arrayContaining(["11111111-1111-5111-8111-111111111111"]));
    expect(reader.readControl).toHaveBeenCalledTimes(3);
  });



  it("adds same-host global scope only for an explicitly opted-in registered root", async () => {
    const root = readerFixture(); root.calls.exact.mockResolvedValue([]);
    const retriever = new MemoryRetriever({ reader: root.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project_and_global", childSearch: true }, now: () => Date.parse(NOW) });
    const common = { query: "alpha", host: "prime" as const, project: { id: "project-1", label: "repo", identityKind: "registered" as const }, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "all" as const };
    await retriever.search({ ...common, isChild: false });
    expect(root.calls.exact).toHaveBeenCalledTimes(2);
    expect(root.calls.exact.mock.calls[1]![0].filter.must).toContainEqual({ key: "scope", match: { value: "global" } });
    root.calls.exact.mockClear();
    await retriever.search({ ...common, isChild: true });
    expect(root.calls.exact).toHaveBeenCalledTimes(1);
    expect(root.calls.exact.mock.calls[0]![0].filter.must).toContainEqual({ key: "project_id", match: { value: "project-1" } });
  });



  it("returns registered global curated evidence only to an opted-in root", async () => {
    const values = fixtures(); const base = readerFixture();
    const foreign = canonical<EpisodeRecord>({ ...values.episode, id: "44444444-4444-5444-8444-444444444444", sourceEntryId: "entry-global", projectId: "project-2", contentHash: "pending" });
    const projection = projectCurationItem("prime", "coord-hash", 9, { category: "fact", scope: "global", subject: "shared-alpha", predicate: "is", evidence: [foreign.id], value: "safe" }, new Map([[foreign.id, foreign]]));
    const globalMemory = canonical<CuratedMemoryRecord>({ recordType: "curated_memory", id: projection.observationId, ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: values.activePolicy.id, expiresAt: null, coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 9, contentId: projection.contentId, observationId: projection.observationId, eventAt: NOW, effectiveAt: NOW, sourceEpisodeIds: [foreign.id], primaryEvidenceEpisodeId: foreign.id, effectiveOrder: projection.effectiveOrder, stateKey: projection.stateKey, category: "fact", scope: "global", subject: "shared-alpha", predicate: "is", value: "safe", text: projection.text, vector: VECTOR, contentHash: "pending" });
    base.calls.exact.mockImplementation(async ({ filter }) => filter.must.some((condition) => "key" in condition && condition.key === "scope" && "match" in condition && "value" in condition.match && condition.match.value === "global") ? [{ record: globalMemory, score: 1 }] : []);
    vi.mocked(base.reader.retrieve).mockResolvedValue([foreign]);
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project_and_global", childSearch: true }, now: () => Date.parse(NOW) });
    const request = { query: "shared-alpha", host: "prime" as const, project: { id: "project-1", label: "repo", identityKind: "registered" as const }, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "all" as const };
    await expect(retriever.search({ ...request, isChild: false })).resolves.toMatchObject({ hits: [expect.objectContaining({ id: globalMemory.id, scope: "global" })] });
    await expect(retriever.search({ ...request, isChild: true })).resolves.toMatchObject({ hits: [] });
  });

  it("fails closed for an unknown project identity and never raises global scope", async () => {
    const base = readerFixture();
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project_and_global", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "unknown", label: "unknown" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "all" });
    expect(result.hits).toEqual([]);
    expect(base.reader.readControl).not.toHaveBeenCalled();
  });



  it("rejects a source policy from a stale privacy epoch", async () => {
    const values = fixtures(); const base = readerFixture();
    const stalePolicy = canonical<ProcessingPolicyRecord>({ ...values.policyRecord, privacyEpoch: 3, contentHash: "pending" });
    vi.mocked(base.reader.readPolicies).mockResolvedValue([stalePolicy]);
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
    expect(result.hits).toEqual([]);
  });

  it("rejects expired concrete evidence after curated scoring", async () => {
    const values = fixtures(); const base = readerFixture();
    base.calls.exact.mockResolvedValue([{ record: values.current, score: 1 }]);
    const expired = canonical<EpisodeRecord>({ ...values.episode, expiresAt: "2026-08-13T14:00:00.000Z", contentHash: "pending" });
    vi.mocked(base.reader.retrieve).mockResolvedValue([expired]);
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, maxClockSkewMs: 300_000, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "curated" });
    expect(result.hits).toEqual([]);
  });

  it("requires Qdrant residency and data-use labels in every source policy", async () => {
    const values = fixtures(); const base = readerFixture();
    const reader = { ...base.reader, destination: { id: "q-local", residency: "remote", dataUse: "memory" } as const } satisfies MemoryReadStore;
    const retriever = new MemoryRetriever({ reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
    expect(result.hits).toEqual([]);
  });

  it("filters a source-policy Qdrant destination mismatch before returning memory", async () => {
    const values = fixtures(); const base = readerFixture();
    const mismatchedPolicy = { ...values.activePolicy, destinationIds: { ...values.activePolicy.destinationIds, qdrant: "qdrant:other" }, id: "pending" }; mismatchedPolicy.id = processingPolicyHash(mismatchedPolicy);
    const policyRecord = canonical<ProcessingPolicyRecord>({ recordType: "processing_policy", id: mismatchedPolicy.id, ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: mismatchedPolicy.id, expiresAt: null, policy: mismatchedPolicy, canonicalHash: mismatchedPolicy.id, contentHash: "pending" });
    const control = canonical<ControlRecord>({ ...values.control, processingPolicyId: mismatchedPolicy.id, contentHash: "pending" }); vi.mocked(base.reader.readControl).mockResolvedValue(control); vi.mocked(base.reader.readPolicies).mockResolvedValue([policyRecord]);
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "all" });
    expect(result.hits).toEqual([]); expect(base.calls.exact).toHaveBeenCalledTimes(1); expect(base.reader.search).not.toHaveBeenCalled();
  });


  it("blocks a collection-wide Qdrant destination revocation before sending the query", async () => {
    const values = fixtures(); const base = readerFixture(); const control = canonical<ControlRecord>({ ...values.control, revokedDestinationIds: [base.reader.destination.id], contentHash: "pending" }); vi.mocked(base.reader.readControl).mockResolvedValue(control);
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "all" });
    expect(result.hits).toEqual([]); expect(base.calls.exact).not.toHaveBeenCalled(); expect(base.reader.search).not.toHaveBeenCalled();
  });

  it("keeps episodes and curated modes type-exact even on the local exact lane", async () => {
    const values = fixtures(); const base = readerFixture(); const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const request = { query: "alpha", host: "prime" as const, project: { id: "project-1", label: "repo", identityKind: "registered" as const }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" } };
    await retriever.search({ ...request, mode: "episodes" });
    expect(base.calls.exact.mock.calls[0]![0].filter.must).toContainEqual({ key: "record_type", match: { value: "episode" } });
    base.calls.exact.mockClear(); base.calls.exact.mockResolvedValue([{ record: values.current, score: 1 }]); vi.mocked(base.reader.retrieve).mockResolvedValue([values.episode]);
    await retriever.search({ ...request, mode: "curated" });
    expect(base.calls.exact.mock.calls[0]![0].filter.must).toContainEqual({ key: "record_type", match: { any: ["curated_memory", "curated_current"] } });
  });

  it("skips revoked dense egress before embedding while preserving the safe exact lane", async () => {
    const values = fixtures(); const base = readerFixture();
    const revokedId = "embed:local";
    const revoked = canonical<ControlRecord>({ ...values.control, revokedDestinationIds: [revokedId], contentHash: "pending" });
    vi.mocked(base.reader.readControl).mockResolvedValue(revoked);
    const transport = vi.fn(async () => new Response(JSON.stringify({ data: [{ embedding: VECTOR }] }), { status: 200 }));
    vi.stubGlobal("fetch", transport);
    try {
      const client = new EmbeddingsClient({ baseUrl: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 2500 });
      const validated = bindEmbeddingDocumentClient({ endpoint: "http://127.0.0.1:8080/v1", client });
      const destination = { id: "embed:local", residency: "local", dataUse: "memory" };
      const embedding = bindEmbeddingDestination(createEmbeddingDestinationFactory({ endpoint: "http://127.0.0.1:8080/v1", destination, client: validated, egressMode: "allowlist", coordinationPolicyHash: revoked.coordinationPolicyHash, coordinationPolicyEpoch: revoked.coordinationPolicyEpoch }), destination);
      const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, embedding, embeddingDestination: destination, maxClockSkewMs: 300_000, now: () => Date.parse(NOW) });
      const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
      expect(result.hits).toHaveLength(1);
      expect(transport).not.toHaveBeenCalled();
      expect(base.reader.search).not.toHaveBeenCalled();
    } finally { vi.unstubAllGlobals(); }
  });

  it("does not embed when an exact candidate producer policy denies the embedding destination", async () => {
    const values = fixtures(); const base = readerFixture();
    const deniedBase = { ...values.activePolicy, id: "pending", destinationIds: { ...values.activePolicy.destinationIds, embedding: "embed:denied" } }; const deniedPolicy = { ...deniedBase, id: processingPolicyHash(deniedBase) };
    const deniedRecord = canonical<ProcessingPolicyRecord>({ ...values.policyRecord, id: deniedPolicy.id, processingPolicyId: deniedPolicy.id, policy: deniedPolicy, canonicalHash: deniedPolicy.id, contentHash: "pending" });
    const deniedEpisode = canonical<EpisodeRecord>({ ...values.episode, processingPolicyId: deniedPolicy.id, contentHash: "pending" });
    vi.mocked(base.calls.exact).mockResolvedValue([{ record: deniedEpisode, score: 1 }]);
    const policies = new Map([[values.policyRecord.id, values.policyRecord], [deniedRecord.id, deniedRecord]]); vi.mocked(base.reader.readPolicies).mockImplementation(async (ids) => ids.map((id) => policies.get(id)!).filter((value) => value !== undefined));
    const transport = vi.fn(async () => new Response(JSON.stringify({ data: [{ embedding: VECTOR }] }), { status: 200 })); vi.stubGlobal("fetch", transport);
    try {
      const client = new EmbeddingsClient({ baseUrl: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 2500 });
      const validated = bindEmbeddingDocumentClient({ endpoint: "http://127.0.0.1:8080/v1", client }); const destination = { id: "embed:local", residency: "local", dataUse: "memory" };
      const embedding = bindEmbeddingDestination(createEmbeddingDestinationFactory({ endpoint: "http://127.0.0.1:8080/v1", destination, client: validated, egressMode: "allowlist", coordinationPolicyHash: values.control.coordinationPolicyHash, coordinationPolicyEpoch: values.control.coordinationPolicyEpoch }), destination);
      const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, embedding, embeddingDestination: destination, maxClockSkewMs: 300_000, now: () => Date.parse(NOW) });
      const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
      expect(result.hits).toEqual([expect.objectContaining({ id: deniedEpisode.id, lane: "exact" })]); expect(transport).not.toHaveBeenCalled(); expect(base.reader.search).not.toHaveBeenCalled();
    } finally { vi.unstubAllGlobals(); }
  });

  it("uses one genuine bound embedding and a guarded dense episode lane", async () => {
    const values = fixtures(); const base = readerFixture();
    vi.mocked(base.calls.exact).mockResolvedValue([]);
    vi.mocked(base.reader.search).mockResolvedValue([{ record: values.episode, score: 0.92 }]);
    const transport = vi.fn(async (_url: string, init?: RequestInit) => { expect(JSON.parse(String(init?.body))).toEqual({ model: "bge-m3", input: "search_query: alpha" }); return new Response(JSON.stringify({ data: [{ embedding: VECTOR }] }), { status: 200 }); });
    vi.stubGlobal("fetch", transport);
    try {
      const client = new EmbeddingsClient({ baseUrl: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 2500 });
      const validated = bindEmbeddingDocumentClient({ endpoint: "http://127.0.0.1:8080/v1", client }); const destination = { id: "embed:local", residency: "local", dataUse: "memory" };
      const embedding = bindEmbeddingDestination(createEmbeddingDestinationFactory({ endpoint: "http://127.0.0.1:8080/v1", destination, client: validated, egressMode: "allowlist", coordinationPolicyHash: values.control.coordinationPolicyHash, coordinationPolicyEpoch: values.control.coordinationPolicyEpoch }), destination);
      const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, embedding, embeddingDestination: destination, queryPrefix: "search_query: ", maxClockSkewMs: 300_000, now: () => Date.parse(NOW) });
      const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
      expect(result.hits).toEqual([expect.objectContaining({ lane: "episodes", id: values.episode.id })]);
      expect(transport).toHaveBeenCalledTimes(1);
      expect(base.reader.search).toHaveBeenCalledWith(expect.objectContaining({ lane: "episodes", vector: NORMALIZED_VECTOR, filter: expect.objectContaining({ must: expect.arrayContaining([{ key: "project_id", match: { value: "project-1" } }]) }) }));
    } finally { vi.unstubAllGlobals(); }
  });



  it("descends an active RAPTOR summary to concrete authorized evidence", async () => {
    const values = fixtures(); const base = readerFixture(); const generationId = "generation-active";
    const control = canonical<ControlRecord>({ ...values.control, activeGeneration: generationId, contentHash: "pending" }); vi.mocked(base.reader.readControl).mockResolvedValue(control); vi.mocked(base.calls.exact).mockResolvedValue([]);
    const summary = canonical<RaptorSummaryRecord>({ recordType: "raptor_summary", id: "summary-active", ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: values.activePolicy.id, expiresAt: null, coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 9, generationId, clusterId: "cluster-a", membershipHash: manifestHash([values.episode.id]), level: 1, memberIds: [values.episode.id], manifestHash: "merkle-a", summary: "alpha grouped memory", vector: VECTOR, modelId: "summary-model", embeddingDimension: 1024, promptRevision: "raptor-summary-v2", algorithm: "raptor-umap140-diag-gmm-v1", seed: 7, jobId: "job-a", fencingToken: 1, temporalFrom: NOW, temporalTo: NOW, coveredProjects: ["project-1"], algorithmParameters: { kind: "summary" }, contentHash: "pending" });
    vi.mocked(base.reader.search).mockResolvedValue([{ record: summary, score: 0.98 }]); vi.mocked(base.reader.retrieveEvidence).mockResolvedValue([values.episode]);
    const transport = vi.fn(async () => new Response(JSON.stringify({ data: [{ embedding: VECTOR }] }), { status: 200 })); vi.stubGlobal("fetch", transport);
    try {
      const client = new EmbeddingsClient({ baseUrl: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 2500 }); const destination = { id: "embed:local", residency: "local", dataUse: "memory" };
      const embedding = bindEmbeddingDestination(createEmbeddingDestinationFactory({ endpoint: "http://127.0.0.1:8080/v1", destination, client: bindEmbeddingDocumentClient({ endpoint: "http://127.0.0.1:8080/v1", client }), egressMode: "allowlist", coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch }), destination);
      const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, embedding, embeddingDestination: destination, maxClockSkewMs: 300_000, now: () => Date.parse(NOW) });
      const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "raptor" });
      expect(result.hits).toEqual([expect.objectContaining({ id: values.episode.id, lane: "raptor", recordType: "episode", evidenceIds: [values.episode.id] })]);
      expect(result.hits.some((hit) => hit.id === summary.id)).toBe(false);
      expect(base.reader.retrieveEvidence).toHaveBeenCalledWith([values.episode.id]);
    } finally { vi.unstubAllGlobals(); }
  });

  it("chunks a multi-hit curated evidence closure without weakening validation", async () => {
    const values = fixtures(); const base = readerFixture();
    const episodes = Array.from({ length: 1025 }, (_, index) => canonical<EpisodeRecord>({
      ...values.episode,
      id: `episode-${String(index).padStart(4, "0")}`,
      sourceEntryId: `entry-${String(index).padStart(4, "0")}`,
      turnId: `turn-${String(index).padStart(4, "0")}`,
      contentHash: "pending",
    }));
    const byId = new Map(episodes.map((episode) => [episode.id, episode]));
    const makeCurrent = (subject: string, evidence: EpisodeRecord[]): CuratedCurrentRecord => {
      const projected = projectCurationItem("prime", "coord-hash", 9, { category: "fact", scope: "project", projectId: "project-1", subject, predicate: "is", evidence: evidence.map((episode) => episode.id), value: "safe" }, byId);
      return canonical<CuratedCurrentRecord>({ recordType: "curated_current", id: projected.currentId, ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: values.activePolicy.id, expiresAt: null, coordinationPolicyHash: "coord-hash", coordinationPolicyEpoch: 9, contentId: projected.contentId, observationId: projected.observationId, version: 1, stateKey: projected.stateKey, scope: "project", projectId: "project-1", resolution: "resolved", effectiveOrder: projected.effectiveOrder, sourceEpisodeIds: evidence.map((episode) => episode.id), text: projected.text, vector: VECTOR, contentHash: "pending" });
    };
    const first = makeCurrent("alpha-one", episodes.slice(0, 1024));
    const second = makeCurrent("alpha-two", episodes.slice(1024));
    base.calls.exact.mockResolvedValue([{ record: first, score: 1 }, { record: second, score: 0.9 }]);
    vi.mocked(base.reader.retrieve).mockImplementation(async (refs) => {
      expect(refs.length).toBeLessThanOrEqual(1024);
      return refs.map((ref) => byId.get(ref.id)!).filter(Boolean);
    });
    const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "curated" });
    expect(result.hits.map((hit) => hit.id).sort()).toEqual([first.id, second.id].sort());
    expect(base.reader.retrieve).toHaveBeenCalledTimes(2);
  });

  it("expands curated evidence and rejects a cross-project source after scoring", async () => {
    const values = fixtures(); const base = readerFixture();
    vi.mocked(base.calls.exact).mockResolvedValue([]);
    vi.mocked(base.reader.search).mockResolvedValue([{ record: values.current, score: 0.95 }]);
    const foreign = canonical<EpisodeRecord>({ ...values.episode, id: "33333333-3333-5333-8333-333333333333", sourceEntryId: "entry-foreign", projectId: "project-2", contentHash: "pending" });
    const transport = vi.fn(async () => new Response(JSON.stringify({ data: [{ embedding: VECTOR }] }), { status: 200 })); vi.stubGlobal("fetch", transport);
    try {
      const client = new EmbeddingsClient({ baseUrl: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 2500 }); const destination = { id: "embed:local", residency: "local", dataUse: "memory" };
      const embedding = bindEmbeddingDestination(createEmbeddingDestinationFactory({ endpoint: "http://127.0.0.1:8080/v1", destination, client: bindEmbeddingDocumentClient({ endpoint: "http://127.0.0.1:8080/v1", client }), egressMode: "allowlist", coordinationPolicyHash: values.control.coordinationPolicyHash, coordinationPolicyEpoch: values.control.coordinationPolicyEpoch }), destination);
      vi.mocked(base.reader.retrieve).mockResolvedValue([foreign]);
      const retriever = new MemoryRetriever({ reader: base.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, embedding, embeddingDestination: destination, maxClockSkewMs: 300_000, now: () => Date.parse(NOW) });
      const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "current" });
      expect(result.hits).toEqual([]);
      expect(base.reader.retrieve).toHaveBeenCalledWith(expect.arrayContaining([expect.objectContaining({ recordType: "episode", id: values.episode.id })]));
    } finally { vi.unstubAllGlobals(); }
  });

  it("rejects unauthorized model destinations before returning any scored hit", async () => {
    const { reader, calls } = readerFixture();
    const retriever = new MemoryRetriever({ reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const result = await retriever.search({ query: "alpha", host: "prime", project: { id: "project-1", label: "repo", identityKind: "registered" }, isChild: false, modelDestination: { id: "other/model", residency: "local", dataUse: "memory" }, mode: "episodes" });
    expect(result.hits).toEqual([]);
    expect(calls.tombstones).not.toHaveBeenCalled();
  });

  it("returns zero for any final tombstone or control mutation", async () => {
    const first = readerFixture(); const values = fixtures();
    first.reader.readTombstones = vi.fn(async (targets) => [canonical({ recordType: "tombstone", id: "22222222-2222-5222-8222-222222222222", ownerHost: "prime", schemaRevision: 1, createdAt: NOW, privacyEpoch: 4, processingPolicyId: values.activePolicy.id, expiresAt: null, scope: "occurrence", targetId: targets[0]!, contentHash: "pending" })]);
    const retriever = new MemoryRetriever({ reader: first.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    const request = { query: "alpha", host: "prime" as const, project: { id: "project-1", label: "repo", identityKind: "registered" as const }, isChild: false, modelDestination: { id: "provider/model", residency: "local", dataUse: "memory" }, mode: "episodes" as const };
    await expect(retriever.search(request)).resolves.toMatchObject({ hits: [] });

    const second = readerFixture(); const changed = canonical<ControlRecord>({ ...values.control, version: values.control.version + 1, contentHash: "pending" });
    vi.mocked(second.reader.readControl).mockResolvedValueOnce(values.control).mockResolvedValueOnce(changed);
    const retriever2 = new MemoryRetriever({ reader: second.reader, config: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true }, now: () => Date.parse(NOW) });
    await expect(retriever2.search(request)).resolves.toMatchObject({ hits: [] });
  });
});


describe("guarded Qdrant memory reader", () => {
  it("sends only the fixed owner/project/status/scan/expiry filter and never exposes mutation methods", async () => {
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      expect(body.filter.min_should).toEqual(expect.objectContaining({ min_count: 1, conditions: expect.arrayContaining([{ key: "text", match: { text: "alpha" } }, { key: "tool_name", match: { value: "alpha" } }, { key: "error_fingerprint", match: { value: "alpha" } }]) }));
      expect(body.filter.must).toEqual(expect.arrayContaining([
        { key: "owner_host", match: { value: "prime" } },
        { key: "project_id", match: { value: "project-1" } },
        { key: "status", match: { value: "active" } },
        { key: "secret_scan", match: { value: "passed" } },
      ]));
      expect(body.with_vector).toBe(true);
      expect(body.limit).toBe(256);
      return new Response(JSON.stringify({ result: { points: [], next_page_offset: null } }), { status: 200 });
    });
    const store = createGuardedMemoryReadStore({ baseUrl: "http://qdrant.test", collection: "prime_memory", ownerHost: "prime", timeoutMs: 2500, maxClockSkewMs: 300_000, destination: { id: "qdrant:local", residency: "local", dataUse: "memory" }, egressMode: "allowlist", fetchImpl });
    const filter = laneFilter({ ownerHost: "prime", lane: "exact", projectId: "project-1", global: false, now: Date.parse(NOW), maxClockSkewMs: 300_000, privacyEpoch: 4, coordinationPolicyEpoch: 9 });
    await expect(store.exact({ query: "alpha", filter, limit: 20 })).resolves.toEqual([]);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect((store as unknown as Record<string, unknown>).upsert).toBeUndefined();
    expect((store as unknown as Record<string, unknown>).request).toBeUndefined();
  });
  it("returns at most the exact-lane limit even when a page contains more matches", async () => {
    const values = fixtures();
    const episodes = Array.from({ length: 25 }, (_, index) => canonical<EpisodeRecord>({ ...values.episode, id: `bounded-${String(index).padStart(3, "0")}`, sourceEntryId: `entry-bounded-${index}`, contentHash: "pending" }));
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({ result: { points: episodes.map((episode) => ({ id: physicalPointIdFor("episode", episode.id), payload: recordPayload(episode), vector: { semantic: VECTOR } })), next_page_offset: null } }), { status: 200 }));
    const store = createGuardedMemoryReadStore({ baseUrl: "http://qdrant.test", collection: "prime_memory", ownerHost: "prime", timeoutMs: 2500, maxClockSkewMs: 300_000, destination: { id: "qdrant:local", residency: "local", dataUse: "memory" }, egressMode: "allowlist", fetchImpl });
    const filter = laneFilter({ ownerHost: "prime", lane: "exact", projectId: "project-1", global: false, now: Date.parse(NOW), maxClockSkewMs: 300_000, privacyEpoch: 4, coordinationPolicyEpoch: 9 });
    const result = await store.exact({ query: "alpha", filter, limit: 20 });
    expect(result).toHaveLength(20);
  });

  it("rejects a structurally forged filter before any Qdrant request", async () => {
    const fetchImpl = vi.fn(); const store = createGuardedMemoryReadStore({ baseUrl: "http://qdrant.test", collection: "prime_memory", ownerHost: "prime", timeoutMs: 2500, maxClockSkewMs: 300_000, destination: { id: "qdrant:local", residency: "local", dataUse: "memory" }, egressMode: "allowlist", fetchImpl });
    const forged = { must: [{ key: "owner_host", match: { value: "prime" } }, { key: "record_type", match: { value: "episode" } }, { key: "status", match: { value: "active" } }, { key: "secret_scan", match: { value: "passed" } }, { key: "privacy_epoch", match: { value: 4 } }], must_not: [], should: [{ is_null: { key: "expires_at" } }, { key: "expires_at", range: { gt: "1970-01-01T00:00:00.000Z" } }] };
    await expect(store.exact({ query: "alpha", filter: forged as never, limit: 20 })).rejects.toThrow("Guarded Qdrant filter");
    expect(fetchImpl).not.toHaveBeenCalled();
  });

});
