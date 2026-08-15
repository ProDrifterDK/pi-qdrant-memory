import { beforeAll, describe, expect, it } from "vitest";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { beginForgetBarrier } from "../../src/coordination/control.js";
import { createJob } from "../../src/coordination/jobs.js";
import { createTombstone } from "../../src/coordination/tombstones.js";
import { runCurationFromLifecycle, runRaptorFromLifecycle, type RootCurationLifecycleInput, type RootRaptorLifecycleInput } from "../../src/coordination/root.js";
import { intersectPolicies, type ProcessingPolicy } from "../../src/domain/policy.js";
import { coverageId } from "../../src/domain/ids.js";
import { laneFilter } from "../../src/retrieval/filters.js";
import { createGuardedMemoryReadStore, MemoryRetriever } from "../../src/retrieval/search.js";
import { createQdrantCoordinationStore, jobIdFor as jobIdentity, recordPayload } from "../../src/qdrant/write.js";
import { physicalPointId } from "../../src/qdrant/schema.js";
import { canonicalizeEmbeddingVector } from "../../src/clients/embeddings.js";
import { deterministicUuid, isolatedConfig, isolatedOptions, isolatedQdrantUrl, isolatedRunId, initializeIsolated, startEmbeddingStub, task14Episode, task14Policy, task14PolicyRecord, task14Runtime, waitForIsolatedQdrant, TASK14_EMBEDDING_DESTINATION, TASK14_LLM_DESTINATION, TASK14_QDRANT_DESTINATION, type Task14Runtime } from "./qdrant-fixtures.js";
import { bindIngestRuntime } from "../../src/coordination/ingest.js";
import { canonicalRecordHash, type EpisodeRecord } from "../../src/domain/records.js";
import { createIngestProcessor, createOutboxDelivery } from "../../src/outbox/delivery.js";
import { createOutbox } from "../../src/outbox/store.js";

const concurrencyEnabled = process.env.PI_QDRANT_MEMORY_TEST_CONCURRENCY === "true";

describe("isolated Qdrant coordination concurrency matrix", () => {
  it("requires the explicit loopback harness opt-in", () => {
    expect(() => isolatedQdrantUrl({})).toThrow(/isolated run ID and URL required/);
  });
});

const real = concurrencyEnabled ? describe : describe.skip;
let matrixUrl: string;
let matrixStore: Awaited<ReturnType<typeof initializeIsolated>>;
let matrixReady: Promise<void> | undefined;
async function ensureMatrix(): Promise<void> {
  matrixReady ??= (async () => {
    matrixUrl = isolatedQdrantUrl(process.env);
    await waitForIsolatedQdrant(matrixUrl);
    matrixStore = await initializeIsolated(matrixUrl, "pi");
    await initializeIsolated(matrixUrl, "prime");
  })();
  await matrixReady;
}
real("real insert-only/OCC races", () => {
  let store: Awaited<ReturnType<typeof initializeIsolated>>;
  let url: string;

  beforeAll(async () => { await ensureMatrix(); url = matrixUrl; store = matrixStore; }, 120_000);

  it("allows one canonical winner for equal jobs and rejects a hash collision", async () => {
    const control = await store.readControl();
    const membership = [deterministicUuid(`${isolatedRunId(process.env)}:race:a`), deterministicUuid(`${isolatedRunId(process.env)}:race:b`)].sort();
    const input = { ownerHost: "pi" as const, membership, policyIntersectionId: control.processingPolicyId, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "task14-race-v1", privacyEpoch: control.privacyEpoch, createdAt: "2026-08-15T00:00:00.000Z", expiresAt: null };
    const winners = await Promise.all(Array.from({ length: 20 }, () => createJob(store, input)));
    expect(new Set(winners.map(value => value.id))).toHaveLength(1);
    await expect(createJob(store, { ...input, createdAt: "2026-08-15T00:00:00.001Z" })).rejects.toThrow(/collision|content hash|immutable|existing point/i);
  }, 120_000);

  it("serializes concurrent privacy OCC transitions without regressing the epoch", async () => {
    const before = await store.readControl();
    const results = await Promise.allSettled([
      beginForgetBarrier(store, { now: Date.parse("2026-08-15T00:02:00.000Z") }),
      beginForgetBarrier(store, { now: Date.parse("2026-08-15T00:02:00.001Z") }),
    ]);
    const fulfilled = results.filter((result): result is PromiseFulfilledResult<Awaited<ReturnType<typeof beginForgetBarrier>>> => result.status === "fulfilled");
    expect(fulfilled.length).toBeGreaterThanOrEqual(1);
    const after = await store.readControl();
    expect(after.privacyEpoch).toBe(before.privacyEpoch + fulfilled.length);
    expect(after.version).toBe(before.version + fulfilled.length);
    expect(after.activeGeneration).toBeNull();
  }, 120_000);

  it("keeps a second host physically isolated during the race", async () => {
    const prime = createQdrantCoordinationStore(isolatedOptions(url, "prime"));
    await expect(prime.readControl()).resolves.toMatchObject({ ownerHost: "prime" });
    await expect(prime.readJob(deterministicUuid(`${isolatedRunId(process.env)}:race:missing`))).resolves.toBeNull();
  });

  it("recovers one shared-home pending producer under concurrent real-Qdrant adopters", async () => {
    const stub = await startEmbeddingStub(Array.from({ length: 1024 }, () => 0.125));
    const homeDir = await mkdtemp(join(tmpdir(), "task14-shared-home-adopt-"));
    try {
      const runtime = await task14Runtime(url, "pi", stub.baseUrl); const policy = task14Policy("pi"); const nodeId = `node-${"d".repeat(32)}`; const machineId = "task14-shared-machine";
      const stale = await createOutbox({ host: "pi", homeDir, sharedFilesystem: true, nodeId, machineId, producerUuid: randomUUID() });
      const source = task14Episode({ id: deterministicUuid(`${isolatedRunId(process.env)}:shared-home-episode`), ownerHost: "pi", control: runtime.control, policy, text: "task14 shared home recovery", vector: Array.from({ length: 1024 }, () => 0.125) });
      const { vector: _vector, ...withoutVector } = source; const pending = { ...withoutVector, sourceEntryId: "entry-shared-home", sessionId: "session-shared-home", turnId: "turn-shared-home", producerId: stale.producerUuid, nodeId, contentHash: "pending" }; const episode = { ...pending, contentHash: canonicalRecordHash(pending) } as EpisodeRecord;
      const job = await stale.enqueue({ episodes: [episode], policy }); await stale.closeProducer();
      const leftOutbox = await createOutbox({ host: "pi", homeDir, sharedFilesystem: true, nodeId, machineId, producerUuid: randomUUID() });
      const rightOutbox = await createOutbox({ host: "pi", homeDir, sharedFilesystem: true, nodeId, machineId, producerUuid: randomUUID() });
      const processor = createIngestProcessor({ runtime: bindIngestRuntime(runtime), localPolicy: policy, now: () => Date.parse("2026-08-15T00:00:01.000Z"), maxClockSkewMs: 0 });
      const common = { outboxRoot: stale.root, processor, now: () => Date.parse("2026-08-15T00:00:01.000Z"), maxClockSkewMs: 0 };
      const left = createOutboxDelivery({ ...common, producerPath: leftOutbox.producerPath }); const right = createOutboxDelivery({ ...common, producerPath: rightOutbox.producerPath });
      await Promise.all([left.adopt(stale.producerPath), right.adopt(stale.producerPath)]);
      const results = await Promise.all([left.deliver({}), right.deliver({})]);
      expect(results.reduce((sum, value) => sum + value.delivered, 0)).toBeGreaterThanOrEqual(1);
      await expect(runtime.qdrant.retrieve<EpisodeRecord>("episode", episode.id)).resolves.toMatchObject({ id: episode.id, nodeId, producerId: stale.producerUuid });
      await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
    } finally { await rm(homeDir, { recursive: true, force: true }); await stub.close(); }
  }, 180_000);
});

/** Shared real-Qdrant curation input using the exact production admin identity. */
function curationInput(runtime: Task14Runtime, membership: readonly string[], policy: ProcessingPolicy, completions: { count: number }, overrides: Partial<RootCurationLifecycleInput> = {}): RootCurationLifecycleInput {
  return {
    host: "pi", store: runtime.store, env: {}, nodeId: "task14-node", leaseMs: 30_000, maxClockSkewMs: 0,
    workerPolicy: policy, extractorRevision: "curation-v1", producerPolicies: [policy],
    embedding: runtime.embedding,
    llm: {
      memoryModel: { id: "task14-model", provider: "task14-provider", contextWindow: 1_000_000, maxTokens: 65_536 } as never,
      modelRegistry: { complete: async () => { completions.count += 1; return { content: [{ type: "text", text: JSON.stringify({ items: [{ category: "fact", scope: "project", subject: "task14", predicate: "curated", value: "yes", evidence: [membership[0]] }] }) }] }; } },
      llmDestination: TASK14_LLM_DESTINATION,
      llmDestinationBinding: { providerId: "task14-provider", modelId: "task14-model", destinationId: TASK14_LLM_DESTINATION.id },
    },
    membership, maxOutputTokens: 2_048, timeoutMs: 30_000,
    ...overrides,
  };
}

real("real curation lease/fencing matrix", () => {
  let url: string;
  beforeAll(async () => { await ensureMatrix(); url = matrixUrl; }, 120_000);

  it("runs two curators on one membership with a single LLM egress and terminal lease", async () => {
    const stub = await startEmbeddingStub(Array.from({ length: 1024 }, (_unused, index) => ((index % 29) - 14) / 14));
    try {
      const runtime = await task14Runtime(url, "pi", stub.baseUrl);
      const control = await runtime.store.readControl();
      const policy = task14Policy("pi");
      await runtime.qdrant.insertAndReadback(task14PolicyRecord(policy, control));
      const episodeId = deterministicUuid(`${isolatedRunId(process.env)}:curation-race:1`);
      await runtime.qdrant.insertAndReadback(task14Episode({ id: episodeId, ownerHost: "pi", control, policy, text: "task14 curation race episode", vector: canonicalizeEmbeddingVector(Array.from({ length: 1024 }, () => 0.25), 1024) }));
      const completions = { count: 0 };
      const [left, right] = await Promise.all([
        runCurationFromLifecycle(SessionManager.inMemory(), curationInput(runtime, [episodeId], policy, completions, { nodeId: "task14-node-left" })),
        runCurationFromLifecycle(SessionManager.inMemory(), curationInput(runtime, [episodeId], policy, completions, { nodeId: "task14-node-right" })),
      ]);
      // Regardless of interleaving, insert-only + lease CAS allow exactly one
      // proposal/LLM completion for the deterministic job identity.
      expect(completions.count, JSON.stringify({ left, right })).toBe(1);
      expect([left.state, right.state]).toContain("completed");
      const jobId = jobIdentity({ ownerHost: "pi", membership: [episodeId], policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "curation-v1", policyIntersectionId: policy.id, privacyEpoch: control.privacyEpoch });
      const lease = await runtime.store.readLease(jobId);
      expect(lease).toMatchObject({ state: "completed", acceptedProposalId: expect.any(String), acceptedManifestHash: expect.any(String) });
      const covered = await runtime.store.readCoverage([coverageId({ ownerHost: "pi", episodeId, extractorRevision: "curation-v1", coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch, policyIntersectionId: policy.id, privacyEpoch: control.privacyEpoch })]);
      expect(covered).toHaveLength(1);

      // Content/state forget scopes must hide already-materialized records and
      // keep stale physical reinsertion invisible at final retrieval.
      const reader = createGuardedMemoryReadStore({ ...isolatedOptions(url, "pi"), destination: TASK14_QDRANT_DESTINATION, egressMode: "allowlist" });
      const now = Date.parse("2026-08-15T00:30:00.000Z");
      const filter = laneFilter({ ownerHost: "pi", lane: "curated", projectId: "task14-project", global: false, now, maxClockSkewMs: 0, privacyEpoch: control.privacyEpoch, coordinationPolicyEpoch: control.coordinationPolicyEpoch });
      const materialized = await reader.search({ lane: "curated", vector: canonicalizeEmbeddingVector(Array.from({ length: 1024 }, () => 0.25), 1024), limit: 10, filter });
      const observation = materialized.map((hit) => hit.record).find((record) => record.recordType === "curated_memory");
      expect(observation?.recordType).toBe("curated_memory");
      if (observation?.recordType !== "curated_memory") throw new TypeError("Task 14 curated observation is missing");
      const retriever = new MemoryRetriever({ reader, config: isolatedConfig(url, "pi").retrieval, embedding: runtime.embedding, embeddingDestination: TASK14_EMBEDDING_DESTINATION, now: () => now });
      const search = () => retriever.search({ query: "task14 curated yes", host: "pi", project: { id: "task14-project", identityKind: "registered" }, isChild: false, modelDestination: TASK14_LLM_DESTINATION, mode: "curated", limit: 10 });
      expect((await search()).hits.length).toBeGreaterThan(0);
      await createTombstone(runtime.store, { ownerHost: "pi", scope: "content", targetId: observation.contentId, createdAt: "2026-08-15T00:31:00.000Z", privacyEpoch: control.privacyEpoch, processingPolicyId: policy.id });
      await createTombstone(runtime.store, { ownerHost: "pi", scope: "state", targetId: observation.stateKey, createdAt: "2026-08-15T00:31:01.000Z", privacyEpoch: control.privacyEpoch, processingPolicyId: policy.id });
      await expect(reader.readTombstones([observation.contentId, observation.stateKey])).resolves.toHaveLength(2);
      const stalePoints = materialized.map(({ record }) => ({ id: physicalPointId(record.recordType, record.id), payload: recordPayload(record), vector: "vector" in record && record.vector !== undefined ? { semantic: [...record.vector] } : {} }));
      const staleResponse = await fetch(`${url}/collections/pi_memory/points?wait=true&ordering=strong`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ points: stalePoints }) });
      expect(staleResponse.ok).toBe(true);
      expect((await search()).hits).toHaveLength(0);
    } finally { await stub.close(); }
  }, 120_000);

  it("reclaims an expired lease with a higher fence and rejects the stale writer", async () => {
    const stub = await startEmbeddingStub(Array.from({ length: 1024 }, () => 0.25));
    try {
      const runtime = await task14Runtime(url, "pi", stub.baseUrl);
      const control = await runtime.store.readControl();
      const policy = task14Policy("pi", "task14-policy-stale");
      await runtime.qdrant.insertAndReadback(task14PolicyRecord(policy, control));
      const episodeId = deterministicUuid(`${isolatedRunId(process.env)}:curation-stale:1`);
      await runtime.qdrant.insertAndReadback(task14Episode({ id: episodeId, ownerHost: "pi", control, policy, text: "task14 stale fence episode", vector: canonicalizeEmbeddingVector(Array.from({ length: 1024 }, () => 0.5), 1024), eventAt: "2026-08-15T00:10:00.000Z" }));
      const completions = { count: 0 };
      let markStarted!: () => void; let releaseSlow!: () => void;
      const started = new Promise<void>((resolve) => { markStarted = resolve; });
      const released = new Promise<void>((resolve) => { releaseSlow = resolve; });
      const now = { value: Date.parse("2026-08-15T00:20:00.000Z") };
      const slowRegistry = { complete: async () => { markStarted(); await released; completions.count += 1; return { content: [{ type: "text", text: JSON.stringify({ items: [{ category: "fact", scope: "project", subject: "task14", predicate: "stale", value: "no", evidence: [episodeId] }] }) }] }; } };
      // Hold the stale model response in flight, advance the trusted clock past
      // lease expiry, and let a fresh worker take over before releasing it.
      const stalePromise = runCurationFromLifecycle(SessionManager.inMemory(), curationInput(runtime, [episodeId], policy, completions, {
        nodeId: "task14-node-stale", leaseMs: 500, clock: () => now.value,
        llm: { memoryModel: { id: "task14-model", provider: "task14-provider", contextWindow: 1_000_000, maxTokens: 65_536 } as never, modelRegistry: slowRegistry, llmDestination: TASK14_LLM_DESTINATION, llmDestinationBinding: { providerId: "task14-provider", modelId: "task14-model", destinationId: TASK14_LLM_DESTINATION.id } },
      }));
      await started;
      now.value += 1_000;
      let fresh: Awaited<ReturnType<typeof runCurationFromLifecycle>>;
      const freshRegistry = { complete: async () => { completions.count += 1; return { content: [{ type: "text", text: JSON.stringify({ items: [{ category: "fact", scope: "project", subject: "task14", predicate: "stale", value: "fresh", evidence: [episodeId] }] }) }] }; } };
      try { fresh = await runCurationFromLifecycle(SessionManager.inMemory(), curationInput(runtime, [episodeId], policy, completions, { nodeId: "task14-node-fresh", clock: () => now.value, llm: { memoryModel: { id: "task14-model", provider: "task14-provider", contextWindow: 1_000_000, maxTokens: 65_536 } as never, modelRegistry: freshRegistry, llmDestination: TASK14_LLM_DESTINATION, llmDestinationBinding: { providerId: "task14-provider", modelId: "task14-model", destinationId: TASK14_LLM_DESTINATION.id } } })); }
      finally { releaseSlow(); }
      expect(fresh.state, JSON.stringify(fresh)).toBe("completed");
      const stale = await stalePromise;
      expect(["pending", "no_claim"]).toContain(stale.state);
      const jobId = jobIdentity({ ownerHost: "pi", membership: [episodeId], policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "curation-v1", policyIntersectionId: policy.id, privacyEpoch: control.privacyEpoch });
      const lease = await runtime.store.readLease(jobId);
      expect(lease).toMatchObject({ state: "completed" });
      expect(lease!.fencingToken).toBeGreaterThanOrEqual(2);
      // The stale in-flight completion must not have produced a second accepted proposal.
      expect(completions.count).toBe(2);
    } finally { await stub.close(); }
  }, 120_000);
});

function raptorInput(runtime: Task14Runtime, leaves: RootRaptorLifecycleInput["leaves"], policy: ProcessingPolicy, jobId: string): RootRaptorLifecycleInput {
  return {
    host: "pi", store: runtime.store, env: {}, nodeId: "task14-raptor-node", leaseMs: 30_000, maxClockSkewMs: 0,
    extractorRevision: "admin-raptor-v1", jobId, clock: () => Date.now(), workerPolicy: policy, leaves,
    llm: { destination: TASK14_LLM_DESTINATION, complete: async () => JSON.stringify({ summary: "task14 admin raptor summary" }) },
    embedding: runtime.embedding, modelId: "task14-model", homeDir: "/tmp/task14-home", seed: "task14-seed",
    maxLevels: 5, summaryInputTokens: 512, umapDimensions: 2, localNeighbors: 2, gmmMaxClusters: 4, membershipThreshold: 0.1,
  };
}

real("real admin RAPTOR queue and publication race", () => {
  let url: string;
  beforeAll(async () => { await ensureMatrix(); url = matrixUrl; }, 120_000);

  async function seedRaptor(runtime: Task14Runtime, control: Awaited<ReturnType<Task14Runtime["store"]["readControl"]>>, policy: ProcessingPolicy, tag: string) {
    await runtime.qdrant.insertAndReadback(task14PolicyRecord(policy, control));
    const leaves: RootRaptorLifecycleInput["leaves"] = [];
    for (let index = 0; index < 4; index += 1) {
      const id = deterministicUuid(`${isolatedRunId(process.env)}:${tag}:${index}`);
      const vector = canonicalizeEmbeddingVector(Array.from({ length: 1024 }, (_unused, dimension) => Math.sin(index * 0.31 + dimension * 0.017)), 1024);
      await runtime.qdrant.insertAndReadback(task14Episode({ id, ownerHost: "pi", control, policy, text: `task14 ${tag} evidence ${index}`, vector, eventAt: new Date(Date.parse("2026-08-15T00:00:00.000Z") + index).toISOString() }));
      leaves.push({ id, text: `task14 ${tag} evidence ${index}`, vector, tokens: 16, projectId: "task14-project", eventAt: new Date(Date.parse("2026-08-15T00:00:00.000Z") + index).toISOString(), policy });
    }
    return leaves;
  }

  it("claims a queued admin-raptor-v1 job and terminally completes it after publication", async () => {
    const stub = await startEmbeddingStub(Array.from({ length: 1024 }, () => 0.125));
    try {
      const runtime = await task14Runtime(url, "pi", stub.baseUrl);
      const control = await runtime.store.readControl();
      const policy = task14Policy("pi", "task14-policy-raptor");
      const leaves = await seedRaptor(runtime, control, policy, "raptor-admin");
      const intersection = intersectPolicies([policy], policy);
      expect(intersection).not.toBeNull();
      const membership = Object.freeze(leaves.map(leaf => leaf.id).sort());
      // Exact productionOperation identity for `raptor rebuild --enqueue`.
      const job = await createJob(runtime.store, { ownerHost: "pi", membership, policyIntersectionId: intersection!.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "admin-raptor-v1", privacyEpoch: control.privacyEpoch, createdAt: leaves.map(leaf => leaf.eventAt).sort()[0]!, expiresAt: intersection!.expiresAt });
      expect(job).toMatchObject({ membership, policyId: intersection!.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "admin-raptor-v1", privacyEpoch: control.privacyEpoch, createdAt: leaves.map(leaf => leaf.eventAt).sort()[0]!, expiresAt: intersection!.expiresAt });
      const result = await runRaptorFromLifecycle(SessionManager.inMemory(), raptorInput(runtime, leaves, policy, job.id));
      expect(result.state, JSON.stringify(result)).toBe("completed");
      const lease = await runtime.store.readLease(job.id);
      expect(lease).toMatchObject({ state: "completed", terminalOperation: "raptor", acceptedProposalId: null, acceptedManifestHash: null });
      const after = await runtime.store.readControl();
      expect(after.activeGeneration).not.toBeNull();
    } finally { await stub.close(); }
  }, 180_000);

  it("publishes exactly one generation when two runners race one admin RAPTOR job", async () => {
    const stub = await startEmbeddingStub(Array.from({ length: 1024 }, () => 0.125));
    try {
      const originalFetch = globalThis.fetch;
      const publicationWaiters: Array<{ input: RequestInfo | URL; init: RequestInit | undefined; resolve(response: Response): void }> = [];
      const publicationGenerationIds: string[] = [];
      globalThis.fetch = (async (input, init) => {
        if (init?.method === "PUT") {
          try {
            const body = JSON.parse(String(init.body ?? "{}"));
            const point = body.points?.[0];
            if (point?.payload?.record_type === "collection_control" && point.payload.active_generation !== null) {
              publicationGenerationIds.push(point.payload.active_generation);
              return await new Promise<Response>((resolve) => {
                publicationWaiters.push({ input, init, resolve });
                if (publicationWaiters.length === 2) for (const waiter of publicationWaiters.splice(0)) void originalFetch(waiter.input, waiter.init).then(waiter.resolve);
              });
            }
          } catch { /* ordinary requests use the captured transport */ }
        }
        return originalFetch(input, init);
      }) as typeof fetch;
      const runtime = await task14Runtime(url, "pi", stub.baseUrl);
      const control = await runtime.store.readControl();
      const policy = task14Policy("pi", "task14-policy-raptor-race");
      const leaves = await seedRaptor(runtime, control, policy, "raptor-race");
      const alternateLeaves = await seedRaptor(runtime, control, policy, "raptor-race-alt");
      const intersection = intersectPolicies([policy], policy)!;
      const membership = Object.freeze(leaves.map(leaf => leaf.id).sort());
      const alternateMembership = Object.freeze(alternateLeaves.map(leaf => leaf.id).sort());
      const createdAt = leaves.map(leaf => leaf.eventAt).sort()[0]!;
      const firstJob = await createJob(runtime.store, { ownerHost: "pi", membership, policyIntersectionId: intersection.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "admin-raptor-v1", privacyEpoch: control.privacyEpoch, createdAt, expiresAt: intersection.expiresAt });
      const secondJob = await createJob(runtime.store, { ownerHost: "pi", membership: alternateMembership, policyIntersectionId: intersection.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "admin-raptor-v1", privacyEpoch: control.privacyEpoch, createdAt, expiresAt: intersection.expiresAt });
      expect(secondJob.id).not.toBe(firstJob.id);
      try {
        const [left, right] = await Promise.all([
          runRaptorFromLifecycle(SessionManager.inMemory(), raptorInput(runtime, leaves, policy, firstJob.id)),
          runRaptorFromLifecycle(SessionManager.inMemory(), raptorInput(runtime, alternateLeaves, policy, secondJob.id)),
        ]);
        const completed = [left, right].filter(value => value.state === "completed");
        expect(new Set(publicationGenerationIds)).toHaveLength(2);
        expect(completed, JSON.stringify({ left, right })).toHaveLength(1);
        expect([left, right].some(value => value.state === "pending" && value.reason === "publication_lost")).toBe(true);
        const [firstLease, secondLease] = await Promise.all([runtime.store.readLease(firstJob.id), runtime.store.readLease(secondJob.id)]);
        expect([firstLease, secondLease].filter(value => value?.state === "completed")).toHaveLength(1);
        expect([firstLease, secondLease].filter(value => value?.terminalOperation === "raptor")).toHaveLength(1);
        const after = await runtime.store.readControl();
        expect(after.activeGeneration).toBe((completed[0] as { generationId: string }).generationId);
      } finally {
        globalThis.fetch = originalFetch;
      }
    } finally { await stub.close(); }
  }, 180_000);
});
