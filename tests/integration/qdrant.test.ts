import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { statusCollectionInfo, statusHealth } from "../../src/admin/transport.js";
import { beginForgetBarrier } from "../../src/coordination/control.js";
import { createJob } from "../../src/coordination/jobs.js";
import { createTombstone } from "../../src/coordination/tombstones.js";
import { COLLECTION_CONTROL_ID, V2_CONTRACT_HASH } from "../../src/qdrant/schema.js";
import { createGuardedMemoryReadStore, MemoryRetriever } from "../../src/retrieval/search.js";
import { laneFilter } from "../../src/retrieval/filters.js";
import { createQdrantCoordinationStore, bindQdrantDestination, createQdrantSafeBundle } from "../../src/qdrant/write.js";
import { activateBootstrapWorkerPolicy } from "../../src/extension.js";
import { productionOperation } from "../../src/admin/production.js";
import { intersectPolicies } from "../../src/domain/policy.js";
import { isolatedCollection, isolatedOptions, initializeIsolated, isolatedQdrantUrl, isolatedRunId, qdrantVersion, waitForIsolatedQdrant, deterministicUuid, isolatedConfig, startEmbeddingStub, task14Episode, task14Policy, task14PolicyRecord, task14Runtime, ISOLATED_QDRANT_VERSION, TASK14_QDRANT_DESTINATION, TASK14_EMBEDDING_DESTINATION, TASK14_LLM_DESTINATION } from "./qdrant-fixtures.js";
import type { HostId } from "../../src/types.js";

const noServiceEnv: Record<string, string | undefined> = {};

describe("isolated Qdrant 1.17.1 harness", () => {
  it("fails closed before contacting a service when the run guard is absent", () => {
    expect(() => isolatedQdrantUrl(noServiceEnv)).toThrow("isolated run ID and URL required");
    expect(() => isolatedRunId(noServiceEnv)).toThrow("isolated run ID and URL required");
    expect(() => isolatedQdrantUrl({ PI_QDRANT_MEMORY_TEST_RUN_ID: "abcdefghijkl", PI_QDRANT_MEMORY_TEST_QDRANT_URL: "https://example.invalid" })).toThrow("loopback Qdrant required");
    expect(() => isolatedQdrantUrl({ PI_QDRANT_MEMORY_TEST_RUN_ID: "abcdefghijkl", PI_QDRANT_MEMORY_TEST_QDRANT_URL: "http://127.0.0.1:6333", CI: "false" })).toThrow("default Qdrant port refused");
  });
});

const configured = process.env.PI_QDRANT_MEMORY_TEST_QDRANT_URL !== undefined && process.env.PI_QDRANT_MEMORY_TEST_RUN_ID !== undefined;
const isolated = configured ? describe : describe.skip;

isolated("real Qdrant 1.17.1 collection and coordination contract", () => {
  let url: string;
  const stores = new Map<HostId, Awaited<ReturnType<typeof initializeIsolated>>>();
  const originalFetch = globalThis.fetch;

  beforeAll(async () => {
    url = isolatedQdrantUrl(process.env);
    if (process.env.PI_QDRANT_MEMORY_TEST_DEBUG === "true") {
      globalThis.fetch = async (input, init) => { const response = await originalFetch(input, init); if (!response.ok) console.error("task14 store response", response.status, String(input), await response.clone().text()); return response; };
    }
    await waitForIsolatedQdrant(url);
    expect(await qdrantVersion(url)).toBe(ISOLATED_QDRANT_VERSION);
    for (const host of ["pi", "prime"] as const) stores.set(host, await initializeIsolated(url, host));
  }, 120_000);

  afterAll(async () => {
    stores.clear();
    globalThis.fetch = originalFetch;
  });

  it("creates physically separate host collections with owner-bound metadata/control", async () => {
    const pi = stores.get("pi")!;
    const prime = stores.get("prime")!;
    expect(pi.collection).toBe(isolatedCollection("pi"));
    expect(prime.collection).toBe(isolatedCollection("prime"));
    const [piInfo, primeInfo] = await Promise.all([
      statusCollectionInfo(isolatedOptions(url, "pi"), fetch),
      statusCollectionInfo(isolatedOptions(url, "prime"), fetch),
    ]);
    expect(piInfo.dimension).toBe(1024);
    expect(primeInfo.dimension).toBe(1024);
    expect(piInfo.distance).toBe("Dot");
    expect(primeInfo.distance).toBe("Dot");
    await expect(pi.readControl()).resolves.toMatchObject({ ownerHost: "pi", id: COLLECTION_CONTROL_ID, state: "active" });
    await expect(prime.readControl()).resolves.toMatchObject({ ownerHost: "prime", id: COLLECTION_CONTROL_ID, state: "active" });
    await expect(statusHealth(isolatedOptions(url, "pi"), fetch)).resolves.toBeDefined();
  });

  it("converges concurrent equal insert-only jobs and keeps distinct IDs separate", async () => {
    const store = stores.get("pi")!;
    const control = await store.readControl();
    const membership = [deterministicUuid(`${isolatedRunId(process.env)}:same:1`), deterministicUuid(`${isolatedRunId(process.env)}:same:2`)].sort();
    const input = { ownerHost: "pi" as const, membership, policyIntersectionId: control.processingPolicyId, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "task14-concurrent-v1", privacyEpoch: control.privacyEpoch, createdAt: "2026-08-15T00:00:00.000Z", expiresAt: null };
    const equal = await Promise.all(Array.from({ length: 20 }, () => createJob(store, input)));
    expect(new Set(equal.map(job => job.id))).toHaveLength(1);
    const distinct = await Promise.all(Array.from({ length: 20 }, (_, index) => createJob(store, { ...input, membership: [deterministicUuid(`${isolatedRunId(process.env)}:distinct:${index}`)] })));
    expect(new Set(distinct.map(job => job.id))).toHaveLength(20);
    const page = await store.scrollJobs(undefined, 256);
    const ids = new Set(page.jobs.map(job => job.id));
    expect(ids.has(equal[0]!.id)).toBe(true);
    for (const job of distinct) expect(ids.has(job.id)).toBe(true);
  }, 120_000);

  it("does not leak points or control epochs across host collections", async () => {
    const pi = stores.get("pi")!;
    const prime = stores.get("prime")!;
    const piControl = await pi.readControl();
    const primeControl = await prime.readControl();
    expect(piControl.ownerHost).toBe("pi");
    expect(primeControl.ownerHost).toBe("prime");
    expect(primeControl.privacyEpoch).toBe(0);
    const next = await beginForgetBarrier(pi, { now: Date.parse("2026-08-15T00:01:00.000Z") });
    expect(next.ownerHost).toBe("pi");
    expect(next.privacyEpoch).toBe(piControl.privacyEpoch + 1);
    await expect(prime.readControl()).resolves.toMatchObject({ ownerHost: "prime", privacyEpoch: primeControl.privacyEpoch });
    await expect(pi.readControl()).resolves.toMatchObject({ ownerHost: "pi", privacyEpoch: next.privacyEpoch, activeGeneration: null });
  });

  it("rejects a host collection mismatch before any request", () => {
    expect(() => isolatedConfig(url, "pi")).not.toThrow();
    expect(() => createQdrantCoordinationStore({ ...isolatedOptions(url, "pi"), collection: "prime_memory" })).toThrow(/collection does not match owner host/i);
  });

  it("isolates divergent machine policies and retention before model egress", async () => {
    const stub = await startEmbeddingStub(Array.from({ length: 1024 }, (_unused, index) => ((index % 31) - 15) / 15));
    try {
      const pi = await task14Runtime(url, "pi", stub.baseUrl);
      const prime = await task14Runtime(url, "prime", stub.baseUrl);
      const piControl = await pi.store.readControl(); const primeControl = await prime.store.readControl();
      const piPolicy = task14Policy("pi", "task14-machine-a");
      const expiredPolicy = task14Policy("pi", "task14-machine-a-expired", { expiresAt: "2099-01-01T00:00:00.000Z" });
      const primePolicy = task14Policy("prime", "task14-machine-b", { destinationIds: { qdrant: TASK14_QDRANT_DESTINATION.id, embedding: "embed:machine-b", llm: "llm:machine-b" }, originProvider: "machine-b-provider" });
      await pi.qdrant.insertAndReadback(task14PolicyRecord(piPolicy, piControl));
      await pi.qdrant.insertAndReadback(task14PolicyRecord(expiredPolicy, piControl));
      await prime.qdrant.insertAndReadback(task14PolicyRecord(primePolicy, primeControl));
      const vector = [...await pi.embedding.embed({ model: "bge-m3", text: "task14 machine policy probe" })];
      const piEpisode = task14Episode({ id: deterministicUuid(`${isolatedRunId(process.env)}:machine-a`), ownerHost: "pi", control: piControl, policy: piPolicy, text: "task14 machine a visible", vector });
      const expiredEpisode = task14Episode({ id: deterministicUuid(`${isolatedRunId(process.env)}:machine-a-expired`), ownerHost: "pi", control: piControl, policy: expiredPolicy, text: "task14 expired invisible", vector });
      const primeEpisode = task14Episode({ id: deterministicUuid(`${isolatedRunId(process.env)}:machine-b`), ownerHost: "prime", control: primeControl, policy: primePolicy, text: "task14 machine b policy blocked", vector });
      await pi.qdrant.insertAndReadback(piEpisode); await pi.qdrant.insertAndReadback(expiredEpisode); await prime.qdrant.insertAndReadback(primeEpisode);
      const now = Date.parse("2100-01-01T00:00:00.000Z");
      const piReader = createGuardedMemoryReadStore({ ...isolatedOptions(url, "pi"), destination: TASK14_QDRANT_DESTINATION, egressMode: "allowlist" });
      const primeReader = createGuardedMemoryReadStore({ ...isolatedOptions(url, "prime"), destination: TASK14_QDRANT_DESTINATION, egressMode: "allowlist" });
      const piFilter = laneFilter({ ownerHost: "pi", lane: "episodes", projectId: "task14-project", global: false, now, maxClockSkewMs: 0, privacyEpoch: piControl.privacyEpoch, coordinationPolicyEpoch: piControl.coordinationPolicyEpoch });
      const primeFilter = laneFilter({ ownerHost: "prime", lane: "episodes", projectId: "task14-project", global: false, now, maxClockSkewMs: 0, privacyEpoch: primeControl.privacyEpoch, coordinationPolicyEpoch: primeControl.coordinationPolicyEpoch });
      expect((await piReader.search({ lane: "episodes", vector, limit: 10, filter: piFilter })).map((hit) => hit.record.id)).toEqual([piEpisode.id]);
      expect((await primeReader.search({ lane: "episodes", vector, limit: 10, filter: primeFilter })).map((hit) => hit.record.id)).toContain(primeEpisode.id);
      await expect(piReader.retrieve([{ recordType: "episode", id: primeEpisode.id }])).resolves.toEqual([]);
      await expect(primeReader.retrieve([{ recordType: "episode", id: piEpisode.id }])).resolves.toEqual([]);
      const primeRetriever = new MemoryRetriever({ reader: primeReader, config: isolatedConfig(url, "prime").retrieval, embedding: prime.embedding, embeddingDestination: TASK14_EMBEDDING_DESTINATION, now: () => now });
      const beforeRequests = stub.requests.length;
      const blocked = await primeRetriever.search({ query: "task14 machine b policy blocked", host: "prime", project: { id: "task14-project", identityKind: "registered" }, isChild: false, modelDestination: TASK14_LLM_DESTINATION, limit: 5 });
      expect(blocked.hits).toEqual([]);
      expect(stub.requests.length).toBe(beforeRequests);
    } finally { await stub.close(); }
  }, 120_000);

  it("hides a forgotten episode at retrieval even when stale reinsertion converges", async () => {
    const vector = Array.from({ length: 1024 }, (_unused, index) => ((index % 29) - 14) / 14);
    const stub = await startEmbeddingStub(vector);
    try {
      const runtime = await task14Runtime(url, "pi", stub.baseUrl);
      const control = await runtime.store.readControl();
      const policy = task14Policy("pi", "task14-policy-forget");
      await runtime.qdrant.insertAndReadback(task14PolicyRecord(policy, control));
      const embedded = [...await runtime.embedding.embed({ model: "bge-m3", text: "task14 forget retrieval probe" })];
      const episode = task14Episode({ id: deterministicUuid(`${isolatedRunId(process.env)}:forget:1`), ownerHost: "pi", control, policy, text: "task14 forget retrieval probe", vector: embedded });
      expect(await runtime.qdrant.insertAndReadback(episode)).toBe("inserted");
      const reader = createGuardedMemoryReadStore({ ...isolatedOptions(url, "pi"), destination: TASK14_QDRANT_DESTINATION, egressMode: "allowlist" });
      const now = Date.parse("2026-08-15T01:00:00.000Z");
      const direct = await reader.search({ lane: "episodes", vector: embedded, limit: 5, filter: laneFilter({ ownerHost: "pi", lane: "episodes", projectId: "task14-project", global: false, now, maxClockSkewMs: 0, privacyEpoch: control.privacyEpoch, coordinationPolicyEpoch: control.coordinationPolicyEpoch }) });
      expect(direct.map(hit => hit.record.id)).toContain(episode.id);
      await expect(reader.readPolicies([policy.id])).resolves.toMatchObject([{ id: policy.id, policy: { destinationIds: { qdrant: TASK14_QDRANT_DESTINATION.id, llm: TASK14_LLM_DESTINATION.id } } }]);
      await expect(reader.readTombstones([episode.id])).resolves.toHaveLength(0);
      const retriever = new MemoryRetriever({ reader, config: isolatedConfig(url, "pi").retrieval, embedding: runtime.embedding, embeddingDestination: TASK14_EMBEDDING_DESTINATION, now: () => now });
      const search = () => retriever.search({ query: "task14 forget retrieval probe", host: "pi", project: { id: "task14-project", identityKind: "registered" }, isChild: false, modelDestination: TASK14_LLM_DESTINATION, limit: 5 });
      const before = await search();
      expect(stub.requests.length).toBeGreaterThan(0);
      expect(before.hits.map(hit => hit.id)).toContain(episode.id);
      await createTombstone(runtime.store, { ownerHost: "pi", scope: "occurrence", targetId: episode.id, targetKind: "episode", provenanceIds: [], createdAt: "2026-08-15T00:30:00.000Z", privacyEpoch: control.privacyEpoch, processingPolicyId: control.processingPolicyId });
      expect((await search()).hits).toHaveLength(0);
      // Stale physical reinsertion of the same point converges (insert-only)
      // and the tombstone barrier keeps the record logically invisible.
      expect(await runtime.qdrant.insertAndReadback(episode)).toBe("existing");
      expect((await search()).hits).toHaveLength(0);
      // The prime host collection is untouched by the pi tombstone.
      const prime = stores.get("prime")!;
      await expect(prime.readTombstones([episode.id])).resolves.toHaveLength(0);
    } finally { await stub.close(); }
  }, 120_000);

  it("activates the bootstrap worker policy so human curation enqueue resolves it", async () => {
    // Prime remains the exact active version-0 bootstrap through this file.
    const probe = stores.get("prime")!;
    const control = await probe.readControl();
    expect(control).toMatchObject({ version: 0, state: "active", processingPolicyId: V2_CONTRACT_HASH, coordinationPolicyHash: V2_CONTRACT_HASH });
    const options = isolatedOptions(url, "prime");
    const bundle = createQdrantSafeBundle({ options, destination: TASK14_QDRANT_DESTINATION, egressMode: "allowlist", coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch });
    const bound = bindQdrantDestination(bundle.qdrant, TASK14_QDRANT_DESTINATION);
    const policy = task14Policy("prime", "task14-policy-activation");
    await bound.insertAndReadback(task14PolicyRecord(policy, control));
    const vector = Array.from({ length: 1024 }, () => 0.5);
    const episode = task14Episode({ id: deterministicUuid(`${isolatedRunId(process.env)}:activation:1`), ownerHost: "prime", control, policy, text: "task14 bootstrap activation probe", vector });
    await bound.insertAndReadback(episode);
    const config = isolatedConfig(url, "prime");
    const adminEnv = { PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY: "task14-admin" };
    // Red proof at the production seam: the placeholder has no policy record.
    await expect(productionOperation(config, adminEnv, { command: "curate", action: "enqueue" })).rejects.toThrow(/active worker policy/i);
    const activated = await activateBootstrapWorkerPolicy({
      control, policy, now: () => Date.parse("2026-08-15T00:02:00.000Z"),
      io: {
        readPolicy: (id) => bound.retrieve("processing_policy", id),
        insertPolicy: (record) => bound.insertAndReadback(record),
        activate: (workerPolicyId) => bundle.store.activateWorkerPolicyControl(workerPolicyId),
        readControl: () => probe.readControl(),
      },
    });
    expect(activated).toMatchObject({ version: 1, coordinationPolicyEpoch: 1, processingPolicyId: policy.id, coordinationPolicyHash: policy.id, state: "active" });
    // Idempotent: an already-activated control performs no writes.
    const again = await activateBootstrapWorkerPolicy({
      control: activated, policy,
      io: {
        readPolicy: async () => { throw new Error("must not read policy"); },
        insertPolicy: async () => { throw new Error("must not insert"); },
        activate: async () => { throw new Error("must not activate"); },
        readControl: () => probe.readControl(),
      },
    });
    expect(again).toBe(activated);
    // Green: the human admin path now resolves the active worker policy.
    const result = await productionOperation(config, adminEnv, { command: "curate", action: "enqueue" }) as Record<string, unknown>;
    expect(result).toMatchObject({ ok: true, queued: true, privacyEpoch: 0, membershipCount: 1 });
    expect(result.jobId).toBeTypeOf("string");
    expect(intersectPolicies([policy], policy)?.id).toBe(policy.id);
  }, 120_000);
});
