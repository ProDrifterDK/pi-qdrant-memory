import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord } from "../../src/domain/records.js";
import { COLLECTION_CONTROL_ID, controlPayload } from "../../src/qdrant/schema.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { createQdrantSafeBundle, recordPayload, type ProductionCoordinationStore } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, type BoundEmbeddingDestination } from "../../src/clients/embeddings.js";
import { readJob } from "../../src/coordination/jobs.js";
import { readLease } from "../../src/coordination/leases.js";
import { createTombstone } from "../../src/coordination/tombstones.js";
import { runCurationFromLifecycle, type RootCurationLifecycleInput, type CurationRunResult } from "../../src/coordination/root.js";
import type { AuthorizedDestination, HostId } from "../../src/types.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";

// ---------------------------------------------------------------------------
// Canonical fixtures — no overlap with other suites.
// ---------------------------------------------------------------------------
const OWNER: HostId = "pi";
const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination: AuthorizedDestination = { id: "embed:local", residency: "local", dataUse: "memory" };
const llmDestination: AuthorizedDestination = { id: "llm:local", residency: "local", dataUse: "memory" };
const coordination = { policyHash: "coordination-policy-hash-coverage-lifecycle-v1", policyEpoch: 1 } as const;
const NOW = "2026-08-10T00:00:00.000Z";
const NOW_MS = Date.parse(NOW);
const EXTRACTOR = "extractor-coverage-lifecycle-v1";
const providerBinding = { providerId: "provider-local", modelId: "provider-model", destinationId: "llm:local" };

const EP_A = "00000000-0000-5000-8000-0000000000c1";
const EP_B = "00000000-0000-5000-8000-0000000000c2";
const EP_C = "00000000-0000-5000-8000-0000000000c3";

function json(value: unknown, status = 200): Response { return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } }); }
afterEach(() => { vi.unstubAllGlobals(); });

/**
 * Per-test producer policy. Each test case builds its OWN producer policy so
 * the producerPolicy.id (deterministic hash of destinationIds + originProvider +
 * revision + ...) is fresh; the seed episode must reference the EXACT same
 * `processingPolicyId` so the worker's compatibility group resolves a match.
 */
function producerPolicy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const pending = {
    id: "pending", ownerHost: OWNER,
    destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null,
    residency: "local", dataUse: "memory", policyRevision: "coverage-lifecycle-producer-default",
    ...overrides,
  } as ProcessingPolicy;
  return { ...pending, id: processingPolicyHash(pending) };
}

function workerPolicy(revision: string): ProcessingPolicy {
  return producerPolicy({ policyRevision: revision });
}

function episode(id: string, producer: ProcessingPolicy, overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const base: EpisodeRecord = {
    recordType: "episode", id, ownerHost: OWNER, schemaRevision: 1,
    createdAt: NOW, privacyEpoch: 0, processingPolicyId: producer.id, expiresAt: null, contentHash: "pending",
    sourceEntryId: `entry-${id}`, host: OWNER, projectId: "project-coverage-lifecycle",
    projectIdentityKind: "registered", sessionId: "session-coverage", turnId: "turn-coverage",
    agentRole: "root", depth: 0, eventKind: "user", eventAt: NOW,
    modelId: "capture-model", embeddingDimension: 1024, originProvider: "provider-local",
    destinationId: qdrantDestination.id, status: "active", redactionStatus: "redacted",
    secretScan: "passed", text: "safe [token redacted]",
    vector: Array.from({ length: 1024 }, () => 0.25),
    sessionSequence: 1,
  };
  const value = { ...base, ...overrides } as EpisodeRecord;
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}

function emptyControl(): ControlRecord {
  const base: ControlRecord = {
    ownerHost: OWNER, schemaRevision: 1, createdAt: NOW, privacyEpoch: 0,
    processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control",
    id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null,
    coordinationPolicyEpoch: coordination.policyEpoch, coordinationPolicyHash: coordination.policyHash,
    state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending",
  };
  const value = { ...base } as ControlRecord;
  return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord;
}

type Fault = "drop-coverage-write" | "drop-coverage-read" | "alter-coverage-read";
interface WirePoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }

/**
 * Filter-honoring fake REST backend with three coverage fault hooks:
 *  - drop-coverage-write: PUT drops every coverage record (materializeCuration
 *    throws on its own readback; lease released-with-pair).
 *  - drop-coverage-read: completion retrieve omits coverage records
 *    (check-19 fails and the accepted pair is released for retry).
 *  - alter-coverage-read: completion retrieve rewrites coverage identity
 *    (validated parsing fails and the accepted pair is released for retry).
 */
function backendWithControl(fault: Fault | undefined, seed: WirePoint[] = []): { points: Map<string, WirePoint>; options: QdrantClientOptions; setFault: (next: Fault | undefined) => void } {
  const points = new Map<string, WirePoint>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) }]]);
  for (const point of seed) points.set(point.id, point);
  let activeFault: Fault | undefined = fault;
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input);
    const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: WirePoint[]; offset?: string | null; limit?: number; update_mode?: string; update_filter?: { must: Array<{ key: string; match?: { value?: unknown }; is_null?: { key: string }; range?: { gt?: string; lte?: string } }> } };
    if (new URL(url).pathname.endsWith("/points") && init.method === "POST") {
      const requestedIds = body?.ids ?? [];
      const requestedCoverageCount = requestedIds.filter((id) => points.get(id)?.payload.record_type === "coverage").length;
      const completionCoverageRead = requestedCoverageCount > 1;
      const result: WirePoint[] = [];
      for (const id of requestedIds) {
        const stored = points.get(id);
        if (stored === undefined) continue;
        const isCoverage = stored.payload.record_type === "coverage";
        if (isCoverage && completionCoverageRead && activeFault === "drop-coverage-read") continue;
        const next: WirePoint = { id: stored.id, payload: { ...stored.payload }, ...(stored.vector?.semantic === undefined ? {} : { vector: { semantic: [...stored.vector.semantic] } }) };
        if (isCoverage && completionCoverageRead && activeFault === "alter-coverage-read") {
          next.payload = { ...next.payload, episode_id: "00000000-0000-5000-8000-deadbeef0000" };
        }
        result.push(next);
      }
      return json({ result, status: "ok" });
    }
    if (url.includes("/points/scroll")) {
      const offset = body?.offset === null || body?.offset === undefined ? undefined : (body.offset as string);
      const jobPoints = [...points.values()].filter((p) => p.payload.record_type === "job").sort((a, b) => (a.id < b.id ? -1 : 1));
      const limit = body?.limit ?? 256;
      const startIndex = offset === undefined ? 0 : jobPoints.findIndex((p) => p.id === offset) + 1;
      const page = startIndex >= 0 ? jobPoints.slice(startIndex, startIndex + limit) : [];
      const lastId = page[page.length - 1]?.id;
      const nextOffset = (startIndex + limit < jobPoints.length && lastId !== undefined) ? lastId : undefined;
      const response: { points: WirePoint[]; next_page_offset?: string | null } = { points: page.map((p) => ({ id: p.id, payload: { ...p.payload } })) };
      if (nextOffset !== undefined) response.next_page_offset = nextOffset;
      return json({ result: response, status: "ok" });
    }
    if (url.includes("/points?") && init.method === "PUT") {
      for (const p of body?.points ?? []) {
        const isCoverage = p.payload.record_type === "coverage";
        if (isCoverage && activeFault === "drop-coverage-write") continue;
        if (body?.update_mode === "insert_only" && points.has(p.id)) continue;
        points.set(p.id, { id: p.id, payload: { ...p.payload }, ...(p.vector?.semantic === undefined ? {} : { vector: { semantic: [...p.vector.semantic] } }) });
      }
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  };
  vi.stubGlobal("fetch", fetchImpl);
  const state = { points, options: undefined as unknown as QdrantClientOptions, setFault: (next: Fault | undefined) => { activeFault = next; } };
  state.options = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: OWNER, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
  return state;
}

function seedEpisodePoint(backend: { points: Map<string, WirePoint> }, record: EpisodeRecord): void {
  const payload = recordPayload(record) as Record<string, unknown>;
  backend.points.set(record.id, { id: record.id, payload, vector: { semantic: [...(record.vector ?? Array.from({ length: 1024 }, () => 0.25))] } });
}

function bindEmbedding(): BoundEmbeddingDestination {
  const qdrantFetch = globalThis.fetch;
  vi.stubGlobal("fetch", async (input, init) => {
    if (String(input).includes("/embeddings")) return json({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] });
    if (qdrantFetch === undefined) throw new Error("transport unavailable");
    return qdrantFetch(input, init);
  });
  const client = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return bindEmbeddingDestination(factory, embeddingDestination);
}

function runtime(backend: { points: Map<string, WirePoint>; options: QdrantClientOptions }): { store: ProductionCoordinationStore; embedding: BoundEmbeddingDestination } {
  const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return { store: bundle.store, embedding: bindEmbedding() };
}

function rootManager(): SessionManager { return SessionManager.inMemory(); }

interface LlmCounter { count: number }

interface BaseOptionsInput {
  store: ProductionCoordinationStore;
  embedding: BoundEmbeddingDestination;
  membership: readonly string[];
  producerPolicies: readonly ProcessingPolicy[];
  workerPolicy: ProcessingPolicy;
  llmEvidence?: string;
  llmCallCounter?: LlmCounter;
}

function baseOptions(input: BaseOptionsInput): RootCurationLifecycleInput {
  const evidenceId = input.llmEvidence ?? input.membership[0] ?? EP_A;
  const llmText = `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${evidenceId}"]}]}`;
  return {
    host: OWNER, store: input.store, nodeId: "node-coverage-lifecycle",
    leaseMs: 30_000, maxClockSkewMs: 0, clock: () => NOW_MS,
    workerPolicy: input.workerPolicy, extractorRevision: EXTRACTOR,
    producerPolicies: input.producerPolicies, embedding: input.embedding,
    llm: {
      memoryModel: { id: "provider-model", provider: "provider-local", contextWindow: 1_000_000, maxTokens: 65_536 } as never,
      modelRegistry: {
        complete: async () => {
          input.llmCallCounter && (input.llmCallCounter.count += 1);
          return { content: [{ type: "text", text: llmText }] };
        },
      },
      llmDestination, llmDestinationBinding: providerBinding,
    },
    membership: [...input.membership],
    createdAt: () => NOW,
    env: {},
  };
}

async function runOnce(options: RootCurationLifecycleInput): Promise<CurationRunResult> {
  return runCurationFromLifecycle(rootManager(), options);
}

function countCoverage(backend: { points: Map<string, WirePoint> }): number {
  return [...backend.points.values()].filter((p) => p.payload.record_type === "coverage").length;
}

// ---------------------------------------------------------------------------
// End-to-end observable coverage lifecycle — every test drives the genuine
// runCurationFromLifecycle entry point. No vi.mock, no vi.hoisted, no test
// issuer, no forged authority. The production RootWorkerContext is minted
// inside runCurationFromLifecycle; every LeaseAuthority is produced by the
// genuine lease kernel.
// ---------------------------------------------------------------------------
describe("Task 9 curation coverage lifecycle — observable end-to-end", () => {
  it("exact coverage cardinality completes the lease: result.completed + lease.state='completed' + coverage count == membership", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-happy" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    const epC = episode(EP_C, producer);
    const backend = backendWithControl(undefined);
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB); seedEpisodePoint(backend, epC);
    const rt = runtime(backend);
    const counter: LlmCounter = { count: 0 };
    const result = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B, EP_C],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-happy"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    expect(result.state).toBe("completed");
    expect(result.jobId).toBeDefined();
    expect(counter.count).toBe(1);
    const lease = await readLease(rt.store, result.jobId!);
    expect(lease?.state).toBe("completed");
    expect(lease?.acceptedProposalId).not.toBeNull();
    expect(lease?.acceptedManifestHash).not.toBeNull();
    const job = await readJob(rt.store, result.jobId!);
    expect(job).not.toBeNull();
    expect(countCoverage(backend)).toBe(job!.membership.length);
    const coverageIds = [...backend.points.values()].filter((p) => p.payload.record_type === "coverage").map((p) => p.id).sort();
    expect(new Set(coverageIds).size).toBe(coverageIds.length);
    expect(coverageIds.length).toBe(3);
  });

  it("coverage cardinality omitted on write leaves the lease released-with-pair (no second LLM needed for resume)", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-drop-write" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    const backend = backendWithControl("drop-coverage-write");
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB);
    const rt = runtime(backend);
    const counter: LlmCounter = { count: 0 };
    const result = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-drop-write"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    // materializeCuration throws on its own readback assertion; the durable
    // released-with-pair is observable via the public lease reader.
    expect(result.state).toBe("pending");
    expect(result.jobId).toBeDefined();
    expect(counter.count).toBe(1);
    const lease = await readLease(rt.store, result.jobId!);
    expect(lease?.state).toBe("released");
    expect(lease?.acceptedProposalId).not.toBeNull();
    expect(lease?.acceptedManifestHash).not.toBeNull();
    expect(countCoverage(backend)).toBe(0);
    // Materialization succeeded for observations/links/currents; only coverage
    // is missing in the store.
    const observations = [...backend.points.values()].filter((p) => p.payload.record_type === "curated_memory").length;
    expect(observations).toBe(1);
  });

  it("coverage cardinality dropped on read releases the accepted pair (terminal CAS never attempted)", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-drop-read" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    const backend = backendWithControl("drop-coverage-read");
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB);
    const rt = runtime(backend);
    const counter: LlmCounter = { count: 0 };
    const result = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-drop-read"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    // completeJob's check-19 cardinality gate fails (read returns empty
    // coverage). completeJob returns false → runCurationCore returns
    // "pending" with reason "job-completion-readback". The CAS is never
    // attempted; the root worker releases the still-accepted durable pair.
    expect(result.state).toBe("pending");
    expect(result.reason).toBe("job-completion-readback");
    expect(counter.count).toBe(1);
    const lease = await readLease(rt.store, result.jobId!);
    expect(lease?.state).toBe("released");
    expect(lease?.acceptedProposalId).not.toBeNull();
    expect(lease?.acceptedManifestHash).not.toBeNull();
    // The coverage points were written correctly (cardinality matches
    // membership); only the read returns zero.
    expect(countCoverage(backend)).toBe(2);
  });

  it("coverage identity altered on read releases the accepted pair (terminal CAS never attempted)", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-alter-read" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    const backend = backendWithControl("alter-coverage-read");
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB);
    const rt = runtime(backend);
    const counter: LlmCounter = { count: 0 };
    const result = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-alter-read"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    expect(result.state).toBe("pending");
    expect(result.reason).toBe("materialization-failed");
    expect(counter.count).toBe(1);
    const lease = await readLease(rt.store, result.jobId!);
    expect(lease?.state).toBe("released");
    expect(lease?.acceptedProposalId).not.toBeNull();
    expect(lease?.acceptedManifestHash).not.toBeNull();
    expect(countCoverage(backend)).toBe(2);
  });

  it("retry of an accepted-then-released lease completes without invoking the LLM a second time (resume via pair)", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-retry" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    // Fault the FIRST run only: drop the coverage on write so the first run
    // ends in released-with-pair. Then clear the fault and run again on the
    // SAME producer/membership; the resume path must consume the pair
    // without invoking the LLM.
    const backend = backendWithControl("drop-coverage-write");
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB);
    const rt = runtime(backend);
    const counter: LlmCounter = { count: 0 };
    const first = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-retry"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    expect(first.state).toBe("pending");
    expect(counter.count).toBe(1);
    const firstLease = await readLease(rt.store, first.jobId!);
    expect(firstLease?.state).toBe("released");
    expect(firstLease?.acceptedProposalId).not.toBeNull();
    expect(firstLease?.acceptedManifestHash).not.toBeNull();
    expect(countCoverage(backend)).toBe(0);
    backend.setFault(undefined);
    const second = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-retry"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    // The resume path consumed the durable pair; no second LLM was fired.
    expect(second.state).toBe("completed");
    expect(second.jobId).toBe(first.jobId);
    expect(counter.count).toBe(1);
    const finalLease = await readLease(rt.store, second.jobId!);
    expect(finalLease?.state).toBe("completed");
    const finalJob = await readJob(rt.store, second.jobId!);
    expect(finalJob).not.toBeNull();
    expect(countCoverage(backend)).toBe(finalJob!.membership.length);
  });

  it("tombstone over a membership episode causes the lifecycle to leave the lease pending/no_claim without consuming any coverage identity", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-tombstone" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    const backend = backendWithControl(undefined);
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB);
    const rt = runtime(backend);
    // Tombstone EP_A before invoking the lifecycle; the tombstone barrier
    // inside the worker must keep the cycle durable but non-completing. No
    // completed lease and no coverage point may be persisted for the
    // tombstoned target.
    await createTombstone(rt.store, {
      ownerHost: OWNER, scope: "occurrence", targetId: EP_A, targetKind: "episode",
      createdAt: NOW, privacyEpoch: 0, processingPolicyId: producer.id,
    });
    const counter: LlmCounter = { count: 0 };
    const result = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-tombstone"),
      llmEvidence: EP_B, llmCallCounter: counter,
    }));
    // The result must not be "completed" — the tombstone barrier prevents
    // terminal coverage. The lease stays in a non-completed state and the
    // lifecycle returns either "pending" with a tombstone-derived reason or
    // "no_claim" depending on the planner behavior.
    expect(["pending", "no_claim"]).toContain(result.state);
    if (result.state === "pending") {
      const lease = await readLease(rt.store, result.jobId!);
      expect(lease).not.toBeNull();
      expect(lease?.state).not.toBe("completed");
      expect(["leased", "released", "accepted"]).toContain(lease?.state ?? "leased");
    }
    // No terminal CAS to "completed" was made; no coverage points persist.
    expect(countCoverage(backend)).toBe(0);
    // The tombstone itself is observable in the backend.
    const tombstones = [...backend.points.values()].filter((p) => p.payload.record_type === "tombstone");
    expect(tombstones.length).toBeGreaterThan(0);
  });

  it("lease state observable via readLease never advances to 'completed' when materializeCuration readback fails (drop-coverage-write) and the durable pair is preserved for resume", async () => {
    const producer = producerPolicy({ policyRevision: "coverage-lifecycle-cap-write" });
    const epA = episode(EP_A, producer);
    const epB = episode(EP_B, producer);
    const epC = episode(EP_C, producer);
    const backend = backendWithControl("drop-coverage-write");
    seedEpisodePoint(backend, epA); seedEpisodePoint(backend, epB); seedEpisodePoint(backend, epC);
    const rt = runtime(backend);
    const counter: LlmCounter = { count: 0 };
    const result = await runOnce(baseOptions({
      store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B, EP_C],
      producerPolicies: [producer], workerPolicy: workerPolicy("coverage-lifecycle-cap-write"),
      llmEvidence: EP_A, llmCallCounter: counter,
    }));
    expect(result.state).toBe("pending");
    const lease = await readLease(rt.store, result.jobId!);
    expect(lease).not.toBeNull();
    expect(lease?.state).not.toBe("completed");
    expect(lease?.acceptedProposalId).not.toBeNull();
    expect(lease?.acceptedManifestHash).not.toBeNull();
    expect(countCoverage(backend)).toBe(0);
  });
});
