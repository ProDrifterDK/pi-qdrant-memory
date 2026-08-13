import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type JobRecord } from "../../src/domain/records.js";
import { COLLECTION_CONTROL_ID, controlPayload } from "../../src/qdrant/schema.js";
import { processingPolicyHash, intersectPolicies, type ProcessingPolicy } from "../../src/domain/policy.js";
import { createQdrantSafeBundle, recordPayload, type ProductionCoordinationStore } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, type BoundEmbeddingDestination } from "../../src/clients/embeddings.js";
import { readJob } from "../../src/coordination/jobs.js";
import { readLease } from "../../src/coordination/leases.js";
import { createTombstone } from "../../src/coordination/tombstones.js";
import { runCurationFromLifecycle, type RootCurationLifecycleInput, type CurationRunResult } from "../../src/coordination/root.js";
import { jobId } from "../../src/domain/ids.js";
import type { AuthorizedDestination, HostId } from "../../src/types.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import { parseCurationProposalEnvelope } from "../../src/curation/provenance.js";

// ---------------------------------------------------------------------------
// Canonical fixtures (no overlap with other suites).
// ---------------------------------------------------------------------------
const OWNER: HostId = "pi";
const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination: AuthorizedDestination = { id: "embed:local", residency: "local", dataUse: "memory" };
const llmDestination: AuthorizedDestination = { id: "llm:local", residency: "local", dataUse: "memory" };
const coordination = { policyHash: "coordination-policy-hash-resume-v1", policyEpoch: 1 } as const;
const NOW = "2026-08-10T00:00:00.000Z";
const NOW_MS = Date.parse(NOW);
const EXTRACTOR = "extractor-resume-v1";
const providerBinding = { providerId: "provider-local", modelId: "provider-model", destinationId: "llm:local" };

const EP_A = "00000000-0000-5000-8000-0000000000a1";
const EP_B = "00000000-0000-5000-8000-0000000000a2";
const EP_C = "00000000-0000-5000-8000-0000000000a3";
const EP_D = "00000000-0000-5000-8000-0000000000a4";
const EP_E = "00000000-0000-5000-8000-0000000000a5";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}
afterEach(() => vi.unstubAllGlobals());

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const base = {
    id: "pending", ownerHost: OWNER,
    destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null,
    residency: "local", dataUse: "memory", policyRevision: "resume-v1",
    ...overrides,
  };
  return { ...base, id: processingPolicyHash(base as ProcessingPolicy) } as ProcessingPolicy;
}

function episode(overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const base: EpisodeRecord = {
    recordType: "episode", id: EP_A, ownerHost: OWNER, schemaRevision: 1,
    createdAt: NOW, privacyEpoch: 0, processingPolicyId: policy().id, expiresAt: null, contentHash: "pending",
    sourceEntryId: `entry-${EP_A}`, host: OWNER, projectId: "project-resume",
    projectIdentityKind: "registered", sessionId: "session-resume", turnId: "turn-resume",
    agentRole: "root", depth: 0, eventKind: "user", eventAt: NOW,
    modelId: "capture-model", embeddingDimension: 1024, originProvider: "provider-local",
    destinationId: qdrantDestination.id, status: "active", redactionStatus: "redacted",
    secretScan: "passed", text: "safe [token redacted]",
    vector: Array.from({ length: 1024 }, () => 0.25),
    sessionSequence: 1,
  };
  const value = { ...base, ...overrides } as EpisodeRecord;
  // Episode hash commits the vector; recompute it WITH the vector present.
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}

function emptyControl(overrides: Partial<ControlRecord> = {}): ControlRecord {
  const base: ControlRecord = {
    ownerHost: OWNER, schemaRevision: 1, createdAt: NOW, privacyEpoch: 0,
    processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control",
    id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null,
    coordinationPolicyEpoch: coordination.policyEpoch, coordinationPolicyHash: coordination.policyHash,
    state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending",
  };
  const value = { ...base, ...overrides } as ControlRecord;
  return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord;
}

type Fault = "drop-current-once" | "drop-observation-once" | "drop-current" | "drop-observation";

interface WirePoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] }; }

/**
 * Backend harness with fault injection that mirrors the temporal.test.ts
 * pattern. The scroll endpoint is parameterised so each test can drive its
 * own scroll behaviour WITHOUT relying on empty pages or trivial mocks.
 */
interface BackendHarness {
  points: Map<string, WirePoint>;
  options: QdrantClientOptions;
  scrollRequests: number;
  scrollOffsets: string[];
  setFault: (next: Fault | undefined) => void;
  setScrollHandler: (handler: ((offset: string | undefined) => { ids: readonly string[]; next?: string }) | undefined) => void;
}

function backendWithControl(seed: WirePoint[] = []): BackendHarness {
  const points = new Map<string, WirePoint>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) as Record<string, unknown> }]]);
  for (const point of seed) points.set(point.id, point);
  let activeFault: Fault | undefined;
  let onceUsed = false;
  let scrollHandler: ((offset: string | undefined) => { ids: readonly string[]; next?: string }) | undefined;
  const state: BackendHarness = {
    points, options: undefined as unknown as QdrantClientOptions,
    scrollRequests: 0, scrollOffsets: [], setFault: (next) => { activeFault = next; onceUsed = false; },
    setScrollHandler: (handler) => { scrollHandler = handler; },
  };
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input);
    const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: WirePoint[]; offset?: string | null; limit?: number; update_mode?: string; update_filter?: { must: Array<{ key: string; match?: { value?: unknown }; is_null?: { key: string }; range?: { gt?: string; lte?: string } }> } };
    if (url.includes("/points/retrieve")) {
      const result: WirePoint[] = [];
      for (const id of body?.ids ?? []) {
        const stored = points.get(id);
        if (stored === undefined) continue;
        const point: WirePoint = { id: stored.id, payload: { ...stored.payload }, ...(stored.vector === undefined ? {} : { vector: { semantic: [...stored.vector.semantic] } }) };
        const type = point.payload.record_type;
        const isObservation = type === "curated_memory";
        const isCurrent = type === "curated_current";
        const faultHit = (activeFault === "drop-current" || activeFault === "drop-current-once") && isCurrent
          || (activeFault === "drop-observation" || activeFault === "drop-observation-once") && isObservation
          || activeFault === "drop-current-once" && isCurrent && !onceUsed
          || activeFault === "drop-observation-once" && isObservation && !onceUsed;
        if (faultHit) {
          if (activeFault === "drop-current-once" || activeFault === "drop-observation-once") onceUsed = true;
          if (activeFault?.startsWith("drop-observation") || activeFault?.startsWith("drop-current")) delete point.vector;
        }
        result.push(point);
      }
      return json({ result, status: "ok" });
    }
    if (url.includes("/points/scroll")) {
      state.scrollRequests += 1;
      const offset = body?.offset === null || body?.offset === undefined ? undefined : (body.offset as string);
      state.scrollOffsets.push(offset ?? "(undefined)");
      if (scrollHandler !== undefined) {
        const custom = scrollHandler(offset);
        const page = custom.ids.map((id) => {
          const stored = points.get(id);
          if (stored === undefined) throw new Error(`missing scroll fixture ${id}`);
          return { id, payload: { ...stored.payload } };
        });
        return json({ result: { points: page, ...(custom.next === undefined ? {} : { next_page_offset: custom.next }) }, status: "ok" });
      }
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
        if (body?.update_mode === "insert_only" && points.has(p.id)) continue;
        points.set(p.id, { id: p.id, payload: { ...p.payload }, ...(p.vector === undefined ? {} : { vector: { semantic: [...p.vector.semantic] } }) });
      }
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  };
  vi.stubGlobal("fetch", fetchImpl);
  state.options = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: OWNER, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
  return state;
}

function seedEpisodePoint(backend: BackendHarness, record: EpisodeRecord): void {
  const payload = recordPayload(record) as Record<string, unknown>;
  backend.points.set(record.id, { id: record.id, payload, vector: { semantic: [...(record.vector ?? Array.from({ length: 1024 }, () => 0.25))] } });
}

/** Seed a canonical-valid JobRecord point directly. Used by scroll tests so
 * the worker discovers real, physical job IDs (not just empty pages). */
function seedJobPoint(backend: BackendHarness, job: JobRecord): void {
  const point = recordPayload(job) as Record<string, unknown>;
  backend.points.set(job.id, { id: job.id, payload: point });
}

function bindEmbedding(): BoundEmbeddingDestination {
  const qdrantFetch = globalThis.fetch;
  vi.stubGlobal("fetch", async (input, init) => {
    if (String(input).includes("/embeddings")) {
      return json({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] });
    }
    if (qdrantFetch === undefined) throw new Error("transport unavailable");
    return qdrantFetch(input, init);
  });
  const client = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return bindEmbeddingDestination(factory, embeddingDestination);
}

function runtime(backend: BackendHarness): { store: ProductionCoordinationStore; embedding: BoundEmbeddingDestination } {
  const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return { store: bundle.store, embedding: bindEmbedding() };
}

function rootManager(): SessionManager { return SessionManager.inMemory(); }

function baseOptions(input: {
  store: ProductionCoordinationStore;
  embedding: BoundEmbeddingDestination;
  membership: readonly string[];
  producerPolicies: readonly ProcessingPolicy[];
  workerPolicy: ProcessingPolicy;
  llmText?: string;
  llmEvidence?: string;
  llmCallCounter?: { count: number };
  scan?: (text: string) => "passed" | "rejected" | "error";
  createdAt?: () => string;
}): RootCurationLifecycleInput {
  const evidenceId = input.llmEvidence ?? input.membership[0] ?? EP_A;
  const llmText = input.llmText ?? `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${evidenceId}"]}]}`;
  return {
    host: OWNER, store: input.store, nodeId: "node-resume",
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
    ...(input.scan === undefined ? {} : { scan: input.scan }),
    createdAt: input.createdAt ?? (() => NOW),
    env: {},
  };
}

async function runOnce(options: RootCurationLifecycleInput): Promise<CurationRunResult> {
  return runCurationFromLifecycle(rootManager(), options);
}

/** Construct a canonical-valid JobRecord whose ID matches the worker's jobIdFor formula. */
function jobFixture(id: string, membership: readonly string[], policyIntersectionId: string): JobRecord {
  void id;
  const record: JobRecord = {
    recordType: "job", id: "pending", ownerHost: OWNER, schemaRevision: 1, createdAt: NOW, privacyEpoch: 0,
    processingPolicyId: policyIntersectionId, expiresAt: null,
    policyId: policyIntersectionId, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch,
    membership: [...membership], extractorRevision: EXTRACTOR,
    coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch,
    contentHash: "pending",
  };
  const computedId = jobId(OWNER, membership, coordination.policyHash, EXTRACTOR, coordination.policyEpoch, policyIntersectionId, 0);
  const withId = { ...record, id: computedId } as JobRecord;
  return { ...withId, contentHash: canonicalRecordHash(withId) } as JobRecord;
}

// ---------------------------------------------------------------------------
// Scroll isolation tests — every assertion forces real, non-empty pages with
// physical IDs that match the seeded job points.
// ---------------------------------------------------------------------------
describe("Task 9 accepted proposal envelope snapshots", () => {
  it("rejects proxies and accessors without invocation and freezes an owned clone", () => {
    const envelope = { schema: "curation_proposal_v1", items: [], provenance: { host: OWNER, providerId: "provider-local", modelId: "provider-model", destinationId: "llm:local", policyId: "policy-local", policyEpoch: 1, policyHash: "policy-local", promptRevision: "curation-prompt-v1", invokedAt: NOW } };
    let accessorGets = 0;
    const accessorEnvelope = { ...envelope, provenance: { ...envelope.provenance } };
    Object.defineProperty(accessorEnvelope.provenance, "providerId", { enumerable: true, configurable: true, get() { accessorGets += 1; return "provider-local"; } });
    expect(parseCurationProposalEnvelope(accessorEnvelope)).toBeNull();
    expect(accessorGets).toBe(0);
    let proxyGets = 0;
    const proxy = new Proxy(envelope, { get(target, key) { proxyGets += 1; return Reflect.get(target, key); } });
    expect(parseCurationProposalEnvelope(proxy)).toBeNull();
    expect(proxyGets).toBe(0);
    const parsed = parseCurationProposalEnvelope(envelope);
    expect(parsed).not.toBeNull();
    expect(Object.isFrozen(parsed)).toBe(true);
    expect(Object.isFrozen(parsed!.provenance)).toBe(true);
    envelope.provenance.modelId = "mutated";
    expect(parsed!.provenance.modelId).toBe("provider-model");
  });
});

describe("Task 9 scrollJobs isolation", () => {
  it("fails closed when a seventeenth non-empty canonical job page exists", async () => {
    const intersection = policy({ policyRevision: "scroll-page17" });
    const jobs = Array.from({ length: 34 }, (_, index) => {
      const member = `00000000-0000-5000-8000-${(0x100 + index).toString(16).padStart(12, "0")}`;
      return jobFixture("ignored", [member], intersection.id);
    }).sort((left, right) => left.id.localeCompare(right.id));
    const backend = backendWithControl(jobs.map((job) => ({ id: job.id, payload: recordPayload(job) as Record<string, unknown> })));
    const ids = jobs.map((job) => job.id);
    backend.setScrollHandler((offset) => {
      const start = offset === undefined ? 0 : ids.indexOf(offset) + 1;
      const page = ids.slice(start, start + 2);
      const next = start + 2 < ids.length ? page.at(-1) : undefined;
      return { ids: page, ...(next === undefined ? {} : { next }) };
    });
    const producer = policy({ policyRevision: "scroll-page17-producer" });
    seedEpisodePoint(backend, episode({ id: EP_A, processingPolicyId: producer.id }));
    const rt = runtime(backend);
    const result = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_A], producerPolicies: [producer], workerPolicy: policy({ policyRevision: "worker-scroll-page17" }) }));
    expect(result).toMatchObject({ state: "pending", reason: "job-discovery-unavailable" });
    expect(backend.scrollRequests).toBe(16);
    expect(new Set(backend.scrollOffsets).size).toBe(backend.scrollOffsets.length);
  });

  it("rejects a cycle of non-empty canonical job pages", async () => {
    const intersection = policy({ policyRevision: "scroll-cycle" });
    const jobs = [EP_A, EP_B, EP_C, EP_D].map((member) => jobFixture("ignored", [member], intersection.id)).sort((a, b) => a.id.localeCompare(b.id));
    const backend = backendWithControl(jobs.map((job) => ({ id: job.id, payload: recordPayload(job) as Record<string, unknown> })));
    const first = jobs.slice(0, 2).map((job) => job.id);
    const second = jobs.slice(2).map((job) => job.id);
    backend.setScrollHandler((offset) => offset === first.at(-1)
      ? { ids: second, next: second.at(-1)! }
      : { ids: first, next: first.at(-1)! });
    const producer = policy({ policyRevision: "scroll-cycle-producer" });
    seedEpisodePoint(backend, episode({ id: EP_A, processingPolicyId: producer.id }));
    const rt = runtime(backend);
    const result = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_A], producerPolicies: [producer], workerPolicy: policy({ policyRevision: "worker-scroll-cycle" }) }));
    expect(result).toMatchObject({ state: "pending", reason: "job-discovery-unavailable" });
    expect(backend.scrollRequests).toBeGreaterThan(1);
    expect(backend.scrollRequests).toBeLessThanOrEqual(16);
  });

  it("rejects duplicated canonical job IDs across pages", async () => {
    const intersection = policy({ policyRevision: "scroll-duplicate" });
    const jobs = [EP_A, EP_B].map((member) => jobFixture("ignored", [member], intersection.id)).sort((a, b) => a.id.localeCompare(b.id));
    const backend = backendWithControl(jobs.map((job) => ({ id: job.id, payload: recordPayload(job) as Record<string, unknown> })));
    const ids = jobs.map((job) => job.id);
    backend.setScrollHandler((offset) => offset === undefined ? { ids, next: ids.at(-1)! } : { ids });
    const producer = policy({ policyRevision: "scroll-duplicate-producer" });
    seedEpisodePoint(backend, episode({ id: EP_A, processingPolicyId: producer.id }));
    const rt = runtime(backend);
    const result = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_A], producerPolicies: [producer], workerPolicy: policy({ policyRevision: "worker-scroll-duplicate" }) }));
    expect(result).toMatchObject({ state: "pending", reason: "job-discovery-unavailable" });
    expect(backend.scrollRequests).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// High-level genuine API tests — every assertion drives runCurationFromLifecycle
// (the only public issuer) and the branded ProductionCoordinationStore. No
// RootWorkerContext, no LeaseAuthority, no symbol forging.
// ---------------------------------------------------------------------------
describe("Task 9 resume hardening — high-level genuine APIs", () => {
  it("accepted subset beats a later full uncovered candidate without a second LLM", async () => {
    const producer = policy({ policyRevision: "producer-resume-accepted-subset" });
    const ep1 = episode({ id: EP_A, processingPolicyId: producer.id });
    const ep2 = episode({ id: EP_B, processingPolicyId: producer.id, sourceEntryId: "entry-B" });
    const backend = backendWithControl([]);
    seedEpisodePoint(backend, ep1); seedEpisodePoint(backend, ep2);
    const rt = runtime(backend);
    const workerPolicy = policy({ policyRevision: "worker-resume-accepted-subset" });
    const counter = { count: 0 };
    backend.setFault("drop-current-once");
    const first = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_A], producerPolicies: [producer], workerPolicy, llmEvidence: EP_A, llmCallCounter: counter }));
    expect(first.state).toBe("pending");
    expect(counter.count).toBe(1);
    expect(await readLease(rt.store, first.jobId!)).toMatchObject({ state: "released", acceptedProposalId: expect.any(String), acceptedManifestHash: expect.any(String) });
    backend.setFault(undefined);
    const second = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B], producerPolicies: [producer], workerPolicy, llmEvidence: EP_A, llmCallCounter: counter }));
    expect(second).toMatchObject({ state: "completed", jobId: first.jobId, observations: 1 });
    expect(counter.count).toBe(1);
    expect((await readJob(rt.store, second.jobId!))?.membership).toEqual([EP_A]);
  });

  it("released-with-pair retry never invokes llm/maxOutputTokens/timeoutMs/scan/createdAt getters", async () => {
    // Drop the curated_current vector on the first attempt so materializeCuration
    // fails AFTER acceptance. The lease is released with the pair persisted.
    // The retry resumes via the pair without firing the LLM again.
    // During the resume path, the worker MUST NOT touch any of the listed
    // fresh-call getters.
    const producerB = policy({ policyRevision: "producer-resume-released-pair" });
    const ep = episode({ id: EP_C, processingPolicyId: producerB.id });
    const backend = backendWithControl([]);
    seedEpisodePoint(backend, ep);
    const rt = runtime(backend);
    const workerPolicy = policy({ policyRevision: "worker-resume-released-pair" });
    // Fault the first run so the current point drops on its readback.
    backend.setFault("drop-current-once");
    const counter = { count: 0 };
    const first = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_C], producerPolicies: [producerB], workerPolicy, llmEvidence: EP_C, llmCallCounter: counter }));
    // Materialization fails; lease should be released but with the pair.
    expect(first.state).toBe("pending");
    expect(counter.count).toBe(1);
    const firstLease = await readLease(rt.store, first.jobId!);
    expect(firstLease?.state).toBe("released");
    expect(firstLease?.acceptedProposalId).not.toBeNull();
    expect(firstLease?.acceptedManifestHash).not.toBeNull();
    backend.setFault(undefined);
    // For the retry, replace every fresh-call seam with an explosive getter.
    const getterCounts = { llm: 0, maxOutputTokens: 0, timeoutMs: 0, scan: 0, createdAt: 0 };
    const opts = baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_C], producerPolicies: [producerB], workerPolicy, llmEvidence: EP_C });
    Object.defineProperty(opts, "llm", { configurable: true, get() { getterCounts.llm += 1; throw new Error("llm getter fired"); } });
    Object.defineProperty(opts, "maxOutputTokens", { configurable: true, get() { getterCounts.maxOutputTokens += 1; throw new Error("maxOutputTokens getter fired"); } });
    Object.defineProperty(opts, "timeoutMs", { configurable: true, get() { getterCounts.timeoutMs += 1; throw new Error("timeoutMs getter fired"); } });
    Object.defineProperty(opts, "scan", { configurable: true, get() { getterCounts.scan += 1; throw new Error("scan getter fired"); } });
    Object.defineProperty(opts, "createdAt", { configurable: true, get() { getterCounts.createdAt += 1; throw new Error("createdAt getter fired"); } });
    counter.count = 0;
    const retry = await runOnce(opts);
    // Resume path consumed the pair; LLM was not fired AND no fresh-call
    // getter was invoked.
    expect(retry.state).toBe("completed");
    expect(counter.count).toBe(0);
    expect(getterCounts.llm).toBe(0);
    expect(getterCounts.maxOutputTokens).toBe(0);
    expect(getterCounts.timeoutMs).toBe(0);
    expect(getterCounts.scan).toBe(0);
    expect(getterCounts.createdAt).toBe(0);
  });

  it("blocked accepted tombstoned does not starve accepted clean another producer", async () => {
    // Producer A: EP_A is fully tombstoned; no compatible plan is actionable.
    // Producer B: EP_B is clean and accepted. The worker must process producer
    // B without starving on producer A.
    const producerA = policy({ policyRevision: "producer-stuck-tombstoned" });
    const producerB = policy({ policyRevision: "producer-clean-other" });
    const epA = episode({ id: EP_A, processingPolicyId: producerA.id });
    const epB = episode({ id: EP_B, processingPolicyId: producerB.id, sourceEntryId: "entry-B" });
    const backend = backendWithControl([]);
    seedEpisodePoint(backend, epA);
    seedEpisodePoint(backend, epB);
    const rt = runtime(backend);
    const workerPolicy = policy({ policyRevision: "worker-resume-no-starve" });
    // Tombstone EP_A so producer-A cannot be curated.
    await createTombstone(rt.store, {
      ownerHost: OWNER, scope: "occurrence", targetId: EP_A, targetKind: "episode",
      createdAt: NOW, privacyEpoch: 0, processingPolicyId: producerA.id,
    });
    const counter = { count: 0 };
    const result = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_A, EP_B], producerPolicies: [producerA, producerB], workerPolicy, llmEvidence: EP_B, llmCallCounter: counter }));
    expect(result.state).toBe("completed");
    // The LLM was fired exactly once (for producer-B only); producer-A's
    // tombstoned membership was blocked and did not starve producer-B.
    expect(counter.count).toBe(1);
    expect(result.jobId).toBeDefined();
    const job = await readJob(rt.store, result.jobId!);
    expect(job?.membership).toEqual([EP_B]);
  });

  it("early released-no-pair does not win later accepted global", async () => {
    // First run: an LLM-only cycle that fails AFTER materialisation but
    // before acceptance (no accepted pair persists in the lease). This is the
    // "released-no-pair" durable work identity.
    // Second run: the same membership now accepts via the existing lease
    // (which may or may not have a pair). The retry resume path must consume
    // the LATEST lease state, not pick a stale released-no-pair.
    const producer = policy({ policyRevision: "producer-resume-ordering" });
    const ep = episode({ id: EP_C, processingPolicyId: producer.id });
    const backend = backendWithControl([]);
    seedEpisodePoint(backend, ep);
    const rt = runtime(backend);
    const workerPolicy = policy({ policyRevision: "worker-resume-ordering" });
    // First run: fail the LLM to leave a leased (later released) job with no
    // accepted pair. (A throw inside complete.complete triggers fail() before
    // acceptance, so the lease is in "leased" or "released" without a pair.)
    const failingLlm = { count: 0 };
    const failOnce = async () => {
      failingLlm.count += 1;
      throw new Error("synthetic LLM failure");
    };
    const firstOpts = baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_C], producerPolicies: [producer], workerPolicy, llmEvidence: EP_C });
    firstOpts.llm.modelRegistry.complete = failOnce;
    const first = await runOnce(firstOpts);
    expect(first.state).toBe("pending");
    expect(failingLlm.count).toBe(1);
    const earlyLease = await readLease(rt.store, first.jobId!);
    expect(earlyLease?.acceptedProposalId).toBeNull();
    expect(earlyLease?.acceptedManifestHash).toBeNull();
    // Second run: a working LLM completes the cycle. The accepted pair is
    // persisted; no second LLM "later" is required because the lease has
    // already been materialised. If the early released-no-pair had won
    // globally, the second run would re-call the LLM. With total1, the LLM
    // is called exactly once across the full retry sequence.
    const counter = { count: 0 };
    const second = await runOnce(baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_C], producerPolicies: [producer], workerPolicy, llmEvidence: EP_C, llmCallCounter: counter }));
    expect(second.state).toBe("completed");
    expect(counter.count).toBe(1);
  });

  it("rejects a model mutation that occurs during completion before persistence", async () => {
    const producer = policy({ policyRevision: "producer-mutation-during" });
    const ep = episode({ id: EP_D, processingPolicyId: producer.id });
    const backend = backendWithControl([]);
    seedEpisodePoint(backend, ep);
    const rt = runtime(backend);
    const counter = { count: 0 };
    const opts = baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_D], producerPolicies: [producer], workerPolicy: policy({ policyRevision: "worker-mutation-during" }), llmEvidence: EP_D, llmCallCounter: counter });
    const mutableModel = opts.llm.memoryModel as unknown as { id: string };
    const complete = opts.llm.modelRegistry.complete!;
    opts.llm.modelRegistry.complete = async (...args: Parameters<typeof complete>) => {
      const response = await complete(...args);
      mutableModel.id = "mutated-model-id";
      return response;
    };
    const result = await runOnce(opts);
    expect(result).toMatchObject({ state: "pending", reason: "model-changed" });
    expect(counter.count).toBe(1);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "proposal")).toHaveLength(0);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory" || point.payload.record_type === "curated_current")).toHaveLength(0);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "coverage")).toHaveLength(0);
  });

  it("unknown API_KEY accessors and symbol keys on options are never invoked", async () => {
    // The lifecycle must not enumerate unknown options, must not invoke getters
    // it does not own, and must not read symbol-keyed properties.
    const producer = policy({ policyRevision: "producer-unknown-accessors" });
    const ep = episode({ id: EP_E, processingPolicyId: producer.id });
    const backend = backendWithControl([]);
    seedEpisodePoint(backend, ep);
    const rt = runtime(backend);
    const workerPolicy = policy({ policyRevision: "worker-unknown-accessors" });
    const opts = baseOptions({ store: rt.store, embedding: rt.embedding, membership: [EP_E], producerPolicies: [producer], workerPolicy, llmEvidence: EP_E });
    const apiKeyHits = { value: 0 };
    const authHits = { value: 0 };
    Object.defineProperty(opts, "API_KEY", { configurable: true, get() { apiKeyHits.value += 1; throw new Error("API_KEY accessor fired"); }, enumerable: false });
    Object.defineProperty(opts, "authorization", { configurable: true, get() { authHits.value += 1; throw new Error("authorization accessor fired"); }, enumerable: false });
    const secretSymbol = Symbol.for("secret-token");
    Object.defineProperty(opts, secretSymbol, { configurable: true, get() { throw new Error("secret symbol accessor fired"); }, enumerable: false });
    const result = await runOnce(opts);
    // The high-level API succeeded; no unknown/symbol getter was touched.
    expect(result.state).toBe("completed");
    expect(apiKeyHits.value).toBe(0);
    expect(authHits.value).toBe(0);
    // Stringify must not leak the symbol or the unknown keys.
    const stringified = JSON.stringify(opts, (_, value) => (typeof value === "symbol" ? undefined : value));
    expect(stringified).not.toContain("API_KEY");
    expect(stringified).not.toContain("authorization");
  });
});
