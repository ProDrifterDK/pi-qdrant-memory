import { afterEach, describe, expect, it, vi } from "vitest";
import { coordinationRecordFromPayload, createQdrantSafeBundle, ProductionCoordinationStore, recordPayload } from "../../src/qdrant/write.js";
import { canonicalRecordHash, type ControlRecord, type CuratedMemoryRecord, type EpisodeRecord, type JobRecord } from "../../src/domain/records.js";
import { COLLECTION_CONTROL_ID, controlPayload } from "../../src/qdrant/schema.js";
import { canonicalStringify } from "../../src/domain/canonical.js";
import { contentId, coverageId, observationId, stateKey } from "../../src/domain/ids.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { compareEffectiveOrders, deriveEffectiveOrder, foldHistorySegments } from "../../src/curation/temporal.js";
import { projectConflictAggregate } from "../../src/curation/projection.js";
import { runCurationFromLifecycle, type RootCurationLifecycleInput } from "../../src/coordination/root.js";
import { readLease } from "../../src/coordination/leases.js";
import { createTombstone } from "../../src/coordination/tombstones.js";
import { readJob } from "../../src/coordination/jobs.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, type BoundEmbeddingDestination } from "../../src/clients/embeddings.js";
import type { AuthorizedDestination } from "../../src/types.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import { SessionManager } from "@earendil-works/pi-coding-agent";

const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination: AuthorizedDestination = { id: "embed:local", residency: "local", dataUse: "memory" };
const llmDestination: AuthorizedDestination = { id: "llm:local", residency: "local", dataUse: "memory" };
const coordination = { policyHash: "coordination-policy-hash-v9", policyEpoch: 1 } as const;
const NOW = "2026-08-10T00:00:00.000Z";
const NOW_MS = Date.parse(NOW);
const OWNER = "pi" as const;
const EXTRACTOR = "extractor-9";
const EPISODE_1 = "00000000-0000-5000-8000-000000000001";
const EPISODE_2 = "00000000-0000-5000-8000-000000000002";
const providerBinding = { providerId: "provider-local", modelId: "provider-model", destinationId: "llm:local" };

type Fault = "drop-observation" | "alter-observation" | "drop-current" | "alter-current" | "drop-observation-once" | "drop-current-once" | "drop-coverage";
interface WirePoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] }; }

function json(value: unknown, status = 200): Response { return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } }); }
afterEach(() => { vi.unstubAllGlobals(); });

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const pending = {
    id: "pending", ownerHost: OWNER,
    destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null,
    residency: "local", dataUse: "memory", policyRevision: "v1", ...overrides,
  } satisfies ProcessingPolicy;
  return { ...pending, id: processingPolicyHash(pending) };
}

function episode(overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const base = {
    recordType: "episode" as const, id: EPISODE_1, ownerHost: OWNER, schemaRevision: 1 as const,
    createdAt: NOW, privacyEpoch: 0, processingPolicyId: policy().id, expiresAt: null,
    contentHash: "pending", sourceEntryId: "entry-1", host: OWNER, projectId: "project-1",
    projectIdentityKind: "registered" as const, sessionId: "session-1", turnId: "turn-1",
    agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: NOW,
    modelId: "capture-model", embeddingDimension: 1024, originProvider: "provider-local",
    destinationId: qdrantDestination.id, status: "active" as const, redactionStatus: "redacted" as const,
    secretScan: "passed" as const, text: "safe [token redacted]", vector: Array.from({ length: 1024 }, () => 0.25),
  } satisfies EpisodeRecord;
  const value = { ...base, ...overrides } as EpisodeRecord;
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}

function emptyControl(overrides: Partial<ControlRecord> = {}): ControlRecord {
  const base = {
    ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0,
    processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control" as const,
    id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null,
    coordinationPolicyEpoch: coordination.policyEpoch, coordinationPolicyHash: coordination.policyHash,
    state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending",
  };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord;
}

function backendWithControl(fault?: Fault, coverageGate?: { onFirst: () => void; wait: Promise<void> }, onCoverage?: () => void): { points: Map<string, WirePoint>; options: QdrantClientOptions; setFault: (next: Fault | undefined) => void } {
  let activeFault = fault;
  let coverageGateUsed = false;
  const points = new Map<string, WirePoint>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) }]]);
  let onceUsed = false;
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input);
    const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as {
      ids?: string[]; points?: WirePoint[]; update_mode?: string;
      update_filter?: { must: Array<{ key: string; match?: { value?: unknown; any?: unknown[] }; is_null?: { key: string }; range?: { gt?: string; lte?: string } }> };
    };
    if (url.includes("/points/retrieve")) {
      const result: WirePoint[] = [];
      for (const id of body?.ids ?? []) {
        const stored = points.get(id);
        if (stored === undefined) continue;
        const point: WirePoint = { id: stored.id, payload: { ...stored.payload }, ...(stored.vector === undefined ? {} : { vector: { semantic: [...stored.vector.semantic] } }) };
        const type = point.payload.record_type;
        const isObservation = type === "curated_memory";
        const isCurrent = type === "curated_current";
        const isCoverage = type === "coverage";
        if (activeFault === "drop-coverage" && isCoverage) continue;
        const shouldFault = (activeFault === "drop-observation" || activeFault === "alter-observation") && isObservation
          || (activeFault === "drop-current" || activeFault === "alter-current") && isCurrent
          || activeFault === "drop-observation-once" && isObservation && !onceUsed
          || activeFault === "drop-current-once" && isCurrent && !onceUsed;
        if (shouldFault) {
          if (activeFault === "drop-observation-once" || activeFault === "drop-current-once") onceUsed = true;
          if (activeFault === "drop-observation" || activeFault === "drop-current" || activeFault === "drop-observation-once" || activeFault === "drop-current-once") delete point.vector;
          else if (point.vector !== undefined) point.vector.semantic[0] = point.vector.semantic[0] + 1;
        }
        result.push(point);
      }
      return json({ result, status: "ok" });
    }
    if (url.includes("/points/scroll")) return json({ result: { points: [], next_page_offset: null }, status: "ok" });
    if (url.includes("/points?") && init.method === "PUT") {
      const current = body?.points?.[0] === undefined ? undefined : points.get(body.points[0]!.id)?.payload;
      const value = (key: string): unknown => current?.[key];
      const matches = (body?.update_filter?.must ?? []).every((condition) => {
        if (condition.is_null !== undefined) return value(condition.is_null.key) === null;
        if (condition.range?.gt !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) > Date.parse(condition.range.gt);
        if (condition.range?.lte !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) <= Date.parse(condition.range.lte);
        if (condition.match?.any !== undefined) return condition.match.any.includes(value(condition.key));
        return value(condition.key) === condition.match?.value;
      });
      if (body?.update_mode === "update_only" && !matches) return json({ result: { status: "acknowledged" }, status: "ok" });
      for (const incoming of body?.points ?? []) {
        if (incoming.payload.record_type === "coverage" && coverageGate !== undefined && !coverageGateUsed) {
          coverageGateUsed = true;
          coverageGate.onFirst();
          await coverageGate.wait;
        }
        if (body?.update_mode === "insert_only" && points.has(incoming.id)) continue;
        points.set(incoming.id, { id: incoming.id, payload: { ...incoming.payload }, ...(incoming.vector === undefined ? {} : { vector: { semantic: [...incoming.vector.semantic] } }) });
        if (incoming.payload.record_type === "coverage") onCoverage?.();
      }
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  };
  vi.stubGlobal("fetch", fetchImpl);
  return { points, setFault: (next) => { activeFault = next; }, options: { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: OWNER, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" } };
}

function seedEpisode(backend: { points: Map<string, WirePoint> }, record: EpisodeRecord): void {
  backend.points.set(record.id, { id: record.id, payload: recordPayload(record) as Record<string, unknown>, vector: { semantic: [...record.vector!] } });
}

function runtime(backend: { points: Map<string, WirePoint>; options: QdrantClientOptions }, embedImpl: (text: string) => Promise<readonly number[]> = async () => Array.from({ length: 1024 }, () => 0.25), binding: { readonly policyHash: string; readonly policyEpoch: number } = coordination): { store: ProductionCoordinationStore; embedding: BoundEmbeddingDestination } {
  const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: binding.policyHash, coordinationPolicyEpoch: binding.policyEpoch });
  const qdrantFetch = globalThis.fetch;
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    if (String(input).includes("/embeddings")) {
      const body = JSON.parse(String(init?.body ?? "{}")) as { input?: string };
      return json({ data: [{ embedding: await embedImpl(body.input ?? "") }] });
    }
    if (qdrantFetch === undefined) throw new Error("qdrant transport unavailable");
    return qdrantFetch(input, init);
  });
  const client = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client }), egressMode: "allowlist", coordinationPolicyHash: binding.policyHash, coordinationPolicyEpoch: binding.policyEpoch });
  return { store: bundle.store, embedding: bindEmbeddingDestination(factory, embeddingDestination) };
}

function curationOptions(input: { store: ProductionCoordinationStore; embedding: BoundEmbeddingDestination; membership: readonly string[]; producerPolicy: ProcessingPolicy; producerPolicies?: readonly ProcessingPolicy[]; workerPolicy?: ProcessingPolicy; llmText?: string; llmEvidence?: string; onLlm?: () => void; maxClockSkew?: number; extractorRevision?: string; scan?: (text: string) => "passed" | "rejected" | "error"; createdAt?: () => string }): RootCurationLifecycleInput {
  const workerPolicy = input.workerPolicy ?? policy({ policyRevision: "worker-v9" });
  return {
    host: OWNER, store: input.store, nodeId: "node-a", leaseMs: 30_000, maxClockSkewMs: input.maxClockSkew ?? 0, clock: () => NOW_MS,
    workerPolicy, extractorRevision: input.extractorRevision ?? EXTRACTOR, producerPolicies: input.producerPolicies ?? [input.producerPolicy], embedding: input.embedding,
    llm: {
      memoryModel: { id: "provider-model", provider: "provider-local", contextWindow: 1_000_000, maxTokens: 65_536 } as never,
      modelRegistry: { complete: async () => { input.onLlm?.(); return { content: [{ type: "text", text: input.llmText ?? `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${input.llmEvidence ?? EPISODE_1}"]}]}` }] }; } },
      llmDestination, llmDestinationBinding: providerBinding,
    },
    membership: [...input.membership], ...(input.scan === undefined ? {} : { scan: input.scan }), createdAt: input.createdAt ?? (() => NOW), env: {},
  };
}

async function runHighLevel(options: RootCurationLifecycleInput): Promise<Awaited<ReturnType<typeof runCurationFromLifecycle>>> {
  return runCurationFromLifecycle(SessionManager.inMemory(), options);
}

async function coverageFor(store: ProductionCoordinationStore, jobIdValue: string): Promise<readonly unknown[]> {
  const job = await readJob(store, jobIdValue);
  if (job === null) return [];
  const ids = job.membership.map((episodeId) => coverageId({ ownerHost: OWNER, episodeId, extractorRevision: job.extractorRevision, coordinationPolicyHash: job.coordinationPolicyHash, coordinationPolicyEpoch: job.coordinationPolicyEpoch, policyIntersectionId: job.policyId, privacyEpoch: job.privacyEpoch }));
  return store.readCoverage(ids);
}

describe("Task 9 temporal curation", () => {
  it("orders same-session conflict representatives by causal sequence before wall clock", () => {
    const base = {
      recordType: "curated_memory" as const, ownerHost: OWNER, schemaRevision: 1 as const,
      createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy", expiresAt: null,
      contentHash: "hash", stateKey: "state", effectiveAt: NOW, validFrom: NOW,
      validTo: null, text: "safe", confidence: 1, sourceEpisodeIds: [EPISODE_1],
      provenance: [EPISODE_1], coordinationPolicyHash: coordination.policyHash,
      coordinationPolicyEpoch: 1, vector: Array.from({ length: 1024 }, () => 0.25),
    };
    const sequenceOne = { ...base, id: "observation-a", observationId: "observation-a", contentId: "content-a", eventAt: "2026-08-10T01:00:00.000Z", effectiveOrder: { kind: "session" as const, sessionId: "session-1", sequence: 1, eventAt: "2026-08-10T01:00:00.000Z", episodeId: EPISODE_1, contentId: "content-a" } } as CuratedMemoryRecord;
    const sequenceTwo = { ...base, id: "observation-b", observationId: "observation-b", contentId: "content-b", eventAt: "2026-08-09T23:00:00.000Z", effectiveOrder: { kind: "session" as const, sessionId: "session-1", sequence: 2, eventAt: "2026-08-09T23:00:00.000Z", episodeId: EPISODE_2, contentId: "content-b" }, sourceEpisodeIds: [EPISODE_2], provenance: [EPISODE_2] } as CuratedMemoryRecord;
    const projected = projectConflictAggregate([sequenceOne, sequenceTwo]);
    const reversed = projectConflictAggregate([sequenceTwo, sequenceOne]);
    expect(projected.effectiveOrder).toEqual(sequenceTwo.effectiveOrder);
    expect(canonicalStringify(reversed)).toBe(canonicalStringify(projected));
  });

  it("derives durable same-session causal order and folds A->B->A history without mutating observations", () => {
    const first = episode({ sessionSequence: 3 });
    const second = episode({ id: EPISODE_2, sourceEntryId: "entry-2", sessionSequence: 7, eventAt: "2026-08-10T00:01:00.000Z" });
    const firstOrder = deriveEffectiveOrder([first], "content-a");
    const secondOrder = deriveEffectiveOrder([second], "content-b");
    expect(compareEffectiveOrders(firstOrder, secondOrder, 0)).toBe("before");
    expect(firstOrder).toMatchObject({ kind: "session", sessionId: "session-1", sequence: 3 });
    const make = (value: string, at: string, evidence: string): CuratedMemoryRecord => {
      const target = stateKey({ host: OWNER, scope: "project", projectId: "project-1", category: "fact", subject: "editor", predicate: "preferred" });
      const valueId = contentId(coordination.policyHash, target, value);
      const order = [at, evidence, valueId] as [string, string, string];
      const occurrenceId = observationId(coordination.policyEpoch, valueId, evidence, order);
      const record = { recordType: "curated_memory" as const, id: occurrenceId, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: policy().id, expiresAt: null, contentHash: "pending", contentId: valueId, observationId: occurrenceId, eventAt: at, effectiveAt: at, sourceEpisodeIds: [evidence], primaryEvidenceEpisodeId: evidence, effectiveOrder: order, stateKey: target, category: "fact", scope: "project", subject: "editor", predicate: "preferred", value, coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch } as CuratedMemoryRecord;
      record.contentHash = canonicalRecordHash(record);
      return record;
    };
    const observations = [make("vim", NOW, EPISODE_1), make("emacs", "2026-08-10T00:01:00.000Z", EPISODE_2), make("vim", "2026-08-10T00:02:00.000Z", EPISODE_1)];
    const before = canonicalStringify(observations);
    const target = stateKey({ host: OWNER, scope: "project", projectId: "project-1", category: "fact", subject: "editor", predicate: "preferred" });
    const a = contentId(coordination.policyHash, target, "vim");
    const b = contentId(coordination.policyHash, target, "emacs");
    const segments = foldHistorySegments(observations, 0);
    expect(segments.map((segment) => segment.contentId)).toEqual([a, b, a]);
    expect(segments).toHaveLength(3);
    expect(segments[0]!.validFrom).toBe(NOW);
    expect(segments[0]!.validTo).toBe(observations[1]!.eventAt);
    expect(segments[1]!.validTo).toBe(observations[2]!.eventAt);
    expect(segments[2]!.validTo).toBeNull();
    expect(segments[0]!.primaryEvidenceEpisodeId).toBe(EPISODE_1);
    expect(segments.flatMap((segment) => segment.observationIds)).toEqual(observations.map((observation) => observation.id));
    expect(canonicalStringify(observations)).toBe(before);
  });

  it("treats a single all-tombstoned compatible group as terminal without LLM, embedding, coverage, or retry", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-all-tombstone" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    let llmCalls = 0; let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer, onLlm: () => { llmCalls += 1; } });
    await createTombstone(runtimeValue.store, { ownerHost: OWNER, scope: "occurrence", targetId: ep.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: producer.id });
    const first = await runHighLevel(options);
    const second = await runHighLevel(options);
    expect(first).toMatchObject({ state: "completed", reason: "all-tombstoned", observations: 0 });
    expect(second).toMatchObject({ state: "completed", reason: "all-tombstoned", observations: 0 });
    expect(llmCalls).toBe(0); expect(embeddingCalls).toBe(0);
    expect([...backend.points.values()].some((point) => ["curated_memory", "curated_current", "coverage"].includes(String(point.payload.record_type)))).toBe(false);
  });

  it("skips an all-tombstoned first producer group and processes the next actionable group", async () => {
    const backend = backendWithControl();
    const producerA = policy({ policyRevision: "producer-terminal-first" });
    const producerB = policy({ policyRevision: "producer-actionable-second" });
    const epA = episode({ processingPolicyId: producerA.id });
    const epB = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producerB.id, eventAt: "2026-08-10T00:01:00.000Z" });
    seedEpisode(backend, epA); seedEpisode(backend, epB);
    const runtimeValue = runtime(backend);
    let llmCalls = 0;
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [epA.id, epB.id], producerPolicy: producerA, producerPolicies: [producerA, producerB], onLlm: () => { llmCalls += 1; }, llmEvidence: epB.id });
    await createTombstone(runtimeValue.store, { ownerHost: OWNER, scope: "occurrence", targetId: epA.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: producerA.id });
    const outcome = await runHighLevel(options);
    expect(outcome.state).toBe("completed"); expect(llmCalls).toBe(1);
    expect(await coverageFor(runtimeValue.store, outcome.jobId!)).toHaveLength(1);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory")).toHaveLength(1);
  });

  it("keeps partial tombstones as an exact membership barrier, including accepted work", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-partial-tombstone" });
    const ep1 = episode({ processingPolicyId: producer.id });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id });
    seedEpisode(backend, ep1); seedEpisode(backend, ep2);
    let llmCalls = 0; let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, onLlm: () => { llmCalls += 1; } });
    await createTombstone(runtimeValue.store, { ownerHost: OWNER, scope: "occurrence", targetId: ep1.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: producer.id });
    const outcome = await runHighLevel(options);
    expect(outcome.state).toBe("pending"); expect(llmCalls).toBe(0); expect(embeddingCalls).toBe(0);
    expect(outcome.jobId).toBeDefined();
    const partialJob = await readJob(runtimeValue.store, outcome.jobId!);
    expect(partialJob?.membership).toEqual([ep1.id, ep2.id]);
    expect([...backend.points.values()].filter((point) => ["curated_memory", "curated_current", "coverage"].includes(String(point.payload.record_type)))).toHaveLength(0);
  });

  it("keeps causal ordering session-local and treats legacy/cross-session ambiguity as unresolved", () => {
    const sameEarly = episode({ sessionSequence: 3, eventAt: "2026-08-10T00:02:00.000Z" });
    const sameLate = episode({ id: EPISODE_2, sourceEntryId: "entry-2", sessionSequence: 7, eventAt: "2026-08-10T00:01:00.000Z" });
    const sameA = deriveEffectiveOrder([sameEarly], "content-a");
    const sameB = deriveEffectiveOrder([sameLate], "content-b");
    expect(compareEffectiveOrders(sameA, sameB, 0)).toBe("before");
    const crossA = episode({ sessionId: "session-a", sessionSequence: 100, eventAt: "2026-08-10T00:00:00.000Z" });
    const crossB = episode({ id: EPISODE_2, sourceEntryId: "entry-2", sessionId: "session-b", sessionSequence: 1, eventAt: "2026-08-10T00:00:00.500Z" });
    expect(compareEffectiveOrders(deriveEffectiveOrder([crossA], "content-a"), deriveEffectiveOrder([crossB], "content-b"), 1000)).toBe("within_skew");
    expect(compareEffectiveOrders("session:3", "session:7", 0)).toBe("within_skew");
    const samePositionA = deriveEffectiveOrder([episode({ sessionSequence: 9 })], "content-a");
    const samePositionB = deriveEffectiveOrder([episode({ id: EPISODE_2, sourceEntryId: "entry-2", sessionSequence: 9 })], "content-b");
    expect(compareEffectiveOrders(samePositionA, samePositionB, 0)).toBe("within_skew");
  });

  it("materializes through the genuine SessionManager lifecycle with exact vectors, coverage, and explicit terminal completion", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-v9" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    const runtimeValue = runtime(backend);
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer }));
    expect(outcome.state).toBe("completed");
    expect(outcome.observations).toBe(1);
    const lease = await readLease(runtimeValue.store, outcome.jobId!);
    expect(lease?.state).toBe("completed");
    expect(await coverageFor(runtimeValue.store, outcome.jobId!)).toHaveLength(1);
    const curated = [...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory");
    const currents = [...backend.points.values()].filter((point) => point.payload.record_type === "curated_current");
    expect(curated).toHaveLength(1);
    expect(currents).toHaveLength(1);
    expect(curated[0]!.vector?.semantic).toHaveLength(1024);
    expect(currents[0]!.vector?.semantic).toHaveLength(1024);
    expect(curated[0]!.vector!.semantic.every(Number.isFinite)).toBe(true);
    expect(canonicalStringify(curated[0]!.vector)).toBe(canonicalStringify(currents[0]!.vector));
    const retry = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer }));
    expect(retry).toMatchObject({ state: "completed", reason: "already-covered", observations: 0 });
  });

  it("materializes the same evidence into a new policy-epoch view without mutating the retired view", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-policy-migration" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    const epochOne = runtime(backend);
    expect((await runHighLevel(curationOptions({ store: epochOne.store, embedding: epochOne.embedding, membership: [ep.id], producerPolicy: producer }))).state).toBe("completed");
    const oldObservation = [...backend.points.values()].find((point) => point.payload.record_type === "curated_memory")!;
    const oldCurrent = [...backend.points.values()].find((point) => point.payload.record_type === "curated_current")!;
    const oldObservationBytes = canonicalStringify(oldObservation);
    const oldCurrentBytes = canonicalStringify(oldCurrent);
    const epochTwoBinding = { policyHash: "coordination-policy-hash-v10", policyEpoch: 2 } as const;
    const nextControl = emptyControl({ version: 2, coordinationPolicyHash: epochTwoBinding.policyHash, coordinationPolicyEpoch: epochTwoBinding.policyEpoch });
    backend.points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(nextControl) });
    const epochTwo = runtime(backend, async () => Array.from({ length: 1024 }, () => 0.25), epochTwoBinding);
    const migrated = await runHighLevel(curationOptions({ store: epochTwo.store, embedding: epochTwo.embedding, membership: [ep.id], producerPolicy: producer }));
    expect(migrated.state).toBe("completed");
    const observations = [...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory");
    const currents = [...backend.points.values()].filter((point) => point.payload.record_type === "curated_current");
    expect(observations).toHaveLength(2);
    expect(currents).toHaveLength(2);
    expect(new Set(observations.map((point) => point.id)).size).toBe(2);
    expect(new Set(currents.map((point) => point.id)).size).toBe(2);
    expect(observations.map((point) => point.payload.coordination_policy_epoch).sort()).toEqual([1, 2]);
    expect(currents.map((point) => point.payload.coordination_policy_epoch).sort()).toEqual([1, 2]);
    expect(canonicalStringify(backend.points.get(oldObservation.id))).toBe(oldObservationBytes);
    expect(canonicalStringify(backend.points.get(oldCurrent.id))).toBe(oldCurrentBytes);
  });

  it("preserves an explicit null curated value through materialization and canonical persistence", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-null" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    const runtimeValue = runtime(backend);
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer, llmText: `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":null,"evidence":["${EPISODE_1}"]}]}` }));
    expect(outcome.state).toBe("completed");
    const observation = [...backend.points.values()].find((point) => point.payload.record_type === "curated_memory");
    expect(observation?.payload.value).toBeNull();
    expect(observation?.payload.content_id).toBe(contentId(coordination.policyHash, stateKey({ host: OWNER, scope: "project", projectId: "project-1", category: "fact", subject: "editor", predicate: "preferred" }), null));
  });

  it("creates a conflict for same-session causal position with different content and completes without choosing an arrival winner", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-same-position-conflict" });
    const ep1 = episode({ processingPolicyId: producer.id, sessionSequence: 5 });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id, sessionSequence: 5 });
    seedEpisode(backend, ep1); seedEpisode(backend, ep2);
    const runtimeValue = runtime(backend);
    const llmText = `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${ep1.id}"]},{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"emacs","evidence":["${ep2.id}"]}]}`;
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, llmText }));
    expect(outcome.state).toBe("completed");
    const manifests = [...backend.points.values()].filter((point) => point.payload.record_type === "conflict_manifest");
    expect(manifests).toHaveLength(1);
    expect((manifests[0]!.payload.members as unknown[]).length).toBe(2);
    const currents = [...backend.points.values()].filter((point) => point.payload.record_type === "curated_current");
    expect(currents).toHaveLength(1);
    expect(currents[0]!.payload.resolution).toBe("conflict");
    expect(currents[0]!.payload.content_id).toBeUndefined();
    expect(currents[0]!.payload.observation_id).toBeUndefined();
    expect(currents[0]!.vector).toBeUndefined();
    expect(await coverageFor(runtimeValue.store, outcome.jobId!)).toHaveLength(2);
  });

  it("completes an older accepted job after a strictly later cross-policy current wins", async () => {
    const backend = backendWithControl();
    const producerA = policy({ policyRevision: "producer-cross-policy-old" });
    const producerB = policy({ policyRevision: "producer-cross-policy-new" });
    const epA = episode({ processingPolicyId: producerA.id });
    const epB = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producerB.id, eventAt: "2026-08-10T00:01:00.000Z" });
    seedEpisode(backend, epA); seedEpisode(backend, epB);
    const runtimeValue = runtime(backend);
    backend.setFault("drop-current-once");
    let oldLlmCalls = 0;
    const oldFirst = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [epA.id], producerPolicy: producerA, onLlm: () => { oldLlmCalls += 1; } }));
    expect(oldFirst.state).toBe("pending");
    expect(await readLease(runtimeValue.store, oldFirst.jobId!)).toMatchObject({ state: "released", acceptedProposalId: expect.any(String) });
    backend.setFault(undefined);
    const newText = `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"emacs","evidence":["${epB.id}"]}]}`;
    expect((await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [epB.id], producerPolicy: producerB, llmText: newText, llmEvidence: epB.id }))).state).toBe("completed");
    const currentBeforeRetry = [...backend.points.values()].find((point) => point.payload.record_type === "curated_current")!;
    const oldRetry = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [epA.id], producerPolicy: producerA, onLlm: () => { oldLlmCalls += 1; } }));
    expect(oldRetry).toMatchObject({ state: "completed", jobId: oldFirst.jobId, observations: 1 });
    expect(oldLlmCalls).toBe(1);
    const currentAfterRetry = [...backend.points.values()].find((point) => point.payload.record_type === "curated_current")!;
    expect(currentAfterRetry.payload.content_hash).toBe(currentBeforeRetry.payload.content_hash);
    expect(currentAfterRetry.payload.processing_policy_id).toBe(currentBeforeRetry.payload.processing_policy_id);
  });

  it("completes a conflict whose canonical member was materialized by an earlier job", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-cross-job-conflict" });
    const ep1 = episode({ processingPolicyId: producer.id });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id, createdAt: "2026-08-10T00:00:01.000Z" });
    seedEpisode(backend, ep1); seedEpisode(backend, ep2);
    const runtimeValue = runtime(backend);
    const first = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id], producerPolicy: producer, llmEvidence: ep1.id, llmText: `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${ep1.id}"]}]}` }));
    expect(first.state).toBe("completed");
    const second = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, llmEvidence: ep2.id, llmText: `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"emacs","evidence":["${ep2.id}"]}]}` }));
    expect(second).toMatchObject({ state: "completed" });
    expect((await readJob(runtimeValue.store, second.jobId!))?.membership).toEqual([ep2.id]);
    const manifests = [...backend.points.values()].filter((point) => point.payload.record_type === "conflict_manifest");
    const latest = manifests.sort((left, right) => String(left.id).localeCompare(String(right.id))).at(-1);
    expect(latest?.payload.members).toHaveLength(2);
    expect(await coverageFor(runtimeValue.store, second.jobId!)).toHaveLength(1);
  });

  it("completes a later same-content occurrence without conflict version churn", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-conflict-same-content-retry" });
    const ep1 = episode({ processingPolicyId: producer.id, sessionSequence: 7 });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id, sessionSequence: 7 });
    const ep3 = episode({ id: "00000000-0000-5000-8000-000000000003", sourceEntryId: "entry-3", processingPolicyId: producer.id, sessionSequence: 7 });
    for (const entry of [ep1, ep2, ep3]) seedEpisode(backend, entry);
    const runtimeValue = runtime(backend);
    const firstText = `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${ep1.id}"]},{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"emacs","evidence":["${ep2.id}"]}]}`;
    expect((await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, llmText: firstText }))).state).toBe("completed");
    const currentBefore = [...backend.points.values()].find((point) => point.payload.record_type === "curated_current")!;
    const manifestCount = [...backend.points.values()].filter((point) => point.payload.record_type === "conflict_manifest").length;
    const retryText = `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${ep3.id}"]}]}`;
    const retry = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id, ep3.id].sort(), producerPolicy: producer, llmText: retryText, llmEvidence: ep3.id }));
    expect(retry).toMatchObject({ state: "completed", observations: 1 });
    const currentAfter = [...backend.points.values()].find((point) => point.payload.record_type === "curated_current")!;
    expect(currentAfter.payload.version).toBe(currentBefore.payload.version);
    expect(currentAfter.payload.content_hash).toBe(currentBefore.payload.content_hash);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "conflict_manifest")).toHaveLength(manifestCount);
    expect(await coverageFor(runtimeValue.store, retry.jobId!)).toHaveLength(1);
  });

  it("grows a nine-member within-skew conflict manifest and still writes coverage/completes", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-nine-conflict" });
    const episodes = Array.from({ length: 9 }, (_, index) => episode({ id: `00000000-0000-5000-8000-0000000000${String(index + 1).padStart(2, "0")}`, sourceEntryId: `entry-${index + 1}`, processingPolicyId: producer.id, eventAt: `2026-08-10T00:00:00.${String(index).padStart(3, "0")}Z` }));
    for (const entry of episodes) seedEpisode(backend, entry);
    const runtimeValue = runtime(backend);
    const items = episodes.map((entry, index) => ({ category: "fact", scope: "project", subject: "editor", predicate: "preferred", value: `value-${index}`, evidence: [entry.id] }));
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: episodes.map((entry) => entry.id).sort(), producerPolicy: producer, maxClockSkew: 1000, llmText: JSON.stringify({ items }) }));
    expect(outcome.state).toBe("completed");
    const manifests = [...backend.points.values()].filter((point) => point.payload.record_type === "conflict_manifest");
    expect(manifests.length).toBeGreaterThanOrEqual(1);
    expect(await coverageFor(runtimeValue.store, outcome.jobId!)).toHaveLength(9);
  });

  it("processes each compatible producer group instead of starving later groups, then returns already covered", async () => {
    const backend = backendWithControl();
    const producerA = policy({ policyRevision: "producer-group-a" });
    const producerB = policy({ policyRevision: "producer-group-b" });
    const epA = episode({ processingPolicyId: producerA.id });
    const epB = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producerB.id, eventAt: "2026-08-10T00:01:00.000Z" });
    seedEpisode(backend, epA); seedEpisode(backend, epB);
    const runtimeValue = runtime(backend);
    let llmCalls = 0;
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [epA.id, epB.id], producerPolicy: producerA, producerPolicies: [producerA, producerB], onLlm: () => { llmCalls += 1; }, llmEvidence: epA.id });
    const first = await runHighLevel(options);
    expect(first.state).toBe("completed");
    const optionsB = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [epA.id, epB.id], producerPolicy: producerB, producerPolicies: [producerA, producerB], onLlm: () => { llmCalls += 1; }, llmEvidence: epB.id });
    const second = await runHighLevel(optionsB);
    expect(second.state).toBe("completed");
    expect(llmCalls).toBe(2);
    const third = await runHighLevel(optionsB);
    expect(third).toMatchObject({ state: "completed", reason: "already-covered", observations: 0 });
  });

  it("uses the most recent episode as primary evidence for multi-evidence observations", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-primary" });
    const ep1 = episode({ processingPolicyId: producer.id, eventAt: "2026-08-10T00:00:00.000Z" });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id, eventAt: "2026-08-10T00:01:00.000Z" });
    seedEpisode(backend, ep1); seedEpisode(backend, ep2);
    const runtimeValue = runtime(backend);
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, llmText: `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["${ep1.id}","${ep2.id}"]}]}` }));
    expect(outcome.state).toBe("completed");
    const point = [...backend.points.values()].find((entry) => entry.payload.record_type === "curated_memory");
    expect(point?.payload.primary_evidence_episode_id).toBe(ep2.id);
  });

  it.each(["drop-observation", "alter-observation", "drop-current", "alter-current"] as const)("rejects %s vector readback before coverage or completion", async (fault) => {
    const backend = backendWithControl(fault);
    const producer = policy({ policyRevision: `producer-${fault}` });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    const runtimeValue = runtime(backend);
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer }));
    expect(outcome.state).toBe("pending");
    expect(outcome.jobId).toBeDefined();
    expect(await coverageFor(runtimeValue.store, outcome.jobId!)).toHaveLength(0);
    const lease = await readLease(runtimeValue.store, outcome.jobId!);
    expect(lease?.state).not.toBe("completed");
  });

  it("reuses an accepted partial materialization after a transient vector-drop without repeating LLM or embedding", async () => {
    const backend = backendWithControl("drop-current");
    const producer = policy({ policyRevision: "producer-retry" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    let llmCalls = 0;
    let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer });
    const registry = options.llm.modelRegistry;
    options.llm.modelRegistry = { complete: async (...args: Parameters<typeof registry.complete>) => { llmCalls += 1; return registry.complete(...args); } };
    const first = await runHighLevel(options);
    expect(first.state).toBe("pending");
    expect(await readLease(runtimeValue.store, first.jobId!)).toMatchObject({ state: "released", acceptedProposalId: expect.any(String), acceptedManifestHash: expect.any(String) });
    backend.setFault(undefined);
    const second = await runHighLevel(options);
    expect(second.state).toBe("completed");
    expect(llmCalls).toBe(1);
    expect(embeddingCalls).toBe(1);
    expect(await coverageFor(runtimeValue.store, first.jobId!)).toHaveLength(1);
  });

  it("preserves an accepted pair and performs no second LLM after a partial tombstone", async () => {
    const backend = backendWithControl("drop-current");
    const producer = policy({ policyRevision: "producer-accepted-partial-tombstone" });
    const ep1 = episode({ processingPolicyId: producer.id });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id });
    seedEpisode(backend, ep1); seedEpisode(backend, ep2);
    let llmCalls = 0; let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, onLlm: () => { llmCalls += 1; } });
    const first = await runHighLevel(options);
    expect(first.state).toBe("pending");
    expect(await readLease(runtimeValue.store, first.jobId!)).toMatchObject({ state: "released", acceptedProposalId: expect.any(String), acceptedManifestHash: expect.any(String) });
    await createTombstone(runtimeValue.store, { ownerHost: OWNER, scope: "occurrence", targetId: ep1.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: producer.id });
    backend.setFault(undefined);
    const second = await runHighLevel(options);
    expect(second.state).toBe("pending");
    expect(llmCalls).toBe(1);
    expect(embeddingCalls).toBe(1);
    expect(await coverageFor(runtimeValue.store, first.jobId!)).toHaveLength(0);
    expect(await readLease(runtimeValue.store, first.jobId!)).toMatchObject({ state: "released", acceptedProposalId: expect.any(String), acceptedManifestHash: expect.any(String) });
  });

  it("rejects a finite tampered persisted observation vector on accepted retry", async () => {
    const backend = backendWithControl("drop-current");
    const producer = policy({ policyRevision: "producer-vector-tamper" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer });
    const first = await runHighLevel(options);
    expect(first.state).toBe("pending");
    const observationPoint = [...backend.points.values()].find((point) => point.payload.record_type === "curated_memory");
    expect(observationPoint).toBeDefined();
    observationPoint!.vector!.semantic[0] = observationPoint!.vector!.semantic[0] === 0.25 ? 0.5 : 0.25;
    backend.setFault(undefined);
    const second = await runHighLevel(options);
    expect(second.state).toBe("pending");
    expect(embeddingCalls).toBe(1);
    expect(await coverageFor(runtimeValue.store, first.jobId!)).toHaveLength(0);
  });

  it("converges two concurrent overlapping accepted jobs before coverage without duplicate vectors or overwriting first payload", async () => {
    let enteredResolve!: () => void;
    let releaseResolve!: () => void;
    const entered = new Promise<void>((resolve) => { enteredResolve = resolve; });
    const release = new Promise<void>((resolve) => { releaseResolve = resolve; });
    const backend = backendWithControl(undefined, { onFirst: enteredResolve, wait: release });
    const producer = policy({ policyRevision: "producer-overlap" });
    const ep1 = episode({ processingPolicyId: producer.id });
    const ep2 = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producer.id, createdAt: "2026-08-10T00:00:01.000Z" });
    const ep3 = episode({ id: "00000000-0000-5000-8000-000000000003", sourceEntryId: "entry-3", processingPolicyId: producer.id, createdAt: "2026-08-10T00:00:02.000Z" });
    seedEpisode(backend, ep1); seedEpisode(backend, ep2); seedEpisode(backend, ep3);
    let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const optionsA = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep1.id, ep2.id], producerPolicy: producer, llmEvidence: ep2.id });
    const optionsB = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep2.id, ep3.id], producerPolicy: producer, llmEvidence: ep2.id });
    const firstRun = runHighLevel(optionsA);
    await entered;
    const firstObservation = [...backend.points.values()].find((point) => point.payload.record_type === "curated_memory");
    const firstLink = [...backend.points.values()].find((point) => point.payload.record_type === "evidence_link");
    expect(firstObservation).toBeDefined();
    expect(firstLink).toBeDefined();
    const firstObservationPayload = canonicalStringify(firstObservation!.payload);
    const firstLinkPayload = canonicalStringify(firstLink!.payload);
    const secondRun = await runHighLevel(optionsB);
    expect(secondRun.state).toBe("completed");
    releaseResolve();
    const first = await firstRun;
    expect(first.state).toBe("completed");
    expect(first.jobId).not.toBe(secondRun.jobId);
    expect(embeddingCalls).toBe(1);
    const observations = [...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory");
    const links = [...backend.points.values()].filter((point) => point.payload.record_type === "evidence_link");
    expect(observations).toHaveLength(1);
    expect(links).toHaveLength(1);
    expect(canonicalStringify(observations[0]!.payload)).toBe(firstObservationPayload);
    expect(canonicalStringify(links[0]!.payload)).toBe(firstLinkPayload);
    expect(links[0]!.payload.job_id).toBe(first.jobId);
    expect((await readJob(runtimeValue.store, first.jobId!))?.createdAt).toBe(ep1.createdAt);
    expect((await readJob(runtimeValue.store, secondRun.jobId!))?.createdAt).toBe(ep2.createdAt);
    expect(await coverageFor(runtimeValue.store, first.jobId!)).toHaveLength(2);
    expect(await coverageFor(runtimeValue.store, secondRun.jobId!)).toHaveLength(2);
    expect((await readLease(runtimeValue.store, first.jobId!))?.state).toBe("completed");
    expect((await readLease(runtimeValue.store, secondRun.jobId!))?.state).toBe("completed");
  });

  it.each(["rejected", "error"] as const)("injected scanner %s cannot permit derived egress", async (verdict) => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: `producer-scanner-${verdict}` });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const outcome = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer, scan: () => verdict }));
    expect(outcome.state).toBe("pending");
    expect(embeddingCalls).toBe(0);
    expect([...backend.points.values()].some((point) => ["curated_memory", "curated_current", "coverage"].includes(String(point.payload.record_type)))).toBe(false);
  });

  it("rejects raw secret-bearing values before proposal persistence even when scanner passes", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-built-in-floor" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    let embeddingCalls = 0;
    const runtimeValue = runtime(backend, async () => { embeddingCalls += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const raw = "sk-abcdefghijklmnopqrstuvwxyz123456";
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer, scan: () => "passed", llmText: `{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"${raw}","evidence":["${EPISODE_1}"]}]}` });
    const outcome = await runHighLevel(options);
    expect(outcome.state).toBe("pending");
    expect(embeddingCalls).toBe(0);
    expect(canonicalStringify([...backend.points.values()])).not.toContain(raw);
    expect([...backend.points.values()].some((point) => ["curated_memory", "curated_current", "coverage"].includes(String(point.payload.record_type)))).toBe(false);
  });

  it("rejects an accepted job mutation during embedding before any derived write", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-job-mutation-embedding" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    let embedCalls = 0;
    const runtimeValue = runtime(backend, async () => {
      embedCalls += 1;
      const point = [...backend.points.values()].find((candidate) => candidate.payload.record_type === "job")!;
      const parsed = coordinationRecordFromPayload(point.payload, OWNER) as JobRecord;
      const changed = { ...parsed, createdAt: "2026-08-10T00:00:01.000Z", contentHash: "pending" } as JobRecord;
      changed.contentHash = canonicalRecordHash(changed);
      point.payload = recordPayload(changed) as Record<string, unknown>;
      return Array.from({ length: 1024 }, () => 0.25);
    });
    const result = await runHighLevel(curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer }));
    expect(result).toMatchObject({ state: "pending", reason: "materialization-failed" });
    expect(embedCalls).toBe(1);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory" || point.payload.record_type === "curated_current" || point.payload.record_type === "coverage")).toHaveLength(0);
  });

  it("keeps scanner/revocation failures before any derived write", async () => {
    const backend = backendWithControl();
    const producer = policy({ policyRevision: "producer-gate" });
    const ep = episode({ processingPolicyId: producer.id });
    seedEpisode(backend, ep);
    const runtimeValue = runtime(backend);
    const options = curationOptions({ store: runtimeValue.store, embedding: runtimeValue.embedding, membership: [ep.id], producerPolicy: producer });
    const before = backend.points.size;
    const revoked = { ...emptyControl({ revokedDestinationIds: [embeddingDestination.id] }), contentHash: "pending" };
    backend.points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload({ ...revoked, contentHash: canonicalRecordHash(revoked) } as ControlRecord) });
    const outcome = await runHighLevel(options);
    expect(outcome.state).not.toBe("completed");
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "curated_memory")).toHaveLength(0);
    expect(backend.points.size).toBe(before);
  });
});
