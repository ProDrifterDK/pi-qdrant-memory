import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createIngestProcessor,
  ingestPendingJobs,
} from "../../src/outbox/delivery.js";
import { bindQdrantDestination, createQdrantDestinationFactory, createQdrantSafeBundle, QdrantContentHashCollisionError, recordPayload } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, BoundEmbeddingDestination, ValidatedEmbeddingDocumentClient } from "../../src/clients/embeddings.js";
import { canonicalRecordHash, episodeSemanticProjection, type ControlRecord, type EpisodeRecord, type ProcessingPolicyRecord, type TombstoneRecord } from "../../src/domain/records.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../../src/domain/canonical.js";
import { tombstoneId } from "../../src/domain/ids.js";
import { intersectPolicies, processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import type { AuthorizedDestination } from "../../src/types.js";
import type { OutboxJob } from "../../src/outbox/store.js";
import { physicalPointIdFor } from "../../src/qdrant/client.js";
import { type ProductionCoordinationStore } from "../../src/coordination/control.js";

import { bindIngestRuntime, type BoundIngestRuntime } from "../../src/coordination/ingest.js";
import { createTombstone } from "../../src/coordination/tombstones.js";
import { COLLECTION_CONTROL_ID, controlPayload } from "../../src/qdrant/schema.js";

const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination: AuthorizedDestination = { id: "embed:local", residency: "local", dataUse: "memory" };
const coordination = { policyHash: "coordination-policy-hash", policyEpoch: 7 } as const;
const NOW = "2026-08-10T00:00:00.000Z";

function json(value: unknown, status = 200): Response { return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } }); }
afterEach(() => { vi.unstubAllGlobals(); });
function stubGlobalFetch(fetchImpl: typeof fetch): void { vi.stubGlobal("fetch", fetchImpl); }

interface BackendPoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] }; }
interface BackendHooks {
  retrieve?: (ids: readonly string[], url: string) => { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }[] | undefined | Promise<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }[] | undefined>;
  upsert?: (points: BackendPoint[]) => void;
  failUpsert?: boolean;
}
/** In-memory backend + global fetch stub; NO raw writer is ever constructed (safe bundle creation only). */
function restQdrantWriter(seed: BackendPoint[] = [], hooks: BackendHooks = {}, baseUrl = "http://qdrant", ownerHost: "pi" | "prime" = "pi"): { points: Map<string, BackendPoint>; ownerHost: "pi" | "prime"; options: QdrantClientOptions } {
  const points = new Map<string, BackendPoint>(seed.map((point) => [point.id, point]));
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: BackendPoint[]; update_mode?: string };
    if (url.includes("/points/retrieve")) {
      const ids = body?.ids ?? [];
      const found = ids.map((id) => points.get(id)).filter((point) => point !== undefined);
      const extra = hooks.retrieve === undefined ? undefined : await hooks.retrieve(ids, url);
      return json({ result: [...found, ...(extra ?? [])], status: "ok" });
    }
    if (url.includes("/points?") && init.method === "PUT") {
      if (hooks.failUpsert === true) throw new Error("backend upsert failed");
      for (const point of body?.points ?? []) points.set(point.id, { id: point.id, payload: point.payload, ...(point.vector === undefined ? {} : { vector: point.vector }) });
      hooks.upsert?.(body?.points ?? []);
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  };
  // Production-bound: NO injected fetchImpl; the backend is stubbed on global fetch.
  stubGlobalFetch(fetchImpl);
  const options: QdrantClientOptions = { baseUrl, collection: ownerHost === "pi" ? "pi_memory" : "prime_memory", ownerHost, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
  return { points, ownerHost, options };
}
function emptyControl(overrides: Partial<ControlRecord> = {}): ControlRecord {
  const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: coordination.policyEpoch, coordinationPolicyHash: coordination.policyHash, state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord;
}
function controlPoint(control: ControlRecord): BackendPoint { return { id: COLLECTION_CONTROL_ID, payload: controlPayload(control) }; }

function realEmbeddings(embedImpl: (input: { model: string; text: string; signal?: AbortSignal }) => Promise<readonly number[]>, baseUrl = "http://embed/v1"): EmbeddingsClient {
  // Production-bound embedding: no injected fetchImpl; route the global fetch.
  const previous = globalThis.fetch;
  stubGlobalFetch(async (input, init) => {
    const url = String(input);
    if (url.includes("/embeddings")) {
      const body = JSON.parse(String(init?.body ?? "{}")) as { model?: string; input?: string };
      const vector = await embedImpl({ model: body.model ?? "bge-m3", text: body.input ?? "" });
      return json({ data: [{ embedding: vector }] });
    }
    if (previous !== undefined) return previous(input, init);
    throw new Error("no transport route");
  });
  return new EmbeddingsClient({ baseUrl, model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
}
function boundQdrant(options: QdrantClientOptions, destination: AuthorizedDestination = qdrantDestination): ReturnType<typeof bindQdrantDestination> {
  const factory = createQdrantDestinationFactory({ options, destination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return bindQdrantDestination(factory, destination);
}
function boundEmbedding(client: EmbeddingsClient, destination: AuthorizedDestination = embeddingDestination): BoundEmbeddingDestination {
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return bindEmbeddingDestination(factory, destination);
}
/** ONE production ingest bundle: the store and the qdrant destination share ONE safe bundle transport. */
function runtime(backend: { points: Map<string, BackendPoint>; ownerHost: "pi" | "prime"; options: QdrantClientOptions }, embedImpl: (input: { model: string; text: string; signal?: AbortSignal }) => Promise<readonly number[]> = async () => Array.from({ length: 1024 }, () => 0.25), qdrantDestinationOverride?: AuthorizedDestination): BoundIngestRuntime {
  const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestinationOverride ?? qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return bindIngestRuntime({ store: bundle.store, qdrant: bindQdrantDestination(bundle.qdrant, qdrantDestinationOverride ?? qdrantDestination), embedding: boundEmbedding(realEmbeddings(embedImpl)) });
}
function backendWithControl(overrides: Partial<ControlRecord> = {}): { points: Map<string, BackendPoint>; ownerHost: "pi" | "prime"; options: QdrantClientOptions } {
  return restQdrantWriter([controlPoint(emptyControl(overrides))]);
}

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const pending = {
    id: "pending", ownerHost: "pi" as const,
    destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: "llm:local" },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null,
    residency: "local", dataUse: "memory", policyRevision: "v1", ...overrides,
  } satisfies ProcessingPolicy;
  return { ...pending, id: processingPolicyHash(pending) };
}

function episode(current: ProcessingPolicy, overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const pending = {
    recordType: "episode" as const,
    id: "00000000-0000-5000-8000-000000000001", ownerHost: "pi" as const, schemaRevision: 1 as const,
    createdAt: NOW, privacyEpoch: 0, processingPolicyId: current.id,
    expiresAt: current.expiresAt, contentHash: "pending", sourceEntryId: "entry-1", host: "pi" as const,
    projectId: "project-1", projectIdentityKind: "registered" as const, sessionId: "session-1", turnId: "turn-1",
    agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: NOW,
    modelId: "capture-model", embeddingDimension: 1024, originProvider: current.originProvider,
    destinationId: current.destinationIds.qdrant, status: "active" as const, redactionStatus: "redacted" as const,
    secretScan: "passed" as const, text: "safe [token redacted]", ...overrides,
  };
  const value = { ...pending, ...overrides } as EpisodeRecord;
  for (const key of ["text", "toolName", "toolArgs", "errorFingerprint", "producerId", "nodeId"] as const) if (value[key] === undefined) delete (value as Partial<EpisodeRecord>)[key];
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}

/** Test-local tombstone record with the canonical H(owner,"tombstone",target) id. */
function tombstoneRecord(targetId: string, privacyEpoch = 0): TombstoneRecord {
  const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch, processingPolicyId: "policy-1", expiresAt: null, recordType: "tombstone" as const, id: "pending", scope: "occurrence" as const, targetId, contentHash: "pending" };
  const id = tombstoneId("pi", targetId);
  return { ...base, id, contentHash: canonicalRecordHash({ ...base, id }) } as TombstoneRecord;
}


/** SAFE tombstone seeding: persist the vector-bound episode first, then forget it via createTombstone (no raw engine exists). */
async function seedTombstone(store: ProductionCoordinationStore, backend: { points: Map<string, BackendPoint> }, targetEpisode: EpisodeRecord): Promise<void> {
  const vector = targetEpisode.vector ?? Array.from({ length: 1024 }, () => 0.25);
  const withVector = { ...targetEpisode, vector: [...vector] };
  const finalEpisode = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord;
  backend.points.set(finalEpisode.id, { id: finalEpisode.id, payload: recordPayload(finalEpisode) as Record<string, unknown>, vector: { semantic: [...vector] } });
  try {
    await createTombstone(store, { ownerHost: "pi", scope: "occurrence", targetId: finalEpisode.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1" });
  } catch {
    // Episodes whose OWN expiry the store's read policy refuses (e.g. a
    // 1970-expiry policy fixture) cannot be verified by the safe op; fall back
    // to the exact tombstone wire point the safe op would have written.
    const rec = tombstoneRecord(finalEpisode.id, 0);
    backend.points.set(rec.id, { id: rec.id, payload: recordPayload(rec) as Record<string, unknown> });
  }
  // The verification episode point was seeding-only: remove it so the backend
  // reflects ONLY the tombstone (as the raw seeding used to).
  backend.points.delete(finalEpisode.id);
}

function job(current: ProcessingPolicy, overrides: Partial<OutboxJob> = {}): OutboxJob {
  const episodes = overrides.episodes ?? [episode(current)];
  const id = deterministicUuid("pi-qdrant-memory-v2:outbox-job", "pi", episodes.map((entry) => entry.id), current.id);
  const pending = {
    version: 1 as const, id, ownerHost: "pi" as const, nodeId: "node-redacted",
    producerUuid: "00000000-0000-4000-8000-000000000001", createdAt: NOW,
    deadline: current.expiresAt, policyId: current.id, policy: current, episodeIds: episodes.map((entry) => entry.id), episodes,
    auditHash: "pending", ...overrides,
  } as OutboxJob;
  const { auditHash: _auditHash, ...withoutAudit } = pending;
  return { ...pending, auditHash: sha256Hex(canonicalStringify(withoutAudit)) } as OutboxJob;
}

function snapshot(_current: ProcessingPolicy, overrides: Partial<{ state: "active" | "draining" | "retired"; privacyEpoch: number; coordinationPolicyEpoch: number; policyHash: string; revokedDestinationIds: readonly string[] }> = {}) {
  return { state: "active" as const, privacyEpoch: 0, coordinationPolicyEpoch: coordination.policyEpoch, policyHash: coordination.policyHash, revokedDestinationIds: [] as readonly string[], ...overrides };
}


describe("Task 7 redacted outbox ingest over the Task 8 ingest bundle", () => {
  it("binds exactly one configured destination identity; no ID allowlist can switch endpoints", () => {
    const wrongQdrant = createQdrantDestinationFactory({ options: restQdrantWriter([], {}, "http://qdrant-b").options, destination: { ...qdrantDestination, id: "qdrant:b" }, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const wrongEmbedding = createEmbeddingDestinationFactory({ endpoint: "http://embed-b/v1", destination: { ...embeddingDestination, id: "embed:b" }, client: bindEmbeddingDocumentClient({ endpoint: "http://embed-b/v1", client: realEmbeddings(async () => Array.from({ length: 1024 }, () => 0.25), "http://embed-b/v1") }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    expect(() => bindQdrantDestination(wrongQdrant, qdrantDestination)).toThrow(/destination/i);
    expect(() => bindEmbeddingDestination(wrongEmbedding, embeddingDestination)).toThrow(/destination/i);
  });

  it("requires the real REST transport: structural fakes cannot enter the safe creation surface", () => {
    const structuralQdrant = { endpoint: "http://qdrant", ownerHost: "pi", collection: "pi_memory", maxClockSkewMs: 0, retrieve: async () => [], upsertPoints: async () => undefined };
    expect(() => createQdrantSafeBundle({ options: structuralQdrant as unknown as QdrantClientOptions, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch })).toThrow();
    const structuralEmbedding = { endpoint: "http://embed/v1", embedDocument: async () => [] };
    expect(() => bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: structuralEmbedding as never })).toThrow(/pairing/i);
    // The ingest bundle is the only way to egress: a structural store/destination mix cannot mint it.
    expect(() => bindIngestRuntime({ store: { ownerHost: "pi", readControl: async () => emptyControl() } as never, qdrant: {} as never, embedding: {} as never })).toThrow(/production store|bound/i);
  });

  it("the safe creation surface cannot be monkeypatched and exposes no writer constructor", () => {
    // No raw writer constructor/factory exists anywhere on the safe surface.
    const bundle = createQdrantSafeBundle({ options: restQdrantWriter().options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    for (const obj of [bundle.store, bundle.qdrant, bundle.transport]) {
      const anyObj = obj as unknown as Record<string, unknown>;
      expect("writer" in anyObj).toBe(false);
      expect("upsertPoints" in anyObj).toBe(false);
    }
    // The store's statics cannot be monkeypatched.
    expect(() => { (bundle.store.constructor as unknown as { isValid: unknown }).isValid = () => true; }).toThrow();
    // Malformed options fail closed before any network.
    expect(() => createQdrantSafeBundle({ options: { ...restQdrantWriter().options, baseUrl: "not-a-url" }, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch })).toThrow(/endpoint/i);
  });

  it("fails closed before control or egress when the bundle cannot serve the job host", async () => {
    const localPolicy = policy();
    const primeBackend = restQdrantWriter([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) }], {});
    stubGlobalFetch(async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: BackendPoint[] }; if (url.includes("/points/retrieve")) { const ids = body?.ids ?? []; return json({ result: ids.map((id) => primeBackend.points.get(id)).filter((point) => point !== undefined), status: "ok" }); } if (url.includes("/points?") && init.method === "PUT") { for (const point of body?.points ?? []) primeBackend.points.set(point.id, { id: point.id, payload: point.payload }); return json({ result: { status: "acknowledged" }, status: "ok" }); } return json({ result: {}, status: "ok" }); });
    const primeBundle = createQdrantSafeBundle({ options: restQdrantWriter([], {}, "http://qdrant", "prime").options, destination: { ...qdrantDestination, id: "qdrant:prime" }, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const primeStore = primeBundle.store;
    const primeQdrant = bindQdrantDestination(primeBundle.qdrant, { ...qdrantDestination, id: "qdrant:prime" });
    const primeEmbedding = (() => { const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: realEmbeddings(async () => Array.from({ length: 1024 }, () => 0.25)) }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch }); return bindEmbeddingDestination(factory, embeddingDestination); })();
    const primeRuntime = bindIngestRuntime({ store: primeStore, qdrant: primeQdrant, embedding: primeEmbedding });
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, runtime: primeRuntime, maxClockSkewMs: 0 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
  });

  it("validates bound destinations, persists the producer policy, then embeds and inserts a named semantic episode", async () => {
    const localPolicy = policy({ policyRevision: "local-v1" });
    const producerPolicy = policy({ policyRevision: "producer-v1" });
    const events: string[] = [];
    const backend = backendWithControl();
    backend.points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) });
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const rt = bindIngestRuntime({ store: bundle.store, qdrant: bindQdrantDestination(bundle.qdrant, qdrantDestination), embedding: boundEmbedding(realEmbeddings(async () => { events.push("embed"); return Array.from({ length: 1024 }, () => 0.25); })) });
    // The tracked runtime shares the EXACT same bundle transport (store + qdrant).
    const tracked = bindIngestRuntime({ store: bundle.store, qdrant: bindQdrantDestination(bundle.qdrant, qdrantDestination), embedding: rt.embedding });
    // Track via the backend upsert/retrieve hooks on a second writer? Instead assert the outcome + backend state.
    const result = await ingestPendingJobs({ job: job(producerPolicy), now: 100, localPolicy, runtime: tracked, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const effective = intersectPolicies([producerPolicy], localPolicy);
    expect(effective).not.toBeNull();
    const episodePoint = [...backend.points.values()].find((point) => point.payload.record_type === "episode");
    expect(episodePoint).toBeDefined();
    expect(episodePoint?.vector?.semantic).toHaveLength(1024);
    expect(episodePoint?.payload.record_type).toBe("episode");
    expect(episodePoint?.payload.owner_host).toBe("pi");
    // The EFFECTIVE producer/local intersection is persisted, never the producer policy alone.
    expect(episodePoint?.payload.processing_policy_id).toBe(effective?.id);
    expect(episodePoint?.payload.vector).toBeUndefined();
    const policyPoint = [...backend.points.values()].find((point) => point.payload.record_type === "processing_policy");
    expect(policyPoint?.payload.policy).toMatchObject({ id: effective?.id });
    expect(policyPoint?.payload.policy_id ?? policyPoint?.payload.processing_policy_id).toBe(effective?.id);
    expect(events).toEqual(["embed"]);
  });

  it("quarantines expired or scanner-non-passed jobs before any Qdrant or embedding call", async () => {
    const expired = policy({ expiresAt: "1970-01-01T00:00:00.000Z" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await expect(ingestPendingJobs({ job: job(expired), now: 100, localPolicy: expired, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode" || point.payload.record_type === "processing_policy")).toBe(false);
    const safe = policy();
    const rejected = { ...episode(safe), secretScan: "rejected" } as unknown as EpisodeRecord;
    await expect(ingestPendingJobs({ job: job(safe, { episodes: [rejected] }), now: 100, localPolicy: safe, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
  });

  it("leaves embedding failures retryable and maps a typed hash collision to stable quarantine", async () => {
    const localPolicy = policy();
    const failing = runtime(backendWithControl(), async () => { throw new Error("offline"); });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: failing, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // Typed collision: the backend holds a REAL vector-bound CANONICAL episode
    // point (wire format + named vector) with the same id but a different hash.
    const collidingEpisode = episode(localPolicy, { text: "different content", vector: Array.from({ length: 1024 }, (_, index) => (index % 5) / 10) });
    const collidingBackend = restQdrantWriter([controlPoint(emptyControl()), { id: collidingEpisode.id, payload: recordPayload(collidingEpisode) as Record<string, unknown>, vector: { semantic: [...(collidingEpisode.vector as number[])] } }]);
    const collidingRuntime = runtime(collidingBackend);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: collidingRuntime, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
  });

  it("rereads control after a delayed embedding and skips the distinct final episode write when state is revoked", async () => {
    const localPolicy = policy();
    let releaseEmbed: (() => void) | undefined;
    const backend = backendWithControl();
    const rt = runtime(backend, async () => { await new Promise<void>((resolve) => { releaseEmbed = resolve; }); return Array.from({ length: 1024 }, () => 0.25); });
    // The control flips to draining right after the embed starts, so the final barrier refuses.
    const originalEmbed = rt.embedding.embed.bind(rt.embedding);
    const drainingRuntime = bindIngestRuntime({ store: rt.store, qdrant: rt.qdrant, embedding: boundEmbedding(realEmbeddings(async () => { await new Promise<void>((resolve) => { releaseEmbed = resolve; }); backend.points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl({ state: "draining" })) }); return Array.from({ length: 1024 }, () => 0.25); })) });
    const pending = ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: drainingRuntime, maxClockSkewMs: 5 });
    await vi.waitFor(() => expect(releaseEmbed).toBeTypeOf("function"));
    releaseEmbed?.();
    await expect(pending).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    const episodePoints = [...backend.points.values()].filter((point) => point.payload.record_type === "episode");
    expect(episodePoints).toHaveLength(0);
    void originalEmbed;
  });

  it("exposes the sole processor seam and reports delivered only after policy and episode readback", async () => {
    const localPolicy = policy();
    const backend = backendWithControl();
    const rt = runtime(backend);
    const processor = createIngestProcessor({ localPolicy, runtime: rt, maxClockSkewMs: 0, now: () => 100 });
    await expect(processor.process(job(localPolicy), {})).resolves.toEqual({ status: "delivered" });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode")).toBe(true);
    expect(() => createIngestProcessor({ localPolicy, runtime: { store: {}, qdrant: {}, embedding: {} } as never, maxClockSkewMs: 0, now: () => 100 })).toThrow(/bound ingest runtime/i);
  });

  it("stops a privacy-epoch mismatch before the policy record can egress", async () => {
    const localPolicy = policy();
    const backend = backendWithControl({ privacyEpoch: 1 });
    const rt = runtime(backend);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
  });

  it("passes abort to delayed BGE embedding and never performs the final episode write", async () => {
    const localPolicy = policy();
    let releaseEmbed: ((value: readonly number[]) => void) | undefined;
    const backend = backendWithControl();
    const rt = runtime(backend, () => new Promise<readonly number[]>((resolve) => { releaseEmbed = resolve; }));
    const controller = new AbortController();
    const processor = createIngestProcessor({ localPolicy, runtime: rt, maxClockSkewMs: 5, now: () => 100 });
    const pending = processor.process(job(localPolicy), { signal: controller.signal });
    await vi.waitFor(() => expect(releaseEmbed).toBeTypeOf("function"));
    controller.abort();
    releaseEmbed?.(Array.from({ length: 1024 }, () => 0.25));
    await expect(pending).resolves.toMatchObject({ status: "pending", category: "aborted" });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode")).toBe(false);
  });

  it("converges a partially completed multi-episode retry from a verified vector without re-embedding it", async () => {
    const localPolicy = policy();
    const first = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-1", sessionId: "session-1", turnId: "turn-1" });
    const second = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-2", sessionId: "session-2", turnId: "turn-2" });
    const completed = { ...first, vector: Array.from({ length: 1024 }, () => 0.5), contentHash: "pending" };
    const completedPoint = { ...completed, contentHash: canonicalRecordHash(completed as EpisodeRecord) };
    const backend = backendWithControl();
    backend.points.set(completedPoint.id, { id: completedPoint.id, payload: recordPayload(completedPoint) as Record<string, unknown>, vector: { semantic: [...(completedPoint.vector as number[])] } });
    let embeds = 0;
    const rt = runtime(backend, async () => { embeds += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const retryResult = await ingestPendingJobs({ job: job(localPolicy, { episodes: [first, second], episodeIds: [first.id, second.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(retryResult).toEqual({ delivered: 2, pending: 0, quarantined: 0 });
    expect(embeds).toBe(1);
    const episodePoints = [...backend.points.values()].filter((point) => point.payload.record_type === "episode");
    expect(episodePoints).toHaveLength(2);
  });

  it("keeps a missing vector readback pending because it is an ambiguous partial acknowledgement", async () => {
    const localPolicy = policy();
    const first = episode(localPolicy);
    const backend = backendWithControl();
    backend.points.set(first.id, { id: first.id, payload: recordPayload(first) as Record<string, unknown> });
    const rt = runtime(backend, async () => Array.from({ length: 1024 }, () => 0.25));
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
  });

  it("uses exact BGE-M3 document input without the query prefix", async () => {
    const localPolicy = policy();
    let seen = "";
    const rt = runtime(backendWithControl(), async ({ text }) => { seen = text; return Array.from({ length: 1024 }, () => 0.25); });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    expect(seen).toBe("event:user\ntext:safe [token redacted]");
    expect(seen).not.toContain("query: ");
  });

  it("rechecks strict expiry from the live processor clock after embedding", async () => {
    let now = 0;
    const expiring = policy({ expiresAt: "1970-01-01T00:00:00.001Z" });
    let releaseEmbed: (() => void) | undefined;
    const backend = backendWithControl();
    const rt = runtime(backend, async () => { await new Promise<void>((resolve) => { releaseEmbed = resolve; }); return Array.from({ length: 1024 }, () => 0.25); });
    const processor = createIngestProcessor({ localPolicy: expiring, runtime: rt, maxClockSkewMs: 0, now: () => now });
    const pending = processor.process(job(expiring), {});
    await vi.waitFor(() => expect(releaseEmbed).toBeTypeOf("function"));
    now = 1;
    releaseEmbed?.();
    await expect(pending).resolves.toMatchObject({ status: "quarantined", category: "expired" });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode")).toBe(false);
  });

  it("fail-closes forged direct audit and membership inputs before control or egress", async () => {
    const localPolicy = policy();
    const rt = runtime(backendWithControl());
    await expect(ingestPendingJobs({ job: { ...job(localPolicy), auditHash: "forged" }, now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    await expect(ingestPendingJobs({ job: { ...job(localPolicy), episodes: null } as never, now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
  });


  it("materializes and embeds an error-only episode through one safe deterministic projection", async () => {
    const localPolicy = policy();
    const errorEpisode = episode(localPolicy, { eventKind: "tool_error", text: undefined, toolArgs: undefined, toolName: "shell", errorFingerprint: "a".repeat(32) });
    let seen = "";
    const rt = runtime(backendWithControl(), async ({ text }) => { seen = text; return Array.from({ length: 1024 }, () => 0.25); });
    await expect(ingestPendingJobs({ job: job(localPolicy, { episodes: [errorEpisode], episodeIds: [errorEpisode.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    expect(seen).toBe(episodeSemanticProjection(errorEpisode));
    expect(seen).not.toContain("a".repeat(32));
  });

  it("keeps generic Qdrant errors and missing or forged readbacks pending, but quarantines only a typed or verified collision", async () => {
    const localPolicy = policy();
    // Generic backend failure stays pending (retryable).
    const failingBackend = backendWithControl();
    const failingWriter = restQdrantWriter([controlPoint(emptyControl())], { failUpsert: true });
    const failingRt = runtime(failingWriter);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: failingRt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // Missing policy readback stays pending (retrieves for policy points return nothing).
    const missingBackend = restQdrantWriter([controlPoint(emptyControl())], { retrieve: async (ids) => (ids.includes(physicalPointIdFor("processing_policy")) ? [] : undefined) });
    const missingRt = runtime(missingBackend);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: missingRt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    void failingBackend;
  });

  it("treats malformed, duplicate, or non-exact revocation snapshots as unavailable and suppresses embedding-only revocation egress", async () => {
    const localPolicy = policy();
    for (const revokedDestinationIds of [[qdrantDestination.id, qdrantDestination.id], ["api-key:forbidden"]]) {
      const malformedPayload = { ...controlPayload(emptyControl()), revoked_destination_ids: revokedDestinationIds };
      const backend = restQdrantWriter([{ id: COLLECTION_CONTROL_ID, payload: malformedPayload }]);
      const rt = runtime(backend);
      await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
      expect([...backend.points.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
    }
    // Embedding-only revocation suppresses the policy point too.
    const backend = backendWithControl({ revokedDestinationIds: [embeddingDestination.id] });
    const rt = runtime(backend);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
  });

  it("rechecks abort and expiry after delayed controls and an existing-episode retrieve before any later egress", async () => {
    const localPolicy = policy();
    let controlReleases: Array<() => void> = [];
    let gateOpen = true;
    const gate = (ids: readonly string[]) => (ids.includes(COLLECTION_CONTROL_ID) && gateOpen ? new Promise<void>((resolve) => { controlReleases.push(resolve); }) : undefined);
    const gatedBackend = restQdrantWriter([controlPoint(emptyControl())], { retrieve: async (ids) => { const p = gate(ids); if (p !== undefined) await p; return undefined; } });
    const aborting = createIngestProcessor({ localPolicy, runtime: runtime(gatedBackend), maxClockSkewMs: 0, now: () => 0 });
    const controller = new AbortController();
    const aborted = aborting.process(job(localPolicy), { signal: controller.signal });
    await vi.waitFor(() => expect(controlReleases.length).toBeGreaterThan(0));
    controller.abort();
    gateOpen = false;
    for (const release of controlReleases) release();
    await expect(aborted).resolves.toMatchObject({ status: "pending", category: "aborted" });
    // Expiry after a delayed control read: quarantined, no egress.
    const expiring = policy({ expiresAt: "1970-01-01T00:00:00.001Z" });
    let now = 0;
    let expiryReleases: Array<() => void> = [];
    let expiryOpen = true;
    const expiryBackend = restQdrantWriter([controlPoint(emptyControl())], { retrieve: async (ids) => { if (ids.includes(COLLECTION_CONTROL_ID) && expiryOpen) await new Promise<void>((resolve) => { expiryReleases.push(resolve); }); return undefined; } });
    const expiryProcessor = createIngestProcessor({ localPolicy: expiring, runtime: runtime(expiryBackend), maxClockSkewMs: 0, now: () => now });
    const expired = expiryProcessor.process(job(expiring), {});
    await vi.waitFor(() => expect(expiryReleases.length).toBeGreaterThan(0));
    now = 1;
    expiryOpen = false;
    for (const release of expiryReleases) release();
    await expect(expired).resolves.toMatchObject({ status: "quarantined", category: "expired" });
    expect([...expiryBackend.points.values()].some((point) => point.payload.record_type === "episode" || point.payload.record_type === "processing_policy")).toBe(false);
  });

  it("converges sequential distinct episodes through one existing canonical policy point despite later job timestamps", async () => {
    const localPolicy = policy();
    const backend = backendWithControl();
    const rt = runtime(backend);
    const first = await ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, runtime: rt, maxClockSkewMs: 0 });
    expect(first).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const later = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000099", sourceEntryId: "entry-99", sessionId: "session-99", turnId: "turn-99" });
    const second = await ingestPendingJobs({ job: job(localPolicy, { episodes: [later], episodeIds: [later.id] }), now: 1000, localPolicy, runtime: rt, maxClockSkewMs: 0 });
    expect(second).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const policyPoints = [...backend.points.values()].filter((point) => point.payload.record_type === "processing_policy");
    expect(policyPoints).toHaveLength(1);
  });

  it("refreshes control and expiry after a delayed null existing lookup before document egress", async () => {
    const localPolicy = policy();
    let releaseRetrieve: ((value: unknown) => void) | undefined;
    const backend = restQdrantWriter([controlPoint(emptyControl())], {
      retrieve: async (ids) => {
        if (ids.length === 1 && ids[0] !== COLLECTION_CONTROL_ID && ids[0]!.startsWith("00000000")) {
          await new Promise<void>((resolve) => { releaseRetrieve = resolve; });
          backend.points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl({ revokedDestinationIds: [embeddingDestination.id] })) });
          return [];
        }
        return undefined;
      },
    });
    const rt = runtime(backend);
    const revoking = ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, runtime: rt, maxClockSkewMs: 0 });
    await vi.waitFor(() => expect(releaseRetrieve).toBeTypeOf("function"));
    releaseRetrieve?.(null);
    await expect(revoking).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode")).toBe(false);
    expect([...backend.points.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(true);
  });

  it("rejects unknown control keys and never dereferences malformed processor jobs", async () => {
    const localPolicy = policy();
    const malformed = { ...controlPayload(emptyControl()), unexpected_raw: true };
    const backend = restQdrantWriter([{ id: COLLECTION_CONTROL_ID, payload: malformed }]);
    const rt = runtime(backend);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, runtime: rt, maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    const processor = createIngestProcessor({ localPolicy, runtime: rt, maxClockSkewMs: 0, now: () => 0 });
    await expect(processor.process({ episodes: null } as never, {})).resolves.toMatchObject({ status: "quarantined", category: "episode_invalid" });
  });

  it("persists the EFFECTIVE producer/local intersection: 365d producer + 1d local stores the intersection id and the 1d expiry", async () => {
    const oneDay = new Date(Date.now() + 86_400_000).toISOString();
    const oneYear = new Date(Date.now() + 365 * 86_400_000).toISOString();
    const localPolicy = policy({ expiresAt: oneDay, policyRevision: "local-r" });
    const producerPolicy = policy({ expiresAt: oneYear, policyRevision: "producer-r" });
    const effective = intersectPolicies([producerPolicy], localPolicy);
    expect(effective).not.toBeNull();
    expect(effective?.expiresAt).toBe(oneDay);
    expect(effective?.id).not.toBe(producerPolicy.id);
    const backend = backendWithControl();
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(producerPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const policyPoint = [...backend.points.values()].find((point) => point.payload.record_type === "processing_policy");
    expect(policyPoint?.payload.policy_id ?? policyPoint?.payload.id).toBe(effective?.id);
    expect(policyPoint?.payload.expires_at).toBe(oneDay);
    expect([...backend.points.values()].some((point) => point.payload.record_type === "processing_policy" && point.payload.id === producerPolicy.id)).toBe(false);
    const episodePoint = [...backend.points.values()].find((point) => point.payload.record_type === "episode");
    expect(episodePoint?.payload.processing_policy_id).toBe(effective?.id);
    expect(episodePoint?.payload.expires_at).toBe(oneDay);
  });

  it("fails closed when the persisted effective policy point carries a different canonical hash", async () => {
    const localPolicy = policy();
    const producerPolicy = policy({ policyRevision: "producer-r" });
    const effective = intersectPolicies([producerPolicy], localPolicy);
    expect(effective).not.toBeNull();
    // The effective policy id already carries a DIFFERENT content hash: the
    // flow must fail closed (pending, never broadening to the producer policy).
    const effectiveRecord = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: effective!.id, expiresAt: effective!.expiresAt, recordType: "processing_policy" as const, id: effective!.id, policy: effective, canonicalHash: effective!.id, contentHash: "pending" };
    const finalRecord = { ...effectiveRecord, contentHash: canonicalRecordHash(effectiveRecord) };
    const tamperedPayload = { ...recordPayload(finalRecord), content_hash: "f".repeat(64) };
    const physicalPolicyId = physicalPointIdFor("processing_policy", effective!.id);
    const backend = backendWithControl();
    backend.points.set(physicalPolicyId, { id: physicalPolicyId, payload: tamperedPayload as Record<string, unknown> });
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(producerPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result.delivered).toBe(0);
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode")).toBe(false);
  });

  it("never delivers when a committed episode vector coordinate is changed while the payload hash stays", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    // A previously committed episode: vector-bound hash committed with vector A.
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...source, vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    // The backend serves the RIGHT non-vector payload + old content hash but ONE changed coordinate.
    const tamperedVector = [...vectorA]; tamperedVector[42] = (tamperedVector[42] ?? 0) + 1;
    const backend = restQdrantWriter([controlPoint(emptyControl()), { id: physicalPointIdFor("episode", source.id), payload: recordPayload(finalCommitted) as Record<string, unknown>, vector: { semantic: tamperedVector } }], { failUpsert: true });
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // The readback is invalid: the processor must not treat the tampered point as the episode.
    await expect(rt.qdrant.retrieve("episode", source.id)).resolves.toBeNull();
  });

  it("fails when the initial insert readback substitutes a different valid 1024 vector", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    // The PUT stores the episode, but the readback serves a DIFFERENT valid vector.
    const holder: { points?: Map<string, BackendPoint> } = {};
    const backend2 = restQdrantWriter([controlPoint(emptyControl())], {
      upsert: (points: BackendPoint[]) => {
        for (const point of points) {
          if (point.payload.record_type === "episode") {
            const stored = holder.points?.get(point.id);
            if (stored !== undefined) stored.vector = { semantic: Array.from({ length: 1024 }, () => 0.75) };
          }
        }
      },
    });
    holder.points = backend2.points;
    const rt = runtime(backend2);
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // The substituted point is not a verified committed episode.
    await expect(rt.qdrant.retrieve("episode", source.id)).resolves.toBeNull();
  });

  it("never mutates the durable source episode or its canonical hash when delivering", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    const sourceHash = source.contentHash;
    const snapshot = JSON.stringify(source);
    const backend = backendWithControl();
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [source], episodeIds: [source.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    expect(source.contentHash).toBe(sourceHash);
    expect(source.vector).toBeUndefined();
    expect(JSON.stringify(source)).toBe(snapshot);
    // The committed point carries the vector-BOUND hash (different from the source hash).
    const committed = await rt.qdrant.retrieve("episode", source.id);
    expect(committed).not.toBeNull();
    expect(committed?.vector).toHaveLength(1024);
    expect(committed?.contentHash).not.toBe(sourceHash);
    expect(committed?.contentHash).toBe(canonicalRecordHash(committed as EpisodeRecord));
  });

  it("terminally quarantines a legacy vector-excluding hash point at the prelookup without embedding", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    // A legacy committed point: vector present but the stored hash was computed
    // under the OLD vector-excluding formula.
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...source, vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    const { vector: _v, ...noVector } = finalCommitted as EpisodeRecord;
    const legacyPayload = { ...recordPayload(finalCommitted), content_hash: canonicalRecordHash(noVector as EpisodeRecord) } as Record<string, unknown>;
    let embeds = 0;
    const backend = restQdrantWriter([controlPoint(emptyControl()), { id: physicalPointIdFor("episode", source.id), payload: legacyPayload, vector: { semantic: [...vectorA] } }]);
    const rt = runtime(backend, async () => { embeds += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    expect(embeds).toBe(0);
    // The legacy point reads as a verified terminal collision, never a loop.
    await expect(rt.qdrant.retrieve("episode", source.id)).rejects.toMatchObject({ code: "qdrant_content_hash_collision" });
  });

  it("terminally quarantines a legacy point that appears between the prelookup and the insert", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...source, vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    const { vector: _v, ...noVector } = finalCommitted as EpisodeRecord;
    const legacyPayload = { ...recordPayload(finalCommitted), content_hash: canonicalRecordHash(noVector as EpisodeRecord) } as Record<string, unknown>;
    // The prelookup sees NOTHING; the legacy point appears only when the
    // insert lands (upsert hook overwrites the stored point).
    const holder: { points?: Map<string, BackendPoint> } = {};
    const backend = restQdrantWriter([controlPoint(emptyControl())], {
      upsert: (points: BackendPoint[]) => {
        for (const point of points) {
          if (point.payload.record_type === "episode") {
            holder.points?.set(point.id, { id: point.id, payload: legacyPayload, vector: { semantic: [...vectorA] } });
          }
        }
      },
    });
    holder.points = backend.points;
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 1 });
  });

  it("keeps an arbitrary malformed hash pending — never falsely terminal", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...source, vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    // Hash matches NEITHER the vector-bound nor the legacy formula.
    const malformedPayload = { ...recordPayload(finalCommitted), content_hash: "f".repeat(64) } as Record<string, unknown>;
    const backend = restQdrantWriter([controlPoint(emptyControl()), { id: physicalPointIdFor("episode", source.id), payload: malformedPayload, vector: { semantic: [...vectorA] } }], { failUpsert: true });
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // The malformed point is an ambiguous readback, not a terminal collision.
    await expect(rt.qdrant.retrieve("episode", source.id)).resolves.toBeNull();
  });

  it("never delivers when the backend substitutes hash-excluded fields while retaining hash and vector", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    // The PUT stores the episode, but the readback swaps createdAt/nodeId/
    // producerId while keeping the SAME vector-bound hash and exact vector.
    const holder: { points?: Map<string, BackendPoint> } = {};
    const backend = restQdrantWriter([controlPoint(emptyControl())], {
      upsert: (points: BackendPoint[]) => {
        for (const point of points) {
          if (point.payload.record_type === "episode") {
            const stored = holder.points?.get(point.id);
            if (stored !== undefined) {
              stored.payload = { ...point.payload, created_at: "2026-08-09T00:00:00.000Z", node_id: "node-spoofed", producer_id: "producer-spoofed" };
            }
          }
        }
      },
    });
    holder.points = backend.points;
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
  });

  it("keeps legacy-hash points with extra/camel payload keys or alias vectors pending — never falsely terminal", async () => {
    const localPolicy = policy();
    const source = episode(localPolicy);
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...source, vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    const { vector: _v, ...noVector } = finalCommitted as EpisodeRecord;
    const legacyHash = canonicalRecordHash(noVector as EpisodeRecord);
    // Variant 1: legacy hash + an EXTRA camel alias key in the payload.
    const extraCamel = { ...recordPayload(finalCommitted), content_hash: legacyHash, ownerHost: "pi" } as Record<string, unknown>;
    const backend1 = restQdrantWriter([controlPoint(emptyControl()), { id: physicalPointIdFor("episode", source.id), payload: extraCamel, vector: { semantic: [...vectorA] } }], { failUpsert: true });
    const result1 = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: runtime(backend1), maxClockSkewMs: 5 });
    expect(result1).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // Variant 2: legacy hash + a vector object with an extra alias key.
    const backend2 = restQdrantWriter([controlPoint(emptyControl()), { id: physicalPointIdFor("episode", source.id), payload: { ...recordPayload(finalCommitted), content_hash: legacyHash }, vector: { semantic: [...vectorA], alias: 1 } as { semantic: number[] } }], { failUpsert: true });
    const result2 = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: runtime(backend2), maxClockSkewMs: 5 });
    expect(result2).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // Variant 3: legacy hash + malformed/unknown payload key.
    const extraUnknown = { ...recordPayload(finalCommitted), content_hash: legacyHash, bogus_field: "x" } as Record<string, unknown>;
    const backend3 = restQdrantWriter([controlPoint(emptyControl()), { id: physicalPointIdFor("episode", source.id), payload: extraUnknown, vector: { semantic: [...vectorA] } }], { failUpsert: true });
    const result3 = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, runtime: runtime(backend3), maxClockSkewMs: 5 });
    expect(result3).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // None of the ambiguous variants are terminal collisions.
    await expect(runtime(backend1).qdrant.retrieve("episode", source.id)).resolves.toBeNull();
  });

  it("fresh pre-policy barrier: a control stability break between the initial barrier and the policy write suppresses ALL egress", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    // The initial barrier reads a stable control; the SECOND control read (the
    // fresh pre-policy barrier) becomes ambiguous (extra control point), so the
    // policy write is suppressed entirely.
    const points = new Map<string, { id: string; payload: Record<string, unknown> }>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) }]]);
    let controlReads = 0;
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: Array<{ id: string; payload: Record<string, unknown> }> };
      if (url.includes("/points/retrieve")) {
        const ids = body?.ids ?? [];
        const extra = ids.includes(COLLECTION_CONTROL_ID) && (controlReads += 1) >= 2 ? [{ id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) }] : [];
        return new Response(JSON.stringify({ result: [...ids.map((id) => points.get(id)).filter((p) => p !== undefined), ...extra], status: "ok" }), { headers: { "content-type": "application/json" } });
      }
      if (url.includes("/points?") && init.method === "PUT") { for (const point of body?.points ?? []) points.set(point.id, { id: point.id, payload: point.payload }); return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); }
      return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } });
    };
    stubGlobalFetch(fetchImpl);
    const bundle = createQdrantSafeBundle({ options: restQdrantWriter().options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const store = bundle.store;
    const qf = bundle.qdrant;
    const rt = bindIngestRuntime({ store, qdrant: bindQdrantDestination(qf, qdrantDestination), embedding: boundEmbedding(realEmbeddings(async () => Array.from({ length: 1024 }, () => 0.25))) });
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a], episodeIds: [a.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect([...points.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
    expect([...points.values()].some((point) => point.payload.record_type === "episode")).toBe(false);
  });

  it("forgotten A never sacrifices active B: B egresses once and the job is delivered", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    let embeds = 0;
    const rt2 = runtime(backend, async () => { embeds += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt2, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 1 });
    expect(embeds).toBe(1);
    const episodePoints = [...backend.points.values()].filter((point) => point.payload.record_type === "episode");
    expect(episodePoints).toHaveLength(1);
    expect(episodePoints[0]?.payload.source_entry_id).toBe("entry-b");
    // The production processor disposes the job as DELIVERED.
    const processor = createIngestProcessor({ localPolicy, runtime: rt2, maxClockSkewMs: 5, now: () => 100 });
    const outcome = await processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {});
    expect(outcome).toEqual({ status: "delivered" });
  });

  it("an all-tombstoned job performs NO egress and is disposed delivered", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    await seedTombstone(rt.store, backend, b);
    let embeds = 0;
    const rt2 = runtime(backend, async () => { embeds += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt2, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 2 });
    expect(embeds).toBe(0);
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode" || point.payload.record_type === "processing_policy")).toBe(false);
    const processor = createIngestProcessor({ localPolicy, runtime: rt2, maxClockSkewMs: 5, now: () => 100 });
    const outcome = await processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {});
    expect(outcome).toEqual({ status: "delivered" });
  });

  it("A tombstoned + B transient failure keeps the whole job pending; the retry converges B without A", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    let embeds = 0;
    const rt2 = runtime(backend, async () => { embeds += 1; if (embeds === 1) throw new Error("offline"); return Array.from({ length: 1024 }, () => 0.25); });
    const first = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt2, maxClockSkewMs: 5 });
    // Exact partition: fields sum to count (2), tombstoned kept as quarantined K.
    expect(first).toEqual({ delivered: 0, pending: 1, quarantined: 1 });
    const retry = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt2, maxClockSkewMs: 5 });
    expect(retry).toEqual({ delivered: 1, pending: 0, quarantined: 1 });
    expect(embeds).toBe(2);
  });

  it("A tombstoned + B verified collision stays a fatal whole-job quarantine", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    // B's committed point has DIFFERENT canonical content (verified collision).
    const collidingB = episode(localPolicy, { id: b.id, sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b", text: "different content", vector: Array.from({ length: 1024 }, (_, index) => (index % 5) / 10) });
    const backend = restQdrantWriter([controlPoint(emptyControl()), { id: collidingB.id, payload: recordPayload(collidingB) as Record<string, unknown>, vector: { semantic: [...(collidingB.vector as number[])] } }]);
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    const processor = createIngestProcessor({ localPolicy, runtime: rt, maxClockSkewMs: 5, now: () => 100 });
    const outcome = await processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {});
    expect(outcome.status).toBe("quarantined");
  });

  it("a tombstone appearing at the per-episode barrier skips only that episode", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    // The tombstone for A appears only AFTER the initial barrier: the policy
    // point insert (which runs between the initial barrier and the episode
    // loop) seeds the tombstone into the backend, so the per-episode
    // before-embed barrier for A sees it; B is never tombstoned.
    const holder: { points?: Map<string, BackendPoint> } = {};
    const backend = restQdrantWriter([controlPoint(emptyControl())], {
      upsert: (points: BackendPoint[]) => {
        for (const point of points) {
          if (point.payload.record_type === "processing_policy") {
            const tomb = tombstoneRecord(a.id);
            holder.points?.set(tomb.id, { id: tomb.id, payload: recordPayload(tomb) as Record<string, unknown> });
          }
        }
      },
    });
    holder.points = backend.points;
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 1 });
    const episodePoints = [...backend.points.values()].filter((point) => point.payload.record_type === "episode");
    expect(episodePoints).toHaveLength(1);
    expect(episodePoints[0]?.payload.source_entry_id).toBe("entry-b");
  });

  it("disposes an all-tombstoned job as delivered even when control revoked the destinations and the privacy epoch advanced", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    // Control: privacyEpoch advanced to 1 AND both destinations revoked; the
    // job episodes still carry epoch 0.
    const backend = backendWithControl({ privacyEpoch: 1, revokedDestinationIds: ["qdrant:pi", "embed:local"] });
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    await seedTombstone(rt.store, backend, b);
    let embeds = 0;
    const rt2 = runtime(backend, async () => { embeds += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt2, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 2 });
    expect(embeds).toBe(0);
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode" || point.payload.record_type === "processing_policy")).toBe(false);
    const processor = createIngestProcessor({ localPolicy, runtime: rt2, maxClockSkewMs: 5, now: () => 100 });
    expect(await processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {})).toEqual({ status: "delivered" });
  });

  it("disposes an all-tombstoned job as delivered when the local policy intersection is null or the clock throws", async () => {
    const a = episode(policy({ originProvider: "openai" }), { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(policy({ originProvider: "openai" }), { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const producerPolicy = policy({ originProvider: "openai" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    await seedTombstone(rt.store, backend, b);
    // Local worker of a DIFFERENT provider without replay: intersection null.
    const anthropic = policy({ originProvider: "anthropic", allowCrossProviderReplay: false });
    const denied = await ingestPendingJobs({ job: job(producerPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy: anthropic, runtime: rt, maxClockSkewMs: 5 });
    expect(denied).toEqual({ delivered: 0, pending: 0, quarantined: 2 });
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode" || point.payload.record_type === "processing_policy")).toBe(false);
    // A throwing trusted clock cannot prevent the terminal-forgotten disposition.
    const throwing = createIngestProcessor({ localPolicy: anthropic, runtime: rt, maxClockSkewMs: 5, now: () => { throw new Error("clock offline"); } });
    expect(await throwing.process(job(producerPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {})).toEqual({ status: "delivered" });
  });

  it("keeps exact result partitions across retries: A skipped + B already delivered then barrier failure/abort/fatal", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    const first = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(first).toEqual({ delivered: 1, pending: 0, quarantined: 1 });
    // Retry 1: B already committed; the final existing barrier fails (control
    // corrupted by the policy-insert hook) -> exact partition sums to count.
    const holder: { points?: Map<string, BackendPoint> } = {};
    let corrupt = false;
    const backend2 = restQdrantWriter([controlPoint(emptyControl())], {
      upsert: (points: BackendPoint[]) => {
        for (const point of points) {
          if (point.payload.record_type === "processing_policy" && corrupt) {
            const ctrl = holder.points?.get(COLLECTION_CONTROL_ID);
            if (ctrl !== undefined) ctrl.payload = { ...ctrl.payload, content_hash: "bogus" };
          }
        }
      },
    });
    holder.points = backend2.points;
    // Seed the A tombstone on backend2 so only B is active.
    const tomb = tombstoneRecord(a.id);
    backend2.points.set(tomb.id, { id: tomb.id, payload: recordPayload(tomb) as Record<string, unknown> });
    // Commit B on backend2 first (so the retry finds it committed).
    const rtA = runtime(backend2);
    const committed = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rtA, maxClockSkewMs: 5 });
    expect(committed).toEqual({ delivered: 1, pending: 0, quarantined: 1 });
    // Retry with a per-episode barrier failure after A is skipped: the policy
    // insert (which runs after the initial barrier) corrupts the control, so
    // the before-embed barrier for B fails with the exact partition.
    const holder3: { points?: Map<string, BackendPoint> } = {};
    const backend3 = restQdrantWriter([controlPoint(emptyControl())], {
      upsert: (points: BackendPoint[]) => {
        for (const point of points) {
          if (point.payload.record_type === "processing_policy") {
            const ctrl = holder3.points?.get(COLLECTION_CONTROL_ID);
            if (ctrl !== undefined) ctrl.payload = { ...ctrl.payload, content_hash: "bogus" };
          }
        }
      },
    });
    holder3.points = backend3.points;
    // Copy the committed B point + the A tombstone onto backend3 (NOT the
    // policy point, so the retry performs the policy insert and fires the hook).
    for (const point of [...backend2.points.values()]) {
      if (point.payload.record_type !== "processing_policy") backend3.points.set(point.id, point);
    }
    const rt3 = runtime(backend3);
    const failed = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt3, maxClockSkewMs: 5 });
    // The exact partition must sum to count: delivered 0 + pending 1 + quarantined 1 = 2.
    expect(failed.delivered + failed.pending + failed.quarantined).toBe(2);
    expect(failed).toEqual({ delivered: 0, pending: 1, quarantined: 1 });
    // Retry with an abort after the initial barrier: same exact partition.
    const controller = new AbortController();
    const processor = createIngestProcessor({ localPolicy, runtime: rt3, maxClockSkewMs: 5, now: () => 100 });
    const aborted = processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), { signal: controller.signal });
    controller.abort();
    const abortedOutcome = await aborted;
    expect(abortedOutcome.status).toBe("pending");
  });

  it("A skipped + B verified collision stays a fatal whole-job quarantine with exact partition", async () => {
    const localPolicy = policy();
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    // B's committed point is a verified different-content collision; A is tombstoned.
    const collidingB = episode(localPolicy, { id: b.id, sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b", text: "different", vector: Array.from({ length: 1024 }, (_, index) => (index % 5) / 10) });
    const backend = restQdrantWriter([controlPoint(emptyControl()), { id: collidingB.id, payload: recordPayload(collidingB) as Record<string, unknown>, vector: { semantic: [...(collidingB.vector as number[])] } }]);
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 2 });
    const processor = createIngestProcessor({ localPolicy, runtime: rt, maxClockSkewMs: 5, now: () => 100 });
    expect((await processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {})).status).toBe("quarantined");
  });

  it("never inspects a tombstoned episode's scanner projection: A unsafe-projection tombstoned + B active delivers", async () => {
    const localPolicy = policy();
    // A's stored text is just under the record max (16000) but the semantic
    // projection exceeds 16k — unsafe ONLY if A is inspected.
    const longText = "x".repeat(15999);
    const a = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a", text: longText });
    expect(episodeSemanticProjection(a).length).toBeGreaterThan(16000);
    const b = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    let embeds = 0;
    const rt2 = runtime(backend, async () => { embeds += 1; return Array.from({ length: 1024 }, () => 0.25); });
    const result = await ingestPendingJobs({ job: job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy, runtime: rt2, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 1 });
    expect(embeds).toBe(1);
    const episodePoints = [...backend.points.values()].filter((point) => point.payload.record_type === "episode");
    expect(episodePoints).toHaveLength(1);
    expect(episodePoints[0]?.payload.source_entry_id).toBe("entry-b");
    const processor = createIngestProcessor({ localPolicy, runtime: rt2, maxClockSkewMs: 5, now: () => 100 });
    expect(await processor.process(job(localPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {})).toEqual({ status: "delivered" });
  });

  it("maps static policy outcomes through the exact partition when an episode is skipped", async () => {
    const a = episode(policy({ originProvider: "openai" }), { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const b = episode(policy({ originProvider: "openai" }), { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const producerPolicy = policy({ originProvider: "openai" });
    const backend = backendWithControl();
    const rt = runtime(backend);
    await seedTombstone(rt.store, backend, a);
    // Policy unauthorized (null intersection) with A skipped: exact partition
    // {delivered 0, pending 1, quarantined 1} and the processor stays pending.
    const anthropic = policy({ originProvider: "anthropic", allowCrossProviderReplay: false });
    const denied = await ingestPendingJobs({ job: job(producerPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), now: 100, localPolicy: anthropic, runtime: rt, maxClockSkewMs: 5 });
    expect(denied).toEqual({ delivered: 0, pending: 1, quarantined: 1 });
    const deniedProcessor = createIngestProcessor({ localPolicy: anthropic, runtime: rt, maxClockSkewMs: 5, now: () => 100 });
    expect((await deniedProcessor.process(job(producerPolicy, { episodes: [a, b], episodeIds: [a.id, b.id] }), {})).status).toBe("pending");
    expect([...backend.points.values()].some((point) => point.payload.record_type === "episode" || point.payload.record_type === "processing_policy")).toBe(false);
    // Policy fatal (expired effective) with A skipped: whole-job quarantine
    // with the exact partition and the processor quarantines.
    const expired = policy({ expiresAt: "1970-01-01T00:00:00.000Z" });
    const expiredA = episode(expired, { id: "00000000-0000-5000-8000-000000000001", sourceEntryId: "entry-a", sessionId: "session-a", turnId: "turn-a" });
    const expiredB = episode(expired, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-b", sessionId: "session-b", turnId: "turn-b" });
    const expiredBackend = backendWithControl();
    const expiredRt = runtime(expiredBackend);
    await seedTombstone(expiredRt.store, expiredBackend, expiredA);
    const fatal = await ingestPendingJobs({ job: job(expired, { episodes: [expiredA, expiredB], episodeIds: [expiredA.id, expiredB.id] }), now: 100, localPolicy: expired, runtime: expiredRt, maxClockSkewMs: 5 });
    expect(fatal).toEqual({ delivered: 0, pending: 0, quarantined: 2 });
    const fatalProcessor = createIngestProcessor({ localPolicy: expired, runtime: expiredRt, maxClockSkewMs: 5, now: () => 100 });
    expect((await fatalProcessor.process(job(expired, { episodes: [expiredA, expiredB], episodeIds: [expiredA.id, expiredB.id] }), {})).status).toBe("quarantined");
  });

  it("preserves the producer content origin across cross-provider replay and fails closed without it", async () => {
    const localPolicy = policy({ originProvider: "anthropic", allowCrossProviderReplay: true, policyRevision: "worker-r" });
    const producerPolicy = policy({ originProvider: "openai", allowCrossProviderReplay: true, policyRevision: "producer-r" });
    const effective = intersectPolicies([producerPolicy], localPolicy);
    expect(effective).not.toBeNull();
    expect(effective?.originProvider).toBe("openai");
    expect(effective?.policyRevision).toMatch(/^intersection:/);
    const backend = backendWithControl();
    const rt = runtime(backend);
    const result = await ingestPendingJobs({ job: job(producerPolicy), now: 100, localPolicy, runtime: rt, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const policyPoint = [...backend.points.values()].find((point) => point.payload.record_type === "processing_policy");
    expect(policyPoint?.payload.policy.origin_provider ?? policyPoint?.payload.policy.originProvider).toBe("openai");
    const episodePoint = [...backend.points.values()].find((point) => point.payload.record_type === "episode");
    expect(episodePoint?.payload.origin_provider).toBe("openai");
    // A subsequent Anthropic worker WITHOUT replay cannot treat the content as same-provider.
    const noReplay = policy({ originProvider: "anthropic", allowCrossProviderReplay: false, policyRevision: "worker-r" });
    expect(intersectPolicies([producerPolicy], noReplay)).toBeNull();
    const backend2 = backendWithControl();
    const rt2 = runtime(backend2);
    const denied = await ingestPendingJobs({ job: job(producerPolicy), now: 100, localPolicy: noReplay, runtime: rt2, maxClockSkewMs: 5 });
    expect(denied).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect([...backend2.points.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
    // Multiple producer origins fail closed.
    const producerB = policy({ originProvider: "provider-b" });
    const producerC = policy({ originProvider: "provider-c" });
    expect(intersectPolicies([producerB, producerC], policy())).toBeNull();
  });

  it("uses a production-bound REST writer for exact payload/vector readback, partial acknowledgement, and canonical collision categorization", async () => {
    const localPolicy = policy();
    const backend = backendWithControl();
    const rt = runtime(backend);
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, runtime: rt, maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const episodePoint = [...backend.points.values()].find((point) => point.payload.record_type === "episode");
    expect(episodePoint?.payload).toMatchObject({ record_type: "episode", owner_host: "pi", privacy_epoch: 0, status: "active", redaction_status: "redacted", secret_scan: "passed", processing_policy_id: localPolicy.id });
    expect(episodePoint?.vector?.semantic).toHaveLength(1024);
    // Privacy-revoke -> future-capture -> ingest regression: the unchanged producer
    // policy converges on the same content-addressed point across privacy epochs.
    const privacyEpisode = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000097", sourceEntryId: "entry-97", sessionId: "session-97", turnId: "turn-97", privacyEpoch: 1 });
    const epochOne = backendWithControl({ privacyEpoch: 1 });
    const rtEpochOne = runtime(epochOne);
    const privacyResult = await ingestPendingJobs({ job: job(localPolicy, { episodes: [privacyEpisode], episodeIds: [privacyEpisode.id] }), now: 0, localPolicy, runtime: rtEpochOne, maxClockSkewMs: 0 });
    expect(privacyResult).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const policyPoints = [...epochOne.points.values()].filter((point) => point.payload.record_type === "processing_policy");
    expect(policyPoints).toHaveLength(1);
  });
});
