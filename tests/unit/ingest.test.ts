import { describe, expect, it, vi } from "vitest";
import {
  createIngestProcessor,
  ingestPendingJobs,
  type BoundEmbeddingDestination,
  type BoundQdrantDestination,
  type IngestControlReader,
} from "../../src/outbox/delivery.js";
import { bindQdrantDestination, bindQdrantSessionWriter, createQdrantDestinationFactory, QdrantContentHashCollisionError, ValidatedQdrantSessionWriter } from "../../src/qdrant/write.js";
import { EmbeddingsClient, bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory } from "../../src/clients/embeddings.js";
import { canonicalRecordHash, episodeSemanticProjection } from "../../src/domain/records.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../../src/domain/canonical.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import type { AuthorizedDestination } from "../../src/types.js";
import type { EpisodeRecord, MemoryRecord, ProcessingPolicyRecord } from "../../src/domain/records.js";
import type { OutboxJob } from "../../src/outbox/store.js";
import { createQdrantSessionWriter } from "../../src/qdrant/client.js";

const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination: AuthorizedDestination = { id: "embed:local", residency: "local", dataUse: "memory" };
const llmDestination: AuthorizedDestination = { id: "llm:local", residency: "local", dataUse: "memory" };
const coordination = { policyHash: "coordination-policy-hash", policyEpoch: 7 } as const;
function factoryQdrantClient(endpoint: string, overrides: Partial<Record<"upsertPoints" | "retrieve", (...args: never[]) => unknown>> = {}) {
  return bindQdrantSessionWriter({ endpoint, client: { endpoint, ownerHost: "pi", collection: "pi_memory", maxClockSkewMs: 0, upsertPoints: async () => undefined, retrieve: async () => [], ...overrides } as never });
}
function factoryEmbeddingClient(endpoint: string, embedDocument: (input: { model: string; text: string; signal?: AbortSignal }) => Promise<readonly number[]> = async () => Array.from({ length: 1024 }, () => 0.25)) {
  return bindEmbeddingDocumentClient({ endpoint, client: { endpoint, embedDocument } });
}

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const pending = {
    id: "pending", ownerHost: "pi" as const,
    destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null,
    residency: "local", dataUse: "memory", policyRevision: "v1", ...overrides,
  } satisfies ProcessingPolicy;
  return { ...pending, id: processingPolicyHash(pending) };
}

function episode(current: ProcessingPolicy, overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const pending = {
    recordType: "episode" as const,
    id: "00000000-0000-5000-8000-000000000001", ownerHost: "pi" as const, schemaRevision: 1 as const,
    createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: current.id,
    expiresAt: current.expiresAt, contentHash: "pending", sourceEntryId: "entry-1", host: "pi" as const,
    projectId: "project-1", projectIdentityKind: "registered" as const, sessionId: "session-1", turnId: "turn-1",
    agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: "2026-08-10T00:00:00.000Z",
    modelId: "capture-model", embeddingDimension: 1024, originProvider: current.originProvider,
    destinationId: current.destinationIds.qdrant, status: "active" as const, redactionStatus: "redacted" as const,
    secretScan: "passed" as const, text: "safe [token redacted]", ...overrides,
  };
  const value = { ...pending, ...overrides } as EpisodeRecord;
  for (const key of ["text", "toolName", "toolArgs", "errorFingerprint", "producerId", "nodeId"] as const) if (value[key] === undefined) delete (value as Partial<EpisodeRecord>)[key];
  return { ...value, contentHash: canonicalRecordHash(value) };
}

function job(current: ProcessingPolicy, overrides: Partial<OutboxJob> = {}): OutboxJob {
  const episodes = overrides.episodes ?? [episode(current)];
  const id = deterministicUuid("pi-qdrant-memory-v2:outbox-job", "pi", episodes.map((entry) => entry.id), current.id);
  const pending = {
    version: 1 as const, id, ownerHost: "pi" as const, nodeId: "node-redacted",
    producerUuid: "00000000-0000-4000-8000-000000000001", createdAt: "2026-08-10T00:00:00.000Z",
    deadline: current.expiresAt, policyId: current.id, policy: current, episodeIds: episodes.map((entry) => entry.id), episodes,
    auditHash: "pending", ...overrides,
  } as OutboxJob;
  const { auditHash: _auditHash, ...withoutAudit } = pending;
  return { ...pending, auditHash: sha256Hex(canonicalStringify(withoutAudit)) } as OutboxJob;
}

function control(current: ProcessingPolicy, snapshots?: Array<ReturnType<typeof snapshot>>): IngestControlReader {
  const values = snapshots ?? [snapshot(current), snapshot(current), snapshot(current)];
  return { read: vi.fn(async () => values.shift() ?? snapshot(current)) };
}
function snapshot(_current: ProcessingPolicy, overrides: Partial<{ state: "active" | "draining" | "retired"; privacyEpoch: number; coordinationPolicyEpoch: number; policyHash: string; revokedDestinationIds: readonly string[] }> = {}) {
  return { state: "active" as const, privacyEpoch: 0, coordinationPolicyEpoch: coordination.policyEpoch, policyHash: coordination.policyHash, revokedDestinationIds: [] as readonly string[], ...overrides };
}

function fakeBoundQdrant(destination = qdrantDestination, options: { collision?: boolean; existing?: readonly EpisodeRecord[]; dropEpisodeVector?: boolean } = {}): BoundQdrantDestination & { insertAndReadback: ReturnType<typeof vi.fn>; retrieve: ReturnType<typeof vi.fn> } {
  const records = new Map<string, MemoryRecord>((options.existing ?? []).map((record) => [record.id, record]));
  const insertAndReadback = vi.fn(async (record: ProcessingPolicyRecord | EpisodeRecord) => {
    const current = records.get(record.id);
    if (options.collision && record.recordType === "episode") throw new QdrantContentHashCollisionError();
    if (current !== undefined) {
      if (current.contentHash !== record.contentHash) throw new Error("content hash collision");
      return "existing" as const;
    }
    records.set(record.id, record);
    return "inserted" as const;
  });
  const retrieve = vi.fn(async <T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string) => {
    const found = records.get(id) as T | undefined;
    if (found === undefined) return null;
    if (options.dropEpisodeVector && recordType === "episode") return { ...(found as EpisodeRecord), vector: undefined } as T;
    return found;
  });
  return { destination, ownerHost: "pi", collection: "pi_memory", coordination, insertAndReadback, retrieve };
}
function fakeBoundEmbedding(destination = embeddingDestination, embed = vi.fn(async () => Array.from({ length: 1024 }, () => 0.25))): BoundEmbeddingDestination & { embed: typeof embed } {
  return { destination, embed };
}

describe("Task 7 redacted outbox ingest", () => {
  it("binds exactly one configured destination identity; no ID allowlist can switch endpoints", () => {
    const wrongQdrant = createQdrantDestinationFactory({ endpoint: "http://qdrant-b", destination: { ...qdrantDestination, id: "qdrant:b" }, client: factoryQdrantClient("http://qdrant-b"), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const wrongEmbedding = createEmbeddingDestinationFactory({ endpoint: "http://embed-b", destination: { ...embeddingDestination, id: "embed:b" }, client: factoryEmbeddingClient("http://embed-b"), egressMode: "allowlist" });
    expect(() => bindQdrantDestination(wrongQdrant, qdrantDestination)).toThrow(/destination/i);
    expect(() => bindEmbeddingDestination(wrongEmbedding, embeddingDestination)).toThrow(/destination/i);
  });

  it("requires an explicit endpoint-paired client seam and snapshots mutable factory inputs", async () => {
    const qEndpointA = "http://qdrant-a"; const qEndpointB = "http://qdrant-b"; const embedEndpointA = "http://embed-a/v1"; const embedEndpointB = "http://embed-b/v1";
    expect(() => bindQdrantSessionWriter({ endpoint: qEndpointA, client: { endpoint: qEndpointB, ownerHost: "pi", collection: "pi_memory", maxClockSkewMs: 0, retrieve: async () => [], upsertPoints: async () => undefined } as never })).toThrow(/pairing/i);
    expect(() => bindEmbeddingDocumentClient({ endpoint: embedEndpointA, client: { endpoint: embedEndpointB, embedDocument: async () => [] } })).toThrow(/pairing/i);
    const qCalls: string[] = []; const rawQdrant = { endpoint: qEndpointA, ownerHost: "pi", collection: "pi_memory", maxClockSkewMs: 0, retrieve: async () => { qCalls.push("a"); return []; }, upsertPoints: async () => undefined };
    const oldQdrant = bindQdrantSessionWriter({ endpoint: qEndpointA, client: rawQdrant as never }); expect(() => { rawQdrant.retrieve = async () => { qCalls.push("b"); return []; }; }).toThrow(); expect(() => { rawQdrant.endpoint = qEndpointB; }).toThrow();
    const qInput = { endpoint: qEndpointA, destination: qdrantDestination, client: oldQdrant, egressMode: "allowlist" as const, coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch };
    const qFactory = createQdrantDestinationFactory(qInput); qInput.endpoint = qEndpointB; qInput.client = factoryQdrantClient(qEndpointB); qInput.destination = { ...qdrantDestination, id: "qdrant:b" };
    await expect(bindQdrantDestination(qFactory, qdrantDestination).retrieve("episode", "00000000-0000-5000-8000-000000000001")).resolves.toBeNull(); expect(qCalls).toEqual(["a"]);
    const embedCalls: string[] = []; const rawEmbedding = { endpoint: embedEndpointA, embedDocument: async () => { embedCalls.push("a"); return Array.from({ length: 1024 }, () => 0.25); } };
    const oldEmbedding = bindEmbeddingDocumentClient({ endpoint: embedEndpointA, client: rawEmbedding }); expect(() => { rawEmbedding.embedDocument = async () => { embedCalls.push("b"); return Array.from({ length: 1024 }, () => 0.25); }; }).toThrow(); expect(() => { rawEmbedding.endpoint = embedEndpointB; }).toThrow();
    const embeddingInput = { endpoint: embedEndpointA, destination: embeddingDestination, client: oldEmbedding, egressMode: "allowlist" as const };
    const embeddingFactory = createEmbeddingDestinationFactory(embeddingInput); embeddingInput.endpoint = embedEndpointB; embeddingInput.client = factoryEmbeddingClient(embedEndpointB); embeddingInput.destination = { ...embeddingDestination, id: "embed:b" };
    await expect(bindEmbeddingDestination(embeddingFactory, embeddingDestination).embed({ model: "bge-m3", text: "safe" })).resolves.toHaveLength(1024); expect(embedCalls).toEqual(["a"]);
    const fetchA = vi.fn(async () => new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] }), { headers: { "content-type": "application/json" } }));
    const mutableOptions = { baseUrl: embedEndpointA, model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100, fetchImpl: fetchA }; const frozenClient = new EmbeddingsClient(mutableOptions);
    mutableOptions.baseUrl = embedEndpointB; mutableOptions.model = "other"; await frozenClient.embedDocument({ model: "bge-m3", text: "safe" });
    expect(String(fetchA.mock.calls[0]?.[0])).toContain(embedEndpointA); expect(JSON.parse(String(fetchA.mock.calls[0]?.[1]?.body))).toMatchObject({ model: "bge-m3" });
  });

  it("requires immutable host-scoped Qdrant collections before client construction or capability binding", () => {
    const fetchImpl = vi.fn();
    for (const [ownerHost, collection] of [["pi", "prime_memory"], ["prime", "pi_memory"], ["pi", "memory"]] as const) {
      expect(() => createQdrantSessionWriter({ baseUrl: "http://qdrant", collection, ownerHost, timeoutMs: 100, fetchImpl })).toThrow(/collection/i);
      const raw = { endpoint: "http://qdrant", collection, ownerHost, maxClockSkewMs: 0, retrieve: vi.fn(async () => []), upsertPoints: vi.fn(async () => undefined) };
      expect(() => bindQdrantSessionWriter({ endpoint: "http://qdrant", client: raw as never })).toThrow(/collection/i);
      expect(raw.retrieve).not.toHaveBeenCalled(); expect(raw.upsertPoints).not.toHaveBeenCalled();
    }
    expect(fetchImpl).not.toHaveBeenCalled();
    const raw = { endpoint: "http://qdrant", collection: "pi_memory", ownerHost: "pi" as const, maxClockSkewMs: 0, retrieve: vi.fn(async () => []), upsertPoints: vi.fn(async () => undefined) };
    bindQdrantSessionWriter({ endpoint: "http://qdrant", client: raw as never });
    expect(() => { raw.collection = "prime_memory"; }).toThrow();
    const forged = Object.create(ValidatedQdrantSessionWriter.prototype) as ValidatedQdrantSessionWriter;
    Object.defineProperties(forged, { endpoint: { value: "http://qdrant" }, ownerHost: { value: "pi" }, collection: { value: "prime_memory" } });
    expect(() => createQdrantDestinationFactory({ endpoint: "http://qdrant", destination: qdrantDestination, client: forged, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch })).toThrow(/collection/i);
    expect(raw.retrieve).not.toHaveBeenCalled(); expect(raw.upsertPoints).not.toHaveBeenCalled();
  });

  it("fails closed before control or egress when a bound Qdrant host/collection cannot serve the job host", async () => {
    const localPolicy = policy();
    for (const binding of [{ ownerHost: "prime" as const, collection: "prime_memory" as const }, { ownerHost: "pi" as const, collection: "prime_memory" as const }]) {
      const original = fakeBoundQdrant(); const qdrant = { ...original, ...binding } as BoundQdrantDestination & { insertAndReadback: typeof original.insertAndReadback; retrieve: typeof original.retrieve };
      const embedding = fakeBoundEmbedding(); const reader = control(localPolicy);
      await expect(ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, qdrant, embedding, control: reader, maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
      expect(reader.read).not.toHaveBeenCalled(); expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(embedding.embed).not.toHaveBeenCalled();
    }
  });

  it("validates bound destinations, persists the producer policy, then embeds and inserts a named semantic episode", async () => {
    const localPolicy = policy({ policyRevision: "local-v1" });
    const producerPolicy = policy({ policyRevision: "producer-v1" });
    const events: string[] = [];
    const qdrant = fakeBoundQdrant();
    const insert = qdrant.insertAndReadback.getMockImplementation()!;
    const retrieve = qdrant.retrieve.getMockImplementation()!;
    qdrant.insertAndReadback.mockImplementation(async (record: ProcessingPolicyRecord | EpisodeRecord) => { events.push(`insert:${record.recordType}`); return insert(record); });
    qdrant.retrieve.mockImplementation(async <T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string) => { events.push(`read:${recordType}`); return retrieve(recordType, id) as Promise<T | null>; });
    const embed = vi.fn(async (input: { model: string; text: string }) => { events.push("embed"); return Array.from({ length: 1024 }, () => 0.25); });
    const embedding = fakeBoundEmbedding(embeddingDestination, embed);
    const snapshots = [snapshot(localPolicy), snapshot(localPolicy), snapshot(localPolicy)];
    const reader: IngestControlReader = { read: vi.fn(async () => { events.push("control"); return snapshots.shift() ?? snapshot(localPolicy); }) };
    const result = await ingestPendingJobs({ job: job(producerPolicy), now: 100, localPolicy, qdrant, embedding, control: reader, maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    expect(events).toEqual(["control", "insert:processing_policy", "read:processing_policy", "control", "read:episode", "control", "embed", "control", "insert:episode", "read:episode"]);
    expect(qdrant.insertAndReadback.mock.calls[0]?.[0]).toMatchObject({ recordType: "processing_policy", id: producerPolicy.id, canonicalHash: producerPolicy.id, policy: producerPolicy });
    expect(embed).toHaveBeenCalledWith(expect.objectContaining({ model: "bge-m3", text: "event:user\ntext:safe [token redacted]" }));
    const written = qdrant.insertAndReadback.mock.calls[1]?.[0] as EpisodeRecord;
    expect(written).toMatchObject({ recordType: "episode", ownerHost: "pi", schemaRevision: 1, status: "active", redactionStatus: "redacted", secretScan: "passed", processingPolicyId: producerPolicy.id, vector: expect.any(Array) });
    expect(written.vector).toHaveLength(1024);
    expect(written.vector?.every(Number.isFinite)).toBe(true);
    expect(JSON.stringify(embed.mock.calls)).not.toMatch(/secret-token|Bearer/i);
  });

  it.each([
    ["qdrant:pi", "qdrant revocation"],
    ["embed:local", "embedding revocation"],
  ])("never egresses the revoked bound %s destination (%s)", async (revokedDestinationId) => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
    const result = await ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding, control: control(localPolicy, [snapshot(localPolicy, { revokedDestinationIds: [revokedDestinationId] })]), maxClockSkewMs: 5 });
    expect(result).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect(qdrant.insertAndReadback).not.toHaveBeenCalled();
    expect(embedding.embed).not.toHaveBeenCalled();
  });

  it("quarantines expired or scanner-non-passed jobs before any Qdrant or embedding call", async () => {
    const expired = policy({ expiresAt: "1970-01-01T00:00:00.000Z" });
    const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
    await expect(ingestPendingJobs({ job: job(expired), now: 100, localPolicy: expired, qdrant, embedding, control: control(expired), maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(embedding.embed).not.toHaveBeenCalled();
    const safe = policy();
    const rejected = { ...episode(safe), secretScan: "rejected" } as unknown as EpisodeRecord;
    await expect(ingestPendingJobs({ job: job(safe, { episodes: [rejected] }), now: 100, localPolicy: safe, qdrant, embedding, control: control(safe), maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(embedding.embed).not.toHaveBeenCalled();
  });

  it("leaves embedding failures retryable and maps a typed hash collision to stable quarantine", async () => {
    const localPolicy = policy();
    for (const embedding of [
      fakeBoundEmbedding(embeddingDestination, vi.fn(async () => { throw new Error("offline"); })),
      fakeBoundEmbedding(embeddingDestination, vi.fn(async () => Array.from({ length: 1023 }, () => 0.25))),
    ]) {
      const qdrant = fakeBoundQdrant();
      await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
      expect(qdrant.insertAndReadback).toHaveBeenCalledTimes(1);
    }
    const qdrant = fakeBoundQdrant(qdrantDestination, { collision: true });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
  });

  it("rereads control after a delayed embedding and skips the distinct final episode write when state is revoked", async () => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant();
    let release: ((value: readonly number[]) => void) | undefined;
    const embedding = fakeBoundEmbedding(embeddingDestination, vi.fn(() => new Promise<readonly number[]>((resolve) => { release = resolve; })));
    const reader = control(localPolicy, [snapshot(localPolicy), snapshot(localPolicy), snapshot(localPolicy), snapshot(localPolicy, { state: "draining" })]);
    const pending = ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding, control: reader, maxClockSkewMs: 5 });
    await vi.waitFor(() => expect(embedding.embed).toHaveBeenCalledTimes(1));
    release?.(Array.from({ length: 1024 }, () => 0.25));
    await expect(pending).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect(qdrant.insertAndReadback).toHaveBeenCalledTimes(1);
  });

  it("exposes the sole processor seam and reports delivered only after policy and episode readback", async () => {
    const localPolicy = policy(); const processor = createIngestProcessor({ localPolicy, qdrant: fakeBoundQdrant(), embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 5, now: () => 100 });
    await expect(processor.process(job(localPolicy), {})).resolves.toEqual({ status: "delivered" });
  });
  it("stops a privacy-epoch mismatch before the policy record can egress", async () => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding, control: control(localPolicy, [snapshot(localPolicy, { privacyEpoch: 1 })]), maxClockSkewMs: 5 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(embedding.embed).not.toHaveBeenCalled();
  });

  it("passes abort to delayed BGE embedding and never performs the final episode write", async () => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant(); let release: ((value: readonly number[]) => void) | undefined;
    const embedding = fakeBoundEmbedding(embeddingDestination, vi.fn(({ signal }: { signal?: AbortSignal }) => new Promise<readonly number[]>((resolve, reject) => {
      release = resolve; signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    })));
    const processor = createIngestProcessor({ localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 0, now: () => 100 });
    const controller = new AbortController(); const pending = processor.process(job(localPolicy), { signal: controller.signal });
    await vi.waitFor(() => expect(embedding.embed).toHaveBeenCalledTimes(1));
    controller.abort(); release?.(Array.from({ length: 1024 }, () => 0.25));
    await expect(pending).resolves.toMatchObject({ status: "pending", category: "aborted" });
    expect(qdrant.insertAndReadback).toHaveBeenCalledTimes(1);
  });

  it("converges a partially completed multi-episode retry from a verified vector without re-embedding it", async () => {
    const localPolicy = policy(); const first = episode(localPolicy); const second = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000002", sourceEntryId: "entry-2", sessionId: "session-2", turnId: "turn-2" });
    const existing = { ...first, vector: Array.from({ length: 1024 }, () => 0.25) };
    const qdrant = fakeBoundQdrant(qdrantDestination, { existing: [existing] }); const embedding = fakeBoundEmbedding();
    await expect(ingestPendingJobs({ job: job(localPolicy, { episodes: [first, second], episodeIds: [first.id, second.id] }), now: 100, localPolicy, qdrant, embedding, control: control(localPolicy, [snapshot(localPolicy), snapshot(localPolicy), snapshot(localPolicy), snapshot(localPolicy)]), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 2, pending: 0, quarantined: 0 });
    expect(embedding.embed).toHaveBeenCalledTimes(1);
    expect(embedding.embed).toHaveBeenCalledWith(expect.objectContaining({ text: "event:user\ntext:safe [token redacted]" }));
  });

  it("keeps a missing vector readback pending because it is an ambiguous partial acknowledgement", async () => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant(qdrantDestination, { dropEpisodeVector: true });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
  });

  it("uses exact BGE-M3 document input without the query prefix", async () => {
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] }), { headers: { "content-type": "application/json" } }));
    const client = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 100, fetchImpl });
    const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client }), egressMode: "allowlist" });
    await expect(bindEmbeddingDestination(factory, embeddingDestination).embed({ model: "bge-m3", text: "document only" })).resolves.toHaveLength(1024);
    expect(JSON.parse(String(fetchImpl.mock.calls[0]?.[1]?.body))).toEqual({ model: "bge-m3", input: "document only" });
  });

  it("rechecks strict expiry from the live processor clock after embedding", async () => {
    const expiring = policy({ expiresAt: "1970-01-01T00:00:00.001Z" }); const qdrant = fakeBoundQdrant(); let now = 0;
    const embedding = fakeBoundEmbedding(embeddingDestination, vi.fn(async () => { now = 1; return Array.from({ length: 1024 }, () => 0.25); }));
    const processor = createIngestProcessor({ localPolicy: expiring, qdrant, embedding, control: control(expiring), maxClockSkewMs: 0, now: () => now });
    await expect(processor.process(job(expiring), {})).resolves.toMatchObject({ status: "quarantined", category: "expired" });
    expect(qdrant.insertAndReadback).toHaveBeenCalledTimes(1);
  });


  it("fail-closes forged direct audit and membership inputs before control or egress", async () => {
    const localPolicy = policy();
    const first = job(localPolicy);
    const duplicate = job(localPolicy, { episodes: [episode(localPolicy), episode(localPolicy)], episodeIds: [episode(localPolicy).id, episode(localPolicy).id] });
    const forged = { ...first, auditHash: "0".repeat(64) } as OutboxJob;
    for (const candidate of [forged, duplicate]) {
      const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
      await expect(ingestPendingJobs({ job: candidate, now: 100, localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
      expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(qdrant.retrieve).not.toHaveBeenCalled(); expect(embedding.embed).not.toHaveBeenCalled();
    }
  });

  it("materializes and embeds an error-only episode through one safe deterministic projection", async () => {
    const localPolicy = policy();
    const errorOnly = episode(localPolicy, { eventKind: "tool_error", text: undefined, toolArgs: undefined, toolName: "shell", errorFingerprint: "a".repeat(32) });
    const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
    await expect(ingestPendingJobs({ job: job(localPolicy, { episodes: [errorOnly], episodeIds: [errorOnly.id] }), now: 100, localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    expect(embedding.embed).toHaveBeenCalledWith(expect.objectContaining({ text: episodeSemanticProjection(errorOnly) }));
    expect(JSON.stringify(embedding.embed.mock.calls)).not.toContain("a".repeat(32));
  });

  it("keeps generic Qdrant errors and missing or forged readbacks pending, but quarantines only a typed or verified collision", async () => {
    const localPolicy = policy();
    const generic = fakeBoundQdrant();
    generic.insertAndReadback.mockImplementation(async (record: ProcessingPolicyRecord | EpisodeRecord) => { if (record.recordType === "episode") throw new Error("content hash collision"); return "inserted" as const; });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant: generic, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });

    const typed = fakeBoundQdrant(); const typedInsert = typed.insertAndReadback.getMockImplementation()!;
    typed.insertAndReadback.mockImplementation(async (record: ProcessingPolicyRecord | EpisodeRecord) => { if (record.recordType === "episode") throw new QdrantContentHashCollisionError(); return typedInsert(record); });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant: typed, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });

    const missingPolicy = fakeBoundQdrant(); const missingPolicyRead = missingPolicy.retrieve.getMockImplementation()!;
    missingPolicy.retrieve.mockImplementation(async (type, id) => type === "processing_policy" ? null : missingPolicyRead(type, id));
    const missingEmbedding = fakeBoundEmbedding();
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant: missingPolicy, embedding: missingEmbedding, control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    expect(missingEmbedding.embed).not.toHaveBeenCalled();

    const missingEpisode = fakeBoundQdrant(); const missingEpisodeRead = missingEpisode.retrieve.getMockImplementation()!;
    missingEpisode.retrieve.mockImplementation(async (type, id) => type === "episode" ? null : missingEpisodeRead(type, id));
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant: missingEpisode, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });

    const forged = fakeBoundQdrant(); const forgedRead = forged.retrieve.getMockImplementation()!; let episodeRead = 0;
    forged.retrieve.mockImplementation(async (type, id) => { const value = await forgedRead(type, id); if (type === "episode" && ++episodeRead === 2 && value !== null) return { ...(value as EpisodeRecord), privacyEpoch: 99 }; return value; });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant: forged, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });

    const different = fakeBoundQdrant(); const differentRead = different.retrieve.getMockImplementation()!;
    different.retrieve.mockImplementation(async (type, id) => { const value = await differentRead(type, id); if (type === "processing_policy" && value !== null) { const changed = { ...(value as ProcessingPolicyRecord), privacyEpoch: 1, contentHash: "pending" } as ProcessingPolicyRecord; return { ...changed, contentHash: canonicalRecordHash(changed) }; } return value; });
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant: different, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
  });

  it("treats malformed, duplicate, or non-exact revocation snapshots as unavailable and suppresses embedding-only revocation egress", async () => {
    const localPolicy = policy();
    for (const revokedDestinationIds of [[qdrantDestination.id, qdrantDestination.id], ["api-key:forbidden"]]) {
      const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
      await expect(ingestPendingJobs({ job: job(localPolicy), now: 100, localPolicy, qdrant, embedding, control: control(localPolicy, [snapshot(localPolicy, { revokedDestinationIds })]), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
      expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(embedding.embed).not.toHaveBeenCalled();
    }
  });

  it("rechecks abort and expiry after delayed controls and an existing-episode retrieve before any later egress", async () => {
    const localPolicy = policy();
    let releaseControl: ((value: ReturnType<typeof snapshot>) => void) | undefined;
    const delayedControl: IngestControlReader = { read: vi.fn(() => new Promise((resolve) => { releaseControl = resolve; })) };
    const qdrant = fakeBoundQdrant(); const aborting = createIngestProcessor({ localPolicy, qdrant, embedding: fakeBoundEmbedding(), control: delayedControl, maxClockSkewMs: 0, now: () => 0 });
    const controller = new AbortController(); const aborted = aborting.process(job(localPolicy), { signal: controller.signal });
    await vi.waitFor(() => expect(delayedControl.read).toHaveBeenCalledTimes(1)); controller.abort(); releaseControl?.(snapshot(localPolicy));
    await expect(aborted).resolves.toMatchObject({ status: "pending", category: "aborted" });
    expect(qdrant.insertAndReadback).not.toHaveBeenCalled(); expect(qdrant.retrieve).not.toHaveBeenCalled();

    let now = 0; let releaseExpiry: ((value: ReturnType<typeof snapshot>) => void) | undefined;
    const expiring = policy({ expiresAt: "1970-01-01T00:00:00.001Z" }); const expiryControl: IngestControlReader = { read: vi.fn(() => new Promise((resolve) => { releaseExpiry = resolve; })) };
    const expiryQdrant = fakeBoundQdrant(); const expiryProcessor = createIngestProcessor({ localPolicy: expiring, qdrant: expiryQdrant, embedding: fakeBoundEmbedding(), control: expiryControl, maxClockSkewMs: 0, now: () => now });
    const expired = expiryProcessor.process(job(expiring), {}); await vi.waitFor(() => expect(expiryControl.read).toHaveBeenCalledTimes(1)); now = 1; releaseExpiry?.(snapshot(expiring));
    await expect(expired).resolves.toMatchObject({ status: "quarantined", category: "expired" });
    expect(expiryQdrant.insertAndReadback).not.toHaveBeenCalled(); expect(expiryQdrant.retrieve).not.toHaveBeenCalled();

    let releaseRetrieve: ((value: EpisodeRecord | null) => void) | undefined;
    const delayedExisting = fakeBoundQdrant(); const baseRetrieve = delayedExisting.retrieve.getMockImplementation()!;
    delayedExisting.retrieve.mockImplementation(async (type, id) => type === "episode" ? new Promise((resolve) => { releaseRetrieve = resolve; }) : baseRetrieve(type, id));
    const delayedProcessor = createIngestProcessor({ localPolicy, qdrant: delayedExisting, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0, now: () => 0 });
    const delayedController = new AbortController(); const delayed = delayedProcessor.process(job(localPolicy), { signal: delayedController.signal });
    await vi.waitFor(() => expect(releaseRetrieve).toBeTypeOf("function")); delayedController.abort(); releaseRetrieve?.(null);
    await expect(delayed).resolves.toMatchObject({ status: "pending", category: "aborted" });
    expect(delayedExisting.insertAndReadback).toHaveBeenCalledTimes(1);
  });


  it("converges sequential distinct episodes through one existing canonical policy point despite later job timestamps", async () => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
    const first = job(localPolicy, { createdAt: "2026-08-10T00:00:00.000Z" });
    const secondEpisode = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000099", sourceEntryId: "entry-99", sessionId: "session-99", turnId: "turn-99" });
    const second = job(localPolicy, { createdAt: "2026-08-11T00:00:00.000Z", episodes: [secondEpisode], episodeIds: [secondEpisode.id] });
    await expect(ingestPendingJobs({ job: first, now: 100, localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    await expect(ingestPendingJobs({ job: second, now: 100, localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
  });

  it("refreshes control and expiry after a delayed null existing lookup before document egress", async () => {
    const localPolicy = policy(); let release: ((value: EpisodeRecord | null) => void) | undefined;
    const qdrant = fakeBoundQdrant(); const base = qdrant.retrieve.getMockImplementation()!;
    qdrant.retrieve.mockImplementation(async (type, id) => type === "episode" ? new Promise((resolve) => { release = resolve; }) : base(type, id));
    const embedding = fakeBoundEmbedding();
    const revoking = ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, qdrant, embedding, control: control(localPolicy, [snapshot(localPolicy), snapshot(localPolicy), snapshot(localPolicy, { revokedDestinationIds: [embeddingDestination.id] })]), maxClockSkewMs: 0 });
    await vi.waitFor(() => expect(release).toBeTypeOf("function")); release?.(null);
    await expect(revoking).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(embedding.embed).not.toHaveBeenCalled();

    let now = 0; let releaseExpiry: ((value: EpisodeRecord | null) => void) | undefined; const expiring = policy({ expiresAt: "1970-01-01T00:00:00.001Z" });
    const expiryQdrant = fakeBoundQdrant(); const expiryBase = expiryQdrant.retrieve.getMockImplementation()!;
    expiryQdrant.retrieve.mockImplementation(async (type, id) => type === "episode" ? new Promise((resolve) => { releaseExpiry = resolve; }) : expiryBase(type, id));
    const expiryEmbedding = fakeBoundEmbedding(); const processor = createIngestProcessor({ localPolicy: expiring, qdrant: expiryQdrant, embedding: expiryEmbedding, control: control(expiring), maxClockSkewMs: 0, now: () => now });
    const pending = processor.process(job(expiring), {}); await vi.waitFor(() => expect(releaseExpiry).toBeTypeOf("function")); now = 1; releaseExpiry?.(null);
    await expect(pending).resolves.toMatchObject({ status: "quarantined", category: "expired" }); expect(expiryEmbedding.embed).not.toHaveBeenCalled();
  });

  it("rejects unknown control keys and never dereferences malformed processor jobs", async () => {
    const localPolicy = policy(); const qdrant = fakeBoundQdrant(); const embedding = fakeBoundEmbedding();
    const unknownControl: IngestControlReader = { read: async () => ({ ...snapshot(localPolicy), unexpected: true } as never) };
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, qdrant, embedding, control: unknownControl, maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(qdrant.insertAndReadback).not.toHaveBeenCalled();
    const processor = createIngestProcessor({ localPolicy, qdrant: fakeBoundQdrant(), embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0, now: () => 0 });
    await expect(processor.process({ episodes: null } as never, {})).resolves.toMatchObject({ status: "quarantined", category: "episode_invalid" });
  });

  it("uses a production-bound REST writer for exact payload/vector readback, partial acknowledgement, and canonical collision categorization", async () => {
    const points = new Map<string, { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>(); const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
    let addUnknownEpisodePayload = false;
    const fetchImpl = vi.fn(async (url: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>; calls.push({ url, body });
      if (url.endsWith("/points/retrieve")) { const ids = body.ids as string[]; return new Response(JSON.stringify({ result: ids.flatMap((id) => { const point = points.get(id); return point === undefined ? [] : [{ ...point, ...(body.with_vector === true ? {} : { vector: undefined }) }]; }) }), { headers: { "content-type": "application/json" } }); }
      if (init?.method === "PUT") { for (const point of body.points as Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>) { const payload = { ...point.payload, ...(addUnknownEpisodePayload && point.payload.record_type === "episode" ? { unexpected_raw: true } : {}) }; points.set(point.id, { id: point.id, payload, ...(point.vector === undefined ? {} : { vector: point.vector }) }); } return new Response(JSON.stringify({ result: { status: "acknowledged" } }), { headers: { "content-type": "application/json" } }); }
      throw new Error(`unexpected request ${url}`);
    });
    const client = createQdrantSessionWriter({ baseUrl: "http://qdrant/v1", collection: "pi_memory", ownerHost: "pi", timeoutMs: 100, fetchImpl });
    const factory = createQdrantDestinationFactory({ endpoint: "http://qdrant/v1", destination: qdrantDestination, client: bindQdrantSessionWriter({ endpoint: "http://qdrant/v1", client }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const qdrant = bindQdrantDestination(factory, qdrantDestination); const localPolicy = policy(); const embedding = fakeBoundEmbedding();
    await expect(ingestPendingJobs({ job: job(localPolicy), now: 0, localPolicy, qdrant, embedding, control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 1, pending: 0, quarantined: 0 });
    const written = calls.flatMap((call) => call.body.points as Array<Record<string, unknown>> | undefined ?? []); const episodePoint = written.find((point) => (point.payload as Record<string, unknown>).record_type === "episode")!;
    expect(episodePoint.payload).toMatchObject({ record_type: "episode", owner_host: "pi", privacy_epoch: 0, status: "active", redaction_status: "redacted", secret_scan: "passed", processing_policy_id: localPolicy.id }); expect((episodePoint.vector as { semantic: number[] }).semantic).toHaveLength(1024);
    const nextEpisode = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000098", sourceEntryId: "entry-98", sessionId: "session-98", turnId: "turn-98" }); addUnknownEpisodePayload = true;
    await expect(ingestPendingJobs({ job: job(localPolicy, { episodes: [nextEpisode], episodeIds: [nextEpisode.id] }), now: 0, localPolicy, qdrant, embedding: fakeBoundEmbedding(), control: control(localPolicy), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    addUnknownEpisodePayload = false;
    const privacyEpisode = episode(localPolicy, { id: "00000000-0000-5000-8000-000000000097", sourceEntryId: "entry-97", sessionId: "session-97", turnId: "turn-97", privacyEpoch: 1 });
    await expect(ingestPendingJobs({ job: job(localPolicy, { episodes: [privacyEpisode], episodeIds: [privacyEpisode.id] }), now: 0, localPolicy, qdrant, embedding: fakeBoundEmbedding(), control: control(localPolicy, [snapshot(localPolicy, { privacyEpoch: 1 })]), maxClockSkewMs: 0 })).resolves.toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    const policyPoint = written.find((point) => (point.payload as Record<string, unknown>).record_type === "processing_policy")!;
    points.get(String(policyPoint.id))!.vector = { semantic: Array.from({ length: 1024 }, () => 0.25) };
    await expect(qdrant.retrieve("processing_policy", localPolicy.id)).resolves.toBeNull();
    points.get(String(episodePoint.id))!.payload.ownerHost = "pi";
    await expect(qdrant.retrieve("episode", job(localPolicy).episodes[0]!.id)).resolves.toBeNull();
  });

});
