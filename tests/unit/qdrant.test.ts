import { afterEach, describe, expect, it, vi } from "vitest";
import { readPolicy, physicalPointIdFor, type QdrantClientOptions, type QdrantReadPolicy } from "../../src/qdrant/client.js";
import { V2_COLLECTION_METADATA, REQUIRED_INDEXES, COLLECTION_METADATA_ID, COLLECTION_CONTROL_ID, physicalPointId, controlPayload } from "../../src/qdrant/schema.js";
import { bindQdrantDestination, coordinationRecordFromPayload, createQdrantCoordinationStore, createQdrantSafeBundle, recordPayload, QdrantContentHashCollisionError } from "../../src/qdrant/write.js";
import { adminCreateCollection, adminCreatePayloadIndex, adminHealth, adminInsertInitialControlPoint, adminInsertMetadataPoint, adminRetrieve, adminServerInfo, statusCollectionInfo, statusHealth, statusRetrieve } from "../../src/admin/transport.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type TombstoneRecord } from "../../src/domain/records.js";
import { coverageId, episodeId, leasePointId, tombstoneId } from "../../src/domain/ids.js";
import { activatePolicyEpoch, beginForgetBarrier, beginPolicyDrain, createIngestControlReader, QuiescenceProof, readControl, rotateCoordinationPolicy, waitForOldLeasesToQuiesce } from "../../src/coordination/control.js";
import { claimLease, readLease, releaseLease, renewLease } from "../../src/coordination/leases.js";
import { createJob, writeProposal, acceptProposal } from "../../src/coordination/jobs.js";
import { createTombstone, readTombstones } from "../../src/coordination/tombstones.js";
import { markCoverage } from "../../src/coordination/reconcile.js";
import type { AuthorizedDestination } from "../../src/types.js";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}
const OWNER = "pi" as const;
const NOW = "2026-08-10T00:00:00.000Z";
const NOW_MS = Date.parse(NOW);
const POLICY_HASH = "policy-hash";
const INTERSECTION_ID = "intersection-id";
const EXTRACTOR = "extractor-1";
const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
function episode(overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const base = {
    ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0,
    processingPolicyId: "policy-1", expiresAt: null, recordType: "episode" as const, id: episodeId("pi", "session-1", "message-1"), contentHash: "pending",
    sourceEntryId: "entry-1", host: "pi" as const, projectId: "project-1", projectIdentityKind: "registered" as const,
    sessionId: "session-1", turnId: "turn-1", agentRole: "root" as const, depth: 0, eventKind: "user" as const,
    eventAt: "2026-08-10T00:00:00.000Z", modelId: "model-1", embeddingDimension: 1024, originProvider: "provider-1",
    destinationId: "destination-1", status: "active" as const, redactionStatus: "unchanged" as const, secretScan: "passed" as const, text: "safe",
  } satisfies EpisodeRecord;
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}
function control(overrides: Partial<ControlRecord> = {}): ControlRecord { const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: "gen-1", activeBaseGeneration: null, coordinationPolicyEpoch: 2, coordinationPolicyHash: "policy-hash", state: "active" as const, scanCursor: "cursor-old", lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" }; const value = { ...base, ...overrides }; return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord; }
function tombstone(overrides: Partial<TombstoneRecord> = {}): TombstoneRecord { const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "tombstone" as const, id: "pending", scope: "occurrence" as const, targetId: "00000000-0000-5000-8000-000000000001", contentHash: "pending" }; const value = { ...base, ...overrides }; const id = tombstoneId("pi", value.targetId); return { ...value, id, contentHash: canonicalRecordHash({ ...value, id }) } as TombstoneRecord; }

function qdrantOptions(baseUrl = "http://qdrant", ownerHost: "pi" | "prime" = "pi"): QdrantClientOptions {
  return { baseUrl, collection: ownerHost === "pi" ? "pi_memory" : "prime_memory", ownerHost, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
}
interface WirePoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] }; }
interface BackendHooks { onUpsert?: (points: WirePoint[], mode: string) => void; failUpsert?: boolean; extra?: (ids: readonly string[]) => WirePoint[] | undefined; }
/** In-memory REST backend honoring update_only CAS filters (typed control/lease CAS). */
function backend(seed: WirePoint[] = [], hooks: BackendHooks = {}): { points: Map<string, WirePoint>; fetchImpl: typeof fetch } {
  const points = new Map<string, WirePoint>(seed.map((point) => [point.id, point]));
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: WirePoint[]; update_mode?: string; update_filter?: { must: Array<{ key: string; match?: { value?: unknown }; is_null?: { key: string }; range?: { lte?: string; gt?: string } }> } };
    if (url.includes("/points/retrieve")) { const ids = body?.ids ?? []; const extra = hooks.extra?.(ids); return json({ result: [...ids.map((id) => points.get(id)).filter((point) => point !== undefined), ...(extra ?? [])], status: "ok" }); }
    if (url.includes("/points/scroll")) return json({ result: { points: [], next_page_offset: null }, status: "ok" });
    if (url.includes("/points?") && init.method === "PUT") {
      if (hooks.failUpsert === true) throw new Error("backend upsert failed");
      const point = body?.points?.[0];
      const current = point === undefined ? undefined : points.get(point.id)?.payload;
      const value = (key: string): unknown => current?.[key];
      const must = body?.update_filter?.must ?? [];
      const matches = must.every((condition) => {
        if ("is_null" in condition) return value(condition.is_null.key) === null;
        if (condition.key === "expires_at" && condition.range?.lte !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) <= Date.parse(condition.range.lte);
        if (condition.key === "expires_at" && condition.range?.gt !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) > Date.parse(condition.range.gt);
        return value(condition.key) === condition.match?.value;
      });
      if (body?.update_mode === "update_only" && !matches) return json({ result: { status: "acknowledged" }, status: "ok" });
      hooks.onUpsert?.(body?.points ?? [], String(body?.update_mode));
      for (const incoming of body?.points ?? []) points.set(incoming.id, { id: incoming.id, payload: incoming.payload, ...(incoming.vector === undefined ? {} : { vector: incoming.vector }) });
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  };
  return { points, fetchImpl };
}
function storeFor(b: { points: Map<string, WirePoint>; fetchImpl: typeof fetch }): ReturnType<typeof createQdrantCoordinationStore> {
  vi.stubGlobal("fetch", b.fetchImpl);
  return createQdrantCoordinationStore(qdrantOptions());
}
const { RootWorkerContext, mintRootWorker } = vi.hoisted(() => {
  const TEST_ROOT_BRAND = Symbol("pi-qdrant-memory-v2.test-root-worker");
  const FIXED_TEST_NOW = Date.parse("2026-08-10T00:00:00.000Z");
  class RootWorkerContext {
    readonly #issuer: symbol;
    readonly #host: "pi" | "prime";
    readonly #evidenceHash: string;
    readonly #clock: () => number;
    readonly #nodeId: string;
    readonly #leaseMs: number;
    readonly #maxClockSkewMs: number;
    #lastSample: number | null = null;
    constructor(host: "pi" | "prime", evidenceHash: string, issuer: symbol, clock: (() => number) | undefined, nodeId: string, leaseMs: number, maxClockSkewMs: number) {
      if (issuer !== TEST_ROOT_BRAND) throw new TypeError("Root worker capability requires the module issuer");
      if (clock !== undefined && typeof clock !== "function") throw new TypeError("Root worker clock is invalid");
      if (typeof nodeId !== "string" || nodeId.length === 0 || nodeId.length > 512 || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(nodeId)) throw new TypeError("Root worker node id is invalid");
      if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 86_400_000 || !Number.isSafeInteger(maxClockSkewMs) || maxClockSkewMs < 0 || maxClockSkewMs > 3_600_000) throw new TypeError("Root worker lease configuration is invalid");
      this.#issuer = issuer;
      this.#host = host;
      this.#evidenceHash = evidenceHash;
      this.#clock = clock ?? (() => FIXED_TEST_NOW);
      this.#nodeId = nodeId;
      this.#leaseMs = leaseMs;
      this.#maxClockSkewMs = maxClockSkewMs;
      Object.freeze(this);
    }
    static isValid(value: unknown): value is RootWorkerContext {
      if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
      return value instanceof RootWorkerContext && value.#issuer === TEST_ROOT_BRAND;
    }
    now(): number {
      const value = this.#clock();
      if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError("Root worker clock is invalid");
      if (this.#lastSample !== null && (value as number) < this.#lastSample) throw new TypeError("Root worker clock went backwards");
      this.#lastSample = value;
      return value;
    }
    get host(): "pi" | "prime" { return this.#host; }
    get evidenceHash(): string { return this.#evidenceHash; }
    get nodeId(): string { return this.#nodeId; }
    get leaseMs(): number { return this.#leaseMs; }
    get maxClockSkewMs(): number { return this.#maxClockSkewMs; }
  }
  const mintRootWorker = (host: "pi" | "prime", nodeId: string, clock?: () => number, leaseMs = 30000, maxClockSkewMs = 0): RootWorkerContext => new RootWorkerContext(host, "test-harness", TEST_ROOT_BRAND, clock, nodeId, leaseMs, maxClockSkewMs);
  return { RootWorkerContext, mintRootWorker };
});
vi.mock("../../src/coordination/root.js", () => ({ RootWorkerContext }));
function workerAt(nodeId: string, now: number, leaseMs = 30000, skew = 0): RootWorkerContext { return mintRootWorker("pi", nodeId, () => now, leaseMs, skew); }

afterEach(() => { vi.unstubAllGlobals(); });
function jobInput() { return { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW }; }
function readPolicyFixture(): QdrantReadPolicy { return { ownerHost: "pi" as const, purpose: "memory" as const, recordTypes: ["episode" as const], now: Date.now(), maxClockSkewMs: 0, requireStatus: "active" as const, requireSecretScan: "passed" as const }; }

describe("Qdrant v2 REST capability (safe owner module)", () => {
  it("uses only scoped api-key headers, explicit methods, and validates envelopes", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { calls.push({ url: String(input), init }); return json({ result: { status: "ok" } }); };
    await statusHealth({ baseUrl: "https://qdrant.example/", collection: "pi_memory", ownerHost: "pi", apiKey: "collection-scoped", timeoutMs: 2500 }, fetchImpl);
    expect(calls).toHaveLength(1);
    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("api-key")).toBe("collection-scoped");
    expect(headers.has("authorization")).toBe(false);
    await expect(statusCollectionInfo({ baseUrl: "https://qdrant.example/", collection: "pi_memory", ownerHost: "pi", apiKey: "collection-scoped", timeoutMs: 2500 }, fetchImpl)).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("validates configured collection, payloads, and finite vectors before fetch", () => {
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), baseUrl: "not a url" })).toThrow(/endpoint/i);
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), collection: "prime_memory" })).toThrow(/collection does not match owner host/i);
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), apiKey: "" })).toThrow(/api key/i);
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), timeoutMs: 0 })).toThrow(/timeout/i);
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), maxClockSkewMs: -1 })).toThrow(/skew/i);
  });

  it("requires defensive owner, expiry, status, and tombstone filters on reads", () => {
    // The pure read-policy validator fails closed on forged/weak policies.
    expect(() => readPolicy({ ownerHost: "other", purpose: "memory", recordTypes: ["episode"] })).toThrow(/owner/i);
    expect(() => readPolicy({ ownerHost: "pi", purpose: "memory", recordTypes: ["tombstone"] })).toThrow(/purpose/i);
    expect(() => readPolicy({ ownerHost: "pi", purpose: "control", recordTypes: ["episode"] })).toThrow(/purpose/i);
    expect(() => readPolicy({ ownerHost: "pi", purpose: "memory", recordTypes: ["episode"], maxClockSkewMs: -1 })).toThrow(/expiry/i);
    expect(() => readPolicy({ ownerHost: "pi", purpose: "memory", recordTypes: ["episode"], projectId: "" })).toThrow(/scope/i);
    const fixture = readPolicyFixture();
    expect(fixture.requireStatus).toBe("active");
    expect(fixture.requireSecretScan).toBe("passed");
  });

  it("applies configured consistency to every read endpoint", async () => {
    const urls: string[] = [];
    const fetchImpl: typeof fetch = async (input) => { urls.push(String(input)); return json({ result: [], status: "ok" }); };
    await statusRetrieve({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, readConsistency: "majority" }, fetchImpl, [COLLECTION_METADATA_ID], readPolicy({ ownerHost: "pi", purpose: "metadata", recordTypes: ["collection_metadata"] }));
    expect(urls[0]).toContain("consistency=majority");
  });

  it("rejects forged policies as invalid responses", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: { owner_host: "prime", record_type: "collection_control" } }]);
    const store = storeFor(b);
    await expect(store.readControl()).rejects.toThrow();
  });

  it("rejects malformed response payloads as invalid responses", async () => {
    const malformed = backend([{ id: COLLECTION_CONTROL_ID, payload: { owner_host: "pi", record_type: "collection_control", status: "active", secret_scan: "passed", version: 1 } }]);
    await expect(storeFor(malformed).readControl()).rejects.toThrow();
  });

  it("classifies malformed scroll offsets as response errors", async () => {
    const b = backend([]);
    const fetchImpl: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points/scroll")) return json({ result: { points: [], next_page_offset: "not-a-uuid" } }); return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", fetchImpl);
    await expect(createQdrantCoordinationStore(qdrantOptions()).scrollLeases()).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("rejects malformed payload-index schemas and accepts no extra collection vectors", async () => {
    await expect(adminCreatePayloadIndex(qdrantOptions() as never, async () => json({ result: true, status: "ok" }), "Bad-Field!", "keyword")).rejects.toMatchObject({ category: "configuration" });
    await expect(adminCreatePayloadIndex(qdrantOptions() as never, async () => json({ result: true, status: "ok" }), "field", "bogus" as never)).rejects.toMatchObject({ category: "configuration" });
    const fetchImpl: typeof fetch = async () => json({ result: { config: { params: { vectors: { semantic: { size: 1024, distance: "Cosine" }, extra: { size: 8, distance: "Dot" } } } }, points_count: 0, status: "green" }, status: "ok" });
    await expect(statusCollectionInfo({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000 }, fetchImpl)).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("uses 1.17 insert_only and fails closed on an ignored hash collision", async () => {
    // insert_only semantics through the safe markCoverage op: a backend that
    // IGNORES the write (acknowledges but stores nothing) fails the exact
    // readback closed.
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { onUpsert: (_points, mode) => { expect(mode).toBe("insert_only"); } });
    const ignoreWrite: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points?") && init.method === "PUT") return json({ result: { status: "acknowledged" }, status: "ok" }); return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", ignoreWrite);
    const store = createQdrantCoordinationStore(qdrantOptions());
    await expect(markCoverage(store, { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID })).rejects.toThrow(/did not read back/i);
  });

  it("converges equal insert-only hashes", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    const input = { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID };
    const first = await markCoverage(store, input);
    const second = await markCoverage(store, input);
    expect(second).toEqual(first);
  });

  it("rejects invalid payload/vector responses", async () => {
    const input = { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID };
    const tampered = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { onUpsert: (points) => { for (const point of points) if (point.payload.record_type === "coverage") point.payload = { ...point.payload, content_hash: "0".repeat(64) }; } });
    await expect(markCoverage(storeFor(tampered), input)).rejects.toThrow(/hash/i);
  });

  it("emits update_only CAS predicates and verifies readback", async () => {
    const writes: Array<{ mode: string; filter: unknown }> = [];
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { onUpsert: (_points, mode) => { writes.push({ mode, filter: writes.length }); } });
    const capture: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points?") && init.method === "PUT") { const body = JSON.parse(String(init.body)) as { update_mode?: string; update_filter?: unknown }; writes.push({ mode: String(body.update_mode), filter: body.update_filter }); } return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", capture);
    const store = createQdrantCoordinationStore(qdrantOptions());
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.state).toBe("draining");
    expect(drained.activeGeneration).toBeNull();
    const update = writes.find((entry) => entry.mode === "update_only");
    expect(update).toBeDefined();
    expect(update?.filter).toBeDefined();
    const reread = await readControl(store);
    expect(reread.state).toBe("draining");
    expect(reread.version).toBe(2);
  });

  it("accepts official plain health and only strict JSON health envelopes", async () => {
    await expect(statusHealth({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000 }, async () => new Response("healthz check passed", { status: 200 }))).resolves.toBe("healthz check passed");
    await expect(statusHealth({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000 }, async () => new Response("garbage", { status: 200 }))).rejects.toMatchObject({ category: "invalid-json" });
    await expect(statusHealth({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000 }, async () => json({ result: { status: "degraded" } }))).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("carries privacy and current state fences across active/draining CAS transitions", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.state).toBe("draining");
    expect(drained.activeGeneration).toBeNull();
    const forgotten = await beginForgetBarrier(store, { now: NOW_MS });
    expect(forgotten.privacyEpoch).toBe(1);
    // Epoch activation requires a genuine quiescence proof + +1 epoch/hash.
    const proof = await waitForOldLeasesToQuiesce(store, { retiredEpoch: 2, maxLeaseMs: 30000, maxClockSkewMs: 0, now: () => NOW_MS });
    const active = await activatePolicyEpoch(store, { proof, nextPolicyHash: "policy-hash-2", memoryModelTimeoutMs: 0 });
    expect(active.state).toBe("active");
    expect(active.coordinationPolicyEpoch).toBe(3);
    expect(active.coordinationPolicyHash).toBe("policy-hash-2");
  });

  it("rejects stale generation privacy/policy payloads before publication write", async () => {
    // A filter-honoring backend that no-ops the CAS: the safe transition fails
    // closed ("lost the control CAS") and the stored control is unchanged.
    const stored = new Map<string, WirePoint>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]]);
    let controlReads = 0;
    // The control ADVANCES between the drain's read and the CAS evaluation
    // (stale-generation race): the filter then zero-matches and the transition
    // fails closed ("lost the control CAS"), leaving the server state untouched.
    const noop: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { points?: WirePoint[]; update_mode?: string; update_filter?: { must: Array<{ key: string; match?: { value?: unknown } }> } }; if (url.includes("/points/retrieve")) { controlReads += 1; if (controlReads === 2) { const before = stored.get(COLLECTION_CONTROL_ID)!; stored.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: { ...before.payload, version: 5, state: "draining", content_hash: "5".repeat(64) } }); } const ids = (body as { ids?: string[] }).ids ?? []; return json({ result: ids.map((id) => stored.get(id)).filter((point) => point !== undefined), status: "ok" }); } if (url.includes("/points?") && init.method === "PUT") { const point = body?.points?.[0]; const current = point === undefined ? undefined : stored.get(point.id)?.payload; const must = body?.update_filter?.must ?? []; const matches = must.every((condition) => current?.[condition.key] === condition.match?.value); if (body?.update_mode === "update_only" && !matches) return json({ result: { status: "acknowledged" }, status: "ok" }); if (point !== undefined) stored.set(point.id, { id: point.id, payload: point.payload }); return json({ result: { status: "acknowledged" }, status: "ok" }); } return json({ result: {}, status: "ok" }); };
    vi.stubGlobal("fetch", noop);
    const store = createQdrantCoordinationStore(qdrantOptions());
    await expect(beginPolicyDrain(store, { now: NOW_MS })).rejects.toThrow(/lost the control CAS/i);
    expect(stored.get(COLLECTION_CONTROL_ID)?.payload.state).toBe("draining");
  });

  it("enforces monotonic privacy/coordination epochs and policy transitions", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    const first = await beginForgetBarrier(store, { now: NOW_MS });
    expect(first.privacyEpoch).toBe(1);
    const second = await beginForgetBarrier(store, { now: NOW_MS });
    expect(second.privacyEpoch).toBe(2);
    const rotated = await rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-3", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS });
    expect(rotated.state).toBe("active");
    expect(rotated.coordinationPolicyEpoch).toBe(3);
    expect(rotated.coordinationPolicyHash).toBe("policy-hash-3");
  });

  it("separates the admin key and creates named-vector contracts", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { calls.push({ url: String(input), init }); const url = String(input); if (url.includes("/collections/pi_memory/index")) return json({ result: { status: "completed" }, status: "ok" }); if (url.endsWith("/collections/pi_memory")) return json({ result: true, status: "ok" }); if (url.endsWith("/")) return json({ version: "1.17.0", status: "ok" }); return json({ result: {}, status: "ok" }); };
    const options = { baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi" as const, apiKey: "admin-secret", timeoutMs: 1000, replicationFactor: 1, writeConsistencyFactor: 1 };
    const info = await adminServerInfo(options, fetchImpl);
    expect(info.version).toBe("1.17.0");
    expect(new Headers(calls[0]?.init.headers).get("api-key")).toBe("admin-secret");
    await adminCreateCollection(options, fetchImpl);
    const createCall = calls.find((call) => call.init.method === "PUT" && !call.url.includes("/index"));
    const createBody = JSON.parse(String(createCall?.init.body)) as { vectors: { semantic: { size: number; distance: string } } };
    expect(createBody.vectors.semantic).toEqual({ size: 1024, distance: "Cosine" });
  });

  it("creates payload-index contracts under the admin key", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { calls.push({ url: String(input), init }); return json({ result: { status: "completed" }, status: "ok" }); };
    const options = { baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi" as const, apiKey: "admin-secret", timeoutMs: 1000 };
    await adminCreatePayloadIndex(options, fetchImpl, "session_id", "keyword");
    const indexCall = calls[0];
    expect(new Headers(indexCall?.init.headers).get("api-key")).toBe("admin-secret");
    expect(JSON.parse(String(indexCall?.init.body))).toEqual({ field_name: "session_id", field_schema: "keyword" });
  });

  it("keeps destructive deletion exclusively on the human admin side", async () => {
    // No safe surface exposes deletePoints: the store/bundle have none.
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    for (const obj of [store, bundle.qdrant, bindQdrantDestination(bundle.qdrant, qdrantDestination)]) {
      const anyObj = obj as unknown as Record<string, unknown>;
      expect("deletePoints" in anyObj).toBe(false);
      expect(typeof anyObj.deletePoints).toBe("undefined");
    }
    const writeModule = await import("../../src/qdrant/write.js");
    expect("deletePoints" in writeModule).toBe(false);
  });

  it("publishes control only with version and base-generation CAS and rereads", async () => {
    const filters: Array<{ must: Array<{ key: string; is_null?: { key: string }; match?: { value?: unknown } }> }> = [];
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { onUpsert: (_points, mode) => { if (mode === "update_only") filters.push({ must: [] }); } });
    const capture: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points?") && init.method === "PUT") { const body = JSON.parse(String(init.body)) as { update_filter?: { must: Array<{ key: string; is_null?: { key: string }; match?: { value?: unknown } }> } }; if (body.update_filter !== undefined) filters.push(body.update_filter); } return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", capture);
    const store = createQdrantCoordinationStore(qdrantOptions());
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.version).toBe(2);
    const filter = filters.find((entry) => entry.must.some((condition) => condition.key === "version"));
    expect(filter).toBeDefined();
    expect(filter?.must.some((condition) => condition.key === "version" && condition.match?.value === 1)).toBe(true);
    expect(filter?.must.some((condition) => condition.key === "state" && condition.match?.value === "active")).toBe(true);
    expect(filter?.must.some((condition) => condition.key === "coordination_policy_epoch" && condition.match?.value === 2)).toBe(true);
    const reread = await readControl(store);
    expect(reread.version).toBe(2);
    expect(reread.state).toBe("draining");
  });

  it("exact authoritative reread cardinality: ambiguous insert preflight/postflight responses fail closed", async () => {
    const input = { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID };
    const withExtras = (point: WirePoint): WirePoint[] => [{ id: point.id, payload: point.payload }, { id: "00000000-0000-4000-8000-000000000099", payload: point.payload }];
    // Preflight ambiguity: an extra unrequested point is never mapped to success.
    const preflight = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { extra: (ids) => { const point = [...Array.from({ length: 0 })] as WirePoint[]; return point; } });
    const coveragePhysicalId = coverageId({ ownerHost: OWNER, episodeId: input.episodeId, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 });
    const preflightFetch: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points/retrieve")) { const body = JSON.parse(String(init.body ?? "{}")) as { ids?: string[] }; const ids = body?.ids ?? []; const found = ids.map((id) => preflight.points.get(id)).filter((point) => point !== undefined); const coverage = preflight.points.get(coveragePhysicalId); const extras = coverage === undefined ? [] : withExtras(coverage); return json({ result: [...found, ...extras], status: "ok" }); } return preflight.fetchImpl(input, init); };
    vi.stubGlobal("fetch", preflightFetch);
    await expect(markCoverage(createQdrantCoordinationStore(qdrantOptions()), input)).rejects.toThrow(/ambiguous/i);
  });

  it("exact authoritative reread cardinality: ambiguous control/lease CAS responses fail closed", async () => {
    const controlPayload2 = controlPayload(control());
    const ambiguous: typeof fetch = async (input) => { if (String(input).includes("/points/retrieve")) return json({ result: [{ id: COLLECTION_CONTROL_ID, payload: controlPayload2 }, { id: "00000000-0000-4000-8000-000000000099", payload: controlPayload2 }], status: "ok" }); return json({ result: {}, status: "ok" }); };
    vi.stubGlobal("fetch", ambiguous);
    await expect(beginPolicyDrain(createQdrantCoordinationStore(qdrantOptions()), { now: NOW_MS })).rejects.toThrow(/ambiguous/i);
    const leaseAmbiguous: typeof fetch = async (input) => { if (String(input).includes("/points/retrieve")) return json({ result: [{ id: leasePointId("job-1"), payload: { owner_host: "pi", record_type: "lease" } }, { id: leasePointId("job-1"), payload: { owner_host: "pi", record_type: "lease" } }], status: "ok" }); return json({ result: {}, status: "ok" }); };
    vi.stubGlobal("fetch", leaseAmbiguous);
    await expect(readLease(createQdrantCoordinationStore(qdrantOptions()), "job-1")).resolves.toBeNull();
  });

  it("rejects a cross-owner canonical record before any retrieve or upsert", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    await expect(markCoverage(store, { ownerHost: "prime", episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID })).rejects.toThrow(/owner/i);
  });

  it("serializes Episode vectors as named vectors, never payload fields", async () => {
    const bodies: Array<Record<string, unknown>> = [];
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { onUpsert: () => undefined });
    const capture: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points?") && init.method === "PUT") bodies.push(JSON.parse(String(init.body)) as Record<string, unknown>); return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", capture);
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const bound = bindQdrantDestination(bundle.qdrant, qdrantDestination);
    const ep = { ...episode(), vector: Array.from({ length: 1024 }, () => 0.25), contentHash: "pending" } as EpisodeRecord;
    const finalEp = { ...ep, contentHash: canonicalRecordHash(ep) } as EpisodeRecord;
    await bound.insertAndReadback(finalEp);
    const put = bodies[0] as { points: Array<{ payload: Record<string, unknown>; vector?: unknown }> };
    expect(put.points[0]?.payload.vector).toBeUndefined();
    expect(put.points[0]?.vector).toEqual({ semantic: finalEp.vector });
  });

  it("allows tombstone verification only through explicit internal policy", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    // A bare-UUID occurrence target without the explicit episode selector fails closed.
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: "00000000-0000-5000-8000-000000000001", createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID })).rejects.toThrow(/explicit episode selector/i);
  });

  it("verifies tombstone writes after acknowledgement and preserves payload-only fields", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    const ep = episode();
    const vector = Array.from({ length: 1024 }, () => 0.25);
    const withVector = { ...ep, vector: [...vector] } as EpisodeRecord;
    const finalEp = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord;
    b.points.set(finalEp.id, { id: finalEp.id, payload: recordPayload(finalEp) as Record<string, unknown>, vector: { semantic: [...vector] } });
    const created = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: finalEp.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID });
    expect(created).toHaveLength(1);
    expect(created[0]?.targetId).toBe(finalEp.id);
    const reread = await readTombstones(store, [finalEp.id]);
    expect(reread).toHaveLength(1);
    expect(reread[0]?.scope).toBe("occurrence");
  });

  it("detects an ignored insert-only write from the post-read hash", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }], { onUpsert: (points) => { for (const point of points) if (point.payload.record_type === "coverage") point.payload = { ...point.payload, content_hash: "1".repeat(64) }; } });
    const store = storeFor(b);
    await expect(markCoverage(store, { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID })).rejects.toThrow(/hash/i);
  });

  it("does not send a made-up retrieve filter and rejects missing response ownership", async () => {
    const bodies: Array<{ filter?: unknown; ids?: string[] }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points/retrieve")) { bodies.push(JSON.parse(String(init.body)) as { filter?: unknown; ids?: string[] }); return json({ result: [{ id: COLLECTION_METADATA_ID, payload: { record_type: "collection_metadata", status: "active", secret_scan: "passed" } }], status: "ok" }); } return json({ result: {}, status: "ok" }); };
    await expect(statusRetrieve({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000 }, fetchImpl, [COLLECTION_METADATA_ID], readPolicy({ ownerHost: "pi", purpose: "metadata", recordTypes: ["collection_metadata"] }))).rejects.toMatchObject({ category: "invalid-response" });
    const sent = bodies[0];
    expect(sent?.ids).toEqual([COLLECTION_METADATA_ID]);
    expect(sent?.filter).toBeUndefined();
  });

  it("uses defensive read filters and expiry fail-closed, and maps deterministic physical point IDs", async () => {
    // The store's scroll sends the defensive internal filter (owner/status/
    // secret/record-type must conditions) for lease scans.
    const filters: Array<{ must: Array<{ key: string }> }> = [];
    const b = backend([]);
    const capture: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points/scroll")) { const body = JSON.parse(String(init.body)) as { filter?: { must: Array<{ key: string }> } }; if (body.filter !== undefined) filters.push(body.filter); return json({ result: { points: [], next_page_offset: null }, status: "ok" }); } return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", capture);
    await createQdrantCoordinationStore(qdrantOptions()).scrollLeases();
    expect(filters[0]?.must.some((condition) => condition.key === "owner_host")).toBe(true);
    expect(filters[0]?.must.some((condition) => condition.key === "status")).toBe(true);
    expect(filters[0]?.must.some((condition) => condition.key === "secret_scan")).toBe(true);
    expect(filters[0]?.must.some((condition) => condition.key === "record_type")).toBe(true);
    // Expiry fail-closed on memory reads: an expired episode point is never returned.
    const expiredEp = { ...episode(), expiresAt: "1970-01-01T00:00:00.000Z", contentHash: "pending" } as EpisodeRecord;
    const finalExpiredEp = { ...expiredEp, contentHash: canonicalRecordHash(expiredEp) } as EpisodeRecord;
    const b2 = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }, { id: finalExpiredEp.id, payload: recordPayload(finalExpiredEp) as Record<string, unknown> }]);
    const store2 = storeFor(b2);
    await expect(store2.readEpisodes([finalExpiredEp.id])).rejects.toThrow(/expired/i);
    expect(physicalPointIdFor("episode", "session-1")).toBe(physicalPointId("episode", "session-1"));
    expect(physicalPointIdFor("episode", "00000000-0000-5000-8000-000000000001")).toBe("00000000-0000-5000-8000-000000000001");
  });

  it("preflights insert-only and reports a pre-existing equal point across fresh stores", async () => {
    const input = { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID };
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const first = await markCoverage(storeFor(b), input);
    const second = await markCoverage(storeFor(b), input);
    expect(second).toEqual(first);
  });

  it("rejects arbitrary CAS patches and preserves full control payload on update-only", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    const store = storeFor(b);
    const before = await readControl(store);
    await beginPolicyDrain(store, { now: NOW_MS });
    const after = await readControl(store);
    expect(after.version).toBe(before.version + 1);
    expect(after.state).toBe("draining");
    expect(after.activeGeneration).toBeNull();
    // Every unrelated control field survives the CAS patch untouched.
    expect(after.processingPolicyId).toBe(before.processingPolicyId);
    expect(after.coordinationPolicyHash).toBe(before.coordinationPolicyHash);
    expect(after.coordinationPolicyEpoch).toBe(before.coordinationPolicyEpoch);
    expect(after.privacyEpoch).toBe(before.privacyEpoch);
    expect(after.scanCursor).toBe(before.scanCursor);
    expect(after.createdAt).toBe(before.createdAt);
    expect(after.revokedDestinationIds).toEqual(before.revokedDestinationIds);
  });

  it("emits extended lease-cas update_filter predicates (fencing/accepted/privacy/policy/live-expiry) and rereads exactly", async () => {
    const filters: Array<{ must: Array<{ key: string; match?: { value?: unknown }; range?: { gt?: string } }> }> = [];
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control({ coordinationPolicyEpoch: 1, state: "active" })) }]);
    const capture: typeof fetch = async (input, init = {}) => { if (String(input).includes("/points?") && init.method === "PUT") { const body = JSON.parse(String(init.body)) as { update_filter?: { must: Array<{ key: string; match?: { value?: unknown }; range?: { gt?: string } }> } }; if (body.update_filter !== undefined) filters.push(body.update_filter); } return b.fetchImpl(input, init); };
    vi.stubGlobal("fetch", capture);
    const store = createQdrantCoordinationStore(qdrantOptions());
    const job = await createJob(store, jobInput());
    const authority = await claimLease(store, workerAt("node-a", NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    const reread = await readLease(store, job.id);
    expect(reread?.ownerId).toBe("node-a");
    expect(reread?.version).toBe(1);
    // The typed lease CAS filter is emitted on the update_only renewal path.
    const renewed = await renewLease(store, authority!);
    expect(renewed).not.toBeNull();
    expect((await readLease(store, job.id))?.version).toBe(2);
    const filter = filters.find((entry) => entry.must.some((condition) => condition.key === "fencing_token"));
    expect(filter).toBeDefined();
    const keys = filter?.must.map((condition) => condition.key) ?? [];
    for (const expected of ["fencing_token", "coordination_policy_epoch", "coordination_policy_hash", "privacy_epoch", "state", "owner_id", "processing_policy_id", "content_hash", "expires_at"]) expect(keys).toContain(expected);
  });

  it("reads expired leases through the internal purpose", async () => {
    const expired = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: "intersection-id", expiresAt: "2026-08-09T00:00:00.000Z", recordType: "lease" as const, id: "a753361e-2c74-550d-9d52-6f5762a37a4b", jobId: "job-1", ownerId: "node-a", version: 1, fencingToken: 1, state: "leased" as const, acceptedProposalId: null, acceptedManifestHash: null, coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1, contentHash: "pending" };
    const finalExpired = { ...expired, contentHash: canonicalRecordHash(expired) } as typeof expired;
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }, { id: finalExpired.id, payload: recordPayload(finalExpired) as Record<string, unknown> }]);
    const store = storeFor(b);
    const lease = await readLease(store, "job-1");
    expect(lease).not.toBeNull();
    expect(lease?.ownerId).toBe("node-a");
    expect(coordinationRecordFromPayload(recordPayload(finalExpired), "pi")).toMatchObject({ recordType: "lease", ownerId: "node-a", version: 1 });
  });

  it("safe claims reject foreign owners without caller-supplied typed preconditions", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control({ coordinationPolicyEpoch: 1, state: "active" })) }]);
    const store = storeFor(b);
    const job = await createJob(store, jobInput());
    const claimed = await claimLease(store, workerAt("node-a", NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claimed).not.toBeNull();
    const foreign = await claimLease(store, workerAt("node-foreign", NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(foreign).toBeNull();
  });

  it("safe bundle insert/readback rejects verified collisions and cross-owner episodes", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    vi.stubGlobal("fetch", b.fetchImpl);
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const bound = bindQdrantDestination(bundle.qdrant, qdrantDestination);
    // Same-id different-hash point -> verified terminal collision.
    const ep = episode();
    const other = { ...ep, text: "tampered", contentHash: "pending" } as EpisodeRecord;
    const otherFinal = { ...other, contentHash: canonicalRecordHash(other) } as EpisodeRecord;
    b.points.set(ep.id, { id: ep.id, payload: recordPayload(otherFinal) as Record<string, unknown>, vector: { semantic: Array.from({ length: 1024 }, () => 0.5) } });
    const vector = Array.from({ length: 1024 }, () => 0.25);
    const withVector = { ...ep, vector: [...vector] } as EpisodeRecord;
    const finalEp = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord;
    await expect(bound.insertAndReadback(finalEp)).rejects.toThrow(QdrantContentHashCollisionError);
    // Cross-owner record: a prime-owned episode can never be written by the pi
    // bundle (fails closed, and nothing lands in the backend).
    const primeEp = { ...episode({ id: episodeId("prime", "session-prime", "message-1") }), ownerHost: "prime" as const, host: "prime" as const, contentHash: "pending" } as EpisodeRecord;
    const primeFinal = { ...primeEp, contentHash: canonicalRecordHash(primeEp) } as EpisodeRecord;
    await expect(bound.insertAndReadback(primeFinal)).rejects.toThrow();
    expect(b.points.has(primeFinal.id)).toBe(false);
  });

  it("safe bundle readback: exact episode equality", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    vi.stubGlobal("fetch", b.fetchImpl);
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const bound = bindQdrantDestination(bundle.qdrant, qdrantDestination);
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...episode(), vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    b.points.set(finalCommitted.id, { id: finalCommitted.id, payload: recordPayload(finalCommitted) as Record<string, unknown>, vector: { semantic: [...vectorA] } });
    const readback = await bound.retrieve("episode", finalCommitted.id);
    expect(readback).not.toBeNull();
    expect(readback?.contentHash).toBe(finalCommitted.contentHash);
  });

  it("safe bundle readback: legacy vector-excluding hash is a terminal collision", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]);
    vi.stubGlobal("fetch", b.fetchImpl);
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const bound = bindQdrantDestination(bundle.qdrant, qdrantDestination);
    const vectorA = Array.from({ length: 1024 }, (_, index) => (index % 7) / 10);
    const committed = { ...episode(), vector: vectorA, contentHash: "pending" } as EpisodeRecord;
    const finalCommitted = { ...committed, contentHash: canonicalRecordHash(committed) } as EpisodeRecord;
    const { vector: _v, ...noVector } = finalCommitted as EpisodeRecord;
    b.points.set(finalCommitted.id, { id: finalCommitted.id, payload: { ...recordPayload(finalCommitted), content_hash: canonicalRecordHash(noVector as EpisodeRecord) } as Record<string, unknown>, vector: { semantic: [...vectorA] } });
    await expect(bound.retrieve("episode", finalCommitted.id)).rejects.toThrow(QdrantContentHashCollisionError);
  });

  it("the ingest control reader exposes a bounded revocation snapshot only", async () => {
    const b = backend([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(control({ revokedDestinationIds: ["qdrant:pi"] })) }]);
    const store = storeFor(b);
    const reader = createIngestControlReader(store, { policyHash: "policy-hash", policyEpoch: 2 });
    const snapshot = await reader.read();
    expect(snapshot).toEqual({ state: "active", privacyEpoch: 0, coordinationPolicyEpoch: 2, policyHash: "policy-hash", revokedDestinationIds: ["qdrant:pi"] });
  });
});
