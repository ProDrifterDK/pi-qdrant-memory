import { describe, expect, it } from "vitest";
import { MemoryClientError } from "../../src/clients/http.js";
import { QdrantAdminClient, QdrantReadClient, QdrantSessionWriter, readPolicy } from "../../src/qdrant/client.js";
import { V2_COLLECTION_METADATA, REQUIRED_INDEXES, COLLECTION_METADATA_ID, COLLECTION_CONTROL_ID, physicalPointId, controlPayload } from "../../src/qdrant/schema.js";
import { insertOnly, updateOnlyCas, publishControlCas } from "../../src/qdrant/write.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type TombstoneRecord } from "../../src/domain/records.js";
import { episodeId } from "../../src/domain/ids.js";
import { deterministicUuid } from "../../src/domain/canonical.js";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}
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
function control(overrides: Partial<ControlRecord> = {}): ControlRecord { const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: "gen-1", activeBaseGeneration: null, coordinationPolicyEpoch: 2, coordinationPolicyHash: "policy-hash", state: "active" as const, scanCursor: "cursor-old", lastForgetBarrier: null, contentHash: "pending" }; const value = { ...base, ...overrides }; return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord; }
function tombstone(overrides: Partial<TombstoneRecord> = {}): TombstoneRecord { const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "tombstone" as const, id: episodeId("pi", "tombstone", "id"), scope: "occurrence" as const, targetId: "target", contentHash: "pending" }; const value = { ...base, ...overrides }; return { ...value, contentHash: canonicalRecordHash(value) } as TombstoneRecord; }

function client(fetchImpl: typeof fetch): QdrantSessionWriter {
  return new QdrantSessionWriter({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "collection-scoped", timeoutMs: 2500, fetchImpl });
}
function readPolicyFixture() { return { ownerHost: "pi" as const, purpose: "memory" as const, recordTypes: ["episode" as const], now: Date.now(), maxClockSkewMs: 0, requireStatus: "active" as const, requireSecretScan: "passed" as const }; }

describe("Qdrant v2 REST capability clients", () => {
  it("uses only scoped api-key headers, explicit methods, and validates envelopes", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      calls.push({ url: String(input), init });
      return json({ result: { status: "ok" } });
    };
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example/", collection: "pi_memory", ownerHost: "pi", apiKey: "collection-scoped", timeoutMs: 2500, fetchImpl });
    await read.health();
    expect(Object.getOwnPropertyNames(Object.getPrototypeOf(read))).toEqual(expect.arrayContaining(["health", "collectionInfo", "retrieve", "scroll", "search", "count"]));
    expect(Object.getOwnPropertyNames(Object.getPrototypeOf(read))).not.toContain("request");
    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("api-key")).toBe("collection-scoped");
    expect(headers.has("authorization")).toBe(false);
    await expect(read.collectionInfo()).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("validates configured collection, payloads, and finite vectors before fetch", async () => {
    const fetchImpl: typeof fetch = async () => json({ result: [] });
    expect(() => new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "../other", ownerHost: "pi", timeoutMs: 1, fetchImpl })).toThrow(/collection/i);
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl });
    await expect(read.search({ vector: [Number.NaN], limit: 1, policy: readPolicyFixture() })).rejects.toThrow(/finite/i);
    await expect(read.retrieve([episodeId("pi", "s", "id")], readPolicyFixture())).resolves.toEqual([]);
  });

  it("requires defensive owner, expiry, status, and tombstone filters on reads", async () => {
    const calls: Array<{ url: string; body?: unknown }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      calls.push({ url: String(input), body: init.body === undefined ? undefined : JSON.parse(String(init.body)) });
      if (String(input).includes("/scroll")) return json({ result: { points: [] } });
      if (String(input).includes("/retrieve")) return json({ result: [] });
      return json({ result: { count: 0 } });
    };
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl });
    await read.scroll({ policy: readPolicyFixture() });
    const filter = (calls[0]?.body as { filter: { must: unknown[]; must_not: unknown[]; should: unknown[] } }).filter;
    expect(JSON.stringify(filter)).toContain("owner_host");
    expect(JSON.stringify(filter)).toContain("expires_at");
    expect(JSON.stringify(filter)).toContain("status");
    expect(JSON.stringify(filter)).toContain("tombstone");
  });

  it("applies configured consistency to every read endpoint and emits count/search contracts", async () => {
    const calls: Array<{ url: string; body: Record<string, unknown> }> = []; const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, body }); if (url.includes("/points/scroll")) return json({ result: { points: [] } }); if (url.includes("/points/search")) return json({ result: [] }); if (url.includes("/points/count")) return json({ result: { count: 0 } }); return json({ result: [] }); };
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, readConsistency: "quorum", fetchImpl }); const policy = readPolicyFixture(); await read.retrieve([physicalPointId("episode", "retrieve")], policy); await read.scroll({ policy }); await read.search({ vector: Array.from({ length: 1024 }, () => 0), limit: 1, policy }); await read.count(policy); expect(calls).toHaveLength(4); for (const call of calls) expect(new URL(call.url).searchParams.get("consistency")).toBe("quorum"); expect(calls.find((call) => call.url.includes("/points/count"))?.body).toMatchObject({ exact: true, filter: expect.any(Object) }); expect(calls.find((call) => call.url.includes("/points/retrieve"))?.body).not.toHaveProperty("filter");
  });
  it("rejects forged policies and malformed response payloads as invalid responses", async () => {
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl: async () => json({ result: [{ id: physicalPointId("episode", "bad"), payload: "not-an-object" }] }) });
    await expect(read.retrieve([physicalPointId("episode", "bad")], { ...readPolicyFixture(), now: Number.NaN })).rejects.toMatchObject({ category: "configuration" }); await expect(read.retrieve([physicalPointId("episode", "bad")], readPolicyFixture())).rejects.toMatchObject({ category: "invalid-response" });
  });
  it("classifies malformed scroll offsets as response errors", async () => { const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl: async () => json({ result: { points: [], next_page_offset: "not-a-uuid" } }) }); await expect(read.scroll({ policy: readPolicyFixture() })).rejects.toMatchObject({ category: "invalid-response" }); });
  it("rejects malformed payload-index schemas and accepts no extra collection vectors", async () => {
    const bad = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl: async () => json({ result: { config: { params: { vectors: { semantic: { size: 1024, distance: "Cosine" }, extra: { size: 1, distance: "Cosine" } } } }, payload_schema: { owner_host: "keyword" } } }) }); await expect(bad.collectionInfo()).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("uses 1.17 insert_only and fails closed on an ignored hash collision", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      calls.push({ url: String(input), init });
      if (String(input).endsWith("/points?wait=true&ordering=strong")) return json({ result: { status: "acknowledged" }, status: "ok" });
      if (String(input).includes("/points/retrieve")) return json({ result: [{ id: physicalPointId("episode", episode().id), payload: { owner_host: "pi", record_type: "episode", status: "active", secret_scan: "passed", content_hash: "different", expires_at: null } }] });
      return json({ result: { status: "ok" } });
    };
    const target = episode();
    await expect(insertOnly(client(fetchImpl), target)).rejects.toThrow(/content hash collision/i);
    const writeCall = calls.find((call) => call.url.includes("/points?"));
    expect(writeCall).toBeUndefined();
    const body = writeCall === undefined ? {} : JSON.parse(String(writeCall.init.body)) as { update_mode: string };
    expect(body.update_mode).toBeUndefined();
    const requestUrl = new URL(writeCall?.url ?? "http://invalid");
    expect(requestUrl.hostname).toBe("invalid");
  });

  it("converges equal insert-only hashes and rejects invalid payload/vector responses", async () => {
    let point: EpisodeRecord | undefined;
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      if (String(input).includes("/points/retrieve")) return json({ result: point === undefined ? [] : [{ id: physicalPointId("episode", point.id), payload: { content_hash: point.contentHash, owner_host: "pi", record_type: "episode", status: "active", secret_scan: "passed", expires_at: null } }] });
      if (String(input).includes("/points?")) { point = episode(); return json({ result: { status: "acknowledged" }, status: "ok" }); }
      return json({ result: { status: "ok" } });
    };
    const writer = client(fetchImpl);
    await expect(insertOnly(writer, episode())).resolves.toBe("inserted");
    await expect(insertOnly(writer, episode())).resolves.toBe("existing");
    const badId = episodeId("pi", "bad", "point");
    const bad = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl: async () => json({ result: [{ id: badId, payload: {}, vector: { semantic: [Number.NaN] } }] }) });
    await expect(bad.retrieve([badId], readPolicyFixture())).rejects.toMatchObject({ category: "invalid-response" });
  });

  it("emits update_only CAS predicates and verifies readback", async () => {
    const calls: Array<{ url: string; body: Record<string, unknown> }> = []; let stored: Record<string, unknown> = controlPayload(control());
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>;
      calls.push({ url, body });
      if (url.includes("/points/retrieve")) return json({ result: [{ id: COLLECTION_CONTROL_ID, payload: stored }] });
      if (body.points !== undefined) stored = ((body.points as Array<{ payload: Record<string, unknown> }>)[0]?.payload ?? stored);
      return json({ result: { status: "acknowledged" }, status: "ok" });
    };
    await expect(updateOnlyCas(client(fetchImpl), { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, state: "active" } })).resolves.toBe(true);
    const writeCall = calls.find((call) => call.body.update_mode === "update_only");
    expect(writeCall?.body.update_mode).toBe("update_only");
    expect(writeCall?.body.update_filter).toEqual(expect.objectContaining({ must: expect.arrayContaining([
      { key: "version", match: { value: 1 } },
      { key: "coordination_policy_epoch", match: { value: 2 } },
    ]) }));
    const url = new URL(writeCall?.url ?? "https://invalid");
    expect(url.searchParams.get("wait")).toBe("true");
    expect(url.searchParams.get("ordering")).toBe("strong");
  });

  it("accepts official plain health and only strict JSON health envelopes", async () => {
    const read = (body: BodyInit, contentType?: string) => new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl: async () => new Response(body, { status: 200, headers: contentType === undefined ? {} : { "content-type": contentType } }) });
    await expect(read("healthz check passed", "text/plain").health()).resolves.toBe("healthz check passed"); await expect(read(JSON.stringify({ result: { status: "ok" } }), "application/json").health()).resolves.toBeTruthy(); await expect(read(JSON.stringify({ result: {} }), "application/json").health()).rejects.toMatchObject({ category: "invalid-response" });
  });
  it.each(["1.17.0", "1.17.0+build.7"])("accepts Qdrant %s", async (version) => { const admin = new QdrantAdminClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "human-admin", timeoutMs: 1, fetchImpl: async () => json({ version }) }); await expect(admin.serverInfo()).resolves.toEqual({ version }); });
  it.each(["1.16.9", "1.17.0-alpha.1", "01.17.0", "1.17.0+", "1.17.0+build..7", "999999999999999999999999.17.0", "not-a-version"])("rejects unsupported/malformed Qdrant version %s", async (version) => { const admin = new QdrantAdminClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "human-admin", timeoutMs: 1, fetchImpl: async () => json({ version }) }); await expect(admin.serverInfo()).rejects.toMatchObject({ category: "invalid-response" }); });

  it("carries privacy and current state fences across active/draining CAS transitions", async () => {
    let stored: Record<string, unknown> = controlPayload(control()); const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, body }); if (url.includes("/retrieve")) return json({ result: [{ id: COLLECTION_CONTROL_ID, payload: stored }] }); if (body.points !== undefined) stored = ((body.points as Array<{ payload: Record<string, unknown> }>)[0]?.payload ?? stored); return json({ result: { status: "acknowledged" } }); };
    const writer = client(fetchImpl); await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, state: "draining" } })).resolves.toBe(true); await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 2, expectedEpoch: 2, patch: { version: 3, state: "active" } })).resolves.toBe(true); await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 3, expectedEpoch: 2, patch: { version: 4, state: "retired" } })).resolves.toBe(true);
    const writes = calls.filter((call) => call.body.update_mode === "update_only"); expect(writes).toHaveLength(3); expect((writes[0]?.body.update_filter as { must: Array<{ key: string; match?: { value?: unknown } }> }).must).toEqual(expect.arrayContaining([{ key: "privacy_epoch", match: { value: 0 } }, { key: "state", match: { value: "active" } }])); expect((writes[1]?.body.update_filter as { must: Array<{ key: string; match?: { value?: unknown } }> }).must).toEqual(expect.arrayContaining([{ key: "privacy_epoch", match: { value: 0 } }, { key: "state", match: { value: "draining" } }]));
    const next = control({ version: 5, state: "active" }); await expect(publishControlCas(writer, { expectedVersion: 4, expectedBaseGeneration: null, next })).resolves.toBe(false);
  });

  it("rejects stale generation privacy/policy payloads before publication write", async () => {
    const current = control({ privacyEpoch: 2 }); const next = control({ version: 2, privacyEpoch: 1 }); let writes = 0;
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/retrieve")) return json({ result: [{ id: COLLECTION_CONTROL_ID, payload: controlPayload(current) }] }); if (init.body !== undefined && JSON.parse(String(init.body)).points !== undefined) writes += 1; return json({ result: { status: "acknowledged" } }); };
    await expect(publishControlCas(client(fetchImpl), { expectedVersion: 1, expectedBaseGeneration: null, next })).resolves.toBe(false); expect(writes).toBe(0);
  });
  it("enforces monotonic privacy/coordination epochs and policy transitions", async () => {
    const current = control({ privacyEpoch: 2, coordinationPolicyEpoch: 2 }); let stored: Record<string, unknown> = controlPayload(current); let writes = 0;
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>; if (url.includes("/retrieve")) return json({ result: [{ id: COLLECTION_CONTROL_ID, payload: stored }] }); if (body.points !== undefined) { writes += 1; stored = ((body.points as Array<{ payload: Record<string, unknown> }>)[0]?.payload ?? stored); } return json({ result: { status: "acknowledged" } }); };
    const writer = client(fetchImpl);
    await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, privacyEpoch: 1 } })).resolves.toBe(false);
    await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, privacyEpoch: 4 } })).resolves.toBe(false);
    await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, coordinationPolicyHash: "changed" } })).resolves.toBe(false);
    await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, privacyEpoch: 3, coordinationPolicyEpoch: 3, coordinationPolicyHash: "policy-3", processingPolicyId: "policy-3" } })).resolves.toBe(true); expect(writes).toBe(1);
  });

  it("separates admin key and creates named-vector/index contracts", async () => {
    const calls: Array<{ url: string; method?: string; body?: unknown; headers: Headers }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      calls.push({ url: String(input), method: init.method, body: init.body === undefined ? undefined : JSON.parse(String(init.body)), headers: new Headers(init.headers) });
      return json({ result: true, status: "ok" });
    };
    const admin = new QdrantAdminClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "human-admin", timeoutMs: 1, fetchImpl, replicationFactor: 1, writeConsistencyFactor: 1 });
    await admin.createCollection();
    for (const [field, schema] of REQUIRED_INDEXES) await admin.createPayloadIndex(field, schema);
    const collection = calls[0]?.body as { vectors: Record<string, unknown> };
    expect(collection.vectors.semantic).toEqual({ size: 1024, distance: "Cosine" });
    expect(calls.every((call) => call.headers.get("api-key") === "human-admin" && !call.headers.has("authorization"))).toBe(true);
    expect(V2_COLLECTION_METADATA.schema).toBe("pi-qdrant-memory-v2");
    expect(COLLECTION_METADATA_ID).toBe(deterministicUuid("pi-qdrant-memory-v2", "collection_metadata"));
    expect(COLLECTION_CONTROL_ID).toBe(deterministicUuid("pi-qdrant-memory-v2", "collection_control"));
  });

  it("keeps destructive deletion exclusively on the human admin client", async () => {
    const calls: Array<{ url: string; method?: string; body?: Record<string, unknown> }> = [];
    const id = episodeId("pi", "delete", "point");
    const admin = new QdrantAdminClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", apiKey: "human-admin", timeoutMs: 1, fetchImpl: async (input, init = {}) => { calls.push({ url: String(input), method: init.method, body: init.body === undefined ? undefined : JSON.parse(String(init.body)) }); return json({ result: { status: "acknowledged" } }); } });
    await admin.deletePoints([id]);
    expect(calls[0]?.url).toContain("/points/delete?wait=true"); expect(calls[0]?.url).not.toContain("ordering="); expect(calls[0]?.method).toBe("POST"); expect(calls[0]?.body).toEqual({ points: [id] });
  });

  it("publishes control only with version and base-generation CAS and rereads", async () => {
    const next = {
      ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0,
      processingPolicyId: "policy-1", expiresAt: null, contentHash: "pending", recordType: "collection_control" as const,
      id: COLLECTION_CONTROL_ID, version: 2, activeGeneration: "gen-2", activeBaseGeneration: "gen-1", coordinationPolicyEpoch: 2,
      coordinationPolicyHash: "policy-hash", state: "active" as const, scanCursor: null, lastForgetBarrier: null,
    } satisfies ControlRecord;
    next.contentHash = canonicalRecordHash(next);
    const calls: Array<{ url: string; body: Record<string, unknown> }> = []; let stored: Record<string, unknown> = controlPayload(control({ activeBaseGeneration: "gen-1" }));
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const body = (init.body === undefined ? {} : JSON.parse(String(init.body))) as Record<string, unknown>; calls.push({ url, body });
      if (url.includes("/retrieve")) return json({ result: [{ id: next.id, payload: stored }] }); if (body.points !== undefined) stored = ((body.points as Array<{ payload: Record<string, unknown> }>)[0]?.payload ?? stored); return json({ result: { status: "acknowledged" }, status: "ok" });
    };
    await expect(publishControlCas(client(fetchImpl), { expectedVersion: 1, expectedBaseGeneration: "gen-1", next })).resolves.toBe(true);
    const writeCall = calls.find((call) => call.body.update_mode === "update_only");
    expect(writeCall?.body.update_mode).toBe("update_only");
    expect(writeCall?.body.update_filter).toEqual(expect.objectContaining({ must: expect.arrayContaining([
      { key: "version", match: { value: 1 } },
      { key: "active_base_generation", match: { value: "gen-1" } },
    ]) }));
  });
});


  it("serializes Episode vectors as named vectors, never payload fields", async () => {
    const target = episode({ vector: Array.from({ length: 1024 }, () => 0) }); let stored: Record<string, unknown> | undefined; let sent: Record<string, unknown> | undefined;
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as Record<string, unknown>; if (url.includes("/points/retrieve")) return json({ result: stored === undefined ? [] : [{ id: physicalPointId("episode", target.id), payload: stored }] }); if (url.includes("/points?")) { const point = (body?.points as Array<{ payload: Record<string, unknown>; vector?: Record<string, unknown> }>)[0]!; sent = point as unknown as Record<string, unknown>; stored = point.payload; return json({ result: { status: "acknowledged" } }); } return json({ result: { status: "ok" } }); };
    await expect(insertOnly(client(fetchImpl), target)).resolves.toBe("inserted"); const point = sent as { payload: Record<string, unknown>; vector?: { semantic: number[] } }; expect(point.payload.vector).toBeUndefined(); expect(point.vector?.semantic).toHaveLength(1024);
  });

  it("allows tombstone verification only through explicit internal policy", async () => {
    const target = tombstone(); const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, body }); if (url.includes("/points/scroll")) return json({ result: { points: [{ id: physicalPointId("tombstone", target.id), payload: { owner_host: "pi", record_type: "tombstone", status: "active", secret_scan: "passed", expires_at: null } }] } }); return json({ result: [] }); };
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1, fetchImpl });
    const policy = readPolicy({ ownerHost: "pi", purpose: "internal", recordTypes: ["tombstone"], maxClockSkewMs: 0 });
    await expect(read.scroll({ policy, limit: 1 })).resolves.toHaveProperty("points");
    expect((calls[0]?.body.filter as { must_not: unknown[] }).must_not).toEqual([]);
    await expect(read.scroll({ policy: { ...policy, purpose: "memory", recordTypes: ["tombstone"] } as typeof policy, limit: 1 })).rejects.toThrow(/policy/i);
  });

  it("verifies tombstone writes after acknowledgement and preserves payload-only fields", async () => {
    const target = tombstone({ provenanceId: "provenance-1" }); let stored: Record<string, unknown> | undefined; let writes = 0; const calls: Array<{ url: string; method?: string; body?: Record<string, unknown>; headers: Headers }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, method: init.method, body, headers: new Headers(init.headers) }); if (url.includes("/points/retrieve")) return json({ result: stored === undefined ? [] : [{ id: physicalPointId("tombstone", target.id), payload: stored }] }); if (url.includes("/points?")) { writes += 1; stored = ((body?.points as Array<{ payload: Record<string, unknown> }>)[0]?.payload); return json({ result: { status: "acknowledged" } }); } return json({ result: { status: "ok" } }); };
    const writer = client(fetchImpl); await expect(insertOnly(writer, target)).resolves.toBe("inserted"); await expect(insertOnly(writer, target)).resolves.toBe("existing");
    const writeCall = calls.find((call) => call.url.includes("/points?")); expect(writeCall?.method).toBe("PUT"); expect(writeCall?.body?.update_mode).toBe("insert_only"); expect((writeCall?.body?.points as unknown[]).length).toBe(1); expect(new URL(writeCall!.url).searchParams.get("wait")).toBe("true"); expect(new URL(writeCall!.url).searchParams.get("ordering")).toBe("strong"); expect(writeCall?.headers.get("api-key")).toBe("collection-scoped"); const point = ((writeCall?.body?.points as Array<{ payload: Record<string, unknown>; vector?: unknown }>)[0]); expect(point?.vector).toBeUndefined(); expect(point?.payload.provenance_id).toBe("provenance-1"); expect(point?.payload.provenanceId).toBeUndefined(); expect(point?.payload.vector).toBeUndefined();
  });

  it("detects an ignored insert-only write from the post-read hash", async () => {
    const target = tombstone(); let reads = 0; let writeSeen = false; const calls: Array<{ url: string; body?: Record<string, unknown> }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, body }); if (url.includes("/points/retrieve")) { reads += 1; return json({ result: reads === 1 ? [] : [{ id: physicalPointId("tombstone", target.id), payload: { owner_host: "pi", record_type: "tombstone", status: "active", secret_scan: "passed", expires_at: null, content_hash: "different" } }] }); } if (url.includes("/points?")) { writeSeen = true; return json({ result: { status: "acknowledged" } }); } return json({ result: { status: "ok" } }); };
    await expect(insertOnly(client(fetchImpl), target)).rejects.toThrow(/collision/i); expect(writeSeen).toBe(true); const write = calls.find((call) => call.url.includes("/points?")); expect(write?.body?.update_mode).toBe("insert_only"); expect(new URL(write!.url).searchParams.toString()).toBe("wait=true&ordering=strong"); expect(calls.filter((call) => call.url.includes("/points?")).length).toBe(1);
  });

describe("strict REST request and physical point contracts", () => {
  const policy = { ownerHost: "pi" as const, purpose: "memory" as const, recordTypes: ["episode" as const], now: Date.now(), maxClockSkewMs: 0, requireStatus: "active" as const, requireSecretScan: "passed" as const };
  it("does not send a made-up retrieve filter and rejects missing response ownership", async () => {
    const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
    const id = episodeId("pi", "session-1", "retrieve-message");
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000, fetchImpl: async (input, init = {}) => { calls.push({ url: String(input), body: JSON.parse(String(init.body)) }); return json({ result: [{ id, payload: { record_type: "episode", status: "active", secret_scan: "passed" } }] }); } });
    await expect(read.retrieve([id], policy)).rejects.toMatchObject({ category: "invalid-response" });
    expect(calls[0]?.body.filter).toBeUndefined();
  });
  it("uses Qdrant is_null expiry conditions and a real named-vector search body", async () => {
    const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
    const id = episodeId("pi", "session-1", "search-message");
    const fetchImpl: typeof fetch = async (input, init = {}) => { const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url: String(input), body }); if (String(input).includes("/search")) return json({ result: [{ id, score: 0.9, payload: { owner_host: "pi", record_type: "episode", status: "active", secret_scan: "passed", expires_at: null } }] }); if (String(input).includes("/scroll")) return json({ result: { points: [] } }); return json({ result: { count: 1 } }); };
    const read = new QdrantReadClient({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000, fetchImpl });
    await read.scroll({ policy });
    await read.search({ vector: Array.from({ length: 1024 }, () => 0), limit: 1, policy });
    const scrollFilter = calls[0]?.body.filter as { should?: unknown[] };
    expect(JSON.stringify(scrollFilter)).toContain('\"is_null\":{\"key\":\"expires_at\"}');
    expect(calls[1]?.body.using).toBeUndefined();
    expect(calls[1]?.body.vector).toEqual({ name: "semantic", vector: Array.from({ length: 1024 }, () => 0) });
  });
  it("accepts only UUID physical point IDs and maps logical record IDs deterministically", async () => {
    const fetchImpl: typeof fetch = async () => json({ result: { status: "acknowledged" } });
    const writer = new QdrantSessionWriter({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000, fetchImpl });
    expect(physicalPointId("episode", "content-hash-not-a-uuid")).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[45][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
    await expect(writer.upsertPoints([{ id: "logical-record-id", payload: { owner_host: "pi" } }], "insert_only")).rejects.toMatchObject({ category: "configuration" });
  });
  it("preflights insert-only and reports a pre-existing equal point across fresh client instances", async () => {
    const record = episode(); const physical = physicalPointId(record.recordType, record.id); let writes = 0;
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/retrieve")) return json({ result: [{ id: physical, payload: { owner_host: "pi", record_type: "episode", status: "active", secret_scan: "passed", expires_at: null, content_hash: record.contentHash } }] }); writes += 1; return json({ result: { status: "acknowledged" } }); };
    await expect(insertOnly(new QdrantSessionWriter({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000, fetchImpl }), record)).resolves.toBe("existing");
    expect(writes).toBe(1);
  });
  it("rejects arbitrary CAS patches and preserves full control payload on update-only", async () => {
    const calls: Array<{ url: string; body: Record<string, unknown> }> = []; let stored: Record<string, unknown> = controlPayload(control());
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); const body = init.body === undefined ? {} : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, body }); if (url.includes("/retrieve")) return json({ result: [{ id: COLLECTION_CONTROL_ID, vector: { semantic: Array.from({ length: 1024 }, () => 0) }, payload: stored }] }); if (body.points !== undefined) stored = ((body.points as Array<{ payload: Record<string, unknown> }>)[0]?.payload ?? stored); return json({ result: { status: "acknowledged" } }); };
    const writer = new QdrantSessionWriter({ baseUrl: "https://qdrant.example", collection: "pi_memory", ownerHost: "pi", timeoutMs: 1000, fetchImpl });
    await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, scanCursor: "cursor-new" } })).resolves.toBe(true);
    const sent = calls.find((call) => call.url.includes("/points?"))?.body.points as Array<{ payload: Record<string, unknown> }>;
    expect(sent[0]?.payload.state).toBe("active");
    expect(sent[0]?.payload.scan_cursor).toBe("cursor-new");
    await expect(updateOnlyCas(writer, { id: COLLECTION_CONTROL_ID, expectedVersion: 1, expectedEpoch: 2, patch: { version: 2, ownerHost: "prime" } })).rejects.toThrow(/patch|immutable|allow/i);
  });
});
