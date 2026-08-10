import { describe, expect, it } from "vitest";
import { initializeDestination } from "../../src/admin/init.js";
import { memoryStatus } from "../../src/admin/status.js";
import { defaultCliDependencies } from "../../src/admin/cli.js";
import { collectionMetadataPayload, COLLECTION_METADATA_ID, COLLECTION_CONTROL_ID, REQUIRED_INDEXES, bootstrapControlHash, controlPayload, V2_CONTRACT_HASH } from "../../src/qdrant/schema.js";
import type { RuntimeConfig } from "../../src/types.js";

function config(): RuntimeConfig {
  return {
    host: "pi", enabled: true, autoRecall: true, configPath: "/tmp/config.json",
    qdrant: { url: "http://destination", collection: "pi_memory", apiKey: "runtime-secret", replicationFactor: 1, writeConsistencyFactor: 1 },
    embeddings: { baseUrl: "http://embeddings/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", apiKey: "embedding-secret" },
    retrieval: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0.05, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true },
    projects: { registrations: {} },
    capture: { enabled: false, projectAllowlist: [], projectDenylist: [], episodeRetentionDays: "indefinite", toolArgsChars: 2000, toolResultChars: 4000 },
    privacy: { egressMode: "local_only", allowedQdrantDestinations: [], allowedEmbeddingDestinations: [], allowedLlmDestinations: [], allowActiveModelFallback: false, allowCrossProviderReplay: false },
    coordination: { maxClockSkewMs: 300000, readConsistency: 1, leaseMs: 30000, reconcileIntervalMs: 900000 },
    outbox: { maxJobs: 10000, maxBytes: 268435456, retryBaseMs: 500, retryMaxMs: 30000, sharedFilesystem: false },
    curation: { turnTrigger: 10, toolTrigger: 15, maxInputTokens: 12000 },
    memoryModel: { timeoutMs: 30000, maxOutputTokens: 2048 },
    raptor: { rebuildEpisodeDelta: 64, maxLevels: 5, summaryInputTokens: 12000, umapDimensions: 10, localNeighbors: 10, gmmMaxClusters: 50, membershipThreshold: 0.1 },
  };
}

describe("destination-only v2 admin shell", () => {
  it("returns immutable destination contract details without a network call", async () => {
    await expect(initializeDestination(config())).resolves.toMatchObject({ host: "pi", ownerHost: "pi", collection: "pi_memory", schema: "pi-qdrant-memory-v2", schemaRevision: 1, vector: { name: "semantic", model: "bge-m3", dimension: 1024, distance: "Cosine" }, initialized: false });
  });

  it("reports destination and policy state without a second collection", async () => {
    const result = await memoryStatus(config());
    expect(result.destination).toMatchObject({ endpoint: "http://destination", collection: "pi_memory", ownerHost: "pi", schema: "pi-qdrant-memory-v2", exists: false, healthy: false, keyConfigured: true });
    expect(result).not.toHaveProperty("source");
    expect(result.capture.enabled).toBe(false);
    expect(result.privacy.egressMode).toBe("local_only");
    expect(result.qdrant.probed).toBe(false);
  });
});


describe("Qdrant 1.17 initialization and runtime status probes", () => {
  const existingControl = (() => { const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-10T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: V2_CONTRACT_HASH, expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 0, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 0, coordinationPolicyHash: V2_CONTRACT_HASH, state: "active" as const, scanCursor: null, lastForgetBarrier: null, contentHash: "pending" }; return { ...base, contentHash: bootstrapControlHash(base) }; })();
  function collectionResponse(): unknown {
    const payload_schema: Record<string, unknown> = {};
    for (const [field, data_type] of REQUIRED_INDEXES) payload_schema[field] = { data_type };
    return { result: { status: "green", points_count: 2, config: { params: { vectors: { semantic: { size: 1024, distance: "Cosine" } } } }, payload_schema }, status: "ok" };
  }
  it("initializes metadata/control with the human key and rereads immutable points", async () => {
    const calls: Array<{ url: string; method: string; body?: Record<string, unknown>; headers: Headers }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as Record<string, unknown>;
      calls.push({ url, method: init.method ?? "GET", ...(body === undefined ? {} : { body }), headers: new Headers(init.headers) });
      if (url.endsWith("/")) return new Response(JSON.stringify({ title: "qdrant", version: "1.17.1" }), { status: 200 });
      if (url.includes("/points/retrieve")) {
        const ids = (body?.ids ?? []) as string[];
        return new Response(JSON.stringify({ result: ids[0] === COLLECTION_METADATA_ID ? [{ id: COLLECTION_METADATA_ID, payload: collectionMetadataPayload("pi") }] : [{ id: COLLECTION_CONTROL_ID, payload: controlPayload(existingControl) }], status: "ok" }), { status: 200 });
      }
      if (url.includes("/collections/pi_memory") && init.method === "GET") return new Response(JSON.stringify(collectionResponse()), { status: 200 });
      return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 });
    };
    const result = await initializeDestination(config(), { adminApiKey: "human-admin", fetchImpl });
    expect(result).toMatchObject({ initialized: true, collectionCreated: false, qdrantVersion: "1.17.1" });
    expect(calls.some((call) => call.url.endsWith("/version"))).toBe(false);
    expect(calls.some((call) => call.url.endsWith("/") && call.method === "GET")).toBe(true);
    expect(calls.filter((call) => call.method === "PUT" && call.url.includes("/points?")).map((call) => call.body?.update_mode)).toEqual([]);
    expect(calls.filter((call) => call.url.includes("/index?")).length).toBe(REQUIRED_INDEXES.length);
    expect(calls.every((call) => call.headers.get("api-key") === "human-admin" && !call.headers.has("authorization"))).toBe(true);
  });

  it("status probes only the configured collection with the runtime key", async () => {
    const calls: Array<{ url: string; headers: Headers }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      calls.push({ url: String(input), headers: new Headers(init.headers) });
      if (String(input).endsWith("/healthz")) return new Response(JSON.stringify({ result: { status: "ok" }, status: "ok" }), { status: 200 });
      if (String(input).includes("/points/retrieve")) return new Response(JSON.stringify({ result: [{ id: COLLECTION_METADATA_ID, payload: collectionMetadataPayload("pi") }], status: "ok" }), { status: 200 });
      return new Response(JSON.stringify(collectionResponse()), { status: 200 });
    };
    const result = await memoryStatus(config(), { fetchImpl });
    expect(result.qdrant).toEqual({ healthy: true, destinationHealthy: true, probed: true });
    expect(result.destination).toMatchObject({ exists: true, healthy: true, keyConfigured: true });
    expect(calls.every((call) => call.headers.get("api-key") === "runtime-secret" && !call.headers.has("authorization"))).toBe(true);
  });

  it("creates a new collection before metadata/control and performs bootstrap inserts exactly once", async () => {
    const calls: Array<{ url: string; method: string; body?: Record<string, unknown> }> = []; let collection = false; let metadata = false; let control = false; let storedControl: Record<string, unknown> | undefined;
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const method = init.method ?? "GET"; const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as Record<string, unknown>; calls.push({ url, method, ...(body === undefined ? {} : { body }) });
      if (url.endsWith("/")) return new Response(JSON.stringify({ title: "qdrant", version: "1.17.1" }), { status: 200 });
      if (url.includes("/collections/pi_memory") && method === "GET" && !url.includes("/points")) { if (!collection) return new Response("missing", { status: 404 }); return new Response(JSON.stringify(collectionResponse()), { status: 200 }); }
      if (url.includes("/collections/pi_memory/points/retrieve")) { const ids = (body?.ids ?? []) as string[]; if (ids[0] === COLLECTION_METADATA_ID && !metadata) return new Response(JSON.stringify({ result: [] }), { status: 200 }); return new Response(JSON.stringify({ result: [{ id: ids[0], payload: ids[0] === COLLECTION_METADATA_ID ? collectionMetadataPayload("pi") : storedControl ?? controlPayload(existingControl) }] }), { status: 200 }); }
      if (url.endsWith("/collections/pi_memory") && method === "PUT") { collection = true; return new Response(JSON.stringify({ result: true, status: "ok" }), { status: 200 }); }
      if (url.includes("/points?") && method === "PUT") { const points = (body?.points ?? []) as Array<{ payload?: Record<string, unknown> }>; if (points[0]?.payload?.record_type === "collection_metadata") metadata = true; if (points[0]?.payload?.record_type === "collection_control") { control = true; storedControl = points[0]?.payload; } return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 }); }
      return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 });
    };
    const result = await initializeDestination(config(), { adminApiKey: "human-admin", fetchImpl });
    expect(result).toMatchObject({ initialized: true, collectionCreated: true });
    expect(metadata).toBe(true); expect(control).toBe(true);
    expect(calls.filter((call) => call.url.includes("/index?")).length).toBe(REQUIRED_INDEXES.length);
  });

  it.each(["missing", "foreign"])("fails closed for pre-existing %s metadata before mutating indexes", async (kind) => {
    const calls: Array<{ url: string; method: string }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const method = init.method ?? "GET"; calls.push({ url, method });
      if (url.endsWith("/")) return new Response(JSON.stringify({ title: "qdrant", version: "1.17.1" }), { status: 200 });
      if (url.includes("/points/retrieve")) { const payload = kind === "missing" ? [] : [{ id: COLLECTION_METADATA_ID, payload: collectionMetadataPayload("prime") }]; return new Response(JSON.stringify({ result: payload }), { status: 200 }); }
      if (url.includes("/collections/pi_memory") && method === "GET") return new Response(JSON.stringify(collectionResponse()), { status: 200 });
      return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 });
    };
    await expect(initializeDestination(config(), { adminApiKey: "human-admin", fetchImpl })).rejects.toThrow(/metadata|foreign|missing/i);
    expect(calls.some((call) => call.url.includes("/index?"))).toBe(false);
    expect(calls.some((call) => call.url.includes("/points?") && call.method === "PUT")).toBe(false);
  });

  it.each(["bad-date", "empty-policy", "extra-field"])("rejects malformed pre-existing bootstrap control (%s) before indexes", async (kind) => {
    const calls: string[] = []; const bad: Record<string, unknown> = controlPayload(existingControl); if (kind === "bad-date") bad.created_at = "not-an-iso-date"; if (kind === "empty-policy") bad.processing_policy_id = ""; if (kind === "extra-field") bad.unexpected = true;
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); calls.push(`${init.method ?? "GET"} ${url}`); if (url.endsWith("/")) return new Response(JSON.stringify({ title: "qdrant", version: "1.17.1" }), { status: 200 }); if (url.includes("/points/retrieve")) { const body = JSON.parse(String(init.body)) as { ids: string[] }; return new Response(JSON.stringify({ result: [{ id: body.ids[0], payload: body.ids[0] === COLLECTION_METADATA_ID ? collectionMetadataPayload("pi") : bad }] }), { status: 200 }); } if (url.includes("/collections/pi_memory") && init.method === "GET") return new Response(JSON.stringify(collectionResponse()), { status: 200 }); return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 }); };
    await expect(initializeDestination(config(), { adminApiKey: "human-admin", fetchImpl })).rejects.toThrow(/control|metadata/i); expect(calls.some((call) => call.includes("/index?"))).toBe(false);
  });

  it("rereads a concurrent create winner with bounded metadata/control retries", async () => {
    const calls: string[] = []; let readCount = 0; let metadataReads = 0; let controlReads = 0;
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); calls.push(`${init.method ?? "GET"} ${url}`);
      if (url.endsWith("/")) return new Response(JSON.stringify({ title: "qdrant", version: "1.17.1" }), { status: 200 });
      if (url.includes("/collections/pi_memory/points/retrieve")) { const body = JSON.parse(String(init.body)) as { ids: string[] }; if (body.ids[0] === COLLECTION_METADATA_ID) { metadataReads += 1; return new Response(JSON.stringify({ result: metadataReads === 1 ? [] : [{ id: COLLECTION_METADATA_ID, payload: collectionMetadataPayload("pi") }] }), { status: 200 }); } controlReads += 1; return new Response(JSON.stringify({ result: controlReads === 1 ? [] : [{ id: COLLECTION_CONTROL_ID, payload: controlPayload(existingControl) }] }), { status: 200 }); }
      if (url.includes("/collections/pi_memory") && init.method === "GET") { readCount += 1; if (readCount === 1) return new Response("missing", { status: 404 }); return new Response(JSON.stringify(collectionResponse()), { status: 200 }); }
      if (url.endsWith("/collections/pi_memory") && init.method === "PUT") return new Response("conflict", { status: 409 });
      return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 });
    };
    const result = await initializeDestination({ ...config(), host: "pi" }, { adminApiKey: "human-admin", fetchImpl, retryAttempts: 3 });
    expect(result).toMatchObject({ initialized: true, collectionCreated: false }); expect(metadataReads).toBe(2); expect(controlReads).toBe(2); expect(calls.some((call) => call.includes("/index?"))).toBe(true);
  });

  it("does not read ambient admin credentials from direct library initialization", async () => {
    const previous = process.env.PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY; process.env.PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY = "ambient-secret";
    try { await expect(initializeDestination(config())).resolves.toMatchObject({ initialized: false, collectionCreated: false }); } finally { if (previous === undefined) delete process.env.PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY; else process.env.PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY = previous; }
  });

  it("fails CLI initialization closed when the human admin key is absent", async () => {
    const deps = defaultCliDependencies({ env: { PI_QDRANT_MEMORY_HOST: "pi", PI_QDRANT_MEMORY_QDRANT_API_KEY: "runtime-only" } });
    expect(() => deps.initialize(config())).toThrow(/human.*admin.*key/i);
  });

});
