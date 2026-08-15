import { describe, expect, it, vi } from "vitest";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type LeaseRecord, type ProcessingPolicy, type ProcessingPolicyRecord } from "../../src/domain/records.js";
import { processingPolicyHash } from "../../src/domain/policy.js";
import { leasePointId, manifestHash } from "../../src/domain/ids.js";
import { controlPayload, controlRecordFromPayload, COLLECTION_CONTROL_ID, COLLECTION_METADATA_ID, collectionMetadataPayload, physicalPointId, REQUIRED_INDEXES } from "../../src/qdrant/schema.js";
import { defaultCliDependencies, main, type CliDependencies } from "../../src/admin/cli.js";
import { AdminPlanError } from "../../src/admin/errors.js";
import { createStoredPlan } from "../../src/admin/production.js";
import { recordPayload } from "../../src/qdrant/write.js";
import { inspectRecords } from "../../src/admin/inspect.js";
import { planForget, runForget } from "../../src/admin/forget.js";
import { planPrivacyRevoke, revokePrivacy } from "../../src/admin/privacy.js";
import type { RuntimeConfig } from "../../src/types.js";

function config(): RuntimeConfig {
  return {
    host: "pi", enabled: true, autoRecall: true, configPath: "/tmp/pi-qdrant-memory-config.json",
    qdrant: { url: "http://127.0.0.1:6333", collection: "pi_memory", replicationFactor: 1, writeConsistencyFactor: 1 },
    embeddings: { baseUrl: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: " },
    retrieval: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0.05, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true },
    projects: { registrations: {} },
    capture: { enabled: false, projectAllowlist: [], projectDenylist: [], episodeRetentionDays: "indefinite", toolArgsChars: 2000, toolResultChars: 4000 },
    privacy: { egressMode: "local_only", allowedQdrantDestinations: [], allowedEmbeddingDestinations: [], allowedLlmDestinations: [], allowActiveModelFallback: false, allowCrossProviderReplay: false },
    coordination: { maxClockSkewMs: 300000, readConsistency: 1, leaseMs: 30000, reconcileIntervalMs: 900000 },
    outbox: { maxJobs: 10000, maxBytes: 268435456, retryBaseMs: 500, retryMaxMs: 30000, sharedFilesystem: false },
    curation: { turnTrigger: 10, toolTrigger: 15, maxInputTokens: 12000 }, memoryModel: { timeoutMs: 30000, maxOutputTokens: 2048 },
    raptor: { rebuildEpisodeDelta: 64, maxLevels: 5, summaryInputTokens: 12000, umapDimensions: 10, localNeighbors: 10, gmmMaxClusters: 50, membershipThreshold: 0.1 },
  };
}

function controlAt(privacyEpoch: number, version = 1, state: ControlRecord["state"] = "active", processingPolicyId = "policy-id"): ControlRecord {
  const base = { ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-14T00:00:00.000Z", privacyEpoch, processingPolicyId, expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version, activeGeneration: state === "active" ? "generation-1" : null, activeBaseGeneration: null, coordinationPolicyEpoch: 0, coordinationPolicyHash: "coordination-policy-hash", state, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [] as string[], contentHash: "pending" };
  return { ...base, contentHash: canonicalRecordHash(base) };
}

function cli(overrides: Partial<CliDependencies> = {}): CliDependencies {
  return { env: { PI_QDRANT_MEMORY_HOST: "pi" }, loadConfig: async () => config(), initialize: async () => ({ host: "pi", collection: "pi_memory", ownerHost: "pi", schema: "pi-qdrant-memory-v2", schemaRevision: 1, vector: { name: "semantic", model: "bge-m3", dimension: 1024, distance: "Dot" }, capture: { enabled: false, episodeRetentionDays: "indefinite" }, initialized: false, collectionCreated: false }), status: async () => ({ host: "pi", configPath: "/tmp/config", enabled: true, autoRecall: true, destination: { endpoint: "http://127.0.0.1:6333", collection: "pi_memory", ownerHost: "pi", schema: "pi-qdrant-memory-v2", dimension: 1024, distance: "Dot", exists: false, healthy: false, keyConfigured: false }, embeddings: { endpoint: "http://127.0.0.1:8080/v1", model: "bge-m3", dimension: 1024, healthy: false, keyConfigured: false }, capture: { enabled: false, episodeRetentionDays: "indefinite" }, privacy: { egressMode: "local_only", qdrantDestinations: 0, embeddingDestinations: 0, llmDestinations: 0 }, qdrant: { healthy: false, destinationHealthy: false, probed: false } }), writeStdout: () => undefined, writeStderr: () => undefined, ...overrides };
}

describe("human admin operations", () => {
  it("keeps privacy plans deterministic and requires exact approval", async () => {
    const plan = planPrivacyRevoke({ ownerHost: "pi", currentPrivacyEpoch: 2, destinationIds: ["qdrant:local"], requestedAt: "2026-08-14T00:00:00.000Z" });
    await expect(revokePrivacy({ plan, approvedPlanId: "wrong" }, {})).rejects.toThrow(/approval/i);
    let advanced = false;
    const result = await revokePrivacy({ plan, approvedPlanId: plan.id }, { advancePrivacyEpoch: async input => { advanced = input.nextEpoch === 3; }, rereadControl: async () => ({ ownerHost: "pi", privacyEpoch: 3, activeGeneration: null, state: "draining", revokedDestinationIds: ["qdrant:local"] }) });
    expect(advanced).toBe(true); expect(result).toMatchObject({ ok: true, privacyEpoch: 3, logicalInvisible: true });
  });

  it("resolves a current view before separating occurrence from recurrence", async () => {
    const plan = await planForget({ selection: { curatedCurrentId: "current-1" }, scope: "occurrence", resolveCurrent: async id => ({ id, observationId: "observation-1", contentId: "content-1", stateKey: "state-1", sourceEpisodeIds: ["episode-1"] }) });
    expect(plan.targets).toContain("observation-1"); expect(plan.recurrenceBlocked).toBe(false);
    await expect(runForget({ plan, approvedPlanId: "wrong" }, {})).rejects.toThrow(/approval/i);
    const result = await runForget({ plan, approvedPlanId: plan.id }, { readControl: async () => ({ ownerHost: "pi", privacyEpoch: 0, activeGeneration: "generation-1" }), createTombstones: async input => { expect(input.scope).toBe("occurrence"); expect(input.targetIds).toContain("observation-1"); }, beginForgetBarrier: async () => ({ privacyEpoch: 1, activeGeneration: null }), rereadBarrier: async () => ({ privacyEpoch: 1, activeGeneration: null }), readTombstones: async () => [{ scope: "occurrence", targetId: "observation-1" }] });
    expect(result).toMatchObject({ ok: true, logicalInvisible: true, recurrenceBlocked: false });
  });

  it("bounds and redacts inspection metadata", async () => {
    const result = await inspectRecords({ limit: 2 }, { records: [{ id: "episode-1", recordType: "episode", text: "raw secret text", vector: [1], contentHash: "a".repeat(64) }, { id: "episode-2", recordType: "episode", status: "active" }, { id: "episode-3", recordType: "episode" }] });
    expect(result.count).toBe(2); expect(result.records[0]).not.toHaveProperty("text"); expect(result.records[0]).not.toHaveProperty("vector"); expect(result.records[0]).toHaveProperty("contentHash"); expect(result.truncated).toBe(true);
  });

  it("parses privacy plan/apply as a human-only command", async () => {
    let output = "";
    const plan = planPrivacyRevoke({ ownerHost: "pi", currentPrivacyEpoch: 0, requestedAt: "2026-08-14T00:00:00.000Z" });
    const deps = cli({ writeStdout: value => { output += value; }, privacyPlan: () => plan, privacyApply: async (_config, input) => ({ ok: true, planId: input.plan.id, previousPrivacyEpoch: 0, privacyEpoch: 1, generationInvalidated: true, logicalInvisible: true, inFlightCallsCannotBeRevoked: true, reconciled: false }) });
    await expect(main(["privacy", "revoke", "--plan", "--json"], deps)).resolves.toBe(0);
    expect(output).toContain(plan.id);
    output = "";
    await expect(main(["privacy", "revoke", `--plan=${plan.id}`, "--json"], deps)).resolves.toBe(0);
    expect(output).toContain(plan.id);
    output = "";
    await expect(main(["privacy", "revoke", "--approve", plan.id, "--json"], deps)).resolves.toBe(0);
    expect(output).toContain("privacyEpoch");
    const stale = cli({ privacyPlan: () => plan, privacyApply: async () => { throw new AdminPlanError("stale plan"); } });
    await expect(main(["privacy", "revoke", "--approve", plan.id, "--json"], stale)).resolves.toBe(2);
  });

  it("executes production default privacy/status/inspect dependencies without injected seams", async () => {
    const homeDir = mkdtempSync(`${tmpdir()}/pi-qdrant-admin-`);
    const dynamicPoints = new Map<string, { id: string; payload: Record<string, unknown> }>();
    const episodeId = "00000000-0000-5000-8000-000000000001";
    const policy: ProcessingPolicy = { id: "pending", ownerHost: "pi", destinationIds: { qdrant: "qdrant:pi", embedding: "embedding:local", llm: "llm:local" }, originProvider: "test-provider", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "private", policyRevision: "1" };
    policy.id = processingPolicyHash(policy);
    let current = controlAt(3, 1, "active", policy.id);
    const metadata = collectionMetadataPayload("pi");
    const episodeBase: EpisodeRecord = { ownerHost: "pi", schemaRevision: 1, createdAt: "2026-08-14T00:00:00.000Z", privacyEpoch: 3, processingPolicyId: policy.id, expiresAt: null, recordType: "episode", id: episodeId, contentHash: "pending", sourceEntryId: "entry-1", host: "pi", projectId: "project-1", projectIdentityKind: "local_only", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2026-08-14T00:00:00.000Z", modelId: "test-model", embeddingDimension: 1024, originProvider: "test-provider", destinationId: "llm:local", status: "active", redactionStatus: "unchanged", secretScan: "passed", text: "safe test episode", vector: Array.from({ length: 1024 }, () => 0) };
    const episode = { ...episodeBase, contentHash: canonicalRecordHash(episodeBase) } as EpisodeRecord;
    const policyBase: ProcessingPolicyRecord = { ownerHost: "pi", schemaRevision: 1, createdAt: episode.createdAt, privacyEpoch: 3, processingPolicyId: policy.id, expiresAt: policy.expiresAt, recordType: "processing_policy", id: policy.id, policy, canonicalHash: policy.id, contentHash: "pending" };
    const policyRecord = { ...policyBase, contentHash: canonicalRecordHash(policyBase) } as ProcessingPolicyRecord;
    const episodePayload = recordPayload(episode) as Record<string, unknown>;
    const policyPayload = recordPayload(policyRecord) as Record<string, unknown>;
    const schema: Record<string, unknown> = {};
    for (const [field, dataType] of REQUIRED_INDEXES) schema[field] = { data_type: dataType };
    const collection = { result: { status: "green", points_count: 2, config: { params: { vectors: { semantic: { size: 1024, distance: "Dot" } } } }, payload_schema: schema }, status: "ok" };
    const episodePoint = { id: episodeId, payload: { ...episodePayload, privacy_epoch: 4 } };
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input);
      const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: Array<{ id?: string; payload?: Record<string, unknown> }> };
      if (url.endsWith("/healthz")) return new Response(JSON.stringify({ result: { status: "ok" }, status: "ok" }), { status: 200 });
      if (url.includes("/collections/pi_memory") && init.method === "GET" && !url.includes("/points")) return new Response(JSON.stringify(collection), { status: 200 });
      if (new URL(url).pathname.endsWith("/points") && init.method === "POST") {
        const id = body?.ids?.[0];
        const result = id === COLLECTION_CONTROL_ID ? [{ id, payload: controlPayload(current) }] : id === COLLECTION_METADATA_ID ? [{ id, payload: metadata }] : id === physicalPointId("episode", episodeId) ? [{ id, payload: episodePayload, vector: { semantic: Array.from({ length: 1024 }, () => 0) } }] : id === physicalPointId("processing_policy", policy.id) ? [{ id, payload: policyPayload }] : dynamicPoints.has(id ?? "") ? [dynamicPoints.get(id!)!] : [];
        return new Response(JSON.stringify({ result, status: "ok" }), { status: 200 });
      }
      if (url.includes("/points/scroll")) {
        const request = body as { filter?: { must?: Array<{ key?: string; match?: { value?: unknown } }> } } | undefined;
        const typeCondition = request?.filter?.must?.find(entry => entry.key === "record_type")?.match;
        const includesEpisode = typeCondition?.value === "episode" || typeCondition?.any?.includes("episode") === true;
        return new Response(JSON.stringify({ result: { points: includesEpisode ? [{ ...episodePoint, payload: { ...episodePoint.payload, privacy_epoch: current.privacyEpoch } }] : [], next_page_offset: null }, status: "ok" }), { status: 200 });
      }
      if (url.includes("/points/count")) return new Response(JSON.stringify({ result: { count: 0 }, status: "ok" }), { status: 200 });
      if (url.includes("/points?") && init.method === "PUT") {
        const point = body?.points?.[0];
        if (point?.id === COLLECTION_CONTROL_ID && point.payload !== undefined) current = controlRecordFromPayload(point.payload, "pi");
        else if (point?.id !== undefined && point.payload !== undefined) dynamicPoints.set(point.id, { id: point.id, payload: point.payload });
        return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { status: 200 });
      }
      return new Response(JSON.stringify({ result: {}, status: "ok" }), { status: 200 });
    };
    vi.stubGlobal("fetch", fetchImpl);
    try {
      const env = { PI_QDRANT_MEMORY_HOST: "pi", PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY: "admin-key" };
      const deps = defaultCliDependencies({ env, homeDir, writeStdout: () => undefined, writeStderr: () => undefined });
      let output = "";
      const outputDeps = defaultCliDependencies({ env, homeDir, writeStdout: value => { output += value; }, writeStderr: () => undefined });
      const loadedConfig = await deps.loadConfig("pi");
      const queued = await deps.operate!(loadedConfig, { command: "reconcile", action: "enqueue" });
      expect(queued).toMatchObject({ ok: true, command: "reconcile", queued: true });
      const queuedJob = [...dynamicPoints.values()].find(point => point.payload.record_type === "job");
      expect(queuedJob?.payload).toMatchObject({ extractor_revision: "curation-v1", processing_policy_id: policy.id, expires_at: null, created_at: episode.createdAt });
      const sameQueued = await deps.operate!(loadedConfig, { command: "reconcile", action: "enqueue" });
      expect(sameQueued).toMatchObject({ jobId: (queued as { jobId: string }).jobId, membershipCount: 1 });
      expect([...dynamicPoints.values()].filter(point => point.payload.record_type === "job")).toHaveLength(1);
      const raptorQueued = await deps.operate!(loadedConfig, { command: "raptor", action: "enqueue" });
      expect(raptorQueued).toMatchObject({ ok: true, command: "raptor", queued: true, membershipCount: 1 });
      const raptorJob = [...dynamicPoints.values()].find(point => point.payload.extractor_revision === "admin-raptor-v1");
      expect(raptorJob?.payload).toMatchObject({ processing_policy_id: policy.id, expires_at: null, created_at: episode.eventAt });
      expect([...dynamicPoints.values()].filter(point => point.payload.record_type === "job")).toHaveLength(2);
      const queuedJobId = (queued as { jobId: string }).jobId;
      const leaseBase: LeaseRecord = { ownerHost: "pi", schemaRevision: 1, createdAt: episode.createdAt, privacyEpoch: 3, processingPolicyId: policy.id, expiresAt: "2099-01-01T00:00:00.000Z", recordType: "lease", id: leasePointId(queuedJobId), jobId: queuedJobId, ownerId: "worker-test", version: 1, fencingToken: 1, coordinationPolicyHash: "coordination-policy-hash", coordinationPolicyEpoch: 0, state: "completed", acceptedProposalId: "proposal-test", acceptedManifestHash: manifestHash([episodeId]), contentHash: "pending" };
      let terminalLease = { ...leaseBase, contentHash: canonicalRecordHash(leaseBase) } as LeaseRecord;
      dynamicPoints.set(terminalLease.id, { id: terminalLease.id, payload: recordPayload(terminalLease) as Record<string, unknown> });
      await expect(deps.operate!(loadedConfig, { command: "reconcile", action: "wait", jobId: queuedJobId })).resolves.toMatchObject({ state: "completed", jobId: queuedJobId });
      terminalLease = { ...terminalLease, state: "released", contentHash: "pending" };
      terminalLease = { ...terminalLease, contentHash: canonicalRecordHash(terminalLease) } as LeaseRecord;
      dynamicPoints.set(terminalLease.id, { id: terminalLease.id, payload: recordPayload(terminalLease) as Record<string, unknown> });
      await expect(deps.operate!(loadedConfig, { command: "reconcile", action: "wait", jobId: queuedJobId })).rejects.toThrow(/released|terminal/i);
      await expect(main(["privacy", "revoke", "--plan", "--destination", "qdrant:pi", "--json"], outputDeps)).resolves.toBe(0);
      const planned = JSON.parse(output) as { id: string; fromPrivacyEpoch: number };
      expect(planned.fromPrivacyEpoch).toBe(3);
      output = "";
      const applyExit = await main(["privacy", "revoke", "--approve", planned.id, "--json"], outputDeps);
      expect(applyExit, output).toBe(0);
      expect(current.privacyEpoch).toBe(4); expect(current.revokedDestinationIds).toContain("qdrant:pi");
      const status = await deps.status(loadedConfig);
      expect(status.qdrant.probed).toBe(true); expect(status.privacy.epoch).toBe(4); expect(status.destination.ownerHost).toBe("pi");
      const inspected = await deps.inspect!(loadedConfig, { limit: 1 });
      expect(inspected.count).toBe(1); expect(inspected.records[0]).toMatchObject({ id: episodeId, recordType: "episode" }); expect(inspected.records[0]).not.toHaveProperty("payload");
      const occurrence = `occurrence:${"b".repeat(64)}`;
      output = "";
      await expect(main(["forget", "--observation", occurrence, "--json"], outputDeps)).resolves.toBe(0);
      const forgetPlan = JSON.parse(output) as { id: string };
      output = "";
      const forgetExit = await main(["forget", "--approve", forgetPlan.id, "--json"], outputDeps);
      expect(forgetExit, output).toBe(0);
      expect(current.privacyEpoch).toBe(5);
    } finally { vi.unstubAllGlobals(); }
  });

  it("converges repeated logical plans without overwriting immutable content", async () => {
    const root = mkdtempSync(`${tmpdir()}/pi-qdrant-plans-`);
    const runtime = { ...config(), configPath: `${root}/config.json` };
    const store = createStoredPlan(runtime);
    const first = planPrivacyRevoke({ ownerHost: "pi", currentPrivacyEpoch: 7, requestedAt: "2026-08-14T00:00:00.000Z" });
    const repeated = planPrivacyRevoke({ ownerHost: "pi", currentPrivacyEpoch: 7, requestedAt: "2026-08-15T00:00:00.000Z" });
    expect(repeated.id).toBe(first.id);
    await store.save("privacy", first);
    await expect(store.save("privacy", repeated)).resolves.toBeUndefined();
    await expect(store.save("privacy", { ...first, reason: "different" })).rejects.toThrow(/collision|mismatch/i);
    await expect(store.load("privacy", first.id)).resolves.toMatchObject({ id: first.id, requestedAt: first.requestedAt });
  });

  it("exposes only the v2 command/help surface without loading config", async () => {
    let output = "";
    const deps = cli({ writeStdout: value => { output += value; } });
    await expect(main(["--help", "--json"], deps)).resolves.toBe(0);
    expect(output).toContain("project"); expect(output).toContain("forget"); expect(output).not.toContain("import");
    output = ""; await expect(main(["forget", "--help"], deps)).resolves.toBe(0); expect(output).toContain("--episode <id>"); expect(output).not.toContain("[selector]");
  });
});
