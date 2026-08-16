import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, type BoundEmbeddingDestination } from "../../src/clients/embeddings.js";
import { runRaptorFromLifecycle, type RootRaptorLifecycleInput } from "../../src/coordination/root.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord } from "../../src/domain/records.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { episodeId } from "../../src/domain/ids.js";
import { COLLECTION_CONTROL_ID, controlPayload } from "../../src/qdrant/schema.js";
import { createQdrantSafeBundle, recordPayload, type ProductionCoordinationStore } from "../../src/qdrant/write.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import type { AuthorizedDestination } from "../../src/types.js";

const NOW = "2026-08-10T00:00:00.000Z";
const NOW_MS = Date.parse(NOW);
const OWNER = "pi" as const;
const COORDINATION_HASH = "raptor-lifecycle-coordination-v1";
const qdrantDestination: AuthorizedDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination: AuthorizedDestination = { id: "embed:local", residency: "local", dataUse: "memory" };
const llmDestination: AuthorizedDestination = { id: "llm:local", residency: "local", dataUse: "memory" };

interface WirePoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }
interface WireCondition { key?: string; match?: { value?: unknown }; is_null?: { key: string }; range?: { gt?: string; lte?: string } }
interface WireFilter { must?: WireCondition[] }
function json(value: unknown): Response { return new Response(JSON.stringify(value), { headers: { "content-type": "application/json" } }); }
function clonePoint(point: WirePoint): WirePoint { return { id: point.id, payload: { ...point.payload }, ...(point.vector?.semantic === undefined ? {} : { vector: { semantic: [...point.vector.semantic] } }) }; }
function filterMatches(point: WirePoint, filter: WireFilter | undefined): boolean {
  return (filter?.must ?? []).every((condition) => {
    if (condition.match !== undefined && condition.key !== undefined) return point.payload[condition.key] === condition.match.value;
    if (condition.is_null !== undefined) return point.payload[condition.is_null.key] === null || point.payload[condition.is_null.key] === undefined;
    if (condition.range !== undefined && condition.key !== undefined) {
      const value = point.payload[condition.key]; if (typeof value !== "string") return false;
      return (condition.range.gt === undefined || value > condition.range.gt) && (condition.range.lte === undefined || value <= condition.range.lte);
    }
    return false;
  });
}
function control(overrides: Partial<ControlRecord> = {}): ControlRecord {
  const base: ControlRecord = { ownerHost: OWNER, schemaRevision: 1, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "control-policy", expiresAt: null, recordType: "collection_control", id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 1, coordinationPolicyHash: COORDINATION_HASH, state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" };
  const pending = { ...base, ...overrides, contentHash: "pending" } as ControlRecord;
  return { ...pending, contentHash: canonicalRecordHash(pending) } as ControlRecord;
}
function backend(): { points: Map<string, WirePoint>; options: QdrantClientOptions; mutateControl(change: (value: ControlRecord) => Partial<ControlRecord>): void; onEmbedding: (() => void) | undefined } {
  const points = new Map<string, WirePoint>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]]);
  const state = {
    points,
    options: { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: OWNER, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" } as QdrantClientOptions,
    onEmbedding: undefined as (() => void) | undefined,
    mutateControl(change: (value: ControlRecord) => Partial<ControlRecord>): void {
      const raw = points.get(COLLECTION_CONTROL_ID)?.payload; if (raw === undefined) throw new Error("missing control");
      const current: ControlRecord = { ownerHost: OWNER, schemaRevision: 1, createdAt: raw.created_at as string, privacyEpoch: raw.privacy_epoch as number, processingPolicyId: raw.processing_policy_id as string, expiresAt: raw.expires_at as null, recordType: "collection_control", id: COLLECTION_CONTROL_ID, version: raw.version as number, activeGeneration: raw.active_generation as string | null, activeBaseGeneration: raw.active_base_generation as string | null, coordinationPolicyEpoch: raw.coordination_policy_epoch as number, coordinationPolicyHash: raw.coordination_policy_hash as string, state: raw.state as ControlRecord["state"], scanCursor: raw.scan_cursor as string | null, lastForgetBarrier: raw.last_forget_barrier as string | null, revokedDestinationIds: [...raw.revoked_destination_ids as string[]], contentHash: raw.content_hash as string };
      const next = control({ ...current, ...change(current) }); points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(next) });
    },
  };
  vi.stubGlobal("fetch", async (input, init = {}) => {
    const url = String(input);
    if (url.includes("/embeddings")) { const hook = state.onEmbedding; state.onEmbedding = undefined; hook?.(); return json({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] }); }
    const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: WirePoint[]; update_mode?: "insert_only" | "update_only"; update_filter?: WireFilter };
    if (new URL(url).pathname.endsWith("/points") && init.method === "POST") return json({ result: (body?.ids ?? []).map((id) => points.get(id)).filter((point): point is WirePoint => point !== undefined).map(clonePoint), status: "ok" });
    if (url.includes("/points?") && init.method === "PUT") {
      for (const incoming of body?.points ?? []) {
        const prior = points.get(incoming.id);
        if (body?.update_mode === "insert_only") { if (prior === undefined) points.set(incoming.id, clonePoint(incoming)); continue; }
        if (body?.update_mode === "update_only" && prior !== undefined && filterMatches(prior, body.update_filter)) points.set(incoming.id, clonePoint(incoming));
      }
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  });
  return state;
}
function policy(revision = "raptor-lifecycle-policy-v1"): ProcessingPolicy {
  const pending: ProcessingPolicy = { id: "pending", ownerHost: OWNER, destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id }, originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: revision };
  return { ...pending, id: processingPolicyHash(pending) };
}
const stateForStore = new WeakMap<ProductionCoordinationStore, ReturnType<typeof backend>>();
function seedDurableLeaves(state: ReturnType<typeof backend>, inputLeaves: RootRaptorLifecycleInput["leaves"], privacyEpoch = 0): void {
  for (const leaf of inputLeaves) {
    const pending: EpisodeRecord = { recordType: "episode", id: leaf.id, ownerHost: OWNER, schemaRevision: 1, createdAt: NOW, privacyEpoch, processingPolicyId: leaf.policy.id, expiresAt: null, contentHash: "pending", sourceEntryId: `entry-${leaf.id}`, host: OWNER, projectId: leaf.projectId, projectIdentityKind: "registered", sessionId: "session-raptor", turnId: `turn-${leaf.id}`, agentRole: "root", depth: 0, eventKind: "user", eventAt: leaf.eventAt, modelId: "capture-model", embeddingDimension: 1024, originProvider: leaf.policy.originProvider, destinationId: qdrantDestination.id, status: "active", redactionStatus: "unchanged", secretScan: "passed", text: leaf.text, vector: [...leaf.vector] };
    const record = { ...pending, contentHash: canonicalRecordHash(pending) } as EpisodeRecord;
    state.points.set(record.id, { id: record.id, payload: recordPayload(record), vector: { semantic: [...record.vector!] } });
  }
}
function runtime(state: ReturnType<typeof backend>): { store: ProductionCoordinationStore; embedding: BoundEmbeddingDestination } {
  const store = createQdrantSafeBundle({ options: state.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: COORDINATION_HASH, coordinationPolicyEpoch: 1 }).store; stateForStore.set(store, state);
  const client = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 1000 });
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client }), egressMode: "allowlist", coordinationPolicyHash: COORDINATION_HASH, coordinationPolicyEpoch: 1 });
  return { store, embedding: bindEmbeddingDestination(factory, embeddingDestination) };
}
function leaves(count: number, sourcePolicy: ProcessingPolicy, prefix: string, long = false): RootRaptorLifecycleInput["leaves"] {
  return Array.from({ length: count }, (_, index) => ({ id: episodeId(OWNER, "raptor-session", `${prefix}-${String(index).padStart(3, "0")}`), text: long ? `safe memory ${"x".repeat(340)}` : `safe memory ${index}`, vector: Array.from({ length: 1024 }, () => Math.fround(index < count / 2 ? index * 0.05 : 8 + index * 0.05)), tokens: long ? 120 : 16, projectId: "project-raptor", eventAt: NOW, policy: sourcePolicy }));
}
function options(input: { store: ProductionCoordinationStore; embedding: BoundEmbeddingDestination; leaves: RootRaptorLifecycleInput["leaves"]; sourcePolicy: ProcessingPolicy; complete: (input: { envelope: string; signal?: AbortSignal }) => Promise<string>; reuseCandidates?: RootRaptorLifecycleInput["reuseCandidates"]; leaseMs?: number; clock?: () => number }): RootRaptorLifecycleInput {
  const state = stateForStore.get(input.store); if (state === undefined) throw new Error("missing test backend"); seedDurableLeaves(state, input.leaves);
  return { host: OWNER, store: input.store, env: {}, nodeId: "raptor-node", leaseMs: input.leaseMs ?? 30_000, maxClockSkewMs: 0, extractorRevision: "raptor-extractor-v1", clock: input.clock ?? (() => NOW_MS), workerPolicy: input.sourcePolicy, leaves: input.leaves, llm: { destination: llmDestination, complete: input.complete }, embedding: input.embedding, modelId: "memory-model", homeDir: "/home/tester", seed: "s2", maxLevels: 5, summaryInputTokens: 512, umapDimensions: 2, localNeighbors: 2, gmmMaxClusters: 4, membershipThreshold: 0.1, ...(input.reuseCandidates === undefined ? {} : { reuseCandidates: input.reuseCandidates }) };
}
function raptorPoints(state: ReturnType<typeof backend>): WirePoint[] { return [...state.points.values()].filter((point) => point.payload.record_type === "raptor_summary"); }
function activeGeneration(state: ReturnType<typeof backend>): string | null { return state.points.get(COLLECTION_CONTROL_ID)?.payload.active_generation as string | null; }
afterEach(() => { vi.unstubAllGlobals(); });

describe("Task 10 nominal RAPTOR lifecycle and publication", () => {
  it("recurses without leaking hash IDs to the scanner and retries a partial fenced attempt", async () => {
    const state = backend(); const bound = runtime(state); const sourcePolicy = policy(); const prompts: string[] = []; let rejectRecursive = true;
    const complete = async ({ envelope }: { envelope: string }): Promise<string> => { prompts.push(envelope); if (rejectRecursive && envelope.includes("safe level summary")) return JSON.stringify({ summary: "0123456789abcdef".repeat(4) }); return JSON.stringify({ summary: "safe level summary" }); };
    const input = options({ ...bound, sourcePolicy, leaves: leaves(4, sourcePolicy, "recursive", true), complete });
    const first = await runRaptorFromLifecycle(SessionManager.inMemory(), input);
    expect(first).toEqual({ state: "pending", reason: "scanner" }); expect(raptorPoints(state).length).toBeGreaterThan(0); expect(activeGeneration(state)).toBeNull();
    const partialIds = new Set(raptorPoints(state).map((point) => point.id)); rejectRecursive = false; prompts.length = 0;
    const second = await runRaptorFromLifecycle(SessionManager.inMemory(), input);
    expect(second.state).toBe("completed"); if (second.state !== "completed") throw new Error("retry did not complete");
    expect(activeGeneration(state)).toBe(second.generationId); expect([...partialIds].every((id) => state.points.has(id))).toBe(true);
    const terminalLease = [...state.points.values()].find((point) => point.payload.record_type === "lease");
    expect(terminalLease?.payload).toMatchObject({ state: "completed", terminal_operation: "raptor" });
    expect(prompts.some((prompt) => prompt.includes("safe level summary"))).toBe(true);
    expect(prompts.every((prompt) => !/(?<!sha256:)[a-f0-9]{64}/u.test(prompt))).toBe(true);
  });

  it("renews a short lease while a slow model completion is in flight", async () => {
    const state = backend(); const bound = runtime(state); const sourcePolicy = policy("lease-heartbeat");
    let now = NOW_MS; let calls = 0;
    const complete = async (): Promise<string> => {
      calls += 1;
      const ticker = setInterval(() => { now += 20; }, 20);
      try { await new Promise((resolve) => setTimeout(resolve, 160)); }
      finally { clearInterval(ticker); }
      return JSON.stringify({ summary: "safe renewed summary" });
    };
    const result = await runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...bound, sourcePolicy, leaves: leaves(2, sourcePolicy, "lease-heartbeat"), complete, leaseMs: 90, clock: () => now }));
    expect(result.state).toBe("completed"); expect(calls).toBe(1);
    const terminalLease = [...state.points.values()].find((point) => point.payload.record_type === "lease");
    expect(terminalLease?.payload).toMatchObject({ state: "completed", terminal_operation: "raptor" });
    expect(Number(terminalLease?.payload.version)).toBeGreaterThan(2);
  });

  it("structurally redacts summaries, persists 1024-vector nodes, and reuses validated summaries without LLM egress", async () => {
    const state = backend(); const bound = runtime(state); const sourcePolicy = policy("reuse-policy"); let calls = 0;
    const firstInput = options({ ...bound, sourcePolicy, leaves: leaves(2, sourcePolicy, "reuse"), complete: async () => { calls += 1; return JSON.stringify({ summary: "password: swordfish" }); } });
    const first = await runRaptorFromLifecycle(SessionManager.inMemory(), firstInput); expect(first.state).toBe("completed"); if (first.state !== "completed") throw new Error("first build failed");
    const generated = first.summaries.filter((summary) => summary.vector !== undefined); expect(generated).toHaveLength(1); expect(generated[0]!.summary).toContain("[password redacted]"); expect(generated[0]!.vector).toHaveLength(1024);
    const nextPolicy = policy("reuse-policy-next");
    calls = 0; const second = await runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...bound, sourcePolicy: nextPolicy, leaves: leaves(2, nextPolicy, "reuse"), complete: async () => { calls += 1; return JSON.stringify({ summary: "should not run" }); }, reuseCandidates: first.summaries }));
    expect(second.state).toBe("completed"); if (second.state !== "completed") throw new Error("reuse build failed"); expect(second.reused).toBe(1); expect(calls).toBe(0); expect(second.generationId).not.toBe(first.generationId);
  });

  it("rejects prior-epoch durable leaves before RAPTOR model egress", async () => {
    const state = backend(); const bound = runtime(state); const sourcePolicy = policy("old-privacy"); const inputLeaves = leaves(2, sourcePolicy, "old-privacy"); let calls = 0;
    const input = options({ ...bound, sourcePolicy, leaves: inputLeaves, complete: async () => { calls += 1; return JSON.stringify({ summary: "must not run" }); } });
    for (const leaf of inputLeaves) { const point = state.points.get(leaf.id)!; point.payload = { ...point.payload, privacy_epoch: 1 }; }
    expect(await runRaptorFromLifecycle(SessionManager.inMemory(), input)).toEqual({ state: "pending", reason: "authority_changed" });
    expect(calls).toBe(0); expect(raptorPoints(state)).toHaveLength(0); expect(activeGeneration(state)).toBeNull();
  });

  it("publishes a manifest covering 1025 durable leaves without truncation", async () => {
    const state = backend(); const bound = runtime(state); const sourcePolicy = policy("large-corpus"); let calls = 0;
    const large = Array.from({ length: 1025 }, (_, index) => ({ id: episodeId(OWNER, "raptor-session", `large-${String(index).padStart(4, "0")}`), text: `safe memory ${"x".repeat(1200)}`, vector: Array.from({ length: 1024 }, () => 0), tokens: 400, projectId: "project-raptor", eventAt: NOW, policy: sourcePolicy }));
    const result = await runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...bound, sourcePolicy, leaves: large, complete: async () => { calls += 1; return JSON.stringify({ summary: "must not run" }); } }));
    expect(result.state).toBe("completed"); if (result.state !== "completed") throw new Error("large corpus build failed");
    expect(result.manifest.chunks).toHaveLength(2); expect(result.manifest.chunks.flatMap((chunk) => chunk.memberIds)).toEqual(large.map((leaf) => leaf.id).sort()); expect(calls).toBe(0);
    expect(activeGeneration(state)).toBe(result.generationId);
  }, 15_000);

  it("rejects delayed LLM and embedding revocation races before any generation becomes visible", async () => {
    const llmState = backend(); const llmBound = runtime(llmState); const sourcePolicy = policy("llm-race"); let changed = false;
    const llmResult = await runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...llmBound, sourcePolicy, leaves: leaves(2, sourcePolicy, "llm-race"), complete: async () => { if (!changed) { changed = true; llmState.mutateControl((value) => ({ version: value.version + 1, revokedDestinationIds: [llmDestination.id] })); } return JSON.stringify({ summary: "safe summary" }); } }));
    expect(llmResult.state).toBe("pending"); expect(activeGeneration(llmState)).toBeNull(); expect(raptorPoints(llmState)).toHaveLength(0);

    vi.unstubAllGlobals(); const embedState = backend(); const embedBound = runtime(embedState); const embedPolicy = policy("embedding-race");
    embedState.onEmbedding = () => embedState.mutateControl((value) => ({ version: value.version + 1, revokedDestinationIds: [embeddingDestination.id] }));
    const embedResult = await runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...embedBound, sourcePolicy: embedPolicy, leaves: leaves(2, embedPolicy, "embed-race"), complete: async () => JSON.stringify({ summary: "safe summary" }) }));
    expect(embedResult.state).toBe("pending"); expect(activeGeneration(embedState)).toBeNull(); expect(raptorPoints(embedState)).toHaveLength(0);
  });

  it("publishes exactly one winner when independent builders race the same control base", async () => {
    const state = backend(); const bound = runtime(state); const sourcePolicy = policy("publication-race"); let arrivals = 0; let release!: () => void; const gate = new Promise<void>((resolve) => { release = resolve; });
    const complete = async (): Promise<string> => { arrivals += 1; if (arrivals === 2) release(); await gate; return JSON.stringify({ summary: "safe race summary" }); };
    const [left, right] = await Promise.all([
      runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...bound, sourcePolicy, leaves: leaves(2, sourcePolicy, "race-left"), complete })),
      runRaptorFromLifecycle(SessionManager.inMemory(), options({ ...bound, sourcePolicy, leaves: leaves(2, sourcePolicy, "race-right"), complete })),
    ]);
    const completed = [left, right].filter((result) => result.state === "completed"); expect(completed).toHaveLength(1); expect(activeGeneration(state)).toBe((completed[0] as { generationId: string }).generationId);
    expect([left, right].filter((result) => result.state === "pending")).toHaveLength(1);
  });
});
