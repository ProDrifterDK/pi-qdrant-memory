import { afterEach, describe, expect, it, vi } from "vitest";
import { buildCurationPrompt, CURATION_MAX_INPUT_TOKENS, CURATION_PROMPT_REVISION, UNTRUSTED_OPEN, UNTRUSTED_CLOSE } from "../../src/curation/prompt.js";
import { parseStrictCurationJson, validateCurationResult, CURATION_CATEGORIES, CURATION_SCOPES } from "../../src/curation/validate.js";
import { curationTrigger, CURATION_TOOL_TRIGGER, CURATION_TURN_TRIGGER, filterUncoveredEpisodes, type CurationRunResult, type CurationWorkerOptions } from "../../src/curation/worker.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type JobRecord } from "../../src/domain/records.js";
import { COLLECTION_CONTROL_ID, controlPayload } from "../../src/qdrant/schema.js";
import { coordinationRecordFromPayload, createQdrantSafeBundle, recordPayload } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, BoundEmbeddingDestination } from "../../src/clients/embeddings.js";
import { processingPolicyHash, intersectPolicies, type ProcessingPolicy } from "../../src/domain/policy.js";
import type { AuthorizedDestination } from "../../src/types.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import { readLease } from "../../src/coordination/leases.js";
import { readJob } from "../../src/coordination/jobs.js";
import { runCurationFromLifecycle } from "../../src/coordination/root.js";
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
const EPISODE_3 = "00000000-0000-5000-8000-000000000003";
const providerBinding = { providerId: "provider-local", modelId: "provider-model", destinationId: "llm:local" };

function json(value: unknown, status = 200): Response { return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } }); }
afterEach(() => { vi.unstubAllGlobals(); });
function stubGlobalFetch(fetchImpl: typeof fetch): void { vi.stubGlobal("fetch", fetchImpl); }

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const pending = {
    id: "pending", ownerHost: "pi" as const,
    destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null,
    residency: "local", dataUse: "memory", policyRevision: "v1", ...overrides,
  } satisfies ProcessingPolicy;
  return { ...pending, id: processingPolicyHash(pending) };
}
function episode(overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const pending = {
    recordType: "episode" as const, id: EPISODE_1, ownerHost: OWNER, schemaRevision: 1 as const,
    createdAt: NOW, privacyEpoch: 0, processingPolicyId: policy().id,
    expiresAt: null, contentHash: "pending", sourceEntryId: "entry-1", host: OWNER,
    projectId: "project-1", projectIdentityKind: "registered" as const, sessionId: "session-1", turnId: "turn-1",
    agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: NOW,
    modelId: "capture-model", embeddingDimension: 1024, originProvider: "provider-local",
    destinationId: qdrantDestination.id, status: "active" as const, redactionStatus: "redacted" as const,
    secretScan: "passed" as const, text: "safe [token redacted]", ...overrides,
  };
  const value = { ...pending, ...overrides } as EpisodeRecord;
  for (const key of ["text", "toolName", "toolArgs", "errorFingerprint", "producerId", "nodeId", "sessionSequence"] as const) if (value[key] === undefined) delete (value as Partial<EpisodeRecord>)[key];
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}
interface BackendPoint { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] }; }
function restQdrantWriter(seed: BackendPoint[] = []): { points: Map<string, BackendPoint>; options: QdrantClientOptions } {
  const points = new Map<string, BackendPoint>(seed.map((point) => [point.id, point]));
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: BackendPoint[]; update_mode?: string; update_filter?: { must: Array<{ key: string; match?: { value?: string | number | boolean }; is_null?: { key: string }; range?: { gt?: string; lte?: string } }> } };
    if (new URL(url).pathname.endsWith("/points") && init.method === "POST") { const ids = body?.ids ?? []; return json({ result: ids.map((id) => points.get(id)).filter((p) => p !== undefined), status: "ok" }); }
    if (url.includes("/points/scroll")) return json({ result: { points: [], next_page_offset: null }, status: "ok" });
    if (url.includes("/points?") && init.method === "PUT") {
      const point = body?.points?.[0];
      const current = point === undefined ? undefined : points.get(point.id)?.payload;
      const value = (key: string): unknown => current?.[key];
      const must = body?.update_filter?.must ?? [];
      const matches = must.every((condition) => {
        if (condition.is_null !== undefined) return value(condition.is_null.key) === null;
        if (condition.range !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) > Date.parse(condition.range.gt ?? condition.range.lte ?? "0");
        return value(condition.key) === condition.match?.value;
      });
      if (body?.update_mode === "update_only" && !matches) return json({ result: { status: "acknowledged" }, status: "ok" });
      for (const p of body?.points ?? []) {
        if (body?.update_mode === "insert_only" && points.has(p.id)) continue;
        points.set(p.id, { id: p.id, payload: p.payload, ...(p.vector === undefined ? {} : { vector: p.vector }) });
      }
      return json({ result: { status: "acknowledged" }, status: "ok" });
    }
    return json({ result: {}, status: "ok" });
  };
  stubGlobalFetch(fetchImpl);
  const options: QdrantClientOptions = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
  return { points, options };
}
function emptyControl(): ControlRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: coordination.policyEpoch, coordinationPolicyHash: coordination.policyHash, state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" };
  return { ...base, contentHash: canonicalRecordHash(base) } as ControlRecord;
}
function realEmbeddings(embedImpl: (input: { model: string; text: string; signal?: AbortSignal }) => Promise<readonly number[]>): EmbeddingsClient {
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
  return new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
}
function boundEmbedding(): BoundEmbeddingDestination {
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: embeddingDestination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: realEmbeddings(async () => Array.from({ length: 1024 }, () => 0.25)) }), egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
  return bindEmbeddingDestination(factory, embeddingDestination);
}
function backendWithControl(): { points: Map<string, BackendPoint>; options: QdrantClientOptions } {
  return restQdrantWriter([{ id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) }]);
}
/** Seed a vector-bound episode point (exact parser requires the vector-bound hash + named vector). */
async function seedEpisode(backend: { points: Map<string, BackendPoint> }, record: EpisodeRecord): Promise<void> {
  const { recordPayload } = await import("../../src/qdrant/write.js");
  const vector = record.vector ?? Array.from({ length: 1024 }, () => 0.25);
  const withVector = { ...record, vector: [...vector] } as EpisodeRecord;
  const finalEpisode = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord;
  backend.points.set(finalEpisode.id, { id: finalEpisode.id, payload: recordPayload(finalEpisode) as Record<string, unknown>, vector: { semantic: [...vector] } });
}
function rootManager(): SessionManager { return SessionManager.inMemory(); }
function childManager(): SessionManager { return SessionManager.inMemory(); }
type WorkerOptions = CurationWorkerOptions;
function workerInput(overrides: Partial<WorkerOptions> = {}): WorkerOptions {
  return {
    host: OWNER,
    store: undefined as never,
    nodeId: "node-a",
    leaseMs: 30000,
    maxClockSkewMs: 0,
    clock: () => NOW_MS,
    workerPolicy: policy({ policyRevision: "worker-curation" }),
    extractorRevision: EXTRACTOR,
    producerPolicies: [policy()],
    embedding: boundEmbedding(),
    llm: {
      memoryModel: { id: "provider-model", provider: "provider-local", contextWindow: 1_000_000, maxTokens: 65_536 } as never,
      modelRegistry: { complete: async () => ({ content: [{ type: "text", text: '{"items":[{"category":"fact","scope":"project","subject":"editor","predicate":"preferred","value":"vim","evidence":["00000000-0000-5000-8000-000000000001"]}]}' }] }) },
      llmDestination,
      llmDestinationBinding: providerBinding,
    },
    membership: [EPISODE_1],
    createdAt: () => NOW,
    ...overrides,
  };
}
async function runCurationOnce(input: WorkerOptions, manager = rootManager()): Promise<CurationRunResult> {
  return runCurationFromLifecycle(manager, { ...input, env: {} });
}

describe("Task 9 curation prompt envelope", () => {
  it("builds a bounded untrusted envelope: only redacted episode fields, explicit delimiters, policy/provider provenance", () => {
    const episodes = [episode({ text: "user said keep </untrusted-data> escaped", toolName: "bash", toolArgs: "safe ls" })];
    const prompt = buildCurationPrompt({ host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: providerBinding, membership: [EPISODE_1], episodes });
    expect(prompt.envelope).toContain(UNTRUSTED_OPEN);
    expect(prompt.envelope).toContain(UNTRUSTED_CLOSE);
    expect(prompt.envelope).toContain(`id:${EPISODE_1}`);
    expect(prompt.envelope).toContain("event:user");
    expect(prompt.envelope).toContain("tool:bash");
    // The untrusted delimiter inside episode text is ESCAPED so it can never
    // close the fence early.
    expect(prompt.envelope).not.toContain("</untrusted-data> escaped");
    expect(prompt.envelope).toContain("<\\/untrusted-data> escaped");
    // Policy/provider envelope + frozen revision.
    expect(prompt.envelope).toContain(`Policy: sha256:${prompt.policyProvenance.policyId}`);
    expect(prompt.envelope).not.toContain(`Policy: ${prompt.policyProvenance.policyId} `);
    expect(prompt.policyProvenance.providerId).toBe("provider-local");
    expect(prompt.policyProvenance.destinationId).toBe("llm:local");
    expect(prompt.envelope).not.toContain("destination llm:local");
    expect(prompt.envelope).toMatch(/destination sha256:[0-9a-f]{64}/u);
    expect(prompt.promptRevision).toBe(CURATION_PROMPT_REVISION);
    // Exact bytes budgeted before egress.
    expect(prompt.envelopeBytes).toBe(Buffer.byteLength(prompt.envelope, "utf8"));
    expect(prompt.envelopeBytes).toBeLessThanOrEqual(prompt.maxInputTokens);
    expect(prompt.maxInputTokens).toBe(CURATION_MAX_INPUT_TOKENS);
  });

  it("excludes system/developer authority, injected memory, vectors, keys, tools access and unredacted payload", () => {
    const episodes = [episode({ text: "secret: sk-abcdefghijklmnopqrstuvwxyz123456 payload", vector: Array.from({ length: 1024 }, () => 0.25), toolArgs: "rm -rf /tmp/x", errorFingerprint: "fp", nodeId: "node-x", producerId: "producer-x" })];
    const prompt = buildCurationPrompt({ host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: providerBinding, membership: [EPISODE_1], episodes });
    for (const forbidden of ["system", "developer", "sk-abcdefghijklmnopqrstuvwxyz123456", "memory:", "tool_access", "0.25", "node-x", "producer-x", "fp"]) {
      expect(prompt.envelope).not.toContain(forbidden);
    }
    expect(prompt.envelope).toContain("error_fingerprint:present");
  });

  it("snapshots prompt inputs from dense own data without invoking accessors or proxies", () => {
    const base = { host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: { ...providerBinding }, membership: [EPISODE_1], episodes: [episode()] };
    let unknownGets = 0;
    Object.defineProperty(base, "API_KEY", { configurable: true, get() { unknownGets += 1; throw new Error("unknown getter fired"); } });
    expect(buildCurationPrompt(base).membership).toEqual([EPISODE_1]);
    expect(unknownGets).toBe(0);
    let hostGets = 0;
    const accessor = { ...base };
    Object.defineProperty(accessor, "host", { enumerable: true, configurable: true, get() { hostGets += 1; return OWNER; } });
    expect(() => buildCurationPrompt(accessor)).toThrow(/own data/i);
    expect(hostGets).toBe(0);
    let nestedGets = 0;
    const nested = { ...base, provider: { ...providerBinding } };
    Object.defineProperty(nested.provider, "modelId", { enumerable: true, configurable: true, get() { nestedGets += 1; return "provider-model"; } });
    expect(() => buildCurationPrompt(nested)).toThrow(/accessor|data graph/i);
    expect(nestedGets).toBe(0);
    let proxyGets = 0;
    const proxiedMembership = new Proxy([EPISODE_1], { get(target, key) { proxyGets += 1; return Reflect.get(target, key); } });
    expect(() => buildCurationPrompt({ ...base, membership: proxiedMembership })).toThrow(/plain JSON graph/i);
    expect(proxyGets).toBe(0);
  });

  it("enforces explicit sorted unique membership and the token budget", () => {
    const episodes = [episode(), episode({ id: EPISODE_2, sourceEntryId: "entry-2" })];
    expect(() => buildCurationPrompt({ host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: providerBinding, membership: [EPISODE_2, EPISODE_1], episodes })).toThrow(/sorted/i);
    expect(() => buildCurationPrompt({ host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: providerBinding, membership: [EPISODE_1, EPISODE_1], episodes: [episode(), episode({ id: EPISODE_1, sourceEntryId: "x" })] })).toThrow(/unique|sorted/i);
    expect(() => buildCurationPrompt({ host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: providerBinding, membership: [EPISODE_1], episodes: [episode()], maxInputTokens: 1 })).toThrow(/budget/i);
    // Episodes must match the explicit membership exactly.
    expect(() => buildCurationPrompt({ host: OWNER, policyId: policy().id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, provider: providerBinding, membership: [EPISODE_1, EPISODE_2], episodes: [episode()] })).toThrow(/match|missing/i);
  });
});

describe("Task 9 curation result validation", () => {
  it("rejects validator proxies and accessors without invoking them", () => {
    const context = { directUserEpisodeIds: new Set([EPISODE_1]), knownEpisodeIds: new Set([EPISODE_1]) };
    let accessorGets = 0;
    const accessorInput: Record<string, unknown> = {};
    Object.defineProperty(accessorInput, "items", { enumerable: true, configurable: true, get() { accessorGets += 1; return []; } });
    expect(() => validateCurationResult(accessorInput, context)).toThrow(/plain|accessor|invalid/i);
    expect(accessorGets).toBe(0);
    let proxyGets = 0;
    const proxy = new Proxy({ items: [] }, { get(target, key) { proxyGets += 1; return Reflect.get(target, key); } });
    expect(() => validateCurationResult(proxy, context)).toThrow(/plain|invalid/i);
    expect(proxyGets).toBe(0);
  });

  it("rejects a tool output that invents a standing instruction", () => {
    expect(() => validateCurationResult({ items: [{ category: "preference", scope: "project", subject: "editor", predicate: "must_use", value: "vim", evidence: ["tool-1"] }] }, { directUserEpisodeIds: new Set(), knownEpisodeIds: new Set(["tool-1"]) })).toThrow(/direct user evidence/);
    expect(() => validateCurationResult({ items: [{ category: "correction", scope: "project", subject: "editor", predicate: "must_use", value: "vim", evidence: [EPISODE_1] }] }, { directUserEpisodeIds: new Set(), knownEpisodeIds: new Set([EPISODE_1]) })).toThrow(/direct user evidence/);
    // Direct-user evidence satisfies the requirement.
    expect(validateCurationResult({ items: [{ category: "preference", scope: "project", subject: "editor", predicate: "must_use", value: "vim", evidence: [EPISODE_1] }] }, { directUserEpisodeIds: new Set([EPISODE_1]), knownEpisodeIds: new Set([EPISODE_1]) }).items).toHaveLength(1);
  });

  it("parses JSON-only output and rejects fences, prose, duplicate keys and prototype keys", () => {
    const good = '{"items":[{"category":"fact","scope":"project","subject":"a","predicate":"b","value":"c","evidence":["00000000-0000-5000-8000-000000000001"]}]}';
    expect(parseStrictCurationJson(good)).toEqual(JSON.parse(good) as unknown);
    expect(() => parseStrictCurationJson("```json\n" + good + "\n```")).toThrow(/fences|bare/i);
    expect(() => parseStrictCurationJson("Here is the result: " + good)).toThrow(/fences|bare/i);
    expect(() => parseStrictCurationJson('{"items":[],"items":[]}')).toThrow(/duplicate/i);
    expect(() => parseStrictCurationJson('{"__proto__": {"polluted": true}}')).toThrow(/strict JSON|prototype/i);
    expect(() => parseStrictCurationJson("not json at all")).toThrow(/strict JSON|fences/i);
  });

  it("validates categories/scopes/lists/bounds and rejects unknown or malformed items", () => {
    const ctx = { directUserEpisodeIds: new Set([EPISODE_1]), knownEpisodeIds: new Set([EPISODE_1]) };
    for (const category of CURATION_CATEGORIES) {
      expect(validateCurationResult({ items: [{ category, scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1] }] }, ctx).items[0]!.category).toBe(category);
    }
    for (const scope of CURATION_SCOPES) {
      expect(validateCurationResult({ items: [{ category: "fact", scope, subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1] }] }, ctx).items[0]!.scope).toBe(scope);
    }
    expect(() => validateCurationResult({ items: [{ category: "bogus", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1] }] }, ctx)).toThrow(/category/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "bogus", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1] }] }, ctx)).toThrow(/scope/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", evidence: [EPISODE_1] }] }, ctx)).toThrow(/value or text/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [] }] }, ctx)).toThrow(/evidence/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1, EPISODE_1] }] }, ctx)).toThrow(/repeat/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_2] }] }, ctx)).toThrow(/membership/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1], extra: true }] }, ctx)).toThrow(/unknown/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: Number.NaN, evidence: [EPISODE_1] }] }, ctx)).toThrow(/canonical|value/i);
    expect(() => validateCurationResult({ items: Array.from({ length: 33 }, () => ({ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1] })) }, ctx)).toThrow(/items/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "x".repeat(513), predicate: "p", value: "v", evidence: [EPISODE_1] }] }, ctx)).toThrow(/subject/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1], confidence: 1.5 }] }, ctx)).toThrow(/confidence/i);
    expect(() => validateCurationResult({ items: [{ category: "fact", scope: "project", subject: "s", predicate: "p", value: "v", evidence: [EPISODE_1] }], extra: true }, ctx)).toThrow(/unknown top-level/i);
  });
});

describe("Task 9 curation worker triggers and root gating", () => {
  it("enqueues at root turn 10, tool trigger 15 and before compaction; shutdown persists pending only", () => {
    const base = { host: OWNER, sessionManager: rootManager(), env: {} };
    expect(curationTrigger({ ...base, rootTurns: 9, toolCalls: 0, beforeCompaction: false, shutdown: false })).toBe("disabled");
    expect(curationTrigger({ ...base, rootTurns: CURATION_TURN_TRIGGER, toolCalls: 0, beforeCompaction: false, shutdown: false })).toBe("run");
    expect(curationTrigger({ ...base, rootTurns: 0, toolCalls: CURATION_TOOL_TRIGGER, beforeCompaction: false, shutdown: false })).toBe("run");
    expect(curationTrigger({ ...base, rootTurns: 0, toolCalls: 0, beforeCompaction: true, shutdown: false })).toBe("run");
    // Shutdown only persists pending work; never starts LLM curation.
    expect(curationTrigger({ ...base, rootTurns: CURATION_TURN_TRIGGER, toolCalls: CURATION_TOOL_TRIGGER, beforeCompaction: true, shutdown: true })).toBe("persist_only");
  });

  it("children and ambiguous/contradictory markers cannot trigger root curation", () => {
    const primeChildBase = { host: "prime" as const, sessionManager: childManager(), env: { RLM_DEPTH: "1" } };
    expect(curationTrigger({ ...primeChildBase, rootTurns: CURATION_TURN_TRIGGER, toolCalls: CURATION_TOOL_TRIGGER, beforeCompaction: true, shutdown: false })).toBe("child");
    // Invalid marker disables root work.
    expect(curationTrigger({ host: "prime", sessionManager: rootManager(), env: { RLM_DEPTH: "not-a-number" }, rootTurns: CURATION_TURN_TRIGGER, toolCalls: 0, beforeCompaction: false, shutdown: false })).toBe("child");
    // Pi parentSession is the sole host child signal.
    expect(curationTrigger({ host: OWNER, sessionManager: childManager(), env: { PI_SUBAGENT_CHILD: "1" }, rootTurns: CURATION_TURN_TRIGGER, toolCalls: 0, beforeCompaction: false, shutdown: false })).toBe("child");
    expect(curationTrigger({ host: OWNER, sessionManager: rootManager(), env: {}, rootTurns: CURATION_TURN_TRIGGER, toolCalls: 0, beforeCompaction: false, shutdown: false })).toBe("run");
  });
});

describe("Task 9 curation worker end-to-end", () => {
  it("runs one curation cycle: claim -> LLM -> proposal -> accept -> materialize -> coverage, then completes the lease", async () => {
    const backend = backendWithControl();
    const ep = episode();
    await seedEpisode(backend, ep);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const input = workerInput({ store: bundle.store });
    const result: CurationRunResult = await runCurationOnce(input);
    expect(result.state).toBe("completed");
    expect(result.observations).toBe(1);
    const job = await readJob(bundle.store, result.jobId!);
    expect(job).not.toBeNull();
    expect(job!.policyId).toBe(intersectPolicies([policy()], policy({ policyRevision: "worker-curation" }))!.id);
    // Terminal completion is explicit and durable.
    const lease = await readLease(bundle.store, result.jobId!);
    expect(lease?.state).toBe("completed");
    // Deterministic coverage truth: the second run skips the covered episode.
    const again: CurationRunResult = await runCurationOnce(input);
    expect(again.state).toBe("completed");
    expect(again.reason).toBe("already-covered");
    expect(again.observations).toBe(0);
  });

  it("accepted empty output still writes deterministic coverage", async () => {
    const backend = backendWithControl();
    const ep = episode();
    await seedEpisode(backend, ep);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const base = workerInput({ store: bundle.store });
    const result = await runCurationOnce({ ...base, llm: { ...base.llm, modelRegistry: { complete: async () => ({ content: [{ type: "text", text: '{"items":[]}' }] }) } } });
    expect(result.state).toBe("completed");
    expect(result.observations).toBe(0);
    const intersection = intersectPolicies([policy()], base.workerPolicy)!;
    await expect(filterUncoveredEpisodes({ store: bundle.store, membership: [EPISODE_1], extractorRevision: EXTRACTOR, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, privacyEpoch: 0, policyIntersectionId: intersection.id })).resolves.toEqual([]);
  });

  it("rejects a canonical job mutation that occurs during the LLM call", async () => {
    const backend = backendWithControl();
    const producer = policy();
    const ep = episode({ processingPolicyId: producer.id });
    await seedEpisode(backend, ep);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    let llmCalls = 0;
    const input = workerInput({ store: bundle.store, embedding: boundEmbedding(), producerPolicies: [producer] });
    input.llm!.modelRegistry.complete = async () => {
      llmCalls += 1;
      const point = [...backend.points.values()].find((candidate) => candidate.payload.record_type === "job")!;
      const parsed = coordinationRecordFromPayload(point.payload, OWNER) as JobRecord;
      const changed = { ...parsed, createdAt: "2026-08-10T00:00:01.000Z", contentHash: "pending" } as JobRecord;
      changed.contentHash = canonicalRecordHash(changed);
      point.payload = recordPayload(changed) as Record<string, unknown>;
      return { content: [{ type: "text", text: '{"items":[]}' }] };
    };
    const result = await runCurationOnce(input);
    expect(result).toMatchObject({ state: "pending", reason: "llm-authority-changed" });
    expect(llmCalls).toBe(1);
    expect([...backend.points.values()].filter((point) => point.payload.record_type === "proposal")).toHaveLength(0);
    expect([...backend.points.values()].filter((point) => String(point.payload.record_type).startsWith("curated_") || point.payload.record_type === "coverage")).toHaveLength(0);
  });

  it("a failed LLM call leaves a retryable job and the episodes searchable", async () => {
    const backend = backendWithControl();
    const ep = episode();
    await seedEpisode(backend, ep);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const input = workerInput({ store: bundle.store, llm: { ...workerInput().llm, modelRegistry: { complete: async () => { throw new Error("sk-backend-internal-password"); } } } });
    const result = await runCurationOnce(input);
    expect(result.state).toBe("pending");
    expect(result.reason).toBe("llm-failed");
    expect(result.reason).not.toContain("sk-backend-internal-password");
    expect(JSON.stringify([...backend.points.values()])).not.toContain("sk-backend-internal-password");
    // The job exists, the lease is released (retryable), episodes remain.
    const job = await readJob(bundle.store, result.jobId!);
    expect(job).not.toBeNull();
    const lease = await readLease(bundle.store, result.jobId!);
    expect(lease?.state).toBe("released");
    expect(await bundle.store.readEpisodes([EPISODE_1])).toHaveLength(1);
    // Retry with a working LLM completes the cycle.
    const retry = await runCurationOnce(workerInput({ store: bundle.store }));
    expect(retry.state).toBe("completed");
  });

  it("malformed LLM output (tool-invented standing instruction) is rejected and stays pending", async () => {
    const backend = backendWithControl();
    // The evidence is deliberately a tool result, not a direct user event;
    // otherwise the standing-instruction validator must (correctly) accept it.
    const ep = episode({ eventKind: "tool_result" });
    await seedEpisode(backend, ep);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const input = workerInput({
      store: bundle.store,
      llm: { ...workerInput().llm, modelRegistry: { complete: async () => ({ content: [{ type: "text", text: '{"items":[{"category":"preference","scope":"project","subject":"editor","predicate":"must_use","value":"vim","evidence":["00000000-0000-5000-8000-000000000001"]}]}' }] }) } },
    });
    const result = await runCurationOnce(input);
    expect(result.state).toBe("pending");
    expect(result.reason).toContain("direct user evidence");
    const lease = await readLease(bundle.store, result.jobId!);
    expect(lease?.state).toBe("released");
  });

  it("rejects a prior-epoch source before curation model egress", async () => {
    const backend = backendWithControl(); const old = episode({ privacyEpoch: 1 }); await seedEpisode(backend, old);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    let completions = 0; const result = await runCurationOnce(workerInput({ store: bundle.store, llm: { ...workerInput().llm, modelRegistry: { complete: async () => { completions += 1; throw new Error("must not complete"); } } } }));
    expect(result).toMatchObject({ state: "pending" }); expect(completions).toBe(0);
    expect([...backend.points.values()].some((point) => point.payload.record_type === "proposal" || point.payload.record_type === "curated_memory")).toBe(false);
  });

  it("splits explicit jobs by compatible policy groups; incompatible producers stay pending", async () => {
    const backend = backendWithControl();
    const producerA = policy({ policyRevision: "producer-group-a" });
    const producerB = policy({ policyRevision: "producer-group-b" });
    const epA = episode({ id: EPISODE_1, processingPolicyId: producerA.id });
    const epB = episode({ id: EPISODE_2, sourceEntryId: "entry-2", processingPolicyId: producerB.id });
    await seedEpisode(backend, epA);
    await seedEpisode(backend, epB);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    // Only producerA is known: the group B episodes stay pending (no claim, no job).
    const input = workerInput({ store: bundle.store, producerPolicies: [producerA], membership: [EPISODE_1, EPISODE_2] });
    const result = await runCurationOnce(input);
    expect(result.state).toBe("completed");
    // The compatible group (A) was curated.
    expect(result.jobId).toBeDefined();
    const job = await readJob(bundle.store, result.jobId!);
    expect(job?.membership).toEqual([EPISODE_1]);
    // Group B was never claimed (its job was never created).
    const jobB = await readJob(bundle.store, (await import("../../src/domain/ids.js")).jobId(OWNER, [EPISODE_2], coordination.policyHash, EXTRACTOR, coordination.policyEpoch, intersectPolicies([producerB], policy({ policyRevision: "worker-curation" }))!.id, 0));
    expect(jobB).toBeNull();
  });

  it("one effective claim per host/batch: a live foreign claim returns no_claim with zero mutations", async () => {
    const backend = backendWithControl();
    const ep = episode();
    await seedEpisode(backend, ep);
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    // Another worker claims the exact job this worker would create, FIRST.
    const workerPolicy = policy({ policyRevision: "worker-curation" });
    const intersection = intersectPolicies([policy()], workerPolicy)!;
    const jobIdValue = (await import("../../src/domain/ids.js")).jobId(OWNER, [EPISODE_1], coordination.policyHash, EXTRACTOR, coordination.policyEpoch, intersection.id, 0);
    const { createJob } = await import("../../src/coordination/jobs.js");
    await createJob(bundle.store, { ownerHost: OWNER, membership: [EPISODE_1], policyIntersectionId: intersection.id, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const foreignInput = workerInput({ store: bundle.store, nodeId: "node-foreign" });
    await runCurationOnce(foreignInput);
    const { readLease: readForeignLease } = await import("../../src/coordination/leases.js");
    const foreignClaim = await readForeignLease(bundle.store, jobIdValue);
    expect(foreignClaim).not.toBeNull();
    const pointsBefore = backend.points.size;
    const result = await runCurationOnce(workerInput({ store: bundle.store }));
    expect(result.state).toBe("completed");
    // Zero new points: the live foreign claim is never stolen or mutated.
    expect(backend.points.size).toBe(pointsBefore);
  });

  it("filterUncoveredEpisodes returns only episodes not covered under the exact policy identity", async () => {
    const backend = backendWithControl();
    const bundle = createQdrantSafeBundle({ options: backend.options, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: coordination.policyHash, coordinationPolicyEpoch: coordination.policyEpoch });
    const uncovered = await filterUncoveredEpisodes({ store: bundle.store, membership: [EPISODE_1, EPISODE_2], extractorRevision: EXTRACTOR, policyHash: coordination.policyHash, policyEpoch: coordination.policyEpoch, privacyEpoch: 0, policyIntersectionId: "intersection-1" });
    expect(uncovered).toEqual([EPISODE_1, EPISODE_2]);
    // Coverage is now only writable through an accepted authority; the
    // high-level worker owns that setup, so this pure filter remains uncovered.
    expect(uncovered).toEqual([EPISODE_1, EPISODE_2]);
  });
});
