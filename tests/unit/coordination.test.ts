import { afterEach, describe, expect, it, vi } from "vitest";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../../src/domain/canonical.js";
import { contentId, coverageId, curatedCurrentId, episodeId, jobId, leasePointId, manifestHash, observationId, proposalContentHash, proposalIdFor, stateKey, tombstoneId } from "../../src/domain/ids.js";
import { canonicalRecordHash, type ControlRecord, type CoverageRecord, type EpisodeRecord, type JobRecord, type LeaseRecord, type ProposalRecord, type TombstoneRecord } from "../../src/domain/records.js";
import { COLLECTION_CONTROL_ID, bootstrapControlHash, controlPayload } from "../../src/qdrant/schema.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import { bindQdrantDestination, createQdrantDestinationFactory, createQdrantSafeBundle, recordPayload } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, BoundEmbeddingDestination, createEmbeddingDestinationFactory, EmbeddingsClient, ValidatedEmbeddingDocumentClient } from "../../src/clients/embeddings.js";
import { bindIngestRuntime } from "../../src/coordination/ingest.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { ingestPendingJobs, type IngestControlReader, type IngestTombstoneReader } from "../../src/outbox/delivery.js";
import {
  activatePolicyEpoch, beginForgetBarrier, beginPolicyDrain, createIngestControlReader,
  createQdrantCoordinationStore, initializeControl, ProductionCoordinationStore, QuiescenceProof, readControl, readForUpdate, rotateCoordinationPolicy,
  waitForOldLeasesToQuiesce,
} from "../../src/coordination/control.js";
import { LeaseAuthority, acceptLeaseAuthority, claimLease, isLeaseExpired, readLease, releaseLease, renewLease } from "../../src/coordination/leases.js";
import { acceptProposal, createJob, proposalHashFor, readActiveAcceptance, readJob, writeProposal, type CreateJobInput } from "../../src/coordination/jobs.js";
import { QdrantContentHashCollisionError, QdrantLegacyEpisodeHashError } from "../../src/domain/qdrant-errors.js";
const { RootWorkerContext, mintRootWorker } = vi.hoisted(() => {
  // Tests-only root harness: lives in the test file, never emitted in dist.
  const TEST_ROOT_BRAND = Symbol("pi-qdrant-memory-v2.test-root-worker");
  // Deterministic default trusted clock: NOW_MS (same fixed instant the tests
  // use for leases/claims). Individual tests inject a mutable clock to advance
  // time across slow reads (fresh sampling) or to fail the clock.
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
import { findMissingEpisodes, markCoverage, reconcileCoverage } from "../../src/coordination/reconcile.js";
import { BoundIngestTombstoneReader, createIngestTombstoneReader, createTombstone, isVisibleAfterTombstoneCheck, readTombstones, tombstoneTargets } from "../../src/coordination/tombstones.js";
import type { MemoryRecord } from "../../src/domain/records.js";

const NOW = "2026-08-10T00:00:00.000Z";
const NOW_MS = Date.parse(NOW);
const OWNER = "pi" as const;
const POLICY_HASH = "coordination-policy-hash-v2";
const INTERSECTION_ID = "intersection-policy-id";
const EXTRACTOR = "extractor-1";
const EPISODE_UUID = "00000000-0000-5000-8000-000000000001";
const qdrantDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
const embeddingDestination = { id: "embed:local", residency: "local", dataUse: "memory" };

function iso(ms: number): string { return new Date(ms).toISOString(); }
/** Standard validated options for safe production construction (no raw writer ever exists). */
function qdrantOptions(ownerHost: "pi" | "prime" = "pi", baseUrl = "http://qdrant"): QdrantClientOptions {
  return { baseUrl, collection: ownerHost === "pi" ? "pi_memory" : "prime_memory", ownerHost, apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
}
/** Reconstruct a safe createJob input from a job record (SAFE seeding; no raw engine access). */
function jobInputFrom(record: JobRecord): CreateJobInput {
  return { ownerHost: record.ownerHost, membership: [...record.membership], policyIntersectionId: record.processingPolicyId, policyHash: record.coordinationPolicyHash, policyEpoch: record.coordinationPolicyEpoch, extractorRevision: record.extractorRevision, privacyEpoch: record.privacyEpoch, createdAt: record.createdAt, ...(record.expiresAt === null ? {} : { expiresAt: record.expiresAt }) };
}
/** SAFE tombstone seeding: the episode must be persisted first (createTombstone verifies it exactly). */
async function seedTombstone(store: ProductionCoordinationStore, points: Map<string, { id: string; payload: Record<string, unknown> }>, targetEpisode: EpisodeRecord): Promise<void> {
  // Episodes are vector-bound: persist the exact 1024-vector + vector-bound hash
  // so the store's exact episode parser accepts the seeding read.
  const vector = targetEpisode.vector ?? Array.from({ length: 1024 }, () => 0.25);
  const withVector = { ...targetEpisode, vector: [...vector] };
  const finalEpisode = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord;
  points.set(finalEpisode.id, { id: finalEpisode.id, payload: recordPayload(finalEpisode), vector: { semantic: [...vector] } });
  await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: finalEpisode.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID });
}
function emptyControl(): ControlRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 1, coordinationPolicyHash: POLICY_HASH, state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" };
  return { ...base, contentHash: canonicalRecordHash(base) } as ControlRecord;
}
function worker(nodeId = "node-a"): RootWorkerContext { return mintRootWorker(OWNER, nodeId); }
function primeWorker(): RootWorkerContext { return mintRootWorker("prime", "node-prime"); }
/** Worker with a deterministic mutable trusted clock for fresh-sampling tests. */
function timedWorker(ms: number, nodeId = "node-a", leaseMs = 30000, skew = 0): { worker: RootWorkerContext; set(value: number): void } {
  const box = { value: ms };
  return { worker: mintRootWorker(OWNER, nodeId, () => box.value, leaseMs, skew), set: (value: number) => { box.value = value; } };
}
/** Worker whose trusted clock always throws (fail-closed clock). */
function brokenClockWorker(): RootWorkerContext { return mintRootWorker(OWNER, "node-a", () => { throw new Error("clock offline"); }); }
/** Worker for a nominal node with a FIXED trusted clock (standalone claims). */
function workerAt(nodeId: string, now: number, leaseMs = 30000, skew = 0): RootWorkerContext { return mintRootWorker(OWNER, nodeId, () => now, leaseMs, skew); }
async function acceptedCoverageAuthority(store: ProductionCoordinationStore, membershipInput: readonly string[], createdAt = NOW): Promise<LeaseAuthority> {
  const membership = [...new Set(membershipInput)].sort();
  void createdAt;
  const job = await createJob(store, { ownerHost: OWNER, membership, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, createdAt, expiresAt: null });
  const claimed = await claimLease(store, workerAt(`coverage-${job.id}`, NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
  if (claimed === null) throw new Error("coverage fixture could not claim its job");
  const proposal = await writeProposal(store, claimed, { membership, content: { summary: "coverage fixture" }, createdAt });
  const accepted = await acceptProposal(store, claimed, { proposalId: proposal.id });
  if (accepted === null) throw new Error("coverage fixture could not accept its proposal");
  return accepted;
}
afterEach(() => { vi.unstubAllGlobals(); });
function stubGlobalFetch(fetchImpl: typeof fetch): void { vi.stubGlobal("fetch", fetchImpl); }
/** Real production store over stubbed global fetch (NO raw writer is ever constructed in tests). */
function productionRestWriter(baseUrl = "http://qdrant", ownerHost: "pi" | "prime" = "pi"): { store: ProductionCoordinationStore; points: Map<string, { id: string; payload: Record<string, unknown> }> } {
  const points = restPoints();
  stubGlobalFetch(restFetch(points));
  return { store: createQdrantCoordinationStore(qdrantOptions(ownerHost, baseUrl)), points };
}
function restPoints(): Map<string, { id: string; payload: Record<string, unknown> }> {
  return new Map([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]]);
}
function restFetch(points: Map<string, { id: string; payload: Record<string, unknown> }>, intercept?: (ids: readonly string[]) => { id: string; payload: Record<string, unknown> }[] | undefined): typeof fetch {
  return async (input, init = {}) => {
    const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { points?: Array<{ id: string; payload: Record<string, unknown> }>; update_mode?: string; ids?: string[] };
    if (url.includes("/points/retrieve")) {
      const ids = body?.ids ?? [];
      const extra = intercept?.(ids);
      const found = ids.map((id) => points.get(id)).filter((point) => point !== undefined);
      return new Response(JSON.stringify({ result: [...found, ...(extra ?? [])], status: "ok" }), { headers: { "content-type": "application/json" } });
    }
    if (url.includes("/points?") && init.method === "PUT") { for (const point of body?.points ?? []) points.set(point.id, { id: point.id, payload: point.payload }); return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); }
    return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } });
  };
}
function productionStore(points: Map<string, { id: string; payload: Record<string, unknown> }>, intercept?: (ids: readonly string[]) => { id: string; payload: Record<string, unknown> }[] | undefined): { store: ProductionCoordinationStore } {
  stubGlobalFetch(restFetch(points, intercept));
  return { store: createQdrantCoordinationStore(qdrantOptions()) };
}
/** Production store over REAL vector-bound episodes + the default control point. */
function productionVisibilityStore(episodes: readonly EpisodeRecord[] = []): { store: ProductionCoordinationStore; points: Map<string, { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }> } {
  const points = restPoints();
  for (const ep of episodes) points.set(ep.id, { id: ep.id, payload: recordPayload(ep), ...(ep.vector === undefined ? {} : { vector: { semantic: [...ep.vector] } }) });
  return { store: productionStore(points).store, points };
}
function boundRuntimeEmbedding(destination: { id: string; residency: string; dataUse: string } = { id: "embed:local", residency: "local", dataUse: "memory" }, coordinationHash = POLICY_HASH, coordinationEpoch = 1): BoundEmbeddingDestination {
  // Route embedding URLs to the embedding transport and delegate everything
  // else to the previously stubbed global fetch (e.g. the Qdrant backend).
  const previous = globalThis.fetch;
  stubGlobalFetch(async (input, init) => {
    const url = String(input);
    if (url.includes("/embeddings")) return new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] }), { headers: { "content-type": "application/json" } });
    if (previous !== undefined) return previous(input, init);
    throw new Error("no transport route");
  });
  const factory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 }) }), egressMode: "allowlist", coordinationPolicyHash: coordinationHash, coordinationPolicyEpoch: coordinationEpoch });
  return bindEmbeddingDestination(factory, destination);
}
function tombstoneRecord(targetId: string, privacyEpoch = 1): TombstoneRecord {
  const base = { recordType: "tombstone" as const, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch, processingPolicyId: "policy-1", expiresAt: null, id: tombstoneId(OWNER, targetId), scope: "occurrence" as const, targetId, contentHash: "pending" };
  return { ...base, contentHash: canonicalRecordHash(base) } as TombstoneRecord;
}

function processingPolicy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const base = { id: "pending", ownerHost: OWNER, destinationIds: { qdrant: "qdrant:pi", embedding: "embed:local", llm: "llm:local" }, originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "v1", ...overrides };
  return { ...base, id: processingPolicyHash(base) };
}

function control(overrides: Partial<ControlRecord> = {}): ControlRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 1, coordinationPolicyHash: POLICY_HASH, state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as ControlRecord;
}
function bootstrapControl(overrides: Partial<ControlRecord> = {}): ControlRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "control-policy-id", expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 0, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 0, coordinationPolicyHash: POLICY_HASH, state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], ...overrides, contentHash: "pending" };
  return { ...base, contentHash: bootstrapControlHash(base) } as ControlRecord;
}
function lease(jobIdValue: string, overrides: Partial<LeaseRecord> = {}): LeaseRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID, expiresAt: iso(NOW_MS + 30000), recordType: "lease" as const, id: leasePointId(jobIdValue), jobId: jobIdValue, ownerId: "node-a", version: 1, fencingToken: 1, state: "leased" as const, acceptedProposalId: null, acceptedManifestHash: null, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as LeaseRecord;
}
function jobRecord(jobIdValue: string, overrides: Partial<JobRecord> = {}): JobRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID, expiresAt: null, recordType: "job" as const, id: jobIdValue, policyId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, membership: ["episode-1"], extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as JobRecord;
}
function proposalRecord(jobIdValue: string, overrides: Partial<ProposalRecord> = {}): ProposalRecord {
  const content = { summary: "safe summary" };
  const merged = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID, expiresAt: null, recordType: "proposal" as const, jobId: jobIdValue, ownerId: "node-a", fencingToken: 2, membership: ["episode-1"], content, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, ...overrides, id: "pending", proposalHash: "pending", manifestHash: "pending", contentHash: "pending" };
  const proposalHash = proposalContentHash({ ownerHost: merged.ownerHost, jobId: merged.jobId, ownerId: merged.ownerId, membership: merged.membership, content: merged.content, policyHash: merged.coordinationPolicyHash, policyEpoch: merged.coordinationPolicyEpoch, fencingToken: merged.fencingToken, privacyEpoch: merged.privacyEpoch, policyIntersectionId: merged.processingPolicyId });
  const value = { ...merged, id: proposalIdFor(merged.jobId, proposalHash, merged.coordinationPolicyEpoch, merged.fencingToken), proposalHash, manifestHash: manifestHash(merged.membership) };
  return { ...value, contentHash: canonicalRecordHash(value as ProposalRecord) } as ProposalRecord;
}
function episode(overrides: Partial<EpisodeRecord> = {}): EpisodeRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "episode" as const, id: EPISODE_UUID, contentHash: "pending", sourceEntryId: "entry-1", host: OWNER, projectId: "project-1", projectIdentityKind: "registered" as const, sessionId: "session-1", turnId: "turn-1", agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: NOW, modelId: "model-1", embeddingDimension: 1024, originProvider: "provider-1", destinationId: "qdrant:pi", status: "active" as const, redactionStatus: "unchanged" as const, secretScan: "passed" as const, text: "safe" };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as EpisodeRecord;
}
function coverageRecord(jobIdValue: string, episodeIdValue: string, overrides: Partial<CoverageRecord> = {}): CoverageRecord {
  const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID, expiresAt: null, recordType: "coverage" as const, id: coverageId({ ownerHost: OWNER, episodeId: episodeIdValue, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 }), episodeId: episodeIdValue, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
  const value = { ...base, ...overrides };
  return { ...value, contentHash: canonicalRecordHash(value) } as CoverageRecord;
}

/** Atomic in-memory CoordinationStore test double with strict per-call CAS semantics. */
/** Filter-honoring in-memory REST backend (typed control/lease CAS) for production-store tests. */
function casBackend(seed: Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }> = [], hooks: { onUpsert?: (points: Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>, mode: string) => void; extra?: (ids: readonly string[]) => Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }> | undefined } = {}): { points: Map<string, { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>; fetchImpl: typeof fetch } {
  const points = new Map<string, { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>(seed.map((point) => [point.id, point]));
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { ids?: string[]; points?: Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>; update_mode?: string; update_filter?: { must: Array<{ key: string; match?: { value?: unknown }; is_null?: { key: string }; range?: { lte?: string; gt?: string } }> } };
    if (url.includes("/points/retrieve")) { const ids = body?.ids ?? []; const extra = hooks.extra === undefined ? undefined : await hooks.extra(ids); return new Response(JSON.stringify({ result: [...ids.map((id) => points.get(id)).filter((point) => point !== undefined), ...(extra ?? [])], status: "ok" }), { headers: { "content-type": "application/json" } }); }
    if (url.includes("/points/scroll")) return new Response(JSON.stringify({ result: { points: [...points.values()].filter((point) => point.payload.record_type === "lease").sort((a, b) => (a.id < b.id ? -1 : 1)).slice(0, 256), next_page_offset: null }, status: "ok" }), { headers: { "content-type": "application/json" } });
    if (url.includes("/points?") && init.method === "PUT") {
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
      if (body?.update_mode === "update_only" && !matches) return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } });
      if (hooks.onUpsert !== undefined) await hooks.onUpsert(body?.points ?? [], String(body?.update_mode));
      for (const incoming of body?.points ?? []) { if (body?.update_mode === "insert_only" && points.has(incoming.id)) continue; points.set(incoming.id, { id: incoming.id, payload: incoming.payload, ...(incoming.vector === undefined ? {} : { vector: incoming.vector }) }); }
      return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } });
    }
    return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } });
  };
  return { points, fetchImpl };
}
/** PRODUCTION store over a filter-honoring backend (the structural fake is gone: only genuine Production stores exist). */
function fakeStoreWithBackend(seed: { control?: ControlRecord; leases?: LeaseRecord[]; jobs?: JobRecord[]; proposals?: ProposalRecord[]; tombstones?: TombstoneRecord[]; coverage?: CoverageRecord[]; episodes?: EpisodeRecord[] } = {}, hooks: { onUpsert?: (points: Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }>, mode: string) => void | Promise<void>; extra?: (ids: readonly string[]) => Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }> | undefined | Promise<Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }> | undefined> } = {}): { store: ProductionCoordinationStore; backend: ReturnType<typeof casBackend> } {
  const points: Array<{ id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }> = [];
  points.push({ id: COLLECTION_CONTROL_ID, payload: controlPayload(seed.control ?? control()) });
  for (const record of [...(seed.leases ?? []), ...(seed.jobs ?? []), ...(seed.proposals ?? []), ...(seed.tombstones ?? []), ...(seed.coverage ?? [])]) points.push({ id: physicalPointOf(record), payload: recordPayload(record) });
  for (const ep of seed.episodes ?? []) { const vector = ep.vector ?? Array.from({ length: 1024 }, () => 0.25); const withVector = { ...ep, vector: [...vector] } as EpisodeRecord; const finalEp = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord; points.push({ id: finalEp.id, payload: recordPayload(finalEp) as Record<string, unknown>, vector: { semantic: [...vector] } }); }
  const b = casBackend(points, hooks);
  vi.stubGlobal("fetch", b.fetchImpl);
  return { store: createQdrantCoordinationStore(qdrantOptions()), backend: b };
}
function fakeStore(seed: { control?: ControlRecord; leases?: LeaseRecord[]; jobs?: JobRecord[]; proposals?: ProposalRecord[]; tombstones?: TombstoneRecord[]; coverage?: CoverageRecord[]; episodes?: EpisodeRecord[] } = {}): ProductionCoordinationStore {
  return fakeStoreWithBackend(seed).store;
}
function physicalPointOf(record: MemoryRecord): string {
  if (record.recordType === "collection_control") return COLLECTION_CONTROL_ID;
  if (record.recordType === "lease") return leasePointId(record.jobId);
  return record.id;
}

describe("Task 8 coordination protocol", () => {
  it("keeps one owner-independent collection control point and rereads the Task 3 bootstrap", async () => {
    expect(COLLECTION_CONTROL_ID).toBe(deterministicUuid("pi-qdrant-memory-v2", "collection_control"));
    const store = fakeStore({ control: bootstrapControl() });
    const stored = await initializeControl(store, bootstrapControl());
    expect(stored.id).toBe(COLLECTION_CONTROL_ID);
    expect(stored.version).toBe(0);
    expect(stored.privacyEpoch).toBe(0);
    expect(stored.coordinationPolicyEpoch).toBe(0);
    expect(stored.state).toBe("active");
    expect(stored.revokedDestinationIds).toEqual([]);
    await expect(initializeControl(store, control())).rejects.toThrow(/bootstrap|initialized/i);
  });

  it("reads bounded monotonic collection-wide revocations from the single control point", async () => {
    // The store preserves revocations across every safe transition; there is no
    // raw revocation CAS anymore (the raw surface is gone).
    const store = fakeStore({ control: control({ revokedDestinationIds: ["qdrant:pi"] }) });
    let current = await readControl(store);
    expect(current.revokedDestinationIds).toEqual(["qdrant:pi"]);
    // Safe transitions keep the revocation set intact (monotonic control CAS).
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.revokedDestinationIds).toEqual(["qdrant:pi"]);
    const forgotten = await beginForgetBarrier(store, { now: NOW_MS });
    expect(forgotten.revokedDestinationIds).toEqual(["qdrant:pi"]);
    expect(forgotten.privacyEpoch).toBe(1);
    // The ingest control reader surfaces the bounded revocation snapshot.
    const reader = createIngestControlReader(store, { policyHash: POLICY_HASH, policyEpoch: 1 });
    await expect(reader.read()).resolves.toMatchObject({ revokedDestinationIds: ["qdrant:pi"] });
    const unbounded = { ...current, version: current.version + 1, revokedDestinationIds: Array.from({ length: 1025 }, (_, index) => `dest-${index}`), contentHash: "pending" };
    expect(() => canonicalRecordHash(unbounded)).toThrow(/revoked|bounded/i);
  });

  it("exposes the Task 7 control snapshot from production control state", async () => {
    const store = fakeStore();
    const reader: IngestControlReader = createIngestControlReader(store, { policyHash: POLICY_HASH, policyEpoch: 1 });
    await expect(reader.read()).resolves.toEqual({ state: "active", privacyEpoch: 0, coordinationPolicyEpoch: 1, policyHash: POLICY_HASH, revokedDestinationIds: [] });
  });

  it("drains via one-point OCC with active generation disabled and rereads before/after", async () => {
    const store = fakeStore({ control: control({ activeGeneration: "gen-1" }) });
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.state).toBe("draining");
    expect(drained.activeGeneration).toBeNull();
    expect(drained.version).toBe(2);
    expect(drained.coordinationPolicyEpoch).toBe(1);
    await expect(beginPolicyDrain(store, { now: NOW_MS })).rejects.toThrow(/drain|active/i);
    await expect(claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: "job-1", policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
  });

  it("issues an unforgeable quiescence proof and rejects forged or stale proofs at activation", async () => {
    const store = fakeStore({
      control: control({ state: "draining", version: 2 }),
      leases: [lease("job-1", { coordinationPolicyEpoch: 1, expiresAt: iso(NOW_MS + 5000), version: 1, fencingToken: 1, state: "leased" }), lease("job-2", { coordinationPolicyEpoch: 1, expiresAt: iso(NOW_MS - 1000), version: 1, fencingToken: 1, state: "leased" })],
    });
    let now = NOW_MS + 3000;
    const pending = waitForOldLeasesToQuiesce(store, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 1000, timeoutMs: 60000, pollIntervalMs: 5, now: () => now });
    await new Promise((resolve) => setTimeout(resolve, 20));
    now = NOW_MS + 20000;
    const proof = await pending;
    expect(QuiescenceProof.isValid(proof)).toBe(true);
    expect(proof.matches(control({ state: "draining", version: 2 }))).toBe(true);
    // Forged structural proofs and stale proofs are rejected before any CAS.
    const forged = Object.create(QuiescenceProof.prototype) as QuiescenceProof;
    expect(QuiescenceProof.isValid(forged)).toBe(false);
    await expect(activatePolicyEpoch(store, { proof: forged, nextPolicyHash: "policy-hash-v3", memoryModelTimeoutMs: 0 })).rejects.toThrow(/proof/i);
    await expect(activatePolicyEpoch(store, { proof: { matches: () => true } as unknown as QuiescenceProof, nextPolicyHash: "policy-hash-v3", memoryModelTimeoutMs: 0 })).rejects.toThrow(/proof/i);
    const active = await activatePolicyEpoch(store, { proof, nextPolicyHash: "policy-hash-v3", memoryModelTimeoutMs: 0 });
    expect(active.state).toBe("active");
    expect(active.coordinationPolicyEpoch).toBe(2);
    expect(active.coordinationPolicyHash).toBe("policy-hash-v3");
    expect(active.version).toBe(3);
    // A stale proof against the now-active control is refused.
    await expect(activatePolicyEpoch(store, { proof, nextPolicyHash: "policy-hash-v4", memoryModelTimeoutMs: 0 })).rejects.toThrow(/proof|draining/i);
  });

  it("never activates without quiescence and aborts the bounded LLM wait", async () => {
    const store = fakeStore({ control: control({ state: "draining", version: 2 }) });
    const controller = new AbortController();
    const aborted = activatePolicyEpoch(store, { proof: await waitForOldLeasesToQuiesce(store, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, timeoutMs: 100, pollIntervalMs: 5, now: () => NOW_MS + 40000 }), nextPolicyHash: "policy-hash-v3", memoryModelTimeoutMs: 30000, signal: controller.signal });
    controller.abort();
    await expect(aborted).rejects.toThrow(/abort/i);
    const after = await readControl(store);
    expect(after.state).toBe("draining");
    expect(after.coordinationPolicyEpoch).toBe(1);
  });

  it("rotates the coordination policy through drain, quiescence proof and epoch activation", async () => {
    const store = fakeStore({ control: control({ version: 1 }), leases: [lease("job-1", { coordinationPolicyEpoch: 1, expiresAt: iso(NOW_MS - 5000) })] });
    const job2 = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job2));
    await expect(claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job2.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.not.toBeNull();
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.state).toBe("draining");
    const rotated = await waitForOldLeasesToQuiesce(store, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, timeoutMs: 1000, pollIntervalMs: 5, now: () => NOW_MS + 31000 }).then((proof) => activatePolicyEpoch(store, { proof, nextPolicyHash: "policy-hash-v3", memoryModelTimeoutMs: 0 }));
    expect(rotated.coordinationPolicyEpoch).toBe(2);
    expect(rotated.coordinationPolicyHash).toBe("policy-hash-v3");
    const job3 = jobRecord(jobId(OWNER, ["episode-2"], "policy-hash-v3", EXTRACTOR, 2, INTERSECTION_ID, 0), { membership: ["episode-2"], coordinationPolicyEpoch: 2, coordinationPolicyHash: "policy-hash-v3", policyHash: "policy-hash-v3", policyEpoch: 2 });
    await createJob(store, jobInputFrom(job3));
    await expect(claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job3.id, policyEpoch: 2, policyHash: "policy-hash-v3", privacyEpoch: 0 })).resolves.not.toBeNull();
    const job4 = jobRecord(jobId(OWNER, ["episode-3"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-3"] });
    await createJob(store, jobInputFrom(job4));
    await expect(claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job4.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    // rotateCoordinationPolicy composes the same steps.
    await expect(rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-v4", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS })).resolves.toMatchObject({ coordinationPolicyEpoch: 3, coordinationPolicyHash: "policy-hash-v4" });
  });

  it("raises a forget barrier on the same control point and bumps privacy epoch", async () => {
    const store = fakeStore({ control: control({ activeGeneration: "gen-9" }) });
    const barrier = await beginForgetBarrier(store, { now: NOW_MS });
    expect(barrier.privacyEpoch).toBe(1);
    expect(barrier.activeGeneration).toBeNull();
    expect(barrier.lastForgetBarrier).toBe(NOW);
    expect(barrier.version).toBe(2);
    await expect(beginForgetBarrier(store, { now: NOW_MS })).resolves.toMatchObject({ privacyEpoch: 2 });
  });

  it("claims fresh leases insert-only, never returns an expired same-owner claim as live, and reacquires released claims", async () => {
    const store = fakeStore();
    const job1 = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job1));
    const wA = timedWorker(NOW_MS, "node-a");
    const first = await claimLease(store, wA.worker, { jobId: job1.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(first).not.toBeNull();
    expect(first?.jobId).toBe(job1.id);
    expect(first?.ownerId).toBe("node-a");
    expect(first?.version).toBe(1);
    expect(first?.fencingToken).toBe(1);
    expect(first?.state).toBe("leased");
    expect(first?.expiresAt).toBe(iso(NOW_MS + 30000));
    // Concurrent acquire by another owner fails while the claim is live.
    await expect(claimLease(store, workerAt("node-b", NOW_MS + 1000, 30000, 1000), { jobId: job1.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    // Renewal keeps the same fencing token and advances version (new authority).
    wA.set(NOW_MS + 1000);
    const renewed = await renewLease(store, first!);
    expect(renewed).not.toBeNull();
    expect(renewed?.version).toBe(2);
    expect(renewed?.fencingToken).toBe(1);
    expect(renewed?.expiresAt).toBe(iso(NOW_MS + 31000));
    // Steal requires conservative expiry: expiresAt + skew <= now, exact owner pinned.
    await expect(claimLease(store, workerAt("node-b", NOW_MS + 31000, 30000, 2000), { jobId: job1.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    const wB = timedWorker(NOW_MS + 33000, "node-b");
    const stolen = await claimLease(store, wB.worker, { jobId: job1.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(stolen).not.toBeNull();
    expect(stolen?.ownerId).toBe("node-b");
    expect(stolen?.version).toBe(3);
    expect(stolen?.fencingToken).toBe(2);
    expect(stolen?.state).toBe("leased");
    expect(stolen?.fencingToken ?? 0).toBeGreaterThan(first?.fencingToken ?? 0);
    // A stale owner cannot renew after the steal (old authority is stale by version/fence).
    await expect(renewLease(store, first!)).resolves.toBeNull();
    // Release rereads; the released claim is immediately reacquirable with fence+1.
    wB.set(NOW_MS + 34000);
    expect(await releaseLease(store, stolen!)).toBe(true);
    const released = await readLease(store, job1.id);
    expect(released?.state).toBe("released");
    const reacquired = await claimLease(store, wB.worker, { jobId: job1.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(reacquired).not.toBeNull();
    expect(reacquired?.ownerId).toBe("node-b");
    expect(reacquired?.version).toBe(5);
    expect(reacquired?.fencingToken).toBe(3);
    expect(reacquired?.state).toBe("leased");
    // An expired same-owner claim is never returned as live: fenced reacquire
    // invalidates a crashed same-ID worker (fence+1, conservative cut).
    const job9 = jobRecord(jobId(OWNER, ["episode-9"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-9"] });
    await createJob(store, jobInputFrom(job9));
    const oldClaim = await claimLease(store, workerAt("node-a", NOW_MS - 60000, 30000, 0), { jobId: job9.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(oldClaim?.expiresAt).toBe(iso(NOW_MS - 30000));
    const w9 = timedWorker(NOW_MS, "node-a");
    const sameOwnerExpired = await claimLease(store, w9.worker, { jobId: job9.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(sameOwnerExpired).not.toBeNull();
    expect(sameOwnerExpired?.expiresAt).toBe(iso(NOW_MS + 30000));
    expect(sameOwnerExpired?.version).toBe(2);
    expect(sameOwnerExpired?.fencingToken).toBe(2);
    // An expired claim cannot be renewed: renewal requires a live claim.
    w9.set(NOW_MS + 40000);
    await expect(renewLease(store, sameOwnerExpired!)).resolves.toBeNull();
    expect((await readLease(store, job9.id))?.fencingToken).toBe(2);
  });

  it("exploit regression: public owner/fence strings can never mint authority — A cannot impersonate B with zero mutations", async () => {
    const upserts: string[] = [];
    const store = fakeStoreWithBackend({}, { onUpsert: (_points, mode) => { upserts.push(mode); } }).store;
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const authorityB = await claimLease(store, workerAt("node-b", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authorityB).not.toBeNull();
    // Node A reads the PUBLIC claim strings (ownerId "node-b", fence 1) and
    // tries to act: the APIs expose no tuple path — a node-a worker can only
    // ever derive authority from its OWN nodeId.
    const claimFromStore = await readLease(store, job.id);
    expect(claimFromStore?.ownerId).toBe("node-b");
    const workerA = workerAt("node-a", NOW_MS);
    const claimA = await claimLease(store, workerA, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claimA).toBeNull(); // node-a can never claim node-b's live lease
    const writesBefore = upserts.length;
    await expect(claimLease(store, workerA, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    expect(upserts.length).toBe(writesBefore); // zero lease mutation
    // A node-a worker minting an authority for its own job cannot write/accept
    // under node-b's strings — the authority carries node-a's identity.
    const jobA2 = jobRecord(jobId(OWNER, ["episode-2"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-2"] });
    await createJob(store, jobInputFrom(jobA2));
    const authorityA = await claimLease(store, workerA, { jobId: jobA2.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(authorityA).not.toBeNull();
    expect(authorityA?.ownerId).toBe("node-a");
    expect(authorityA?.fencingToken).toBe(1);
    // A copied structural object with node-b's strings is not an authority.
    const structural = { store, worker: workerA, ownerHost: "pi" as const, nodeId: "node-b", jobId: jobA2.id, ownerId: "node-b", version: 1, fencingToken: 1, state: "leased" as const, acceptedProposalId: null, acceptedManifestHash: null, contentHash: "x", processingPolicyId: INTERSECTION_ID, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, privacyEpoch: 0, expiresAt: iso(NOW_MS + 30000), leaseMs: 30000, jobDeadline: null };
    expect(LeaseAuthority.isValid(structural)).toBe(false);
    await expect(writeProposal(store, structural as never, { membership: ["episode-2"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/authority/i);
    const writesBefore2 = upserts.length;
    await expect(writeProposal(store, structural as never, { membership: ["episode-2"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/authority/i);
    expect(upserts.length).toBe(writesBefore2); // zero proposal mutation
  });

  it("a genuine old authority cannot release inside the skew grace; after conservative expiry a genuine contender gets fence+1", async () => {
    const upserts: string[] = [];
    const store = fakeStoreWithBackend({}, { onUpsert: (_points, mode) => { upserts.push(mode); } }).store;
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a");
    const authorityA = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(authorityA).not.toBeNull();
    // Locally expired but inside the grace: the GENUINE old authority cannot release (no CAS).
    wA.set(NOW_MS + 31000);
    const writesBefore = upserts.length;
    expect(await releaseLease(store, authorityA!)).toBe(false);
    expect(upserts.length).toBe(writesBefore);
    // A contender claim stays null until conservative expiry.
    await expect(claimLease(store, workerAt("node-b", NOW_MS + 31000, 30000, 300000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    // After expiry+skew the genuine contender gets fence+1.
    const wB = timedWorker(NOW_MS + 330001, "node-b");
    const authorityB = await claimLease(store, wB.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(authorityB).not.toBeNull();
    expect(authorityB?.fencingToken).toBe(2);
    expect(authorityB?.ownerId).toBe("node-b");
  });

  it("current genuine authority: proposal, accept, renew, materialize and release under real defaults; proposal proves original owner+fence", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = timedWorker(NOW_MS, "node-a");
    const authority = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(authority).not.toBeNull();
    const proposal = await writeProposal(store, authority!, { membership: ["episode-1"], content: { summary: "safe" }, policyHash: POLICY_HASH, policyEpoch: 1, privacyEpoch: 0, policyIntersectionId: INTERSECTION_ID, createdAt: NOW, maxClockSkewMs: 300000 });
    // The immutable proposal records the ORIGINAL producing owner + fence.
    expect(proposal.ownerId).toBe("node-a");
    expect(proposal.fencingToken).toBe(1);
    const accepted = await acceptProposal(store, authority!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 300000 });
    expect(accepted).not.toBeNull();
    const active = await readActiveAcceptance(store, accepted!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 300000 });
    expect(active?.proposalId).toBe(proposal.id);
    // Renew returns a NEW authority (the old one is stale immediately).
    w.set(NOW_MS + 10000);
    const renewed = await renewLease(store, accepted!);
    expect(renewed).not.toBeNull();
    expect(renewed?.version).toBe(3);
    // The pre-renew authority is stale after the CAS.
    await expect(renewLease(store, accepted!)).resolves.toBeNull();
    // Release consumes the CURRENT authority and succeeds.
    w.set(NOW_MS + 20000);
    expect(await releaseLease(store, renewed!)).toBe(true);
    const after = await readLease(store, job.id);
    expect(after?.state).toBe("released");
    // Cross-worker: an authority minted by a different worker object fails.
    const otherWorker = workerAt("node-a", NOW_MS);
    const otherAuthority = await claimLease(store, otherWorker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(otherAuthority).not.toBeNull();
    const crossWorker = mintRootWorker(OWNER, "node-a", () => NOW_MS);
    const impostor = { ...otherAuthority } as unknown as LeaseAuthority;
    expect(LeaseAuthority.isValid(impostor)).toBe(false);
  });

  it("raw rotation is not exported: no caller-supplied next transition can ever mint an accepted authority", async () => {
    // The raw rotation must NOT be part of the public surface.
    const leasesModule = await import("../../src/coordination/leases.js");
    expect("rotateLeaseAuthority" in leasesModule).toBe(false);
    const exported = Object.keys(leasesModule).sort();
    expect(exported).toContain("LeaseAuthority");
    expect(exported).toContain("acceptLeaseAuthority");
    expect(exported).not.toContain("rotateLeaseAuthority");
    expect(exported).not.toContain("mintLeaseAuthority");
    // A genuine LEASED authority can only advance through the safe acceptance:
    // a stale/foreign proposal never mints an accepted authority.
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const authority = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    // A proposal for a DIFFERENT job can never be accepted.
    const foreignJob = jobRecord(jobId(OWNER, ["episode-9"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-9"] });
    await createJob(store, jobInputFrom(foreignJob));
    const foreignAuthority = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: foreignJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    const foreignProposal = await writeProposal(store, foreignAuthority!, { membership: ["episode-9"], content: { summary: "safe summary" }, createdAt: NOW });
    await expect(acceptProposal(store, authority!, { proposalId: foreignProposal.id })).rejects.toThrow(/bound|proposal|job/i);
    // A proposal with the WRONG owner/fence (node-b's identity) cannot mint acceptance:
    // node-b must first steal the live claim, so node-a's authority is stale and
    // its acceptance is refused on the claim/authority linkage.
    const stolen = await claimLease(store, workerAt("node-b", NOW_MS + 31000, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(stolen).not.toBeNull();
    const wrongOwner = await writeProposal(store, stolen!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    expect(wrongOwner.ownerId).toBe("node-b");
    await expect(acceptProposal(store, authority!, { proposalId: wrongOwner.id })).rejects.toThrow(/stale|bound|proposal|authority/i);
    const after = await readLease(store, job.id);
    expect(after?.state).toBe("leased");
    expect(after?.acceptedProposalId).toBeNull();
  });

  it("authority is opaque: no store/worker getters, structural copies fail, class and prototype frozen", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const authority = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    // NO powerful store/worker leakage.
    expect("store" in (authority as unknown as Record<string, unknown>)).toBe(false);
    expect("worker" in (authority as unknown as Record<string, unknown>)).toBe(false);
    expect((authority as unknown as Record<string, unknown>).store).toBeUndefined();
    expect((authority as unknown as Record<string, unknown>).worker).toBeUndefined();
    expect(JSON.stringify(authority)).toBe("{}");
    // matchesStore is the only store binding operation.
    expect(authority?.matchesStore(store)).toBe(true);
    const otherStore = fakeStore();
    expect(authority?.matchesStore(otherStore)).toBe(false);
    // now() delegates to the private bound worker (validating monotonic clock).
    expect(typeof authority?.now).toBe("function");
    expect(authority?.now()).toBe(NOW_MS);
    expect(authority?.now()).toBe(NOW_MS);
    // Structural copies and prototype forgeries fail.
    const copy = { ...authority } as unknown as LeaseAuthority;
    expect(LeaseAuthority.isValid(copy)).toBe(false);
    const forged = Object.create(LeaseAuthority.prototype) as LeaseAuthority;
    expect(LeaseAuthority.isValid(forged)).toBe(false);
    expect(Object.isFrozen(LeaseAuthority)).toBe(true);
    expect(Object.isFrozen(LeaseAuthority.prototype)).toBe(true);
    expect(() => { (LeaseAuthority as unknown as Record<string, unknown>).isValid = () => true; }).toThrow();
    expect(() => { (LeaseAuthority.prototype as unknown as Record<string, unknown>).hack = 1; }).toThrow();
  });

  it("bound skew cannot be downgraded: operations within the job future-skew window fail; the drain path still allows genuine release", async () => {
    const store = fakeStore();
    // Job deadline NOW_MS+60000 with skew 300000: at NOW_MS+10000 the job is
    // expired (deadline <= now+skew) even though the lease is live.
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { expiresAt: iso(NOW_MS + 310000) });
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a", 30000, 300000);
    const authority = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    wA.set(NOW_MS + 10000);
    // No caller can downgrade the bound skew: these all fail with the BOUND 300000.
    await expect(renewLease(store, authority!)).resolves.toBeNull();
    await expect(releaseLease(store, authority!)).resolves.toBe(false);
    await expect(writeProposal(store, authority!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/expired/i);
    // The expired job cannot write a proposal (rejected above); accepting with
    // any proposal id on the expired job fails closed on the fresh-clock check.
    await expect(acceptProposal(store, authority!, { proposalId: "00000000-0000-4000-8000-000000000099" })).rejects.toThrow(/expired|proposal/i);
    // Drain path: beginPolicyDrain -> a genuine LIVE authority can still release
    // (coord hash+epoch/privacy unchanged), enabling quiescence; the claim
    // deadline check uses the BOUND skew so an expired job cannot release.
    const liveJob = jobRecord(jobId(OWNER, ["episode-2"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-2"] });
    await createJob(store, jobInputFrom(liveJob));
    const job2 = jobRecord(jobId(OWNER, ["episode-3"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-3"] });
    await createJob(store, jobInputFrom(job2));
    const wB = timedWorker(NOW_MS, "node-b");
    const liveAuthority = await claimLease(store, wB.worker, { jobId: liveJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(liveAuthority).not.toBeNull();
    const wC = timedWorker(NOW_MS, "node-c");
    const third = await claimLease(store, wC.worker, { jobId: job2.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(third).not.toBeNull();
    const drained = await beginPolicyDrain(store, { now: NOW_MS });
    expect(drained.state).toBe("draining");
    wB.set(NOW_MS + 10000);
    expect(await releaseLease(store, liveAuthority!)).toBe(true);
    const released = await readLease(store, liveJob.id);
    expect(released?.state).toBe("released");
    // A draining control with a CHANGED privacy epoch rejects the release.
    await beginForgetBarrier(store, { now: NOW_MS });
    wC.set(NOW_MS + 10000);
    expect(await releaseLease(store, third!)).toBe(false);
    // Forged/expired authorities cannot exploit the drain release.
    const forged = Object.create(LeaseAuthority.prototype) as LeaseAuthority;
    await expect(releaseLease(store, forged)).rejects.toThrow(/authority/i);
  });

  it("production store exposes NO raw mutators, writer escape or engine; raw qdrant helpers are not public", async () => {
    const { store } = productionStore(restPoints());
    const storeAny = store as unknown as Record<string, unknown>;
    for (const raw of ["compareAndSwapControl", "casLease", "insertLease", "insertJob", "insertProposal", "insertCoverage", "insertTombstone", "upsertPoints", "client", "writer", "session", "engine", "protocol"]) {
      expect(raw in storeAny).toBe(false);
      expect(typeof storeAny[raw]).toBe("undefined");
    }
    // Structural fake protocol objects cannot be mixed with the real store.
    const fake = { ownerHost: "pi", readControl: async () => emptyControl(), readLease: async () => null, readJob: async () => null, readProposal: async () => null, readTombstones: async () => [], readCoverage: async () => [], readEpisodes: async () => [], readEpisode: async () => null, scrollLeases: async () => ({ leases: [] }) } as never;
    expect(ProductionCoordinationStore.isValid(fake)).toBe(false);
    // The public leases module exposes ONLY safe operations (no raw engine seam).
    const leasesModule = await import("../../src/coordination/leases.js");
    expect(Object.keys(leasesModule).sort()).toEqual(["LeaseAuthority", "acceptLeaseAuthority", "claimLease", "isLeaseExpired", "readLease", "releaseLease", "renewLease"].sort());
    // The safe named APIs still work (job seeded through the SAFE createJob op).
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const authority = await claimLease(store, workerAt("node-a", NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    // A genuine authority cannot submit arbitrary expiry cutoffs or next records.
    const current = await readLease(store, job.id);
    expect(current).not.toBeNull();
    // Package subpath imports of raw modules are fenced.
    const pkg = JSON.parse(await (await import("../../package.json", { with: { type: "json" } })).default ? JSON.stringify(await import("../../package.json", { with: { type: "json" } })) : "{}") as { exports?: Record<string, unknown> };
    expect(pkg.exports).toBeDefined();
    for (const subpath of ["dist/coordination/control.js", "dist/qdrant/write.js", "dist/*"]) {
      expect(JSON.stringify(pkg.exports)).not.toContain(subpath);
    }
  });

  it("no caller leaseMs/skew parameter path exists: the claim derives them ONLY from the genuine worker", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    // A worker issued with skew 300000 binds it: a claim CANNOT pass skew=0 or a
    // 24h TTL — the input has no such fields.
    const w = timedWorker(NOW_MS, "node-a", 60000, 300000);
    const authority = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    expect(authority?.leaseMs).toBe(60000);
    expect(authority?.maxClockSkewMs).toBe(300000);
    expect(authority?.expiresAt).toBe(iso(NOW_MS + 60000));
    // The successor binds the same configuration.
    w.set(NOW_MS + 10000);
    const renewed = await renewLease(store, authority!);
    expect(renewed).not.toBeNull();
    expect(renewed?.leaseMs).toBe(60000);
    expect(renewed?.maxClockSkewMs).toBe(300000);
  });

  it("renew samples the FINAL clock strictly AFTER the final claim reread: a lease expiring between them is never renewed", async () => {
    let leaseReads = 0;
    let counting = false;
    const store = fakeStoreWithBackend({}, { extra: (ids) => { if (counting && ids.some((id) => id === leasePointId(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0)))) { leaseReads += 1; if (leaseReads === 3) wA.set(NOW_MS + 60000); } return undefined; } }).store;
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claim).not.toBeNull();
    expect(claim?.expiresAt).toBe(iso(NOW_MS + 30000));
    leaseReads = 0;
    counting = true;
    // Advance the clock to just before expiry, then make the FINAL claim reread
    // (the last awaited operation in renewLease) advance the clock PAST the
    // freshly computed renewal expiry (now + leaseMs): with the old ordering the
    // final clock was sampled before that reread and the lease looked live.
    wA.set(NOW_MS + 29000);
    // Old ordering sampled the final clock BEFORE the final claim reread and
    // would have seen NOW+29000 (live vs the NOW+59000 renewal expiry) and
    // minted; the clock must be sampled after the LAST await, so the crossed
    // renewal expiry yields null.
    await expect(renewLease(store, claim!)).resolves.toBeNull();
    // The crossed final clock refuses the successor authority even though the
    // CAS landed: no renewed authority is ever minted after the last await.
    const persisted = await readLease(store, job.id);
    expect(persisted?.version).toBe(2);
  });

  it("claim insert-race rereads the CURRENT claim before claimOrSteal (never the captured null)", async () => {
    const holder: { backend?: ReturnType<typeof casBackend> } = {};
    let first = true;
    const { store, backend: racedBackend } = fakeStoreWithBackend({}, { onUpsert: async (points, mode) => {
      if (mode === "insert_only" && points.some((point) => point.payload.record_type === "lease") && first) {
        first = false;
        // A concurrent worker inserts a lease during the race (directly into
        // the shared backend map), then the insert fails.
        holder.backend!.points.set(leasePointId(job.id), { id: leasePointId(job.id), payload: recordPayload(lease("job-1", { id: leasePointId(job.id), jobId: job.id, ownerId: "node-x", version: 1, fencingToken: 1, state: "leased" as const, expiresAt: iso(NOW_MS + 30000), processingPolicyId: INTERSECTION_ID, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, privacyEpoch: 0 })) });
        throw new Error("insert raced");
      }
    } });
    holder.backend = racedBackend;
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a");
    const raced = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    // The claim must be minted from the CURRENT (node-x) claim via the exact
    // reread, never from the captured null: node-a cannot steal a live claim.
    expect(raced).toBeNull();
    const current = await readLease(store, job.id);
    expect(current?.ownerId).toBe("node-x");
  });

  it("fences a stale lease and accepts exactly one proposal on the claim point with root-only capability", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const first = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    // The old worker's proposal is written while its claim is still live.
    const oldProposal = await writeProposal(store, first!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    const wB = timedWorker(NOW_MS + 100000, "node-b");
    const stolen = await claimLease(store, wB.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(stolen).not.toBeNull();
    expect(stolen?.fencingToken ?? 0).toBeGreaterThan(first?.fencingToken ?? 0);
    const newProposal = await writeProposal(store, stolen!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    // Stale authority: the old worker's authority can never accept (it no longer
    // matches the persisted claim), in either interleaving order.
    await expect(acceptProposal(store, first!, { proposalId: oldProposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/stale|fencing/i);
    const accepted = await acceptProposal(store, stolen!, { proposalId: newProposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(accepted).not.toBeNull();
    expect(accepted?.state).toBe("accepted");
    const claim = await readLease(store, job.id);
    expect(claim?.state).toBe("accepted");
    expect(claim?.acceptedProposalId).toBe(newProposal.id);
    expect(claim?.acceptedManifestHash).toBe(newProposal.manifestHash);
    // Exactly one proposal: a second different proposal cannot be accepted, and
    // the pre-accept authority is stale immediately after the CAS.
    await expect(acceptProposal(store, stolen!, { proposalId: oldProposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/stale|bound|fencing/i);
    // Acceptance survives steal/release (claim point is the authority) and is readable only through the ACCEPTED authority.
    const active = await readActiveAcceptance(store, accepted!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(active).toMatchObject({ proposalId: newProposal.id, manifestHash: newProposal.manifestHash, claimVersion: claim?.version, job: { id: job.id }, proposal: { id: newProposal.id } });
    // A live accepted claim cannot be stolen before conservative expiry.
    await expect(claimLease(store, workerAt("node-d", NOW_MS + 110000, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    // An accepted claim can be renewed and released while preserving the acceptance.
    wB.set(NOW_MS + 105000);
    const renewedAccepted = await renewLease(store, accepted!);
    expect(renewedAccepted).not.toBeNull();
    expect(renewedAccepted?.state).toBe("accepted");
    expect(renewedAccepted?.fencingToken).toBe(2);
    expect(await releaseLease(store, renewedAccepted!)).toBe(true);
    // The released accepted claim is reacquirable and the acceptance survives as the authority.
    const reacquiredAccepted = await claimLease(store, wB.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(reacquiredAccepted).not.toBeNull();
    expect(reacquiredAccepted?.state).toBe("accepted");
    expect(reacquiredAccepted?.fencingToken).toBe(3);
    // Accept-then-steal interleaving (after conservative expiry): the acceptance stays
    // authoritative on the claim point (steal preserves it), and the new owner still
    // cannot accept a second proposal.
    const wC = timedWorker(NOW_MS + 200000, "node-c");
    const stolenByC = await claimLease(store, wC.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(stolenByC).not.toBeNull();
    expect(stolenByC?.ownerId).toBe("node-c");
    expect(stolenByC?.fencingToken).toBe(4);
    const afterSteal = await readLease(store, job.id);
    expect(afterSteal?.acceptedProposalId).toBe(newProposal.id);
    expect(afterSteal?.state).toBe("accepted");
    // An ACCEPTED-state authority cannot accept (only leased authorities can);
    // a stale/foreign proposal never mints an accepted authority.
    await expect(acceptProposal(store, stolenByC!, { proposalId: oldProposal.id })).resolves.toBeNull();
    expect(await readActiveAcceptance(store, stolenByC!)).toMatchObject({ proposalId: newProposal.id, manifestHash: newProposal.manifestHash, claimVersion: afterSteal?.version, job: { id: job.id }, proposal: { id: newProposal.id } });
  });

  it("rejects forged worker capabilities, child evidence, and policy drift before reads or CAS", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const claim = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    // Forged structural capability: prototype clone without the private brand.
    const forged = Object.create(RootWorkerContext.prototype) as RootWorkerContext;
    expect(RootWorkerContext.isValid(forged)).toBe(false);
    await expect(acceptProposal(store, forged as never, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/authority|worker/i);
    // A forged LeaseAuthority (prototype clone / public constructor) fails before reads.
    const forgedAuthority = Object.create(LeaseAuthority.prototype) as LeaseAuthority;
    expect(LeaseAuthority.isValid(forgedAuthority)).toBe(false);
    expect(() => new LeaseAuthority({} as never, Symbol("forged"))).toThrow(/issuer/i);
    await expect(acceptProposal(store, forgedAuthority, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/authority|issuer/i);
    // Task 8 ships NO successful public root issuer; the tests-only harness is
    // the only construction path and it lives in this test file (never dist).
    expect(() => new RootWorkerContext("pi", "h", Symbol("forged"))).toThrow(/issuer/i);
    // The production module itself must expose no issuer (proved in root-issuance.test.ts).
    expect(() => new QuiescenceProof(control(), 0, Symbol("forged"))).toThrow(/issuer/i);
    expect(() => new BoundIngestTombstoneReader({ ownerHost: "pi", readTombstones: async () => [] }, "pi", Symbol("forged"))).toThrow(/issuer/i);
    expect(() => new ProductionCoordinationStore(qdrantOptions(), Symbol("forged"))).toThrow(/issuer/i);
    // Wrong-host capability fails before the CAS.
    const primeCapability = primeWorker();
    const primeAuthority = claimLease(store, primeCapability, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    await expect(primeAuthority).resolves.toBeNull();
    // A worker for a different store can never use an authority minted on this store.
    const otherStore = fakeStore();
    const crossStoreAuthority = await claimLease(otherStore, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(crossStoreAuthority).toBeNull(); // job missing on the other store
    // Policy drift (control no longer matches the observed epoch) fails before the CAS.
    await rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-v3", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS });
    await expect(acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/policy/i);
    const claimAfterDrift = await readLease(store, job.id);
    expect(claimAfterDrift?.acceptedProposalId).toBeNull();
  });

  it("makes post-CAS policy change inactive for materialization and blocks tombstoned membership", async () => {
    const membership = [EPISODE_UUID];
    const store = fakeStore({ episodes: [episode()] });
    const job = jobRecord(jobId(OWNER, membership, POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership });
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposal = await writeProposal(store, claim!, { membership, content: { summary: "safe summary" }, createdAt: NOW });
    // Rotate control BEFORE acceptance: the acceptance fails on the control policy, before any claim CAS.
    await rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-v3", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS });
    await expect(acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/policy/i);
    const claimAfterFailedAccept = await readLease(store, job.id);
    expect(claimAfterFailedAccept?.acceptedProposalId).toBeNull();
    // Tombstoned membership blocks materialization of an otherwise-accepted claim.
    const freshStore = fakeStore({ episodes: [episode()] });
    const freshJob = jobRecord(jobId(OWNER, membership, POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership });
    await createJob(freshStore, jobInputFrom(freshJob));
    const wB = timedWorker(NOW_MS, "node-a");
    const freshClaim = await claimLease(freshStore, wB.worker, { jobId: freshJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const freshProposal = await writeProposal(freshStore, freshClaim!, { membership, content: { summary: "safe summary" }, createdAt: NOW });
    const freshAccepted = await acceptProposal(freshStore, freshClaim!, { proposalId: freshProposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(freshAccepted).not.toBeNull();
    await createTombstone(freshStore, { ownerHost: OWNER, scope: "occurrence", targetId: EPISODE_UUID, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    await expect((readActiveAcceptance(freshStore, freshAccepted!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 }))).resolves.toBeNull();
    // Post-CAS policy drift: acceptance physically landed, but materialization rereads the active control and refuses.
    const driftedStore = fakeStore({ episodes: [episode()] });
    const driftedJob = jobRecord(jobId(OWNER, membership, POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership });
    await createJob(driftedStore, jobInputFrom(driftedJob));
    const wC = timedWorker(NOW_MS, "node-a");
    const driftedClaim = await claimLease(driftedStore, wC.worker, { jobId: driftedJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const driftedProposal = await writeProposal(driftedStore, driftedClaim!, { membership, content: { summary: "safe summary" }, createdAt: NOW });
    const driftedAccepted = await acceptProposal(driftedStore, driftedClaim!, { proposalId: driftedProposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(driftedAccepted).not.toBeNull();
    await rotateCoordinationPolicy(driftedStore, { nextPolicyHash: "policy-hash-v3", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS });
    await expect((readActiveAcceptance(driftedStore, driftedAccepted!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 }))).resolves.toBeNull();
  });

  it("never returns true when the claim is released or stolen during the slow tombstone read", async () => {
    const holder: { store?: ProductionCoordinationStore; claim?: LeaseAuthority; released?: boolean; armed?: boolean } = {};
    const { store, backend: tombBackend } = fakeStoreWithBackend({}, { extra: async (ids) => { if (holder.armed === true && !holder.released && ids.some((id) => id === tombstoneId(OWNER, EPISODE_UUID))) { holder.released = true; await releaseLease(holder.store!, holder.claim!); } return undefined; } });
    holder.store = store;
    const job = jobRecord(jobId(OWNER, [EPISODE_UUID], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: [EPISODE_UUID] });
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    holder.claim = claim!;
    const proposal = await writeProposal(store, claim!, { membership: [EPISODE_UUID], content: { summary: "safe summary" }, createdAt: NOW });
    holder.armed = true;
    // A release that lands DURING the tombstone read invalidates the authority check.
    expect(await acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).toBeNull();
    const after = await readLease(store, job.id);
    expect(after?.state).toBe("released");
  });

  it("never accepts when slow reads advance the trusted clock across expiry, and never lands a CAS", async () => {
    const gateHolder: { release?: () => void; started?: () => boolean } = {};
    let proposalIdValue = "";
    const { store, backend: propGateBackend } = fakeStoreWithBackend({}, { extra: async (ids) => { if (proposalIdValue !== "" && ids.includes(proposalIdValue)) { await new Promise<void>((resolve) => { gateHolder.started = () => true; gateHolder.release = resolve; }); } return undefined; } });
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    proposalIdValue = proposal.id;
    // Gate the slow proposal read; while it is in flight the trusted clock advances past expiry.
    const pending = acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    await vi.waitFor(() => expect(gateHolder.started?.()).toBe(true));
    w.set(NOW_MS + 200000);
    gateHolder.release?.();
    await expect(pending).rejects.toThrow(/expired|stale/i);
    const after = await readLease(store, job.id);
    expect(after?.state).toBe("leased");
    expect(after?.acceptedProposalId).toBeNull();
  });

  it("returns no materialization authority when the final control reread outlives the claim expiry", async () => {
    const holder2: { release?: () => void; started?: () => boolean } = {};
    let controlCalls = 0;
    let armed2 = false;
    const { store, backend: controlGateBackend } = fakeStoreWithBackend({}, { extra: async (ids) => { if (armed2 && ids.some((id) => id === COLLECTION_CONTROL_ID)) { controlCalls += 1; if (controlCalls === 2) { await new Promise<void>((resolve) => { holder2.started = () => true; holder2.release = resolve; }); } } return undefined; } });
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    const accepted = await acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(accepted).not.toBeNull();
    armed2 = true;
    // Gate the SECOND control reread inside readActiveAcceptance (same store).
    const pending = readActiveAcceptance(store, accepted!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    await vi.waitFor(() => expect(holder2.started?.()).toBe(true));
    w.set(NOW_MS + 200000);
    holder2.release?.();
    await expect(pending).resolves.toBeNull();
  });

  it("ignores any smuggled numeric now: only the worker's trusted clock authorizes", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    const accepted = await acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(accepted).not.toBeNull();
    // The trusted clock has advanced past the claim expiry: materialization is null.
    w.set(NOW_MS + 200000);
    await expect(readActiveAcceptance(store, accepted!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).resolves.toBeNull();
    // An ACCEPTED authority can never accept again (only leased ones can).
    await expect(acceptProposal(store, accepted!, { proposalId: proposal.id })).resolves.toBeNull();
  });

  it("fails closed when the trusted clock throws or returns invalid values, with no CAS", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const claim = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    // Broken-clock workers can never even mint a claim authority (claimLease samples the clock).
    for (const bad of [brokenClockWorker(), mintRootWorker(OWNER, "node-a", () => Number.NaN), mintRootWorker(OWNER, "node-a", () => -1), mintRootWorker(OWNER, "node-a", () => 1.5)]) {
      await expect(claimLease(store, bad, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, })).rejects.toThrow();
    }
    const after = await readLease(store, job.id);
    expect(after?.state).toBe("leased");
    expect(after?.acceptedProposalId).toBeNull();
  });

  it("returns null — never authority — when the clock fails after the CAS landed", async () => {
    const box = { value: NOW_MS };
    const gateHolder: { release?: () => void; started?: () => boolean } = {};
    const { store, backend: casGateBackend } = fakeStoreWithBackend({}, { onUpsert: async (points, mode) => { if (mode === "update_only" && points.some((point) => point.payload.record_type === "lease")) { await new Promise<void>((resolve) => { gateHolder.started = () => true; gateHolder.release = resolve; }); } } });
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = mintRootWorker(OWNER, "node-a", () => box.value);
    const claim = await claimLease(store, w, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim).not.toBeNull();
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    // Gate the CAS itself; the clock turns invalid AFTER the CAS is armed (the
    // fresh pre-CAS sample already happened) but BEFORE the authority rereads.
    const pending = acceptProposal(store, claim!, { proposalId: proposal.id });
    await vi.waitFor(() => expect(gateHolder.started?.()).toBe(true));
    box.value = -1;
    gateHolder.release?.();
    await expect(pending).resolves.toBeNull();
    const after = await readLease(store, job.id);
    expect(after?.state).toBe("accepted");
  });

  it("requires a branded accepted authority for materialization and refuses foreign capabilities", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    const accepted = await acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 });
    expect(accepted).not.toBeNull();
    // A forged/structural authority is refused before any read.
    const forged = Object.create(LeaseAuthority.prototype) as LeaseAuthority;
    expect(LeaseAuthority.isValid(forged)).toBe(false);
    await expect(readActiveAcceptance(store, forged, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/authority/i);
    // A leased-state authority (pre-accept) cannot materialize.
    await expect(readActiveAcceptance(store, claim!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).resolves.toBeNull();
    // An authority for a different store cannot materialize on this one.
    const otherStore = fakeStore();
    const otherJob = jobRecord(jobId(OWNER, ["episode-9"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-9"] });
    await createJob(otherStore, jobInputFrom(otherJob));
    const otherAuthority = await claimLease(otherStore, workerAt("node-a", NOW_MS, 30000, 0), { jobId: otherJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(otherAuthority).not.toBeNull();
    await expect(readActiveAcceptance(store, otherAuthority!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/store/i);
  });

  it("freezes authority statics: isValid cannot be monkeypatched and root/tombstone checks stay closed", () => {
    for (const cap of [QuiescenceProof, ProductionCoordinationStore, BoundIngestTombstoneReader, LeaseAuthority]) {
      const statics = cap as unknown as Record<string, unknown>;
      expect(() => { statics.isValid = () => true; }).toThrow();
      expect(() => { Object.defineProperty(cap, "isValid", { value: () => true }); }).toThrow();
      expect(() => { (cap.prototype as unknown as Record<string, unknown>).hack = 1; }).toThrow();
    }
    // RootWorkerContext is the tests-only harness here; the REAL class is frozen
    // and proven in root-issuance.test.ts. Brand checks remain closed even after
    // a structural store is presented to the reader factory.
    const structural = { ownerHost: "pi", readTombstones: async () => [] } as never;
    expect(ProductionCoordinationStore.isValid(structural)).toBe(false);
    expect(() => createIngestTombstoneReader(structural, "pi")).toThrow(/production store/i);
  });

  it("requires the EXACT writer transport: same-scalar separate bundles fail before egress; creation accepts validated options only", () => {
    // Two DIFFERENT safe bundles with identical scalars: separate transports.
    const a = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const b = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    expect(a.transport).not.toBe(b.transport);
    const qdrantA = bindQdrantDestination(a.qdrant, qdrantDestination);
    const qdrantB = bindQdrantDestination(b.qdrant, qdrantDestination);
    expect(() => bindIngestRuntime({ store: a.store, qdrant: qdrantB, embedding: boundRuntimeEmbedding() })).toThrow(/exact writer transport/i);
    expect(() => bindIngestRuntime({ store: b.store, qdrant: qdrantA, embedding: boundRuntimeEmbedding() })).toThrow(/exact writer transport/i);
    // Within one bundle the store and the destination share the exact transport.
    expect(() => bindIngestRuntime({ store: a.store, qdrant: qdrantA, embedding: boundRuntimeEmbedding() })).not.toThrow();
    // Creation APIs accept validated OPTIONS only: malformed options fail closed before any network.
    expect(() => createQdrantSafeBundle({ options: { ...qdrantOptions(), collection: "prime_memory" }, destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 })).toThrow(/collection does not match owner host/i);
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), baseUrl: "not a url" })).toThrow(/endpoint/i);
    // No constructor/factory/writer object exists anywhere in the safe surface.
    for (const obj of [a.store, qdrantA, a.qdrant]) {
      const anyObj = obj as unknown as Record<string, unknown>;
      for (const raw of ["writer", "upsertPoints", "client", "session", "engine", "casLease", "insertLease"]) {
        expect(raw in anyObj).toBe(false);
        expect(typeof anyObj[raw]).toBe("undefined");
      }
    }
  });

  it("snapshots the transport at construction: replacing global fetch cannot bypass a bound bundle", async () => {
    // Backend A: control + no tombstones (the captured transport).
    const backendA = restPoints();
    backendA.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(emptyControl()) });
    stubGlobalFetch(restFetch(backendA));
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const rt = bindIngestRuntime({ store: bundle.store, qdrant: bindQdrantDestination(bundle.qdrant, qdrantDestination), embedding: boundRuntimeEmbedding() });
    // Replace global fetch with a fake active/no-tombstone transport: the bound
    // store must keep using the CAPTURED transport A (control point present).
    stubGlobalFetch(async () => new Response(JSON.stringify({ result: { points: [] }, status: "ok" }), { headers: { "content-type": "application/json" } }));
    await expect(rt.store.readControl()).resolves.toMatchObject({ state: "active" });
    // And with a TOMBSTONE inserted into A, the barrier still sees it through the captured transport.
    const target = episode({ id: "00000000-0000-5000-8000-000000000001" });
    await seedTombstone(rt.store, backendA, target);
    const tombstones = await rt.tombstones.readTombstoned(["00000000-0000-5000-8000-000000000001"]);
    expect(tombstones).toEqual(["00000000-0000-5000-8000-000000000001"]);
  });

  it("never discloses options, api keys, transport functions or token contents", async () => {
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    // Emitted properties/accessors cannot reveal apiKey/fetchImpl/options.
    expect((bundle.store as unknown as Record<string, unknown>).options).toBeUndefined();
    expect(Object.keys(bundle.store)).not.toContain("options");
    expect(Object.getOwnPropertyNames(bundle.store)).not.toContain("options");
    expect(JSON.stringify(bundle.store)).not.toContain("apiKey");
    expect(JSON.stringify(bundle.store)).not.toContain("collection-scoped");
    expect(JSON.stringify(bundle.store)).not.toContain("fetchImpl");
    const token = bundle.transport;
    expect(token).toBeDefined();
    expect(Object.keys(token as object)).toEqual([]);
    expect(Object.getOwnPropertySymbols(token as object)).toEqual([]);
    expect(JSON.stringify(token)).toBe("{}");
    // Embedding options are also private.
    stubGlobalFetch(async () => new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] }), { headers: { "content-type": "application/json" } }));
    const emb = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
    expect((emb as unknown as Record<string, unknown>).options).toBeUndefined();
    expect(Object.keys(emb)).not.toContain("options");
    expect(Object.getOwnPropertyNames(emb)).not.toContain("options");
    expect(JSON.stringify(emb)).not.toContain("apiKey");
    expect(JSON.stringify(emb.transport)).toBe("{}");
    expect(Object.keys(emb.transport)).toEqual([]);
  });

  it("creates immutable explicit-membership jobs and persisted proposals whose identity covers epoch and intersection", async () => {
    const store = fakeStore();
    const membership = ["episode-2", "episode-1"].sort();
    const created = await createJob(store, { ownerHost: OWNER, membership, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, policyIntersectionId: INTERSECTION_ID, createdAt: NOW, privacyEpoch: 0 });
    expect(created.id).toBe(jobId(OWNER, membership, POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    expect(created.membership).toEqual(membership);
    expect(created.policyId).toBe(INTERSECTION_ID);
    expect(jobId(OWNER, membership, POLICY_HASH, EXTRACTOR, 2, INTERSECTION_ID, 0)).not.toBe(created.id);
    expect(jobId(OWNER, membership, POLICY_HASH, EXTRACTOR, 1, "other-intersection", 0)).not.toBe(created.id);
    await expect(createJob(store, { ownerHost: OWNER, membership, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, policyIntersectionId: INTERSECTION_ID, createdAt: NOW, privacyEpoch: 0 })).resolves.toMatchObject({ id: created.id });
    const w = worker("node-a");
    const claim = await claimLease(store, w, { jobId: created.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim).not.toBeNull();
    const baseWrite = { membership, content: { summary: "safe" }, policyHash: POLICY_HASH, policyEpoch: 1, privacyEpoch: 0, policyIntersectionId: INTERSECTION_ID, createdAt: NOW, maxClockSkewMs: 0 };
    const proposal = await writeProposal(store, claim!, baseWrite);
    expect(proposal.id).toBe(proposalIdFor(created.id, proposalHashFor({ ownerHost: OWNER, jobId: created.id, ownerId: "node-a", membership, content: { summary: "safe" }, policyHash: POLICY_HASH, policyEpoch: 1, fencingToken: claim?.fencingToken ?? 0, privacyEpoch: 0, policyIntersectionId: INTERSECTION_ID }), 1, claim?.fencingToken ?? 0));
    expect(proposal.manifestHash).toBe(manifestHash(membership));
    expect(proposal.membership).toEqual(membership);
    expect(proposal.content).toEqual({ summary: "safe" });
    expect(proposal.ownerId).toBe("node-a");
    expect(proposal.fencingToken).toBe(claim?.fencingToken);
    await expect(writeProposal(store, claim!, baseWrite)).resolves.toMatchObject({ id: proposal.id });
  });

  it("gates materialization authority on the caller fence: stale owner/token, transfers and mid-barrier release/steal", async () => {
    const holderG: { store?: ProductionCoordinationStore; claimB?: LeaseAuthority; released?: boolean; armed?: boolean } = {};
    const { store, backend: gateBackend } = fakeStoreWithBackend({ episodes: [episode()] }, { extra: async (ids) => { if (holderG.armed === true && holderG.released !== true && ids.some((id) => id === tombstoneId(OWNER, EPISODE_UUID))) { holderG.released = true; await releaseLease(holderG.store!, holderG.claimB!); } return undefined; } });
    holderG.store = store;
    const job = jobRecord(jobId(OWNER, [EPISODE_UUID], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: [EPISODE_UUID] });
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a");
    const claimA = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposal = await writeProposal(store, claimA!, { membership: [EPISODE_UUID], content: { summary: "safe summary" }, createdAt: NOW });
    const accepted = await acceptProposal(store, claimA!, { proposalId: proposal.id });
    expect(accepted).not.toBeNull();
    // The stale pre-accept authority (A) gets null even though the acceptance exists.
    await expect(readActiveAcceptance(store, claimA!)).resolves.toBeNull();
    // Transfer after conservative expiry: B (new authority) gets materialization authority.
    const wB = timedWorker(NOW_MS + 100000, "node-b");
    const claimB = await claimLease(store, wB.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    holderG.claimB = claimB!;
    expect(claimB).not.toBeNull();
    expect(claimB?.state).toBe("accepted");
    expect(claimB?.fencingToken ?? 0).toBe((claimA?.fencingToken ?? 0) + 1);
    await expect(readActiveAcceptance(store, claimB!)).resolves.toMatchObject({ proposalId: proposal.id, manifestHash: proposal.manifestHash, claimVersion: claimB?.version, job: { id: job.id }, proposal: { id: proposal.id } });
    holderG.armed = true;
    // Stale A authority after the transfer stays null.
    await expect(readActiveAcceptance(store, accepted!)).resolves.toBeNull();
    // Release/steal during the tombstone read gives null.
    await expect(readActiveAcceptance(store, claimB!)).resolves.toBeNull();
  });

  it("finite job deadline: claim/renew/steal fail closed at/after now+skew with zero lease mutation", async () => {
    const upserts: string[] = [];
    const store = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { if (points.some((point) => point.payload.record_type === "lease")) upserts.push(mode); } }).store;
    const expiredJob = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { expiresAt: iso(NOW_MS + 300000) });
    await createJob(store, jobInputFrom(expiredJob));
    const claim = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 300000), { jobId: expiredJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claim).toBeNull();
    expect(upserts).toEqual([]);
    // Live at claim; the deadline crosses now+skew before the renewal.
    const liveJob = jobRecord(jobId(OWNER, ["episode-2"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-2"], expiresAt: iso(NOW_MS + 310000) });
    await createJob(store, jobInputFrom(liveJob));
    const wA = timedWorker(NOW_MS, "node-a", 30000, 300000);
    const initial = await claimLease(store, wA.worker, { jobId: liveJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(initial).not.toBeNull();
    wA.set(NOW_MS + 10000);
    const renewed = await renewLease(store, initial!);
    expect(renewed).toBeNull();
    expect(upserts.filter((mode) => mode === "update_only")).toEqual([]);
    // Same-owner reacquire and contender steal fail closed on the expired job.
    const same = await claimLease(store, workerAt("node-a", NOW_MS + 330001, 30000, 300000), { jobId: liveJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(same).toBeNull();
    const steal = await claimLease(store, workerAt("node-b", NOW_MS + 330001, 30000, 300000), { jobId: liveJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(steal).toBeNull();
    expect(upserts.filter((mode) => mode === "update_only")).toEqual([]);
  });

  it("deadline-live job sooner than the lease TTL caps claim and renewal expiry exactly at the job deadline", async () => {
    const store = fakeStore();
    const deadlineJob = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { expiresAt: iso(NOW_MS + 10000) });
    await createJob(store, jobInputFrom(deadlineJob));
    const wA = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, wA.worker, { jobId: deadlineJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim?.expiresAt).toBe(iso(NOW_MS + 10000));
    wA.set(NOW_MS + 1000);
    const renewed = await renewLease(store, claim!);
    expect(renewed?.expiresAt).toBe(iso(NOW_MS + 10000));
  });

  it("expired job rejects writeProposal before insert and accept before CAS; proposal deadline mismatch and materialization fail closed", async () => {
    const store = fakeStore();
    const expiredJob = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { expiresAt: iso(NOW_MS) });
    await createJob(store, jobInputFrom(expiredJob));
    const expiredClaim = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: expiredJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(expiredClaim).toBeNull();
    // Live job: proposal inherits the job deadline; the acceptance flow works.
    const liveJob = jobRecord(jobId(OWNER, ["episode-2"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-2"], expiresAt: iso(NOW_MS + 60000) });
    await createJob(store, jobInputFrom(liveJob));
    const wA = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, wA.worker, { jobId: liveJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim).not.toBeNull();
    const proposal = await writeProposal(store, claim!, { membership: ["episode-2"], content: { summary: "safe" }, createdAt: NOW });
    expect(proposal.expiresAt).toBe(iso(NOW_MS + 60000));
    expect(proposal.ownerId).toBe("node-a");
    // A proposal id that was never written can never be accepted (fail closed).
    await expect(acceptProposal(store, claim!, { proposalId: "00000000-0000-4000-8000-000000000099" })).rejects.toThrow(/bound|proposal/i);
    const accepted = await acceptProposal(store, claim!, { proposalId: proposal.id });
    expect(accepted).not.toBeNull();
    // After the job deadline crosses, materialization returns null.
    wA.set(NOW_MS + 60001);
    await expect(readActiveAcceptance(store, accepted!)).resolves.toBeNull();
  });

  it("job deadline crossing during slow reads: accept performs no CAS; materialization returns null", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { expiresAt: iso(NOW_MS + 100) });
    await createJob(store, jobInputFrom(job));
    const box = { value: NOW_MS };
    const w = mintRootWorker(OWNER, "node-a", () => box.value);
    const claim = await claimLease(store, w, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim).not.toBeNull();
    const proposal = await writeProposal(store, claim!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW });
    expect(proposal.expiresAt).toBe(iso(NOW_MS + 100));
    const gateHolder: { release?: () => void; started?: () => boolean } = {};
    let gateProposalIdD = "";
    const { store: deadlineStore } = fakeStoreWithBackend({}, { extra: async (ids) => { if (gateProposalIdD !== "" && ids.includes(gateProposalIdD)) { await new Promise<void>((resolve) => { gateHolder.started = () => true; gateHolder.release = resolve; }); } return undefined; } });
    // Same job/claim on the gated store.
    const jobD = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { expiresAt: iso(NOW_MS + 100) });
    await createJob(deadlineStore, jobInputFrom(jobD));
    const wD = mintRootWorker(OWNER, "node-a", () => box.value);
    const claimD = await claimLease(deadlineStore, wD, { jobId: jobD.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposalD = await writeProposal(deadlineStore, claimD!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW });
    gateProposalIdD = proposalD.id;
    const pending = acceptProposal(deadlineStore, claimD!, { proposalId: proposalD.id });
    await vi.waitFor(() => expect(gateHolder.started?.()).toBe(true));
    box.value = NOW_MS + 101;
    gateHolder.release?.();
    await expect(pending).rejects.toThrow(/expired/i);
    const after = await readLease(deadlineStore, jobD.id);
    expect(after?.state).toBe("leased");
    expect(after?.acceptedProposalId).toBeNull();
    await expect(readActiveAcceptance(deadlineStore, claimD!)).resolves.toBeNull();
  });

  it("real config defaults (skew 300000, lease 30000): fresh claim renews, accepts and materializes", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const w = timedWorker(NOW_MS, "node-a", 30000, 300000);
    const claim = await claimLease(store, w.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claim?.expiresAt).toBe(iso(NOW_MS + 30000));
    w.set(NOW_MS + 10000);
    const renewed = await renewLease(store, claim!);
    expect(renewed).not.toBeNull();
    expect(renewed?.expiresAt).toBe(iso(NOW_MS + 40000));
    const proposal = await writeProposal(store, renewed!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW });
    const accepted = await acceptProposal(store, renewed!, { proposalId: proposal.id });
    expect(accepted).not.toBeNull();
    const active = await readActiveAcceptance(store, accepted!);
    expect(active?.proposalId).toBe(proposal.id);
    expect(active?.manifestHash).toBe(proposal.manifestHash);
    expect(Object.isFrozen(active?.job)).toBe(true);
    expect(Object.isFrozen(active?.job.membership)).toBe(true);
    expect(Object.isFrozen(active?.proposal)).toBe(true);
    expect(Object.isFrozen(active?.proposal.content)).toBe(true);
    // Operations AT the exact expiresAt cannot authorize.
    w.set(NOW_MS + 40000);
    await expect(readActiveAcceptance(store, accepted!)).resolves.toBeNull();
    const claim2 = await claimLease(store, workerAt("node-a", NOW_MS + 40000, 30000, 300000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claim2).toBeNull();
  });

  it("real config defaults: production-store renewal CAS uses the exact owner floor", async () => {
    const backend = restPoints();
    const { store } = productionStore(backend);
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const wA = timedWorker(NOW_MS, "node-a", 30000, 300000);
    const claim = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claim?.expiresAt).toBe(iso(NOW_MS + 30000));
    wA.set(NOW_MS + 10000);
    const renewed = await renewLease(store, claim!);
    expect(renewed).not.toBeNull();
    expect(renewed?.expiresAt).toBe(iso(NOW_MS + 40000));
  });

  it("real config defaults: same-owner local expiry inside the skew grace yields null and no CAS; conservative steal/reacquire still bound by the grace", async () => {
    const upserts: string[] = [];
    const store = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { if (points.some((point) => point.payload.record_type === "lease")) upserts.push(mode); } }).store;
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const wA = timedWorker(NOW_MS, "node-a", 30000, 300000);
    const claim = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claim).not.toBeNull();
    wA.set(NOW_MS + 31000);
    const writesBefore = upserts.length;
    const sameOwner = await claimLease(store, wA.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(sameOwner).toBeNull();
    const renewed = await renewLease(store, claim!);
    expect(renewed).toBeNull();
    const released = await releaseLease(store, claim!);
    expect(released).toBe(false);
    expect(upserts.length).toBe(writesBefore);
    const steal = await claimLease(store, workerAt("node-b", NOW_MS + 31000, 30000, 300000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(steal).toBeNull();
    // After conservative expiry (expiresAt + skew): same-owner fenced reacquire +1 works.
    const wA2 = timedWorker(NOW_MS + 330001, "node-a");
    const reacquired = await claimLease(store, wA2.worker, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(reacquired).not.toBeNull();
    expect(reacquired?.fencingToken).toBe(2);
    // A contender steal also works after conservative expiry.
    const stolen = await claimLease(store, workerAt("node-b", NOW_MS + 660002, 30000, 300000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(stolen?.ownerId).toBe("node-b");
  });

  it("converges same-target forget across privacy AND processing-policy changes under the fixed tombstone identity", async () => {
    const store = fakeStore({ episodes: [episode()] });
    const target = EPISODE_UUID;
    const first = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: target, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    const later = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: target, targetKind: "episode", createdAt: NOW, privacyEpoch: 5, processingPolicyId: "policy-2" });
    expect(later[0]?.id).toBe(first[0]?.id);
    const all = await readTombstones(store, [target]);
    expect(all.filter((entry) => entry.targetId === target)).toHaveLength(1);
    const epoch1Policy1 = { recordType: "tombstone" as const, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1", expiresAt: null, id: tombstoneId(OWNER, target), scope: "occurrence" as const, targetId: target, contentHash: "pending" };
    const epoch9Policy9 = { ...epoch1Policy1, privacyEpoch: 9, processingPolicyId: "policy-9" };
    expect(canonicalRecordHash(epoch1Policy1)).toBe(canonicalRecordHash(epoch9Policy9));
  });

  it("rejects transitive structural minting: same-owner different-endpoint bundles, forged embedding and mixed bundles", async () => {
    const bundleA = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const bundleB = createQdrantSafeBundle({ options: qdrantOptions("pi", "http://qdrant-other"), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const storeA = bundleA.store;
    const storeB = bundleB.store;
    const qdrantA = bindQdrantDestination(bundleA.qdrant, qdrantDestination);
    const qdrantB = bindQdrantDestination(bundleB.qdrant, qdrantDestination);
    expect(() => bindIngestRuntime({ store: storeA, qdrant: qdrantB, embedding: boundRuntimeEmbedding() })).toThrow(/identity mismatch|exact writer transport/i);
    expect(() => bindIngestRuntime({ store: storeB, qdrant: qdrantA, embedding: boundRuntimeEmbedding() })).toThrow(/identity mismatch|exact writer transport/i);
    const mismatchedEmbedding = boundRuntimeEmbedding(embeddingDestination, "other-hash", 2);
    expect(() => bindIngestRuntime({ store: storeA, qdrant: qdrantA, embedding: mismatchedEmbedding })).toThrow(/coordination identity mismatch/i);
    expect(() => bindIngestRuntime({ store: storeA, qdrant: qdrantB, embedding: boundRuntimeEmbedding() })).toThrow(/exact writer transport/i);
    expect(() => new ValidatedEmbeddingDocumentClient("http://embed/v1", new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100, fetchImpl: async () => new Response(JSON.stringify({ data: [{ embedding: [] }] }), { headers: { "content-type": "application/json" } }) }), Symbol("forged"))).toThrow(/issuer/i);
    expect(() => new BoundEmbeddingDestination({ endpoint: "http://embed/v1", destination: embeddingDestination, coordination: { policyHash: POLICY_HASH, policyEpoch: 1 }, embed: async () => [] }, Symbol("forged"))).toThrow(/issuer/i);
    const forgedEmbedding = Object.create(BoundEmbeddingDestination.prototype) as BoundEmbeddingDestination;
    expect(BoundEmbeddingDestination.isValid(forgedEmbedding)).toBe(false);
    expect(() => { (ValidatedEmbeddingDocumentClient as unknown as Record<string, unknown>).isValid = () => true; }).toThrow();
    expect(() => { (BoundEmbeddingDestination.prototype as unknown as Record<string, unknown>).hack = 1; }).toThrow();
    expect(ProductionCoordinationStore.isValid({ ownerHost: "pi", readControl: async () => emptyControl() } as never)).toBe(false);
    expect(() => bindIngestRuntime({ store: { ownerHost: "pi", readControl: async () => emptyControl() } as never, qdrant: qdrantA, embedding: boundRuntimeEmbedding() })).toThrow(/production store/i);
  });

  it("discovers a late episode inserted behind the saved cursor through bounded overlap", async () => {
    const store = fakeStore();
    const idB = "00000000-0000-5000-8000-000000000002";
    const idC = "00000000-0000-5000-8000-000000000003";
    const idD = "00000000-0000-5000-8000-000000000004";
    const late = "00000000-0000-5000-8000-000000000000";
    const b = episode({ id: idB, sourceEntryId: "entry-b" });
    const c = episode({ id: idC, sourceEntryId: "entry-c" });
    const d = episode({ id: idD, sourceEntryId: "entry-d" });
    const lateEpisode = episode({ id: late, sourceEntryId: "entry-late" });
    const coverageAuthority = await acceptedCoverageAuthority(store, [b.id, c.id, d.id]);
    for (const entry of [b, c, d]) await markCoverage(store, coverageAuthority, { ownerHost: OWNER, episodeId: entry.id, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID });
    // First sweep advances PAST the late id (it was offline and not scanned).
    const first = await reconcileCoverage({ store, listEpisodes: async () => ({ episodes: [b, c], nextOffset: idC }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 });
    expect(first.nextOffset).toBe(idC);
    expect(first.missing).toEqual([]);
    // The late episode is inserted BEHIND the saved cursor; an overlapping
    // resumed page (starting at/before the cursor, strictly sorted) discovers it.
    const pages: Array<{ episodes: EpisodeRecord[]; nextOffset?: string }> = [
      { episodes: [lateEpisode, b, d], nextOffset: idD },
      { episodes: [] },
    ];
    const resumed = await reconcileCoverage({ store, offset: idC, listEpisodes: async () => pages.shift() ?? { episodes: [] }, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 });
    expect(resumed.missing.map((entry) => entry.id)).toEqual([late]);
    expect(resumed.nextOffset).toBe(idD);
  });

  it("a late episode outside the overlap window is discovered by the next normal periodic cycle from undefined", async () => {
    const store = fakeStore();
    const idB = "00000000-0000-5000-8000-000000000002";
    const idC = "00000000-0000-5000-8000-000000000003";
    const idD = "00000000-0000-5000-8000-000000000004";
    const b = episode({ id: idB, sourceEntryId: "entry-b" });
    const c = episode({ id: idC, sourceEntryId: "entry-c" });
    const d = episode({ id: idD, sourceEntryId: "entry-d" });
    const coverageAuthority = await acceptedCoverageAuthority(store, [b.id, c.id, d.id]);
    for (const entry of [b, c, d]) await markCoverage(store, coverageAuthority, { ownerHost: OWNER, episodeId: entry.id, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID });
    const late = "00000000-0000-5000-8000-000000000099";
    const lateEpisode = episode({ id: late, sourceEntryId: "entry-late" });
    // Cycle 1 pages do NOT contain the late episode (it was inserted after the
    // sweep passed its ID range, outside any overlap). Each normal periodic
    // call is ONE slice, resumed by the returned cursor.
    let cycle = 1;
    const scanner = vi.fn(async (offset?: string, limit?: number) => {
      if (cycle === 1) return offset === undefined ? { episodes: [b, c], nextOffset: idC } : offset === idC ? { episodes: [d], nextOffset: idD } : { episodes: [] };
      return offset === undefined ? { episodes: [b, c, lateEpisode], nextOffset: late } : { episodes: [] };
    });
    const base = { store, listEpisodes: scanner, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 };
    const sweep1a = await reconcileCoverage(base);
    expect(sweep1a.nextOffset).toBe(idC);
    const sweep1b = await reconcileCoverage({ ...base, offset: sweep1a.nextOffset });
    expect(sweep1b.nextOffset).toBe(idD);
    const sweep1c = await reconcileCoverage({ ...base, offset: sweep1b.nextOffset });
    expect(sweep1c.truncated).toBe(false);
    expect(sweep1c.missing).toEqual([]);
    // The NEXT normal periodic invocation with offset undefined begins a fresh
    // full cycle and discovers the late episode — no manual/operator API.
    cycle = 2;
    const sweep2 = await reconcileCoverage(base);
    expect(sweep2.missing.map((entry) => entry.id)).toEqual([late]);
  });

  it("reconcileCoverage passes the external offset to the scanner and returns an advancing cursor", async () => {
    const store = fakeStore();
    const idB = "00000000-0000-5000-8000-000000000002";
    const idD = "00000000-0000-5000-8000-000000000004";
    const b = episode({ id: idB });
    const d = episode({ id: idD });
    const offsets: Array<string | undefined> = [];
    const result = await reconcileCoverage({ store, offset: idB, listEpisodes: async (offset?: string, limit?: number) => { offsets.push(offset); return { episodes: [b, d], nextOffset: idD }; }, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 });
    expect(offsets).toEqual([idB]);
    expect(result.nextOffset).toBe(idD);
    expect(result.truncated).toBe(true);
  });

  it("deduplicates overlap IDs within one call, never reports them twice, and never regresses the cursor on overlap-only maxMissing", async () => {
    const store = fakeStore();
    const idA = "00000000-0000-5000-8000-000000000001";
    const idB = "00000000-0000-5000-8000-000000000002";
    const idC = "00000000-0000-5000-8000-000000000003";
    const a = episode({ id: idA, sourceEntryId: "entry-a" });
    const b = episode({ id: idB, sourceEntryId: "entry-b" });
    const c = episode({ id: idC, sourceEntryId: "entry-c" });
    const coverageAuthority = await acceptedCoverageAuthority(store, [idB]);
    await markCoverage(store, coverageAuthority, { ownerHost: OWNER, episodeId: idB, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID });
    // Same call, two slices: the second page re-serves the first page's last id
    // (overlap); the uncovered a appears in BOTH pages but only once in missing.
    const slices: Array<{ episodes: EpisodeRecord[]; nextOffset?: string }> = [
      { episodes: [a, b], nextOffset: idB },
      { episodes: [a, c], nextOffset: idC },
      { episodes: [] },
    ];
    const result = await findMissingEpisodes({ store, listEpisodes: async () => slices.shift() ?? { episodes: [] }, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 3, maxMissing: 1024 });
    expect(result.missing.map((entry) => entry.id)).toEqual([idA, idC]);
    expect(result.scanned).toBe(3);
    // Overlap-only maxMissing: cap=1 hit on the overlap member a -> the resume
    // cursor stays the prior forward cursor (input offset idB), never idA.
    const overlapOnly = await findMissingEpisodes({ store, offset: idB, listEpisodes: async () => ({ episodes: [a, b, c], nextOffset: idC }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1, maxMissing: 1 });
    expect(overlapOnly.missing.map((entry) => entry.id)).toEqual([idA]);
    expect(overlapOnly.nextOffset).toBe(idB);
    expect(overlapOnly.truncated).toBe(true);
    // After forward progress the resume is exactly the last processed forward ID.
    const forwardCap = await findMissingEpisodes({ store, offset: idB, listEpisodes: async () => ({ episodes: [b, c], nextOffset: idC }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1, maxMissing: 1 });
    expect(forwardCap.missing.map((entry) => entry.id)).toEqual([idC]);
    expect(forwardCap.nextOffset).toBe(idC);
  });

  it("rejects repeated or nonadvancing next cursors and foreign-episode slices before coverage reads", async () => {
    const store = fakeStore();
    const idA = "00000000-0000-5000-8000-000000000001";
    const idB = "00000000-0000-5000-8000-000000000002";
    const a = episode({ id: idA });
    const b = episode({ id: idB });
    // nextOffset == input cursor (repeated): rejected.
    await expect(findMissingEpisodes({ store, offset: idB, listEpisodes: async () => ({ episodes: [a, b], nextOffset: idB }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).rejects.toThrow(/advance|cursor/i);
    // nextOffset < input cursor (regressing): rejected (cursor must be the last id AND advance).
    await expect(findMissingEpisodes({ store, offset: idB, listEpisodes: async () => ({ episodes: [a, b], nextOffset: idA }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).rejects.toThrow(/advance|cursor/i);
    // A foreign-owner episode is rejected before any coverage read.
    const foreign = episode({ id: "00000000-0000-5000-8000-000000000007", ownerHost: "prime", host: "prime" });
    const { store: foreignStore, backend: foreignBackend } = fakeStoreWithBackend();
    await expect(findMissingEpisodes({ store: foreignStore, listEpisodes: async () => ({ episodes: [foreign] }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).rejects.toThrow(/foreign|owner/i);
    // No coverage point was ever written/read for the foreign episode: the backend is untouched.
    expect([...foreignBackend.points.values()].filter((point) => point.payload.record_type === "coverage")).toEqual([]);
  });

  it("owner-domain gates reject cross-owner mutations with zero writes for markCoverage/createJob/writeProposal", async () => {
    const upserts: string[] = [];
    const store = fakeStoreWithBackend({}, { onUpsert: (_points, mode) => { upserts.push(mode); } }).store;
    // markCoverage: store owner pi vs input owner prime -> rejected before mutation.
    const foreignCoverageEpisode = "00000000-0000-5000-8000-000000000001";
    const coverageAuthority = await acceptedCoverageAuthority(store, [foreignCoverageEpisode]);
    upserts.length = 0; // Ignore genuine authority setup; the operation under test must not mutate.
    await expect(markCoverage(store, coverageAuthority, { ownerHost: "prime", episodeId: foreignCoverageEpisode, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID })).rejects.toThrow(/owner/i);
    expect(upserts).toEqual([]);
    // createJob: store owner pi vs input owner prime -> rejected before mutation.
    await expect(createJob(store, { ownerHost: "prime", membership: ["episode-1"], policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, policyIntersectionId: INTERSECTION_ID, createdAt: NOW, privacyEpoch: 0 })).rejects.toThrow(/owner/i);
    expect(upserts).toEqual([]);
    // writeProposal: a forged/structural authority is rejected before any mutation.
    const forgedAuthority = Object.create(LeaseAuthority.prototype) as LeaseAuthority;
    await expect(writeProposal(store, forgedAuthority, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/authority/i);
    expect(upserts).toEqual([]);
    // writeProposal: an authority minted on a DIFFERENT store is rejected before mutation.
    const otherStore = fakeStore();
    const otherJob = jobRecord(jobId(OWNER, ["episode-2"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-2"] });
    await createJob(otherStore, jobInputFrom(otherJob));
    const foreignAuthority = await claimLease(otherStore, workerAt("node-a", NOW_MS, 30000, 0), { jobId: otherJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(foreignAuthority).not.toBeNull();
    await expect(writeProposal(store, foreignAuthority!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/store/i);
    expect(upserts).toEqual([]);
    // writeProposal: a prime-host worker cannot mint an authority on a pi store.
    const primeAuthority = await claimLease(store, primeWorker(), { jobId: otherJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(primeAuthority).toBeNull();
  });

  it("rejects empty slices with a resume cursor and invalid scan inputs before the scanner", async () => {
    const store = fakeStore();
    await expect(findMissingEpisodes({ store, listEpisodes: async () => ({ episodes: [], nextOffset: "leap" }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).rejects.toThrow(/empty slice/i);
    await expect(findMissingEpisodes({ store, listEpisodes: async () => ({ episodes: [] }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).resolves.toMatchObject({ missing: [], truncated: false });
    await expect(findMissingEpisodes({ store, listEpisodes: async () => ({ episodes: [] }), extractorRevision: "", policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).rejects.toThrow(/extractor/i);
    await expect(findMissingEpisodes({ store, listEpisodes: async () => ({ episodes: [] }), extractorRevision: EXTRACTOR, policyEpoch: -1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })).rejects.toThrow(/epoch/i);
    await expect(findMissingEpisodes({ store, listEpisodes: async () => ({ episodes: [] }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, offset: "x".repeat(600), maxSlices: 1 })).rejects.toThrow(/cursor/i);
  });

  it("marks deterministic coverage and reconciles within turn bounds with ID-based truth", async () => {
    const store = fakeStore();
    const coveredEpisode = episode({ id: episodeId(OWNER, "session-1", "message-1") });
    const lateEpisode = episode({ id: episodeId(OWNER, "session-1", "message-2"), sourceEntryId: "entry-2" });
    const otherRevision = episode({ id: episodeId(OWNER, "session-2", "message-1"), sourceEntryId: "entry-3" });
    const sortedForScan = (entries: EpisodeRecord[]): Array<{ episodes: EpisodeRecord[]; nextOffset?: string }> => {
      const sorted = [...entries].sort((a, b) => (a.id < b.id ? -1 : 1));
      return [{ episodes: sorted, nextOffset: sorted[sorted.length - 1]?.id }];
    };
    const coverageAuthority = await acceptedCoverageAuthority(store, [coveredEpisode.id]);
    await markCoverage(store, coverageAuthority, { ownerHost: OWNER, episodeId: coveredEpisode.id, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID });
    expect(coverageId({ ownerHost: OWNER, episodeId: coveredEpisode.id, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 })).toBe(coverageId({ ownerHost: OWNER, episodeId: coveredEpisode.id, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 }));
    expect(coverageId({ ownerHost: OWNER, episodeId: coveredEpisode.id, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 2, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 })).not.toBe(coverageId({ ownerHost: OWNER, episodeId: coveredEpisode.id, extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 }));
    const slices: Array<{ episodes: EpisodeRecord[]; nextOffset?: string }> = [
      { episodes: [coveredEpisode, lateEpisode], nextOffset: lateEpisode.id },
      { episodes: [otherRevision], nextOffset: otherRevision.id },
      { episodes: [] },
    ];
    const byId = (a: EpisodeRecord, b: EpisodeRecord): number => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
    const orderedSlices = (() => {
      const all = [coveredEpisode, lateEpisode, otherRevision].sort(byId);
      return [{ episodes: all.slice(0, 2), nextOffset: all[1]!.id }, { episodes: all.slice(2), nextOffset: all[2]!.id }, { episodes: [] }];
    })();
    const listEpisodes = vi.fn(async (offset?: string, limit?: number) => { const slice = orderedSlices.shift() ?? { episodes: [] }; return { episodes: slice.episodes, ...(slice.nextOffset === undefined ? {} : { nextOffset: slice.nextOffset }) }; });
    const result = await findMissingEpisodes({ store, listEpisodes, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 4 });
    expect(result.missing.map((entry) => entry.id).sort()).toEqual([lateEpisode.id, otherRevision.id].sort());
    expect(result.scanned).toBe(3);
    expect(result.truncated).toBe(false);
    // Turn-bounded: one slice per operator sweep, externally resumable via the ID cursor.
    const resume: Array<{ episodes: EpisodeRecord[]; nextOffset?: string }> = [
      { episodes: [coveredEpisode, lateEpisode].sort(byId), nextOffset: [coveredEpisode, lateEpisode].sort(byId)[1]!.id },
      { episodes: [otherRevision] },
    ];
    const firstSweep = await reconcileCoverage({ store, listEpisodes: async (offset?: string, limit?: number) => { const slice = resume.shift() ?? { episodes: [] }; return { episodes: slice.episodes, ...(slice.nextOffset === undefined ? {} : { nextOffset: slice.nextOffset }) }; }, extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 });
    expect(firstSweep.missing.map((entry) => entry.id)).toEqual([lateEpisode.id]);
    expect(firstSweep.nextOffset).toBe(lateEpisode.id);
    expect(firstSweep.truncated).toBe(true);
    // maxSlices and maxMissing bounds stop the scan without unbounded accumulation.
    const many = Array.from({ length: 10 }, (_, index) => episode({ id: episodeId(OWNER, `scan-${index}`, "message-1"), sourceEntryId: `entry-${index}` })).sort((a, b) => (a.id < b.id ? -1 : 1));
    const bounded = await findMissingEpisodes({ store, listEpisodes: async (offset?: string, limit?: number) => ({ episodes: many, nextOffset: many[many.length - 1]!.id }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 2, maxMissing: 5 });
    expect(bounded.scanned).toBe(5);
    expect(bounded.missing.length).toBe(5);
    // Exact per-item resume: nextOffset is the last processed episode id; maxMissing never exceeded.
    expect(bounded.truncated).toBe(true);
    expect(bounded.nextOffset).toBe(bounded.missing[bounded.missing.length - 1]!.id);
    // Forged coverage records (wrong derived ID) are rejected by the parser and cannot suppress missing work.
    const forged = { ...coverageRecord(EXTRACTOR, coveredEpisode.id), id: "forged-coverage-id", contentHash: "pending" };
    expect(() => canonicalRecordHash(forged)).toThrow(/coverage|id/i);
  });

  it("validates store owner, clocks, policy and duplicates before the first tombstone read", async () => {
    const store = fakeStore({ episodes: [episode()] });
    const target = EPISODE_UUID;
    // A prime-owner tombstone on the pi store is rejected before the first read.
    await expect(createTombstone(store, { ownerHost: "prime", scope: "occurrence", targetId: target, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" })).rejects.toThrow(/owner/i);
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: target, targetKind: "episode", createdAt: "not-a-date", privacyEpoch: 1, processingPolicyId: "policy-1" })).rejects.toThrow(/createdAt/i);
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: target, targetKind: "episode", createdAt: NOW, privacyEpoch: -1, processingPolicyId: "policy-1" })).rejects.toThrow(/privacy/i);
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: target, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "" })).rejects.toThrow(/policy/i);
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: target, targetKind: "episode", provenanceIds: [target, target], createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" })).rejects.toThrow(/provenance/i);
    expect(store.readEpisode).toBeTypeOf("function");
  });

  it("domain-separates canonical scope targets and preserves the exact tombstone formula", async () => {
    const stateTarget = stateKey({ host: "pi", scope: "project", projectId: "p", category: "fact", subject: "same", predicate: "same" });
    const contentTarget = contentId("policy", stateTarget, "same");
    const occurrenceTarget = observationId(1, contentTarget, EPISODE_UUID, "session:7");
    const ids = [occurrenceTarget, contentTarget, stateTarget].map((target) => tombstoneId("pi", target));
    expect(new Set(ids).size).toBe(3);
    expect(tombstoneId("pi", contentTarget)).toBe(tombstoneId("pi", contentTarget));
    expect(tombstoneId("prime", contentTarget)).not.toBe(tombstoneId("pi", contentTarget));
    expect(occurrenceTarget).toMatch(/^occurrence:/);
    expect(contentTarget).toMatch(/^content:/);
    expect(stateTarget).toMatch(/^state:/);
    await expect(createTombstone(fakeStore(), { ownerHost: "pi", scope: "content", targetId: occurrenceTarget, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1" })).rejects.toThrow(/scope|target/i);
  });

  it("creates immutable tombstones, verifies episode targets by store lookup, and converges source tombstones", async () => {
    const staleTarget = episodeId(OWNER, "session-9", "message-9");
    const vector = Array.from({ length: 1024 }, (_, index) => (index % 5) / 10);
    const { store } = productionVisibilityStore([episode({ vector }), episode({ id: staleTarget, sourceEntryId: "entry-stale", vector })]);
    const stateTarget = stateKey({ host: "pi", scope: "project", projectId: "p", category: "fact", subject: "same", predicate: "same" });
    const sourceEpisode = EPISODE_UUID;
    const created = await createTombstone(store, { ownerHost: OWNER, scope: "state", targetId: stateTarget, provenanceIds: [sourceEpisode], targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    expect(created.length).toBe(2);
    expect(created.map((entry) => entry.scope)).toEqual(["state", "occurrence"]);
    expect(created[0]?.id).toBe(tombstoneId(OWNER, stateTarget));
    expect(created[1]?.id).toBe(tombstoneId(OWNER, sourceEpisode));
    // The same source forgotten under a second target converges (no provenanceId-dependent collision).
    const contentTarget = contentId("policy", stateTarget, "other");
    const second = await createTombstone(store, { ownerHost: OWNER, scope: "content", targetId: contentTarget, provenanceIds: [sourceEpisode], targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    expect(second.length).toBe(2);
    expect(second[1]?.id).toBe(tombstoneId(OWNER, sourceEpisode));
    const reread = await readTombstones(store, [stateTarget, sourceEpisode, contentTarget, "not-targeted"]);
    expect(reread.map((entry) => entry.targetId).sort()).toEqual([sourceEpisode, contentTarget, stateTarget]);
    // Bare-UUID occurrence targets require the explicit episode selector and an existing episode.
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: episodeId(OWNER, "ghost", "message"), createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" })).rejects.toThrow(/episode|selector/i);
    await expect(createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: sourceEpisode, createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" })).rejects.toThrow(/episode|selector/i);
    const verified = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: sourceEpisode, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    expect(verified[0]?.id).toBe(tombstoneId(OWNER, sourceEpisode));
    // Duplicate reinsertion converges idempotently and stays logically invisible.
    const firstInsert = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: staleTarget, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    const secondInsert = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: staleTarget, targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    expect(secondInsert[0]?.id).toBe(firstInsert[0]?.id);
    // A SECOND forget of the same target at a LATER privacy epoch converges
    // idempotently under the fixed H(owner,"tombstone",target) identity: the
    // exact-read-existing path skips the insert instead of content-hash colliding.
    const laterEpoch = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: staleTarget, targetKind: "episode", createdAt: NOW, privacyEpoch: 3, processingPolicyId: "policy-1" });
    expect(laterEpoch[0]?.id).toBe(firstInsert[0]?.id);
    const allForStale = await readTombstones(store, [staleTarget]);
    expect(allForStale.filter((entry) => entry.targetId === staleTarget)).toHaveLength(1);
    const staleRecord = { ...episode({ id: staleTarget, privacyEpoch: 1, vector }), contentHash: "pending" };
    const staleEpisode = { ...staleRecord, contentHash: canonicalRecordHash(staleRecord) };
    expect(await isVisibleAfterTombstoneCheck(store, staleEpisode)).toBe(false);
  });

  it("hides fail-closed by occurrence epoch, content/state recurrence, and provenance closure with bounded manifest resolution", async () => {
    const vector = Array.from({ length: 1024 }, (_, index) => (index % 5) / 10);
    const { store } = productionVisibilityStore([episode({ vector })]);
    const stateTarget = stateKey({ host: "pi", scope: "project", projectId: "p", category: "fact", subject: "same", predicate: "same" });
    const sourceEpisode = EPISODE_UUID;
    await createTombstone(store, { ownerHost: OWNER, scope: "state", targetId: stateTarget, provenanceIds: [sourceEpisode], targetKind: "episode", createdAt: NOW, privacyEpoch: 1, processingPolicyId: "policy-1" });
    const tombstones = await readTombstones(store, [stateTarget, sourceEpisode]);
    const futureStateRecord = { recordType: "curated_memory" as const, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 4, processingPolicyId: "policy-1", expiresAt: null, contentHash: "pending", id: observationId(1, contentId(POLICY_HASH, stateTarget, "same"), sourceEpisode, "session:1"), contentId: contentId(POLICY_HASH, stateTarget, "same"), observationId: observationId(1, contentId(POLICY_HASH, stateTarget, "same"), sourceEpisode, "session:1"), eventAt: NOW, effectiveAt: NOW, effectiveOrder: "session:1", sourceEpisodeIds: [sourceEpisode], stateKey: stateTarget, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 };
    expect(await isVisibleAfterTombstoneCheck(store, { ...futureStateRecord, contentHash: canonicalRecordHash(futureStateRecord) })).toBe(false);
    const oldEpochEpisode = { ...episode({ privacyEpoch: 0 }), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...oldEpochEpisode, contentHash: canonicalRecordHash(oldEpochEpisode) })).toBe(false);
    const freshEpisode = { ...episode({ privacyEpoch: 2 }), contentHash: "pending" };
    // Occurrence tombstones are PERMANENT: a later-epoch reinsertion of the exact target never leaks.
    expect(await isVisibleAfterTombstoneCheck(store, { ...freshEpisode, contentHash: canonicalRecordHash(freshEpisode) })).toBe(false);
    // Manifest-bearing records are invisible fail-closed until Task 10 installs a
    // verifiable content-addressed persisted resolver; a caller-supplied
    // structural resolver is never visibility authority.
    const summary = { recordType: "raptor_summary" as const, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, contentHash: "pending", id: "summary-1", generationId: "g-1", clusterId: "c-1", membershipHash: manifestHash([sourceEpisode]), level: 1, manifestHash: manifestHash([sourceEpisode]), summary: "s", modelId: "m", embeddingDimension: 1024, promptRevision: "p", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: NOW, temporalTo: NOW, coveredProjects: ["project-1"], algorithmParameters: {}, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
    const summaryRecord = { ...summary, contentHash: canonicalRecordHash(summary) };
    expect(await isVisibleAfterTombstoneCheck(store, summaryRecord)).toBe(false);
    // Regression: a lying structural resolver (clean leaf for a hash covering a
    // tombstoned episode) cannot make a manifest-bearing record visible — the
    // resolver authority is removed entirely.
    const cleanStore = productionVisibilityStore([episode({ vector }), episode({ id: episodeId(OWNER, "session-clean", "message-1"), sourceEntryId: "entry-clean", vector })]).store;
    const cleanEpisodeId = episodeId(OWNER, "session-clean", "message-1");
    const cleanSummary = { ...summaryRecord, manifestHash: manifestHash([cleanEpisodeId]), membershipHash: manifestHash([cleanEpisodeId]), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(cleanStore, { ...cleanSummary, contentHash: canonicalRecordHash(cleanSummary as never) })).toBe(false);
    // manifestHash + memberIds mismatch also stays invisible (manifest fails closed).
    const mismatched = { ...summaryRecord, memberIds: [sourceEpisode], manifestHash: manifestHash([cleanEpisodeId]), membershipHash: manifestHash([sourceEpisode]), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(cleanStore, { ...mismatched, contentHash: canonicalRecordHash(mismatched as never) })).toBe(false);
    // A RAPTOR record with ONLY direct memberIds (no manifest) is episode
    // provenance when every member is an exact persisted episode UUID.
    const directSummary = { ...summaryRecord, memberIds: [sourceEpisode, cleanEpisodeId], membershipHash: manifestHash([sourceEpisode, cleanEpisodeId]), contentHash: "pending" };
    delete (directSummary as Record<string, unknown>).manifestHash;
    expect(await isVisibleAfterTombstoneCheck(cleanStore, { ...directSummary, contentHash: canonicalRecordHash(directSummary as never) })).toBe(true);
    // A single child-summary (non-UUID) member makes the record invisible.
    const childMember = { ...summaryRecord, memberIds: ["child-summary-hash"], membershipHash: manifestHash(["child-summary-hash"]), contentHash: "pending" };
    delete (childMember as Record<string, unknown>).manifestHash;
    expect(await isVisibleAfterTombstoneCheck(cleanStore, { ...childMember, contentHash: canonicalRecordHash(childMember as never) })).toBe(false);
    // A ghost (non-persisted) episode member also fails closed.
    const ghostId = episodeId(OWNER, "ghost", "message-1");
    const ghostMember = { ...summaryRecord, memberIds: [ghostId], membershipHash: manifestHash([ghostId]), contentHash: "pending" };
    delete (ghostMember as Record<string, unknown>).manifestHash;
    expect(await isVisibleAfterTombstoneCheck(cleanStore, { ...ghostMember, contentHash: canonicalRecordHash(ghostMember as never) })).toBe(false);
// Derived records without complete provenance are invisible fail-closed.
    const broken = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "curated_memory" as const, id: "broken-1", contentId: "content-1", observationId: "occurrence-1", eventAt: NOW, effectiveAt: NOW, effectiveOrder: "session:1", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" } as unknown as MemoryRecord;
    expect(await isVisibleAfterTombstoneCheck(cleanStore, broken)).toBe(false);
    // primaryEvidence alone is not full curated-memory closure: without nonempty
    // verified source episodes (or a resolved manifest) the record is invisible.
    const primaryOnly = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "curated_memory" as const, id: observationId(1, "content-1", EPISODE_UUID, "session:1"), contentId: "content-1", observationId: observationId(1, "content-1", EPISODE_UUID, "session:1"), eventAt: NOW, effectiveAt: NOW, effectiveOrder: "session:1", primaryEvidenceEpisodeId: EPISODE_UUID, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(cleanStore, { ...primaryOnly, contentHash: canonicalRecordHash(primaryOnly) })).toBe(false);

  });

  it("maps readEpisodes exactly over REAL vector-bound episode points and rejects extras, duplicates, missing vectors and malformed readbacks", async () => {
    // A real vector-bound committed episode: payload (no vector) + named semantic vector + vector-bound hash.
    const ep = episode({ vector: Array.from({ length: 1024 }, (_, index) => (index % 7) / 10) });
    const points = restPoints();
    const payload = recordPayload(ep);
    points.set(ep.id, { id: ep.id, payload, vector: { semantic: [...(ep.vector as number[])] } });
    const store = productionStore(points).store;
    const found = await store.readEpisodes([ep.id]);
    expect(found.map((entry) => entry.id)).toEqual([ep.id]);
    expect(found[0]?.vector).toEqual(ep.vector);
    expect(found[0]?.contentHash).toBe(canonicalRecordHash(found[0]!));
    // Extras not in the requested set are rejected.
    const extraStore = productionStore(points, (ids) => [{ id: episodeId(OWNER, "extra", "message-1"), payload, vector: { semantic: [...(ep.vector as number[])] } }]).store;
    await expect(extraStore.readEpisodes([ep.id])).rejects.toThrow(/extras|duplicates/i);
    // Duplicate points for one requested id are rejected.
    const duplicateStore = productionStore(points, (ids) => [{ id: ep.id, payload, vector: { semantic: [...(ep.vector as number[])] } }]).store;
    await expect(duplicateStore.readEpisodes([ep.id])).rejects.toThrow(/extras|duplicates/i);
    // Missing named vector fails closed.
    const noVectorPoints = restPoints();
    noVectorPoints.set(ep.id, { id: ep.id, payload });
    const noVectorStore = productionStore(noVectorPoints).store;
    await expect(noVectorStore.readEpisodes([ep.id])).rejects.toThrow(/malformed|invalid|identity|hash/i);
    // Malformed payloads are rejected.
    const malformedPoints = restPoints();
    malformedPoints.set(ep.id, { id: ep.id, payload: { ...payload, content_hash: "bogus" }, vector: { semantic: [...(ep.vector as number[])] } });
    const malformedStore = productionStore(malformedPoints).store;
    await expect(malformedStore.readEpisodes([ep.id])).rejects.toThrow(/malformed|invalid|identity|hash/i);
  });

  it("reads a real vector-bound episode through the production store (readEpisode) and verifies tombstone targets with it", async () => {
    const ep = episode({ vector: Array.from({ length: 1024 }, (_, index) => (index % 7) / 10) });
    const points = restPoints();
    const payload = recordPayload(ep);
    points.set(ep.id, { id: ep.id, payload, vector: { semantic: [...(ep.vector as number[])] } });
    const store = productionStore(points).store;
    const single = await store.readEpisode(ep.id);
    expect(single).not.toBeNull();
    expect(single?.vector).toEqual(ep.vector);
    expect(single?.contentHash).toBe(canonicalRecordHash(single!));
    // Occurrence tombstone creation verifies the episode target through the
    // production store; the same store serves the insert/readback.
    const tombstones = await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: ep.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1" });
    expect(tombstones.map((entry) => entry.targetId)).toContain(ep.id);
    // A no-vector (legacy/ambiguous) point is NOT an episode target anymore.
    const legacyPoints = restPoints();
    legacyPoints.set(ep.id, { id: ep.id, payload });
    const legacyStore = productionStore(legacyPoints).store;
    await expect(createTombstone(legacyStore, { ownerHost: OWNER, scope: "occurrence", targetId: ep.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1" })).rejects.toThrow(/episode target/i);
  });

  it("resolves derived provenance closure through vector-bound production episodes and tombstone visibility", async () => {
    const ep = episode({ vector: Array.from({ length: 1024 }, (_, index) => (index % 3) / 10) });
    const points = restPoints();
    points.set(ep.id, { id: ep.id, payload: recordPayload(ep), vector: { semantic: [...(ep.vector as number[])] } });
    const store = productionStore(points).store;
    const curated = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "curated_memory" as const, id: "occurrence:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", contentId: "content:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", observationId: "occurrence:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", eventAt: NOW, effectiveAt: NOW, effectiveOrder: "session:1", sourceEpisodeIds: [ep.id], coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...curated, contentHash: canonicalRecordHash(curated) })).toBe(true);
    // After an occurrence tombstone over the vector-bound episode, invisible.
    await createTombstone(store, { ownerHost: OWNER, scope: "occurrence", targetId: ep.id, targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1" });
    expect(await isVisibleAfterTombstoneCheck(store, { ...curated, contentHash: canonicalRecordHash(curated) })).toBe(false);
  });

  it("rejects a backwards trusted clock during slow reads with no CAS or authority", async () => {
    const store = fakeStore();
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(store, jobInputFrom(job));
    const box = { value: NOW_MS + 50000 };
    const w = mintRootWorker(OWNER, "node-a", () => box.value);
    const claim = await claimLease(store, w, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim).not.toBeNull();
    const proposal = await writeProposal(store, claim!, { membership: job.membership, content: { summary: "safe summary" }, createdAt: NOW });
    // Gate the slow proposal read; the clock then moves BACKWARDS below its
    // first accepted sample (200 -> 100 style), which must fail closed.
    const gateHolder: { release?: () => void; started?: () => boolean; release2?: () => void; started2?: () => boolean } = {};
    let gateProposalId = "";
    let controlCalls = 0;
    let armed2 = false;
    const { store: gatedStore } = fakeStoreWithBackend({}, { extra: async (ids) => {
      if (gateProposalId !== "" && ids.includes(gateProposalId)) { await new Promise<void>((resolve) => { gateHolder.started = () => true; gateHolder.release = resolve; }); }
      if (armed2 && ids.includes(COLLECTION_CONTROL_ID)) { controlCalls += 1; if (controlCalls === 2) { await new Promise<void>((resolve) => { gateHolder.started2 = () => true; gateHolder.release2 = resolve; }); } }
      return undefined;
    } });
    const jobG = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    await createJob(gatedStore, jobInputFrom(jobG));
    const wG = mintRootWorker(OWNER, "node-a", () => box.value);
    const claimG = await claimLease(gatedStore, wG, { jobId: jobG.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    const proposalG = await writeProposal(gatedStore, claimG!, { membership: jobG.membership, content: { summary: "safe summary" }, createdAt: NOW });
    gateProposalId = proposalG.id;
    const pending = acceptProposal(gatedStore, claimG!, { proposalId: proposalG.id });
    await vi.waitFor(() => expect(gateHolder.started?.()).toBe(true));
    box.value = NOW_MS;
    gateHolder.release?.();
    await expect(pending).rejects.toThrow(/backwards|clock/i);
    const after = await readLease(gatedStore, jobG.id);
    expect(after?.state).toBe("leased");
    expect(after?.acceptedProposalId).toBeNull();
    // Equal samples are allowed (monotonic, non-strict).
    const equal = mintRootWorker(OWNER, "node-a", () => NOW_MS);
    expect(equal.now()).toBe(NOW_MS);
    expect(equal.now()).toBe(NOW_MS);
    // Materialization also refuses a backwards clock.
    const box2 = { value: NOW_MS + 50000 };
    const w2 = mintRootWorker(OWNER, "node-a", () => box2.value);
    const claim2 = await claimLease(gatedStore, w2, { jobId: jobG.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim2).not.toBeNull();
    gateProposalId = "";
    const accepted = await acceptProposal(gatedStore, claim2!, { proposalId: proposalG.id });
    expect(accepted).not.toBeNull();
    armed2 = true;
    const pending2 = readActiveAcceptance(gatedStore, accepted!);
    await vi.waitFor(() => expect(gateHolder.started2?.()).toBe(true));
    box2.value = NOW_MS;
    gateHolder.release2?.();
    await expect(pending2).resolves.toBeNull();
  });

  it("enforces exact physical response cardinality on every production-store read path", async () => {
    const ep = episode({ vector: Array.from({ length: 1024 }, (_, index) => (index % 7) / 10) });
    const points = restPoints();
    const payload = recordPayload(ep);
    points.set(ep.id, { id: ep.id, payload, vector: { semantic: [...(ep.vector as number[])] } });
    const extraEpisodeId = episodeId(OWNER, "extra", "message-1");
    // readEpisode: an extra/unrequested point in the response fails closed (null).
    const extraStore = productionStore(points, (ids) => [{ id: extraEpisodeId, payload, vector: { semantic: [...(ep.vector as number[])] } }]).store;
    await expect(extraStore.readEpisode(ep.id)).resolves.toBeNull();
    // Duplicate requested points also fail closed.
    const duplicateStore = productionStore(points, (ids) => [{ id: ep.id, payload, vector: { semantic: [...(ep.vector as number[])] } }]).store;
    await expect(duplicateStore.readEpisode(ep.id)).resolves.toBeNull();
    // readOne (lease): extras/duplicates fail closed (null).
    const leasePoint = lease("job-1");
    const leasePoints = restPoints();
    leasePoints.set(leasePoint.id, { id: leasePoint.id, payload: recordPayload(leasePoint) });
    const leaseStore = productionStore(leasePoints, (ids) => [{ id: "extra-lease-point", payload: recordPayload(leasePoint) }]).store;
    await expect(leaseStore.readLease("job-1")).resolves.toBeNull();
    // readControl: extras/duplicates fail closed (throws).
    const controlPoints = restPoints();
    const controlRecord = control();
    controlPoints.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(controlRecord) });
    const controlStore = productionStore(controlPoints, (ids) => [{ id: episodeId(OWNER, "extra-control", "message-1"), payload: controlPayload(controlRecord) }]).store;
    await expect(controlStore.readControl()).rejects.toThrow(/ambiguous|missing/i);
    // readTombstones: unrequested points fail closed.
    const tomb = tombstoneRecord(ep.id);
    const tombPoints = restPoints();
    tombPoints.set(tomb.id, { id: tomb.id, payload: recordPayload(tomb) });
    const tombStore = productionStore(tombPoints, (ids) => [{ id: tombstoneId(OWNER, "00000000-0000-5000-8000-000000000099"), payload: recordPayload(tombstoneRecord("00000000-0000-5000-8000-000000000099")) }]).store;
    await expect(tombStore.readTombstones([ep.id])).rejects.toThrow(/unrequested|duplicate/i);
    // readCoverage: unrequested points fail closed.
    const cov = coverageRecord("job-1", ep.id);
    const covPoints = restPoints();
    covPoints.set(cov.id, { id: cov.id, payload: recordPayload(cov) });
    const covStore = productionStore(covPoints, (ids) => [{ id: coverageId({ ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000002", extractorRevision: EXTRACTOR, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 }), payload: recordPayload(coverageRecord("job-1", "00000000-0000-5000-8000-000000000002")) }]).store;
    await expect(covStore.readCoverage([cov.id])).rejects.toThrow(/unrequested|duplicate/i);
  });

  it("rejects canonical payloads served at foreign physical point IDs on every coordination read", async () => {
    // Lease: canonical payload for job-B served at job-A's lease point id.
    const leaseForB = lease("job-B");
    const leasePoints = restPoints();
    leasePoints.set(leasePointId("job-A"), { id: leasePointId("job-A"), payload: recordPayload(leaseForB) });
    const leaseStore = productionStore(leasePoints).store;
    await expect(leaseStore.readLease("job-A")).resolves.toBeNull();
    // Job: canonical job-B payload at job-A's point id.
    const jobBId = jobId(OWNER, ["episode-b"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0);
    const jobAId = jobId(OWNER, ["episode-a"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0);
    const jobForB = jobRecord(jobBId, { membership: ["episode-b"] });
    const jobPoints = restPoints();
    jobPoints.set(jobAId, { id: jobAId, payload: recordPayload(jobForB) });
    const jobStore = productionStore(jobPoints).store;
    await expect(jobStore.readJob(jobAId)).resolves.toBeNull();
    // Proposal: canonical job-B proposal payload at job-A's proposal point id.
    const proposalForA = proposalRecord("job-A", { membership: ["episode-a"] });
    const proposalForB = proposalRecord("job-B", { membership: ["episode-b"] });
    const proposalPoints = restPoints();
    proposalPoints.set(proposalForA.id, { id: proposalForA.id, payload: recordPayload(proposalForB) });
    const proposalStore = productionStore(proposalPoints).store;
    await expect(proposalStore.readProposal(proposalForA.id)).resolves.toBeNull();
    // Tombstone: canonical target-X payload at target-Y's point id rejects the batch.
    const tombForX = tombstoneRecord("00000000-0000-5000-8000-000000000001");
    const targetY = "00000000-0000-5000-8000-000000000002";
    const tombPoints = restPoints();
    tombPoints.set(tombstoneId(OWNER, targetY), { id: tombstoneId(OWNER, targetY), payload: recordPayload(tombForX) });
    const tombStore = productionStore(tombPoints).store;
    await expect(tombStore.readTombstones([targetY])).rejects.toThrow(/identity|invalid/i);
    // Coverage: canonical episode-B coverage payload at episode-A's coverage point id.
    const covForA = coverageRecord("job-1", "00000000-0000-5000-8000-000000000001");
    const covForB = coverageRecord("job-1", "00000000-0000-5000-8000-000000000002");
    const covPoints = restPoints();
    covPoints.set(covForA.id, { id: covForA.id, payload: recordPayload(covForB) });
    const covStore = productionStore(covPoints).store;
    await expect(covStore.readCoverage([covForA.id])).rejects.toThrow(/identity|invalid/i);
    // Control: the canonical control payload served at a foreign point id is
    // missing at the real control point (empty map, no default seeding).
    const controlPoints = new Map<string, { id: string; payload: Record<string, unknown> }>();
    const controlRecord = control();
    controlPoints.set(episodeId(OWNER, "foreign", "control"), { id: episodeId(OWNER, "foreign", "control"), payload: controlPayload(controlRecord) });
    const controlStore = productionStore(controlPoints).store;
    await expect(controlStore.readControl()).rejects.toThrow(/missing|ambiguous/i);
    // Scroll: a foreign job point inside the lease scroll fails closed.
    const scrollPoints = restPoints();
    stubGlobalFetch(async (input, init) => {
      const url = String(input);
      if (url.includes("/points/scroll")) return new Response(JSON.stringify({ result: { points: [{ id: leasePointId("job-A"), payload: recordPayload(lease("job-B")) }], next_page_offset: null }, status: "ok" }), { headers: { "content-type": "application/json" } });
      return restFetch(scrollPoints)(input, init);
    });
    const scrollStore = createQdrantCoordinationStore(qdrantOptions());
    await expect(scrollStore.scrollLeases()).rejects.toThrow(/malformed|foreign|lease/i);
  });

  it("cannot accept or materialize a cross-job canonical payload alias", async () => {
    const jobBId = jobId(OWNER, ["episode-b"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0);
    const aliasB = lease(jobBId);
    const aliasBAccepted = lease(jobBId, { state: "accepted" as const, acceptedProposalId: "00000000-0000-4000-8000-000000000099", acceptedManifestHash: "m" });
    const holderCj: { armed?: boolean } = {};
    const { store } = fakeStoreWithBackend({}, { extra: (ids) => holderCj.armed === true && ids.includes(leasePointId(jobA.id)) ? [{ id: leasePointId(jobA.id), payload: recordPayload(aliasB) }] : undefined });
    const jobA = jobRecord(jobId(OWNER, ["episode-a"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0), { membership: ["episode-a"] });
    await createJob(store, jobInputFrom(jobA));
    const w = timedWorker(NOW_MS, "node-a");
    const claim = await claimLease(store, w.worker, { jobId: jobA.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim).not.toBeNull();
    const proposal = await writeProposal(store, claim!, { membership: ["episode-a"], content: { summary: "safe summary" }, createdAt: NOW });
    holderCj.armed = true;
    // A canonical lease FOR job-B served as job-A's claim (cross-job alias):
    // the claim/job linkage must refuse it before any CAS or authority.
    await expect(acceptProposal(store, claim!, { proposalId: proposal.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).rejects.toThrow(/stale|job|claim/i);
    // Materialization: the accepted-state cross-job alias claim is refused by the job linkage.
    const holderCj2: { armed?: boolean } = {};
    const { store: store2 } = fakeStoreWithBackend({}, { extra: (ids) => holderCj2.armed === true && ids.includes(leasePointId(jobA.id)) ? [{ id: leasePointId(jobA.id), payload: recordPayload(aliasBAccepted) }] : undefined });
    await createJob(store2, jobInputFrom(jobA));
    const w2 = timedWorker(NOW_MS, "node-a");
    const claim2 = await claimLease(store2, w2.worker, { jobId: jobA.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, });
    expect(claim2).not.toBeNull();
    holderCj2.armed = true;
    await expect(readActiveAcceptance(store2, claim2!, { policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, maxClockSkewMs: 0 })).resolves.toBeNull();
  });

  it("final visibility is total fail-closed: zero-target kinds, throwing readers and structural fakes return false", async () => {
    const vector = Array.from({ length: 1024 }, (_, index) => (index % 5) / 10);
    const { store } = productionVisibilityStore([episode({ vector })]);
    // Unsupported/non-retrievable kinds with ZERO targets: false, never a throw.
    const policy = processingPolicy();
    const policyRecord: MemoryRecord = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, recordType: "processing_policy" as const, id: policy.id, policy, canonicalHash: policy.id, contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...policyRecord, contentHash: canonicalRecordHash(policyRecord) })).toBe(false);
    expect(await isVisibleAfterTombstoneCheck(store, control())).toBe(false);
    const tomb = tombstoneRecord("00000000-0000-5000-8000-000000000001");
    expect(await isVisibleAfterTombstoneCheck(store, { ...tomb, contentHash: canonicalRecordHash(tomb) })).toBe(false);
    const job = jobRecord(jobId(OWNER, ["episode-1"], POLICY_HASH, EXTRACTOR, 1, INTERSECTION_ID, 0));
    expect(await isVisibleAfterTombstoneCheck(store, { ...job, contentHash: canonicalRecordHash(job) })).toBe(false);
    // Throwing readers are invisible (memory unavailable), never an exception.
    const validCurated = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "curated_memory" as const, id: `occurrence:${"b".repeat(64)}`, contentId: `content:${"b".repeat(64)}`, observationId: `occurrence:${"b".repeat(64)}`, eventAt: NOW, effectiveAt: NOW, effectiveOrder: "session:1", sourceEpisodeIds: [EPISODE_UUID], coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
    // A STRUCTURAL fake returning same-owner minimal IDs + empty tombstones
    // can never mint visibility (nominal production authority required).
    const structuralFake = { readEpisodes: async () => [episode({ vector })], readTombstones: async () => [] };
    expect(await isVisibleAfterTombstoneCheck(structuralFake as never, { ...validCurated, contentHash: canonicalRecordHash(validCurated) })).toBe(false);
    // Reader failures on the branded production store are invisible (memory
    // unavailable), never an exception.
    const throwingEpisodes = productionStore(restPoints(), () => { throw new Error("episodes offline"); }).store;
    expect(await isVisibleAfterTombstoneCheck(throwingEpisodes, { ...validCurated, contentHash: canonicalRecordHash(validCurated) })).toBe(false);
    const tombPoints = restPoints();
    tombPoints.set(EPISODE_UUID, { id: EPISODE_UUID, payload: recordPayload(episode({ vector })), vector: { semantic: [...vector] } });
    let tombCalls = 0;
    const throwingTombstones = productionStore(tombPoints, () => { tombCalls += 1; if (tombCalls >= 2) throw new Error("tombstones offline"); return undefined; }).store;
    expect(await isVisibleAfterTombstoneCheck(throwingTombstones, { ...validCurated, contentHash: canonicalRecordHash(validCurated) })).toBe(false);
    // Invalid batch identity (a valid episode payload at an unrequested point)
    // is invisible, never propagated.
    const aliasPoints = restPoints();
    aliasPoints.set(episodeId(OWNER, "alias", "x"), { id: episodeId(OWNER, "alias", "x"), payload: recordPayload(episode({ vector })), vector: { semantic: [...vector] } });
    const invalidBatch = productionStore(aliasPoints).store;
    expect(await isVisibleAfterTombstoneCheck(invalidBatch, { ...validCurated, contentHash: canonicalRecordHash(validCurated) })).toBe(false);
    // Valid retrieval behavior on the branded production store is unchanged.
    expect(await isVisibleAfterTombstoneCheck(store, { ...validCurated, contentHash: canonicalRecordHash(validCurated) })).toBe(true);
  });

  it("rejects untombstoneable domain targets in final visibility", async () => {
    const ep = episode({ vector: Array.from({ length: 1024 }, (_, index) => (index % 5) / 10) });
    const store = productionVisibilityStore([ep]).store;
    const hex = "a".repeat(64);
    const base = { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, recordType: "curated_memory" as const, id: `occurrence:${hex}`, contentId: "content-raw", observationId: `occurrence:${hex}`, eventAt: NOW, effectiveAt: NOW, effectiveOrder: "session:1", sourceEpisodeIds: [ep.id], coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
    // Raw contentId can never be tombstoned -> invisible despite verified sources.
    expect(await isVisibleAfterTombstoneCheck(store, { ...base, contentHash: canonicalRecordHash(base) })).toBe(false);
    // Raw stateKey -> invisible.
    const rawState = { ...base, contentId: `content:${hex}`, stateKey: "raw-state", contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...rawState, contentHash: canonicalRecordHash(rawState) })).toBe(false);
    // Invalid raw occurrence selector -> invisible.
    const rawOccurrence = { ...base, id: "occurrence-raw", contentId: `content:${hex}`, observationId: "occurrence-raw", contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...rawOccurrence, contentHash: canonicalRecordHash(rawOccurrence) })).toBe(false);
    // Valid domain-separated targets preserve visibility.
    const valid = { ...base, contentId: `content:${hex}`, observationId: `occurrence:${hex}`, stateKey: `state:${hex}`, contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...valid, contentHash: canonicalRecordHash(valid) })).toBe(true);
  });

  it("rejects duplicate batch input IDs on production-store readTombstones/readCoverage/readEpisodes", async () => {
    const ep = episode({ vector: Array.from({ length: 1024 }, (_, index) => (index % 7) / 10) });
    const points = restPoints();
    points.set(ep.id, { id: ep.id, payload: recordPayload(ep), vector: { semantic: [...(ep.vector as number[])] } });
    const store = productionStore(points).store;
    await expect(store.readEpisodes([ep.id, ep.id])).rejects.toThrow(/invalid/i);
    await expect(store.readTombstones([ep.id, ep.id])).rejects.toThrow(/invalid/i);
    const cov = coverageRecord("job-1", ep.id);
    const covPoints = restPoints();
    covPoints.set(cov.id, { id: cov.id, payload: recordPayload(cov) });
    const covStore = productionStore(covPoints).store;
    await expect(covStore.readCoverage([cov.id, cov.id])).rejects.toThrow(/invalid/i);
  });

  it("frozen collision error brands cannot be monkeypatched to reclassify failures as terminal", () => {
    expect(Object.isFrozen(QdrantContentHashCollisionError)).toBe(true);
    expect(Object.isFrozen(QdrantContentHashCollisionError.prototype)).toBe(true);
    expect(Object.isFrozen(QdrantLegacyEpisodeHashError)).toBe(true);
    expect(Object.isFrozen(QdrantLegacyEpisodeHashError.prototype)).toBe(true);
    // Symbol.hasInstance reassignment is refused (strict-mode assignment on a frozen function).
    expect(() => { (QdrantContentHashCollisionError as unknown as Record<symbol, unknown>)[Symbol.hasInstance] = () => true; }).toThrow();
    expect(() => { (QdrantLegacyEpisodeHashError as unknown as Record<symbol, unknown>)[Symbol.hasInstance] = () => true; }).toThrow();
    // Prototype tampering is refused.
    expect(() => { (QdrantContentHashCollisionError.prototype as unknown as Record<string, unknown>).hack = 1; }).toThrow();
  });

  it("treats RAPTOR direct member lists as episode provenance only when every member is an exact persisted episode UUID", async () => {
    const leaf = episodeId(OWNER, "leaf", "message-1");
    const childSummary = "child-summary-hash";
    const vector = Array.from({ length: 1024 }, (_, index) => (index % 5) / 10);
    const store = productionVisibilityStore([episode({ vector }), episode({ id: leaf, sourceEntryId: "entry-leaf", vector })]).store;
    const summary = { recordType: "raptor_summary" as const, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, contentHash: "pending", id: "summary-root", generationId: "g-1", clusterId: "c-1", membershipHash: manifestHash([leaf]), level: 1, summary: "s", modelId: "m", embeddingDimension: 1024, promptRevision: "p", algorithm: "gmm", seed: 1, jobId: "job-1", fencingToken: 1, temporalFrom: NOW, temporalTo: NOW, coveredProjects: ["project-1"], algorithmParameters: {}, coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, contentHash: "pending" };
    // memberIds-only (no manifest): every member an exact persisted episode UUID -> visible.
    const direct = { ...summary, memberIds: [leaf], membershipHash: manifestHash([leaf]), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...direct, contentHash: canonicalRecordHash(direct as never) })).toBe(true);
    // A child-summary member (non-UUID) fails closed.
    const child = { ...summary, memberIds: [childSummary], membershipHash: manifestHash([childSummary]), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...child, contentHash: canonicalRecordHash(child as never) })).toBe(false);
    // A ghost (non-persisted) episode member fails closed.
    const ghost = { ...summary, memberIds: [episodeId(OWNER, "ghost", "message-1")], membershipHash: manifestHash([episodeId(OWNER, "ghost", "message-1")]), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...ghost, contentHash: canonicalRecordHash(ghost as never) })).toBe(false);
    // memberIds alongside a manifestHash stays invisible (manifest fails closed for Task 10).
    const withManifest = { ...summary, memberIds: [leaf], manifestHash: "some-manifest", membershipHash: manifestHash([leaf]), contentHash: "pending" };
    expect(await isVisibleAfterTombstoneCheck(store, { ...withManifest, contentHash: canonicalRecordHash(withManifest as never) })).toBe(false);
    // Tombstone targets never include the raw member (child summary) id.
    const targets = tombstoneTargets({ ...child, contentHash: canonicalRecordHash(child as never) });
    expect(targets.some((target) => target.targetId === childSummary)).toBe(false);
  });

  it("enumerates occurrence/content/state targets and provenance closure per record type", () => {
    const stateTarget = stateKey({ host: "pi", scope: "project", projectId: "p", category: "fact", subject: "s", predicate: "p" });
    const contentTarget = contentId("policy", stateTarget, "value");
    const sourceEpisode = EPISODE_UUID;
    const current = { recordType: "curated_current" as const, ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "policy-1", expiresAt: null, contentHash: "pending", id: curatedCurrentId(OWNER, stateTarget, 1), version: 1, stateKey: stateTarget, resolution: "resolved" as const, contentId: contentTarget, observationId: observationId(1, contentTarget, sourceEpisode, "session:1"), effectiveOrder: "session:1", sourceEpisodeIds: [sourceEpisode], coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1, text: "safe", vector: Array.from({ length: 1024 }, () => 0.25) };
    const targets = tombstoneTargets({ ...current, contentHash: canonicalRecordHash(current) });
    expect(targets).toEqual(expect.arrayContaining([{ scope: "occurrence", targetId: current.observationId }, { scope: "content", targetId: contentTarget }, { scope: "state", targetId: stateTarget }, { scope: "occurrence", targetId: sourceEpisode }]));
  });

  it("options-based creation pins owner/collection and rejects forged or retargeted inputs before network", async () => {
    // Creation accepts validated OPTIONS only: a structural raw object fails
    // closed before any network.
    const raw = Object.freeze({ endpoint: "http://qdrant", ownerHost: "pi" as const, collection: "pi_memory" as const, maxClockSkewMs: 0, retrieve: vi.fn(async () => []), scroll: vi.fn(async () => ({ points: [] })), upsertPoints: vi.fn(async () => undefined) });
    expect(() => createQdrantCoordinationStore(raw as unknown as QdrantClientOptions)).toThrow();
    // A valid options object pins the store to its owner/collection.
    const { store } = productionRestWriter();
    expect(store.ownerHost).toBe("pi");
    expect(store.collection).toBe("pi_memory");
    await store.readControl().catch(() => undefined);
    // Wrong collection for the owner fails before network.
    expect(() => createQdrantCoordinationStore({ ...qdrantOptions(), collection: "prime_memory" })).toThrow(/collection does not match owner host/i);
    // A fresh prime store is prime, never pi.
    const prime = productionRestWriter("http://qdrant", "prime");
    expect(prime.store.ownerHost).toBe("prime");
    expect(prime.store.collection).toBe("prime_memory");
    // Method retargeting after construction is impossible: the store is frozen.
    expect(() => { (store as unknown as Record<string, unknown>).readControl = async () => { throw new Error("pwned"); }; }).toThrow();
    expect(() => { Object.defineProperty(store, "readControl", { value: async () => { throw new Error("pwned"); } }); }).toThrow();
  });

  it("safe claimLease observes filter-honoring zero-match CAS and delayed concurrent races (typed lease CAS through the safe surface)", async () => {
    const stored = new Map<string, { id: string; payload: Record<string, unknown> }>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]]);
    let raceGate: (() => void) | undefined;
    let gateRelease: (() => void) | undefined;
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { points?: Array<{ id: string; payload: Record<string, unknown> }>; update_mode?: string; update_filter?: unknown };
      if (url.includes("/points/retrieve")) { const ids = (body as { ids?: string[] }).ids ?? []; return new Response(JSON.stringify({ result: ids.map((id) => stored.get(id)).filter((point) => point !== undefined), status: "ok" }), { headers: { "content-type": "application/json" } }); }
      if (url.includes("/points?") && init.method === "PUT") {
        // Delayed race: a concurrent write lands BEFORE this upsert's filter is evaluated.
        if (raceGate !== undefined) { raceGate(); raceGate = undefined; await new Promise<void>((resolve) => { gateRelease = resolve; }); }
        const point = body?.points?.[0];
        const current = point === undefined ? undefined : stored.get(point.id)?.payload;
        const value = (key: string): unknown => current?.[key];
        const must = (body?.update_filter as { must: Array<{ key: string; match?: { value?: unknown }; is_null?: { key: string }; range?: { lte?: string; gt?: string } }> } | undefined)?.must ?? [];
        const matches = must.every((condition) => {
          if ("is_null" in condition) return value(condition.is_null.key) === null;
          if (condition.key === "expires_at" && condition.range?.lte !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) <= Date.parse(condition.range.lte);
          if (condition.key === "expires_at" && condition.range?.gt !== undefined) return typeof value("expires_at") === "string" && Date.parse(value("expires_at") as string) > Date.parse(condition.range.gt);
          return value(condition.key) === condition.match?.value;
        });
        if (body?.update_mode === "update_only" && !matches) return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } });
        if (point !== undefined) stored.set(point.id, { id: point.id, payload: point.payload });
        return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } });
      }
      return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } });
    };
    stubGlobalFetch(fetchImpl);
    const store = createQdrantCoordinationStore(qdrantOptions());
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    // Node-a claims the job through the SAFE surface (typed lease CAS with strong ordering/wait).
    const seedAuthority = await claimLease(store, workerAt("node-a", NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(seedAuthority).not.toBeNull();
    expect(stored.get(leasePointId(job.id))?.payload.version).toBe(1);
    expect(stored.get(leasePointId(job.id))?.payload.owner_id).toBe("node-a");
    // Zero-match: a wrong owner cannot steal a LIVE claim -> the safe op returns
    // null and the server acknowledged nothing (version/owner unchanged).
    await expect(claimLease(store, workerAt("node-zzz", NOW_MS + 1000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })).resolves.toBeNull();
    expect(stored.get(leasePointId(job.id))?.payload.version).toBe(1);
    expect(stored.get(leasePointId(job.id))?.payload.owner_id).toBe("node-a");
    // Matched steal lands exactly and rereads exactly (after conservative expiry).
    const stolen = await claimLease(store, workerAt("node-b", NOW_MS + 31000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(stolen).not.toBeNull();
    expect(stored.get(leasePointId(job.id))?.payload.version).toBe(2);
    expect(stored.get(leasePointId(job.id))?.payload.owner_id).toBe("node-b");
    // Delayed race: a concurrent advance lands before the filter is evaluated -> zero-match -> null.
    raceGate = () => { const before = stored.get(leasePointId(job.id))!; stored.set(leasePointId(job.id), { id: leasePointId(job.id), payload: { ...before.payload, owner_id: "node-d", version: 4, fencing_token: 4 } }); };
    const racing = claimLease(store, workerAt("node-c", NOW_MS + 62000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
    gateRelease?.();
    await expect(racing).resolves.toBeNull();
    expect(stored.get(leasePointId(job.id))?.payload.version).toBe(4);
  });

  it("uses the production REST client for typed lease CAS with strong ordering, wait, and exact readback", async () => {
    const points = new Map<string, { id: string; payload: Record<string, unknown> }>([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(control()) }]]);
    const writeCalls: Array<{ mode: string; filter?: unknown }> = [];
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input); const body = init.body === undefined ? undefined : JSON.parse(String(init.body)) as { points?: Array<{ id: string; payload: Record<string, unknown> }>; update_mode?: string; update_filter?: unknown };
      if (url.includes("/points/retrieve")) { const ids = (body as { ids?: string[] }).ids ?? []; return new Response(JSON.stringify({ result: ids.map((id) => points.get(id)).filter((point) => point !== undefined), status: "ok" }), { headers: { "content-type": "application/json" } }); }
      if (url.includes("/points?") && init.method === "PUT") { writeCalls.push({ mode: String(body?.update_mode), filter: body?.update_filter }); const point = body?.points?.[0]; if (point !== undefined) points.set(point.id, { id: point.id, payload: point.payload }); return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); }
      return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } });
    };
    stubGlobalFetch(fetchImpl);
    const store = createQdrantCoordinationStore(qdrantOptions());
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, policyIntersectionId: INTERSECTION_ID, createdAt: NOW, privacyEpoch: 0 });
    const claimed = await claimLease(store, workerAt("node-a", NOW_MS, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(claimed?.fencingToken).toBe(1);
    expect(writeCalls.some((call) => call.mode === "insert_only")).toBe(true);
    const stolenAuthority = await claimLease(store, workerAt("node-b", NOW_MS + 40000, 30000, 0), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(stolenAuthority).not.toBeNull();
    expect(stolenAuthority?.fencingToken).toBe(2);
    const stealWrite = writeCalls.find((call) => call.mode === "update_only");
    expect(stealWrite).toBeDefined();
    const proposal = await writeProposal(store, stolenAuthority!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW });
    const accepted = await acceptProposal(store, stolenAuthority!, { proposalId: proposal.id });
    expect(accepted).not.toBeNull();
    const reread = await readLease(store, job.id);
    expect(reread?.state).toBe("accepted");
    expect(reread?.acceptedProposalId).toBe(proposal.id);
    expect(reread?.acceptedManifestHash).toBe(proposal.manifestHash);
  });

  it("quarantines tombstoned episodes before any policy insert or embedding and rereads the barrier", async () => {
    const qdrantDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
    const localPolicy = processingPolicy({ policyRevision: "local-v1" });
    const producerPolicy = processingPolicy({ policyRevision: "producer-v1" });
    const episodes = [episode({ id: episodeId(OWNER, "session-1", "message-1"), processingPolicyId: producerPolicy.id, expiresAt: producerPolicy.expiresAt, originProvider: producerPolicy.originProvider, destinationId: producerPolicy.destinationIds.qdrant })];
    const jobValue = { version: 1 as const, id: deterministicUuid("pi-qdrant-memory-v2:outbox-job", OWNER, episodes.map((entry) => entry.id), producerPolicy.id), ownerHost: OWNER, nodeId: "node-redacted", producerUuid: "00000000-0000-4000-8000-000000000001", createdAt: NOW, deadline: producerPolicy.expiresAt, policyId: producerPolicy.id, policy: producerPolicy, episodeIds: episodes.map((entry) => entry.id), episodes, auditHash: "pending" };
    const { auditHash: _auditHash, ...withoutAudit } = jobValue;
    const job = { ...jobValue, auditHash: sha256Hex(canonicalStringify(withoutAudit)) } as never;
    const qdrant = { destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, ownerHost: "pi" as const, collection: "pi_memory" as const, coordination: { policyHash: POLICY_HASH, policyEpoch: 1 }, insertAndReadback: vi.fn(async () => "inserted" as const), retrieve: vi.fn(async () => null) };
    const embedding = { destination: { id: "embed:local", residency: "local", dataUse: "memory" }, embed: vi.fn(async () => Array.from({ length: 1024 }, () => 0.25)) };
    const controlReader: IngestControlReader = { read: vi.fn(async () => ({ state: "active" as const, privacyEpoch: 0, coordinationPolicyEpoch: 1, policyHash: POLICY_HASH, revokedDestinationIds: [] as readonly string[] })) };
    const backend = restPoints();
    stubGlobalFetch(restFetch(backend));
    const bundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    // SAFE tombstone seeding: the episode is persisted first and forgotten via createTombstone.
    await seedTombstone(bundle.store, backend, episodes[0]!);
    const rt = bindIngestRuntime({ store: bundle.store, qdrant: bindQdrantDestination(bundle.qdrant, qdrantDestination), embedding: boundRuntimeEmbedding() });
    const result = await ingestPendingJobs({ job: job as never, now: NOW_MS, localPolicy, runtime: rt, maxClockSkewMs: 0 });
    expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    // NO policy point was ever written and the job episode was NOT egressed: the
    // only episode point is the one seedTombstone persisted for verification.
    expect([...backend.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
    expect([...backend.values()].filter((point) => point.payload.record_type === "episode")).toHaveLength(1);
  });

  it("blocks a mid-flight tombstone before embedding, fails closed on reader failure, and rejects invalid reader output", async () => {
    const qdrantDestination = { id: "qdrant:pi", residency: "local", dataUse: "memory" };
    const localPolicy = processingPolicy({ policyRevision: "local-v1" });
    const producerPolicy = processingPolicy({ policyRevision: "producer-v1" });
    const makeJob = (episodeIdValue: string) => {
      const episodes = [episode({ id: episodeIdValue, sourceEntryId: "entry-1", sessionId: "session-1", turnId: "turn-1", processingPolicyId: producerPolicy.id, expiresAt: producerPolicy.expiresAt, originProvider: producerPolicy.originProvider, destinationId: producerPolicy.destinationIds.qdrant })];
      const jobValue = { version: 1 as const, id: deterministicUuid("pi-qdrant-memory-v2:outbox-job", OWNER, episodes.map((entry) => entry.id), producerPolicy.id), ownerHost: OWNER, nodeId: "node-redacted", producerUuid: "00000000-0000-4000-8000-000000000001", createdAt: NOW, deadline: producerPolicy.expiresAt, policyId: producerPolicy.id, policy: producerPolicy, episodeIds: episodes.map((entry) => entry.id), episodes, auditHash: "pending" };
      const { auditHash: _auditHash, ...withoutAudit } = jobValue;
      return { job: { ...jobValue, auditHash: sha256Hex(canonicalStringify(withoutAudit)) } as never, episodeId: episodes[0]!.id };
    };
    const records = new Map<string, MemoryRecord>();
    const qdrant = { destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, ownerHost: "pi" as const, collection: "pi_memory" as const, coordination: { policyHash: POLICY_HASH, policyEpoch: 1 }, insertAndReadback: vi.fn(async (record: MemoryRecord) => { const existing = records.get(record.id); if (existing !== undefined) { if (existing.contentHash !== record.contentHash) throw new TypeError("collision"); return "existing" as const; } records.set(record.id, record); return "inserted" as const; }), retrieve: vi.fn(async (type: string, id: string) => records.get(id) ?? null) };
    const embedding = { destination: { id: "embed:local", residency: "local", dataUse: "memory" }, embed: vi.fn(async () => Array.from({ length: 1024 }, () => 0.25)) };
    const controlReader: IngestControlReader = { read: vi.fn(async () => ({ state: "active" as const, privacyEpoch: 0, coordinationPolicyEpoch: 1, policyHash: POLICY_HASH, revokedDestinationIds: [] as readonly string[] })) };
    // Mid-flight: the tombstone appears only after the INITIAL barrier (from the
    // second tombstone read onward). The FRESH pre-policy barrier observes it
    // and suppresses the policy write entirely (terminal dispose, no egress).
    const first = makeJob(episodeId(OWNER, "session-1", "message-1"));
    let tombstoneReads = 0;
    const tombstone = tombstoneRecord(first.episodeId);
    const tombstonePayload = recordPayload(tombstone);
    const backend = restPoints();
    stubGlobalFetch(restFetch(backend, (ids) => { if (ids.some((id) => id === tombstone.id)) { tombstoneReads += 1; return tombstoneReads >= 2 ? [{ id: tombstone.id, payload: tombstonePayload }] : []; } return undefined; }));
    const midBundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const midRt = bindIngestRuntime({ store: midBundle.store, qdrant: bindQdrantDestination(midBundle.qdrant, qdrantDestination), embedding: boundRuntimeEmbedding() });
    const midFlight = await ingestPendingJobs({ job: first.job as never, now: NOW_MS, localPolicy, runtime: midRt, maxClockSkewMs: 0 });
    expect(midFlight).toEqual({ delivered: 0, pending: 0, quarantined: 1 });
    expect([...backend.values()].some((point) => point.payload.record_type === "episode")).toBe(false);
    expect([...backend.values()].some((point) => point.payload.record_type === "processing_policy")).toBe(false);
    // Reader failure is fail-closed: the job stays local/pending, never bypasses the barrier.
    const second = makeJob(episodeId(OWNER, "session-2", "message-1"));
    const failingBackend = restPoints();
    stubGlobalFetch(restFetch(failingBackend, (ids) => { if (ids.some((id) => id === tombstoneId(OWNER, second.episodeId))) throw new Error("offline"); return undefined; }));
    const failingBundle = createQdrantSafeBundle({ options: qdrantOptions(), destination: qdrantDestination, egressMode: "allowlist", coordinationPolicyHash: POLICY_HASH, coordinationPolicyEpoch: 1 });
    const failingRt = bindIngestRuntime({ store: failingBundle.store, qdrant: bindQdrantDestination(failingBundle.qdrant, qdrantDestination), embedding: boundRuntimeEmbedding() });
    const failed = await ingestPendingJobs({ job: second.job as never, now: NOW_MS, localPolicy, runtime: failingRt, maxClockSkewMs: 0 });
    expect(failed).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
    // The production reader cannot be minted from a structural store or a foreign owner:
    // structural `{ownerHost, readTombstones}` fakes and any non-branded store fail closed.
    expect(() => createIngestTombstoneReader({ ownerHost: "pi", readTombstones: async () => [] } as never, "pi")).toThrow(/production store/i);
    expect(() => createIngestTombstoneReader(midBundle.store, "prime")).toThrow(/owner/i);
    expect(ProductionCoordinationStore.isValid(midBundle.store)).toBe(true);
    expect(ProductionCoordinationStore.isValid({ ownerHost: "pi", readTombstones: async () => [] } as never)).toBe(false);
  });

  it("QuiescenceProof is bound to the private store scope: a proof from another genuine store is refused at activation", async () => {
    const { store: storeA } = fakeStoreWithBackend();
    const { store: storeB } = fakeStoreWithBackend();
    // Store A drains and mints a genuine proof over its own leases.
    await beginPolicyDrain(storeA, { now: NOW_MS });
    const proofA = await waitForOldLeasesToQuiesce(storeA, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, now: () => NOW_MS });
    expect(QuiescenceProof.isValid(proofA)).toBe(true);
    // Store B drains too (identical endpoint/control identity): A's proof must
    // be refused — the private scope differs even though the control matches.
    await beginPolicyDrain(storeB, { now: NOW_MS });
    await expect(activatePolicyEpoch(storeB, { proof: proofA, nextPolicyHash: "policy-hash-2", memoryModelTimeoutMs: 0 })).rejects.toThrow(/scope|genuine|proof/i);
    // The matching store accepts its own proof.
    const active = await activatePolicyEpoch(storeA, { proof: proofA, nextPolicyHash: "policy-hash-2", memoryModelTimeoutMs: 0 });
    expect(active.coordinationPolicyEpoch).toBe(2);
    expect(active.coordinationPolicyHash).toBe("policy-hash-2");
    // No raw issuer exists: the proof can only be minted through the store's lexical quiescence.
    const writeModule = await import("../../src/qdrant/write.js");
    expect(Object.keys(writeModule).some((name) => /OnProtocol$|mintQuiescence|issueQuiescence/.test(name))).toBe(false);
  });

  it("LeaseAuthority is bound to the private store scope: cross-store claim/renew/release/accept are refused", async () => {
    const { store: storeA } = productionStore(restPoints());
    const { store: storeB } = productionStore(restPoints());
    const jobA = await createJob(storeA, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const jobB = await createJob(storeB, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const authorityA = await claimLease(storeA, workerAt("node-a", NOW_MS), { jobId: jobA.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authorityA).not.toBeNull();
    // The authority minted on A cannot act on B (public-object AND private-scope binding).
    await expect(renewLease(storeB, authorityA!)).rejects.toThrow(/store|scope/i);
    await expect(releaseLease(storeB, authorityA!)).rejects.toThrow(/store|scope/i);
    await expect(writeProposal(storeB, authorityA!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/store|scope/i);
    await expect(acceptProposal(storeB, authorityA!, { proposalId: "00000000-0000-4000-8000-000000000099" })).rejects.toThrow(/store|scope/i);
    await expect(readActiveAcceptance(storeB, authorityA!)).rejects.toThrow(/store/i);
    // The same-named job on B can be claimed by B's own authority (fresh scope).
    const authorityB = await claimLease(storeB, workerAt("node-a", NOW_MS), { jobId: jobB.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authorityB).not.toBeNull();
    expect(authorityB?.matchesStore(storeB)).toBe(true);
    expect(authorityA?.matchesStore(storeB)).toBe(false);
  });


  it("BRAND FIRST: direct store.writeProposal rejects a structural fake authority before any getter, fetch or mutation", async () => {
    const upserts: string[] = [];
    const { store, backend: brandBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { upserts.push(mode); } });
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    upserts.length = 0;
    // A structural fake whose EVERY getter/method throws if touched: the brand
    // check must fire BEFORE anything is read from the object.
    const structuralFake = new Proxy({}, {
      get(_target, prop) { throw new Error("structural getter touched: " + String(prop)); },
      has(_target, prop) { throw new Error("structural has touched: " + String(prop)); },
      getOwnPropertyDescriptor(_target, prop) { throw new Error("structural descriptor touched: " + String(prop)); },
    }) as unknown as LeaseAuthority;
    await expect(store.writeProposal(structuralFake, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/authority/i);
    expect(upserts).toEqual([]);
    // The same fake is rejected by the direct renew/release/accept paths.
    await expect(store.renewLease(structuralFake)).rejects.toThrow(/authority/i);
    await expect(store.releaseLease(structuralFake)).rejects.toThrow(/authority/i);
    await expect(store.acceptProposal(structuralFake, { proposalId: "00000000-0000-4000-8000-000000000099" })).rejects.toThrow(/authority/i);
    expect(upserts).toEqual([]);
    // A genuine authority minted on ANOTHER store fails the exact-store path too.
    const { store: otherStore } = fakeStoreWithBackend();
    const otherJob = await createJob(otherStore, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const otherAuthority = await claimLease(otherStore, workerAt("node-a", NOW_MS), { jobId: otherJob.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(otherAuthority).not.toBeNull();
    await expect(store.writeProposal(otherAuthority!, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })).rejects.toThrow(/store|authority/i);
    expect(upserts).toEqual([]);
    expect(job.id).toBe(otherJob.id); // same-named job on both stores: only the private scope separates them
  });

  it("BRAND FIRST: every public coordination wrapper rejects structural fakes with throwing getters before touching them", async () => {
    const structural = new Proxy({}, {
      get(_target, prop) { throw new Error("structural getter touched: " + String(prop)); },
      has(_target, prop) { throw new Error("structural has touched: " + String(prop)); },
    }) as never;
    const wrappers: Array<[string, () => Promise<unknown>]> = [
      ["readControl", () => readControl(structural)],
      ["readForUpdate", () => readForUpdate(structural)],
      ["initializeControl", () => initializeControl(structural, control())],
      ["beginPolicyDrain", () => beginPolicyDrain(structural, { now: NOW_MS })],
      ["waitForOldLeasesToQuiesce", () => waitForOldLeasesToQuiesce(structural, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0 })],
      ["activatePolicyEpoch", () => activatePolicyEpoch(structural, { proof: null as never, nextPolicyHash: "h", memoryModelTimeoutMs: 0 })],
      ["rotateCoordinationPolicy", () => rotateCoordinationPolicy(structural, { nextPolicyHash: "h", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS })],
      ["beginForgetBarrier", () => beginForgetBarrier(structural, { now: NOW_MS })],
      ["createIngestControlReader", () => Promise.resolve().then(() => createIngestControlReader(structural, { policyHash: "h", policyEpoch: 0 }))],
      ["claimLease", () => claimLease(structural, workerAt("node-a", NOW_MS), { jobId: "j", policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 })],
      ["renewLease", () => renewLease(structural, null as never)],
      ["releaseLease", () => releaseLease(structural, null as never)],
      ["acceptLeaseAuthority", () => acceptLeaseAuthority(structural, null as never, "p")],
      ["readLease", () => readLease(structural, "j")],
      ["createJob", () => createJob(structural, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW })],
      ["writeProposal", () => writeProposal(structural, null as never, { membership: ["episode-1"], content: { summary: "safe" }, createdAt: NOW })],
      ["acceptProposal", () => acceptProposal(structural, null as never, { proposalId: "p" })],
      ["readActiveAcceptance", () => readActiveAcceptance(structural, null as never)],
      ["readJob", () => readJob(structural, "j")],
      ["createTombstone", () => createTombstone(structural, { ownerHost: OWNER, scope: "occurrence", targetId: "00000000-0000-5000-8000-000000000001", targetKind: "episode", createdAt: NOW, privacyEpoch: 0, processingPolicyId: INTERSECTION_ID })],
      ["readTombstones", () => readTombstones(structural, ["x"])],
      ["createIngestTombstoneReader", () => Promise.resolve().then(() => createIngestTombstoneReader(structural, "pi"))],
      ["markCoverage", () => markCoverage(structural, { ownerHost: OWNER, episodeId: "00000000-0000-5000-8000-000000000001", extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, createdAt: NOW, processingPolicyId: INTERSECTION_ID })],
      ["findMissingEpisodes", () => findMissingEpisodes({ store: structural as never, listEpisodes: async () => ({ episodes: [] }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0, maxSlices: 1 })],
      ["reconcileCoverage", () => reconcileCoverage({ store: structural as never, listEpisodes: async () => ({ episodes: [] }), extractorRevision: EXTRACTOR, policyEpoch: 1, policyHash: POLICY_HASH, policyIntersectionId: INTERSECTION_ID, privacyEpoch: 0 })],
    ];
    for (const [name, call] of wrappers) {
      await expect(call(), name).rejects.toThrow(/genuine production store|authority|store|issuer|brand/i);
    }
    // isVisibleAfterTombstoneCheck is fail-closed by design: it RESOLVES false
    // for a structural store, never trusting its getters/methods.
    await expect(isVisibleAfterTombstoneCheck(structural as never, { ownerHost: OWNER, schemaRevision: 1 as const, createdAt: NOW, privacyEpoch: 0, processingPolicyId: "p", expiresAt: null, recordType: "episode" as const, id: "e", contentHash: "c", sourceEntryId: "s", host: OWNER, projectId: "p", projectIdentityKind: "registered" as const, sessionId: "s", turnId: "t", agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: NOW, modelId: "m", embeddingDimension: 1024, originProvider: "o", destinationId: "d", status: "active" as const, redactionStatus: "unchanged" as const, secretScan: "passed" as const, text: "safe" })).resolves.toBe(false);
  });


  it("BLOCKER: a smuggled ClaimLeaseInput can never adopt another worker's live claim (no spread, unknown keys rejected)", async () => {
    const upserts: string[] = [];
    const { store, backend: smuggleBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { upserts.push(mode); } });
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    // Worker B legitimately holds a LIVE claim.
    const authorityB = await claimLease(store, workerAt("node-b", NOW_MS), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authorityB).not.toBeNull();
    expect(authorityB?.ownerId).toBe("node-b");
    upserts.length = 0;
    // Worker A tries to claim with a smuggled ownerId: node-b (the live owner).
    // The unknown-key rejection must fire BEFORE any field is read and BEFORE
    // any mutation — the claim returns null and B's claim is untouched.
    const smuggled = { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0, ownerId: "node-b", ownerHost: "pi", now: NOW_MS, leaseMs: 86400000, maxClockSkewMs: 0 };
    await expect(claimLease(store, workerAt("node-a", NOW_MS), smuggled)).rejects.toThrow(/unknown key/i);
    expect(upserts).toEqual([]);
    const claimAfter = await readLease(store, job.id);
    expect(claimAfter?.ownerId).toBe("node-b");
    expect(claimAfter?.version).toBe(1);
    // A symbol-keyed extra is also rejected.
    const withSymbol = { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 };
    (withSymbol as Record<symbol, unknown>)[Symbol("ownerId")] = "node-b";
    await expect(claimLease(store, workerAt("node-a", NOW_MS), withSymbol)).rejects.toThrow(/symbol/i);
    expect(upserts).toEqual([]);
    // A clean input still works for the genuine owner.
    const authorityA = await claimLease(store, workerAt("node-a", NOW_MS + 31000), { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authorityA).not.toBeNull();
    expect(authorityA?.ownerId).toBe("node-a");
  });


  it("rotate PREFLIGHTS the entire input before any mutation: invalid arguments leave the control active with zero writes", async () => {
    const upserts: string[] = [];
    const { store, backend: rotateBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { if (points.some((point) => point.payload.record_type === "collection_control")) upserts.push(mode); } });
    const baseline = await readControl(store);
    expect(baseline.state).toBe("active");
    expect(baseline.version).toBe(1);
    const cases: Array<[string, Parameters<typeof rotateCoordinationPolicy>[1]]> = [
      ["invalid next policy hash", { nextPolicyHash: "bad hash!", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS }],
      ["excessive explicit timeout", { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, quiesceTimeoutMs: 90_000_001, now: NOW_MS }],
      ["invalid memory timeout", { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: -1, now: NOW_MS }],
      ["invalid lease window", { nextPolicyHash: "policy-hash-2", maxLeaseMs: 86_400_001, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS }],
      ["invalid clock", { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: -1 }],
      ["aborted signal", { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS, signal: new AbortController().signal }],
    ];
    const aborted = new AbortController();
    aborted.abort();
    for (const [name, input] of cases) {
      const inputValue = name === "aborted signal" ? { ...input, signal: aborted.signal } : input;
      upserts.length = 0;
      await expect(rotateCoordinationPolicy(store, inputValue as never), name).rejects.toThrow();
      expect(upserts, name).toEqual([]);
      const after = await readControl(store);
      expect(after.state, name).toBe("active");
      expect(after.version, name).toBe(1);
      expect(after.coordinationPolicyEpoch, name).toBe(1);
    }
    // The max VALID runtime combo rotates successfully (lease 300000 + skew 3600000 -> default timeout 3900000, now within the coherent bound).
    upserts.length = 0;
    const rotated = await rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 300000, maxClockSkewMs: 3_600_000, memoryModelTimeoutMs: 0, now: NOW_MS });
    expect(rotated.state).toBe("active");
    expect(rotated.coordinationPolicyEpoch).toBe(2);
    expect(rotated.coordinationPolicyHash).toBe("policy-hash-2");
    expect(upserts.filter((mode) => mode === "update_only")).not.toEqual([]);
  });


  it("rotate rejects malformed signals BEFORE any mutation (genuine AbortSignal brand + bound methods required)", async () => {
    const upserts: string[] = [];
    const { store, backend: signalBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { if (points.some((point) => point.payload.record_type === "collection_control")) upserts.push(mode); } });
    const baseline = await readControl(store);
    expect(baseline.state).toBe("active");
    // Structural {aborted:false} without real methods: rejected pre-mutation.
    await expect(rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 100, now: NOW_MS, signal: { aborted: false } as never })).rejects.toThrow(/genuine AbortSignal|Signal/i);
    expect(upserts).toEqual([]);
    // Nonfunction method surface: rejected.
    await expect(rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 100, now: NOW_MS, signal: { aborted: false, addEventListener: "nope", removeEventListener: "nope" } as never })).rejects.toThrow(/Signal/i);
    expect(upserts).toEqual([]);
    // Throwing method getters: rejected (fail-closed before any use).
    const throwing = { aborted: false, get addEventListener() { throw new Error("getter touched"); }, get removeEventListener() { throw new Error("getter touched"); } };
    await expect(rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 100, now: NOW_MS, signal: throwing as never })).rejects.toThrow();
    expect(upserts).toEqual([]);
    const after = await readControl(store);
    expect(after.state).toBe("active");
    expect(after.version).toBe(1);
    // Object.create(AbortSignal.prototype) with OWN working methods + false
    // aborted: the captured NATIVE internal-slot getter rejects it BEFORE any
    // of its own properties/getters are touched (addReads stays 0) and the
    // control is untouched.
    let addReads = 0;
    const fakeSignal = Object.create(AbortSignal.prototype, {
      aborted: { value: false, configurable: true },
      addEventListener: { get() { addReads += 1; return () => undefined; }, configurable: true },
      removeEventListener: { value: () => undefined, configurable: true },
    });
    await expect(rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS, signal: fakeSignal as unknown as AbortSignal })).rejects.toThrow(/genuine AbortSignal/i);
    expect(addReads).toBe(0);
    const afterImposter = await readControl(store);
    expect(afterImposter.state).toBe("active");
    expect(afterImposter.version).toBe(1);
    expect(upserts).toEqual([]);
    // A GENUINE AbortController.signal with a positive wait works end-to-end.
    const controller = new AbortController();
    const rotated = await rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 5, now: NOW_MS, signal: controller.signal });
    expect(rotated.state).toBe("active");
    expect(rotated.coordinationPolicyEpoch).toBe(2);
    expect(rotated.coordinationPolicyHash).toBe("policy-hash-2");
  });


  it("native internal-slot signal check: Proxy traps and own property overrides are never invoked pre-mutation", async () => {
    const upserts: string[] = [];
    const { store, backend: trapBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { if (points.some((point) => point.payload.record_type === "collection_control")) upserts.push(mode); } });
    // A Proxy pretending to carry the old symbol brand + traps on EVERY access:
    // the WeakSet brand check + the captured NATIVE getter reject it without
    // triggering any proxy trap.
    let ownTrapCount = 0;
    const trapSignal = new Proxy({}, {
      get(_t, prop) { ownTrapCount += 1; return undefined; },
      has(_t, prop) { ownTrapCount += 1; return false; },
      getOwnPropertyDescriptor(_t, prop) { ownTrapCount += 1; return undefined; },
      ownKeys() { ownTrapCount += 1; return []; },
      getPrototypeOf() { ownTrapCount += 1; return AbortSignal.prototype; },
    }) as unknown as AbortSignal;
    await expect(rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 0, now: NOW_MS, signal: trapSignal })).rejects.toThrow(/genuine AbortSignal/i);
    // Our code never reads a property from the object: the only observable
    // access is the ENGINE's internal slot probe inside the captured native
    // getter (unavoidable), which returns undefined and rejects. The control
    // is untouched and zero writes happened.
    expect(ownTrapCount).toBeLessThanOrEqual(1);
    expect(upserts).toEqual([]);
    const after = await readControl(store);
    expect(after.state).toBe("active");
    expect(after.version).toBe(1);
    // A genuine AbortController.signal with own THROWING overrides: the native
    // captured getter/methods are used, the overrides are never invoked, and
    // the positive LLM wait works (abort mid-wait also cancels cleanly).
    const controller = new AbortController();
    Object.defineProperty(controller.signal, "aborted", { get() { throw new Error("own aborted getter invoked"); } });
    Object.defineProperty(controller.signal, "addEventListener", { value() { throw new Error("own addEventListener invoked"); } });
    const rotated = await rotateCoordinationPolicy(store, { nextPolicyHash: "policy-hash-2", maxLeaseMs: 30000, maxClockSkewMs: 0, memoryModelTimeoutMs: 5, now: NOW_MS, signal: controller.signal });
    expect(rotated.state).toBe("active");
    expect(rotated.coordinationPolicyEpoch).toBe(2);
  });


  it("quiescence scans AT the deadline boundary: worst-case lease expires exactly at lease+skew and succeeds", async () => {
    // A draining control + a worst-case old lease expiring exactly at
    // started + maxLeaseMs. At current === deadline (= started + lease + skew)
    // the lease is conservatively expired (expiresAt + skew <= current) and
    // the scan must quiesce — NOT false-time-out.
    const { store } = fakeStoreWithBackend({ leases: [lease("job-1", { expiresAt: iso(NOW_MS + 30000) })] });
    await beginPolicyDrain(store, { now: NOW_MS });
    // The clock is AT the deadline from the very first scan: the lease is
    // conservatively expired exactly at the boundary and quiescence succeeds
    // in one iteration (no sleep, no false timeout).
    const proof = await waitForOldLeasesToQuiesce(store, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, now: () => NOW_MS + 30000, pollIntervalMs: 1 });
    expect(QuiescenceProof.isValid(proof)).toBe(true);
    // The rotation completes (activate succeeds) after the boundary success.
    const rotated = await activatePolicyEpoch(store, { proof, nextPolicyHash: "policy-hash-2", memoryModelTimeoutMs: 0 });
    expect(rotated.state).toBe("active");
    expect(rotated.coordinationPolicyEpoch).toBe(2);
    // Same lease + SKEW boundary: expiresAt + skew === deadline -> still quiesced.
    const { store: storeSkew } = fakeStoreWithBackend({ leases: [lease("job-1", { expiresAt: iso(NOW_MS + 30000) })] });
    await beginPolicyDrain(storeSkew, { now: NOW_MS });
    const proofSkew = await waitForOldLeasesToQuiesce(storeSkew, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 5000, now: () => NOW_MS + 35000, pollIntervalMs: 1 });
    expect(QuiescenceProof.isValid(proofSkew)).toBe(true);
    // 1ms beyond the deadline: NOT quiesced at the boundary -> timeout (the
    // collection stays draining operationally).
    const { store: storeBeyond } = fakeStoreWithBackend({ leases: [lease("job-1", { expiresAt: iso(NOW_MS + 30001) })] });
    await beginPolicyDrain(storeBeyond, { now: NOW_MS });
    // The clock advances to the deadline on the second scan: the lease is NOT
    // conservatively expired there (1ms beyond), so the scan at the boundary
    // does not quiesce and the deadline throws (no busy loop).
    let beyondCalls = 0;
    const beyondNow = () => { beyondCalls += 1; return beyondCalls === 1 ? NOW_MS : NOW_MS + 30000; };
    await expect(waitForOldLeasesToQuiesce(storeBeyond, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, now: beyondNow, pollIntervalMs: 1 })).rejects.toThrow(/deadline/i);
    const afterBeyond = await readControl(storeBeyond);
    expect(afterBeyond.state).toBe("draining");
    // Already-quiescent with timeoutMs 0: one scan at the start succeeds.
    const { store: storeQuiet } = fakeStoreWithBackend();
    await beginPolicyDrain(storeQuiet, { now: NOW_MS });
    const proofQuiet = await waitForOldLeasesToQuiesce(storeQuiet, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, timeoutMs: 0, now: () => NOW_MS, pollIntervalMs: 1 });
    expect(QuiescenceProof.isValid(proofQuiet)).toBe(true);
  });


  it("EXACT-ONCE in the control authority block: swapped retiredEpoch/skew/proof/hash getters are read once and the first snapshot wins", async () => {
    // (1) Quiescence: retiredEpoch getter returns 1 first, 99 later. The
    // pinned check + the lease filter use ONLY the snapshotted epoch — a
    // re-read would see 99 and fail the pin or skip the live lease.
    const { store: qStore } = fakeStoreWithBackend({ leases: [lease("job-1", { expiresAt: iso(NOW_MS + 1000) })] });
    await beginPolicyDrain(qStore, { now: NOW_MS });
    let epochReads = 0;
    let nowCalls = 0;
    const qInput = {
      get retiredEpoch() { epochReads += 1; return epochReads === 1 ? 1 : 99; },
      maxLeaseMs: 30000,
      maxClockSkewMs: 0,
      pollIntervalMs: 1,
      now: () => { nowCalls += 1; return nowCalls === 1 ? NOW_MS : NOW_MS + 1000; },
    };
    const qProof = await waitForOldLeasesToQuiesce(qStore, qInput as never);
    expect(QuiescenceProof.isValid(qProof)).toBe(true);
    expect(epochReads).toBe(1);
    // (2) Quiescence skew: first 5000, later 0. The expiry uses the pinned
    // skew — a lease expiring NOW+1000 quiesces at NOW+1000 with skew 5000,
    // and would NOT quiesce with a re-read 0.
    const { store: sStore } = fakeStoreWithBackend({ leases: [lease("job-1", { expiresAt: iso(NOW_MS + 1000) })] });
    await beginPolicyDrain(sStore, { now: NOW_MS });
    let skewReads = 0;
    const sProof = await waitForOldLeasesToQuiesce(sStore, { retiredEpoch: 1, maxLeaseMs: 30000, get maxClockSkewMs() { skewReads += 1; return skewReads === 1 ? 5000 : 0; }, now: () => NOW_MS + 6000, pollIntervalMs: 1 } as never);
    expect(QuiescenceProof.isValid(sProof)).toBe(true);
    expect(skewReads).toBe(1);
    // (3) Activation: proof getter genuine-first/fake-later + hash getter
    // valid-first/different-later + timeout getter large-first/zero-later.
    // All reads happen exactly once; the FIRST snapshots drive the entire
    // activation (brand check, matches, sleep, next record, winner compare).
    const { store: aStore } = fakeStoreWithBackend();
    await beginPolicyDrain(aStore, { now: NOW_MS });
    const genuineProof = await waitForOldLeasesToQuiesce(aStore, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0, now: () => NOW_MS, pollIntervalMs: 1 });
    const fakeProof = Object.create(QuiescenceProof.prototype) as QuiescenceProof;
    let proofReads = 0;
    let hashReads = 0;
    let timeoutReads = 0;
    const aInput = {
      get proof() { proofReads += 1; return proofReads === 1 ? genuineProof : fakeProof; },
      get nextPolicyHash() { hashReads += 1; return hashReads === 1 ? "policy-hash-2" : "policy-hash-X"; },
      get memoryModelTimeoutMs() { timeoutReads += 1; return timeoutReads === 1 ? 5 : 0; },
    };
    const activated = await activatePolicyEpoch(aStore, aInput as never);
    expect(activated.state).toBe("active");
    expect(activated.coordinationPolicyEpoch).toBe(2);
    expect(activated.coordinationPolicyHash).toBe("policy-hash-2"); // the FIRST snapshot won
    expect(proofReads).toBe(1);
    expect(hashReads).toBe(1);
    expect(timeoutReads).toBe(1);
  });


  it("SECURITY AUTHORITY FIRST: the capability is branded+scoped before ANY other caller input getter fires", async () => {
    const upserts: string[] = [];
    const { store, backend: orderBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { upserts.push(mode); } });
    // M1: a Proxy input for activation whose proof getter returns a FORGED
    // proof and whose signal/hash/timeout getters all THROW if touched: only
    // the proof getter may fire (the authority reject happens first).
    let proofReads = 0;
    let otherReads = 0;
    const activateInput = new Proxy({}, {
      get(_t, prop) {
        if (prop === "proof") { proofReads += 1; return Object.create(QuiescenceProof.prototype); }
        otherReads += 1;
        throw new Error("unrelated getter touched: " + String(prop));
      },
      has(_t, prop) { if (prop === "proof") return true; otherReads += 1; return false; },
    });
    await expect(activatePolicyEpoch(store, activateInput as never)).rejects.toThrow(/authority|proof/i);
    expect(proofReads).toBe(1);
    expect(otherReads).toBe(0);
    expect(upserts).toEqual([]);
    // M2: a direct store.acceptProposal with a structural fake authority + an
    // input whose proposalId getter THROWS: the authority brand rejects before
    // the input is touched.
    const fakeAuthority = Object.create(LeaseAuthority.prototype) as LeaseAuthority;
    let proposalIdReads = 0;
    const fakeInput = new Proxy({}, { get(_t, prop) { proposalIdReads += 1; throw new Error("input getter touched: " + String(prop)); }, has() { proposalIdReads += 1; return false; } });
    await expect(store.acceptProposal(fakeAuthority, fakeInput as never)).rejects.toThrow(/authority/i);
    expect(proposalIdReads).toBe(0);
    // Table ordering across the capability entries: a forged authority is
    // rejected before any unrelated input/capability getter.
    const throwingMembership = { get membership() { throw new Error("membership getter touched"); } };
    await expect(store.writeProposal(fakeAuthority, throwingMembership as never)).rejects.toThrow(/authority/i);
    await expect(store.renewLease(fakeAuthority)).rejects.toThrow(/authority/i);
    await expect(store.releaseLease(fakeAuthority)).rejects.toThrow(/authority/i);
    await expect(store.acceptLease(fakeAuthority, "p")).rejects.toThrow(/authority/i);
    await expect(store.activatePolicyEpoch({ proof: Object.create(QuiescenceProof.prototype), nextPolicyHash: "h", memoryModelTimeoutMs: 0 } as never)).rejects.toThrow(/authority|proof/i);
    expect(upserts).toEqual([]);
  });


  it("EXACT-ONCE persistence: writeProposal/tombstone/createJob getter swaps are read once and the FIRST validated values persist", async () => {
    const upserts: string[] = [];
    const { store, backend: persistBackend } = fakeStoreWithBackend({}, { onUpsert: (points, mode) => { upserts.push(mode); } });
    const job = await createJob(store, { ownerHost: OWNER, membership: ["episode-1"], policyIntersectionId: INTERSECTION_ID, policyHash: POLICY_HASH, policyEpoch: 1, extractorRevision: EXTRACTOR, privacyEpoch: 0, createdAt: NOW });
    const w = workerAt("node-a", NOW_MS);
    const authority = await claimLease(store, w, { jobId: job.id, policyEpoch: 1, policyHash: POLICY_HASH, privacyEpoch: 0 });
    expect(authority).not.toBeNull();
    // writeProposal: membership/content/createdAt getters return A first then B.
    // Each is read EXACTLY ONCE; the persisted proposal (hash/manifest/membership/
    // content) matches A and the job.
    let membershipReads = 0;
    let contentReads = 0;
    let createdAtReads = 0;
    const proposalInput = {
      get membership() { membershipReads += 1; return membershipReads === 1 ? ["episode-1"] : ["episode-9"]; },
      get content() { contentReads += 1; return contentReads === 1 ? { summary: "safe A" } : { summary: "tampered B" }; },
      get createdAt() { createdAtReads += 1; return createdAtReads === 1 ? NOW : "2099-01-01T00:00:00.000Z"; },
    };
    const proposal = await writeProposal(store, authority!, proposalInput as never);
    expect(membershipReads).toBe(1);
    expect(contentReads).toBe(1);
    expect(createdAtReads).toBe(1);
    expect(proposal.membership).toEqual(["episode-1"]);
    expect(proposal.content).toEqual({ summary: "safe A" });
    expect(proposal.createdAt).toBe(NOW);
    expect(proposal.ownerId).toBe("node-a");
    expect(proposal.proposalHash).toBe(proposalHashFor({ ownerHost: OWNER, jobId: job.id, ownerId: "node-a", membership: ["episode-1"], content: { summary: "safe A" }, policyHash: POLICY_HASH, policyEpoch: 1, fencingToken: 1, privacyEpoch: 0, policyIntersectionId: INTERSECTION_ID }));
    // createTombstone: scope/target/owner/policy/epoch/targetKind/provenance
    // getters swap valid->invalid; each read once; the FIRST validated target
    // is the persisted one.
    const ep = episode({ id: "00000000-0000-5000-8000-000000000001" });
    const vector = Array.from({ length: 1024 }, () => 0.25);
    const withVector = { ...ep, vector: [...vector] } as EpisodeRecord;
    const finalEp = { ...withVector, contentHash: canonicalRecordHash(withVector) } as EpisodeRecord;
    persistBackend.points.set(finalEp.id, { id: finalEp.id, payload: recordPayload(finalEp) as Record<string, unknown>, vector: { semantic: [...vector] } });
    let scopeReads = 0;
    let targetReads = 0;
    let ownerReads = 0;
    let policyReads = 0;
    let epochReads = 0;
    let kindReads = 0;
    let provenanceReads = 0;
    const tombInput = {
      get ownerHost() { ownerReads += 1; return ownerReads === 1 ? "pi" : "prime"; },
      get scope() { scopeReads += 1; return scopeReads === 1 ? "occurrence" : "content"; },
      get targetId() { targetReads += 1; return targetReads === 1 ? finalEp.id : "00000000-0000-5000-8000-000000000099"; },
      get targetKind() { kindReads += 1; return kindReads === 1 ? "episode" : undefined; },
      get createdAt() { return NOW; },
      get privacyEpoch() { epochReads += 1; return epochReads === 1 ? 0 : 9; },
      get processingPolicyId() { policyReads += 1; return policyReads === 1 ? INTERSECTION_ID : "other-policy"; },
      get provenanceIds() { provenanceReads += 1; return provenanceReads === 1 ? [] : ["00000000-0000-5000-8000-000000000099"]; },
    };
    const tombstones = await createTombstone(store, tombInput as never);
    expect(ownerReads).toBe(1);
    expect(scopeReads).toBe(1);
    expect(targetReads).toBe(1);
    expect(epochReads).toBe(1);
    expect(policyReads).toBe(1);
    expect(kindReads).toBe(1);
    expect(provenanceReads).toBe(1);
    expect(tombstones).toHaveLength(1);
    expect(tombstones[0]?.targetId).toBe(finalEp.id);
    const reread = await readTombstones(store, [finalEp.id]);
    expect(reread).toHaveLength(1);
    expect(reread[0]?.targetId).toBe(finalEp.id);
  });

});
