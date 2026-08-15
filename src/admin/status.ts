import { MemoryClientError } from "../clients/http.js";
import { readPolicy, type QdrantClientOptions } from "../qdrant/client.js";
import { statusCollectionInfo, statusHealth, statusRetrieve, statusCount } from "./transport.js";
import { COLLECTION_CONTROL_ID, COLLECTION_METADATA_ID, controlRecordFromPayload, isCollectionMetadataPayload } from "../qdrant/schema.js";
import type { RuntimeConfig } from "../types.js";

export interface MemoryStatusAudit {
  metadata?: { ownerHost?: RuntimeConfig["host"]; schemaRevision?: number; vector?: { name: string; dimension: number; distance: string }; pointCount?: number | null };
  policy?: { hash?: string; mismatch?: boolean };
  outbox?: { jobs?: number; bytes?: number; oldestAt?: string | null; failures?: number };
  coverage?: { missing?: number; oldestAt?: string | null; lastReconcileAt?: string | null };
  jobs?: { queued?: number; leased?: number; failed?: number };
  generation?: { active?: string | null; manifestHash?: string | null; levels?: number; orphans?: number };
  privacy?: { epoch?: number; revokedDestinationIds?: readonly string[] };
  records?: Record<string, number>;
  embeddingHealthy?: boolean;
  dedicatedLlmAvailable?: boolean;
  fallbackLlmAvailable?: boolean;
  lastErrorCategory?: string | null;
}

export interface MemoryStatus {
  host: RuntimeConfig["host"];
  configPath: string;
  enabled: boolean;
  autoRecall: boolean;
  destination: {
    endpoint: string;
    collection: string;
    ownerHost: RuntimeConfig["host"];
    schema: "pi-qdrant-memory-v2";
    dimension: 1024;
    distance: "Dot";
    exists: boolean;
    healthy: boolean;
    keyConfigured: boolean;
    authMode?: "configured" | "not_configured";
    pointCount?: number | null;
  };
  embeddings: {
    endpoint: string;
    model: string;
    dimension: 1024;
    healthy: boolean;
    keyConfigured: boolean;
    authMode?: "configured" | "not_configured";
  };
  capture: {
    enabled: boolean;
    episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
    explicitRetention?: boolean;
    explicitEgress?: boolean;
  };
  privacy: {
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    qdrantDestinations: number;
    embeddingDestinations: number;
    llmDestinations: number;
    epoch?: number;
    revokedDestinationIds?: readonly string[];
  };
  qdrant: {
    healthy: boolean;
    destinationHealthy: boolean;
    probed: boolean;
  };
  /** Redacted operational audit fields. No API keys, record payloads or text. */
  metadata?: { ownerHost: RuntimeConfig["host"]; schemaRevision: number; vector: { name: "semantic"; dimension: 1024; distance: "Dot" }; pointCount: number | null };
  policy?: { hash: string | null; mismatch: boolean };
  projects?: { registered: number; aliases: readonly string[] };
  scopes?: { root: RuntimeConfig["retrieval"]["rootScope"]; childSearch: boolean };
  outbox?: { jobs: number; bytes: number; oldestAt: string | null; failures: number };
  coverage?: { missing: number; oldestAt: string | null; lastReconcileAt: string | null };
  jobs?: { queued: number; leased: number; failed: number };
  generation?: { active: string | null; manifestHash: string | null; levels: number; orphans: number };
  recordCounts?: Readonly<Record<string, number>>;
  embeddingHealth?: boolean;
  dedicatedLlmAvailable?: boolean;
  fallbackLlmAvailable?: boolean;
  lastErrorCategory?: string | null;
}

export interface MemoryStatusDependencies {
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
  audit?(): Promise<MemoryStatusAudit> | MemoryStatusAudit;
}
function isMissing(error: unknown): boolean { return error instanceof MemoryClientError && error.category === "http" && error.status === 404; }
function safeCount(value: number | undefined, fallback = 0): number { return value !== undefined && Number.isSafeInteger(value) && value >= 0 ? value : fallback; }
function safeDate(value: string | null | undefined): string | null { if (value === null || value === undefined) return null; return typeof value === "string" && Number.isFinite(Date.parse(value)) ? new Date(value).toISOString() : null; }
function safeHash(value: string | undefined): string | null { return value !== undefined && /^[a-f0-9]{64}$/u.test(value) ? value : null; }
function safeRedactedId(value: string): string | null { return typeof value === "string" && value.length > 0 && value.length <= 256 && !/(?:api[-_]?key|token|secret|password)/iu.test(value) ? value : null; }
function endpointOrigin(value: string): string { try { return new URL(value).origin; } catch { return value; } }

/** Probe only the configured host collection with its collection-scoped key. */
export async function memoryStatus(config: RuntimeConfig, deps: MemoryStatusDependencies = {}): Promise<MemoryStatus> {
  const base: MemoryStatus = {
    host: config.host, configPath: config.configPath, enabled: config.enabled, autoRecall: config.autoRecall,
    destination: { endpoint: endpointOrigin(config.qdrant.url), collection: config.qdrant.collection, ownerHost: config.host, schema: "pi-qdrant-memory-v2", dimension: 1024, distance: "Dot", exists: false, healthy: false, keyConfigured: config.qdrant.apiKey !== undefined, authMode: config.qdrant.apiKey === undefined ? "not_configured" : "configured", pointCount: null },
    embeddings: { endpoint: endpointOrigin(config.embeddings.baseUrl), model: config.embeddings.model, dimension: config.embeddings.dimension, healthy: false, keyConfigured: config.embeddings.apiKey !== undefined, authMode: config.embeddings.apiKey === undefined ? "not_configured" : "configured" },
    capture: { enabled: config.capture.enabled, episodeRetentionDays: config.capture.episodeRetentionDays },
    privacy: { egressMode: config.privacy.egressMode, qdrantDestinations: config.privacy.allowedQdrantDestinations.length, embeddingDestinations: config.privacy.allowedEmbeddingDestinations.length, llmDestinations: config.privacy.allowedLlmDestinations.length },
    qdrant: { healthy: false, destinationHealthy: false, probed: false },
    metadata: { ownerHost: config.host, schemaRevision: 1, vector: { name: "semantic", dimension: 1024, distance: "Dot" }, pointCount: null },
    policy: { hash: null, mismatch: false },
    projects: { registered: Object.keys(config.projects.registrations).length, aliases: Object.freeze(Object.keys(config.projects.registrations).sort()) },
    scopes: { root: config.retrieval.rootScope, childSearch: config.retrieval.childSearch },
    outbox: { jobs: 0, bytes: 0, oldestAt: null, failures: 0 },
    coverage: { missing: 0, oldestAt: null, lastReconcileAt: null },
    jobs: { queued: 0, leased: 0, failed: 0 },
    generation: { active: null, manifestHash: null, levels: 0, orphans: 0 },
    recordCounts: Object.freeze({}),
    embeddingHealth: false,
    dedicatedLlmAvailable: false,
    fallbackLlmAvailable: false,
    lastErrorCategory: null,
  };
  if (deps.audit !== undefined) {
    const audit = await deps.audit();
    if (audit.metadata !== undefined) {
      const pointCount = audit.metadata.pointCount === null || audit.metadata.pointCount === undefined ? null : safeCount(audit.metadata.pointCount);
      base.metadata = { ownerHost: audit.metadata.ownerHost === config.host ? config.host : base.metadata!.ownerHost, schemaRevision: audit.metadata.schemaRevision === 1 ? 1 : base.metadata!.schemaRevision, vector: audit.metadata.vector?.name === "semantic" && audit.metadata.vector.dimension === 1024 && audit.metadata.vector.distance === "Dot" ? { name: "semantic", dimension: 1024, distance: "Dot" } : base.metadata!.vector, pointCount };
      base.destination.pointCount = pointCount;
    }
    if (audit.policy !== undefined) base.policy = { hash: safeHash(audit.policy.hash), mismatch: audit.policy.mismatch === true };
    if (audit.outbox !== undefined) base.outbox = { jobs: safeCount(audit.outbox.jobs), bytes: safeCount(audit.outbox.bytes), oldestAt: safeDate(audit.outbox.oldestAt), failures: safeCount(audit.outbox.failures) };
    if (audit.coverage !== undefined) base.coverage = { missing: safeCount(audit.coverage.missing), oldestAt: safeDate(audit.coverage.oldestAt), lastReconcileAt: safeDate(audit.coverage.lastReconcileAt) };
    if (audit.jobs !== undefined) base.jobs = { queued: safeCount(audit.jobs.queued), leased: safeCount(audit.jobs.leased), failed: safeCount(audit.jobs.failed) };
    if (audit.generation !== undefined) base.generation = { active: audit.generation.active === null || audit.generation.active === undefined ? null : safeRedactedId(audit.generation.active), manifestHash: audit.generation.manifestHash === null || audit.generation.manifestHash === undefined ? null : safeHash(audit.generation.manifestHash), levels: safeCount(audit.generation.levels), orphans: safeCount(audit.generation.orphans) };
    if (audit.privacy !== undefined) { const revoked = Object.freeze((audit.privacy.revokedDestinationIds ?? []).map(safeRedactedId).filter((value): value is string => value !== null).sort()); base.privacy = { ...base.privacy, ...(audit.privacy.epoch === undefined ? {} : { epoch: safeCount(audit.privacy.epoch) }), revokedDestinationIds: revoked }; }
    if (audit.records !== undefined) {
      const entries = Object.entries(audit.records).filter(([key, value]) => /^[a-z_]+$/u.test(key) && Number.isSafeInteger(value) && (value as number) >= 0).map(([key, value]) => [key, value as number] as const);
      entries.sort((left, right) => left[0].localeCompare(right[0]));
      base.recordCounts = Object.freeze(Object.fromEntries(entries));
    }
    if (audit.embeddingHealthy !== undefined) base.embeddingHealth = audit.embeddingHealthy === true;
    if (audit.dedicatedLlmAvailable !== undefined) base.dedicatedLlmAvailable = audit.dedicatedLlmAvailable === true;
    if (audit.fallbackLlmAvailable !== undefined) base.fallbackLlmAvailable = audit.fallbackLlmAvailable === true;
    if (audit.lastErrorCategory !== undefined) base.lastErrorCategory = audit.lastErrorCategory === null ? null : safeRedactedId(audit.lastErrorCategory);
  }
  if (deps.fetchImpl === undefined) return base;
  const statusOptions: QdrantClientOptions = { baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, ...(config.qdrant.apiKey === undefined ? {} : { apiKey: config.qdrant.apiKey }), timeoutMs: config.retrieval.timeoutMs, ...(deps.signal === undefined ? {} : { signal: deps.signal }), readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs };
  base.qdrant.probed = true;
  try { await statusHealth(statusOptions, deps.fetchImpl); base.qdrant.healthy = true; } catch { base.lastErrorCategory = "health_unavailable"; }
  try {
    const info = await statusCollectionInfo(statusOptions, deps.fetchImpl);
    base.destination.exists = true; base.destination.pointCount = info.pointsCount; base.metadata = { ...base.metadata!, pointCount: info.pointsCount }; base.qdrant.destinationHealthy = false;
    const metadata = await statusRetrieve(statusOptions, deps.fetchImpl, [COLLECTION_METADATA_ID], readPolicy({ ownerHost: config.host, purpose: "metadata", recordTypes: ["collection_metadata"], maxClockSkewMs: config.coordination.maxClockSkewMs }));
    base.destination.healthy = metadata.length === 1 && isCollectionMetadataPayload(metadata[0]!.payload, config.host); base.qdrant.destinationHealthy = base.destination.healthy;
    if (!base.destination.healthy) base.lastErrorCategory = "metadata_mismatch";
    if (base.destination.healthy) {
      try {
        const controlPoints = await statusRetrieve(statusOptions, deps.fetchImpl, [COLLECTION_CONTROL_ID], readPolicy({ ownerHost: config.host, purpose: "control", recordTypes: ["collection_control"], maxClockSkewMs: config.coordination.maxClockSkewMs }));
        if (controlPoints.length !== 1) throw new Error("control readback is ambiguous");
        const control = controlRecordFromPayload(controlPoints[0]!.payload, config.host);
        base.privacy = { ...base.privacy, epoch: control.privacyEpoch, revokedDestinationIds: Object.freeze([...control.revokedDestinationIds].sort()) };
        base.policy = { hash: control.coordinationPolicyHash, mismatch: control.ownerHost !== config.host };
        base.generation = { active: control.activeGeneration, manifestHash: null, levels: 0, orphans: 0 };
        const countTypes = ["episode", "curated_memory", "curated_current", "raptor_summary", "job", "lease"] as const;
        const counts: Record<string, number> = {};
        for (const recordType of countTypes) {
          try {
            const purpose = ["job", "lease"].includes(recordType) ? "internal" as const : "query" as const;
            counts[recordType] = await statusCount(statusOptions, deps.fetchImpl, readPolicy({ ownerHost: config.host, purpose, recordTypes: [recordType], maxClockSkewMs: config.coordination.maxClockSkewMs }));
          } catch { counts[recordType] = 0; }
        }
        base.recordCounts = Object.freeze(Object.fromEntries(Object.entries(counts).sort(([a], [b]) => a.localeCompare(b))));
        base.jobs = { queued: counts.job ?? 0, leased: counts.lease ?? 0, failed: 0 };
      } catch { base.lastErrorCategory = "control_mismatch"; }
    }
  } catch (error: unknown) {
    if (isMissing(error)) base.destination.exists = false; else if (base.lastErrorCategory === null) base.lastErrorCategory = "destination_unavailable";
  }
  return base;
}
