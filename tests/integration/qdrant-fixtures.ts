import { createHash, randomBytes } from "node:crypto";
import { createServer, type Server } from "node:http";
import { initializeDestination } from "../../src/admin/init.js";
import { bindQdrantDestination, createQdrantCoordinationStore, createQdrantSafeBundle, type BoundQdrantDestination, type ProductionCoordinationStore } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient, type BoundEmbeddingDestination } from "../../src/clients/embeddings.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type ProcessingPolicyRecord } from "../../src/domain/records.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import type { AuthorizedDestination, HostId, RuntimeConfig } from "../../src/types.js";

export const ISOLATED_QDRANT_VERSION = "1.17.1" as const;
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "::1"]);
const RUN_ID_PATTERN = /^[a-z0-9]{12,32}$/u;
const RETIRED_SOURCE = ["hermes", "_memory"].join("");

/**
 * Return only an explicitly provisioned loopback endpoint. Integration tests
 * intentionally have no fallback: a missing run ID or URL must fail before a
 * request can reach a configured or remote collection.
 */
export function isolatedQdrantUrl(env: Record<string, string | undefined>): string {
  const raw = env.PI_QDRANT_MEMORY_TEST_QDRANT_URL;
  const runId = env.PI_QDRANT_MEMORY_TEST_RUN_ID;
  if (raw === undefined || runId === undefined || !RUN_ID_PATTERN.test(runId)) throw new Error("isolated run ID and URL required");
  let parsed: URL;
  try { parsed = new URL(raw); } catch { throw new Error("isolated Qdrant URL is invalid"); }
  const hostname = parsed.hostname.replace(/^\[|\]$/gu, "");
  if (!LOOPBACK_HOSTS.has(hostname)) throw new Error("loopback Qdrant required");
  if (!(["http:", "https:"] as readonly string[]).includes(parsed.protocol) || parsed.username !== "" || parsed.password !== "" || parsed.search !== "" || parsed.hash !== "") throw new Error("isolated Qdrant URL must be a plain HTTP endpoint");
  if (parsed.port === "6333" && env.CI !== "true") throw new Error("default Qdrant port refused outside CI");
  if (parsed.pathname.toLowerCase().includes(RETIRED_SOURCE)) throw new Error("source collection refused");
  return parsed.href.replace(/\/$/u, "");
}

export function isolatedRunId(env: Record<string, string | undefined>): string {
  const value = env.PI_QDRANT_MEMORY_TEST_RUN_ID;
  if (value === undefined || !RUN_ID_PATTERN.test(value)) throw new Error("isolated run ID and URL required");
  return value;
}

export function isolatedCollection(host: HostId): "pi_memory" | "prime_memory" {
  return host === "pi" ? "pi_memory" : "prime_memory";
}

export function isolatedOptions(url: string, host: HostId): QdrantClientOptions {
  return { baseUrl: url, collection: isolatedCollection(host), ownerHost: host, timeoutMs: 5_000, readConsistency: 1, maxClockSkewMs: 0 };
}

/** Minimal complete runtime config consumed by destination initialization. */
export function isolatedConfig(url: string, host: HostId): RuntimeConfig {
  return {
    host, enabled: true, autoRecall: true, configPath: `/tmp/pi-qdrant-memory-task14-${host}.json`,
    qdrant: { url, collection: isolatedCollection(host), replicationFactor: 1, writeConsistencyFactor: 1 },
    embeddings: { baseUrl: "http://127.0.0.1:1/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: " },
    retrieval: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0.05, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16_000, timeoutMs: 5_000, rootScope: "project", childSearch: true },
    projects: { registrations: {} },
    capture: { enabled: false, projectAllowlist: [], projectDenylist: [], episodeRetentionDays: "indefinite", toolArgsChars: 2_000, toolResultChars: 4_000 },
    privacy: { egressMode: "local_only", allowedQdrantDestinations: [], allowedEmbeddingDestinations: [], allowedLlmDestinations: [], allowActiveModelFallback: false, allowCrossProviderReplay: false },
    coordination: { maxClockSkewMs: 0, readConsistency: 1, leaseMs: 30_000, reconcileIntervalMs: 900_000 },
    outbox: { maxJobs: 10_000, maxBytes: 268_435_456, retryBaseMs: 500, retryMaxMs: 30_000, sharedFilesystem: false },
    curation: { turnTrigger: 10, toolTrigger: 15, maxInputTokens: 12_000 }, memoryModel: { timeoutMs: 30_000, maxOutputTokens: 2_048 },
    raptor: { rebuildEpisodeDelta: 64, maxLevels: 5, summaryInputTokens: 12_000, umapDimensions: 10, localNeighbors: 10, gmmMaxClusters: 50, membershipThreshold: 0.1 },
  };
}

export async function qdrantVersion(url: string): Promise<string> {
  const response = await fetch(`${url}/`);
  if (!response.ok) throw new Error(`Qdrant version probe failed: ${response.status}`);
  const body = await response.json() as { version?: unknown };
  if (body.version !== ISOLATED_QDRANT_VERSION) throw new Error(`Qdrant ${String(body.version)} is not ${ISOLATED_QDRANT_VERSION}`);
  return body.version;
}

export async function waitForIsolatedQdrant(url: string, attempts = 30): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try { await qdrantVersion(url); return; } catch { await new Promise(resolve => setTimeout(resolve, 500)); }
  }
  throw new Error(`Qdrant ${ISOLATED_QDRANT_VERSION} did not become ready`);
}

const freshServers = new Set<string>();
export function isolatedSentinel(runId: string): string {
  if (!RUN_ID_PATTERN.test(runId)) throw new Error("isolated run ID and URL required");
  return `task14_guard_${runId}`;
}
export async function assertFreshIsolatedServer(url: string, env: Record<string, string | undefined> = process.env): Promise<void> {
  const runId = isolatedRunId(env);
  const key = `${url}|${runId}`;
  if (freshServers.has(key)) return;
  const response = await fetch(`${url}/collections`);
  if (!response.ok) throw new Error(`isolated Qdrant collection-list probe failed: ${response.status}`);
  const body = await response.json() as { result?: { collections?: unknown } };
  const collections = body.result?.collections;
  const names = Array.isArray(collections) ? collections.map((entry) => typeof entry === "object" && entry !== null ? (entry as { name?: unknown }).name : undefined) : [];
  if (names.length !== 1 || names[0] !== isolatedSentinel(runId)) throw new Error(`isolated Qdrant endpoint is not bound to fresh run ${runId}`);
  freshServers.add(key);
}

export async function initializeIsolated(url: string, host: HostId): Promise<ReturnType<typeof createQdrantCoordinationStore>> {
  await assertFreshIsolatedServer(url);
  const config = isolatedConfig(url, host);
  const debugFetch: typeof fetch | undefined = process.env.PI_QDRANT_MEMORY_TEST_DEBUG === "true" ? async (input, init) => {
    const response = await fetch(input, init);
    if (process.env.PI_QDRANT_MEMORY_TEST_DEBUG === "true") console.error("task14 qdrant response", response.status, String(input), await response.clone().text());
    return response;
  } : undefined;
  await initializeDestination(config, { adminApiKey: `task14-${isolatedRunId(process.env)}`, now: () => Date.parse("2026-08-15T00:00:00.000Z"), retryAttempts: 6, retryDelayMs: 100, ...(debugFetch === undefined ? {} : { fetchImpl: debugFetch }) });
  return createQdrantCoordinationStore(isolatedOptions(url, host));
}

export function deterministicUuid(seed: string): string {
  const digest = createHash("sha256").update(seed, "utf8").digest("hex");
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-8${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

export function randomRunId(): string { return randomBytes(8).toString("hex"); }

/** Fixed redacted destination identities shared by the isolated matrix. */
export const TASK14_QDRANT_DESTINATION: AuthorizedDestination = Object.freeze({ id: "qdrant:task14", residency: "local", dataUse: "memory" });
export const TASK14_EMBEDDING_DESTINATION: AuthorizedDestination = Object.freeze({ id: "embed:task14", residency: "local", dataUse: "memory" });
export const TASK14_LLM_DESTINATION: AuthorizedDestination = Object.freeze({ id: "llm:task14", residency: "local", dataUse: "memory" });

export function task14Policy(ownerHost: HostId, revision = "task14-policy-v1", overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const pending: ProcessingPolicy = { destinationIds: { qdrant: TASK14_QDRANT_DESTINATION.id, embedding: TASK14_EMBEDDING_DESTINATION.id, llm: TASK14_LLM_DESTINATION.id }, originProvider: "task14-provider", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: revision, ...overrides, id: "pending", ownerHost };
  return { ...pending, id: processingPolicyHash(pending) };
}

export function task14PolicyRecord(policy: ProcessingPolicy, control: ControlRecord): ProcessingPolicyRecord {
  const base = { ownerHost: policy.ownerHost, schemaRevision: 1 as const, createdAt: control.createdAt, privacyEpoch: control.privacyEpoch, processingPolicyId: policy.id, expiresAt: policy.expiresAt, recordType: "processing_policy" as const, id: policy.id, policy, canonicalHash: policy.id, contentHash: "pending" };
  return { ...base, contentHash: canonicalRecordHash(base) };
}

export function task14Episode(input: { id: string; ownerHost: HostId; control: ControlRecord; policy: ProcessingPolicy; text: string; vector: number[]; eventAt?: string }): EpisodeRecord {
  const base = { ownerHost: input.ownerHost, schemaRevision: 1 as const, createdAt: input.eventAt ?? "2026-08-15T00:00:00.000Z", privacyEpoch: input.control.privacyEpoch, processingPolicyId: input.policy.id, expiresAt: input.policy.expiresAt, recordType: "episode" as const, id: input.id, contentHash: "pending", sourceEntryId: `entry-${input.id}`, host: input.ownerHost, projectId: "task14-project", projectIdentityKind: "registered" as const, sessionId: "task14-session", turnId: `turn-${input.id}`, agentRole: "root" as const, depth: 0, eventKind: "user" as const, eventAt: input.eventAt ?? "2026-08-15T00:00:00.000Z", modelId: "task14-capture", embeddingDimension: 1024, originProvider: input.policy.originProvider, destinationId: TASK14_QDRANT_DESTINATION.id, status: "active" as const, redactionStatus: "unchanged" as const, secretScan: "passed" as const, text: input.text, vector: [...input.vector] };
  return { ...base, contentHash: canonicalRecordHash(base) };
}

export interface EmbeddingStub { readonly baseUrl: string; readonly requests: string[]; close(): Promise<void>; }
/** Loopback embedding endpoint returning one deterministic 1024-vector. */
export async function startEmbeddingStub(vector: readonly number[]): Promise<EmbeddingStub> {
  const requests: string[] = [];
  const server: Server = createServer((request, response) => {
    if (request.method !== "POST" || request.url !== "/v1/embeddings") { response.writeHead(404).end(); return; }
    requests.push(String(request.url));
    const body = JSON.stringify({ data: [{ embedding: [...vector] }] });
    response.writeHead(200, { "content-type": "application/json", "content-length": Buffer.byteLength(body) });
    response.end(body);
  });
  await new Promise<void>((resolveListen, rejectListen) => { server.once("error", rejectListen); server.listen(0, "127.0.0.1", () => resolveListen()); });
  const address = server.address();
  if (address === null || typeof address !== "object") throw new Error("embedding stub did not bind");
  return {
    baseUrl: `http://127.0.0.1:${address.port}/v1`,
    requests,
    async close() { await new Promise<void>((resolveClose) => { server.closeAllConnections?.(); server.close(() => resolveClose()); }); },
  };
}

export interface Task14Runtime {
  readonly store: ProductionCoordinationStore;
  readonly qdrant: BoundQdrantDestination;
  readonly embedding: BoundEmbeddingDestination;
  readonly control: ControlRecord;
}
/** Real production store + bound destinations against the isolated server. */
export async function task14Runtime(url: string, host: HostId, embeddingBaseUrl: string): Promise<Task14Runtime> {
  const options = isolatedOptions(url, host);
  const probe = createQdrantCoordinationStore(options);
  const control = await probe.readControl();
  const bundle = createQdrantSafeBundle({ options, destination: TASK14_QDRANT_DESTINATION, egressMode: "allowlist", coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch });
  const qdrant = bindQdrantDestination(bundle.qdrant, TASK14_QDRANT_DESTINATION);
  const client = new EmbeddingsClient({ baseUrl: embeddingBaseUrl, model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", timeoutMs: 5_000 });
  const factory = createEmbeddingDestinationFactory({ endpoint: embeddingBaseUrl, destination: TASK14_EMBEDDING_DESTINATION, client: bindEmbeddingDocumentClient({ endpoint: embeddingBaseUrl, client }), egressMode: "allowlist", coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch });
  return { store: bundle.store, qdrant, embedding: bindEmbeddingDestination(factory, TASK14_EMBEDDING_DESTINATION), control };
}
