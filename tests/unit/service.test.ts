import type { ContextEvent, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { RecallCache } from "../../src/cache.js";
import { MemoryClientError } from "../../src/clients/http.js";
import { MEMORY_CONTEXT_CUSTOM_TYPE } from "../../src/format.js";
import { MemoryService, type MemoryWarning } from "../../src/service.js";
import type { MemoryCandidate, MemorySearchResult } from "../../src/retrieval/search.js";
import type { RuntimeConfig } from "../../src/types.js";

type AgentMessages = ContextEvent["messages"];

const hit: MemoryCandidate = {
  id: "memory-1",
  text: "A recalled fact",
  rawScore: 0.9,
  adjustedScore: 0.95,
  lane: "project",
  projectId: "project-id",
  projectLabel: "project",
  sourceType: "conversation",
  sourceSystem: "pi",
};

function config(overrides: Partial<RuntimeConfig["retrieval"]> = {}): RuntimeConfig {
  return {
    host: "prime",
    enabled: true,
    autoRecall: true,
    configPath: "/safe/config.json",
    qdrant: { url: "http://qdrant.test", collection: "prime_memory", replicationFactor: 1, writeConsistencyFactor: 1 },
    embeddings: { baseUrl: "http://embeddings.test/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: " },
    retrieval: {
      topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0.05,
      contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000,
      timeoutMs: 2500, rootScope: "project", childSearch: true, ...overrides,
    },
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

function user(text: string, timestamp = 1): AgentMessages[number] {
  return { role: "user", content: text, timestamp } as AgentMessages[number];
}

function assistant(text: string, timestamp = 2): AgentMessages[number] {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    api: "test",
    provider: "test",
    model: "test",
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    stopReason: "stop",
    timestamp,
  } as AgentMessages[number];
}

function branchMessage(message: AgentMessages[number], id: string) {
  return { type: "message", id, parentId: null, timestamp: new Date(0).toISOString(), message };
}

function context(input: {
  sessionId?: string;
  cwd?: string;
  branch?: unknown[] | (() => unknown[]);
  header?: unknown;
} = {}): ExtensionContext {
  const branch = input.branch ?? [];
  return {
    cwd: input.cwd ?? "/workspace/project",
    hasUI: true,
    signal: undefined,
    ui: { notify: vi.fn() },
    sessionManager: {
      getSessionId: () => input.sessionId ?? "session-1",
      getBranch: () => typeof branch === "function" ? branch() : branch,
      getHeader: () => input.header ?? null,
    },
  } as unknown as ExtensionContext;
}

function makeService(input: {
  runtimeConfig?: RuntimeConfig;
  cache?: RecallCache<MemorySearchResult>;
  search?: ReturnType<typeof vi.fn>;
  projectResolver?: (cwd: string) => Promise<{ id: string; label: string }>;
  warnings?: MemoryWarning[];
  qdrant?: { health(signal?: AbortSignal): Promise<void>; collectionInfo(signal?: AbortSignal): Promise<{ dimension: number; distance: string }> };
  embeddings?: { embedQuery(query: string, signal?: AbortSignal): Promise<number[]> };
} = {}) {
  const runtimeConfig = input.runtimeConfig ?? config();
  const search = input.search ?? vi.fn(async ({ query }: { query: string }) => ({ query, hits: [hit] }));
  const warnings = input.warnings ?? [];
  const qdrant = input.qdrant ?? {
    health: vi.fn(async () => undefined),
    collectionInfo: vi.fn(async () => ({ dimension: runtimeConfig.embeddings.dimension, distance: "Cosine" })),
  };
  const embeddings = input.embeddings ?? {
    embedQuery: vi.fn(async () => Array.from({ length: runtimeConfig.embeddings.dimension }, () => 0)),
  };
  const service = new MemoryService({
    host: runtimeConfig.host,
    config: runtimeConfig,
    retriever: { search },
    projectResolver: input.projectResolver ?? (async () => ({ id: "project-id", label: "project" })),
    cache: input.cache ?? new RecallCache({ maxEntries: 32, ttlMs: 300_000 }),
    warningSink: (warning) => { warnings.push(warning); },
    qdrant,
    embeddings,
  });
  return { service, search, warnings, qdrant, embeddings };
}

describe("MemoryService recall lifecycle", () => {
  it("uses the same key before and after the host persists the accepted user leaf", async () => {
    const branch: unknown[] = [branchMessage(user("Explain the cache consistency requirements in detail"), "prior")];
    const ctx = context({ branch: () => branch });
    const { service, search } = makeService();

    service.prefetch("and the retry case?", ctx);
    branch.push(branchMessage(user("and the retry case?", 3), "current"));
    const messages: AgentMessages = [
      user("Explain the cache consistency requirements in detail"),
      assistant("Earlier response"),
      user("and the retry case?", 3),
    ];
    const result = await service.inject(messages, ctx);

    expect(search).toHaveBeenCalledTimes(1);
    expect(search.mock.calls[0]?.[0]).toMatchObject({
      query: "Explain the cache consistency requirements in detail\n\nand the retry case?",
      host: "prime",
      project: { id: "project-id", label: "project" },
      limit: 5,
    });
    expect(result).not.toBe(messages);
    expect(result.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toHaveLength(1);
    expect(ctx.sessionManager.getBranch()).not.toContainEqual(expect.objectContaining({ customType: MEMORY_CONTEXT_CUSTOM_TYPE }));
  });

  it("removes prior plugin messages and appends at most one fresh block on retries and tool calls", async () => {
    const ctx = context();
    const { service, search } = makeService();
    const messages: AgentMessages = [user("A sufficiently detailed repeated memory question")];
    const first = await service.inject(messages, ctx);
    const second = await service.inject(first, ctx);

    expect(search).toHaveBeenCalledTimes(1);
    expect(second.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toHaveLength(1);
    expect(messages).toHaveLength(1);
  });

  it("reuses queued and repeated effective queries without using the mutable leaf id", async () => {
    const branch = [branchMessage(user("A substantive prompt about portable extension behavior"), "prior")];
    const ctx = context({ branch });
    const { service, search } = makeService();

    service.prefetch("more?", ctx);
    service.prefetch("more?", ctx);
    await service.inject([
      user("A substantive prompt about portable extension behavior"),
      user("more?", 2),
    ], ctx);
    await service.inject([
      user("A substantive prompt about portable extension behavior"),
      user("more?", 3),
    ], ctx);

    expect(search).toHaveBeenCalledTimes(1);
  });

  it("isolates branch-effective queries and session ids and supports explicit session clear", async () => {
    const { service, search } = makeService();
    const alpha = "Alpha branch has a substantive cache discussion";
    const beta = "Beta branch has a different substantive discussion";
    const alphaMessages: AgentMessages = [user(alpha), user("continue", 2)];
    const betaMessages: AgentMessages = [user(beta), user("continue", 2)];

    await service.inject(alphaMessages, context({ sessionId: "session-a" }));
    await service.inject(betaMessages, context({ sessionId: "session-a" }));
    await service.inject(alphaMessages, context({ sessionId: "session-b" }));
    expect(search).toHaveBeenCalledTimes(3);

    service.clear();
    await service.inject(alphaMessages, context({ sessionId: "session-a" }));
    expect(search).toHaveBeenCalledTimes(4);
  });

  it("separates services with different non-secret configuration revisions", async () => {
    const cache = new RecallCache<MemorySearchResult>({ maxEntries: 32, ttlMs: 300_000 });
    const firstSearch = vi.fn(async ({ query }: { query: string }) => ({ query, hits: [hit] }));
    const secondSearch = vi.fn(async ({ query }: { query: string }) => ({ query, hits: [hit] }));
    const first = makeService({ cache, search: firstSearch, runtimeConfig: config({ minScore: 0.35 }) }).service;
    const second = makeService({ cache, search: secondSearch, runtimeConfig: config({ minScore: 0.5 }) }).service;
    const messages: AgentMessages = [user("A repeated query across configuration revisions")];

    await first.inject(messages, context());
    await second.inject(messages, context());
    expect(firstSearch).toHaveBeenCalledTimes(1);
    expect(secondSearch).toHaveBeenCalledTimes(1);
  });

  it("does not recall slash commands or empty prompts", async () => {
    const { service, search } = makeService();
    const slash = [user("/status")] as AgentMessages;
    const result = await service.inject(slash, context());
    expect(result).toEqual(slash);
    expect(result).not.toBe(slash);
    expect(search).not.toHaveBeenCalled();
  });

  it("returns copied original messages on Qdrant failure and warns once per redacted category", async () => {
    const secretValues = [
      "secret query text",
      "memory body secret",
      "https://user:password@qdrant.invalid/path",
      "secret-api-key",
      "/absolute/secret/path",
    ];
    const search = vi.fn(async () => {
      throw new MemoryClientError("network", secretValues.join(" "));
    });
    const { service, warnings } = makeService({ search });
    const messages = [user(secretValues[0]!)] as AgentMessages;

    const first = await service.inject(messages, context());
    const second = await service.inject(messages, context());

    expect(first).toEqual(messages);
    expect(first).not.toBe(messages);
    expect(second).toEqual(messages);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toEqual({
      category: "network",
      message: "pi-qdrant-memory: recall unavailable (network).",
    });
    for (const secret of secretValues) expect(JSON.stringify(warnings)).not.toContain(secret);
  });

  it("fails explicit search with a sanitized error after emitting a throttled warning", async () => {
    const search = vi.fn(async () => { throw new Error("query and response secret"); });
    const { service, warnings } = makeService({ search });
    const ctx = context();

    await expect(service.search("A valid explicit memory query", 5, ctx)).rejects.toThrow("Memory recall is unavailable");
    expect(warnings).toEqual([{ category: "internal", message: "pi-qdrant-memory: recall unavailable (internal)." }]);
    expect(JSON.stringify(warnings)).not.toContain("query and response secret");
  });
});

describe("MemoryService health", () => {
  it("uses a fixed non-sensitive probe and validates dimension and Cosine distance", async () => {
    const health = vi.fn(async () => undefined);
    const collectionInfo = vi.fn(async () => ({ dimension: 1024, distance: "cosine" }));
    const embedQuery = vi.fn(async () => Array.from({ length: 1024 }, () => 0));
    const { service, warnings } = makeService({
      qdrant: { health, collectionInfo },
      embeddings: { embedQuery },
    });

    await expect(service.checkHealth(context())).resolves.toBeUndefined();
    expect(health).toHaveBeenCalledTimes(1);
    expect(collectionInfo).toHaveBeenCalledTimes(1);
    expect(embedQuery).toHaveBeenCalledWith("pi-qdrant-memory health probe", undefined);
    expect(warnings).toEqual([]);
  });

  it("warns once without blocking the session when health metadata is incompatible", async () => {
    const { service, warnings } = makeService({
      qdrant: {
        health: vi.fn(async () => undefined),
        collectionInfo: vi.fn(async () => ({ dimension: 99, distance: "Dot" })),
      },
    });

    await expect(service.checkHealth(context())).resolves.toBeUndefined();
    await expect(service.checkHealth(context())).resolves.toBeUndefined();
    expect(warnings).toEqual([{
      category: "configuration",
      message: "pi-qdrant-memory: recall unavailable (configuration).",
    }]);
  });
});
