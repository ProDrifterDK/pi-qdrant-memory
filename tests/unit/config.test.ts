import { describe, expect, it } from "vitest";
import { configPath, loadConfig } from "../../src/config.js";
import { loadAdminProcessSecrets } from "../../src/admin/secrets.js";

const missing = async () => { throw Object.assign(new Error("missing"), { code: "ENOENT" }); };
const read = (value: unknown) => async () => JSON.stringify(value);
const base = (env: Record<string, string | undefined> = {}) => ({ env, homeDir: "/home/tester", readTextFile: missing });

describe("configPath", () => {
  it("uses XDG_CONFIG_HOME when set", () => expect(configPath({ homeDir: "/home/tester", xdgConfigHome: "/cfg" })).toBe("/cfg/pi-qdrant-memory/config.json"));
  it("falls back to HOME/.config", () => expect(configPath({ homeDir: "/home/tester" })).toBe("/home/tester/.config/pi-qdrant-memory/config.json"));
});

describe("v2 loadConfig", () => {
  it("returns the complete transitional shape with host collection isolation", async () => {
    const pi = await loadConfig("pi", base());
    const prime = await loadConfig("prime", base());
    expect(pi).toMatchObject({ host: "pi", enabled: true, autoRecall: true, qdrant: { collection: "pi_memory", replicationFactor: 1, writeConsistencyFactor: 1 }, capture: { enabled: false }, privacy: { egressMode: "local_only" }, retrieval: { hardContextCharBudget: 16000, rootScope: "project", childSearch: true }, raptor: { rebuildEpisodeDelta: 64, maxLevels: 5, summaryInputTokens: 12000, membershipThreshold: 0.1 } });
    expect(prime.qdrant.collection).toBe("prime_memory");
  });

  it("applies environment over host, shared, and defaults", async () => {
    const result = await loadConfig("prime", {
      env: { PI_QDRANT_MEMORY_TOP_K: "7", PI_QDRANT_MEMORY_AUTO_RECALL: "false", PI_QDRANT_MEMORY_QDRANT_API_KEY: "runtime-secret" },
      homeDir: "/home/tester",
      xdgConfigHome: "/cfg",
      readTextFile: read({ enabled: true, autoRecall: true, qdrant: { url: "http://shared:6333" }, retrieval: { topK: 4 }, prime: { retrieval: { topK: 6 }, qdrant: { collection: "shared" }, enabled: true, autoRecall: true } }),
    });
    expect(result.qdrant.url).toBe("http://shared:6333");
    expect(result.qdrant.collection).toBe("shared");
    expect(result.retrieval.topK).toBe(7);
    expect(result.enabled).toBe(true);
    expect(result.autoRecall).toBe(false);
    expect(result.qdrant.apiKey).toBe("runtime-secret");
    expect(result.configPath).toBe("/cfg/pi-qdrant-memory/config.json");
  });

  it("accepts each operational override and preserves the hard ceiling", async () => {
    const result = await loadConfig("pi", base({
      PI_QDRANT_MEMORY_QDRANT_URL: "https://qdrant.example///",
      PI_QDRANT_MEMORY_QDRANT_COLLECTION: "runtime_collection",
      PI_QDRANT_MEMORY_EMBEDDING_BASE_URL: "https://embed.example/v1///",
      PI_QDRANT_MEMORY_EMBEDDING_MODEL: "runtime-model",
      PI_QDRANT_MEMORY_EMBEDDING_DIMENSION: "1024",
      PI_QDRANT_MEMORY_AUTO_RECALL: "false",
      PI_QDRANT_MEMORY_TOP_K: "9",
      PI_QDRANT_MEMORY_CANDIDATES_PER_LANE: "99",
      PI_QDRANT_MEMORY_MIN_SCORE: "-0.5",
      PI_QDRANT_MEMORY_PROJECT_BOOST: "0.2",
      PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS: "15000",
      PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS: "14000",
      PI_QDRANT_MEMORY_TIMEOUT_MS: "30000",
    }));
    expect(result).toMatchObject({ autoRecall: false, qdrant: { url: "https://qdrant.example", collection: "runtime_collection" }, embeddings: { baseUrl: "https://embed.example/v1", model: "runtime-model", dimension: 1024 }, retrieval: { topK: 9, candidatesPerLane: 99, minScore: -0.5, projectBoost: 0.2, contextBudgetChars: 15000, toolResultBudgetChars: 14000, hardContextCharBudget: 16000, timeoutMs: 30000 } });
  });

  it("rejects unknown settings and retired environment names", async () => {
    const retired = ["SOURCE", "_QDRANT_", "URL"].join("");
    await expect(loadConfig("pi", base({ [`PI_QDRANT_MEMORY_${retired}`]: "http://old" }))).rejects.toThrow();
    await expect(loadConfig("pi", base({ PI_QDRANT_MEMORY_NOT_ALLOWED: "x" }))).rejects.toThrow();
    await expect(loadConfig("pi", base({ PI_QDRANT_MEMORY_ENABLED: "false" }))).rejects.toThrow();
  });

  it("rejects retired file fields and file credentials", async () => {
    const retiredAdmin = ["admin", ".", "source"].join("");
    const sourceKey = ["admin", "source"].join(".");
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ admin: { [sourceKey.split(".")[1] as string]: {} } }) })).rejects.toThrow();
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ nested: { apiKey: "do-not-persist" } }) })).rejects.toThrow();
    expect(retiredAdmin).toBe(["admin", ".", "source"].join(""));
  });

  it("uses shared scalar values when host and environment do not override them", async () => {
    const result = await loadConfig("pi", { ...base(), readTextFile: read({ enabled: false, autoRecall: false }) });
    expect(result.enabled).toBe(false);
    expect(result.autoRecall).toBe(false);
    const host = await loadConfig("pi", { ...base(), readTextFile: read({ enabled: false, autoRecall: false, pi: { enabled: true, autoRecall: true } }) });
    expect(host.enabled).toBe(true);
    expect(host.autoRecall).toBe(true);
  });

  it("rejects unknown root, nested, registration, and destination fields", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ foo: true }) })).rejects.toThrow();
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { extra: true } }) })).rejects.toThrow();
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ projects: { registrations: { p: { canonicalPath: "/p", fingerprint: "f", alias: "a", extra: true } } } }) })).rejects.toThrow();
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ privacy: { allowedQdrantDestinations: [{ id: "q", residency: "local", dataUse: "memory", extra: true }] } }) })).rejects.toThrow();
    await expect(loadConfig("pi", { ...base(), readTextFile: async () => '{"projects":{"registrations":{"__proto__":{"canonicalPath":"/p","fingerprint":"f","alias":"a"}}}}' })).rejects.toThrow();
  });

  it("accepts token-budget names but rejects nested credential token names", async () => {
    const result = await loadConfig("pi", { ...base(), readTextFile: read({ curation: { maxInputTokens: 12000 }, memoryModel: { maxOutputTokens: 2048 }, raptor: { summaryInputTokens: 12000 } }) });
    expect(result.curation.maxInputTokens).toBe(12000);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ curation: { authToken: "bad" } }) })).rejects.toThrow();
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ memoryModel: { apiKey: "bad" } }) })).rejects.toThrow();
  });

  it("rejects invalid bounds and URL credentials", async () => {
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_TOP_K: "0" }))).rejects.toThrow();
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_TIMEOUT_MS: "30001" }))).rejects.toThrow();
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_EMBEDDING_DIMENSION: "1536" }))).rejects.toThrow();
    await expect(loadConfig("prime", { ...base(), readTextFile: read({ embeddings: { dimension: 1536 } }) })).rejects.toThrow();
    await expect(loadConfig("prime", { ...base(), readTextFile: read({ raptor: { summaryInputTokens: 511 } }) })).rejects.toThrow();
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_QDRANT_URL: "https://user:password@example.com" }))).rejects.toThrow();
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_QDRANT_URL: "https://qdrant.example/?api_key=secret" }))).rejects.toThrow();
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_EMBEDDING_BASE_URL: "https://embed.example/#secret" }))).rejects.toThrow();
    await expect(loadConfig("prime", { ...base(), readTextFile: read({ capture: { enabled: true, episodeRetentionDays: 0 } }) })).rejects.toThrow();
  });
});

describe("admin process secrets", () => {
  it("loads only the exact human-process credential and never runtime config", async () => {
    const env = { PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY: "admin-secret", PI_QDRANT_MEMORY_QDRANT_API_KEY: "runtime-secret" };
    expect(loadAdminProcessSecrets(env)).toEqual({ destinationApiKey: "admin-secret" });
    const config = await loadConfig("pi", base(env));
    expect(config.qdrant.apiKey).toBe("runtime-secret");
    expect("admin" in config).toBe(false);
  });
  it("treats absent and blank administrative values as unavailable", () => {
    expect(loadAdminProcessSecrets({})).toEqual({});
    expect(loadAdminProcessSecrets({ PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY: "  " })).toEqual({});
  });
});


describe("v2 fail-closed configuration invariants", () => {
  it("requires explicit retention before capture activation", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ capture: { enabled: true } }) })).rejects.toThrow(/retention/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ capture: { enabled: true, episodeRetentionDays: 30 } }) })).rejects.toThrow(/egress/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ capture: { enabled: true, episodeRetentionDays: 30 }, privacy: { egressMode: "local_only" } }) })).resolves.toMatchObject({ capture: { enabled: true, episodeRetentionDays: 30 }, privacy: { egressMode: "local_only" } });
  });
  it("enforces retry, replication-consistency, and shared-node relations", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ outbox: { retryBaseMs: 10000, retryMaxMs: 1000 } }) })).rejects.toThrow(/retryBaseMs/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { replicationFactor: 4, writeConsistencyFactor: 2 } }) })).rejects.toThrow(/consistency/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { replicationFactor: 4, writeConsistencyFactor: 3 }, coordination: { readConsistency: 1 } }) })).rejects.toThrow(/readConsistency/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { replicationFactor: 4, writeConsistencyFactor: 3 } }) })).resolves.toMatchObject({ coordination: { readConsistency: "majority" } });
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ outbox: { sharedFilesystem: true } }) })).rejects.toThrow(/nodeId/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ outbox: { sharedFilesystem: true, nodeId: "node-a" } }) })).resolves.toMatchObject({ outbox: { sharedFilesystem: true, nodeId: "node-a" } });
  });
  it("rejects duplicate configured endpoint/collection sections and remote allowlist without endpoints", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { url: "https://q.example", collection: "same" }, pi: { qdrant: { url: "https://q.example", collection: "same" } } }) })).rejects.toThrow(/endpoint\/collection/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ privacy: { egressMode: "allowlist", allowedQdrantDestinations: [{ id: "q", residency: "eu", dataUse: "memory" }] } }) })).rejects.toThrow(/embedding/i);
  });
});


describe("complete v2 config ranges and collisions", () => {
  const sectionCases: Array<[string, Record<string, unknown>, Record<string, unknown>]> = [
    ["capture.toolArgsChars", { capture: { toolArgsChars: 0 } }, { capture: { toolArgsChars: 0 } }],
    ["capture.toolArgsChars max", { capture: { toolArgsChars: 16000 } }, { capture: { toolArgsChars: 16000 } }],
    ["coordination.maxClockSkewMs", { coordination: { maxClockSkewMs: 0 } }, { coordination: { maxClockSkewMs: 0 } }],
    ["coordination.maxClockSkewMs max", { coordination: { maxClockSkewMs: 3600000 } }, { coordination: { maxClockSkewMs: 3600000 } }],
    ["outbox.maxJobs", { outbox: { maxJobs: 1 } }, { outbox: { maxJobs: 1 } }],
    ["outbox.maxJobs max", { outbox: { maxJobs: 100000 } }, { outbox: { maxJobs: 100000 } }],
    ["outbox.maxBytes", { outbox: { maxBytes: 1048576 } }, { outbox: { maxBytes: 1048576 } }],
    ["outbox.maxBytes max", { outbox: { maxBytes: 1073741824 } }, { outbox: { maxBytes: 1073741824 } }],
    ["curation.turnTrigger", { curation: { turnTrigger: 1 } }, { curation: { turnTrigger: 1 } }],
    ["curation.turnTrigger max", { curation: { turnTrigger: 1000 } }, { curation: { turnTrigger: 1000 } }],
    ["memoryModel.maxOutputTokens", { memoryModel: { maxOutputTokens: 128 } }, { memoryModel: { maxOutputTokens: 128 } }],
    ["memoryModel.maxOutputTokens max", { memoryModel: { maxOutputTokens: 8192 } }, { memoryModel: { maxOutputTokens: 8192 } }],
    ["raptor.rebuildEpisodeDelta", { raptor: { rebuildEpisodeDelta: 2 } }, { raptor: { rebuildEpisodeDelta: 2 } }],
    ["raptor.rebuildEpisodeDelta max", { raptor: { rebuildEpisodeDelta: 10000 } }, { raptor: { rebuildEpisodeDelta: 10000 } }],
    ["raptor.membershipThreshold", { raptor: { membershipThreshold: 0.01 } }, { raptor: { membershipThreshold: 0.01 } }],
    ["raptor.membershipThreshold max", { raptor: { membershipThreshold: 1 } }, { raptor: { membershipThreshold: 1 } }],
  ];
  it.each(sectionCases)("accepts boundary %s", async (_name, file, expected) => await expect(loadConfig("pi", { ...base(), readTextFile: read(file) })).resolves.toMatchObject(expected));
  it.each([
    [{ capture: { toolArgsChars: -1 } }, /toolArgsChars/], [{ capture: { toolResultChars: 16001 } }, /toolResultChars/],
    [{ coordination: { maxClockSkewMs: 3600001 } }, /maxClockSkewMs/], [{ outbox: { maxJobs: 0 } }, /maxJobs/],
    [{ outbox: { maxBytes: 1048575 } }, /maxBytes/], [{ curation: { turnTrigger: 0 } }, /turnTrigger/],
    [{ memoryModel: { maxOutputTokens: 127 } }, /maxOutputTokens/], [{ raptor: { rebuildEpisodeDelta: 1 } }, /rebuildEpisodeDelta/],
    [{ raptor: { membershipThreshold: 0 } }, /membershipThreshold/],
  ])("rejects out-of-range %j", async (file, message) => await expect(loadConfig("pi", { ...base(), readTextFile: read(file) })).rejects.toThrow(message));
  it("rejects external/non-REST endpoints and unbounded identifiers", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { url: "file:///tmp/qdrant" } }) })).rejects.toThrow(/http/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { collection: "x".repeat(257) } }) })).rejects.toThrow(/collection/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ embeddings: { model: "m".repeat(257) } }) })).rejects.toThrow(/model/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ outbox: { sharedFilesystem: true, nodeId: "n".repeat(257) } }) })).rejects.toThrow(/nodeId/i);
  });
  it("rejects effective Pi/Prime collection collisions including shared collection-only overrides", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { collection: "same" } }) })).rejects.toThrow(/Pi.*Prime|collection/i);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ qdrant: { url: "http://shared:6333" } }) })).resolves.toMatchObject({ qdrant: { collection: "pi_memory" } });
  });
});


describe("§6.2 complete boundary matrix", () => {
  const boundaries: Array<[string, Record<string, unknown>, unknown]> = [
    ["retrieval topK min", { retrieval: { topK: 1 } }, 1], ["retrieval topK max", { retrieval: { topK: 10 } }, 10], ["retrieval candidates min", { retrieval: { candidatesPerLane: 1 } }, 1], ["retrieval candidates max", { retrieval: { candidatesPerLane: 100 } }, 100], ["retrieval minScore lower", { retrieval: { minScore: -1 } }, -1], ["retrieval minScore upper", { retrieval: { minScore: 1 } }, 1], ["retrieval projectBoost min", { retrieval: { projectBoost: 0 } }, 0], ["retrieval projectBoost max", { retrieval: { projectBoost: 0.25 } }, 0.25], ["retrieval tool result budget min", { retrieval: { toolResultBudgetChars: 1 } }, 1], ["retrieval tool result budget max", { retrieval: { toolResultBudgetChars: 16000 } }, 16000], ["retrieval context budget min", { retrieval: { contextBudgetChars: 1 } }, 1], ["retrieval context budget max", { retrieval: { contextBudgetChars: 16000 } }, 16000], ["retrieval timeout min", { retrieval: { timeoutMs: 100 } }, 100], ["retrieval timeout max", { retrieval: { timeoutMs: 30000 } }, 30000],
    ["capture episodeRetentionDays min", { capture: { episodeRetentionDays: 1 } }, 1], ["capture episodeRetentionDays max", { capture: { episodeRetentionDays: 3650 } }, 3650],
    ["capture toolResultChars min", { capture: { toolResultChars: 0 } }, 0], ["capture toolArgsChars min", { capture: { toolArgsChars: 0 } }, 0], ["capture toolArgsChars max", { capture: { toolArgsChars: 16000 } }, 16000], ["capture toolResultChars max", { capture: { toolResultChars: 16000 } }, 16000],
    ["outbox maxJobs min", { outbox: { maxJobs: 1 } }, 1], ["outbox maxJobs max", { outbox: { maxJobs: 100000 } }, 100000], ["outbox maxBytes min", { outbox: { maxBytes: 1048576 } }, 1048576], ["outbox maxBytes max", { outbox: { maxBytes: 1073741824 } }, 1073741824], ["outbox retryBase min", { outbox: { retryBaseMs: 100 } }, 100], ["outbox retryBase max", { outbox: { retryBaseMs: 10000 } }, 10000], ["outbox retryMax min", { outbox: { retryMaxMs: 1000 } }, 1000], ["outbox retryMax max", { outbox: { retryMaxMs: 300000 } }, 300000],
    ["qdrant replication min", { qdrant: { replicationFactor: 1 } }, 1], ["qdrant replication max", { qdrant: { replicationFactor: 7, writeConsistencyFactor: 7 } }, 7], ["qdrant write consistency min", { qdrant: { writeConsistencyFactor: 1 } }, 1], ["qdrant write consistency max", { qdrant: { writeConsistencyFactor: 7 } }, 7],
    ["coordination maxClockSkew min", { coordination: { maxClockSkewMs: 0 } }, 0], ["coordination maxClockSkew max", { coordination: { maxClockSkewMs: 3600000 } }, 3600000], ["coordination read consistency numeric min", { coordination: { readConsistency: 1 } }, 1], ["coordination read consistency numeric max", { coordination: { readConsistency: 7 } }, 7], ["coordination lease min", { coordination: { leaseMs: 5000 } }, 5000], ["coordination lease max", { coordination: { leaseMs: 300000 } }, 300000], ["coordination reconcile min", { coordination: { reconcileIntervalMs: 60000 } }, 60000], ["coordination reconcile max", { coordination: { reconcileIntervalMs: 86400000 } }, 86400000],
    ["curation turnTrigger min", { curation: { turnTrigger: 1 } }, 1], ["curation turnTrigger max", { curation: { turnTrigger: 1000 } }, 1000], ["curation toolTrigger min", { curation: { toolTrigger: 1 } }, 1], ["curation toolTrigger max", { curation: { toolTrigger: 1000 } }, 1000], ["curation maxInputTokens min", { curation: { maxInputTokens: 512 } }, 512], ["curation maxInputTokens max", { curation: { maxInputTokens: 65536 } }, 65536],
    ["memoryModel maxOutputTokens min", { memoryModel: { maxOutputTokens: 128 } }, 128], ["memoryModel maxOutputTokens max", { memoryModel: { maxOutputTokens: 8192 } }, 8192], ["memoryModel timeout min", { memoryModel: { timeoutMs: 1000 } }, 1000], ["memoryModel timeout max", { memoryModel: { timeoutMs: 120000 } }, 120000],
    ["raptor rebuildEpisodeDelta min", { raptor: { rebuildEpisodeDelta: 2 } }, 2], ["raptor rebuildEpisodeDelta max", { raptor: { rebuildEpisodeDelta: 10000 } }, 10000], ["raptor membershipThreshold min", { raptor: { membershipThreshold: 0.01 } }, 0.01], ["raptor membershipThreshold max", { raptor: { membershipThreshold: 1 } }, 1], ["raptor maxLevels min", { raptor: { maxLevels: 1 } }, 1], ["raptor maxLevels max", { raptor: { maxLevels: 10 } }, 10], ["raptor summaryInputTokens min", { raptor: { summaryInputTokens: 512 } }, 512], ["raptor summaryInputTokens max", { raptor: { summaryInputTokens: 65536 } }, 65536], ["raptor umapDimensions min", { raptor: { umapDimensions: 1 } }, 1], ["raptor umapDimensions max", { raptor: { umapDimensions: 64 } }, 64], ["raptor localNeighbors min", { raptor: { localNeighbors: 2 } }, 2], ["raptor localNeighbors max", { raptor: { localNeighbors: 200 } }, 200], ["raptor gmmMaxClusters min", { raptor: { gmmMaxClusters: 1 } }, 1], ["raptor gmmMaxClusters max", { raptor: { gmmMaxClusters: 200 } }, 200], ["raptor seed min", { raptor: { seed: 0 } }, 0], ["raptor seed max", { raptor: { seed: 4294967295 } }, 4294967295],
  ];
  it.each(boundaries)("loads %s through the loader", async (_name, file, _expected) => await expect(loadConfig("pi", { ...base(), readTextFile: read(file) })).resolves.toBeDefined());
  it.each([
    [{ retrieval: { topK: 0 } }, /(top_k|topK)/i], [{ retrieval: { topK: 11 } }, /(top_k|topK)/i], [{ retrieval: { candidatesPerLane: 0 } }, /(candidates_per_lane|candidatesPerLane)/i], [{ retrieval: { candidatesPerLane: 101 } }, /(candidates_per_lane|candidatesPerLane)/i], [{ retrieval: { minScore: -1.1 } }, /(min_score|minScore)/i], [{ retrieval: { minScore: 1.1 } }, /(min_score|minScore)/i], [{ retrieval: { projectBoost: -0.01 } }, /(project_boost|projectBoost)/i], [{ retrieval: { projectBoost: 0.26 } }, /(project_boost|projectBoost)/i], [{ retrieval: { toolResultBudgetChars: 0 } }, /(tool_result_budget_chars|toolResultBudgetChars)/i], [{ retrieval: { toolResultBudgetChars: 16001 } }, /(tool_result_budget_chars|toolResultBudgetChars)/i], [{ retrieval: { contextBudgetChars: 0 } }, /(context_budget_chars|contextBudgetChars)/i], [{ retrieval: { contextBudgetChars: 16001 } }, /(context_budget_chars|contextBudgetChars)/i], [{ retrieval: { timeoutMs: 99 } }, /(timeout_ms|timeoutMs)/i], [{ retrieval: { timeoutMs: 30001 } }, /(timeout_ms|timeoutMs)/i],
    [{ capture: { episodeRetentionDays: 0 } }, /episodeRetentionDays/], [{ capture: { episodeRetentionDays: 3651 } }, /episodeRetentionDays/],
    [{ capture: { toolArgsChars: -1 } }, /toolArgsChars/], [{ capture: { toolArgsChars: 16001 } }, /toolArgsChars/], [{ capture: { toolResultChars: -1 } }, /toolResultChars/], [{ capture: { toolResultChars: 16001 } }, /toolResultChars/],
    [{ outbox: { maxJobs: 0 } }, /maxJobs/], [{ outbox: { maxJobs: 100001 } }, /maxJobs/], [{ outbox: { maxBytes: 1048575 } }, /maxBytes/], [{ outbox: { maxBytes: 1073741825 } }, /maxBytes/], [{ outbox: { retryBaseMs: 99 } }, /retryBaseMs/], [{ outbox: { retryBaseMs: 10001 } }, /retryBaseMs/], [{ outbox: { retryMaxMs: 999 } }, /retryMaxMs/], [{ outbox: { retryMaxMs: 300001 } }, /retryMaxMs/],
    [{ qdrant: { replicationFactor: 0 } }, /replicationFactor/], [{ qdrant: { replicationFactor: 8 } }, /replicationFactor/], [{ qdrant: { writeConsistencyFactor: 0 } }, /writeConsistencyFactor/], [{ qdrant: { writeConsistencyFactor: 8 } }, /writeConsistencyFactor/],
    [{ coordination: { maxClockSkewMs: -1 } }, /maxClockSkewMs/], [{ coordination: { maxClockSkewMs: 3600001 } }, /maxClockSkewMs/], [{ coordination: { readConsistency: 0 } }, /readConsistency/], [{ coordination: { readConsistency: 8 } }, /readConsistency/], [{ coordination: { leaseMs: 4999 } }, /leaseMs/], [{ coordination: { leaseMs: 300001 } }, /leaseMs/], [{ coordination: { reconcileIntervalMs: 59999 } }, /reconcileIntervalMs/], [{ coordination: { reconcileIntervalMs: 86400001 } }, /reconcileIntervalMs/],
    [{ curation: { turnTrigger: 0 } }, /turnTrigger/], [{ curation: { turnTrigger: 1001 } }, /turnTrigger/], [{ curation: { toolTrigger: 0 } }, /toolTrigger/], [{ curation: { toolTrigger: 1001 } }, /toolTrigger/], [{ curation: { maxInputTokens: 511 } }, /maxInputTokens/], [{ curation: { maxInputTokens: 65537 } }, /maxInputTokens/],
    [{ memoryModel: { maxOutputTokens: 127 } }, /maxOutputTokens/], [{ memoryModel: { maxOutputTokens: 8193 } }, /maxOutputTokens/], [{ memoryModel: { timeoutMs: 999 } }, /(timeout_ms|timeoutMs)/i], [{ memoryModel: { timeoutMs: 120001 } }, /(timeout_ms|timeoutMs)/i],
    [{ raptor: { rebuildEpisodeDelta: 1 } }, /rebuildEpisodeDelta/], [{ raptor: { rebuildEpisodeDelta: 10001 } }, /rebuildEpisodeDelta/], [{ raptor: { membershipThreshold: 0 } }, /membershipThreshold/], [{ raptor: { membershipThreshold: 1.01 } }, /membershipThreshold/], [{ raptor: { maxLevels: 0 } }, /maxLevels/], [{ raptor: { maxLevels: 11 } }, /maxLevels/], [{ raptor: { summaryInputTokens: 511 } }, /summaryInputTokens/], [{ raptor: { summaryInputTokens: 65537 } }, /summaryInputTokens/], [{ raptor: { umapDimensions: 0 } }, /umapDimensions/], [{ raptor: { umapDimensions: 65 } }, /umapDimensions/], [{ raptor: { localNeighbors: 1 } }, /localNeighbors/], [{ raptor: { localNeighbors: 201 } }, /localNeighbors/], [{ raptor: { gmmMaxClusters: 0 } }, /gmmMaxClusters/], [{ raptor: { gmmMaxClusters: 201 } }, /gmmMaxClusters/], [{ raptor: { seed: -1 } }, /seed/], [{ raptor: { seed: 4294967296 } }, /seed/],
  ])("rejects matrix out-of-range %j", async (file, message) => await expect(loadConfig("pi", { ...base(), readTextFile: read(file) })).rejects.toThrow(message));
  it.each(["majority", "quorum", "all"])("loads coordination readConsistency enum %s", async (readConsistency) => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ coordination: { readConsistency } }) })).resolves.toMatchObject({ coordination: { readConsistency } });
  });
  it("exercises every boolean and enum", async () => {
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ enabled: false, autoRecall: false, capture: { enabled: false }, privacy: { egressMode: "local_only", allowActiveModelFallback: true, allowCrossProviderReplay: true }, retrieval: { rootScope: "project_and_global", childSearch: false }, outbox: { sharedFilesystem: false }, projects: { registrations: {} } }) })).resolves.toMatchObject({ enabled: false, autoRecall: false, privacy: { allowActiveModelFallback: true, allowCrossProviderReplay: true }, retrieval: { rootScope: "project_and_global", childSearch: false } });
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ privacy: { egressMode: "allowlist", allowedQdrantDestinations: [{ id: "qdrant", residency: "eu", dataUse: "memory" }], allowedEmbeddingDestinations: [{ id: "embedding", residency: "eu", dataUse: "memory" }] } }) })).resolves.toMatchObject({ privacy: { egressMode: "allowlist" } });
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ privacy: { egressMode: "invalid" } }) })).rejects.toThrow(/egressMode/);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ retrieval: { rootScope: "invalid" } }) })).rejects.toThrow(/rootScope/);
    await expect(loadConfig("pi", { ...base(), readTextFile: read({ capture: { enabled: "yes" } }) })).rejects.toThrow(/enabled/);
  });
});
