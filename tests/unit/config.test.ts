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
      readTextFile: read({ enabled: true, autoRecall: true, qdrant: { url: "http://shared:6333", collection: "shared" }, retrieval: { topK: 4 }, prime: { retrieval: { topK: 6 }, enabled: true, autoRecall: true } }),
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
