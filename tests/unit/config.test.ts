import { describe, expect, it } from "vitest";
import { configPath, loadConfig } from "../../src/config.js";

const read = (value: unknown) => async () => JSON.stringify(value);
const missing = async () => {
  throw Object.assign(new Error("missing"), { code: "ENOENT" });
};
const base = (overrides: Record<string, string | undefined> = {}) => ({
  env: overrides,
  homeDir: "/home/tester",
  readTextFile: missing,
});

describe("configPath", () => {
  it("uses XDG_CONFIG_HOME when it is set", () => {
    expect(configPath({ homeDir: "/home/tester", xdgConfigHome: "/cfg" })).toBe(
      "/cfg/pi-qdrant-memory/config.json",
    );
  });

  it("falls back to HOME/.config", () => {
    expect(configPath({ homeDir: "/home/tester" })).toBe(
      "/home/tester/.config/pi-qdrant-memory/config.json",
    );
  });
});

describe("loadConfig", () => {
  it("loads documented defaults without reading project files", async () => {
    const result = await loadConfig("pi", base());

    expect(result).toEqual({
      host: "pi",
      enabled: true,
      autoRecall: true,
      configPath: "/home/tester/.config/pi-qdrant-memory/config.json",
      qdrant: { url: "http://127.0.0.1:6333", collection: "pi_memory" },
      embeddings: {
        baseUrl: "http://127.0.0.1:8080/v1",
        model: "bge-m3",
        dimension: 1024,
        queryPrefix: "search_query: ",
      },
      retrieval: {
        topK: 5,
        candidatesPerLane: 20,
        minScore: 0.35,
        projectBoost: 0.05,
        contextBudgetChars: 1200,
        toolResultBudgetChars: 8000,
        hardContextCharBudget: 16000,
        timeoutMs: 2500,
      },
      admin: {
        source: {
          url: "http://127.0.0.1:6333",
          collection: "hermes_memory",
          schema: "hermes-qdrant-memory-v0.9-compatible",
        },
      },
    });
  });

  it("applies env > host > shared > defaults", async () => {
    const result = await loadConfig("prime", {
      env: {
        PI_QDRANT_MEMORY_TOP_K: "7",
        PI_QDRANT_MEMORY_QDRANT_API_KEY: "runtime-secret",
      },
      homeDir: "/home/tester",
      xdgConfigHome: "/cfg",
      readTextFile: read({
        qdrant: { url: "http://shared:6333", collection: "shared" },
        retrieval: { topK: 4, minScore: 0.4 },
        prime: { retrieval: { topK: 6 }, enabled: false },
      }),
    });

    expect(result.qdrant.url).toBe("http://shared:6333");
    expect(result.qdrant.collection).toBe("shared");
    expect(result.retrieval.topK).toBe(7);
    expect(result.retrieval.minScore).toBe(0.4);
    expect(result.qdrant.apiKey).toBe("runtime-secret");
    expect(result.enabled).toBe(false);
    expect(result.configPath).toBe("/cfg/pi-qdrant-memory/config.json");
  });

  it("applies every documented environment override and normalizes URLs", async () => {
    const result = await loadConfig("pi", {
      ...base({
        PI_QDRANT_MEMORY_QDRANT_URL: "https://qdrant.example///",
        PI_QDRANT_MEMORY_QDRANT_COLLECTION: "runtime_collection",
        PI_QDRANT_MEMORY_EMBEDDING_BASE_URL: "https://embed.example/v1///",
        PI_QDRANT_MEMORY_EMBEDDING_MODEL: "runtime-model",
        PI_QDRANT_MEMORY_EMBEDDING_DIMENSION: "1536",
        PI_QDRANT_MEMORY_TOP_K: "9",
        PI_QDRANT_MEMORY_CANDIDATES_PER_LANE: "99",
        PI_QDRANT_MEMORY_MIN_SCORE: "-0.5",
        PI_QDRANT_MEMORY_PROJECT_BOOST: "0.2",
        PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS: "15000",
        PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS: "14000",
        PI_QDRANT_MEMORY_TIMEOUT_MS: "30000",
        PI_QDRANT_MEMORY_AUTO_RECALL: "false",
        PI_QDRANT_MEMORY_SOURCE_QDRANT_URL: "https://source.example/",
        PI_QDRANT_MEMORY_SOURCE_QDRANT_COLLECTION: "source_collection",
        PI_QDRANT_MEMORY_QDRANT_API_KEY: "runtime-secret",
        PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY: "admin-secret",
        PI_QDRANT_MEMORY_SOURCE_QDRANT_API_KEY: "source-secret",
        PI_QDRANT_MEMORY_EMBEDDING_API_KEY: "embedding-secret",
      }),
    });

    expect(result).toMatchObject({
      autoRecall: false,
      qdrant: {
        url: "https://qdrant.example",
        collection: "runtime_collection",
        apiKey: "runtime-secret",
      },
      embeddings: {
        baseUrl: "https://embed.example/v1",
        model: "runtime-model",
        dimension: 1536,
        apiKey: "embedding-secret",
      },
      retrieval: {
        topK: 9,
        candidatesPerLane: 99,
        minScore: -0.5,
        projectBoost: 0.2,
        contextBudgetChars: 15000,
        toolResultBudgetChars: 14000,
        hardContextCharBudget: 16000,
        timeoutMs: 30000,
      },
      admin: {
        destinationApiKey: "admin-secret",
        source: {
          url: "https://source.example",
          collection: "source_collection",
        },
      },
    });
  });

  it("valid environment values override invalid lower-priority values", async () => {
    const result = await loadConfig("prime", {
      env: {
        PI_QDRANT_MEMORY_QDRANT_URL: "https://runtime.example/",
        PI_QDRANT_MEMORY_TOP_K: "6",
        PI_QDRANT_MEMORY_AUTO_RECALL: "false",
      },
      homeDir: "/home/tester",
      readTextFile: read({
        qdrant: { url: "not-a-url" },
        retrieval: { topK: 0 },
        prime: { autoRecall: "not-a-boolean" },
      }),
    });

    expect(result.qdrant.url).toBe("https://runtime.example");
    expect(result.retrieval.topK).toBe(6);
    expect(result.autoRecall).toBe(false);
  });

  it("rejects secrets stored anywhere in JSON", async () => {
    await expect(
      loadConfig("pi", {
        env: {},
        homeDir: "/home/tester",
        readTextFile: read({ nested: [{ credentials: { apiKey: "must-not-live-here" } }] }),
      }),
    ).rejects.toThrow("API keys are allowed only through environment variables");
  });

  it("rejects malformed JSON and non-object roots", async () => {
    await expect(
      loadConfig("pi", { ...base(), readTextFile: async () => "not json" }),
    ).rejects.toThrow();
    await expect(
      loadConfig("pi", { ...base(), readTextFile: async () => JSON.stringify([]) }),
    ).rejects.toThrow();
  });

  it("treats only ENOENT as an absent config file", async () => {
    await expect(
      loadConfig("pi", {
        ...base(),
        readTextFile: async () => {
          throw Object.assign(new Error("permission denied"), { code: "EACCES" });
        },
      }),
    ).rejects.toThrow("permission denied");
  });

  it("rejects URL credentials and strips only trailing slashes", async () => {
    await expect(
      loadConfig("pi", {
        ...base({ PI_QDRANT_MEMORY_QDRANT_URL: "https://user:password@example.com///" }),
      }),
    ).rejects.toThrow();

    const result = await loadConfig("pi", {
      ...base({ PI_QDRANT_MEMORY_QDRANT_URL: "https://example.com/path///?keep=slash/" }),
    });
    expect(result.qdrant.url).toBe("https://example.com/path///?keep=slash");
  });

  it.each([
    ["PI_QDRANT_MEMORY_TOP_K", "0"],
    ["PI_QDRANT_MEMORY_TOP_K", "11"],
    ["PI_QDRANT_MEMORY_CANDIDATES_PER_LANE", "0"],
    ["PI_QDRANT_MEMORY_CANDIDATES_PER_LANE", "101"],
    ["PI_QDRANT_MEMORY_MIN_SCORE", "-1.01"],
    ["PI_QDRANT_MEMORY_MIN_SCORE", "1.01"],
    ["PI_QDRANT_MEMORY_PROJECT_BOOST", "-0.01"],
    ["PI_QDRANT_MEMORY_PROJECT_BOOST", "0.26"],
    ["PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS", "0"],
    ["PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS", "16001"],
    ["PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS", "0"],
    ["PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS", "16001"],
    ["PI_QDRANT_MEMORY_EMBEDDING_DIMENSION", "0"],
    ["PI_QDRANT_MEMORY_EMBEDDING_DIMENSION", "65537"],
    ["PI_QDRANT_MEMORY_TIMEOUT_MS", "99"],
    ["PI_QDRANT_MEMORY_TIMEOUT_MS", "30001"],
  ])("rejects invalid %s=%s", async (name, value) => {
    await expect(loadConfig("prime", base({ [name]: value }))).rejects.toThrow();
  });

  it.each([
    ["PI_QDRANT_MEMORY_TOP_K", "1", 1],
    ["PI_QDRANT_MEMORY_TOP_K", "10", 10],
    ["PI_QDRANT_MEMORY_CANDIDATES_PER_LANE", "1", 1],
    ["PI_QDRANT_MEMORY_CANDIDATES_PER_LANE", "100", 100],
    ["PI_QDRANT_MEMORY_MIN_SCORE", "-1", -1],
    ["PI_QDRANT_MEMORY_MIN_SCORE", "1", 1],
    ["PI_QDRANT_MEMORY_PROJECT_BOOST", "0", 0],
    ["PI_QDRANT_MEMORY_PROJECT_BOOST", "0.25", 0.25],
    ["PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS", "1", 1],
    ["PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS", "16000", 16000],
    ["PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS", "1", 1],
    ["PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS", "16000", 16000],
    ["PI_QDRANT_MEMORY_EMBEDDING_DIMENSION", "1", 1],
    ["PI_QDRANT_MEMORY_EMBEDDING_DIMENSION", "65536", 65536],
    ["PI_QDRANT_MEMORY_TIMEOUT_MS", "100", 100],
    ["PI_QDRANT_MEMORY_TIMEOUT_MS", "30000", 30000],
  ])("accepts valid boundary %s=%s", async (name, value, expected) => {
    const result = await loadConfig("prime", base({ [name]: value }));
    const key = {
      PI_QDRANT_MEMORY_TOP_K: "topK",
      PI_QDRANT_MEMORY_CANDIDATES_PER_LANE: "candidatesPerLane",
      PI_QDRANT_MEMORY_MIN_SCORE: "minScore",
      PI_QDRANT_MEMORY_PROJECT_BOOST: "projectBoost",
      PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS: "contextBudgetChars",
      PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS: "toolResultBudgetChars",
      PI_QDRANT_MEMORY_EMBEDDING_DIMENSION: "dimension",
      PI_QDRANT_MEMORY_TIMEOUT_MS: "timeoutMs",
    }[name];
    const actual = key === "dimension" ? result.embeddings.dimension : result.retrieval[key as keyof typeof result.retrieval];
    expect(actual).toBe(expected);
  });

  it("keeps the hard context ceiling fixed after file merging", async () => {
    const result = await loadConfig("prime", {
      ...base(),
      readTextFile: read({ retrieval: { hardContextCharBudget: 999999 } }),
    });
    expect(result.retrieval.hardContextCharBudget).toBe(16000);
  });

  it("validates boolean and fixed source schema values", async () => {
    await expect(loadConfig("prime", base({ PI_QDRANT_MEMORY_AUTO_RECALL: "yes" }))).rejects.toThrow();
    await expect(
      loadConfig("prime", {
        ...base(),
        readTextFile: read({ admin: { hermesSource: { schema: "other" } } }),
      }),
    ).rejects.toThrow();
  });
});
