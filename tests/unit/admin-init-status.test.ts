import { describe, expect, it } from "vitest";
import { initializeDestination } from "../../src/admin/init.js";
import { memoryStatus } from "../../src/admin/status.js";
import type { RuntimeConfig } from "../../src/types.js";

function config(): RuntimeConfig {
  return {
    host: "pi",
    enabled: true,
    autoRecall: true,
    configPath: "/tmp/config.json",
    qdrant: { url: "http://destination", collection: "pi_memory", apiKey: "runtime-secret" },
    embeddings: { baseUrl: "http://embeddings/v1", model: "bge-m3", dimension: 3, queryPrefix: "search_query: ", apiKey: "embedding-secret" },
    retrieval: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0.05, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500 },
    admin: { destinationApiKey: "admin-secret", source: { url: "http://source", collection: "hermes_memory", schema: "hermes-qdrant-memory-v0.9-compatible", apiKey: "source-secret" } },
  };
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}

describe("destination initialization", () => {
  it("creates the destination with cosine vectors and safety indexes", async () => {
    const calls: Array<{ url: string; method?: string; body?: unknown; key?: string | null }> = [];
    const fetchImpl = (async (url: string, init?: RequestInit) => {
      calls.push({ url, method: init?.method, body: init?.body === undefined ? undefined : JSON.parse(String(init.body)), key: new Headers(init?.headers).get("api-key") });
      if (init?.method === "GET") return json({ status: { error: "missing" } }, 404);
      return json({ result: init?.method === "PUT" && url.endsWith("/collections/pi_memory") ? true : { status: "completed", operation_id: null } });
    }) as typeof fetch;
    await expect(initializeDestination(config(), { fetchImpl })).resolves.toEqual({ created: true, collection: "pi_memory", dimension: 3, distance: "Cosine" });
    expect(calls).toContainEqual({ url: "http://destination/collections/pi_memory", method: "PUT", body: { vectors: { size: 3, distance: "Cosine" } }, key: "admin-secret" });
    expect(calls.filter((call) => call.url.endsWith("/index?wait=true")).map((call) => call.body)).toEqual([
      { field_name: "host", field_schema: "keyword" },
      { field_name: "project_id", field_schema: "keyword" },
      { field_name: "status", field_schema: "keyword" },
      { field_name: "secret_scan", field_schema: "keyword" },
      { field_name: "source_type", field_schema: "keyword" },
    ]);
    expect(calls.every((call) => call.key === "admin-secret")).toBe(true);
  });

  it("rejects an existing incompatible collection", async () => {
    const fetchImpl = (async () => json({ result: { points_count: 0, config: { params: { vectors: { size: 4, distance: "Euclid" } } }, payload_schema: {} } })) as typeof fetch;
    await expect(initializeDestination(config(), { fetchImpl })).rejects.toMatchObject({ category: "configuration" });
  });
});

describe("memoryStatus", () => {
  it("is mutation-free, redacted, and handles a missing destination", async () => {
    const calls: Array<{ url: string; method?: string; key?: string | null; body?: string }> = [];
    const fetchImpl = (async (url: string, init?: RequestInit) => {
      calls.push({ url, method: init?.method, key: new Headers(init?.headers).get("api-key"), body: typeof init?.body === "string" ? init.body : undefined });
      if (url === "http://embeddings/v1/embeddings") return json({ data: [{ embedding: [0.1, 0.2, 0.3] }] });
      if (url.endsWith("/healthz")) return new Response("healthz check passed", { status: 200 });
      if (url === "http://destination/collections/pi_memory") return json({ status: { error: "missing" } }, 404);
      if (url === "http://source/collections/hermes_memory") return json({ result: { points_count: 5, config: { params: { vectors: { size: 3, distance: "Cosine" } } }, payload_schema: {} } });
      throw new Error("unexpected endpoint");
    }) as typeof fetch;
    const result = await memoryStatus(config(), { fetchImpl });
    expect(result.destination.collection).toBe("pi_memory");
    expect(result.destination.exists).toBe(false);
    expect(result.destination.pointCount).toBeNull();
    expect(result.embeddings.healthy).toBe(true);
    expect(result.source.pointCount).toBe(5);
    expect(JSON.stringify(result)).not.toContain("secret");
    expect(calls.every((call) => !call.url.includes("?"))).toBe(true);
    expect(calls.some((call) => call.method === "PUT")).toBe(false);
    expect(calls.find((call) => call.url.startsWith("http://destination/"))?.key).toBe("admin-secret");
    expect(calls.find((call) => call.url.startsWith("http://source/"))?.key).toBe("source-secret");
  });
});
