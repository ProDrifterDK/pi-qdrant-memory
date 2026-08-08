import { describe, expect, it } from "vitest";
import { MemoryClientError } from "../../src/clients/http.js";
import { AdminQdrantClient } from "../../src/admin/qdrant-admin.js";

interface Call { url: string; method?: string; headers?: Headers; body?: unknown }

function response(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}

function client(fetchImpl: typeof fetch): AdminQdrantClient {
  return new AdminQdrantClient({
    baseUrl: "http://qdrant/",
    apiKey: "admin-secret",
    timeoutMs: 2500,
    fetchImpl,
  });
}

describe("AdminQdrantClient", () => {
  it("uses only the administrative key and parses compatible metadata", async () => {
    const calls: Call[] = [];
    const fetchImpl = (async (url: string, init?: RequestInit) => {
      calls.push({ url, method: init?.method, headers: new Headers(init?.headers) });
      return response({ result: {
        points_count: null,
        config: { params: { vectors: { size: 1024, distance: "Cosine" } } },
        payload_schema: {},
      } });
    }) as typeof fetch;
    await expect(client(fetchImpl).collectionInfo("pi_memory")).resolves.toMatchObject({
      dimension: 1024,
      distance: "Cosine",
      pointCount: null,
    });
    expect(calls[0]?.url).toBe("http://qdrant/collections/pi_memory");
    expect(calls[0]?.headers?.get("api-key")).toBe("admin-secret");
    expect(calls[0]?.headers?.get("authorization")).toBeNull();
  });

  it("creates keyword indexes, scrolls with vectors, and upserts with wait=true", async () => {
    const calls: Call[] = [];
    const fetchImpl = (async (url: string, init?: RequestInit) => {
      calls.push({ url, method: init?.method, headers: new Headers(init?.headers), body: init?.body === undefined ? undefined : JSON.parse(String(init.body)) });
      if (url.includes("/scroll")) return response({ result: { points: [{ id: 7, vector: [0, 1], payload: { text: "safe" } }], next_page_offset: 8 } });
      return response({ result: { status: "completed", operation_id: null } });
    }) as typeof fetch;
    const qdrant = client(fetchImpl);
    await qdrant.createKeywordIndex("pi_memory", "host");
    await expect(qdrant.scroll("pi_memory", "offset-a", 256)).resolves.toEqual({
      points: [{ id: 7, vector: [0, 1], payload: { text: "safe" } }],
      nextOffset: 8,
    });
    await qdrant.upsert("pi_memory", [{ id: "point-a", vector: [0, 1], payload: { text: "safe" } }]);
    expect(calls.map((call) => [call.url, call.method, call.body])).toEqual([
      ["http://qdrant/collections/pi_memory/index?wait=true", "PUT", { field_name: "host", field_schema: "keyword" }],
      ["http://qdrant/collections/pi_memory/points/scroll", "POST", { offset: "offset-a", limit: 256, with_payload: true, with_vector: true }],
      ["http://qdrant/collections/pi_memory/points?wait=true", "PUT", { points: [{ id: "point-a", vector: [0, 1], payload: { text: "safe" } }] }],
    ]);
    expect(calls.every((call) => call.headers?.get("api-key") === "admin-secret")).toBe(true);
  });

  it("rejects invalid scroll points and non-completed upsert outcomes", async () => {
    const badScroll = new AdminQdrantClient({
      baseUrl: "http://qdrant",
      apiKey: "admin-secret",
      timeoutMs: 2500,
      fetchImpl: (async () => response({ result: { points: [{ id: 1, vector: [Number.NaN], payload: {} }] } })) as typeof fetch,
    });
    await expect(badScroll.scroll("pi_memory")).rejects.toMatchObject({ category: "invalid-response" });

    const timedOut = new AdminQdrantClient({
      baseUrl: "http://qdrant",
      apiKey: "admin-secret",
      timeoutMs: 2500,
      fetchImpl: (async () => response({ result: { status: "wait_timeout", operation_id: 1 } })) as typeof fetch,
    });
    const error = await timedOut.upsert("pi_memory", [{ id: 1, vector: [0], payload: {} }]).catch((value: unknown) => value);
    expect(error).toBeInstanceOf(MemoryClientError);
    expect((error as MemoryClientError).category).toBe("invalid-response");
  });
});
