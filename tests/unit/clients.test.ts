import { describe, expect, it, vi, afterEach } from "vitest";
import { fetchJson, MemoryClientError } from "../../src/clients/http.js";
import { EmbeddingsClient } from "../../src/clients/embeddings.js";
import { ReadonlyQdrantClient } from "../../src/clients/qdrant-readonly.js";

function errorDetails(error: unknown): { category: string; status?: number; text: string } {
  expect(error).toBeInstanceOf(MemoryClientError);
  const clientError = error as MemoryClientError;
  return { category: clientError.category, status: clientError.status, text: clientError.toString() };
}

describe("abortable HTTP client", () => {
  afterEach(() => vi.useRealTimers());

  it("parses successful JSON without exposing request details on parse failure", async () => {
    const query = "secret query text";
    const apiKey = "secret-api-key";
    const authorization = `Bearer ${apiKey}`;
    const body = JSON.stringify({ query });
    const url = "http://service.test/v1/items?token=secret-url-token";

    const error = await fetchJson(url, { method: "POST", headers: { authorization }, body }, {
      timeoutMs: 2500,
      fetchImpl: vi.fn(async () => new Response("not-json: secret response body", { status: 200 })),
    }).catch((value: unknown) => value);

    const details = errorDetails(error);
    expect(details.category).toBe("invalid-json");
    expect(details.status).toBeUndefined();
    expect(details.text).not.toContain(apiKey);
    expect(details.text).not.toContain(query);
    expect(details.text).not.toContain(authorization);
    expect(details.text).not.toContain("secret response body");
    expect(details.text).not.toContain("secret-url-token");
  });

  it("redacts URL, query, headers, body, and response body for HTTP failures", async () => {
    const query = "secret query text";
    const apiKey = "secret-api-key";
    const authorization = `Bearer ${apiKey}`;
    const responseBody = "response body with secret-api-key";
    const url = "http://service.test/v1/items?query=secret-url-token";

    const error = await fetchJson(
      url,
      { method: "POST", headers: { authorization }, body: JSON.stringify({ query }) },
      {
        timeoutMs: 2500,
        fetchImpl: vi.fn(async () => new Response(responseBody, { status: 404 })),
      },
    ).catch((value: unknown) => value);

    const details = errorDetails(error);
    expect(details.category).toBe("http");
    expect(details.status).toBe(404);
    for (const secret of [apiKey, query, authorization, responseBody, "secret-url-token"])
      expect(details.text).not.toContain(secret);
  });

  it("distinguishes host cancellation from timeout and removes the host listener", async () => {
    vi.useFakeTimers();
    const host = new AbortController();
    const add = vi.spyOn(host.signal, "addEventListener");
    const remove = vi.spyOn(host.signal, "removeEventListener");
    const fetchImpl = vi.fn((_url: string, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
      }),
    );

    const pending = fetchJson("http://service.test/data", {}, { timeoutMs: 100, signal: host.signal, fetchImpl }).catch((value: unknown) => value);
    host.abort();
    const cancelled = await pending;
    expect(errorDetails(cancelled).category).toBe("cancelled");
    expect(add).toHaveBeenCalledTimes(1);
    expect(remove).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reports timeout and clears its timer", async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn((_url: string, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
      }),
    );

    const pending = fetchJson("http://service.test/data", {}, { timeoutMs: 100, fetchImpl }).catch((value: unknown) => value);
    await vi.advanceTimersByTimeAsync(100);
    const timedOut = await pending;
    expect(errorDetails(timedOut).category).toBe("timeout");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("times out when JSON body consumption stalls after headers", async () => {
    vi.useFakeTimers();
    let bodyCancelled = false;
    const fetchImpl = vi.fn(async (_url: string, _init?: RequestInit) => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('{"result":'));
        },
        cancel() {
          bodyCancelled = true;
        },
      });
      return new Response(stream, { status: 200, headers: { "content-type": "application/json" } });
    });

    const pending = fetchJson("http://service.test/data", {}, { timeoutMs: 100, fetchImpl }).catch((value: unknown) => value);
    const watchdog = new Promise<symbol>((resolve) => setTimeout(() => resolve(Symbol.for("body-stall")), 150));
    const outcome = Promise.race([pending, watchdog]);
    await vi.advanceTimersByTimeAsync(150);
    const result = await outcome;
    expect(typeof result).toBe("object");
    expect(errorDetails(result).category).toBe("timeout");
    expect(bodyCancelled).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("propagates host cancellation after headers while JSON body is pending", async () => {
    vi.useFakeTimers();
    const host = new AbortController();
    const add = vi.spyOn(host.signal, "addEventListener");
    const remove = vi.spyOn(host.signal, "removeEventListener");
    let bodyStarted = false;
    let bodyAborted = false;
    let fetchSignal: AbortSignal | undefined;
    let releaseFetch: ((response: Response) => void) | undefined;
    const fetchImpl = vi.fn((_url: string, init?: RequestInit) => {
      fetchSignal = init?.signal;
      return new Promise<Response>((resolve) => { releaseFetch = resolve; });
    });
    const stalledBody = (): Promise<never> => new Promise((_resolve, reject) => {
      bodyStarted = true;
      fetchSignal?.addEventListener("abort", () => {
        bodyAborted = true;
        reject(new DOMException("aborted", "AbortError"));
      }, { once: true });
    });

    const pending = fetchJson("http://service.test/data", {}, {
      timeoutMs: 100,
      signal: host.signal,
      fetchImpl,
    }).catch((value: unknown) => value);
    expect(releaseFetch).toBeTypeOf("function");
    const response = {
      ok: true,
      status: 200,
      clone: () => ({ arrayBuffer: stalledBody }),
      json: stalledBody,
    } as unknown as Response;
    releaseFetch?.(response);
    for (let attempt = 0; attempt < 10 && !bodyStarted; attempt += 1) await Promise.resolve();
    expect(bodyStarted).toBe(true);

    host.abort();
    const watchdog = new Promise<symbol>((resolve) => setTimeout(() => resolve(Symbol.for("body-cancel")), 150));
    const outcome = Promise.race([pending, watchdog]);
    await vi.advanceTimersByTimeAsync(150);
    const result = await outcome;
    expect(typeof result).toBe("object");
    expect(errorDetails(result).category).toBe("cancelled");
    expect(bodyAborted).toBe(true);
    expect(add).toHaveBeenCalledTimes(1);
    expect(remove).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("maps non-abort fetch failures to a redacted network error", async () => {
    const error = await fetchJson("http://service.test/data?secret=1", {}, {
      timeoutMs: 2500,
      fetchImpl: vi.fn(async () => { throw new Error("network secret query text"); }),
    }).catch((value: unknown) => value);
    const details = errorDetails(error);
    expect(details.category).toBe("network");
    expect(details.text).not.toContain("network secret query text");
  });
});

describe("EmbeddingsClient", () => {
  it("prefixes and validates query embeddings", async () => {
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      expect(JSON.parse(String(init?.body))).toEqual({ model: "bge-m3", input: "search_query: alpha" });
      expect(init?.method).toBe("POST");
      expect(new Headers(init?.headers).get("content-type")).toBe("application/json");
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer embedding-secret");
      return new Response(JSON.stringify({ data: [{ embedding: [0.1, 0.2, 0.3] }] }), { status: 200 });
    });
    const client = new EmbeddingsClient({
      baseUrl: "http://embed/v1",
      model: "bge-m3",
      dimension: 3,
      queryPrefix: "search_query: ",
      apiKey: "embedding-secret",
      timeoutMs: 2500,
      fetchImpl,
    });
    await expect(client.embedQuery("alpha")).resolves.toEqual([0.1, 0.2, 0.3]);
  });

  it.each([
    [[0.1, 0.2], "dimension"],
    [[0.1, Number.NaN, 0.3], "finite"],
    [[0.1, Number.POSITIVE_INFINITY, 0.3], "finite"],
    [[0.1, "0.2", 0.3], "finite"],
  ])("rejects invalid embedding vectors (%s)", async (embedding, _reason) => {
    const client = new EmbeddingsClient({
      baseUrl: "http://embed/v1",
      model: "bge-m3",
      dimension: 3,
      queryPrefix: "search_query: ",
      timeoutMs: 2500,
      fetchImpl: vi.fn(async () => new Response(JSON.stringify({ data: [{ embedding }] }), { status: 200 })),
    });
    const error = await client.embedQuery("alpha").catch((value: unknown) => value);
    expect(errorDetails(error).category).toBe("invalid-response");
  });
});

describe("ReadonlyQdrantClient", () => {
  it("checks plain-text health with the api-key header", async () => {
    const fetchImpl = vi.fn(async (url: string, init?: RequestInit) => {
      expect(url).toBe("http://qdrant/healthz");
      expect(init?.method).toBe("GET");
      expect(new Headers(init?.headers).get("api-key")).toBe("qdrant-readonly-secret");
      expect(new Headers(init?.headers).get("authorization")).toBeNull();
      return new Response("healthz check passed", { status: 200, headers: { "content-type": "text/plain" } });
    });
    const client = new ReadonlyQdrantClient({
      baseUrl: "http://qdrant",
      collection: "pi_memory",
      apiKey: "qdrant-readonly-secret",
      timeoutMs: 2500,
      fetchImpl,
    });
    await expect(client.health()).resolves.toBeUndefined();
  });

  it("validates a single dense-vector collection shape", async () => {
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({ result: {
      config: { params: { vectors: { size: 3, distance: "Cosine" } } },
    } }), { status: 200 }));
    const client = new ReadonlyQdrantClient({ baseUrl: "http://qdrant", collection: "pi_memory", timeoutMs: 2500, fetchImpl });
    await expect(client.collectionInfo()).resolves.toEqual({ dimension: 3, distance: "Cosine" });
  });

  it.each([
    { vectors: { text: { size: 3, distance: "Cosine" } } },
    { vectors: { size: 3 } },
    { vectors: { size: 0, distance: "Cosine" } },
    { vectors: { size: 3, distance: 4 } },
    { vectors: [{ size: 3, distance: "Cosine" }] },
  ])("rejects malformed or named vector metadata: %j", async (vectors) => {
    const client = new ReadonlyQdrantClient({
      baseUrl: "http://qdrant",
      collection: "pi_memory",
      timeoutMs: 2500,
      fetchImpl: vi.fn(async () => new Response(JSON.stringify({ result: { config: { params: { vectors } } } }), { status: 200 })),
    });
    const error = await client.collectionInfo().catch((value: unknown) => value);
    expect(errorDetails(error).category).toBe("invalid-response");
  });

  it("uses only the Qdrant search endpoint at runtime", async () => {
    const fetchImpl = vi.fn(async (url: string, init?: RequestInit) => {
      expect(url).toBe("http://qdrant/collections/pi_memory/points/search");
      expect(init?.method).toBe("POST");
      expect(new Headers(init?.headers).get("api-key")).toBe("qdrant-readonly-secret");
      expect(new Headers(init?.headers).get("authorization")).toBeNull();
      expect(JSON.parse(String(init?.body))).toEqual({
        vector: [1, 0, 0],
        limit: 5,
        filter: { must: [] },
        with_payload: true,
        with_vector: false,
      });
      return new Response(JSON.stringify({ result: [{ id: "point-a", score: 0.9, payload: { text: "safe" } }] }), { status: 200 });
    });
    const client = new ReadonlyQdrantClient({
      baseUrl: "http://qdrant",
      collection: "pi_memory",
      apiKey: "qdrant-readonly-secret",
      timeoutMs: 2500,
      fetchImpl,
    });
    await expect(client.search({ vector: [1, 0, 0], limit: 5, filter: { must: [] } })).resolves.toEqual([
      { id: "point-a", score: 0.9, payload: { text: "safe" } },
    ]);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it.each([
    { id: {}, score: 0.9, payload: {} },
    { id: "point-a", score: Number.NaN, payload: {} },
    { id: "point-a", score: 0.9, payload: null },
    { id: "point-a", score: 0.9, payload: [] },
  ])("rejects malformed search hits: %j", async (hit) => {
    const client = new ReadonlyQdrantClient({
      baseUrl: "http://qdrant",
      collection: "pi_memory",
      timeoutMs: 2500,
      fetchImpl: vi.fn(async () => new Response(JSON.stringify({ result: [hit] }), { status: 200 })),
    });
    const error = await client.search({ vector: [1], limit: 1, filter: { must: [] } }).catch((value: unknown) => value);
    expect(errorDetails(error).category).toBe("invalid-response");
  });

  it("does not expose mutation or generic request capabilities", () => {
    const prototype = ReadonlyQdrantClient.prototype as unknown as Record<string, unknown>;
    expect(prototype.createCollection).toBeUndefined();
    expect(prototype.createIndex).toBeUndefined();
    expect(prototype.upsert).toBeUndefined();
    expect(prototype.delete).toBeUndefined();
    expect(prototype.request).toBeUndefined();
  });
});
