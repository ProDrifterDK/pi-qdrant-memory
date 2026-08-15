import { readFile } from "node:fs/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../../src/config.js";
import { completeMemory, sanitizeAuthHeaders } from "../../src/curation/llm.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";

const message = (text: string) => ({ content: [{ type: "text", text }] });

function model(overrides: Record<string, unknown> = {}) {
  return {
    id: "memory-model", name: "Memory model", api: "openai-responses", provider: "provider-a",
    baseUrl: "https://models.invalid", reasoning: false, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 4096, maxTokens: 1024,
    ...overrides,
  };
}

function policy(host: "pi" | "prime" = "pi", overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const value: ProcessingPolicy = {
    id: "placeholder", ownerHost: host, destinationIds: { qdrant: `qdrant:${host}`, embedding: `embed:${host}`, llm: "llm:provider-a" },
    originProvider: "provider-a", allowCrossProviderReplay: false, expiresAt: null, residency: "eu", dataUse: "memory", policyRevision: "r1", ...overrides,
  };
  return { ...value, id: processingPolicyHash(value) };
}

function memoryContext(selected: ReturnType<typeof model>, overrides: Record<string, unknown> = {}) {
  const host = (overrides.host as "pi" | "prime" | undefined) ?? "pi";
  const currentPolicy = (overrides.policy as ProcessingPolicy | undefined) ?? policy(host);
  const llmId = currentPolicy.destinationIds.llm!;
  return {
    host, modelRegistry: {}, memoryModel: selected, policy: currentPolicy,
    llmDestination: { id: llmId, residency: currentPolicy.residency, dataUse: currentPolicy.dataUse },
    llmDestinationBinding: { providerId: selected.provider, modelId: selected.id, destinationId: llmId },
    ...overrides,
  };
}

function input(selected = model(), overrides: Record<string, unknown> = {}) {
  return {
    envelope: "<untrusted-data>safe input</untrusted-data>", model: selected, hostContext: { messages: [] },
    maxInputTokens: 256, maxOutputTokens: 256, timeoutMs: 1000, promptRevision: "curation-v1",
    memoryContext: memoryContext(selected), ...overrides,
  };
}

function pendingReason(value: Awaited<ReturnType<typeof completeMemory>>) {
  expect(value.state).toBe("pending");
  if (value.state !== "pending") throw new Error("expected pending completion");
  return value.reason;
}

describe("completeMemory reflected portable LLM bridge", () => {
  afterEach(() => vi.useRealTimers());

  it("has one reflected namespace bridge and no static or dynamic completion access", async () => {
    const source = await readFile("src/curation/llm.ts", "utf8");
    expect(source).toMatch(/import \* as PiAi from ["']@earendil-works\/pi-ai["']/);
    expect(source).toMatch(/Reflect\.get\(.*modelRegistry.*complete/);
    expect(source).toMatch(/Reflect\.get\(.*aiNamespace.*completeSimple/);
    expect(source).not.toMatch(/PiAi\.completeSimple/);
    expect(source).not.toMatch(/import\(|eval\(/);
    expect(source).not.toMatch(/import type .*ResolvedRequestAuth[^L]/);
  });

  it("prefers the reflected Pi registry path, sends a fresh envelope context, and bounds the call", async () => {
    const complete = vi.fn(async () => message('{"items":[]}'));
    const dedicated = model({ id: "dedicated", maxTokens: 16_384, contextWindow: 32_768 });
    const hostile = { systemPrompt: "do not forward", messages: [{ role: "user", content: "unbounded", timestamp: 0 }], tools: [{ name: "bad" }] };
    const result = await completeMemory(input(dedicated, {
      hostContext: hostile, memoryContext: memoryContext(dedicated, { modelRegistry: { complete } }),
      aiNamespace: { completeSimple: vi.fn(async () => message('{"wrong":true}')) },
    }));

    expect(result).toMatchObject({ state: "completed", text: '{"items":[]}' });
    expect(complete).toHaveBeenCalledTimes(1);
    expect(complete.mock.calls[0]?.[0]).toBe(dedicated);
    expect(complete.mock.calls[0]?.[1]).toMatchObject({ messages: [{ role: "user", content: "<untrusted-data>safe input</untrusted-data>" }] });
    expect(complete.mock.calls[0]?.[1]).not.toBe(hostile);
    expect((complete.mock.calls[0]?.[1] as { tools?: unknown }).tools).toBeUndefined();
    expect(complete.mock.calls[0]?.[2]).toMatchObject({ maxTokens: 256, timeoutMs: 1000, temperature: 0, signal: expect.any(AbortSignal) });
  });

  it("uses reflected Prime fallback only after fresh sanitized structural auth", async () => {
    const completeSimple = vi.fn(async () => message('{"items":["safe"]}'));
    const auth = { ok: true as const, apiKey: "registry-key", headers: { "x-safe": "ok", "x-drop": null } };
    const getApiKeyAndHeaders = vi.fn(async () => auth);
    const selected = model();
    const result = await completeMemory(input(selected, {
      memoryContext: memoryContext(selected, { host: "prime", policy: policy("prime"), modelRegistry: { getApiKeyAndHeaders } }),
      aiNamespace: { completeSimple },
    }));

    expect(result).toMatchObject({ state: "completed", text: '{"items":["safe"]}' });
    expect(getApiKeyAndHeaders).toHaveBeenCalledWith(selected);
    const options = completeSimple.mock.calls[0]?.[2] as Record<string, unknown>;
    expect(options).toMatchObject({ apiKey: "registry-key", headers: { "x-safe": "ok" }, maxTokens: 256, timeoutMs: 1000, temperature: 0 });
    expect(options.headers).not.toBe(auth.headers);
    expect(options.headers).not.toHaveProperty("x-drop");
  });

  it("drops null and non-string auth headers into a fresh record", () => {
    const source = { keep: "value", nil: null, number: 2 as unknown as string };
    const sanitized = sanitizeAuthHeaders(source);
    expect(sanitized).toEqual({ keep: "value" });
    expect(sanitized).not.toBe(source);
  });

  it("returns typed pending states for unavailable bridges, invalid output, and output limits", async () => {
    const selected = model();
    const piMissing = await completeMemory(input(selected, { memoryContext: memoryContext(selected), aiNamespace: { completeSimple: vi.fn(async () => message("unsafe")) } }));
    expect(pendingReason(piMissing)).toBe("no_completion_method");

    const primeSelected = model();
    const primeMissing = await completeMemory(input(primeSelected, { memoryContext: memoryContext(primeSelected, { host: "prime", policy: policy("prime") }), aiNamespace: {} }));
    expect(pendingReason(primeMissing)).toBe("no_completion_method");

    const invalid = await completeMemory(input(selected, { memoryContext: memoryContext(selected, { modelRegistry: { complete: vi.fn(async () => ({ bad: true })) } }) }));
    expect(pendingReason(invalid)).toBe("invalid_response");
    const oversized = await completeMemory(input(selected, { memoryContext: memoryContext(selected, { modelRegistry: { complete: vi.fn(async () => message("x".repeat(257))) } }) }));
    expect(pendingReason(oversized)).toBe("output_limit");
  });

  it("gates active fallback and preserves computed, redacted provenance", async () => {
    const complete = vi.fn(async () => message("safe"));
    const active = model({ id: "active" });
    const disabled = await completeMemory(input(active, { memoryContext: memoryContext(active, { memoryModel: undefined, activeModel: active, modelRegistry: { complete } }) }));
    expect(pendingReason(disabled)).toBe("fallback_disabled");

    const currentPolicy = policy();
    const allowed = await completeMemory(input(active, {
      memoryContext: memoryContext(active, { memoryModel: undefined, activeModel: active, activeProviderId: "provider-a", sessionProviderId: "provider-a", allowActiveModelFallback: true, policy: currentPolicy, modelRegistry: { complete } }),
    }));
    expect(allowed).toMatchObject({ state: "completed", provenance: { providerId: "provider-a", modelId: "active", policyId: currentPolicy.id, policyHash: processingPolicyHash(currentPolicy) } });
    expect(JSON.stringify(allowed)).not.toMatch(/safe input|authorization/i);
  });

  it("requires both producer policy and processing context to authorize cross-provider replay", async () => {
    const selected = model({ provider: "provider-b", id: "model-b" }); const complete = vi.fn(async () => message("safe"));
    const deniedPolicy = policy("pi", { destinationIds: { qdrant: "qdrant:pi", embedding: "embed:pi", llm: "llm:provider-b" }, originProvider: "provider-a", allowCrossProviderReplay: false });
    const denied = await completeMemory(input(selected, { memoryContext: memoryContext(selected, { policy: deniedPolicy, allowCrossProviderReplay: true, modelRegistry: { complete } }) }));
    expect(pendingReason(denied)).toBe("cross_provider_disabled"); expect(complete).not.toHaveBeenCalled();
    const allowedPolicy = policy("pi", { destinationIds: { qdrant: "qdrant:pi", embedding: "embed:pi", llm: "llm:provider-b" }, originProvider: "provider-a", allowCrossProviderReplay: true });
    const deniedContext = await completeMemory(input(selected, { memoryContext: memoryContext(selected, { policy: allowedPolicy, allowCrossProviderReplay: false, modelRegistry: { complete } }) }));
    expect(pendingReason(deniedContext)).toBe("cross_provider_disabled"); expect(complete).not.toHaveBeenCalled();
    const allowed = await completeMemory(input(selected, { memoryContext: memoryContext(selected, { policy: allowedPolicy, allowCrossProviderReplay: true, modelRegistry: { complete } }) }));
    expect(allowed).toMatchObject({ state: "completed", provenance: { providerId: "provider-b", modelId: "model-b" } }); expect(complete).toHaveBeenCalledTimes(1);
  });

  it("links registry cancellation and timeout to the host signal and cleans resources", async () => {
    vi.useFakeTimers();
    const host = new AbortController();
    const remove = vi.spyOn(host.signal, "removeEventListener");
    const selected = model();
    const waiting = vi.fn((_model: unknown, _context: unknown, options: { signal?: AbortSignal }) => new Promise((_, reject) => {
      options.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    }));
    const cancelled = completeMemory(input(selected, { signal: host.signal, memoryContext: memoryContext(selected, { modelRegistry: { complete: waiting } }) }));
    host.abort();
    expect(pendingReason(await cancelled)).toBe("cancelled");
    expect(remove).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);

    const timedOut = completeMemory(input(selected, { memoryContext: memoryContext(selected, { modelRegistry: { complete: waiting } }) }));
    await vi.advanceTimersByTimeAsync(1000);
    expect(pendingReason(await timedOut)).toBe("timeout");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects BGE-M3 at config and runtime before egress", async () => {
    await expect(loadConfig("pi", { env: {}, homeDir: "/home/tester", readTextFile: async () => JSON.stringify({ memoryModel: { modelId: "BAAI/bge-m3" } }) })).rejects.toThrow(/BGE-M3|generation/i);
    const complete = vi.fn(async () => message("unsafe"));
    const bge = model({ id: "BAAI/bge m3", name: "BGE M3" });
    const result = await completeMemory(input(bge, { memoryContext: memoryContext(bge, { modelRegistry: { complete } }) }));
    expect(pendingReason(result)).toBe("unsupported_model");
    expect(complete).not.toHaveBeenCalled();
  });
});
