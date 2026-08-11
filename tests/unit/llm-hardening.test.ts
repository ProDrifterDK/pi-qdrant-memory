import { readFile } from "node:fs/promises";
import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../../src/config.js";
import { completeMemory } from "../../src/curation/llm.js";
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

function policy(overrides: Partial<ProcessingPolicy> = {}): ProcessingPolicy {
  const value: ProcessingPolicy = {
    id: "placeholder", ownerHost: "pi", destinationIds: { qdrant: "qdrant:pi", embedding: "embed:pi", llm: "llm:provider-a" },
    originProvider: "provider-a", allowCrossProviderReplay: false, expiresAt: null,
    residency: "eu", dataUse: "memory", policyRevision: "r1", ...overrides,
  };
  return { ...value, id: processingPolicyHash(value) };
}

function context(selected = model(), overrides: Record<string, unknown> = {}) {
  const currentPolicy = policy();
  return {
    host: "pi" as const, modelRegistry: {}, memoryModel: selected, policy: currentPolicy,
    llmDestination: { id: "llm:provider-a", residency: "eu", dataUse: "memory" },
    llmDestinationBinding: { providerId: selected.provider, modelId: selected.id, destinationId: "llm:provider-a" },
    ...overrides,
  };
}

function input(overrides: Record<string, unknown> = {}) {
  const selected = model();
  return {
    envelope: "<untrusted-data>bounded envelope</untrusted-data>", model: selected,
    hostContext: { messages: [] }, maxInputTokens: 256, maxOutputTokens: 256, timeoutMs: 1000,
    promptRevision: "curation-v1", memoryContext: context(selected), ...overrides,
  };
}

function reason(value: Awaited<ReturnType<typeof completeMemory>>) {
  expect(value.state).toBe("pending");
  if (value.state !== "pending") throw new Error("completion unexpectedly egressed");
  return value.reason;
}

describe("memory LLM hardening contract", () => {
  it("requires a content-addressed, current policy and an exact required LLM destination", async () => {
    const complete = vi.fn(async () => message("safe"));
    const selected = model();
    const forged = { ...policy(), id: "arbitrary-policy-id" };
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, policy: forged }) })))).toBe("policy");

    const expired = policy({ expiresAt: "2020-01-01T00:00:00.000Z" });
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, policy: expired }) })))).toBe("policy");

    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, llmDestination: undefined }) })))).toBe("policy");
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, policy: policy({ ownerHost: "prime" }) }) })))).toBe("policy");
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, llmDestination: { id: "llm:provider-a", residency: "us", dataUse: "memory" } }) })))).toBe("policy");
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, policyHash: "caller-selected-hash" }) })))).toBe("policy");
    expect(complete).not.toHaveBeenCalled();
  });

  it("derives provenance policy hash instead of accepting caller-controlled text", async () => {
    const complete = vi.fn(async () => message("safe"));
    const selected = model();
    const currentPolicy = policy();
    const result = await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, policy: currentPolicy, policyHash: processingPolicyHash(currentPolicy) }) }));
    expect(result).toMatchObject({ state: "completed", provenance: { policyId: currentPolicy.id, policyHash: processingPolicyHash(currentPolicy) } });
    expect(JSON.stringify(result)).not.toContain("caller-selected-hash");
  });

  it("binds input.model to a context-selected model and never uses it as an unauthorised candidate", async () => {
    const complete = vi.fn(async () => message("safe"));
    const dedicated = model({ id: "dedicated" });
    expect(reason(await completeMemory(input({ model: model({ id: "attacker-choice" }), memoryContext: context(dedicated, { modelRegistry: { complete } }) })))).toBe("no_model");
    expect(reason(await completeMemory(input({ memoryContext: { ...context(model()), memoryModel: undefined, activeModel: undefined, modelRegistry: { complete } } })))).toBe("no_model");

    const active = model({ id: "active" });
    expect(reason(await completeMemory(input({ model: model({ id: "not-active" }), memoryContext: context(active, { memoryModel: undefined, activeModel: active, activeProviderId: "provider-a", sessionProviderId: "provider-a", allowActiveModelFallback: true, modelRegistry: { complete }, llmDestinationBinding: { providerId: "provider-a", modelId: "active", destinationId: "llm:provider-a" } }) })))).toBe("no_model");
    expect(complete).not.toHaveBeenCalled();
  });

  it("enforces replay opt-ins for dedicated and active models and rejects incoherent active markers", async () => {
    const complete = vi.fn(async () => message("safe"));
    const dedicated = model({ id: "dedicated", provider: "provider-b" });
    const foreignPolicy = policy({ originProvider: "provider-a" });
    expect(reason(await completeMemory(input({ model: dedicated, memoryContext: context(dedicated, { modelRegistry: { complete }, policy: foreignPolicy, llmDestinationBinding: { providerId: "provider-b", modelId: "dedicated", destinationId: "llm:provider-a" } }) })))).toBe("cross_provider_disabled");

    const active = model({ id: "active" });
    expect(reason(await completeMemory(input({ model: active, memoryContext: context(active, { memoryModel: undefined, activeModel: active, allowActiveModelFallback: true, activeProviderId: "provider-a", sessionProviderId: "provider-b", modelRegistry: { complete }, llmDestinationBinding: { providerId: "provider-a", modelId: "active", destinationId: "llm:provider-a" } }) })))).toBe("policy");
    expect(complete).not.toHaveBeenCalled();
  });

  it("requires an explicit selected-model-to-authorized-destination binding", async () => {
    const complete = vi.fn(async () => message("safe"));
    const selected = model();
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, llmDestinationBinding: { providerId: "provider-b", modelId: selected.id, destinationId: "llm:provider-a" } }) })))).toBe("policy");
    expect(reason(await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { complete }, llmDestinationBinding: undefined }) })))).toBe("policy");
    expect(complete).not.toHaveBeenCalled();
  });

  it("does not use Prime namespace fallback from Pi", async () => {
    const completeSimple = vi.fn(async () => message("unsafe namespace egress"));
    const auth = vi.fn(async () => ({ ok: true as const }));
    const selected = model();
    const result = await completeMemory(input({ model: selected, memoryContext: context(selected, { modelRegistry: { getApiKeyAndHeaders: auth } }), aiNamespace: { completeSimple } }));
    expect(reason(result)).toBe("no_completion_method");
    expect(auth).not.toHaveBeenCalled();
    expect(completeSimple).not.toHaveBeenCalled();
  });

  it("covers authentication and completion with one cancellation/timeout budget", async () => {
    vi.useFakeTimers();
    const selected = model();
    const auth = vi.fn(async () => await new Promise<never>(() => undefined));
    const completion = completeMemory(input({ model: selected, memoryContext: context(selected, { host: "prime", policy: policy({ ownerHost: "prime" }), modelRegistry: { getApiKeyAndHeaders: auth } }), aiNamespace: { completeSimple: vi.fn(async () => message("never")) } }));
    await vi.advanceTimersByTimeAsync(1000);
    const observed = await Promise.race([completion, Promise.resolve({ state: "still-waiting" } as const)]);
    expect(reason(observed as Awaited<ReturnType<typeof completeMemory>>)).toBe("timeout");
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  });

  it("never invokes auth or completion when already aborted", async () => {
    const abort = new AbortController(); abort.abort();
    const registryComplete = vi.fn(async () => message("unsafe"));
    const selected = model();
    expect(reason(await completeMemory(input({ model: selected, signal: abort.signal, memoryContext: context(selected, { modelRegistry: { complete: registryComplete } }) })))).toBe("cancelled");
    expect(registryComplete).not.toHaveBeenCalled();

    const auth = vi.fn(async () => ({ ok: true as const }));
    const namespaceComplete = vi.fn(async () => message("unsafe"));
    expect(reason(await completeMemory(input({ model: selected, signal: abort.signal, memoryContext: context(selected, { host: "prime", policy: policy({ ownerHost: "prime" }), modelRegistry: { getApiKeyAndHeaders: auth } }), aiNamespace: { completeSimple: namespaceComplete } })))).toBe("cancelled");
    expect(auth).not.toHaveBeenCalled();
    expect(namespaceComplete).not.toHaveBeenCalled();
  });

  it("sends only a fresh exact bounded-envelope context without host prompts or tools", async () => {
    const complete = vi.fn(async () => message("safe"));
    const selected = model();
    const hostileHostContext = {
      systemPrompt: "raw secret prompt must not egress", messages: [{ role: "user", content: "x".repeat(100_000), timestamp: 0 }],
      tools: [{ name: "exfiltrate", description: "unsafe", parameters: {} }],
    };
    const result = await completeMemory(input({ model: selected, envelope: "<untrusted-data>the exact envelope</untrusted-data>", hostContext: hostileHostContext, memoryContext: context(selected, { modelRegistry: { complete } }) }));
    expect(result).toMatchObject({ state: "completed" });
    const sent = complete.mock.calls[0]?.[1] as { messages?: Array<{ role: string; content: unknown }>; tools?: unknown; systemPrompt?: unknown };
    expect(sent).not.toBe(hostileHostContext);
    expect(sent).toMatchObject({ messages: [{ role: "user", content: "<untrusted-data>the exact envelope</untrusted-data>" }] });
    expect(sent.tools).toBeUndefined();
    expect(sent.systemPrompt).toBeUndefined();
    expect(JSON.stringify(result)).not.toMatch(/raw secret prompt|100000|exfiltrate/i);
  });

  it("validates contextWindow/maxTokens and rejects BGE-M3 aliases before egress", async () => {
    const complete = vi.fn(async () => message("unsafe"));
    const alias = model({ id: "BAAI/bge m3", name: "BGE M3" });
    expect(reason(await completeMemory(input({ model: alias, memoryContext: context(alias, { modelRegistry: { complete }, llmDestinationBinding: { providerId: "provider-a", modelId: "BAAI/bge m3", destinationId: "llm:provider-a" } }) })))).toBe("unsupported_model");

    const constrained = model({ contextWindow: 300, maxTokens: 256 });
    expect(reason(await completeMemory(input({ model: constrained, memoryContext: context(constrained, { modelRegistry: { complete } }) })))).toBe("invalid_input");
    const outputLimited = model({ maxTokens: 255 });
    expect(reason(await completeMemory(input({ model: outputLimited, memoryContext: context(outputLimited, { modelRegistry: { complete } }) })))).toBe("invalid_input");
    expect(complete).not.toHaveBeenCalled();
  });

  it("rejects BGE-M3 spelling variants in generation-model configuration", async () => {
    for (const modelId of ["BAAI/bge-m3", "bge_m3", "BGE.M3", "bge m3"]) {
      await expect(loadConfig("pi", { env: {}, homeDir: "/home/tester", readTextFile: async () => JSON.stringify({ memoryModel: { modelId } }) })).rejects.toThrow();
    }
  });

  it("keeps the critical hardening paths structurally present", async () => {
    const source = await readFile("src/curation/llm.ts", "utf8");
    expect(source).toMatch(/const policyHash = processingPolicyHash\(policy\);[\s\S]*policyHash !== policy\.id/);
    expect(source).toMatch(/isPolicyExpired\(policy\)/);
    expect(source).toMatch(/context\.host !== "prime"/);
    expect(source).toMatch(/const outboundContext: Context/);
    expect(source).toMatch(/llmDestinationBinding/);
  });
});
