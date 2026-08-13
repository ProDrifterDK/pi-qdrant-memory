import type {
  ContextEvent,
  ExtensionAPI,
  ExtensionContext,
  ExtensionFactory,
  ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import extension, { createMemoryExtension } from "../../src/extension.js";
import { MEMORY_CONTEXT_CUSTOM_TYPE } from "../../src/format.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type ProcessingPolicyRecord } from "../../src/domain/records.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { COLLECTION_CONTROL_ID } from "../../src/qdrant/schema.js";
import { physicalPointIdFor } from "../../src/qdrant/client.js";
import { recordPayload } from "../../src/qdrant/write.js";
import { destinationForEndpoint } from "../../src/security/egress.js";

type Handler = (event: any, ctx: ExtensionContext) => unknown;
type AgentMessages = ContextEvent["messages"];

function fakeApi() {
  const tools: ToolDefinition[] = [];
  const handlers = new Map<string, Handler[]>();
  const api = {
    registerTool(tool: ToolDefinition) { tools.push(tool); },
    on(event: string, handler: Handler) {
      const registered = handlers.get(event) ?? [];
      registered.push(handler);
      handlers.set(event, registered);
    },
  } as unknown as ExtensionAPI;
  const handler = (event: string): Handler => {
    const found = handlers.get(event)?.[0];
    if (found === undefined) throw new Error(`Missing ${event} handler`);
    return found;
  };
  return { api, tools, handlers, handler };
}

function ctx(input: {
  sessionId?: string;
  header?: unknown;
  branch?: unknown[];
} = {}) {
  const branch = input.branch ?? [];
  const notifications: Array<{ message: string; type?: string }> = [];
  const value = {
    cwd: "/workspace/project",
    hasUI: true,
    signal: undefined,
    ui: {
      notify(message: string, type?: string) { notifications.push({ message, type }); },
    },
    sessionManager: {
      getSessionId: () => input.sessionId ?? "session-1",
      getBranch: () => branch,
      getHeader: () => input.header ?? null,
    },
    model: { id: "model", provider: "provider", baseUrl: "http://127.0.0.1:9999/v1" },
  } as unknown as ExtensionContext;
  return { value, branch, notifications };
}

function user(text: string, timestamp = 1): AgentMessages[number] {
  return { role: "user", content: text, timestamp } as AgentMessages[number];
}

function hostConfig(autoRecall = true) {
  return JSON.stringify({
    embeddings: { dimension: 1024 },
    retrieval: {
      topK: 5,
      candidatesPerLane: 2,
      contextBudgetChars: 1200,
      toolResultBudgetChars: 8000,
      timeoutMs: 2500,
    },
    prime: { enabled: true, autoRecall },
    outbox: { nodeId: "node-test" },
    coordination: { readConsistency: 1, maxClockSkewMs: 300000 },
    pi: { enabled: true, autoRecall },
  });
}

function runtimeFetch(host: "prime" | "pi") {
  const calls: Array<{ url: string; body?: any }> = [];
  const collection = host === "pi" ? "pi_memory" : "prime_memory";
  const qdrantDestination = destinationForEndpoint("http://127.0.0.1:6333", "node-test", { residency: "local", dataUse: "memory" });
  const embeddingDestination = destinationForEndpoint("http://127.0.0.1:8080/v1", "node-test", { residency: "local", dataUse: "memory" });
  const llmDestination = destinationForEndpoint("http://127.0.0.1:9999/v1", "node-test", { residency: "local", dataUse: "memory" });
  const policyBase: ProcessingPolicy = { id: "pending", ownerHost: host, destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, llm: llmDestination.id }, originProvider: "provider", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "processing-policy-v1" };
  const policy: ProcessingPolicy = { ...policyBase, id: processingPolicyHash(policyBase) };
  const canonical = <T extends { contentHash: string }>(value: T): T => ({ ...value, contentHash: canonicalRecordHash(value as never) });
  const policyRecord = canonical<ProcessingPolicyRecord>({ recordType: "processing_policy", id: policy.id, ownerHost: host, schemaRevision: 1, createdAt: "2026-08-13T15:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, policy, canonicalHash: policy.id, contentHash: "pending" });
  const control = canonical<ControlRecord>({ recordType: "collection_control", id: COLLECTION_CONTROL_ID, ownerHost: host, schemaRevision: 1, createdAt: "2026-08-13T15:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 1, coordinationPolicyHash: "coord-hash", state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" });
  const episode = canonical<EpisodeRecord>({ recordType: "episode", id: "11111111-1111-5111-8111-111111111111", ownerHost: host, schemaRevision: 1, createdAt: "2026-08-13T15:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, sourceEntryId: "entry-1", host, projectId: "project-id", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2026-08-13T15:00:00.000Z", modelId: "model", embeddingDimension: 1024, originProvider: "provider", destinationId: llmDestination.id, status: "active", redactionStatus: "unchanged", secretScan: "passed", text: "Portable recalled context for root agents", vector: Array.from({ length: 1024 }, () => 0.1), contentHash: "pending" });
  const points = new Map([
    [COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: recordPayload(control) }],
    [physicalPointIdFor("processing_policy", policy.id), { id: physicalPointIdFor("processing_policy", policy.id), payload: recordPayload(policyRecord) }],
    [episode.id, { id: episode.id, payload: recordPayload(episode), vector: { semantic: episode.vector! } }],
  ]);
  const fetchImpl = vi.fn(async (rawUrl: string, init?: RequestInit) => {
    const parsedUrl = new URL(rawUrl); const body = init?.body === undefined ? undefined : JSON.parse(String(init.body)); calls.push({ url: rawUrl, body });
    if (parsedUrl.pathname === "/healthz") return new Response("ok");
    if (parsedUrl.pathname === `/collections/${collection}`) return new Response(JSON.stringify({ result: { config: { params: { vectors: { semantic: { size: 1024, distance: "Cosine" } } } } } }));
    if (parsedUrl.pathname.endsWith("/embeddings")) return new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.1) }] }));
    if (parsedUrl.pathname.endsWith("/points/scroll")) return new Response(JSON.stringify({ result: { points: [{ id: episode.id, payload: recordPayload(episode), vector: { semantic: episode.vector! } }], next_page_offset: null } }));
    if (parsedUrl.pathname.endsWith("/points/search")) {
      const wantsEpisode = body?.filter?.must?.some((condition: any) => condition.key === "record_type" && (condition.match?.value === "episode" || condition.match?.any?.includes("episode")));
      return new Response(JSON.stringify({ result: wantsEpisode ? [{ id: episode.id, score: 0.9, payload: recordPayload(episode), vector: { semantic: episode.vector! } }] : [] }));
    }
    if (parsedUrl.pathname.endsWith("/points/retrieve")) {
      const result = (body?.ids ?? []).map((id: string) => points.get(id)).filter(Boolean);
      return new Response(JSON.stringify({ result }));
    }
    return new Response("not found", { status: 404 });
  });
  return { fetchImpl: fetchImpl as typeof fetch, calls };
}

function factoryFor(host: "prime" | "pi", input: { autoRecall?: boolean; fetchImpl?: typeof fetch } = {}) {
  const runtime = runtimeFetch(host);
  return {
    factory: createMemoryExtension({
      env: { PI_QDRANT_MEMORY_HOST: host },
      argv: [],
      homeDir: "/home/test",
      readTextFile: async () => hostConfig(input.autoRecall ?? true),
      fetchImpl: input.fetchImpl ?? runtime.fetchImpl,
      projectResolver: async () => ({ id: "project-id", label: "project", identityKind: "registered" }),
    }),
    calls: runtime.calls,
    fetchImpl: runtime.fetchImpl,
  };
}

async function invokeBefore(handler: Handler, prompt: string, context: ExtensionContext) {
  return handler({ type: "before_agent_start", prompt, systemPrompt: "", systemPromptOptions: {} }, context);
}

describe("portable memory extension", () => {
  it("exports an async-compatible default ExtensionFactory", () => {
    expect(extension).toBeTypeOf("function");
    expect(extension satisfies ExtensionFactory).toBe(extension);
  });

  it("registers the unavailable tool and all lifecycle hooks when host resolution is unknown", async () => {
    const fetchImpl = vi.fn<typeof fetch>();
    const factory = createMemoryExtension({
      env: {},
      argv: [],
      homeDir: "/home/test",
      readTextFile: vi.fn(async () => hostConfig()),
      fetchImpl,
    });
    const fake = fakeApi();
    await factory(fake.api);

    expect(fake.tools.map((tool) => tool.name)).toEqual(["memory_search"]);
    expect([...fake.handlers.keys()]).toEqual(expect.arrayContaining([
      "before_agent_start",
      "context",
      "session_start",
      "session_shutdown",
    ]));

    const context = ctx();
    await invokeBefore(fake.handler("before_agent_start"), "A natural language recall request", context.value);
    const result = await fake.handler("context")({ type: "context", messages: [user("A natural language recall request")] }, context.value);
    expect(result).toBeUndefined();
    const toolResult = await fake.tools[0]!.execute(
      "call-1",
      { query: "A natural language recall request" },
      undefined,
      undefined,
      context.value,
    );
    expect(toolResult.content).toEqual([{ type: "text", text: "Memory search is temporarily unavailable." }]);
    expect(fetchImpl).not.toHaveBeenCalled();

    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("session_start")({ type: "session_start", reason: "reload" }, context.value);
    expect(context.notifications).toEqual([{
      message: "pi-qdrant-memory: recall unavailable (host).",
      type: "warning",
    }]);
  });

  it("prefetches once for Prime roots and injects one copied ephemeral custom message", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Portable recalled context for root agents")];

    await invokeBefore(fake.handler("before_agent_start"), "Portable recalled context for root agents", context.value);
    const result = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    const recalled = result.messages.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE);

    expect(fake.tools.map((tool) => tool.name)).toEqual(["memory_search"]);
    expect(recalled).toHaveLength(1);
    expect(recalled[0]).toMatchObject({
      role: "custom",
      customType: MEMORY_CONTEXT_CUSTOM_TYPE,
      display: false,
      details: { hitCount: 1 },
    });
    expect(result.messages).not.toBe(messages);
    expect(context.value.sessionManager.getBranch()).not.toContainEqual(expect.objectContaining({ customType: MEMORY_CONTEXT_CUSTOM_TYPE }));
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(1);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/embeddings"))).toHaveLength(0);
  });

  it("keeps the explicit tool but disables auto recall for Prime children and invalid depth", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const child = ctx({ header: { rlmDepth: 2 } });

    await invokeBefore(fake.handler("before_agent_start"), "A child prompt that must not auto recall", child.value);
    expect(await fake.handler("context")({ type: "context", messages: [user("A child prompt that must not auto recall")] }, child.value)).toBeUndefined();
    expect(runtime.calls).toEqual([]);

    const toolResult = await fake.tools[0]!.execute(
      "call-child",
      { query: "Portable recalled context for root agents", limit: 3 },
      undefined,
      undefined,
      child.value,
    );
    expect(toolResult.content[0]).toMatchObject({ type: "text" });
    expect((toolResult.content[0] as { text: string }).text).toContain("<memory-context");

    const invalid = ctx({ header: { rlmDepth: "invalid" } });
    const count = runtime.calls.length;
    await invokeBefore(fake.handler("before_agent_start"), "Invalid depth must fail closed", invalid.value);
    expect(await fake.handler("context")({ type: "context", messages: [user("Invalid depth must fail closed")] }, invalid.value)).toBeUndefined();
    expect(runtime.calls).toHaveLength(count);
  });

  it("enables Pi auto-recall only for a verified root depth", async () => {
    const runtime = factoryFor("pi");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Portable recalled context for root agents")];

    await invokeBefore(fake.handler("before_agent_start"), "Portable recalled context for root agents", context.value);
    const result = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    expect(result.messages.some((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toBe(true);

    const disabledRuntime = factoryFor("pi", { autoRecall: false });
    const disabledFake = fakeApi();
    await disabledRuntime.factory(disabledFake.api);
    await invokeBefore(disabledFake.handler("before_agent_start"), "Pi config disables this recall prompt", context.value);
    expect(await disabledFake.handler("context")({ type: "context", messages }, context.value)).toBeUndefined();
  });

  it("revalidates settled context retries and clears in-flight recall state on lifecycle", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Portable recalled context for root agents")];

    const first = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    const retry = await fake.handler("context")({ type: "context", messages: first.messages }, context.value) as { messages: AgentMessages };
    expect(retry.messages.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toHaveLength(1);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(2);

    await fake.handler("session_start")({ type: "session_start", reason: "resume" }, context.value);
    expect(runtime.calls.some((call) => new URL(call.url).pathname === "/healthz")).toBe(true);
    await fake.handler("context")({ type: "context", messages }, context.value);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(3);

    await fake.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    await fake.handler("context")({ type: "context", messages }, context.value);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(4);
  });
});
