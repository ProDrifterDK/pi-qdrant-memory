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
    pi: { enabled: true, autoRecall },
  });
}

function runtimeFetch(host: "prime" | "pi") {
  const calls: Array<{ url: string; body?: any }> = [];
  const fetchImpl = vi.fn(async (url: string, init?: RequestInit) => {
    const body = init?.body === undefined ? undefined : JSON.parse(String(init.body));
    calls.push({ url, body });
    if (url.endsWith("/healthz")) return new Response("ok");
    const collection = host === "pi" ? "pi_memory" : "prime_memory";
    if (url.endsWith(`/collections/${collection}`)) {
      return new Response(JSON.stringify({ result: { config: { params: { vectors: { size: 1024, distance: "Cosine" } } } } }));
    }
    if (url.endsWith("/embeddings")) {
      return new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.1) }] }));
    }
    if (url.endsWith("/points/search")) {
      const isProject = body?.filter?.must?.some((condition: any) => condition.key === "project_id");
      const result = isProject ? [{
        id: "memory-1",
        score: 0.9,
        payload: {
          text: "Portable recalled context",
          host,
          project_id: "project-id",
          project_label: "project",
          source_type: "conversation",
          source_system: "hermes",
          status: "active",
          secret_scan: "passed",
        },
      }] : [];
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
      projectResolver: async () => ({ id: "project-id", label: "project" }),
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
    const messages = [user("Explain portable root-agent auto recall behavior")];

    await invokeBefore(fake.handler("before_agent_start"), "Explain portable root-agent auto recall behavior", context.value);
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
    expect(runtime.calls.filter((call) => call.url.endsWith("/embeddings"))).toHaveLength(1);
    expect(runtime.calls.filter((call) => call.url.endsWith("/points/search"))).toHaveLength(2);
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
      { query: "Explicit recall remains available to child agents", limit: 3 },
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

  it("uses Pi auto-recall configuration without inspecting Prime depth", async () => {
    const runtime = factoryFor("pi");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: "not-a-prime-depth" } });
    const messages = [user("Pi should recall this sufficiently detailed prompt")];

    await invokeBefore(fake.handler("before_agent_start"), "Pi should recall this sufficiently detailed prompt", context.value);
    const result = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    expect(result.messages.some((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toBe(true);

    const disabledRuntime = factoryFor("pi", { autoRecall: false });
    const disabledFake = fakeApi();
    await disabledRuntime.factory(disabledFake.api);
    await invokeBefore(disabledFake.handler("before_agent_start"), "Pi config disables this recall prompt", context.value);
    expect(await disabledFake.handler("context")({ type: "context", messages }, context.value)).toBeUndefined();
  });

  it("deduplicates context retries and clears recall cache on session lifecycle", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Repeat this root prompt across provider tool calls")];

    const first = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    const retry = await fake.handler("context")({ type: "context", messages: first.messages }, context.value) as { messages: AgentMessages };
    expect(retry.messages.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toHaveLength(1);
    expect(runtime.calls.filter((call) => call.url.endsWith("/embeddings"))).toHaveLength(1);

    await fake.handler("session_start")({ type: "session_start", reason: "resume" }, context.value);
    const healthProbe = runtime.calls.find((call) => call.url.endsWith("/embeddings") && call.body?.input === "search_query: pi-qdrant-memory health probe");
    expect(healthProbe).toBeDefined();
    await fake.handler("context")({ type: "context", messages }, context.value);
    const recallEmbeddings = runtime.calls.filter((call) => call.url.endsWith("/embeddings") && call.body?.input !== "search_query: pi-qdrant-memory health probe");
    expect(recallEmbeddings).toHaveLength(2);

    await fake.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    await fake.handler("context")({ type: "context", messages }, context.value);
    expect(runtime.calls.filter((call) => call.url.endsWith("/embeddings") && call.body?.input !== "search_query: pi-qdrant-memory health probe")).toHaveLength(3);
  });
});
