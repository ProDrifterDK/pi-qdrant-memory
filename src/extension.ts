import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import type {
  ExtensionContext,
  ExtensionFactory,
} from "@earendil-works/pi-coding-agent";
import { RecallCache } from "./cache.js";
import { EmbeddingsClient } from "./clients/embeddings.js";
import { MemoryClientError } from "./clients/http.js";
import { ReadonlyQdrantClient } from "./clients/qdrant-readonly.js";
import { loadConfig } from "./config.js";
import { detectHost, resolvePrimeRlmDepth } from "./host.js";
import { resolveProjectIdentity, type ProjectIdentity } from "./project.js";
import { MemoryRetriever, type MemorySearchResult } from "./retrieval/search.js";
import {
  MemoryService,
  type MemoryWarning,
  type MemoryWarningSink,
} from "./service.js";
import { createMemorySearchTool, type ExplicitSearchService } from "./tool.js";
import type { HostId, RuntimeConfig } from "./types.js";

const DEFAULT_TOP_K = 5;
const DEFAULT_TOOL_BUDGET = 8000;
const HARD_CONTEXT_BUDGET = 16000;
const CACHE_TTL_MS = 5 * 60 * 1000;
const CACHE_MAX_ENTRIES = 32;

type Environment = Record<string, string | undefined>;

export interface MemoryExtensionDependencies {
  env?: Environment;
  argv?: readonly string[];
  homeDir?: string;
  xdgConfigHome?: string;
  readTextFile?(path: string): Promise<string>;
  fetchImpl?: typeof fetch;
  projectResolver?(cwd: string): Promise<ProjectIdentity>;
  now?: () => number;
  warningSink?: MemoryWarningSink;
}

class DisabledMemoryService implements ExplicitSearchService {
  async search(): Promise<never> {
    throw new MemoryClientError("configuration", "Memory search is unavailable");
  }
}

function clientOptions<T extends object>(
  base: T,
  apiKey: string | undefined,
  fetchImpl: typeof fetch | undefined,
): T & { apiKey?: string; fetchImpl?: typeof fetch } {
  const result = { ...base } as T & { apiKey?: string; fetchImpl?: typeof fetch };
  if (apiKey !== undefined) result.apiKey = apiKey;
  if (fetchImpl !== undefined) result.fetchImpl = fetchImpl;
  return result;
}

function deliverWarning(warning: MemoryWarning, ctx: ExtensionContext): void {
  if (ctx.hasUI) {
    try {
      ctx.ui.notify(warning.message, "warning");
      return;
    } catch {
      // Fall through to the fixed, redacted stderr warning.
    }
  }
  console.warn(warning.message);
}

/** Host/config gate shared by both lifecycle hooks. */
export function serviceAutoRecallEnabled(
  ctx: ExtensionContext,
  host: HostId | undefined,
  config: RuntimeConfig | undefined,
  env: Environment = process.env,
): boolean {
  if (
    host === undefined ||
    config === undefined ||
    config.host !== host ||
    !config.enabled ||
    !config.autoRecall
  ) return false;
  if (host === "pi") return true;
  try {
    return resolvePrimeRlmDepth(ctx.sessionManager.getHeader(), env) === 0;
  } catch {
    return false;
  }
}

/** Build a testable factory while keeping the default export host-portable. */
export function createMemoryExtension(
  dependencies: MemoryExtensionDependencies = {},
): ExtensionFactory {
  return async (pi) => {
    const env = dependencies.env ?? process.env;
    const argv = dependencies.argv ?? process.argv;
    const warned = new Set<string>();
    const warnOnce = (warning: MemoryWarning, ctx: ExtensionContext): void => {
      if (warned.has(warning.category)) return;
      warned.add(warning.category);
      try {
        (dependencies.warningSink ?? deliverWarning)(warning, ctx);
      } catch {
        // Observability is optional and never blocks extension initialization.
      }
    };

    const detectionInput = { env, argv } as {
      explicit?: string;
      env: Environment;
      argv: readonly string[];
    };
    if (env.PI_QDRANT_MEMORY_HOST !== undefined) {
      detectionInput.explicit = env.PI_QDRANT_MEMORY_HOST;
    }
    const detection = detectHost(detectionInput);
    let host: HostId | undefined;
    let config: RuntimeConfig | undefined;
    let service: MemoryService | undefined;
    let disabledWarning: MemoryWarning | undefined;

    if (!detection.ok) {
      disabledWarning = {
        category: "host",
        message: "pi-qdrant-memory: recall unavailable (host).",
      };
    } else {
      host = detection.host;
      try {
        const configDependencies = {
          env,
          homeDir: dependencies.homeDir ?? homedir(),
          readTextFile: dependencies.readTextFile ?? ((path: string) => readFile(path, "utf8")),
        } as {
          env: Environment;
          homeDir: string;
          xdgConfigHome?: string;
          readTextFile(path: string): Promise<string>;
        };
        const xdgConfigHome = dependencies.xdgConfigHome ?? env.XDG_CONFIG_HOME;
        if (xdgConfigHome !== undefined) configDependencies.xdgConfigHome = xdgConfigHome;
        config = await loadConfig(host, configDependencies);

        if (config.enabled) {
          const embeddings = new EmbeddingsClient(clientOptions({
            baseUrl: config.embeddings.baseUrl,
            model: config.embeddings.model,
            dimension: config.embeddings.dimension,
            queryPrefix: config.embeddings.queryPrefix,
            timeoutMs: config.retrieval.timeoutMs,
          }, config.embeddings.apiKey, dependencies.fetchImpl));
          const qdrant = new ReadonlyQdrantClient(clientOptions({
            baseUrl: config.qdrant.url,
            collection: config.qdrant.collection,
            timeoutMs: config.retrieval.timeoutMs,
          }, config.qdrant.apiKey, dependencies.fetchImpl));
          const retriever = new MemoryRetriever({
            embeddings,
            qdrant,
            config: config.retrieval,
          });
          const cacheOptions = {
            maxEntries: CACHE_MAX_ENTRIES,
            ttlMs: CACHE_TTL_MS,
          } as { maxEntries: number; ttlMs: number; now?: () => number };
          if (dependencies.now !== undefined) cacheOptions.now = dependencies.now;
          service = new MemoryService({
            host,
            config,
            retriever,
            projectResolver: dependencies.projectResolver ?? resolveProjectIdentity,
            cache: new RecallCache<MemorySearchResult>(cacheOptions),
            warningSink: warnOnce,
            qdrant,
            embeddings,
          });
        }
      } catch {
        config = undefined;
        disabledWarning = {
          category: "configuration",
          message: "pi-qdrant-memory: recall unavailable (configuration).",
        };
      }
    }

    const searchService: ExplicitSearchService = service ?? new DisabledMemoryService();
    pi.registerTool(createMemorySearchTool({
      service: searchService,
      defaultLimit: config?.retrieval.topK ?? DEFAULT_TOP_K,
      toolResultBudgetChars: config?.retrieval.toolResultBudgetChars ?? DEFAULT_TOOL_BUDGET,
      hardContextCharBudget: config?.retrieval.hardContextCharBudget ?? HARD_CONTEXT_BUDGET,
    }));

    pi.on("before_agent_start", async (event, ctx) => {
      if (service === undefined || !serviceAutoRecallEnabled(ctx, host, config, env)) return;
      service.prefetch(event.prompt, ctx);
    });

    pi.on("context", async (event, ctx) => {
      if (service === undefined || !serviceAutoRecallEnabled(ctx, host, config, env)) return;
      return { messages: await service.inject(event.messages, ctx) };
    });

    pi.on("session_start", async (_event, ctx) => {
      service?.clear();
      if (disabledWarning !== undefined) warnOnce(disabledWarning, ctx);
      await service?.checkHealth(ctx);
    });

    pi.on("session_shutdown", async () => {
      service?.clear();
    });
  };
}

const extension: ExtensionFactory = createMemoryExtension();
export default extension;
