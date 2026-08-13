import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { RecallCache } from "./cache.js";
import { EmbeddingsClient, bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory } from "./clients/embeddings.js";
import { MemoryClientError } from "./clients/http.js";
import { destinationForEndpoint } from "./security/egress.js";
import { loadConfig } from "./config.js";
import { detectHost, resolvePrimeRlmDepth } from "./host.js";
import { resolveProjectIdentity } from "./project.js";
import { createGuardedMemoryReadStore, MemoryRetriever } from "./retrieval/search.js";
import { MemoryService, } from "./service.js";
import { createMemorySearchTool } from "./tool.js";
const DEFAULT_TOP_K = 5;
const DEFAULT_TOOL_BUDGET = 8000;
const HARD_CONTEXT_BUDGET = 16000;
const CACHE_TTL_MS = 5 * 60 * 1000;
const CACHE_MAX_ENTRIES = 32;
class DisabledMemoryService {
    async search() {
        throw new MemoryClientError("configuration", "Memory search is unavailable");
    }
}
function clientOptions(base, apiKey, fetchImpl) {
    const result = { ...base };
    if (apiKey !== undefined)
        result.apiKey = apiKey;
    if (fetchImpl !== undefined)
        result.fetchImpl = fetchImpl;
    return result;
}
function deliverWarning(warning, ctx) {
    if (ctx.hasUI) {
        try {
            ctx.ui.notify(warning.message, "warning");
            return;
        }
        catch {
            // Fall through to the fixed, redacted stderr warning.
        }
    }
    console.warn(warning.message);
}
/** Host/config gate shared by both lifecycle hooks. */
export function serviceAutoRecallEnabled(ctx, host, config, env = process.env) {
    if (host === undefined ||
        config === undefined ||
        config.host !== host ||
        !config.enabled ||
        !config.autoRecall)
        return false;
    try {
        return resolvePrimeRlmDepth(ctx.sessionManager.getHeader(), env) === 0;
    }
    catch {
        return false;
    }
}
function sameDestination(left, right) { return left.id === right.id && left.residency === right.residency && left.dataUse === right.dataUse; }
function activeModelDestination(ctx, config) {
    const model = ctx.model;
    if (model === undefined)
        return undefined;
    if (config.privacy.egressMode === "local_only") {
        const nodeId = config.outbox.nodeId;
        if (nodeId === undefined)
            return undefined;
        try {
            return destinationForEndpoint(model.baseUrl, nodeId, { residency: "local", dataUse: "memory" });
        }
        catch {
            return undefined;
        }
    }
    const ids = new Set([model.id, `${model.provider}/${model.id}`]);
    const matches = config.privacy.allowedLlmDestinations.filter((destination) => ids.has(destination.id));
    return matches.length === 1 ? Object.freeze({ ...matches[0] }) : undefined;
}
function configuredQdrantDestination(config) {
    if (config.privacy.egressMode === "local_only") {
        const nodeId = config.outbox.nodeId;
        if (nodeId === undefined)
            return undefined;
        try {
            return destinationForEndpoint(config.qdrant.url, nodeId, { residency: "local", dataUse: "memory" });
        }
        catch {
            return undefined;
        }
    }
    return config.privacy.allowedQdrantDestinations.length === 1 ? Object.freeze({ ...config.privacy.allowedQdrantDestinations[0] }) : undefined;
}
function configuredEmbeddingDestination(config) {
    if (config.privacy.egressMode === "local_only") {
        const nodeId = config.outbox.nodeId;
        if (nodeId === undefined)
            return undefined;
        try {
            return destinationForEndpoint(config.embeddings.baseUrl, nodeId, { residency: "local", dataUse: "memory" });
        }
        catch {
            return undefined;
        }
    }
    return config.privacy.allowedEmbeddingDestinations.length === 1 ? Object.freeze({ ...config.privacy.allowedEmbeddingDestinations[0] }) : undefined;
}
function contextIsChild(ctx, _host, env) {
    try {
        return resolvePrimeRlmDepth(ctx.sessionManager.getHeader(), env) > 0;
    }
    catch {
        return true;
    }
}
/** Build a testable factory while keeping the default export host-portable. */
export function createMemoryExtension(dependencies = {}) {
    return async (pi) => {
        const env = dependencies.env ?? process.env;
        const argv = dependencies.argv ?? process.argv;
        const warned = new Set();
        const warnOnce = (warning, ctx) => {
            if (warned.has(warning.category))
                return;
            warned.add(warning.category);
            try {
                (dependencies.warningSink ?? deliverWarning)(warning, ctx);
            }
            catch {
                // Observability is optional and never blocks extension initialization.
            }
        };
        const detectionInput = { env, argv };
        if (env.PI_QDRANT_MEMORY_HOST !== undefined) {
            detectionInput.explicit = env.PI_QDRANT_MEMORY_HOST;
        }
        const detection = detectHost(detectionInput);
        let host;
        let config;
        let service;
        let disabledWarning;
        if (!detection.ok) {
            disabledWarning = {
                category: "host",
                message: "pi-qdrant-memory: recall unavailable (host).",
            };
        }
        else {
            host = detection.host;
            try {
                const configDependencies = {
                    env,
                    homeDir: dependencies.homeDir ?? homedir(),
                    readTextFile: dependencies.readTextFile ?? ((path) => readFile(path, "utf8")),
                };
                const xdgConfigHome = dependencies.xdgConfigHome ?? env.XDG_CONFIG_HOME;
                if (xdgConfigHome !== undefined)
                    configDependencies.xdgConfigHome = xdgConfigHome;
                config = await loadConfig(host, configDependencies);
                if (config.enabled) {
                    const activeConfig = config;
                    const embeddings = new EmbeddingsClient(clientOptions({
                        baseUrl: config.embeddings.baseUrl,
                        model: config.embeddings.model,
                        dimension: config.embeddings.dimension,
                        queryPrefix: config.embeddings.queryPrefix,
                        timeoutMs: config.retrieval.timeoutMs,
                    }, config.embeddings.apiKey, dependencies.fetchImpl));
                    let validatedEmbeddings;
                    try {
                        validatedEmbeddings = bindEmbeddingDocumentClient({ endpoint: config.embeddings.baseUrl, client: embeddings });
                    }
                    catch {
                        validatedEmbeddings = undefined;
                    }
                    const qdrantDestination = configuredQdrantDestination(activeConfig);
                    if (qdrantDestination === undefined)
                        throw new MemoryClientError("configuration", "Qdrant destination binding is unavailable");
                    const qdrant = createGuardedMemoryReadStore(clientOptions({
                        baseUrl: config.qdrant.url,
                        collection: config.qdrant.collection,
                        ownerHost: host,
                        timeoutMs: config.retrieval.timeoutMs,
                        readConsistency: config.coordination.readConsistency,
                        maxClockSkewMs: config.coordination.maxClockSkewMs,
                        destination: qdrantDestination,
                        egressMode: config.privacy.egressMode,
                        ...(config.outbox.nodeId === undefined ? {} : { nodeId: config.outbox.nodeId }),
                    }, config.qdrant.apiKey, dependencies.fetchImpl));
                    const resolveEmbedding = async (control) => {
                        const destination = configuredEmbeddingDestination(activeConfig);
                        if (destination === undefined || validatedEmbeddings === undefined || control.ownerHost !== host || control.state !== "active" || control.revokedDestinationIds.includes(destination.id))
                            return undefined;
                        const factory = createEmbeddingDestinationFactory({ endpoint: activeConfig.embeddings.baseUrl, destination, client: validatedEmbeddings, egressMode: activeConfig.privacy.egressMode, ...(activeConfig.outbox.nodeId === undefined ? {} : { nodeId: activeConfig.outbox.nodeId }), coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch });
                        return Object.freeze({ embedding: bindEmbeddingDestination(factory, destination), destination });
                    };
                    const retriever = new MemoryRetriever({ reader: qdrant, config: config.retrieval, resolveEmbedding, queryPrefix: config.embeddings.queryPrefix, maxClockSkewMs: config.coordination.maxClockSkewMs, ...(dependencies.now === undefined ? {} : { now: dependencies.now }) });
                    const cacheOptions = {
                        maxEntries: CACHE_MAX_ENTRIES,
                        ttlMs: CACHE_TTL_MS,
                    };
                    if (dependencies.now !== undefined)
                        cacheOptions.now = dependencies.now;
                    service = new MemoryService({
                        host,
                        config,
                        retriever,
                        projectResolver: dependencies.projectResolver ?? resolveProjectIdentity,
                        cache: new RecallCache(cacheOptions),
                        warningSink: warnOnce,
                        modelDestination: (ctx) => (dependencies.modelDestinationResolver ?? activeModelDestination)(ctx, activeConfig),
                        isChild: (ctx) => (dependencies.isChildResolver ?? contextIsChild)(ctx, host, env),
                        qdrant,
                        embeddingHealth: async (signal) => {
                            const before = await qdrant.readControl();
                            const resolved = await resolveEmbedding(before);
                            if (resolved === undefined)
                                return;
                            await resolved.embedding.embed({ model: activeConfig.embeddings.model, text: "pi-qdrant-memory health probe", ...(signal === undefined ? {} : { signal }) });
                            const after = await qdrant.readControl();
                            if (before.contentHash !== after.contentHash)
                                throw new MemoryClientError("configuration", "Memory authority changed during health probe");
                        },
                    });
                }
            }
            catch {
                config = undefined;
                disabledWarning = {
                    category: "configuration",
                    message: "pi-qdrant-memory: recall unavailable (configuration).",
                };
            }
        }
        const searchService = service ?? new DisabledMemoryService();
        pi.registerTool(createMemorySearchTool({
            service: searchService,
            defaultLimit: config?.retrieval.topK ?? DEFAULT_TOP_K,
            toolResultBudgetChars: config?.retrieval.toolResultBudgetChars ?? DEFAULT_TOOL_BUDGET,
            hardContextCharBudget: config?.retrieval.hardContextCharBudget ?? HARD_CONTEXT_BUDGET,
        }));
        pi.on("before_agent_start", async (event, ctx) => {
            if (service === undefined || !serviceAutoRecallEnabled(ctx, host, config, env))
                return;
            service.prefetch(event.prompt, ctx);
        });
        pi.on("context", async (event, ctx) => {
            if (service === undefined || !serviceAutoRecallEnabled(ctx, host, config, env))
                return;
            return { messages: await service.inject(event.messages, ctx) };
        });
        pi.on("session_start", async (_event, ctx) => {
            service?.clear();
            if (disabledWarning !== undefined)
                warnOnce(disabledWarning, ctx);
            await service?.checkHealth(ctx);
        });
        pi.on("session_shutdown", async () => {
            service?.clear();
        });
    };
}
const extension = createMemoryExtension();
export default extension;
//# sourceMappingURL=extension.js.map