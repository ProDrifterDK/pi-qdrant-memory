import { createHash } from "node:crypto";
import { MemoryClientError } from "./clients/http.js";
import { formatMemoryContext, MEMORY_CONTEXT_CUSTOM_TYPE, } from "./format.js";
import { buildEffectiveQuery, priorUserPromptsFromBranch, userTextFromMessage, } from "./query.js";
const HEALTH_PROBE = "pi-qdrant-memory health probe";
const KNOWN_ERROR_CATEGORIES = new Set([
    "timeout",
    "cancelled",
    "network",
    "http",
    "invalid-json",
    "invalid-response",
    "configuration",
]);
function sha256(value) {
    return createHash("sha256").update(value, "utf8").digest("hex");
}
function nonSecretRevision(host, config) {
    // Fixed key order makes this canonical without admitting URL or credential
    // fields. Endpoint changes arrive through a freshly constructed service and
    // cache, while all retrieval semantics are revision-separated here.
    return sha256(JSON.stringify({
        host,
        collection: config.qdrant.collection,
        embeddings: {
            model: config.embeddings.model,
            dimension: config.embeddings.dimension,
            queryPrefix: config.embeddings.queryPrefix,
        },
        retrieval: {
            topK: config.retrieval.topK,
            candidatesPerLane: config.retrieval.candidatesPerLane,
            minScore: config.retrieval.minScore,
            projectBoost: config.retrieval.projectBoost,
            contextBudgetChars: config.retrieval.contextBudgetChars,
            toolResultBudgetChars: config.retrieval.toolResultBudgetChars,
            hardContextCharBudget: config.retrieval.hardContextCharBudget,
            timeoutMs: config.retrieval.timeoutMs,
        },
    }));
}
function categoryFor(error) {
    if (error instanceof MemoryClientError)
        return error.category;
    if (typeof error === "object" && error !== null) {
        const candidate = error.category;
        if (typeof candidate === "string" && KNOWN_ERROR_CATEGORIES.has(candidate)) {
            return candidate;
        }
    }
    return "internal";
}
function isPluginContext(message) {
    if (typeof message !== "object" || message === null)
        return false;
    const record = message;
    return record.role === "custom" && record.customType === MEMORY_CONTEXT_CUSTOM_TYPE;
}
function sanitizedFailure(category) {
    const clientCategory = KNOWN_ERROR_CATEGORIES.has(category)
        ? category
        : "configuration";
    return new MemoryClientError(clientCategory, "Memory recall is unavailable");
}
/** Coordinates scoped retrieval, promise reuse, fail-open injection, and health warnings. */
export class MemoryService {
    dependencies;
    configRevision;
    warned = new Set();
    constructor(dependencies) {
        this.dependencies = dependencies;
        if (dependencies.host !== dependencies.config.host ||
            !dependencies.config.enabled) {
            throw new MemoryClientError("configuration", "Memory service configuration is disabled or mismatched");
        }
        this.configRevision = nonSecretRevision(dependencies.host, dependencies.config);
    }
    async search(query, limit, ctx, signal) {
        try {
            const normalized = query.trim();
            if (normalized.length < 1 || normalized.length > 4000) {
                throw new MemoryClientError("configuration", "Memory query length is invalid");
            }
            const project = await this.dependencies.projectResolver(ctx.cwd);
            return await this.dependencies.retriever.search({
                query: normalized,
                host: this.dependencies.host,
                project,
                limit,
                ...(signal === undefined ? {} : { signal }),
            });
        }
        catch (error) {
            const category = categoryFor(error);
            this.warn(category, ctx);
            throw sanitizedFailure(category);
        }
    }
    prefetch(prompt, ctx) {
        try {
            const prior = priorUserPromptsFromBranch(ctx.sessionManager.getBranch());
            const effectiveQuery = buildEffectiveQuery(prompt, prior);
            if (effectiveQuery === undefined)
                return;
            void this.recall(effectiveQuery, ctx).catch((error) => {
                this.warn(categoryFor(error), ctx);
            });
        }
        catch (error) {
            this.warn(categoryFor(error), ctx);
        }
    }
    async inject(messages, ctx) {
        const copied = [...messages];
        const withoutPriorContext = copied.filter((message) => !isPluginContext(message));
        try {
            const prompts = [];
            for (const message of withoutPriorContext) {
                const prompt = userTextFromMessage(message);
                if (prompt !== undefined)
                    prompts.push(prompt);
            }
            const current = prompts.at(-1);
            if (current === undefined)
                return withoutPriorContext;
            const effectiveQuery = buildEffectiveQuery(current, prompts.slice(0, -1));
            if (effectiveQuery === undefined)
                return withoutPriorContext;
            const result = await this.recall(effectiveQuery, ctx);
            const block = formatMemoryContext(result.hits, this.dependencies.config.retrieval.contextBudgetChars);
            if (block.length === 0)
                return withoutPriorContext;
            const recalled = {
                role: "custom",
                customType: MEMORY_CONTEXT_CUSTOM_TYPE,
                content: block,
                display: false,
                details: { hitCount: result.hits.length },
                timestamp: Date.now(),
            };
            return [...withoutPriorContext, recalled];
        }
        catch (error) {
            this.warn(categoryFor(error), ctx);
            return withoutPriorContext;
        }
    }
    async checkHealth(ctx) {
        try {
            const { qdrant, embeddings } = this.dependencies;
            if (qdrant === undefined || embeddings === undefined) {
                throw new MemoryClientError("configuration", "Memory health dependencies are unavailable");
            }
            await qdrant.health(ctx.signal);
            const collection = await qdrant.collectionInfo(ctx.signal);
            if (collection.dimension !== this.dependencies.config.embeddings.dimension ||
                collection.distance.toLowerCase() !== "cosine") {
                throw new MemoryClientError("configuration", "Memory collection is incompatible");
            }
            await embeddings.embedQuery(HEALTH_PROBE, ctx.signal);
        }
        catch (error) {
            this.warn(categoryFor(error), ctx);
        }
    }
    clear() {
        this.dependencies.cache.clear();
    }
    async recall(effectiveQuery, ctx) {
        const project = await this.dependencies.projectResolver(ctx.cwd);
        const cacheKey = sha256(JSON.stringify([
            ctx.sessionManager.getSessionId(),
            project.id,
            effectiveQuery,
            this.configRevision,
        ]));
        return this.dependencies.cache.getOrCreate(cacheKey, () => this.dependencies.retriever.search({
            query: effectiveQuery,
            host: this.dependencies.host,
            project,
            limit: this.dependencies.config.retrieval.topK,
            ...(ctx.signal === undefined ? {} : { signal: ctx.signal }),
        }));
    }
    warn(category, ctx) {
        if (this.warned.has(category))
            return;
        this.warned.add(category);
        const warning = {
            category,
            message: `pi-qdrant-memory: recall unavailable (${category}).`,
        };
        try {
            this.dependencies.warningSink(warning, ctx);
        }
        catch {
            // Warning delivery is optional and must never affect an agent turn.
        }
    }
}
//# sourceMappingURL=service.js.map