import { join } from "node:path";
const PREFIX = "PI_QDRANT_MEMORY_";
const SOURCE_SCHEMA = "hermes-qdrant-memory-v0.9-compatible";
const FORBIDDEN_FILE_KEYS = new Set([
    "apikey",
    "authorization",
    "token",
    "password",
    "secret",
]);
const DEFAULTS = {
    enabled: true,
    autoRecall: true,
    qdrant: {
        url: "http://127.0.0.1:6333",
        collection: "pi_memory",
    },
    embeddings: {
        baseUrl: "http://127.0.0.1:8080/v1",
        model: "bge-m3",
        dimension: 1024,
        queryPrefix: "search_query: ",
    },
    retrieval: {
        topK: 5,
        candidatesPerLane: 20,
        minScore: 0.35,
        projectBoost: 0.05,
        contextBudgetChars: 1200,
        toolResultBudgetChars: 8000,
        hardContextCharBudget: 16000,
        timeoutMs: 2500,
    },
    admin: {
        source: {
            url: "http://127.0.0.1:6333",
            collection: "hermes_memory",
            schema: SOURCE_SCHEMA,
        },
    },
};
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function mergeRecords(...records) {
    const result = {};
    for (const record of records) {
        for (const [key, value] of Object.entries(record)) {
            const current = result[key];
            if (isRecord(current) && isRecord(value)) {
                result[key] = mergeRecords(current, value);
            }
            else {
                result[key] = value;
            }
        }
    }
    return result;
}
function section(parent, key, label) {
    const value = parent[key];
    if (value === undefined)
        return {};
    if (!isRecord(value))
        throw new Error(`${label} must be an object`);
    return value;
}
function inspectFileSecrets(value) {
    if (Array.isArray(value)) {
        for (const item of value)
            inspectFileSecrets(item);
        return;
    }
    if (!isRecord(value))
        return;
    for (const [key, child] of Object.entries(value)) {
        const normalized = key.toLowerCase();
        const compact = normalized.replace(/[^a-z0-9]/g, "");
        if (FORBIDDEN_FILE_KEYS.has(normalized) ||
            FORBIDDEN_FILE_KEYS.has(compact) ||
            compact.includes("apikey") ||
            compact.includes("authorization") ||
            compact.includes("password") ||
            compact.includes("token") ||
            compact.includes("secret")) {
            throw new Error("API keys are allowed only through environment variables");
        }
        inspectFileSecrets(child);
    }
}
function boundedNumber(name, raw, min, max) {
    const value = typeof raw === "number"
        ? raw
        : typeof raw === "string" && raw.trim() !== ""
            ? Number(raw)
            : Number.NaN;
    if (!Number.isFinite(value) || value < min || value > max) {
        throw new Error(`${name} must be a finite number between ${min} and ${max}`);
    }
    return value;
}
function boundedInteger(name, raw, min, max) {
    const value = boundedNumber(name, raw, min, max);
    if (!Number.isInteger(value))
        throw new Error(`${name} must be an integer`);
    return value;
}
function stringValue(name, raw, fallback) {
    if (raw === undefined)
        return fallback;
    if (typeof raw !== "string" || raw.length === 0) {
        throw new Error(`${name} must be a non-empty string`);
    }
    return raw;
}
function booleanValue(name, raw, fallback) {
    if (raw === undefined)
        return fallback;
    if (typeof raw !== "boolean")
        throw new Error(`${name} must be a boolean`);
    return raw;
}
function environmentBoolean(name, raw, fallback) {
    if (raw === undefined)
        return fallback;
    if (raw === "true")
        return true;
    if (raw === "false")
        return false;
    throw new Error(`${name} must be true or false`);
}
function environmentRaw(env, name, fallback) {
    return env[`${PREFIX}${name}`] ?? fallback;
}
function environmentNumber(env, name, fallback, min, max, integer = false) {
    const raw = environmentRaw(env, name, fallback);
    return integer
        ? boundedInteger(`${PREFIX}${name}`, raw, min, max)
        : boundedNumber(`${PREFIX}${name}`, raw, min, max);
}
function environmentSecret(env, name) {
    const raw = env[`${PREFIX}${name}`];
    if (raw === undefined || raw.trim() === "")
        return undefined;
    return raw;
}
function normalizeUrl(name, raw, fallback) {
    const value = stringValue(name, raw, fallback);
    let parsed;
    try {
        parsed = new URL(value);
    }
    catch {
        throw new Error(`${name} must be a valid URL`);
    }
    if (parsed.username !== "" || parsed.password !== "") {
        throw new Error(`${name} must not include a username or password`);
    }
    return value.replace(/\/+$/, "");
}
function absentFile(error) {
    return isRecord(error) && error.code === "ENOENT";
}
export function configPath(deps) {
    const configHome = deps.xdgConfigHome !== undefined && deps.xdgConfigHome !== ""
        ? deps.xdgConfigHome
        : join(deps.homeDir, ".config");
    return join(configHome, "pi-qdrant-memory", "config.json");
}
export async function loadConfig(host, deps) {
    const path = configPath(deps);
    let file = {};
    try {
        const raw = await deps.readTextFile(path);
        const parsed = JSON.parse(raw);
        if (!isRecord(parsed))
            throw new Error("Configuration root must be a JSON object");
        inspectFileSecrets(parsed);
        file = parsed;
    }
    catch (error) {
        if (!absentFile(error))
            throw error;
    }
    const hostOverrides = section(file, host, `${host} configuration`);
    const sharedQdrant = section(file, "qdrant", "qdrant configuration");
    const hostQdrant = section(hostOverrides, "qdrant", `${host}.qdrant configuration`);
    const qdrant = mergeRecords(DEFAULTS.qdrant, sharedQdrant, hostQdrant);
    const sharedEmbeddings = section(file, "embeddings", "embeddings configuration");
    const hostEmbeddings = section(hostOverrides, "embeddings", `${host}.embeddings configuration`);
    const embeddings = mergeRecords(DEFAULTS.embeddings, sharedEmbeddings, hostEmbeddings);
    const sharedRetrieval = section(file, "retrieval", "retrieval configuration");
    const hostRetrieval = section(hostOverrides, "retrieval", `${host}.retrieval configuration`);
    const retrieval = mergeRecords(DEFAULTS.retrieval, sharedRetrieval, hostRetrieval);
    const sharedAdmin = section(file, "admin", "admin configuration");
    const hostAdmin = section(hostOverrides, "admin", `${host}.admin configuration`);
    const sharedSource = mergeRecords(section(sharedAdmin, "source", "admin.source configuration"), section(sharedAdmin, "hermesSource", "admin.hermesSource configuration"));
    const hostSource = mergeRecords(section(hostAdmin, "source", `${host}.admin.source configuration`), section(hostAdmin, "hermesSource", `${host}.admin.hermesSource configuration`));
    const source = mergeRecords(DEFAULTS.admin.source, sharedSource, hostSource);
    const enabled = booleanValue(`${host}.enabled`, hostOverrides.enabled, DEFAULTS.enabled);
    const autoRecallEnv = deps.env[`${PREFIX}AUTO_RECALL`];
    const autoRecall = autoRecallEnv !== undefined
        ? environmentBoolean(`${PREFIX}AUTO_RECALL`, autoRecallEnv, DEFAULTS.autoRecall)
        : booleanValue(`${host}.autoRecall`, hostOverrides.autoRecall, DEFAULTS.autoRecall);
    const qdrantUrl = normalizeUrl(`${PREFIX}QDRANT_URL`, environmentRaw(deps.env, "QDRANT_URL", qdrant.url), DEFAULTS.qdrant.url);
    const embeddingBaseUrl = normalizeUrl(`${PREFIX}EMBEDDING_BASE_URL`, environmentRaw(deps.env, "EMBEDDING_BASE_URL", embeddings.baseUrl), DEFAULTS.embeddings.baseUrl);
    const sourceUrl = normalizeUrl(`${PREFIX}SOURCE_QDRANT_URL`, environmentRaw(deps.env, "SOURCE_QDRANT_URL", source.url), DEFAULTS.admin.source.url);
    const sourceSchema = source.schema;
    if (sourceSchema !== SOURCE_SCHEMA) {
        throw new Error(`admin.source.schema must be ${SOURCE_SCHEMA}`);
    }
    const qdrantApiKey = environmentSecret(deps.env, "QDRANT_API_KEY");
    const embeddingApiKey = environmentSecret(deps.env, "EMBEDDING_API_KEY");
    const destinationApiKey = environmentSecret(deps.env, "ADMIN_QDRANT_API_KEY");
    const sourceApiKey = environmentSecret(deps.env, "SOURCE_QDRANT_API_KEY");
    const result = {
        host,
        enabled,
        autoRecall,
        configPath: path,
        qdrant: {
            url: qdrantUrl,
            collection: stringValue(`${PREFIX}QDRANT_COLLECTION`, environmentRaw(deps.env, "QDRANT_COLLECTION", qdrant.collection), DEFAULTS.qdrant.collection),
        },
        embeddings: {
            baseUrl: embeddingBaseUrl,
            model: stringValue(`${PREFIX}EMBEDDING_MODEL`, environmentRaw(deps.env, "EMBEDDING_MODEL", embeddings.model), DEFAULTS.embeddings.model),
            dimension: environmentNumber(deps.env, "EMBEDDING_DIMENSION", embeddings.dimension, 1, 65536, true),
            queryPrefix: stringValue("embeddings.queryPrefix", embeddings.queryPrefix, DEFAULTS.embeddings.queryPrefix),
        },
        retrieval: {
            topK: environmentNumber(deps.env, "TOP_K", retrieval.topK, 1, 10, true),
            candidatesPerLane: environmentNumber(deps.env, "CANDIDATES_PER_LANE", retrieval.candidatesPerLane, 1, 100, true),
            minScore: environmentNumber(deps.env, "MIN_SCORE", retrieval.minScore, -1, 1),
            projectBoost: environmentNumber(deps.env, "PROJECT_BOOST", retrieval.projectBoost, 0, 0.25),
            contextBudgetChars: environmentNumber(deps.env, "CONTEXT_BUDGET_CHARS", retrieval.contextBudgetChars, 1, 16000, true),
            toolResultBudgetChars: environmentNumber(deps.env, "TOOL_RESULT_BUDGET_CHARS", retrieval.toolResultBudgetChars, 1, 16000, true),
            hardContextCharBudget: 16000,
            timeoutMs: environmentNumber(deps.env, "TIMEOUT_MS", retrieval.timeoutMs, 100, 30000, true),
        },
        admin: {
            source: {
                url: sourceUrl,
                collection: stringValue(`${PREFIX}SOURCE_QDRANT_COLLECTION`, environmentRaw(deps.env, "SOURCE_QDRANT_COLLECTION", source.collection), DEFAULTS.admin.source.collection),
                schema: SOURCE_SCHEMA,
            },
        },
    };
    if (qdrantApiKey !== undefined)
        result.qdrant.apiKey = qdrantApiKey;
    if (embeddingApiKey !== undefined)
        result.embeddings.apiKey = embeddingApiKey;
    if (destinationApiKey !== undefined)
        result.admin.destinationApiKey = destinationApiKey;
    if (sourceApiKey !== undefined)
        result.admin.source.apiKey = sourceApiKey;
    return result;
}
//# sourceMappingURL=config.js.map