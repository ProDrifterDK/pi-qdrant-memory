import { join } from "node:path";
const PREFIX = "PI_QDRANT_MEMORY_";
const RETIRED_ENV_PREFIX = ["SOURCE", "_QDRANT_"].join("");
const RETIRED_ADMIN_FIELD = ["admin", ".", "source"].join("");
const RETIRED_ADMIN_ALIAS = ["admin", ".", "hermes", "Source"].join("");
const RETIRED_SOURCE_FIELD = ["source"].join("");
const RETIRED_SOURCE_ALIAS = ["hermes", "Source"].join("");
const FILE_SECRET_MARKERS = new Set(["apikey", "authorization", "credential", "credentials", "password", "secret", "token", "authtoken", "accesstoken", "refreshtoken"]);
const OPERATIONAL_SUFFIXES = new Set([
    "QDRANT_URL", "QDRANT_COLLECTION", "EMBEDDING_BASE_URL", "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSION", "AUTO_RECALL", "TOP_K", "CANDIDATES_PER_LANE",
    "MIN_SCORE", "PROJECT_BOOST", "CONTEXT_BUDGET_CHARS", "TOOL_RESULT_BUDGET_CHARS", "TIMEOUT_MS",
]);
const SECRET_SUFFIXES = new Set(["QDRANT_API_KEY", "EMBEDDING_API_KEY", "ADMIN_QDRANT_API_KEY"]);
function defaultConfig(host) {
    return {
        enabled: true,
        autoRecall: true,
        qdrant: {
            url: "http://127.0.0.1:6333",
            collection: host === "pi" ? "pi_memory" : "prime_memory",
            replicationFactor: 1,
            writeConsistencyFactor: 1,
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
            rootScope: "project",
            childSearch: true,
        },
        projects: { registrations: {} },
        capture: {
            enabled: false,
            projectAllowlist: [],
            projectDenylist: [],
            episodeRetentionDays: "indefinite",
            toolArgsChars: 2000,
            toolResultChars: 4000,
        },
        privacy: {
            egressMode: "local_only",
            allowedQdrantDestinations: [],
            allowedEmbeddingDestinations: [],
            allowedLlmDestinations: [],
            allowActiveModelFallback: false,
            allowCrossProviderReplay: false,
        },
        coordination: {
            maxClockSkewMs: 300000,
            readConsistency: 1,
            leaseMs: 30000,
            reconcileIntervalMs: 900000,
        },
        outbox: {
            maxJobs: 10000,
            maxBytes: 268435456,
            retryBaseMs: 500,
            retryMaxMs: 30000,
            sharedFilesystem: false,
        },
        curation: { turnTrigger: 10, toolTrigger: 15, maxInputTokens: 12000 },
        memoryModel: { timeoutMs: 30000, maxOutputTokens: 2048 },
        raptor: {
            rebuildEpisodeDelta: 64,
            maxLevels: 5,
            summaryInputTokens: 12000,
            umapDimensions: 10,
            localNeighbors: 10,
            gmmMaxClusters: 50,
            membershipThreshold: 0.1,
        },
    };
}
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function mergeRecords(...records) {
    const result = Object.create(null);
    for (const record of records) {
        for (const [key, value] of Object.entries(record)) {
            const current = result[key];
            result[key] = isRecord(current) && isRecord(value)
                ? mergeRecords(current, value)
                : value;
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
const ROOT_KEYS = new Set(["enabled", "autoRecall", "qdrant", "embeddings", "retrieval", "projects", "capture", "privacy", "coordination", "outbox", "curation", "memoryModel", "raptor", "prime", "pi"]);
const HOST_KEYS = new Set(["enabled", "autoRecall", "qdrant", "embeddings", "retrieval", "projects", "capture", "privacy", "coordination", "outbox", "curation", "memoryModel", "raptor"]);
const DANGEROUS_KEYS = new Set(["__proto__", "prototype", "constructor"]);
const SECTION_KEYS = {
    qdrant: new Set(["url", "collection", "replicationFactor", "writeConsistencyFactor"]),
    embeddings: new Set(["baseUrl", "model", "dimension", "queryPrefix"]),
    retrieval: new Set(["topK", "candidatesPerLane", "minScore", "projectBoost", "contextBudgetChars", "toolResultBudgetChars", "hardContextCharBudget", "timeoutMs", "rootScope", "childSearch"]),
    projects: new Set(["registrations"]),
    capture: new Set(["enabled", "projectAllowlist", "projectDenylist", "episodeRetentionDays", "toolArgsChars", "toolResultChars"]),
    privacy: new Set(["egressMode", "allowedQdrantDestinations", "allowedEmbeddingDestinations", "allowedLlmDestinations", "allowActiveModelFallback", "allowCrossProviderReplay"]),
    coordination: new Set(["maxClockSkewMs", "readConsistency", "leaseMs", "reconcileIntervalMs"]),
    outbox: new Set(["maxJobs", "maxBytes", "retryBaseMs", "retryMaxMs", "nodeId", "sharedFilesystem"]),
    curation: new Set(["turnTrigger", "toolTrigger", "maxInputTokens"]),
    memoryModel: new Set(["modelId", "timeoutMs", "maxOutputTokens"]),
    raptor: new Set(["rebuildEpisodeDelta", "maxLevels", "summaryInputTokens", "umapDimensions", "localNeighbors", "gmmMaxClusters", "membershipThreshold", "seed"]),
};
function assertKeys(value, allowed, path) {
    if (!isRecord(value))
        throw new Error(`${path} must be an object`);
    for (const key of Object.keys(value)) {
        if (DANGEROUS_KEYS.has(key))
            throw new Error(`Unsafe configuration field ${path}.${key}`);
        if (!allowed.has(key))
            throw new Error(`Unknown configuration field ${path}.${key}`);
    }
}
function validateDestinationEntries(value, path) {
    if (value === undefined)
        return;
    if (!Array.isArray(value))
        throw new Error(`${path} must be an array`);
    value.forEach((entry, index) => assertKeys(entry, new Set(["id", "residency", "dataUse"]), `${path}[${index}]`));
}
function validateFileShape(root) {
    assertKeys(root, ROOT_KEYS, "configuration");
    const validateHost = (value, path) => {
        if (value === undefined)
            return;
        assertKeys(value, HOST_KEYS, path);
        for (const sectionName of Object.keys(SECTION_KEYS)) {
            const sectionValue = value[sectionName];
            if (sectionValue === undefined)
                continue;
            assertKeys(sectionValue, SECTION_KEYS[sectionName], `${path}.${sectionName}`);
            if (sectionName === "projects" && isRecord(sectionValue.registrations)) {
                for (const [key, registration] of Object.entries(sectionValue.registrations)) {
                    if (DANGEROUS_KEYS.has(key))
                        throw new Error(`Unsafe configuration field ${path}.${sectionName}.registrations.${key}`);
                    assertKeys(registration, new Set(["canonicalPath", "fingerprint", "alias"]), `${path}.${sectionName}.registrations.${key}`);
                }
            }
            if (sectionName === "privacy") {
                validateDestinationEntries(sectionValue.allowedQdrantDestinations, `${path}.${sectionName}.allowedQdrantDestinations`);
                validateDestinationEntries(sectionValue.allowedEmbeddingDestinations, `${path}.${sectionName}.allowedEmbeddingDestinations`);
                validateDestinationEntries(sectionValue.allowedLlmDestinations, `${path}.${sectionName}.allowedLlmDestinations`);
            }
        }
    };
    for (const sectionName of Object.keys(SECTION_KEYS)) {
        const sectionValue = root[sectionName];
        if (sectionValue === undefined)
            continue;
        assertKeys(sectionValue, SECTION_KEYS[sectionName], `configuration.${sectionName}`);
        if (sectionName === "projects" && isRecord(sectionValue.registrations)) {
            for (const [key, registration] of Object.entries(sectionValue.registrations)) {
                if (DANGEROUS_KEYS.has(key))
                    throw new Error(`Unsafe configuration field configuration.projects.registrations.${key}`);
                assertKeys(registration, new Set(["canonicalPath", "fingerprint", "alias"]), `configuration.projects.registrations.${key}`);
            }
        }
        if (sectionName === "privacy") {
            validateDestinationEntries(sectionValue.allowedQdrantDestinations, `configuration.${sectionName}.allowedQdrantDestinations`);
            validateDestinationEntries(sectionValue.allowedEmbeddingDestinations, `configuration.${sectionName}.allowedEmbeddingDestinations`);
            validateDestinationEntries(sectionValue.allowedLlmDestinations, `configuration.${sectionName}.allowedLlmDestinations`);
        }
    }
    validateHost(root.prime, "configuration.prime");
    validateHost(root.pi, "configuration.pi");
}
function isCredentialKey(normalized) {
    return FILE_SECRET_MARKERS.has(normalized) ||
        ["apikey", "authorization", "credential", "credentials", "password", "secret", "authtoken", "accesstoken", "refreshtoken"].some(marker => normalized.endsWith(marker));
}
function inspectFile(value, path = "") {
    if (Array.isArray(value)) {
        value.forEach((item, index) => inspectFile(item, `${path}[${index}]`));
        return;
    }
    if (!isRecord(value))
        return;
    for (const [key, child] of Object.entries(value)) {
        const fullPath = path === "" ? key : `${path}.${key}`;
        if (DANGEROUS_KEYS.has(key))
            throw new Error("Unsafe configuration field");
        const normalized = key.toLowerCase().replace(/[^a-z0-9]/g, "");
        if (isCredentialKey(normalized))
            throw new Error("Credentials are allowed only through environment variables");
        if (fullPath === RETIRED_ADMIN_FIELD || fullPath === RETIRED_ADMIN_ALIAS || key === RETIRED_SOURCE_FIELD || key === RETIRED_SOURCE_ALIAS) {
            throw new Error("Retired configuration field");
        }
        inspectFile(child, fullPath);
    }
}
function boundedNumber(name, raw, min, max) {
    const value = typeof raw === "number"
        ? raw
        : typeof raw === "string" && raw.trim() !== "" ? Number(raw) : Number.NaN;
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
    if (typeof raw !== "string" || raw.length === 0)
        throw new Error(`${name} must be a non-empty string`);
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
function environmentSecret(env, name) {
    const raw = env[`${PREFIX}${name}`];
    return raw === undefined || raw.trim() === "" ? undefined : raw;
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
    if (parsed.username !== "" || parsed.password !== "" || parsed.search !== "" || parsed.hash !== "") {
        throw new Error(`${name} must not include credentials or query metadata`);
    }
    return value.replace(/\/+$/, "");
}
function absentFile(error) {
    return isRecord(error) && error.code === "ENOENT";
}
function checkEnvironmentNames(env) {
    for (const name of Object.keys(env)) {
        if (!name.startsWith(PREFIX))
            continue;
        const suffix = name.slice(PREFIX.length);
        if (suffix === "HOST")
            continue;
        if (suffix.startsWith(RETIRED_ENV_PREFIX) || OPERATIONAL_SUFFIXES.has(suffix) || SECRET_SUFFIXES.has(suffix)) {
            if (OPERATIONAL_SUFFIXES.has(suffix) || SECRET_SUFFIXES.has(suffix))
                continue;
            throw new Error(`Unsupported environment setting ${name}`);
        }
        throw new Error(`Unsupported environment setting ${name}`);
    }
}
function listValue(name, raw, fallback) {
    if (raw === undefined)
        return [...fallback];
    if (!Array.isArray(raw) || raw.some(value => typeof value !== "string" || value.length === 0)) {
        throw new Error(`${name} must be an array of non-empty strings`);
    }
    return [...raw];
}
function destinations(name, raw, fallback) {
    if (raw === undefined)
        return [...fallback];
    if (!Array.isArray(raw))
        throw new Error(`${name} must be an array`);
    return raw.map((value, index) => {
        if (!isRecord(value))
            throw new Error(`${name}[${index}] must be an object`);
        return {
            id: stringValue(`${name}[${index}].id`, value.id, ""),
            residency: stringValue(`${name}[${index}].residency`, value.residency, ""),
            dataUse: stringValue(`${name}[${index}].dataUse`, value.dataUse, ""),
        };
    });
}
function retention(name, raw, fallback) {
    if (raw === undefined)
        return fallback;
    if (raw === "indefinite")
        return raw;
    return boundedInteger(name, raw, 1, 3650);
}
function consistency(name, raw, fallback) {
    if (raw === undefined)
        return fallback;
    if (raw === "majority" || raw === "quorum" || raw === "all")
        return raw;
    return boundedInteger(name, raw, 1, 7);
}
export function configPath(deps) {
    const configHome = deps.xdgConfigHome !== undefined && deps.xdgConfigHome !== ""
        ? deps.xdgConfigHome
        : join(deps.homeDir, ".config");
    return join(configHome, "pi-qdrant-memory", "config.json");
}
export async function loadConfig(host, deps) {
    checkEnvironmentNames(deps.env);
    const path = configPath(deps);
    let file = {};
    try {
        const parsed = JSON.parse(await deps.readTextFile(path));
        if (!isRecord(parsed))
            throw new Error("Configuration root must be a JSON object");
        inspectFile(parsed);
        validateFileShape(parsed);
        file = parsed;
    }
    catch (error) {
        if (!absentFile(error))
            throw error;
    }
    const defaults = defaultConfig(host);
    const hostOverrides = section(file, host, `${host} configuration`);
    const qdrant = mergeRecords(defaults.qdrant, section(file, "qdrant", "qdrant configuration"), section(hostOverrides, "qdrant", `${host}.qdrant configuration`));
    const embeddings = mergeRecords(defaults.embeddings, section(file, "embeddings", "embeddings configuration"), section(hostOverrides, "embeddings", `${host}.embeddings configuration`));
    const retrieval = mergeRecords(defaults.retrieval, section(file, "retrieval", "retrieval configuration"), section(hostOverrides, "retrieval", `${host}.retrieval configuration`));
    const projects = mergeRecords(defaults.projects, section(file, "projects", "projects configuration"), section(hostOverrides, "projects", `${host}.projects configuration`));
    const capture = mergeRecords(defaults.capture, section(file, "capture", "capture configuration"), section(hostOverrides, "capture", `${host}.capture configuration`));
    const privacy = mergeRecords(defaults.privacy, section(file, "privacy", "privacy configuration"), section(hostOverrides, "privacy", `${host}.privacy configuration`));
    const coordination = mergeRecords(defaults.coordination, section(file, "coordination", "coordination configuration"), section(hostOverrides, "coordination", `${host}.coordination configuration`));
    const outbox = mergeRecords(defaults.outbox, section(file, "outbox", "outbox configuration"), section(hostOverrides, "outbox", `${host}.outbox configuration`));
    const curation = mergeRecords(defaults.curation, section(file, "curation", "curation configuration"), section(hostOverrides, "curation", `${host}.curation configuration`));
    const memoryModel = mergeRecords(defaults.memoryModel, section(file, "memoryModel", "memoryModel configuration"), section(hostOverrides, "memoryModel", `${host}.memoryModel configuration`));
    const raptor = mergeRecords(defaults.raptor, section(file, "raptor", "raptor configuration"), section(hostOverrides, "raptor", `${host}.raptor configuration`));
    const autoRecallRaw = deps.env[`${PREFIX}AUTO_RECALL`];
    const sharedEnabled = file.enabled;
    const sharedAutoRecall = file.autoRecall;
    const result = {
        host,
        configPath: path,
        enabled: booleanValue(`${host}.enabled`, hostOverrides.enabled, booleanValue("enabled", sharedEnabled, defaults.enabled)),
        autoRecall: autoRecallRaw === undefined
            ? booleanValue(`${host}.autoRecall`, hostOverrides.autoRecall, booleanValue("autoRecall", sharedAutoRecall, defaults.autoRecall))
            : environmentBoolean(`${PREFIX}AUTO_RECALL`, autoRecallRaw, defaults.autoRecall),
        qdrant: {
            url: normalizeUrl(`${PREFIX}QDRANT_URL`, environmentRaw(deps.env, "QDRANT_URL", qdrant.url), defaults.qdrant.url),
            collection: stringValue(`${PREFIX}QDRANT_COLLECTION`, environmentRaw(deps.env, "QDRANT_COLLECTION", qdrant.collection), defaults.qdrant.collection),
            replicationFactor: boundedInteger("qdrant.replicationFactor", qdrant.replicationFactor, 1, 7),
            writeConsistencyFactor: boundedInteger("qdrant.writeConsistencyFactor", qdrant.writeConsistencyFactor, 1, 7),
        },
        embeddings: {
            baseUrl: normalizeUrl(`${PREFIX}EMBEDDING_BASE_URL`, environmentRaw(deps.env, "EMBEDDING_BASE_URL", embeddings.baseUrl), defaults.embeddings.baseUrl),
            model: stringValue(`${PREFIX}EMBEDDING_MODEL`, environmentRaw(deps.env, "EMBEDDING_MODEL", embeddings.model), defaults.embeddings.model),
            dimension: (() => {
                const value = boundedInteger(`${PREFIX}EMBEDDING_DIMENSION`, environmentRaw(deps.env, "EMBEDDING_DIMENSION", embeddings.dimension), 1, 65536);
                if (value !== 1024)
                    throw new Error("embeddings.dimension must be 1024");
                return 1024;
            })(),
            queryPrefix: stringValue("embeddings.queryPrefix", embeddings.queryPrefix, defaults.embeddings.queryPrefix),
        },
        retrieval: {
            topK: boundedInteger(`${PREFIX}TOP_K`, environmentRaw(deps.env, "TOP_K", retrieval.topK), 1, 10),
            candidatesPerLane: boundedInteger(`${PREFIX}CANDIDATES_PER_LANE`, environmentRaw(deps.env, "CANDIDATES_PER_LANE", retrieval.candidatesPerLane), 1, 100),
            minScore: boundedNumber(`${PREFIX}MIN_SCORE`, environmentRaw(deps.env, "MIN_SCORE", retrieval.minScore), -1, 1),
            projectBoost: boundedNumber(`${PREFIX}PROJECT_BOOST`, environmentRaw(deps.env, "PROJECT_BOOST", retrieval.projectBoost), 0, 0.25),
            contextBudgetChars: boundedInteger(`${PREFIX}CONTEXT_BUDGET_CHARS`, environmentRaw(deps.env, "CONTEXT_BUDGET_CHARS", retrieval.contextBudgetChars), 1, 16000),
            toolResultBudgetChars: boundedInteger(`${PREFIX}TOOL_RESULT_BUDGET_CHARS`, environmentRaw(deps.env, "TOOL_RESULT_BUDGET_CHARS", retrieval.toolResultBudgetChars), 1, 16000),
            hardContextCharBudget: 16000,
            timeoutMs: boundedInteger(`${PREFIX}TIMEOUT_MS`, environmentRaw(deps.env, "TIMEOUT_MS", retrieval.timeoutMs), 100, 30000),
            rootScope: retrieval.rootScope === "project_and_global" ? "project_and_global" : retrieval.rootScope === "project" ? "project" : (() => { throw new Error("retrieval.rootScope must be project or project_and_global"); })(),
            childSearch: booleanValue("retrieval.childSearch", retrieval.childSearch, true),
        },
        projects: {
            registrations: isRecord(projects.registrations) ? Object.fromEntries(Object.entries(projects.registrations).map(([key, value]) => {
                if (!isRecord(value))
                    throw new Error(`projects.registrations.${key} must be an object`);
                return [key, {
                        canonicalPath: stringValue(`projects.registrations.${key}.canonicalPath`, value.canonicalPath, ""),
                        fingerprint: stringValue(`projects.registrations.${key}.fingerprint`, value.fingerprint, ""),
                        alias: stringValue(`projects.registrations.${key}.alias`, value.alias, ""),
                    }];
            })) : (() => { throw new Error("projects.registrations must be an object"); })(),
        },
        capture: {
            enabled: booleanValue("capture.enabled", capture.enabled, false),
            projectAllowlist: listValue("capture.projectAllowlist", capture.projectAllowlist, []),
            projectDenylist: listValue("capture.projectDenylist", capture.projectDenylist, []),
            episodeRetentionDays: retention("capture.episodeRetentionDays", capture.episodeRetentionDays, "indefinite"),
            toolArgsChars: boundedInteger("capture.toolArgsChars", capture.toolArgsChars, 0, 16000),
            toolResultChars: boundedInteger("capture.toolResultChars", capture.toolResultChars, 0, 16000),
        },
        privacy: {
            egressMode: privacy.egressMode === "allowlist" ? "allowlist" : privacy.egressMode === "local_only" ? "local_only" : (() => { throw new Error("privacy.egressMode must be local_only or allowlist"); })(),
            allowedQdrantDestinations: destinations("privacy.allowedQdrantDestinations", privacy.allowedQdrantDestinations, []),
            allowedEmbeddingDestinations: destinations("privacy.allowedEmbeddingDestinations", privacy.allowedEmbeddingDestinations, []),
            allowedLlmDestinations: destinations("privacy.allowedLlmDestinations", privacy.allowedLlmDestinations, []),
            allowActiveModelFallback: booleanValue("privacy.allowActiveModelFallback", privacy.allowActiveModelFallback, false),
            allowCrossProviderReplay: booleanValue("privacy.allowCrossProviderReplay", privacy.allowCrossProviderReplay, false),
        },
        coordination: {
            maxClockSkewMs: boundedInteger("coordination.maxClockSkewMs", coordination.maxClockSkewMs, 0, 3600000),
            readConsistency: consistency("coordination.readConsistency", coordination.readConsistency, 1),
            leaseMs: boundedInteger("coordination.leaseMs", coordination.leaseMs, 5000, 300000),
            reconcileIntervalMs: boundedInteger("coordination.reconcileIntervalMs", coordination.reconcileIntervalMs, 60000, 86400000),
        },
        outbox: {
            maxJobs: boundedInteger("outbox.maxJobs", outbox.maxJobs, 1, 100000),
            maxBytes: boundedInteger("outbox.maxBytes", outbox.maxBytes, 1048576, 1073741824),
            retryBaseMs: boundedInteger("outbox.retryBaseMs", outbox.retryBaseMs, 100, 10000),
            retryMaxMs: boundedInteger("outbox.retryMaxMs", outbox.retryMaxMs, 1000, 300000),
            ...(outbox.nodeId === undefined ? {} : { nodeId: stringValue("outbox.nodeId", outbox.nodeId, "") }),
            sharedFilesystem: booleanValue("outbox.sharedFilesystem", outbox.sharedFilesystem, false),
        },
        curation: {
            turnTrigger: boundedInteger("curation.turnTrigger", curation.turnTrigger, 1, 1000),
            toolTrigger: boundedInteger("curation.toolTrigger", curation.toolTrigger, 1, 1000),
            maxInputTokens: boundedInteger("curation.maxInputTokens", curation.maxInputTokens, 512, 65536),
        },
        memoryModel: {
            ...(memoryModel.modelId === undefined ? {} : { modelId: stringValue("memoryModel.modelId", memoryModel.modelId, "") }),
            timeoutMs: boundedInteger("memoryModel.timeoutMs", memoryModel.timeoutMs, 1000, 120000),
            maxOutputTokens: boundedInteger("memoryModel.maxOutputTokens", memoryModel.maxOutputTokens, 128, 8192),
        },
        raptor: {
            rebuildEpisodeDelta: boundedInteger("raptor.rebuildEpisodeDelta", raptor.rebuildEpisodeDelta, 2, 10000),
            maxLevels: boundedInteger("raptor.maxLevels", raptor.maxLevels, 1, 10),
            summaryInputTokens: boundedInteger("raptor.summaryInputTokens", raptor.summaryInputTokens, 512, 65536),
            umapDimensions: boundedInteger("raptor.umapDimensions", raptor.umapDimensions, 1, 64),
            localNeighbors: boundedInteger("raptor.localNeighbors", raptor.localNeighbors, 2, 200),
            gmmMaxClusters: boundedInteger("raptor.gmmMaxClusters", raptor.gmmMaxClusters, 1, 200),
            membershipThreshold: boundedNumber("raptor.membershipThreshold", raptor.membershipThreshold, 0.01, 1),
            ...(raptor.seed === undefined ? {} : { seed: boundedInteger("raptor.seed", raptor.seed, 0, 4294967295) }),
        },
    };
    const qdrantApiKey = environmentSecret(deps.env, "QDRANT_API_KEY");
    const embeddingApiKey = environmentSecret(deps.env, "EMBEDDING_API_KEY");
    if (qdrantApiKey !== undefined)
        result.qdrant.apiKey = qdrantApiKey;
    if (embeddingApiKey !== undefined)
        result.embeddings.apiKey = embeddingApiKey;
    return result;
}
//# sourceMappingURL=config.js.map