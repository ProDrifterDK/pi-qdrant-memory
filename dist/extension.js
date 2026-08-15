import { constants as fsConstants } from "node:fs";
import { lstat, open, opendir, readFile, realpath } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, normalize, resolve, sep } from "node:path";
import { RecallCache } from "./cache.js";
import { capturePersistedEntries, persistCaptureActivationFile } from "./capture/episode.js";
import { EmbeddingsClient, bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory } from "./clients/embeddings.js";
import { MemoryClientError } from "./clients/http.js";
import { destinationForEndpoint } from "./security/egress.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "./domain/canonical.js";
import { canonicalRecordHash, episodeSemanticProjection } from "./domain/records.js";
import { intersectPolicies, processingPolicyHash } from "./domain/policy.js";
import { createOutbox, resolveOutboxNodeId } from "./outbox/store.js";
import { createIngestProcessor, createOutboxDelivery } from "./outbox/delivery.js";
import { bindIngestRuntime } from "./coordination/ingest.js";
import { runCurationFromLifecycle, runRaptorFromLifecycle } from "./coordination/root.js";
import { createJob } from "./coordination/jobs.js";
import { bindQdrantDestination, createQdrantSafeBundle } from "./qdrant/write.js";
import { completeMemory } from "./curation/llm.js";
import { RAPTOR_PROMPT_REVISION } from "./raptor/builder.js";
import { loadConfig } from "./config.js";
import { detectHost, resolveHostAgentMarker } from "./host.js";
import { resolveProjectIdentity } from "./project.js";
import { createGuardedMemoryReadStore, MemoryRetriever } from "./retrieval/search.js";
import { MemoryService, } from "./service.js";
import { createMemorySearchTool } from "./tool.js";
const DEFAULT_TOP_K = 5;
const DEFAULT_TOOL_BUDGET = 8000;
const HARD_CONTEXT_BUDGET = 16000;
const CACHE_TTL_MS = 5 * 60 * 1000;
const CACHE_MAX_ENTRIES = 32;
const LIFECYCLE_DELIVERY_BATCH = 64;
const LIFECYCLE_RECOVERY_PRODUCERS = 64;
const LIFECYCLE_RECOVERY_NODES = 512;
const RECOVERY_SCAN_ENTRIES = 512;
const RECOVERY_SCAN_BYTES = 4 * 1024 * 1024;
// Inode-safe recovery performs several syscalls per candidate; keep a finite
// startup bound without starving the 64-producer page on slower supported hosts.
const RECOVERY_SCAN_TIME_MS = 2_000;
const RECOVERY_MAX_TIME = Date.parse("2100-12-31T23:59:59.999Z");
const RAPTOR_PAGE_SIZE = 256;
const RAPTOR_MAX_LEAVES = 65_536;
const RAPTOR_MAX_PAGES = Math.ceil(RAPTOR_MAX_LEAVES / RAPTOR_PAGE_SIZE);
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
        return resolveHostAgentMarker(host, ctx.sessionManager.getHeader(), env).rootWorkAllowed;
    }
    catch {
        return false;
    }
}
function sameDestination(left, right) { return left.id === right.id && left.residency === right.residency && left.dataUse === right.dataUse; }
function selectedMemoryModel(ctx, config) {
    if (config.memoryModel.modelId !== undefined) {
        const registry = ctx.modelRegistry;
        if (registry === undefined || typeof registry.getAvailable !== "function")
            return undefined;
        try {
            const candidates = registry.getAvailable().filter((model) => model.id === config.memoryModel.modelId || `${model.provider}/${model.id}` === config.memoryModel.modelId);
            return candidates.length === 1 ? candidates[0] : undefined;
        }
        catch {
            return undefined;
        }
    }
    return config.privacy.allowActiveModelFallback ? ctx.model : undefined;
}
function snapshotCompletionRegistry(registry) {
    if (registry === undefined || registry === null || typeof registry !== "object")
        return undefined;
    const complete = Reflect.get(registry, "complete");
    const getAuth = Reflect.get(registry, "getApiKeyAndHeaders");
    return Object.freeze({
        ...(typeof complete === "function" ? { complete: (...args) => Reflect.apply(complete, registry, args) } : {}),
        ...(typeof getAuth === "function" ? { getApiKeyAndHeaders: (model) => Reflect.apply(getAuth, registry, [model]) } : {}),
    });
}
function destinationForModel(model, config) {
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
function modelMatchesConfiguredId(model, modelId) {
    return model.id === modelId || `${model.provider}/${model.id}` === modelId;
}
/**
 * Capture authorization is a producer snapshot, not a model-registry lookup.
 * A configured allowlist destination can be named from the immutable config;
 * local-only/fallback destinations are derived only from the already selected
 * session model.  The live registry is deliberately reserved for scheduling.
 */
function snapshotLlmAuthorization(input) {
    const configuredId = input.config.memoryModel.modelId;
    const sessionModel = input.ctx.model;
    if (configuredId !== undefined) {
        if (input.config.privacy.egressMode === "allowlist") {
            // Without registry metadata the configured selector itself is the only
            // exact model identity available.  Provider/model suffix aliases are
            // deliberately rejected rather than authorizing a different provider.
            const matches = input.config.privacy.allowedLlmDestinations.filter((destination) => destination.id === configuredId);
            if (matches.length === 1)
                return Object.freeze({ destination: Object.freeze({ ...matches[0] }) });
        }
        else {
            // A dedicated local model may differ from the session model.  In that
            // case the operator must provide one exact endpoint-derived destination
            // descriptor; scheduling later recomputes the selected registry model's
            // local destination and requires the same ID/labels.  No registry access
            // or endpoint wildcard is needed during capture or shutdown.
            const configured = input.config.privacy.allowedLlmDestinations;
            if (configured.length === 1)
                return Object.freeze({ destination: Object.freeze({ ...configured[0] }) });
        }
        // With no static destination descriptor, the already-selected active
        // session model is still a safe exact local snapshot when it is precisely
        // the configured model.  Otherwise capture remains fail-closed for LLM
        // processing instead of minting a guessed destination.
        if (sessionModel !== undefined && modelMatchesConfiguredId(sessionModel, configuredId)) {
            const destination = destinationForModel(sessionModel, input.config);
            return destination === undefined ? Object.freeze({}) : Object.freeze({ destination });
        }
        return Object.freeze({});
    }
    if (!input.config.privacy.allowActiveModelFallback || sessionModel === undefined)
        return Object.freeze({});
    const destination = destinationForModel(sessionModel, input.config);
    return destination === undefined ? Object.freeze({}) : Object.freeze({ destination });
}
function activeModelDestination(ctx, config) {
    const model = ctx.model;
    return model === undefined ? undefined : destinationForModel(model, config);
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
async function createProductionRuntime(session, outbox, policy, dependencies) {
    // Production mutation authority never accepts an injected transport. Tests
    // inject the high-level lifecycle seam instead of minting raw Qdrant powers.
    if (dependencies.fetchImpl !== undefined)
        throw new MemoryClientError("configuration", "Lifecycle mutation transport is unavailable");
    const qdrantDestination = configuredQdrantDestination(session.config);
    const embeddingDestination = configuredEmbeddingDestination(session.config);
    if (qdrantDestination === undefined || embeddingDestination === undefined)
        throw new MemoryClientError("configuration", "Lifecycle destinations are unavailable");
    // Obtain the active coordination identity through the already-guarded read
    // surface, then construct ONE lexical safe bundle for writes + control.
    const reader = createGuardedMemoryReadStore({
        baseUrl: session.config.qdrant.url, collection: session.config.qdrant.collection, ownerHost: session.host,
        timeoutMs: session.config.retrieval.timeoutMs, readConsistency: session.config.coordination.readConsistency,
        maxClockSkewMs: session.config.coordination.maxClockSkewMs, destination: qdrantDestination,
        egressMode: session.config.privacy.egressMode, ...(session.config.qdrant.apiKey === undefined ? {} : { apiKey: session.config.qdrant.apiKey }),
        ...(session.config.outbox.nodeId === undefined ? {} : { nodeId: session.config.outbox.nodeId }),
    });
    const control = await reader.readControl();
    const bundle = createQdrantSafeBundle({
        options: { baseUrl: session.config.qdrant.url, collection: session.config.qdrant.collection, ownerHost: session.host, timeoutMs: session.config.retrieval.timeoutMs,
            readConsistency: session.config.coordination.readConsistency, maxClockSkewMs: session.config.coordination.maxClockSkewMs,
            replicationFactor: session.config.qdrant.replicationFactor, writeConsistencyFactor: session.config.qdrant.writeConsistencyFactor,
            ...(session.config.qdrant.apiKey === undefined ? {} : { apiKey: session.config.qdrant.apiKey }) },
        destination: qdrantDestination, egressMode: session.config.privacy.egressMode,
        ...(session.config.outbox.nodeId === undefined ? {} : { nodeId: session.config.outbox.nodeId }),
        coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch,
    });
    const qdrant = bindQdrantDestination(bundle.qdrant, qdrantDestination);
    const embeddingClient = new EmbeddingsClient({ baseUrl: session.config.embeddings.baseUrl, model: session.config.embeddings.model, dimension: 1024, queryPrefix: session.config.embeddings.queryPrefix, timeoutMs: session.config.retrieval.timeoutMs, ...(session.config.embeddings.apiKey === undefined ? {} : { apiKey: session.config.embeddings.apiKey }) });
    const validated = bindEmbeddingDocumentClient({ endpoint: session.config.embeddings.baseUrl, client: embeddingClient });
    const embedding = bindEmbeddingDestination(createEmbeddingDestinationFactory({ endpoint: session.config.embeddings.baseUrl, destination: embeddingDestination, client: validated, egressMode: session.config.privacy.egressMode, ...(session.config.outbox.nodeId === undefined ? {} : { nodeId: session.config.outbox.nodeId }), coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch }), embeddingDestination);
    const runtime = bindIngestRuntime({ store: bundle.store, qdrant, embedding });
    const processor = createIngestProcessor({ localPolicy: policy, runtime, maxClockSkewMs: session.config.coordination.maxClockSkewMs, now: Date.now });
    const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: Date.now, maxClockSkewMs: session.config.coordination.maxClockSkewMs, retryBaseMs: session.config.outbox.retryBaseMs, retryMaxMs: session.config.outbox.retryMaxMs, attemptTimeoutMs: session.config.retrieval.timeoutMs });
    return Object.freeze({ store: bundle.store, delivery, embedding, workerPolicy: policy, control });
}
function withRuntimeNodeId(config, nodeId) {
    if (config.outbox.nodeId === nodeId)
        return config;
    return Object.freeze({ ...config, outbox: Object.freeze({ ...config.outbox, nodeId }) });
}
function contextIsChild(ctx, host, env) {
    try {
        return !resolveHostAgentMarker(host, ctx.sessionManager.getHeader(), env).rootWorkAllowed;
    }
    catch {
        return true;
    }
}
function canonicalCaptureMarker(host, marker) {
    if (host === "prime")
        return Object.freeze({ host: "prime", header: undefined, env: Object.freeze({ RLM_DEPTH: String(marker.role === "root" ? 0 : Math.max(1, marker.depth)) }) });
    return Object.freeze({ host: "pi", header: undefined, env: marker.role === "root" ? Object.freeze({}) : Object.freeze({ PI_SUBAGENT_CHILD: "1", PI_SUBAGENT_DEPTH: String(Math.max(1, marker.depth)) }) });
}
function validProjectIdentity(project) {
    if ((project.identityKind !== "registered" && project.identityKind !== "local_only") || project.registrationValid !== true || typeof project.id !== "string" || project.id.length === 0 || project.id.length > 512)
        return false;
    if (project.identityKind === "registered" ? project.reason !== undefined : project.reason !== undefined && project.reason !== "unregistered")
        return false;
    if (typeof project.canonicalPath !== "string" || project.canonicalPath.length === 0 || project.canonicalPath.length > 4096 || !isAbsolute(project.canonicalPath) || normalize(project.canonicalPath) !== project.canonicalPath)
        return false;
    if (typeof project.fingerprint !== "string" || project.fingerprint.length === 0 || project.fingerprint.length > 512)
        return false;
    return true;
}
function sameProjectIdentity(left, right) {
    return validProjectIdentity(left) && validProjectIdentity(right) && left.id === right.id && left.identityKind === right.identityKind && left.canonicalPath === right.canonicalPath && left.fingerprint === right.fingerprint;
}
function safeSessionId(ctx) {
    try {
        const value = ctx.sessionManager.getSessionId();
        return typeof value === "string" && value.length > 0 && value.length <= 512 ? value : undefined;
    }
    catch {
        return undefined;
    }
}
function sessionEntries(ctx) {
    return () => ctx.sessionManager.getEntries();
}
const RECOVERY_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const RECOVERY_PRODUCER_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const RECOVERY_HASH = /^[a-f0-9]{64}$/u;
const RECOVERY_NODE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const RECOVERY_ISO_DATE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
const RECOVERY_RESERVATION_KEYS = [
    "version", "reservationId", "jobId", "jobAuditHash", "policyId", "deadline",
    "nodeId", "producerUuid", "requestedBytes", "auditHash",
];
function recoveryErrno(error, code) {
    return typeof error === "object" && error !== null && "code" in error && error.code === code;
}
function recoveryRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function recoveryExactKeys(value, expected) {
    const actual = Object.keys(value).sort();
    const keys = [...expected].sort();
    return actual.length === keys.length && actual.every((key, index) => key === keys[index]);
}
function recoveryBudgetAllows(budget) {
    return budget.entries < RECOVERY_SCAN_ENTRIES && budget.bytes < RECOVERY_SCAN_BYTES && Date.now() - budget.startedAt < RECOVERY_SCAN_TIME_MS;
}
function recoveryBudgetEntry(budget) {
    if (!recoveryBudgetAllows(budget))
        return false;
    budget.entries += 1;
    return true;
}
async function recoveryCanonicalPath(path) {
    const absolute = resolve(path);
    if (!isAbsolute(path) || path !== absolute)
        return false;
    try {
        return await realpath(path) === absolute;
    }
    catch {
        return false;
    }
}
async function recoveryDirectory(path) {
    if (!(await recoveryCanonicalPath(path)))
        return false;
    try {
        const info = await lstat(path);
        return info.isDirectory() && !info.isSymbolicLink();
    }
    catch {
        return false;
    }
}
async function readRecoveryJson(path, budget, maxBytes, canonical = false) {
    const info = await lstat(path);
    if (!(await recoveryCanonicalPath(path)) || !recoveryBudgetAllows(budget))
        throw new Error("Recovery artifact is unsafe");
    if (!info.isFile() || info.isSymbolicLink() || info.size < 1 || info.size > maxBytes || budget.bytes + info.size > RECOVERY_SCAN_BYTES)
        throw new Error("Recovery artifact is unsafe");
    const handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    try {
        const opened = await handle.stat();
        if (!opened.isFile() || opened.isSymbolicLink() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== info.size)
            throw new Error("Recovery artifact changed");
        const bytes = await handle.readFile();
        budget.bytes += bytes.length;
        const after = await lstat(path);
        if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== info.size || bytes.length !== info.size)
            throw new Error("Recovery artifact changed");
        const text = Buffer.from(bytes).toString("utf8");
        const parsed = JSON.parse(text);
        if (canonical && canonicalStringify(parsed) !== text)
            throw new Error("Recovery artifact is not canonical");
        return parsed;
    }
    finally {
        await handle.close().catch(() => undefined);
    }
}
async function boundedRecoveryEntries(path, limit, budget, directories) {
    if (!recoveryBudgetAllows(budget) || !(await recoveryDirectory(path)))
        return Object.freeze([]);
    const names = [];
    try {
        const directory = await opendir(path);
        try {
            for await (const entry of directory) {
                if (names.length >= limit || !recoveryBudgetEntry(budget))
                    break;
                if (entry.isSymbolicLink())
                    continue;
                if (directories ? entry.isDirectory() : entry.isFile())
                    names.push(entry.name);
            }
        }
        finally {
            await directory.close().catch(() => undefined);
        }
    }
    catch {
        return Object.freeze([]);
    }
    return Object.freeze(names.sort());
}
function validateRecoveryReservation(value, fileName) {
    if (!recoveryRecord(value) || !recoveryExactKeys(value, RECOVERY_RESERVATION_KEYS) || value.version !== 1)
        throw new Error("Recovery reservation is malformed");
    if (typeof value.reservationId !== "string" || !RECOVERY_UUID.test(value.reservationId) || fileName !== `${value.reservationId}.json`)
        throw new Error("Recovery reservation is malformed");
    if (typeof value.jobId !== "string" || !RECOVERY_UUID.test(value.jobId) || typeof value.jobAuditHash !== "string" || !RECOVERY_HASH.test(value.jobAuditHash) || typeof value.policyId !== "string" || !RECOVERY_HASH.test(value.policyId))
        throw new Error("Recovery reservation is malformed");
    if (value.deadline !== null && (typeof value.deadline !== "string" || !RECOVERY_ISO_DATE.test(value.deadline) || !Number.isFinite(Date.parse(value.deadline))))
        throw new Error("Recovery reservation is malformed");
    if (typeof value.nodeId !== "string" || !RECOVERY_NODE.test(value.nodeId) || value.nodeId === "." || value.nodeId === ".." || value.nodeId === "local" || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value.nodeId))
        throw new Error("Recovery reservation is malformed");
    if (typeof value.producerUuid !== "string" || !RECOVERY_PRODUCER_UUID.test(value.producerUuid))
        throw new Error("Recovery reservation is malformed");
    if (typeof value.requestedBytes !== "number" || !Number.isSafeInteger(value.requestedBytes) || value.requestedBytes < 1 || value.requestedBytes > 1_073_741_824)
        throw new Error("Recovery reservation is malformed");
    if (typeof value.auditHash !== "string" || !RECOVERY_HASH.test(value.auditHash))
        throw new Error("Recovery reservation is malformed");
    const withoutAudit = { ...value };
    delete withoutAudit.auditHash;
    if (value.auditHash !== sha256Hex(canonicalStringify(withoutAudit)) || value.reservationId !== deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", value.nodeId, value.producerUuid, value.jobId))
        throw new Error("Recovery reservation is malformed");
    return value;
}
async function recoveryReservationProducers(root, budget) {
    const producers = new Set();
    const reservations = join(root, "reservations");
    for (const name of await boundedRecoveryEntries(reservations, RECOVERY_SCAN_ENTRIES, budget, false)) {
        if (!recoveryBudgetAllows(budget) || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.json$/u.test(name))
            continue;
        try {
            const value = validateRecoveryReservation(await readRecoveryJson(join(reservations, name), budget, 1_048_576, true), name);
            producers.add(value.producerUuid);
        }
        catch { /* malformed, foreign, or changing reservations remain fail-closed */ }
    }
    return producers;
}
async function recoveryDirectoryState(path) {
    if (!(await recoveryCanonicalPath(path))) {
        try {
            await lstat(path);
            return { exists: true, nonempty: false, valid: false };
        }
        catch (error) {
            return recoveryErrno(error, "ENOENT") ? { exists: false, nonempty: false, valid: true } : { exists: true, nonempty: false, valid: false };
        }
    }
    try {
        const directory = await opendir(path);
        let nonempty = false;
        try {
            for await (const entry of directory) {
                if (entry.isSymbolicLink())
                    return { exists: true, nonempty: false, valid: false };
                nonempty = true;
                break;
            }
        }
        finally {
            await directory.close().catch(() => undefined);
        }
        return { exists: true, nonempty, valid: true };
    }
    catch (error) {
        return recoveryErrno(error, "ENOENT") ? { exists: false, nonempty: false, valid: true } : { exists: true, nonempty: false, valid: false };
    }
}
async function producerHasRecoveryWork(path, reservationProducers, budget) {
    let sawProductionDirectory = false;
    const candidates = [join(path, "jobs"), join(path, "control"), join(path, "quarantine")];
    for (const name of await boundedRecoveryEntries(path, 128, budget, true))
        if (/^jobs\.fenced-[a-f0-9]{32}$/u.test(name))
            candidates.push(join(path, name));
    for (const candidate of candidates) {
        const state = await recoveryDirectoryState(candidate);
        if (!state.valid)
            return false;
        sawProductionDirectory ||= state.exists;
        if (state.nonempty)
            return true;
    }
    const producerUuid = path.split(sep).at(-1);
    return !sawProductionDirectory || (producerUuid !== undefined && reservationProducers.has(producerUuid));
}
async function producerRecoveryAt(path, producerUuid, budget) {
    let parsed;
    try {
        parsed = await readRecoveryJson(join(path, "recovery.json"), budget, 1_048_576, true);
    }
    catch (error) {
        if (recoveryErrno(error, "ENOENT"))
            return 0;
        return undefined;
    }
    if (!recoveryRecord(parsed) || !recoveryExactKeys(parsed, ["auditHash", "kind", "producerUuid", "recoveredAt", "version"]) || parsed.version !== 1 || parsed.kind !== "recovery_rotation" || parsed.producerUuid !== producerUuid || typeof parsed.recoveredAt !== "number" || !Number.isSafeInteger(parsed.recoveredAt) || parsed.recoveredAt < 0 || parsed.recoveredAt > RECOVERY_MAX_TIME || typeof parsed.auditHash !== "string" || !RECOVERY_HASH.test(parsed.auditHash))
        return undefined;
    const hashInput = { ...parsed };
    delete hashInput.auditHash;
    return parsed.auditHash === sha256Hex(canonicalStringify(hashInput)) ? parsed.recoveredAt : undefined;
}
async function closedProducerPaths(host, homeDir, env, operationNow = Date.now(), maxClockSkewMs = 0) {
    const name = host === "pi" ? "PI_CODING_AGENT_DIR" : "PRIME_AGENT_CODING_AGENT_DIR";
    const agentDir = env[name]?.trim() || join(homeDir, host === "pi" ? ".pi/agent" : ".prime/agent");
    const root = join(agentDir, "pi-qdrant-memory", "outbox");
    const budget = { startedAt: Date.now(), entries: 0, bytes: 0 };
    const reservationProducers = await recoveryReservationProducers(root, budget);
    const candidates = [];
    for (const node of await boundedRecoveryEntries(root, LIFECYCLE_RECOVERY_NODES, budget, true)) {
        const nodePath = join(root, node);
        for (const producer of await boundedRecoveryEntries(nodePath, LIFECYCLE_RECOVERY_NODES, budget, true)) {
            if (!recoveryBudgetAllows(budget))
                break;
            const path = join(nodePath, producer);
            try {
                const state = await readRecoveryJson(join(path, "state.json"), budget, 1_048_576);
                if (!recoveryRecord(state))
                    continue;
                const value = state;
                const staleActive = value.state === "active" && typeof value.heartbeatAt === "number" && Number.isSafeInteger(value.heartbeatAt) && value.heartbeatAt >= 0 && operationNow > value.heartbeatAt && operationNow - value.heartbeatAt > 60_000 + maxClockSkewMs;
                if (value.state !== "closed" && !staleActive)
                    continue;
                if (!(await producerHasRecoveryWork(path, reservationProducers, budget)))
                    continue;
                const recoveryAt = await producerRecoveryAt(path, producer, budget);
                if (recoveryAt === undefined)
                    continue;
                candidates.push({ path, recoveryAt, key: `${node}/${producer}` });
            }
            catch { /* unreadable paths are never adopted */ }
        }
    }
    candidates.sort((left, right) => left.recoveryAt - right.recoveryAt || left.key.localeCompare(right.key));
    return Object.freeze(candidates.slice(0, LIFECYCLE_RECOVERY_PRODUCERS).map((candidate) => candidate.path));
}
function lifecycleWarning(category) {
    return { category: "internal", message: `pi-qdrant-memory: lifecycle unavailable (${category}).` };
}
function captureExpiry(config, eventAt) {
    return eventAt === null || config.capture.episodeRetentionDays === "indefinite"
        ? null
        : new Date(eventAt + config.capture.episodeRetentionDays * 86_400_000).toISOString();
}
function capturePolicy(input) {
    const qdrant = configuredQdrantDestination(input.config);
    const embedding = configuredEmbeddingDestination(input.config);
    if (qdrant === undefined || embedding === undefined || qdrant.residency !== embedding.residency || qdrant.dataUse !== embedding.dataUse)
        return undefined;
    const authorization = snapshotLlmAuthorization(input);
    const modelDestination = authorization.destination;
    const authorizedLlm = modelDestination !== undefined && modelDestination.residency === qdrant.residency && modelDestination.dataUse === qdrant.dataUse ? modelDestination : undefined;
    // Provenance is always the active SESSION model.  The dedicated memory
    // model is a separate processing destination and can never rewrite origin.
    const sessionModel = input.ctx.model;
    const pending = {
        id: "pending", ownerHost: input.host,
        destinationIds: { qdrant: qdrant.id, embedding: embedding.id, ...(authorizedLlm === undefined ? {} : { llm: authorizedLlm.id }) },
        originProvider: sessionModel?.provider ?? "unknown",
        allowCrossProviderReplay: input.config.privacy.allowCrossProviderReplay,
        expiresAt: null, residency: qdrant.residency, dataUse: qdrant.dataUse,
        policyRevision: "capture-lifecycle-v1",
    };
    const policy = Object.freeze({ ...pending, destinationIds: Object.freeze({ ...pending.destinationIds }), id: processingPolicyHash(pending) });
    return Object.freeze({ policy, ...(sessionModel?.id === undefined ? {} : { modelId: sessionModel.id }) });
}
function capturePolicyForEvent(base, config, eventAt) {
    const expiresAt = captureExpiry(config, eventAt);
    if (base.policy.expiresAt === expiresAt)
        return base;
    const pending = { ...base.policy, expiresAt, destinationIds: { ...base.policy.destinationIds } };
    const policy = Object.freeze({ ...pending, destinationIds: Object.freeze({ ...pending.destinationIds }), id: processingPolicyHash(pending) });
    return Object.freeze({ policy, ...(base.modelId === undefined ? {} : { modelId: base.modelId }) });
}
function createProductionLifecycleCoordinatorInternal(input) {
    const now = input.now ?? Date.now;
    let outbox;
    let runtime;
    let policy;
    let producerBinding;
    let active;
    let outboxAdmissionFull = false;
    let raptorTask;
    let raptorController;
    async function episodesForIds(activeRuntime, episodeIds, expectedControl) {
        if (expectedControl.state !== "active" || episodeIds.length === 0 || episodeIds.length > RAPTOR_MAX_LEAVES || new Set(episodeIds).size !== episodeIds.length)
            return undefined;
        const episodes = [];
        for (let index = 0; index < episodeIds.length; index += 1024)
            episodes.push(...await activeRuntime.store.readEpisodes(episodeIds.slice(index, index + 1024), expectedControl.privacyEpoch).catch(() => []));
        const after = await activeRuntime.store.readControl().catch(() => null);
        if (after === null || after.contentHash !== expectedControl.contentHash)
            return undefined;
        const byId = new Map(episodes.map((episode) => [episode.id, episode]));
        if (byId.size !== episodeIds.length || episodeIds.some((id) => byId.get(id)?.id !== id || byId.get(id)?.privacyEpoch !== expectedControl.privacyEpoch))
            return undefined;
        return Object.freeze(episodeIds.map((id) => byId.get(id)));
    }
    async function policiesForEpisodes(activeRuntime, episodes, expectedControl) {
        if (episodes.length === 0 || episodes.some((episode) => episode.privacyEpoch !== expectedControl.privacyEpoch))
            return undefined;
        const policyIds = [...new Set(episodes.map((episode) => episode.processingPolicyId))].sort();
        const records = [];
        for (let index = 0; index < policyIds.length; index += 1024)
            records.push(...await activeRuntime.store.readProcessingPolicies(policyIds.slice(index, index + 1024)).catch(() => []));
        const after = await activeRuntime.store.readControl().catch(() => null);
        if (after === null || after.contentHash !== expectedControl.contentHash)
            return undefined;
        const byId = new Map(records.map((record) => [record.id, record.policy]));
        if (byId.size !== policyIds.length || policyIds.some((id) => byId.get(id)?.id !== id))
            return undefined;
        return Object.freeze(policyIds.map((id) => byId.get(id)));
    }
    async function allEpisodeIds(activeRuntime, expectedControl) {
        const ids = [];
        const seenIds = new Set();
        const seenOffsets = new Set();
        let offset;
        for (let page = 0; page < RAPTOR_MAX_PAGES; page += 1) {
            if (offset !== undefined) {
                if (seenOffsets.has(offset))
                    return undefined;
                seenOffsets.add(offset);
            }
            const result = await activeRuntime.store.scrollEpisodeIds(offset, RAPTOR_PAGE_SIZE, expectedControl.privacyEpoch).catch(() => undefined);
            if (result === undefined)
                return undefined;
            for (const id of result.episodeIds) {
                if (seenIds.has(id))
                    return undefined;
                seenIds.add(id);
                ids.push(id);
                if (ids.length > RAPTOR_MAX_LEAVES)
                    return undefined;
            }
            if (result.nextOffset === undefined) {
                const after = await activeRuntime.store.readControl().catch(() => null);
                return after?.contentHash === expectedControl.contentHash ? Object.freeze(ids.sort()) : undefined;
            }
            if (result.nextOffset === offset || result.episodeIds.length === 0)
                return undefined;
            offset = result.nextOffset;
        }
        return undefined;
    }
    async function activeRaptorLeafIds(activeRuntime, expectedControl) {
        if (expectedControl.activeGeneration === null)
            return Object.freeze([]);
        const root = await activeRuntime.store.readRaptorSummary(expectedControl.activeGeneration).catch(() => null);
        if (root === null || root.id !== expectedControl.activeGeneration || root.generationId !== expectedControl.activeGeneration || root.algorithm !== "raptor-manifest-root-v1" || root.memberIds === undefined || root.memberIds.length > 1024)
            return undefined;
        const leaves = [];
        const seen = new Set();
        for (const chunkId of root.memberIds) {
            const chunk = await activeRuntime.store.readRaptorSummary(chunkId).catch(() => null);
            if (chunk === null || chunk.id !== chunkId || chunk.generationId !== expectedControl.activeGeneration || chunk.algorithm !== "raptor-manifest-chunk-v1" || chunk.memberIds === undefined || chunk.memberIds.length > 1024)
                return undefined;
            for (const id of chunk.memberIds) {
                if (seen.has(id))
                    return undefined;
                seen.add(id);
                leaves.push(id);
                if (leaves.length > RAPTOR_MAX_LEAVES)
                    return undefined;
            }
        }
        const after = await activeRuntime.store.readControl().catch(() => null);
        return after?.contentHash === expectedControl.contentHash ? Object.freeze(leaves.sort()) : undefined;
    }
    async function previousRaptorSummaries(activeRuntime, expectedControl) {
        if (expectedControl.activeGeneration === null)
            return Object.freeze([]);
        const summaries = [];
        const seenIds = new Set();
        const seenOffsets = new Set();
        let offset;
        for (let page = 0; page < RAPTOR_MAX_PAGES; page += 1) {
            if (offset !== undefined) {
                if (seenOffsets.has(offset))
                    return Object.freeze([]);
                seenOffsets.add(offset);
            }
            const result = await activeRuntime.store.scrollRaptorSummaries(expectedControl.activeGeneration, offset, RAPTOR_PAGE_SIZE).catch(() => undefined);
            if (result === undefined)
                return Object.freeze(summaries);
            for (const summary of result.summaries) {
                if (summary.generationId !== expectedControl.activeGeneration || seenIds.has(summary.id))
                    return Object.freeze([]);
                seenIds.add(summary.id);
                summaries.push(summary);
                if (summaries.length > RAPTOR_MAX_LEAVES)
                    return Object.freeze(summaries.slice(0, RAPTOR_MAX_LEAVES));
            }
            if (result.nextOffset === undefined) {
                const after = await activeRuntime.store.readControl().catch(() => null);
                return after?.contentHash === expectedControl.contentHash ? Object.freeze(summaries) : Object.freeze([]);
            }
            if (result.nextOffset === offset)
                return Object.freeze([]);
            offset = result.nextOffset;
        }
        return Object.freeze(summaries);
    }
    const coordinator = {
        async start(session) {
            await persistCaptureActivationFile({ host: session.host, sessionId: session.sessionId, getEntries: session.getEntries, env: input.env, homeDir: input.homeDir, now });
            outboxAdmissionFull = false;
            outbox = await createOutbox({ host: session.host, homeDir: input.homeDir, env: input.env, ...(session.config.outbox.nodeId === undefined ? {} : { nodeId: session.config.outbox.nodeId }), sharedFilesystem: session.config.outbox.sharedFilesystem, maxJobs: session.config.outbox.maxJobs, maxBytes: session.config.outbox.maxBytes, now, notifyFull: () => { outboxAdmissionFull = true; } });
            try {
                const effectiveSession = Object.freeze({ ...session, config: withRuntimeNodeId(session.config, outbox.nodeId) });
                const binding = capturePolicy(effectiveSession);
                producerBinding = binding;
                policy = binding?.policy;
                runtime = policy === undefined ? undefined : await input.runtimeFactory(effectiveSession, outbox, policy).catch(() => undefined);
                active = effectiveSession;
            }
            catch (error) {
                await outbox.closeProducer().catch(() => undefined);
                outbox = undefined;
                policy = undefined;
                producerBinding = undefined;
                runtime = undefined;
                active = undefined;
                throw error;
            }
        },
        async recover(producerPaths) {
            if (runtime === undefined || active === undefined)
                return;
            for (const path of producerPaths.slice(0, LIFECYCLE_RECOVERY_PRODUCERS)) {
                try {
                    await runtime.delivery.adopt(path);
                }
                catch { /* active, foreign, or unsafe producers stay untouched */ }
            }
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), Math.min(5_000, active.config.retrieval.timeoutMs));
            timer.unref?.();
            try {
                await runtime.delivery.deliver({ signal: controller.signal, maxJobs: Math.min(active.config.outbox.maxJobs, LIFECYCLE_DELIVERY_BATCH) });
            }
            catch { /* adopted jobs remain durable */ }
            finally {
                clearTimeout(timer);
            }
            const control = await runtime.store.readControl().catch(() => null);
            if (control === null || control.state !== "active")
                return Object.freeze([]);
            const ids = await allEpisodeIds(runtime, control);
            if (ids === undefined || ids.length === 0)
                return Object.freeze([]);
            return await episodesForIds(runtime, ids, control) ?? Object.freeze([]);
        },
        async capture(value) {
            if (active === undefined || outbox === undefined || active.sessionId !== value.sessionId)
                return Object.freeze([]);
            const currentPolicy = policy;
            if (currentPolicy === undefined)
                return Object.freeze([]);
            const control = runtime === undefined ? undefined : await runtime.store.readControl().catch(() => undefined);
            if (runtime !== undefined && (control === undefined || control.state !== "active"))
                return Object.freeze([]);
            let acceptedRecords;
            let acceptanceFailed = false;
            outboxAdmissionFull = false;
            const episodes = await capturePersistedEntries({
                sessionId: value.sessionId, lifecycle: value.lifecycle, getEntries: value.getEntries,
                activationDir: await (async () => {
                    const path = await persistCaptureActivationFile({ host: value.host, sessionId: value.sessionId, getEntries: value.getEntries, env: input.env, homeDir: input.homeDir, now });
                    return path;
                })(),
                host: value.host, homeDir: input.homeDir, projectId: value.project.id,
                projectIdentityKind: value.project.identityKind ?? "local_only",
                projectAllowlist: value.config.capture.projectAllowlist, projectDenylist: value.config.capture.projectDenylist,
                marker: canonicalCaptureMarker(value.host, value.marker),
                policyId: currentPolicy.id, privacyEpoch: control?.privacyEpoch ?? 0, expiresAt: null,
                originProvider: currentPolicy.originProvider,
                destinationId: currentPolicy.destinationIds.qdrant,
                nodeId: outbox.nodeId, producerId: outbox.producerUuid,
                toolArgsChars: value.config.capture.toolArgsChars, toolResultChars: value.config.capture.toolResultChars, now,
                acceptEpisodes: async (records) => {
                    try {
                        const bound = [];
                        for (const record of records) {
                            const eventAt = Date.parse(record.eventAt);
                            if (!Number.isFinite(eventAt))
                                throw new Error("capture event clock");
                            const binding = producerBinding;
                            if (binding === undefined)
                                throw new Error("capture policy");
                            const eventPolicy = capturePolicyForEvent(binding, value.config, eventAt).policy;
                            const pending = { ...record, processingPolicyId: eventPolicy.id, originProvider: eventPolicy.originProvider,
                                destinationId: eventPolicy.destinationIds.qdrant, expiresAt: eventPolicy.expiresAt,
                                ...(binding.modelId === undefined ? {} : { modelId: binding.modelId }), contentHash: "pending" };
                            const episode = Object.freeze({ ...pending, contentHash: canonicalRecordHash(pending) });
                            // One event-relative policy per immutable job keeps retention
                            // exact while every outbox admission remains below 1024 records.
                            await outbox.enqueue({ episodes: Object.freeze([episode]), policy: eventPolicy });
                            bound.push(episode);
                        }
                        acceptedRecords = Object.freeze(bound);
                    }
                    catch (error) {
                        acceptanceFailed = true;
                        throw error;
                    }
                },
            });
            if (acceptanceFailed || outboxAdmissionFull)
                throw new Error("capture acceptance unavailable");
            return acceptedRecords ?? (episodes.length === 0 ? episodes : Object.freeze([]));
        },
        async deliver(deliveryInput = {}) {
            await outbox?.heartbeat();
            if (runtime !== undefined)
                await runtime.delivery.deliver({ ...deliveryInput, maxJobs: Math.min(deliveryInput.maxJobs ?? LIFECYCLE_DELIVERY_BATCH, LIFECYCLE_DELIVERY_BATCH) });
        },
        async scheduleRoot(value) {
            const activeRuntime = runtime;
            const runtimeSession = active;
            if (activeRuntime === undefined || runtimeSession === undefined || runtimeSession.sessionId !== value.sessionId || !value.marker.rootWorkAllowed || value.episodes.length === 0 || value.episodes.length > 1024 || outbox === undefined)
                return false;
            const runtimeConfig = runtimeSession.config;
            const manager = value.ctx.sessionManager;
            const control = await activeRuntime.store.readControl().catch(() => null);
            if (control === null || control.state !== "active")
                return false;
            const inputMembership = Object.freeze(value.episodes.map((episode) => episode.id).sort());
            const delivered = await episodesForIds(activeRuntime, inputMembership, control);
            if (delivered === undefined)
                return false;
            const groups = new Map();
            for (const episode of delivered) {
                const group = groups.get(episode.processingPolicyId) ?? [];
                group.push(episode);
                groups.set(episode.processingPolicyId, group);
            }
            const completed = new Set();
            const durableGroups = [];
            for (const policyId of [...groups.keys()].sort()) {
                const stored = groups.get(policyId);
                const membership = Object.freeze(stored.map((episode) => episode.id).sort());
                const producerPolicies = await policiesForEpisodes(activeRuntime, stored, control);
                if (producerPolicies === undefined || producerPolicies.length !== 1 || producerPolicies[0].id !== policyId)
                    continue;
                const intersection = intersectPolicies(producerPolicies, activeRuntime.workerPolicy);
                if (intersection === null)
                    continue;
                const first = stored.find((episode) => episode.id === membership[0]);
                if (first === undefined)
                    continue;
                const job = await createJob(activeRuntime.store, { ownerHost: value.host, membership, policyIntersectionId: intersection.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "curation-v1", privacyEpoch: control.privacyEpoch, createdAt: first.createdAt, expiresAt: intersection.expiresAt }).catch(() => null);
                const afterJob = await activeRuntime.store.readControl().catch(() => null);
                if (job === null || afterJob?.contentHash !== control.contentHash || job.membership.length !== membership.length || job.membership.some((id, index) => id !== membership[index]) || job.policyId !== intersection.id || job.policyHash !== control.coordinationPolicyHash || job.policyEpoch !== control.coordinationPolicyEpoch || job.privacyEpoch !== control.privacyEpoch || job.createdAt !== first.createdAt || job.expiresAt !== intersection.expiresAt)
                    continue;
                durableGroups.push({ membership, producerPolicies, intersection });
                membership.forEach((id) => completed.add(id));
            }
            if (completed.size === 0)
                return false;
            // Durable work identity and exact readback always precede model registry
            // access. Registry/model failures leave the acknowledged jobs pending.
            if (value.reason === "shutdown")
                return Object.freeze({ completedEpisodeIds: Object.freeze([...completed].sort()) });
            let model;
            let llmDestination;
            let binding;
            try {
                model = selectedMemoryModel(value.ctx, runtimeConfig);
                llmDestination = model === undefined ? undefined : destinationForModel(model, runtimeConfig);
                binding = model === undefined || llmDestination === undefined ? undefined : Object.freeze({ providerId: model.provider, modelId: model.id, destinationId: llmDestination.id });
            }
            catch {
                return Object.freeze({ completedEpisodeIds: Object.freeze([]) });
            }
            if (model === undefined || llmDestination === undefined || binding === undefined)
                return Object.freeze({ completedEpisodeIds: Object.freeze([]) });
            for (const group of durableGroups) {
                if (group.intersection.destinationIds.llm !== llmDestination.id)
                    continue;
                await runCurationFromLifecycle(manager, {
                    host: value.host, store: activeRuntime.store, nodeId: `${outbox.nodeId}-lifecycle`, leaseMs: runtimeConfig.coordination.leaseMs,
                    maxClockSkewMs: runtimeConfig.coordination.maxClockSkewMs, clock: now, workerPolicy: activeRuntime.workerPolicy,
                    extractorRevision: "curation-v1", producerPolicies: group.producerPolicies, embedding: activeRuntime.embedding, membership: group.membership,
                    llm: { memoryModel: model, modelRegistry: value.ctx.modelRegistry, llmDestination, llmDestinationBinding: binding },
                    maxOutputTokens: runtimeConfig.memoryModel.maxOutputTokens, timeoutMs: runtimeConfig.memoryModel.timeoutMs, env: input.env,
                }).catch(() => undefined);
            }
            // RAPTOR is admitted only after the exact durable curation job/readback,
            // but its full-corpus discovery, clustering and model work never remains
            // on the awaited host lifecycle path. One background build per session
            // is abortable; failures leave the prior generation active and retryable.
            if (raptorTask === undefined) {
                const backgroundModel = model;
                const backgroundDestination = llmDestination;
                const backgroundBinding = binding;
                const backgroundRegistry = snapshotCompletionRegistry(value.ctx.modelRegistry);
                const backgroundNodeId = outbox.nodeId;
                if (backgroundRegistry === undefined)
                    return Object.freeze({ completedEpisodeIds: Object.freeze([...completed].sort()) });
                const controller = new AbortController();
                raptorController = controller;
                const task = (async () => {
                    // A macrotask boundary guarantees the awaited host handler can
                    // settle before any immediately-resolved store mocks or local I/O
                    // let background RAPTOR monopolize the microtask queue.
                    await new Promise((resolve) => setImmediate(resolve));
                    if (controller.signal.aborted)
                        return;
                    try {
                        const raptorControl = await activeRuntime.store.readControl().catch(() => null);
                        if (controller.signal.aborted || raptorControl === null || raptorControl.state !== "active" || raptorControl.contentHash !== control.contentHash)
                            return;
                        const previous = await activeRaptorLeafIds(activeRuntime, raptorControl);
                        if (controller.signal.aborted || previous === undefined)
                            return;
                        const fullMembership = await allEpisodeIds(activeRuntime, raptorControl);
                        if (controller.signal.aborted || fullMembership === undefined || fullMembership.length === 0)
                            return;
                        const previousSet = new Set(previous);
                        const delta = fullMembership.filter((id) => !previousSet.has(id));
                        if (delta.length < runtimeConfig.raptor.rebuildEpisodeDelta)
                            return;
                        const stored = await episodesForIds(activeRuntime, fullMembership, raptorControl);
                        if (controller.signal.aborted || stored === undefined)
                            return;
                        const fullPolicies = await policiesForEpisodes(activeRuntime, stored, raptorControl);
                        if (controller.signal.aborted || fullPolicies === undefined)
                            return;
                        const raptorPolicy = intersectPolicies(fullPolicies, activeRuntime.workerPolicy);
                        if (raptorPolicy === null || raptorPolicy.destinationIds.llm !== backgroundDestination.id)
                            return;
                        const policyById = new Map(fullPolicies.map((candidate) => [candidate.id, candidate]));
                        const leaves = stored.map((episode) => ({ id: episode.id, text: episodeSemanticProjection(episode), vector: episode.vector ?? [], tokens: Math.max(1, Math.ceil(episodeSemanticProjection(episode).length / 4)), projectId: episode.projectId, eventAt: episode.eventAt, policy: policyById.get(episode.processingPolicyId) }));
                        if (controller.signal.aborted || leaves.some((leaf) => leaf.policy === undefined || leaf.vector.length !== 1024))
                            return;
                        const reuseCandidates = await previousRaptorSummaries(activeRuntime, raptorControl);
                        if (controller.signal.aborted)
                            return;
                        await runRaptorFromLifecycle(manager, {
                            host: value.host, store: activeRuntime.store, env: input.env, nodeId: `${backgroundNodeId}-raptor`, leaseMs: runtimeConfig.coordination.leaseMs,
                            maxClockSkewMs: runtimeConfig.coordination.maxClockSkewMs, extractorRevision: "raptor-v1", clock: now,
                            workerPolicy: activeRuntime.workerPolicy, leaves, embedding: activeRuntime.embedding, signal: controller.signal,
                            llm: { destination: backgroundDestination, complete: async ({ envelope, signal }) => {
                                    const result = await completeMemory({ envelope, model: backgroundModel, hostContext: { messages: [] }, maxInputTokens: runtimeConfig.raptor.summaryInputTokens, maxOutputTokens: runtimeConfig.memoryModel.maxOutputTokens, timeoutMs: runtimeConfig.memoryModel.timeoutMs, ...(signal === undefined ? {} : { signal }), memoryContext: { host: value.host, modelRegistry: backgroundRegistry, memoryModel: backgroundModel, policy: raptorPolicy, llmDestination: backgroundDestination, llmDestinationBinding: backgroundBinding, allowCrossProviderReplay: raptorPolicy.allowCrossProviderReplay }, promptRevision: RAPTOR_PROMPT_REVISION });
                                    if (result.state !== "completed")
                                        throw new Error("RAPTOR completion pending");
                                    return result.text;
                                } },
                            modelId: backgroundModel.id, homeDir: input.homeDir, seed: runtimeConfig.raptor.seed ?? raptorControl.coordinationPolicyHash,
                            maxLevels: runtimeConfig.raptor.maxLevels, summaryInputTokens: runtimeConfig.raptor.summaryInputTokens, umapDimensions: runtimeConfig.raptor.umapDimensions,
                            localNeighbors: runtimeConfig.raptor.localNeighbors, gmmMaxClusters: runtimeConfig.raptor.gmmMaxClusters, membershipThreshold: runtimeConfig.raptor.membershipThreshold,
                            reuseCandidates,
                        });
                    }
                    catch { /* durable jobs and prior generation remain retryable */ }
                })();
                raptorTask = task;
                void task.finally(() => { if (raptorTask === task) {
                    raptorTask = undefined;
                    raptorController = undefined;
                } });
            }
            return Object.freeze({ completedEpisodeIds: Object.freeze([...completed].sort()) });
        },
        async drainAdminJobs(value) {
            const activeRuntime = runtime;
            const runtimeSession = active;
            if (activeRuntime === undefined || runtimeSession === undefined || runtimeSession.sessionId !== value.sessionId || !value.marker.rootWorkAllowed || raptorTask !== undefined)
                return;
            const control = await activeRuntime.store.readControl().catch(() => null);
            if (control === null || control.state !== "active")
                return;
            const jobs = [];
            const seen = new Set();
            const cursors = new Set();
            let offset;
            for (let page = 0; page < 16; page += 1) {
                const slice = await activeRuntime.store.scrollJobs(offset, 256).catch(() => undefined);
                if (slice === undefined)
                    return;
                if (slice.jobs.some(job => seen.has(job.id)))
                    return;
                for (const job of slice.jobs) {
                    seen.add(job.id);
                    if (job.ownerHost === value.host && (job.extractorRevision === "curation-v1" || job.extractorRevision === "admin-raptor-v1"))
                        jobs.push(job);
                }
                if (slice.nextOffset === undefined)
                    break;
                if (slice.jobs.length === 0 || cursors.has(slice.nextOffset))
                    return;
                cursors.add(slice.nextOffset);
                offset = slice.nextOffset;
            }
            if (jobs.length === 0)
                return;
            // Queue records are durable before this model-registry lookup. A missing
            // model or destination leaves them claimable and retryable.
            const runtimeConfig = runtimeSession.config;
            const manager = value.ctx.sessionManager;
            let model;
            let llmDestination;
            let binding;
            try {
                model = selectedMemoryModel(value.ctx, runtimeConfig);
                llmDestination = model === undefined ? undefined : destinationForModel(model, runtimeConfig);
                binding = model === undefined || llmDestination === undefined ? undefined : Object.freeze({ providerId: model.provider, modelId: model.id, destinationId: llmDestination.id });
            }
            catch {
                return;
            }
            if (model === undefined || llmDestination === undefined || binding === undefined)
                return;
            const ordered = jobs.sort((left, right) => left.id.localeCompare(right.id)).slice(0, 16);
            for (const job of ordered) {
                const lease = await activeRuntime.store.readLease(job.id).catch(() => undefined);
                if (lease === undefined || lease?.state === "completed")
                    continue;
                const stored = await episodesForIds(activeRuntime, job.membership, control);
                if (stored === undefined)
                    continue;
                const producerPolicies = await policiesForEpisodes(activeRuntime, stored, control);
                if (producerPolicies === undefined)
                    continue;
                const intersection = intersectPolicies(producerPolicies, activeRuntime.workerPolicy);
                // A human process cannot mint a worker intersection. Only an exact
                // source/worker policy match can claim the immutable queued identity.
                if (intersection === null || intersection.id !== job.policyId || intersection.destinationIds.llm !== llmDestination.id)
                    continue;
                if (job.extractorRevision === "curation-v1") {
                    await runCurationFromLifecycle(manager, {
                        host: value.host, store: activeRuntime.store, nodeId: `${outbox?.nodeId ?? "admin"}-queued-curation`, leaseMs: runtimeConfig.coordination.leaseMs,
                        maxClockSkewMs: runtimeConfig.coordination.maxClockSkewMs, clock: now, workerPolicy: activeRuntime.workerPolicy,
                        extractorRevision: job.extractorRevision, producerPolicies, embedding: activeRuntime.embedding, membership: job.membership,
                        llm: { memoryModel: model, modelRegistry: value.ctx.modelRegistry, llmDestination, llmDestinationBinding: binding },
                        maxOutputTokens: runtimeConfig.memoryModel.maxOutputTokens, timeoutMs: runtimeConfig.memoryModel.timeoutMs, env: input.env,
                    }).catch(() => undefined);
                    continue;
                }
                if (job.membership.length !== stored.length || stored.some((episode, index) => episode.id !== job.membership[index]))
                    continue;
                const leaves = stored.map(episode => ({ id: episode.id, text: episodeSemanticProjection(episode), vector: episode.vector ?? [], tokens: Math.max(1, Math.ceil(episodeSemanticProjection(episode).length / 4)), projectId: episode.projectId, eventAt: episode.eventAt, policy: producerPolicies.find(policy => policy.id === episode.processingPolicyId) }));
                if (leaves.some(leaf => leaf.policy === undefined || leaf.vector.length !== 1024))
                    continue;
                const reuseCandidates = await previousRaptorSummaries(activeRuntime, control);
                if (reuseCandidates === undefined)
                    continue;
                const registry = snapshotCompletionRegistry(value.ctx.modelRegistry);
                if (registry === undefined)
                    continue;
                const controller = new AbortController();
                await runRaptorFromLifecycle(manager, {
                    host: value.host, store: activeRuntime.store, env: input.env, nodeId: `${outbox?.nodeId ?? "admin"}-admin-raptor`, leaseMs: runtimeConfig.coordination.leaseMs,
                    maxClockSkewMs: runtimeConfig.coordination.maxClockSkewMs, extractorRevision: job.extractorRevision, jobId: job.id, clock: now,
                    workerPolicy: activeRuntime.workerPolicy, leaves, embedding: activeRuntime.embedding, signal: controller.signal,
                    llm: { destination: llmDestination, complete: async ({ envelope, signal }) => {
                            const result = await completeMemory({ envelope, model, hostContext: { messages: [] }, maxInputTokens: runtimeConfig.raptor.summaryInputTokens, maxOutputTokens: runtimeConfig.memoryModel.maxOutputTokens, timeoutMs: runtimeConfig.memoryModel.timeoutMs, ...(signal === undefined ? {} : { signal }), memoryContext: { host: value.host, modelRegistry: registry, memoryModel: model, policy: intersection, llmDestination, llmDestinationBinding: binding, allowCrossProviderReplay: intersection.allowCrossProviderReplay }, promptRevision: RAPTOR_PROMPT_REVISION });
                            if (result.state !== "completed")
                                throw new Error("RAPTOR completion pending");
                            return result.text;
                        } },
                    modelId: model.id, homeDir: input.homeDir, seed: runtimeConfig.raptor.seed ?? control.coordinationPolicyHash,
                    maxLevels: runtimeConfig.raptor.maxLevels, summaryInputTokens: runtimeConfig.raptor.summaryInputTokens, umapDimensions: runtimeConfig.raptor.umapDimensions,
                    localNeighbors: runtimeConfig.raptor.localNeighbors, gmmMaxClusters: runtimeConfig.raptor.gmmMaxClusters, membershipThreshold: runtimeConfig.raptor.membershipThreshold,
                    reuseCandidates,
                }).catch(() => undefined);
            }
        },
        async shutdown(shutdownInput) {
            raptorController?.abort();
            await outbox?.heartbeat().catch(() => undefined);
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), Math.min(5_000, shutdownInput.config.memoryModel.timeoutMs));
            timer.unref?.();
            try {
                await runtime?.delivery.shutdown({ signal: controller.signal, maxJobs: shutdownInput.config.outbox.maxJobs });
            }
            catch { /* pending files remain durable */ }
            finally {
                clearTimeout(timer);
                await outbox?.closeProducer().catch(() => undefined);
            }
        },
        clear() { raptorController?.abort(); raptorController = undefined; raptorTask = undefined; active = undefined; outbox = undefined; runtime = undefined; policy = undefined; producerBinding = undefined; outboxAdmissionFull = false; },
    };
    return Object.freeze(coordinator);
}
/** Build a testable factory while keeping the default export host-portable. */
export function createMemoryExtension(dependencies = {}) {
    return async (pi) => {
        const env = dependencies.env ?? process.env;
        const argv = dependencies.argv ?? process.argv;
        const warned = new Set();
        const warnOnce = (warning, ctx, dedupeKey = warning.category) => {
            if (warned.has(dedupeKey))
                return;
            warned.add(dedupeKey);
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
        const lifecycleInput = { homeDir: dependencies.homeDir ?? homedir(), env, ...(dependencies.now === undefined ? {} : { now: dependencies.now }) };
        let lifecycle = dependencies.lifecycleCoordinator ?? (dependencies.lifecycleCoordinatorFactory === undefined
            ? createProductionLifecycleCoordinatorInternal({ ...lifecycleInput, runtimeFactory: (session, outbox, policy) => createProductionRuntime(session, outbox, policy, dependencies) })
            : dependencies.lifecycleCoordinatorFactory(lifecycleInput));
        let sessionState;
        let captureEnabled = false;
        let projectResolver = dependencies.projectResolver ?? resolveProjectIdentity;
        let rootTurns = 0;
        let rootToolCalls = 0;
        let pendingRootEpisodes = [];
        const appendPendingRootEpisodes = (episodes) => {
            pendingRootEpisodes = [...new Map([...pendingRootEpisodes, ...episodes].map((episode) => [episode.id, episode])).values()].slice(0, RAPTOR_MAX_LEAVES);
        };
        const nextRootBatch = () => {
            const policies = new Set();
            const batch = [];
            for (const episode of pendingRootEpisodes) {
                if (!policies.has(episode.processingPolicyId) && policies.size >= 64)
                    break;
                policies.add(episode.processingPolicyId);
                batch.push(episode);
                if (batch.length >= 1024)
                    break;
            }
            return Object.freeze(batch);
        };
        const drainRootBatches = async (ctx, reason) => {
            if (sessionState === undefined || host === undefined || config === undefined)
                return false;
            const maxAttempts = reason === "shutdown" ? 1 : LIFECYCLE_RECOVERY_PRODUCERS;
            for (let attempt = 0; attempt < maxAttempts && pendingRootEpisodes.length > 0; attempt += 1) {
                const batch = nextRootBatch();
                if (batch.length === 0)
                    return false;
                const scheduled = await lifecycle.scheduleRoot({ host, config, sessionId: sessionState.sessionId, cwd: ctx.cwd, project: sessionState.project, marker: sessionState.marker, getEntries: sessionEntries(ctx), ctx, episodes: batch, reason });
                if (scheduled === false)
                    return false;
                const requested = new Set(batch.map((episode) => episode.id));
                const completedIds = typeof scheduled === "object" && scheduled !== null && Array.isArray(scheduled.completedEpisodeIds) ? scheduled.completedEpisodeIds : batch.map((episode) => episode.id);
                if (completedIds.length === 0 || completedIds.some((id) => !requested.has(id)))
                    return false;
                const completed = new Set(completedIds);
                pendingRootEpisodes = pendingRootEpisodes.filter((episode) => !completed.has(episode.id));
            }
            return pendingRootEpisodes.length === 0;
        };
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
                projectResolver = dependencies.projectResolver ?? ((cwd) => resolveProjectIdentity(cwd, { homeDir: configDependencies.homeDir, registrations: config.projects.registrations, ...(configDependencies.xdgConfigHome === undefined ? {} : { xdgConfigHome: configDependencies.xdgConfigHome }) }));
                if (config.enabled) {
                    const resolvedNodeId = config.privacy.egressMode === "local_only" && config.outbox.nodeId === undefined
                        ? await resolveOutboxNodeId({ host, homeDir: configDependencies.homeDir, env, sharedFilesystem: config.outbox.sharedFilesystem })
                        : config.outbox.nodeId;
                    const activeConfig = resolvedNodeId === undefined ? config : withRuntimeNodeId(config, resolvedNodeId);
                    const embeddings = new EmbeddingsClient(clientOptions({
                        baseUrl: activeConfig.embeddings.baseUrl,
                        model: activeConfig.embeddings.model,
                        dimension: activeConfig.embeddings.dimension,
                        queryPrefix: activeConfig.embeddings.queryPrefix,
                        timeoutMs: activeConfig.retrieval.timeoutMs,
                    }, activeConfig.embeddings.apiKey, dependencies.fetchImpl));
                    let validatedEmbeddings;
                    try {
                        validatedEmbeddings = bindEmbeddingDocumentClient({ endpoint: activeConfig.embeddings.baseUrl, client: embeddings });
                    }
                    catch {
                        validatedEmbeddings = undefined;
                    }
                    const qdrantDestination = configuredQdrantDestination(activeConfig);
                    if (qdrantDestination === undefined)
                        throw new MemoryClientError("configuration", "Qdrant destination binding is unavailable");
                    const qdrant = createGuardedMemoryReadStore(clientOptions({
                        baseUrl: activeConfig.qdrant.url,
                        collection: activeConfig.qdrant.collection,
                        ownerHost: host,
                        timeoutMs: activeConfig.retrieval.timeoutMs,
                        readConsistency: activeConfig.coordination.readConsistency,
                        maxClockSkewMs: activeConfig.coordination.maxClockSkewMs,
                        destination: qdrantDestination,
                        egressMode: activeConfig.privacy.egressMode,
                        ...(activeConfig.outbox.nodeId === undefined ? {} : { nodeId: activeConfig.outbox.nodeId }),
                    }, activeConfig.qdrant.apiKey, dependencies.fetchImpl));
                    const resolveEmbedding = async (control) => {
                        const destination = configuredEmbeddingDestination(activeConfig);
                        if (destination === undefined || validatedEmbeddings === undefined || control.ownerHost !== host || control.state !== "active" || control.revokedDestinationIds.includes(destination.id))
                            return undefined;
                        const factory = createEmbeddingDestinationFactory({ endpoint: activeConfig.embeddings.baseUrl, destination, client: validatedEmbeddings, egressMode: activeConfig.privacy.egressMode, ...(activeConfig.outbox.nodeId === undefined ? {} : { nodeId: activeConfig.outbox.nodeId }), coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch });
                        return Object.freeze({ embedding: bindEmbeddingDestination(factory, destination), destination });
                    };
                    const retriever = new MemoryRetriever({ reader: qdrant, config: activeConfig.retrieval, resolveEmbedding, queryPrefix: activeConfig.embeddings.queryPrefix, maxClockSkewMs: activeConfig.coordination.maxClockSkewMs, ...(dependencies.now === undefined ? {} : { now: dependencies.now }) });
                    const cacheOptions = {
                        maxEntries: CACHE_MAX_ENTRIES,
                        ttlMs: CACHE_TTL_MS,
                    };
                    if (dependencies.now !== undefined)
                        cacheOptions.now = dependencies.now;
                    service = new MemoryService({
                        host,
                        config: activeConfig,
                        retriever,
                        projectResolver,
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
        const captureFor = async (lifecycleName, ctx) => {
            if (!captureEnabled || host === undefined || config === undefined || sessionState === undefined)
                return Object.freeze([]);
            let marker;
            try {
                marker = resolveHostAgentMarker(host, ctx.sessionManager.getHeader(), env);
                const project = await projectResolver(ctx.cwd);
                if (!sameProjectIdentity(sessionState.project, project))
                    throw new Error("project identity");
                sessionState = { ...sessionState, project, marker };
            }
            catch {
                sessionState = { ...sessionState, marker: Object.freeze({ role: "child", depth: 1, valid: false, rootWorkAllowed: false }) };
                warnOnce(lifecycleWarning("capture"), ctx, "lifecycle:capture");
                return Object.freeze([]);
            }
            try {
                const episodes = await lifecycle.capture({
                    host, config, sessionId: sessionState.sessionId, cwd: ctx.cwd,
                    project: sessionState.project, marker, getEntries: sessionEntries(ctx), ctx,
                    lifecycle: lifecycleName,
                });
                return Object.freeze([...episodes]);
            }
            catch {
                // Downstream admission can recover after bounded delivery; do not
                // rewrite a genuine root marker into a permanent invalid child.
                warnOnce(lifecycleWarning("capture"), ctx, "lifecycle:capture");
                return Object.freeze([]);
            }
        };
        pi.on("session_start", async (_event, ctx) => {
            try {
                service?.clear();
            }
            catch { /* cache reset is best effort */ }
            try {
                lifecycle.clear();
            }
            catch {
                warnOnce(lifecycleWarning("start"), ctx, "lifecycle:start");
            }
            sessionState = undefined;
            rootTurns = 0;
            rootToolCalls = 0;
            pendingRootEpisodes = [];
            if (disabledWarning !== undefined)
                warnOnce(disabledWarning, ctx);
            await service?.checkHealth(ctx);
            if (host === undefined || config === undefined || !config.capture.enabled)
                return;
            try {
                const sessionId = safeSessionId(ctx);
                if (sessionId === undefined)
                    throw new Error("session");
                const project = await projectResolver(ctx.cwd);
                if (!validProjectIdentity(project))
                    throw new Error("project identity");
                const marker = resolveHostAgentMarker(host, ctx.sessionManager.getHeader(), env);
                const state = { sessionId, project, marker };
                const recovery = await closedProducerPaths(host, dependencies.homeDir ?? homedir(), env, (dependencies.now ?? Date.now)(), config.coordination.maxClockSkewMs);
                await lifecycle.start({ host, config, sessionId, cwd: ctx.cwd, project, marker, getEntries: sessionEntries(ctx), ctx });
                const recovered = await lifecycle.recover?.(recovery);
                sessionState = state;
                captureEnabled = true;
                if (marker.rootWorkAllowed && Array.isArray(recovered) && recovered.length > 0) {
                    appendPendingRootEpisodes(recovered);
                    try {
                        await drainRootBatches(ctx, "recovery");
                    }
                    catch {
                        warnOnce(lifecycleWarning("root"), ctx, "lifecycle:root");
                    }
                }
                if (marker.rootWorkAllowed) {
                    try {
                        await lifecycle.drainAdminJobs?.({ host, config, sessionId, cwd: ctx.cwd, project, marker, getEntries: sessionEntries(ctx), ctx });
                    }
                    catch {
                        warnOnce(lifecycleWarning("root"), ctx, "lifecycle:admin");
                    }
                }
            }
            catch {
                captureEnabled = false;
                sessionState = undefined;
                warnOnce(lifecycleWarning("start"), ctx, "lifecycle:start");
            }
        });
        pi.on("agent_end", async (_event, ctx) => {
            const episodes = await captureFor("agent_end", ctx);
            if (sessionState === undefined || host === undefined || config === undefined)
                return;
            try {
                await lifecycle.deliver({ ...(ctx.signal === undefined ? {} : { signal: ctx.signal }) });
            }
            catch {
                warnOnce(lifecycleWarning("delivery"), ctx, "lifecycle:delivery");
            }
            if (!sessionState.marker.rootWorkAllowed)
                return;
            if (episodes.length > 0)
                appendPendingRootEpisodes(episodes);
            rootTurns += 1;
            rootToolCalls += episodes.filter((episode) => episode.eventKind === "tool_call").length;
            const rootAttempted = pendingRootEpisodes.length > 0 && (rootTurns >= config.curation.turnTrigger || rootToolCalls >= config.curation.toolTrigger);
            if (rootAttempted) {
                try {
                    if (await drainRootBatches(ctx, "threshold")) {
                        rootTurns = 0;
                        rootToolCalls = 0;
                    }
                }
                catch {
                    warnOnce(lifecycleWarning("root"), ctx, "lifecycle:root");
                }
            }
            else {
                try {
                    await lifecycle.drainAdminJobs?.({ host, config, sessionId: sessionState.sessionId, cwd: ctx.cwd, project: sessionState.project, marker: sessionState.marker, getEntries: sessionEntries(ctx), ctx });
                }
                catch {
                    warnOnce(lifecycleWarning("root"), ctx, "lifecycle:admin");
                }
            }
        });
        pi.on("session_before_compact", async (_event, ctx) => {
            const episodes = await captureFor("session_before_compact", ctx);
            if (sessionState === undefined || host === undefined || config === undefined)
                return;
            try {
                await lifecycle.deliver({ ...(ctx.signal === undefined ? {} : { signal: ctx.signal }) });
            }
            catch {
                warnOnce(lifecycleWarning("delivery"), ctx, "lifecycle:delivery");
            }
            if (!sessionState.marker.rootWorkAllowed)
                return;
            if (episodes.length > 0)
                appendPendingRootEpisodes(episodes);
            const rootAttempted = pendingRootEpisodes.length > 0;
            try {
                if (await drainRootBatches(ctx, "compact")) {
                    rootTurns = 0;
                    rootToolCalls = 0;
                }
            }
            catch {
                warnOnce(lifecycleWarning("root"), ctx, "lifecycle:root");
            }
            if (!rootAttempted) {
                try {
                    await lifecycle.drainAdminJobs?.({ host, config, sessionId: sessionState.sessionId, cwd: ctx.cwd, project: sessionState.project, marker: sessionState.marker, getEntries: sessionEntries(ctx), ctx });
                }
                catch {
                    warnOnce(lifecycleWarning("root"), ctx, "lifecycle:admin");
                }
            }
        });
        pi.on("session_shutdown", async (_event, ctx) => {
            try {
                await captureFor("session_shutdown", ctx);
                if (captureEnabled && sessionState !== undefined && host !== undefined && config !== undefined) {
                    // The coordinator owns the sole bounded final flush. Root work is
                    // recovered from durable outbox/Qdrant episodes on the next start;
                    // shutdown never enters registry, curation, or RAPTOR paths.
                    try {
                        await lifecycle.shutdown({ host, config, sessionId: sessionState.sessionId, cwd: ctx.cwd, project: sessionState.project, marker: sessionState.marker, getEntries: sessionEntries(ctx), ctx, lifecycle: "session_shutdown" });
                    }
                    catch {
                        warnOnce(lifecycleWarning("shutdown"), ctx, "lifecycle:shutdown");
                    }
                }
            }
            finally {
                captureEnabled = false;
                sessionState = undefined;
                rootTurns = 0;
                rootToolCalls = 0;
                pendingRootEpisodes = [];
                try {
                    service?.clear();
                }
                catch { /* cache reset is best effort */ }
                try {
                    lifecycle.clear();
                }
                catch {
                    warnOnce(lifecycleWarning("shutdown"), ctx, "lifecycle:shutdown");
                }
            }
        });
    };
}
const extension = createMemoryExtension();
export default extension;
//# sourceMappingURL=extension.js.map