import { MemoryClientError } from "../clients/http.js";
import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { parseMemoryRecord } from "../domain/records.js";
import { QdrantAdminClient, readPolicy } from "../qdrant/client.js";
import { COLLECTION_CONTROL_ID, COLLECTION_METADATA_ID, REQUIRED_INDEXES, V2_COLLECTION_METADATA, V2_CONTRACT_HASH, assertBootstrapControl, isBootstrapControlPayload, isCollectionMetadataPayload, isValidBootstrapControlPayload } from "../qdrant/schema.js";
function defaultControl(config, now) { const instant = new Date(now()); if (!Number.isFinite(instant.getTime()))
    throw new TypeError("Initialization clock is invalid"); const base = { ownerHost: config.host, schemaRevision: 1, createdAt: instant.toISOString(), privacyEpoch: 0, processingPolicyId: V2_CONTRACT_HASH, expiresAt: null, recordType: "collection_control", id: COLLECTION_CONTROL_ID, version: 0, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 0, coordinationPolicyHash: V2_CONTRACT_HASH, state: "active", scanCursor: null, lastForgetBarrier: null, contentHash: "pending" }; const copy = { ...base }; delete copy.contentHash; delete copy.createdAt; return { ...base, contentHash: sha256Hex(canonicalStringify(copy)) }; }
function validateReplicaConfig(config) { const replication = config.qdrant.replicationFactor; const consistency = config.qdrant.writeConsistencyFactor; if (!Number.isSafeInteger(replication) || !Number.isSafeInteger(consistency) || replication < 1 || consistency < 1 || consistency > replication)
    throw new TypeError("Qdrant replica configuration is invalid"); if (replication === 1 && consistency !== 1) {
    if (consistency !== 1)
        throw new TypeError("Single-node Qdrant requires write consistency 1/1");
}
else if (consistency < Math.ceil((replication + 1) / 2))
    throw new TypeError("Qdrant write consistency is below the cluster majority"); }
function notFound(error) { return error instanceof MemoryClientError && error.category === "http" && error.status === 404; }
function conflict(error) { return error instanceof MemoryClientError && error.category === "http" && error.status === 409; }
function result(config, initialized, created, version) { return { host: config.host, collection: config.qdrant.collection, ownerHost: config.host, schema: "pi-qdrant-memory-v2", schemaRevision: 1, vector: { name: "semantic", model: "bge-m3", dimension: 1024, distance: "Cosine" }, capture: { enabled: config.capture.enabled, episodeRetentionDays: config.capture.episodeRetentionDays }, initialized, collectionCreated: created, ...(version === undefined ? {} : { qdrantVersion: version }) }; }
function validateIndexes(schema) { if (schema === undefined)
    throw new Error("Qdrant payload index schema is missing"); for (const [field, expected] of REQUIRED_INDEXES) {
    const value = schema[field];
    if (value === undefined || typeof value !== "object" || value === null || Array.isArray(value) || value.data_type !== expected)
        throw new Error(`Qdrant payload index mismatch: ${field}`);
} }
function metadataPolicy(config, now) { return readPolicy({ ownerHost: config.host, purpose: "metadata", recordTypes: ["collection_metadata"], now, maxClockSkewMs: config.coordination.maxClockSkewMs }); }
function controlPolicy(config, now) { return readPolicy({ ownerHost: config.host, purpose: "control", recordTypes: ["collection_control"], now, maxClockSkewMs: config.coordination.maxClockSkewMs }); }
async function delay(ms) { if (ms > 0)
    await new Promise((resolve) => setTimeout(resolve, ms)); }
/** Destination initialization never consults ambient process credentials. */
export async function initializeDestination(config, deps = {}) {
    if (deps.fetchImpl === undefined && deps.adminApiKey === undefined)
        return result(config, false, false);
    validateReplicaConfig(config);
    if (deps.adminApiKey === undefined || deps.adminApiKey.trim() === "")
        throw new TypeError("Human Qdrant admin key is required");
    const admin = new QdrantAdminClient({ baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, apiKey: deps.adminApiKey, timeoutMs: config.retrieval.timeoutMs, ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }), ...(deps.signal === undefined ? {} : { signal: deps.signal }), readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs, replicationFactor: config.qdrant.replicationFactor, writeConsistencyFactor: config.qdrant.writeConsistencyFactor });
    const now = deps.now ?? (() => Date.now());
    const control = deps.initialControl ?? defaultControl(config, now);
    assertBootstrapControl(control, config.host);
    const qdrantVersion = await admin.serverInfo();
    let created = false;
    let concurrentWinner = false;
    try {
        await admin.collectionInfo();
    }
    catch (error) {
        if (!notFound(error))
            throw error;
        try {
            await admin.createCollection();
            created = true;
            await admin.collectionInfo();
        }
        catch (createError) {
            if (!conflict(createError))
                throw createError;
            concurrentWinner = true;
            const attempts = Math.max(1, Math.min(5, deps.retryAttempts ?? 3));
            let reread = false;
            for (let attempt = 0; attempt < attempts; attempt += 1) {
                try {
                    await admin.collectionInfo();
                    reread = true;
                    break;
                }
                catch (error) {
                    if (!notFound(error))
                        throw error;
                    await delay(deps.retryDelayMs ?? 0);
                }
            }
            if (!reread)
                throw new Error("Concurrent collection creation did not become readable");
        }
    }
    const metadataPolicyValue = metadataPolicy(config, now());
    let metadata = await admin.retrieve([COLLECTION_METADATA_ID], metadataPolicyValue);
    if (concurrentWinner && metadata.length === 0) {
        const attempts = Math.max(1, Math.min(5, deps.retryAttempts ?? 3));
        for (let attempt = 1; attempt < attempts && metadata.length === 0; attempt += 1) {
            await delay(deps.retryDelayMs ?? 0);
            metadata = await admin.retrieve([COLLECTION_METADATA_ID], metadataPolicy(config, now()));
        }
    }
    if (created) {
        if (metadata.length !== 0)
            throw new Error("New collection unexpectedly contains metadata before initialization");
        await admin.insertMetadataPoint(config.host);
        const metadataReadback = await admin.retrieve([COLLECTION_METADATA_ID], metadataPolicy(config, now()));
        if (metadataReadback.length !== 1 || !isCollectionMetadataPayload(metadataReadback[0].payload, config.host))
            throw new Error("Qdrant collection metadata contract mismatch");
    }
    else {
        if (metadata.length !== 1 || !isCollectionMetadataPayload(metadata[0].payload, config.host))
            throw new Error("Pre-existing collection metadata is missing or foreign");
        let existingControl = await admin.retrieve([COLLECTION_CONTROL_ID], controlPolicy(config, now()));
        if (concurrentWinner && existingControl.length === 0) {
            const attempts = Math.max(1, Math.min(5, deps.retryAttempts ?? 3));
            for (let attempt = 1; attempt < attempts && existingControl.length === 0; attempt += 1) {
                await delay(deps.retryDelayMs ?? 0);
                existingControl = await admin.retrieve([COLLECTION_CONTROL_ID], controlPolicy(config, now()));
            }
        }
        if (existingControl.length !== 1 || !isValidBootstrapControlPayload(existingControl[0].payload, config.host))
            throw new Error("Pre-existing collection control is missing or invalid");
    }
    const info = await admin.collectionInfo();
    if (info.dimension !== V2_COLLECTION_METADATA.embedding_dimension || info.distance !== V2_COLLECTION_METADATA.distance)
        throw new Error("Qdrant collection vector contract mismatch");
    for (const [field, schema] of REQUIRED_INDEXES)
        await admin.createPayloadIndex(field, schema);
    validateIndexes((await admin.collectionInfo()).payloadSchema);
    if (created) {
        await admin.insertInitialControlPoint(control);
        const readback = await admin.retrieve([COLLECTION_CONTROL_ID], controlPolicy(config, now()));
        if (readback.length !== 1 || !isBootstrapControlPayload(readback[0].payload, control, config.host))
            throw new Error("Bootstrap control readback mismatch");
    }
    return result(config, true, created, qdrantVersion.version);
}
//# sourceMappingURL=init.js.map