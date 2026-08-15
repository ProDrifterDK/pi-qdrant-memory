import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { readPolicy } from "../qdrant/client.js";
import { statusScroll } from "./transport.js";
const MAX_RECORDS = 256;
const MAX_ID = 512;
const SECRET = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;
const HIDDEN_KEYS = /(?:^|_)(?:text|content|payload|vector|tool|args|result|message|prompt|completion|secret|credential|token|password|authorization|api[-_]?key)(?:$|_)/iu;
const ALLOWED_KEYS = new Set(["id", "recordType", "ownerHost", "host", "projectId", "status", "createdAt", "eventAt", "expiresAt", "privacyEpoch", "processingPolicyId", "coordinationPolicyEpoch", "coordinationPolicyHash", "contentHash", "schemaRevision", "version", "state", "scope", "resolution", "generationId", "level", "clusterId", "membershipHash", "jobId", "policyId", "policyEpoch", "extractorRevision", "episodeId", "observationId", "contentId", "stateKey", "targetId", "manifestHash", "redactionStatus", "secretScan"]);
function safeId(value) {
    if (typeof value !== "string" || value.length === 0 || value.length > MAX_ID || SECRET.test(value) || /[\u0000-\u001f\u007f-\u009f]/u.test(value))
        throw new TypeError("Inspect ID is invalid or not redacted");
    return value;
}
function boundedLimit(value) { const limit = value ?? 64; if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_RECORDS)
    throw new TypeError("Inspect limit is invalid"); return limit; }
function safeScalar(value) {
    if (value === null || typeof value === "boolean" || typeof value === "number" && Number.isFinite(value))
        return value;
    if (typeof value === "string" && value.length <= MAX_ID && !SECRET.test(value) && !/[\u0000-\u001f\u007f-\u009f]/u.test(value))
        return value;
    return undefined;
}
/** Project metadata only; raw text, vectors, payloads, credentials and tool
 * material never cross the operator inspection boundary. */
export function redactInspectRecord(value) {
    if (typeof value !== "object" || value === null || Array.isArray(value))
        throw new TypeError("Inspect record is invalid");
    const source = value;
    const output = {};
    for (const key of Object.keys(source).sort()) {
        if (!ALLOWED_KEYS.has(key) || HIDDEN_KEYS.test(key))
            continue;
        const scalar = safeScalar(source[key]);
        if (scalar !== undefined)
            output[key] = scalar;
    }
    if (typeof output.id === "string")
        safeId(output.id);
    if (typeof output.contentHash === "string" && !/^[a-f0-9]{64}$/u.test(output.contentHash))
        delete output.contentHash;
    return Object.freeze(output);
}
/** Bounded deterministic operator inspection. */
export async function inspectRecords(options = {}, source = {}) {
    const limit = boundedLimit(options.limit);
    const ids = options.ids?.map((value) => safeId(value));
    const recordTypes = options.recordTypes?.map((value) => safeId(value));
    const query = { limit, ...(ids === undefined ? {} : { ids: [...new Set(ids)] }), ...(recordTypes === undefined ? {} : { recordTypes: [...new Set(recordTypes)] }) };
    const raw = source.read === undefined ? source.records ?? [] : await source.read(query);
    if (!Array.isArray(raw))
        throw new Error("Inspect source returned invalid records");
    const records = [];
    for (const value of raw) {
        if (records.length >= limit)
            break;
        let projected;
        try {
            projected = redactInspectRecord(value);
        }
        catch {
            continue;
        }
        if (ids !== undefined && (projected.id === undefined || !ids.includes(projected.id)))
            continue;
        if (recordTypes !== undefined && (projected.recordType === undefined || !recordTypes.includes(projected.recordType)))
            continue;
        records.push(projected);
    }
    records.sort((left, right) => canonicalStringify(left).localeCompare(canonicalStringify(right)));
    const truncated = raw.length > records.length || raw.length > limit;
    const contentHash = sha256Hex(canonicalStringify(records));
    return Object.freeze({ ok: true, records: Object.freeze(records), count: records.length, truncated, contentHash });
}
const WIRE_FIELDS = {
    record_type: "recordType", owner_host: "ownerHost", project_id: "projectId", created_at: "createdAt", event_at: "eventAt", expires_at: "expiresAt", privacy_epoch: "privacyEpoch", processing_policy_id: "processingPolicyId", coordination_policy_epoch: "coordinationPolicyEpoch", coordination_policy_hash: "coordinationPolicyHash", content_hash: "contentHash", schema_revision: "schemaRevision", record_id: "id", id: "id", status: "status", secret_scan: "secretScan", redaction_status: "redactionStatus", version: "version", state: "state", scope: "scope", resolution: "resolution", generation_id: "generationId", level: "level", cluster_id: "clusterId", membership_hash: "membershipHash", job_id: "jobId", policy_id: "policyId", policy_epoch: "policyEpoch", extractor_revision: "extractorRevision", episode_id: "episodeId", observation_id: "observationId", content_id: "contentId", state_key: "stateKey", target_id: "targetId", manifest_hash: "manifestHash"
};
const INSPECT_TYPES = ["episode", "curated_memory", "curated_current", "raptor_summary", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone"];
function wireMetadata(id, payload) {
    const output = { id };
    for (const [key, target] of Object.entries(WIRE_FIELDS)) {
        const value = payload[key];
        if (value !== undefined)
            output[target] = value;
    }
    return output;
}
/** Bounded production read path. It uses the named read-only transport and
 * projects wire payloads before the generic allowlist redactor sees them. */
export async function inspectQdrantRecords(config, options = {}, fetchImpl = globalThis.fetch) {
    const limit = boundedLimit(options.limit);
    const requestedIds = options.ids?.map((value) => safeId(value));
    const requestedTypes = options.recordTypes === undefined ? [...INSPECT_TYPES] : options.recordTypes.map((value) => safeId(value));
    if (requestedTypes.length === 0 || requestedTypes.length > INSPECT_TYPES.length || requestedTypes.some((value) => !INSPECT_TYPES.includes(value)))
        throw new TypeError("Inspect record types are invalid");
    const internal = requestedTypes.some((value) => ["job", "lease", "proposal", "coverage", "evidence_link", "tombstone"].includes(value));
    const policy = readPolicy({ ownerHost: config.host, purpose: internal ? "internal" : "query", recordTypes: requestedTypes, maxClockSkewMs: config.coordination.maxClockSkewMs });
    const clientOptions = { baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, ...(config.qdrant.apiKey === undefined ? {} : { apiKey: config.qdrant.apiKey }), timeoutMs: config.retrieval.timeoutMs, readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs };
    const rows = [];
    let offset;
    for (let page = 0; page < 4 && rows.length < limit; page += 1) {
        const result = await statusScroll(clientOptions, fetchImpl, policy, offset, Math.min(64, limit));
        for (const point of result.points) {
            const projected = wireMetadata(point.id, point.payload);
            const logicalId = typeof projected.id === "string" ? projected.id : point.id;
            if (requestedIds !== undefined && !requestedIds.includes(logicalId) && !requestedIds.includes(point.id))
                continue;
            rows.push(projected);
            if (rows.length >= limit)
                break;
        }
        if (result.nextOffset === undefined)
            break;
        offset = result.nextOffset;
    }
    return inspectRecords({ ...(requestedIds === undefined ? {} : { ids: requestedIds }), recordTypes: requestedTypes, limit }, { records: rows });
}
export const inspect = inspectRecords;
export const boundedInspect = inspectRecords;
//# sourceMappingURL=inspect.js.map