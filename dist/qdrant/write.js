import { canonicalRecordHash, parseMemoryRecord } from "../domain/records.js";
import { canonicalStringify } from "../domain/canonical.js";
import { physicalPointId, COLLECTION_CONTROL_ID, assertBootstrapControl, controlPayload, controlRecordFromPayload } from "./schema.js";
import { readPolicy } from "./client.js";
const CONTROL_PATCH_KEYS = new Set(["version", "processingPolicyId", "activeGeneration", "activeBaseGeneration", "privacyEpoch", "coordinationPolicyEpoch", "coordinationPolicyHash", "state", "scanCursor", "lastForgetBarrier", "contentHash"]);
function fail(message) { throw new TypeError(message); }
function isRecord(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
function mapKey(key) {
    const names = { recordType: "record_type", ownerHost: "owner_host", schemaRevision: "schema_revision", createdAt: "created_at", privacyEpoch: "privacy_epoch", processingPolicyId: "processing_policy_id", expiresAt: "expires_at", contentHash: "content_hash", sourceEntryId: "source_entry_id", projectId: "project_id", projectIdentityKind: "project_identity_kind", sessionId: "session_id", turnId: "turn_id", agentRole: "agent_role", eventKind: "event_kind", eventAt: "event_at", modelId: "model_id", embeddingDimension: "embedding_dimension", originProvider: "origin_provider", destinationId: "destination_id", secretScan: "secret_scan", toolName: "tool_name", toolArgs: "tool_args", errorFingerprint: "error_fingerprint", producerId: "producer_id", nodeId: "node_id", coordinationPolicyHash: "coordination_policy_hash", coordinationPolicyEpoch: "coordination_policy_epoch", contentId: "content_id", observationId: "observation_id", effectiveAt: "effective_at", sourceEpisodeIds: "source_episode_ids", manifestHash: "manifest_hash", primaryEvidenceEpisodeId: "primary_evidence_episode_id", effectiveOrder: "effective_order", stateKey: "state_key", category: "category", scope: "scope", subject: "subject", predicate: "predicate", confidence: "confidence", generationId: "generation_id", clusterId: "cluster_id", membershipHash: "membership_hash", level: "level", memberIds: "member_ids", summary: "summary", promptRevision: "prompt_revision", algorithm: "algorithm", seed: "seed", jobId: "job_id", fencingToken: "fencing_token", temporalFrom: "temporal_from", temporalTo: "temporal_to", coveredProjects: "covered_projects", algorithmParameters: "algorithm_parameters", activeGeneration: "active_generation", activeBaseGeneration: "active_base_generation", state: "state", scanCursor: "scan_cursor", lastForgetBarrier: "last_forget_barrier", policy: "policy", canonicalHash: "canonical_hash", policyId: "policy_id", policyHash: "policy_hash", policyEpoch: "policy_epoch", membership: "membership", leaseExpiresAt: "lease_expires_at", leaseOwner: "lease_owner", acceptedProposalId: "accepted_proposal_id", acceptedManifestHash: "accepted_manifest_hash", episodeId: "episode_id", extractorRevision: "extractor_revision", sourceId: "source_id", targetId: "target_id", provenanceId: "provenance_id", resolution: "resolution", conflictManifestHash: "conflict_manifest_hash", value: "value" };
    return names[key] ?? key;
}
function recordPayload(record) { const parsed = parseMemoryRecord(record); const payload = {}; for (const [key, value] of Object.entries(parsed)) {
    if (key === "vector")
        continue;
    const mapped = mapKey(key);
    if (mapped === key && /[A-Z]/u.test(key))
        fail(`Unmapped record field: ${key}`);
    payload[mapped] = value;
} payload.status = payload.status ?? "active"; payload.secret_scan = payload.secret_scan ?? "passed"; return payload; }
function recordPoint(record) { const payload = record.recordType === "collection_control" ? controlPayload(record) : recordPayload(record); const point = { id: physicalPointId(record.recordType, record.id), payload }; if ("vector" in record && record.vector !== undefined)
    point.vector = { semantic: record.vector }; return point; }
function policyFor(client, recordType, purpose = "write_verification") { return readPolicy({ ownerHost: client.ownerHost, purpose, recordTypes: [recordType], maxClockSkewMs: client.maxClockSkewMs }); }
function contentHash(payload) { return payload.content_hash; }
function collision(expected, actual) { throw new Error(`content hash collision for ${expected}: ${String(actual)}`); }
function checkHash(payload, expected) { if (contentHash(payload) !== expected)
    collision(expected, contentHash(payload)); }
async function retrieveOne(client, id, policy, includeVector = false) { const points = await client.retrieve([id], policy, { includeVector }); return points.find((point) => point.id === id); }
/** Insert-only is at-least-once: preflight and postflight reads classify observed state; a concurrent race is inherently ambiguous. */
export async function insertOnly(client, record) {
    if (!(record.recordType === "collection_control" && record.version === 0))
        parseMemoryRecord(record);
    if (record.recordType === "collection_control" && record.version === 0)
        assertBootstrapControl(record, client.ownerHost);
    else if (record.contentHash !== canonicalRecordHash(record))
        fail("Memory record canonical hash mismatch");
    const point = recordPoint(record);
    const policy = policyFor(client, record.recordType === "collection_control" ? "collection_control" : record.recordType, record.recordType === "collection_control" ? "control" : "write_verification");
    const before = await retrieveOne(client, point.id, policy);
    let existing = false;
    if (before !== undefined) {
        checkHash(before.payload, record.contentHash);
        existing = true;
    }
    await client.upsertPoints([point], "insert_only");
    const after = await retrieveOne(client, point.id, policy);
    if (after === undefined)
        throw new Error(`insert-only write did not read back point ${point.id}`);
    checkHash(after.payload, record.contentHash);
    return existing ? "existing" : "inserted";
}
export async function insertInitialControl(client, control) { assertBootstrapControl(control, client.ownerHost); return insertOnly(client, control); }
function patchPayload(patch) { const result = {}; for (const key of Object.keys(patch)) {
    if (!CONTROL_PATCH_KEYS.has(key))
        fail(`CAS patch key is not mutable: ${key}`);
    const value = patch[key];
    if (key === "version" || key === "privacyEpoch" || key === "coordinationPolicyEpoch") {
        if (!Number.isSafeInteger(value) || value < 0)
            fail(`CAS patch field is invalid: ${key}`);
    }
    else if (key === "state") {
        if (!["active", "draining", "retired"].includes(String(value)))
            fail("CAS patch state is invalid");
    }
    else if (["activeGeneration", "activeBaseGeneration", "scanCursor", "lastForgetBarrier"].includes(key)) {
        if (value !== null && (typeof value !== "string" || value.length === 0))
            fail(`CAS patch field is invalid: ${key}`);
    }
    else if (key === "processingPolicyId") {
        if (typeof value !== "string" || value.length === 0 || value.length > 512)
            fail("CAS patch processing policy is invalid");
    }
    else if (key === "coordinationPolicyHash") {
        if (typeof value !== "string" || value.length === 0 || value.length > 512)
            fail("CAS patch policy hash is invalid");
    }
    else if (key === "contentHash") {
        if (typeof value !== "string" || !/^[a-f0-9]{64}$/u.test(value))
            fail("CAS patch content hash is invalid");
    }
    result[mapKey(key)] = value;
} return result; }
function ownerPrecondition(client, expectedVersion, expectedPrivacyEpoch, expectedEpoch, expectedState, expectedBaseGeneration) { return { kind: "collection-control-cas", ownerHost: client.ownerHost, recordType: "collection_control", expectedVersion, expectedPrivacyEpoch, expectedEpoch, expectedState, ...(expectedBaseGeneration === undefined ? {} : { expectedBaseGeneration }) }; }
function mergePayload(existing, patch) { return { ...existing, ...patch }; }
function deepEqual(left, right) { try {
    return canonicalStringify(left) === canonicalStringify(right);
}
catch {
    return false;
} }
export async function updateOnlyCas(client, input) {
    if (input.id !== COLLECTION_CONTROL_ID)
        fail("CAS control ID is invalid");
    if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0 || !Number.isSafeInteger(input.expectedEpoch) || input.expectedEpoch < 0)
        fail("CAS version/epoch is invalid");
    if (!isRecord(input.patch) || input.patch.version !== input.expectedVersion + 1)
        fail("CAS patch must advance version exactly once");
    const patch = patchPayload(input.patch);
    const policy = policyFor(client, "collection_control", "control");
    const current = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (current === undefined)
        return false;
    const currentRecord = controlRecordFromPayload(current.payload, client.ownerHost);
    if (currentRecord.state === "retired" || currentRecord.version !== input.expectedVersion || currentRecord.coordinationPolicyEpoch !== input.expectedEpoch)
        return false;
    const suppliedHash = input.patch.contentHash;
    const candidate = { ...currentRecord, ...input.patch, contentHash: "pending" };
    parseMemoryRecord(candidate);
    const privacyDelta = candidate.privacyEpoch - currentRecord.privacyEpoch;
    const coordinationDelta = candidate.coordinationPolicyEpoch - currentRecord.coordinationPolicyEpoch;
    if (privacyDelta < 0 || privacyDelta > 1 || coordinationDelta < 0 || coordinationDelta > 1)
        return false;
    if (coordinationDelta === 0 && (candidate.coordinationPolicyHash !== currentRecord.coordinationPolicyHash || candidate.processingPolicyId !== currentRecord.processingPolicyId))
        return false;
    const computedHash = canonicalRecordHash(candidate);
    if (suppliedHash !== undefined && suppliedHash !== computedHash)
        fail("CAS patch content hash is inconsistent");
    candidate.contentHash = computedHash;
    const point = recordPoint(candidate);
    if (current.vector !== undefined)
        point.vector = current.vector;
    await client.upsertPoints([point], "update_only", ownerPrecondition(client, input.expectedVersion, currentRecord.privacyEpoch, input.expectedEpoch, currentRecord.state));
    const reread = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (reread === undefined)
        return false;
    const rereadRecord = controlRecordFromPayload(reread.payload, client.ownerHost);
    return deepEqual(controlPayload(rereadRecord), controlPayload(candidate));
}
export async function publishControlCas(client, input) {
    if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0 || input.next.id !== COLLECTION_CONTROL_ID || input.next.version !== input.expectedVersion + 1 || input.next.ownerHost !== client.ownerHost)
        fail("Control CAS input is invalid");
    parseMemoryRecord(input.next);
    if (input.next.contentHash !== canonicalRecordHash(input.next))
        fail("Next control canonical hash mismatch");
    const policy = policyFor(client, "collection_control", "control");
    const current = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (current === undefined)
        return false;
    const currentRecord = controlRecordFromPayload(current.payload, client.ownerHost);
    if (currentRecord.state !== "active" || input.next.state !== "active")
        return false;
    if (input.next.privacyEpoch !== currentRecord.privacyEpoch || input.next.coordinationPolicyEpoch !== currentRecord.coordinationPolicyEpoch || input.next.coordinationPolicyHash !== currentRecord.coordinationPolicyHash || input.next.processingPolicyId !== currentRecord.processingPolicyId)
        return false;
    const expectedPayload = controlPayload(input.next);
    if (currentRecord.version === input.next.version && deepEqual(controlPayload(currentRecord), expectedPayload))
        return true;
    if (currentRecord.version !== input.expectedVersion || currentRecord.activeBaseGeneration !== input.expectedBaseGeneration)
        return false;
    const point = recordPoint(input.next);
    if (current.vector !== undefined)
        point.vector = current.vector;
    await client.upsertPoints([point], "update_only", ownerPrecondition(client, input.expectedVersion, currentRecord.privacyEpoch, currentRecord.coordinationPolicyEpoch, currentRecord.state, input.expectedBaseGeneration));
    const reread = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (reread === undefined)
        return false;
    const rereadRecord = controlRecordFromPayload(reread.payload, client.ownerHost);
    return deepEqual(controlPayload(rereadRecord), expectedPayload);
}
//# sourceMappingURL=write.js.map