import { fetchJson, fetchOk, MemoryClientError } from "../clients/http.js";
import { canonicalStringify, deterministicUuid } from "../domain/canonical.js";
import { assertBootstrapControl, collectionControlPoint, collectionMetadataPoint, collectionVectors, isPhysicalPointId } from "./schema.js";
/** The only collection an owner may write or read through this client family. */
export function expectedQdrantCollection(ownerHost) { return ownerHost === "pi" ? "pi_memory" : "prime_memory"; }
export function validatePurpose(purpose, recordTypes) { if (!["memory", "control", "metadata", "query", "internal", "write_verification"].includes(purpose))
    throw new TypeError("Read purpose is invalid"); if ((purpose === "metadata" && (recordTypes.length !== 1 || recordTypes[0] !== "collection_metadata")) || (purpose === "control" && (recordTypes.length !== 1 || recordTypes[0] !== "collection_control")) || ((purpose === "memory" || purpose === "query") && (recordTypes.length === 0 || recordTypes.some((type) => !["episode", "curated_memory", "curated_current", "raptor_summary"].includes(type)))) || ((purpose === "internal" || purpose === "write_verification") && recordTypes.length === 0))
    throw new TypeError("Read purpose and record types do not match"); }
export function readPolicy(input) {
    // GLOBAL RULE: snapshot every field EXACTLY ONCE; the returned policy is
    // built EXPLICITLY (no caller spread — smuggled keys are never carried).
    const ownerHost = input.ownerHost;
    const purpose = input.purpose;
    const recordTypes = [...input.recordTypes];
    const projectId = input.projectId;
    const processingPolicyId = input.processingPolicyId;
    const privacyEpoch = input.privacyEpoch;
    const now = input.now ?? Date.now();
    const skew = input.maxClockSkewMs ?? 0;
    if (ownerHost !== "pi" && ownerHost !== "prime")
        throw new TypeError("Read owner is invalid");
    if (recordTypes.length === 0 || recordTypes.some((type) => !["episode", "curated_memory", "curated_current", "conflict_manifest", "raptor_summary", "collection_control", "processing_policy", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone", "collection_metadata"].includes(type)))
        throw new TypeError("Read record type policy is invalid");
    if (!Number.isFinite(now) || !Number.isFinite(skew) || skew < 0)
        throw new TypeError("Read expiry policy is invalid");
    if (projectId !== undefined && (typeof projectId !== "string" || projectId.length === 0) || processingPolicyId !== undefined && (typeof processingPolicyId !== "string" || processingPolicyId.length === 0) || privacyEpoch !== undefined && (!Number.isSafeInteger(privacyEpoch) || privacyEpoch < 0))
        throw new TypeError("Read scope policy is invalid");
    validatePurpose(purpose, recordTypes);
    const policy = { ownerHost, purpose, recordTypes, now, maxClockSkewMs: skew, requireStatus: "active", requireSecretScan: "passed" };
    if (projectId !== undefined)
        policy.projectId = projectId;
    if (processingPolicyId !== undefined)
        policy.processingPolicyId = processingPolicyId;
    if (privacyEpoch !== undefined)
        policy.privacyEpoch = privacyEpoch;
    return policy;
}
export function physicalPointIdFor(recordType, logicalId) {
    return isPhysicalPointId(logicalId) ? logicalId : deterministicUuid("pi-qdrant-memory-v2:point", recordType, logicalId);
}
//# sourceMappingURL=client.js.map