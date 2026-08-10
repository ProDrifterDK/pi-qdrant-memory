import { canonicalStringify, deterministicUuid, sha256Hex } from "./canonical.js";
const SCHEMA_NAMESPACE = "pi-qdrant-memory-v2";
const MAX_ID_LENGTH = 512;
const MAX_MEMBERS = 1024;
const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
function hashParts(value) {
    return sha256Hex(canonicalStringify(value));
}
function requireText(name, value) {
    if (typeof value !== "string" || value.length === 0 || value.length > MAX_ID_LENGTH)
        throw new TypeError(`${name} must be a bounded non-empty string`);
    return value;
}
function requireId(name, value) {
    requireText(name, value);
    if (/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value))
        throw new TypeError(`${name} must be redacted`);
    return value;
}
/** Logical state identity; it is independent of the current value and owner. */
export function stateKey(input) {
    if (input.host !== "pi" && input.host !== "prime")
        throw new TypeError("stateKey.host must be pi or prime");
    requireId("stateKey.scope", input.scope);
    requireId("stateKey.category", input.category);
    requireId("stateKey.subject", input.subject);
    requireId("stateKey.predicate", input.predicate);
    if (input.projectId !== undefined && input.projectId !== null)
        requireId("stateKey.projectId", input.projectId);
    return hashParts({ category: input.category, host: input.host, predicate: input.predicate, projectId: input.projectId ?? null, schema: SCHEMA_NAMESPACE, scope: input.scope, subject: input.subject });
}
/** Reusable value identity under one coordination policy. */
export function contentId(policyHash, logicalStateKey, canonicalValue) {
    requireText("contentId.policyHash", policyHash);
    requireId("contentId.stateKey", logicalStateKey);
    return hashParts({ canonicalValue, policyHash, stateKey: logicalStateKey });
}
/** Validate the two causal-order encodings permitted by §8.2. */
export function validateEffectiveOrder(value) {
    if (typeof value === "string") {
        if (value.length > MAX_ID_LENGTH || !/^session:(?:0|[1-9]\d*)$/u.test(value))
            throw new TypeError("effectiveOrder causal sequence is invalid");
        return;
    }
    if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype || Object.getOwnPropertySymbols(value).length > 0 || Object.getOwnPropertyNames(value).length !== 4 || value.length !== 3 || value.some((item) => typeof item !== "string"))
        throw new TypeError("effectiveOrder tuple is invalid");
    const [eventAt, primaryEpisodeId, contentId] = value;
    const dateMatch = ISO_DATE.exec(eventAt);
    const parsed = Date.parse(eventAt);
    if (dateMatch === null || Number(dateMatch[1]) < 1970 || Number(dateMatch[1]) > 2100 || !Number.isFinite(parsed) || new Date(parsed).toISOString() !== eventAt)
        throw new TypeError("effectiveOrder event timestamp is invalid");
    requireId("effectiveOrder.primaryEpisodeId", primaryEpisodeId);
    requireId("effectiveOrder.contentId", contentId);
}
/** Insert-only occurrence identity. effectiveOrder may be a causal tuple. */
export function observationId(policyEpoch, logicalContentId, primaryEvidenceEpisodeId, effectiveOrder) {
    if (!Number.isSafeInteger(policyEpoch) || policyEpoch < 0)
        throw new TypeError("observationId.policyEpoch must be a non-negative integer");
    requireId("observationId.contentId", logicalContentId);
    requireId("observationId.primaryEvidenceEpisodeId", primaryEvidenceEpisodeId);
    validateEffectiveOrder(effectiveOrder);
    return hashParts({ contentId: logicalContentId, effectiveOrder, policyEpoch, primaryEvidenceEpisodeId });
}
export function evidenceLinkId(observation, episode, extractorRevision) {
    requireId("evidenceLinkId.observationId", observation);
    requireId("evidenceLinkId.episodeId", episode);
    if (typeof extractorRevision === "string")
        requireId("evidenceLinkId.extractorRevision", extractorRevision);
    else if (!Number.isSafeInteger(extractorRevision) || extractorRevision < 0 || extractorRevision > MAX_ID_LENGTH)
        throw new TypeError("evidenceLinkId.extractorRevision is invalid");
    return deterministicUuid(`${SCHEMA_NAMESPACE}:evidence-link`, observation, episode, extractorRevision);
}
export function episodeId(inputOrHost, sessionId, messageId, part) {
    const input = typeof inputOrHost === "string"
        ? (part === undefined
            ? { host: inputOrHost, sessionId: sessionId ?? "", messageId: messageId ?? "" }
            : { host: inputOrHost, sessionId: sessionId ?? "", messageId: messageId ?? "", part })
        : inputOrHost;
    if (input.host !== "pi" && input.host !== "prime")
        throw new TypeError("episodeId.host must be pi or prime");
    requireId("episodeId.sessionId", input.sessionId);
    requireId("episodeId.messageId", input.messageId);
    if (input.part !== undefined && (typeof input.part === "string" ? (input.part.length === 0 || input.part.length > MAX_ID_LENGTH) : (!Number.isSafeInteger(input.part) || input.part < 0 || input.part > MAX_ID_LENGTH)))
        throw new TypeError("episodeId.part is invalid");
    return deterministicUuid(`${SCHEMA_NAMESPACE}:episode`, input.host, input.sessionId, input.messageId, input.part ?? null);
}
export function jobId(ownerOrInput, membership, policyHash, extractorRevision) {
    const input = typeof ownerOrInput === "string"
        ? { ownerHost: ownerOrInput, membership: membership ?? [], policyHash: policyHash ?? "", extractorRevision: extractorRevision ?? "" }
        : ownerOrInput;
    if (input.ownerHost !== "pi" && input.ownerHost !== "prime")
        throw new TypeError("jobId.ownerHost must be pi or prime");
    requireId("jobId.policyHash", input.policyHash);
    requireId("jobId.extractorRevision", input.extractorRevision);
    if (!Array.isArray(input.membership) || input.membership.length === 0 || input.membership.length > MAX_MEMBERS)
        throw new TypeError("jobId.membership must contain bounded IDs");
    input.membership.forEach((id, index) => requireId(`jobId.membership[${index}]`, id));
    return deterministicUuid(`${SCHEMA_NAMESPACE}:job`, input.ownerHost, [...input.membership], input.policyHash, input.extractorRevision);
}
export function manifestHash(memberIds) {
    if (!Array.isArray(memberIds) || memberIds.length === 0 || memberIds.length > MAX_MEMBERS)
        throw new TypeError("manifest member IDs are invalid or unbounded");
    memberIds.forEach((id, index) => requireId(`manifest member ID ${index}`, id));
    return hashParts([...memberIds]);
}
export function tombstoneId(scope, targetId, provenanceId) {
    if (scope !== "occurrence" && scope !== "content" && scope !== "state")
        throw new TypeError("tombstone scope is invalid");
    requireId("tombstoneId.targetId", targetId);
    if (provenanceId !== undefined)
        requireId("tombstoneId.provenanceId", provenanceId);
    return deterministicUuid(`${SCHEMA_NAMESPACE}:tombstone`, scope, targetId, provenanceId ?? null);
}
//# sourceMappingURL=ids.js.map