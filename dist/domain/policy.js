import { canonicalStringify, sha256Hex } from "./canonical.js";
const MAX_ID = 256;
const MAX_LABEL = 128;
const SECRET = /(api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;
const DATE_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
function bounded(name, value, max, redact = false) {
    if (typeof value !== "string" || value.length === 0 || value.length > max || (redact && SECRET.test(value)))
        throw new TypeError(`${name} must be bounded and redacted`);
}
function destination(name, value) { bounded(name, value, MAX_ID, true); if (!/^[A-Za-z0-9._:/-]+$/u.test(value))
    throw new TypeError(`${name} must be an exact destination ID`); }
function label(name, value) { bounded(name, value, MAX_LABEL, true); if (!/^[A-Za-z0-9._:/ -]+$/u.test(value))
    throw new TypeError(`${name} must be a redacted label`); }
function date(name, value) { bounded(name, value, 24); const parsed = Date.parse(value); if (!DATE_RE.test(value) || !Number.isFinite(parsed) || new Date(parsed).toISOString() !== value)
    throw new TypeError(`${name} must be a bounded ISO timestamp`); }
function validPolicyShape(policy) {
    const allowed = new Set(["id", "ownerHost", "destinationIds", "originProvider", "allowCrossProviderReplay", "expiresAt", "residency", "dataUse", "policyRevision"]);
    for (const key of Object.keys(policy))
        if (!allowed.has(key))
            throw new TypeError("unknown processing policy field");
    bounded("policy.id", policy.id, MAX_ID, true);
    if (policy.ownerHost !== "pi" && policy.ownerHost !== "prime")
        throw new TypeError("policy ownerHost is invalid");
    if (typeof policy.destinationIds !== "object" || policy.destinationIds === null || Array.isArray(policy.destinationIds))
        throw new TypeError("policy destinationIds is invalid");
    destination("destinationIds.qdrant", policy.destinationIds.qdrant);
    destination("destinationIds.embedding", policy.destinationIds.embedding);
    if (policy.destinationIds.llm !== undefined)
        destination("destinationIds.llm", policy.destinationIds.llm);
    bounded("policy.originProvider", policy.originProvider, MAX_ID, true);
    if (typeof policy.allowCrossProviderReplay !== "boolean")
        throw new TypeError("policy replay flag is invalid");
    if (policy.expiresAt !== null)
        date("policy.expiresAt", policy.expiresAt);
    label("policy.residency", policy.residency);
    label("policy.dataUse", policy.dataUse);
    bounded("policy.policyRevision", policy.policyRevision, MAX_ID, true);
    for (const key of Object.keys(policy.destinationIds))
        if (!["qdrant", "embedding", "llm"].includes(key))
            throw new TypeError("unknown policy destination capability");
}
export function processingPolicyHash(policy) {
    validPolicyShape(policy);
    const { id: _computedId, ...withoutId } = policy;
    return sha256Hex(canonicalStringify(withoutId));
}
function validPolicy(policy) {
    validPolicyShape(policy);
    if (policy.id !== processingPolicyHash(policy))
        throw new TypeError("processing policy ID is not content addressed");
}
function earliestExpiry(policies) {
    const dates = policies.filter((policy) => policy.expiresAt !== null).map((policy) => Date.parse(policy.expiresAt));
    return dates.length === 0 ? null : new Date(Math.min(...dates)).toISOString();
}
/**
 * Intersect exact destination capabilities and labels across producer and
 * worker policies, preserving the producer CONTENT ORIGIN. For one canonical
 * producer origin the effective origin is that producer origin even when a
 * different worker provider replays (with every allow flag set). Multiple
 * producer origins fail closed until a provider-set schema exists. The
 * effective policy revision is content-addressed from the sorted producer
 * identities plus the distinguished worker identity.
 */
export function intersectPolicies(policies, worker) {
    validPolicy(worker);
    policies.forEach(validPolicy);
    // Empty producer list converges to a FRESH exact copy of the worker policy
    // (never the caller-owned object, never an intersection revision).
    if (policies.length === 0)
        return { ...worker, destinationIds: { ...worker.destinationIds } };
    const all = [...policies, worker];
    if (all.some((policy) => policy.ownerHost !== worker.ownerHost))
        return null;
    const first = all[0];
    if (all.some((policy) => policy.residency !== first.residency || policy.dataUse !== first.dataUse))
        return null;
    const capabilities = ["qdrant", "embedding", "llm"];
    const destinationIds = { qdrant: first.destinationIds.qdrant, embedding: first.destinationIds.embedding };
    for (const capability of capabilities) {
        const values = all.map((policy) => policy.destinationIds[capability]);
        const present = values.some((value) => value !== undefined);
        if (present && values.some((value) => value === undefined || value !== values[0]))
            return null;
        if (capability === "llm" && values[0] !== undefined)
            destinationIds.llm = values[0];
    }
    // Producer content origin: multiple producer origins fail closed; one origin is preserved.
    const producerOrigins = new Set(policies.map((policy) => policy.originProvider));
    if (producerOrigins.size > 1)
        return null;
    const origin = producerOrigins.size === 1 ? [...producerOrigins][0] : worker.originProvider;
    if (origin !== worker.originProvider && (!worker.allowCrossProviderReplay || policies.some((policy) => !policy.allowCrossProviderReplay)))
        return null;
    // Identical producer/worker policy converges to the unchanged policy as a
    // FRESH clone (never a caller-owned mutable alias).
    if (policies.length === 1 && policies[0].id === worker.id)
        return { ...worker, destinationIds: { ...worker.destinationIds } };
    // Content-addressed effective revision from sorted producer identities + worker identity.
    const producerIdentities = [...policies].map((policy) => ({ id: policy.id, policyRevision: policy.policyRevision })).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    const intersectionRevision = `intersection:${sha256Hex(canonicalStringify({ producers: producerIdentities, workerId: worker.id, workerRevision: worker.policyRevision }))}`;
    const result = {
        ...worker,
        destinationIds,
        allowCrossProviderReplay: all.every((policy) => policy.allowCrossProviderReplay),
        expiresAt: earliestExpiry(all),
        residency: first.residency,
        dataUse: first.dataUse,
        originProvider: origin,
        policyRevision: intersectionRevision,
    };
    return { ...result, id: processingPolicyHash(result) };
}
export function isPolicyExpired(policy, now = Date.now(), skewMs = 0) {
    validPolicy(policy);
    return policy.expiresAt !== null && Date.parse(policy.expiresAt) <= now + skewMs;
}
//# sourceMappingURL=policy.js.map