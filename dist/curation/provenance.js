import { canonicalStringify } from "../domain/canonical.js";
import { types as nodeTypes } from "node:util";
export const CURATION_PROPOSAL_SCHEMA = "curation_proposal_v1";
const PROVENANCE_KEYS = ["destinationId", "host", "invokedAt", "modelId", "policyEpoch", "policyHash", "policyId", "promptRevision", "providerId"];
function bounded(value) {
    return typeof value === "string" && value.length > 0 && value.length <= 512 && !/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value);
}
function iso(value) {
    if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value))
        return false;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}
function deepFreeze(value) {
    if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
        for (const child of Object.values(value))
            deepFreeze(child);
        Object.freeze(value);
    }
    return value;
}
function ownedCanonical(value) {
    const active = new Set();
    const clone = (candidate) => {
        if (candidate === null || typeof candidate === "string" || typeof candidate === "boolean")
            return candidate;
        if (typeof candidate === "number") {
            if (!Number.isFinite(candidate))
                throw new TypeError("non-finite");
            return candidate;
        }
        if (typeof candidate !== "object" || nodeTypes.isProxy(candidate) || active.has(candidate))
            throw new TypeError("non-canonical");
        active.add(candidate);
        try {
            if (Object.getOwnPropertySymbols(candidate).length > 0)
                throw new TypeError("symbol");
            if (Array.isArray(candidate)) {
                if (Object.getPrototypeOf(candidate) !== Array.prototype)
                    throw new TypeError("array");
                const lengthDescriptor = Object.getOwnPropertyDescriptor(candidate, "length");
                if (lengthDescriptor === undefined || !("value" in lengthDescriptor) || !Number.isSafeInteger(lengthDescriptor.value) || lengthDescriptor.value < 0 || lengthDescriptor.value > 4096 || Object.getOwnPropertyNames(candidate).length !== lengthDescriptor.value + 1)
                    throw new TypeError("array");
                const result = [];
                for (let index = 0; index < lengthDescriptor.value; index += 1) {
                    const descriptor = Object.getOwnPropertyDescriptor(candidate, String(index));
                    if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
                        throw new TypeError("array");
                    result.push(clone(descriptor.value));
                }
                return result;
            }
            const prototype = Object.getPrototypeOf(candidate);
            if (prototype !== Object.prototype && prototype !== null)
                throw new TypeError("object");
            const result = {};
            for (const name of Object.getOwnPropertyNames(candidate)) {
                const descriptor = Object.getOwnPropertyDescriptor(candidate, name);
                if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
                    throw new TypeError("object");
                Object.defineProperty(result, name, { value: clone(descriptor.value), enumerable: true, writable: true, configurable: true });
            }
            return result;
        }
        finally {
            active.delete(candidate);
        }
    };
    return JSON.parse(canonicalStringify(clone(value)));
}
/**
 * Parse the strict proposal envelope from an owned canonical snapshot.  The
 * canonical snapshot is taken before reading any field, so a backend object
 * with accessors/proxies or a mutation during validation can never be partly
 * trusted or frozen in place.
 */
export function parseCurationProposalEnvelope(value) {
    let owned;
    try {
        owned = ownedCanonical(value);
    }
    catch {
        return null;
    }
    if (typeof owned !== "object" || owned === null || Array.isArray(owned))
        return null;
    const content = owned;
    if (content.schema !== CURATION_PROPOSAL_SCHEMA || !Array.isArray(content.items) || typeof content.provenance !== "object" || content.provenance === null || Array.isArray(content.provenance))
        return null;
    if (Object.keys(content).sort().join(",") !== "items,provenance,schema")
        return null;
    const provenance = content.provenance;
    if (Object.keys(provenance).sort().join(",") !== PROVENANCE_KEYS.join(","))
        return null;
    if (!bounded(provenance.host) || (provenance.host !== "pi" && provenance.host !== "prime") || !bounded(provenance.providerId) || !bounded(provenance.modelId) || !bounded(provenance.destinationId) || !bounded(provenance.policyId) || !bounded(provenance.policyHash) || !Number.isSafeInteger(provenance.policyEpoch) || provenance.policyEpoch < 0 || !bounded(provenance.promptRevision) || !iso(provenance.invokedAt))
        return null;
    deepFreeze(owned);
    return owned;
}
export function provenanceMatches(provenance, expected) {
    for (const key of Object.keys(expected))
        if (provenance[key] !== expected[key])
            return false;
    return true;
}
//# sourceMappingURL=provenance.js.map