import { createHash } from "node:crypto";
function invalid(message) {
    throw new TypeError(`Cannot canonicalize ${message}`);
}
function normalizeArray(value, ancestors) {
    if (Object.getPrototypeOf(value) !== Array.prototype)
        invalid("a non-plain array");
    if (Object.getOwnPropertySymbols(value).length > 0)
        invalid("symbol-keyed array properties");
    const names = Object.getOwnPropertyNames(value);
    if (names.length !== value.length + 1 || !names.includes("length"))
        invalid("a sparse array or array with extra properties");
    const output = [];
    Object.defineProperty(output, "toJSON", { value: undefined, enumerable: false });
    for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (descriptor === undefined || !("value" in descriptor) || !descriptor.enumerable)
            invalid("a sparse array or array accessor");
        output.push(normalizeValue(descriptor.value, ancestors));
    }
    return output;
}
function normalizeObject(value, ancestors) {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null)
        invalid("a non-plain object");
    if (Object.getOwnPropertySymbols(value).length > 0)
        invalid("symbol-keyed object properties");
    const output = Object.create(null);
    for (const name of Object.getOwnPropertyNames(value).sort()) {
        const descriptor = Object.getOwnPropertyDescriptor(value, name);
        if (descriptor === undefined || !("value" in descriptor) || !descriptor.enumerable)
            invalid("a non-enumerable property or accessor");
        output[name] = normalizeValue(descriptor.value, ancestors);
    }
    return output;
}
function normalizeValue(value, ancestors) {
    if (value === null)
        return null;
    switch (typeof value) {
        case "boolean":
        case "string":
            return value;
        case "number":
            if (!Number.isFinite(value))
                invalid("a non-finite number");
            return Object.is(value, -0) ? 0 : value;
        case "undefined":
        case "bigint":
        case "symbol":
        case "function":
            return invalid(`a ${typeof value} value`);
        case "object":
            if (ancestors.has(value))
                invalid("a cyclic value");
            ancestors.add(value);
            try {
                return Array.isArray(value) ? normalizeArray(value, ancestors) : normalizeObject(value, ancestors);
            }
            finally {
                ancestors.delete(value);
            }
    }
    return invalid("an unsupported value");
}
/** JSON with sorted object keys and no user-controlled toJSON/accessors. */
export function canonicalStringify(value) {
    return JSON.stringify(normalizeValue(value, new Set()));
}
export function sha256Hex(value) {
    return createHash("sha256").update(value, "utf8").digest("hex");
}
function uuidFromDigest(digest) {
    const bytes = Buffer.from(digest.subarray(0, 16));
    // UUID version 5 layout with SHA-256 bytes (UUIDv5-like, not RFC UUIDv5).
    bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
    bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
    const hex = bytes.toString("hex");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
/**
 * Produce a deterministic UUID from a domain namespace and canonical inputs.
 * Inputs are encoded as a tuple, so concatenation/separator ambiguity cannot
 * create an ID collision.
 */
export function deterministicUuid(namespace, ...inputs) {
    if (namespace.length === 0)
        throw new TypeError("UUID namespace must not be empty");
    const bytes = createHash("sha256").update(canonicalStringify({ namespace, inputs }), "utf8").digest();
    return uuidFromDigest(bytes);
}
/** Compatibility-free deterministic point UUID for administrative identity checks. */
export function deterministicPointId(targetHost, sourceCollection, sourceId) {
    const hex = sha256Hex(canonicalStringify({ sourceCollection, sourceId, targetHost })).slice(0, 32);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
//# sourceMappingURL=canonical.js.map