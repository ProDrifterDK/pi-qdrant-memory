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
    if (names.length !== value.length + 1 || !names.includes("length")) {
        invalid("sparse arrays or arrays with extra properties");
    }
    const output = [];
    // JSON.stringify consults inherited toJSON methods. Shadow it so even a
    // monkey-patched Array.prototype cannot influence canonical output.
    Object.defineProperty(output, "toJSON", { value: undefined, enumerable: false });
    for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (descriptor === undefined || !("value" in descriptor) || !descriptor.enumerable) {
            invalid("sparse arrays or array accessors");
        }
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
    const names = Object.getOwnPropertyNames(value).sort();
    for (const name of names) {
        const descriptor = Object.getOwnPropertyDescriptor(value, name);
        if (descriptor === undefined || !("value" in descriptor) || !descriptor.enumerable) {
            invalid("non-enumerable properties or accessors");
        }
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
        case "object": {
            if (ancestors.has(value))
                invalid("a cyclic value");
            ancestors.add(value);
            try {
                return Array.isArray(value)
                    ? normalizeArray(value, ancestors)
                    : normalizeObject(value, ancestors);
            }
            finally {
                ancestors.delete(value);
            }
        }
    }
    return invalid("an unsupported value");
}
/** Serialize an explicitly normalized JSON value; no toJSON method is invoked. */
export function canonicalStringify(value) {
    return JSON.stringify(normalizeValue(value, new Set()));
}
export function sha256Hex(value) {
    return createHash("sha256").update(value, "utf8").digest("hex");
}
export function deterministicPointId(targetHost, sourceCollection, sourceId) {
    const hex = sha256Hex(canonicalStringify({ sourceCollection, sourceId, targetHost })).slice(0, 32);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}
//# sourceMappingURL=canonical.js.map