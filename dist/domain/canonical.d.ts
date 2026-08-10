/** JSON with sorted object keys and no user-controlled toJSON/accessors. */
export declare function canonicalStringify(value: unknown): string;
export declare function sha256Hex(value: string): string;
/**
 * Produce a deterministic UUID from a domain namespace and canonical inputs.
 * Inputs are encoded as a tuple, so concatenation/separator ambiguity cannot
 * create an ID collision.
 */
export declare function deterministicUuid(namespace: string, ...inputs: readonly unknown[]): string;
/** Compatibility-free deterministic point UUID for administrative identity checks. */
export declare function deterministicPointId(targetHost: "pi" | "prime", sourceCollection: string, sourceId: string | number): string;
