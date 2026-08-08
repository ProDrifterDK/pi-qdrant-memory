import type { HostId } from "../types.js";
/** Serialize an explicitly normalized JSON value; no toJSON method is invoked. */
export declare function canonicalStringify(value: unknown): string;
export declare function sha256Hex(value: string): string;
export declare function deterministicPointId(targetHost: HostId, sourceCollection: string, sourceId: string | number): string;
