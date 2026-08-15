import { type AgentMarker } from "./capture/episode.js";
import type { CollectionMetadataContract, HostId } from "./types.js";
export type HostDetectionResult = {
    ok: true;
    host: HostId;
} | {
    ok: false;
    reason: "unknown" | "conflict" | "invalid-explicit-host";
};
export declare function detectHost(input: {
    explicit?: string;
    env: Record<string, string | undefined>;
    argv: readonly string[];
}): HostDetectionResult;
export declare function resolvePrimeRlmDepth(header: unknown, env: Record<string, string | undefined>): number;
/**
 * Resolve the exact host lifecycle marker through the hardened capture parser.
 * Invalid or contradictory metadata is always represented as an ineligible
 * child, so callers cannot accidentally turn ambiguity into root authority.
 */
export declare function resolveHostAgentMarker(host: HostId, header: unknown, env: Record<string, string | undefined>): AgentMarker;
/** Fail-closed compatibility hook used before a host accepts a destination. */
export declare function validateCollectionMetadata(expectedHost: HostId, metadata: Partial<CollectionMetadataContract>, expectedModel?: string, expectedDimension?: number): asserts metadata is CollectionMetadataContract;
export declare const assertCollectionMetadata: typeof validateCollectionMetadata;
