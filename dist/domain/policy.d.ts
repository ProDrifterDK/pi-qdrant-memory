import type { HostId } from "../types.js";
export interface ProcessingPolicy {
    id: string;
    ownerHost: HostId;
    destinationIds: {
        qdrant: string;
        embedding: string;
        llm?: string;
    };
    originProvider: string;
    allowCrossProviderReplay: boolean;
    expiresAt: string | null;
    residency: string;
    dataUse: string;
    policyRevision: string;
}
/** Worker-policy origin for provider-agnostic capture: the session provider
 * is volatile provenance (recorded per episode), never worker identity. The
 * sentinel is a wildcard in every origin check below; destination allowlists
 * remain the egress barrier. */
export declare const PROVIDER_AGNOSTIC_ORIGIN = "any";
export declare function processingPolicyHash(policy: ProcessingPolicy): string;
/**
 * Intersect exact destination capabilities and labels across producer and
 * worker policies, preserving the producer CONTENT ORIGIN. For one canonical
 * producer origin the effective origin is that producer origin even when a
 * different worker provider replays (with every allow flag set). Multiple
 * producer origins fail closed until a provider-set schema exists. The
 * effective policy revision is content-addressed from the sorted producer
 * identities plus the distinguished worker identity.
 */
export declare function intersectPolicies(policies: readonly ProcessingPolicy[], worker: ProcessingPolicy): ProcessingPolicy | null;
export declare function isPolicyExpired(policy: ProcessingPolicy, now?: number, skewMs?: number): boolean;
