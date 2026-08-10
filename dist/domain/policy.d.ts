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
export declare function processingPolicyHash(policy: ProcessingPolicy): string;
/** Intersect exact destination capabilities and labels across producer and worker policies. */
export declare function intersectPolicies(policies: readonly ProcessingPolicy[], worker: ProcessingPolicy): ProcessingPolicy | null;
export declare function isPolicyExpired(policy: ProcessingPolicy, now?: number, skewMs?: number): boolean;
