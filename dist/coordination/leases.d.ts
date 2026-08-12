import { ProductionCoordinationStore, LeaseAuthority } from "../qdrant/write.js";
export { LeaseAuthority, isLeaseExpired } from "../qdrant/write.js";
import { RootWorkerContext } from "./root.js";
import type { LeaseRecord } from "../domain/records.js";
/**
 * Thin Production-brand-only lease surface. There is NO structural-store
 * mutation path: inputs that are not a genuine Production store fail closed,
 * and every mutation delegates to the store's named safe method (the protocol
 * implementation lives lexically inside qdrant/write.ts and is not exported).
 */
export interface ClaimLeaseInput {
    jobId: string;
    policyEpoch: number;
    policyHash: string;
    privacyEpoch: number;
}
/** Claim a live job lease with a genuine root worker; returns a NEW authority or null. */
export declare function claimLease(store: ProductionCoordinationStore, worker: RootWorkerContext, input: ClaimLeaseInput): Promise<LeaseAuthority | null>;
/** Renew an owned live claim using the genuine authority; returns a NEW authority (the old one is stale by version/content hash). */
export declare function renewLease(store: ProductionCoordinationStore, authority: LeaseAuthority): Promise<LeaseAuthority | null>;
/** Release a genuinely live owned claim; consumes the authority (no successor). Locally expired or skew-grace claims can NEVER release. */
export declare function releaseLease(store: ProductionCoordinationStore, authority: LeaseAuthority): Promise<boolean>;
/**
 * The ONE safe acceptance operation. Everything acceptProposal could require
 * is validated inside the store's lexical safe method; calling this wrapper
 * directly can never bypass a job/proposal/tombstone/control invariant.
 */
export declare function acceptLeaseAuthority(store: ProductionCoordinationStore, authority: LeaseAuthority, proposalId: string): Promise<LeaseAuthority | null>;
/** Read the current lease claim for a job (safe read). */
export declare function readLease(store: ProductionCoordinationStore, jobId: string): Promise<LeaseRecord | null>;
