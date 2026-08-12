import { ProductionCoordinationStore, LeaseAuthority, isLeaseExpired } from "../qdrant/write.js";
export { LeaseAuthority, isLeaseExpired } from "../qdrant/write.js";
import { RootWorkerContext } from "./root.js";
/** Claim a live job lease with a genuine root worker; returns a NEW authority or null. */
export async function claimLease(store, worker, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Lease claim requires a genuine production store");
    return store.claimLease(worker, input);
}
/** Renew an owned live claim using the genuine authority; returns a NEW authority (the old one is stale by version/content hash). */
export async function renewLease(store, authority) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Lease renewal requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Lease renewal requires a genuine lease authority");
    return store.renewLease(authority);
}
/** Release a genuinely live owned claim; consumes the authority (no successor). Locally expired or skew-grace claims can NEVER release. */
export async function releaseLease(store, authority) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Lease release requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Lease release requires a genuine lease authority");
    return store.releaseLease(authority);
}
/**
 * The ONE safe acceptance operation. Everything acceptProposal could require
 * is validated inside the store's lexical safe method; calling this wrapper
 * directly can never bypass a job/proposal/tombstone/control invariant.
 */
export async function acceptLeaseAuthority(store, authority, proposalId) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Accept proposal requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Accept proposal requires a genuine lease authority");
    return store.acceptLease(authority, proposalId);
}
/** Read the current lease claim for a job (safe read). */
export async function readLease(store, jobId) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Lease read requires a genuine production store");
    return store.readLease(jobId);
}
//# sourceMappingURL=leases.js.map