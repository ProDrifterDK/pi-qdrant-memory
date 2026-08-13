import { ProductionCoordinationStore, LeaseAuthority, validateSortedMembership, jobIdFor, proposalHashFor } from "../qdrant/write.js";
export { validateSortedMembership, jobIdFor, proposalHashFor } from "../qdrant/write.js";
import { canonicalStringify } from "../domain/canonical.js";
/** Immutable explicit-membership job point; identity is enforced by the record parser. */
export async function createJob(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Job creation requires a genuine production store");
    return store.createJob(input);
}
/**
 * Write an immutable proposal bound to the genuine authority. Everything
 * (identity/fence/policy/privacy/deadline/fresh clocks/exact readback) is
 * enforced inside the store's lexical safe method.
 */
export async function writeProposal(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Proposal write requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Proposal requires a genuine lease authority");
    return store.writeProposal(authority, input);
}
/** Thin safe wrapper over the ONE complete safe acceptance operation. */
export async function acceptProposal(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Accept proposal requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Accept proposal requires a genuine lease authority");
    if (input === null || typeof input !== "object" || typeof input.proposalId !== "string" || input.proposalId.length === 0 || input.proposalId.length > 512)
        throw new TypeError("Accept proposal inputs are invalid");
    return store.acceptProposal(authority, input);
}
/** Materialization gate: reads control/claim/proposal/job/tombstones through the genuine store. */
function freezeCanonicalSnapshot(value) {
    const clone = JSON.parse(canonicalStringify(value));
    const freeze = (part) => {
        if (part === null || typeof part !== "object" || Object.isFrozen(part))
            return;
        for (const key of Object.keys(part))
            freeze(part[key]);
        Object.freeze(part);
    };
    freeze(clone);
    return clone;
}
function exactCanonical(left, right) {
    try {
        return canonicalStringify(left) === canonicalStringify(right);
    }
    catch {
        return false;
    }
}
export async function readActiveAcceptance(store, authority) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Active acceptance requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Active acceptance requires a genuine lease authority");
    if (!authority.matchesStore(store))
        throw new TypeError("Active acceptance authority does not match the store");
    if (authority.state !== "accepted")
        return null;
    const policyEpoch = authority.coordinationPolicyEpoch;
    const policyHash = authority.coordinationPolicyHash;
    const privacyEpoch = authority.privacyEpoch;
    const sample = () => { try {
        return authority.now();
    }
    catch {
        return null;
    } };
    const controlMatches = (control) => control.ownerHost === authority.ownerHost && control.state === "active" && control.coordinationPolicyEpoch === policyEpoch && control.coordinationPolicyHash === policyHash && control.privacyEpoch === privacyEpoch;
    const jobMatches = (job) => job !== null && job.id === authority.jobId && job.ownerHost === authority.ownerHost && job.coordinationPolicyEpoch === policyEpoch && job.coordinationPolicyHash === policyHash && job.privacyEpoch === privacyEpoch;
    const claimMatches = (claim, job, now) => claim !== null && authority.matchesClaim(claim) && claim.state === "accepted" && claim.acceptedProposalId !== null && claim.acceptedManifestHash !== null && Date.parse(claim.expiresAt) > now && !jobExpired(job, now, authority.maxClockSkewMs) && claimIdentityMatchesJob(claim, job);
    const proposalMatches = (proposal, job, proposalId, manifestHash) => proposal !== null && proposal.id === proposalId && proposal.jobId === job.id && proposal.manifestHash === manifestHash && proposal.ownerHost === authority.ownerHost && proposal.coordinationPolicyEpoch === policyEpoch && proposal.coordinationPolicyHash === policyHash && proposal.privacyEpoch === privacyEpoch && proposal.expiresAt === job.expiresAt && proposal.processingPolicyId === job.policyId && proposal.membership.length === job.membership.length && proposal.membership.every((id, index) => id === job.membership[index]);
    // Bootstrap only the immutable accepted pair needed to address the proposal;
    // the authoritative sandwich itself starts below in one uniform order.
    const bootstrapClaim = await store.readLease(authority.jobId);
    if (bootstrapClaim === null || !authority.matchesClaim(bootstrapClaim) || bootstrapClaim.state !== "accepted" || bootstrapClaim.acceptedProposalId === null || bootstrapClaim.acceptedManifestHash === null)
        return null;
    const proposalId = bootstrapClaim.acceptedProposalId;
    const manifestHash = bootstrapClaim.acceptedManifestHash;
    const control = await store.readControl();
    const job = await store.readJob(authority.jobId);
    const proposal = await store.readProposal(proposalId);
    const claim = await store.readLease(authority.jobId);
    if (!controlMatches(control) || !jobMatches(job) || !proposalMatches(proposal, job, proposalId, manifestHash))
        return null;
    const tombstones = await store.readTombstones(job.membership);
    const initialNow = sample();
    if (initialNow === null || !claimMatches(claim, job, initialNow) || !exactCanonical(claim, bootstrapClaim) || tombstones.length > 0)
        return null;
    const controlMid = await store.readControl();
    const jobMid = await store.readJob(authority.jobId);
    const proposalMid = await store.readProposal(proposalId);
    const claimMid = await store.readLease(authority.jobId);
    const tombstonesMid = await store.readTombstones(job.membership);
    const midNow = sample();
    if (midNow === null || !jobMatches(jobMid) || !claimMatches(claimMid, jobMid, midNow) || !proposalMatches(proposalMid, jobMid, proposalId, manifestHash) || !controlMatches(controlMid) || tombstonesMid.length > 0)
        return null;
    if (!exactCanonical(controlMid, control) || !exactCanonical(jobMid, job) || !exactCanonical(proposalMid, proposal) || !exactCanonical(claimMid, claim) || !exactCanonical(tombstonesMid, tombstones))
        return null;
    // Final lane is deliberately ordered control -> job -> proposal -> claim ->
    // tombstones -> fresh clock. No authoritative record can change between a
    // successful acceptance receipt and the returned owned snapshots.
    const controlFinal = await store.readControl();
    const jobFinal = await store.readJob(authority.jobId);
    const proposalFinal = await store.readProposal(proposalId);
    const claimFinal = await store.readLease(authority.jobId);
    const tombstonesFinal = await store.readTombstones(job.membership);
    const finalNow = sample();
    if (finalNow === null || !jobMatches(jobFinal) || !claimMatches(claimFinal, jobFinal, finalNow) || !proposalMatches(proposalFinal, jobFinal, proposalId, manifestHash) || !controlMatches(controlFinal) || tombstonesFinal.length > 0)
        return null;
    if (!exactCanonical(controlFinal, control) || !exactCanonical(jobFinal, job) || !exactCanonical(proposalFinal, proposal) || !exactCanonical(claimFinal, claim) || !exactCanonical(tombstonesFinal, tombstones))
        return null;
    return Object.freeze({
        proposalId,
        manifestHash,
        claimVersion: claimFinal.version,
        job: freezeCanonicalSnapshot(jobFinal),
        proposal: freezeCanonicalSnapshot(proposalFinal),
    });
}
/** Terminal completion is capability-gated and succeeds only after exact immutable readbacks. */
export async function completeJob(store, authority) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Job completion requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Job completion requires a genuine accepted lease authority");
    return store.completeJob(authority);
}
function claimIdentityMatchesJob(claim, job) {
    if (job.expiresAt !== null && Date.parse(claim.expiresAt) > Date.parse(job.expiresAt))
        return false;
    return claim.ownerHost === job.ownerHost && claim.processingPolicyId === job.policyId && claim.coordinationPolicyEpoch === job.coordinationPolicyEpoch && claim.coordinationPolicyHash === job.coordinationPolicyHash && claim.privacyEpoch === job.privacyEpoch && claim.jobId === job.id && claim.id === leasePointId(job.id);
}
import { jobExpired } from "./deadline.js";
import { leasePointId } from "../domain/ids.js";
/** Read a job (safe read). */
export async function readJob(store, jobIdValue) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Job read requires a genuine production store");
    return store.readJob(jobIdValue);
}
/** Read an immutable curated observation through the genuine store. */
export async function readObservation(store, authority, observationId) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Observation read requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Observation read requires a genuine accepted lease authority");
    return store.readObservation(authority, observationId);
}
/** Read the policy-epoch-specific current view through the genuine accepted authority. */
export async function readCurrent(store, authority, currentId) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Current read requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Current read requires a genuine accepted lease authority");
    return store.readCurrent(authority, currentId);
}
/** Insert an immutable curated observation through the genuine store. */
export async function insertObservation(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Observation write requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Observation write requires a genuine accepted lease authority");
    return store.insertObservation(authority, input);
}
/** Insert an immutable evidence link through the genuine accepted authority. */
export async function insertEvidenceLink(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Evidence link write requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Evidence link write requires a genuine accepted lease authority");
    return store.insertEvidenceLink(authority, input);
}
/** Insert a content-addressed conflict manifest through the genuine accepted authority. */
export async function insertConflictManifest(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Conflict manifest write requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Conflict manifest write requires a genuine accepted lease authority");
    return store.insertConflictManifest(authority, input);
}
/** OCC update of the policy-epoch-specific curated current. */
export async function upsertCuratedCurrent(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Current write requires a genuine production store");
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Current write requires a genuine accepted lease authority");
    return store.upsertCuratedCurrent(authority, input);
}
//# sourceMappingURL=jobs.js.map