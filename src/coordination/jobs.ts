import { ProductionCoordinationStore, LeaseAuthority, validateSortedMembership, jobIdFor, proposalHashFor } from "../qdrant/write.js";
export { validateSortedMembership, jobIdFor, proposalHashFor } from "../qdrant/write.js";
import type { HostId } from "../types.js";
import type { JobRecord, ProposalRecord } from "../domain/records.js";

/**
 * Thin Production-brand-only job/proposal surface. There is NO structural-
 * store mutation path: inputs that are not a genuine Production store fail
 * closed, and every mutation delegates to the store's named safe method.
 */
export interface CreateJobInput {
  ownerHost: HostId;
  membership: readonly string[];
  policyIntersectionId: string;
  policyHash: string;
  policyEpoch: number;
  extractorRevision: string;
  privacyEpoch: number;
  createdAt: string;
  expiresAt?: string;
}
export interface ProposalContent { summary?: string; observations?: unknown; [key: string]: unknown; }
export interface WriteProposalInput {
  membership: readonly string[];
  content: ProposalContent;
  createdAt: string;
}
export interface ActiveAcceptance { proposalId: string; manifestHash: string; claimVersion: number; }

/** Immutable explicit-membership job point; identity is enforced by the record parser. */
export async function createJob(store: ProductionCoordinationStore, input: CreateJobInput): Promise<JobRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Job creation requires a genuine production store");
  return store.createJob(input);
}

/**
 * Write an immutable proposal bound to the genuine authority. Everything
 * (identity/fence/policy/privacy/deadline/fresh clocks/exact readback) is
 * enforced inside the store's lexical safe method.
 */
export async function writeProposal(store: ProductionCoordinationStore, authority: LeaseAuthority, input: WriteProposalInput): Promise<ProposalRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Proposal write requires a genuine production store");
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Proposal requires a genuine lease authority");
  return store.writeProposal(authority, input);
}

/** Thin safe wrapper over the ONE complete safe acceptance operation. */
export async function acceptProposal(store: ProductionCoordinationStore, authority: LeaseAuthority, input: { proposalId: string }): Promise<LeaseAuthority | null> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Accept proposal requires a genuine production store");
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Accept proposal requires a genuine lease authority");
  if (input === null || typeof input !== "object" || typeof input.proposalId !== "string" || input.proposalId.length === 0 || input.proposalId.length > 512) throw new TypeError("Accept proposal inputs are invalid");
  return store.acceptProposal(authority, input);
}

/** Materialization gate: reads control/claim/proposal/job/tombstones through the genuine store. */
export async function readActiveAcceptance(store: ProductionCoordinationStore, authority: LeaseAuthority): Promise<ActiveAcceptance | null> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Active acceptance requires a genuine production store");
  if (!LeaseAuthority.isValid(authority)) throw new TypeError("Active acceptance requires a genuine lease authority");
  if (!authority.matchesStore(store)) throw new TypeError("Active acceptance authority does not match the store");
  if (authority.state !== "accepted") return null;
  const input = { policyEpoch: authority.coordinationPolicyEpoch, policyHash: authority.coordinationPolicyHash, privacyEpoch: authority.privacyEpoch, maxClockSkewMs: authority.maxClockSkewMs, jobId: authority.jobId };
  const sample = (): number | null => { try { return authority.now(); } catch { return null; } };
  const control = await store.readControl();
  if (authority.ownerHost !== control.ownerHost) return null;
  if (control.state !== "active" || control.coordinationPolicyEpoch !== input.policyEpoch || control.coordinationPolicyHash !== input.policyHash || control.privacyEpoch !== input.privacyEpoch) return null;
  const job = await store.readJob(input.jobId);
  if (job === null || job.id !== input.jobId || job.coordinationPolicyEpoch !== input.policyEpoch || job.coordinationPolicyHash !== input.policyHash || job.privacyEpoch !== input.privacyEpoch) return null;
  const claim = await store.readLease(input.jobId);
  const claimNow = sample();
  if (claimNow === null || claim === null || !authority.matchesClaim(claim) || claim.state !== "accepted" || claim.acceptedProposalId === null || claim.acceptedManifestHash === null || Date.parse(claim.expiresAt) <= claimNow || jobExpired(job, claimNow, input.maxClockSkewMs) || !claimIdentityMatchesJob(claim, job)) return null;
  const proposal = await store.readProposal(claim.acceptedProposalId);
  if (proposal === null || proposal.id !== claim.acceptedProposalId || proposal.jobId !== authority.jobId || proposal.manifestHash !== claim.acceptedManifestHash || proposal.coordinationPolicyEpoch !== input.policyEpoch || proposal.coordinationPolicyHash !== input.policyHash || proposal.privacyEpoch !== input.privacyEpoch || proposal.expiresAt !== job.expiresAt || proposal.ownerHost !== control.ownerHost || proposal.processingPolicyId !== job.policyId || proposal.membership.length !== job.membership.length || proposal.membership.some((id, index) => id !== job.membership[index])) return null;
  const tombstones = await store.readTombstones(job.membership);
  if (tombstones.length > 0) return null;
  const afterSlow = sample();
  if (afterSlow === null || Date.parse(claim.expiresAt) <= afterSlow || jobExpired(job, afterSlow, input.maxClockSkewMs)) return null;
  const claimAfter = await store.readLease(input.jobId);
  const rereadNow = sample();
  if (rereadNow === null || claimAfter === null || !authority.matchesClaim(claimAfter) || Date.parse(claimAfter.expiresAt) <= rereadNow || jobExpired(job, rereadNow, input.maxClockSkewMs) || !claimIdentityMatchesJob(claimAfter, job)) return null;
  const controlAfter = await store.readControl();
  const finalNow = sample();
  if (finalNow === null || Date.parse(claimAfter.expiresAt) <= finalNow || jobExpired(job, finalNow, input.maxClockSkewMs)) return null;
  if (controlAfter.state !== "active" || controlAfter.coordinationPolicyEpoch !== input.policyEpoch || controlAfter.coordinationPolicyHash !== input.policyHash || controlAfter.privacyEpoch !== input.privacyEpoch) return null;
  if (claimAfter.acceptedProposalId === null || claimAfter.acceptedManifestHash === null) return null;
  return { proposalId: claimAfter.acceptedProposalId, manifestHash: claimAfter.acceptedManifestHash, claimVersion: claimAfter.version };
}
function claimIdentityMatchesJob(claim: { jobId: string; id: string; ownerHost: string; processingPolicyId: string; coordinationPolicyEpoch: number; coordinationPolicyHash: string; privacyEpoch: number; expiresAt: string }, job: { id: string; ownerHost: string; policyId: string; coordinationPolicyEpoch: number; coordinationPolicyHash: string; privacyEpoch: number; expiresAt: string | null }): boolean {
  if (job.expiresAt !== null && Date.parse(claim.expiresAt) > Date.parse(job.expiresAt)) return false;
  return claim.ownerHost === job.ownerHost && claim.processingPolicyId === job.policyId && claim.coordinationPolicyEpoch === job.coordinationPolicyEpoch && claim.coordinationPolicyHash === job.coordinationPolicyHash && claim.privacyEpoch === job.privacyEpoch && claim.jobId === job.id && claim.id === leasePointId(job.id);
}
import { jobExpired } from "./deadline.js";
import { leasePointId } from "../domain/ids.js";

/** Read a job (safe read). */
export async function readJob(store: ProductionCoordinationStore, jobIdValue: string): Promise<JobRecord | null> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Job read requires a genuine production store");
  return store.readJob(jobIdValue);
}
