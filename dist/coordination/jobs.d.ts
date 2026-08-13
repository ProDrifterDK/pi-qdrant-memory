import { ProductionCoordinationStore, LeaseAuthority } from "../qdrant/write.js";
export { validateSortedMembership, jobIdFor, proposalHashFor } from "../qdrant/write.js";
import type { HostId } from "../types.js";
import type { ConflictManifestRecord, CuratedCurrentRecord, CuratedMemoryRecord, EvidenceLinkRecord, JobRecord, ProposalRecord } from "../domain/records.js";
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
    expiresAt: string | null;
}
/** Generic Task 8 proposal payload. Task 9 curation validates its strict
 * envelope separately at the worker/materialization/completion boundaries. */
export interface ProposalContent {
    summary?: string;
    observations?: unknown;
    [key: string]: unknown;
}
export interface WriteProposalInput {
    membership: readonly string[];
    content: ProposalContent;
    createdAt: string;
}
export interface ActiveAcceptance {
    readonly proposalId: string;
    readonly manifestHash: string;
    readonly claimVersion: number;
    /** Exact canonical snapshots proven stable by the acceptance sandwich. */
    readonly job: JobRecord;
    readonly proposal: ProposalRecord;
}
/** Immutable explicit-membership job point; identity is enforced by the record parser. */
export declare function createJob(store: ProductionCoordinationStore, input: CreateJobInput): Promise<JobRecord>;
/**
 * Write an immutable proposal bound to the genuine authority. Everything
 * (identity/fence/policy/privacy/deadline/fresh clocks/exact readback) is
 * enforced inside the store's lexical safe method.
 */
export declare function writeProposal(store: ProductionCoordinationStore, authority: LeaseAuthority, input: WriteProposalInput): Promise<ProposalRecord>;
/** Thin safe wrapper over the ONE complete safe acceptance operation. */
export declare function acceptProposal(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    proposalId: string;
}): Promise<LeaseAuthority | null>;
export declare function readActiveAcceptance(store: ProductionCoordinationStore, authority: LeaseAuthority): Promise<ActiveAcceptance | null>;
/** Terminal completion is capability-gated and succeeds only after exact immutable readbacks. */
export declare function completeJob(store: ProductionCoordinationStore, authority: LeaseAuthority): Promise<boolean>;
/** Read a job (safe read). */
export declare function readJob(store: ProductionCoordinationStore, jobIdValue: string): Promise<JobRecord | null>;
/** Read an immutable curated observation through the genuine store. */
export declare function readObservation(store: ProductionCoordinationStore, authority: LeaseAuthority, observationId: string): Promise<CuratedMemoryRecord | null>;
/** Read the policy-epoch-specific current view through the genuine accepted authority. */
export declare function readCurrent(store: ProductionCoordinationStore, authority: LeaseAuthority, currentId: string): Promise<CuratedCurrentRecord | null>;
/** Insert an immutable curated observation through the genuine store. */
export declare function insertObservation(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    record: CuratedMemoryRecord;
}): Promise<CuratedMemoryRecord>;
/** Insert an immutable evidence link through the genuine accepted authority. */
export declare function insertEvidenceLink(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    record: EvidenceLinkRecord;
}): Promise<EvidenceLinkRecord>;
/** Insert a content-addressed conflict manifest through the genuine accepted authority. */
export declare function insertConflictManifest(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    record: ConflictManifestRecord;
}): Promise<ConflictManifestRecord>;
/** OCC update of the policy-epoch-specific curated current. */
export declare function upsertCuratedCurrent(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    record: CuratedCurrentRecord;
    expectedVersion: number | null;
}): Promise<CuratedCurrentRecord | null>;
