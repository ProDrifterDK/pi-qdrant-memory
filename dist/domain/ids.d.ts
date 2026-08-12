import type { HostId } from "../types.js";
export type TombstoneScope = "occurrence" | "content" | "state";
export interface StateKeyInput {
    host: HostId;
    scope: string;
    projectId?: string | null;
    category: string;
    subject: string;
    predicate: string;
}
/**
 * Logical state identity; it is independent of the current value and owner.
 * The `state:` tag makes the target domain verifiable at runtime.
 */
export declare function stateKey(input: StateKeyInput): string;
/** Reusable value identity under one coordination policy. */
export declare function contentId(policyHash: string, logicalStateKey: string, canonicalValue: unknown): string;
export type EffectiveOrder = `session:${number}` | readonly [string, string, string];
/** Validate the two causal-order encodings permitted by §8.2. */
export declare function validateEffectiveOrder(value: unknown): asserts value is EffectiveOrder;
/** Insert-only occurrence identity. effectiveOrder may be a causal tuple. */
export declare function observationId(policyEpoch: number, logicalContentId: string, primaryEvidenceEpisodeId: string, effectiveOrder: unknown): string;
export declare function evidenceLinkId(observation: string, episode: string, extractorRevision: string | number): string;
export interface EpisodeIdentityInput {
    host: HostId;
    sessionId: string;
    messageId: string;
    part?: string | number;
}
export declare function episodeId(input: EpisodeIdentityInput): string;
export declare function episodeId(host: HostId, sessionId: string, messageId: string, part?: string | number): string;
export interface JobIdentityInput {
    ownerHost: HostId;
    membership: readonly string[];
    policyHash: string;
    extractorRevision: string;
    coordinationPolicyEpoch: number;
    policyIntersectionId: string;
    privacyEpoch: number;
}
export declare function jobId(ownerHost: HostId, membership: readonly string[], policyHash: string, extractorRevision: string, coordinationPolicyEpoch: number, policyIntersectionId: string, privacyEpoch: number): string;
export declare function jobId(input: JobIdentityInput): string;
export declare function manifestHash(memberIds: readonly string[]): string;
/**
 * Tombstone point identity: the exact `H(owner_host,"tombstone",target_id)`
 * formula (spec §13.6.1) so targets and provenance source IDs are directly
 * batch-retrievable. Targets are domain-tagged and verified before insertion.
 */
export declare function tombstoneId(ownerHost: HostId, targetId: string): string;
export interface CoverageIdentityInput {
    ownerHost: HostId;
    episodeId: string;
    extractorRevision: string;
    coordinationPolicyHash: string;
    coordinationPolicyEpoch: number;
    policyIntersectionId: string;
    privacyEpoch: number;
}
/**
 * Deterministic coverage identity: owner + episode + extractor + active
 * coordination hash/epoch + processing-policy intersection + privacy epoch,
 * so coverage is policy-specific and pre-forget coverage can never suppress
 * post-forget work.
 */
export declare function coverageId(input: CoverageIdentityInput): string;
/** Mutable lease point for a job: the lease is separate from immutable job/proposal identity. */
export declare function leasePointId(jobIdValue: string): string;
/** Immutable proposal point identity bound to the job, content hash, epoch and fencing token. */
export declare function proposalIdFor(jobIdValue: string, proposalHash: string, coordinationPolicyEpoch: number, fencingToken: number): string;
/** Runtime domain verification for tombstone targets. */
export declare function isStateTarget(value: unknown): value is string;
export declare function isContentTarget(value: unknown): value is string;
/** Occurrence targets are tagged observations or episode point IDs. */
export declare function isOccurrenceTarget(value: unknown): value is string;
/** Content-addressed proposal hash: binds owner/job/membership/output/epochs/hash/fence. */
export declare function proposalContentHash(input: {
    ownerHost: HostId;
    jobId: string;
    ownerId: string;
    membership: readonly string[];
    content: unknown;
    policyHash: string;
    policyEpoch: number;
    fencingToken: number;
    privacyEpoch: number;
    policyIntersectionId: string;
}): string;
export declare function isTombstoneTarget(scope: TombstoneScope, value: unknown): value is string;
