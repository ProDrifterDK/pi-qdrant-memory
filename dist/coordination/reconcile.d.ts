import { type CoverageRecord, type EpisodeRecord } from "../domain/records.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import type { HostId } from "../types.js";
export interface MarkCoverageInput {
    ownerHost: HostId;
    episodeId: string;
    extractorRevision: string;
    policyEpoch: number;
    policyHash: string;
    privacyEpoch: number;
    createdAt: string;
    processingPolicyId: string;
}
/**
 * Coverage truth is deterministic coverage IDs + batch retrieve; the scan
 * cursor is optimization only. The identity is policy-specific: owner +
 * episode + extractor + active coordination hash/epoch + processing-policy
 * intersection + privacy epoch, so pre-forget coverage can never suppress
 * post-forget work.
 */
export declare function markCoverage(store: ProductionCoordinationStore, input: MarkCoverageInput): Promise<CoverageRecord>;
export interface EpisodeSlice {
    episodes: readonly EpisodeRecord[];
    nextOffset?: string;
}
export interface FindMissingInput {
    store: ProductionCoordinationStore;
    /** ID-offset bounded slice scan of persisted episodes; never a timestamp cursor. */
    listEpisodes(offset: string | undefined, limit: number): Promise<EpisodeSlice>;
    extractorRevision: string;
    policyEpoch: number;
    policyHash: string;
    policyIntersectionId: string;
    privacyEpoch: number;
    batchSize?: number;
    /** Turn bound: maximum number of slices scanned per call (default 4). */
    maxSlices?: number;
    /** Turn bound: maximum number of missing results returned per call (default 1024). */
    maxMissing?: number;
    offset?: string;
    signal?: AbortSignal;
}
export interface FindMissingResult {
    missing: EpisodeRecord[];
    scanned: number;
    nextOffset?: string;
    truncated?: boolean;
}
/** Scan episodes in bounded slices, batch-retrieve their coverage IDs, enqueue missing/late episodes. */
export declare function findMissingEpisodes(input: FindMissingInput): Promise<FindMissingResult>;
/**
 * One bounded slice of the full operator sweep: externally resumable via the
 * returned ID-offset cursor, abortable, and safe to re-run with overlap.
 */
/**
 * One bounded slice of the sweep, EXTERNALLY RESUMABLE: the caller's `offset`
 * (and any overlap state) is passed straight through to the scanner, so
 * one-slice periodic calls actually resume. EOF (no nextOffset) completes the
 * sweep; the next normal periodic invocation with `offset: undefined` begins a
 * fresh full cycle, which — together with bounded overlap — guarantees an
 * episode inserted behind a saved cursor is eventually coverage-checked.
 */
export declare function reconcileCoverage(input: Omit<FindMissingInput, "maxSlices">): Promise<FindMissingResult>;
