import { type MemoryRecord, type TombstoneRecord } from "../domain/records.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import { type TombstoneScope } from "../domain/ids.js";
import type { HostId } from "../types.js";
import type { IngestTombstoneReader } from "../outbox/delivery.js";
export interface CreateTombstoneInput {
    ownerHost: HostId;
    scope: TombstoneScope;
    /** Runtime-domain-verifiable target: tagged state/content/occurrence ID, or episode UUID. */
    targetId: string;
    /** Bare-UUID occurrence targets require an explicit episode selector and exact store lookup. */
    targetKind?: "episode";
    /** Source/provenance IDs that must also become occurrence tombstones (provenance closure). */
    provenanceIds?: readonly string[];
    createdAt: string;
    privacyEpoch: number;
    processingPolicyId: string;
}
/**
 * Insert immutable tombstones (strong ordering/wait, reread) for a target and
 * its provenance source IDs. Mismatched scope/target types fail closed; bare
 * UUID occurrence targets are verified as episodes through an explicit
 * selector and an exact store lookup. Provenance source tombstones are
 * provenance-independent, so the same source converges across forgotten
 * targets (no provenanceId-dependent collision).
 */
export declare function createTombstone(store: ProductionCoordinationStore, input: CreateTombstoneInput): Promise<TombstoneRecord[]>;
export declare function readTombstones(store: ProductionCoordinationStore, targetIds: readonly string[]): Promise<TombstoneRecord[]>;
export interface TombstoneTarget {
    scope: TombstoneScope;
    targetId: string;
}
/** Enumerate occurrence/content/state targets plus provenance closure for a record. */
export declare function tombstoneTargets(record: MemoryRecord): TombstoneTarget[];
/**
 * Fail-closed final visibility. The tombstone batch is read by this function
 * (bounded target expansion + batch-read seam), never trusted from a caller's
 * incomplete list. Occurrence tombstones hide the exact occurrence PERMANENTLY;
 * content/state tombstones block current AND future recurrence.
 *
 * Provenance authority: until Task 10 installs a verifiable content-addressed
 * persisted manifest resolver, ANY manifest-bearing or manifest-only derived
 * record is invisible fail-closed (no caller-supplied structural resolver is
 * ever trusted as visibility authority). RAPTOR `memberIds` count as episode
 * provenance only when EVERY member is directly an exact persisted episode
 * UUID verified through store.readEpisodes; a single child-summary ID makes
 * the record invisible. Curated direct `sourceEpisodeIds` remain
 * exact-verified; `primaryEvidence`/observation ids alone are not closure.
 */
export declare function isVisibleAfterTombstoneCheck(store: ProductionCoordinationStore, record: MemoryRecord): Promise<boolean>;
/**
 * Nominal, frozen, bound tombstone barrier reader. The owner is tied to the
 * PRODUCTION store's pinned owner host; structural `{ownerHost, readTombstones}`
 * fakes and any non-branded store are rejected. The readTombstoned operation
 * validates bounded episode IDs, batch-reads the exact
 * H(owner,"tombstone",episode) points and returns only occurrence-scoped
 * target IDs. There is no public mint and no test binder in dist.
 */
export declare class BoundIngestTombstoneReader implements IngestTombstoneReader {
    #private;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(store: ProductionCoordinationStore, ownerHost: HostId, issuer: symbol);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is BoundIngestTombstoneReader;
    get ownerHost(): HostId;
    readTombstoned(episodeIds: readonly string[]): Promise<readonly string[]>;
}
/** Production Task 8 tombstone barrier reader: requires the branded production store of the owner. */
export declare function createIngestTombstoneReader(store: ProductionCoordinationStore, ownerHost: HostId): BoundIngestTombstoneReader;
