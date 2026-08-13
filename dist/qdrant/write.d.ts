import { type ConflictManifestRecord, type ControlRecord, type CoverageRecord, type CuratedCurrentRecord, type CuratedMemoryRecord, type EpisodeRecord, type EvidenceLinkRecord, type JobRecord, type LeaseRecord, type MemoryRecord, type ProcessingPolicyRecord, type ProposalRecord, type TombstoneRecord } from "../domain/records.js";
import type { AuthorizedDestination, HostId, RuntimeConfig } from "../types.js";
import { type QdrantClientOptions, type QdrantPoint } from "./client.js";
import { RootWorkerContext } from "../coordination/root.js";
export { QdrantContentHashCollisionError, QdrantLegacyEpisodeHashError, QDRANT_CONTENT_HASH_COLLISION, QDRANT_LEGACY_EPISODE_HASH } from "../domain/qdrant-errors.js";
import type { ClaimLeaseInput } from "../coordination/leases.js";
import type { CreateJobInput, ProposalContent, WriteProposalInput } from "../coordination/jobs.js";
import type { CreateTombstoneInput } from "../coordination/tombstones.js";
import type { MarkCoverageInput } from "../coordination/reconcile.js";
type Payload = Record<string, unknown>;
export declare function recordPayload(record: MemoryRecord): Payload;
/** Strict inverse of recordPayload; status/secret_scan are wire-only defaults except for episode. */
export declare function recordFromPayload(value: unknown, ownerHost: "pi" | "prime", semanticVector?: readonly number[]): MemoryRecord;
/** Strict inverse of recordPayload for coordination points; status/secret_scan are wire-only defaults. */
export declare function coordinationRecordFromPayload(value: unknown, ownerHost: "pi" | "prime"): JobRecord | LeaseRecord | ProposalRecord | CoverageRecord | TombstoneRecord;
/** Strict episode readback parser for internal coordination reads (tombstone target verification). */
export declare function episodeRecordFromPayload(value: unknown, ownerHost: "pi" | "prime"): EpisodeRecord;
export interface QdrantDestinationFactoryInput {
    options: QdrantClientOptions;
    destination: AuthorizedDestination;
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    nodeId?: string;
    coordinationPolicyHash: string;
    coordinationPolicyEpoch: number;
}
export interface QdrantDestinationFactory {
    bind(destination: AuthorizedDestination): BoundQdrantDestination;
}
/**
 * ONE shared exact vector-aware Episode point parser (payload + named semantic
 * vector): exact physical/logical ID, owner, record shape, exact canonical
 * wire payload, exact named vector and vector-bound contentHash must ALL
 * verify. Missing vector, malformed payload, hash mismatch or any structural
 * deviation returns null (fail closed). Reused by BoundQdrantDestination and
 * by ProductionCoordinationStore so episode reads never diverge.
 */
export declare function parseBoundEpisodePoint(point: QdrantPoint, ownerHost: "pi" | "prime"): EpisodeRecord | null;
/**
 * Narrow internal legacy-hash classifier: a present exact-ID Episode point
 * whose stored contentHash is valid ONLY under the former vector-excluding
 * formula (vector present, hash computed without it). Distinct from both a
 * verified current point and an arbitrary malformed hash.
 */
export declare function isLegacyEpisodePoint(point: QdrantPoint, ownerHost: "pi" | "prime"): boolean;
/**
 * Nominal, frozen, privately branded bound Qdrant destination. It snapshots
 * the endpoint/owner/collection/coordination identity and binds insert/readback
 * once; forged prototypes and monkeypatched statics fail the brand check.
 */
export declare class BoundQdrantDestination {
    #private;
    readonly endpoint: string;
    readonly destination: AuthorizedDestination;
    readonly ownerHost: "pi" | "prime";
    readonly collection: "pi_memory" | "prime_memory";
    readonly coordination: {
        readonly policyHash: string;
        readonly policyEpoch: number;
    };
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(input: {
        endpoint: string;
        destination: AuthorizedDestination;
        ownerHost: "pi" | "prime";
        collection: "pi_memory" | "prime_memory";
        coordination: {
            policyHash: string;
            policyEpoch: number;
        };
        transportToken: object;
        insertAndReadback: (record: ProcessingPolicyRecord | EpisodeRecord) => Promise<"inserted" | "existing">;
        retrieve: <T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string) => Promise<T | null>;
    }, issuer: symbol);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is BoundQdrantDestination;
    insertAndReadback(record: ProcessingPolicyRecord | EpisodeRecord): Promise<"inserted" | "existing">;
    retrieve<T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string): Promise<T | null>;
    /** Opaque per-instance transport identity; compared by `===` in the ingest bundle. */
    get transport(): object;
}
/** Create a closure that snapshots one canonical endpoint/client/destination pairing. The raw session is constructed LEXICALLY from validated options. */
export declare function createQdrantDestinationFactory(input: QdrantDestinationFactoryInput): QdrantDestinationFactory;
/** Bind an exact expected identity; callers cannot pass an independent allowlist. */
export declare function bindQdrantDestination(factory: QdrantDestinationFactory, destination: AuthorizedDestination): BoundQdrantDestination;
/**
 * The PRODUCTION coordination store exposes NO raw mutators (no control
 * compare-and-swap, lease/job/proposal/coverage/tombstone inserts, generic
 * upsert, session, writer or client escape). Its public
 * surface is validated READS plus the opaque transport token; every mutation
 * flows through the NAMED SAFE high-level methods on this class
 * (claim/renew/release/accept/createJob/writeProposal/markCoverage/
 * createTombstone + named control transitions), which delegate to
 * package-internal `...OnProtocol` implementations over the TRUE #-private
 * `#protocol` field.
 * There is NO exported registry, facade, register/resolve function or raw
 * protocol escape anywhere in the package: `#protocol` is unreachable from
 * outside this class.
 */
export declare class ProductionCoordinationStore {
    #private;
    readonly ownerHost: "pi" | "prime";
    readonly endpoint: string;
    readonly collection: "pi_memory" | "prime_memory";
    readonly maxClockSkewMs: number;
    /** Public constructor is unusable without the module-private issuer symbol; the raw session is constructed LEXICALLY from validated options. */
    constructor(options: QdrantClientOptions, issuer: symbol, sharedSession?: object);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is ProductionCoordinationStore;
    /** Opaque per-instance transport identity; compared by `===` in the ingest bundle. */
    get transport(): object;
    private internalPolicy;
    readOne<T extends JobRecord | LeaseRecord | ProposalRecord | CoverageRecord | TombstoneRecord>(recordType: T["recordType"], id: string): Promise<T | null>;
    readControl(): Promise<ControlRecord>;
    readLease(jobIdValue: string): Promise<LeaseRecord | null>;
    readJob(jobIdValue: string): Promise<JobRecord | null>;
    readProposal(id: string): Promise<ProposalRecord | null>;
    readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]>;
    readCoverage(coverageIds: readonly string[]): Promise<CoverageRecord[]>;
    scrollLeases(offset?: string, limit?: number): Promise<{
        leases: LeaseRecord[];
        nextOffset?: string;
    }>;
    /** Bounded authoritative job discovery for crash-resume selection. */
    scrollJobs(offset?: string, limit?: number): Promise<{
        jobs: JobRecord[];
        nextOffset?: string;
    }>;
    readEpisode(episodeIdValue: string): Promise<EpisodeRecord | null>;
    readEpisodes(episodeIds: readonly string[]): Promise<EpisodeRecord[]>;
    private assertAcceptedAuthorityBase;
    private assertCuratedRecordAgainstJob;
    claimLease(worker: RootWorkerContext, input: ClaimLeaseInput): Promise<LeaseAuthority | null>;
    renewLease(authority: LeaseAuthority): Promise<LeaseAuthority | null>;
    releaseLease(authority: LeaseAuthority): Promise<boolean>;
    acceptLease(authority: LeaseAuthority, proposalId: string): Promise<LeaseAuthority | null>;
    acceptProposal(authority: LeaseAuthority, input: {
        proposalId: string;
    }): Promise<LeaseAuthority | null>;
    createJob(input: CreateJobInput): Promise<JobRecord>;
    completeJob(authority: LeaseAuthority): Promise<boolean>;
    writeProposal(authority: LeaseAuthority, input: WriteProposalInput): Promise<ProposalRecord>;
    markCoverage(authority: LeaseAuthority, input: MarkCoverageInput): Promise<CoverageRecord>;
    readObservation(authority: LeaseAuthority, id: string): Promise<CuratedMemoryRecord | null>;
    readCurrent(authority: LeaseAuthority, id: string): Promise<CuratedCurrentRecord | null>;
    readConflictManifest(authority: LeaseAuthority, id: string): Promise<ConflictManifestRecord | null>;
    insertObservation(authority: LeaseAuthority, input: {
        record: CuratedMemoryRecord;
    }): Promise<CuratedMemoryRecord>;
    insertEvidenceLink(authority: LeaseAuthority, input: {
        record: EvidenceLinkRecord;
    }): Promise<EvidenceLinkRecord>;
    insertConflictManifest(authority: LeaseAuthority, input: {
        record: ConflictManifestRecord;
    }): Promise<ConflictManifestRecord>;
    upsertCuratedCurrent(authority: LeaseAuthority, input: {
        record: CuratedCurrentRecord;
        expectedVersion: number | null;
    }): Promise<CuratedCurrentRecord | null>;
    createTombstone(input: CreateTombstoneInput): Promise<TombstoneRecord[]>;
    initializeControl(initial: ControlRecord): Promise<ControlRecord>;
    beginPolicyDrain(input: {
        now: number;
    }): Promise<ControlRecord>;
    waitForOldLeasesToQuiesce(input: {
        retiredEpoch: number;
        maxLeaseMs: number;
        maxClockSkewMs: number;
        timeoutMs?: number;
        pollIntervalMs?: number;
        now?: () => number;
        signal?: AbortSignal;
    }): Promise<QuiescenceProof>;
    activatePolicyEpoch(input: {
        proof: QuiescenceProof;
        nextPolicyHash: string;
        memoryModelTimeoutMs: number;
        signal?: AbortSignal;
    }): Promise<ControlRecord>;
    rotateCoordinationPolicy(input: {
        nextPolicyHash: string;
        maxLeaseMs: number;
        maxClockSkewMs: number;
        memoryModelTimeoutMs: number;
        quiesceTimeoutMs?: number;
        now: number;
        signal?: AbortSignal;
    }): Promise<ControlRecord>;
    beginForgetBarrier(input: {
        now: number;
    }): Promise<ControlRecord>;
}
/** Production seam: validated OPTIONS in, ONLY the safe store out (never accepts/returns a raw writer/session). */
export declare function createQdrantCoordinationStore(options: QdrantClientOptions): ProductionCoordinationStore;
export interface QdrantSafeBundleInput {
    options: QdrantClientOptions;
    destination: AuthorizedDestination;
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    nodeId?: string;
    coordinationPolicyHash: string;
    coordinationPolicyEpoch: number;
}
export interface QdrantSafeBundle {
    store: ProductionCoordinationStore;
    qdrant: QdrantDestinationFactory;
    transport: object;
}
/**
 * ONE safe construction for the ingest bundle: a single LEXICAL session
 * serves BOTH the production store and the destination factory, so the exact
 * transport-object binding (store.transport === bound-destination.transport)
 * holds without any raw session being exported, returned or accepted.
 */
export declare function createQdrantSafeBundle(input: QdrantSafeBundleInput): QdrantSafeBundle;
interface LeaseAuthorityState {
    store: ProductionCoordinationStore;
    /** Module-private per-store authority scope: minted ONLY from the owning store's lexical methods. */
    scope: object;
    worker: RootWorkerContext;
    ownerHost: HostId;
    nodeId: string;
    jobId: string;
    leasePointIdValue: string;
    ownerId: string;
    version: number;
    fencingToken: number;
    state: "leased" | "accepted" | "released";
    acceptedProposalId: string | null;
    acceptedManifestHash: string | null;
    contentHash: string;
    processingPolicyId: string;
    coordinationPolicyHash: string;
    coordinationPolicyEpoch: number;
    privacyEpoch: number;
    expiresAt: string;
    leaseMs: number;
    jobDeadline: string | null;
    maxClockSkewMs: number;
}
export declare class LeaseAuthority {
    #private;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(state: LeaseAuthorityState, issuer: symbol);
    /** Brand check: only genuine in-module authorities pass; forged prototypes and structural objects fail. */
    static isValid(value: unknown): value is LeaseAuthority;
    /** Exact store binding (object identity); never leaks the live store. */
    matchesStore(store: ProductionCoordinationStore): boolean;
    /** Private per-store scope binding; only the owning store's lexical methods can mint or consume. */
    matchesScope(scope: object): boolean;
    /** EXACT current-claim match: every identity/liveness field must equal the persisted claim. */
    matchesClaim(claim: LeaseRecord): boolean;
    /** Trusted fresh clock: delegates to the private bound worker; validates every call. */
    now(): number;
    get ownerHost(): HostId;
    get nodeId(): string;
    get jobId(): string;
    get ownerId(): string;
    get version(): number;
    get fencingToken(): number;
    get state(): "leased" | "accepted" | "released";
    get acceptedProposalId(): string | null;
    get acceptedManifestHash(): string | null;
    get contentHash(): string;
    get processingPolicyId(): string;
    get coordinationPolicyHash(): string;
    get coordinationPolicyEpoch(): number;
    get privacyEpoch(): number;
    get expiresAt(): string;
    get leaseMs(): number;
    get jobDeadline(): string | null;
    get maxClockSkewMs(): number;
}
/** Conservative lease expiry: skew may duplicate work but never authorizes stale publication. */
export declare function isLeaseExpired(lease: Pick<LeaseRecord, "expiresAt">, now: number, maxClockSkewMs: number): boolean;
/** Raw claim state (including released) for diagnostics and quiescence checks. */
export declare function validateSortedMembership(membership: readonly string[]): void;
export declare function jobIdFor(input: Pick<CreateJobInput, "ownerHost" | "membership" | "policyHash" | "policyEpoch" | "extractorRevision" | "policyIntersectionId" | "privacyEpoch">): string;
/** Content-addressed proposal hash: binds owner/job/membership/output/epochs/hash/fence/privacy. */
export declare function proposalHashFor(input: {
    ownerHost: HostId;
    jobId: string;
    ownerId: string;
    membership: readonly string[];
    content: ProposalContent;
    policyHash: string;
    policyEpoch: number;
    fencingToken: number;
    privacyEpoch: number;
    policyIntersectionId: string;
}): string;
/** Unforgeable quiescence proof: branded, tied to the exact draining control identity. */
export declare class QuiescenceProof {
    #private;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(control: ControlRecord, completedAt: number, scope: object, issuer: symbol);
    /** Private per-store scope binding; only the owning store's lexical methods can mint or consume. */
    matchesScope(scope: object): boolean;
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is QuiescenceProof;
    matches(control: ControlRecord): boolean;
}
