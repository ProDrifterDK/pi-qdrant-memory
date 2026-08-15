import { LeaseAuthority, ProductionCoordinationStore, QuiescenceProof } from "../qdrant/write.js";
export { ProductionCoordinationStore, QuiescenceProof, createQdrantCoordinationStore } from "../qdrant/write.js";
import type { ControlRecord, RaptorSummaryRecord } from "../domain/records.js";
import type { IngestControlReader } from "../outbox/delivery.js";
/**
 * Thin Production-brand-only coordination surface. There is NO structural-
 * store mutation path: inputs that are not a genuine Production store fail
 * closed, and every mutation delegates to the store's named safe method (all
 * protocol implementations live lexically inside qdrant/write.ts and are not
 * exported anywhere).
 */
/** Read the single owner-independent collection-control point (strong, exact payload). */
export declare function readControl(store: ProductionCoordinationStore): Promise<ControlRecord>;
/** Reread the Task 3 insert-only v0 bootstrap point; later mutations belong only to the store. */
export declare function initializeControl(store: ProductionCoordinationStore, initial: ControlRecord): Promise<ControlRecord>;
/** Read the current control for a CAS cycle; callers reread before and after every transition. */
export declare function readForUpdate(store: ProductionCoordinationStore): Promise<ControlRecord>;
/** Bounded frozen control snapshot for curation barriers (exact-once reads, plain frozen locals). */
export interface ControlSnapshot {
    readonly state: "active" | "draining" | "retired";
    readonly privacyEpoch: number;
    readonly coordinationPolicyEpoch: number;
    readonly coordinationPolicyHash: string;
    readonly revokedDestinationIds: readonly string[];
}
/** Reread control privacy/coordination epochs + revocations as ONE bounded frozen snapshot. */
export declare function readControlSnapshot(store: ProductionCoordinationStore): Promise<ControlSnapshot>;
/** CAS active->draining, clears active generation and derived-current visibility; workers stop claiming/egressing. */
export declare function beginPolicyDrain(store: ProductionCoordinationStore, input: {
    now: number;
}): Promise<ControlRecord>;
/**
 * Bounded, abortable quiescence over the genuine store: polls its own strong
 * control/lease state, re-pins the control identity and returns an
 * unforgeable QuiescenceProof bound to THIS store's private authority scope.
 * A proof minted by any other store is rejected at activation.
 */
export declare function waitForOldLeasesToQuiesce(store: ProductionCoordinationStore, input: {
    retiredEpoch: number;
    maxLeaseMs: number;
    maxClockSkewMs: number;
    timeoutMs?: number;
    pollIntervalMs?: number;
    now?: () => number;
    signal?: AbortSignal;
}): Promise<QuiescenceProof>;
/** After the proof and the bounded LLM timeout, CAS draining->active with epoch+1 and the new hash. */
export declare function activatePolicyEpoch(store: ProductionCoordinationStore, input: {
    proof: QuiescenceProof;
    nextPolicyHash: string;
    memoryModelTimeoutMs: number;
    signal?: AbortSignal;
}): Promise<ControlRecord>;
/** Rotate the coordination policy: drain, quiesce (branded proof), LLM timeout, activate epoch+1/hash. */
export declare function rotateCoordinationPolicy(store: ProductionCoordinationStore, input: {
    nextPolicyHash: string;
    maxLeaseMs: number;
    maxClockSkewMs: number;
    memoryModelTimeoutMs: number;
    quiesceTimeoutMs?: number;
    now: number;
    signal?: AbortSignal;
}): Promise<ControlRecord>;
/** Forget barrier on the same control point: privacy epoch +1, active generation cleared, barrier recorded. */
export declare function beginForgetBarrier(store: ProductionCoordinationStore, input: {
    now: number;
    revokedDestinationIds?: readonly string[];
}): Promise<ControlRecord>;
/** Exact stable RAPTOR checkpoint over control/job/lease/tombstones and bound destinations. */
export declare function readRaptorBarrier(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    destinationIds: readonly string[];
    evidenceIds: readonly string[];
}): Promise<string>;
/** Capability-gated immutable RAPTOR node insert with exact vector-aware readback. */
export declare function writeRaptorSummary(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    record: RaptorSummaryRecord;
    destinationIds: readonly string[];
    evidenceIds: readonly string[];
}): Promise<RaptorSummaryRecord>;
/** Capability-gated one-winner active-generation CAS. */
export declare function publishRaptorGeneration(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    expected: ControlRecord;
    generationId: string;
    destinationIds: readonly string[];
    evidenceIds: readonly string[];
}): Promise<boolean>;
/**
 * Nominal, frozen, privately branded bound control reader: bounded revocation
 * snapshot from the single control point of its genuine production store.
 * Forged prototypes and monkeypatched statics fail the brand check; a
 * structural store can never mint a branded reader.
 */
export declare class BoundIngestControlReader implements IngestControlReader {
    #private;
    readonly policyHash: string;
    readonly policyEpoch: number;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(store: ProductionCoordinationStore, input: {
        policyHash: string;
        policyEpoch: number;
    }, issuer: symbol);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is BoundIngestControlReader;
    read(): Promise<Awaited<ReturnType<IngestControlReader["read"]>>>;
}
/** Production Task 7 control reader: bounded revocation snapshot from the single control point. */
export declare function createIngestControlReader(store: ProductionCoordinationStore, input: {
    policyHash: string;
    policyEpoch: number;
}): BoundIngestControlReader;
