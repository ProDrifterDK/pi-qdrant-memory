import { type ConflictManifestRecord, type CoverageRecord, type CuratedCurrentRecord, type CuratedMemoryRecord, type EvidenceLinkRecord } from "../domain/records.js";
import { type ProcessingPolicy } from "../domain/policy.js";
import { ProductionCoordinationStore, LeaseAuthority } from "../qdrant/write.js";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
import type { SecretScanner } from "../security/redaction.js";
import { type CurationResult } from "./validate.js";
import { projectEffectiveOrder, compareProjectionOrders } from "./projection.js";
export type OrderComparison = "before" | "after" | "equal" | "within_skew";
export declare const compareEffectiveOrders: typeof compareProjectionOrders;
export declare const deriveEffectiveOrder: typeof projectEffectiveOrder;
export declare function derivedCuratedText(item: {
    category: string;
    scope: string;
    subject: string;
    predicate: string;
    value?: unknown;
    text?: string;
    evidence: readonly string[];
}): string;
export interface MaterializeCurationInput {
    store: ProductionCoordinationStore;
    result: CurationResult;
    /** The exact producer/worker policy intersection (content-addressed, validated). */
    policy: ProcessingPolicy;
    /** Opaque Task 7 bound embedding capability; no active-model/LLM endpoint may substitute. */
    embedding: BoundEmbeddingDestination;
    extractorRevision: string;
    /** Optional injected final scanner (may only further restrict; never promote). */
    scan?: SecretScanner;
}
export interface MaterializationOutcome {
    readonly observations: readonly CuratedMemoryRecord[];
    readonly evidenceLinks: readonly EvidenceLinkRecord[];
    readonly currents: readonly CuratedCurrentRecord[];
    readonly conflicts: readonly ConflictManifestRecord[];
    readonly coverage: readonly CoverageRecord[];
}
export interface HistorySegment {
    readonly stateKey: string;
    readonly contentId: string;
    readonly observationIds: readonly string[];
    readonly primaryEvidenceEpisodeId: string;
    /** Derived from observation eventAt; observations are never mutated. */
    readonly validFrom: string;
    readonly validTo: string | null;
    readonly category?: string;
    readonly scope?: string;
    readonly subject?: string;
    readonly predicate?: string;
    readonly value?: unknown;
    readonly text?: string;
}
/**
 * A->B->A history folding over immutable observations: observations are
 * ordered by strict causal order and CONSECUTIVE equal canonical content folds
 * into ONE interval, so A->B->A preserves TWO A intervals. Every observation
 * belongs to exactly one segment (no superseded state reuse or cycle) and the
 * derived valid_from/valid_to come from observation eventAt without mutating
 * them. Semantic near-duplicates are best-effort only and never folded here.
 */
export declare function foldHistorySegments(observations: readonly CuratedMemoryRecord[], maxClockSkewMs: number): readonly HistorySegment[];
/**
 * Task 9 materialization: after strict validation and an accepted active-
 * policy proposal, re-verify the acceptance with FRESH control/claim/proposal/
 * tombstone reads, intersect the source/local/active policies, and use ONLY
 * the opaque BoundEmbeddingDestination. The canonical curated text is
 * structurally redacted AND final-scanned before BGE-M3 embedding; a scanner
 * reject/error, embedding failure or policy/revocation change throws and
 * leaves the job retryable with NO current write and no text egress. Control
 * is reread after the embedding call; observations/evidence links and
 * policy-specific state/current ids are inserted only if the epochs and the
 * destination still match. Same evidence under a new policy epoch creates new
 * IDs and hides the old view without mutating old records.
 */
export type MaterializeCurationOptions = MaterializeCurationInput;
export declare function materializeCuration(authority: LeaseAuthority, input: MaterializeCurationOptions): Promise<MaterializationOutcome>;
/** Deterministic content-addressed evidence digest for audit trails. */
export declare function evidenceDigest(evidence: readonly string[]): string;
