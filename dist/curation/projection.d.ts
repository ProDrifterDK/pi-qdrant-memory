import { type EffectiveOrder } from "../domain/ids.js";
import type { EpisodeRecord, CuratedMemoryRecord } from "../domain/records.js";
export interface ProjectionItem {
    readonly category: string;
    readonly scope: string;
    readonly subject: string;
    readonly predicate: string;
    readonly evidence: readonly string[];
    readonly value?: unknown;
    readonly text?: string;
    readonly confidence?: number;
}
export interface CurationProjection {
    readonly stateKey: string;
    readonly contentId: string;
    readonly effectiveOrder: EffectiveOrder;
    readonly primary: EpisodeRecord;
    readonly evidence: readonly EpisodeRecord[];
    readonly observationId: string;
    readonly currentId: string;
    readonly text: string;
}
export declare function primaryEvidence(evidence: readonly EpisodeRecord[]): EpisodeRecord;
export declare function projectEffectiveOrder(evidence: readonly EpisodeRecord[], value: string): EffectiveOrder;
export declare function projectCurationText(item: ProjectionItem): string;
export declare function projectCurationItem(host: "pi" | "prime", policyHash: string, policyEpoch: number, item: ProjectionItem, episodes: ReadonlyMap<string, EpisodeRecord>): CurationProjection;
export interface ConflictAggregateProjection {
    readonly members: readonly string[];
    readonly representatives: readonly CuratedMemoryRecord[];
    readonly sourceEpisodeIds: readonly string[];
    readonly effectiveOrder: EffectiveOrder;
    readonly createdAt: string;
}
/** Pure aggregate over the complete sorted conflict membership. Arrival order is
 * never consulted: members, logical representatives, source closure, causal
 * order and envelope timestamp all derive from the immutable member set. */
export declare function projectConflictAggregate(membersInput: readonly CuratedMemoryRecord[]): ConflictAggregateProjection;
/** Shared OCC order comparator used by materialization and completion. */
export declare function effectiveOrderTuple(value: EffectiveOrder): readonly [string, string, string] | null;
export declare function compareProjectionOrders(a: EffectiveOrder, b: EffectiveOrder, skew: number): "before" | "after" | "equal" | "within_skew";
