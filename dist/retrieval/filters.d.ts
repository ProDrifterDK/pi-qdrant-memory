import type { HostId } from "../types.js";
export type RetrievalLane = "current" | "historical" | "episodes" | "curated" | "raptor" | "exact";
export type FilterScalar = string | number | boolean;
export type FilterCondition = {
    key: string;
    match: {
        value: FilterScalar;
    };
} | {
    key: string;
    match: {
        any: FilterScalar[];
    };
} | {
    key: string;
    range: {
        gt?: string;
        gte?: string;
        lte?: string;
    };
} | {
    is_null: {
        key: string;
    };
};
export declare class GuardedLaneFilter {
    #private;
    readonly must: FilterCondition[];
    readonly must_not: FilterCondition[];
    readonly should: FilterCondition[];
    constructor(input: {
        must: FilterCondition[];
        mustNot: FilterCondition[];
        should: FilterCondition[];
    }, issuer: symbol);
    static isValid(value: unknown): value is GuardedLaneFilter;
}
export interface GuardedLaneFilterInput {
    ownerHost: HostId;
    lane: RetrievalLane;
    projectId: string;
    global: boolean;
    now: number;
    maxClockSkewMs: number;
    privacyEpoch: number;
    coordinationPolicyEpoch: number;
    activeGeneration?: string;
    exactRecordTypes?: readonly ("episode" | "curated_memory" | "curated_current")[];
    after?: string;
    before?: string;
}
/** Build the immutable mandatory Qdrant filter for one internal lane. No model argument reaches this function. */
export declare function laneFilter(input: GuardedLaneFilterInput): GuardedLaneFilter;
