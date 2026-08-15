import { type TombstoneScope } from "../domain/ids.js";
import type { HostId, RuntimeConfig } from "../types.js";
export type ForgetScope = TombstoneScope;
export interface ForgetSelection {
    episodeId?: string;
    observationId?: string;
    curatedCurrentId?: string;
    contentId?: string;
    stateKey?: string;
    targetId?: string;
}
export interface CurrentSelection {
    id: string;
    observationId?: string;
    contentId?: string;
    stateKey?: string;
    sourceEpisodeIds?: readonly string[];
    evidenceEpisodeIds?: readonly string[];
    manifestHash?: string;
}
export interface ForgetPlanDependencies {
    resolveCurrent?(id: string): Promise<CurrentSelection | null>;
    readCurrent?(id: string): Promise<CurrentSelection | null>;
    resolveRecord?(selection: ForgetSelection): Promise<Partial<CurrentSelection> & {
        episodeIds?: readonly string[];
    } | null>;
}
export interface ForgetPlanInput extends ForgetPlanDependencies {
    ownerHost?: HostId;
    selection: ForgetSelection;
    /** Optional already-reread current view seam for embedders/tests. */
    current?: CurrentSelection;
    scope?: ForgetScope;
    requestedAt?: string | Date;
}
export interface ForgetTargetClosure {
    readonly episodes: readonly string[];
    readonly observations: readonly string[];
    readonly currentViews: readonly string[];
    readonly evidence: readonly string[];
    readonly generations: readonly string[];
    readonly manifests: readonly string[];
    readonly outboxProposals: readonly string[];
}
export interface ForgetPlan {
    readonly id: string;
    readonly operation: "forget";
    readonly ownerHost?: HostId;
    readonly scope: ForgetScope;
    readonly targets: readonly string[];
    readonly closure: ForgetTargetClosure;
    readonly selection: ForgetSelection;
    readonly recurrenceBlocked: boolean;
    readonly requestedAt: string;
    readonly requiresHumanApproval: true;
    readonly logicalBarrier: "tombstone_and_epoch";
    readonly physicalDeletion: "eventual_no_backup_claim";
}
export interface ForgetDependencies extends ForgetPlanDependencies {
    readControl?(): Promise<{
        ownerHost: HostId;
        privacyEpoch: number;
        activeGeneration: string | null;
    }>;
    beginForgetBarrier?(input: {
        now: number;
        expectedEpoch?: number;
        signal?: AbortSignal;
    }): Promise<{
        privacyEpoch: number;
        activeGeneration: string | null;
    } | unknown>;
    createTombstones?(input: {
        scope: ForgetScope;
        targetIds: readonly string[];
        provenanceIds: readonly string[];
        privacyEpoch?: number;
        signal?: AbortSignal;
    }): Promise<unknown>;
    readTombstones?(targetIds: readonly string[]): Promise<readonly unknown[]>;
    invalidateCurrentViews?(): Promise<void>;
    invalidateCoverage?(): Promise<void>;
    quarantineOutbox?(): Promise<void>;
    reconcile?(): Promise<void>;
    rereadBarrier?(): Promise<{
        privacyEpoch: number;
        activeGeneration: string | null;
    } | unknown>;
}
export interface RunForgetInput {
    plan: ForgetPlan;
    approvedPlanId: string;
    signal?: AbortSignal;
    now?: number;
}
export interface RunForgetResult {
    readonly ok: true;
    readonly planId: string;
    readonly scope: ForgetScope;
    readonly logicalInvisible: true;
    readonly recurrenceBlocked: boolean;
    readonly physicalDeletion: "eventual_no_backup_claim";
    readonly tombstonesWritten: boolean;
    readonly barrierConfirmed: boolean;
}
/** Read one current view through the named read-only admin transport. */
export declare function readQdrantCurrentSelection(config: RuntimeConfig, id: string, fetchImpl?: typeof fetch): Promise<CurrentSelection | null>;
export declare function planForget(input: ForgetPlanInput): Promise<ForgetPlan>;
export declare const createForgetPlan: typeof planForget;
/** Apply a plan only after exact human approval and a reread barrier. */
export declare function runForget(input: RunForgetInput, deps?: ForgetDependencies): Promise<RunForgetResult>;
export declare const applyForget: typeof runForget;
export declare const forget: typeof runForget;
/** Small helper for callers that need to validate domain-tagged IDs before
 * invoking a durable store. Plain IDs are accepted in plans because operator
 * selectors may be aliases; production adapters should map them to tagged
 * tombstone targets before writing. */
export declare function isForgetTarget(scope: ForgetScope, value: string): boolean;
