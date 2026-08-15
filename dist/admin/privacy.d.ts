import type { ControlRecord } from "../domain/records.js";
import type { HostId } from "../types.js";
export interface PrivacyRevokePlan {
    readonly id: string;
    readonly operation: "privacy_revoke";
    readonly ownerHost: HostId;
    readonly fromPrivacyEpoch: number;
    readonly toPrivacyEpoch: number;
    readonly destinationIds: readonly string[];
    readonly requestedAt: string;
    readonly reason: string;
    readonly drainsWorkers: true;
    readonly invalidatesGeneration: true;
    readonly inFlightCallsCannotBeRevoked: true;
}
export interface PrivacyRevokePlanInput {
    ownerHost: HostId;
    /** A control snapshot is preferred; scalar fields are a test/CLI seam. */
    control?: Pick<ControlRecord, "ownerHost" | "privacyEpoch" | "activeGeneration" | "state">;
    currentPrivacyEpoch?: number;
    destinationIds?: readonly string[];
    requestedAt?: string | Date;
    reason?: string;
}
export type PrivacyControlSnapshot = Pick<ControlRecord, "ownerHost" | "privacyEpoch" | "activeGeneration" | "state" | "revokedDestinationIds">;
export interface PrivacyRevokeDependencies {
    readControl?(): Promise<PrivacyControlSnapshot>;
    beginDrain?(input: {
        now: number;
    }): Promise<unknown>;
    waitForQuiescence?(input: {
        retiredEpoch: number;
        signal?: AbortSignal;
    }): Promise<unknown>;
    /** The only write seam. Implementations must CAS the collection control. */
    advancePrivacyEpoch?(input: {
        expectedEpoch: number;
        nextEpoch: number;
        invalidateGeneration: true;
        revokedDestinationIds: readonly string[];
        reason: string;
        signal?: AbortSignal;
    }): Promise<unknown>;
    /** Equivalent named seam for a genuine coordination store. */
    beginForgetBarrier?(input: {
        now: number;
        expectedEpoch: number;
        revokedDestinationIds?: readonly string[];
        signal?: AbortSignal;
    }): Promise<unknown>;
    recordRevocations?(input: {
        destinationIds: readonly string[];
        privacyEpoch: number;
        reason: string;
        signal?: AbortSignal;
    }): Promise<void>;
    rereadControl?(): Promise<PrivacyControlSnapshot>;
    invalidateGeneration?(): Promise<void>;
    reconcile?(): Promise<void>;
}
export interface PrivacyRevokeResult {
    readonly ok: true;
    readonly planId: string;
    readonly previousPrivacyEpoch: number;
    readonly privacyEpoch: number;
    readonly generationInvalidated: true;
    readonly logicalInvisible: true;
    readonly inFlightCallsCannotBeRevoked: true;
    readonly reconciled: boolean;
}
/** Create a deterministic, redacted collection privacy-revocation plan. */
export declare function planPrivacyRevoke(input: PrivacyRevokePlanInput): PrivacyRevokePlan;
export declare const createPrivacyRevokePlan: typeof planPrivacyRevoke;
export declare const planRevoke: typeof planPrivacyRevoke;
export interface RevokePrivacyInput {
    plan: PrivacyRevokePlan;
    approvedPlanId: string;
    signal?: AbortSignal;
    now?: number;
}
/** Apply one exact plan. Missing write capabilities fail closed rather than
 * claiming a privacy barrier that was never CASed. */
export declare function revokePrivacy(input: RevokePrivacyInput, deps?: PrivacyRevokeDependencies): Promise<PrivacyRevokeResult>;
export declare const applyPrivacyRevoke: typeof revokePrivacy;
export declare const privacyRevoke: typeof revokePrivacy;
