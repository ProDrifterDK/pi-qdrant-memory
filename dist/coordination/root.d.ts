import { SessionManager } from "@earendil-works/pi-coding-agent";
import type { HostId } from "../types.js";
import { type CurationRunResult, type CurationWorkerInput } from "../curation/worker.js";
/**
 * Nominal root capability.  There is intentionally no public issuer or
 * runtime/factory adapter: only the high-level lifecycle operation below can
 * construct this class, and the lease kernel accepts only this private brand.
 */
export declare class RootWorkerContext {
    #private;
    constructor(host: HostId, evidenceHash: string, issuer: symbol, clock: (() => number) | undefined, nodeId: string | undefined, leaseMs: number | undefined, maxClockSkewMs: number | undefined);
    static isValid(value: unknown): value is RootWorkerContext;
    now(): number;
    get host(): HostId;
    get evidenceHash(): string;
    get nodeId(): string;
    get leaseMs(): number;
    get maxClockSkewMs(): number;
}
/**
 * The sole successful curation entry point. It consumes a genuine
 * SessionManager instance and returns only a result; RootWorkerContext never
 * crosses this boundary. A structural manager, subclass, proxy, or raw header
 * fails closed before store/network work.
 */
export type RootCurationLifecycleInput = Omit<CurationWorkerInput, "rootWorker"> & {
    membership: readonly string[];
    env: Record<string, string | undefined>;
};
export declare function runCurationFromLifecycle(sessionManager: SessionManager, input: RootCurationLifecycleInput): Promise<CurationRunResult>;
