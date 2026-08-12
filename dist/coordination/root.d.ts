import type { HostId } from "../types.js";
export declare class RootWorkerContext {
    #private;
    /**
     * Public constructor is unusable without the module-private issuer symbol.
     * The clock is PRIVATE captured state: only `now()` (validating every call)
     * exposes it. Task 9 supplies the real host clock, the validated immutable
     * `nodeId` (from runtime node configuration) AND the bound lease
     * configuration (`leaseMs`, `maxClockSkewMs`) at issuance; the default clock
     * is the wall clock. There is NO public clock issuer and no structural
     * clock can pass the brand check, and a root can never claim with a caller
     * chosen skew/TTL.
     */
    constructor(host: HostId, evidenceHash: string, issuer: symbol, clock: (() => number) | undefined, nodeId: string, leaseMs: number, maxClockSkewMs: number);
    /** Exposed validating operation only; issuance stays module-private and unused in Task 8. */
    static isValid(value: unknown): value is RootWorkerContext;
    /**
     * Trusted FRESH clock: validates a safe nonnegative integer on EVERY call
     * AND enforces true monotonicity — a value strictly below the previous
     * accepted sample is rejected (equal is allowed), and the last-sample state
     * updates only on success. A throwing, NaN, fractional or backwards clock
     * fails closed at the call site (no authority is ever minted from a stale
     * or invalid sample).
     */
    now(): number;
    get host(): HostId;
    get evidenceHash(): string;
    /** Validated immutable nominal node identity; lease ownership is derived from it. */
    get nodeId(): string;
    /** Bound lease TTL (seconds-scale) issued with the capability; claim derives it, never the caller. */
    get leaseMs(): number;
    /** Bound clock skew issued with the capability; claim derives it, never the caller. */
    get maxClockSkewMs(): number;
}
