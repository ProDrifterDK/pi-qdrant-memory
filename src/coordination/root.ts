/**
 * Verified root-worker capability.
 *
 * Task 8 ships NO successful public root issuer: there is no exported mint,
 * factory, raw runtime factory or caller-supplied evidence path. Instances are
 * constructed only inside this module with the module-private issuer symbol,
 * and no exported function creates them, so production root operations fail
 * closed until Task 9's real host adapter owns issuance. Forged structural
 * objects and any external construction fail the brand check before any store
 * read or network operation.
 */
const ROOT_WORKER_ISSUER = Symbol("pi-qdrant-memory-v2.root-worker-issuer");
import type { HostId } from "../types.js";

const SECRET = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;
export class RootWorkerContext {
  readonly #issuer: symbol;
  readonly #host: HostId;
  readonly #evidenceHash: string;
  readonly #clock: () => number;
  readonly #nodeId: string;
  readonly #leaseMs: number;
  readonly #maxClockSkewMs: number;
  /** Last accepted sample; private #-field state survives Object.freeze and is nonforgeable. */
  #lastSample: number | null = null;
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
  constructor(host: HostId, evidenceHash: string, issuer: symbol, clock: (() => number) | undefined, nodeId: string, leaseMs: number, maxClockSkewMs: number) {
    if (issuer !== ROOT_WORKER_ISSUER) throw new TypeError("Root worker capability requires the module issuer");
    if (clock !== undefined && typeof clock !== "function") throw new TypeError("Root worker clock is invalid");
    if (typeof nodeId !== "string" || nodeId.length === 0 || nodeId.length > 512 || SECRET.test(nodeId)) throw new TypeError("Root worker node id is invalid");
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 86_400_000) throw new TypeError("Root worker lease TTL is invalid");
    if (!Number.isSafeInteger(maxClockSkewMs) || maxClockSkewMs < 0 || maxClockSkewMs > 3_600_000) throw new TypeError("Root worker clock skew is invalid");
    this.#issuer = issuer;
    this.#host = host;
    this.#evidenceHash = evidenceHash;
    this.#clock = clock ?? (() => Date.now());
    this.#nodeId = nodeId;
    this.#leaseMs = leaseMs;
    this.#maxClockSkewMs = maxClockSkewMs;
    Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private and unused in Task 8. */
  static isValid(value: unknown): value is RootWorkerContext {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof RootWorkerContext && value.#issuer === ROOT_WORKER_ISSUER;
  }
  /**
   * Trusted FRESH clock: validates a safe nonnegative integer on EVERY call
   * AND enforces true monotonicity — a value strictly below the previous
   * accepted sample is rejected (equal is allowed), and the last-sample state
   * updates only on success. A throwing, NaN, fractional or backwards clock
   * fails closed at the call site (no authority is ever minted from a stale
   * or invalid sample).
   */
  now(): number {
    const value = this.#clock();
    if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError("Root worker clock is invalid");
    if (this.#lastSample !== null && (value as number) < this.#lastSample) throw new TypeError("Root worker clock went backwards");
    this.#lastSample = value;
    return value;
  }
  get host(): HostId { return this.#host; }
  get evidenceHash(): string { return this.#evidenceHash; }
  /** Validated immutable nominal node identity; lease ownership is derived from it. */
  get nodeId(): string { return this.#nodeId; }
  /** Bound lease TTL (seconds-scale) issued with the capability; claim derives it, never the caller. */
  get leaseMs(): number { return this.#leaseMs; }
  /** Bound clock skew issued with the capability; claim derives it, never the caller. */
  get maxClockSkewMs(): number { return this.#maxClockSkewMs; }
}
Object.freeze(RootWorkerContext);
Object.freeze(RootWorkerContext.prototype);
