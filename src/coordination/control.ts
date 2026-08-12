import { ProductionCoordinationStore, QuiescenceProof, createQdrantCoordinationStore } from "../qdrant/write.js";
export { ProductionCoordinationStore, QuiescenceProof, createQdrantCoordinationStore } from "../qdrant/write.js";
import type { ControlRecord } from "../domain/records.js";
import type { IngestControlReader } from "../outbox/delivery.js";

/**
 * Thin Production-brand-only coordination surface. There is NO structural-
 * store mutation path: inputs that are not a genuine Production store fail
 * closed, and every mutation delegates to the store's named safe method (all
 * protocol implementations live lexically inside qdrant/write.ts and are not
 * exported anywhere).
 */

/** Read the single owner-independent collection-control point (strong, exact payload). */
export async function readControl(store: ProductionCoordinationStore): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Control read requires a genuine production store");
  return store.readControl();
}
/** Reread the Task 3 insert-only v0 bootstrap point; later mutations belong only to the store. */
export async function initializeControl(store: ProductionCoordinationStore, initial: ControlRecord): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Control initialization requires a genuine production store");
  return store.initializeControl(initial);
}
/** Read the current control for a CAS cycle; callers reread before and after every transition. */
export async function readForUpdate(store: ProductionCoordinationStore): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Control read requires a genuine production store");
  return store.readControl();
}

/** CAS active->draining, clears active generation and derived-current visibility; workers stop claiming/egressing. */
export async function beginPolicyDrain(store: ProductionCoordinationStore, input: { now: number }): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Policy drain requires a genuine production store");
  return store.beginPolicyDrain(input);
}

/**
 * Bounded, abortable quiescence over the genuine store: polls its own strong
 * control/lease state, re-pins the control identity and returns an
 * unforgeable QuiescenceProof bound to THIS store's private authority scope.
 * A proof minted by any other store is rejected at activation.
 */
export async function waitForOldLeasesToQuiesce(store: ProductionCoordinationStore, input: { retiredEpoch: number; maxLeaseMs: number; maxClockSkewMs: number; timeoutMs?: number; pollIntervalMs?: number; now?: () => number; signal?: AbortSignal }): Promise<QuiescenceProof> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Lease quiescence requires a genuine production store");
  return store.waitForOldLeasesToQuiesce(input);
}

/** After the proof and the bounded LLM timeout, CAS draining->active with epoch+1 and the new hash. */
export async function activatePolicyEpoch(store: ProductionCoordinationStore, input: { proof: QuiescenceProof; nextPolicyHash: string; memoryModelTimeoutMs: number; signal?: AbortSignal }): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Policy activation requires a genuine production store");
  return store.activatePolicyEpoch(input);
}

/** Rotate the coordination policy: drain, quiesce (branded proof), LLM timeout, activate epoch+1/hash. */
export async function rotateCoordinationPolicy(store: ProductionCoordinationStore, input: { nextPolicyHash: string; maxLeaseMs: number; maxClockSkewMs: number; memoryModelTimeoutMs: number; quiesceTimeoutMs?: number; now: number; signal?: AbortSignal }): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Policy rotation requires a genuine production store");
  return store.rotateCoordinationPolicy(input);
}

/** Forget barrier on the same control point: privacy epoch +1, active generation cleared, barrier recorded. */
export async function beginForgetBarrier(store: ProductionCoordinationStore, input: { now: number }): Promise<ControlRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Forget barrier requires a genuine production store");
  return store.beginForgetBarrier(input);
}

/** Module-private unexported issuer: control readers are constructed only through the factory. */
const INGEST_CONTROL_READER_ISSUER = Symbol("pi-qdrant-memory-v2.ingest-control-reader-issuer");

/**
 * Nominal, frozen, privately branded bound control reader: bounded revocation
 * snapshot from the single control point of its genuine production store.
 * Forged prototypes and monkeypatched statics fail the brand check; a
 * structural store can never mint a branded reader.
 */
export class BoundIngestControlReader implements IngestControlReader {
  readonly #issuer: symbol;
  readonly #store: ProductionCoordinationStore;
  readonly policyHash: string;
  readonly policyEpoch: number;
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(store: ProductionCoordinationStore, input: { policyHash: string; policyEpoch: number }, issuer: symbol) {
    if (issuer !== INGEST_CONTROL_READER_ISSUER) throw new TypeError("Control reader requires the module issuer");
    if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Control reader requires the genuine production store");
    // GLOBAL RULE: snapshot the untrusted policy fields EXACTLY ONCE; validate
    // and persist ONLY the locals (a malicious accessor cannot validate one
    // value and persist another).
    const policyHash = input.policyHash;
    const policyEpoch = input.policyEpoch;
    if (typeof policyHash !== "string" || policyHash.length === 0 || policyHash.length > 512 || !Number.isSafeInteger(policyEpoch) || policyEpoch < 0) throw new TypeError("Control reader binding is invalid");
    this.#issuer = issuer;
    this.#store = store;
    this.policyHash = policyHash;
    this.policyEpoch = policyEpoch;
    Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is BoundIngestControlReader {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof BoundIngestControlReader && value.#issuer === INGEST_CONTROL_READER_ISSUER;
  }
  async read(): Promise<Awaited<ReturnType<IngestControlReader["read"]>>> {
    const control = await this.#store.readControl();
    return { state: control.state, privacyEpoch: control.privacyEpoch, coordinationPolicyEpoch: control.coordinationPolicyEpoch, policyHash: control.coordinationPolicyHash, revokedDestinationIds: [...control.revokedDestinationIds] };
  }
}
Object.freeze(BoundIngestControlReader);
Object.freeze(BoundIngestControlReader.prototype);

/** Production Task 7 control reader: bounded revocation snapshot from the single control point. */
export function createIngestControlReader(store: ProductionCoordinationStore, input: { policyHash: string; policyEpoch: number }): BoundIngestControlReader {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Control reader requires the genuine production store");
  return new BoundIngestControlReader(store, input, INGEST_CONTROL_READER_ISSUER);
}
