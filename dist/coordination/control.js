import { ProductionCoordinationStore, QuiescenceProof, createQdrantCoordinationStore } from "../qdrant/write.js";
export { ProductionCoordinationStore, QuiescenceProof, createQdrantCoordinationStore } from "../qdrant/write.js";
/**
 * Thin Production-brand-only coordination surface. There is NO structural-
 * store mutation path: inputs that are not a genuine Production store fail
 * closed, and every mutation delegates to the store's named safe method (all
 * protocol implementations live lexically inside qdrant/write.ts and are not
 * exported anywhere).
 */
/** Read the single owner-independent collection-control point (strong, exact payload). */
export async function readControl(store) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Control read requires a genuine production store");
    return store.readControl();
}
/** Reread the Task 3 insert-only v0 bootstrap point; later mutations belong only to the store. */
export async function initializeControl(store, initial) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Control initialization requires a genuine production store");
    return store.initializeControl(initial);
}
/** Read the current control for a CAS cycle; callers reread before and after every transition. */
export async function readForUpdate(store) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Control read requires a genuine production store");
    return store.readControl();
}
/** Reread control privacy/coordination epochs + revocations as ONE bounded frozen snapshot. */
export async function readControlSnapshot(store) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Control snapshot requires a genuine production store");
    const control = await store.readControl();
    return Object.freeze({
        state: control.state, privacyEpoch: control.privacyEpoch,
        coordinationPolicyEpoch: control.coordinationPolicyEpoch, coordinationPolicyHash: control.coordinationPolicyHash,
        revokedDestinationIds: Object.freeze([...control.revokedDestinationIds]),
    });
}
/** CAS active->draining, clears active generation and derived-current visibility; workers stop claiming/egressing. */
export async function beginPolicyDrain(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Policy drain requires a genuine production store");
    return store.beginPolicyDrain(input);
}
/**
 * Bounded, abortable quiescence over the genuine store: polls its own strong
 * control/lease state, re-pins the control identity and returns an
 * unforgeable QuiescenceProof bound to THIS store's private authority scope.
 * A proof minted by any other store is rejected at activation.
 */
export async function waitForOldLeasesToQuiesce(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Lease quiescence requires a genuine production store");
    return store.waitForOldLeasesToQuiesce(input);
}
/** After the proof and the bounded LLM timeout, CAS draining->active with epoch+1 and the new hash. */
export async function activatePolicyEpoch(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Policy activation requires a genuine production store");
    return store.activatePolicyEpoch(input);
}
/** Rotate the coordination policy: drain, quiesce (branded proof), LLM timeout, activate epoch+1/hash. */
export async function rotateCoordinationPolicy(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Policy rotation requires a genuine production store");
    return store.rotateCoordinationPolicy(input);
}
/** Forget barrier on the same control point: privacy epoch +1, active generation cleared, barrier recorded. */
export async function beginForgetBarrier(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Forget barrier requires a genuine production store");
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
export class BoundIngestControlReader {
    #issuer;
    #store;
    policyHash;
    policyEpoch;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(store, input, issuer) {
        if (issuer !== INGEST_CONTROL_READER_ISSUER)
            throw new TypeError("Control reader requires the module issuer");
        if (!ProductionCoordinationStore.isValid(store))
            throw new TypeError("Control reader requires the genuine production store");
        // GLOBAL RULE: snapshot the untrusted policy fields EXACTLY ONCE; validate
        // and persist ONLY the locals (a malicious accessor cannot validate one
        // value and persist another).
        const policyHash = input.policyHash;
        const policyEpoch = input.policyEpoch;
        if (typeof policyHash !== "string" || policyHash.length === 0 || policyHash.length > 512 || !Number.isSafeInteger(policyEpoch) || policyEpoch < 0)
            throw new TypeError("Control reader binding is invalid");
        this.#issuer = issuer;
        this.#store = store;
        this.policyHash = policyHash;
        this.policyEpoch = policyEpoch;
        Object.freeze(this);
    }
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value) {
        if (typeof value !== "object" || value === null || !(#issuer in value))
            return false;
        return value instanceof BoundIngestControlReader && value.#issuer === INGEST_CONTROL_READER_ISSUER;
    }
    async read() {
        const control = await this.#store.readControl();
        return { state: control.state, privacyEpoch: control.privacyEpoch, coordinationPolicyEpoch: control.coordinationPolicyEpoch, policyHash: control.coordinationPolicyHash, revokedDestinationIds: [...control.revokedDestinationIds] };
    }
}
Object.freeze(BoundIngestControlReader);
Object.freeze(BoundIngestControlReader.prototype);
/** Production Task 7 control reader: bounded revocation snapshot from the single control point. */
export function createIngestControlReader(store, input) {
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Control reader requires the genuine production store");
    return new BoundIngestControlReader(store, input, INGEST_CONTROL_READER_ISSUER);
}
//# sourceMappingURL=control.js.map