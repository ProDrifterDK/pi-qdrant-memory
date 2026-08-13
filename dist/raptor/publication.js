import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { LeaseAuthority, ProductionCoordinationStore } from "../qdrant/write.js";
function text(name, value, max = 512) { if (typeof value !== "string" || value.length === 0 || value.length > max)
    throw new TypeError(`RAPTOR ${name} is invalid`); return value; }
function epoch(name, value) { if (!Number.isSafeInteger(value) || value < 0)
    throw new TypeError(`RAPTOR ${name} is invalid`); return value; }
export function publicationIdentity(input) {
    const owned = { manifestRoot: text("manifest root", input.manifestRoot), membershipHash: text("membership hash", input.membershipHash), baseGeneration: input.baseGeneration, privacyEpoch: epoch("privacy epoch", input.privacyEpoch), coordinationPolicyEpoch: epoch("coordination policy epoch", input.coordinationPolicyEpoch), coordinationPolicyHash: text("coordination policy hash", input.coordinationPolicyHash), policyId: text("policy ID", input.policyId), algorithm: text("algorithm", input.algorithm), promptRevision: text("prompt revision", input.promptRevision), modelId: text("model ID", input.modelId), seed: text("seed", String(input.seed), 4096) };
    if (owned.baseGeneration !== null)
        text("base generation", owned.baseGeneration);
    return sha256Hex(canonicalStringify({ domain: "raptor-generation-v1", ...owned }));
}
function generationSnapshot(input) {
    const baseGeneration = input.baseGeneration;
    if (baseGeneration !== null)
        text("base generation", baseGeneration);
    const value = Object.freeze({ id: text("generation ID", input.id), manifestRoot: text("manifest root", input.manifestRoot), membershipHash: text("membership hash", input.membershipHash), baseGeneration, privacyEpoch: epoch("privacy epoch", input.privacyEpoch), coordinationPolicyEpoch: epoch("coordination policy epoch", input.coordinationPolicyEpoch), coordinationPolicyHash: text("coordination policy hash", input.coordinationPolicyHash), jobId: text("job ID", input.jobId), fencingToken: epoch("fencing token", input.fencingToken), status: input.status });
    if (value.status !== "building")
        throw new TypeError("Only building generations can publish");
    return value;
}
/** Nominal production-only publication. No structural store/CAS protocol is exported. */
export async function publishGeneration(store, authority, input) {
    if (!ProductionCoordinationStore.isValid(store) || !LeaseAuthority.isValid(authority))
        throw new TypeError("RAPTOR publication requires genuine production capabilities");
    const generation = generationSnapshot(input.generation);
    const control = input.control;
    if (generation.jobId !== authority.jobId || generation.fencingToken !== authority.fencingToken || generation.privacyEpoch !== authority.privacyEpoch || generation.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || generation.coordinationPolicyHash !== authority.coordinationPolicyHash || generation.baseGeneration !== control.activeGeneration)
        return false;
    return store.publishRaptorGeneration(authority, { expected: control, generationId: generation.id, destinationIds: input.destinationIds, evidenceIds: input.tombstoneTargets });
}
export function generationIsVisible(control, generation) { return control.state === "active" && control.activeGeneration === generation.id && control.privacyEpoch === generation.privacyEpoch && control.coordinationPolicyEpoch === generation.coordinationPolicyEpoch && control.coordinationPolicyHash === generation.coordinationPolicyHash; }
//# sourceMappingURL=publication.js.map