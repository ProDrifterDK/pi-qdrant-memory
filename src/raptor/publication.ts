import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import type { ControlRecord } from "../domain/records.js";
import { LeaseAuthority, ProductionCoordinationStore } from "../qdrant/write.js";

export interface GenerationIdentityInput { readonly manifestRoot: string; readonly membershipHash: string; readonly baseGeneration: string | null; readonly privacyEpoch: number; readonly coordinationPolicyEpoch: number; readonly coordinationPolicyHash: string; readonly policyId: string; readonly algorithm: string; readonly promptRevision: string; readonly modelId: string; readonly seed: string | number; }
export interface Generation { readonly id: string; readonly manifestRoot: string; readonly membershipHash: string; readonly baseGeneration: string | null; readonly privacyEpoch: number; readonly coordinationPolicyEpoch: number; readonly coordinationPolicyHash: string; readonly jobId: string; readonly fencingToken: number; readonly status: "building" | "published" | "retired"; }

function text(name: string, value: unknown, max = 512): string { if (typeof value !== "string" || value.length === 0 || value.length > max) throw new TypeError(`RAPTOR ${name} is invalid`); return value; }
function epoch(name: string, value: unknown): number { if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError(`RAPTOR ${name} is invalid`); return value as number; }
export function publicationIdentity(input: GenerationIdentityInput): string {
  const owned = { manifestRoot: text("manifest root", input.manifestRoot), membershipHash: text("membership hash", input.membershipHash), baseGeneration: input.baseGeneration, privacyEpoch: epoch("privacy epoch", input.privacyEpoch), coordinationPolicyEpoch: epoch("coordination policy epoch", input.coordinationPolicyEpoch), coordinationPolicyHash: text("coordination policy hash", input.coordinationPolicyHash), policyId: text("policy ID", input.policyId), algorithm: text("algorithm", input.algorithm), promptRevision: text("prompt revision", input.promptRevision), modelId: text("model ID", input.modelId), seed: text("seed", String(input.seed), 4096) };
  if (owned.baseGeneration !== null) text("base generation", owned.baseGeneration);
  return sha256Hex(canonicalStringify({ domain: "raptor-generation-v1", ...owned }));
}
function generationSnapshot(input: Generation): Generation {
  const baseGeneration = input.baseGeneration; if (baseGeneration !== null) text("base generation", baseGeneration);
  const value = Object.freeze({ id: text("generation ID", input.id), manifestRoot: text("manifest root", input.manifestRoot), membershipHash: text("membership hash", input.membershipHash), baseGeneration, privacyEpoch: epoch("privacy epoch", input.privacyEpoch), coordinationPolicyEpoch: epoch("coordination policy epoch", input.coordinationPolicyEpoch), coordinationPolicyHash: text("coordination policy hash", input.coordinationPolicyHash), jobId: text("job ID", input.jobId), fencingToken: epoch("fencing token", input.fencingToken), status: input.status });
  if (value.status !== "building") throw new TypeError("Only building generations can publish"); return value;
}
/** Nominal production-only publication. No structural store/CAS protocol is exported. */
export async function publishGeneration(store: ProductionCoordinationStore, authority: LeaseAuthority, input: { readonly control: ControlRecord; readonly generation: Generation; readonly tombstoneTargets: readonly string[]; readonly destinationIds: readonly string[] }): Promise<boolean> {
  if (!ProductionCoordinationStore.isValid(store) || !LeaseAuthority.isValid(authority)) throw new TypeError("RAPTOR publication requires genuine production capabilities");
  const generation = generationSnapshot(input.generation); const control = input.control;
  if (generation.jobId !== authority.jobId || generation.fencingToken !== authority.fencingToken || generation.privacyEpoch !== authority.privacyEpoch || generation.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || generation.coordinationPolicyHash !== authority.coordinationPolicyHash || generation.baseGeneration !== control.activeGeneration) return false;
  return store.publishRaptorGeneration(authority, { expected: control, generationId: generation.id, destinationIds: input.destinationIds, evidenceIds: input.tombstoneTargets });
}
export function generationIsVisible(control: Pick<ControlRecord, "state" | "activeGeneration" | "privacyEpoch" | "coordinationPolicyEpoch" | "coordinationPolicyHash">, generation: Generation): boolean { return control.state === "active" && control.activeGeneration === generation.id && control.privacyEpoch === generation.privacyEpoch && control.coordinationPolicyEpoch === generation.coordinationPolicyEpoch && control.coordinationPolicyHash === generation.coordinationPolicyHash; }
