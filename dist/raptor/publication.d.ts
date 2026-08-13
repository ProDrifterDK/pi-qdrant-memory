import type { ControlRecord } from "../domain/records.js";
import { LeaseAuthority, ProductionCoordinationStore } from "../qdrant/write.js";
export interface GenerationIdentityInput {
    readonly manifestRoot: string;
    readonly membershipHash: string;
    readonly baseGeneration: string | null;
    readonly privacyEpoch: number;
    readonly coordinationPolicyEpoch: number;
    readonly coordinationPolicyHash: string;
    readonly policyId: string;
    readonly algorithm: string;
    readonly promptRevision: string;
    readonly modelId: string;
    readonly seed: string | number;
}
export interface Generation {
    readonly id: string;
    readonly manifestRoot: string;
    readonly membershipHash: string;
    readonly baseGeneration: string | null;
    readonly privacyEpoch: number;
    readonly coordinationPolicyEpoch: number;
    readonly coordinationPolicyHash: string;
    readonly jobId: string;
    readonly fencingToken: number;
    readonly status: "building" | "published" | "retired";
}
export declare function publicationIdentity(input: GenerationIdentityInput): string;
/** Nominal production-only publication. No structural store/CAS protocol is exported. */
export declare function publishGeneration(store: ProductionCoordinationStore, authority: LeaseAuthority, input: {
    readonly control: ControlRecord;
    readonly generation: Generation;
    readonly tombstoneTargets: readonly string[];
    readonly destinationIds: readonly string[];
}): Promise<boolean>;
export declare function generationIsVisible(control: Pick<ControlRecord, "state" | "activeGeneration" | "privacyEpoch" | "coordinationPolicyEpoch" | "coordinationPolicyHash">, generation: Generation): boolean;
