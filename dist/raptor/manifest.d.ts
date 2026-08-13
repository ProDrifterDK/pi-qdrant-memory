import type { HostId } from "../types.js";
export interface RaptorManifestChunk {
    readonly id: string;
    readonly index: number;
    readonly memberIds: readonly string[];
    readonly contentHash: string;
}
export interface RaptorManifestRoot {
    readonly id: string;
    readonly ownerHost: HostId;
    readonly membershipHash: string;
    readonly merkleRoot: string;
    readonly chunkIds: readonly string[];
    readonly leafCount: number;
    readonly chunkSize: number;
    readonly policyId: string;
    readonly policyHash: string;
    readonly policyEpoch: number;
    readonly privacyEpoch: number;
    readonly algorithm: string;
    readonly promptRevision: string;
    readonly modelId: string;
    readonly seed: string;
}
export interface RaptorManifest {
    readonly root: RaptorManifestRoot;
    readonly chunks: readonly RaptorManifestChunk[];
}
export interface BuildManifestInput {
    readonly ownerHost: HostId;
    readonly leafIds: readonly string[];
    readonly chunkSize: number;
    readonly policyId: string;
    readonly policyHash: string;
    readonly policyEpoch: number;
    readonly privacyEpoch: number;
    readonly algorithm: string;
    readonly promptRevision: string;
    readonly modelId: string;
    readonly seed: string | number;
}
export declare function buildManifest(input: BuildManifestInput): RaptorManifest;
export declare function verifyManifest(input: RaptorManifest): boolean;
