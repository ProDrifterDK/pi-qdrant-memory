import type { AuthorizedDestination, RedactedEgressMaterial, RuntimeConfig } from "../types.js";
import type { RedactionResult } from "./redaction.js";
export interface EgressDestination extends AuthorizedDestination {
    endpoint: string;
    nodeId: string;
}
export declare function localDestinationId(endpoint: string, nodeId: string, labels?: Pick<AuthorizedDestination, "residency" | "dataUse">): string;
export declare function destinationForEndpoint(endpoint: string, nodeId: string, labels?: Pick<AuthorizedDestination, "residency" | "dataUse">): EgressDestination;
export type EgressMaterialGate = RedactionResult;
export interface EgressPayloadOptions {
    homeDir: string;
    maxChars: number;
}
export interface EgressCheckOptions {
    nodeId?: string;
    residency?: string;
    dataUse?: string;
}
export declare function isFinalEgressMaterial(value: EgressMaterialGate | undefined, options?: EgressPayloadOptions): value is RedactedEgressMaterial;
export declare function assertFinalEgressMaterial(value: EgressMaterialGate, options: EgressPayloadOptions): asserts value is RedactedEgressMaterial;
export declare function isDestinationAllowed(mode: RuntimeConfig["privacy"]["egressMode"], destination: AuthorizedDestination | EgressDestination, allowlist: readonly AuthorizedDestination[], options?: EgressCheckOptions): boolean;
export declare function canEgress(input: {
    mode: RuntimeConfig["privacy"]["egressMode"];
    destination: AuthorizedDestination | EgressDestination;
    allowlist: readonly AuthorizedDestination[];
    options?: EgressCheckOptions;
    material: EgressMaterialGate;
    payload: EgressPayloadOptions;
}): boolean;
export declare function assertEgressAllowed(input: Parameters<typeof canEgress>[0]): void;
