import type { AuthorizedDestination, RedactedEgressMaterial, RuntimeConfig } from "../types.js";
import type { RedactionResult } from "./redaction.js";
import { type SecretScanner } from "./redaction.js";
export interface EgressDestination extends AuthorizedDestination {
    endpoint: string;
    nodeId: string;
}
export declare function assertPseudonymousNodeId(nodeId: unknown, options?: {
    allowDerivedDigest?: boolean;
}): asserts nodeId is string;
export declare function canonicalEgressEndpoint(endpoint: string): string;
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
export interface CuratedEgressOptions {
    maxChars?: number;
    homeDir?: string;
    scan?: SecretScanner;
}
/**
 * ONE gate for derived curated text before BGE-M3 embedding: the canonical
 * curated text is structurally redacted AND final-scanned in a single call;
 * only `secret_scan="passed"` material returns (a scanner reject/error yields
 * a dropped result that the caller must treat as quarantine with NO text
 * egress and NO embedding). The returned material is an owned plain object.
 */
export declare function gateCuratedEgressText(text: string, options?: CuratedEgressOptions): RedactedEgressMaterial;
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
/**
 * Pin a configured endpoint to one declared destination identity.  The caller
 * supplies no separate ID allowlist: for remote allowlist mode the configured
 * pair is the authorization object; for local-only mode the identity is
 * recomputed from the canonical loopback/Unix endpoint and node ID.
 */
export declare function bindConfiguredDestination(input: {
    endpoint: string;
    configuredDestination: AuthorizedDestination;
    requestedDestination: AuthorizedDestination;
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    nodeId?: string;
}): AuthorizedDestination;
