import type { AuthorizedDestination, RuntimeConfig } from "../types.js";
export interface EgressDestination extends AuthorizedDestination {
    endpoint: string;
    nodeId: string;
}
export declare function localDestinationId(endpoint: string, nodeId: string, labels?: Pick<AuthorizedDestination, "residency" | "dataUse">): string;
export declare function destinationForEndpoint(endpoint: string, nodeId: string, labels?: Pick<AuthorizedDestination, "residency" | "dataUse">): EgressDestination;
export interface EgressCheckOptions {
    nodeId?: string;
    residency?: string;
    dataUse?: string;
}
export declare function isDestinationAllowed(mode: RuntimeConfig["privacy"]["egressMode"], destination: AuthorizedDestination | EgressDestination, allowlist: readonly AuthorizedDestination[], options?: EgressCheckOptions): boolean;
export declare function canEgress(input: {
    mode: RuntimeConfig["privacy"]["egressMode"];
    destination: AuthorizedDestination | EgressDestination;
    allowlist: readonly AuthorizedDestination[];
    options?: EgressCheckOptions;
}): boolean;
export declare function assertEgressAllowed(input: Parameters<typeof canEgress>[0]): void;
