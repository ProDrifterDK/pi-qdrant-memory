import { type ControlRecord } from "../domain/records.js";
import type { RuntimeConfig } from "../types.js";
export interface InitializeDestinationResult {
    host: RuntimeConfig["host"];
    collection: string;
    ownerHost: RuntimeConfig["host"];
    schema: "pi-qdrant-memory-v2";
    schemaRevision: 1;
    vector: {
        name: "semantic";
        model: "bge-m3";
        dimension: 1024;
        distance: "Dot";
    };
    capture: {
        enabled: boolean;
        episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
    };
    disclosure: "Loopback binding provides functional isolation, not cryptographic privacy.";
    initialized: boolean;
    collectionCreated: boolean;
    qdrantVersion?: string;
}
export interface InitializeDestinationDependencies {
    signal?: AbortSignal;
    fetchImpl?: typeof fetch;
    adminApiKey?: string;
    now?: () => number;
    initialControl?: ControlRecord;
    retryAttempts?: number;
    retryDelayMs?: number;
}
export interface InitializationDisclosure {
    /** The operator selected a bounded retention period or `indefinite`. */
    retention: RuntimeConfig["capture"]["episodeRetentionDays"];
    /** The operator acknowledged the configured egress mode/destination set. */
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    confirmed: boolean;
}
/** Validate the human disclosure gate before a CLI can enable capture. Runtime
 * config loading already enforces explicit file retention/egress fields; this
 * second gate prevents a shell invocation from silently enabling capture. */
export declare function validateInitializationDisclosure(config: RuntimeConfig, disclosure: InitializationDisclosure | undefined): void;
/** Destination initialization never consults ambient process credentials. */
export declare function initializeDestination(config: RuntimeConfig, deps?: InitializeDestinationDependencies): Promise<InitializeDestinationResult>;
