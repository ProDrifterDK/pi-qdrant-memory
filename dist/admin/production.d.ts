import { type ProductionCoordinationStore } from "../qdrant/write.js";
import type { QdrantClientOptions } from "../qdrant/client.js";
import { type ForgetDependencies, type ForgetPlan } from "./forget.js";
import { type PrivacyRevokeDependencies, type PrivacyRevokePlan } from "./privacy.js";
import type { RuntimeConfig } from "../types.js";
export interface StoredPlanDependencies {
    readTextFile?(path: string): Promise<string>;
    writeTextFile?(path: string, text: string): Promise<void>;
}
export interface StoredPlan {
    save(kind: "privacy" | "forget", plan: PrivacyRevokePlan | ForgetPlan): Promise<void>;
    load<T extends PrivacyRevokePlan | ForgetPlan>(kind: "privacy" | "forget", id: string): Promise<T>;
}
/** XDG-local immutable plan store. Existing files are never replaced and all
 * symlinked plan paths fail closed. */
export declare function createStoredPlan(config: RuntimeConfig, dependencies?: StoredPlanDependencies): StoredPlan;
export declare function operatorQdrantOptions(config: RuntimeConfig, env: Record<string, string | undefined>): QdrantClientOptions;
export declare function productionStore(config: RuntimeConfig, env: Record<string, string | undefined>): ProductionCoordinationStore;
export declare function productionPrivacyDependencies(config: RuntimeConfig, env: Record<string, string | undefined>, reconcile: () => Promise<void>): {
    store: ProductionCoordinationStore;
    deps: PrivacyRevokeDependencies;
};
export declare function productionForgetDependencies(config: RuntimeConfig, env: Record<string, string | undefined>, reconcile: () => Promise<void>): {
    store: ProductionCoordinationStore;
    deps: ForgetDependencies;
};
export interface ProductionOperationRequest {
    command: "curate" | "raptor" | "reconcile";
    action: "enqueue" | "wait";
    jobId?: string;
}
/** Enqueue/wait uses only named immutable-job and lease reads; it never
 * manufactures a worker/lease authority in the human process. Curation and
 * reconcile use the normal curation worker identity; only RAPTOR has a
 * dedicated admin extractor consumed by the lifecycle drain. */
export declare function productionOperation(config: RuntimeConfig, env: Record<string, string | undefined>, request: ProductionOperationRequest): Promise<Record<string, unknown>>;
