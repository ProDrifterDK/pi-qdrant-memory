import type { RuntimeConfig } from "../types.js";
export interface InitializeDestinationResult {
    created: boolean;
    collection: string;
    dimension: number;
    distance: "Cosine";
}
interface Dependencies {
    fetchImpl?: typeof fetch;
    signal?: AbortSignal;
}
export declare function initializeDestination(config: RuntimeConfig, deps?: Dependencies): Promise<InitializeDestinationResult>;
export {};
