import type { HostId } from "../types.js";
import { type ImportPlan } from "./import-plan.js";
import type { AdminQdrantClient } from "./qdrant-admin.js";
export interface ImportOptions {
    sourceIdentity: string;
    sourceCollection: string;
    destinationCollection: string;
    targetHost: HostId;
    configuredModel: string;
    configuredDimension: number;
    declaredSourceModel?: string;
    signal?: AbortSignal;
}
export interface ImportClients {
    source: Pick<AdminQdrantClient, "collectionInfo" | "scroll">;
    destination: Pick<AdminQdrantClient, "collectionInfo" | "upsert">;
}
export declare class ImportValidationError extends Error {
    constructor(message: string);
}
export declare class ImportInfrastructureError extends Error {
    constructor(message: string);
}
export declare class ImportApprovalMismatchError extends ImportValidationError {
    constructor();
}
export declare function planHermesImport(options: ImportOptions, clients: ImportClients): Promise<ImportPlan>;
export declare function applyHermesImport(options: ImportOptions & {
    approvedPlanId: string;
}, clients: ImportClients): Promise<{
    planId: string;
    upserted: number;
    batches: number;
}>;
