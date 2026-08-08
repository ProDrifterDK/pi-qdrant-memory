import type { HostId } from "../types.js";
import type { AdminPoint } from "./qdrant-admin.js";
export declare const MAX_TAG_COUNT = 32;
export declare const MAX_TAG_LENGTH = 256;
export declare const MAX_TAG_TOTAL_LENGTH = 4096;
export declare const MAX_SOURCE_TYPE_LENGTH = 128;
export declare const MAX_PROJECT_LABEL_LENGTH = 255;
export declare const MAX_SOURCE_POINT_ID_LENGTH = 256;
export interface ImportPlan {
    planId: string;
    transformVersion: 1;
    targetHost: HostId;
    sourceCollection: string;
    destinationCollection: string;
    accepted: AdminPoint[];
    rejected: Record<string, number>;
    report: {
        accepted: number;
        rejected: number;
        bySourceType: Record<string, number>;
        byProjectLabel: Record<string, number>;
    };
}
export type NormalizedHermesPoint = {
    accepted: true;
    point: AdminPoint;
    projectLabel?: string;
    sourceType: string;
    model?: string;
} | {
    accepted: false;
    reason: string;
};
interface NormalizeInput {
    point: AdminPoint;
    targetHost: HostId;
    sourceCollection: string;
    configuredModel: string;
}
interface BuildImportPlanInput {
    points: readonly AdminPoint[];
    targetHost: HostId;
    sourceIdentity: string;
    sourceCollection: string;
    sourceDimension: number;
    sourceDistance: string;
    destinationCollection: string;
    destinationDimension: number;
    destinationDistance: string;
    configuredModel: string;
    declaredSourceModel?: string;
}
export declare function normalizeHermesPoint(input: NormalizeInput): NormalizedHermesPoint;
export declare function buildImportPlan(input: BuildImportPlanInput): ImportPlan;
export {};
