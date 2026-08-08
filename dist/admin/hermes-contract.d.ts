import type { AdminPoint } from "./qdrant-admin.js";
export type HermesRejectionReason = "id" | "vector" | "text" | "model" | "project-path" | "source-type" | "created-at" | "tags" | "fact-status" | "stale" | "review-required" | "quarantined" | "raptor-excluded" | "raptor-forgotten";
export type HermesValidation = {
    eligible: true;
    point: AdminPoint;
    model?: string;
} | {
    eligible: false;
    reason: HermesRejectionReason;
};
export declare function validateHermesPoint(point: AdminPoint): HermesValidation;
