import type { HostId, SecretScanStatus } from "../types.js";
import type { EpisodeRecord } from "../domain/records.js";
export declare const CAPTURE_LIFECYCLES: readonly ["agent_end", "session_before_compact", "session_shutdown"];
export type CaptureLifecycle = typeof CAPTURE_LIFECYCLES[number];
export interface PersistedEntry {
    id: string;
    type: string;
    message?: unknown;
    [key: string]: unknown;
}
export interface CaptureInput {
    sessionId: string;
    lifecycle: CaptureLifecycle;
    getEntries: () => readonly PersistedEntry[];
    activationDir: string;
    host: HostId;
    homeDir?: string;
    projectId?: string;
    projectIdentityKind?: "registered" | "local_only";
    projectAllowlist?: readonly string[];
    projectDenylist?: readonly string[];
    marker?: AgentMarkerInput;
    policyId?: string;
    privacyEpoch?: number;
    expiresAt?: string | null;
    modelId?: string;
    originProvider?: string;
    destinationId?: string;
    nodeId?: string;
    producerId?: string;
    maxTextChars?: number;
    toolArgsChars?: number;
    toolResultChars?: number;
    now?: () => number | string | Date;
    acceptEpisodes?: (episodes: readonly EpisodeRecord[]) => Promise<void> | void;
    /** Injectable final-scanner seam; it receives only structurally redacted text. */
    scan?: (text: string) => SecretScanStatus;
}
export interface ActivationInput {
    sessionId: string;
    getEntries: () => readonly PersistedEntry[];
    readActivation: (key: string) => Promise<string | undefined>;
    writeActivation: (key: string, value: string) => Promise<void>;
    now: () => number | string | Date;
    host?: HostId;
}
export interface CaptureAudit {
    redaction: number;
    scanner_rejected: number;
    scanner_error: number;
    invalid_entry: number;
}
export interface AgentMarkerInput {
    host: HostId;
    header?: unknown;
    env?: Record<string, string | undefined>;
}
export interface AgentMarker {
    role: "root" | "child";
    depth: number;
    valid: boolean;
    rootWorkAllowed: boolean;
    producerId?: string;
}
export type CaptureEpisodeRecord = EpisodeRecord;
export type CaptureQuarantineCategory = "redaction" | "scanner_rejected" | "scanner_error" | "invalid_entry";
export declare function captureStateKey(host: HostId, sessionId: string): string;
export declare function captureStateFilename(sessionId: string): string;
export declare function captureStatePath(agentDir: string, sessionId: string): string;
/** Resolve only the host-owned coding-agent directory; repository paths never participate. */
export declare function resolveCaptureAgentDirectory(input: {
    host: HostId;
    env: Record<string, string | undefined>;
    homeDir: string;
}): Promise<string | null>;
/** Resolve child markers without trusting ambiguous or malformed host metadata. */
export declare function resolveAgentMarker(input: AgentMarkerInput): AgentMarker;
export declare function captureRootWorkAllowed(marker: AgentMarkerInput): boolean;
export declare function capturePersistedEntries(input: CaptureInput): Promise<CaptureEpisodeRecord[]>;
/** Persist the current getEntries tail before capture is enabled. */
export declare function activateCapture(input: ActivationInput): Promise<void>;
export declare function clearCaptureActivation(sessionId: string, host?: HostId): void;
export declare function persistCaptureActivationFile(input: {
    host: HostId;
    sessionId: string;
    getEntries: () => readonly PersistedEntry[];
    env: Record<string, string | undefined>;
    homeDir: string;
    now: () => number | string | Date;
}): Promise<string>;
