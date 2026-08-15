import type { ExtensionContext, ExtensionFactory } from "@earendil-works/pi-coding-agent";
import { type AgentMarker } from "./capture/episode.js";
import { type ProjectIdentity } from "./project.js";
import { type MemoryWarningSink } from "./service.js";
import type { EpisodeRecord } from "./domain/records.js";
import type { AuthorizedDestination, HostId, RuntimeConfig } from "./types.js";
type Environment = Record<string, string | undefined>;
export type MemoryCaptureLifecycle = "agent_end" | "session_before_compact" | "session_shutdown";
export interface MemoryLifecycleSession {
    readonly host: HostId;
    readonly config: RuntimeConfig;
    readonly sessionId: string;
    readonly cwd: string;
    readonly project: ProjectIdentity;
    readonly marker: AgentMarker;
    readonly getEntries: () => readonly unknown[];
    readonly ctx: ExtensionContext;
}
export interface MemoryLifecycleCaptureInput extends MemoryLifecycleSession {
    readonly lifecycle: MemoryCaptureLifecycle;
}
export interface MemoryRootScheduleInput extends MemoryLifecycleSession {
    readonly episodes: readonly EpisodeRecord[];
    readonly reason: "threshold" | "compact" | "shutdown" | "recovery";
}
export interface MemoryRootScheduleResult {
    /** IDs whose durable curation work was acknowledged; omitted means all input IDs. */
    readonly completedEpisodeIds?: readonly string[];
}
export interface MemoryLifecycleCoordinator {
    start(input: MemoryLifecycleSession): Promise<void>;
    recover?(producerPaths: readonly string[]): Promise<readonly EpisodeRecord[] | void>;
    capture(input: MemoryLifecycleCaptureInput): Promise<readonly EpisodeRecord[]>;
    deliver(input?: {
        readonly signal?: AbortSignal;
        readonly maxJobs?: number;
    }): Promise<unknown>;
    scheduleRoot(input: MemoryRootScheduleInput): Promise<boolean | void | MemoryRootScheduleResult>;
    /** Drain human-created immutable coordination jobs through the same root
     * lifecycle workers; absent on injected test coordinators. */
    drainAdminJobs?(input: MemoryLifecycleSession): Promise<void>;
    shutdown(input: MemoryLifecycleCaptureInput): Promise<unknown>;
    clear(): void;
}
export interface MemoryExtensionDependencies {
    env?: Environment;
    argv?: readonly string[];
    homeDir?: string;
    xdgConfigHome?: string;
    readTextFile?(path: string): Promise<string>;
    fetchImpl?: typeof fetch;
    projectResolver?(cwd: string): Promise<ProjectIdentity>;
    now?: () => number;
    warningSink?: MemoryWarningSink;
    modelDestinationResolver?(ctx: ExtensionContext, config: RuntimeConfig): AuthorizedDestination | undefined;
    isChildResolver?(ctx: ExtensionContext, host: HostId, env: Environment): boolean;
    /** Injectable high-level lifecycle seam. Production uses the hardened coordinator. */
    lifecycleCoordinator?: MemoryLifecycleCoordinator;
    lifecycleCoordinatorFactory?(input: {
        homeDir: string;
        env: Environment;
        now?: () => number;
    }): MemoryLifecycleCoordinator;
}
/** Host/config gate shared by both lifecycle hooks. */
export declare function serviceAutoRecallEnabled(ctx: ExtensionContext, host: HostId | undefined, config: RuntimeConfig | undefined, env?: Environment): boolean;
/** Build a testable factory while keeping the default export host-portable. */
export declare function createMemoryExtension(dependencies?: MemoryExtensionDependencies): ExtensionFactory;
declare const extension: ExtensionFactory;
export default extension;
