import type { ExtensionContext, ExtensionFactory } from "@earendil-works/pi-coding-agent";
import { type ProjectIdentity } from "./project.js";
import { type MemoryWarningSink } from "./service.js";
import type { AuthorizedDestination, HostId, RuntimeConfig } from "./types.js";
type Environment = Record<string, string | undefined>;
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
}
/** Host/config gate shared by both lifecycle hooks. */
export declare function serviceAutoRecallEnabled(ctx: ExtensionContext, host: HostId | undefined, config: RuntimeConfig | undefined, env?: Environment): boolean;
/** Build a testable factory while keeping the default export host-portable. */
export declare function createMemoryExtension(dependencies?: MemoryExtensionDependencies): ExtensionFactory;
declare const extension: ExtensionFactory;
export default extension;
