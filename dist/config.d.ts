import type { ConfigLoadDependencies, HostId, RuntimeConfig } from "./types.js";
export declare function configPath(deps: Pick<ConfigLoadDependencies, "homeDir" | "xdgConfigHome">): string;
export declare function loadConfig(host: HostId, deps: ConfigLoadDependencies): Promise<RuntimeConfig>;
