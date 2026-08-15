import type { ConfigLoadDependencies, HostId, RuntimeConfig } from "./types.js";
/** Human disclosure values used by the admin init gate. This helper is
 * intentionally independent of CLI parsing so RuntimeConfig remains the only
 * runtime shape and no confirmation value enters worker/extension state. */
export interface CaptureActivationDisclosure {
    retention: RuntimeConfig["capture"]["episodeRetentionDays"];
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    confirmed: boolean;
}
export declare function validateCaptureActivation(config: RuntimeConfig, disclosure: CaptureActivationDisclosure | undefined): void;
export declare function configPath(deps: Pick<ConfigLoadDependencies, "homeDir" | "xdgConfigHome">): string;
export declare function loadConfig(host: HostId, deps: ConfigLoadDependencies): Promise<RuntimeConfig>;
