export type ProjectIdentityKind = "registered" | "local_only";
export interface ProjectIdentity {
    id: string;
    label: string;
    identityKind?: ProjectIdentityKind;
    canonicalPath?: string;
    fingerprint?: string;
    registrationValid?: boolean;
    reason?: "unregistered" | "path_mismatch" | "fingerprint_mismatch" | "symlink_escape";
}
export interface ProjectRegistryBinding {
    canonicalPath: string;
    fingerprint: string;
    alias: string;
}
export interface ProjectDependencies {
    gitTopLevel(cwd: string): Promise<string>;
    canonicalize(path: string): Promise<string>;
    gitOrigin?(cwd: string): Promise<string | undefined>;
    gitRootCommits?(cwd: string): Promise<readonly string[]>;
    readTextFile?(path: string): Promise<string>;
    writeTextFile?(path: string, text: string): Promise<void>;
    registryPath?: string;
    /** Explicit XDG registry path, useful for the human CLI and tests. */
    configPath?: string;
    /** Private XDG state sidecar; never parsed as package configuration. */
    statePath?: string;
    homeDir?: string;
    xdgConfigHome?: string;
    installationSalt?: string;
    /** A caller that explicitly sets false cannot mutate operator registrations. */
    operator?: boolean;
}
declare function fingerprintFromOrigin(origin: string): string;
export { fingerprintFromOrigin };
export declare function resolveProjectIdentity(cwd: string, deps?: ProjectDependencies): Promise<ProjectIdentity>;
export interface ProjectRegisterInput {
    path: string;
    alias: string;
    operator?: boolean;
}
export declare function registerProject(input: ProjectRegisterInput, deps?: ProjectDependencies): Promise<ProjectRegistryBinding>;
export declare function registerProject(path: string, alias: string, deps?: ProjectDependencies): Promise<ProjectRegistryBinding>;
export declare function unregisterProject(aliasOrPath: string, deps?: ProjectDependencies): Promise<boolean>;
export interface ProjectStatus {
    identity: ProjectIdentity;
    registration?: ProjectRegistryBinding;
    registered: boolean;
    reason?: ProjectIdentity["reason"];
}
export declare function projectStatus(cwd: string, deps?: ProjectDependencies): Promise<ProjectStatus>;
export declare function projectIdentityFromStoredPath(path: string): ProjectIdentity;
