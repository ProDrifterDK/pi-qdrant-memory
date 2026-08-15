import { type ProjectDependencies, type ProjectIdentity, type ProjectRegistryBinding, type ProjectStatus } from "../project.js";
/** Human-operated project registration input. The confirmation bit is never
 * accepted from RuntimeConfig or a model/tool payload; the CLI owns it. */
export interface ProjectRegisterCommand {
    path: string;
    alias: string;
    confirmed: boolean;
}
export interface ProjectUnregisterCommand {
    alias: string;
    confirmed: boolean;
}
export interface ProjectCommandDependencies extends ProjectDependencies {
    /** Optional operator confirmation seam for embedders that do not use the CLI. */
    confirm?(prompt: string): Promise<boolean>;
}
export interface ProjectCommandResult {
    ok: true;
    operation: "register" | "unregister" | "status";
    binding?: ProjectRegistryBinding;
    removed?: boolean;
    status?: ProjectStatus;
}
/** Register only canonical XDG path/fingerprint/alias data. */
export declare function registerProjectCommand(input: ProjectRegisterCommand, deps?: ProjectCommandDependencies): Promise<ProjectCommandResult>;
/** Remove an operator registration; no repository-provided value is trusted. */
export declare function unregisterProjectCommand(input: ProjectUnregisterCommand, deps?: ProjectCommandDependencies): Promise<ProjectCommandResult>;
/** Resolve current identity and registration validity without mutating state. */
export declare function projectStatusCommand(path: string, deps?: ProjectCommandDependencies): Promise<ProjectCommandResult>;
export declare const registerProject: typeof registerProjectCommand;
export declare const unregisterProject: typeof unregisterProjectCommand;
export declare const projectStatus: typeof projectStatusCommand;
export type { ProjectDependencies, ProjectIdentity, ProjectRegistryBinding, ProjectStatus };
export type { ProjectIdentity as RegisteredProjectIdentity };
