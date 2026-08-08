export interface ProjectIdentity {
    id: string;
    label: string;
}
export interface ProjectDependencies {
    gitTopLevel(cwd: string): Promise<string>;
    canonicalize(path: string): Promise<string>;
}
export declare function resolveProjectIdentity(cwd: string, deps?: ProjectDependencies): Promise<ProjectIdentity>;
export declare function projectIdentityFromStoredPath(path: string): ProjectIdentity;
