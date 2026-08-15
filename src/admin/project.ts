import {
  projectStatus as resolveProjectStatus,
  registerProject as registerProjectBinding,
  unregisterProject as unregisterProjectBinding,
  type ProjectDependencies,
  type ProjectIdentity,
  type ProjectRegistryBinding,
  type ProjectStatus,
} from "../project.js";

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

function requireConfirmation(confirmed: boolean, operation: string): void {
  if (confirmed !== true) throw new Error(`${operation} requires explicit human confirmation`);
}

/** Register only canonical XDG path/fingerprint/alias data. */
export async function registerProjectCommand(input: ProjectRegisterCommand, deps?: ProjectCommandDependencies): Promise<ProjectCommandResult> {
  requireConfirmation(input.confirmed, "Project registration");
  const binding = await registerProjectBinding({ path: input.path, alias: input.alias, operator: true }, deps);
  return { ok: true, operation: "register", binding };
}

/** Remove an operator registration; no repository-provided value is trusted. */
export async function unregisterProjectCommand(input: ProjectUnregisterCommand, deps?: ProjectCommandDependencies): Promise<ProjectCommandResult> {
  requireConfirmation(input.confirmed, "Project unregistration");
  const removed = await unregisterProjectBinding(input.alias, deps);
  return { ok: true, operation: "unregister", removed };
}

/** Resolve current identity and registration validity without mutating state. */
export async function projectStatusCommand(path: string, deps?: ProjectCommandDependencies): Promise<ProjectCommandResult> {
  const status = await resolveProjectStatus(path, deps);
  return { ok: true, operation: "status", status };
}

// Explicit aliases keep the admin module discoverable without exposing a
// generic registry writer under a model-callable package surface.
export const registerProject = registerProjectCommand;
export const unregisterProject = unregisterProjectCommand;
export const projectStatus = projectStatusCommand;
export type { ProjectDependencies, ProjectIdentity, ProjectRegistryBinding, ProjectStatus };
export type { ProjectIdentity as RegisteredProjectIdentity };
