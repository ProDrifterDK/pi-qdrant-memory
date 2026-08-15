import { projectStatus as resolveProjectStatus, registerProject as registerProjectBinding, unregisterProject as unregisterProjectBinding, } from "../project.js";
function requireConfirmation(confirmed, operation) {
    if (confirmed !== true)
        throw new Error(`${operation} requires explicit human confirmation`);
}
/** Register only canonical XDG path/fingerprint/alias data. */
export async function registerProjectCommand(input, deps) {
    requireConfirmation(input.confirmed, "Project registration");
    const binding = await registerProjectBinding({ path: input.path, alias: input.alias, operator: true }, deps);
    return { ok: true, operation: "register", binding };
}
/** Remove an operator registration; no repository-provided value is trusted. */
export async function unregisterProjectCommand(input, deps) {
    requireConfirmation(input.confirmed, "Project unregistration");
    const removed = await unregisterProjectBinding(input.alias, deps);
    return { ok: true, operation: "unregister", removed };
}
/** Resolve current identity and registration validity without mutating state. */
export async function projectStatusCommand(path, deps) {
    const status = await resolveProjectStatus(path, deps);
    return { ok: true, operation: "status", status };
}
// Explicit aliases keep the admin module discoverable without exposing a
// generic registry writer under a model-callable package surface.
export const registerProject = registerProjectCommand;
export const unregisterProject = unregisterProjectCommand;
export const projectStatus = projectStatusCommand;
//# sourceMappingURL=project.js.map