import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { realpath } from "node:fs/promises";
import { basename, isAbsolute, normalize, resolve } from "node:path";
function defaultGitTopLevel(cwd) {
    return new Promise((resolveOutput, reject) => {
        execFile("git", ["-C", cwd, "rev-parse", "--show-toplevel"], { shell: false, encoding: "utf8" }, (error, stdout) => {
            if (error) {
                reject(error);
                return;
            }
            resolveOutput(String(stdout));
        });
    });
}
const defaultDependencies = {
    gitTopLevel: defaultGitTopLevel,
    canonicalize: (path) => realpath(path),
};
function identityForPath(path) {
    const id = createHash("sha256").update(path, "utf8").digest("hex");
    return { id, label: basename(path) };
}
export async function resolveProjectIdentity(cwd, deps = defaultDependencies) {
    let candidate = resolve(cwd);
    try {
        const gitRoot = (await deps.gitTopLevel(cwd)).trim();
        if (gitRoot.length === 0)
            throw new Error("Git returned an empty project root");
        candidate = gitRoot;
    }
    catch {
        candidate = resolve(cwd);
    }
    try {
        candidate = await deps.canonicalize(candidate);
    }
    catch {
        candidate = normalize(resolve(cwd));
    }
    return identityForPath(candidate);
}
export function projectIdentityFromStoredPath(path) {
    if (!isAbsolute(path))
        throw new Error("Stored project path must be absolute");
    return identityForPath(normalize(path));
}
//# sourceMappingURL=project.js.map