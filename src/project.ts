import { createHash, randomBytes } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, normalize, relative, resolve } from "node:path";
import type { HostId } from "./types.js";
import { configPath } from "./config.js";
import { canonicalStringify, sha256Hex } from "./domain/canonical.js";

const SECRET = /(api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;

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

interface RegistryFile {
  version: 1;
  projects: { registrations: Record<string, ProjectRegistryBinding> };
  raw?: Record<string, unknown>;
}
interface InstallationState { installationSalt: string; }

function execGit(args: readonly string[], cwd: string): Promise<string> {
  return new Promise((resolveOutput, reject) => {
    execFile("git", ["-C", cwd, ...args], { shell: false, encoding: "utf8" }, (error, stdout) => {
      if (error) { reject(error); return; }
      resolveOutput(String(stdout).trim());
    });
  });
}
function defaultGitTopLevel(cwd: string): Promise<string> { return execGit(["rev-parse", "--show-toplevel"], cwd); }
function defaultGitOrigin(cwd: string): Promise<string | undefined> { return execGit(["config", "--get", "remote.origin.url"], cwd).catch(() => undefined); }
function defaultGitRootCommits(cwd: string): Promise<readonly string[]> {
  return execGit(["rev-list", "--max-parents=0", "HEAD"], cwd).then((value) => value.split(/\s+/u).filter(Boolean)).catch(() => []);
}
const defaultDependencies: ProjectDependencies = {
  gitTopLevel: defaultGitTopLevel,
  canonicalize: (path) => realpath(path),
  gitOrigin: defaultGitOrigin,
  gitRootCommits: defaultGitRootCommits,
  readTextFile: (path) => readFile(path, "utf8"),
  writeTextFile: async (path, text) => { await mkdir(dirname(path), { recursive: true, mode: 0o700 }); await writeFile(path, text, { encoding: "utf8", mode: 0o600 }); },
};

function registryFilePath(deps: ProjectDependencies): string | undefined {
  if (deps.registryPath !== undefined) return deps.registryPath;
  if (deps.configPath !== undefined) return deps.configPath;
  if (deps.homeDir !== undefined) return deps.xdgConfigHome === undefined
    ? configPath({ homeDir: deps.homeDir })
    : configPath({ homeDir: deps.homeDir, xdgConfigHome: deps.xdgConfigHome });
  return undefined;
}
function safeAlias(alias: string): string {
  if (typeof alias !== "string" || alias.length === 0 || alias.length > 256 || /[\u0000-\u001f]/u.test(alias) || /(?:api[-_]?key|token|secret|password)/iu.test(alias) || alias === "__proto__" || alias === "prototype" || alias === "constructor") throw new TypeError("Project alias must be stable, bounded, non-secret, and safe");
  return alias;
}
function inside(root: string, candidate: string): boolean {
  const path = relative(root, candidate);
  return path === "" || (path !== ".." && !path.startsWith(`..${"/"}`) && !isAbsolute(path));
}
function fingerprintFromOrigin(origin: string): string {
  let value = origin.trim();
  value = value.replace(/^\w+:\/\//u, "");
  value = value.replace(/^[^/\s]+@/u, "");
  value = value.split(/[?#]/u, 1)[0] ?? value;
  value = value.replace(/\.git$/iu, "");
  value = value.replace(/\/+$/u, "");
  if (value.startsWith("git@") && value.includes(":")) value = value.replace(/^git@/u, "");
  value = value.replace(/^([^/]+):/u, "$1/");
  return value;
}
async function projectFingerprint(root: string, deps: ProjectDependencies): Promise<string> {
  const origin = await (deps.gitOrigin ?? defaultGitOrigin)(root);
  if (origin !== undefined && origin.trim() !== "") {
    const fingerprint = fingerprintFromOrigin(origin);
    if (fingerprint.length === 0 || fingerprint.length > 512 || SECRET.test(fingerprint)) throw new Error("Project origin fingerprint is invalid");
    return `origin:${fingerprint}`;
  }
  const commits = await (deps.gitRootCommits ?? defaultGitRootCommits)(root);
  if (commits.length > 256) throw new Error("Project root commit fingerprint is unbounded");
  const boundedCommits = [...commits].map((value) => value.trim());
  if (boundedCommits.some((value) => !/^[0-9a-f]{7,128}$/iu.test(value))) throw new Error("Project root commit fingerprint is invalid");
  if (boundedCommits.length > 0) return `roots:${boundedCommits.sort().join(",")}`;
  return "roots:unknown";
}

export { fingerprintFromOrigin };
const generatedSalts = new Map<string, string>();
function stateFilePath(deps: ProjectDependencies): string | undefined {
  if (deps.statePath !== undefined) return deps.statePath;
  const registry = registryFilePath(deps);
  return registry === undefined ? undefined : join(dirname(registry), "state.json");
}
function generatedInstallationSalt(deps: ProjectDependencies): string {
  const key = stateFilePath(deps) ?? deps.homeDir ?? "process-installation";
  const existing = generatedSalts.get(key); if (existing !== undefined) return existing;
  const generated = randomBytes(24).toString("hex"); generatedSalts.set(key, generated); return generated;
}
async function loadInstallationState(deps: ProjectDependencies): Promise<InstallationState> {
  const path = stateFilePath(deps);
  if (path === undefined || deps.readTextFile === undefined) return { installationSalt: generatedInstallationSalt(deps) };
  try {
    const parsed: unknown = JSON.parse(await deps.readTextFile(path));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("state");
    const root = parsed as Record<string, unknown>;
    if (Object.keys(root).some((key) => key !== "installationSalt") || typeof root.installationSalt !== "string" || root.installationSalt.length === 0 || root.installationSalt.length > 256) throw new Error("state");
    return { installationSalt: root.installationSalt };
  } catch (error: unknown) {
    if (typeof error === "object" && error !== null && (error as { code?: string }).code === "ENOENT") {
      const generated = { installationSalt: deps.installationSalt ?? generatedInstallationSalt(deps) };
      if (deps.writeTextFile !== undefined) await deps.writeTextFile(path, JSON.stringify(generated) + "\n");
      return generated;
    }
    throw new Error("Project state is unreadable");
  }
}
async function saveInstallationState(state: InstallationState, deps: ProjectDependencies): Promise<void> {
  const path = stateFilePath(deps); if (path === undefined || deps.writeTextFile === undefined) return;
  await deps.writeTextFile(path, JSON.stringify(state) + "\n");
}
function emptyRegistry(): RegistryFile {
  return { version: 1, projects: { registrations: Object.create(null) as Record<string, ProjectRegistryBinding> }, raw: {} };
}
function normalizeBinding(value: unknown, key: string): ProjectRegistryBinding {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`Project registry binding ${key} is invalid`);
  const item = value as Record<string, unknown>;
  if (Object.keys(item).some((field) => !["canonicalPath", "fingerprint", "alias"].includes(field))) throw new Error(`Project registry binding ${key} has unknown fields`);
  if (typeof item.canonicalPath !== "string" || !isAbsolute(item.canonicalPath) || item.canonicalPath.length > 4096 || typeof item.fingerprint !== "string" || item.fingerprint.length === 0 || item.fingerprint.length > 512 || typeof item.alias !== "string" || item.alias.length === 0 || item.alias.length > 256 || item.alias !== key) throw new Error(`Project registry binding ${key} is invalid`);
  safeAlias(item.alias);
  if (SECRET.test(item.fingerprint)) throw new Error(`Project registry binding ${key} is not redacted`);
  return { canonicalPath: normalize(item.canonicalPath), fingerprint: item.fingerprint, alias: item.alias };
}
async function loadRegistry(deps: ProjectDependencies): Promise<RegistryFile> {
  const path = registryFilePath(deps);
  if (path === undefined || deps.readTextFile === undefined) return emptyRegistry();
  try {
    const parsed: unknown = JSON.parse(await deps.readTextFile(path));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("registry");
    const root = parsed as Record<string, unknown>;
    if (Object.prototype.hasOwnProperty.call(root, "installationSalt")) throw new Error("Project installation salt must be stored in private state");
    const projects = root.projects;
    if (projects === undefined) return { version: 1, projects: { registrations: Object.create(null) as Record<string, ProjectRegistryBinding> }, raw: root };
    if (typeof projects !== "object" || projects === null || Array.isArray(projects)) throw new Error("registry");
    const projectsRecord = projects as Record<string, unknown>;
    const registrationsValue = projectsRecord.registrations;
    if (registrationsValue === undefined) return { version: 1, projects: { registrations: Object.create(null) as Record<string, ProjectRegistryBinding> }, raw: root };
    if (typeof registrationsValue !== "object" || registrationsValue === null || Array.isArray(registrationsValue)) throw new Error("registry");
    const normalized: Record<string, ProjectRegistryBinding> = Object.create(null) as Record<string, ProjectRegistryBinding>;
    for (const [key, value] of Object.entries(registrationsValue)) normalized[key] = normalizeBinding(value, key);
    return { version: 1, projects: { registrations: normalized }, raw: root };
  } catch (error: unknown) {
    if (typeof error === "object" && error !== null && (error as { code?: string }).code === "ENOENT") return emptyRegistry();
    if (error instanceof Error && error.message === "Project registry is unreadable") throw error;
    throw new Error("Project registry is unreadable");
  }
}
async function saveRegistry(registry: RegistryFile, deps: ProjectDependencies): Promise<void> {
  const path = registryFilePath(deps);
  if (path === undefined || deps.writeTextFile === undefined) throw new Error("Project registration requires an XDG writable registry");
  const parent = dirname(path);
  const output: Record<string, unknown> = { ...(registry.raw ?? {}) };
  const priorProjects = typeof output.projects === "object" && output.projects !== null && !Array.isArray(output.projects)
    ? output.projects as Record<string, unknown>
    : {};
  delete output.installationSalt;
  output.projects = { ...priorProjects, registrations: registry.projects.registrations };
  if (deps.writeTextFile === defaultDependencies.writeTextFile) await mkdir(parent, { recursive: true, mode: 0o700 });
  await deps.writeTextFile(path, JSON.stringify(output, null, 2) + "\n");
}
function localIdentity(path: string, fingerprint: string, salt: string, reason: ProjectIdentity["reason"]): ProjectIdentity {
  const id = sha256Hex(canonicalStringify({ installationSalt: salt, canonicalPath: path, vcsFingerprint: fingerprint }));
  return { id, label: basename(path), identityKind: "local_only", canonicalPath: path, fingerprint, registrationValid: false, ...(reason === undefined ? {} : { reason }) };
}

export async function resolveProjectIdentity(cwd: string, deps: ProjectDependencies = defaultDependencies): Promise<ProjectIdentity> {
  const requested = resolve(cwd);
  let requestedCanonical = requested;
  try { requestedCanonical = await deps.canonicalize(requested); } catch { requestedCanonical = normalize(requested); }
  let candidate = requestedCanonical;
  try {
    const gitRoot = (await deps.gitTopLevel(requested)).trim();
    if (gitRoot.length === 0) throw new Error("Git returned an empty project root");
    candidate = await deps.canonicalize(gitRoot);
  } catch { /* non-repository projects are identified by their canonical cwd */ }
  const fingerprint = await projectFingerprint(candidate, deps);
  const registry = await loadRegistry(deps);
  const state = await loadInstallationState(deps);
  const bindings = Object.values(registry.projects.registrations);
  const matchingPaths = bindings.filter((binding) => inside(binding.canonicalPath, candidate) && inside(binding.canonicalPath, requestedCanonical));
  if (matchingPaths.length > 1) throw new Error("Project registry bindings are ambiguous");
  const matchingPath = matchingPaths[0];
  if (matchingPath !== undefined) {
    if (matchingPath.fingerprint === fingerprint) return { id: matchingPath.alias, label: basename(candidate), identityKind: "registered", canonicalPath: candidate, fingerprint, registrationValid: true };
    return localIdentity(candidate, fingerprint, state.installationSalt, "fingerprint_mismatch");
  }
  const escaped = bindings.some((binding) =>
    (inside(binding.canonicalPath, requested) && !inside(binding.canonicalPath, requestedCanonical)) ||
    (inside(binding.canonicalPath, requestedCanonical) && !inside(binding.canonicalPath, candidate)),
  );
  return localIdentity(candidate, fingerprint, state.installationSalt, escaped ? "symlink_escape" : "unregistered");
}

export interface ProjectRegisterInput { path: string; alias: string; operator?: boolean; }

export async function registerProject(input: ProjectRegisterInput, deps?: ProjectDependencies): Promise<ProjectRegistryBinding>;
export async function registerProject(path: string, alias: string, deps?: ProjectDependencies): Promise<ProjectRegistryBinding>;
export async function registerProject(inputOrPath: ProjectRegisterInput | string, aliasOrDeps?: string | ProjectDependencies, maybeDeps: ProjectDependencies = defaultDependencies): Promise<ProjectRegistryBinding> {
  const input: ProjectRegisterInput = typeof inputOrPath === "string" ? { path: inputOrPath, alias: aliasOrDeps as string } : inputOrPath;
  const deps = typeof aliasOrDeps === "object" ? aliasOrDeps : maybeDeps;
  if (input.operator === false || deps.operator === false) throw new Error("Project registration is operator-only");
  const alias = safeAlias(input.alias);
  const canonicalPath = await deps.canonicalize(resolve(input.path));
  const root = (await deps.gitTopLevel(canonicalPath).catch(() => canonicalPath)).trim();
  const canonicalRoot = await deps.canonicalize(root);
  if (!inside(canonicalRoot, canonicalPath)) throw new Error("Project path escapes its canonical Git root");
  const fingerprint = await projectFingerprint(canonicalRoot, deps);
  const registry = await loadRegistry(deps);
  const overlap = Object.values(registry.projects.registrations).find((binding) => binding.alias === alias || inside(binding.canonicalPath, canonicalRoot) || inside(canonicalRoot, binding.canonicalPath));
  if (overlap !== undefined) throw new Error("Project registration alias/path overlap is ambiguous");
  registry.projects.registrations[alias] = { canonicalPath: canonicalRoot, fingerprint, alias };
  await saveRegistry(registry, deps);
  const state = await loadInstallationState(deps); await saveInstallationState(state, deps);
  return registry.projects.registrations[alias]!;
}

export async function unregisterProject(aliasOrPath: string, deps?: ProjectDependencies): Promise<boolean> {
  if (deps?.operator === false) throw new Error("Project registration is operator-only");
  const activeDeps = deps ?? defaultDependencies;
  const registry = await loadRegistry(activeDeps);
  let alias = Object.prototype.hasOwnProperty.call(registry.projects.registrations, aliasOrPath) ? aliasOrPath : undefined;
  if (alias === undefined && isAbsolute(aliasOrPath)) alias = Object.entries(registry.projects.registrations).find(([, binding]) => binding.canonicalPath === normalize(aliasOrPath))?.[0];
  if (alias === undefined) return false;
  delete registry.projects.registrations[alias];
  await saveRegistry(registry, activeDeps);
  return true;
}

export interface ProjectStatus {
  identity: ProjectIdentity;
  registration?: ProjectRegistryBinding;
  registered: boolean;
  reason?: ProjectIdentity["reason"];
}
export async function projectStatus(cwd: string, deps: ProjectDependencies = defaultDependencies): Promise<ProjectStatus> {
  const identity = await resolveProjectIdentity(cwd, deps);
  const registry = await loadRegistry(deps);
  const registration = Object.values(registry.projects.registrations).find((binding) => binding.alias === identity.id || binding.canonicalPath === identity.canonicalPath);
  return { identity, ...(registration === undefined ? {} : { registration }), registered: identity.identityKind === "registered", ...(identity.reason === undefined ? {} : { reason: identity.reason }) };
}

export function projectIdentityFromStoredPath(path: string): ProjectIdentity {
  if (!isAbsolute(path)) throw new Error("Stored project path must be absolute");
  const normalized = normalize(path);
  return localIdentity(normalized, "roots:unknown", "stored-path", "unregistered");
}
