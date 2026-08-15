import { describe, expect, it } from "vitest";
import { projectIdentityFromStoredPath, projectStatus, registerProject, resolveProjectIdentity, unregisterProject, type ProjectDependencies } from "../../src/project.js";

describe("resolveProjectIdentity", () => {
  it("hashes the canonical git root without exposing its path", async () => {
    const identity = await resolveProjectIdentity("/work/repo/subdir", {
      gitTopLevel: async () => "/work/repo\n",
      canonicalize: async (value) => value,
    });
    expect(identity.label).toBe("repo");
    expect(identity.id).toMatch(/^[a-f0-9]{64}$/);
    expect(identity.id).not.toContain("/work/repo");
  });

  it("falls back to cwd when Git resolution fails", async () => {
    const identity = await resolveProjectIdentity("/work/not-a-repo/subdir", {
      gitTopLevel: async () => {
        throw new Error("not a repository");
      },
      canonicalize: async (value) => value,
    });
    expect(identity.label).toBe("subdir");
  });

  it("uses the canonicalized fallback path", async () => {
    let canonicalized: string | undefined;
    const identity = await resolveProjectIdentity("/work/repo/subdir", {
      gitTopLevel: async () => "/work/repo\n",
      canonicalize: async (value) => {
        canonicalized = value;
        return "/canonical/repo";
      },
    });
    expect(canonicalized).toBe("/work/repo");
    expect(identity.label).toBe("repo");
  });
});

describe("projectIdentityFromStoredPath", () => {
  it("normalizes an imported absolute path lexically without requiring it to exist", () => {
    const identity = projectIdentityFromStoredPath("/does/not/exist/../repo//");
    expect(identity.label).toBe("repo");
    expect(identity.id).toMatch(/^[a-f0-9]{64}$/);
  });

  it("rejects relative paths", () => {
    expect(() => projectIdentityFromStoredPath("relative/repo")).toThrow("absolute");
  });
});


describe("operator project registration", () => {
  function deps(files: Map<string, string>, origin = "https://github.com/example/repo.git?token=ignored"): ProjectDependencies {
    return {
      registryPath: "/xdg/pi-qdrant-memory/config.json",
      installationSalt: "installation-a",
      gitTopLevel: async () => "/repo",
      canonicalize: async (value) => value === "/repo" || value === "/repo/sub" ? value : value,
      gitOrigin: async () => origin,
      readTextFile: async (path) => {
        const content = files.get(path);
        if (content === undefined) throw Object.assign(new Error("missing"), { code: "ENOENT" });
        return content;
      },
      writeTextFile: async (path, text) => { files.set(path, text); },
    };
  }
  it("registers canonical path and redacted fingerprint in XDG, then resolves the alias", async () => {
    const files = new Map<string, string>(); const projectDeps = deps(files);
    await expect(registerProject("/repo", "stable-repo", projectDeps)).resolves.toMatchObject({ canonicalPath: "/repo", alias: "stable-repo", fingerprint: "origin:github.com/example/repo" });
    await expect(resolveProjectIdentity("/repo/sub", projectDeps)).resolves.toMatchObject({ id: "stable-repo", identityKind: "registered", registrationValid: true });
    await expect(projectStatus("/repo/sub", projectDeps)).resolves.toMatchObject({ registered: true, registration: { alias: "stable-repo" } });
    await expect(unregisterProject("stable-repo", projectDeps)).resolves.toBe(true);
    await expect(resolveProjectIdentity("/repo/sub", projectDeps)).resolves.toMatchObject({ identityKind: "local_only", reason: "unregistered" });
  });
  it("prefers strict active-host registrations supplied by the config loader", async () => {
    const projectDeps = { ...deps(new Map()), registrations: { "host-repo": { canonicalPath: "/repo", alias: "host-repo", fingerprint: "origin:github.com/example/repo" } } };
    await expect(resolveProjectIdentity("/repo/sub", projectDeps)).resolves.toMatchObject({ id: "host-repo", identityKind: "registered", registrationValid: true });
  });
  it("fails closed when a registered path is reached through a symlink escape", async () => {
    const files = new Map<string, string>(); const projectDeps = deps(files);
    await registerProject("/repo", "stable-repo", projectDeps);
    const escaped = { ...projectDeps, canonicalize: async (value: string) => value === "/repo/link" ? "/outside" : value };
    await expect(resolveProjectIdentity("/repo/link", escaped)).resolves.toMatchObject({ identityKind: "local_only", reason: "symlink_escape" });
  });
  it("does not converge on fingerprint mismatch, origin spoofing, or non-operator callers", async () => {
    const files = new Map<string, string>(); const projectDeps = deps(files);
    await registerProject("/repo", "stable-repo", projectDeps);
    await expect(registerProject({ path: "/repo", alias: "forbidden", operator: false }, projectDeps)).rejects.toThrow(/operator/i);
    await expect(resolveProjectIdentity("/repo/sub", deps(files, "https://github.com/other/repo.git"))).resolves.toMatchObject({ identityKind: "local_only", reason: "fingerprint_mismatch" });
    await expect(resolveProjectIdentity("/other/repo", deps(files, "https://github.com/example/repo.git"))).resolves.toMatchObject({ identityKind: "local_only" });
  });
});


describe("persisted operator project registry", () => {
  function persistentDeps(files: Map<string, string>, salt?: string): ProjectDependencies {
    const configPath = "/xdg/pi-qdrant-memory/config.json";
    const statePath = "/xdg/pi-qdrant-memory/state.json";
    return {
      configPath,
      statePath,
      gitTopLevel: async () => "/repo",
      canonicalize: async (value) => value,
      gitOrigin: async () => "https://github.com/example/repo.git?credentials=ignored",
      readTextFile: async (path) => {
        const value = files.get(path);
        if (value === undefined) throw Object.assign(new Error("missing"), { code: "ENOENT" });
        return value;
      },
      writeTextFile: async (path, value) => { files.set(path, value); },
      ...(salt === undefined ? {} : { installationSalt: salt }),
    } as ProjectDependencies & { statePath: string };
  }
  it("persists registration in config and salt in a private sidecar without corrupting loadConfig", async () => {
    const files = new Map([["/xdg/pi-qdrant-memory/config.json", JSON.stringify({ qdrant: { url: "http://127.0.0.1:6333" } })]]);
    const deps = persistentDeps(files);
    await registerProject("/repo", "stable-repo", deps);
    const config = JSON.parse(files.get("/xdg/pi-qdrant-memory/config.json")!);
    expect(config.installationSalt).toBeUndefined();
    expect(config.projects.registrations["stable-repo"]).toMatchObject({ alias: "stable-repo", canonicalPath: "/repo" });
    expect(files.has("/xdg/pi-qdrant-memory/state.json")).toBe(true);
    const loaded = await loadConfigWithFiles(files, deps);
    expect(loaded.projects.registrations["stable-repo"]?.alias).toBe("stable-repo");
    const second = await resolveProjectIdentity("/repo", persistentDeps(files));
    expect(second).toMatchObject({ id: "stable-repo", identityKind: "registered" });
  });
  it("keeps local-only IDs stable across dependency instances and isolates installation salts", async () => {
    const files = new Map<string, string>();
    const first = await resolveProjectIdentity("/repo", persistentDeps(files, "salt-a"));
    const second = await resolveProjectIdentity("/repo", persistentDeps(files, "salt-a"));
    const other = await resolveProjectIdentity("/repo", persistentDeps(new Map(), "salt-b"));
    expect(first).toMatchObject({ identityKind: "local_only", registrationValid: true, reason: "unregistered" });
    expect(second.id).toBe(first.id);
    expect(other.id).not.toBe(first.id);
  });
  it("fails closed on malformed persisted bindings and ambiguous overlaps", async () => {
    const malformed = new Map([["/xdg/pi-qdrant-memory/config.json", JSON.stringify({ projects: { registrations: { bad: { canonicalPath: "/repo" } } } })]]);
    await expect(resolveProjectIdentity("/repo", persistentDeps(malformed, "salt-a"))).rejects.toThrow(/registry/i);
    const files = new Map<string, string>(); const deps = persistentDeps(files, "salt-a");
    await registerProject("/repo", "stable-repo", deps);
    await expect(registerProject("/repo/sub", "other-alias", deps)).rejects.toThrow(/overlap|ambiguous/i);
  });
});

async function loadConfigWithFiles(files: Map<string, string>, deps: ProjectDependencies) {
  const { loadConfig } = await import("../../src/config.js");
  return loadConfig("pi", { env: {}, homeDir: "/home/tester", xdgConfigHome: "/xdg", readTextFile: async (path) => {
    const value = files.get(path); if (value === undefined) throw Object.assign(new Error("missing"), { code: "ENOENT" }); return value;
  } });
}


describe("registry key safety", () => {
  it.each(["__proto__", "prototype", "constructor"]) ("rejects dangerous alias %s from registration", async (alias) => {
    const files = new Map<string, string>();
    const projectDeps: ProjectDependencies = {
      registryPath: "/xdg/config.json", installationSalt: "salt", gitTopLevel: async () => "/repo", canonicalize: async (value) => value, gitOrigin: async () => "https://github.com/example/repo.git",
      readTextFile: async (path) => { const value = files.get(path); if (value === undefined) throw Object.assign(new Error("missing"), { code: "ENOENT" }); return value; }, writeTextFile: async (path, value) => { files.set(path, value); },
    };
    await expect(registerProject("/repo", alias, projectDeps)).rejects.toThrow(/alias/i);
  });
  it("rejects dangerous aliases in raw persisted JSON", async () => {
    const files = new Map([["/xdg/config.json", JSON.stringify({ projects: { registrations: { constructor: { canonicalPath: "/repo", fingerprint: "origin:github.com/example/repo", alias: "constructor" } } } })]]);
    const projectDeps: ProjectDependencies = { registryPath: "/xdg/config.json", statePath: "/xdg/state.json", readTextFile: async (path) => files.get(path) ?? JSON.stringify({ installationSalt: "salt" }), gitTopLevel: async () => "/repo", canonicalize: async (value) => value, gitOrigin: async () => "https://github.com/example/repo.git" };
    await expect(resolveProjectIdentity("/repo", projectDeps)).rejects.toThrow(/registry|alias/i);
  });
});
