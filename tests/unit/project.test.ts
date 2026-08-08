import { describe, expect, it } from "vitest";
import { projectIdentityFromStoredPath, resolveProjectIdentity } from "../../src/project.js";

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
