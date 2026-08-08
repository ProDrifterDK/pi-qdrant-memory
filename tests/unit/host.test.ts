import { describe, expect, it } from "vitest";
import { detectHost, resolvePrimeRlmDepth } from "../../src/host.js";

describe("detectHost", () => {
  it("honors an explicit host override", () => {
    expect(detectHost({ explicit: "prime", env: {}, argv: ["node", "pi"] })).toEqual({
      ok: true,
      host: "prime",
    });
  });

  it.each([
    [{ PRIME_AGENT_CODING_AGENT_DIR: "/prime" }, ["node"], "prime"],
    [{ PI_CODING_AGENT_DIR: "/pi" }, ["node"], "pi"],
    [{}, ["node", "/usr/local/bin/prime-agent"], "prime"],
    [{}, ["node", "/usr/local/bin/pi"], "pi"],
  ])("recognizes process markers (%j, %j)", (env, argv, host) => {
    expect(detectHost({ env, argv })).toEqual({ ok: true, host });
  });

  it("fails closed when markers conflict", () => {
    expect(
      detectHost({
        env: { PRIME_AGENT_CODING_AGENT_DIR: "/prime", PI_CODING_AGENT_DIR: "/pi" },
        argv: ["node"],
      }),
    ).toEqual({ ok: false, reason: "conflict" });
  });

  it("lets an explicit valid host override conflicting markers", () => {
    expect(
      detectHost({
        explicit: "pi",
        env: { PRIME_AGENT_CODING_AGENT_DIR: "/prime", PI_CODING_AGENT_DIR: "/pi" },
        argv: ["node", "prime-agent"],
      }),
    ).toEqual({ ok: true, host: "pi" });
  });

  it("rejects an unknown explicit host and unknown processes", () => {
    expect(detectHost({ explicit: "other", env: {}, argv: ["node"] })).toEqual({
      ok: false,
      reason: "invalid-explicit-host",
    });
    expect(detectHost({ env: {}, argv: ["node", "worker"] })).toEqual({
      ok: false,
      reason: "unknown",
    });
  });
});

describe("resolvePrimeRlmDepth", () => {
  it("prefers persisted Prime depth", () => {
    expect(resolvePrimeRlmDepth({ rlmDepth: 2 }, { RLM_DEPTH: "1" })).toBe(2);
  });

  it("uses the environment fallback", () => {
    expect(resolvePrimeRlmDepth({}, { RLM_DEPTH: "1" })).toBe(1);
  });

  it.each([
    ["-1", "non-negative integer"],
    ["1.5", "non-negative integer"],
    ["nope", "non-negative integer"],
  ])("rejects invalid environment depth %s", (value, message) => {
    expect(() => resolvePrimeRlmDepth({}, { RLM_DEPTH: value })).toThrow(message);
  });

  it("rejects invalid persisted depth", () => {
    expect(() => resolvePrimeRlmDepth({ rlmDepth: Number.MAX_SAFE_INTEGER + 1 }, {})).toThrow(
      "non-negative integer",
    );
    expect(() => resolvePrimeRlmDepth({ rlmDepth: "2" }, {})).toThrow("non-negative integer");
  });

  it("defaults missing depth to zero", () => {
    expect(resolvePrimeRlmDepth({}, {})).toBe(0);
  });
});
