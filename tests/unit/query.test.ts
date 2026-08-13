import { describe, expect, it } from "vitest";
import {
  buildEffectiveQuery,
  isNaturalLanguagePrompt,
  priorUserPromptsFromBranch,
  parseRetrievalWindow,
  userTextFromMessage,
} from "../../src/query.js";

describe("isNaturalLanguagePrompt", () => {
  it("accepts non-empty prose and rejects commands or whitespace", () => {
    expect(isNaturalLanguagePrompt("  investigate this  ")).toBe(true);
    expect(isNaturalLanguagePrompt("   ")).toBe(false);
    expect(isNaturalLanguagePrompt(" /help")).toBe(false);
  });
});

describe("userTextFromMessage", () => {
  it("accepts user string content", () => {
    expect(userTextFromMessage({ role: "user", content: "hello" })).toBe("hello");
  });

  it("joins only text content blocks", () => {
    expect(
      userTextFromMessage({
        role: "user",
        content: [
          { type: "text", text: "first" },
          { type: "image", data: "ignored" },
          { type: "text", text: "second" },
        ],
      }),
    ).toBe("firstsecond");
  });

  it("ignores non-user and malformed messages", () => {
    expect(userTextFromMessage({ role: "assistant", content: "no" })).toBeUndefined();
    expect(userTextFromMessage({ role: "user", content: [{ type: "image" }] })).toBe("");
    expect(userTextFromMessage("not a message")).toBeUndefined();
  });
});

describe("priorUserPromptsFromBranch", () => {
  it("keeps substantive user text in branch order", () => {
    expect(
      priorUserPromptsFromBranch([
        { type: "message", message: { role: "user", content: "first" } },
        { type: "thinking", message: { role: "user", content: "ignored" } },
        { type: "message", message: { role: "assistant", content: "ignored" } },
        { type: "message", message: { role: "user", content: "second" } },
      ]),
    ).toEqual(["first", "second"]);
  });
});

describe("buildEffectiveQuery", () => {
  it("combines a short continuation with the latest substantive prompt", () => {
    expect(buildEffectiveQuery("sí", ["investiga Qdrant para Prime Agent"])).toBe(
      "investiga Qdrant para Prime Agent\n\nsí",
    );
  });

  it("returns long current prompts without prior context", () => {
    expect(buildEffectiveQuery("x".repeat(20), ["prior substantive prompt"])).toBe("x".repeat(20));
  });

  it("caps the effective query at 4000 characters", () => {
    expect(buildEffectiveQuery("continúa", ["x".repeat(5000)])).toHaveLength(4000);
  });

  it("normalizes whitespace and ignores commands and whitespace-only prompts", () => {
    expect(buildEffectiveQuery("  hello\n\tworld ", [])).toBe("hello world");
    expect(buildEffectiveQuery(" /help ", ["a substantive prompt that should not be used"])).toBeUndefined();
    expect(buildEffectiveQuery("   ", ["a substantive prompt that should not be used"])).toBeUndefined();
  });
});


describe("parseRetrievalWindow", () => {
  it("accepts strict RFC3339 offsets and canonicalizes them to instants", () => {
    expect(parseRetrievalWindow("2026-08-13T10:30:00-04:00", "2026-08-13T16:00:00+01:00")).toEqual({
      after: "2026-08-13T14:30:00.000Z",
      before: "2026-08-13T15:00:00.000Z",
    });
    expect(parseRetrievalWindow(undefined, "2026-08-13T15:00:00Z")).toEqual({ before: "2026-08-13T15:00:00.000Z" });
  });

  it.each([
    ["2026-08-13", undefined],
    ["2026-08-13T15:00:00", undefined],
    ["2026-02-30T15:00:00Z", undefined],
    ["2026-08-13T25:00:00Z", undefined],
    ["2026-08-13T15:00:00+25:00", undefined],
    ["2026-08-13T16:00:00Z", "2026-08-13T15:00:00Z"],
  ])("rejects malformed or reversed bounds (%s, %s)", (after, before) => {
    expect(() => parseRetrievalWindow(after, before)).toThrow(/retrieval time/i);
  });
});
