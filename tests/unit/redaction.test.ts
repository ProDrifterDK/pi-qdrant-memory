import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { redactAndScan, redactStructure } from "../../src/security/redaction.js";
import { scanFinalText } from "../../src/capture/scanner.js";
import { canEgress, destinationForEndpoint } from "../../src/security/egress.js";

describe("Task 4 structural redaction and final scanner", () => {
  it("redacts sensitive fields, URLs, HOME, controls and normalizes Unicode before hashing", () => {
    const result = redactStructure({
      text: `Authorization: Bearer secret-token-123456
apiKey="sk-abcdefghijklmnopqrstuvwxyz123456" cookie: session=abc
https://alice:pw@example.test/path?token=query-secret&ok=1 /home/tester/project e\u0301\u0007`,
      maxChars: 2000,
      homeDir: "/home/tester",
    });
    expect(result.redactionStatus).toBe("redacted");
    expect(result.text).not.toContain("secret-token-123456");
    expect(result.text).not.toContain("sk-abcdefghijklmnopqrstuvwxyz123456");
    expect(result.text).not.toContain("/home/tester");
    expect(result.text).toContain("é");
    expect(result.text).not.toMatch(/[\u0000-\u0008\u000b-\u001f\u007f]/u);
    expect(result.text).toContain("\n");
    expect(result.contentHash).toMatch(/^[a-f0-9]{64}$/u);
    expect(scanFinalText(result.text)).toBe("passed");
  });

  it("does not classify real UUIDs, UUIDv7, ULID, or commit SHA as bare secrets", () => {
    expect(scanFinalText("019fdef5-34fc-7189-8d71-ca9f9f9d9fc7")).toBe("passed");
    expect(scanFinalText("550e8400-e29b-41d4-a716-446655440000")).toBe("passed");
    expect(scanFinalText("01ARZ3NDEKTSV4RRFFQ69G5FAV")).toBe("passed");
    expect(scanFinalText("0123456789abcdef0123456789abcdef01234567")).toBe("passed");
    expect(scanFinalText("opaque 0123456789abcdef0123456789abcdef0123456789abcdef")).toBe("rejected");
    expect(scanFinalText("token=019fdef5-34fc-7189-8d71-ca9f9f9d9fc7")).toBe("rejected");
  });

  it("returns the exact final redacted contract and honors injected rejected/error scanners", () => {
    const unchanged = redactAndScan({ text: "https://example.test/path?a=1", maxChars: 2000, homeDir: "/home/tester", scan: () => "passed" });
    expect(unchanged).toMatchObject({ text: "https://example.test/path?a=1", redactionStatus: "unchanged", secretScan: "passed", dropped: false });
    expect(unchanged.contentHash).toBe(createHash("sha256").update(unchanged.text).digest("hex"));
    expect(redactAndScan({ text: "safe", maxChars: 20, homeDir: "/home/x", scan: () => "rejected" })).toMatchObject({ secretScan: "rejected", dropped: true, text: "" });
    expect(redactAndScan({ text: "safe", maxChars: 20, homeDir: "/home/x", scan: () => "error" })).toMatchObject({ secretScan: "error", dropped: true, text: "" });
  });

  it("authorizes a genuinely redacted payload after canonical rescan", () => {
    const destination = destinationForEndpoint("http://127.0.0.1:6333", "node-redacted");
    const final = redactAndScan({ text: "password=hunter2long", maxChars: 2000, homeDir: "/home/x" });
    expect(final.redactionStatus).toBe("redacted"); expect(final.secretScan).toBe("passed");
    expect(canEgress({ mode: "local_only", destination, allowlist: [], material: final, payload: { maxChars: 2000, homeDir: "/home/x" } })).toBe(true);
  });

  it("requires a canonical, rescanned payload before destination authorization", () => {
    const destination = destinationForEndpoint("http://127.0.0.1:6333", "node-a");
    const base = { mode: "local_only" as const, destination, allowlist: [] as const };
    expect(canEgress(base)).toBe(false);
    expect(canEgress({ ...base, material: { text: "raw Authorization: Bearer raw-token-123456", redactionStatus: "unchanged", secretScan: "passed", dropped: false, contentHash: "fake" }, payload: { maxChars: 2000, homeDir: "/home/x" } })).toBe(false);
    const final = redactAndScan({ text: "safe", maxChars: 2000, homeDir: "/home/x" });
    expect(canEgress({ ...base, material: final, payload: { maxChars: 2000, homeDir: "/home/x" } })).toBe(true);
    expect(canEgress({ ...base, material: { ...final, text: "$HOME/raw" }, payload: { maxChars: 2000, homeDir: "/home/x" } })).toBe(false);
  });

  it("keeps HOME boundaries and line boundaries while redacting nested/camel-case JSON", () => {
    const result = redactAndScan({ text: "path=/home/test/x\nkeep this", maxChars: 2000, homeDir: "/home/test" });
    expect(result.text).toContain("$HOME/x"); expect(result.text).toContain("keep this"); expect(result.secretScan).toBe("passed");
    const nested = redactAndScan({ text: JSON.stringify({ password: { nested: "short-secret" }, values: [{ clientSecret: "hidden" }, { refreshToken: "hidden2" }] }), maxChars: 2000, homeDir: "/home/x" });
    expect(nested.secretScan).toBe("passed"); expect(nested.text).not.toContain("short-secret"); expect(nested.text).not.toContain("hidden2");
  });

  it("hard-bounds output and drops an empty unsafe fragment", () => {
    const bounded = redactStructure({ text: "safe ".repeat(100), maxChars: 12, homeDir: "/home/x" });
    expect([...bounded.text].length).toBeLessThanOrEqual(12);
    expect(redactStructure({ text: "", maxChars: 10, homeDir: "/home/x" }).redactionStatus).toBe("dropped");
  });

  it("rejects random mixed-case alphabetic high-entropy tokens", () => {
    expect(scanFinalText("OrasWHms" + "RzAUZKdm" + "YhuKeDJv" + "LTsYqhHd" + "NYbfgvmj")).toBe("rejected");
    expect(scanFinalText("TheQuickBrownFoxJumpsOverTheLazy")).toBe("passed");
  });

  it("never lets an injected scanner promote built-in rejection", () => {
    const high = "opaque 0123456789abcdef0123456789abcdef0123456789abcdef";
    const result = redactAndScan({ text: high, maxChars: 512, homeDir: "/home/x", scan: () => "passed" });
    expect(result.secretScan).toBe("rejected"); expect(result.dropped).toBe(true); expect(result.text).toBe("");
  });

  it("redacts sensitive compound names in JSON and assignment forms", () => {
    const fields = { secretKey: "short-secret", secret_key: "short-secret", tokenValue: "short-secret", apiKeyValue: "short-secret", passwordValue: "short-secret", authorizationHeader: "short-secret", clientSecret: "short-secret", refreshToken: "short-secret" };
    const json = redactAndScan({ text: JSON.stringify(fields), maxChars: 8000, homeDir: "/home/x" });
    expect(json.redactionStatus).toBe("redacted"); expect(json.secretScan).toBe("passed"); expect(json.text).not.toContain("short-secret");
    for (const name of Object.keys(fields)) { const result = redactAndScan({ text: `${name}=short-secret`, maxChars: 512, homeDir: "/home/x" }); expect(result.secretScan).toBe("passed"); expect(result.text).not.toContain("short-secret"); }
    const stable = redactAndScan({ text: json.text, maxChars: 8000, homeDir: "/home/x" }); expect(stable.text).toBe(json.text); expect(stable.secretScan).toBe("passed");
    const benign = redactAndScan({ text: "monkey=banana", maxChars: 512, homeDir: "/home/x" }); expect(benign.text).toBe("monkey=banana"); expect(benign.redactionStatus).toBe("unchanged"); expect(benign.secretScan).toBe("passed");
  });

  it("allows SHA-256 only with an explicit safe label", () => {
    const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    expect(scanFinalText(digest)).toBe("rejected");
    expect(scanFinalText(`opaque ${digest}`)).toBe("rejected");
    expect(scanFinalText(`sha256: ${digest}`)).toBe("passed");
    expect(scanFinalText(`contentHash=${digest}`)).toBe("passed");
    expect(scanFinalText(`token=${digest}`)).toBe("rejected");
  });

  it("fails closed on over-deep valid JSON instead of returning an unchanged secret", () => {
    let value = '{"password":"short-secret"}';
    for (let index = 0; index < 400; index += 1) value = `{"nested":${value}}`;
    const result = redactAndScan({ text: value, maxChars: 200000, homeDir: "/home/x" });
    expect(result.dropped).toBe(true);
    expect(result.secretScan).toBe("error");
    expect(result.text).toBe("");
    expect(JSON.stringify(result)).not.toContain("short-secret");
  });

  it("rejects known and high entropy secrets only after the final structural text", () => {
    expect(scanFinalText("Authorization: [authorization redacted]")).toBe("passed");
    expect(scanFinalText("Bearer abcdefghijklmnop1234567890")).toBe("rejected");
    expect(scanFinalText("opaque 0123456789abcdef0123456789abcdef0123456789abcdef")).toBe("rejected");
    expect(scanFinalText({ bad: true })).toBe("error");
  });

  it.each([
    "sk-abcdefghijklmnopqrstuvwxyz", "sk-proj-abcdefghijklmnopqrstuvwxyz123456", "ghp_abcdefghijklmnopqrstuvwxyz123456",
    "github_pat_abcdefghijklmnopqrstuvwxyz123456", "AK" + "IA" + "ABCDEFGHIJKLMNOP", "ASIAABCDEFGHIJKLMNOP",
    "Authorization: Bearer abcdefghijklmnop", "Bearer abcdefghijklmnop",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnop",
    "-----BEGIN PRIVATE KEY-----", "-----BEGIN OPENSSH PRIVATE KEY-----", "-----BEGIN ENCRYPTED PRIVATE KEY-----",
    "https://user:password123@example.com/resource", "password=hunter2long", "API_KEY: abcdefghijklmnop",
    "private_key = long-private-material", "Password:", "access_token=abcdefghijklmnop", "my_password: hunter2long",
    "x-api-key = abcdefghijklmnop", "user_token=abcdefghijklmnop", "github_token=abcdefghijklmnop",
    "api_token=abcdefghijklmnop", "label=value access_token=abcdefghijklmnop", '"access_token": "abcdefghijklmnop"',
    "redis://:password123@host", "Bearer abc123.def456-ghi789",
  ])("rejects known secret shape: %s", (value) => expect(scanFinalText(value)).toBe("rejected"));

  it.each([
    "token budget is 2000", "password rotation policy", "api key detection guidance", "token bucket algorithm",
    "password=<redacted>", "PASSWORD = [PLACEHOLDER]", "api-key: ***", "secret = <empty>",
    "lowercase aws lookalike akiaabcdefghijklmnop", "uppercase prefix lookalike SK-ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "bearer behavior is documented", "the Bearer specification is defined in RFC 6750",
    "Bearer authentication is described in the docs", "Use bearer tokens for API access",
    "The bearer of this message is responsible", "access_token=<ReDaCtEd>", "x-api-key=[EMPTY]",
    "https://example.com/user:guide",
  ])("allows documented false-positive: %s", (value) => expect(scanFinalText(value)).toBe("passed"));

  it.each([
    "password=<redacted> trailing", "token=[placeholder] # comment", 'secret="<redacted>"',
    "authorization=Bearer abcdefghijklmnop", "credentials=",
  ])("requires a complete placeholder RHS: %s", (value) => expect(scanFinalText(value)).toBe("rejected"));

  it("scans the full bounded input without prefix truncation and reports malformed scanner input", () => {
    expect(scanFinalText(`${"ordinary text ".repeat(10_000)}password=hunter2long`)).toBe("rejected");
    expect(scanFinalText({ value: "not text" })).toBe("error");
  });

  it("remains bounded on multi-megabyte benign prose while scanning the complete value", () => {
    const value = "Bearer authentication is described in the docs; token budget is 2000.\n".repeat(50_000);
    expect(value.length).toBeGreaterThan(3_000_000);
    expect(scanFinalText(value)).toBe("passed");
  }, 10_000);

  it("requires passed safe material for egress", () => {
    const destination = { id: "q", residency: "local", dataUse: "memory" };
    const base = { mode: "allowlist" as const, destination, allowlist: [destination] };
    const safe = redactAndScan({ text: "safe", maxChars: 100, homeDir: "/home/x" });
    expect(canEgress({ ...base, material: safe, payload: { maxChars: 100, homeDir: "/home/x" } })).toBe(true);
    expect(canEgress({ ...base, material: { ...safe, text: "raw", redactionStatus: "dropped", dropped: true }, payload: { maxChars: 100, homeDir: "/home/x" } })).toBe(false);
    expect(canEgress({ ...base, material: { ...safe, text: "secret", redactionStatus: "redacted", secretScan: "rejected", dropped: true }, payload: { maxChars: 100, homeDir: "/home/x" } })).toBe(false);
  });
});
