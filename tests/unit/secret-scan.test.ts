import { describe, expect, it } from "vitest";
import { containsSecret } from "../../src/admin/secret-scan.js";

describe("containsSecret", () => {
  it.each([
    "sk-abcdefghijklmnopqrstuvwxyz",
    "sk-proj-abcdefghijklmnopqrstuvwxyz123456",
    "ghp_abcdefghijklmnopqrstuvwxyz123456",
    "github_pat_abcdefghijklmnopqrstuvwxyz123456",
    "AKIAABCDEFGHIJKLMNOP",
    "ASIAABCDEFGHIJKLMNOP",
    "Authorization: Bearer abcdefghijklmnop",
    "Bearer abcdefghijklmnop",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnop",
    "-----BEGIN PRIVATE KEY-----",
    "-----BEGIN OPENSSH PRIVATE KEY-----",
    "-----BEGIN ENCRYPTED PRIVATE KEY-----",
    "https://user:password123@example.com/resource",
    "password=hunter2long",
    "API_KEY: abcdefghijklmnop",
    "private_key = long-private-material",
    "Password:",
    "access_token=abcdefghijklmnop",
    "my_password: hunter2long",
    "x-api-key = abcdefghijklmnop",
    "user_token=abcdefghijklmnop",
    "github_token=abcdefghijklmnop",
    "api_token=abcdefghijklmnop",
    "label=value access_token=abcdefghijklmnop",
    '"access_token": "abcdefghijklmnop"',
    "redis://:password123@host",
    "Bearer abc123.def456-ghi789",
  ])("blocks secret-shaped text: %s", (value) => expect(containsSecret(value)).toBe(true));

  it.each([
    "token budget is 2000",
    "password rotation policy",
    "api key detection guidance",
    "token bucket algorithm",
    "password=<redacted>",
    "PASSWORD = [PLACEHOLDER]",
    "api-key: ***",
    "secret = <empty>",
    "lowercase aws lookalike akiaabcdefghijklmnop",
    "uppercase prefix lookalike SK-ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "bearer behavior is documented",
    "the Bearer specification is defined in RFC 6750",
    "Bearer authentication is described in the docs",
    "Use bearer tokens for API access",
    "The bearer of this message is responsible",
    "access_token=<ReDaCtEd>",
    "x-api-key=[EMPTY]",
    "https://example.com/user:guide",
  ])("allows benign text: %s", (value) => expect(containsSecret(value)).toBe(false));

  it.each([
    "password=<redacted> trailing",
    "token=[placeholder] # comment",
    'secret="<redacted>"',
    "authorization=Bearer abcdefghijklmnop",
    "credentials=",
  ])("allows placeholders only when they are the complete trimmed RHS: %s", (value) => {
    expect(containsSecret(value)).toBe(true);
  });

  it("scans the full input without a prefix truncation", () => {
    expect(containsSecret(`${"ordinary text ".repeat(10_000)}password=hunter2long`)).toBe(true);
  });

  it("remains linear on multi-megabyte benign and bearer-prose input", () => {
    const value = "Bearer authentication is described in the docs; token budget is 2000.\n".repeat(50_000);
    expect(value.length).toBeGreaterThan(3_000_000);
    expect(containsSecret(value)).toBe(false);
  }, 10_000);
});
