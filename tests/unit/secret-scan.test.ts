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
});
