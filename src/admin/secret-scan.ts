const PLACEHOLDERS = new Set([
  "***",
  "****",
  "<redacted>",
  "[redacted]",
  "<placeholder>",
  "[placeholder]",
  "<empty>",
  "[empty]",
]);

const CREDENTIAL_SUFFIXES = [
  "api_key",
  "api-key",
  "password",
  "passwd",
  "secret",
  "token",
  "authorization",
  "bearer",
  "credentials",
  "private_key",
] as const;

// These patterns intentionally favor precision over trying to recognize every
// credential format. Each uses fixed prefixes and simple character classes;
// none contains nested or ambiguous quantified groups.
const SECRET_PATTERNS: readonly RegExp[] = [
  /(?:^|[^A-Za-z0-9_-])sk-[A-Za-z0-9_-]{20,}(?=$|[^A-Za-z0-9_-])/,
  /(?:^|[^A-Za-z0-9_])(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}(?=$|[^A-Za-z0-9])/,
  /(?:^|[^A-Za-z0-9_])github_pat_[A-Za-z0-9_]{20,}(?=$|[^A-Za-z0-9_])/,
  /(?:^|[^A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?=$|[^A-Z0-9])/,
  /(?:^|[^A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{7,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?=$|[^A-Za-z0-9_-])/,
  /-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----/,
  /(?:^|[^A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]{1,31}:\/\/[^\s/:@]*:[^\s/@]+@[^\s/@]+/,
];

// A standalone opaque bearer form is high-confidence only when its token is
// the final value on the line. Authorization/bearer assignments are handled
// separately, so prose such as "Bearer authentication is ..." is not blocked.
const STANDALONE_BEARER = /(?:^|[^A-Za-z0-9_-])Bearer[ \t]+[A-Za-z0-9._~+/-]{12,}[ \t]*(?=\r?$)/im;
const UNQUOTED_ASSIGNMENT = /(?:^|[^A-Za-z0-9_'"-])([A-Za-z][A-Za-z0-9_-]*)[ \t]*[:=][ \t]*/gim;
const QUOTED_ASSIGNMENT = /(?:^|[^A-Za-z0-9_-])(["'])([A-Za-z][A-Za-z0-9_-]*)\1[ \t]*[:=][ \t]*/gim;

function placeholder(value: string): boolean {
  return PLACEHOLDERS.has(value.trim().toLowerCase());
}

function credentialIdentifier(value: string): boolean {
  const key = value.toLowerCase();
  return CREDENTIAL_SUFFIXES.some(
    (suffix) => key === suffix || key.endsWith(`_${suffix}`) || key.endsWith(`-${suffix}`),
  );
}

function endOfLine(text: string, start: number): number {
  const carriageReturn = text.indexOf("\r", start);
  const lineFeed = text.indexOf("\n", start);
  if (carriageReturn < 0) return lineFeed < 0 ? text.length : lineFeed;
  if (lineFeed < 0) return carriageReturn;
  return Math.min(carriageReturn, lineFeed);
}

function hasSecretAssignment(text: string, pattern: RegExp, keyIndex: number): boolean {
  pattern.lastIndex = 0;
  for (let match = pattern.exec(text); match !== null; match = pattern.exec(text)) {
    const key = match[keyIndex];
    if (key === undefined || !credentialIdentifier(key)) continue;
    const rightHandSide = text.slice(pattern.lastIndex, endOfLine(text, pattern.lastIndex));
    if (!placeholder(rightHandSide)) return true;
  }
  return false;
}

/** High-confidence, linear-time local screening. The input is never truncated. */
export function containsSecret(text: string): boolean {
  for (const pattern of SECRET_PATTERNS) {
    pattern.lastIndex = 0;
    if (pattern.test(text)) return true;
  }
  STANDALONE_BEARER.lastIndex = 0;
  if (STANDALONE_BEARER.test(text)) return true;
  return (
    hasSecretAssignment(text, UNQUOTED_ASSIGNMENT, 1) ||
    hasSecretAssignment(text, QUOTED_ASSIGNMENT, 2)
  );
}
