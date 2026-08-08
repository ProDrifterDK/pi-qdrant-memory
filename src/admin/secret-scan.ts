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

// These patterns intentionally favor precision over trying to recognize every
// credential format. Each uses fixed prefixes and bounded/simple character
// classes; none contains nested or ambiguous quantified groups.
const SECRET_PATTERNS: readonly RegExp[] = [
  /(?:^|[^A-Za-z0-9_-])sk-[A-Za-z0-9_-]{20,}(?=$|[^A-Za-z0-9_-])/,
  /(?:^|[^A-Za-z0-9_])(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}(?=$|[^A-Za-z0-9])/,
  /(?:^|[^A-Za-z0-9_])github_pat_[A-Za-z0-9_]{20,}(?=$|[^A-Za-z0-9_])/,
  /(?:^|[^A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?=$|[^A-Z0-9])/,
  /(?:^|[^A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{7,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?=$|[^A-Za-z0-9_-])/,
  /-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----/,
  /(?:^|[^A-Za-z0-9])Bearer[ \t]+[A-Za-z0-9._~+/-]{12,}(?=$|[^A-Za-z0-9._~+/-])/i,
  /(?:^|[^A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]{1,31}:\/\/[^\s/:@]+:[^\s/@]+@[^\s/@]+/,
];

const ASSIGNMENT = /(?:^|[^A-Za-z0-9_-])(api_key|api-key|password|passwd|secret|token|authorization|bearer|credentials|private_key)\b[ \t]*[:=][ \t]*([^\r\n]*)/gim;

function placeholder(value: string): boolean {
  return PLACEHOLDERS.has(value.trim().toLowerCase());
}

/** High-confidence, linear-time local screening. The input is never truncated. */
export function containsSecret(text: string): boolean {
  for (const pattern of SECRET_PATTERNS) {
    pattern.lastIndex = 0;
    if (pattern.test(text)) return true;
  }

  ASSIGNMENT.lastIndex = 0;
  for (let match = ASSIGNMENT.exec(text); match !== null; match = ASSIGNMENT.exec(text)) {
    const rightHandSide = match[2];
    if (rightHandSide === undefined || !placeholder(rightHandSide)) return true;
  }
  return false;
}
