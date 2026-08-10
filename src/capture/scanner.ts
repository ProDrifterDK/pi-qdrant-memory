const SECRET_PATTERNS: readonly RegExp[] = [
  /(?:^|[^A-Za-z0-9_-])sk-(?:proj-)?[A-Za-z0-9_-]{16,}(?=$|[^A-Za-z0-9_-])/u,
  /(?:^|[^A-Za-z0-9_])(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{16,}(?=$|[^A-Za-z0-9])/u,
  /(?:^|[^A-Za-z0-9_])github_pat_[A-Za-z0-9_]{16,}(?=$|[^A-Za-z0-9_])/u,
  /(?:^|[^A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?=$|[^A-Z0-9])/u,
  /(?:^|[^A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{7,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?=$|[^A-Za-z0-9_-])/u,
  /-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----/u,
  /(?:^|[^A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]{1,31}:\/\/[^\s/:@]*:[^\s/@]+@[^\s/@]+/u,
];
const ASSIGNMENT = /\b([A-Za-z][A-Za-z0-9_.-]{1,80})\s*(?::|=)\s*(?:Bearer\s+)?([^\r\n]*?)(?=\s+[A-Za-z][A-Za-z0-9_.-]{1,80}\s*(?::|=)|$)/giu;
const QUOTED_ASSIGNMENT = /["']([A-Za-z][A-Za-z0-9_.-]{1,80})["']\s*(?::|=)\s*["']([^"']*)["']/giu;
const STANDALONE_BEARER = /(?:^|[^A-Za-z0-9_-])Bearer[ \t]+[A-Za-z0-9._~+/-]{12,}[ \t]*(?=\r?$)/gm;
const PLACEHOLDER = /^(?:\*{3,}|<\s*(?:redacted|placeholder|empty|removed)\s*>|\[\s*(?:[A-Za-z_]+\s+)?(?:redacted|placeholder|empty|removed)\s*\])$/iu;
const HIGH_ENTROPY_CANDIDATE = /[A-Za-z0-9+/_-]{24,}/gu;

function entropy(value: string): number {
  const counts = new Map<string, number>();
  for (const character of value) counts.set(character, (counts.get(character) ?? 0) + 1);
  let result = 0;
  for (const count of counts.values()) { const p = count / value.length; result -= p * Math.log2(p); }
  return result;
}
const SENSITIVE_WORDS = new Set([
  "authorization", "cookie", "setcookie", "apikey", "accesstoken", "authtoken", "bearer",
  "password", "passwd", "secret", "credential", "credentials", "privatekey", "token", "key",
]);
function sensitiveName(name: string): boolean {
  const normalized = name.replace(/([a-z0-9])([A-Z])/gu, "$1_$2").toLowerCase().replace(/[^a-z0-9]/gu, "");
  const segments = name.replace(/([a-z0-9])([A-Z])/gu, "$1 $2").split(/[^A-Za-z0-9]+/gu).filter(Boolean).map((part) => part.toLowerCase());
  return SENSITIVE_WORDS.has(normalized) || [...SENSITIVE_WORDS].some((word) => word !== "key" && normalized.endsWith(word)) || segments.some((part) => SENSITIVE_WORDS.has(part));
}
function isSafeOpaqueFormat(value: string): boolean {
  // These are host/project identifiers, not credentials: UUID/UUIDv7, ULID,
  // and the fixed-width SHA-1/SHA-256 forms used by Git and content IDs.
  return /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{40})$/iu.test(value)
    || /^[0-9A-HJKMNP-TV-Z]{26}$/iu.test(value);
}
function isHighEntropy(value: string): boolean {
  if (value.startsWith("[") && value.endsWith("]")) return false;
  if (/^(?:redacted|placeholder|removed|empty)$/iu.test(value)) return false;
  const entropic = entropy(value);
  const mixedCaseAlphabet = /^[A-Za-z]+$/u.test(value) && /[a-z]/u.test(value) && /[A-Z]/u.test(value) && entropic >= 4.6;
  return value.length >= 24 && entropic >= 3.0 && ((/[A-Za-z]/u.test(value) && /\d/u.test(value)) || mixedCaseAlphabet);
}

function labeledSha256(value: string, start: number): boolean {
  const prefix = value.slice(Math.max(0, start - 64), start);
  return /(?:sha[-_]?256|checksum|content[-_]?hash)\s*["']?\s*(?::|=|is)\s*["']?\s*$/iu.test(prefix);
}
/** Scan exactly the final structurally redacted representation. */
export function scanFinalText(value: unknown): "passed" | "rejected" | "error" {
  try {
    if (typeof value !== "string" || value.length > 8_000_000) return "error";
    const markerSafe = value.replace(/\[[A-Za-z][A-Za-z0-9_ -]{0,64} redacted\]/gu, "<redacted>");
    for (const pattern of SECRET_PATTERNS) if (pattern.test(markerSafe)) return "rejected";
    STANDALONE_BEARER.lastIndex = 0;
    if (STANDALONE_BEARER.test(markerSafe)) return "rejected";
    if (/\b[A-Za-z][A-Za-z0-9_.-]{1,80}\s*(?::|=)\s*(?=$|[\r\n])/gu.test(markerSafe)) {
      const emptyAssignment = /\b([A-Za-z][A-Za-z0-9_.-]{1,80})\s*(?::|=)\s*(?=$|[\r\n])/u.exec(markerSafe);
      if (emptyAssignment !== null && sensitiveName(emptyAssignment[1] ?? "")) return "rejected";
    }
    ASSIGNMENT.lastIndex = 0;
    for (let match = ASSIGNMENT.exec(markerSafe); match !== null; match = ASSIGNMENT.exec(markerSafe)) {
      const name = match[1] ?? "";
      const assigned = match[2] ?? "";
      if (sensitiveName(name)) { let normalized = assigned; try { normalized = decodeURIComponent(assigned); } catch { return "rejected"; } if (!PLACEHOLDER.test(normalized.trim())) return "rejected"; }
    }
    QUOTED_ASSIGNMENT.lastIndex = 0;
    for (let match = QUOTED_ASSIGNMENT.exec(markerSafe); match !== null; match = QUOTED_ASSIGNMENT.exec(markerSafe)) {
      if (sensitiveName(match[1] ?? "") && !PLACEHOLDER.test((match[2] ?? "").trim())) return "rejected";
    }
    HIGH_ENTROPY_CANDIDATE.lastIndex = 0;
    for (let match = HIGH_ENTROPY_CANDIDATE.exec(markerSafe); match !== null; match = HIGH_ENTROPY_CANDIDATE.exec(markerSafe)) {
      if (!isSafeOpaqueFormat(match[0]) && isHighEntropy(match[0]) && !(match[0].length === 64 && labeledSha256(markerSafe, match.index))) return "rejected";
    }
    return "passed";
  } catch {
    return "error";
  }
}
