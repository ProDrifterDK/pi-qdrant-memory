import { createHash } from "node:crypto";
import { isAbsolute, resolve } from "node:path";
import { scanFinalText } from "../capture/scanner.js";
const MAX_HARD_CHARS = 1_000_000;
const PLACEHOLDER = /^(?:(?:\[|<)(?:[A-Za-z][A-Za-z0-9_ -]{0,64}\s+)?(?:redacted|placeholder|empty|removed)(?:\]|>))$/iu;
const SENSITIVE_WORDS = new Set([
    "authorization", "cookie", "setcookie", "apikey", "accesstoken", "authtoken", "bearer",
    "password", "passwd", "secret", "credential", "credentials", "privatekey", "token", "key",
]);
function normalizedName(name) {
    return name.replace(/([a-z0-9])([A-Z])/gu, "$1_$2").toLowerCase().replace(/[^a-z0-9]/gu, "");
}
function nameSegments(name) {
    return name.replace(/([a-z0-9])([A-Z])/gu, "$1 $2").split(/[^A-Za-z0-9]+/gu).filter(Boolean).map((part) => part.toLowerCase());
}
function sensitiveName(name) {
    const normalized = normalizedName(name);
    return SENSITIVE_WORDS.has(normalized) || [...SENSITIVE_WORDS].some((word) => word !== "key" && normalized.endsWith(word)) || nameSegments(name).some((part) => SENSITIVE_WORDS.has(part));
}
const KNOWN_TOKEN_PATTERNS = [
    [/(?:^|[^A-Za-z0-9_-])sk-(?:proj-)?[A-Za-z0-9_-]{16,}(?=$|[^A-Za-z0-9_-])/gu, "api_key"],
    [/(?:^|[^A-Za-z0-9_])(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{16,}(?=$|[^A-Za-z0-9])/gu, "token"],
    [/(?:^|[^A-Za-z0-9_])github_pat_[A-Za-z0-9_]{16,}(?=$|[^A-Za-z0-9_])/gu, "token"],
    [/(?:^|[^A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?=$|[^A-Z0-9])/gu, "api_key"],
    [/(?:^|[^A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{7,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?=$|[^A-Za-z0-9_-])/gu, "token"],
    [/-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----[\s\S]*?-----END [^-]+ PRIVATE KEY-----/gu, "secret"],
];
const URL_PATTERN = /\b(?:https?|wss?):\/\/[^\s<>"']+/giu;
function marker(kind) { return `[${kind} redacted]`; }
function kindFor(name) {
    const lower = name.toLowerCase();
    if (lower.includes("cookie"))
        return "cookie";
    if (lower.includes("authoriz") || lower === "bearer")
        return "authorization";
    if (lower.includes("api") || lower.includes("key"))
        return "api_key";
    if (lower.includes("password") || lower.includes("passwd"))
        return "password";
    if (lower.includes("secret") || lower.includes("credential") || lower.includes("private"))
        return "secret";
    return "token";
}
function isPlaceholder(value) { return PLACEHOLDER.test(value.trim()); }
function boundedText(value, maxChars) {
    const chars = [...value];
    if (chars.length <= maxChars)
        return { value, truncated: false };
    return { value: chars.slice(0, maxChars).join(""), truncated: true };
}
function normalizeControls(value) {
    return value.replace(/\r\n?/gu, "\n").replace(/\p{Cc}/gu, (character) => character === "\n" ? "\n" : " ").replace(/[\uFEFF\u200B\u200C\u200D]/gu, "");
}
function canonicalizeHome(value, homeDir) {
    if (!isAbsolute(homeDir) || homeDir === "/")
        return value;
    const home = resolve(homeDir).replace(/[\\/]+$/u, "");
    let output = "";
    let cursor = 0;
    while (cursor < value.length) {
        const found = value.indexOf(home, cursor);
        if (found < 0) {
            output += value.slice(cursor);
            break;
        }
        const before = found === 0 ? "" : value[found - 1];
        const after = value[found + home.length] ?? "";
        const beforeOk = before === "" || /[\\/\s(=:"']/u.test(before);
        const afterOk = after === "" || /[\\/\s)"',;:]/u.test(after);
        if (beforeOk && afterOk) {
            output += value.slice(cursor, found) + "$HOME";
            cursor = found + home.length;
        }
        else {
            output += value.slice(cursor, found + home.length);
            cursor = found + home.length;
        }
    }
    return output;
}
function redactUrls(value) {
    let changed = false;
    const result = value.replace(URL_PATTERN, (raw) => {
        const suffix = /^[.,;:!?)]$/u.exec(raw.slice(-1)) !== null ? raw.slice(-1) : "";
        const candidate = suffix === "" ? raw : raw.slice(0, -1);
        let parsed;
        try {
            parsed = new URL(candidate);
        }
        catch {
            return raw;
        }
        if (parsed.username !== "" || parsed.password !== "")
            changed = true;
        parsed.username = "";
        parsed.password = "";
        const pairs = [];
        let queryChanged = false;
        for (const [key, entry] of parsed.searchParams.entries()) {
            if (sensitiveName(decodeURIComponent(key.replace(/\+/gu, " ")))) {
                pairs.push(`${encodeURIComponent(key)}=${marker(kindFor(key))}`);
                queryChanged = true;
            }
            else
                pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(entry)}`);
        }
        if (queryChanged)
            changed = true;
        const hasCredentials = parsed.username !== "" || parsed.password !== "" || candidate.includes("@");
        if (!hasCredentials && !queryChanged)
            return raw;
        parsed.search = "";
        parsed.hash = "";
        const base = `${parsed.origin}${hasCredentials ? `/${marker("url_credentials")}` : ""}${parsed.pathname === "/" ? "" : parsed.pathname}`;
        const query = pairs.length === 0 ? "" : `?${pairs.join("&")}`;
        return base + query + suffix;
    });
    return { value: result, changed };
}
const MAX_JSON_NODES = 10_000;
const MAX_JSON_DEPTH = 256;
function redactJsonValue(value) {
    const stack = [{ value: value, depth: 0 }];
    let changed = false;
    let nodes = 0;
    while (stack.length > 0) {
        const current = stack.pop();
        nodes += 1;
        if (nodes > MAX_JSON_NODES || current.depth > MAX_JSON_DEPTH)
            return { changed: false, failed: true };
        for (const [name, child] of Object.entries(current.value)) {
            if (sensitiveName(name) && !(typeof child === "string" && isPlaceholder(child))) {
                current.value[name] = marker(kindFor(name));
                changed = true;
                continue;
            }
            if (child !== null && typeof child === "object")
                stack.push({ value: child, depth: current.depth + 1 });
        }
    }
    return { changed, failed: false };
}
function redactJsonText(value) {
    const trimmed = value.trim();
    if (!trimmed.startsWith("{") && !trimmed.startsWith("["))
        return { value, changed: false, failed: false };
    let parsed;
    try {
        parsed = JSON.parse(trimmed);
    }
    catch (error) {
        return { value, changed: false, failed: error instanceof RangeError };
    }
    if (parsed === null || typeof parsed !== "object")
        return { value, changed: false, failed: false };
    const result = redactJsonValue(parsed);
    if (result.failed)
        return { value: "", changed: false, failed: true };
    if (!result.changed)
        return { value, changed: false, failed: false };
    try {
        const newline = value.includes("\n");
        const indent = newline ? (value.match(/\n([ \t]+)[^ \t]/u)?.[1] ?? "  ") : undefined;
        const serialized = JSON.stringify(parsed, null, indent);
        const prefix = value.slice(0, value.indexOf(trimmed));
        const suffix = value.slice(value.indexOf(trimmed) + trimmed.length);
        return { value: prefix + serialized + suffix, changed: true, failed: false };
    }
    catch {
        return { value: "", changed: false, failed: true };
    }
}
function redactStructuredFields(value) {
    let changed = false;
    let result = value;
    const json = redactJsonText(result);
    if (json.failed)
        return { value: "", changed: false, failed: true };
    changed ||= json.changed;
    result = json.value;
    // Quoted JSON/YAML-like fields. The value is intentionally field-local and bounded.
    result = result.replace(/(["'])([^"']+)\1\s*:\s*(["'])([^"']*)\3/gu, (whole, quote, name, valueQuote, fieldValue) => {
        if (!sensitiveName(name) || isPlaceholder(fieldValue))
            return whole;
        changed = true;
        return `${quote}${name}${quote}: ${valueQuote}${marker(kindFor(name))}${valueQuote}`;
    });
    // Header and assignment forms, including unquoted Authorization: Bearer ... values.
    result = result.replace(/\b([A-Za-z][A-Za-z0-9_.-]{1,80})\s*(?::|=)\s*(?:Bearer\s+)?(?:"([^"]*)"|'([^']*)'|(\[[^\]\n]{1,200}\]|<[^>\n]{1,200}>|[^\s,;}\]\n]+))/giu, (whole, name, doubleValue, singleValue, bareValue) => {
        if (!sensitiveName(name))
            return whole;
        const fieldValue = doubleValue ?? singleValue ?? bareValue ?? "";
        if (isPlaceholder(fieldValue))
            return whole;
        changed = true;
        return `${name}: ${marker(kindFor(name))}`;
    });
    return { value: result, changed, failed: false };
}
function redactKnownTokens(value) {
    let changed = false;
    let result = value;
    for (const [pattern, kind] of KNOWN_TOKEN_PATTERNS) {
        pattern.lastIndex = 0;
        result = result.replace(pattern, (match) => { changed = true; return marker(kind); });
    }
    result = result.replace(/\bBearer\s+[A-Za-z0-9._~+\/-]{12,}/giu, (match) => { changed = true; return marker("token"); });
    return { value: result, changed };
}
/**
 * Canonicalize and structurally redact one text field. This function never logs
 * its input and never returns an unbounded string.
 */
function structuralRedact(input) {
    let text;
    // Existing typed markers are already canonical and must be idempotent: a
    // final rescan of an envelope containing persisted `[token redacted]` bytes
    // must report unchanged when it performs no rewrite.
    let changed = false;
    const emptyHash = () => createHash("sha256").update("", "utf8").digest("hex");
    try {
        if (typeof input.text !== "string" || !Number.isSafeInteger(input.maxChars) || input.maxChars < 0 || input.maxChars > MAX_HARD_CHARS || typeof input.homeDir !== "string")
            throw new TypeError("invalid redaction input");
        text = input.text.normalize("NFC");
        if (text !== input.text)
            changed = true;
        const normalized = normalizeControls(text);
        changed ||= normalized !== text;
        text = normalized;
        const withHome = canonicalizeHome(text, input.homeDir);
        changed ||= withHome !== text;
        text = withHome;
        const fields = redactStructuredFields(text);
        if (fields.failed)
            return { text: "", redactionStatus: "dropped", contentHash: emptyHash() };
        changed ||= fields.changed;
        text = fields.value;
        const urls = redactUrls(text);
        changed ||= urls.changed;
        text = urls.value;
        const known = redactKnownTokens(text);
        changed ||= known.changed;
        text = known.value;
        const bounded = boundedText(text, Math.min(input.maxChars, MAX_HARD_CHARS));
        changed ||= bounded.truncated;
        text = bounded.value;
        if (text.length === 0)
            return { text: "", redactionStatus: "dropped", contentHash: emptyHash() };
        return { text, redactionStatus: changed ? "redacted" : "unchanged", contentHash: createHash("sha256").update(text, "utf8").digest("hex") };
    }
    catch {
        return { text: "", redactionStatus: "dropped", contentHash: emptyHash() };
    }
}
export function redactStructure(input) {
    return structuralRedact(input);
}
export function redactAndScan(input) {
    const structural = structuralRedact(input);
    const emptyHash = createHash("sha256").update("", "utf8").digest("hex");
    if (structural.redactionStatus === "dropped")
        return { ...structural, text: "", secretScan: "error", dropped: true, contentHash: emptyHash };
    let verdict;
    try {
        // The built-in scanner is a mandatory floor. An injected seam may only
        // further restrict material; it can never promote rejected/error input.
        verdict = scanFinalText(structural.text);
        if (verdict === "passed" && input.scan !== undefined)
            verdict = input.scan(structural.text);
        if (verdict !== "passed" && verdict !== "rejected" && verdict !== "error")
            verdict = "error";
    }
    catch {
        verdict = "error";
    }
    if (verdict !== "passed")
        return { ...structural, text: "", secretScan: verdict, dropped: true, contentHash: emptyHash };
    return { ...structural, secretScan: "passed", dropped: false, contentHash: createHash("sha256").update(structural.text, "utf8").digest("hex") };
}
export function redactField(input) {
    const kind = kindFor(input.name);
    if (sensitiveName(input.name) && !isPlaceholder(input.value)) {
        return redactAndScan({ text: marker(kind), maxChars: input.maxChars, homeDir: input.homeDir });
    }
    return redactAndScan({ text: input.value, maxChars: input.maxChars, homeDir: input.homeDir });
}
//# sourceMappingURL=redaction.js.map