import { createHash, randomBytes } from "node:crypto";
import { chmod, mkdir, open, readFile, rename, rm, stat, lstat } from "node:fs/promises";
import { dirname, isAbsolute, join, normalize, resolve, sep } from "node:path";
import { canonicalRecordHash } from "../domain/records.js";
import { episodeId } from "../domain/ids.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../domain/canonical.js";
import { redactAndScan, redactStructure } from "../security/redaction.js";
import { selectPersistedEntries } from "./select.js";
export const CAPTURE_LIFECYCLES = ["agent_end", "session_before_compact", "session_shutdown"];
const states = new Map();
const MAX_STATE_IDS = 20_000;
const SAFE_ID = /^[A-Za-z0-9._:$\/-]{1,512}$/u;
const HOST_DEFAULTS = { pi: ".pi/agent", prime: ".prime/agent" };
function canonicalSession(sessionId, homeDir = "/") {
    if (typeof sessionId !== "string" || sessionId.length === 0 || sessionId.length > 512)
        return undefined;
    const result = redactAndScan({ text: sessionId, maxChars: 512, homeDir });
    if (result.dropped || result.secretScan !== "passed" || result.redactionStatus !== "unchanged" || result.text !== sessionId || !SAFE_ID.test(result.text))
        return undefined;
    return result.text;
}
function stateKey(host, sessionId) { const canonical = canonicalSession(sessionId); if (canonical === undefined)
    throw new TypeError("Invalid session ID"); return `capture:${host}:${sha256Hex(canonical)}`; }
export function captureStateKey(host, sessionId) { return stateKey(host, sessionId); }
export function captureStateFilename(sessionId) { const canonical = canonicalSession(sessionId); if (canonical === undefined)
    throw new TypeError("Invalid session ID"); return `state-${sha256Hex(canonical)}.json`; }
export function captureStatePath(agentDir, sessionId) { return join(agentDir, "pi-qdrant-memory", "capture", captureStateFilename(sessionId)); }
function isHost(value) { return value === "pi" || value === "prime"; }
function boundedId(value, fallback, homeDir = "/") {
    if (value === undefined || value === null || value === "")
        return fallback;
    // Every identifier crosses the final scanner. Never hash or return the raw
    // candidate: a rejected secret gets only the bounded fallback.
    const safe = typeof value === "string" && value.length <= 512
        ? redactAndScan({ text: value, maxChars: 512, homeDir })
        : undefined;
    if (safe === undefined || safe.dropped || safe.secretScan !== "passed" || safe.redactionStatus !== "unchanged" || safe.text !== value || safe.text.length === 0)
        return fallback;
    if (/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token|key)/iu.test(value))
        return fallback;
    if (SAFE_ID.test(safe.text))
        return safe.text;
    return sha256Hex(`stable-id:${safe.text}`).slice(0, 32);
}
function timestampMillis(value) {
    if (value instanceof Date)
        return Number.isFinite(value.getTime()) ? value.getTime() : undefined;
    if (typeof value === "number")
        return Number.isFinite(value) ? value : undefined;
    if (typeof value !== "string" || value.trim() === "")
        return undefined;
    const numeric = Number(value);
    if (Number.isFinite(numeric))
        return numeric;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : undefined;
}
function stableNow(value) {
    const result = timestampMillis(value);
    return result !== undefined && Number.isSafeInteger(result) ? result : 0;
}
function isoTimestamp(value, fallbackMs) {
    const parsed = timestampMillis(value);
    const candidate = parsed !== undefined ? parsed : fallbackMs;
    const date = new Date(Number.isFinite(candidate) ? candidate : fallbackMs);
    return Number.isFinite(date.getTime()) ? date.toISOString() : new Date(fallbackMs).toISOString();
}
function validSession(value) { return typeof value === "string" && value.length > 0 && value.length <= 512; }
function canonicalEntryId(value, homeDir = "/") {
    if (typeof value !== "string" || value.length === 0 || value.length > 512)
        return undefined;
    const result = redactAndScan({ text: value, maxChars: 512, homeDir });
    return result.dropped || result.secretScan !== "passed" || result.redactionStatus !== "unchanged" || result.text !== value || !SAFE_ID.test(value) ? undefined : value;
}
function snapshotEntries(source, homeDir = "/") {
    const raw = typeof source === "function" ? source() : source;
    if (!Array.isArray(raw))
        return { entries: [], invalid: 1, unsafeIdentity: true };
    const entries = [];
    let invalid = 0;
    let unsafeIdentity = false;
    for (const entry of raw) {
        if (typeof entry?.id !== "string" || entry.id.length === 0 || typeof entry.type !== "string") {
            invalid += 1;
            unsafeIdentity = true;
            continue;
        }
        if (canonicalEntryId(entry.id, homeDir) === undefined) {
            invalid += 1;
            unsafeIdentity = true;
            continue;
        }
        entries.push(entry);
    }
    return { entries, invalid, unsafeIdentity };
}
function cloneEntries(source, homeDir = "/") { return snapshotEntries(source, homeDir).entries; }
function safeEntryIdentity(value, homeDir = "/") {
    const canonical = canonicalEntryId(value, homeDir);
    if (canonical === undefined)
        throw new Error("Unsafe persisted entry identity");
    return canonical;
}
function tailHash(entries, homeDir = "/") {
    return sha256Hex(canonicalStringify(entries.map((entry) => safeEntryIdentity(entry.id, homeDir))));
}
function auditHash(audit) { return sha256Hex(canonicalStringify({ audit, version: 1 })); }
const AUDIT_KEYS = ["redaction", "scanner_rejected", "scanner_error", "invalid_entry"];
const STATE_KEYS = ["version", "sessionId", "host", "activatedAt", "tailEntryId", "tailIndex", "tailCount", "tailHash", "capturedIds", "quarantineIds", "audit", "auditHash"];
function exactKeys(value, keys) {
    const actual = Object.keys(value).sort();
    const expected = [...keys].sort();
    return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}
function validAudit(value) {
    if (typeof value !== "object" || value === null)
        return false;
    const audit = value;
    return exactKeys(audit, AUDIT_KEYS) && AUDIT_KEYS.every((key) => Number.isSafeInteger(audit[key]) && audit[key] >= 0 && audit[key] <= MAX_STATE_IDS);
}
function validState(value, expectedSession, expectedHost) {
    if (typeof value !== "object" || value === null)
        return false;
    const state = value;
    if (!exactKeys(state, STATE_KEYS) || state.version !== 1 || !validSession(state.sessionId) || canonicalSession(state.sessionId) !== state.sessionId || (expectedSession !== undefined && state.sessionId !== canonicalSession(expectedSession)) || !isHost(state.host) || (expectedHost !== undefined && state.host !== expectedHost))
        return false;
    if (typeof state.activatedAt !== "number" || !Number.isSafeInteger(state.activatedAt) || state.activatedAt < 0 || !Number.isSafeInteger(state.tailIndex) || !Number.isSafeInteger(state.tailCount) || (typeof state.tailEntryId !== "string" && state.tailEntryId !== null) || typeof state.tailHash !== "string" || !/^[a-f0-9]{64}$/u.test(state.tailHash) || !Array.isArray(state.capturedIds) || state.capturedIds.length > MAX_STATE_IDS || state.capturedIds.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512 || !SAFE_ID.test(id) || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(id)) || new Set(state.capturedIds).size !== state.capturedIds.length || !Array.isArray(state.quarantineIds) || state.quarantineIds.length > MAX_STATE_IDS || state.quarantineIds.some((id) => typeof id !== "string" || !/^[0-9a-f-]{36}$/u.test(id)) || new Set(state.quarantineIds).size !== state.quarantineIds.length || !validAudit(state.audit) || state.auditHash !== auditHash(state.audit))
        return false;
    const tailIndex = state.tailIndex;
    const tailCount = state.tailCount;
    const tailIsSafe = state.tailEntryId === null || (typeof state.tailEntryId === "string" && canonicalEntryId(state.tailEntryId) === state.tailEntryId);
    if (tailIndex < -1 || tailCount < 0 || tailIndex !== tailCount - 1 || !tailIsSafe || (tailCount === 0 ? state.tailEntryId !== null : typeof state.tailEntryId !== "string" || state.tailEntryId.length === 0))
        return false;
    return true;
}
async function readStateFile(path, sessionId, host) {
    if (!(await safeExistingPath(path)))
        throw new Error("Capture state path is unsafe");
    let raw;
    try {
        raw = await readFile(path, "utf8");
    }
    catch (error) {
        if (error.code === "ENOENT")
            return undefined;
        throw error;
    }
    let parsed;
    try {
        parsed = JSON.parse(raw);
    }
    catch {
        throw new Error("Capture activation state is invalid");
    }
    if (!validState(parsed, sessionId, host))
        throw new Error("Capture activation state is invalid");
    return parsed;
}
async function writeStateFile(path, state) {
    const parent = dirname(path);
    if (!(await safeExistingPath(parent)))
        throw new Error("Capture state path is unsafe");
    await mkdir(parent, { recursive: true, mode: 0o700 });
    if (!(await safeExistingPath(parent)) || !(await safeExistingPath(path)))
        throw new Error("Capture state path is unsafe");
    await chmod(parent, 0o700);
    const pluginDir = dirname(parent);
    if (pluginDir !== "/" && pluginDir !== ".")
        await chmod(pluginDir, 0o700);
    const temp = `${path}.tmp-${process.pid}-${randomBytes(16).toString("hex")}`;
    const handle = await open(temp, "wx", 0o600);
    try {
        await handle.writeFile(JSON.stringify(state), "utf8");
        await handle.sync();
    }
    finally {
        await handle.close();
    }
    if (!(await safeExistingPath(temp))) {
        await rm(temp, { force: true });
        throw new Error("Capture temporary path is unsafe");
    }
    await chmod(temp, 0o600);
    await rename(temp, path);
    try {
        const directory = await open(parent, "r");
        await directory.sync();
        await directory.close();
    }
    catch { /* unsupported directory fsync */ }
    const readback = await readStateFile(path, state.sessionId, state.host);
    if (readback === undefined || JSON.stringify(readback) !== JSON.stringify(state))
        throw new Error("Capture state readback failed");
}
async function safeExistingPath(path) {
    const absolute = resolve(path);
    if (!isAbsolute(absolute))
        return false;
    const pieces = absolute.split(sep).filter(Boolean);
    let cursor = absolute.startsWith(sep) ? sep : "";
    for (const piece of pieces) {
        cursor = cursor === sep ? `${cursor}${piece}` : `${cursor}${sep}${piece}`;
        try {
            const info = await lstat(cursor);
            if (info.isSymbolicLink())
                return false;
        }
        catch (error) {
            if (error.code === "ENOENT")
                break;
            return false;
        }
    }
    return true;
}
/** Resolve only the host-owned coding-agent directory; repository paths never participate. */
export async function resolveCaptureAgentDirectory(input) {
    const expectedName = input.host === "pi" ? "PI_CODING_AGENT_DIR" : "PRIME_AGENT_CODING_AGENT_DIR";
    const otherName = input.host === "pi" ? "PRIME_AGENT_CODING_AGENT_DIR" : "PI_CODING_AGENT_DIR";
    const expected = input.env[expectedName];
    const other = input.env[otherName];
    if ((other !== undefined && other.trim() !== "") || (expected !== undefined && expected.trim() === ""))
        return null;
    const candidate = expected === undefined ? join(input.homeDir, HOST_DEFAULTS[input.host]) : expected.trim();
    if (!isAbsolute(candidate) || !(await safeExistingPath(candidate)))
        return null;
    const normalized = normalize(resolve(candidate));
    return normalized;
}
function parseDepth(value) {
    if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= 1000)
        return value;
    if (typeof value === "string" && /^\d{1,4}$/u.test(value)) {
        const parsed = Number(value);
        if (parsed <= 1000)
            return parsed;
    }
    return undefined;
}
function obj(value) { return typeof value === "object" && value !== null && !Array.isArray(value) ? value : undefined; }
/** Resolve child markers without trusting ambiguous or malformed host metadata. */
export function resolveAgentMarker(input) {
    if (input.host !== "pi" && input.host !== "prime")
        return { role: "child", depth: 1, valid: false, rootWorkAllowed: false };
    const header = obj(input.header);
    const env = input.env ?? {};
    let valid = input.header === undefined || input.header === null || header !== undefined;
    let depth = 0;
    let child = false;
    if (input.host === "prime") {
        const headerDepth = header?.rlmDepth === undefined ? undefined : parseDepth(header.rlmDepth);
        if (header?.rlmDepth !== undefined && headerDepth === undefined)
            valid = false;
        const envDepth = env.RLM_DEPTH === undefined ? undefined : parseDepth(env.RLM_DEPTH);
        if (env.RLM_DEPTH !== undefined && envDepth === undefined)
            valid = false;
        if (headerDepth !== undefined && envDepth !== undefined && headerDepth !== envDepth)
            valid = false;
        depth = headerDepth ?? envDepth ?? 0;
        child = depth > 0;
    }
    else {
        const parent = header?.parentSession;
        if (parent !== undefined && parent !== null && typeof parent !== "string" && typeof parent !== "object")
            valid = false;
        child = parent !== undefined && parent !== null && parent !== false;
        const childMarker = env.PI_SUBAGENT_CHILD;
        const markerDepth = env.PI_SUBAGENT_DEPTH === undefined ? undefined : parseDepth(env.PI_SUBAGENT_DEPTH);
        if (env.PI_SUBAGENT_DEPTH !== undefined && markerDepth === undefined)
            valid = false;
        if (childMarker !== undefined && childMarker !== "0" && childMarker !== "1")
            valid = false;
        if (childMarker === "1")
            child = true;
        if (markerDepth !== undefined && markerDepth > 0)
            child = true;
        if (childMarker === "0" && child)
            valid = false;
        if (markerDepth === 0 && child)
            valid = false;
        depth = child ? Math.max(1, markerDepth ?? 1) : 0;
    }
    if (!valid && !child) {
        child = true;
        depth = Math.max(1, depth);
    }
    return { role: child ? "child" : "root", depth, valid, rootWorkAllowed: valid && !child };
}
export function captureRootWorkAllowed(marker) { return resolveAgentMarker(marker).rootWorkAllowed; }
function projectAllowed(input) {
    const id = input.projectId;
    const deny = input.projectDenylist ?? [];
    if (id !== undefined && deny.includes(id))
        return false;
    const allow = input.projectAllowlist ?? [];
    return allow.length === 0 || (id !== undefined && allow.includes(id));
}
function finalField(value, maxChars, homeDir) {
    if (value === undefined)
        return { status: "unchanged" };
    const bound = Number.isSafeInteger(maxChars) && maxChars >= 0 ? Math.min(maxChars, 16_000) : 0;
    const redacted = redactStructure({ text: value, maxChars: bound, homeDir });
    return redacted.text.length === 0 ? { status: "dropped" } : { text: redacted.text, status: redacted.redactionStatus };
}
function safeFingerprint(value, homeDir = "/") {
    if (value === undefined)
        return undefined;
    // Selector-generated error fingerprints are already a bounded structured
    // digest. Do not send that digest through the prose entropy scanner.
    if (/^[a-f0-9]{32}$/iu.test(value))
        return value;
    const safe = redactAndScan({ text: value, maxChars: 512, homeDir });
    if (safe.dropped || safe.secretScan !== "passed")
        return undefined;
    return /^[a-f0-9]{32}$/iu.test(safe.text) ? safe.text : sha256Hex(`fingerprint:${safe.text}`).slice(0, 32);
}
function episodeMaterial(entry, input, marker, fallbackAt) {
    const maxText = Number.isSafeInteger(input.maxTextChars) && (input.maxTextChars ?? 0) >= 0 ? Math.min(input.maxTextChars ?? 16_000, 16_000) : 16_000;
    const homeDir = input.homeDir ?? "/";
    const textField = finalField(entry.text, entry.eventKind.startsWith("tool_") ? (input.toolResultChars ?? 4_000) : maxText, homeDir);
    const argsField = finalField(entry.toolArgs, input.toolArgsChars ?? 2_000, homeDir);
    const text = textField.text;
    const toolArgs = argsField.text;
    const safeToolName = entry.toolName === undefined ? undefined : boundedId(entry.toolName, "tool", homeDir);
    const finalText = [text, toolArgs].filter((item) => item !== undefined).join("\n");
    // A finalized tool call can be meaningful with only a safe tool name. Include
    // that bounded name in the final scan, but never store it as free-form text.
    const validationText = [finalText, safeToolName].filter((item) => item !== undefined).join("\n");
    if (validationText.length === 0 && entry.eventKind !== "tool_error")
        return { category: "redaction" };
    const finalCheck = redactAndScan({ text: validationText, maxChars: Math.min(16_000, Math.max(validationText.length, 1)), homeDir, ...(input.scan === undefined ? {} : { scan: input.scan }) });
    if (finalCheck.dropped)
        return { category: finalCheck.secretScan === "error" ? "scanner_error" : finalCheck.secretScan === "rejected" ? "scanner_rejected" : "redaction" };
    const eventAt = isoTimestamp(entry.eventAt, fallbackAt);
    const stableCreatedAt = isoTimestamp(undefined, fallbackAt);
    const sessionId = boundedId(input.sessionId, "session", homeDir);
    const sourceEntryId = boundedId(entry.sourceEntryId, "entry", homeDir);
    const messageFallback = entry.messageId === undefined || entry.messageId === null || entry.messageId === "" ? "message" : sourceEntryId;
    const messageId = boundedId(entry.messageId, messageFallback, homeDir);
    const id = episodeId({ host: input.host, sessionId, messageId, part: entry.partIdentity });
    const record = {
        recordType: "episode", id, ownerHost: input.host, schemaRevision: 1, createdAt: stableCreatedAt, privacyEpoch: input.privacyEpoch ?? 0,
        processingPolicyId: boundedId(input.policyId, "capture-policy", homeDir), expiresAt: input.expiresAt ?? null, contentHash: "pending", sourceEntryId, host: input.host,
        projectId: boundedId(input.projectId, "local_only", homeDir), projectIdentityKind: input.projectIdentityKind ?? "local_only", sessionId, turnId: boundedId(entry.turnId, sourceEntryId, homeDir), agentRole: marker.role, depth: marker.depth,
        eventKind: entry.eventKind, eventAt, modelId: boundedId(input.modelId, "unknown", homeDir), embeddingDimension: 1024, originProvider: boundedId(input.originProvider, "unknown", homeDir), destinationId: boundedId(input.destinationId, "capture:local", homeDir), status: "active", secretScan: "passed",
    };
    if (text !== undefined)
        record.text = text;
    if (toolArgs !== undefined)
        record.toolArgs = toolArgs;
    if (safeToolName !== undefined)
        record.toolName = safeToolName;
    const fingerprint = safeFingerprint(entry.errorFingerprint, homeDir);
    if (fingerprint !== undefined)
        record.errorFingerprint = fingerprint;
    if (input.producerId !== undefined)
        record.producerId = boundedId(input.producerId, "producer", homeDir);
    if (input.nodeId !== undefined)
        record.nodeId = boundedId(input.nodeId, "node", homeDir);
    record.contentHash = canonicalRecordHash(record);
    return { record };
}
async function statePathForActivationDir(activationDir, sessionId) {
    if (typeof activationDir !== "string" || !isAbsolute(activationDir))
        throw new Error("Capture activation directory must be absolute");
    const normalized = normalize(resolve(activationDir));
    const suffix = join("pi-qdrant-memory", "capture");
    if (!normalized.endsWith(`${sep}${suffix}`))
        throw new Error("Capture activation directory is not host-owned");
    let info;
    try {
        info = await stat(normalized);
    }
    catch {
        throw new Error("Capture activation directory is unavailable");
    }
    if (!info.isDirectory() || !(await safeExistingPath(normalized)))
        throw new Error("Capture activation directory is unsafe");
    return join(normalized, captureStateFilename(sessionId));
}
function parseState(raw, sessionId, host) {
    if (raw === undefined)
        return undefined;
    try {
        const parsed = JSON.parse(raw);
        return validState(parsed, sessionId, host) ? parsed : undefined;
    }
    catch {
        return undefined;
    }
}
function stateKeyCanonical(host, canonicalSessionId) { return `capture:${host}:${sha256Hex(canonicalSessionId)}`; }
async function loadActivation(input, canonicalSessionId) {
    const key = stateKeyCanonical(input.host, canonicalSessionId);
    const active = states.get(key);
    if (active !== undefined) {
        try {
            if (active.filePath !== undefined) {
                const requestedPath = await statePathForActivationDir(input.activationDir, canonicalSessionId);
                if (normalize(resolve(active.filePath)) !== requestedPath)
                    return undefined;
            }
            const durable = parseState(await active.read(), canonicalSessionId, input.host);
            if (durable === undefined)
                return undefined;
            active.state = durable;
            return active;
        }
        catch {
            return undefined;
        }
    }
    const path = await statePathForActivationDir(input.activationDir, canonicalSessionId);
    const fromFile = await readStateFile(path, canonicalSessionId, input.host);
    if (fromFile === undefined)
        return undefined;
    const loaded = { state: fromFile, key, filePath: path, read: async () => readFile(path, "utf8").catch(() => undefined), write: (state) => writeStateFile(path, state) };
    states.set(key, loaded);
    return loaded;
}
function afterTail(entries, state, homeDir = "/") {
    if (state.tailCount > entries.length)
        return undefined;
    if (state.tailCount === 0)
        return entries.slice(0);
    if (state.tailIndex < 0 || state.tailIndex >= entries.length || safeEntryIdentity(entries[state.tailIndex].id, homeDir) !== state.tailEntryId)
        return undefined;
    if (tailHash(entries.slice(0, state.tailCount), homeDir) !== state.tailHash)
        return undefined;
    return entries.slice(state.tailIndex + 1);
}
function initialState(sessionId, host, entries, now, homeDir = "/", invalid = 0) {
    const audit = { redaction: 0, scanner_rejected: 0, scanner_error: 0, invalid_entry: Math.min(MAX_STATE_IDS, Math.max(0, invalid)) };
    return { version: 1, sessionId, host, activatedAt: now, tailEntryId: entries.length === 0 ? null : safeEntryIdentity(entries[entries.length - 1].id, homeDir), tailIndex: entries.length - 1, tailCount: entries.length, tailHash: tailHash(entries, homeDir), capturedIds: [], quarantineIds: [], audit, auditHash: auditHash(audit) };
}
function incrementAudit(audit, category) { audit[category] = Math.min(MAX_STATE_IDS, audit[category] + 1); }
async function persistActive(active, next) {
    await active.write(next);
    const durable = parseState(await active.read(), next.sessionId, next.host);
    if (durable === undefined || JSON.stringify(durable) !== JSON.stringify(next))
        throw new Error("Capture state readback failed");
    active.state = durable;
}
/** Capture only after a successfully persisted activation cutoff. */
function quarantineId(input, entry, category, homeDir) {
    const source = entry === undefined ? `session:${canonicalSession(input.sessionId, homeDir) ?? "unknown"}` : safeEntryIdentity(entry.sourceEntryId, homeDir);
    const part = entry === undefined ? null : entry.partIdentity;
    return deterministicUuid("pi-qdrant-memory-v2:capture-quarantine", input.host, source, part, category);
}
function auditEqual(left, right) { return canonicalStringify(left) === canonicalStringify(right); }
export async function capturePersistedEntries(input) {
    if (!CAPTURE_LIFECYCLES.includes(input.lifecycle))
        throw new TypeError("Unsupported capture lifecycle");
    try {
        const canonical = canonicalSession(input.sessionId, input.homeDir ?? "/");
        if (canonical === undefined || !isHost(input.host) || typeof input.getEntries !== "function" || !projectAllowed(input))
            return [];
        const marker = input.marker === undefined ? { role: "root", depth: 0, valid: true, rootWorkAllowed: true } : resolveAgentMarker(input.marker);
        if (input.marker !== undefined && input.marker.host !== input.host)
            Object.assign(marker, { role: "child", depth: Math.max(1, marker.depth), valid: false, rootWorkAllowed: false });
        let active;
        try {
            active = await loadActivation(input, canonical);
        }
        catch {
            return [];
        }
        if (active === undefined)
            return [];
        try {
            const snapshot = snapshotEntries(input.getEntries(), input.homeDir ?? "/");
            if (snapshot.unsafeIdentity) {
                // Never let a filtered entry redefine ordering: quarantine the batch
                // generically and leave the prior cutoff/hash untouched.
                const quarantineIds = new Set(active.state.quarantineIds);
                const audit = { ...active.state.audit };
                const invalidId = quarantineId(input, undefined, "invalid_entry", input.homeDir ?? "/");
                if (!quarantineIds.has(invalidId)) {
                    quarantineIds.add(invalidId);
                    audit.invalid_entry = Math.min(MAX_STATE_IDS, audit.invalid_entry + snapshot.invalid);
                    try {
                        await persistActive(active, { ...active.state, quarantineIds: [...quarantineIds].slice(-MAX_STATE_IDS), audit, auditHash: auditHash(audit) });
                    }
                    catch { /* fail closed */ }
                }
                return [];
            }
            const entries = snapshot.entries;
            const candidates = afterTail(entries, active.state, input.homeDir ?? "/");
            if (candidates === undefined)
                return [];
            const selected = selectPersistedEntries(candidates, { homeDir: input.homeDir ?? "/", ...(input.toolArgsChars === undefined ? {} : { toolArgsChars: input.toolArgsChars }), ...(input.toolResultChars === undefined ? {} : { toolResultChars: input.toolResultChars }) });
            const durableCaptured = new Set(active.state.capturedIds);
            const durableQuarantine = new Set(active.state.quarantineIds);
            const pendingEpisodeIds = new Set();
            const audit = { ...active.state.audit };
            const beforeAudit = { ...audit };
            const output = [];
            if (snapshot.invalid > 0) {
                const invalidId = quarantineId(input, undefined, "invalid_entry", input.homeDir ?? "/");
                if (!durableQuarantine.has(invalidId)) {
                    durableQuarantine.add(invalidId);
                    audit.invalid_entry = Math.min(MAX_STATE_IDS, audit.invalid_entry + snapshot.invalid);
                }
            }
            for (const entry of selected) {
                const outcome = episodeMaterial(entry, input, marker, active.state.activatedAt);
                if (outcome.record === undefined) {
                    if (outcome.category !== undefined) {
                        const id = quarantineId(input, entry, outcome.category, input.homeDir ?? "/");
                        if (!durableQuarantine.has(id)) {
                            durableQuarantine.add(id);
                            incrementAudit(audit, outcome.category);
                        }
                    }
                    continue;
                }
                if (durableCaptured.has(outcome.record.id) || pendingEpisodeIds.has(outcome.record.id))
                    continue;
                pendingEpisodeIds.add(outcome.record.id);
                output.push(outcome.record);
            }
            const auditChanged = !auditEqual(beforeAudit, audit);
            const auditOnly = { ...active.state, quarantineIds: [...durableQuarantine].slice(-MAX_STATE_IDS), audit, auditHash: auditHash(audit) };
            if (output.length > 0 && input.acceptEpisodes !== undefined) {
                try {
                    await input.acceptEpisodes(output);
                }
                catch {
                    // Quarantine/audit is durable even when downstream acceptance is down;
                    // pending episode IDs stay non-durable so retry remains at-least-once.
                    if (auditChanged) {
                        try {
                            await persistActive(active, auditOnly);
                        }
                        catch { /* fail closed */ }
                    }
                    return [];
                }
            }
            if (output.length === 0 && candidates.length === 0 && snapshot.invalid === 0)
                return [];
            const next = {
                ...auditOnly,
                capturedIds: [...new Set([...durableCaptured, ...pendingEpisodeIds])].slice(-MAX_STATE_IDS),
                tailEntryId: entries.length === 0 ? null : safeEntryIdentity(entries[entries.length - 1].id, input.homeDir ?? "/"),
                tailIndex: entries.length - 1,
                tailCount: entries.length,
                tailHash: tailHash(entries, input.homeDir ?? "/"),
            };
            try {
                await persistActive(active, next);
            }
            catch {
                return [];
            }
            return output;
        }
        catch {
            return [];
        }
    }
    catch {
        return [];
    }
}
/** Persist the current getEntries tail before capture is enabled. */
export async function activateCapture(input) {
    const canonical = canonicalSession(input.sessionId);
    if (canonical === undefined || typeof input.getEntries !== "function" || typeof input.readActivation !== "function" || typeof input.writeActivation !== "function")
        throw new TypeError("Invalid capture activation");
    const host = input.host ?? "pi";
    const key = stateKeyCanonical(host, canonical);
    const read = async () => input.readActivation(key);
    const write = async (state) => input.writeActivation(key, JSON.stringify(state));
    const existingRaw = await read();
    if (existingRaw !== undefined) {
        const state = parseState(existingRaw, canonical, host);
        if (state === undefined)
            throw new Error("Capture activation state is invalid");
        states.set(key, { state, key, read, write });
        input.getEntries();
        return;
    }
    const snapshot = snapshotEntries(input.getEntries(), "/");
    if (snapshot.unsafeIdentity)
        throw new Error("Capture activation entry identity is invalid");
    const entries = snapshot.entries;
    const state = initialState(canonical, host, entries, stableNow(input.now()), "/", snapshot.invalid);
    try {
        await write(state);
        const readback = parseState(await read(), canonical, host);
        if (readback === undefined || JSON.stringify(readback) !== JSON.stringify(state))
            throw new Error("Capture activation readback failed");
    }
    catch {
        states.delete(key);
        throw new Error("Capture activation persistence failed");
    }
    states.set(key, { state, key, read, write });
}
export function clearCaptureActivation(sessionId, host) {
    for (const [key, active] of states)
        if (active.state.sessionId === canonicalSession(sessionId))
            if (host === undefined || active.state.host === host)
                states.delete(key);
}
export async function persistCaptureActivationFile(input) {
    const canonical = canonicalSession(input.sessionId, input.homeDir);
    if (canonical === undefined)
        throw new Error("Invalid session ID");
    const agentDir = await resolveCaptureAgentDirectory(input);
    if (agentDir === null)
        throw new Error("Capture agent directory is invalid");
    const activationDir = join(agentDir, "pi-qdrant-memory", "capture");
    const path = captureStatePath(agentDir, canonical);
    // readStateFile distinguishes ENOENT from malformed/permission errors. Any
    // existing-state error is fatal and must never be replaced by a new cutoff.
    const existing = await readStateFile(path, canonical, input.host);
    const key = stateKeyCanonical(input.host, canonical);
    if (existing !== undefined) {
        states.set(key, { state: existing, key, filePath: path, read: async () => readFile(path, "utf8").catch(() => undefined), write: (state) => writeStateFile(path, state) });
        return activationDir;
    }
    if (states.has(key))
        throw new Error("Capture activation state is missing");
    const snapshot = snapshotEntries(input.getEntries(), input.homeDir);
    if (snapshot.unsafeIdentity)
        throw new Error("Capture activation entry identity is invalid");
    const state = initialState(canonical, input.host, snapshot.entries, stableNow(input.now()), input.homeDir, snapshot.invalid);
    await writeStateFile(path, state);
    const read = async () => readFile(path, "utf8").catch(() => undefined);
    states.set(key, { state, key, filePath: path, read, write: (next) => writeStateFile(path, next) });
    return activationDir;
}
//# sourceMappingURL=episode.js.map