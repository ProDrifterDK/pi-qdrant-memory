import { createHash } from "node:crypto";
import { redactStructure } from "../security/redaction.js";
const MAX_TOOL_ARGS = 2_000;
const MAX_TOOL_RESULT = 4_000;
const MEMORY_TOOL = /^(?:memory[_-]search|qdrant[_-]memory(?:[_-]search)?)$/iu;
const PRIVATE_PART = /^(?:thinking|reasoning|signature|thought|redacted_thinking)$/iu;
function record(value) { return typeof value === "object" && value !== null && !Array.isArray(value) ? value : undefined; }
function bounded(value, max) { return [...value].slice(0, max).join(""); }
function str(value) { return typeof value === "string" && value.length > 0 ? value : undefined; }
function timestampValue(value) {
    if (typeof value === "string" && value.length > 0)
        return value;
    if (typeof value === "number" && Number.isFinite(value))
        return value;
    return undefined;
}
function bool(value) { return value === true; }
function statusOf(message, entry) { return str(message.status) ?? str(message.stopReason) ?? str(record(entry)?.status); }
function excludedFinality(message, entry) {
    const status = statusOf(message, entry)?.toLowerCase();
    return bool(message.partial) || bool(message.aborted) || bool(message.isPartial) || status === "partial" || status === "aborted" || status === "incomplete";
}
function messageOf(entry) { return record(entry.message); }
function roleOf(message) { return (str(message.role) ?? str(message.type) ?? "").toLowerCase(); }
function partText(part) {
    if (typeof part === "string")
        return part;
    const value = record(part);
    if (value === undefined)
        return undefined;
    const type = str(value.type)?.toLowerCase();
    if (type !== undefined && PRIVATE_PART.test(type))
        return undefined;
    return str(value.text) ?? str(value.content) ?? str(value.output);
}
function textOf(content) {
    if (typeof content === "string")
        return content;
    if (!Array.isArray(content))
        return partText(content);
    const parts = content.map(partText).filter((part) => part !== undefined);
    return parts.length === 0 ? undefined : parts.join("\n");
}
function partsOf(content) { return Array.isArray(content) ? content : [content]; }
function toolNameFrom(value) {
    return str(value.name) ?? str(value.toolName) ?? str(value.tool_name);
}
function toolArgsFrom(value) {
    const args = value.arguments ?? value.args ?? value.input ?? value.parameters;
    if (args === undefined)
        return undefined;
    if (typeof args === "string")
        return bounded(args, MAX_TOOL_ARGS);
    try {
        return bounded(JSON.stringify(args), MAX_TOOL_ARGS);
    }
    catch {
        return undefined;
    }
}
function ownMemoryTool(name) { return name !== undefined && MEMORY_TOOL.test(name); }
function nonEmptyError(value) {
    if (typeof value === "string")
        return value.trim().length > 0;
    return value !== undefined && value !== null && value !== false;
}
function failureStatus(value) { return typeof value === "string" && /^(?:error|failed|failure|non[-_ ]?success|timeout|timed[-_ ]?out|cancel(?:ed|led)|killed|rejected|denied)$/iu.test(value); }
function nonZero(value) { return typeof value === "number" && Number.isFinite(value) && value !== 0; }
function isMemoryContextText(value) {
    return /<memory[-_]context\b[^>]*>/iu.test(value) || /<\/memory[-_]context>/iu.test(value) || /\[memory[-_]context\]/iu.test(value);
}
function errorFingerprint(value) {
    const text = typeof value === "string" ? value : value === undefined ? undefined : (() => { try {
        return JSON.stringify(value);
    }
    catch {
        return undefined;
    } })();
    return text === undefined || text.length === 0 ? undefined : createHash("sha256").update(text.normalize("NFC"), "utf8").digest("hex").slice(0, 32);
}
function entryPartId(entry, index) { return index === 0 ? 0 : index; }
function cleanText(value, max, homeDir = "/") { if (value === undefined)
    return undefined; const text = bounded(value, max); if (text.length === 0)
    return undefined; const safe = redactStructure({ text, maxChars: max, homeDir }); return safe.text.length === 0 ? undefined : safe.text; }
/** Normalize only final, persisted entries. Event arrays supplied by host callbacks are not accepted here. */
export function selectPersistedEntries(entries, options = {}) {
    if (!Array.isArray(entries))
        return [];
    const toolArgsChars = Number.isSafeInteger(options.toolArgsChars) && (options.toolArgsChars ?? 0) >= 0 ? Math.min(options.toolArgsChars ?? MAX_TOOL_ARGS, MAX_TOOL_ARGS) : MAX_TOOL_ARGS;
    const toolResultChars = Number.isSafeInteger(options.toolResultChars) && (options.toolResultChars ?? 0) >= 0 ? Math.min(options.toolResultChars ?? MAX_TOOL_RESULT, MAX_TOOL_RESULT) : MAX_TOOL_RESULT;
    const homeDir = options.homeDir ?? "/";
    const selected = [];
    for (const entry of entries) {
        if (typeof entry?.id !== "string" || entry.id.length === 0 || entry.id.length > 512)
            continue;
        const message = messageOf(entry);
        const entryType = entry.type.toLowerCase();
        if (["system", "developer", "custom", "memory", "memory_context"].some((kind) => entryType === kind || entryType.startsWith(`${kind}_`)))
            continue;
        if (message === undefined || excludedFinality(message, entry) || bool(entry.partial) || bool(entry.aborted))
            continue;
        const customType = str(message.customType) ?? str(message.contextType);
        if (customType !== undefined && /memory|context|system|developer/iu.test(customType))
            continue;
        const role = roleOf(message);
        if (["system", "developer", "custom", "context", "memory", "memory_context"].includes(role))
            continue;
        const content = message.content ?? message.parts ?? message;
        const eventAt = timestampValue(message.timestamp) ?? timestampValue(message.eventAt) ?? timestampValue(entry.timestamp);
        const turnId = str(message.turnId) ?? str(entry.turnId);
        if (role === "user" || role === "assistant") {
            const parts = partsOf(content);
            let partIndex = 0;
            for (const part of parts) {
                const value = record(part);
                const type = str(value?.type)?.toLowerCase();
                if (type !== undefined && PRIVATE_PART.test(type)) {
                    partIndex += 1;
                    continue;
                }
                const name = value === undefined ? undefined : toolNameFrom(value);
                if (name !== undefined && (type === undefined || /tool(?:call|_call)|function/iu.test(type) || value?.arguments !== undefined || value?.input !== undefined)) {
                    if (!ownMemoryTool(name)) {
                        const call = { sourceEntryId: entry.id, messageId: str(message.id) ?? entry.id, partIdentity: entryPartId(entry, partIndex), eventKind: "tool_call", toolName: bounded(name, 256) };
                        const args = cleanText(toolArgsFrom(value), toolArgsChars, homeDir);
                        if (args !== undefined)
                            call.toolArgs = args;
                        if (eventAt !== undefined)
                            call.eventAt = eventAt;
                        if (turnId !== undefined)
                            call.turnId = turnId;
                        selected.push(call);
                    }
                    partIndex += 1;
                    continue;
                }
                const text = cleanText(partText(part), 16_000, homeDir);
                const memoryWrapper = text !== undefined && isMemoryContextText(text);
                if (text !== undefined && !memoryWrapper) {
                    const item = { sourceEntryId: entry.id, messageId: str(message.id) ?? entry.id, partIdentity: entryPartId(entry, partIndex), eventKind: role, text };
                    if (eventAt !== undefined)
                        item.eventAt = eventAt;
                    if (turnId !== undefined)
                        item.turnId = turnId;
                    selected.push(item);
                }
                partIndex += 1;
            }
            continue;
        }
        if (["tool", "toolresult", "tool_result", "function"].includes(role)) {
            const name = toolNameFrom(message);
            if (ownMemoryTool(name))
                continue;
            const detail = record(message.details);
            const status = statusOf(message, entry) ?? str(detail?.status);
            const detailStatus = detail?.status;
            const explicitFailure = bool(message.isError) || nonEmptyError(message.error) || nonEmptyError(detail?.error) || failureStatus(status) || failureStatus(detailStatus) || nonZero(message.code) || nonZero(message.exitCode) || nonZero(detail?.code) || nonZero(detail?.exitCode);
            const fields = [];
            const metadataFields = explicitFailure ? ["stderr", "status", "code", "exitCode", "error", "message"] : ["status", "code", "exitCode"];
            for (const field of metadataFields) {
                const value = message[field] ?? detail?.[field];
                if (typeof value === "string" || typeof value === "number" || typeof value === "boolean")
                    fields.push(`${field}: ${String(value)}`);
                else if (field === "error" && value !== undefined) {
                    try {
                        fields.push(`error: ${JSON.stringify(value)}`);
                    }
                    catch { /* circular error metadata is represented by the error lane */ }
                }
            }
            if (!explicitFailure) {
                const stderr = message.stderr ?? detail?.stderr;
                if (typeof stderr === "string" && stderr.trim() !== "")
                    fields.push(`stderr: ${stderr}`);
            }
            const body = textOf(content);
            if (explicitFailure && body !== undefined)
                fields.unshift(body);
            const text = cleanText(fields.join("\n"), toolResultChars, homeDir);
            if (text === undefined && !explicitFailure)
                continue;
            const item = { sourceEntryId: entry.id, messageId: str(message.id) ?? entry.id, partIdentity: 0, eventKind: explicitFailure ? "tool_error" : "tool_result" };
            if (text !== undefined)
                item.text = text;
            if (name !== undefined)
                item.toolName = bounded(name, 256);
            if (status !== undefined)
                item.status = status;
            const code = typeof message.code === "number" ? message.code : typeof message.exitCode === "number" ? message.exitCode : typeof detail?.code === "number" ? detail.code : typeof detail?.exitCode === "number" ? detail.exitCode : undefined;
            if (code !== undefined)
                item.code = code;
            if (explicitFailure) {
                const fingerprint = errorFingerprint(text);
                if (fingerprint !== undefined)
                    item.errorFingerprint = fingerprint;
            }
            if (eventAt !== undefined)
                item.eventAt = eventAt;
            if (turnId !== undefined)
                item.turnId = turnId;
            selected.push(item);
        }
    }
    return selected;
}
//# sourceMappingURL=select.js.map