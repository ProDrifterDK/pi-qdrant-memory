export function isNaturalLanguagePrompt(prompt) {
    const trimmed = prompt.trim();
    return trimmed.length > 0 && !trimmed.startsWith("/");
}
export function userTextFromMessage(message) {
    if (typeof message !== "object" || message === null)
        return undefined;
    const record = message;
    if (record.role !== "user")
        return undefined;
    if (typeof record.content === "string")
        return record.content;
    if (!Array.isArray(record.content))
        return undefined;
    return record.content
        .filter((block) => {
        if (typeof block !== "object" || block === null)
            return false;
        const value = block;
        return value.type === "text" && typeof value.text === "string";
    })
        .map((block) => block.text)
        .join("");
}
export function priorUserPromptsFromBranch(entries) {
    const prompts = [];
    for (const entry of entries) {
        if (typeof entry !== "object" || entry === null)
            continue;
        const record = entry;
        if (record.type !== "message")
            continue;
        const text = userTextFromMessage(record.message);
        if (text !== undefined)
            prompts.push(text);
    }
    return prompts;
}
export function buildEffectiveQuery(current, priorUserPrompts) {
    const trimmed = current.replace(/\s+/g, " ").trim();
    if (!isNaturalLanguagePrompt(trimmed))
        return undefined;
    if (trimmed.replace(/\s/g, "").length >= 20)
        return trimmed.slice(0, 4000);
    const prior = [...priorUserPrompts]
        .reverse()
        .map((value) => value.replace(/\s+/g, " ").trim())
        .find((value) => isNaturalLanguagePrompt(value) && value.replace(/\s/g, "").length >= 20);
    return (prior ? `${prior}\n\n${trimmed}` : trimmed).slice(-4000);
}
const STRICT_RFC3339 = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/u;
function retrievalInstant(value) {
    const match = STRICT_RFC3339.exec(value);
    if (match === null)
        throw new TypeError("Invalid retrieval time bound");
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const hour = Number(match[4]);
    const minute = Number(match[5]);
    const second = Number(match[6]);
    const fraction = match[7] ?? "";
    const offsetHour = match[10] === undefined ? 0 : Number(match[10]);
    const offsetMinute = match[11] === undefined ? 0 : Number(match[11]);
    if (month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 || offsetHour > 23 || offsetMinute > 59) {
        throw new TypeError("Invalid retrieval time bound");
    }
    const local = new Date(Date.UTC(year, month - 1, day, hour, minute, second, Number((fraction + "000").slice(0, 3))));
    if (local.getUTCFullYear() !== year || local.getUTCMonth() !== month - 1 || local.getUTCDate() !== day ||
        local.getUTCHours() !== hour || local.getUTCMinutes() !== minute || local.getUTCSeconds() !== second)
        throw new TypeError("Invalid retrieval time bound");
    const millis = Date.parse(value);
    if (!Number.isFinite(millis))
        throw new TypeError("Invalid retrieval time bound");
    return new Date(millis).toISOString();
}
/** Strict, model-safe RFC3339 retrieval window. Offsets are accepted but canonicalized to UTC. */
export function parseRetrievalWindow(after, before) {
    const result = {};
    if (after !== undefined)
        result.after = retrievalInstant(after);
    if (before !== undefined)
        result.before = retrievalInstant(before);
    if (result.after !== undefined && result.before !== undefined && result.after > result.before) {
        throw new TypeError("Invalid retrieval time window");
    }
    return Object.freeze(result);
}
//# sourceMappingURL=query.js.map