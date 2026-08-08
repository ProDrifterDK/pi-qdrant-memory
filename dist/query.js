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
//# sourceMappingURL=query.js.map