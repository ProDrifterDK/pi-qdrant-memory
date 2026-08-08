export function isNaturalLanguagePrompt(prompt: string): boolean {
  const trimmed = prompt.trim();
  return trimmed.length > 0 && !trimmed.startsWith("/");
}

export function userTextFromMessage(message: unknown): string | undefined {
  if (typeof message !== "object" || message === null) return undefined;
  const record = message as Record<string, unknown>;
  if (record.role !== "user") return undefined;

  if (typeof record.content === "string") return record.content;
  if (!Array.isArray(record.content)) return undefined;

  return record.content
    .filter((block): block is { type: "text"; text: string } => {
      if (typeof block !== "object" || block === null) return false;
      const value = block as Record<string, unknown>;
      return value.type === "text" && typeof value.text === "string";
    })
    .map((block) => block.text)
    .join("");
}

export function priorUserPromptsFromBranch(entries: readonly unknown[]): string[] {
  const prompts: string[] = [];
  for (const entry of entries) {
    if (typeof entry !== "object" || entry === null) continue;
    const record = entry as Record<string, unknown>;
    if (record.type !== "message") continue;
    const text = userTextFromMessage(record.message);
    if (text !== undefined) prompts.push(text);
  }
  return prompts;
}

export function buildEffectiveQuery(current: string, priorUserPrompts: readonly string[]): string | undefined {
  const trimmed = current.replace(/\s+/g, " ").trim();
  if (!isNaturalLanguagePrompt(trimmed)) return undefined;
  if (trimmed.replace(/\s/g, "").length >= 20) return trimmed.slice(0, 4000);
  const prior = [...priorUserPrompts]
    .reverse()
    .map((value) => value.replace(/\s+/g, " ").trim())
    .find((value) => isNaturalLanguagePrompt(value) && value.replace(/\s/g, "").length >= 20);
  return (prior ? `${prior}\n\n${trimmed}` : trimmed).slice(-4000);
}
