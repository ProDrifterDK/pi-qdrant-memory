const hostFromExplicit = (explicit) => {
    if (explicit === "prime" || explicit === "pi")
        return explicit;
    return undefined;
};
const hasMarker = (value) => typeof value === "string" && value.trim().length > 0;
const argvBasename = (value) => {
    const parts = value.split(/[\\/]/);
    return parts[parts.length - 1] ?? "";
};
export function detectHost(input) {
    if (input.explicit !== undefined) {
        const host = hostFromExplicit(input.explicit);
        return host === undefined
            ? { ok: false, reason: "invalid-explicit-host" }
            : { ok: true, host };
    }
    const hosts = new Set();
    if (hasMarker(input.env.PRIME_AGENT_CODING_AGENT_DIR))
        hosts.add("prime");
    if (hasMarker(input.env.PI_CODING_AGENT_DIR))
        hosts.add("pi");
    for (const arg of input.argv) {
        const basename = argvBasename(arg);
        if (basename === "prime-agent")
            hosts.add("prime");
        if (basename === "pi")
            hosts.add("pi");
    }
    if (hosts.size === 0)
        return { ok: false, reason: "unknown" };
    if (hosts.size > 1)
        return { ok: false, reason: "conflict" };
    return { ok: true, host: [...hosts][0] };
}
function parsePersistedDepth(value) {
    if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
        return value;
    throw new Error("RLM depth must be a non-negative integer");
}
function parseEnvironmentDepth(value) {
    const trimmed = value.trim();
    if (/^\d+$/.test(trimmed)) {
        const depth = Number(trimmed);
        if (Number.isSafeInteger(depth) && depth >= 0)
            return depth;
    }
    throw new Error("RLM depth must be a non-negative integer");
}
export function resolvePrimeRlmDepth(header, env) {
    const record = typeof header === "object" && header !== null ? header : undefined;
    if (record !== undefined && record.rlmDepth !== undefined) {
        return parsePersistedDepth(record.rlmDepth);
    }
    if (env.RLM_DEPTH !== undefined)
        return parseEnvironmentDepth(env.RLM_DEPTH);
    return 0;
}
//# sourceMappingURL=host.js.map