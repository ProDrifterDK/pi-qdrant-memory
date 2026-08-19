import * as PiAi from "@earendil-works/pi-ai";
import { isPolicyExpired, processingPolicyHash, PROVIDER_AGNOSTIC_ORIGIN } from "../domain/policy.js";
const MAX_INPUT_TOKENS = 65_536;
const MIN_OUTPUT_TOKENS = 128;
const MAX_OUTPUT_TOKENS = 8_192;
const MAX_MODEL_TOKENS = 1_000_000;
const MIN_TIMEOUT_MS = 1_000;
const MAX_TIMEOUT_MS = 120_000;
const LOW_TEMPERATURE = 0;
const REDACTED_ID = /^[A-Za-z0-9._:/-]{1,256}$/u;
const REDACTED_LABEL = /^[A-Za-z0-9._:/ -]{1,128}$/u;
const SECRETISH = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;
class LinkedAbortError extends Error {
    reason;
    constructor(reason) {
        super(reason);
        this.reason = reason;
    }
}
function pending(reason) { return { state: "pending", reason }; }
function isRecord(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
function isCallable(value) { return typeof value === "function"; }
function isRedactedIdentifier(value) { return typeof value === "string" && REDACTED_ID.test(value) && !SECRETISH.test(value); }
function isRedactedLabel(value) { return typeof value === "string" && REDACTED_LABEL.test(value) && !SECRETISH.test(value); }
function isFiniteInteger(value, min, max) { return typeof value === "number" && Number.isSafeInteger(value) && value >= min && value <= max; }
/** Normalization rejects BGE-M3 spelling, vendor-prefix, punctuation, and whitespace aliases. */
function isBgeM3Alias(value) {
    return typeof value === "string" && value.toLowerCase().replace(/[^a-z0-9]/gu, "").includes("bgem3");
}
function isBgeM3(model) {
    return isRecord(model) && (isBgeM3Alias(model.id) || isBgeM3Alias(model.name));
}
function isUsableModel(model) {
    return isRecord(model) && isRedactedIdentifier(model.id) && isRedactedIdentifier(model.provider) &&
        isFiniteInteger(model.contextWindow, 1, MAX_MODEL_TOKENS) && isFiniteInteger(model.maxTokens, 1, MAX_MODEL_TOKENS) &&
        model.maxTokens <= model.contextWindow && !isBgeM3(model);
}
function inputIsBounded(input) {
    return typeof input.envelope === "string" && input.envelope.length > 0 &&
        // A UTF-8 byte bound is conservative for every tokenizer, unlike a heuristic
        // character-to-token ratio that could underestimate hostile Unicode input.
        isFiniteInteger(input.maxInputTokens, 1, MAX_INPUT_TOKENS) && Buffer.byteLength(input.envelope, "utf8") <= input.maxInputTokens &&
        isFiniteInteger(input.maxOutputTokens, MIN_OUTPUT_TOKENS, MAX_OUTPUT_TOKENS) &&
        isFiniteInteger(input.timeoutMs, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS) && isRedactedIdentifier(input.promptRevision);
}
function sameDestination(left, right) {
    return left.id === right.id && left.residency === right.residency && left.dataUse === right.dataUse;
}
/** Validates the exact producer/worker policy intersection and returns only a computed digest. */
function verifiedPolicyHash(context) {
    try {
        const { policy } = context;
        const llmId = policy.destinationIds.llm;
        const policyHash = processingPolicyHash(policy);
        if (policyHash !== policy.id || isPolicyExpired(policy))
            return null;
        if (policy.ownerHost !== context.host || llmId === undefined || context.llmDestination === undefined)
            return null;
        if (!sameDestination(context.llmDestination, { id: llmId, residency: policy.residency, dataUse: policy.dataUse }))
            return null;
        if (context.policyHash !== undefined && context.policyHash !== policyHash)
            return null;
        if (context.policyEpoch !== undefined && !isFiniteInteger(context.policyEpoch, 0, Number.MAX_SAFE_INTEGER))
            return null;
        return policyHash;
    }
    catch {
        return null;
    }
}
function selectModel(input) {
    const context = input.memoryContext;
    if (context.memoryModel !== undefined) {
        return input.model === context.memoryModel ? { model: context.memoryModel, activeFallback: false } : "no_model";
    }
    if (context.activeModel === undefined)
        return "no_model";
    if (context.allowActiveModelFallback !== true)
        return "fallback_disabled";
    if (input.model !== context.activeModel)
        return "no_model";
    // The active model may be used only with two independently coherent host/session markers.
    if (context.activeProviderId !== context.activeModel.provider || context.sessionProviderId !== context.activeModel.provider ||
        !isRedactedIdentifier(context.activeProviderId) || !isRedactedIdentifier(context.sessionProviderId))
        return "policy";
    return { model: context.activeModel, activeFallback: true };
}
function hasReplayPermission(context, model) {
    return context.policy.originProvider === PROVIDER_AGNOSTIC_ORIGIN || model.provider === context.policy.originProvider ||
        (context.allowCrossProviderReplay === true && context.policy.allowCrossProviderReplay === true);
}
/** Requires concrete host resolver evidence for this exact selected provider/model/destination tuple. */
function destinationBindsSelectedModel(context, model) {
    const binding = context.llmDestinationBinding;
    return binding !== undefined && binding.providerId === model.provider && binding.modelId === model.id &&
        binding.destinationId === context.llmDestination.id;
}
function contextFitsModel(input, model) {
    const inputBytes = Buffer.byteLength(input.envelope, "utf8");
    // inputBytes is intentionally the conservative input-token upper bound used by inputIsBounded.
    return input.maxOutputTokens <= model.maxTokens && inputBytes + input.maxOutputTokens <= model.contextWindow;
}
/** Egress cannot inherit host prompts, historic messages, or tools. */
function envelopeContext(envelope) {
    const outboundContext = { messages: [{ role: "user", content: envelope, timestamp: Date.now() }] };
    return outboundContext;
}
function textFromCompletion(value) {
    if (typeof value === "string")
        return value;
    if (!isRecord(value) || !Array.isArray(value.content))
        return null;
    const parts = [];
    for (const item of value.content)
        if (isRecord(item) && item.type === "text" && typeof item.text === "string")
            parts.push(item.text);
    const text = parts.join("");
    return text.length === 0 ? null : text;
}
/** Returns a new record; nullable host defaults are never forwarded to Prime. */
export function sanitizeAuthHeaders(headers) {
    const sanitized = Object.create(null);
    if (headers === undefined)
        return sanitized;
    for (const [name, value] of Object.entries(headers))
        if (typeof value === "string")
            sanitized[name] = value;
    return sanitized;
}
function isSuccessfulAuth(value) {
    if (!isRecord(value) || value.ok !== true)
        return false;
    if (value.apiKey !== undefined && typeof value.apiKey !== "string")
        return false;
    return value.headers === undefined || isRecord(value.headers);
}
function linkedSignal(source, timeoutMs) {
    const controller = new AbortController();
    let rejectAbort = () => undefined;
    let abortReason;
    const waitForAbort = new Promise((_resolve, reject) => { rejectAbort = reject; });
    // A pre-aborted source can reject before an operation is raced; mark that rejection handled.
    void waitForAbort.catch(() => undefined);
    const abort = (reason) => {
        if (abortReason !== undefined)
            return;
        abortReason = reason;
        // Reject before notifying a host implementation, which may synchronously reject its own promise.
        rejectAbort(new LinkedAbortError(reason));
        controller.abort();
    };
    const onSourceAbort = () => abort("cancelled");
    if (source !== undefined) {
        if (source.aborted)
            onSourceAbort();
        else
            source.addEventListener("abort", onSourceAbort, { once: true });
    }
    const timer = setTimeout(() => abort("timeout"), timeoutMs);
    return {
        signal: controller.signal,
        waitForAbort,
        reason: () => abortReason,
        cleanup() { clearTimeout(timer); if (source !== undefined)
            source.removeEventListener("abort", onSourceAbort); },
    };
}
function baseOptions(input, model) {
    return {
        maxTokens: input.maxOutputTokens,
        timeoutMs: input.timeoutMs,
        ...(model.api === "openai-codex-responses" ? {} : { temperature: LOW_TEMPERATURE }),
    };
}
async function invokeWithinBudget(operation, linked) {
    const stopped = linked.reason();
    if (stopped !== undefined)
        return { ok: false, reason: stopped };
    try {
        const value = await Promise.race([Promise.resolve(operation()), linked.waitForAbort]);
        return { ok: true, value };
    }
    catch (error) {
        if (error instanceof LinkedAbortError)
            return { ok: false, reason: error.reason };
        return { ok: false, reason: "failed" };
    }
}
async function invokeCompletion(call, receiver, model, context, options, linked) {
    const boundedOptions = { ...options, signal: linked.signal };
    return invokeWithinBudget(() => call.call(receiver, model, context, boundedOptions), linked);
}
function provenance(input, model, policyHash) {
    const { memoryContext } = input;
    return {
        host: memoryContext.host, providerId: model.provider, modelId: model.id, destinationId: memoryContext.llmDestination.id,
        policyId: memoryContext.policy.id, policyEpoch: memoryContext.policyEpoch ?? 0, policyHash,
        promptRevision: input.promptRevision, invokedAt: new Date().toISOString(),
    };
}
/** Internal bridge path; its exported wrapper turns every unexpected host-shape failure into pending work. */
async function completeMemorySafely(input) {
    if (!inputIsBounded(input))
        return pending("invalid_input");
    const policyHash = verifiedPolicyHash(input.memoryContext);
    if (policyHash === null)
        return pending("policy");
    const selected = selectModel(input);
    if (typeof selected === "string")
        return pending(selected);
    if (!isUsableModel(selected.model))
        return pending(isBgeM3(selected.model) ? "unsupported_model" : "no_model");
    if (!hasReplayPermission(input.memoryContext, selected.model))
        return pending("cross_provider_disabled");
    if (!destinationBindsSelectedModel(input.memoryContext, selected.model))
        return pending("policy");
    if (!contextFitsModel(input, selected.model))
        return pending("invalid_input");
    const context = input.memoryContext;
    const registryComplete = Reflect.get(context.modelRegistry, "complete");
    const options = baseOptions(input, selected.model);
    const outboundContext = envelopeContext(input.envelope);
    const linked = linkedSignal(input.signal, input.timeoutMs);
    try {
        const stopped = linked.reason();
        if (stopped !== undefined)
            return pending(stopped);
        let outcome;
        if (isCallable(registryComplete)) {
            outcome = await invokeCompletion(registryComplete, context.modelRegistry, selected.model, outboundContext, options, linked);
        }
        else {
            // The registry is Pi's only egress path. Namespace fallback belongs solely to Prime.
            if (context.host !== "prime")
                return pending("no_completion_method");
            const aiNamespace = input.aiNamespace ?? PiAi;
            const completeSimple = Reflect.get(aiNamespace, "completeSimple");
            const getAuth = Reflect.get(context.modelRegistry, "getApiKeyAndHeaders");
            if (!isCallable(completeSimple))
                return pending("no_completion_method");
            if (!isCallable(getAuth))
                return pending("auth");
            const authResult = await invokeWithinBudget(() => getAuth.call(context.modelRegistry, selected.model), linked);
            if (!authResult.ok)
                return pending(authResult.reason === "failed" ? "auth" : authResult.reason);
            if (!isSuccessfulAuth(authResult.value))
                return pending("auth");
            const authenticatedOptions = {
                ...options,
                ...(typeof authResult.value.apiKey === "string" && authResult.value.apiKey.length > 0 ? { apiKey: authResult.value.apiKey } : {}),
                headers: sanitizeAuthHeaders(authResult.value.headers),
            };
            outcome = await invokeCompletion(completeSimple, aiNamespace, selected.model, outboundContext, authenticatedOptions, linked);
        }
        if (!outcome.ok)
            return pending(outcome.reason);
        const text = textFromCompletion(outcome.value);
        if (text === null)
            return pending("invalid_response");
        if (Buffer.byteLength(text, "utf8") > input.maxOutputTokens)
            return pending("output_limit");
        return { state: "completed", text, provenance: provenance(input, selected.model, policyHash) };
    }
    finally {
        linked.cleanup();
    }
}
/** Prefers Pi's registry; Prime fallback is authenticated, bounded, and never aborts a host turn. */
export async function completeMemory(input) {
    try {
        return await completeMemorySafely(input);
    }
    catch {
        return pending("failed");
    }
}
//# sourceMappingURL=llm.js.map