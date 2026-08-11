import * as PiAi from "@earendil-works/pi-ai";
import type { Api, Context, Model } from "@earendil-works/pi-ai";
import type { AuthorizedDestination, HostId } from "../types.js";
import { isPolicyExpired, processingPolicyHash, type ProcessingPolicy } from "../domain/policy.js";

const MAX_INPUT_TOKENS = 65_536;
const MIN_OUTPUT_TOKENS = 128;
const MAX_OUTPUT_TOKENS = 8_192;
const MAX_MODEL_TOKENS = 1_000_000;
const MIN_TIMEOUT_MS = 1_000;
const MAX_TIMEOUT_MS = 120_000;
const LOW_TEMPERATURE = 0 as const;
const REDACTED_ID = /^[A-Za-z0-9._:/-]{1,256}$/u;
const REDACTED_LABEL = /^[A-Za-z0-9._:/ -]{1,128}$/u;
const SECRETISH = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;

export type ResolvedRequestAuthLike =
  | { ok: true; apiKey?: string; headers?: Record<string, string | null> }
  | { ok: false; error: string };

export interface MemoryCompletionOptions {
  signal?: AbortSignal;
  maxOutputTokens: number;
  temperature: number;
}

export interface ModelRegistryLike {
  getApiKeyAndHeaders?: (model: Model<Api>) => Promise<ResolvedRequestAuthLike>;
  complete?: unknown;
}

/** A host-verified exact mapping from the selected registry model to one policy destination. */
export interface LlmDestinationModelBinding {
  readonly providerId: string;
  readonly modelId: string;
  readonly destinationId: string;
}

export interface MemoryCompletionContext {
  host: HostId;
  modelRegistry: ModelRegistryLike;
  memoryModel?: Model<Api>;
  activeModel?: Model<Api>;
  activeProviderId?: string;
  sessionProviderId?: string;
  allowActiveModelFallback?: boolean;
  allowCrossProviderReplay?: boolean;
  policy: ProcessingPolicy;
  /** The producer/worker intersection's authorized LLM destination. */
  llmDestination: AuthorizedDestination;
  /** Required trusted resolver evidence; allowlist membership alone is never enough. */
  llmDestinationBinding: LlmDestinationModelBinding;
  policyEpoch?: number;
  /** Optional supplied policy digest is accepted only when it equals the computed digest. */
  policyHash?: string;
}

export interface AiNamespaceLike { completeSimple?: unknown; }

export interface CompletionProvenance {
  readonly host: HostId;
  readonly providerId: string;
  readonly modelId: string;
  readonly destinationId: string;
  readonly policyId: string;
  readonly policyEpoch: number;
  readonly policyHash: string;
  readonly promptRevision: string;
  readonly invokedAt: string;
}

export type MemoryCompletionPendingReason =
  | "invalid_input"
  | "no_model"
  | "unsupported_model"
  | "fallback_disabled"
  | "cross_provider_disabled"
  | "policy"
  | "no_completion_method"
  | "auth"
  | "cancelled"
  | "timeout"
  | "invalid_response"
  | "output_limit"
  | "failed";

export type MemoryCompletionResult =
  | { readonly state: "completed"; readonly text: string; readonly provenance: CompletionProvenance }
  | { readonly state: "pending"; readonly reason: MemoryCompletionPendingReason };

export interface BoundLlmDestination {
  readonly destination: AuthorizedDestination;
  complete(input: { envelope: string; signal?: AbortSignal }): Promise<string>;
}

export interface CompleteMemoryInput {
  envelope: string;
  /** Must be the exact registry model selected by memoryModel or active fallback. */
  model: Model<Api>;
  /** Deliberately not forwarded: egress always receives a fresh envelope-only Context. */
  hostContext: Context;
  maxInputTokens: number;
  maxOutputTokens: number;
  timeoutMs: number;
  signal?: AbortSignal;
  memoryContext: MemoryCompletionContext;
  promptRevision: string;
  aiNamespace?: AiNamespaceLike;
}

interface HostCompletionOptions {
  signal: AbortSignal;
  maxTokens: number;
  timeoutMs: number;
  temperature: typeof LOW_TEMPERATURE;
  apiKey?: string;
  headers?: Record<string, string>;
}
type HostCompletionBaseOptions = Omit<HostCompletionOptions, "signal">;
type HostCompletion = (model: Model<Api>, context: Context, options: HostCompletionOptions) => Promise<unknown>;
type AuthResolver = (model: Model<Api>) => Promise<ResolvedRequestAuthLike>;
type AbortReason = "cancelled" | "timeout";

class LinkedAbortError extends Error {
  constructor(readonly reason: AbortReason) { super(reason); }
}

interface LinkedRequestSignal {
  readonly signal: AbortSignal;
  readonly waitForAbort: Promise<never>;
  reason(): AbortReason | undefined;
  cleanup(): void;
}

function pending(reason: MemoryCompletionPendingReason): MemoryCompletionResult { return { state: "pending", reason }; }
function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function isCallable(value: unknown): value is (...args: never[]) => unknown { return typeof value === "function"; }
function isRedactedIdentifier(value: unknown): value is string { return typeof value === "string" && REDACTED_ID.test(value) && !SECRETISH.test(value); }
function isRedactedLabel(value: unknown): value is string { return typeof value === "string" && REDACTED_LABEL.test(value) && !SECRETISH.test(value); }
function isFiniteInteger(value: unknown, min: number, max: number): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= min && value <= max; }

/** Normalization rejects BGE-M3 spelling, vendor-prefix, punctuation, and whitespace aliases. */
function isBgeM3Alias(value: unknown): boolean {
  return typeof value === "string" && value.toLowerCase().replace(/[^a-z0-9]/gu, "").includes("bgem3");
}
function isBgeM3(model: unknown): boolean {
  return isRecord(model) && (isBgeM3Alias(model.id) || isBgeM3Alias(model.name));
}
function isUsableModel(model: unknown): model is Model<Api> {
  return isRecord(model) && isRedactedIdentifier(model.id) && isRedactedIdentifier(model.provider) &&
    isFiniteInteger(model.contextWindow, 1, MAX_MODEL_TOKENS) && isFiniteInteger(model.maxTokens, 1, MAX_MODEL_TOKENS) &&
    model.maxTokens <= model.contextWindow && !isBgeM3(model);
}

function inputIsBounded(input: CompleteMemoryInput): boolean {
  return typeof input.envelope === "string" && input.envelope.length > 0 &&
    // A UTF-8 byte bound is conservative for every tokenizer, unlike a heuristic
    // character-to-token ratio that could underestimate hostile Unicode input.
    isFiniteInteger(input.maxInputTokens, 1, MAX_INPUT_TOKENS) && Buffer.byteLength(input.envelope, "utf8") <= input.maxInputTokens &&
    isFiniteInteger(input.maxOutputTokens, MIN_OUTPUT_TOKENS, MAX_OUTPUT_TOKENS) &&
    isFiniteInteger(input.timeoutMs, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS) && isRedactedIdentifier(input.promptRevision);
}

function sameDestination(left: AuthorizedDestination, right: { id: string; residency: string; dataUse: string }): boolean {
  return left.id === right.id && left.residency === right.residency && left.dataUse === right.dataUse;
}

/** Validates the exact producer/worker policy intersection and returns only a computed digest. */
function verifiedPolicyHash(context: MemoryCompletionContext): string | null {
  try {
    const { policy } = context;
    const llmId = policy.destinationIds.llm;
    const policyHash = processingPolicyHash(policy);
    if (policyHash !== policy.id || isPolicyExpired(policy)) return null;
    if (policy.ownerHost !== context.host || llmId === undefined || context.llmDestination === undefined) return null;
    if (!sameDestination(context.llmDestination, { id: llmId, residency: policy.residency, dataUse: policy.dataUse })) return null;
    if (context.policyHash !== undefined && context.policyHash !== policyHash) return null;
    if (context.policyEpoch !== undefined && !isFiniteInteger(context.policyEpoch, 0, Number.MAX_SAFE_INTEGER)) return null;
    return policyHash;
  } catch { return null; }
}

function selectModel(input: CompleteMemoryInput): { model: Model<Api>; activeFallback: boolean } | MemoryCompletionPendingReason {
  const context = input.memoryContext;
  if (context.memoryModel !== undefined) {
    return input.model === context.memoryModel ? { model: context.memoryModel, activeFallback: false } : "no_model";
  }
  if (context.activeModel === undefined) return "no_model";
  if (context.allowActiveModelFallback !== true) return "fallback_disabled";
  if (input.model !== context.activeModel) return "no_model";
  // The active model may be used only with two independently coherent host/session markers.
  if (context.activeProviderId !== context.activeModel.provider || context.sessionProviderId !== context.activeModel.provider ||
    !isRedactedIdentifier(context.activeProviderId) || !isRedactedIdentifier(context.sessionProviderId)) return "policy";
  return { model: context.activeModel, activeFallback: true };
}

function hasReplayPermission(context: MemoryCompletionContext, model: Model<Api>): boolean {
  return model.provider === context.policy.originProvider ||
    (context.allowCrossProviderReplay === true && context.policy.allowCrossProviderReplay === true);
}

/** Requires concrete host resolver evidence for this exact selected provider/model/destination tuple. */
function destinationBindsSelectedModel(context: MemoryCompletionContext, model: Model<Api>): boolean {
  const binding = context.llmDestinationBinding;
  return binding !== undefined && binding.providerId === model.provider && binding.modelId === model.id &&
    binding.destinationId === context.llmDestination.id;
}

function contextFitsModel(input: CompleteMemoryInput, model: Model<Api>): boolean {
  const inputBytes = Buffer.byteLength(input.envelope, "utf8");
  // inputBytes is intentionally the conservative input-token upper bound used by inputIsBounded.
  return input.maxOutputTokens <= model.maxTokens && inputBytes + input.maxOutputTokens <= model.contextWindow;
}

/** Egress cannot inherit host prompts, historic messages, or tools. */
function envelopeContext(envelope: string): Context {
  const outboundContext: Context = { messages: [{ role: "user", content: envelope, timestamp: Date.now() }] };
  return outboundContext;
}

function textFromCompletion(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (!isRecord(value) || !Array.isArray(value.content)) return null;
  const parts: string[] = [];
  for (const item of value.content) if (isRecord(item) && item.type === "text" && typeof item.text === "string") parts.push(item.text);
  const text = parts.join("");
  return text.length === 0 ? null : text;
}

/** Returns a new record; nullable host defaults are never forwarded to Prime. */
export function sanitizeAuthHeaders(headers?: Record<string, string | null>): Record<string, string> {
  const sanitized: Record<string, string> = Object.create(null) as Record<string, string>;
  if (headers === undefined) return sanitized;
  for (const [name, value] of Object.entries(headers as Record<string, unknown>)) if (typeof value === "string") sanitized[name] = value;
  return sanitized;
}

function isSuccessfulAuth(value: unknown): value is Extract<ResolvedRequestAuthLike, { ok: true }> {
  if (!isRecord(value) || value.ok !== true) return false;
  if (value.apiKey !== undefined && typeof value.apiKey !== "string") return false;
  return value.headers === undefined || isRecord(value.headers);
}

function linkedSignal(source: AbortSignal | undefined, timeoutMs: number): LinkedRequestSignal {
  const controller = new AbortController();
  let rejectAbort: (error: LinkedAbortError) => void = () => undefined;
  let abortReason: AbortReason | undefined;
  const waitForAbort = new Promise<never>((_resolve, reject) => { rejectAbort = reject as (error: LinkedAbortError) => void; });
  // A pre-aborted source can reject before an operation is raced; mark that rejection handled.
  void waitForAbort.catch(() => undefined);
  const abort = (reason: AbortReason): void => {
    if (abortReason !== undefined) return;
    abortReason = reason;
    // Reject before notifying a host implementation, which may synchronously reject its own promise.
    rejectAbort(new LinkedAbortError(reason));
    controller.abort();
  };
  const onSourceAbort = (): void => abort("cancelled");
  if (source !== undefined) {
    if (source.aborted) onSourceAbort();
    else source.addEventListener("abort", onSourceAbort, { once: true });
  }
  const timer = setTimeout(() => abort("timeout"), timeoutMs);
  return {
    signal: controller.signal,
    waitForAbort,
    reason: () => abortReason,
    cleanup(): void { clearTimeout(timer); if (source !== undefined) source.removeEventListener("abort", onSourceAbort); },
  };
}

function baseOptions(input: CompleteMemoryInput): HostCompletionBaseOptions {
  return { maxTokens: input.maxOutputTokens, timeoutMs: input.timeoutMs, temperature: LOW_TEMPERATURE };
}

type BoundedResult<T> = { ok: true; value: T } | { ok: false; reason: AbortReason | "failed" };
async function invokeWithinBudget<T>(operation: () => Promise<T> | T, linked: LinkedRequestSignal): Promise<BoundedResult<T>> {
  const stopped = linked.reason();
  if (stopped !== undefined) return { ok: false, reason: stopped };
  try {
    const value = await Promise.race([Promise.resolve(operation()), linked.waitForAbort]);
    return { ok: true, value };
  } catch (error: unknown) {
    if (error instanceof LinkedAbortError) return { ok: false, reason: error.reason };
    return { ok: false, reason: "failed" };
  }
}

async function invokeCompletion(call: HostCompletion, receiver: unknown, model: Model<Api>, context: Context, options: HostCompletionBaseOptions, linked: LinkedRequestSignal): Promise<BoundedResult<unknown>> {
  const boundedOptions: HostCompletionOptions = { ...options, signal: linked.signal };
  return invokeWithinBudget(() => call.call(receiver, model, context, boundedOptions), linked);
}

function provenance(input: CompleteMemoryInput, model: Model<Api>, policyHash: string): CompletionProvenance {
  const { memoryContext } = input;
  return {
    host: memoryContext.host, providerId: model.provider, modelId: model.id, destinationId: memoryContext.llmDestination.id,
    policyId: memoryContext.policy.id, policyEpoch: memoryContext.policyEpoch ?? 0, policyHash,
    promptRevision: input.promptRevision, invokedAt: new Date().toISOString(),
  };
}

/** Internal bridge path; its exported wrapper turns every unexpected host-shape failure into pending work. */
async function completeMemorySafely(input: CompleteMemoryInput): Promise<MemoryCompletionResult> {
  if (!inputIsBounded(input)) return pending("invalid_input");
  const policyHash = verifiedPolicyHash(input.memoryContext);
  if (policyHash === null) return pending("policy");
  const selected = selectModel(input);
  if (typeof selected === "string") return pending(selected);
  if (!isUsableModel(selected.model)) return pending(isBgeM3(selected.model) ? "unsupported_model" : "no_model");
  if (!hasReplayPermission(input.memoryContext, selected.model)) return pending("cross_provider_disabled");
  if (!destinationBindsSelectedModel(input.memoryContext, selected.model)) return pending("policy");
  if (!contextFitsModel(input, selected.model)) return pending("invalid_input");

  const context = input.memoryContext;
  const registryComplete = Reflect.get(context.modelRegistry, "complete");
  const options = baseOptions(input);
  const outboundContext = envelopeContext(input.envelope);
  const linked = linkedSignal(input.signal, input.timeoutMs);
  try {
    const stopped = linked.reason();
    if (stopped !== undefined) return pending(stopped);
    let outcome: BoundedResult<unknown>;
    if (isCallable(registryComplete)) {
      outcome = await invokeCompletion(registryComplete as HostCompletion, context.modelRegistry, selected.model, outboundContext, options, linked);
    } else {
      // The registry is Pi's only egress path. Namespace fallback belongs solely to Prime.
      if (context.host !== "prime") return pending("no_completion_method");
      const aiNamespace = input.aiNamespace ?? PiAi;
      const completeSimple = Reflect.get(aiNamespace, "completeSimple");
      const getAuth = Reflect.get(context.modelRegistry, "getApiKeyAndHeaders");
      if (!isCallable(completeSimple)) return pending("no_completion_method");
      if (!isCallable(getAuth)) return pending("auth");
      const authResult = await invokeWithinBudget(() => (getAuth as AuthResolver).call(context.modelRegistry, selected.model), linked);
      if (!authResult.ok) return pending(authResult.reason === "failed" ? "auth" : authResult.reason);
      if (!isSuccessfulAuth(authResult.value)) return pending("auth");
      const authenticatedOptions: HostCompletionBaseOptions = {
        ...options,
        ...(typeof authResult.value.apiKey === "string" && authResult.value.apiKey.length > 0 ? { apiKey: authResult.value.apiKey } : {}),
        headers: sanitizeAuthHeaders(authResult.value.headers),
      };
      outcome = await invokeCompletion(completeSimple as HostCompletion, aiNamespace, selected.model, outboundContext, authenticatedOptions, linked);
    }
    if (!outcome.ok) return pending(outcome.reason);
    const text = textFromCompletion(outcome.value);
    if (text === null) return pending("invalid_response");
    if (Buffer.byteLength(text, "utf8") > input.maxOutputTokens) return pending("output_limit");
    return { state: "completed", text, provenance: provenance(input, selected.model, policyHash) };
  } finally { linked.cleanup(); }
}

/** Prefers Pi's registry; Prime fallback is authenticated, bounded, and never aborts a host turn. */
export async function completeMemory(input: CompleteMemoryInput): Promise<MemoryCompletionResult> {
  try { return await completeMemorySafely(input); }
  catch { return pending("failed"); }
}
