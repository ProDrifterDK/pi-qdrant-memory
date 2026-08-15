#!/usr/bin/env node

import { realpathSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname } from "node:path";
import { execFile } from "node:child_process";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { loadConfig as loadRuntimeConfig, validateCaptureActivation } from "../config.js";
import type { HostId, RuntimeConfig } from "../types.js";
import { loadAdminProcessSecrets } from "./secrets.js";
import { initializeDestination, validateInitializationDisclosure, type InitializeDestinationResult, type InitializationDisclosure } from "./init.js";
import { memoryStatus, type MemoryStatus } from "./status.js";
import { projectStatusCommand, registerProjectCommand, unregisterProjectCommand, type ProjectCommandDependencies, type ProjectCommandResult } from "./project.js";
import { planPrivacyRevoke, revokePrivacy, type PrivacyRevokePlan, type PrivacyRevokeResult } from "./privacy.js";
import { planForget, runForget as applyForgetPlan, type ForgetPlan, type ForgetPlanInput, type RunForgetResult } from "./forget.js";
import { inspectQdrantRecords, type InspectOptions, type InspectResult } from "./inspect.js";
import { createStoredPlan, productionForgetDependencies, productionOperation, productionPrivacyDependencies } from "./production.js";
import { AdminPlanError } from "./errors.js";

const ADMIN_HOST_ENVIRONMENT = "PI_QDRANT_MEMORY_HOST";
const COMMANDS = ["init", "project", "privacy", "status", "curate", "raptor", "reconcile", "inspect", "forget"] as const;
type ShellCommand = typeof COMMANDS[number];
const OPERATION_COMMANDS = ["curate", "raptor", "reconcile"] as const;
type OperationCommand = typeof OPERATION_COMMANDS[number];

export const TOP_LEVEL_HELP = `Usage: pi-qdrant-memory <command> [options]

Commands:
  init       initialize the configured destination contract
  project    register, unregister, or inspect operator project identity
  privacy    revoke a destination privacy epoch
  status     inspect the redacted destination audit
  curate     enqueue or wait for human-triggered curation
  raptor     enqueue or wait for a RAPTOR rebuild
  reconcile  enqueue or wait for coverage reconciliation
  inspect    inspect bounded redacted record metadata
  forget     plan or apply a human-approved privacy forget operation

Run a command with --help for command-specific options.`;

const COMMAND_HELP: Record<ShellCommand, string> = {
  init: "Usage: pi-qdrant-memory init [--retention <days|indefinite>] [--egress <local_only|allowlist>] --confirm [--json]",
  project: "Usage: pi-qdrant-memory project register --path <path> --alias <id> --confirm | project unregister --alias <id> --confirm | project status [--path <path>]",
  privacy: "Usage: pi-qdrant-memory privacy revoke [--plan [<plan-id>]] [--approve <plan-id>] [--destination <id>] [--json]",
  status: "Usage: pi-qdrant-memory status [--json]",
  curate: "Usage: pi-qdrant-memory curate --enqueue|--wait [--job <id>] [--json]",
  raptor: "Usage: pi-qdrant-memory raptor rebuild --enqueue|--wait [--job <id>] [--json]",
  reconcile: "Usage: pi-qdrant-memory reconcile --enqueue|--wait [--job <id>] [--json]",
  inspect: "Usage: pi-qdrant-memory inspect [--id <id>] [--type <record-type>] [--limit <n>] [--json]",
  forget: "Usage: pi-qdrant-memory forget [--scope occurrence|content|state] [selector] [--plan <plan-id>] [--approve <plan-id>] [--json]",
};

export class CliInputError extends Error { constructor(message = "invalid arguments or configuration") { super(message); this.name = "CliInputError"; } }
export class CliConfigError extends Error { constructor(message = "invalid arguments or configuration") { super(message); this.name = "CliConfigError"; } }
export class CliApprovalError extends CliInputError { constructor(message = "approval does not match plan") { super(message); this.name = "CliApprovalError"; } }

export interface CliOperationRequest { command: OperationCommand; action: "enqueue" | "wait"; jobId?: string; }
export interface CliDependencies {
  env: Record<string, string | undefined>;
  cwd?: string;
  loadConfig(host: HostId): Promise<RuntimeConfig>;
  initialize(config: RuntimeConfig): Promise<InitializeDestinationResult>;
  status(config: RuntimeConfig): Promise<MemoryStatus>;
  projectRegister?(input: Parameters<typeof registerProjectCommand>[0], config: RuntimeConfig): Promise<ProjectCommandResult>;
  projectUnregister?(input: Parameters<typeof unregisterProjectCommand>[0], config: RuntimeConfig): Promise<ProjectCommandResult>;
  projectStatus?(path: string, config: RuntimeConfig): Promise<ProjectCommandResult>;
  privacyPlan?(config: RuntimeConfig, input: Parameters<typeof planPrivacyRevoke>[0]): Promise<PrivacyRevokePlan> | PrivacyRevokePlan;
  privacyApply?(config: RuntimeConfig, input: { plan: PrivacyRevokePlan; approvedPlanId: string; signal?: AbortSignal }): Promise<PrivacyRevokeResult>;
  privacyLoadPlan?(config: RuntimeConfig, planId: string): Promise<PrivacyRevokePlan>;
  forgetPlan?(config: RuntimeConfig, input: ForgetPlanInput): Promise<ForgetPlan>;
  forgetLoadPlan?(config: RuntimeConfig, planId: string): Promise<ForgetPlan>;
  forgetApply?(config: RuntimeConfig, input: { plan: ForgetPlan; approvedPlanId: string; signal?: AbortSignal }): Promise<RunForgetResult>;
  inspect?(config: RuntimeConfig, options: InspectOptions): Promise<InspectResult>;
  operate?(config: RuntimeConfig, request: CliOperationRequest): Promise<Record<string, unknown>>;
  writeStdout(value: string): void;
  writeStderr(value: string): void;
}

export interface DefaultCliDependencyOptions {
  env?: Record<string, string | undefined>;
  homeDir?: string;
  cwd?: string;
  readTextFile?(path: string): Promise<string>;
  writeTextFile?(path: string, text: string): Promise<void>;
  writeStdout?(value: string): void;
  writeStderr?(value: string): void;
}

function projectDeps(home: string, env: Record<string, string | undefined>, readTextFile: (path: string) => Promise<string>, writeTextFile: (path: string, text: string) => Promise<void>): ProjectCommandDependencies {
  const gitTopLevel = (cwd: string): Promise<string> => new Promise((resolve, reject) => execFile("git", ["-C", cwd, "rev-parse", "--show-toplevel"], { encoding: "utf8", shell: false }, (error, stdout) => error === null ? resolve(String(stdout).trim()) : reject(error)));
  const canonicalize = async (path: string): Promise<string> => realpathSync(path);
  return { gitTopLevel, canonicalize, homeDir: home, ...(env.XDG_CONFIG_HOME === undefined || env.XDG_CONFIG_HOME === "" ? {} : { xdgConfigHome: env.XDG_CONFIG_HOME }), readTextFile, writeTextFile, operator: true };
}

export function defaultCliDependencies(options: DefaultCliDependencyOptions = {}): CliDependencies {
  const env = options.env ?? process.env;
  const home = options.homeDir ?? homedir();
  const readTextFile = options.readTextFile ?? ((path: string) => readFile(path, "utf8"));
  const writeTextFile = options.writeTextFile ?? (async (path: string, text: string) => {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await writeFile(path, text, { encoding: "utf8", mode: 0o600 });
  });
  const configDependencies = { env, homeDir: home, ...(env.XDG_CONFIG_HOME === undefined || env.XDG_CONFIG_HOME === "" ? {} : { xdgConfigHome: env.XDG_CONFIG_HOME }), readTextFile };
  const makeProjectDeps = () => projectDeps(home, env, readTextFile, writeTextFile);
  const planStorage = (config: RuntimeConfig) => options.writeTextFile === undefined
    ? createStoredPlan(config)
    : createStoredPlan(config, { readTextFile, writeTextFile });
  const assertAdmin = (): void => { if (loadAdminProcessSecrets(env).destinationApiKey === undefined) throw new CliConfigError("Human Qdrant admin key is required for this operation"); };
  const reconcile = async (config: RuntimeConfig): Promise<void> => { assertAdmin(); await productionOperation(config, env, { command: "reconcile", action: "enqueue" }); };
  return {
    env, cwd: options.cwd ?? process.cwd(),
    loadConfig: host => loadRuntimeConfig(host, configDependencies),
    initialize: config => {
      const adminKey = loadAdminProcessSecrets(env).destinationApiKey;
      if (adminKey === undefined) throw new CliConfigError("Human Qdrant admin key is required for CLI initialization");
      return initializeDestination(config, { adminApiKey: adminKey });
    },
    status: config => memoryStatus(config, { fetchImpl: globalThis.fetch }),
    projectRegister: (input) => registerProjectCommand(input, makeProjectDeps()),
    projectUnregister: (input) => unregisterProjectCommand(input, makeProjectDeps()),
    projectStatus: (path) => projectStatusCommand(path, makeProjectDeps()),
    privacyPlan: async (config, input) => {
      assertAdmin();
      const { deps } = productionPrivacyDependencies(config, env, () => reconcile(config));
      const control = await deps.readControl!();
      const plan = planPrivacyRevoke({ ...input, ownerHost: config.host, control });
      await planStorage(config).save("privacy", plan);
      return plan;
    },
    privacyLoadPlan: async (config, planId) => planStorage(config).load<PrivacyRevokePlan>("privacy", planId),
    privacyApply: async (config, input) => {
      assertAdmin();
      const { deps } = productionPrivacyDependencies(config, env, () => reconcile(config));
      return revokePrivacy(input, deps);
    },
    forgetPlan: async (config, input) => {
      assertAdmin();
      const { deps } = productionForgetDependencies(config, env, () => reconcile(config));
      const plan = await planForget({ ...input, ownerHost: config.host, ...(deps.resolveCurrent === undefined ? {} : { resolveCurrent: deps.resolveCurrent }) });
      await planStorage(config).save("forget", plan);
      return plan;
    },
    forgetLoadPlan: async (config, planId) => planStorage(config).load<ForgetPlan>("forget", planId),
    forgetApply: async (config, input) => {
      assertAdmin();
      const { deps } = productionForgetDependencies(config, env, () => reconcile(config));
      return applyForgetPlan(input, deps);
    },
    inspect: (config, inspectOptions) => inspectQdrantRecords(config, inspectOptions, globalThis.fetch),
    operate: (config, request) => { assertAdmin(); return productionOperation(config, env, request); },
    writeStdout: options.writeStdout ?? ((value) => process.stdout.write(value)),
    writeStderr: options.writeStderr ?? ((value) => process.stderr.write(value)),
  };
}

interface ParsedBase { json: boolean; help: boolean; }
function parseWithOptions(args: readonly string[], options: Record<string, { type: "boolean" | "string"; short?: string }>, allowPositionals = false): { values: Record<string, unknown>; positionals: string[] } {
  try {
    const parsed = parseArgs({ args: [...args], strict: true, allowPositionals, options });
    return { values: parsed.values as Record<string, unknown>, positionals: parsed.positionals };
  } catch { throw new CliInputError(); }
}
function baseOptions(): Record<string, { type: "boolean" | "string"; short?: string }> { return { json: { type: "boolean" }, help: { type: "boolean", short: "h" } }; }
function base(values: Record<string, unknown>): ParsedBase { return { json: values.json === true, help: values.help === true }; }
function explicitHost(env: Record<string, string | undefined>): HostId { const value = env[ADMIN_HOST_ENVIRONMENT]; if (value !== "prime" && value !== "pi") throw new CliInputError(); return value; }
function requestedJson(args: readonly string[]): boolean { return args.includes("--json"); }
function topLevelHelpRequest(args: readonly string[]): boolean { return args.length > 0 && args.every(value => value === "--help" || value === "-h" || value === "--json") && args.some(value => value === "--help" || value === "-h"); }
async function configured(host: HostId, deps: CliDependencies): Promise<RuntimeConfig> { try { return await deps.loadConfig(host); } catch { throw new CliConfigError(); } }
function safeOutputValue(value: unknown, key = ""): unknown {
  if (/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(key)) return "[redacted]";
  if (/(?:^|_)(?:payload|vector|prompt|completion|tool_args|tool_result|raw)$/iu.test(key)) return "[redacted]";
  if (Array.isArray(value)) return value.slice(0, 1024).map(item => safeOutputValue(item));
  if (typeof value === "object" && value !== null) return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([childKey, child]) => [childKey, safeOutputValue(child, childKey)]));
  if (typeof value === "string" && value.length > 16000) return `${value.slice(0, 16000)}…`;
  return value;
}
function output(deps: CliDependencies, json: boolean, command: string, value: unknown): void { const safe = safeOutputValue(value) as Record<string, unknown>; deps.writeStdout(json ? `${JSON.stringify({ command, ...safe })}\n` : `${command}\n${JSON.stringify(safe, null, 2)}\n`); }
function help(deps: CliDependencies, json: boolean, usage: string): void { deps.writeStdout(json ? `${JSON.stringify({ usage })}\n` : `${usage}\n`); }
function requireString(value: unknown, name: string): string { if (typeof value !== "string" || value.length === 0 || value.length > 512 || /[\u0000-\u001f\u007f-\u009f]/u.test(value)) throw new CliInputError(`invalid ${name}`); return value; }
function positiveLimit(value: unknown): number { if (value === undefined) return 64; const parsed = Number(value); if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 256) throw new CliInputError(); return parsed; }
function retention(value: unknown): InitializationDisclosure["retention"] { if (value === "indefinite") return value; const parsed = Number(value); if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 3650) throw new CliInputError(); return parsed; }

async function runInit(args: readonly string[], deps: CliDependencies, config: RuntimeConfig): Promise<number> {
  const parsed = parseWithOptions(args, { ...baseOptions(), retention: { type: "string" }, egress: { type: "string" }, confirm: { type: "boolean" } });
  const b = base(parsed.values); if (b.help) { help(deps, b.json, COMMAND_HELP.init); return 0; }
  const disclosure: InitializationDisclosure | undefined = parsed.values.retention === undefined && parsed.values.egress === undefined && parsed.values.confirm !== true ? undefined : { retention: retention(parsed.values.retention ?? config.capture.episodeRetentionDays), egressMode: parsed.values.egress === "local_only" || parsed.values.egress === "allowlist" ? parsed.values.egress : parsed.values.egress === undefined ? config.privacy.egressMode : (() => { throw new CliInputError(); })(), confirmed: parsed.values.confirm === true };
  try {
    validateCaptureActivation(config, disclosure);
    validateInitializationDisclosure(config, disclosure);
  } catch (error: unknown) {
    throw new CliInputError(error instanceof Error ? error.message : "capture disclosure is invalid");
  }
  const result = await deps.initialize(config); output(deps, b.json, "init", result); return 0;
}

async function runProject(args: readonly string[], deps: CliDependencies, config: RuntimeConfig): Promise<number> {
  const parsed = parseWithOptions(args, { ...baseOptions(), path: { type: "string" }, alias: { type: "string" }, confirm: { type: "boolean" } }, true);
  const b = base(parsed.values); if (b.help) { help(deps, b.json, COMMAND_HELP.project); return 0; }
  const subcommand = parsed.positionals[0]; if (subcommand === "register") {
    if (parsed.positionals.length !== 1 || deps.projectRegister === undefined || parsed.values.path === undefined || parsed.values.alias === undefined) throw new CliInputError();
    output(deps, b.json, "project", await deps.projectRegister({ path: requireString(parsed.values.path, "path"), alias: requireString(parsed.values.alias, "alias"), confirmed: parsed.values.confirm === true }, config)); return 0;
  }
  if (subcommand === "unregister") {
    if (parsed.positionals.length !== 1 || deps.projectUnregister === undefined || parsed.values.alias === undefined) throw new CliInputError();
    output(deps, b.json, "project", await deps.projectUnregister({ alias: requireString(parsed.values.alias, "alias"), confirmed: parsed.values.confirm === true }, config)); return 0;
  }
  if (subcommand === "status") {
    if (parsed.positionals.length !== 1 || deps.projectStatus === undefined) throw new CliInputError();
    output(deps, b.json, "project", await deps.projectStatus(requireString((parsed.values.path as string | undefined) ?? deps.cwd ?? process.cwd(), "path"), config)); return 0;
  }
  throw new CliInputError();
}

async function runPrivacy(args: readonly string[], deps: CliDependencies, config: RuntimeConfig): Promise<number> {
  const normalizedArgs = [...args];
  const inlinePlanIndex = normalizedArgs.findIndex(argument => argument.startsWith("--plan="));
  if (inlinePlanIndex >= 0) normalizedArgs[inlinePlanIndex] = `--plan-id=${normalizedArgs[inlinePlanIndex]!.slice("--plan=".length)}`;
  const planIndex = normalizedArgs.indexOf("--plan");
  if (planIndex >= 0 && normalizedArgs[planIndex + 1] !== undefined && !String(normalizedArgs[planIndex + 1]).startsWith("-")) {
    normalizedArgs.splice(planIndex, 2, `--plan-id=${normalizedArgs[planIndex + 1]}`);
  }
  const parsed = parseWithOptions(normalizedArgs, { ...baseOptions(), plan: { type: "boolean" }, "plan-id": { type: "string" }, approve: { type: "string" }, destination: { type: "string" }, destinations: { type: "string" }, reason: { type: "string" } }, true);
  const b = base(parsed.values); if (b.help) { help(deps, b.json, COMMAND_HELP.privacy); return 0; }
  if (parsed.positionals.length !== 1 || parsed.positionals[0] !== "revoke" || deps.privacyPlan === undefined) throw new CliInputError();
  const suppliedPlan = parsed.values["plan-id"] === undefined ? undefined : requireString(parsed.values["plan-id"], "plan");
  const destinationValues = [parsed.values.destination, parsed.values.destinations].filter((value): value is string => typeof value === "string").flatMap(value => value.split(",").map(item => item.trim()).filter(Boolean));
  const privacyInput = { ownerHost: config.host, currentPrivacyEpoch: 0, ...(destinationValues.length === 0 ? {} : { destinationIds: destinationValues }), ...(parsed.values.reason === undefined ? {} : { reason: requireString(parsed.values.reason, "reason") }) };
  const approvalValue = parsed.values.approve === undefined ? undefined : requireString(parsed.values.approve, "approval");
  const loadPlanId = suppliedPlan ?? approvalValue;
  const plan = loadPlanId !== undefined && deps.privacyLoadPlan !== undefined ? await deps.privacyLoadPlan(config, loadPlanId) : await deps.privacyPlan(config, privacyInput);
  if (suppliedPlan !== undefined && suppliedPlan !== plan.id) throw new CliApprovalError("privacy plan does not match current state");
  if (parsed.values.approve === undefined) { output(deps, b.json, "privacy", plan); return 0; }
  if (deps.privacyApply === undefined) throw new CliInputError();
  output(deps, b.json, "privacy", await deps.privacyApply(config, { plan, approvedPlanId: approvalValue! })); return 0;
}

async function runOperation(command: OperationCommand, args: readonly string[], deps: CliDependencies, config: RuntimeConfig): Promise<number> {
  const parsed = parseWithOptions(args, { ...baseOptions(), enqueue: { type: "boolean" }, wait: { type: "boolean" }, job: { type: "string" } }, true);
  const b = base(parsed.values); if (b.help) { help(deps, b.json, COMMAND_HELP[command]); return 0; }
  if (deps.operate === undefined || parsed.positionals.length > (command === "raptor" ? 1 : 0) || command === "raptor" && parsed.positionals[0] !== "rebuild" || parsed.values.enqueue === parsed.values.wait) throw new CliInputError();
  const action = parsed.values.enqueue === true ? "enqueue" : "wait";
  const jobId = parsed.values.job === undefined ? undefined : requireString(parsed.values.job, "job");
  output(deps, b.json, command, await deps.operate(config, { command, action, ...(jobId === undefined ? {} : { jobId }) })); return 0;
}

async function runInspect(args: readonly string[], deps: CliDependencies, config: RuntimeConfig): Promise<number> {
  const parsed = parseWithOptions(args, { ...baseOptions(), id: { type: "string" }, type: { type: "string" }, limit: { type: "string" } });
  const b = base(parsed.values); if (b.help) { help(deps, b.json, COMMAND_HELP.inspect); return 0; }
  if (deps.inspect === undefined) throw new CliInputError();
  const ids = parsed.values.id === undefined ? undefined : [requireString(parsed.values.id, "id")];
  const recordTypes = parsed.values.type === undefined ? undefined : [requireString(parsed.values.type, "record type")];
  output(deps, b.json, "inspect", await deps.inspect(config, { ...(ids === undefined ? {} : { ids }), ...(recordTypes === undefined ? {} : { recordTypes }), limit: positiveLimit(parsed.values.limit) })); return 0;
}

async function runForget(args: readonly string[], deps: CliDependencies, config: RuntimeConfig): Promise<number> {
  const parsed = parseWithOptions(args, { ...baseOptions(), scope: { type: "string" }, plan: { type: "string" }, approve: { type: "string" }, current: { type: "string" }, observation: { type: "string" }, episode: { type: "string" }, content: { type: "string" }, state: { type: "string" } });
  const b = base(parsed.values); if (b.help) { help(deps, b.json, COMMAND_HELP.forget); return 0; }
  if (deps.forgetPlan === undefined) throw new CliInputError();
  const selectors = ["current", "observation", "episode", "content", "state"] as const;
  const selected = selectors.filter(key => parsed.values[key] !== undefined);
  if (selected.length > 1) throw new CliInputError();
  const selection = selected.length === 0 ? undefined : selected[0] === "current" ? { curatedCurrentId: requireString(parsed.values.current, "current") } : selected[0] === "observation" ? { observationId: requireString(parsed.values.observation, "observation") } : selected[0] === "episode" ? { episodeId: requireString(parsed.values.episode, "episode") } : selected[0] === "content" ? { contentId: requireString(parsed.values.content, "content") } : { stateKey: requireString(parsed.values.state, "state") };
  const approvalValue = parsed.values.approve === undefined ? undefined : requireString(parsed.values.approve, "approval");
  const planId = parsed.values.plan === undefined ? approvalValue : requireString(parsed.values.plan, "plan");
  if (planId === undefined && selection === undefined) throw new CliInputError();
  const scope = parsed.values.scope === undefined ? undefined : parsed.values.scope === "occurrence" || parsed.values.scope === "content" || parsed.values.scope === "state" ? parsed.values.scope : (() => { throw new CliInputError(); })();
  const plan = planId !== undefined && selection === undefined && deps.forgetLoadPlan !== undefined
    ? await deps.forgetLoadPlan(config, planId)
    : await deps.forgetPlan(config, { ...(selection === undefined ? { selection: { targetId: planId! } } : { selection }), ...(scope === undefined ? {} : { scope }) });
  if (planId !== undefined && planId !== plan.id) throw new CliApprovalError("forget plan does not match current state");
  if (parsed.values.approve === undefined) { output(deps, b.json, "forget", plan); return 0; }
  if (deps.forgetApply === undefined) throw new CliInputError();
  output(deps, b.json, "forget", await deps.forgetApply(config, { plan, approvedPlanId: approvalValue! })); return 0;
}

async function runCommand(command: ShellCommand, args: readonly string[], deps: CliDependencies): Promise<number> {
  if (args.includes("--help") || args.includes("-h")) { help(deps, requestedJson(args), COMMAND_HELP[command]); return 0; }
  if (command === "status" || command === "init" || command === "project" || command === "privacy" || command === "curate" || command === "raptor" || command === "reconcile" || command === "inspect" || command === "forget") {
    const host = explicitHost(deps.env); const config = await configured(host, deps);
    if (command === "status") { const parsed = parseWithOptions(args, baseOptions()); const b = base(parsed.values); output(deps, b.json, command, await deps.status(config)); return 0; }
    if (command === "init") return runInit(args, deps, config);
    if (command === "project") return runProject(args, deps, config);
    if (command === "privacy") return runPrivacy(args, deps, config);
    if (command === "inspect") return runInspect(args, deps, config);
    if (command === "forget") return runForget(args, deps, config);
    return runOperation(command, args, deps, config);
  }
  throw new CliInputError();
}

export async function main(args: readonly string[], deps: CliDependencies = defaultCliDependencies()): Promise<number> {
  const json = requestedJson(args);
  try {
    if (topLevelHelpRequest(args)) { help(deps, json, TOP_LEVEL_HELP); return 0; }
    const command = args[0] as ShellCommand | undefined;
    if (command === undefined || !COMMANDS.includes(command)) throw new CliInputError();
    return await runCommand(command, args.slice(1), deps);
  } catch (error: unknown) {
    const exit = error instanceof CliInputError || error instanceof CliConfigError || error instanceof AdminPlanError ? 2 : 1;
    deps.writeStderr(json ? `${JSON.stringify({ error: exit === 2 ? "invalid arguments or configuration" : "operation failed" })}\n` : `${exit === 2 ? "invalid arguments or configuration" : "operation failed"}\n`);
    return exit;
  }
}

function entryPointUrl(value: string | undefined): string { if (value === undefined || value === "") return ""; try { return pathToFileURL(realpathSync(value)).href; } catch { return pathToFileURL(value).href; } }
if (import.meta.url === entryPointUrl(process.argv[1])) process.exitCode = await main(process.argv.slice(2), defaultCliDependencies());
