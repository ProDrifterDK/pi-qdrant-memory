#!/usr/bin/env node

import { realpathSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { loadConfig as loadRuntimeConfig } from "../config.js";
import type { HostId, RuntimeConfig } from "../types.js";
import { loadAdminProcessSecrets } from "./secrets.js";
import { initializeDestination, type InitializeDestinationResult } from "./init.js";
import { memoryStatus, type MemoryStatus } from "./status.js";

const ADMIN_HOST_ENVIRONMENT = "PI_QDRANT_MEMORY_HOST";
const COMMANDS = ["init", "project", "privacy", "status", "curate", "raptor", "reconcile", "inspect", "forget"] as const;
type ShellCommand = typeof COMMANDS[number];

const TOP_LEVEL_HELP = `Usage: pi-qdrant-memory <command> [options]

Commands:
  init       initialize the configured destination contract
  project    manage operator project identity
  privacy    inspect privacy policy
  status     inspect the destination contract
  curate     inspect curation state
  raptor     inspect RAPTOR state
  reconcile  inspect coordination state
  inspect    inspect memory records
  forget     manage human privacy deletion

Run a command with --help for command-specific options.`;

class CliInputError extends Error {
  constructor() { super("invalid arguments or configuration"); }
}
class CliConfigError extends Error {
  constructor() { super("invalid arguments or configuration"); }
}

export interface CliDependencies {
  env: Record<string, string | undefined>;
  loadConfig(host: HostId): Promise<RuntimeConfig>;
  initialize(config: RuntimeConfig): Promise<InitializeDestinationResult>;
  status(config: RuntimeConfig): Promise<MemoryStatus>;
  writeStdout(value: string): void;
  writeStderr(value: string): void;
}

export interface DefaultCliDependencyOptions {
  env?: Record<string, string | undefined>;
  homeDir?: string;
  readTextFile?(path: string): Promise<string>;
  writeStdout?(value: string): void;
  writeStderr?(value: string): void;
}

export function defaultCliDependencies(options: DefaultCliDependencyOptions = {}): CliDependencies {
  const env = options.env ?? process.env;
  const home = options.homeDir ?? homedir();
  const readTextFile = options.readTextFile ?? ((path: string) => readFile(path, "utf8"));
  const configDependencies = {
    env,
    homeDir: home,
    ...(env.XDG_CONFIG_HOME === undefined || env.XDG_CONFIG_HOME === "" ? {} : { xdgConfigHome: env.XDG_CONFIG_HOME }),
    readTextFile,
  };
  return {
    env,
    loadConfig: host => loadRuntimeConfig(host, configDependencies),
    initialize: config => initializeDestination(config),
    status: config => memoryStatus(config),
    writeStdout: options.writeStdout ?? ((value) => process.stdout.write(value)),
    writeStderr: options.writeStderr ?? ((value) => process.stderr.write(value)),
  };
}

function parseSimpleCommand(args: readonly string[]): { json: boolean; help: boolean } {
  try {
    const parsed = parseArgs({
      args: [...args], strict: true, allowPositionals: false,
      options: { json: { type: "boolean" }, help: { type: "boolean", short: "h" } },
    });
    return { json: parsed.values.json ?? false, help: parsed.values.help ?? false };
  } catch { throw new CliInputError(); }
}

function explicitHost(env: Record<string, string | undefined>): HostId {
  const value = env[ADMIN_HOST_ENVIRONMENT];
  if (value !== "prime" && value !== "pi") throw new CliInputError();
  return value;
}

function output(deps: CliDependencies, json: boolean, command: string, value: Record<string, unknown>): void {
  const projection = { command, ...value };
  deps.writeStdout(json ? `${JSON.stringify(projection)}\n` : `${command}\n${JSON.stringify(value, null, 2)}\n`);
}

function help(deps: CliDependencies, json: boolean, usage: string): void {
  deps.writeStdout(json ? `${JSON.stringify({ usage })}\n` : `${usage}\n`);
}

function requestedJson(args: readonly string[]): boolean { return args.includes("--json"); }
function topLevelHelpRequest(args: readonly string[]): boolean {
  return args.length > 0 && args.every(value => value === "--help" || value === "-h" || value === "--json") && args.some(value => value === "--help" || value === "-h");
}
function isSystemIoError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && typeof (error as { code?: unknown }).code === "string";
}
async function configured(host: HostId, deps: CliDependencies): Promise<RuntimeConfig> {
  try { return await deps.loadConfig(host); }
  catch (error: unknown) { if (isSystemIoError(error)) throw error; throw new CliConfigError(); }
}

async function runCommand(command: ShellCommand, args: readonly string[], deps: CliDependencies): Promise<number> {
  const parsed = parseSimpleCommand(args);
  if (parsed.help) {
    help(deps, parsed.json, command === "init" ? "Usage: pi-qdrant-memory init [--json]" : command === "status" ? "Usage: pi-qdrant-memory status [--json]" : `Usage: pi-qdrant-memory ${command} [--json]`);
    return 0;
  }
  const host = explicitHost(deps.env);
  if (command === "init" || command === "status" || command === "privacy" || command === "forget") {
    // This credential is scoped to this human process and is never config input.
    void loadAdminProcessSecrets(deps.env);
  }
  const config = await configured(host, deps);
  if (command === "init") {
    const result = await deps.initialize(config);
    output(deps, parsed.json, command, result as unknown as Record<string, unknown>);
  } else if (command === "status") {
    const result = await deps.status(config);
    output(deps, parsed.json, command, result as unknown as Record<string, unknown>);
  } else {
    output(deps, parsed.json, command, { host, status: "contract shell" });
  }
  return 0;
}

export async function main(args: readonly string[], deps: CliDependencies = defaultCliDependencies()): Promise<number> {
  const json = requestedJson(args);
  try {
    if (topLevelHelpRequest(args)) { help(deps, json, TOP_LEVEL_HELP); return 0; }
    const command = args[0] as ShellCommand | undefined;
    if (command === undefined || !COMMANDS.includes(command)) throw new CliInputError();
    return await runCommand(command, args.slice(1), deps);
  } catch (error: unknown) {
    const exit = error instanceof CliInputError || error instanceof CliConfigError ? 2 : 1;
    deps.writeStderr(json ? `${JSON.stringify({ error: exit === 2 ? "invalid arguments or configuration" : "operation failed" })}\n` : `${exit === 2 ? "invalid arguments or configuration" : "operation failed"}\n`);
    return exit;
  }
}

function entryPointUrl(value: string | undefined): string {
  if (value === undefined || value === "") return "";
  try { return pathToFileURL(realpathSync(value)).href; }
  catch { return pathToFileURL(value).href; }
}
if (import.meta.url === entryPointUrl(process.argv[1])) process.exitCode = await main(process.argv.slice(2), defaultCliDependencies());
