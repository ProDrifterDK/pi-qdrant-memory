#!/usr/bin/env node
import type { HostId, RuntimeConfig } from "../types.js";
import { type InitializeDestinationResult } from "./init.js";
import { type MemoryStatus } from "./status.js";
import { registerProjectCommand, unregisterProjectCommand, type ProjectCommandResult } from "./project.js";
import { planPrivacyRevoke, type PrivacyRevokePlan, type PrivacyRevokeResult } from "./privacy.js";
import { type ForgetPlan, type ForgetPlanInput, type RunForgetResult } from "./forget.js";
import { type InspectOptions, type InspectResult } from "./inspect.js";
declare const OPERATION_COMMANDS: readonly ["curate", "raptor", "reconcile"];
type OperationCommand = typeof OPERATION_COMMANDS[number];
export declare const TOP_LEVEL_HELP = "Usage: pi-qdrant-memory <command> [options]\n\nCommands:\n  init       initialize the configured destination contract\n  project    register, unregister, or inspect operator project identity\n  privacy    revoke a destination privacy epoch\n  status     inspect the redacted destination audit\n  curate     enqueue or wait for human-triggered curation\n  raptor     enqueue or wait for a RAPTOR rebuild\n  reconcile  enqueue or wait for coverage reconciliation\n  inspect    inspect bounded redacted record metadata\n  forget     plan or apply a human-approved privacy forget operation\n\nRun a command with --help for command-specific options.";
export declare class CliInputError extends Error {
    constructor(message?: string);
}
export declare class CliConfigError extends Error {
    constructor(message?: string);
}
export declare class CliApprovalError extends CliInputError {
    constructor(message?: string);
}
export interface CliOperationRequest {
    command: OperationCommand;
    action: "enqueue" | "wait";
    jobId?: string;
}
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
    privacyApply?(config: RuntimeConfig, input: {
        plan: PrivacyRevokePlan;
        approvedPlanId: string;
        signal?: AbortSignal;
    }): Promise<PrivacyRevokeResult>;
    privacyLoadPlan?(config: RuntimeConfig, planId: string): Promise<PrivacyRevokePlan>;
    forgetPlan?(config: RuntimeConfig, input: ForgetPlanInput): Promise<ForgetPlan>;
    forgetLoadPlan?(config: RuntimeConfig, planId: string): Promise<ForgetPlan>;
    forgetApply?(config: RuntimeConfig, input: {
        plan: ForgetPlan;
        approvedPlanId: string;
        signal?: AbortSignal;
    }): Promise<RunForgetResult>;
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
export declare function defaultCliDependencies(options?: DefaultCliDependencyOptions): CliDependencies;
export declare function main(args: readonly string[], deps?: CliDependencies): Promise<number>;
export {};
