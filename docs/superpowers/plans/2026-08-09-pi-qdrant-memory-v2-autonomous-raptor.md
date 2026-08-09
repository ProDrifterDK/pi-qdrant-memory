# Pi Qdrant Memory v2 Autonomous RAPTOR Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the v1 read-only/Hermes-import package with `v2.0.0`: autonomous, redacted, host-private episode capture; durable at-least-once ingest; distributed curation and RAPTOR generations; guarded hybrid retrieval; human-only privacy/forget operations; and exact Pi/Prime compatibility.

**Architecture:** One TypeScript ESM Pi extension and human-operated CLI talk directly to Qdrant REST 1.17.1, embeddings, and host model registries. Lifecycle capture scans persisted `sessionManager.getEntries()` at `agent_end`, `session_before_compact`, and `session_shutdown` after a persisted activation cutoff. Redacted immutable episodes flow through per-process outboxes into host-owned records; root workers coordinate leases/jobs and publish temporal curation and content-addressed RAPTOR generations by CAS. Retrieval is multi-lane, policy-filtered, evidence-descended, and injected only as ephemeral untrusted context.

**Tech Stack:** Node.js 20+ with npm >=11.10, TypeScript NodeNext ESM, native `fetch`, `typebox`, Vitest, Qdrant REST 1.17+ (integration pinned to 1.17.1), `umap-js@1.4.0` with the tarball Apache-2.0 notice retained, and the statically imported Pi AI model registry / pinned Prime fallback bridge. No Python, daemon, SDK, live Qdrant, npm publish, or tag operation is part of implementation planning.

## Global Constraints

- The complete approved specification is `docs/superpowers/specs/2026-08-09-pi-qdrant-memory-v2-autonomous-raptor-design.md`; this plan may narrow implementation order but must not weaken that contract.
- v2 supports both Pi `@earendil-works/pi-coding-agent@0.84.1` and Prime Agent commit `a18809e00ea30638584d87b3afea7285a9d7296c`; `compatibility.json` is schema `2` and keeps those exact minimum/latest-tested pins until a successor is actually tested.
- Use Node.js 20+ and npm >=11.10. Later implementation in a fresh feature worktree begins with `npm ci`; do not install dependencies from this planning worktree.
- Do not introduce Python, a daemon, sidecar, coordinator database, Qdrant/embedding/LLM SDK, dynamic imports, inline imports, live Qdrant access, npm publication, release tag, or live initialization. Use REST and host APIs only.
- Remove all executable Hermes/source surface before implementing v2: no `import-hermes` command/module, Hermes source contract, source config, source credentials, source client, tests, docs, package export, or `dist` artifact remains.
- Qdrant minimum is 1.17.0 and verification is exact 1.17.1. Each host gets a distinct default collection (`pi_memory` or `prime_memory`) with immutable `owner_host`, schema `pi-qdrant-memory-v2`, named `semantic` 1024/Cosine vector, required payload indexes, and defensive `owner_host`/status/secret/expiry/policy filters.
- Configuration is user XDG-only with allowlisted environment overrides; repository contents, origin spoofing, model arguments, endpoints, collection names, credentials, and secret values never select authorization. Capture defaults off and activation requires explicit retention plus egress disclosure/confirmation.
- Credentials are environment-only (collection-scoped Qdrant session keys, separate admin key for human init, embedding key); model credentials come from the host registry. `local_only` and exact allowlists, residency/data-use labels, cross-provider replay, producer-policy intersections, privacy epochs, tombstones, and retention deadlines are enforced before every egress.
- Every stored/derived record is immutable unless the contract explicitly names a single-point OCC/CAS view/control update. IDs, hashes, insert-only/update-only modes, read-back verification, strong control writes, fencing tokens, policy epochs, tombstones, and final batch checks provide idempotence without claiming cross-point transactions or exactly-once LLM calls.
- Captured content is selected from persisted `getEntries()` only after the activation cutoff, excludes system/developer/custom/injected/thinking/partial content and full tool output, redacts before disk/embedding/Qdrant/LLM, bounds output, and never marks scanner failure as passed. Both exact lifecycle hosts and exact event names are required; `agent_settled` is never used.
- Children may emit tagged episode leaves and project-only search, but never auto-recall, curating, or RAPTOR. Only a root with valid host/depth markers and a current lease may perform root work. Memory is data with `<memory-context trust="untrusted">`, never authority, and auto-recall is ephemeral/fail-open.
- The LLM bridge must use one static `import * as PiAi` from the package root; it must type-guard `Reflect.get(ctx.modelRegistry, "complete")` first for Pi 0.84.1, and otherwise type-guard `Reflect.get(PiAi, "completeSimple")` for the pinned Prime root bridge after `ctx.modelRegistry.getApiKeyAndHeaders(model)`. It must never statically access either completion property, import a host auth type, use dynamic/inline imports, or route memory generation through BGE-M3 embeddings.
- RAPTOR uses `umap-js@1.4.0` with its Apache-2.0 tarball LICENSE/notice, injected xoshiro128** PRNG, deterministic diagonal GMM EM+BIC in TypeScript, soft memberships, bounded recursive summaries, immutable manifests/generations, and atomic control-point CAS publication with evidence descent.
- Every task is TDD and reviewer-gated: write a concrete red test, observe the named failure, implement the smallest contract, run the named green test, build, stage exact files including `dist`, run `npm run check`, then use the listed conventional commit. A task is not complete until an independent reviewer confirms its interfaces and tests.
- `dist/` is committed and must match source. The final release-prep task stops before `npm publish`, tag creation, live Qdrant init, or live activation; it only documents those later operational steps.

## Locked File Map

The file map is fixed. Existing v1 modules are rewritten or deleted only as listed; no implementation task may invent a competing module or hidden persistence path.

```text
.gitignore
LICENSE
README.md
compatibility.json
package.json
package-lock.json
tsconfig.json
vitest.config.ts
.github/workflows/ci.yml
docs/
  configuration.md
  security.md
  superpowers/
    specs/2026-08-09-pi-qdrant-memory-v2-autonomous-raptor-design.md
    plans/2026-08-08-pi-qdrant-memory-v1.md
    plans/2026-08-09-pi-qdrant-memory-v2-autonomous-raptor.md
src/
  types.ts
  config.ts
  host.ts
  project.ts
  query.ts
  cache.ts
  format.ts
  service.ts
  tool.ts
  extension.ts
  domain/
    canonical.ts
    ids.ts
    records.ts
    policy.ts
  security/
    redaction.ts
    egress.ts
  capture/
    select.ts
    episode.ts
    scanner.ts
  outbox/
    store.ts
    delivery.ts
  qdrant/
    client.ts
    schema.ts
    write.ts
  coordination/
    control.ts
    leases.ts
    jobs.ts
    reconcile.ts
    tombstones.ts
  curation/
    llm.ts
    prompt.ts
    validate.ts
    temporal.ts
    worker.ts
  raptor/
    random.ts
    umap.ts
    gmm.ts
    cluster.ts
    manifest.ts
    builder.ts
    publication.ts
  clients/
    http.ts
    embeddings.ts
  retrieval/
    filters.ts
    merge.ts
    search.ts
  admin/
    cli.ts
    secrets.ts
    init.ts
    status.ts
    project.ts
    privacy.ts
    forget.ts
    inspect.ts
  vendor/
    umap-license-apache-2.0.txt
  (delete src/admin/hermes-contract.ts, src/admin/import-hermes.ts, src/admin/import-plan.ts,
   src/admin/qdrant-admin.ts, src/admin/secret-scan.ts, src/clients/qdrant-readonly.ts,
   and src/admin/canonical.ts)
tests/
  unit/
    config.test.ts
    host.test.ts
    project.test.ts
    policy.test.ts
    records.test.ts
    ids.test.ts
    redaction.test.ts
    capture.test.ts
    outbox.test.ts
    qdrant.test.ts
    admin-init-status.test.ts
    clients.test.ts
    ingest.test.ts
    coordination.test.ts
    curation.test.ts
    temporal.test.ts
    llm.test.ts
    raptor.test.ts
    manifests.test.ts
    query.test.ts
    cache.test.ts
    format.test.ts
    tool.test.ts
    service.test.ts
    extension.test.ts
    retrieval.test.ts
    admin.test.ts
    no-hermes.test.ts
    admin-client.test.ts (delete in Task 3)
    secret-scan.test.ts (delete in Task 4)
  integration/
    embedding-stub.ts
    qdrant.test.ts (delete in Task 1; recreate in Task 14)
    qdrant-concurrency.test.ts
    qdrant-fixtures.ts
  compat/
    run-host-smoke.mjs
    host-fixtures.mjs
    run-isolated-smokes.sh
```

The existing v1 `src/*`, tests, docs, and CI are the starting point; tasks below state whether each file is rewritten, created, or deleted. The lockfile is updated by the implementation worker after `npm ci` in the fresh worktree. `dist/` is generated and committed after every build checkpoint.


## Implementation Bootstrap (before Task 1)

This bootstrap is performed later by the implementation worker, after this plan is reviewed and committed, in a fresh feature worktree. It is not a task in the 15-task count and is not executed while writing this plan.

- [ ] **Bootstrap Step 1 (3 min): Create the clean implementation worktree**

```bash
git worktree add ../pi-qdrant-memory-v2-implementation -b feature/v2-autonomous-raptor
cd ../pi-qdrant-memory-v2-implementation
npm --version
```

Expected: npm reports `11.10.0` or newer. Do not install in the planning worktree.

- [ ] **Bootstrap Step 2 (4 min): Restore the committed v1 baseline before edits**

```bash
npm ci
npm run typecheck
npm test
npm run build
git diff --exit-code -- dist
```

Expected: npm ci, v1 unit tests, typecheck, build, and committed-dist check exit 0. This baseline proves the fresh worktree before Task 1 intentionally removes v1 Hermes behavior.

- [ ] **Bootstrap Step 3 (2 min): Confirm the implementation boundary**

No Python, daemon, sidecar, SDK, live Qdrant request, paid API, npm publication, release tag, or live initialization is allowed during implementation. Isolated Qdrant 1.17.1 and exact-host smokes occur only in their later dedicated tasks.

### Task 1: Break the v1 Contract, Remove Hermes, and Establish the V2 Build Shell

**Files:**
- Modify: `package.json`, `package-lock.json`, `compatibility.json`, `README.md`, `docs/configuration.md`, `docs/security.md`, `.github/workflows/ci.yml`, `src/types.ts`, `src/config.ts`, `src/admin/cli.ts`, `src/admin/init.ts`, `src/admin/status.ts`, `tests/unit/config.test.ts`, `tests/unit/admin-init-status.test.ts`, `tests/unit/service.test.ts`
- Create: `src/admin/secrets.ts`
- Create: `tests/unit/no-hermes.test.ts`
- Delete: `src/admin/hermes-contract.ts`, `src/admin/import-hermes.ts`, `src/admin/import-plan.ts`, `tests/unit/hermes-contract.test.ts`, `tests/unit/import-hermes.test.ts`, `tests/unit/import-plan.test.ts`, `tests/integration/qdrant.test.ts`, and generated `dist/admin/hermes-contract.*`, `dist/admin/import-hermes.*`, `dist/admin/import-plan.*` artifacts

**Interfaces:**
- **Consumes:** the v1 package metadata, v1 config/source imports, v1 admin command parser, and the approved v2 schema/compatibility decision.

- **Produces:** a buildable package with no Hermes executable/source/config/credential surface; compatibility schema 2 with exact host/Qdrant pins; one transitional-but-complete `RuntimeConfig`; and a CLI help/status/init shell with no retired command.

- [ ] **Step 1 (4 min): Add a self-excluding active-surface audit before edits**

The audit scans only active files: recursive `src`, `tests`, and `dist`; `README.md`; `package.json`; `compatibility.json`; `.github/workflows/ci.yml`; `docs/configuration.md`; and `docs/security.md`. It never scans `docs/superpowers`, and it skips its own file. Construct every forbidden token from fragments so the test cannot match its own source:

```typescript
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

async function recursiveFiles(root: string): Promise<string[]> {
  const names = await readdir(root, { recursive: true });
  return names.filter(name => /\.(ts|js|json|md|yml|sh)$/.test(name)).map(name => join(root, name));
}
async function activeFiles(): Promise<string[]> {
  const paths: string[] = [];
  for (const root of ["src", "tests", "dist"]) paths.push.apply(paths, await recursiveFiles(root));
  paths.push("README.md", "package.json", "compatibility.json", ".github/workflows/ci.yml", "docs/configuration.md", "docs/security.md");
  return paths.filter(path => !path.endsWith("no-hermes.test.ts"));
}
const activeRetiredPaths = (paths: readonly string[]) => paths.filter(path => retiredPaths.includes(path));


const deletedPaths = ["src/admin/hermes-contract.ts", "src/admin/import-hermes.ts", "src/admin/import-plan.ts", "tests/unit/hermes-contract.test.ts", "tests/unit/import-hermes.test.ts", "tests/unit/import-plan.test.ts"];
const forbidden = [["import", "-hermes"], ["hermes", "-contract"], ["import", "Hermes"], ["SOURCE", "_QDRANT_"], ["admin", ".source"], ["hermes", "_memory"]].map(parts => parts.join(""));
const importOrExecutableSurface = /(?:\b(?:import|export)\b[^\n]*(?:from\s*)?["'`]?|\b(?:command|collection|source|endpoint|credential)\s*[:=]|\b(?:npm|node|pi|prime|qdrant)\s+)/iu;
const importedSpecifier = /(?:\bfrom\s*|\bimport\s*)["']([^"']+)["']/giu;

describe("v2 active surface", () => {
  it("checks executable retired paths without rejecting negative fixtures", async () => {
    const paths = await activeFiles();
    const source = await Promise.all(paths.map(async path => [path, await readFile(path, "utf8")] as const));
    const runtimeHits = source.filter(([path, value]) => !path.startsWith("tests/") && forbidden.some(token => value.includes(token)) && importOrExecutableSurface.test(value)).map(([path]) => path);
    const testHits = source.filter(([path, value]) => path.startsWith("tests/") && Array.from(value.matchAll(importedSpecifier)).some(([, specifier]) => forbidden.some(token => specifier.includes(token)))).map(([path]) => path);
    const existingDeleted = (await Promise.all(deletedPaths.map(async path => { try { await readFile(path); return path; } catch { return null; } }))).filter((path): path is string => path !== null);
    expect(runtimeHits.concat(testHits, existingDeleted)).toEqual([]);
  });
});
```

- [ ] **Step 2 (3 min): Run the removal audit red and record the failure**

```bash
npx vitest run tests/unit/no-hermes.test.ts
test ! -e tests/integration/qdrant.test.ts
```

Expected: FAIL, naming current v1 Hermes modules/config/help and the Hermes-bearing integration test; no live service is contacted. After Task 1 deletion, the same focused command must pass; this Task-1-only path assertion is intentionally superseded when Task 14 recreates the v2 integration test.

- [ ] **Step 3 (3 min): Set the development package metadata**

In the fresh implementation worktree, set version `2.0.0-dev.0`, npm engine `>=11.10`, and runtime `umap-js@1.4.0` without removing the human CLI bin:

```bash
npm pkg set version=2.0.0-dev.0 engines.npm='>=11.10'
npm pkg set dependencies.umap-js=1.4.0
```

Retain the Pi extension entry, committed `dist`, Node >=20, and intended docs/files.

- [ ] **Step 4 (4 min): Delete Hermes modules/tests and replace the interim admin shell**

Delete every Hermes module/test listed above before adding v2 behavior. Replace `src/admin/cli.ts` help with only `init`, `project`, `privacy`, `status`, `curate`, `raptor`, `reconcile`, `inspect`, and `forget`; make `src/admin/status.ts` a real destination-only v2 status shape and `src/admin/init.ts` a destination-only v2 contract shell. Do not alias retired fields or commands. Keep generic `src/admin/qdrant-admin.ts` and `src/admin/secret-scan.ts` until their named later tasks.

Set compatibility schema 2 with exact host/Qdrant pins:

```json
{
  "schema": 2,
  "primeAgent": { "repository": "https://github.com/PrimeIntellect-ai/prime-agent.git", "minimumCommit": "a18809e00ea30638584d87b3afea7285a9d7296c", "latestTestedCommit": "a18809e00ea30638584d87b3afea7285a9d7296c" },
  "pi": { "package": "@earendil-works/pi-coding-agent", "minimumVersion": "0.84.1", "latestTestedVersion": "0.84.1" },
  "qdrant": { "minimumVersion": "1.17.0", "latestTestedVersion": "1.17.1" }
}
```

- [ ] **Step 5 (4 min): Update and verify the umap lockfile only**

Still in the fresh implementation worktree, record the exact runtime dependency without upgrading unrelated packages:

```bash
npm install --package-lock-only --ignore-scripts
npm ci
```

Expected: package-lock contains `umap-js@1.4.0`, `npm ci` exits 0, and no package publication or live service is involved.

Stage this lockfile only with Task 1 after the source shell is green.

- [ ] **Step 6 (4 min): Declare transitional host, Qdrant, embedding, and retrieval types**

Remove `admin.source`, source credentials, Hermes schema, and source env aliases while retaining v1 runtime fields. Add `HostId`, `RetentionDays`, `AuthorizedDestination`, `ConfigLoadDependencies`, and the canonical `RuntimeConfig` base members for host/config path/enabled/auto-recall, qdrant URL/collection/key/replication/write consistency, embeddings base URL/model/dimension/query prefix/key, and all v1 retrieval limits including literal `hardContextCharBudget: 16000`.

- [ ] **Step 7 (4 min): Add all v2 policy/coordination/capture/outbox/curation/RAPTOR members**

Extend that same single `RuntimeConfig` interface with `projects`, `capture`, `privacy` authorized-destination sets, `coordination`, `outbox`, `curation`, `memoryModel`, `raptor`, and no admin credential member. Keep the exact complete shape from the locked contract; do not create a second config interface. Human-only destination credentials are not part of `RuntimeConfig` (see `AdminProcessSecrets` below):

```typescript
export interface RuntimeConfig {
  projects: { registrations: Record<string, { canonicalPath: string; fingerprint: string; alias: string }> };
  capture: { enabled: boolean; projectAllowlist: string[]; projectDenylist: string[]; episodeRetentionDays: RetentionDays; toolArgsChars: number; toolResultChars: number };
  privacy: { egressMode: "local_only" | "allowlist"; allowedQdrantDestinations: AuthorizedDestination[]; allowedEmbeddingDestinations: AuthorizedDestination[]; allowedLlmDestinations: AuthorizedDestination[]; allowActiveModelFallback: boolean; allowCrossProviderReplay: boolean };
  coordination: { maxClockSkewMs: number; readConsistency: number | "majority" | "quorum" | "all"; leaseMs: number; reconcileIntervalMs: number };
  outbox: { maxJobs: number; maxBytes: number; retryBaseMs: number; retryMaxMs: number; nodeId?: string; sharedFilesystem: boolean };
  curation: { turnTrigger: number; toolTrigger: number; maxInputTokens: number };
  memoryModel: { modelId?: string; timeoutMs: number; maxOutputTokens: number };
  raptor: { rebuildEpisodeDelta: number; maxLevels: number; summaryInputTokens: number; umapDimensions: number; localNeighbors: number; gmmMaxClusters: number; membershipThreshold: number; seed?: number };
}
```

 Intermediate typecheck must pass with all fields present even while loaders still use defaults. The two snippets are declaration-merging slices of one exported `RuntimeConfig` in `src/types.ts`, not separate runtime shapes:

```typescript
export type HostId = "pi" | "prime";
export type RetentionDays = number | "indefinite";
export interface AuthorizedDestination { id: string; residency: string; dataUse: string; }
export interface RuntimeConfig {
  host: HostId; configPath: string; enabled: boolean; autoRecall: boolean;
  qdrant: { url: string; collection: string; apiKey?: string; replicationFactor: number; writeConsistencyFactor: number };
  embeddings: { baseUrl: string; model: string; dimension: 1024; queryPrefix: string; apiKey?: string };
  retrieval: { topK: number; candidatesPerLane: number; minScore: number; projectBoost: number; contextBudgetChars: number; toolResultBudgetChars: number; hardContextCharBudget: 16000; timeoutMs: number; rootScope: "project" | "project_and_global"; childSearch: boolean };
}
```

Define the separate human-only process-secret boundary in `src/admin/secrets.ts`:

```typescript
export interface AdminProcessSecrets { destinationApiKey?: string; }
export function loadAdminProcessSecrets(env: Record<string, string | undefined>): AdminProcessSecrets;
```

`loadAdminProcessSecrets` is called only by human CLI init/status/privacy/forget code, reads `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`, and never enters `RuntimeConfig`, extension context, model tools, outbox jobs, or worker policy records. Unit tests use the exact secret name without persisting the value.

- [ ] **Step 8 (4 min): Make the transitional loader and active docs Hermes-free**

Update `config.ts` to reject retired `SOURCE_QDRANT_*`, file credentials, `admin.source`, and source aliases; preserve the v1 retrieval defaults; and accept only the exact operational suffixes `QDRANT_URL`, `QDRANT_COLLECTION`, `EMBEDDING_BASE_URL`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSION`, `AUTO_RECALL`, `TOP_K`, `CANDIDATES_PER_LANE`, `MIN_SCORE`, `PROJECT_BOOST`, `CONTEXT_BUDGET_CHARS`, `TOOL_RESULT_BUDGET_CHARS`, and `TIMEOUT_MS` under `PI_QDRANT_MEMORY_`. Test exact secrets `PI_QDRANT_MEMORY_QDRANT_API_KEY`, `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`, and `PI_QDRANT_MEMORY_EMBEDDING_API_KEY`; LLM credentials remain registry-only. Rewrite active README/config/security/CI text to v2 terminology while allowing only archival references in `docs/superpowers`. Because the active-surface audit forbids retired literals, rejection code, rejection tests, migration diagnostics, and active docs must construct every retired token (`SOURCE_QDRANT_`, `admin.source`, source aliases) from fragments exactly as the audit does; the compiled `dist` must remain literal-free. Rewrite `tests/unit/service.test.ts`'s fixture in this task to the transitional v2 `RuntimeConfig` and remove its old source collection/`admin.source` shape.

- [ ] **Step 9 (4 min): Run the transitional green suite and typecheck**

```bash
rm -rf dist
npm run build
npx vitest run tests/unit/no-hermes.test.ts tests/unit/config.test.ts tests/unit/admin-init-status.test.ts tests/unit/service.test.ts
npm run typecheck
```

Expected: the stale generated `dist` directory is removed and build regenerates it before the audit; selected tests PASS with regenerated dist, exact env/secret allowlists, and no-Hermes active-surface hits; TypeScript exits 0.

- [ ] **Step 10 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add package.json package-lock.json compatibility.json README.md docs/configuration.md docs/security.md .github/workflows/ci.yml src/types.ts src/config.ts src/admin/cli.ts src/admin/init.ts src/admin/status.ts src/admin/secrets.ts tests/unit/config.test.ts tests/unit/admin-init-status.test.ts tests/unit/service.test.ts tests/unit/no-hermes.test.ts src/admin/hermes-contract.ts src/admin/import-hermes.ts src/admin/import-plan.ts tests/unit/hermes-contract.test.ts tests/unit/import-hermes.test.ts tests/unit/import-plan.test.ts tests/integration/qdrant.test.ts dist
npm run check
git diff --cached --check
```

Expected: build then `npm run check` exits 0 with refreshed `dist` staged; the cached diff contains the Hermes and integration-test deletions and no active retired surface.

- [ ] **Step 11 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after `npm run check`, verifying development version `2.0.0-dev.0`, exact package-lock/umap setup, complete transitional config, exact env/secret names, active docs/CI scan, compatibility pins, and no Hermes alias. Fixes rerun focused green tests, build, exact staging, check, and staged review. After both approvals:

```bash
git commit -m "feat: remove Hermes and start v2 contract"
```

Expected: the conventional commit is created only after both reviewer approvals.

### Task 2: V2 Configuration, Operator Project Identity, Record IDs, and Egress Policy

**Files:**
- Modify: `src/types.ts`, `src/config.ts`, `src/host.ts`, `src/project.ts`, `tests/unit/config.test.ts`, `tests/unit/host.test.ts`, `tests/unit/project.test.ts`
- Create: `src/domain/canonical.ts`, `src/domain/ids.ts`, `src/domain/records.ts`, `src/domain/policy.ts`, `src/security/egress.ts`, `tests/unit/ids.test.ts`, `tests/unit/records.test.ts`, `tests/unit/policy.test.ts`
- Delete: `src/admin/canonical.ts` and its generated `dist/admin/canonical.*`; update any affected imports to `src/domain/canonical.ts`

**Interfaces:**
- **Consumes:** Task 1's Hermes-free `HostId`/config shell, exact host pins, and the spec's v2 configuration, project-registration, record, policy, and privacy contracts.

- **Produces:** the complete fail-closed `RuntimeConfig`; registered/local-only project identity; canonical hashing/UUID and typed episode/curated/control/job/manifest policy records; exact producer-policy intersections; and deterministic IDs used by all later writers.

- [ ] **Step 1 (4 min): Write red configuration precedence/range tests**

Extend `config.test.ts` with table-driven tests for every §6.2 field/range, capture-off/retention activation, host-specific collection defaults, duplicate endpoint/collection rejection, exact operational suffix allowlist, exact three secret names, retired source-field rejection, and owner/schema mismatch hooks.

- [ ] **Step 2 (3 min): Write red project-registration tests**

Extend `project.test.ts` for XDG register/unregister/status, canonical realpath, symlink escape, path/fingerprint mismatch, origin spoofing, and local-only isolation. Assert only a human registration can converge aliases between machines.

- [ ] **Step 3 (4 min): Write red canonical-ID tests**

Add deterministic fixtures for `stateKey`, `contentId`, `observationId`, `evidenceLinkId`, episode IDs, job IDs, manifest hashes, and owner-independent UUID IDs:

```typescript
it("separates content, observation, and evidence identity", () => {
  const s = stateKey({ host: "pi", scope: "project", projectId: "p", category: "preference", subject: "editor", predicate: "uses" });
  const c = contentId("policy-hash", s, "vim");
  const o = observationId(3, c, "00000000-0000-0000-0000-000000000001", "session:7");
  expect(o).not.toBe(c);
  expect(evidenceLinkId(o, "00000000-0000-0000-0000-000000000001", 1)).toMatch(/^[0-9a-f-]{36}$/);
  expect(deterministicUuid("pi-qdrant-memory-v2", "pi", "session", "message")).toMatch(/^[0-9a-f-]{36}$/);
});
```

- [ ] **Step 4 (4 min): Write red record-schema tests**

Add discriminated `EpisodeRecord`, `CuratedMemoryRecord`, `CuratedCurrentRecord`, `RaptorSummaryRecord`, `ControlRecord`, `ProcessingPolicyRecord`, `JobRecord`, `CoverageRecord`, `EvidenceLinkRecord`, and `TombstoneRecord` fixtures. Assert unknown record types, missing provenance/manifest closure, wrong host/schema/privacy/policy epochs, unbounded text, non-finite vectors, and secret-bearing IDs are rejected.

- [ ] **Step 5 (4 min): Write red policy and egress tests**

Assert authorized Qdrant/embedding/LLM destination-set intersection, earliest expiry, provider replay flags, residency/data-use labels, policy/hash/content collision behavior, local-only defaults, and admin credential separation from runtime config.

- [ ] **Step 6 (3 min): Run the focused domain suite red**

```bash
npx vitest run tests/unit/config.test.ts tests/unit/project.test.ts tests/unit/ids.test.ts tests/unit/records.test.ts tests/unit/policy.test.ts
```

Expected: FAIL with missing v2 exports, old config fields/defaults, and no XDG registration/ID implementation.

- [ ] **Step 7 (4 min): Complete the single canonical `RuntimeConfig` loader**

Do not add a second config interface or parallel shape. Fill the Task 1 `RuntimeConfig` loader and defaults for every field already declared: host/config path/enabled/auto-recall; qdrant URL/collection/keys/replication/write consistency; embeddings base URL/model/dimension/query prefix/key; all v1 retrieval limits plus root/child scope; projects registrations; capture enable/allowlist/denylist/retention/tool budgets; privacy egress and three authorized-destination sets/fallback flags; coordination skew/read consistency/lease/reconcile; outbox limits/retry/node/filesystem; curation triggers/input; memory-model ID/timeout/output; RAPTOR rebuild/levels/summary/UMAP/local/GMM/membership/seed; and collection-scoped session Qdrant/embedding credentials only; `AdminProcessSecrets` is not a `RuntimeConfig` member.

Apply `allowlisted env > host > shared > compiled default`, reject unknown/retired fields as specified, require explicit retention when capture activates, reject URL credentials, and enforce every range. The exact operational suffix allowlist is `QDRANT_URL`, `QDRANT_COLLECTION`, `EMBEDDING_BASE_URL`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSION`, `AUTO_RECALL`, `TOP_K`, `CANDIDATES_PER_LANE`, `MIN_SCORE`, `PROJECT_BOOST`, `CONTEXT_BUDGET_CHARS`, `TOOL_RESULT_BUDGET_CHARS`, and `TIMEOUT_MS`; the three globally permitted secret names are `PI_QDRANT_MEMORY_QDRANT_API_KEY`, `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`, and `PI_QDRANT_MEMORY_EMBEDDING_API_KEY`. `loadConfig` may consume only collection-scoped session Qdrant/embedding credentials; the admin secret is read only by `loadAdminProcessSecrets`, returned only to the human CLI process, and is absent from `loadConfig`/`RuntimeConfig` output. Add parameterized tests for every allowlisted suffix, each secret source, admin-secret absence from `RuntimeConfig`, separate CLI-secret loading, and rejection of every other `PI_QDRANT_MEMORY_*` field that is not operationally allowlisted. Defaults include `pi_memory`/`prime_memory`, capture off, local-only egress, project scope, child search true, lease 30000, reconcile 900000, triggers 10/15, and RAPTOR 10/10/50/.10.

- [ ] **Step 8 (4 min): Implement XDG project registration and resolution**

Implement `ProjectRegistryBinding`, `registerProject`, `unregisterProject`, `projectStatus`, and `resolveProjectIdentity`. Canonicalize path with `realpath`, ensure the requested path stays inside its registered canonical path (including symlink checks), fingerprint origin by stripping protocol/userinfo/query/`.git` and falling back to sorted root commits, and hash `installation_salt + canonical_path + vcs_fingerprint` for `local_only`. Never read repository config as package configuration or authorization. `project register` is the human-only caller added in Task 13.

- [ ] **Step 9 (4 min): Implement record schemas and policy intersection**

Create `src/domain/canonical.ts` with sorted-key canonical JSON, SHA-256, and UUIDv5-like deterministic UUID bytes; `src/domain/ids.ts` with `stateKey`, `contentId`, `observationId`, `evidenceLinkId`, episode IDs, job IDs, manifest hashes, and tombstone IDs; `records.ts` with discriminated `EpisodeRecord`, `CuratedMemoryRecord`, `CuratedCurrentRecord`, `RaptorSummaryRecord`, `ControlRecord`, `ProcessingPolicyRecord`, `JobRecord`, `CoverageRecord`, `EvidenceLinkRecord`, and `TombstoneRecord`; and `policy.ts` with redacted destination IDs, residency/data-use labels, expiry, provider, epoch/hash, and intersection functions. Export `type MemoryRecord = EpisodeRecord | CuratedMemoryRecord | CuratedCurrentRecord | RaptorSummaryRecord | ControlRecord | ProcessingPolicyRecord | JobRecord | CoverageRecord | EvidenceLinkRecord | TombstoneRecord` for capability-separated Qdrant writes.

Use the contract's canonical episode hash (identity, redacted text, provenance, schema; no delivery timestamps/producer IDs/vector floats), insert-only observations, policy-specific current IDs, immutable derived provenance and earliest expiry. Add explicit runtime schema guards that reject unknown `record_type`, missing source/manifest closure, wrong host/schema/privacy/policy epochs, unbounded text, non-finite vectors, or secret-bearing IDs. Keep source/provenance IDs bounded and redacted.

```typescript
export interface ProcessingPolicy {
  id: string; ownerHost: HostId;
  qdrant: readonly AuthorizedDestination[]; embedding: readonly AuthorizedDestination[]; llm: readonly AuthorizedDestination[];
  originProvider: string; allowCrossProviderReplay: boolean; expiresAt: string | null; policyRevision: string;
}
export interface ProcessingPolicyRecord { recordType: "processing_policy"; id: string; policy: ProcessingPolicy; canonicalHash: string; expiresAt: string | null; }
export interface EpisodeRecord { recordType: "episode"; id: string; contentHash: string; sourceEntryId: string; host: HostId; sessionId: string; eventAt: string; expiresAt: string | null; agentRole?: "root" | "child"; depth?: number; policyId?: string; projectId?: string; text?: string; }
export interface CuratedMemoryRecord { recordType: "curated_memory"; id: string; contentId: string; observationId: string; eventAt: string; effectiveAt: string; }
export interface CuratedCurrentRecord { recordType: "curated_current"; id: string; contentId: string; observationId: string; version: number; }
export interface RaptorSummaryRecord { recordType: "raptor_summary"; id: string; generationId: string; membershipHash: string; level: number; }
export interface ControlRecord { recordType: "collection_control"; id: string; version: number; activeGeneration: string | null; activeBaseGeneration?: string | null; privacyEpoch: number; coordinationPolicyEpoch: number; coordinationPolicyHash?: string; state: "active" | "draining" | "retired"; scanCursor?: string; lastForgetBarrier?: string | null; }
export interface JobRecord { recordType: "job"; id: string; policyId: string; policyHash: string; policyEpoch: number; }
export interface CoverageRecord { recordType: "coverage"; id: string; episodeId: string; extractorRevision: string; }
export interface EvidenceLinkRecord { recordType: "evidence_link"; id: string; sourceId: string; targetId: string; }
export interface TombstoneRecord { recordType: "tombstone"; id: string; scope: "occurrence" | "content" | "state"; targetId: string; }
export function intersectPolicies(policies: readonly ProcessingPolicy[], worker: ProcessingPolicy): ProcessingPolicy | null;
```

- [ ] **Step 10 (4 min): Run config/domain green tests and typecheck**

```bash
npx vitest run tests/unit/config.test.ts tests/unit/host.test.ts tests/unit/project.test.ts tests/unit/ids.test.ts tests/unit/records.test.ts tests/unit/policy.test.ts
npm run typecheck
```

Expected: all selected tests PASS, including every min/max boundary and project-registration fail-closed case; TypeScript exits 0.

- [ ] **Step 11 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/types.ts src/config.ts src/host.ts src/project.ts src/domain/canonical.ts src/domain/ids.ts src/domain/records.ts src/domain/policy.ts src/security/egress.ts src/admin/canonical.ts tests/unit/config.test.ts tests/unit/host.test.ts tests/unit/project.test.ts tests/unit/ids.test.ts tests/unit/records.test.ts tests/unit/policy.test.ts dist
npm run check
git diff --cached --check
```

Expected: build then `npm run check` exit 0 with regenerated `dist` staged; cached whitespace check is clean and the canonical move/deletion is visible.

- [ ] **Step 12 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after `npm run check`, verifying the v2 table/defaults, no Hermes aliases, exact ID input domains, project registration as the only cross-machine convergence path, owner/policy/expiry invariants, and model/tool authorization boundaries. If either requests a fix, rerun focused green tests, build, exact staging, check, and staged-diff review. After both approvals:

```bash
git commit -m "feat: add v2 config identity and record contracts"
```

Expected: the conventional commit is created only after both approvals.


### Task 3: Qdrant 1.17 Client, Collection Contract, and Verified Atomic Write Primitives

**Files:**
- Modify: `src/clients/http.ts`, `src/admin/init.ts`, `src/admin/status.ts`, `tests/unit/admin-init-status.test.ts`
- Create: `src/qdrant/client.ts`, `src/qdrant/schema.ts`, `src/qdrant/write.ts`, `tests/unit/qdrant.test.ts`
- Delete: `src/admin/qdrant-admin.ts`, `tests/unit/admin-client.test.ts`, and generated `dist/admin/qdrant-admin.*`; retain `src/clients/qdrant-readonly.ts` temporarily until Task 11 rewrites its callers, then delete it with its generated artifact

**Interfaces:**
- **Consumes:** Task 2's `RuntimeConfig`, `ProcessingPolicy`, typed records, canonical hashes, owner/collection defaults, and qdrant minimum 1.17.0/validated 1.17.1.

- **Produces:** capability-separated REST clients (`QdrantReadClient`, session `QdrantSessionWriter`, and human-only `QdrantAdminClient`), metadata/control initialization, named-vector/payload-only point support, insert-only/update-only/OCC/CAS helpers, read-back collision detection, and strong control-write options. Runtime readers/writers expose no generic delete, collection creation, admin init, or model-controlled endpoint.

- [ ] **Step 1 (4 min): Write red REST transport/client tests**

Add fake-fetch tests for health, collection info, retrieve/search/count, methods, JSON validation, abort/timeout, collection validation, and scoped `api-key` headers. Reject bearer headers, generic request escape hatches, non-finite vectors, and model-controlled infrastructure arguments.

- [ ] **Step 2 (4 min): Write red collection/schema tests**

Test named `semantic` 1024/Cosine vectors, payload-only points, all required indexes, immutable owner/schema metadata, separate control ID, owner-independent metadata ID, and Qdrant 1.17 collection initialization/reread behavior.

- [ ] **Step 3 (4 min): Write red conditional-write tests**

Cover `insert_only`, `update_only`, `update_filter`, `wait=true`, `ordering=strong`, read consistency, ignored-insert reread/hash collision, control/lease/job CAS, and admin/session key separation:

```typescript
it("uses 1.17 insert_only and fails closed on an ignored hash collision", async () => {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  const fetchImpl: typeof fetch = async (input, init = {}) => {
    calls.push({ url: String(input), init });
    if (new URL(String(input)).pathname.endsWith("/points")) return json(200, { result: { status: "acknowledged" } });
    if (String(input).includes("/points/retrieve")) return json(200, { result: [{ id: "episode-1", payload: { content_hash: "different" } }] });
    return json(200, { result: { status: "ok" } });
  };
  await expect(insertOnly(sessionWriter(fetchImpl), episode({ id: "episode-1", contentHash: "expected" })))
    .rejects.toThrow(/content hash collision/);
  const body = JSON.parse(String(calls[0]?.init.body));
  expect(body.update_mode).toBe("insert_only");
  const requestUrl = new URL(calls[0]?.url ?? "http://invalid");
  expect(requestUrl.searchParams.get("wait")).toBe("true");
  expect(requestUrl.searchParams.get("ordering")).toBe("strong");
  const headers = new Headers(calls[0]?.init.headers);
  expect(headers.get("api-key")).toBe("collection-scoped");
  expect(headers.has("authorization")).toBe(false);
});
```

- [ ] **Step 4 (3 min): Run Qdrant unit tests red**

```bash
npx vitest run tests/unit/qdrant.test.ts tests/unit/admin-init-status.test.ts
```

Expected: FAIL with missing qdrant modules and v1 client paths still exposing read-only/source assumptions.

- [ ] **Step 5 (4 min): Implement the read/health REST client slice**

Declare capability interfaces in `src/qdrant/client.ts`:

```typescript
export interface QdrantReadClient { health(): Promise<unknown>; collectionInfo(): Promise<unknown>; retrieve(ids: readonly string[]): Promise<unknown[]>; scroll(): Promise<unknown>; search(): Promise<unknown>; count(): Promise<number>; }
export interface QdrantSessionWriter extends QdrantReadClient { upsertPoints(points: readonly unknown[], mode: "insert_only" | "update_only", updateFilter?: Record<string, unknown>): Promise<void>; }
export interface QdrantAdminClient { createCollection(): Promise<void>; createPayloadIndex(): Promise<void>; deletePoints(ids: readonly string[]): Promise<void>; }
```

Create `QdrantReadClient` in `src/qdrant/client.ts` with only `health`, `collectionInfo`, `retrieve`, `scroll`, `search`, and `count`; runtime readers accept only a validated collection-scoped key and endpoint. Keep `fetchOk`/`fetchJson` timeout/abort behavior, validate collection identifiers/JSON envelopes/finite vectors/object payloads, and send credentials only as `api-key`. Session instances accept only the collection-scoped key; no model/tool argument can provide host, collection, endpoint, or key.

- [ ] **Step 6 (4 min): Implement the admin/schema write client slice**

Add `QdrantSessionWriter` with only typed `upsertPoints`/insert-only/update-only/update-filter operations and `QdrantAdminClient` with human-only `createCollection`, `createPayloadIndex`, and `deletePoints`; no session writer/read client has admin/delete methods. Use REST only, expose `update_mode=insert_only|update_only` and typed `update_filter`, and require strong ordering/wait for control/lease/job writes. Migrate `admin/init.ts` and `admin/status.ts` off the deleted v1 admin client while retaining the temporary readonly adapter until Task 11. Admin credentials come only from `AdminProcessSecrets`.

- [ ] **Step 7 (4 min): Implement schema constants and payload indexes**

Create `src/qdrant/schema.ts` with `V2_COLLECTION_METADATA` and all required keyword/integer/datetime/full-text declarations:

```typescript
export const V2_COLLECTION_METADATA = { schema: "pi-qdrant-memory-v2", schema_revision: 1, dense_vector: "semantic", embedding_model: "bge-m3", embedding_dimension: 1024, distance: "Cosine" } as const;
export const REQUIRED_INDEXES = [
  ["record_type", "keyword"], ["owner_host", "keyword"], ["project_id", "keyword"], ["project_identity_kind", "keyword"], ["scope", "keyword"], ["status", "keyword"], ["resolution", "keyword"], ["state_key", "keyword"], ["content_id", "keyword"], ["observation_id", "keyword"], ["session_id", "keyword"], ["turn_id", "keyword"], ["agent_role", "keyword"], ["generation_id", "keyword"], ["job_id", "keyword"], ["category", "keyword"], ["tool_name", "keyword"], ["error_fingerprint", "keyword"], ["secret_scan", "keyword"], ["event_at", "datetime"], ["effective_at", "datetime"], ["created_at", "datetime"], ["lease_expires_at", "datetime"], ["expires_at", "datetime"], ["privacy_epoch", "integer"], ["coordination_policy_epoch", "integer"], ["version", "integer"], ["level", "integer"], ["text", "text"],
] as const;
```

- [ ] **Step 8 (4 min): Implement collection init and immutable metadata readback**

Create two distinct deterministic UUID payload-only points. `collection_metadata` is immutable and collection-global with owner-independent ID `deterministicUuid("pi-qdrant-memory-v2", "collection_metadata")`; it is created `insert_only` and reread/validated after every concurrent init. Its payload includes the first creator's immutable `owner_host`, exact `schema`, `schema_revision`, vector name/model/dimension/distance, and contract hash. `collection_control` is separate with owner-independent UUID ID `deterministicUuid("pi-qdrant-memory-v2", "collection_control")`; Task 3 initializes it exactly once via `insertInitialControl(initialControl)` using `insert_only`, where `initialControl` has `version=0`, `privacy_epoch=0`, active policy epoch/hash, `active_generation=null`, and `state="active"`; only Task 8 performs later OCC/CAS updates. Qdrant collection configuration has no arbitrary metadata bag. A matching vector collection with missing/foreign metadata fails closed because all hosts contend on one owner-independent metadata ID.

Rewrite `admin/init.ts` to create the host-specific collection only when absent, insert/read back both immutable metadata and initial mutable control, reread metadata/indexes, require replication >=2 and `write_consistency_factor >= ceil((replication+1)/2)` for clusters (1/1 single-node), and validate Qdrant behavior as >=1.17 without claiming a generic `/version`.

- [ ] **Step 9 (4 min): Implement insert-only and OCC publication writes**

Create `qdrant/write.ts` helpers:

```typescript
export function insertOnly<T extends MemoryRecord>(client: QdrantSessionWriter, record: T): Promise<"inserted" | "existing">;
export function insertInitialControl(client: QdrantSessionWriter, initialControl: ControlRecord): Promise<"inserted" | "existing">;
export function updateOnlyCas(client: QdrantSessionWriter, input: { id: string; expectedVersion: number; expectedEpoch: number; patch: Record<string, unknown> }): Promise<boolean>;
export function publishControlCas(client: QdrantSessionWriter, input: { expectedVersion: number; expectedBaseGeneration: string | null; next: ControlRecord }): Promise<boolean>;
```

After every insert/ignored insert, retrieve and compare canonical `content_hash`; mismatch throws a collision error. Control/lease/job writes always use strong ordering and wait; partial/HTTP-success responses are reread, never treated as transactions. `update_only` never creates absent points, and `update_filter` predicates include expected version, owner/state, policy epoch, or base generation as applicable.

- [ ] **Step 10 (4 min): Run Qdrant unit tests green and typecheck**

```bash
npx vitest run tests/unit/qdrant.test.ts tests/unit/admin-init-status.test.ts
npm run typecheck
```

Expected: all request-contract tests PASS; ignored insert with equal hash converges, different hash fails closed, payload-only control points work, owner mismatch aborts, and TypeScript exits 0.

- [ ] **Step 11 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/clients/http.ts src/admin/init.ts src/admin/status.ts src/qdrant/client.ts src/qdrant/schema.ts src/qdrant/write.ts src/admin/qdrant-admin.ts tests/unit/admin-client.test.ts tests/unit/qdrant.test.ts tests/unit/admin-init-status.test.ts dist
npm run check
git diff --cached --check
```

Expected: regenerated `dist` is staged; build/check and cached whitespace check exit 0; deleted v1 client artifacts are visible in the cached diff.

- [ ] **Step 12 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for Qdrant >=1.17/update modes, exact metadata/index contract, no SDK/generic mutation path, credentials separation, reread/hash collision handling, owner isolation, strong control writes, and no false transaction claim. Fixes rerun focused tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add Qdrant v2 write and init contract"
```

Expected: commit succeeds only after both reviews approve the staged implementation.


### Task 4: Redaction, Egress Gating, and Persisted Lifecycle Episode Capture

**Files:**
- Modify: `src/security/egress.ts`, `src/types.ts`
- Create: `src/security/redaction.ts`, `src/capture/select.ts`, `src/capture/episode.ts`, `src/capture/scanner.ts`, `tests/unit/redaction.test.ts`, `tests/unit/capture.test.ts`
- Delete: `src/admin/secret-scan.ts`, `tests/unit/secret-scan.test.ts`, and generated `dist/admin/secret-scan.*`

**Interfaces:**
- **Consumes:** Task 2 policy/record contracts and Task 3 Qdrant host/schema boundaries. Does not depend on outbox delivery; capture must be useful and durable even when Qdrant is down.

- **Produces:** redaction-before-disk/egress, secret-scan verdicts and hard budgets, selected episode records, child/root tagging, project allow/deny gating, and lifecycle capture that scans persisted `getEntries()` after a persisted activation cutoff on exactly `agent_end`, `session_before_compact`, and `session_shutdown`.

- [ ] **Step 1 (4 min): Write red structural redaction/scanner tests**

Use fixtures for bearer/API keys, URL credentials/query secrets, home paths, Unicode controls, high entropy, safe structural replacement, scanner rejection/error, final hash, and redaction status. Assert safe redaction remains usable and only final scanner `passed` can be stored.

- [ ] **Step 2 (4 min): Write red capture selection/exclusion tests**

Use persisted-entry fixtures containing system/developer/custom messages, a prior memory context, the extension's own `memory_search` tool call arguments/results (including useful and error-shaped results), thinking/signatures, complete and partial tool results, finalized user/assistant messages, useful tool fields, stderr/status/code, and child markers. Assert the extension tool entries never become episodes and only eligible finalized excerpts survive.

- [ ] **Step 3 (4 min): Write red activation-cutoff/path tests**

Test first activation, persisted cursor/hash, restart dedupe, exact three lifecycle names, rejection of `agent_settled`, hashed session filenames, invalid host agent paths, symlink traversal, empty-session activation, activation/write failure fail-closed, and root/child tagging. `activateCapture` must derive the current tail from its own `getEntries()` snapshot; it accepts no caller cutoff:

```typescript
it("activates at the current tail and captures only later persisted entries", async () => {
  const state = new Map<string, string>();
  const entries = [{ type: "message", id: "before", message: { role: "user", content: "old" } }];
  const deps = {
    readActivation: async (key: string) => state.get(key),
    writeActivation: async (key: string, value: string) => { state.set(key, value); },
    getEntries: () => entries.slice(),
    now: () => 100,
  };
  await activateCapture(Object.assign({}, deps, { sessionId: "s" }));
  entries.push({ type: "message", id: "after", message: { role: "user", content: "new" } });
  const result = await capturePersistedEntries(Object.assign({}, deps, { sessionId: "s", lifecycle: "agent_end", activationDir: "/safe", host: "pi" }));
  expect(result.map(e => e.sourceEntryId)).toEqual(["after"]);
  expect(CAPTURE_LIFECYCLES).toEqual(["agent_end", "session_before_compact", "session_shutdown"]);
  expect(CAPTURE_LIFECYCLES).not.toContain("agent_settled");
});
```

Also assert a failed activation write leaves capture disabled and that an empty snapshot persists an explicit empty-tail/start sentinel, then captures the first entry appended later as post-activation (never historical backfill).

- [ ] **Step 4 (3 min): Run focused capture/redaction tests red**

```bash
npx vitest run tests/unit/redaction.test.ts tests/unit/capture.test.ts
```

Expected: FAIL with missing capture/security modules and no persisted cutoff implementation.

- [ ] **Step 5 (4 min): Implement structural redaction and budgets**

Create `src/security/redaction.ts` with removal of sensitive headers/fields (`authorization`, cookies, API keys, tokens, password, secret), URL userinfo/query credentials, home-path canonicalization to `$HOME`, Unicode NFC/control-character normalization, typed replacement markers, per-field budgets, and total hard budgets. Structural output reports `redactionStatus: "unchanged"|"redacted"|"dropped"`; safe markers remain usable. Do not log raw query, conversation, arguments, headers, paths, or keys.

```typescript
export interface RedactionResult { text: string; redactionStatus: "unchanged" | "redacted" | "dropped"; contentHash: string; }
export function redactStructure(input: { text: string; maxChars: number; homeDir: string }): RedactionResult;
```

- [ ] **Step 6 (4 min): Implement final secret scanning**

Create `src/capture/scanner.ts` with known patterns plus high-entropy detection and `scanFinalText(value): "passed" | "rejected" | "error"`. Scan exactly the final structural text; scanner error is a durable non-recoverable audit category, never `passed`. Only final `passed` text can enter an episode/outbox; rejected/error text is dropped/quarantined with bounded category. Stored episodes never use `secret_scan="redacted"`.

- [ ] **Step 7 (4 min): Implement persisted-entry selection**

Create `capture/select.ts` to accept only normalized persisted `getEntries()` records, select finalized user/assistant messages, tool call names/arguments, useful tool-result excerpts (error/non-success/allowlisted fields), stderr/status/code/error fingerprints, and exclude system/developer/custom/injected memory/thinking/signatures/complete outputs/aborted partials.

- [ ] **Step 8 (4 min): Implement deterministic episode materialization**

Create `capture/episode.ts` to build deterministic `EpisodeRecord` IDs from host/session/message/part identity and add `agentRole`, depth, redacted producer/node IDs, project identity, policy ID, event time, and expiry. Invalid child markers disable root work fail-closed but still permit tagged leaf capture.

- [ ] **Step 9 (4 min): Implement validated activation cutoff persistence**

Resolve the host agent directory only from validated host environment variables (`PI_CODING_AGENT_DIR` for Pi or `PRIME_AGENT_CODING_AGENT_DIR` for Prime) with the documented host default; reject empty, relative, symlink-escaping, or contradictory values. Persist a per-session activation cutoff in `<validated-agent-dir>/pi-qdrant-memory/capture/state-<sha256(sessionId)>.json`, with `0600` file/`0700` parent and the original session ID plus host in the validated payload. Never interpolate a raw session ID into a path. The first successful activation calls `getEntries()` itself, derives and atomically persists the current tail cursor/entry identity (never a caller-supplied cutoff), and every lifecycle scan includes only entries after that persisted tail and deduplicates by deterministic episode ID. If state cannot be persisted, capture remains off for that session rather than scanning historical entries. `capturePersistedEntries()` must call `getEntries()` itself; it must never use event message arrays.

Export the exact lifecycle set for Task 12:

```typescript
export const CAPTURE_LIFECYCLES = ["agent_end", "session_before_compact", "session_shutdown"] as const;
export type CaptureLifecycle = typeof CAPTURE_LIFECYCLES[number];
export interface PersistedEntry { id: string; type: string; message?: unknown; }
export interface CaptureInput { sessionId: string; lifecycle: CaptureLifecycle; getEntries: () => readonly PersistedEntry[]; activationDir: string; host: HostId; }
export async function activateCapture(input: { sessionId: string; getEntries: () => readonly PersistedEntry[]; readActivation: (key: string) => Promise<string | undefined>; writeActivation: (key: string, value: string) => Promise<void>; now: () => number }): Promise<void>;
export async function capturePersistedEntries(input: CaptureInput): Promise<EpisodeRecord[]>;
```

- [ ] **Step 10 (4 min): Run capture/redaction green and typecheck**

```bash
npx vitest run tests/unit/redaction.test.ts tests/unit/capture.test.ts
npm run typecheck
```

Expected: PASS for both exact hosts, cutoff persistence/restart, dedupe, child/root tagging, selected tool excerpts, all exclusions, budgets, scanner-error handling, and redaction before any returned record; TypeScript exits 0.

- [ ] **Step 11 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/security/egress.ts src/security/redaction.ts src/types.ts src/capture/select.ts src/capture/episode.ts src/capture/scanner.ts src/admin/secret-scan.ts tests/unit/secret-scan.test.ts tests/unit/redaction.test.ts tests/unit/capture.test.ts dist
npm run check
git diff --cached --check
```

Expected: build/check pass with regenerated `dist`; the old admin scanner artifact is deleted and only the focused security/capture modules remain.

- [ ] **Step 12 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for exact lifecycle names, persisted `getEntries()` scanning, cutoff-before-history behavior, explicit absence of `agent_settled`, both host paths, child/root policy, all capture exclusions, structural-redaction versus final-scan semantics, scanner status (`passed` only for final clean text), and no raw logs. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: capture redacted lifecycle episodes"
```

Expected: commit succeeds only after both approvals.


### Task 5: Per-Process Durable Outbox, At-Least-Once Delivery, and Adoption

**Files:**
- Modify: `src/config.ts`, `src/security/egress.ts`, `src/types.ts`
- Create: `src/outbox/store.ts`, `src/outbox/delivery.ts`, `tests/unit/outbox.test.ts`

**Interfaces:**
- **Consumes:** Task 2 policy/expiry/node settings and Task 4 redacted `EpisodeRecord` output. Task 5 defines only a processor seam; Task 7 supplies the sole production processor that performs policy/Qdrant/embedding work.

- **Produces:** immutable per-process outbox jobs, atomic fsync/rename persistence with restrictive permissions, bounded queue/backoff/deadline behavior, producer/node identity, offline adoption, and processor-driven delivery that never bypasses the Task 7 policy/embedding/Qdrant gate.

- [ ] **Step 1 (4 min): Write red outbox-store filesystem tests**

Use a temporary home and injected filesystem primitives; assert validated host-agent paths, 0700/0600 modes, exclusive producer directories, atomic temp/fsync/rename, malformed-job quarantine, restart recovery, job/byte caps, and no raw payload.

- [ ] **Step 2 (4 min): Write red processor-seam delivery/adoption tests**

Use a fake `OutboxJobProcessor` returning `delivered`, `pending`, or `quarantined`; assert retry jitter/deadlines, expiry before processor invocation, shared-filesystem node requirements, canonical path/symlink rejection, closed-producer adoption, duplicate delivery, and offline first-return checks. Task 5 tests must not construct a Qdrant client or directly insert an episode:

```typescript
it("deletes only after the injected processor reports successful readback", async () => {
  const outbox = await createOutbox({ host: "prime", homeDir: "/tmp/home", nodeId: "node-redacted", producerUuid: "p-1", fs: fakeFs() });
  const job = await outbox.enqueue({ episodes: [episode({ text: "already redacted" })], policy: policy({ expiresAt: null }) });
  const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) };
  const delivery = createOutboxDelivery({ outboxRoot: "/tmp/home", processor, now: () => 10, maxClockSkewMs: 0 });
  await delivery.deliver({});
  expect(processor.process).toHaveBeenCalledWith(expect.objectContaining({ id: job.id }), expect.anything());
  expect(await exists(job.file)).toBe(false);
});
```

- [ ] **Step 3 (3 min): Run outbox tests red**

```bash
npx vitest run tests/unit/outbox.test.ts
```

Expected: FAIL with missing outbox store/delivery implementations.

- [ ] **Step 4 (4 min): Implement the atomic per-process outbox store**

Create `src/outbox/store.ts` with `createOutbox`, `enqueue`, `listPending`, `markDelivered`, `quarantine`, `heartbeat`, `closeProducer`, and `outboxStatus`. Resolve exact host paths under the validated host agent directory:

```text
<validated-pi-agent-dir>/pi-qdrant-memory/outbox/<node-id>/<producer-uuid>/
<validated-prime-agent-dir>/pi-qdrant-memory/outbox/<node-id>/<producer-uuid>/
```

Generate a CSPRNG 128-bit producer UUID; derive node ID from an explicit safe override or machine ID plus installation salt without raw hostname; require explicit unique `outbox.nodeId` when `sharedFilesystem=true`; use `mkdir` exclusive, write-temp, `fsync(file)`, `rename`, `fsync(directory)`, and modes 0700/0600.

- [ ] **Step 5 (4 min): Persist bounded policy-bearing jobs**

A job stores only redacted content plus its content-addressed `ProcessingPolicy` record, policy ID, immutable deadline, deterministic episode/job IDs, and non-reversible audit hash. It never stores secrets or raw logs. Enforce 10,000 jobs/256 MiB defaults and configured bounds: when full, stop new capture and notify, but never discard accepted jobs. Before adoption, canonicalize producer paths, reject symlink traversal and paths outside the validated host agent directory, and verify producer UUID/node ID/state payloads.

- [ ] **Step 6 (4 min): Implement processor-driven delivery, retry, expiry, and adoption**

Create `src/outbox/delivery.ts` with `createOutboxDelivery` returning `OutboxDelivery`; local locks only optimize duplicate work. Inject `OutboxJobProcessor`, whose `process` result is the only delivery decision: `delivered` permits deletion after successful downstream readback, `pending` retains the immutable job with bounded retry/backoff, and `quarantined` preserves an immutable quarantine record without silently erasing accepted input. A producer heartbeat expiry allows another process to adopt a closed producer; NFS/shared homes may duplicate processing safely. Task 5 performs only local deadline/path/control scheduling checks and never calls Qdrant, embeddings, or an episode writer; Task 7 wires the sole production processor. Shutdown flush is best-effort and leaves durable jobs.

```typescript
export interface OutboxJobProcessor {
  process(job: OutboxJob, input: { signal?: AbortSignal }): Promise<{ status: "delivered" | "pending" | "quarantined"; category?: string }>;
}
export interface DeliveryInput { outboxRoot: string; processor: OutboxJobProcessor; now: () => number; maxClockSkewMs: number; }
export interface OutboxDelivery {
  deliver(input: { signal?: AbortSignal; maxJobs?: number }): Promise<{ delivered: number; pending: number; quarantined: number }>;
  adopt(producerPath: string): Promise<void>;
}
export function createOutboxDelivery(input: DeliveryInput): OutboxDelivery;
```

No local lock decides a valid Qdrant write or publication. A crash before the processor reports successful readback leaves the job; only `delivered` allows active-job deletion.

- [ ] **Step 7 (4 min): Run outbox green and typecheck**

```bash
npx vitest run tests/unit/outbox.test.ts
npm run typecheck
```

Expected: PASS for atomic modes/rename, producer isolation, limits, retry/adoption, duplicate node IDs, shared homes, expiry after offline producer, crash/restart and no secret/raw payload; TypeScript exits 0.

- [ ] **Step 8 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/config.ts src/security/egress.ts src/types.ts src/outbox/store.ts src/outbox/delivery.ts tests/unit/outbox.test.ts dist
npm run check
git diff --cached --check
```

Expected: regenerated outbox declarations/JS are staged; check and cached whitespace validation pass.

- [ ] **Step 9 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for atomic durability, file modes, per-process paths, processor-only delivery seam, no direct Qdrant/embedding/episode-writer bypass, expiry-before-processing, no silent drops, bounded queue behavior, adoption races, and lock non-authority. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add durable memory outbox"
```

Expected: commit succeeds only after both approvals.


### Task 6: Host-Portable LLM Bridge with Reflected Pi/Prime Completion Paths

**Files:**
- Modify: `package.json`, `package-lock.json`, `src/types.ts`, `src/config.ts`
- Create: `src/curation/llm.ts`, `tests/unit/llm.test.ts`

**Interfaces:**
- **Consumes:** Task 2 policy/model settings and Task 4 redacted records; exact Pi 0.84.1 and Prime pinned host APIs from the compatibility contract.

- **Produces:** one host-portable, fail-closed completion interface that resolves a concrete `Model<Api>`, prefers `ctx.modelRegistry.complete` when present, and otherwise feature-detects the Prime root namespace completion without statically accessing a namespace property.

- [ ] **Step 1 (4 min): Write red Pi registry/source-shape tests**

Inject a fake registry `complete`, concrete model/context/options, and assert the Pi reflected path is preferred. Audit exactly one namespace import from `@earendil-works/pi-ai`, reflected registry lookup, no namespace property access, no dynamic/inline imports, and no imported host auth type:

```typescript
const source = await readFile("src/curation/llm.ts", "utf8");
expect(source).toMatch(/import \* as PiAi from ["']@earendil-works\/pi-ai["']/);
expect(source).toMatch(/Reflect\.get\(.*modelRegistry.*complete/);
expect(source).not.toMatch(/PiAi\.completeSimple/);
expect(source).not.toMatch(/import\(|eval\(/);
expect(source).not.toMatch(/import type .*ResolvedRequestAuth[^L]/);
```

- [ ] **Step 2 (4 min): Write red Prime fallback/auth/policy tests**

Inject an AI namespace whose `completeSimple` is reachable only by `Reflect.get`, a structural auth result including nullable headers (null values are dropped before the Prime call), and destination/provider policy. Cover auth failure, missing methods, dedicated-model precedence, fallback disabled, same-provider-only replay, cross-provider opt-in, timeout/cancellation, strict bounds, and no BGE-M3 generation.

- [ ] **Step 3 (3 min): Run LLM tests red**

```bash
npx vitest run tests/unit/llm.test.ts
```

Expected: FAIL because the reflected bridge and structural auth type do not exist.

- [ ] **Step 4 (4 min): Add the peer/dev host dependency without a runtime duplicate**

In the fresh implementation worktree, add only a peer wildcard and a development pin for the host AI namespace; do not add it under runtime `dependencies`:

```bash
npm pkg set 'peerDependencies.@earendil-works/pi-ai=*' 'devDependencies.@earendil-works/pi-ai=0.84.1'
npm install --package-lock-only --ignore-scripts
npm ci
```

Expected: package JSON/lockfile contain peer `*` and dev `0.84.1`, no runtime `@earendil-works/pi-ai` dependency, and npm ci exits 0.

- [ ] **Step 5 (5 min): Implement the static namespace import and structural host types**

Create `src/curation/llm.ts` with exactly one namespace import and no static completion-property access:

```typescript
import * as PiAi from "@earendil-works/pi-ai";
import type { Api, Context, Model } from "@earendil-works/pi-ai";
import type { HostId, AuthorizedDestination } from "../types.js";
import type { ProcessingPolicy } from "../domain/policy.js";

export type ResolvedRequestAuthLike =
  | { ok: true; apiKey?: string; headers?: Record<string, string | null> }
  | { ok: false; error: string };
export interface MemoryCompletionOptions { signal?: AbortSignal; maxOutputTokens: number; temperature: number; }
export interface ModelRegistryLike {
  getApiKeyAndHeaders?: (model: Model<Api>) => Promise<ResolvedRequestAuthLike>;
  complete?: unknown;
}
export interface MemoryCompletionContext {
  host: HostId; modelRegistry: ModelRegistryLike; memoryModel?: Model<Api>; activeModel?: Model<Api>;
  activeProviderId?: string; sessionProviderId?: string; policy: ProcessingPolicy;
}
export interface AiNamespaceLike { completeSimple?: unknown; }
export interface BoundLlmDestination { readonly destination: AuthorizedDestination; complete(input: { envelope: string; signal?: AbortSignal }): Promise<string>; }
export function sanitizeAuthHeaders(headers?: Record<string, string | null>): Record<string, string>;
```

Resolve a concrete dedicated model first, then an active model only when configured policy permits. Import types only for `Api`, `Context`, and `Model`; define the local structural auth union above instead of importing `ResolvedRequestAuth` or any host auth type.

- [ ] **Step 6 (4 min): Implement reflected Pi registry completion**

Use a type guard on `Reflect.get(ctx.modelRegistry, "complete")`; when callable, pass the concrete model, host `Context`, and bounded options. This path is preferred and never reads a namespace completion property.

- [ ] **Step 7 (4 min): Implement reflected Prime namespace completion and auth**

`aiNamespace` defaults to statically imported `PiAi` but is injectable in unit tests. Type-guard `Reflect.get(aiNamespace, "completeSimple")`; require `ctx.modelRegistry.getApiKeyAndHeaders(model)` to return local `{ ok: true, apiKey?, headers?: Record<string, string | null> }`, sanitize by dropping null/non-string values into a fresh `Record<string, string>`, then call the reflected function with a fresh authenticated options object. Missing method/auth means pending/no egress. No dynamic or inline import is allowed. Ensure selected LLM destination is in the producer/worker authorized-policy intersection.

- [ ] **Step 8 (4 min): Add budgets, cancellation, and provider provenance**

Implement `completeMemory(input: { envelope: string; model: Model<Api>; hostContext: Context; maxInputTokens: number; maxOutputTokens: number; timeoutMs: number; memoryContext: MemoryCompletionContext; promptRevision: string; aiNamespace?: AiNamespaceLike })` with low temperature, strict JSON extraction delegated to Task 9 validation, bounded output, timeout, and abort. Record redacted provider/model IDs, policy ID/epoch/hash, prompt revision, and invocation date. A bridge error returns typed pending state without aborting the host turn. Active fallback is same-session/provider by default and requires both fallback flags for another provider.

- [ ] **Step 9 (4 min): Run LLM green and typecheck**

```bash
npx vitest run tests/unit/llm.test.ts
npm run typecheck
```

Expected: PASS for injected Pi and Prime paths, exact auth union, model/context/options calls, dedicated/fallback precedence, policy, cancellation, timeout, output bounds, and no static/dynamic completion import.

- [ ] **Step 10 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add package.json package-lock.json src/types.ts src/config.ts src/curation/llm.ts tests/unit/llm.test.ts dist
npm run check
git diff --cached --check
```

Expected: build/check pass and generated bridge artifacts are staged.

- [ ] **Step 11 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for one static root namespace import, reflected registry and namespace feature detection, local structural auth union, concrete `Model<Api>` resolution, no static namespace completion access, exact peer/dev dependency shape, no dynamic/inline imports, provider policy, and fail-open behavior. Exact host smokes in Task 14 must exercise both real paths. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add reflected portable memory LLM bridge"
```

Expected: commit succeeds only after both approvals.

### Task 7: Redacted Outbox Ingest into Host Qdrant

**Files:**
- Modify: `src/outbox/delivery.ts`, `src/qdrant/write.ts`, `src/clients/embeddings.ts`, `src/security/egress.ts`, `tests/unit/outbox.test.ts`
- Create: `tests/unit/ingest.test.ts`

**Interfaces:**
- **Consumes:** Task 3 Qdrant write primitives, Task 4 redacted episode/policy records, Task 5 durable jobs and `OutboxJobProcessor` seam, and Task 6 destination checks.

- **Produces:** the sole production `OutboxJobProcessor` plus at-least-once episode ingest: expiry/policy gate, BGE-M3 embedding only, named-vector insert-only writes, policy/episode read-back verification, retry/quarantine categories, and no-turn-blocking delivery behavior.

- [ ] **Step 1 (4 min): Write red policy/expiry ingest tests**

Assert each outbox job carries its redacted content-addressed policy record; ingest retrieves and validates that record, authorized-destination sets, expiry/skew, privacy epoch, provider replay, and quarantine behavior before any embedding/Qdrant call.

- [ ] **Step 2 (4 min): Write red embedding/vector ingest tests**

Assert BGE-M3 only, exactly 1024 finite components, named `semantic` vector, no secret/non-passed text, deterministic episode IDs, duplicate convergence, and embedding failure retry.

- [ ] **Step 3 (4 min): Write red bound-destination and Qdrant materialization tests**

Assert owner/schema/status/expiry/policy fields, `redaction_status`, final `secret_scan="passed"`, policy-record and episode readback after inserted/ignored writes, hash collision failure, partial acknowledgement retry, no-turn-blocking behavior, independent Qdrant-only and embedding-only revocations, and delayed-embedding revoke preventing the final episode write. The test must not pass an independent destination-ID allowlist:

```typescript
it("validates bound destinations, persists policy, then embeds and inserts the episode", async () => {
  const embed = vi.fn().mockResolvedValue(Array.from({ length: 1024 }, () => 0.25));
  const localPolicy = policy({
    qdrant: [{ id: "qdrant:pi", residency: "local", dataUse: "memory" }],
    embedding: [{ id: "embed:local", residency: "local", dataUse: "memory" }],
    llm: [{ id: "llm:local", residency: "local", dataUse: "memory" }],
  });
  const qdrant = fakeBoundQdrant({ destination: localPolicy.qdrant[0], policyHash: "policy-hash" });
  const embedding = fakeBoundEmbedding({ destination: localPolicy.embedding[0], embed });
  expect(() => bindQdrantDestination(fakeQdrantFactory({ endpoint: "qdrant-b" }), localPolicy.qdrant[0])).toThrow(/destination/);
  expect(() => bindEmbeddingDestination(fakeEmbeddingFactory({ endpoint: "embed-b" }), localPolicy.embedding[0])).toThrow(/destination/);
  await ingestPendingJobs({ job: redactedJob({ text: "safe [token redacted]" }), now: 100, localPolicy,
    qdrant, embedding, control: { read: vi.fn().mockResolvedValue({ state: "active", privacyEpoch: 0, coordinationPolicyEpoch: 0, policyHash: "policy-hash", revokedDestinationIds: [] }) }, maxClockSkewMs: 5 });
  expect(qdrant.insertAndReadback.mock.calls[0][0]).toMatchObject({ recordType: "processing_policy" });
  expect(embed).toHaveBeenCalledWith(expect.objectContaining({ model: "bge-m3", text: "safe [token redacted]" }));
  expect(qdrant.insertAndReadback).toHaveBeenCalledWith(expect.objectContaining({ recordType: "episode", owner_host: "pi", secret_scan: "passed", vector: { semantic: expect.any(Array) } }));
  expect(JSON.stringify(embed.mock.calls)).not.toMatch(/secret-token|Bearer/);
});
```

Repeat with `revokedDestinationIds: ["qdrant:pi"]` and `["embed:local"]` independently: the first makes no policy/Qdrant call, the second makes no embedding call; exact/local non-embedding paths remain safe. Add a delayed `embed` promise, mutate the control snapshot to `draining` or revoke one destination, and assert final episode insert/readback is skipped and the job is pending.

- [ ] **Step 4 (3 min): Run ingest tests red**

```bash
npx vitest run tests/unit/ingest.test.ts tests/unit/outbox.test.ts
```

Expected: FAIL with no embedding-backed outbox ingestion pipeline.

- [ ] **Step 5 (4 min): Implement policy-record gate and expiry check**

Export the exact bound-capability boundary from `outbox/delivery.ts`:

```typescript
export interface IngestControlReader { read(): Promise<{ state: "active" | "draining" | "retired"; privacyEpoch: number; coordinationPolicyEpoch: number; policyHash: string; revokedDestinationIds: readonly string[] }>; }
export interface BoundQdrantDestination {
  readonly destination: AuthorizedDestination;
  insertAndReadback(record: ProcessingPolicyRecord | EpisodeRecord): Promise<"inserted" | "existing">;
  retrieve<T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string): Promise<T | null>;
}
export interface BoundEmbeddingDestination { readonly destination: AuthorizedDestination; embed(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]>; }
export interface IngestInput { job: OutboxJob; now: number; localPolicy: ProcessingPolicy; qdrant: BoundQdrantDestination; embedding: BoundEmbeddingDestination; control: IngestControlReader; maxClockSkewMs: number; }
export interface DeliveryResult { delivered: number; pending: number; quarantined: number; }
export function ingestPendingJobs(input: IngestInput): Promise<DeliveryResult>;
export function createIngestProcessor(input: Omit<IngestInput, "job" | "now"> & { now: () => number }): OutboxJobProcessor;
```

`createIngestProcessor` is the only production implementation passed to Task 5's `OutboxDelivery`; its `process(job, { signal })` delegates to `ingestPendingJobs` and returns `delivered` only after policy and episode readbacks. `BoundQdrantDestination` and `BoundEmbeddingDestination` are opaque capabilities returned only by validated config/client factories; each factory binds one canonical destination identity to its endpoint/client and rejects mismatched identity/endpoint construction. The processor receives no sibling client or caller-supplied ID list. Before any egress, validate the actual bound Qdrant and embedding destination identities (ID, residency, and data-use) against the producer/job and `localPolicy` intersections and the control snapshot's bounded `revokedDestinationIds`; no caller-supplied independent ID list may authorize a different endpoint. Require `now + maxClockSkewMs < expiresAt` unless indefinite. Preserve this exact sequence: local policy/bound-capability validation → control snapshot read (active state, privacy/policy epoch/hash, and destination revocations) → policy-record `insert_only` through the bound Qdrant capability and canonical readback → control snapshot reread → revalidate active state, Qdrant/embedding revocations, and policy hash → bound embedding call → final control/privacy/policy reread → episode `insertAndReadback` through the same opaque Qdrant capability. A Qdrant-only revocation must prevent policy/Qdrant egress; an embedding-only revocation must prevent embedding while exact/local non-embedding lanes remain safe. If draining/retired state or a revoke changes state during an in-flight embedding, the embedding cannot be undone, but the final reread must prevent the distinct episode write and leave the job retryable. A missing/mismatched policy, revoked epoch, or unauthorized/expired bound destination is pending or quarantined and never egresses.

- [ ] **Step 6 (4 min): Implement BGE-M3 embedding and named-vector ingest**

For each policy-approved episode, call the existing OpenAI-compatible `EmbeddingsClient` with BGE-M3 and validate exactly 1024 finite components; never send a secret-scan non-passed text. Then call the bound memory-record writer's typed `insertOnly` with a named `semantic` vector and retrieve/read back the episode. A Qdrant or embedding failure leaves the immutable job pending with category/backoff; unauthorized/expired jobs become non-reversible quarantine/audit entries and never egress.

- [ ] **Step 7 (4 min): Implement idempotent payload materialization**

Materialize all v2 episode fields and no extras that could leak raw data. Preserve `redaction_status` as `unchanged|redacted` for safe content, but only materialize final `secret_scan="passed"`; scanner `rejected|error` jobs remain quarantined and never enter embeddings/Qdrant. Include `record_type="episode"`, `owner_host`, schema revision, project/scope/role/session/turn/event, bounded tool/error fields, model/provider destination IDs redacted, processing policy ID, privacy/coordination epochs, `expires_at`, `status="active"`, `secret_scan="passed"`, and canonical `content_hash`. Use `insert_only` and retrieve after both inserted and ignored outcomes; same hash is success, different hash is a collision requiring human-visible status. The worker can process duplicate jobs concurrently because record IDs and hashes converge.

- [ ] **Step 8 (4 min): Run ingest green and typecheck**

```bash
npx vitest run tests/unit/ingest.test.ts tests/unit/outbox.test.ts
npm run typecheck
```

Expected: PASS for policy intersection, expiry, embedding budget/dimension, no-secret egress, retries, duplicate IDs, hash collisions, and fail-open job durability; TypeScript exits 0.

- [ ] **Step 9 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/outbox/delivery.ts src/qdrant/write.ts src/clients/embeddings.ts src/security/egress.ts tests/unit/outbox.test.ts tests/unit/ingest.test.ts dist
npm run check
git diff --cached --check
```

Expected: regenerated ingest/delivery artifacts are staged and `npm run check` passes.

- [ ] **Step 10 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for the sole `OutboxJobProcessor` wiring, no direct episode-writer delivery bypass, redaction-before-embedding, final control reread after embedding before episode write, exact bound Qdrant/embedding destination identity and capability pairing, producer/local intersection plus revocation checks, policy-record and episode insert/readback sequence, exact BGE-M3-only embedding role, policy/expiry/privacy checks, deterministic IDs, named vectors, read-back collisions, and durable retry/fail-open behavior. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: ingest episodes through durable outbox"
```

Expected: commit succeeds only after both approvals.


### Task 8: Distributed Control, Leases, Jobs, Fencing, Reconciliation, and Tombstones

**Files:**
- Modify: `src/types.ts`, `src/qdrant/write.ts`, `src/outbox/delivery.ts`
- Create: `src/coordination/control.ts`, `src/coordination/leases.ts`, `src/coordination/jobs.ts`, `src/coordination/reconcile.ts`, `src/coordination/tombstones.ts`, `tests/unit/coordination.test.ts`

**Interfaces:**
- **Consumes:** Task 2 epochs/policies/records and deterministic IDs, Task 3 strong update/CAS primitives, Task 5 durable delivery, and Task 7 episode ingest.

- **Produces:** a no-daemon distributed protocol for control policy state, lease/fencing claims, explicit membership jobs/proposals, overlap reconciliation, coverage, tombstone barriers, and stale-worker invalidation.

- [ ] **Step 1 (4 min): Write red control/lease/fencing tests**

Use a fake Qdrant implementing `insert_only`, `update_only`, `update_filter`, strong ordering, read consistency, delayed responses, policy drain, epoch activation, concurrent acquire, renew, steal/release, and stale fencing. Assert one owner-independent `collection_control` point and old lease quiescence.

- [ ] **Step 2 (4 min): Write red job/proposal/reconcile/tombstone tests**

Cover explicit sorted memberships, accepted proposal CAS, overlap coverage, late episode scans, content/state future recurrence, occurrence scope, provenance closure, final tombstone filtering, no timestamp-cursor coverage, and domain-separated canonical target IDs: the exact `H(owner_host,"tombstone",target_id)` formula remains, while mismatched scope/target types and colliding textual raw selectors fail closed.

```typescript
it("fences a stale lease and accepts only one proposal", async () => {
  const q = fakeControlStore();
  const first = await claimLease(q, { jobId: "job-1", ownerId: "node-a", now: 10, leaseMs: 30, policyEpoch: 1 });
  const stolen = await claimLease(q, { jobId: "job-1", ownerId: "node-b", now: 100, leaseMs: 30, policyEpoch: 1 });
  expect(stolen?.fencingToken).toBeGreaterThan(first?.fencingToken ?? 0);
  await expect(acceptProposal(q, { jobId: "job-1", fencingToken: first?.fencingToken ?? 0, proposalId: "old", policyEpoch: 1 })).rejects.toThrow(/fencing|stale/);
  expect(await acceptProposal(q, { jobId: "job-1", fencingToken: stolen?.fencingToken ?? 0, proposalId: "new", policyEpoch: 1 })).toBe(true);
});

it("domain-separates canonical scope targets without changing the tombstone formula", () => {
  const occurrenceTarget = observationId(1, "content", "00000000-0000-0000-0000-000000000001", "s");
  const contentTarget = contentId("policy", "state", "same");
  const stateTarget = stateKey({ host: "pi", scope: "project", projectId: "p", category: "fact", subject: "same", predicate: "same" });
  const ids = [occurrenceTarget, contentTarget, stateTarget].map(target => tombstoneId("pi", target));
  expect(new Set(ids).size).toBe(3);
  expect(() => createTombstone({ scope: "content", targetId: occurrenceTarget })).toThrow(/scope|target/);
});
```

- [ ] **Step 3 (3 min): Run coordination tests red**

```bash
npx vitest run tests/unit/coordination.test.ts
```

Expected: FAIL with missing control/lease/job/reconcile/tombstone modules and no fencing protocol.

- [ ] **Step 4 (4 min): Implement the mutable collection-control point**

Create `coordination/control.ts` with owner-independent control ID `deterministicUuid("pi-qdrant-memory-v2", "collection_control")` and `readControl`, `initializeControl`, `readForUpdate`, and `beginForgetBarrier`. This point is distinct from immutable `collection_metadata`; Task 3's admin init calls the Task 3 `insertInitialControl` primitive for v0, and this module's `initializeControl` rereads that point; only this module's typed CAS helpers may perform later mutations. Store only immutable `schema_revision`/contract-hash reference plus active generation/version, privacy epoch, coordination policy hash/epoch/state, last forget barrier, and scan cursor; never duplicate mutable vector/schema contract from `collection_metadata`. State transitions use one-point OCC/CAS with `ordering=strong&wait=true`; control creation is insert-only and every response is reread.

- [ ] **Step 5 (4 min): Implement policy draining and epoch activation**

Implement `beginPolicyDrain` as CAS active→draining with `active_generation=null` and derived-current visibility disabled. Workers cannot claim jobs or begin egress while draining. Call `waitForOldLeasesToQuiesce({ retiredEpoch, maxLeaseMs, maxClockSkewMs })`, polling strong control/lease state until every old lease is released or conservatively expired; then await the configured maximum LLM timeout (`memoryModel.timeoutMs`) before CAS-incrementing the coordination policy epoch/hash and returning active. Reread control before and after each transition; a late conforming worker can only leave retired/invisible output. Forget barriers use the same control point and privacy epoch.

- [ ] **Step 6 (4 min): Implement fenced leases**

Create `leases.ts` where initial claim is `insert_only`; steal of an expired lease uses `update_only`, `update_filter` on owner/status/version/expiry, strong ordering/wait, then rereads owner/version/fencing token. Conservative expiry includes `maxClockSkewMs`; skew may duplicate work but cannot authorize stale publication. Renew/release reread after update.

- [ ] **Step 7 (4 min): Implement explicit jobs and proposal CAS**

Create `jobs.ts` with explicit sorted episode membership, extractor revision, coordination policy hash/epoch, serialized producer-policy intersection ID, lease, fencing token, state, proposal ID/hash, and current update version. Deterministic `jobId` covers membership/policy/extractor. `writeProposal` is immutable; `acceptProposal` CASes exactly one proposal only when hash/epochs/token match. A physically stored stale proposal is never active. Root-only workers are enforced here, not by local lock.

```typescript
export interface ControlStore { read(): Promise<ControlRecord>; compareAndSwap(expectedVersion: number, next: ControlRecord): Promise<boolean>; }
export interface LeaseClaim { jobId: string; ownerId: string; version: number; fencingToken: number; expiresAt: string; }
export function claimLease(control: ControlStore, input: { jobId: string; ownerId: string; now: number; leaseMs: number; policyEpoch: number }): Promise<LeaseClaim | null>;
export function acceptProposal(control: ControlStore, input: { jobId: string; proposalId: string; fencingToken: number; policyEpoch: number }): Promise<boolean>;
```

- [ ] **Step 8 (4 min): Implement coverage reconciliation**

Create `reconcile.ts` with cursor/bucket overlap optimization and ID-based truth: scan episodes in bounded slices, batch-retrieve `coverage` IDs for the extractor revision, enqueue missing/late episodes, and allow full operator reconciliation. Do not claim a global ingest sequence or use worker-created timestamps as causal order.

- [ ] **Step 9 (4 min): Implement tombstone closure and final visibility**

Create `tombstones.ts` with deterministic `H(owner_host,"tombstone",target_id)` records, source/provenance closure enumeration, `readTombstones`, `isVisibleAfterTombstoneCheck`, and privacy-epoch checks. Wire `src/outbox/delivery.ts` to read control/tombstones before any policy-record insert or embedding, so the Task 7 control-only first-return guard gains the post-Task-8 tombstone barrier. Occurrence scope targets one episode/observation; content scope matches current and future `content_id`; state scope matches current and future `state_key` recurrence. Tombstone insertion is immutable strong/wait; final retrieval/accept/materialize/publish checks batch-read tombstones with configured majority consistency. Derived records without complete provenance are invisible fail-closed. Cleanup is eventual/idempotent; stale reinsertion can be physically present but not logically visible.

- [ ] **Step 10 (4 min): Run coordination green and typecheck**

```bash
npx vitest run tests/unit/coordination.test.ts
npm run typecheck
```

Expected: PASS for concurrent claims, renew/steal/release, stale fencing, CAS races, draining epochs, overlapping memberships, late episodes, coverage, tombstone closure, and fail-closed final checks; TypeScript exits 0.

- [ ] **Step 11 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/types.ts src/qdrant/write.ts src/outbox/delivery.ts src/coordination/control.ts src/coordination/leases.ts src/coordination/jobs.ts src/coordination/reconcile.ts src/coordination/tombstones.ts tests/unit/coordination.test.ts dist
npm run check
git diff --cached --check
```

Expected: generated coordination artifacts are staged; build/check and cached whitespace check pass.

- [ ] **Step 12 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for strong control writes, OCC predicates, fencing, no lock-as-authority, explicit job membership/proposal acceptance, coverage ID truth, policy/privacy drain, tombstone/provenance closure, and no cross-point transaction claims. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add distributed memory coordination"
```

Expected: commit succeeds only after both approvals.


### Task 9: Temporal Curation, Structured Prompts, Validation, and Root Worker Materialization

**Files:**
- Modify: `src/coordination/jobs.ts`, `src/coordination/control.ts`, `src/domain/records.ts`, `src/security/egress.ts`
- Create: `src/curation/prompt.ts`, `src/curation/validate.ts`, `src/curation/temporal.ts`, `src/curation/worker.ts`, `tests/unit/curation.test.ts`, `tests/unit/temporal.test.ts`

**Interfaces:**
- **Consumes:** Task 2 record/policy/ID contracts, Task 6 LLM bridge, Task 7 ingested episodes and opaque `BoundEmbeddingDestination`, and Task 8 leases/jobs/control/tombstone barriers.

- **Produces:** trigger scheduling, structured untrusted envelopes, strict curation validation, at-least-once proposals, root-only fenced workers, immutable observations/evidence, temporal current OCC, conflict manifests, historical A→B→A folding, and coverage.

- [ ] **Step 1 (3 min): Write red prompt-envelope tests**

Assert only bounded redacted episode data, explicit untrusted delimiters, policy/provider envelope, no tools/system authority, no injected memory, and token budgets.

- [ ] **Step 2 (3 min): Write red validator/evidence tests**

Assert strict JSON, known categories/scopes, bounded lists, direct-user evidence for preference/correction, and rejection of malformed or tool-invented standing instructions:

```typescript
it("rejects a tool output that invents a standing instruction", () => {
  expect(() => validateCurationResult({ items: [{ category: "preference", scope: "project", subject: "editor", predicate: "must_use", value: "vim", evidence: ["tool-1"] }] }, { directUserEpisodeIds: new Set(), knownEpisodeIds: new Set(["tool-1"]) })).toThrow(/direct user evidence/);
});
```

- [ ] **Step 3 (3 min): Write red worker/temporal tests**

Assert root/child triggers, explicit policy grouping, proposal races, same-session causal order, cross-machine skew conflict, late events, current OCC, A→B→A history folding, policy-epoch migration where the same evidence receives new policy-specific state/current IDs while the old view is hidden, and derived-text embedding through an opaque bound capability. Delay embedding, revoke its destination or policy epoch, and assert no current write; scanner reject/error must retry/quarantine without text egress.

- [ ] **Step 4 (3 min): Run curation/temporal tests red**

```bash
npx vitest run tests/unit/curation.test.ts tests/unit/temporal.test.ts
```

Expected: FAIL with missing prompt, validator, temporal fold, and worker implementations.

- [ ] **Step 5 (3 min): Implement bounded untrusted curation prompts**

Create `curation/prompt.ts` with recent explicit episode membership, max input tokens, policy/provider envelope, prompt revision, and `<untrusted-data>` delimiters. Exclude system/developer instructions, injected memory, tool access, vectors, keys, and unredacted payload.

- [ ] **Step 6 (4 min): Implement strict curation-result validation**

Create `curation/validate.ts` with JSON-only parsing, bounded strings/lists, known categories (`preference`, `correction`, `convention`, `fact`, `failure`, `learning`), allowed scopes, evidence episode existence, and no standing instruction from tool output. Tool output cannot supply direct evidence for a preference or correction.

- [ ] **Step 7 (4 min): Implement trigger discovery and root-only fenced worker**

Create `curation/worker.ts` to enqueue at root turn trigger 10, tool trigger 15, before compaction; shutdown only persists pending work. It accepts explicit sorted episode IDs, maximum one effective claim per host/batch, and validates host roles. Prime resolves child first from `sessionManager.getHeader()?.rlmDepth`, then `RLM_DEPTH`; Pi 0.84.1 has no built-in `PI_SUBAGENT_*` fields, so resolves `sessionManager.getHeader()?.parentSession` as the sole host child signal. Accept `PI_SUBAGENT_CHILD=1`/`PI_SUBAGENT_DEPTH>0` only as optional extension-wrapper markers (never assume them from Pi); validate them when present. Invalid or contradictory values disable root curation/RAPTOR; children may ingest/search but cannot claim them. Before LLM egress and proposal acceptance, reread control privacy/coordination epochs and the authorized-policy intersection; split explicit jobs by compatible policy groups when possible, otherwise leave them pending. Failed LLM/validation leaves a retryable job and episodes searchable.

- [ ] **Step 8 (4 min): Implement observation/evidence materialization and policy migration**

Create the observation/evidence portion of `curation/temporal.ts`: after strict result validation and an accepted active-policy proposal, intersect source/local/active policies and use only the opaque `BoundEmbeddingDestination` from Task 7. Validate its actual ID/residency/dataUse and revocation state, structurally redact the canonical curated text, run the final secret scanner, and allow only `secret_scan="passed"` text to BGE-M3 embedding with exactly 1024 finite values. Reread control after the embedding call, then insert observations/evidence links and policy-specific state/current IDs only if epochs/destinations still match. A scanner reject/error, embedding failure, or policy/revoke change leaves the job retryable/quarantined and makes no current write; no active-model/LLM endpoint may substitute for embeddings. When the same evidence arrives under a new policy epoch, create new IDs and hide the old view without mutating old records.

- [ ] **Step 9 (4 min): Implement conflict manifests and current OCC**

Order within a session by event/turn sequence; across sessions compare `(event_at, episode_id, content_id)` only when event times differ by more than `maxClockSkewMs`. Within the skew window, a different value creates a content-addressed conflict manifest by CAS rather than choosing a winner. Update `curated_current` by OCC only for later effective order; a late older observation remains history and cannot rewind current. Equal content/primary evidence converges across jobs.

- [ ] **Step 10 (4 min): Implement A→B→A history folding and primary evidence**

Fold by consecutive canonical content so A→B→A preserves two A intervals, with primary evidence ordered by `(event_at, episode_id)`, no superseded state reuse/cycle, and derived `valid_from`/`valid_to` without mutating observations. Semantic near-duplicates remain best-effort only.

- [ ] **Step 11 (4 min): Implement coverage writes and job completion**

Write coverage for every accepted observation/extractor revision, mark jobs complete only after immutable observation/evidence/current writes and read-back succeed, and leave partial/failed work retryable. Overlapping memberships may add observations but fold to one logical segment; stale outputs remain retired/invisible or abandoned.

- [ ] **Step 12 (4 min): Run curation/temporal green and typecheck**

```bash
npx vitest run tests/unit/curation.test.ts tests/unit/temporal.test.ts
npm run typecheck
```

Expected: PASS for triggers, root/child gating, malformed result rejection, direct evidence, policy intersections, proposal races, accepted-proposal derived embedding, exactly 1024 finite BGE-M3 values, scanner reject/error quarantine, post-embedding control reread, revoked-destination delayed calls, jobs with overlapping membership, same-session causal order, cross-machine skew conflicts, late events, and A→B→A history; TypeScript exits 0.

- [ ] **Step 13 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/coordination/jobs.ts src/coordination/control.ts src/domain/records.ts src/security/egress.ts src/curation/prompt.ts src/curation/validate.ts src/curation/temporal.ts src/curation/worker.ts tests/unit/curation.test.ts tests/unit/temporal.test.ts dist
npm run check
git diff --cached --check
```

Expected: regenerated curation/temporal artifacts are staged and check passes.

- [ ] **Step 14 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for strict untrusted prompts/JSON, structural redaction plus final scanning before derived embedding, opaque bound embedding capability and common policy intersection, exactly 1024 finite BGE-M3 vectors, post-call control reread, no active-model substitution, evidence requirements, exact root/child gating, policy/fencing rereads, causal versus skew ordering, conflict handling, immutable observations, current OCC, and A→B→A semantics. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add temporal autonomous curation"
```

Expected: commit succeeds only after both approvals.


### Task 10: Deterministic RAPTOR Core, UMAP/GMM Clustering, Content-Addressed Manifests, and CAS Publication

**Files:**
- Modify: `src/types.ts`, `src/coordination/control.ts`
- Create: `src/raptor/random.ts`, `src/raptor/umap.ts`, `src/raptor/gmm.ts`, `src/raptor/cluster.ts`, `src/raptor/manifest.ts`, `src/raptor/builder.ts`, `src/raptor/publication.ts`, `src/vendor/umap-license-apache-2.0.txt`, `tests/unit/raptor.test.ts`, `tests/unit/manifests.test.ts`

**Interfaces:**
- **Consumes:** Task 2 records/IDs/policies, Task 6 reflected LLM bridge and opaque `BoundLlmDestination`, Task 7 episode vectors and opaque `BoundEmbeddingDestination`, Task 8 control/fencing/tombstones, and Task 9 curation provenance.

- **Produces:** deterministic RAPTOR generations with UMAP `1.4.0`, injected xoshiro128**, diagonal GMM EM/BIC/soft membership, stable fallback/termination, bounded Merkle manifests, immutable summary nodes, reuse by membership hash, and one active-generation CAS publication.

- [ ] **Step 1 (3 min): Write red PRNG/UMAP fixtures**

Freeze SHA-256 seed expansion, xoshiro128** replay, global/local UMAP random injection, neighbor clamps, zero-variance handling, and N=0/1/2 base cases.

- [ ] **Step 2 (4 min): Write red GMM/BIC fixtures**

Freeze diagonal variance floor, EM iteration/tolerance, candidate K range, BIC formula, soft membership threshold, max-prob membership, singular/non-finite fallback, and deterministic ID-sort/token-greedy partition:

```typescript
it("matches the frozen diagonal GMM BIC formula", () => {
  const fit = fitDiagonalGmm([[0, 0], [1, 1], [9, 9]], { seed: 7, maxClusters: 2 });
  expect(fit.components).toBe(2);
  expect(fit.bic).toBeCloseTo(-2 * fit.logLikelihood + fit.parameterCount * Math.log(3), 10);
  expect(fit.memberships.every(row => Math.abs(row.reduce((a, b) => a + b, 0) - 1) < 1e-9)).toBe(true);
});
```

- [ ] **Step 3 (4 min): Write red DAG/manifest/publication fixtures**

Freeze soft-membership DAG acyclicity, evidence closure, content-addressed chunk/Merkle hashes, summary reuse, stale builder rejection, one-winner control CAS publication, and delayed LLM/embedding revocation races; a policy change between calls must leave nodes pending/invisible and prevent publication:

```typescript
it("publishes exactly one generation by control CAS", async () => {
  const q = fakeControlStore({ version: 4, activeGeneration: "old" });
  const base = await readControl(q);
  const [a, b] = await Promise.all([
    publishGeneration(q, { control: base, generation: generation("a"), fencingToken: 3 }),
    publishGeneration(q, { control: base, generation: generation("b"), fencingToken: 4 }),
  ]);
  expect([a, b].filter(Boolean)).toHaveLength(1);
});
```

- [ ] **Step 4 (3 min): Run RAPTOR tests red**

```bash
npx vitest run tests/unit/raptor.test.ts tests/unit/manifests.test.ts
```

Expected: FAIL with missing random/UMAP/GMM/manifest/builder/publication modules.

- [ ] **Step 5 (4 min): Preserve the umap-js notice and implement seeded PRNG**

Use the exact pinned `umap-js@1.4.0` package and copy its tarball `LICENSE` text verbatim to `src/vendor/umap-license-apache-2.0.txt`, recording the package/version and Apache-2.0 notice in release docs. Do not trust inconsistent package metadata without the tarball notice audit. Create `raptor/random.ts` with SHA-256(seed) -> 128 bits, xoshiro128** state, deterministic integer/float methods, and adapters for both UMAP's injectable `random` and GMM k-means++ initialization. The seed is explicit config or hash of collection + policy revision and is stored in generation identity.

- [ ] **Step 6 (4 min): Implement global and local UMAP reduction**

Create `raptor/umap.ts` around `umap-js@1.4.0`, passing the seeded random function. Run global reduction over the frozen manifest first, then local reduction for each candidate cluster. Clamp global neighbors from the root N and local neighbors to `2..N-1`, dimensions to configured 1..64, and zero-variance coordinates to zero/excluded. Tests assert both global and local calls receive the persisted seed and bounded neighbors.

- [ ] **Step 7 (5 min): Implement diagonal GMM EM, BIC, and soft membership**

Create `raptor/gmm.ts` with standardized coordinates, diagonal covariance, variance floor `1e-6`, max 100 EM iterations, tolerance `1e-4`, finite/singular rejection, soft probability rows, candidates `1..min(50,N-1)` (also bounded by configured max), and exact:

```typescript
BIC = -2 * logLikelihood + p * Math.log(N);
p = K * (2 * D) + (K - 1);
```

Select minimum valid BIC, retain membership only when probability is at least the configured threshold (default `.10`), and always add each point to its maximum-probability cluster. Deduplicate identical memberships. No Python or external GMM library.

- [ ] **Step 8 (4 min): Implement recursive DAG clustering and termination**

Create `raptor/cluster.ts` with base cases: N=0 no generation; N=1 root as-is; N=2 deterministic one cluster if budget permits otherwise separate roots; N>=3 configured components `min(configured,N-2,embeddingDimension)`. Run global then local clustering. Build a DAG where every edge increases exactly one level and reject cycles. Stop on max level, no progress, unchanged membership, or summary budget overflow after recluster.

- [ ] **Step 9 (4 min): Implement RAPTOR policy grouping and stable fallback**

Before summary generation, intersect all source authorized policies. If a cluster has no common LLM/embedding destination, split it into compatible-policy groups or leave it flat/pending; for global multi-machine RAPTOR require a common explicitly permitted dedicated LLM and embedding destination, never a node-local loopback inherited from the coordinator. If embeddings equal, GMM/UMAP is invalid, or no finite fit exists, sort IDs lexicographically and greedily partition under the token budget. Bind one opaque `BoundLlmDestination` and one opaque `BoundEmbeddingDestination` from that common source/local/active intersection; never substitute an active-model or node-local endpoint.

- [ ] **Step 10 (3 min): Freeze clustering/retrieval benchmark thresholds**

Extend `tests/unit/raptor.test.ts` with a frozen corpus and flat-retrieval baseline. Assert same-seed membership-edge Jaccard `= 1.0`, manifest leaf closure `= 1.0`, frozen-query `recall@5 >= flat baseline - 0.02`, and evidence recall `>= 0.95` for eligible leaves; record separate quality observations rather than claiming numerical equivalence to Python RAPTOR. The fixture checks global/local clustering, threshold/max-prob membership, fallback partition, and retrieval evidence descent.

- [ ] **Step 11 (4 min): Implement content-addressed manifest chunks**

Create `raptor/manifest.ts` with sorted eligible leaf IDs, bounded manifest chunks, Merkle root/hash, membership hash, and policy/algorithm/prompt/model/seed identity. Never put an unlimited membership payload in one point.

- [ ] **Step 12 (5 min): Implement generation builder and summary reuse**

Create `raptor/builder.ts` to snapshot active policy/privacy epoch, freeze the manifest, build only modified clusters/ancestors, reuse validated summary/vector nodes when membership+prompt+model+algorithm hash matches, and for each new summary use the bound protocol: call opaque `BoundLlmDestination`, strict-validate its result, apply Task 4 structural redaction and final secret scanning, require `secret_scan="passed"`, reread control, call opaque `BoundEmbeddingDestination` with BGE-M3 and exactly 1024 finite values, reread control again, then write immutable `raptor_summary` text/vector nodes with generation/job/fencing/expiry/provenance. A scanner reject/error or policy/epoch/destination change between LLM→embedding or embedding→write leaves the job retryable and nodes pending/invisible; it never publishes or substitutes an active-model/LLM endpoint for embedding. New episodes remain directly searchable until the next generation.

- [ ] **Step 13 (4 min): Implement publication CAS and orphan visibility**

Create `raptor/publication.ts`: reread control, tombstones, policy/coordination/privacy epochs and lease token before and after node writes; CAS only the control point's version/base generation/epoch to set one active generation. Losing nodes remain immutable and invisible because only the active control manifest is reachable; orphan is derived after retention, never a mutable payload. Forget or policy drain sets active generation null and requires rebuild. A stale builder cannot replace a newer generation.

```typescript
export interface Generation { id: string; manifestRoot: string; baseGeneration: string | null; privacyEpoch: number; coordinationPolicyEpoch: number; status: "building" | "published" | "retired"; }
export function publishGeneration(store: ControlStore, input: { control: ControlRecord; generation: Generation; fencingToken: number }): Promise<boolean>;
```

- [ ] **Step 14 (4 min): Run RAPTOR green and typecheck**

```bash
npx vitest run tests/unit/raptor.test.ts tests/unit/manifests.test.ts
npm run typecheck
```

Expected: PASS for deterministic replay, seed persistence, GMM/BIC fixtures, soft membership/DAG invariants, all base cases/fallbacks/termination, UMAP random injection, license notice presence, bounded manifests, opaque common-policy LLM/embedding bindings, structural redaction/final scan, exactly 1024 finite BGE-M3 vectors, delayed revoke races, summary reuse, stale builder, and single-winner publication; TypeScript exits 0.

- [ ] **Step 15 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/types.ts src/coordination/control.ts src/raptor/random.ts src/raptor/umap.ts src/raptor/gmm.ts src/raptor/cluster.ts src/raptor/manifest.ts src/raptor/builder.ts src/raptor/publication.ts src/vendor/umap-license-apache-2.0.txt tests/unit/raptor.test.ts tests/unit/manifests.test.ts dist
npm run check
git diff --cached --check
```

Expected: generated RAPTOR artifacts and the Apache-2.0 notice are staged; check passes.

- [ ] **Step 16 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for `umap-js@1.4.0`, exact tarball notice, xoshiro injection, GMM formulas/parameters, base cases/fallback/termination, DAG/evidence, opaque common-policy LLM/embedding bindings, structural redaction/final scan before derived embedding, exactly 1024 finite BGE-M3 vectors, control rereads and delayed revoke races, content-addressed chunks/reuse, immutable summaries, fencing/privacy/policy CAS, and no stale publication. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add deterministic RAPTOR generations"
```

Expected: commit succeeds only after both approvals.


### Task 11: Guarded Hybrid Retrieval, Evidence Descent, and Ephemeral Injection

**Files:**
- Modify: `src/retrieval/filters.ts`, `src/retrieval/merge.ts`, `src/retrieval/search.ts`, `src/query.ts`, `src/cache.ts`, `src/format.ts`, `src/service.ts`, `src/tool.ts`, `tests/unit/retrieval.test.ts`, `tests/unit/query.test.ts`, `tests/unit/format.test.ts`, `tests/unit/tool.test.ts`, `tests/unit/service.test.ts`, `tests/unit/clients.test.ts`; remove only readonly-Qdrant cases from `clients.test.ts`, retaining HTTP/embeddings coverage
- Delete: `src/clients/qdrant-readonly.ts` and generated `dist/clients/qdrant-readonly.*`

**Interfaces:**
- **Consumes:** Task 2 project/policy records, Task 3 Qdrant reads, Task 4 capture exclusions, Task 7 opaque `BoundEmbeddingDestination`, Task 8 tombstone/privacy final checks, Task 9 temporal views, and Task 10 active RAPTOR manifest/evidence.

- **Produces:** guarded current/historical/episode/curated/RAPTOR/dense/full-text hybrid retrieval; RRF/diversity/dedup; policy-safe evidence descent; exact model-callable arguments; untrusted ephemeral auto-recall that never enters JSONL or capture.

- [ ] **Step 1 (4 min): Write red filter/scope tests**

Assert every lane has host/project/scope/expiry/status/secret/policy constraints, child project-only behavior, root global opt-in, local-only isolation, invalid project fail-closed, and no model-controlled infrastructure fields.

- [ ] **Step 2 (4 min): Write red lane/fusion/evidence tests**

Cover curated-current/history, dense episode, full-text/tool/error exact, active RAPTOR descent, temporal bounds, RRF/diversity/dedup, policy intersection, and final tombstone/privacy checks:

```typescript
it("rejects cross-project and unauthorized destination hits after scoring", async () => {
  const result = await retriever.search({ query: "alpha", project: project("p1"), modelDestinationId: "node-b", mode: "all" });
  expect(result.hits).toEqual([]);
  expect(qdrant.search.mock.calls.every(([request]) => request.filter.must.some((x: { key: string }) => x.key === "owner_host"))).toBe(true);
});
```

Add a dense-query test with a bound embedding destination whose actual ID/residency/dataUse is revoked or outside the current runtime policy/control: assert the embedding client is not called and dense search returns zero, while an exact/local lane may still return its safely filtered candidate.

- [ ] **Step 3 (4 min): Write red formatter/tool/lifecycle tests**

Assert exact tool arguments `query, limit, mode, after, before`, strict RFC3339 `after`/`before` parsing (valid offsets accepted, malformed dates and `after > before` rejected), untrusted escaping/budgets, no hidden details, copied-message ephemeral injection, no JSONL mutation, and fail-open errors:

```typescript
it("exposes only the v2 tool arguments", () => {
  expect(Object.keys(memorySearchParameters.properties).sort()).toEqual(["after", "before", "limit", "mode", "query"]);
});
```

- [ ] **Step 4 (3 min): Run retrieval/tool tests red**

```bash
npx vitest run tests/unit/retrieval.test.ts tests/unit/query.test.ts tests/unit/format.test.ts tests/unit/tool.test.ts tests/unit/service.test.ts tests/unit/clients.test.ts
```

Expected: FAIL with v1 two-lane filters/arguments and no policy/tombstone/evidence guards.

- [ ] **Step 5 (4 min): Implement lane filters and exact query scope**

Rewrite filters to require exact host, project identity/scope, `expires_at=null OR expires_at>now+maxClockSkewMs`, active/status/secret scan where applicable, and coordination epoch for current/RAPTOR. Children are forced `scope=project`; a root may include same-host `global` only when config explicitly sets `project_and_global`. No request accepts host, collection, endpoint, credential, scope, or policy from model args. Unknown project identity disables auto-recall/child search fail-closed.

- [ ] **Step 6 (4 min): Implement current and historical curated lanes**

Rewrite the curated lane for `curated_current` and historical observations, with policy epoch/valid interval labels, temporal bounds, current/history modes, provenance episode expansion, and deterministic dedup.

- [ ] **Step 7 (4 min): Implement dense episode and exact-match lanes**

Add dense BGE-M3 episode search plus full-text/tool-name/error-code/fingerprint exact candidates, with per-lane raw thresholds, expiry/scope filters, normalized scores, and deterministic candidate ordering.

- [ ] **Step 8 (5 min): Implement RAPTOR descent and reciprocal-rank fusion**

Add active-generation RAPTOR collapsed search, DAG descent to concrete evidence, RRF normalization, diversity, deduplication, and final sort. `mode` supports exactly `all|current|historical|episodes|curated|raptor`; summaries never replace evidence.

- [ ] **Step 9 (4 min): Implement tombstone/policy final check and ephemeral formatter**

Before returning any hit, batch-read tombstones/provenance with configured majority consistency, recheck privacy epoch/control, and require active-model destination in every result's processing-policy intersection. A failed final check returns zero hits fail-closed. Format only bounded redacted text/provenance/temporal labels inside `<memory-context trust="untrusted">`; escape delimiters and never include hidden uncapped text in tool details.

- [ ] **Step 10 (4 min): Implement exact tool and fail-open service behavior**

Use the exact schema:

```typescript
const memorySearchParameters = Type.Object({
  query: Type.String({ minLength: 1, maxLength: 4000 }),
  limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 10 })),
  mode: Type.Optional(Type.Union([Type.Literal("all"), Type.Literal("current"), Type.Literal("historical"), Type.Literal("episodes"), Type.Literal("curated"), Type.Literal("raptor")])),
  after: Type.Optional(Type.String()), before: Type.Optional(Type.String()),
}, { additionalProperties: false });
```

`memory_search` executes only `query, limit?, mode?, after?, before?`, returns bounded untrusted details, and hides internal failures. Auto-recall copies the host message array, removes only the extension's prior custom block, appends one ephemeral block, and never mutates branch/session JSONL or Qdrant. It is enabled only for Pi root/Prime root; child/subagent auto-recall is off, explicit project-only search remains available. All infra/formatter/cache errors are fail-open without injected error text.

- [ ] **Step 11 (4 min): Run retrieval green and typecheck**

```bash
npx vitest run tests/unit/retrieval.test.ts tests/unit/query.test.ts tests/unit/format.test.ts tests/unit/tool.test.ts tests/unit/service.test.ts tests/unit/clients.test.ts
npm run typecheck
```

Expected: PASS for all lane filters, exact modes/arguments, root/global/child scope, destination policy, expiry/tombstones/epoch, RRF/dedup/evidence descent, temporal labels, untrusted formatting, no JSONL recapture, and fail-open service; TypeScript exits 0.

- [ ] **Step 12 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/retrieval/filters.ts src/retrieval/merge.ts src/retrieval/search.ts src/query.ts src/cache.ts src/format.ts src/service.ts src/tool.ts src/clients/qdrant-readonly.ts tests/unit/clients.test.ts tests/unit/retrieval.test.ts tests/unit/query.test.ts tests/unit/format.test.ts tests/unit/tool.test.ts tests/unit/service.test.ts dist
npm run check
git diff --cached --check
```

Expected: generated retrieval/service/tool artifacts are staged and check passes.

- [ ] **Step 13 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for every required safety filter, policy/tombstone final check, exact five-lane behavior/modes, RAPTOR evidence descent, project/child scope, no model-controlled infrastructure args, ephemeral injection/no recapture, and fail-open errors. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add guarded hybrid memory retrieval"
```

Expected: commit succeeds only after both approvals.


evocation fail-closed behavior with exact/local lane safety, every required safety filter, policy/tombstone final check, exact five-lane behavior/modes, RAPTOR evidence descent, project/child scope, no model-controlled infrastructure args, ephemeral injection/no recapture, and fail-open errors. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add guarded hybrid memory retrieval"
```

Expected: commit succeeds only after both approvals.


### Task 12: Extension Lifecycle Wiring, Capture Scheduling, Shutdown Recovery, and Host Eligibility

**Files:**
- Modify: `src/extension.ts`, `src/host.ts`, `src/service.ts`, `tests/unit/extension.test.ts`, `tests/unit/host.test.ts`

**Interfaces:**
- **Consumes:** Task 4 capture lifecycle API, Task 5 outbox/delivery, Task 6 LLM bridge, Task 8/9 worker scheduling, Task 11 retrieval/service/tool, and exact Pi/Prime compatibility pins.

- **Produces:** one extension registration with exact lifecycle handlers, session activation cutoff, redacted capture/ingest on both hosts, root-only curation/RAPTOR scheduling, bounded shutdown recovery, and fail-open host turns.

- [ ] **Step 1 (3 min): Write red extension registration tests**

Assert exactly one `memory_search` tool and handlers `agent_end`, `before_agent_start`, `context`, `session_before_compact`, `session_shutdown`, and `session_start`, with no `agent_settled`:

```typescript
it("registers the exact lifecycle contract", () => {
  const extension = createMemoryExtension(testDependencies());
  expect(Array.from(extension.handlers.keys()).sort()).toEqual(["agent_end", "before_agent_start", "context", "session_before_compact", "session_shutdown", "session_start"]);
  expect(extension.handlers.has("agent_settled")).toBe(false);
  expect(Array.from(extension.tools.keys())).toEqual(["memory_search"]);
});
```

- [ ] **Step 2 (4 min): Write red persisted-capture lifecycle tests**

Mock `getEntries()` and each exact capture event; assert event message arrays are ignored, cutoff is respected, custom memory is not recaptured, outbox enqueue is redacted, shutdown leaves pending jobs, and all turns fail open.

- [ ] **Step 3 (4 min): Write red host child/root eligibility tests**

Cover Pi header `parentSession` child, optional extension-wrapper marker child, contradictory header/marker, valid root, Prime header depth, Prime `RLM_DEPTH`, invalid depth, and no child auto-recall/curation/RAPTOR.

- [ ] **Step 4 (3 min): Run extension tests red**

```bash
npx vitest run tests/unit/extension.test.ts tests/unit/host.test.ts
```

Expected: FAIL because v1 registers only old handlers and has no capture/outbox lifecycle handlers.

- [ ] **Step 5 (4 min): Implement exact host child/root resolution**

In `host.ts`, resolve Prime depth from session header then `RLM_DEPTH`; Pi 0.84.1 has no built-in `PI_SUBAGENT_*` fields; resolve child status from `sessionManager.getHeader()?.parentSession` (the actual host signal), and validate optional extension-wrapper markers `PI_SUBAGENT_CHILD=1`/`PI_SUBAGENT_DEPTH>0` only when present. Header-child versus marker-root contradictions, invalid depth, or invalid parent metadata disable root work and auto-recall while preserving child episode tagging.

- [ ] **Step 6 (4 min): Implement session activation state**

At `session_start`, resolve registered project identity and persist the hashed activation cutoff before capture. Missing capture opt-in leaves recall behavior available while capture remains off. Health checks must not scan or write conversation data.

- [ ] **Step 7 (4 min): Wire the `agent_end` capture handler**

Register exactly `agent_end`. Invoke `capturePersistedEntries()` with a session-manager-backed `getEntries: () => ctx.sessionManager.getEntries()` dependency so the capture function calls `getEntries()` itself; never pass or trust event message arrays. Complete redaction before `outbox.enqueue`, then start bounded delivery. Any failure warns with a fixed redacted category, preserves durable jobs, and returns without aborting the host turn.

- [ ] **Step 8 (4 min): Wire the `session_before_compact` handler**

Register exactly `session_before_compact` and scan persisted entries again. Enqueue one explicit root curation job only when eligible; children enqueue episode leaves only. Ensure memory context is excluded and compaction does not erase pending outbox/proposal work.

- [ ] **Step 9 (4 min): Wire shutdown flush/recovery and ephemeral recall**

Register `session_shutdown`: scan the final persisted entries, enqueue redacted episodes, flush outbox/accepted jobs for a bounded timeout, persist remaining work, release leases best-effort, and never drop pending files. Ensure auto-recall runs only through copied `context` messages and never captures the extension custom block. Clear cache/service state after shutdown. A restart adopts closed producer outboxes and retries pending jobs.

- [ ] **Step 10 (4 min): Run lifecycle green and typecheck**

```bash
npx vitest run tests/unit/extension.test.ts tests/unit/host.test.ts
npm run typecheck
```

Expected: PASS for both exact hosts, persisted getEntries scanning at all three capture events, cutoff/restart, child leaves/root gating, curation triggers, bounded shutdown, outbox recovery, ephemeral recall, no `agent_settled`, no JSONL mutation, and fail-open behavior; TypeScript exits 0.

- [ ] **Step 11 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/extension.ts src/host.ts src/service.ts tests/unit/extension.test.ts tests/unit/host.test.ts dist
npm run check
git diff --cached --check
```

Expected: generated extension artifacts are staged and check passes.

- [ ] **Step 12 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for exact handler names, persisted `getEntries()` source, activation cutoff, child/root gating, outbox/shutdown durability, no `agent_settled`, no auto-recall children, no JSONL mutation, and fail-open turns. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: wire autonomous memory lifecycle"
```

Expected: commit succeeds only after both approvals.


### Task 13: Human-Only Project Registration, Privacy Revocation, Forget Planning, Inspect, and Status CLI

**Files:**
- Modify: `src/admin/cli.ts`, `src/admin/init.ts`, `src/admin/status.ts`, `src/project.ts`, `src/config.ts`, `tests/unit/admin-init-status.test.ts`
- Create: `src/admin/project.ts`, `src/admin/privacy.ts`, `src/admin/forget.ts`, `src/admin/inspect.ts`, `tests/unit/admin.test.ts`

**Interfaces:**
- **Consumes:** Task 1 `AdminProcessSecrets`, Task 2 XDG registration/policy/config, Task 3 init/client/schema, Task 8 control/tombstone/privacy barriers, Task 9 temporal records, Task 10 generations, Task 11 retrieval safety, and Task 12 lifecycle.

- **Produces:** redacted human-operated operations with no model tool path: exact init confirmation, project registration, privacy revoke, status audit, curate/RAPTOR/reconcile enqueue/wait, bounded inspect, and occurrence/content/state forget plans with logical success barriers.

- [ ] **Step 1 (4 min): Write red project/CLI parsing tests**

Assert `project register|unregister|status`, human confirmation, alias/path/fingerprint validation, exit codes, no model tool route, and exact v2 command/help set.

- [ ] **Step 2 (4 min): Write red status/init audit tests**

Assert explicit retention/egress confirmation, destination-only init, exact redacted `status --json` fields, no keys/raw payload, collection metadata/control owner, policy hashes, outbox/coverage/jobs/generation/privacy counts.

- [ ] **Step 3 (4 min): Write red privacy/forget plan tests**

Assert revoke drain/epoch, occurrence/content/state plan scopes, current resolves to observation, approval mismatch, tombstone/provenance closure, future content/state matching, and final logical invisibility:

```typescript
it("requires the exact forget plan and separates occurrence/content/state", async () => {
  const plan = await planForget({ selection: { curatedCurrentId: "current-1" }, scope: "occurrence" });
  expect(plan.targets).toContain("observation-1");
  expect(plan.recurrenceBlocked).toBe(false);
  await expect(runCli(["forget", "--approve", "wrong-plan"])).rejects.toThrow(/plan/);
  await expect(runCli(["forget", "--plan", plan.id, "--approve", plan.id])).resolves.toMatchObject({ ok: true });
});
```

- [ ] **Step 4 (3 min): Run admin tests red**

```bash
npx vitest run tests/unit/admin.test.ts tests/unit/admin-init-status.test.ts
```

Expected: FAIL because v1 CLI has source/import commands and no v2 privacy/project/forget operations.

- [ ] **Step 5 (4 min): Implement operator project registration commands**

Create `admin/project.ts` wrappers for `project register --path <canonical-path> --alias <stable-id>`, `project unregister --alias <stable-id>`, and `project status`. Require human confirmation for writes, persist only XDG path/fingerprint/alias bindings, show redacted mismatch warnings, and reject repository-provided aliases/endpoints/credentials. Runtime must fail closed if path/fingerprint/symlink no longer matches. Registering the same alias on another machine is an explicit operator action.

- [ ] **Step 6 (4 min): Implement init and status JSON audit**

Rewrite `admin/init.ts`/`status.ts` so `init --json` requires host, explicit retention (integer 1..3650 or `indefinite`), destination/egress disclosure and confirmation before enabling capture; creates only the host collection/metadata/indexes from Task 3 and reports that loopback is functional isolation, not cryptographic privacy. `status --json` reports endpoint origin/collection, metadata owner/schema/vector, auth mode without keys, capture opt-in/retention/project registration, egress destination IDs/revocations redacted, policy hash/mismatch, scopes, outbox size/oldest/failures, coverage/reconcile age, jobs/leases, active generation/manifest/levels/orphans, privacy epoch, embedding health, dedicated/fallback LLM availability, exact record counts, and last redacted error category.

- [ ] **Step 7 (4 min): Implement privacy revoke**

Create `admin/privacy.ts` with `privacy revoke --plan`/`--approve` that drains workers, increments collection privacy epoch by control CAS, records revocation, invalidates active generation/current views, and schedules reconciliation; it must state calls already in flight cannot be revoked.

- [ ] **Step 8 (4 min): Implement forget target planning and scope semantics**

Create `admin/forget.ts` to plan a redacted target/provenance closure including episodes, observations, current views, evidence, generations/manifests, and outbox/proposals; display `occurrence` (default), `content`, and `state` scopes separately. A current selection resolves to its observation; occurrence does not block future recurrence, while content/state match future observations with the same `content_id`/`state_key`. Require exact plan IDs and human approval before writes.

- [ ] **Step 9 (4 min): Implement tombstone/privacy barrier approval**

Approval inserts immutable deterministic tombstones with strong/wait, CASes control `privacy_epoch` and `active_generation=null`, rereads tombstones/epoch, invalidates coverage/current views, and reports logical invisibility only after confirmation. Derived nodes GC only after reread of active control, and rebuild is required before RAPTOR retrieval. Final retrieval checks hide physically reinserted stale records; make no claim about backups/snapshots/storage segments.

- [ ] **Step 10 (4 min): Implement forget cleanup and offline recovery**

Cleanup is eventual/idempotent: stale outboxes/proposals quarantine; an offline outbox is not physically erased while its machine is away; first return checks control epoch, target tombstones, and expiry before egress. Rebuild from non-forgotten observations after coverage invalidation; no physical deletion claim is made for unreachable machines or storage segments.

- [ ] **Step 11 (4 min): Implement remaining human commands and safe output**

Create `admin/inspect.ts` with bounded/redacted record inspection and no raw payload/credentials; wire CLI commands `curate --enqueue|--wait`, `raptor rebuild --enqueue|--wait`, `reconcile --enqueue|--wait`, `inspect`, `forget` interactive or plan/approve, and `privacy revoke`. Keep `memory_search` the only model-callable tool. CLI exit codes are 0 success, 2 invalid/config/approval, 1 infrastructure; `--json` output is deterministic and redacted. There is no `import-hermes` parser, alias, help entry, or executable artifact.

- [ ] **Step 12 (4 min): Run admin green and typecheck**

```bash
npx vitest run tests/unit/admin.test.ts tests/unit/admin-init-status.test.ts
npm run typecheck
```

Expected: PASS for project registration, explicit retention/egress confirmation, status audit, privacy epoch/revoke, forget scopes/plans/approval/barrier/rebuild, command parsing/exit codes, bounded inspect, and no secret/text leakage; TypeScript exits 0.

- [ ] **Step 13 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add src/admin/cli.ts src/admin/init.ts src/admin/status.ts src/project.ts src/config.ts src/admin/project.ts src/admin/privacy.ts src/admin/forget.ts src/admin/inspect.ts tests/unit/admin.test.ts tests/unit/admin-init-status.test.ts dist
npm run check
git diff --cached --check
```

Expected: executable `dist/admin/cli.js` and all admin declarations are staged; check passes.

- [ ] **Step 14 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for human-only authorization, registration binding, exact status audit/no leaks, retention/egress disclosure, privacy drain/epoch, forget scope semantics and closure, final tombstone barrier, no live/backups claim, command/exit contracts, and Hermes absence. Fixes rerun tests/build/stage/check/review. After approval:

```bash
git commit -m "feat: add privacy and memory operations CLI"
```

Expected: commit succeeds only after both approvals.


### Task 14: Isolated Qdrant 1.17.1 Concurrency Matrix and Exact Pi/Prime Host Smokes

**Files:**
- Modify: `tests/integration/qdrant.test.ts`, `tests/compat/run-host-smoke.mjs`, `.github/workflows/ci.yml`
- Create: `tests/integration/qdrant-concurrency.test.ts`, `tests/integration/qdrant-fixtures.ts`, `tests/compat/host-fixtures.mjs`, `tests/compat/run-isolated-smokes.sh`

**Interfaces:**
- **Consumes:** all runtime/admin contracts from Tasks 1–13, exact compatibility pins, Qdrant 1.17.1 integration harness, and committed `dist`.

- **Produces:** isolated real-Qdrant schema/write/coordination/privacy/retrieval coverage and exact Pi/Prime lifecycle/model-path smokes with production/live endpoint guards.

- [ ] **Step 1 (4 min): Write the isolated harness guard red test**

Create `qdrant-fixtures.ts` requiring `PI_QDRANT_MEMORY_TEST_QDRANT_URL`, a random run ID/prefix, and loopback. Refuse any non-loopback host, refuse port 6333 outside CI, refuse a missing/random-invalid run ID, refuse source collection names, and require Qdrant image/version 1.17.1. Construct retired names from fragments so the active-surface audit cannot match its own test:

```typescript
export function isolatedQdrantUrl(env: Record<string, string | undefined>): string {
  const raw = env.PI_QDRANT_MEMORY_TEST_QDRANT_URL;
  const runId = env.PI_QDRANT_MEMORY_TEST_RUN_ID;
  if (!raw || !runId || !/^[a-z0-9]{12,32}$/.test(runId)) throw new Error("isolated run ID and URL required");
  const url = new URL(raw);
  const hostname = url.hostname.replace(/^\[|\]$/gu, "");
  if (!["127.0.0.1", "localhost", "::1"].includes(hostname)) throw new Error("loopback Qdrant required");
  if (url.port === "6333" && env.CI !== "true") throw new Error("default Qdrant port refused outside CI");
  const retired = ["hermes", "_memory"].join("");
  if (url.pathname.includes(retired)) throw new Error("source collection refused");
  return url.href.replace(/\/$/u, "");
}
```

- [ ] **Step 2 (3 min): Run the harness red without a service**

```bash
npx vitest run tests/integration/qdrant.test.ts tests/integration/qdrant-concurrency.test.ts
```

Expected: absent env fails closed with `isolated run ID and URL required`; no configured or live collection is contacted.

- [ ] **Step 3 (4 min): Add real 1.17.1 collection/schema tests**

Test exact isolated `pi_memory`-like and `prime_memory`-like random collections, owner-independent `collection_metadata`, separately mutable `collection_control`, named `semantic` vector, every required index, payload-only control points, insert-only/update-only/update-filter/read-back/strong/wait behavior, owner mismatch, consistency, and cleanup/count zero. Assert two physical collections and zero cross-host/project results.

- [ ] **Step 4 (4 min): Add concurrent writer and lease tests**

Run 20 writers with equal and distinct episode IDs, claim/renew/steal/release, stale fencing and delayed responses. Verify deterministic IDs converge, hash collisions fail, no unconditional overwrite occurs, and one valid fencing token is required for publication.

- [ ] **Step 5 (4 min): Add curation/RAPTOR race tests**

Run two curators for identical and overlapping memberships, policy CAS during a delayed LLM, one accepted proposal, convergent content/observation/evidence IDs, two rebuilds from one base with one publish, crash before/after each protocol step, and late response/partition simulation. Verify stale proposals/nodes remain invisible and summary payloads never mutate.

- [ ] **Step 6 (4 min): Add two-machine policy/retention/forget matrix tests**

Use independent outboxes/node IDs for machine A/B with equal and divergent authorized destination sets, residency/data-use labels, provider replay flags, and retention deadlines. Test shared-home duplicate node IDs, adopters, producer offline past expiry, and first-return checks before egress. Interleave forget at ingest, proposal acceptance, materialization, publication, and final retrieval; verify occurrence targets one observation/episode while content/state tombstones hide future matching IDs and stale physical reinsertion remains invisible.

- [ ] **Step 7 (5 min): Add exact-host lifecycle smokes**

Update `run-host-smoke.mjs` expected compatibility JSON to schema 2/Qdrant pins. Require `EXTENSION_PATH`, resolve it with `realpath`, assert it is exactly under the temp host and not under `ROOT`/local `dist`, and load that path (never `PLUGIN_TARBALL` as a load path). Exercise persisted entries through `session_start`, `agent_end`, `session_before_compact`, and `session_shutdown`; Pi tests header `parentSession` child, optional extension-wrapper marker child, contradiction, valid root, and no child recall/curation/RAPTOR. Prime tests header depth then `RLM_DEPTH`. Exercise memory_search, ephemeral recall, shutdown/reload/outbox recovery, simultaneous instances, and assert no JSONL/custom recapture, no source collection, and host-specific owner/collection.

- [ ] **Step 8 (4 min): Prove both real reflected LLM host paths**

Make one exact Pi smoke expose registry `complete` and make the exact Prime smoke expose namespace `completeSimple` only through reflection plus structural auth. Assert concrete model/context/options, auth success/failure, no static namespace completion access, no fallback to embeddings, the Qdrant readiness/version poll, exact scoped installed `EXTENSION_PATH` realpath under the temp host, and resolved PiAi versions 0.84.1 (Pi) / 0.7.1 (Prime). Keep both paths in `host-fixtures.mjs` so the smoke does not rely solely on injected unit fakes.

- [ ] **Step 9 (4 min): Create the isolated-smokes script prologue and Qdrant harness**

Create and `chmod +x` executable `tests/compat/run-isolated-smokes.sh`; do not depend on shell state from another checkbox or agent tool call. Its prologue must own all state, pack this plugin, start Qdrant, capture CID/dynamic loopback port, and install the EXIT trap:

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="$(node -e 'process.stdout.write(require("node:crypto").randomBytes(8).toString("hex"))')"
TMP_ROOT="$(mktemp -d)"
CID="$(docker run -d --rm --name "pi-qdrant-memory-test-$RUN_ID" -p 127.0.0.1::6333 qdrant/qdrant:v1.17.1)"
cleanup() { docker stop "$CID" >/dev/null 2>&1 || true; docker rm -f "$CID" >/dev/null 2>&1 || true; rm -rf "$TMP_ROOT"; }
trap cleanup EXIT
PORT="$(docker port "$CID" 6333/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
export QDRANT_URL="http://127.0.0.1:$PORT"
export PI_QDRANT_MEMORY_TEST_RUN_ID="$RUN_ID" PI_QDRANT_MEMORY_TEST_QDRANT_URL="$QDRANT_URL"
READY=0
for attempt in $(seq 1 30); do
  if QDRANT_PROBE_URL="$QDRANT_URL" node -e 'fetch(process.env.QDRANT_PROBE_URL).then(async r => { const body = await r.json(); if (!r.ok || body.version !== "1.17.1") process.exit(1); }).catch(() => process.exit(1))'; then READY=1; break; fi
  sleep 1
done
test "$READY" = 1
assert_plugin_ai_version() {
  node - "$1" "$2" <<'NODE'
const { createRequire } = require("node:module");
const { dirname, join } = require("node:path");
const { existsSync, readFileSync } = require("node:fs");
const pluginDir = process.argv[2];
const expected = process.argv[3];
const resolved = createRequire(join(pluginDir, "dist", "extension.js")).resolve("@earendil-works/pi-ai");
let dir = dirname(resolved);
while (dir !== dirname(dir) && !existsSync(join(dir, "package.json"))) dir = dirname(dir);
const actual = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")).version;
if (actual !== expected) throw new Error(`wrong resolved pi-ai ${actual}; expected ${expected}`);
NODE
}
mkdir -p "$TMP_ROOT/plugin"
npm --prefix "$ROOT" pack --ignore-scripts --json --pack-destination "$TMP_ROOT/plugin" > "$TMP_ROOT/pack.json"
PLUGIN_TARBALL="$TMP_ROOT/plugin/$(node -p 'JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"))[0].filename' "$TMP_ROOT/pack.json")"
run_integration() { (cd "$ROOT" && npm run test:integration); }
```

The harness requires the random run ID/loopback URL and Qdrant 1.17.1 fixture guard; it must never use a fixed 6333 outside CI, a live endpoint, or the repository's local `dist` as a host install. `npm pack` must be this repository's tarball, not a registry package.

- [ ] **Step 10 (4 min): Add the exact Pi setup function to the isolated script**

Append `run_pi()` to the same script. Install Pi 0.84.1 into a temp host, then install `"$PLUGIN_TARBALL"` there so its peer resolves to Pi's 0.84.1 `@earendil-works/pi-ai`; verify the package version and pass `PLUGIN_TARBALL` to the smoke. Do not point the smoke at this repository's source or local `dist`:

```bash
run_pi() {
  PI_HOST="$TMP_ROOT/pi-host"; mkdir -p "$PI_HOST"
  npm install --prefix "$PI_HOST" --no-save @earendil-works/pi-coding-agent@0.84.1 "$PLUGIN_TARBALL"
  test "$(node -p 'require(process.argv[1]).version' "$PI_HOST/node_modules/@earendil-works/pi-coding-agent/package.json")" = 0.84.1
  PLUGIN_DIR="$PI_HOST/node_modules/@prodrifterdk/pi-qdrant-memory"
  EXTENSION_PATH="$(realpath "$PLUGIN_DIR/dist/extension.js")"
  case "$EXTENSION_PATH" in "$PI_HOST"/*) ;; *) exit 1 ;; esac
  test -f "$EXTENSION_PATH"
  assert_plugin_ai_version "$PLUGIN_DIR" "0.84.1"
  HOST_INDEX="$PI_HOST/node_modules/@earendil-works/pi-coding-agent/dist/index.js" EXPECTED_HOST=pi EXTENSION_PATH="$EXTENSION_PATH" PLUGIN_TARBALL="$PLUGIN_TARBALL" node "$ROOT/tests/compat/run-host-smoke.mjs"
}
```

- [ ] **Step 11 (4 min): Add the exact Prime setup function to the isolated script**

Append `run_prime()` to the same script. Clone Prime, detach exactly `a18809e00ea30638584d87b3afea7285a9d7296c`, run its own `npm ci`/build, then install `"$PLUGIN_TARBALL"` into the mutable temp workspace. This preserves Prime's workspace-resolved pi-ai 0.7.1 for reflected fallback and does not load local repository `dist`:

```bash
run_prime() {
  PRIME_HOST="$TMP_ROOT/prime-agent"
  git clone https://github.com/PrimeIntellect-ai/prime-agent.git "$PRIME_HOST"
  git -C "$PRIME_HOST" checkout --detach a18809e00ea30638584d87b3afea7285a9d7296c
  npm --prefix "$PRIME_HOST" ci
  npm --prefix "$PRIME_HOST" run build
  npm --prefix "$PRIME_HOST" install --no-save "$PLUGIN_TARBALL"
  test "$(git -C "$PRIME_HOST" rev-parse HEAD)" = a18809e00ea30638584d87b3afea7285a9d7296c
  PLUGIN_DIR="$PRIME_HOST/node_modules/@prodrifterdk/pi-qdrant-memory"
  EXTENSION_PATH="$(realpath "$PLUGIN_DIR/dist/extension.js")"
  case "$EXTENSION_PATH" in "$PRIME_HOST"/*) ;; *) exit 1 ;; esac
  test -f "$EXTENSION_PATH"
  assert_plugin_ai_version "$PLUGIN_DIR" "0.7.1"
  HOST_INDEX="$PRIME_HOST/packages/coding-agent/dist/index.js" EXPECTED_HOST=prime EXTENSION_PATH="$EXTENSION_PATH" PLUGIN_TARBALL="$PLUGIN_TARBALL" node "$ROOT/tests/compat/run-host-smoke.mjs"
}
```

- [ ] **Step 12 (4 min): Run the one isolated script and let its trap clean**

Append `run_integration; run_pi; run_prime` as the script's final line and run only the complete script:

```bash
bash tests/compat/run-isolated-smokes.sh
```

Expected: the script packs this plugin, runs integration against its random Qdrant collection, runs Pi and Prime smokes against their temp-host package installs, proves both reflected LLM paths, and always stops/removes the CID and temp root on exit.

- [ ] **Step 13 (4 min): Update CI with the same isolation contract**

Use `tests/compat/run-isolated-smokes.sh` as the CI compatibility job's single owner of Qdrant/CID/dynamic-port/temp-host state; it uses Qdrant `qdrant/qdrant:v1.17.1`, random run ID/prefix, loopback, trap cleanup, and the packed plugin tarball. Retain Node 20/24 typecheck/unit/build/dist matrix. Compatibility jobs install only Pi 0.84.1 and clone/detach only the exact Prime commit, verify package/commit before smoke, and require `needs: verify`; no credentials/live variables or paid APIs. Do not duplicate a fixed service port or assume state from another job.

- [ ] **Step 14 (4 min): Run integration/host green and typecheck**

```bash
bash tests/compat/run-isolated-smokes.sh
npm run typecheck
```

Expected: PASS only against isolated Qdrant 1.17.1; exact smokes name host, collection owner, lifecycle hooks, and both LLM reflection paths; TypeScript exits 0.

- [ ] **Step 15 (4 min): Build, stage exact files, and run the required check**

```bash
npm run build
git add tests/integration/qdrant.test.ts tests/integration/qdrant-concurrency.test.ts tests/integration/qdrant-fixtures.ts tests/compat/run-host-smoke.mjs tests/compat/host-fixtures.mjs tests/compat/run-isolated-smokes.sh .github/workflows/ci.yml dist
npm run check
git diff --cached --check
```

Expected: check passes with generated `dist` staged and CI diff showing only isolated exact-version jobs.

- [ ] **Step 16 (3 min): Reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after check for loopback/random-run/CID/trap isolation, no live endpoint fallback, all concurrency/fencing/CAS/policy/forget cases, equal/divergent two-machine retention/egress, exact Pi/Prime pins and child signals, both reflected LLM paths, no `agent_settled`, and no source collection. Fixes rerun isolated tests/build/stage/check/review. After approval:

```bash
git commit -m "test: cover isolated Qdrant and host compatibility"
```

Expected: commit succeeds only after both approvals.

### Task 15: Documentation, Package/CI Release Preparation, and Stop-Before-Activation Gate

**Files:**
- Modify: `README.md`, `docs/configuration.md`, `docs/security.md`, `package.json`, `package-lock.json`, `compatibility.json`, `.github/workflows/ci.yml`, `vitest.config.ts`, `.gitignore`
- Modify audit/notice: `tests/unit/no-hermes.test.ts`, `src/vendor/umap-license-apache-2.0.txt`
- Include generated: `dist/**`

**Interfaces:**
- **Consumes:** the complete implementation and test matrix from Tasks 1–14.

- **Produces:** release-ready v2 docs/manifests/CI/package audit with exact host/Qdrant pins, committed `dist`, a dry-run tarball/license audit, and an explicit stop before publication, tag, live init, or activation.

- [ ] **Step 1 (4 min): Write the release audit with an explicit active allowlist**

Update `tests/unit/no-hermes.test.ts` with a helper that scans recursive `src`, `tests`, and `dist` plus explicit active README/package/compatibility/CI/configuration/security paths, excludes `docs/superpowers` and its own test file, and constructs every forbidden/placeholder token from fragments. Assert exact package version `2.0.0`, npm `>=11.10`, peer `@earendil-works/pi-ai` wildcard/dev `0.84.1` with no runtime duplicate, umap `1.4.0`, compatibility schema/Qdrant pins, one tool, and no active Hermes/source/SDK/daemon/Python surface. The helper must not use a literal three-dot token or scan itself.

```typescript
async function activeReleaseFiles(): Promise<string[]> {
  const paths: string[] = [];
  for (const root of ["src", "tests", "dist"]) paths.push.apply(paths, await recursiveFiles(root));
  paths.push("README.md", "package.json", "compatibility.json", ".github/workflows/ci.yml", "docs/configuration.md", "docs/security.md");
  return paths.filter(path => !path.endsWith("no-hermes.test.ts"));
}
const placeholderTokens = [["TO", "DO"], ["T", "BD"]].map(parts => parts.join(""));
const retiredTokens = [["import", "-hermes"], ["hermes", "_memory"], ["SOURCE", "_QDRANT_"], ["qdrant", "-admin"]].map(parts => parts.join(""));
const retiredPaths = ["src/admin/qdrant-admin.ts", "tests/unit/admin-client.test.ts", "src/admin/hermes-contract.ts", "src/admin/import-hermes.ts", "src/admin/import-plan.ts", "src/admin/secret-scan.ts", "src/clients/qdrant-readonly.ts"];
```

The release test asserts `activeRetiredPaths(await activeReleaseFiles())` is empty, so `qdrant-admin` and every other retired path are absent after their earlier deletion tasks; it also scans retired tokens in active executable/import surfaces and package paths.

- [ ] **Step 2 (3 min): Run the release audit red against the development version**

```bash
npx vitest run tests/unit/no-hermes.test.ts
```

Expected: FAIL because Task 1 intentionally leaves version `2.0.0-dev.0` until this release gate; the failure is an intentional, non-placeholder red state.

- [ ] **Step 3 (4 min): Rewrite active v2 documentation**

Rewrite README/configuration/security for v2 only: host-private `pi_memory`/`prime_memory`, capture opt-in/retention/egress disclosure, project registration, exact env allowlist/secrets, Qdrant >=1.17, producer authorized-destination policies/privacy epochs/tombstones, child/root behavior, untrusted ephemeral recall, curation/RAPTOR operations, status/forget/revoke, fail-open limits, rollback, and no Hermes migration/import. Keep `docs/superpowers` archival only; active package docs must not teach retired commands/source collections.

- [ ] **Step 4 (3 min): Record the umap tarball notice**

Copy the exact Apache-2.0 `umap-js@1.4.0` tarball LICENSE into `src/vendor/umap-license-apache-2.0.txt` and document the package/version and metadata caveat in the active release audit.

- [ ] **Step 5 (4 min): Set exact release metadata and CI boundary**

Set `package.json` to exact version `2.0.0`; retain npm `>=11.10`, Node >=20, the CLI bin, Pi extension entry, umap runtime pin, peer AI wildcard/dev 0.84.1, and no publication script. Set the intended package `files` allowlist to `dist`, `README.md`, `LICENSE`, `docs/configuration.md`, `docs/security.md`, `compatibility.json`, and `src/vendor/umap-license-apache-2.0.txt`; do not rely on npm's default inclusion for the notice. Keep compatibility schema 2 and exact host/Qdrant pins. CI starts with `npm ci`, runs Node 20/24 typecheck/unit/build/dist checks, isolated Qdrant 1.17.1, and exact host jobs without credentials/live variables.

- [ ] **Step 6 (4 min): Run all local release checks without live services**

```bash
npm run typecheck
npm test
npm run build
git diff --exit-code -- dist
```

Expected: unit tests, TypeScript, build, and committed-dist check exit 0. No daemon is used or killed; no live endpoint is contacted.

- [ ] **Step 7 (4 min): Audit package contents, dependencies, and notice files**

```bash
npm pack --dry-run --ignore-scripts --json
node -e 'const cp=require("node:child_process"), p=require("./package.json"); if(p.version!=="2.0.0") throw Error("release version"); if(p.engines?.npm!==">=11.10") throw Error("npm floor"); if(p.dependencies?.["umap-js"]!=="1.4.0") throw Error("umap pin"); if(p.peerDependencies?.["@earendil-works/pi-ai"]!=="*") throw Error("AI peer"); if(p.devDependencies?.["@earendil-works/pi-ai"]!=="0.84.1") throw Error("AI dev pin"); if(p.dependencies?.["@earendil-works/pi-ai"]) throw Error("runtime AI duplicate"); const rows=JSON.parse(cp.execFileSync("npm",["pack","--dry-run","--ignore-scripts","--json"],{encoding:"utf8"}))[0].files.map(x=>x.path); const ok=x=>x.startsWith("dist/")||["LICENSE","README.md","package.json","package-lock.json","docs/configuration.md","docs/security.md","compatibility.json","src/vendor/umap-license-apache-2.0.txt"].includes(x); if(rows.some(x=>!ok(x))||!rows.includes("src/vendor/umap-license-apache-2.0.txt")) throw Error("package allowlist"); if(rows.some(x=>x.startsWith("docs/superpowers/")||x.includes("hermes")||x.includes("qdrant-admin")||x.includes("secret-scan")||x.includes("qdrant-readonly"))) throw Error("retired package artifact");'
test -f src/vendor/umap-license-apache-2.0.txt
test -f dist/extension.js
test -x dist/admin/cli.js
```

Expected: dry-run JSON lists only intended `dist`, `docs/configuration.md`, `docs/security.md`, README, license, compatibility, and package files; no archival `docs/superpowers` or Hermes/source artifact is listed; the Apache-2.0 notice and executable artifacts exist.

- [ ] **Step 8 (4 min): Stage only Task 15 files and run the required check**

```bash
npm run build
git add README.md docs/configuration.md docs/security.md package.json package-lock.json compatibility.json .github/workflows/ci.yml vitest.config.ts .gitignore src/vendor/umap-license-apache-2.0.txt tests/unit/no-hermes.test.ts dist
npm run check
git diff --cached --check
git diff --cached --stat
```

Expected: build/check pass with `dist` staged, no broad prior-task `src` or `tests` paths staged, and the cached stat contains only Task 15 files plus generated `dist`.

- [ ] **Step 9 (3 min): Final dual reviewer gate and conventional commit**

The independent reviewer and Sol/root reviewer inspect the staged diff after `npm run check` and compare every approved-spec section: early Hermes removal/schema 2; canonical config/IDs/policies/project registration; Qdrant 1.17; redaction/cutoff/exact hooks; outbox/ingest; distributed coordination; reflected host LLM bridge; temporal curation; UMAP/GMM/RAPTOR; manifests/CAS; guarded retrieval/injection; lifecycle; privacy/status/forget; isolated Qdrant/host smokes; package/docs/release boundary. They scan for placeholder markers, vague test steps, wrong type names, missing lock/dependency/notice, mutable derived payloads, `agent_settled`, cross-host/project leakage, Hermes artifacts, or accidental live/publish/tag commands. If either reviewer requests a fix, rerun affected red/green tests, build, exact staging, check, and this gate. After both approvals:

```bash
git commit -m "chore: prepare pi qdrant memory v2"
```

Expected: all changed files conform to the locked Task 15 file map; this commit contains only Task 15 docs/package/CI/audit/notice plus generated `dist`, while archival v1 spec references remain allowed and prior task source/test commits remain separate. No publish/tag/live init runs.

- [ ] **Step 10 (2 min): Record the later GitHub-only activation and rollback boundary**

Stop at the reviewed commit. A separate, later operational procedure may create a GitHub release without npm publication, preserve v2 collections/outboxes on rollback, restore settings/pins, stop v2 workers, and require separate approval for collection deletion. Activation begins from empty, physically separate `pi_memory` and `prime_memory` collections, never backfills old sessions or Hermes, confirms retention/egress per host, performs human-confirmed init, registers projects explicitly, opens new sessions, runs synthetic capture→curation→RAPTOR/restart smokes, and demonstrates bidirectional Pi/Prime/Hermes isolation. This plan does not execute or schedule publication, tags, live initialization, backfill, collection deletion, or activation.

## Final Verification Checklist

Run these commands only from the later fresh implementation worktree after `npm ci` has succeeded. They are verification instructions for the implementer, not commands executed while writing this plan:

```bash
npm --version
npm ci
npm run typecheck
npm test
npm run build
git diff --exit-code -- dist
npm run check
npm pack --dry-run --ignore-scripts --json
git status --short --branch
```

Expected final state:

- `npm --version` is at least 11.10; lockfile installation is reproducible and no unlisted dependency or SDK exists.
- `compatibility.json` is schema 2 with exact Pi 0.84.1, Prime commit `a18809e00ea30638584d87b3afea7285a9d7296c`, Qdrant minimum 1.17.0, and latest tested 1.17.1.
- Active package/source/tests/docs/dist contain no Hermes executable/source/config/credential/client path, no `agent_settled`, no placeholders, and no model-controlled endpoint/collection/credential argument.
- `pi_memory` and `prime_memory` are physically distinct, metadata/control payloads are immutable and owner-checked, Qdrant filters always enforce host/project/status/expiry/policy/tombstone safety, and control writes use strong ordering/wait with reread verification.
- Capture is opt-in with explicit retention/egress policy; persisted `getEntries()` is scanned only after the durable activation cutoff at `agent_end`, `session_before_compact`, and `session_shutdown`; no capture path uses `agent_settled`, event message arrays, system/developer/custom/injected/thinking content, or full tool output.
- Structural redaction precedes local disk, outbox, embedding, Qdrant, and LLM; safe redaction remains searchable with `redactionStatus`, final `secret_scan="passed"` is required for storage, scanner reject/error is quarantined, and logs remain redacted.
- Each accepted episode has a deterministic UUID/hash and durable per-process 0700/0600 outbox job; retries/adoption/expiry and duplicate ingest are idempotent, while embeddings are BGE-M3-only and exactly 1024 finite components.
- Distributed leases/jobs/reconcile use insert-only/update-only/OCC, strong ordering, version/fencing tokens, policy/privacy epochs, explicit coverage IDs, and no local lock as a correctness barrier. Stale workers cannot accept/materialize/publish.
- The LLM bridge statically imports `@earendil-works/pi-ai`, type-guards `Reflect.get(ctx.modelRegistry,"complete")`, and falls back to `Reflect.get(PiAi, "completeSimple")(model, context, options)` only after `getApiKeyAndHeaders(model)` returns `ResolvedRequestAuthLike.ok`; no dynamic/inline imports or BGE generation path exists.
- Temporal curation validates strict JSON and direct evidence, preserves immutable observations/history, handles causal order/skew conflicts, folds A→B→A correctly, and only root claims work; children write leaves and remain project-only with no auto-recall/curation/RAPTOR.
- RAPTOR uses the pinned umap-js tarball notice, seeded xoshiro128**, TypeScript diagonal GMM/BIC, deterministic fallback/base cases/termination, soft-membership DAGs, bounded content-addressed manifests, evidence links, immutable summaries, and one control-point CAS publication.
- Retrieval runs guarded current/historical/episode/curated/RAPTOR/exact lanes, applies policy/tombstone final checks, descends summary evidence, labels temporal history, defaults to project-only, and injects only bounded ephemeral `<memory-context trust="untrusted">`; host turns fail open.
- Human CLI operations alone can register projects, inspect/status, enqueue/reconcile, revoke privacy, and plan/approve forget with occurrence/content/state scopes and epoch/tombstone barriers. Status reveals no conversation text, query, key, header, path, or raw provider payload.
- Unit tests, isolated Qdrant 1.17.1 concurrency tests, exact Pi/Prime host smokes, package dry-run, static typecheck, build, and committed-dist checks are green. CI starts with `npm ci`, has no paid/live credentials, and rejects dist drift.
- The final reviewed state contains only the locked file map and generated `dist`; Task 15’s commit stages only its docs/package/CI/audit/notice files plus `dist`, while implementation stops before npm publication, Git tag, live collection initialization, or live activation.

### Self-review before handing the plan to the implementer

- [x] **Spec coverage:** compared every approved-spec section 1–25 and acceptance criterion 1–17 against Tasks 1–15, including exact lifecycle events, policy intersections, Qdrant update modes, UMAP/GMM parameters, temporal conflict/A→B→A, forget closure, and package stop boundary.
- [x] **Placeholder/type review:** read-only audit found no placeholder markers; checked `Model<Api>`, `Context`, `ResolvedRequestAuthLike`, `ctx.modelRegistry.complete`, reflected namespace completion, `EpisodeRecord`, `ProcessingPolicy`, `ControlRecord`, `Generation`, and aligned red snippets to their declared exports.
- [x] **Dependency review:** verified every task consumes only earlier interfaces; package/lockfile changes precede LLM tests; Qdrant primitives precede outbox ingest/coordination; curation precedes RAPTOR; retrieval/lifecycle/admin/CI follow records and safety barriers.
- [x] **Hermes review:** verified the active-surface audit allowlist excludes archival `docs/superpowers`, excludes its own test, scans active source/tests/dist/docs/package/compatibility/CI, and removes the explicitly listed Hermes files/tests early.
- [x] **Staging/reviewer review:** verified every task has focused red/green commands, expected output, build before exact `git add` including `dist`, `npm run check` after staging, staged diff check, independent reviewer gate, direct Sol/root review, fix rerun rule, and a conventional commit.
- [x] **Operational boundary:** confirmed this plan-writing pass performed no install, project test, Qdrant request, code implementation, commit, push, publish, tag, or live init; those remain implementation/release gates explicitly described above.
