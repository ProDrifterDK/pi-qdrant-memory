# Pi Qdrant Memory

`@prodrifterdk/pi-qdrant-memory` is a Pi Package for **Prime Agent** and **Pi**. It adds one read-only model tool, `memory_search`, and optional automatic recall for eligible natural-language turns. Retrieval uses an OpenAI-compatible embeddings endpoint and a dedicated Qdrant collection.

Memories are untrusted historical excerpts—not instructions or facts guaranteed to be correct. Current user instructions and repository state remain authoritative.

## v1 scope

### Capabilities

- exact host isolation between Prime and Pi records;
- current-project and same-host fallback retrieval lanes;
- one model-callable tool: `memory_search({ query, limit? })`;
- ephemeral auto-recall in Pi turns and Prime root turns;
- fail-open turns when memory infrastructure is unavailable; and
- human-operated `init`, read-only `status`, and approved Hermes import commands.

### Non-goals

v1 has no model-callable store/update/delete operation, no automatic conversation write-through, no learning extraction, no re-embedding import, and no automatic import. It does not replace host JSONL persistence, Prime's continual harness, or repository state.

## Compatibility and prerequisites

The v1 compatibility contract pins:

- Pi `@earendil-works/pi-coding-agent` **0.84.1**; and
- Prime Agent commit **`a18809e00ea30638584d87b3afea7285a9d7296c`**.

See [`compatibility.json`](compatibility.json). The package requires Node.js 20 or newer; Pi 0.84.1 itself currently requires Node.js 22.19 or newer.

Before recall can return results, provide:

1. Qdrant with a dedicated collection (default `pi_memory`) using one 1024-dimensional cosine vector and the documented payload/index contract; and
2. an OpenAI-compatible embeddings endpoint at `/embeddings` (default base URL `http://127.0.0.1:8080/v1`) serving the configured model (default `bge-m3`) and exactly 1024 finite components.

Use loopback endpoints for same-machine services, or authenticated TLS endpoints for remote services. See [configuration](docs/configuration.md) and [security](docs/security.md).

## Install the extension

These commands are pinned to the v1 tag. GitHub installation is available from `v1.0.0`. This release does **not** publish the npm package; registry-only examples below remain unavailable until a separate npm publication.

### Prime Agent

```bash
prime-agent package install git:github.com/ProDrifterDK/pi-qdrant-memory@v1.0.0
PI_QDRANT_MEMORY_HOST=prime prime-agent
```

### Pi

```bash
pi install git:github.com/ProDrifterDK/pi-qdrant-memory@v1.0.0
PI_QDRANT_MEMORY_HOST=pi pi
```

The explicit host override is recommended and fails closed if it is not exactly `prime` or `pi`. Host markers can also be detected as described in [configuration](docs/configuration.md).

## Runtime behavior

`memory_search` accepts only a 1–4,000-character query and an optional limit from 1–10. Host, project, eligibility filters, endpoints, and credentials are not tool arguments. Every Qdrant lane requires exact `host`, `status="active"`, and `secret_scan="passed"` values.

Auto-recall starts before an eligible turn and appends at most one hidden custom message to the copied provider context. It does not append that message to the session branch or JSONL. Pi recalls normally. Prime recalls only when its resolved RLM depth is zero; Prime children with `rlmDepth > 0` do not auto-recall but can still call `memory_search`.

Slash commands, empty prompts, disabled hosts/configuration, and Prime child turns do not auto-recall. A low-information prompt can be combined with the latest substantive user prompt. Identical retrievals may be cached for five minutes; shutdown clears the cache.

If Qdrant, embeddings, configuration, parsing, formatting, or timeout handling fails, the agent turn continues without recalled context. The first warning in each category is redacted and repeated warnings are suppressed for that extension session.

## Administrative CLI

Installing a Pi Package does **not** guarantee its npm `bin` is placed on your global `PATH`.

### Registry invocation

After npm version `1.0.0` has actually been published:

```bash
PI_QDRANT_MEMORY_HOST=prime \
  npx --yes @prodrifterdk/pi-qdrant-memory@1.0.0 status --json
```

### Source checkout invocation

From a source checkout pinned to `v1.0.0`:

```bash
git checkout v1.0.0
npm ci
PI_QDRANT_MEMORY_HOST=prime npm exec -- pi-qdrant-memory status --json
```

`npm exec -- pi-qdrant-memory` is the normal checkout form because the current package declares that bin. If npm cannot expose the current package's own bin, force the local package explicitly; `--offline` prevents an unintended registry fallback:

```bash
PI_QDRANT_MEMORY_HOST=prime \
  npm exec --offline --package=. -- pi-qdrant-memory status --json
```

### Optional global bin

A global bin is optional and separately installed from the registry only after publication:

```bash
npm install --global @prodrifterdk/pi-qdrant-memory@1.0.0
PI_QDRANT_MEMORY_HOST=prime pi-qdrant-memory status --json
```

### Initialization (mutating)

`init` creates the destination collection/indexes when absent. It is mutating and must be an explicit operator decision:

```bash
PI_QDRANT_MEMORY_HOST=prime \
  npx --yes @prodrifterdk/pi-qdrant-memory@1.0.0 init --json
```

Do not use `init` merely to test installation. `status --json` is the read-only connectivity check.

### Exact Hermes dry-run and apply flow

Dry-run reads source and destination metadata/content and writes **zero** points:

```bash
npx --yes @prodrifterdk/pi-qdrant-memory@1.0.0 import-hermes \
  --source-url http://127.0.0.1:6333 \
  --source-collection hermes_memory \
  --source-model bge-m3 \
  --target-host prime \
  --dry-run \
  --json
```

Review the redacted report and its 64-lowercase-hex `planId`. Apply only that exact approval:

```bash
npx --yes @prodrifterdk/pi-qdrant-memory@1.0.0 import-hermes \
  --source-url http://127.0.0.1:6333 \
  --source-collection hermes_memory \
  --source-model bge-m3 \
  --target-host prime \
  --approve <64-lowercase-hex-plan-id> \
  --json
```

`--source-model` is required when every selected source record lacks model metadata; when source records declare a model, it must match the configured embedding model. Apply rereads and rehashes the source and refuses a stale plan. Repeat apply uses deterministic destination IDs and is idempotent for unchanged input. Source credentials and destination credentials are separate. See [security](docs/security.md).

## Troubleshooting

1. Set `PI_QDRANT_MEMORY_HOST` explicitly to `prime` or `pi`; an unknown/conflicting host disables recall.
2. Run only the read-only `status --json` command to inspect configured health and dimensions.
3. Confirm the Qdrant collection uses cosine distance and the configured embedding dimension.
4. Confirm the embeddings endpoint accepts the configured model and a prefixed non-empty input, and returns the exact configured dimension.
5. Check that runtime Qdrant credentials can read health/metadata/search but cannot mutate.
6. Check character budgets, `minScore`, `autoRecall`, and Prime `rlmDepth`/`RLM_DEPTH` settings.

Warnings intentionally omit endpoints, query text, memory text, response bodies, headers, credentials, vectors, and absolute paths.

## Uninstall

Remove the host package registration using the same pinned source:

```bash
prime-agent package remove git:github.com/ProDrifterDK/pi-qdrant-memory@v1.0.0
# or
pi remove git:github.com/ProDrifterDK/pi-qdrant-memory@v1.0.0
```

If installed globally, also run:

```bash
npm uninstall --global @prodrifterdk/pi-qdrant-memory@1.0.0
```

Optionally remove `${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json` and unset its environment variables. Uninstalling the package **does not delete or alter Qdrant collections or imported points**. Data removal is a separate Qdrant-administrator decision; v1 intentionally provides no delete or rollback command.

## Documentation

- [Configuration reference](docs/configuration.md)
- [Security model](docs/security.md)
- [Compatibility contract](compatibility.json)
