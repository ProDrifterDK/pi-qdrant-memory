# Pi Qdrant Memory v2

`@prodrifterdk/pi-qdrant-memory` is a Pi/Prime Agent package. The approved v2 design targets host-private, redacted episode memory, durable delivery, distributed curation, and RAPTOR generations. **Task 1 only establishes the Hermes-free build/configuration shell; the target storage and lifecycle behavior is not implemented yet.**

## Current Task 1 surface

This development build (`2.0.0-dev.0`) provides:

- schema-2 compatibility metadata for Pi 0.84.1, the pinned Prime revision, and Qdrant 1.17.1;
- one transitional `RuntimeConfig` with host-private collection defaults and exact credential boundaries;
- a destination-only `init`/`status` contract shell;
- CLI help for `init`, `project`, `privacy`, `status`, `curate`, `raptor`, `reconcile`, `inspect`, and `forget`; and
- an active-surface audit proving that retired executable/configuration paths are absent.

Task 1 does **not** implement capture hooks, redaction, the durable outbox, Qdrant writes, curation, RAPTOR construction, privacy/forget barriers, or new retrieval behavior. Those are locked target behavior for later tasks. Do not treat the transitional shell as an operational v2 release.

## Compatibility

- Node.js 20 or newer and npm 11.10 or newer.
- Pi `@earendil-works/pi-coding-agent` 0.84.1.
- Prime Agent commit `a18809e00ea30638584d87b3afea7285a9d7296c`.
- Qdrant 1.17.0 or newer; validation targets 1.17.1.

See [`compatibility.json`](compatibility.json). The package is not published by this task.

## Build and CLI shell

```bash
npm ci --include=dev
npm run build
PI_QDRANT_MEMORY_HOST=pi npm exec -- pi-qdrant-memory status --json
PI_QDRANT_MEMORY_HOST=prime npm exec -- pi-qdrant-memory init --json
```

The only configuration file is `${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json`. Repository files never provide configuration, authorization, endpoints, collections, or credentials. Human-only credentials are read from the process environment and do not enter `RuntimeConfig`.

## Target design (not yet implemented by Task 1)

The v2 target uses separate `pi_memory`/`prime_memory` collections, named `semantic` vectors, redaction before any durable or network operation, per-process outboxes, policy epochs, tombstones, untrusted ephemeral context, and immutable curation/RAPTOR records. These statements describe the approved contract for later implementation; the Task 1 shell does not provide those operations.

See [configuration](docs/configuration.md), [security](docs/security.md), and [compatibility](compatibility.json).
