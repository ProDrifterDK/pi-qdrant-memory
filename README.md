# Pi Qdrant Memory v2

`@prodrifterdk/pi-qdrant-memory` provides redacted, durable memory for Pi and Prime Agent. Each host owns a physically separate Qdrant collection (`pi_memory` or `prime_memory`), while the only model-callable surface is `memory_search`.

## Compatibility

- Node.js >=20 and npm >=11.10.
- Pi `@earendil-works/pi-coding-agent` 0.84.1.
- Prime Agent commit `a18809e00ea30638584d87b3afea7285a9d7296c`.
- Qdrant >=1.17.0; the isolated matrix uses 1.17.1.
- BGE-M3 embeddings with exactly 1024 finite float32 components and Dot distance.

Exact pins are recorded in [`compatibility.json`](compatibility.json).

## Safety contract

- Capture is off by default. Enabling it requires explicit retention and egress settings plus human confirmation during `init`.
- Finalized persisted entries are structurally redacted and scanned before local outbox storage, embeddings, Qdrant, or model egress. Scanner rejection or failure is not searchable memory.
- Every producer policy binds the Qdrant, embedding, and LLM destinations, residency, data-use label, expiry, host, and privacy epoch.
- Pi and Prime never share a collection. Project identity is operator-registered; unregistered paths remain installation-local and cannot raise global scope.
- Root sessions may recall, capture, curate, reconcile, and build RAPTOR generations. Child sessions may use explicit project-scoped search but never run automatic recall or root lifecycle work.
- Recalled text is bounded, ephemeral, and injected as `<memory-context trust="untrusted">`; it is never copied into the session transcript.
- Records are immutable. Privacy epochs, tombstones, leases, fencing tokens, and control CAS guard deletion and distributed work.

Redaction reduces accidental disclosure; it is not anonymization. See [Security](docs/security.md).

## Configure and initialize

Configuration is read only from:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json
```

Repository files do not supply endpoints, credentials, project authority, or collection names. Credentials are environment-only. Start with [Configuration](docs/configuration.md), keep capture disabled, then inspect the destination:

```bash
PI_QDRANT_MEMORY_HOST=pi npm exec -- pi-qdrant-memory status --json
PI_QDRANT_MEMORY_HOST=prime npm exec -- pi-qdrant-memory status --json
```

After selecting retention and egress in the config, initialize with the same disclosure:

```bash
PI_QDRANT_MEMORY_HOST=pi npm exec -- pi-qdrant-memory init --retention indefinite --egress local_only --confirm --json
```

`init` is human-only and always requires `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`. Runtime workers use only collection-scoped Qdrant and embedding credentials.

Register each project explicitly:

```bash
PI_QDRANT_MEMORY_HOST=pi npm exec -- pi-qdrant-memory project register --path /absolute/project --alias project-id --confirm
PI_QDRANT_MEMORY_HOST=pi npm exec -- pi-qdrant-memory project status --path /absolute/project
```

## Runtime and operator commands

The extension captures only after a durable activation cutoff and only on `agent_end`, `session_before_compact`, and `session_shutdown`. Delivery is durable and at least once; Qdrant insert-only identity makes retries converge.

```bash
export PI_QDRANT_MEMORY_HOST=pi
pi-qdrant-memory status --json
pi-qdrant-memory inspect --type episode --limit 20 --json
pi-qdrant-memory curate --enqueue --json
pi-qdrant-memory curate --wait --job JOB_ID --json
pi-qdrant-memory reconcile --enqueue --json
pi-qdrant-memory raptor rebuild --enqueue --json
pi-qdrant-memory privacy revoke --plan --destination DESTINATION_ID --json
pi-qdrant-memory forget --scope occurrence --episode EPISODE_ID --json
```

Privacy revocation and forget are plan/approve operations. `--wait` succeeds only after a live root session drains the queue and the lease reaches `completed`; released work remains retryable. Run any command with `--help` for its exact selectors and approval arguments.

Curation preserves direct episode evidence and temporal history. RAPTOR publishes immutable summaries and manifests through one control-point CAS; losing builders do not publish.

## Failure and rollback boundaries

Host turns fail open when memory is unavailable: no recalled context is injected, and bounded redacted warnings are emitted. Capture admission fails closed on invalid policy, exhausted outbox capacity, unsafe filesystem state, or scanner failure.

Rollback by disabling the extension, stopping v2 lifecycle workers, and restoring the prior package/settings pin. Preserve both collections and outboxes for diagnosis; deleting them is a separate destructive operation. A fresh activation starts with empty host-private collections and new sessions. This release does not migrate or backfill another memory system or old session history.

## Development checks

```bash
npm ci --include=dev
npm run check
npm run test:integration -- --run
npm pack --dry-run --ignore-scripts --json
```

The full isolated Qdrant/Pi/Prime matrix is `tests/compat/run-isolated-smokes.sh`. It creates only a random loopback Qdrant 1.17.1 container and temporary host roots, then removes only those resources.

The package is prepared for a later GitHub release. It has no publication script and this repository workflow does not publish, tag, initialize live collections, or activate an installed extension.
