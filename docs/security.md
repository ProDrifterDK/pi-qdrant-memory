# Security model

Pi Qdrant Memory v2 treats host state, Qdrant payloads, recalled text, shared outboxes, model output, and operator selectors as untrusted at their boundaries. Its guarantees depend on least-privilege credentials, private filesystem ownership, accurate destination declarations, and human control of administrative commands.

## Authority boundaries

- Pi owns `pi_memory`; Prime owns `prime_memory`. Endpoint/collection collisions are rejected when both hosts are enabled.
- Runtime credentials are collection-scoped. `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY` is consumed only by human CLI code.
- This extension exposes only the Qdrant-backed model-callable API `qdrant_memory_search`; it coexists with the separately owned Hermes `memory_search` tool. Qdrant search cannot select endpoints, collections, credentials, project registration, privacy changes, curation, reconciliation, RAPTOR, or deletion.
- Project registration is operator authority. Unregistered or mismatched projects are installation-local and cannot raise global scope.
- Root lifecycle work is determined from strict host session markers. Children cannot run automatic recall, capture delivery, curation, reconciliation, or RAPTOR publication. Explicit child search remains bounded and project-only when enabled.

A host/process compromise can act with that process's granted credentials. Use separate service identities or processes when stronger isolation is required.

## Capture and redaction

Capture is off by default. Activation requires explicit retention and egress values in configuration and matching human confirmation during `init`. Capture begins after a durable activation cutoff and selects only finalized persisted entries on `agent_end`, `session_before_compact`, and `session_shutdown`.

System, developer, custom/injected memory, thinking, partial entries, memory-tool traffic, and successful tool bodies are excluded. Tool calls and failures retain only bounded allowlisted fields.

Before durable or network use, the pipeline:

1. structurally selects and bounds allowed fields;
2. redacts known secret, credential, path, and sensitive-value shapes;
3. runs the final scanner over the semantic projection;
4. admits only exact material with `secretScan: "passed"`; and
5. computes deterministic identity and content hashes from the admitted form.

Scanner rejection/error and invalid envelope data are quarantined generically. Raw rejected values and raw hashes are not persisted in audit reasons. Redaction is defense in depth, not proof that personal, proprietary, or regulated data is absent.

## Durable outbox and ingest

Accepted episodes enter private 0700/0600 per-producer outboxes before network egress. Jobs bind host, pseudonymous node, producer UUID, policy, episode IDs, deadline, and audit hash. Capacity admission, reservations, generation retirements, recovery fences, and terminal audits are durable and fail closed on unsafe permissions, symlinks, inode changes, malformed files, clock ambiguity, or incomplete proof.

Delivery is at least once. Qdrant writes are insert-only for immutable records and update-only/CAS for mutable control state; every successful write is read back and verified. Duplicate canonical writes converge, while same-ID/different-content collisions are terminal. A local lock is never distributed correctness authority.

Shared outboxes require an explicit unique pseudonymous `outbox.nodeId`. Recovery uses bounded scans, no-follow opens, pre/post inode checks, durable fences, and stale/closed producer evidence. The admission generation namespace is finite; approaching its 1,000,000-generation ceiling is an availability event requiring maintenance, not permission to bypass the protocol.

## Destination and policy enforcement

Every accepted episode carries a processing policy binding:

- owner host and origin provider;
- Qdrant, embedding, and LLM destination IDs;
- residency and data-use label;
- privacy epoch and optional expiry;
- policy revision and cross-provider replay choice.

Destination IDs are resolved from trusted configuration, never model input. Query embedding occurs only after the active policy and every locally discovered exact-candidate policy authorize the exact Qdrant, embedding, and LLM destinations. Candidate/evidence policies are checked again before a hit is returned. Control changes or revocation between egress and finalization invalidate the operation.

Use loopback services where practical. For remote endpoints use TLS, explicit allowlists, least-privilege credentials, and truthful residency/data-use labels. HTTP redirects are rejected so credentials and request bodies cannot cross origins.

## Distributed curation and RAPTOR

Root workers claim insert-only jobs through leases with expiry, fencing tokens, privacy/policy epochs, and stable control snapshots. Stale or losing workers cannot accept proposals, materialize derived state, complete a lease, or publish a generation.

Curation accepts strict bounded model JSON and only direct authorized episode evidence. Observations, content, temporal history, coverage, manifests, and summaries are immutable. Current-state pointers and collection control use OCC. A→B→A history remains visible.

RAPTOR uses deterministic seeded local clustering and immutable evidence-linked summaries. Publication requires a stable authority snapshot, complete manifest/barrier verification, and one control-point CAS. Concurrent builders may perform duplicate work, but exactly one generation becomes active.

## Retrieval and injection

Retrieval pins host, project/scope, status, scanner result, expiry, privacy epoch, coordination epoch, active RAPTOR generation, and time bounds in Qdrant filters. It then validates payload schemas, policy destinations, evidence closure, tombstones, and a stable control snapshot before returning hits.

Occurrence, content, observation, and state tombstones are applied logically even if stale physical points reappear. RAPTOR summaries are never trusted directly; retrieval descends to concrete authorized evidence within strict depth/member limits.

Injected context is bounded, ephemeral, and labeled `<memory-context trust="untrusted">`. It is not appended to the transcript. Host turns fail open on memory timeouts, unavailable services, malformed payloads, or stale control: no context is injected and only a fixed redacted warning is emitted.

## Privacy and human operations

`status` and `inspect` expose bounded metadata, not conversation text, queries, credentials, headers, paths, or raw provider responses. `privacy revoke` and `forget` are human-only plan/approve workflows. Their plans are immutable, expire, and bind the target policy/control state.

Forget supports occurrence, content, and state closure. Applying it raises the privacy barrier and publishes tombstones before physical cleanup can be considered. Physical deletion is maintenance; logical invisibility does not depend on immediate point removal.

Administrative `--wait` requires a live root lifecycle session to drain queued work. Only `completed` is terminal success; released leases remain retryable.

## Failure, rollback, and operations

- Recall fails open; capture admission and administrative mutations fail closed.
- Keep clocks synchronized within the configured skew allowance.
- Monitor outbox capacity, pending/released leases, scan cursors, control epochs, and admission generation growth.
- On rollback, disable the extension and stop v2 workers before restoring prior package/settings pins. Preserve collections and outboxes for diagnosis.
- Collection or outbox deletion is destructive and requires a separate explicit procedure. Do not reuse a v2 collection for another host or schema.
- Fresh activation starts from empty host-private collections and new sessions. No old-session or external-store backfill is part of this release.

The package contains the exact Apache-2.0 license text from the `umap-js@1.4.0` npm tarball at `src/vendor/umap-license-apache-2.0.txt`. npm lock metadata labels that package `MIT`, which conflicts with the tarball's bundled license text; this release preserves the exact upstream text rather than silently resolving the metadata discrepancy. The tarball exposes no separate NOTICE file. Preserve the shipped license file in redistribution and review the upstream discrepancy for any later dependency upgrade.
