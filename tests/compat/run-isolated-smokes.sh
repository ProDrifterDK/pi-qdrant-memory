#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="$(node -e 'process.stdout.write(require("node:crypto").randomBytes(8).toString("hex"))')"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/pi-qdrant-memory-task14-${RUN_ID}-XXXXXX")"
CID=""
cleanup() {
  if [[ -n "$CID" ]]; then docker rm -f "$CID" >/dev/null 2>&1 || true; fi
  case "$TMP_ROOT" in
    "${TMPDIR:-/tmp}/pi-qdrant-memory-task14-${RUN_ID}-"*) node -e 'require("node:fs").rmSync(process.argv[1], { recursive: true, force: true })' "$TMP_ROOT" ;;
    *) printf '%s\n' "refusing to clean an unexpected temporary path" >&2; return 1 ;;
  esac
}
trap cleanup EXIT

CID="$(docker run -d --rm --name "pi-qdrant-memory-test-${RUN_ID}" -p 127.0.0.1::6333 qdrant/qdrant:v1.17.1)"
PORT=""
for attempt in $(seq 1 30); do
  PORT="$(docker port "$CID" 6333/tcp 2>/dev/null | sed -nE 's/.*:([0-9]+)$/\1/p' | head -n 1 || true)"
  if [[ -n "$PORT" ]]; then break; fi
  sleep 1
done
if [[ -z "$PORT" ]]; then printf '%s\n' "Qdrant did not publish a loopback port" >&2; exit 1; fi
export PI_QDRANT_MEMORY_TEST_RUN_ID="$RUN_ID"
export PI_QDRANT_MEMORY_TEST_QDRANT_URL="http://127.0.0.1:${PORT}"
export CI="${CI:-false}"
READY=0
for attempt in $(seq 1 30); do
  if QDRANT_PROBE_URL="$PI_QDRANT_MEMORY_TEST_QDRANT_URL" node -e 'fetch(process.env.QDRANT_PROBE_URL + "/").then(async response => { const body = await response.json(); if (!response.ok || body.version !== "1.17.1") process.exit(1); }).catch(() => process.exit(1))'; then READY=1; break; fi
  sleep 1
done
test "$READY" = 1
QDRANT_SENTINEL_URL="$PI_QDRANT_MEMORY_TEST_QDRANT_URL" QDRANT_SENTINEL_RUN_ID="$RUN_ID" node <<'NODE'
const base = process.env.QDRANT_SENTINEL_URL;
const runId = process.env.QDRANT_SENTINEL_RUN_ID;
if (!/^[a-z0-9]{12,32}$/u.test(runId ?? "")) throw new Error("invalid sentinel run ID");
const name = `task14_guard_${runId}`;
const response = await fetch(`${base}/collections/${name}`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ vectors: { semantic: { size: 1, distance: "Dot" } } }) });
if (!response.ok) throw new Error(`isolated sentinel creation failed: ${response.status}`);
NODE

mkdir -p "$TMP_ROOT/plugin"
npm --prefix "$ROOT" run build
npm --prefix "$ROOT" pack --ignore-scripts --json --pack-destination "$TMP_ROOT/plugin" > "$TMP_ROOT/pack.json"
PLUGIN_TARBALL="$TMP_ROOT/plugin/$(node -p 'JSON.parse(require("node:fs").readFileSync(process.argv[1], "utf8"))[0].filename' "$TMP_ROOT/pack.json")"
test -f "$PLUGIN_TARBALL"

reset_isolated_collections() {
  QDRANT_RESET_URL="$PI_QDRANT_MEMORY_TEST_QDRANT_URL" node <<'NODE'
const base = process.env.QDRANT_RESET_URL;
for (const collection of ["pi_memory", "prime_memory"]) {
  const response = await fetch(`${base}/collections/${collection}`, { method: "DELETE" });
  if (![200, 404].includes(response.status)) throw new Error(`owned collection reset failed: ${collection} ${response.status}`);
}
NODE
}

run_integration() {
  (cd "$ROOT" && npm run test:integration -- --run)
  reset_isolated_collections
  (cd "$ROOT" && PI_QDRANT_MEMORY_TEST_CONCURRENCY=true npx vitest run tests/integration/qdrant-concurrency.test.ts)
}

assert_plugin_ai_version() {
  node - "$1" "$2" <<'NODE'
const { resolve } = require("node:path");
const { existsSync, readFileSync } = require("node:fs");
const pluginDir = process.argv[2];
const expected = process.argv[3];
const candidates = [
  resolve(pluginDir, "..", "..", "@earendil-works", "pi-ai", "package.json"),
  resolve(pluginDir, "..", "@earendil-works", "pi-ai", "package.json"),
];
const packagePath = candidates.find(path => existsSync(path));
if (packagePath === undefined) throw new Error("resolved pi-ai package metadata was not found");
const actual = JSON.parse(readFileSync(packagePath, "utf8")).version;
if (actual !== expected) throw new Error(`wrong resolved pi-ai ${actual}; expected ${expected}`);
NODE
}

run_pi() {
  PI_HOST="$TMP_ROOT/pi-host"
  mkdir -p "$PI_HOST"
  npm install --prefix "$PI_HOST" --no-save @earendil-works/pi-coding-agent@0.84.1 @earendil-works/pi-ai@0.84.1 "$PLUGIN_TARBALL"
  test "$(node -p 'require(process.argv[1]).version' "$PI_HOST/node_modules/@earendil-works/pi-coding-agent/package.json")" = "0.84.1"
  PLUGIN_DIR="$PI_HOST/node_modules/@prodrifterdk/pi-qdrant-memory"
  EXTENSION_PATH="$(realpath "$PLUGIN_DIR/dist/extension.js")"
  case "$EXTENSION_PATH" in "$PI_HOST"/*) ;; *) exit 1 ;; esac
  test -f "$EXTENSION_PATH"
  assert_plugin_ai_version "$PLUGIN_DIR" "0.84.1"
  HOST_INDEX="$PI_HOST/node_modules/@earendil-works/pi-coding-agent/dist/index.js" EXPECTED_HOST=pi HOST_ROOT="$PI_HOST" EXTENSION_PATH="$EXTENSION_PATH" PLUGIN_TARBALL="$PLUGIN_TARBALL" node "$ROOT/tests/compat/run-host-smoke.mjs"
}

run_prime() {
  PRIME_HOST="$TMP_ROOT/prime-agent"
  PRIME_COMMIT="a18809e00ea30638584d87b3afea7285a9d7296c"
  git clone https://github.com/PrimeIntellect-ai/prime-agent.git "$PRIME_HOST"
  git -C "$PRIME_HOST" checkout --detach "$PRIME_COMMIT"
  npm --prefix "$PRIME_HOST" ci
  npm --prefix "$PRIME_HOST" run build
  npm --prefix "$PRIME_HOST" install --no-save "$PLUGIN_TARBALL"
  test "$(git -C "$PRIME_HOST" rev-parse HEAD)" = "$PRIME_COMMIT"
  PLUGIN_DIR="$PRIME_HOST/node_modules/@prodrifterdk/pi-qdrant-memory"
  EXTENSION_PATH="$(realpath "$PLUGIN_DIR/dist/extension.js")"
  case "$EXTENSION_PATH" in "$PRIME_HOST"/*) ;; *) exit 1 ;; esac
  test -f "$EXTENSION_PATH"
  assert_plugin_ai_version "$PLUGIN_DIR" "0.7.1"
  HOST_INDEX="$PRIME_HOST/packages/coding-agent/dist/index.js" EXPECTED_HOST=prime HOST_ROOT="$PRIME_HOST" EXTENSION_PATH="$EXTENSION_PATH" PLUGIN_TARBALL="$PLUGIN_TARBALL" node "$ROOT/tests/compat/run-host-smoke.mjs"
}

run_integration
case "${PI_QDRANT_MEMORY_TEST_HOSTS:-both}" in
  pi) run_pi ;;
  prime) run_prime ;;
  both) run_pi; run_prime ;;
  *) printf '%s\n' "PI_QDRANT_MEMORY_TEST_HOSTS must be pi, prime, or both" >&2; exit 2 ;;
esac
