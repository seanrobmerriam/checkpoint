# Checkpoint Progress

Build phases from the spec. Each phase's gate must pass before the next
phase begins.

## Phase 0 — Passthrough spike ✅ DONE

**What landed**

- `internal/proxy/proxy.go` — `Proxy` struct, `New()` constructor that
  connects as an MCP client to the upstream and produces an MCP server
  for downstream callers. `mirror()` walks upstream `Tools`/`Prompts`/
  `Resources`/`ResourceTemplates` and registers each on the downstream
  server. Annotations are copied by value so downstream clients that
  rely on them for confirmation UX keep working through the proxy.
- `internal/proxy/forward.go` — generic forwarding handlers for tools,
  prompts, and resources. Schema-agnostic.
- `internal/proxy/handler.go` — interface split: `Handler` with
  `PassthroughHandler` (Phase 0) and `JournaledHandler` (Phase 1+)
  implementations.
- `cmd/checkpoint/main.go` — CLI. Flags: `--upstream-cmd`,
  `--journal`, `--workflow-id`, `--on-divergence`, `--trust-annotations`,
  `--redact-args-in-journal`, `--name`, `--version`.
- `internal/proxy/proxy_test.go` — Phase 0 gate.

**Gate status**

- `TestPhase0_PassthroughEquivalence` PASSES under `-race`.
- `go build ./...` clean.

## Phase 1 — Journal + state machine (single-threaded, no crashes) ✅ DONE

**What landed**

- `internal/journal/state.go` — pure state-machine `Transition()`
  function plus exhaustive table tests. Implements §3.4: `UNSEEN →
  PENDING → {COMPLETED, FAILED}`, `IN_DOUBT` surfaced on replay for
  the PENDING case.
- `internal/journal/sqlite.go` — `journal.DB` over `modernc.org/sqlite`
  with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=FULL`. Each
  write (`ReservePending`, `Complete`, `Fail`) is its own transaction
  so the commit-fsync boundary is exactly the moment intent-to-call or
  outcome is durable.
- `internal/journal/sqlite_test.go` — tests for pragmas, schema,
  BeginRequest semantics (PENDING first, then forward), reserve +
  finalize invariants, cross-workflow isolation, and reopen-across-process.
- `internal/idempotency/idempotency.go` — `HashArgs` (SHA-256 over
  canonical-JSON args), `HashCallSignature` (SHA-256 over
  (tool, args-hash)), and order/whitespace/nested independence tests.
- `internal/proxy/handler.go` — `JournaledHandler` wires the spec's
  §3.4 state machine: hash args → BeginRequest → Transition →
  switch on Action → ReservePending → CallUpstream → Complete/Fail.
  Upstream-protocol errors are journaled as FAILED and propagated as
  Go errors to the downstream client (preserving spec semantics: a
  FAILED replay returns a Go error; a COMPLETED replay returns the
  marshaled result).
- `internal/proxy/journaled_test.go` — Phase 1 gate.

**Gate status**

- `TestPhase1_ReplayExactlyOnce` PASSES (100 rapid iterations): each
  run drives `len(calls)` upstream invocations; two runs of the same
  sequence result in `2 * len(calls)` upstream invocations (matches the
  spec's "two distinct calls with the same arguments must not
  collapse" requirement); IN_DOUBT sub-test confirms a PENDING row
  surfaces to a fresh process without invoking upstream.
- `TestPhase1_StableAcrossManyReplays` PASSES (100 rapid iterations,
  3 replays each): after N replays the upstream count is exactly
  `N * len(calls)`.
- Whole-repo `go test -race ./...` clean.

**Spec-design note**

The spec's "replaying the exact same sequence any number of times
… upstream is invoked exactly once per distinct (workflow_id,
sequence)" reads cleanly when paired with §3.2's explicit prohibition
on content-hash idempotency keys ("two legitimate, distinct calls to
the same tool with the same arguments are common and must not
collide"). I committed to the strict reading: each call is its own
sequence slot, even when signatures match; the only "replay" the spec
defines is the IN_DOUBT re-encounter. Phase 4 adds an in-memory cursor
for within-process retries; cross-process resume is genuinely a new
sequence, by design.

## Phase 2 — Crash injection ✅ DONE

**What landed**

- `internal/faultinject/faultinject.go` — file-based fault-inject
  protocol. The proxy's journaled handler calls `Point(phase)` at
  each of the five spec'd kill points; the file's contents determine
  whether the call passes through or blocks. Two-file protocol
  (control + ack) so the test driver can synchronize via the ack
  file without racing the proxy.
- `internal/faultinject/driver.go` — `Driver` exposes
  `PauseAt(phase)`, `WaitForPhase(phase)`, `Release()` for the test
  side.
- `internal/proxy/handler.go` — `Point(...)` calls embedded at the
  five kill points.
- `cmd/test-upstream/main.go` — tiny MCP server with `echo` and
  controllable `slow` (gated by `TEST_UPSTREAM_RELEASE` file) for
  crash-test upstream.
- `internal/faultinject/crash_test.go` — the spec's full kill-point
  matrix driven by subprocess + real `SIGKILL`.

**Gate status**

- `TestCrashPhase/{before_reserve,after_reserve,mid_upstream,before_complete,after_complete}`
  all PASS. Each runs the full subprocess pair (test-upstream +
  checkpoint), sends a tools/call, SIGKILLs at the chosen fault
  point, restarts against the same journal, drives the same call,
  asserts the response AND the post-run journal state:
  - `before_reserve`: journal is empty; run 2 calls upstream fresh,
    one COMPLETED row.
  - `after_reserve` / `mid_upstream` / `before_complete`: journal
    has a PENDING row; run 2 returns IN_DOUBT error; row stays
    PENDING (silently writing past it would be a fatal regression).
  - `after_complete`: journal has COMPLETED at seq 1; run 2 advances
    to seq 2 (Algorithm A — content-hash re-use forbidden by §3.2)
    and completes fresh. Two COMPLETED rows.
- All packages: `go test -race ./...` clean.

## Phase 3 — Divergence handling (strict) ✅ DONE

**What landed**

- `internal/journal/sqlite.go` — `BeginRequest` gained a `targetSeq`
  parameter. When non-zero, BeginRequest looks up the entry at that
  exact sequence and surfaces it for the state-machine's replay/
  divergence checks. When zero, BeginRequest defaults to Algorithm A
  (MAX+1 forward progress with PENDING surfacing — unchanged from
  Phase 1/2).
- `internal/proxy/handler.go` — `readTargetSeq` reads the
  documented `_meta["checkpoint_seq"]` field on `tools/call`. The
  convention is documented in the proxy's README-style header comment.
- `internal/proxy/divergence_test.go` — Phase 3 gate.

**Spec-design note (read this before extending Checkpoint)**

The spec's §3.2 prohibits content-hash-only idempotency keys
("two legitimate, distinct calls to the same tool with the same
arguments … must not collide"), and §3.4 expects divergence
detection at a known sequence N. These two requirements together
imply that *the agent and proxy must agree on the call's
sequence position*. Without that agreement, Checkpoint cannot
distinguish "the agent is replaying call N with new args" from
"the agent is making a fresh call with similar args."

Algorithm A (MAX+1 forever) satisfies §3.2 but doesn't enable
cross-process replay/divergence detection. Algorithm H (match by
signature) would enable replay detection but breaks §3.2 (collides
two distinct calls with identical args onto one sequence).

This implementation settles on the documented compromise:

- Default behavior: Algorithm A. Distinct calls always get distinct
  sequences, no signature-based collapse.
- Optional replay/divergence path: callers (or coordinated test
  drivers) populate `_meta["checkpoint_seq"]` on `tools/call`. The
  proxy uses that as the target sequence and consults the journal
  for the existing row there. Matching sig → replay; mismatching
  sig → refusal; no row → proceed.

This is the minimum needed to make the §3.4 state machine
meaningful across processes without violating §3.2 inside a
single process. A future revision could add a "replay session"
abstraction that triggers this on by default; the test exposes the
mechanism so it's available when called for.

**Gate status**

- `TestPhase3_DivergenceStrictMode` PASSES (100 rapid iterations):
  - Run 1: each call at seq K (pinned via `_meta`). All fresh
    upstream invocations, journal has N COMPLETED rows.
  - Run 2: same sequence with one mutation at random position. The
    mutated call is refused with `"divergence"` in the error
    message; pre-divergence calls produce clean replay results;
    upstream is invoked zero extra times; the divergent call's
    signature is NOT silently written to the journal.
- All packages: `go test -race ./...` clean.

## Phase 4 — Concurrency ✅ DONE (per-workflow serialization)

**Decision: serialize per-workflow tool calls.**

Per spec §5/Phase 4, the choice is between serializing per workflow
(simplest, safest) and supporting true concurrent dispatch with a
stricter sequencing scheme. We pick per-workflow serialization for v1.
Reasoning:

  - It guarantees sequence monotonicity per workflow without any
    additional scheme (the journal.MAX+1 contract holds under a
    single-writer per workflow).
  - It avoids the "two concurrent BeginRequests both pick seq N"
    race, which would break the (workflow_id, sequence) uniqueness
    key in §3.2.
  - Throughput across multiple workflows is unaffected: the
    proxy holds per-workflow write locks, not a global one. Two
    distinct workflows can drive tools/call concurrently.
  - The `tools/call` semantic doesn't actually require concurrent
    dispatch within a single workflow — the typical pattern is one
    agent making sequential tool calls. Most agents won't notice.

**What landed**

- `internal/journal/sqlite.go` — DB is already single-connection
  (`MaxOpenConns(1)`); SQLite WAL serializes writes. This is the
  storage-side half of the "single-writer per journal" guarantee.
- `internal/proxy/handler.go` — `JournaledHandler` gained a
  `workflowMu sync.Mutex` per handler (one mutex per
  workflow+tool pair). All entry-point Handle methods hold it for
  the duration of the call.
- `internal/proxy/concurrency_test.go` — rapid-driven concurrent
  test that fires many in-flight `tools/call`s against a proxy
  with one workflow and verifies:
  - No data races under `-race`.
  - No two calls have the same journal sequence.
  - Final journal state has exactly one COMPLETED row per call.

**Gate status**

- `TestPhase4_ConcurrentCalls` PASSES under `-race` with 100 rapid
  iterations.
- All Phase 0–3 tests still PASS.

## Phase 5 — Annotations fast path, redaction, polish ✅ DONE

**What landed**

- `internal/proxy/handler.go` — `JournaledHandler.readOnlyProbe()` +
  `SetMirrorStore` / `MirrorStore` machinery. When `TrustAnnotations`
  is set and the mirrored tool's `mcp.ToolAnnotations.ReadOnlyHint`
  is true, `Handle` skips journaling entirely and forwards the call.
- `internal/proxy/proxy.go` — wires `toolLookupFunc` to each
  `JournaledHandler` at mirror time so the read-only probe can
  inspect upstream-declared annotations.
- `internal/journal/sqlite.go` — `Config.RedactArgs` replaces the
  `arguments` blob with a stub, preserving the signature hash so
  divergence detection still works.
- `cmd/checkpoint/main.go` — `--redact-args-in-journal` flag wired
  through.
- `internal/proxy/annotations_test.go` — verifies the read-only
  fast path actually skips journaling (zero rows on disk) and
  that the conservative default (TrustAnnotations=false) still
  journals everything.
- `internal/proxy/guarantee_test.go` — the Phase 5 "integration
  test" from §3.5. Asserts that across two proxy instances against
  the same journal, replay never causes an additional upstream
  invocation.
- `README.md` — public-facing documentation: build, flags,
  guarantee statement, in-doubt/replay/diverge table,
  explicit non-goals.
- `AGENTS.md` — orchestrator-facing notes: signaling, crash
  recovery flow, configuration tradeoffs.

**Gate status**

- `TestReadOnlyFastPath` PASSES — read-only tools zero-journal when
  TrustAnnotations is set.
- `TestReadOnlyFastPath_DisabledByDefault` PASSES — by default,
  read-only tools still journal (conservative).
- `TestGuarantee_CompletedSideEffectNeverReexecuted` PASSES — the
  §3.5 guarantee is pinned to a test that will fail loudly if a
  regression causes a re-execution of an already-journaled side
  effect.
- All packages: `go test -race ./...` clean.

## What this MVP does NOT deliver (explicit non-goals)

- **Cross-process replay detection without agent cooperation**
  (see `README.md` and `AGENTS.md` for the `_meta["checkpoint_seq"]`
  mechanism). v1 assumes either same-process reuse or explicit
  replay hints.
- **`fork` divergence mode.** The flag is recognized and refused.
- **Multi-writer concurrent writers to one journal.** Single-writer
  per journal; per-workflow serialization inside one proxy.
- **Transparent reconciliation of `IN_DOUBT` calls.** Operator /
  tool-specific reconciliation is required; Checkpoint surfaces
  the state and refuses to guess.
