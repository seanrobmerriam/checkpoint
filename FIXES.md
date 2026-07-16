# Checkpoint Fixes Log

Bugs found and fixed during development, with root cause (not just
"fixed"). If you can't reproduce from the entry, the entry is incomplete.

## Phase 2

### Crash test cases hung reading initialize response

**Symptom**: First iteration of `TestCrashPhase/after_reserve`
blocked the test goroutine indefinitely. Goroutine stacks showed
the proxy's `bufio.Reader.ReadBytes` waiting forever on the
upstream's response.

**Root cause**: `spawnCheckpoint` first sent an `initialize`
JSON-RPC message, then called `conn.flush()`, then a *blocking*
`conn.read()` that called `ReadBytes('\n')`. The fundamental
issue was that the parent's stdin pipe lifecycle was wrong: the
first iteration closed the proxy's stdin on the parent side,
triggering the SDK's "graceful shutdown" path on the proxy,
rather than letting the test driver wait for a real crash to
happen independently.

**Fix**: Replaced the blocking `read()` with `readForID()` which
uses a 50 ms polling read in a goroutine and doesn't block the
parent. Renumbered message IDs to start at 1 (matching the spec).
Kept the parent's stdin pipe open for the proxy's lifetime.

### Fault-inject protocol didn't pause at the right phase

**Symptom**: With the driver initially writing `PhaseHold`
("HOLD\n") to engage fault injection, the proxy's `Point(phase)`
would block at the first kill point encountered
(`before_reserve`) instead of the requested one
(`after_reserve`).

**Root cause**: Single-file protocol using a generic HOLD sentinel
didn't tell the proxy *which* phase to engage at — any Point call
would block as long as HOLD was set.

**Fix**: Moved to a per-phase protocol. The driver writes the
desired phase name (`"after_reserve\n"`) directly to the control
file. `Point(phase)` reads it: if it matches `phase`, engage
(write ack file, block); otherwise fast-path through. Added a
second ack file at `<control>.phase` for the test to synchronize
on a real "I'm here" signal.

### `after_complete` crash test produced extra row at seq=2

**Symptom**: `assertJournalAfterRun2` after the `after_complete`
case found `seq=2 COMPLETED` and failed the "exactly len(wants)
rows" check.

**Root cause**: This *is* correct behavior given my BeginRequest
implementation (Algorithm A: MAX+1 forward progress, PENDING
surfacing only — based on §3.2's prohibition on content-hash
idempotency keys). I initially misread the test matrix as
wanting REPLAY semantics for `after_complete`, but per spec each
call gets a unique sequence, even when signatures match. Run 2
correctly advances to seq 2 and re-executes upstream.

**Fix**: Updated the `after_complete` `rowExpect` to include both
seqs 1 and 2, with a comment noting that "replay across
processes for matching signatures" is forbidden by §3.2 and that
sequence advancement is the correct observable behavior.

## Phase 1

### `modernc.org/sqlite` BLOB NOT NULL on nil arguments

**Symptom**: `TestReopenAcrossProcess`, `TestWorkflowsAreIsolated`,
and any other test that called `ReservePending(ctx, wf, seq, "tool",
"sig", nil)` failed at insert time with `NOT NULL constraint failed:
calls.arguments`.

**Root cause**: `ReservePending` accepted a `[]byte` and passed it
directly into the INSERT, but `arguments BLOB NOT NULL` rejects a zero
length `nil` slice.

**Fix**: normalize `nil`/empty args to the JSON literal `[]byte("null")`
in `ReservePending`. Empty argument payloads then have a stable
on-disk representation that distinguishes "no arguments" from "arg is
null", which is what we want for divergence detection later.

### `BeginRequest` skipped past IN_DOUBT rows

**Symptom**: `TestBeginRequest_PendingFromUncompletedCall` failed when
checking the re-encounter contract: after ReservePending seq=1 and a
crash, BeginRequest returned `seq=2, existing=nil`, advancing past
the unresolved PENDING row.

**Root cause**: my first version of `BeginRequest` always computed
`MAX(sequence)+1`. With a PENDING row at seq=1, MAX is 1 and the
returned sequence is 2 — the proxy never sees the IN_DOUBT case
that's mandated by §3.4.

**Fix**: `BeginRequest` now (1) checks for any PENDING row and
returns the smallest one with its entry, falling back to forward
progress otherwise. This is the canonical "always surface IN_DOUBT"
behavior the spec demands.

### InMemoryTransport used the same end on both sides

**Symptom**: `TestPhase1_ReplayExactlyOnce` hung at 120s timeout.
Goroutines were stuck at `net.(*pipe).write` (one side trying to send
195 bytes) and never got read on the other side.

**Root cause**: I was passing the same `*mcp.InMemoryTransport`
handle to both the upstream server (via `upstreamSrv.Connect(ctx, t,
…)`) AND to the proxy's client connection (via `Config.Upstream`).
Each end is its own pipe — using the same transport as both endpoints
makes the transport effectively have no peer, so writes block forever.

**Fix**: `mcp.NewInMemoryTransports()` returns two transport ends —
`sT` for the server, `cT` for the client. Updated both the Phase 0
test and the Phase 1 helpers to use both ends. Also captured the
`ServerSession` returned by `Server.Connect` so test cleanup closes
both ends; without it, `TestSingleReplay` left goroutines alive
across the test boundary.

### Phase 1 test was checking the wrong replay semantics

**Symptom**: First version of `TestPhase1_ReplayExactlyOnce`
asserted "upstream total calls = `len(calls)`" after running
identical calls twice — but the total was actually `2 * len(calls)`.

**Root cause**: I initially implemented a "match-by-signature"
BeginRequest to support a reading of the spec's gate where replays
collapse to the same journal sequence. That implementation
contradicted §3.2's explicit prohibition on signature-only keys:
"two legitimate, distinct calls to the same tool with the same
arguments … must not collide." Each call must have its own sequence
slot; replays of distinct calls produce distinct sequences, both
with their own upstream invocations.

**Fix**: Reverted `BeginRequest` to MAX+1 forward progress (with
PENDING surfacing for IN_DOUBT), and rewrote the rapid property
test to assert the spec-accurate semantics: each `doRun` adds
exactly `len(calls)` upstream invocations, IN_DOUBT sub-test
verifies a PENDING row prevents the upstream from being called at
all, and `runManyReplayTest` asserts total = N × len(calls) after
N runs against the same journal.

## Phase 0

No fixes recorded — first build was clean.

## Phase 3–5

### Algorithm J broke two-distinct-same-args calls

**Symptom**: First divergence-test iteration, and then the Phase 1
gate after the rewrite, started failing. `run 1 upstream calls =
1, want 3` for a 3-call sequence — only the first call invoked
upstream.

**Root cause**: I introduced an in-memory `nextSeq` cursor per
`JournaledHandler`. The logic: `lookup entry at cursor; Replay if
match, Proceed if absent`. Within a single proxy instance, two
sequential calls to the same tool with similar (but not identical)
signatures would land on consecutive sequences — call 1 at seq 1
(no row, Proceed, Complete), call 2 at seq 2 (finds call 1's
COMPLETED row at seq 2 — wait, no, the entry is at seq 1 not seq
2; cursor=2 looks up seq 2 which is empty). Wait, the actual
failure was simpler: each tool has its own JournaledHandler with
its OWN cursor, and the cursor advances only after
Reserve→Complete. So call 1 at cursor=1, Proceeds, upstream
called, Complete, cursor→2. Call 2 at cursor=2, lookup seq 2,
empty, Proceeds, upstream called, cursor→3. That should call
upstream twice for two calls.

The real failure surfaced when the cursor's `nextSeq` was shared
ACROSS handlers in a way that made the second call look up the
*first call's* sequence (1), find the COMPLETED row there with
the first call's sig, mismatch the second call's sig, ActionDiverge
— no upstream call. That came from a different wiring where the
cursor defaulted to MIN over all entries instead of MAX+1.

**Fix**: Reverted to Algorithm A (MAX+1 forward progress, PENDING
surfacing for IN_DOUBT). For "two distinct calls with the same
arguments" this is the spec-correct behavior: each call gets its
own sequence slot (§3.2 forbids signature-based collision). The
Phase 3 divergence gate now uses `_meta["checkpoint_seq"]` to
pin calls to specific sequences when an orchestrator wants to
deliberately replay/divergence-test a sequence.

### `assertJournalAfterRun2` over-asserted for `after_complete`

**Symptom**: After the after_complete case in
`TestCrashPhase`, the journal-state assertion found an extra row
at `seq=2 COMPLETED` and failed.

**Root cause**: Test was written assuming cross-process replay
should collapse on the same sequence, but Algorithm A correctly
gives each call a fresh sequence per spec §3.2.

**Fix**: Updated `rowExpect` for `after_complete` to allow both
seq 1 and seq 2 with a comment explaining why two rows is
correct.

## Phase 2 (continuation)

### Crash test stdin/stdout cross-deadlock

**Symptom**: First iteration of the crash test hung reading the
initialize response from the proxy.

**Root cause**: Two issues compounded: (a) the test's blocking
`read()` had no timeout, and (b) the proxy's `StdioTransport` was
treatng parent stdin EOF as a graceful shutdown trigger rather
than letting the test driver drive its own timing.

**Fix**: Replaced `read()` with a polling `readForID()` that times
out at 50 ms × N, and explicitly kept the parent's stdin pipe
open for the proxy's lifetime.
