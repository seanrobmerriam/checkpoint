# AGENTS.md — orchestrating Checkpoint

This is a short guide for the agent framework or orchestrator that
will drive Checkpoint as its downstream MCP proxy. Read this once
before integrating.

## TL;DR

- Checkpoint is a stdio MCP proxy. It requires no protocol changes
  to upstream — it speaks plain MCP to whatever you point it at.
- It only adds value when you pass `--journal`. Without that flag
  it's a transparent pass-through (the spec calls this Phase 0
  behavior; useful for testing).
- For crash-safety the orchestrator must re-run the same tools
  after the proxy restarts. Checkpoint will replay everything that
  matches the journal, refuse on divergence, and surface
  `IN_DOUBT` for side effects that crashed mid-flight.

## Conventions

1. **One journal per workflow.** A workflow is "one logical run"
   in the spec's terminology. Restarting Checkpoint with a
   different `--journal` path is starting a new workflow. Pick
   the path deterministically (e.g., based on user ID + session
   ID) so a restarted orchestrator lands on the same path.
2. **Don't share a journal across processes.** Checkpoint is
   designed for single-writer per journal. Two checkpoint processes
   pointing at the same `.db` will silently corrupt the WAL.
3. **`--workflow-id` is optional.** If omitted, it defaults to the
   journal path, which is usually what you want. Set it explicitly
   when journals move around (e.g., cloud-storage backend) and
   you want a stable identity.

## Three MCP-level signals to know about

### 1. `tools/call` errors fall into three categories

- **Normal errors** (`IsError: true` in the result body, no Go
  error). These are tool-level errors. They're persisted as
  `COMPLETED` rows in the journal and replayed normally.
- **Protocol errors** (Go-level error from the SDK). These are
  recorded as `FAILED` in the journal; on replay they re-emit as
  protocol errors downstream.
- **Checkpoint errors** (Go-level error, but with a `"checkpoint: ..."`
  prefix and a specific keyword):
  - `in-doubt` — a previous run crashed between Reserve and
    Complete/Fail. Operator must reconcile before continuing.
  - `divergence` — the agent tried to replay a sequence position
    with different arguments than were journaled there. The proxy
    refuses; you must re-plan that part of the workflow.

### 2. `_meta["checkpoint_seq"]` for replay/divergence hints

If you want to surface an "I want to replay call N" intent
explicitly (e.g., after a partial restart), pass:

```json
{"jsonrpc": "2.0", "method": "tools/call", "params": {
  "name": "tool",
  "arguments": {...},
  "_meta": {"checkpoint_seq": 5}
}}
```

The proxy looks up the journal row at exactly that sequence.
Matching signature → replay; mismatching signature → strict
refusal (or fork, if you opt into `--on-divergence=fork` once that
mode ships).

Without this hint, the proxy defaults to forward progress: each
call claims the next free sequence.

### 3. Notifications get pass-through

Checkpoint doesn't journal `notifications/*` traffic. If the
upstream fires a tool list-change notification, Checkpoint
re-mirrors and re-emits to downstream. Don't rely on
`notifications/tools/list_changed` for anything state-changing.

## What to do on a crash

1. Restart Checkpoint with the same `--journal` path.
2. Don't touch the journal file directly. SQLite WAL handles
   durability.
3. Re-run the same sequence of `tools/call`s your agent was
   making. Checkpoint will either replay the cached result
   (no upstream call) or refuse with `in-doubt`/`divergence` if
   the previous run left things in an ambiguous state.
4. Handle `in-doubt` and `divergence` errors as orchestrator-level
   signals: log them, surface them to the user, optionally stop
   the workflow and let the operator reconcile.
5. Don't `DELETE FROM calls` to "force a re-run." That destroys
   the durability guarantee and may cause upstream to be invoked
   twice for already-committed side effects.

## Configuration knobs

| Flag                          | When to enable                                          |
| ----------------------------- | ------------------------------------------------------- |
| `--trust-annotations`         | Only when you trust the upstream server to set `readOnlyHint` truthfully. Skips journaling for read-only tools — saving throughput at the cost of a misreport hazard. |
| `--redact-args-in-journal`     | When argument payloads contain secrets or PII. The hash signature still lets Checkpoint detect divergence, but you lose the ability to render a human-readable diff. |
| `--on-divergence=strict`       | Default. Refuse mismatched replays.                       |
| `--on-divergence=fork`         | Not yet implemented; will branch to a derived workflow on mismatch. |

## What this proxy does NOT do

- Doesn't deduplicate calls across processes that pass the same
  arguments without `_meta["checkpoint_seq"]`. (See §3.2.)
- Doesn't decide which calls were idempotent at the upstream
  level — read-only tools that lie about their hint will be
  double-invoked on restart if `--trust-annotations` is set.
- Doesn't reconcile `IN_DOUBT` calls automatically. Operator or
  tool-specific reconciliation logic must do that.
- Doesn't do real-time streaming outputs. The proxy serializes
  tool calls through the journal; progress notifications are
  passed through but not journaled.
