# Checkpoint

Checkpoint is an [MCP](https://modelcontextprotocol.io/) proxy that
journals every `tools/call` to a durable SQLite store so a crashed
downstream agent can resume a workflow without re-triggering side
effects it already performed.

This is a systems-correctness project before it is a features
project — it sits in front of any other MCP server, mirrors its
tools/prompts/resources verbatim, and only adds journaling on
`tools/call`. A point release of Checkpoint must never silently
re-invoke a side effect its operator thought was committed.

## Build & run

```sh
go build -o checkpoint ./cmd/checkpoint
./checkpoint --upstream-cmd "my-real-server --flag value" --journal path/to/j.db
```

The proxy speaks MCP on **stdio** to its caller (downstream) and
spawns the upstream server as a child process, also over stdio.
Both halves use the same protocol so the upstream server doesn't
have to know it's being wrapped.

## Flags

| Flag                          | Default     | Effect                                                        |
| ----------------------------- | ----------- | ------------------------------------------------------------- |
| `--upstream-cmd`              | (required)  | shell command for the upstream MCP server                     |
| `--journal`                   | (off)       | path to SQLite journal file. Enable resumability.            |
| `--workflow-id`               | = journal   | workflow identifier (defaults to the journal path)            |
| `--on-divergence`             | `strict`    | `strict` (refuse) or `fork` (not yet implemented)             |
| `--trust-annotations`         | `false`     | honor upstream `readOnlyHint:true` and skip journaling        |
| `--redact-args-in-journal`     | `false`     | replace on-disk `arguments` with a stub                       |
| `--name`, `--version`         | `checkpoint`/`0.0.0` | downstream-facing server identity              |

## What Checkpoint actually guarantees

The text below is reproduced verbatim from §3.5 of the design spec
and is pinned to an integration test (`TestGuarantee_CompletedSideEffectNeverReexecuted`):

> Checkpoint guarantees that a completed side effect is never
> re-executed on replay, provided the replayed call sequence
> matches what was journaled. It does not guarantee a call that
> crashed mid-flight was not executed — that window is fundamentally
> unresolvable without upstream-specific reconciliation, and
> Checkpoint surfaces it rather than hiding it.

For the "matching call sequence" precondition to hold across
process restarts, callers must either pass
`_meta: {"checkpoint_seq": N}` on each `tools/call` (the proxy
consults the journal at that exact sequence for replay /
divergence decisions) or keep the agent driving calls within a
single proxy instance whose in-memory cursor advances per call.
Without one of these, the proxy defaults to forward progress
(`MAX(sequence)+1` per call) — distinct calls always get distinct
sequences, by design.

## IN_DOUBT vs Replay vs Diverge (per §3.4)

| Journal state at the targeted seq | Incoming sig     | Action      | What the proxy does                                        |
| --------------------------------- | ---------------- | ----------- | ---------------------------------------------------------- |
| (no row)                          | (any)            | Proceed     | Reserve PENDING (fsync) → call upstream → Complete/Fail    |
| COMPLETED with matching sig       | match            | Replay      | return stored result, **no upstream invocation**            |
| COMPLETED with mismatching sig    | mismatch         | Diverge     | refuse with structured error (strict mode) or branch (fork) |
| PENDING                           | (any)            | In-doubt    | refuse with distinct in-doubt error — operator must reconcile |

`IN_DOUBT` and `Diverge` are surfacing errors, not silent failures.

## What Checkpoint does NOT do (v1)

- **Cross-process replay detection without agent cooperation.** See
  the `_meta["checkpoint_seq"]` extension above. Without that hint
  (or a same-process drive), distinct calls with matching signatures
  collapse onto different sequences, not the same one.
- **Distributed locking.** A single Checkpoint process owns a
  journal at a time. Multi-process writes to one journal are unsafe.
- **`fork` divergence mode.** The flag is recognized but always
  refuses with "not yet implemented."
- **Tool that actually decides if something was idempotent.**
  That's upstream's job. Checkpoint records outcomes; reconciliation
  of `IN_DOUBT` calls is the operator's responsibility.

## Operational recommendations (for orchestrators)

1. **Use a stable journal path per workflow.** Same path across
   restarts → same workflow identity (Checkpoint's default).
2. **Reuse a single proxy instance per workflow** where possible.
   The in-memory cursor restores per-process forward progress; a
   fresh proxy against a populated journal defaults to MAX+1 and
   treats each subsequent call as new.
3. **Pass `_meta: {"checkpoint_seq": N}`** when you want to
   replay a specific step explicitly. Without it, Checkpoint is
   conservative — collisions don't happen, but neither does
   divergence detection.
4. **Don't enable `--trust-annotations` unless you trust the
   upstream server to set `readOnlyHint` truthfully.** A misreport
   would cause Checkpoint to skip journaling for a non-idempotent
   tool; on the next restart, that side effect would be re-executed.

See also `AGENTS.md` for orchestration hints.
