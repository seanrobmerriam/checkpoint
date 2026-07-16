package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/seanrobmerriam/checkpoint/internal/faultinject"
	"github.com/seanrobmerriam/checkpoint/internal/idempotency"
	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// Handler decides what to do with a tools/call arriving on the downstream
// side of the proxy and produces a result.
//
// The proxy registers one Handler per mirrored tool.
//
// PassthroughHandler is the no-journal implementation. JournaledHandler
// is the implementation that routes through the journal + state machine.
type Handler interface {
	Handle(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error)
}

// PassthroughHandler forwards calls to the upstream session verbatim with
// no journal at all — used by Phase 0 tests and as a fall-through when
// no journal is configured.
type PassthroughHandler struct {
	Upstream *mcp.ClientSession
	ToolName string
}

func (h *PassthroughHandler) Handle(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return h.Upstream.CallTool(ctx, &mcp.CallToolParams{
		Name:      h.ToolName,
		Arguments: req.Params.Arguments,
	})
}

// JournaledHandler is the production handler for Phases ≥ 1. It routes
// each call through the journal + state machine per §3.4.
//
// Concurrency: this handler is a per-workflow+tool serialization point.
// All Handle calls on this handler share workflowMu, so two concurrent
// `tools/call`s to the same tool in the same workflow can never
// interleave BeginRequest → ReservePending → CallUpstream → Complete.
// This is the v1 tradeoff documented in PROGRESS.md: simpler, safer,
// still concurrent across workflows.
type JournaledHandler struct {
	Upstream         *mcp.ClientSession
	DB               *journal.DB
	WorkflowID       string
	ToolName         string
	DivergencePolicy journal.DivergencePolicy
	// TrustAnnotations enables §3.3's read-only fast path.
	TrustAnnotations bool

	workflowMu sync.Mutex
	mirrors    MirrorStore
}

// Handle implements Handler. See §3.4 of the spec for the state machine
// semantics.
//
// Sequence assignment: each call to Handle increments MAX(sequence)+1
// for this workflow. This avoids "two legitimate distinct calls with
// the same arguments" collapsing onto the same sequence — a constraint
// the spec calls out explicitly in §3.2. The corollary is that
// cross-process replay (a fresh proxy against an existing journal)
// cannot detect "same call re-attempted" via the journal alone —
// that requires an explicit replay signal outside this MVP, and
// would change the cursor model.
//
// Phase 5 — read-only fast path (§3.3): when TrustAnnotations is set
// and the mirrored tool's annotations declare readOnlyHint=true,
// the handler skips journaling entirely. This is the only
// optimization guarded by a config flag, because annotations are
// upstream-controlled hints we cannot verify.
func (h *JournaledHandler) Handle(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	h.workflowMu.Lock()
	defer h.workflowMu.Unlock()

	// Phase 5 fast path — read-only tools skip journaling.
	// Annotations are a hint from upstream; we only honor them when
	// the operator opts in via TrustAnnotations.
	if h.TrustAnnotations && h.readOnlyProbe() {
		return h.Upstream.CallTool(ctx, &mcp.CallToolParams{
			Name:      h.ToolName,
			Arguments: req.Params.Arguments,
		})
	}

	argsHash, err := idempotency.HashArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: hash args: %w", err)
	}
	sig := idempotency.HashCallSignature(h.ToolName, argsHash)

	// Default: forward-progress (Algorithm A). Tests can override the
	// target sequence via _meta["checkpoint_seq"] to drive replay/
	// divergence paths deterministically.
	targetSeq := readTargetSeq(req)
	seq, existing, err := h.DB.BeginRequest(ctx, h.WorkflowID, sig, targetSeq)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: begin: %w", err)
	}

	decision, err := journal.Transition(existing, journal.Incoming{
		Tool:     h.ToolName,
		ArgsHash: sig,
	}, h.DivergencePolicy)
	if err != nil {
		return nil, err
	}

	switch decision.Action {
	case journal.ActionReplay:
		return decodeReplay(decision)

	case journal.ActionInDoubt:
		return nil, fmt.Errorf("checkpoint: in-doubt at (%s, %d) — "+
			"recorded intent but no outcome; reconcile manually before retrying",
			h.WorkflowID, seq)

	case journal.ActionDiverge:
		return nil, fmt.Errorf("checkpoint: divergence at (%s, %d) — "+
			"replayed call has different arguments than journaled history "+
			"(expected sig %s, got %s); refusing to execute",
			h.WorkflowID, seq,
			decision.ExpectedSignature, decision.ReceivedSignature)

	case journal.ActionProceed:
		// fall through to invoke upstream
	}

	faultinject.Point(faultinject.PointBeforeReserve)
	rawArgs, mErr := marshalArgsForJournal(req.Params.Arguments)
	if mErr != nil {
		return nil, fmt.Errorf("checkpoint: marshal args: %w", mErr)
	}
	if err := h.DB.ReservePending(ctx, h.WorkflowID, seq, h.ToolName, sig, rawArgs); err != nil {
		return nil, fmt.Errorf("checkpoint: reserve: %w", err)
	}
	faultinject.Point(faultinject.PointAfterReserve)

	res, callErr := h.Upstream.CallTool(ctx, &mcp.CallToolParams{
		Name:      h.ToolName,
		Arguments: req.Params.Arguments,
	})
	faultinject.Point(faultinject.PointMidUpstream)

	if callErr != nil {
		errMsg := callErr.Error()
		if dbErr := h.DB.Fail(ctx, h.WorkflowID, seq, errMsg); dbErr != nil {
			return nil, fmt.Errorf("checkpoint: upstream call failed (%v) "+
				"and journaling the failure also failed (%v); state is "+
				"now IN_DOUBT — manual reconciliation required",
				callErr, dbErr)
		}
		return nil, callErr
	}

	faultinject.Point(faultinject.PointBeforeComplete)
	resultJSON, err := json.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: marshal result for storage: %w "+
			"(call returned successfully but state is now IN_DOUBT)", err)
	}
	if err := h.DB.Complete(ctx, h.WorkflowID, seq, resultJSON); err != nil {
		return nil, fmt.Errorf("checkpoint: complete: %w "+
			"(result already returned upstream; this will replay on next attempt)", err)
	}
	faultinject.Point(faultinject.PointAfterComplete)

	return res, nil
}

// decodeReplay turns a Decision into a CallToolResult. Replay never
// touches upstream — that's the whole point.
func decodeReplay(d journal.Decision) (*mcp.CallToolResult, error) {
	if d.ReplayError != "" {
		return nil, errors.New(d.ReplayError)
	}
	if len(d.ReplayResult) == 0 {
		return &mcp.CallToolResult{Content: []mcp.Content{}}, nil
	}
	var res mcp.CallToolResult
	if err := json.Unmarshal(d.ReplayResult, &res); err != nil {
		return nil, fmt.Errorf("checkpoint: decode replay result: %w", err)
	}
	return &res, nil
}

// marshalArgsForJournal normalizes on-the-wire args into bytes for
// storage. idempotency.HashArgs already validated the JSON. Empty
// args become "null" so the BLOB NOT NULL column is satisfied.

// MirrorStore is a tiny interface the journaled handler uses to look
// up the mirrored tool's annotations. The Proxy's ToolByName map
// satisfies it via the wrapper below.
type MirrorStore interface {
	Lookup(name string) (*mcp.Tool, bool)
}

// SetMirrorStore attaches a MirrorStore to a JournaledHandler so
// the read-only fast path can inspect the upstream-declared
// annotation. Called by proxy.mirror() once the tool list has
// been collected.
func (h *JournaledHandler) SetMirrorStore(s MirrorStore) {
	h.mirrors = s
}

// readOnlyProbe returns true when the mirrored tool's annotations
// declare readOnlyHint=true. Returns false (the conservative answer)
// when no mirror store was attached or the tool has no annotations.
func (h *JournaledHandler) readOnlyProbe() bool {
	if h.mirrors == nil {
		return false
	}
	t, ok := h.mirrors.Lookup(h.ToolName)
	if !ok || t == nil || t.Annotations == nil {
		return false
	}
	return t.Annotations.ReadOnlyHint
}

// readTargetSeq extracts a caller-supplied target sequence from
// req.Params.Meta. The convention is `_meta["checkpoint_seq"] =
// <int64>`. When missing or zero, the journal uses MAX+1 (forward
// progress). This is the only metadata the proxy reads from the
// MCP request — see §3.1 on not auto-deriving workflow identity
// from session negotiation.
//
// Test-only escape hatch: it's also a documented feature for
// agents that want to specify their replay position explicitly
// (which is required to make cross-process replay/divergence
// detection work, since the journal alone doesn't know the
// agent's logical call position).
func readTargetSeq(req *mcp.CallToolRequest) int64 {
	if req == nil || req.Params == nil {
		return 0
	}
	m := req.Params.Meta
	if m == nil {
		return 0
	}
	v, ok := m["checkpoint_seq"]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int:
		return int64(x)
	case int64:
		return x
	case string:
		n, err := json.Number(x).Int64()
		if err == nil {
			return n
		}
	}
	return 0
}

// marshalArgsForJournal normalizes on-the-wire args into bytes for
// storage. idempotency.HashArgs already validated the JSON. Empty
// args become "null" so the BLOB NOT NULL column is satisfied.
func marshalArgsForJournal(args any) ([]byte, error) {
	if args == nil {
		return []byte("null"), nil
	}
	switch a := args.(type) {
	case json.RawMessage:
		if len(a) == 0 {
			return []byte("null"), nil
		}
		return a, nil
	case []byte:
		if len(a) == 0 {
			return []byte("null"), nil
		}
		return a, nil
	}
	b, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return b, nil
}
