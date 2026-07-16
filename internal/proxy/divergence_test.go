package proxy

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pgregory.net/rapid"

	"github.com/seanrobmerriam/checkpoint/internal/idempotency"
	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// encodeJSON is a thin shim so the test file doesn't import encoding/json
// in two places.
func encodeJSON(v any) ([]byte, error) { return json.Marshal(v) }

// divEchoIn matches the schema of the divergence-test upstream's
// `echo` tool.
type divEchoIn struct {
	Message string `json:"message"`
}

// divHarness is the upstream fixture for the Phase 3 divergence test.
// It exposes one tool (echo), and counts how many times upstream was
// actually invoked — the proxy must NEVER have invoked upstream past
// a divergence point.
type divHarness struct {
	mu    sync.Mutex
	calls int
}

func startDivHarness(t *testing.T) (*mcp.Server, *divHarness) {
	t.Helper()
	h := &divHarness{}
	srv := mcp.NewServer(&mcp.Implementation{Name: "div-upstream", Version: "0.0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "echo"},
		func(ctx context.Context, req *mcp.CallToolRequest, in divEchoIn) (*mcp.CallToolResult, any, error) {
			h.mu.Lock()
			h.calls++
			h.mu.Unlock()
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: in.Message}},
			}, nil, nil
		})
	return srv, h
}

func (h *divHarness) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// divTarget is the meta-based way a downstream client signals which
// sequence the call targets. The journal's BeginRequest consults this
// when present and surfaces the existing row for replay/divergence
// checks (per §3.4). Without it the proxy would just advance to
// MAX+1 and never detect that a "same logical call" was already
// made with different arguments.
const divTargetMetaKey = "checkpoint_seq"

// TestPhase3_DivergenceStrictMode is the Phase 3 gate. It draws an
// arbitrary sequence of `echo` calls, journals them at sequences 1..N
// (each call sends _meta: {"checkpoint_seq": K}), then re-drives
// the same sequence with one mutated call. Per §3.4 strict mode the
// proxy must refuse at the mutated position with a structured
// divergence error and must not execute past it.
func TestPhase3_DivergenceStrictMode(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(3, 6).Draw(rt, "n")
		orig := make([]toolCall, n)
		for i := 0; i < n; i++ {
			orig[i] = toolCall{
				tool: "echo",
				args: divEchoIn{Message: rapid.StringN(2, 4, 12).Draw(rt, key("orig", i))},
			}
		}
		divPos := rapid.IntRange(0, n-1).Draw(rt, "divPos")
		alt := divEchoIn{Message: rapid.StringN(2, 4, 12).Draw(rt, "alt")}
		for alt.Message == orig[divPos].args.(divEchoIn).Message {
			alt = divEchoIn{Message: rapid.StringN(2, 4, 12).Draw(rt, "alt")}
		}
		div := append([]toolCall{}, orig...)
		div[divPos] = toolCall{tool: "echo", args: alt}

		runDivergenceTest(t, orig, div, divPos)
	})
}

func key(prefix string, i int) string { return prefix + "-" + itoa(i) }

// itoa is a tiny helper without depending on strconv to keep imports
// simple in the hot gen path.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	buf := []byte{}
	for i > 0 {
		buf = append([]byte{byte('0' + i%10)}, buf...)
		i /= 10
	}
	return string(buf)
}

// runDivergenceTest drives the original sequence (with each call
// pinned to its sequence via _meta.checkpoint_seq), then drives a
// divergent sequence also pinned. The proxy refuses at the divergence
// point and never executes past it.
func runDivergenceTest(t *testing.T, orig, div []toolCall, divPos int) {
	t.Helper()
	dir := t.TempDir()
	jPath := joinPath(dir, "journal.db")
	ctx := context.Background()

	srv, harness := startDivHarness(t)

	// Run 1: each call pinned to seq 1..N.
	results1 := doRunDiv(t, ctx, srv, jPath, "div-wf", orig, true /*pinned*/)
	if got := harness.count(); got != len(orig) {
		t.Fatalf("after run 1: upstream saw %d calls, want %d", got, len(orig))
	}
	_ = results1

	// Run 2: divergent sequence, also pinned. The proxy must
	// refuse at divPos and never invoke upstream again.
	pre2 := harness.count()
	results2 := doRunDiv(t, ctx, srv, jPath, "div-wf", div, true /*pinned*/)
	post2 := harness.count()
	if post2 != pre2 {
		t.Fatalf("divergent run 2: upstream count went from %d to %d (replay past divergence must not call upstream)",
			pre2, post2)
	}

	// results2[divPos] must be an ERR-prefixed string with
	// "divergence".
	divergent := results2[divPos]
	if !divergentHasError(divergent, "divergence") {
		t.Fatalf("call %d (divPos=%d) was not rejected with divergence error; got %q",
			divPos+1, divPos, divergent)
	}
	// Positions before divPos: clean replay, no error.
	for i := 0; i < divPos; i++ {
		if startsWithErrPrefix(results2[i]) {
			t.Fatalf("call %d (before divergence): got error %q; expected clean replay result",
				i+1, results2[i])
		}
	}
	// Positions after divPos: per the spec's gate reading, "never
	// executes past it" — the divergent call itself must not have
	// been executed. The post-divergence calls in this test happen
	// to be independent re-runs that proceed normally; we only
	// check that upstream's count didn't advance past the
	// divergence point (which `post2 != pre2` already enforces
	// above).
	_ = startsWithErrPrefix

	// Inspect journal state. Must have exactly len(orig) rows.
	db, err := journal.Open(journal.Config{Path: jPath})
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer db.Close()

	for i := int64(len(orig) + 1); ; i++ {
		e, err := db.Lookup(ctx, "div-wf", i)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if e == nil {
			break
		}
		t.Fatalf("extra journal row at seq=%d: status=%s", i, e.Status)
	}

	// Verify the original seq=divPos+1 row's signature matches the
	// original call's signature, not the divergent one. (If the
	// proxy had silently overwritten, this would fail.)
	originalSig, err := computeSignature(orig[divPos])
	if err != nil {
		t.Fatalf("compute sig: %v", err)
	}
	e, err := db.Lookup(ctx, "div-wf", int64(divPos+1))
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if e.CallSignature != originalSig {
		t.Fatalf("seq=%d signature: %s, want %s — divergence silently overwrote journal?",
			divPos+1, e.CallSignature[:16], originalSig[:16])
	}

	// Also assert: the divergent row was NEVER written. Look up
	// the signature of the divergent call and check no row has it.
	divSig, err := computeSignature(div[divPos])
	if err != nil {
		t.Fatalf("compute div sig: %v", err)
	}
	for i := int64(1); i <= int64(len(orig)); i++ {
		row, _ := db.Lookup(ctx, "div-wf", i)
		if row.CallSignature == divSig {
			t.Fatalf("seq=%d has divergent signature — divergence was supposed to refuse, not execute",
				i)
		}
	}
}

// doRunDiv wires up the upstream + journal + proxy + downstream-client
// stack and drives the given calls, optionally pinning each call to
// its 1-based sequence in the journal via _meta. Returns the
// (possibly ERR-prefixed) string results for each call, in order.
func doRunDiv(t *testing.T, ctx context.Context, srv *mcp.Server, jPath, wf string, calls []toolCall, pinned bool) []string {
	t.Helper()
	db, err := journal.Open(journal.Config{Path: jPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sT, cT := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, sT, nil); err != nil {
		t.Fatalf("upstream connect: %v", err)
	}

	p, err := New(ctx, Config{
		Upstream:   cT,
		WorkflowID: wf,
		Journal:    db,
	})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	cs := drivingClient(t, ctx, p.Downstream)

	out := driveCallsPinned(t, ctx, cs, calls, pinned, wf)
	if err := p.Close(); err != nil {
		t.Logf("close: %v", err)
	}
	return out
}

// driveCallsPinned is driveCalls with optional per-call pinning via
// _meta["checkpoint_seq"]. pinned=true → seq is K's 1-based position;
// pinned=false → no meta, advances to MAX+1.
func driveCallsPinned(t *testing.T, ctx context.Context, cs *mcp.ClientSession, calls []toolCall, pinned bool, wf string) []string {
	t.Helper()
	out := make([]string, len(calls))
	for i, c := range calls {
		params := &mcp.CallToolParams{Name: c.tool, Arguments: c.args}
		if pinned {
			seq := int64(i + 1)
			params.Meta = mcp.Meta{divTargetMetaKey: float64(seq)}
		}
		res, err := cs.CallTool(ctx, params)
		if err != nil {
			out[i] = "ERR:" + err.Error()
			continue
		}
		// Force a useful body for the assertion below.
		if res != nil && res.IsError {
			out[i] = "ERR:" + strings.TrimSpace(string(contentFirstText(res)))
			continue
		}
		raw, _ := marshalAny(res)
		out[i] = string(raw)
		_ = wf
	}
	return out
}

func marshalAny(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	return jsonMarshal(v)
}

// split out helpers to avoid an import cycle with the encoding
// package — keep this file dependency-light.
func jsonMarshal(v any) ([]byte, error) {
	b, _ := encodeJSON(v)
	return b, nil
}

func contentFirstText(res *mcp.CallToolResult) []byte {
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			return []byte(t.Text)
		}
	}
	return nil
}

// computeSignature returns the on-the-wire (tool, args) signature
// for the given tool call.
func computeSignature(c toolCall) (string, error) {
	argsHash, err := idempotency.HashArgs(c.args)
	if err != nil {
		return "", err
	}
	return idempotency.HashCallSignature(c.tool, argsHash), nil
}
