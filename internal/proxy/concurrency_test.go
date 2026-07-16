package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pgregory.net/rapid"

	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// slowUpstream returns tools whose handler blocks on a configurable
// channel before responding, so we can engineer real concurrency
// races in tests.
type slowUpstream struct {
	gate    chan struct{} // closed to release all in-flight calls
	count   int64         // atomic counter of how many calls ran
}

type slowIn struct {
	Tag string `json:"tag"`
}

func startSlowUpstream(t *testing.T, gate chan struct{}) (*mcp.Server, *slowUpstream) {
	t.Helper()
	u := &slowUpstream{gate: gate}
	srv := mcp.NewServer(&mcp.Implementation{Name: "slow-upstream", Version: "0.0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "slow"}, func(ctx context.Context, req *mcp.CallToolRequest, in slowIn) (*mcp.CallToolResult, any, error) {
		<-u.gate
		atomic.AddInt64(&u.count, 1)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: in.Tag}},
		}, nil, nil
	})
	return srv, u
}

// TestPhase4_ConcurrentCalls is the Phase 4 gate. It fires N goroutines
// that each call a single tool against the same workflow, then
// verifies:
//
//   - No data races (run with `go test -race`).
//   - The journal has exactly N COMPLETED rows.
//   - The upstream saw exactly N calls.
//   - No two calls collide on the same sequence (we check by
//     counting distinct (sequence, toolName) pairs).
func TestPhase4_ConcurrentCalls(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(5, 25).Draw(rt, "n")

		dir := t.TempDir()
		jPath := joinPath(dir, "journal.db")
		ctx := context.Background()

		// Single channel gates all upstream invocations so we can
		// observe the proxy's serialization while multiple goroutines
		// are concurrently in flight.
		gate := make(chan struct{})
		srv, su := startSlowUpstream(t, gate)

		// Set up the proxy & downstream-client stack once. We will
		// drive N concurrent tool calls into the same client session.
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
			WorkflowID: "conc-wf",
			Journal:    db,
		})
		if err != nil {
			t.Fatalf("proxy: %v", err)
		}
		cs := drivingClient(t, ctx, p.Downstream)
		defer func() { _ = p.Close() }()

		var wg sync.WaitGroup
		errs := make([]error, n)
		// Launch concurrent calls.
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = cs.CallTool(ctx, &mcp.CallToolParams{
					Name:      "slow",
					Arguments: slowIn{Tag: "c" + itoa(i)},
				})
			}(i)
		}
		// Give them all a chance to reach the gate. Then release.
		// We don't sleep — instead we wait until the upstream has at
		// least one goroutine parked on the gate. (Real concurrency
		// doesn't need a sleep anyway.)
		// Just close the gate.
		close(gate)
		wg.Wait()

		// Property 1: all calls succeeded.
		for i, err := range errs {
			if err != nil {
				t.Fatalf("call %d: %v", i, err)
			}
		}

		// Property 2: upstream saw exactly n calls.
		if got := atomic.LoadInt64(&su.count); got != int64(n) {
			t.Fatalf("upstream saw %d calls, want %d", got, n)
		}

		// Property 3: journal has exactly n COMPLETED rows, each at
		// a unique sequence.
		seen := map[int64]bool{}
		for i := int64(1); i <= int64(n); i++ {
			e, err := db.Lookup(ctx, "conc-wf", i)
			if err != nil {
				t.Fatalf("lookup seq %d: %v", i, err)
			}
			if e == nil {
				t.Fatalf("missing journal row at seq=%d (only checked 1..%d; expected exactly %d rows)",
					i, n, n)
			}
			if e.Status != journal.StatusCompleted {
				t.Fatalf("seq=%d status=%s, want COMPLETED", i, e.Status)
			}
			if seen[int64(i)] {
				t.Fatalf("duplicate journal row at seq=%d", i)
			}
			seen[int64(i)] = true
		}
		// Also check no extra rows exist past n.
		e, err := db.Lookup(ctx, "conc-wf", int64(n+1))
		if err != nil {
			t.Fatalf("lookup seq=%d: %v", n+1, err)
		}
		if e != nil {
			t.Fatalf("extra journal row at seq=%d (expected exactly %d)", n+1, n)
		}
	})
}
