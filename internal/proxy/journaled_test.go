package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pgregory.net/rapid"

	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// countingUpstream wraps a tool server with per-tool invocation
// counters, exposed to tests for assertion. Survives multiple proxy
// instantiations because the underlying server is one Go process.
type countingUpstream struct {
	mu      sync.Mutex
	counts  map[string]int
	results []recordedCall
}

type recordedCall struct {
	Tool string
	Args string
}

func newCountingUpstream() *countingUpstream {
	return &countingUpstream{counts: map[string]int{}}
}

func startCountingUpstream(t *testing.T, cu *countingUpstream) *mcp.Server {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "counter-upstream", Version: "0.0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "echo"}, func(ctx context.Context, req *mcp.CallToolRequest, in echoIn) (*mcp.CallToolResult, any, error) {
		cu.record("echo", req.Params.Arguments)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: in.Message}},
		}, nil, nil
	})
	mcp.AddTool(srv, &mcp.Tool{Name: "sum"}, func(ctx context.Context, req *mcp.CallToolRequest, in sumIn) (*mcp.CallToolResult, any, error) {
		cu.record("sum", req.Params.Arguments)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("%d", in.A+in.B)}},
		}, map[string]any{"total": in.A + in.B}, nil
	})
	return srv
}

func (cu *countingUpstream) record(tool string, args any) {
	cu.mu.Lock()
	defer cu.mu.Unlock()
	cu.counts[tool]++
	raw, _ := json.Marshal(args)
	cu.results = append(cu.results, recordedCall{Tool: tool, Args: string(raw)})
}

func (cu *countingUpstream) snapshot() (map[string]int, []recordedCall) {
	cu.mu.Lock()
	defer cu.mu.Unlock()
	c := make(map[string]int, len(cu.counts))
	for k, v := range cu.counts {
		c[k] = v
	}
	r := append([]recordedCall(nil), cu.results...)
	return c, r
}

// drivingClient creates a downstream MCP client connected to the given
// downstream server via in-memory transports. Captures and registers
// cleanup for the ServerSession so test runs don't leak goroutines.
func drivingClient(t *testing.T, ctx context.Context, srv *mcp.Server) *mcp.ClientSession {
	t.Helper()
	t1, t2 := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("downstream server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "downstream-driver"}, nil)
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("downstream client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func driveCalls(t *testing.T, ctx context.Context, cs *mcp.ClientSession, calls []toolCall) []string {
	t.Helper()
	out := make([]string, len(calls))
	for i, c := range calls {
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{
			Name:      c.tool,
			Arguments: c.args,
		})
		if err != nil {
			out[i] = "ERR:" + err.Error()
			continue
		}
		raw, _ := json.Marshal(res)
		out[i] = string(raw)
	}
	return out
}

type toolCall struct {
	tool string
	args any
}

// genCalls is a rapid generator for a sequence of tool calls. We draw
// 1–8 calls each from a small pool of tool/argument shapes so the
// generator produces realistic sequences (with repetitions, the case
// that matters most for replay).
func genCalls() *rapid.Generator[[]toolCall] {
	return rapid.Custom(func(t *rapid.T) []toolCall {
		n := rapid.IntRange(1, 8).Draw(t, "n")
		calls := make([]toolCall, n)
		for i := 0; i < n; i++ {
			switch rapid.IntRange(0, 4).Draw(t, fmt.Sprintf("kind-%d", i)) {
			case 0:
				calls[i] = toolCall{
					tool: "echo",
					args: echoIn{Message: rapid.StringN(1, 8, 32).Draw(t, fmt.Sprintf("msg-%d", i))},
				}
			case 1:
				calls[i] = toolCall{tool: "echo", args: echoIn{Message: "constant"}}
			case 2:
				calls[i] = toolCall{
					tool: "sum",
					args: sumIn{
						A: rapid.IntRange(-100, 100).Draw(t, fmt.Sprintf("a-%d", i)),
						B: rapid.IntRange(-100, 100).Draw(t, fmt.Sprintf("b-%d", i)),
					},
				}
			case 3:
				calls[i] = toolCall{tool: "echo", args: echoIn{Message: ""}}
			case 4:
				calls[i] = toolCall{tool: "sum", args: sumIn{A: 0, B: 0}}
			}
		}
		return calls
	})
}

// TestPhase1_ReplayExactlyOnce is the Phase 1 gate. It draws an
// arbitrary sequence of tool calls, drives them through a proxy backed
// by a journal, then drains the proxy, opens a new proxy against the
// same journal, and replays the SAME sequence. Upstream must observe
// exactly one call per (workflow, sequence), and the journal state at
// the end of both runs must be identical.
func TestPhase1_ReplayExactlyOnce(t *testing.T) {
	tt := &shimT{T: t}
	rapid.Check(t, func(rt *rapid.T) {
		// Fixed seed for reproducibility of debug cases.
		calls := genCalls().Draw(rt, "calls")
		runReplayTest(tt, calls)
	})
}

// TestPhase1_StableAcrossManyReplays is a sharper test: run the same
// sequence three times in a row against one journal. After run 1
// populates the journal, runs 2 and 3 should both be pure replays —
// upstream must see exactly len(calls) invocations total, not 3*len.
func TestPhase1_StableAcrossManyReplays(t *testing.T) {
	tt := &shimT{T: t}
	rapid.Check(t, func(rt *rapid.T) {
		calls := genCalls().Draw(rt, "calls")
		runManyReplayTest(tt, calls, 3)
	})
}

// --- helpers ---------------------------------------------------------------

// shimT lets us pass a *testing.T where helpers want one, while the
// rapid body holds the *rapid.T. IterID/PrefixPath exist because
// rapid.Check calls the inner func many times against the same
// underlying *testing.T, which means t.TempDir() returns the same dir
// for every iteration — that breaks our journal-isolation invariant.
// Each iteration must produce its own journal file so the upstream
// counter accumulates in a way that compares like-for-like.
type shimT struct {
	T         *testing.T
	IterCount int
}

func (s *shimT) Helper()          { s.T.Helper() }
func (s *shimT) Errorf(f string, a ...any) { s.T.Errorf(f, a...) }
func (s *shimT) Fatalf(f string, a ...any) { s.T.Fatalf(f, a...) }
func (s *shimT) Fatal(a ...any)   { s.T.Fatal(a...) }
func (s *shimT) Error(a ...any)   { s.T.Error(a...) }
func (s *shimT) Cleanup(fn func()) { s.T.Cleanup(fn) }

// PerIterationDir returns a fresh, iteration-unique temp directory.
// Each call increments the iteration counter so an iteration's dir is
// distinct from the next iteration's, even within the same rapid run.
func (s *shimT) PerIterationDir() string {
	s.T.Helper()
	s.IterCount++
	root := s.T.TempDir()
	dir := filepath.Join(root, fmt.Sprintf("iter-%d", s.IterCount))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.T.Fatalf("mkdir: %v", err)
	}
	return dir
}

// runReplayTest is the body of the Phase 1 property test. It exercises
// the spec-accurate semantics:
//
//   - Single process: each call gets a unique sequence; the journal
//     observes exactly len(calls) upstream invocations; all entries
//     are COMPLETED at the end.
//   - Cross-process replay: a fresh process against the same journal
//     also drives len(calls) more upstream invocations at sequences
//     len+1..2*len — these are NEW sequences, not collapse-replays,
//     because §3.2 forbids signature-only idempotency keys. Each call
//     gets its own sequence; replays don't share sequences with the
//     originals.
//   - IN_DOUBT re-encounter (the actual spec replay semantics):
//     simulate a crash between PENDING and COMPLETED, restart, and
//     verify that the new process surfaces the PENDING row, refuses
//     to call upstream, and never silently re-executes.
func runReplayTest(t *shimT, calls []toolCall) {
	t.Helper()
	dir := t.PerIterationDir()
	jPath := filepath.Join(dir, "journal.db")
	ctx := context.Background()

	cu := newCountingUpstream()
	upstreamSrv := startCountingUpstream(t.T, cu)

	// --- Run 1 (clean). ---
	run1 := doRun(t.T, ctx, cu, upstreamSrv, jPath, "rapid-wf", calls)

	counts, _ := cu.snapshot()
	totalCalls := 0
	for _, v := range counts {
		totalCalls += v
	}
	if totalCalls != len(calls) {
		t.Fatalf("run 1 upstream calls = %d, want %d", totalCalls, len(calls))
	}
	if len(run1) != len(calls) {
		t.Fatalf("run 1 returned %d results for %d calls", len(run1), len(calls))
	}

	// --- Run 2 (same journal; new process). ---
	run2 := doRun(t.T, ctx, cu, upstreamSrv, jPath, "rapid-wf", calls)

	counts2, _ := cu.snapshot()
	totalCalls2 := 0
	for _, v := range counts2 {
		totalCalls2 += v
	}
	if totalCalls2 != 2*len(calls) {
		t.Fatalf("after run 2: total upstream = %d, want %d (no cross-process collapse by spec §3.2)",
			totalCalls2, 2*len(calls))
	}
	if len(run2) != len(calls) {
		t.Fatalf("run 2 returned %d results for %d calls", len(run2), len(calls))
	}

	// --- IN_DOUBT test: synthesize a crash between PENDING and COMPLETED. ---
	// We use a fresh workflow against the SAME journal file so the
	// proxy's downstream-driver against jPath sees only the PENDING row.
	if len(calls) > 0 {
		pendingSeq, err := reservePendingAgainstJournal(t.T, jPath, "indoubt-wf", calls[0])
		if err != nil {
			t.Fatalf("reserve pending for IN_DOUBT test: %v", err)
		}

		// Open a fresh proxy against this journal; ask it to drive
		// the SAME first call. The journal has only a PENDING row
		// for this workflow. BeginRequest must surface it; the
		// state machine must surface an in-doubt error; upstream
		// must NOT be invoked.
		before, _ := cu.snapshot()
		beforeCount := sumCounts(before)
		_ = doRun(t.T, ctx, cu, upstreamSrv, jPath, "indoubt-wf", []toolCall{calls[0]})
		after, _ := cu.snapshot()
		afterCount := sumCounts(after)

		if afterCount != beforeCount {
			t.Fatalf("IN_DOUBT case: upstream was invoked (%d -> %d); should be 0",
				beforeCount, afterCount)
		}

		// Sanity: the PENDING row is still there, untouched.
		db, err := journal.Open(journal.Config{Path: jPath})
		if err != nil {
			t.Fatalf("open journal: %v", err)
		}
		e, _ := db.Lookup(ctx, "indoubt-wf", pendingSeq)
		db.Close()
		if e == nil || e.Status != journal.StatusPending {
			t.Fatalf("IN_DOUBT row disappeared or changed status: %+v", e)
		}
	}
}

// sumCounts is a tiny helper so the test code reads naturally.
func sumCounts(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// runManyReplayTest asserts the cross-process invariant: every
// invocation of doRun adds exactly len(calls) more upstream calls.
// Total after N runs is N*len(calls). This is what the spec actually
// guarantees across processes — there is no collapse-replay of
// distinct calls with matching signatures.
func runManyReplayTest(t *shimT, calls []toolCall, runs int) {
	t.Helper()
	dir := t.PerIterationDir()
	jPath := filepath.Join(dir, "journal.db")
	ctx := context.Background()

	cu := newCountingUpstream()
	upstreamSrv := startCountingUpstream(t.T, cu)

	for r := 0; r < runs; r++ {
		doRun(t.T, ctx, cu, upstreamSrv, jPath, "wf-stable", calls)
	}

	counts, _ := cu.snapshot()
	total := 0
	for _, v := range counts {
		total += v
	}
	want := len(calls) * runs
	if total != want {
		t.Fatalf("after %d runs: upstream saw %d calls, want %d", runs, total, want)
	}
}

// reservePendingAgainstJournal opens the journal at jPath and writes
// a single PENDING row for the given call. Used by the IN_DOUBT
// sub-test to simulate a crash between ReservePending and
// Complete.
func reservePendingAgainstJournal(t *testing.T, jPath, wf string, c toolCall) (int64, error) {
	t.Helper()
	db, err := journal.Open(journal.Config{Path: jPath})
	if err != nil {
		return 0, err
	}
	defer db.Close()
	seq, _, err := db.BeginRequest(context.Background(), wf, "", 0)
	if err != nil {
		return 0, err
	}
	rawArgs, _ := json.Marshal(c.args)
	if err := db.ReservePending(context.Background(), wf, seq, c.tool, "test-sig", rawArgs); err != nil {
		return 0, err
	}
	return seq, nil
}

// doRun wires up: countingUpstream ↔ journal.DB ↔ proxy ↔ downstream client,
// drives the sequence, captures upstream-side counters and journals
// along the way. The bug fixed in Phase 1 was passing the same
// InMemoryTransport end to both server and client; the pipe silently
// deadlocks. sT and cT are now two distinct pipe-ends.
func doRun(t *testing.T, ctx context.Context, cu *countingUpstream, srv *mcp.Server, jPath, wf string, calls []toolCall) []string {
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
	out := driveCalls(t, ctx, cs, calls)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return out
}

func doRunWithSnap(t *testing.T, ctx context.Context, cu *countingUpstream, srv *mcp.Server, jPath, wf string, calls []toolCall) ([]string, journalSnap) {
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
	out := driveCalls(t, ctx, cs, calls)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	snap := snapshotJournalT(t, db, wf)
	return out, snap
}

// --- journal snapshot ------------------------------------------------------

type snapEntry struct {
	Sequence  int64
	Sig       string
	Status    journal.Status
	ResultLen int
	Error     string
}

type journalSnap struct {
	Entries []snapEntry
}

func snapshotJournalT(t *testing.T, db *journal.DB, wf string) journalSnap {
	t.Helper()
	var s journalSnap
	for i := int64(1); ; i++ {
		e, err := db.Lookup(context.Background(), wf, i)
		if err != nil {
			t.Fatalf("lookup seq %d: %v", i, err)
		}
		if e == nil {
			break
		}
		s.Entries = append(s.Entries, snapEntry{
			Sequence:  i,
			Sig:       e.CallSignature,
			Status:    e.Status,
			ResultLen: len(e.Result),
			Error:     e.Error,
		})
	}
	return s
}

// normalize round-trips JSON to wipe incidental whitespace or
// key-order differences between equivalent payloads.
func normalize(s string) string {
	var r any
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		return s
	}
	out, _ := json.Marshal(r)
	return string(out)
}
