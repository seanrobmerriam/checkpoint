package proxy

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// readOnlyUpstream wires up an upstream with a tool annotated
// readOnlyHint=true. The handler counts calls so the test can
// verify the fast path actually skipped journaling and re-invoked
// upstream on every request.
type readOnlyUpstream struct {
	calls int
}

func startReadOnlyUpstream(t *testing.T) (*mcp.Server, *readOnlyUpstream) {
	t.Helper()
	u := &readOnlyUpstream{}
	srv := mcp.NewServer(&mcp.Implementation{Name: "ronly-upstream", Version: "0.0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{
		Name: "stats",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint: true,
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, _ map[string]any) (*mcp.CallToolResult, any, error) {
		u.calls++
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "stats"}},
		}, nil, nil
	})
	return srv, u
}

// openJournalForTest is a small helper to keep the annotations tests
// dependency-light.
func openJournalForTest(t *testing.T, path string) (*journal.DB, error) {
	t.Helper()
	return journal.Open(journal.Config{Path: path})
}

func TestReadOnlyFastPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	jPath := joinPath(dir, "journal.db")

	srv, harness := startReadOnlyUpstream(t)

	sT, cT := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, sT, nil); err != nil {
		t.Fatal(err)
	}

	// TrustAnnotations = true → readOnlyHint:true skips journaling.
	db, err := openJournalForTest(t, jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	p, err := New(ctx, Config{
		Upstream:         cT,
		WorkflowID:       "ronly-wf",
		Journal:          db,
		TrustAnnotations: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cs := drivingClient(t, ctx, p.Downstream)
	defer func() { _ = p.Close() }()

	// Drive 3 calls.
	for i := 0; i < 3; i++ {
		if _, err := cs.CallTool(ctx, &mcp.CallToolParams{
			Name: "stats", Arguments: map[string]any{"i": i},
		}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}

	if harness.calls != 3 {
		t.Fatalf("upstream saw %d calls, want 3", harness.calls)
	}

	// CRITICAL: the journal must have ZERO rows for this workflow —
	// the read-only fast path skipped every call.
	for i := int64(1); ; i++ {
		e, err := db.Lookup(ctx, "ronly-wf", i)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if e == nil {
			break
		}
		t.Fatalf("read-only tool should not have journaled, but seq=%d exists: status=%s",
			i, e.Status)
	}
}

func TestReadOnlyFastPath_DisabledByDefault(t *testing.T) {
	// When TrustAnnotations=false (the default), the proxy journals
	// every readOnlyHint:true call. This test pins the conservative
	// default.
	ctx := context.Background()
	dir := t.TempDir()
	jPath := joinPath(dir, "journal.db")

	srv, _ := startReadOnlyUpstream(t)

	sT, cT := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, sT, nil); err != nil {
		t.Fatal(err)
	}

	db, err := openJournalForTest(t, jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	p, err := New(ctx, Config{
		Upstream:   cT,
		WorkflowID: "ronly-disabled",
		Journal:    db,
		// TrustAnnotations: false (default)
	})
	if err != nil {
		t.Fatal(err)
	}
	cs := drivingClient(t, ctx, p.Downstream)
	defer func() { _ = p.Close() }()

	for i := 0; i < 2; i++ {
		if _, err := cs.CallTool(ctx, &mcp.CallToolParams{
			Name: "stats", Arguments: map[string]any{"i": i},
		}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}

	for i := int64(1); ; i++ {
		e, err := db.Lookup(ctx, "ronly-disabled", i)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if e == nil {
			break
		}
		if e.Status != journal.StatusCompleted {
			t.Fatalf("seq=%d status=%s", i, e.Status)
		}
	}
}
