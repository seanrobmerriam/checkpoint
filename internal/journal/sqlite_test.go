package journal

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// testDB returns an in-memory journal. The cache=shared DSN is needed
// so that BeginRequest's internal transaction and other operations see
// the same database file — the default file::memory:? DSN gives each
// connection its own private database.
func testDB(t *testing.T) *DB {
	t.Helper()
	// Actually, since we limit to 1 connection, we can use a temp file.
	// That's actually sturdier for tests because the file outlives the
	// process and lets us inspect on failure.
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.db")
	db, err := Open(Config{Path: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpenClose_RealFile(t *testing.T) {
	db := testDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPragmas_Applied(t *testing.T) {
	db := testDB(t)
	// Re-open and check the values.
	row := db.sqlDB.QueryRow("PRAGMA journal_mode")
	var mode string
	if err := row.Scan(&mode); err != nil {
		t.Fatalf("scan journal_mode: %v", err)
	}
	// journal_mode returns the value actually in effect — should be "wal".
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}

	row = db.sqlDB.QueryRow("PRAGMA synchronous")
	var sync int
	if err := row.Scan(&sync); err != nil {
		t.Fatalf("scan synchronous: %v", err)
	}
	// FULL == 2 in SQLite's PRAGMA synchronous numbering.
	if sync != 2 {
		t.Fatalf("synchronous = %d, want 2 (FULL)", sync)
	}
}

func TestBeginRequest_Empty(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seq, existing, err := db.BeginRequest(ctx, "wf-1", "", 0)
	if err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	if seq != 1 {
		t.Fatalf("first sequence: got %d, want 1", seq)
	}
	if existing != nil {
		t.Fatalf("expected nil entry on empty journal, got %+v", existing)
	}
}

func TestReserveThenComplete_RoundTrip(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const wf = "wf-round"

	// First call: seq=1, no entry.
	seq, existing, err := db.BeginRequest(ctx, wf, "", 0)
	if err != nil {
		t.Fatalf("BeginRequest 1: %v", err)
	}
	if seq != 1 || existing != nil {
		t.Fatalf("first: seq=%d existing=%v", seq, existing)
	}

	if err := db.ReservePending(ctx, wf, seq, "echo", "sig-1", []byte(`{"msg":"hi"}`)); err != nil {
		t.Fatalf("ReservePending: %v", err)
	}

	// Re-reading now: existing is PENDING. BeginRequest must re-surface
	// the PENDING row at the same sequence regardless of incomingSig.
	seq2, existing, err := db.BeginRequest(ctx, wf, "any-sig", 0)
	if err != nil {
		t.Fatalf("BeginRequest 2: %v", err)
	}
	if seq2 != seq {
		t.Fatalf("expected to re-encounter seq=%d, got %d", seq, seq2)
	}
	if existing == nil {
		t.Fatal("expected existing to be PENDING row, got nil")
	}
	if existing.Status != StatusPending {
		t.Fatalf("status: got %q, want PENDING", existing.Status)
	}
	if existing.CallSignature != "sig-1" {
		t.Fatalf("sig: got %q, want sig-1", existing.CallSignature)
	}

	// Complete it.
	if err := db.Complete(ctx, wf, seq, []byte(`{"echo":"hi"}`)); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Re-reading after Complete: BeginRequest advances to seq=2 with
	// nil entry (no PENDING, MAX is now 1). The just-completed row is
	// only available via Lookup. This is the spec's monotonic-sequence
	// behavior — distinct calls always get distinct sequences, even if
	// their signatures collide.
	seq3, existing, err := db.BeginRequest(ctx, wf, "", 0)
	if err != nil {
		t.Fatalf("BeginRequest 3: %v", err)
	}
	if seq3 != 2 || existing != nil {
		t.Fatalf("after complete: seq=%d existing=%v, want seq=2 nil", seq3, existing)
	}

	e1, err := db.Lookup(ctx, wf, 1)
	if err != nil {
		t.Fatalf("Lookup 1: %v", err)
	}
	if e1 == nil || e1.Status != StatusCompleted {
		t.Fatalf("seq 1: %+v", e1)
	}
	if string(e1.Result) != `{"echo":"hi"}` {
		t.Fatalf("seq 1 result: %q", string(e1.Result))
	}
}

func TestReserveThenFail_RoundTrip(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const wf = "wf-fail"
	seq, _, _ := db.BeginRequest(ctx, wf, "", 0)

	if err := db.ReservePending(ctx, wf, seq, "tool", "sig", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Fail(ctx, wf, seq, "boom"); err != nil {
		t.Fatal(err)
	}

	// After Fail, BeginRequest advances to a fresh seq (no PENDING, max
	// is now 1). Read the just-failed row via Lookup.
	e1, err := db.Lookup(ctx, wf, 1)
	if err != nil {
		t.Fatal(err)
	}
	if e1 == nil || e1.Status != StatusFailed {
		t.Fatalf("seq 1: %+v", e1)
	}
	if e1.Error != "boom" {
		t.Fatalf("error: %q", e1.Error)
	}
}

// TestSequenceMonotonic — each BeginRequest advances by 1 even when the
// previous row was already COMPLETED (no row is created on REPLAY).
func TestSequenceMonotonic(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const wf = "wf-mono"

	for wantSeq := int64(1); wantSeq <= 5; wantSeq++ {
		seq, _, err := db.BeginRequest(ctx, wf, "", 0)
		if err != nil {
			t.Fatalf("iter %d: BeginRequest: %v", wantSeq, err)
		}
		if seq != wantSeq {
			t.Fatalf("iter %d: seq=%d, want %d", wantSeq, seq, wantSeq)
		}
		// Reserve + Complete, so the next iteration's lookup of seq=N
		// still sees a COMPLETED row, advancing past it.
		if err := db.ReservePending(ctx, wf, seq, "tool", "sig", nil); err != nil {
			t.Fatalf("iter %d: ReservePending: %v", wantSeq, err)
		}
		if err := db.Complete(ctx, wf, seq, []byte(`"ok"`)); err != nil {
			t.Fatalf("iter %d: Complete: %v", wantSeq, err)
		}
	}
}

// TestFinalize_RefusesToFlipNonPending — defensive check. If the row is
// somehow not PENDING (because of a bug or a previous partial failure),
// the finalize step must NOT silently overwrite it.
func TestFinalize_RefusesToFlipNonPending(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const wf = "wf-flip"
	seq, _, _ := db.BeginRequest(ctx, wf, "", 0)
	if err := db.ReservePending(ctx, wf, seq, "tool", "sig", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Complete(ctx, wf, seq, []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}

	// Try to Complete again. This should fail because the row is COMPLETED.
	if err := db.Complete(ctx, wf, seq, []byte(`"second"`)); err == nil {
		t.Fatal("expected error flipping COMPLETED to COMPLETED, got nil")
	}
	// Try to Fail an already-COMPLETED row.
	if err := db.Fail(ctx, wf, seq, "boom"); err == nil {
		t.Fatal("expected error flipping COMPLETED to FAILED, got nil")
	}

	// Confirm row remains as the first COMPLETED.
	entry, err := db.Lookup(ctx, wf, seq)
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Result) != `"ok"` {
		t.Fatalf("result clobbered: %q", string(entry.Result))
	}
}

// TestBeginRequest_PendingFromUncompletedCall — simulates crash between
// ReservePending and Complete. BeginRequest must surface the PENDING
// row, not advance the counter.
func TestBeginRequest_PendingFromUncompletedCall(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const wf = "wf-pending"

	// We did one ReservePending but never finalized — re-mirror disk now.
	if err := db.ReservePending(ctx, wf, 1, "tool", "sig-1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	seq, existing, err := db.BeginRequest(ctx, wf, "", 0)
	if err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	// Per spec: a PENDING row at sequence N means "we don't know if N
	// happened." BeginRequest does NOT auto-skip past it. The new
	// sequence handed out is the same N, and existing contains the
	// PENDING row so the state machine can return ActionInDoubt.
	if seq != 1 {
		t.Fatalf("expected to re-encounter seq=1, got %d", seq)
	}
	if existing == nil || existing.Status != StatusPending {
		t.Fatalf("expected PENDING entry, got %+v", existing)
	}
}

// TestReopenAcrossProcess — write, close, reopen, verify state. This
// guards against accidental reliance on in-memory caching.
func TestReopenAcrossProcess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.db")
	ctx := context.Background()

	db, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ReservePending(ctx, "wf", 1, "tool", "sig", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Complete(ctx, "wf", 1, []byte(`"hi"`)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	e, err := db2.Lookup(ctx, "wf", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e == nil || e.Status != StatusCompleted || string(e.Result) != `"hi"` {
		t.Fatalf("after reopen: %+v", e)
	}

	// Next sequence after reopen should also be 2, not 1.
	seq, _, err := db2.BeginRequest(ctx, "wf", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 2 {
		t.Fatalf("post-reopen seq: got %d, want 2", seq)
	}
}

// TestWorkflowsAreIsolated — two workflows share the same journal file
// but have independent sequence counters.
func TestWorkflowsAreIsolated(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	seqA, _, _ := db.BeginRequest(ctx, "A", "", 0)
	seqB, _, _ := db.BeginRequest(ctx, "B", "", 0)
	if seqA != 1 || seqB != 1 {
		t.Fatalf("expected both workflows at seq=1: A=%d B=%d", seqA, seqB)
	}

	if err := db.ReservePending(ctx, "A", seqA, "t", "s", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Complete(ctx, "A", seqA, []byte(`"a"`)); err != nil {
		t.Fatal(err)
	}

	// A advances to 2; B remains at 1.
	seqA2, _, _ := db.BeginRequest(ctx, "A", "", 0)
	seqB2, _, _ := db.BeginRequest(ctx, "B", "", 0)
	if seqA2 != 2 {
		t.Fatalf("A seq: got %d, want 2", seqA2)
	}
	if seqB2 != 1 {
		t.Fatalf("B seq: got %d, want 1", seqB2)
	}
}

// TestConcurrentCallsFromSameGoroutineIsSafe — sanity that the gate uses
// a single goroutine (Phase 1). Phase 4 will introduce real concurrency.
func TestConcurrentCallsFromSameGoroutineIsSafe(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const wf = "wf-serial"

	var wg sync.WaitGroup
	// Two interleaved calls — Phase 1 only guarantees correctness in a
	// serialized style, but this shouldn't deadlock or panic.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seq, _, _ := db.BeginRequest(ctx, wf, "", 0)
			_ = seq
			_ = i
		}(i)
	}
	wg.Wait()
}
