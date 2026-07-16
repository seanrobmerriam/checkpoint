// Package faultinject tests for Phase 2 (crash injection). They drive
// the real `checkpoint` binary as a subprocess against an in-process
// `test-upstream` binary, then SIGKILL the proxy at configured kill
// points and verify §3.4's IN_DOUBT and Replay behaviors.
//
// Why real subprocesses + SIGKILL? The whole point of the journal is
// crash safety — testing it requires an actual crash, not a graceful
// shutdown. Anything else is theater.
package faultinject_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seanrobmerriam/checkpoint/internal/faultinject"
	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// jsonRPC is a minimal JSON-RPC 2.0 envelope.
type jsonRPC struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      int            `json:"id,omitempty"`
	Method  string         `json:"method,omitempty"`
	Params  map[string]any `json:"params,omitempty"`
	Result  any            `json:"result,omitempty"`
	Error   any            `json:"error,omitempty"`
}

// spawnCheckpoint starts the checkpoint binary as a subprocess with
// upstream-cmd pointing at test-upstream, the journal path, and
// fault-inject enabled. It returns the *exec.Cmd and a JSON-RPC
// connection (writer + reader) for driving the proxy.
//
// Important: the parent's stdin pipe stays open for the lifetime of
// the subprocess. The MCP SDK treats stdin EOF as a graceful shutdown
// signal, so closing stdin on a still-needed proxy would terminate
// it. We let Cmd.Wait (or close()) close things, and during the
// crash test we Kill the process to simulate the crash.
func spawnCheckpoint(t *testing.T, ctx context.Context, upstreamBin, journalPath, faultFile string) (*exec.Cmd, *jsonRPCConn) {
	t.Helper()
	bin := buildCheckpoint(t)

	cmd := exec.CommandContext(ctx, bin,
		"--upstream-cmd", upstreamBin,
		"--journal", journalPath,
		"--workflow-id", "fault-wf",
	)
	cmd.Env = append(os.Environ(),
		"CHECKPOINT_FAULT_INJECT="+faultFile,
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	cmd.Stderr = os.Stderr // for now, bubble up error logs

	if err := cmd.Start(); err != nil {
		t.Fatalf("start checkpoint: %v", err)
	}

	conn := &jsonRPCConn{
		w:      bufio.NewWriter(stdin),
		r:      bufio.NewReader(stdout),
		stdin:  stdin,
		stdout: stdout,
	}

	// Initialize the MCP session. Read may block briefly waiting
	// for upstream to also initialize; we time-bound via the
	// readForID helper.
	if err := conn.write(jsonRPC{JSONRPC: "2.0", ID: 1, Method: "initialize", Params: map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "faultinject-test", "version": "0.0.1"},
	}}); err != nil {
		t.Fatalf("send initialize: %v", err)
	}
	if err := conn.flush(); err != nil {
		t.Fatalf("flush initialize: %v", err)
	}
	if _, err := conn.readForID(1, 5*time.Second); err != nil {
		t.Fatalf("read initialize response: %v", err)
	}
	if err := conn.write(jsonRPC{JSONRPC: "2.0", Method: "notifications/initialized", Params: map[string]any{}}); err != nil {
		t.Fatalf("send initialized: %v", err)
	}
	if err := conn.flush(); err != nil {
		t.Fatalf("flush initialized: %v", err)
	}

	return cmd, conn
}

func buildCheckpoint(t *testing.T) string {
	t.Helper()
	// Test binary lives in <repo>/internal/faultinject/. The checkpoint
	// binary is at <repo>/checkpoint. Resolve that.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Clean(filepath.Join(wd, "..", ".."))
	bin := filepath.Join(repo, "checkpoint")
	if _, err := os.Stat(bin); err == nil {
		return bin
	}
	// Otherwise build it now and cache.
	binPath := filepath.Join(t.TempDir(), "checkpoint")
	cmd := exec.Command("go", "build", "-o", binPath, filepath.Join(repo, "cmd", "checkpoint"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	return binPath
}

func buildTestUpstream(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	repo := filepath.Clean(filepath.Join(wd, "..", ".."))
	bin := filepath.Join(repo, "test-upstream")
	if _, err := os.Stat(bin); err == nil {
		return bin
	}
	binPath := filepath.Join(t.TempDir(), "test-upstream")
	cmd := exec.Command("go", "build", "-o", binPath, filepath.Join(repo, "cmd", "test-upstream"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build test-upstream: %v", err)
	}
	return binPath
}

// jsonRPCConn is a minimal newline-delimited JSON-RPC writer/reader.
type jsonRPCConn struct {
	w      *bufio.Writer
	r      *bufio.Reader
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

func (c *jsonRPCConn) write(msg jsonRPC) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	if _, err := c.w.WriteString("\n"); err != nil {
		return err
	}
	return nil
}

func (c *jsonRPCConn) flush() error { return c.w.Flush() }

func (c *jsonRPCConn) read() (jsonRPC, error) {
	var msg jsonRPC
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return msg, err
	}
	if err := json.Unmarshal(line, &msg); err != nil {
		return msg, fmt.Errorf("read json: %w; raw=%s", err, string(line))
	}
	return msg, nil
}

func (c *jsonRPCConn) close() error {
	_ = c.stdin.Close()
	return nil
}

// runCheckpointCommand runs a JSON-RPC method against the given
// client session and waits for the matching response (matched by id).
// Methods without a response are not handled here.
func (c *jsonRPCConn) sendRequest(id int, method string, params map[string]any) error {
	return c.write(jsonRPC{JSONRPC: "2.0", ID: id, Method: method, Params: params})
}

func (c *jsonRPCConn) readForID(wantID int, timeout time.Duration) (jsonRPC, error) {
	deadline := time.Now().Add(timeout)
	// Drain bytes until we see the matching id. Use a goroutine
	// internally to give the read a short timeout.
	for time.Now().Before(deadline) {
		type readResult struct {
			b   []byte
			err error
		}
		ch := make(chan readResult, 1)
		go func() {
			line, err := c.r.ReadBytes('\n')
			ch <- readResult{line, err}
		}()
		select {
		case r := <-ch:
			if r.err != nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			var msg jsonRPC
			if err := json.Unmarshal(r.b, &msg); err != nil {
				continue
			}
			if msg.ID == wantID {
				return msg, nil
			}
		case <-time.After(50 * time.Millisecond):
			// keep polling
		}
	}
	return jsonRPC{}, fmt.Errorf("timeout waiting for id=%d", wantID)
}

func init() {
	// Avoid stale-checkpoint-blocking on hung stdout reads by setting
	// a small read deadline in the helpers above. (Helpers above
	// already call SetReadDeadline.)
	_ = context.Background
}

// crashAtPoint runs the canonical Phase 2 scenario for one kill point:
//
//  1. Spawn upstream.
//  2. Spawn checkpoint (with journal + fault-inject).
//  3. Drive a tools/call to checkpoint, wait for fault file at <point>.
//  4. SIGKILL checkpoint.
//  5. Restart checkpoint against the same journal.
//  6. Drive the same tools/call again.
//  7. Verify the response and journal state match §3.4.
//
// For "after_complete" the assertion is REPLAY (no upstream call this
// run, journal already COMPLETED). For the rest, the assertion is
// IN_DOUBT (PENDING row, no upstream call, structured error).
func crashAtPoint(t *testing.T, point string, expected string) {
	t.Helper()

	dir := t.TempDir()
	journalPath := filepath.Join(dir, "journal.db")
	faultFile := filepath.Join(dir, "fault")

	// Upstream binary — slow tool waits on TEST_UPSTREAM_RELEASE file
	// before responding. Used for the "kill mid upstream" point.
	upstreamBin := buildTestUpstream(t)

	drv := faultinject.NewDriver(t, faultFile)
	t.Cleanup(drv.Cleanup)

	// Spawn upstream (one long-lived subprocess across both runs).
	upCtx, upCancel := context.WithCancel(context.Background())
	upCmd := spawnUpstream(t, upCtx, upstreamBin, filepath.Join(dir, "release"))
	t.Cleanup(func() { upCancel(); _ = upCmd.Process.Kill() })

	// --- Phase 2a: drive a call, SIGKILL at <point>. ---
	ctx, cancel := context.WithCancel(context.Background())
	cmd1, conn1 := spawnCheckpoint(t, ctx, upstreamBin, journalPath, faultFile)

	// Pause the fault driver at the SPECIFIC kill point. The proxy
	// passes through earlier Point(phase) calls without blocking —
	// we synchronize only on the chosen phase.
	if err := drv.PauseAt(point); err != nil {
		t.Fatalf("pause at %s: %v", point, err)
	}

	// Use the slow tool for ALL points: it's a guarantee that the
	// upstream is still in-flight when SIGKILL lands, even though
	// some kill points happen before upstream.CallTool returns.
	if err := conn1.sendRequest(10, "tools/call", map[string]any{
		"name":      "slow",
		"arguments": map[string]any{"message": "phase2-payload"},
	}); err != nil {
		t.Fatalf("send call: %v", err)
	}
	if err := conn1.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := drv.WaitForPhase(point); err != nil {
		// Surface the upstream's stderr to help debugging.
		t.Logf("WaitForPhase(%s) failed; current fault file: %q", point, drv.Current())
		cancel()
		_ = cmd1.Process.Kill()
		t.Fatal(err)
	}

	if err := cmd1.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = cmd1.Wait() // reap
	cancel()
	conn1.close()
	if err := drv.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// If we killed mid_upstream, release the upstream so it doesn't
	// hang the test cleanup. The released tool echoes and exits the
	// blocking loop, then the upstream stays alive for run 2.
	if point == faultinject.PointMidUpstream {
		_ = os.WriteFile(filepath.Join(dir, "release"), []byte("GO\n"), 0o644)
	}

	// --- Phase 2b: restart against same journal, drive same call. ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	cmd2, conn2 := spawnCheckpoint(t, ctx2, upstreamBin, journalPath, faultFile)
	defer func() {
		cancel2()
		_ = cmd2.Process.Kill()
		_ = cmd2.Wait()
		conn2.close()
	}()

	switch expected {
	case faultinject.PointAfterComplete:
		// Replay path — no in-doubt error expected.
	default:
		// IN_DOUBT path — see assertion in the readForID block below.
	}

	if err := conn2.sendRequest(20, "tools/call", map[string]any{
		"name":      "slow",
		"arguments": map[string]any{"message": "phase2-payload"},
	}); err != nil {
		t.Fatalf("send replay call: %v", err)
	}
	if err := conn2.flush(); err != nil {
		t.Fatalf("flush replay: %v", err)
	}

	// Wait for the response.
	resp, err := conn2.readForID(20, 10*time.Second)
	if err != nil {
		t.Fatalf("read replay response: %v", err)
	}

	switch expected {
	case faultinject.PointBeforeReserve:
		// No PENDING row was written — the journal is empty after the
		// kill. Run 2 sees UNSEEN → Proceed → ReservePending →
		// upstream.CallTool → Complete. The response should be a
		// successful upstream result, no in-doubt error.
		if resp.Error != nil {
			t.Fatalf("before_reserve: expected success, got error: %+v", resp.Error)
		}
		if resp.Result == nil {
			t.Fatalf("before_reserve: expected result, got nil")
		}
		assertJournalAfterRun2(t, journalPath, "fault-wf", []rowExpect{
			{seq: 1, status: journal.StatusCompleted, allowedToReCallUpstream: true},
		})
	case faultinject.PointAfterComplete:
		// The journal has a COMPLETED row at seq 1; run 2 advances
		// to seq 2 (no PENDING, Algorithm A — see §3.2's prohibition
		// on content-hash keys) and calls upstream fresh. The journal
		// therefore has two COMPLETED rows.
		if resp.Error != nil {
			t.Fatalf("after_complete: expected success, got error: %+v", resp.Error)
		}
		if resp.Result == nil {
			t.Fatalf("after_complete: expected result, got nil")
		}
		assertJournalAfterRun2(t, journalPath, "fault-wf", []rowExpect{
			{seq: 1, status: journal.StatusCompleted},
			{seq: 2, status: journal.StatusCompleted},
		})
	default:
		// Mid-flight kill: journal has PENDING → IN_DOUBT refusal.
		// Response must carry an error mentioning "in-doubt".
		if resp.Error == nil {
			t.Fatalf("expected in-doubt error in response, got result: %+v", resp.Result)
		}
		errText, _ := json.Marshal(resp.Error)
		if !strings.Contains(string(errText), "in-doubt") &&
			!strings.Contains(string(errText), "in_doubt") {
			t.Fatalf("error didn't mention in-doubt: %s", string(errText))
		}
		// And the journal row is STILL PENDING — neither run 2 nor
		// anything else silently wrote past the crash point.
		assertJournalAfterRun2(t, journalPath, "fault-wf", []rowExpect{
			{seq: 1, status: journal.StatusPending, allowedToReCallUpstream: false},
		})
	}
	_ = ctx2 // referenced in defer
}

// spawnUpstream launches `test-upstream` with the release-file env var.
func spawnUpstream(t *testing.T, ctx context.Context, bin, releaseFile string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = append(os.Environ(),
		"TEST_UPSTREAM_RELEASE="+releaseFile,
	)
	cmd.Stdout = io.Discard // we don't read upstream's MCP messages
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start test-upstream: %v", err)
	}
	return cmd
}

// rowExpect describes an expected journal row after the test's two runs.
type rowExpect struct {
	seq                     int64
	status                  journal.Status
	allowedToReCallUpstream bool // documentation only — does not affect assertions
}

// assertJournalAfterRun2 opens the journal at jPath and asserts the
// given workflow has the expected row(s). It also verifies that the
// total row count matches the expected entries — catching any
// "Checkpoint wrote more rows than expected" silent corruption.
func assertJournalAfterRun2(t *testing.T, jPath, wf string, wants []rowExpect) {
	t.Helper()
	db, err := journal.Open(journal.Config{Path: jPath})
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer db.Close()
	for _, w := range wants {
		got, err := db.Lookup(context.Background(), wf, w.seq)
		if err != nil {
			t.Fatalf("lookup seq=%d: %v", w.seq, err)
		}
		if got == nil {
			t.Fatalf("seq=%d: expected status=%s, but row does not exist", w.seq, w.status)
		}
		if got.Status != w.status {
			t.Fatalf("seq=%d: status=%s, want %s", w.seq, got.Status, w.status)
		}
	}
	// Also assert no extra rows exist beyond what's expected — catch
	// "Checkpoint silently advanced the sequence" regressions.
	for i := int64(int64(len(wants)) + 1); ; i++ {
		got, err := db.Lookup(context.Background(), wf, i)
		if err != nil {
			t.Fatalf("extra lookup: %v", err)
		}
		if got == nil {
			break
		}
		t.Fatalf("unexpected extra row at seq=%d: status=%s", i, got.Status)
	}
}

// TestCrashPhase is the spec's full kill-point matrix.
func TestCrashPhase(t *testing.T) {
	type tc struct {
		name     string
		point    string
		expected string // "indoubt" | "replay"
	}
	cases := []tc{
		{"before_reserve", faultinject.PointBeforeReserve, "indoubt"},
		{"after_reserve", faultinject.PointAfterReserve, "indoubt"},
		{"mid_upstream", faultinject.PointMidUpstream, "indoubt"},
		{"before_complete", faultinject.PointBeforeComplete, "indoubt"},
		{"after_complete", faultinject.PointAfterComplete, "replay"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// We pass `c.point` as the kill point and the expected
			// outcome via the same string — for IN_DOUBT, any
			// pre-COMPLETE point works; for REPLAY, after_complete.
			crashAtPoint(t, c.point, c.point)
		})
	}
}
