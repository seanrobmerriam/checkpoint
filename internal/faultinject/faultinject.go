// Package faultinject is the Phase 2 fault-injection harness.
//
// In fault-inject mode (env var CHECKPOINT_FAULT_INJECT=<file>), the
// proxy's journaled handler calls Point(phase) at each of the kill
// points in §5 of the spec:
//
//	before_reserve    — between BeginRequest and the PENDING write
//	after_reserve     — after ReservePending, before upstream.CallTool
//	mid_upstream      — after upstream.CallTool returned, before Complete
//	before_complete   — before COMPLETED write
//	after_complete    — after COMPLETED write, before return
//
// Point writes "<phase>\n" to <file> and blocks until the file's
// contents are changed to "GO\n" (or absence of "<phase>\n" by a
// different signal — see Driver.WaitForPhase semantics). The test
// driver synchronizes via WaitForPhase, optionally SIGKILLs the
// proxy, then resets to "GO" to release held Points.
//
// Not for production use — gated by env var, no overhead when off.
package faultinject

import (
	"os"
	"strings"
	"time"
)

// EnvVar enables fault-inject mode. Set to a file path.
const EnvVar = "CHECKPOINT_FAULT_INJECT"

// Sentinel values the driver writes to / reads from the fault file.
const (
	// PhaseContinue (driver → proxy) means "no fault: pass through,"
	// or "release the proxy's blocked Point call."
	PhaseContinue = "GO"

	// The PointXxx constants are the names of the kill-point phases.
	// The driver writes the desired pause phase directly to the file;
	// Point(phase) pauses only when its phase matches.
	PointBeforeReserve  = "before_reserve"
	PointAfterReserve   = "after_reserve"
	PointMidUpstream    = "mid_upstream"
	PointBeforeComplete = "before_complete"
	PointAfterComplete  = "after_complete"
)

// pollInterval is the polling cadence for the proxy side. 1 ms is
// the lowest granularity that doesn't burn significant CPU in tests
// and is well below the typical test setup overhead.
var pollInterval = 1 * time.Millisecond

// OverridePollInterval is for tests; allows speeding up fault-inject
// in tight loops.
func OverridePollInterval(d time.Duration) { pollInterval = d }

// Point blocks the proxy at this kill-point if the driver has
// armed this phase. Two-file protocol:
//
//	control file (path) — driver writes the phase name it wants paused,
//	    or "GO" to pass through. Point(phase) reads this file.
//	  - "GO"              → fast path.
//	  - "<other phase>"   → fast path (we're not the requested phase).
//	  - == phase          → engage: write <phase>\n to the
//	                       acknowledgment file and block until the
//	                       control file becomes "GO" again.
//
//	ack file (<path>.phase) — Point(phase) writes its phase here when
//	    it engages, so the driver can synchronize via WaitForPhase.
func Point(phase string) {
	ctrlPath, ackPath := paths()
	if ctrlPath == "" {
		return
	}
	current, err := os.ReadFile(ctrlPath)
	if err != nil {
		return
	}
	cur := strings.TrimSpace(string(current))
	if cur == PhaseContinue {
		return
	}
	if cur != phase {
		return // driver wants to pause elsewhere; pass through.
	}

	// Match: write the acknowledgment so the driver can synchronize.
	_ = os.WriteFile(ackPath, []byte(phase+"\n"), 0o644)

	deadline := time.Now().Add(60 * time.Second)
	for {
		b, err := os.ReadFile(ctrlPath)
		if err == nil && strings.TrimSpace(string(b)) == PhaseContinue {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(pollInterval)
	}
}

// paths returns the control-file path (from env) and the
// ack-file path (control + ".phase").
func paths() (string, string) {
	p := filePath()
	if p == "" {
		return "", ""
	}
	return p, p + ".phase"
}

func filePath() string {
	return strings.TrimSpace(os.Getenv(EnvVar))
}
