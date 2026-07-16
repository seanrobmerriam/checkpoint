package faultinject

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// Driver is the test-side handle around the shared fault file. The
// proxy writes phase markers; the driver reads them and writes
// "GO\n" to release.
type Driver struct {
	path string
}

// NewDriver creates the fault file at <path> in PhaseContinue state so
// the proxy proceeds normally until the test arms a specific phase.
func NewDriver(t *testing.T, path string) *Driver {
	t.Helper()
	if err := os.MkdirAll(parentDir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(PhaseContinue+"\n"), 0o644); err != nil {
		t.Fatalf("init fault file: %v", err)
	}
	return &Driver{path: path}
}

// PauseAt arms the fault driver to pause at the named phase. The
// next Point(phase) call in the proxy that matches will block; all
// other phases pass through.
func (d *Driver) PauseAt(phase string) error {
	return d.SetRaw(phase + "\n")
}

// Path returns the fault-file path, useful for passing to subprocess
// constructors.
func (d *Driver) Path() string { return d.path }

// WaitForPhase blocks until the proxy acknowledges engagement by
// writing <phase>\n to the ack file, or until timeout.
func (d *Driver) WaitForPhase(phase string) error {
	want := []byte(phase + "\n")
	deadline := time.Now().Add(10 * time.Second)
	ack := d.path + ".phase"
	for {
		b, err := os.ReadFile(ack)
		if err == nil && string(b) == string(want) {
			return nil
		}
		if time.Now().After(deadline) {
			ctrl, _ := os.ReadFile(d.path)
			return errors.New("WaitForPhase " + phase + ": timeout, ctrl=" +
				string(ctrl) + " ack=" + string(b))
		}
		time.Sleep(pollInterval)
	}
}

// Release marks the fault file as "GO" so the proxy's next Point call
// (and any currently blocked one) returns.
func (d *Driver) Release() error {
	return os.WriteFile(d.path, []byte(PhaseContinue+"\n"), 0o644)
}

// Reset is a convenience for Release that lets the test driver
// pipeline calls.
func (d *Driver) Reset() error { return d.Release() }

// SetRaw writes any contents to the fault file. Used for tests that
// need more sophisticated signaling.
func (d *Driver) SetRaw(s string) error {
	return os.WriteFile(d.path, []byte(s), 0o644)
}

// Current returns the current contents of the fault file.
func (d *Driver) Current() string {
	b, err := os.ReadFile(d.path)
	if err != nil {
		return ""
	}
	return strings.TrimRight(string(b), "\n")
}

// Cleanup removes the fault file.
func (d *Driver) Cleanup() { _ = os.Remove(d.path) }

func parentDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}
