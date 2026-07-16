package proxy

import (
	"path/filepath"
	"strings"
	"testing"
)

// joins dir and journal filename into a single path.
func joinPath(dir, name string) string {
	return filepath.Join(dir, name)
}

// startsWithErrPrefix reports whether s is the "ERR:..." form that
// driveCalls uses when an MCP call returns a Go error.
func startsWithErrPrefix(s string) bool { return strings.HasPrefix(s, "ERR:") }

// divergentHasError reports whether s is an ERR-prefixed string
// containing the given keyword (e.g. "divergence").
func divergentHasError(s, keyword string) bool {
	if !startsWithErrPrefix(s) {
		return false
	}
	return strings.Contains(strings.ToLower(s), strings.ToLower(keyword))
}

// Compile-time guards that test-only helpers don't break in compile.
var _ = testing.Short