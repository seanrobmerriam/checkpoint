package journal

import (
	"errors"
	"testing"
)

// TestTransition is an exhaustive table test for the state machine.
// Every (existing-state, signature-relation) pair from §3.4 maps to a
// recorded case below. If this test ever fails the priority order, the
// spec changed and the state machine must change with it.
func TestTransition(t *testing.T) {
	cases := []struct {
		name      string
		existing  *Entry
		inc       Incoming
		want      Action
		// Optional extras:
		wantResult   []byte // if Action == Replay, expected ReplayResult
		wantErr      string // if Action == Replay, expected ReplayError
		wantExpected string // if Action == Diverge
		wantReceived string // if Action == Diverge
	}{
		// --- UNSEEN ---
		{
			name:     "unseen -> proceed",
			existing: nil,
			inc:      Incoming{Tool: "x", ArgsHash: "h1"},
			want:     ActionProceed,
		},
		{
			name:     "unseen with empty args hash -> proceed",
			existing: nil,
			inc:      Incoming{Tool: "x", ArgsHash: ""},
			want:     ActionProceed,
		},

		// --- PENDING (a.k.a. IN_DOUBT on replay) ---
		{
			name: "PENDING regardless of signature match -> in_doubt",
			existing: &Entry{
				Status:        StatusPending,
				CallSignature: "h1",
			},
			inc:  Incoming{Tool: "x", ArgsHash: "h1"},
			want: ActionInDoubt,
		},
		{
			name: "PENDING with mismatched signature -> still in_doubt (we don't know what happened)",
			existing: &Entry{
				Status:        StatusPending,
				CallSignature: "h1",
			},
			inc:  Incoming{Tool: "x", ArgsHash: "h2"},
			want: ActionInDoubt,
		},

		// --- COMPLETED + signature match -> Replay ---
		{
			name: "COMPLETED with matching signature -> replay (no error)",
			existing: &Entry{
				Status:        StatusCompleted,
				CallSignature: "h1",
				Result:        []byte(`{"ok":true}`),
			},
			inc:        Incoming{Tool: "x", ArgsHash: "h1"},
			want:       ActionReplay,
			wantResult: []byte(`{"ok":true}`),
		},
		{
			name: "COMPLETED with matching signature but empty result -> replay with empty result",
			existing: &Entry{
				Status:        StatusCompleted,
				CallSignature: "h1",
				Result:        []byte{},
			},
			inc:        Incoming{Tool: "x", ArgsHash: "h1"},
			want:       ActionReplay,
			wantResult: []byte{},
		},

		// --- FAILED + signature match -> Replay (with stored error) ---
		{
			name: "FAILED with matching signature -> replay (with stored error)",
			existing: &Entry{
				Status:        StatusFailed,
				CallSignature: "h1",
				Error:         "tool said no",
			},
			inc:      Incoming{Tool: "x", ArgsHash: "h1"},
			want:     ActionReplay,
			wantErr:  "tool said no",
		},

		// --- Diverge: COMPLETED/FAILED + signature mismatch ---
		{
			name: "COMPLETED with different signature -> diverge",
			existing: &Entry{
				Status:        StatusCompleted,
				CallSignature: "h1",
				Result:        []byte(`{"old":true}`),
			},
			inc:           Incoming{Tool: "x", ArgsHash: "h2"},
			want:          ActionDiverge,
			wantExpected:  "h1",
			wantReceived:  "h2",
		},
		{
			name: "FAILED with different signature -> diverge (even old error stored)",
			existing: &Entry{
				Status:        StatusFailed,
				CallSignature: "h1",
				Error:         "old fail",
			},
			inc:          Incoming{Tool: "x", ArgsHash: "h2"},
			want:         ActionDiverge,
			wantExpected: "h1",
			wantReceived: "h2",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec, err := Transition(c.existing, c.inc, PolicyStrict)
			if err != nil {
				t.Fatalf("Transition returned unexpected error: %v", err)
			}
			if dec.Action != c.want {
				t.Errorf("Action: got %s, want %s", dec.Action, c.want)
			}

			if c.want == ActionReplay {
				gotErr := string(dec.ReplayResult)
				if c.wantResult == nil {
					if len(dec.ReplayResult) != 0 {
						t.Errorf("ReplayResult: got %q, want empty", gotErr)
					}
				} else if gotErr != string(c.wantResult) {
					t.Errorf("ReplayResult: got %q, want %q", gotErr, c.wantResult)
				}
				if dec.ReplayError != c.wantErr {
					t.Errorf("ReplayError: got %q, want %q", dec.ReplayError, c.wantErr)
				}
			}

			if c.want == ActionDiverge {
				if dec.ExpectedSignature != c.wantExpected {
					t.Errorf("ExpectedSignature: got %q, want %q", dec.ExpectedSignature, c.wantExpected)
				}
				if dec.ReceivedSignature != c.wantReceived {
					t.Errorf("ReceivedSignature: got %q, want %q", dec.ReceivedSignature, c.wantReceived)
				}
			}
		})
	}
}

// TestTransition_UnknownStatus verifies that a malformed journal row
// (status outside the documented set) errors rather than silently
// defaulting to a state.
func TestTransition_UnknownStatus(t *testing.T) {
	bad := &Entry{Status: Status("WUT"), CallSignature: "h1"}
	_, err := Transition(bad, Incoming{ArgsHash: "h1"}, PolicyStrict)
	if !errors.Is(err, ErrUnknownStatus) {
		t.Fatalf("got %v, want ErrUnknownStatus", err)
	}
}

// TestStatusValid confirms the Valid() helper covers exactly the
// documented statuses and nothing else (including the empty string,
// which would silently round-trip from a NULL column).
func TestStatusValid(t *testing.T) {
	for _, s := range []Status{StatusPending, StatusCompleted, StatusFailed} {
		if !s.Valid() {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range []Status{"", "pending" /* lowercase */, "DONE", "in_doubt"} {
		if s.Valid() {
			t.Errorf("%q should NOT be valid", s)
		}
	}
}

// TestActionString ensures the Stringer output is stable — the gate test
// in Phase 5 may rely on this for log-message assertions.
func TestActionString(t *testing.T) {
	for _, c := range []struct {
		a Action
		s string
	}{
		{ActionProceed, "proceed"},
		{ActionReplay, "replay"},
		{ActionInDoubt, "in_doubt"},
		{ActionDiverge, "diverge"},
	} {
		if c.a.String() != c.s {
			t.Errorf("Action(%d).String() = %q, want %q", c.a, c.a.String(), c.s)
		}
	}
	// unknown
	if got := Action(99).String(); got != "unknown(99)" {
		t.Errorf("unknown Action: got %q, want unknown(99)", got)
	}
}
