// Package journal holds the storage engine and state machine for Checkpoint.
//
// The state machine in state.go is a pure function with exhaustive tests:
// given the on-disk entry for a given (workflow, sequence) — or "no
// entry" — plus the signature of the incoming call, it returns an
// Action. The proxy then executes the action.
//
// Keeping this pure means the decision logic can be tested without a
// database, without a connection, without a clock — and the wire-up
// in the proxy can be reasoned about separately.
package journal

import (
	"errors"
	"fmt"
)

// Status is the lifecycle state of a journal row.
//
// UNSEEN is not persisted; it is the implicit state for "no row" in the
// journal at a given (workflow, sequence). PENDING is recorded before
// the upstream call begins; COMPLETED/FAILED is recorded after the
// upstream call returns. IN_DOUBT is what a PENDING entry means on
// replay — "we recorded intent but never observed the outcome, so we
// don't know whether the side effect happened" — see §3.4 of the spec.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
)

// Valid reports whether s is one of the documented statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusCompleted, StatusFailed:
		return true
	default:
		return false
	}
}

// Entry is a journal row as seen by the state machine. It carries the
// fields needed to make a decision; the journal driver is responsible
// for persisting these and reading them back.
type Entry struct {
	Status        Status
	CallSignature string // hash of canonical arguments (and tool name in some derivations)
	Result        []byte // raw JSON of the upstream result; nil for FAILED
	Error         string // set only when Status == FAILED
}

// Incoming is what the proxy hands the state machine: the tool name and
// the hash of the canonical arguments for the call that's about to be
// made (or that is being replayed).
type Incoming struct {
	Tool       string
	ArgsHash   string
}

// DivergencePolicy decides what to do when a COMPLETED/FAILED entry has
// a CallSignature different from the incoming call's ArgsHash.
//
// This is decision-shape only (Phase 1 cares about strict mode; fork mode
// is a Phase 3 extension). The proxy wires the policy choice in via a
// flag; the state machine itself just reports "diverged" and leaves the
// rest to the caller for now.
type DivergencePolicy int

const (
	// PolicyStrict means: on divergence, return ActionDiverge and let
	// the caller refuse the call. This is the default and the only mode
	// required for Phase 3's gate.
	PolicyStrict DivergencePolicy = iota
	// PolicyFork is not yet implemented; it would branch a new
	// sub-workflow at the divergence point. Calling the state machine
	// with this value returns an error.
	PolicyFork
)

// Action is what the proxy should do next.
//
// Replay and Diverge are terminal in the sense that no journal mutation
// follows (assuming no further crash); Proceed is the only one that
// results in journal writes.
type Action int

const (
	// ActionProceed: no entry exists at this sequence. The proxy should
	// record a PENDING row (fsync'd) and invoke the upstream.
	ActionProceed Action = iota
	// ActionReplay: an entry exists at this sequence, status is
	// COMPLETED or FAILED, and the signature matches. The proxy should
	// return ReplayResult/ReplayError without calling upstream.
	ActionReplay
	// ActionInDoubt: an entry exists at this sequence with status
	// PENDING. The proxy should refuse and surface a distinct error
	// — see §3.4. The proxy may still proceed if the tool is annotated
	// idempotentHint: true (the only documented exception), but that
	// decision lives above the state machine.
	ActionInDoubt
	// ActionDiverge: an entry exists at this sequence, status is
	// COMPLETED or FAILED, but the signature differs from the incoming
	// call. The proxy must refuse or branch depending on policy; the
	// state machine just reports the mismatch.
	ActionDiverge
)

// String returns a stable, lowercase name for the action. Used in test
// failure messages.
func (a Action) String() string {
	switch a {
	case ActionProceed:
		return "proceed"
	case ActionReplay:
		return "replay"
	case ActionInDoubt:
		return "in_doubt"
	case ActionDiverge:
		return "diverge"
	default:
		return fmt.Sprintf("unknown(%d)", int(a))
	}
}

// Decision is what Transition returns. ReplayResult/ReplayError are
// populated only when Action == ActionReplay; Expected/Received are
// populated only when Action == ActionDiverge.
type Decision struct {
	Action Action

	// For ActionReplay:
	ReplayResult []byte
	ReplayError  string

	// For ActionDiverge: the signatures we compared, so the caller can
	// emit a useful error or diff.
	ExpectedSignature string
	ReceivedSignature string
}

// ErrUnknownStatus is returned by Transition if the journal row's status
// is not one of the documented values. A real journal should never store
// such rows, but we want a loud failure if one ever sneaks in.
var ErrUnknownStatus = errors.New("journal: unknown status on entry")

// Transition is the pure state machine. It is the only place decisions
// are made about what to do with an incoming call relative to the
// journal.
//
// existing is nil for UNSEEN. For PENDING, COMPLETED, FAILED it holds the
// row at the incoming sequence. The function does no I/O.
//
// Transition is the single source of truth for what §3.4 says the proxy
// must do at each observed state. Exhaustive tests in state_test.go pin
// every transition.
func Transition(existing *Entry, inc Incoming, _ DivergencePolicy) (Decision, error) {
	if existing == nil {
		// UNSEEN: a fresh call. Record PENDING, call upstream.
		return Decision{Action: ActionProceed}, nil
	}

	switch existing.Status {
	case StatusPending:
		// We recorded intent but never observed the outcome. We do
		// not know whether the upstream side effect happened; the
		// proxy must not auto-replay. The idempotentHint exception
		// lives in the proxy, above the state machine.
		return Decision{Action: ActionInDoubt}, nil

	case StatusCompleted, StatusFailed:
		if existing.CallSignature == inc.ArgsHash {
			return Decision{
				Action:       ActionReplay,
				ReplayResult: existing.Result,
				ReplayError:  existing.Error,
			}, nil
		}
		return Decision{
			Action:            ActionDiverge,
			ExpectedSignature: existing.CallSignature,
			ReceivedSignature: inc.ArgsHash,
		}, nil

	default:
		return Decision{}, ErrUnknownStatus
	}
}
