// Package idempotency derives the sequence numbers and call signatures
// that identify a tools/call request uniquely within a workflow.
//
// Two distinct mechanisms, both intentional (§3.2 of the spec):
//
//   - Sequence: a monotonic counter persisted in the journal. Makes
//     "the Nth call" a durable, restart-proof concept.
//
//   - CallSignature: a SHA-256 over canonicalized (tool, arguments)
//     JSON. Detects whether a replay is asking for the same call as
//     last time, or has changed plans.
//
// The idempotency key is (workflow_id, sequence). CallSignature is the
// mismatch detector, not part of the key.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// HashArgs produces a hex SHA-256 over the canonical-JSON form of args.
//
// Canonical form: keys at every level are sorted lexicographically.
// Whitespace is irrelevant because sorted, compact JSON has none.
//
// We accept any value that json.Marshal can serialize (plus nil). Passing
// raw bytes that aren't valid JSON returns an error — we'll have noticed
// loud failures in Phase 0 if anyone ever managed to do that.
func HashArgs(args any) (string, error) {
	var raw json.RawMessage

	switch v := args.(type) {
	case nil:
		// Hash the literal string "null" — empty input is meaningful,
		// distinct from missing-input.
		raw = json.RawMessage("null")
	case []byte:
		if len(v) == 0 {
			raw = json.RawMessage("null")
		} else if !json.Valid(v) {
			return "", fmt.Errorf("idempotency: HashArgs: invalid JSON bytes: %q", string(v))
		} else {
			raw = json.RawMessage(v)
		}
	case json.RawMessage:
		if len(v) == 0 {
			raw = json.RawMessage("null")
		} else if !json.Valid(v) {
			return "", fmt.Errorf("idempotency: HashArgs: invalid JSON raw: %q", string(v))
		} else {
			raw = v
		}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("idempotency: HashArgs: marshal: %w", err)
		}
		raw = b
	}

	canon, err := canonicalize(raw)
	if err != nil {
		return "", fmt.Errorf("idempotency: HashArgs: canonicalize: %w", err)
	}

	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// HashCallSignature is the (tool, args) signature written to the
// journal. Tool name is included so two tools with identical-looking
// argument shapes produce different signatures — defense in depth,
// since the sequence number already disambiguates.
func HashCallSignature(tool string, argsHash string) string {
	h := sha256.Sum256([]byte(tool + "\x00" + argsHash))
	return hex.EncodeToString(h[:])
}

// canonicalize parses a JSON value and re-emits it with sorted keys at
// every level. This is what makes {"a":1,"b":2} and {"b":2,"a":1} hash
// identically. Arrays preserve order (semantically ordered); only object
// key order is normalized.
func canonicalize(raw json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return marshalSorted(v)
}

func marshalSorted(v any) ([]byte, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		// We emit using json.Marshal on a sorted-key map. Since Go map
		// iteration is randomized we can't rely on it; build the object
		// by hand with sorted keys.
		var buf []byte
		buf = append(buf, '{')
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			buf = append(buf, kb...)
			buf = append(buf, ':')
			child, err := marshalSorted(x[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, child...)
		}
		buf = append(buf, '}')
		return buf, nil

	case []any:
		var buf []byte
		buf = append(buf, '[')
		for i, item := range x {
			if i > 0 {
				buf = append(buf, ',')
			}
			child, err := marshalSorted(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, child...)
		}
		buf = append(buf, ']')
		return buf, nil

	default:
		return json.Marshal(v)
	}
}

// NextSequence returns the next monotonic sequence for a workflow,
// given the current maximum. The journal driver is responsible for
// storing and loading this — this function is a tiny pure helper kept
// here so the policy (1-based, no rollover, must be > 0) lives next
// to the rest of the idempotency rules.
//
// Sequences are 1-based: an empty journal's next sequence is 1.
func NextSequence(currentMax int64) int64 {
	return currentMax + 1
}

// ErrInvalidJSON indicates an args payload that wasn't valid JSON.
var ErrInvalidJSON = errors.New("idempotency: invalid JSON args")
