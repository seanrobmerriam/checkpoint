package idempotency

import (
	"strings"
	"testing"
)

// TestHashArgs_OrderIndependent is the core correctness property of the
// canonical form: reordering keys must not change the hash.
func TestHashArgs_OrderIndependent(t *testing.T) {
	a := []byte(`{"a":1,"b":2,"c":3}`)
	b := []byte(`{"c":3,"a":1,"b":2}`)
	c := []byte(`{"b":2,"c":3,"a":1}`)

	hA, err := HashArgs(a)
	if err != nil {
		t.Fatalf("HashArgs(a): %v", err)
	}
	hB, err := HashArgs(b)
	if err != nil {
		t.Fatalf("HashArgs(b): %v", err)
	}
	hC, err := HashArgs(c)
	if err != nil {
		t.Fatalf("HashArgs(c): %v", err)
	}
	if hA != hB || hB != hC {
		t.Fatalf("hash not order-independent:\n a=%s\n b=%s\n c=%s", hA, hB, hC)
	}
}

// TestHashArgs_NestedOrderIndependent covers nested objects too.
func TestHashArgs_NestedOrderIndependent(t *testing.T) {
	a := []byte(`{"outer":{"z":1,"a":2,"m":{"q":4,"p":3}}}`)
	b := []byte(`{"outer":{"a":2,"m":{"p":3,"q":4},"z":1}}`)

	hA, err := HashArgs(a)
	if err != nil {
		t.Fatalf("HashArgs(a): %v", err)
	}
	hB, err := HashArgs(b)
	if err != nil {
		t.Fatalf("HashArgs(b): %v", err)
	}
	if hA != hB {
		t.Fatalf("nested hash differs:\n a=%s\n b=%s", hA, hB)
	}
}

// TestHashArgs_ArraysAreOrdered — arrays preserve insertion order, since
// JSON arrays are semantically ordered sequences.
func TestHashArgs_ArraysAreOrdered(t *testing.T) {
	a := []byte(`[1,2,3]`)
	b := []byte(`[3,2,1]`)

	hA, err := HashArgs(a)
	if err != nil {
		t.Fatalf("HashArgs(a): %v", err)
	}
	hB, err := HashArgs(b)
	if err != nil {
		t.Fatalf("HashArgs(b): %v", err)
	}
	if hA == hB {
		t.Fatalf("array hash should depend on order; both hashed to %s", hA)
	}
}

// TestHashArgs_WhitespaceIndependent — extra whitespace doesn't change
// the hash because canonical form re-emits compact JSON.
func TestHashArgs_WhitespaceIndependent(t *testing.T) {
	a := []byte(`{"a":1,"b":2}`)
	b := []byte(` { "a" : 1 , "b" : 2 } `)

	hA, _ := HashArgs(a)
	hB, _ := HashArgs(b)
	if hA != hB {
		t.Fatalf("whitespace affected hash:\n a=%s\n b=%s", hA, hB)
	}
}

// TestHashArgs_TypesDiffer — different value types at the same position
// hash differently.
func TestHashArgs_TypesDiffer(t *testing.T) {
	// null vs "null" (string): different hashes.
	hNil, err := HashArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	hStr, err := HashArgs(json_str("null"))
	if err != nil {
		t.Fatal(err)
	}
	if hNil == hStr {
		t.Fatalf("nil and \"null\" string hashed the same: %s", hNil)
	}
}

// TestHashArgs_StructInput — the common case where the proxy receives a
// Go struct, not raw bytes; the function should marshal it for us.
func TestHashArgs_StructInput(t *testing.T) {
	type S struct {
		B int `json:"b"`
		A int `json:"a"`
	}
	hStruct, _ := HashArgs(S{A: 1, B: 2})
	hRaw, _ := HashArgs([]byte(`{"a":1,"b":2}`))
	if hStruct != hRaw {
		t.Fatalf("struct vs raw hashes differ:\n struct=%s\n raw=%s", hStruct, hRaw)
	}
}

// TestHashArgs_InvalidJSON — garbage in, clear error out.
func TestHashArgs_InvalidJSON(t *testing.T) {
	_, err := HashArgs([]byte(`{not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("error message didn't mention invalid JSON: %v", err)
	}
}

// TestHashCallSignature_IncludesTool — same args, different tools, different
// signatures. This is defense-in-depth; sequence already disambiguates.
func TestHashCallSignature_IncludesTool(t *testing.T) {
	h1 := HashCallSignature("foo", "args-hash")
	h2 := HashCallSignature("bar", "args-hash")
	if h1 == h2 {
		t.Fatalf("different tools produced same sig: %s", h1)
	}
}

// TestHashCallSignature_Stable — same inputs produce same output, always.
func TestHashCallSignature_Stable(t *testing.T) {
	a := HashCallSignature("foo", "abc")
	b := HashCallSignature("foo", "abc")
	if a != b {
		t.Fatalf("non-stable: %s vs %s", a, b)
	}
}

// TestNextSequence — empty workflow starts at 1; subsequent calls advance by 1.
func TestNextSequence(t *testing.T) {
	cases := []struct {
		in, want int64
	}{
		{0, 1},
		{1, 2},
		{42, 43},
		{999999, 1000000},
	}
	for _, c := range cases {
		got := NextSequence(c.in)
		if got != c.want {
			t.Errorf("NextSequence(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// tiny helper so the test above compiles cleanly without importing encoding/json.
type json_str string

func (s json_str) MarshalJSON() ([]byte, error) {
	return []byte(`"` + string(s) + `"`), nil
}
