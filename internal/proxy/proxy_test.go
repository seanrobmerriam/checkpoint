// Package proxy tests for Phase 0 (passthrough). The gate is: byte-for-byte
// equivalence between direct upstream calls and proxied calls.
//
// The test builds a reference upstream server (refupstream), a direct
// downstream client talking to refupstream, and a proxied downstream client
// talking to a Proxy that wraps refupstream. Identical calls through both
// downstream clients must produce byte-identical JSON-RPC responses.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type echoIn struct {
	Message string `json:"message" jsonschema:"what to echo back"`
}

type sumIn struct {
	A int `json:"a"`
	B int `json:"b"`
}

// keep these types exported for journaled_test.go to reuse.
var _ = echoIn{}
var _ = sumIn{}

// refUpstream is the reference upstream server we wrap in the proxy. It has
// a variety of feature types (tools with/without annotations, prompts,
// resources, resource templates) so the gate covers more than just one tool.
func refUpstream(t *testing.T) *mcp.Server {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "refupstream", Version: "0.0.1"}, nil)

	// echo: read-only, idempotent
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "echo",
		Description: "echo input back",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, in echoIn) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: in.Message}},
		}, nil, nil
	})

	// sum: not read-only, not necessarily idempotent (irrelevant; just tests default annotation behavior)
	truth := true
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "sum",
		Description: "add two ints",
		Annotations: &mcp.ToolAnnotations{
			DestructiveHint: &truth, // explicitly not destructive
			IdempotentHint:  true,
			OpenWorldHint:   &truth,
			ReadOnlyHint:    false,
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, in sumIn) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("%d", in.A+in.B)}},
		}, map[string]any{"total": in.A + in.B}, nil
	})

	// fail: returns IsError=true. The gate must distinguish "transport error"
	// (Go-level) from "tool error" (MCP-level IsError=true).
	mcp.AddTool(srv, &mcp.Tool{Name: "fail"}, func(ctx context.Context, req *mcp.CallToolRequest, _ map[string]any) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{}, nil, fmt.Errorf("intentional tool failure")
	})

	// a prompt and two resources and a template
	srv.AddPrompt(
		&mcp.Prompt{Name: "greet", Description: "say hello"},
		func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Messages: []*mcp.PromptMessage{{
					Role: "user",
					Content: &mcp.TextContent{Text: "hello"},
				}},
			}, nil
		},
	)

	srv.AddResource(
		&mcp.Resource{URI: "memory://fixed", Name: "fixed", MIMEType: "text/plain"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{{
					URI:      "memory://fixed",
					MIMEType: "text/plain",
					Text:     "fixed-value",
				}},
			}, nil
		},
	)

	srv.AddResourceTemplate(
		&mcp.ResourceTemplate{Name: "tmpl", URITemplate: "memory://x/{id}"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{{
					URI:      req.Params.URI,
					MIMEType: "text/plain",
					Text:     "value-for-" + req.Params.URI,
				}},
			}, nil
		},
	)

	return srv
}

// rpcPair holds a server and a transport end usable as a downstream
// client's transport. The opposite end of the transport has been connected
// to the server already (via t1, t2 := NewInMemoryTransports(); server.Connect(t1)).
type rpcPair struct {
	Srv      *mcp.Server
	ClientTr *mcp.InMemoryTransport
}

// startPair wires refupstream to an in-memory transport pair. Returns the
// transport end that should be used by a *downstream* client (e.g.
// NewClient.Connect(downstreamTr)).
func startPair(ctx context.Context, t *testing.T, srv *mcp.Server) rpcPair {
	t.Helper()
	t1, t2 := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, t1, nil); err != nil {
		t.Fatalf("upstream connect: %v", err)
	}
	return rpcPair{Srv: srv, ClientTr: t2}
}

// callTool marshals args as JSON and calls session.CallTool. Returns the raw
// JSON of the result plus an error string ("" if nil).
func callTool(t *testing.T, ctx context.Context, cs *mcp.ClientSession, name string, args any) (string, string) {
	t.Helper()
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return "", err.Error()
	}
	raw, mErr := json.Marshal(res)
	if mErr != nil {
		t.Fatalf("marshal result: %v", mErr)
	}
	return string(raw), ""
}

// compareBytes fails the test if a != b. We compare JSON byte-for-byte
// after normalizing insignificant whitespace via json.Compact.
func compareBytes(t *testing.T, label string, a, b string) {
	t.Helper()
	if compactJSON(a) == compactJSON(b) {
		return
	}
	t.Fatalf("%s mismatch:\n  got:  %s\n  want: %s", label, a, b)
}

func compactJSON(s string) string {
	var buf bytes.Buffer
	_ = json.Compact(&buf, []byte(s))
	return buf.String()
}

// TestPhase0_PassthroughEquivalence is the Phase 0 gate. It runs a battery
// of tool, prompt, and resource calls through both a direct upstream and a
// proxied upstream and asserts the responses match byte-for-byte.
func TestPhase0_PassthroughEquivalence(t *testing.T) {
	ctx := context.Background()

	ref := refUpstream(t)
	direct := startPair(ctx, t, ref)

	proxyServer := startPair(ctx, t, ref)
	p, err := New(ctx, Config{
		Upstream: proxyServer.ClientTr,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Now wire two downstream clients: one to the direct pair, one to
	// the proxied server.
	clientOpts := &mcp.ClientOptions{}
	directClient := mcp.NewClient(&mcp.Implementation{Name: "direct-client"}, clientOpts)
	directSess, err := directClient.Connect(ctx, direct.ClientTr, nil)
	if err != nil {
		t.Fatalf("direct client connect: %v", err)
	}
	t.Cleanup(func() { _ = directSess.Close() })

	// Connect to proxy downstream using a NEW pair of in-memory transports.
	// server-side is the proxy's downstream server; client-side is what we
	// pass to the client.
	pt1, pt2 := mcp.NewInMemoryTransports()
	go func() {
		if _, err := p.Downstream.Connect(ctx, pt1, nil); err != nil {
			t.Logf("proxy downstream connect: %v", err)
		}
	}()
	proxiedClient := mcp.NewClient(&mcp.Implementation{Name: "proxied-client"}, clientOpts)
	proxiedSess, err := proxiedClient.Connect(ctx, pt2, nil)
	if err != nil {
		t.Fatalf("proxied client connect: %v", err)
	}
	t.Cleanup(func() { _ = proxiedSess.Close() })

	// --- tools/list: byte-for-byte ---
	directListRaw, _ := mustJSON(t, mustMarshal(t, mustListTools(ctx, directSess)))
	proxiedListRaw, _ := mustJSON(t, mustMarshal(t, mustListTools(ctx, proxiedSess)))
	compareBytes(t, "tools/list", directListRaw, proxiedListRaw)

	// --- a variety of tool calls ---
	cases := []struct {
		name string
		args any
	}{
		{"echo", echoIn{Message: "hello"}},
		{"sum", sumIn{A: 7, B: 35}},
		{"echo", echoIn{Message: `with "quotes" and \backslashes`}},
		{"fail", nil},
	}
	for _, c := range cases {
		dRaw, dErr := callTool(t, ctx, directSess, c.name, c.args)
		pRaw, pErr := callTool(t, ctx, proxiedSess, c.name, c.args)
		if dErr != pErr {
			t.Fatalf("%s error mismatch: direct=%q proxied=%q", c.name, dErr, pErr)
		}
		compareBytes(t, "call("+c.name+")", dRaw, pRaw)
	}

	// --- prompts/get ---
	dP, dPErr := directSess.GetPrompt(ctx, &mcp.GetPromptParams{Name: "greet"})
	pP, pPErr := proxiedSess.GetPrompt(ctx, &mcp.GetPromptParams{Name: "greet"})
	if (dPErr == nil) != (pPErr == nil) || (dPErr != nil && dPErr.Error() != pPErr.Error()) {
		t.Fatalf("prompts/get err mismatch: direct=%v proxied=%v", dPErr, pPErr)
	}
	if dPErr == nil {
		dR, _ := json.Marshal(dP)
		pR, _ := json.Marshal(pP)
		compareBytes(t, "prompts/get", string(dR), string(pR))
	}

	// --- resources/read (fixed URI) ---
	dR, dRErr := directSess.ReadResource(ctx, &mcp.ReadResourceParams{URI: "memory://fixed"})
	pR, pRErr := proxiedSess.ReadResource(ctx, &mcp.ReadResourceParams{URI: "memory://fixed"})
	if (dRErr == nil) != (pRErr == nil) || (dRErr != nil && dRErr.Error() != pRErr.Error()) {
		t.Fatalf("resources/read err mismatch: direct=%v proxied=%v", dRErr, pRErr)
	}
	if dRErr == nil {
		dJ, _ := json.Marshal(dR)
		pJ, _ := json.Marshal(pR)
		compareBytes(t, "resources/read fixed", string(dJ), string(pJ))
	}

	// --- resources/read (template URI: ensures template URI arrived through the mirror) ---
	dT, dTErr := directSess.ReadResource(ctx, &mcp.ReadResourceParams{URI: "memory://x/42"})
	pT, pTErr := proxiedSess.ReadResource(ctx, &mcp.ReadResourceParams{URI: "memory://x/42"})
	if (dTErr == nil) != (pTErr == nil) || (dTErr != nil && dTErr.Error() != pTErr.Error()) {
		t.Fatalf("resources/read template err mismatch: direct=%v proxied=%v", dTErr, pTErr)
	}
	if dTErr == nil {
		dJ, _ := json.Marshal(dT)
		pJ, _ := json.Marshal(pT)
		compareBytes(t, "resources/read template", string(dJ), string(pJ))
	}
}

// helpers ------------------------------------------------------------------

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func mustJSON(t *testing.T, b []byte) (string, error) {
	if !json.Valid(b) {
		t.Fatalf("invalid json: %s", b)
	}
	var r any
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := json.Marshal(r) // re-marshal to normalize key order
	return string(out), err
}

func mustListTools(ctx context.Context, cs *mcp.ClientSession) *mcp.ListToolsResult {
	res, err := cs.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		panic(err)
	}
	return res
}

// silence unused imports in case future refactors need them
var _ = strings.NewReader
var _ = net.IPv4
var _ jsonschema.Schema
