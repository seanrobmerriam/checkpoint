package proxy

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// cloneToolForDownstream returns a copy of t suitable for handing to
// Downstream.AddTool. We clone to avoid sharing the *Tool pointer with the
// upstream session (the upstream might mutate or re-use the pointer).
//
// Annotations are preserved by value because they're what drives §3.3 (the
// read-only fast path) and the spec warns that downstream clients rely on
// them for confirmation UX.
func cloneToolForDownstream(t *mcp.Tool) *mcp.Tool {
	cp := *t
	if t.Annotations != nil {
		ann := *t.Annotations
		cp.Annotations = &ann
	}
	// InputSchema / OutputSchema are typically either jsonschema.Schema
	// or json.RawMessage; either way we don't want to mutate them
	// downstream. Aliasing is fine because AddTool only reads them.
	return &cp
}

// makeForwardingHandler returns a ToolHandler that forwards a tools/call
// request to the given upstream client session, then returns the response.
// The handler is schema-agnostic — it never inspects arguments other than
// to pass them through.
//
// Tool errors (IsError=true) are returned as successful MCP calls (no
// protocol-level error), matching what an upstream client would observe.
// Only transport/protocol failures bubble up as Go errors.
func makeForwardingHandler(cs *mcp.ClientSession, toolName string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Pass arguments through verbatim. CallToolParams.Arguments is
		// `any`, and json.RawMessage implements json.Marshaler, so this
		// preserves the on-the-wire shape.
		params := &mcp.CallToolParams{
			Name:      toolName,
			Arguments: req.Params.Arguments,
		}
		if req.Params.Meta != nil {
			params.Meta = req.Params.Meta
		}

		res, err := cs.CallTool(ctx, params)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
}

// makeForwardingPromptHandler forwards a prompts/get request. The
// args-as-arguments pattern carries straight through.
func makeForwardingPromptHandler(cs *mcp.ClientSession, promptName string) mcp.PromptHandler {
	return func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return cs.GetPrompt(ctx, &mcp.GetPromptParams{
			Name:      promptName,
			Arguments: req.Params.Arguments,
		})
	}
}

// makeForwardingResourceHandler forwards resources/read for a specific URI.
// For resource templates the URI is computed by the SDK before reaching the
// handler; we don't need to know that — we just pass req.Params.URI back.
func makeForwardingResourceHandler(cs *mcp.ClientSession, _ string) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: req.Params.URI})
	}
}

// Compile-time assertion that the forwarding handlers satisfy their
// interfaces. Cheap insurance against accidental signature drift.
var (
	_ mcp.ToolHandler     = makeForwardingHandler(nil, "")
	_ mcp.PromptHandler   = makeForwardingPromptHandler(nil, "")
	_ mcp.ResourceHandler = makeForwardingResourceHandler(nil, "")
)

// used in tests / future phases; keeps json imports honest if we delete
// other call sites later.
var _ = json.Marshal
var _ = fmt.Sprintf