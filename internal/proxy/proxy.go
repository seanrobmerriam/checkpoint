// Package proxy implements Checkpoint's bidirectional MCP proxy.
//
// A Proxy is both an MCP client (toward the upstream server it wraps) and an
// MCP server (toward whatever calls it, downstream). It mirrors tools,
// prompts, resources, and resource templates verbatim from upstream,
// preserving annotations exactly, and registers a single generic handler
// per mirrored tool that forwards opaque arguments.
//
// With Config.Journal != nil, tool calls go through the journaled handler
// (Phase 1+). Without it, calls are passthrough only (Phase 0).
package proxy

import (
	"context"
	"fmt"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/seanrobmerriam/checkpoint/internal/journal"
)

// Proxy is the bidirectional MCP proxy. Construct with New, then call
// Downstream to obtain the MCP server that should be exposed to callers.
type Proxy struct {
	Upstream   *mcp.ClientSession
	Downstream *mcp.Server

	// ToolByName maps the upstream tool name to the mirrored *mcp.Tool,
	// useful for tests and for human-readable error reporting.
	ToolByName map[string]*mcp.Tool

	mu       sync.Mutex // guards the mirror state
	mirrored bool
}

// Config configures proxy construction.
type Config struct {
	// Upstream is the transport used to connect to the wrapped MCP server.
	Upstream mcp.Transport

	// DownstreamName is the Implementation.Name used for the proxy's
	// downstream-facing server. Defaults to "checkpoint".
	DownstreamName string
	// DownstreamVersion is the Implementation.Version used for the
	// downstream-facing server. Defaults to "0.0.0".
	DownstreamVersion string

	// WorkflowID is the durable identifier for the journaled run. If
	// empty, defaults to "default".
	WorkflowID string

	// Journal, if non-nil, enables the journaled handler for every tool
	// call. nil means pure passthrough (Phase 0 behavior).
	Journal *journal.DB

	// DivergencePolicy controls what to do on a replayed signature
	// mismatch. Defaults to journal.PolicyStrict; journal.PolicyFork is
	// not yet implemented.
	DivergencePolicy journal.DivergencePolicy

	// TrustAnnotations, when true, lets readOnlyHint:true tools skip the
	// journal (Phase 5). Defaults to false.
	TrustAnnotations bool

	// RedactArgs, when true, replaces the on-disk `arguments` blob with
	// a stub. The signature hash still derives from the original
	// arguments, so divergence detection continues to work via hash
	// comparison — but human-readable value diffs are no longer
	// possible. See spec §4.
	RedactArgs bool
}

// New connects to the upstream, mirrors its tools/prompts/resources onto a
// fresh downstream server, and returns the wired-up Proxy.
func New(ctx context.Context, cfg Config) (*Proxy, error) {
	if cfg.Upstream == nil {
		return nil, fmt.Errorf("proxy.New: Upstream transport is required")
	}
	if cfg.DownstreamName == "" {
		cfg.DownstreamName = "checkpoint"
	}
	if cfg.DownstreamVersion == "" {
		cfg.DownstreamVersion = "0.0.0"
	}
	if cfg.WorkflowID == "" {
		cfg.WorkflowID = "default"
	}

	client := mcp.NewClient(&mcp.Implementation{
		Name:    cfg.DownstreamName + "-client",
		Version: cfg.DownstreamVersion,
	}, nil)

	upstream, err := client.Connect(ctx, cfg.Upstream, nil)
	if err != nil {
		return nil, fmt.Errorf("proxy.New: connect upstream: %w", err)
	}

	server := mcp.NewServer(&mcp.Implementation{
		Name:    cfg.DownstreamName,
		Version: cfg.DownstreamVersion,
	}, nil)

	p := &Proxy{
		Upstream:   upstream,
		Downstream: server,
		ToolByName: map[string]*mcp.Tool{},
	}

	if err := p.mirror(ctx, cfg); err != nil {
		_ = upstream.Close()
		return nil, fmt.Errorf("proxy.New: mirror: %w", err)
	}

	return p, nil
}

// makeHandler constructs the right Handler for a given tool, given the
// runtime config. Splitting this from mirror() lets tests construct a
// Proxy without an actual journal.
func makeHandler(cfg Config, cs *mcp.ClientSession, toolName string) Handler {
	if cfg.Journal == nil {
		return &PassthroughHandler{Upstream: cs, ToolName: toolName}
	}
	return &JournaledHandler{
		Upstream:         cs,
		DB:               cfg.Journal,
		WorkflowID:       cfg.WorkflowID,
		ToolName:         toolName,
		DivergencePolicy: cfg.DivergencePolicy,
		TrustAnnotations: cfg.TrustAnnotations,
	}
}

// mirror fetches the upstream's tools/prompts/resources/resource templates
// and registers each one on the downstream server with a generic
// forwarder. The forwarder is selected by makeHandler based on Config —
// a journal when configured, plain passthrough otherwise.
func (p *Proxy) mirror(ctx context.Context, cfg Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mirrored {
		return fmt.Errorf("proxy: already mirrored")
	}

	for tool, err := range p.Upstream.Tools(ctx, &mcp.ListToolsParams{}) {
		if err != nil {
			return fmt.Errorf("mirror: list tools: %w", err)
		}
		t := tool
		downstreamTool := cloneToolForDownstream(t)
		p.ToolByName[t.Name] = downstreamTool

		h := makeHandler(cfg, p.Upstream, t.Name)
		// Wire the mirror store so the read-only fast path can
		// consult upstream-supplied readOnlyHint annotations. Only
		// journaled handlers need this — passthrough doesn't
		// consult annotations at all.
		if jh, ok := h.(*JournaledHandler); ok {
			jh.SetMirrorStore(toolLookupFunc(func(name string) (*mcp.Tool, bool) {
				v, ok := p.ToolByName[name]
				return v, ok
			}))
		}
		p.Downstream.AddTool(downstreamTool, handlerToToolHandler(h))
	}

	for prompt, err := range p.Upstream.Prompts(ctx, &mcp.ListPromptsParams{}) {
		if err != nil {
			return fmt.Errorf("mirror: list prompts: %w", err)
		}
		pr := prompt
		p.Downstream.AddPrompt(pr, makeForwardingPromptHandler(p.Upstream, pr.Name))
	}

	for res, err := range p.Upstream.Resources(ctx, &mcp.ListResourcesParams{}) {
		if err != nil {
			return fmt.Errorf("mirror: list resources: %w", err)
		}
		r := res
		p.Downstream.AddResource(r, makeForwardingResourceHandler(p.Upstream, r.URI))
	}

	for tmpl, err := range p.Upstream.ResourceTemplates(ctx, &mcp.ListResourceTemplatesParams{}) {
		if err != nil {
			return fmt.Errorf("mirror: list resource templates: %w", err)
		}
		t := tmpl
		p.Downstream.AddResourceTemplate(t, makeForwardingResourceHandler(p.Upstream, t.URITemplate))
	}

	p.mirrored = true
	return nil
}

// toolLookupFunc adapts a function to the MirrorStore interface that
// the journaled handler uses to look up a tool's annotations.
type toolLookupFunc func(name string) (*mcp.Tool, bool)

func (f toolLookupFunc) Lookup(name string) (*mcp.Tool, bool) { return f(name) }

// handlerToToolHandler adapts the Handler interface to mcp.ToolHandler.
// Keeps the proxy's per-mirror wiring tiny — see the call site above.
func handlerToToolHandler(h Handler) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return h.Handle(ctx, req)
	}
}

// Close shuts down the upstream client. The downstream server has its own
// lifecycle; closing it is the caller's responsibility.
func (p *Proxy) Close() error {
	if p.Upstream == nil {
		return nil
	}
	return p.Upstream.Close()
}
