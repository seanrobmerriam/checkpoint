// Command test-upstream is a tiny MCP server intended for use as the
// upstream of Checkpoint during Phase 2 crash-injection tests.
//
// It exposes two tools:
//
//	echo   — returns whatever string you pass it.
//	slow   — blocks until a control file (TEST_UPSTREAM_RELEASE=<path>)
//	         is written with "GO\n", then echoes. Test drivers use
//	         this to engineer a "kill mid-upstream-call" scenario.
//
// Run it via:  test-upstream (the program)
// Configuration is via environment variables:
//
//	TEST_UPSTREAM_RELEASE=<file>   path to a release-marker file
//	                                  (only needed for `slow`; ignored
//	                                  for `echo`).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "test-upstream: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	srv := mcp.NewServer(&mcp.Implementation{Name: "test-upstream", Version: "0.0.1"}, nil)

	mcp.AddTool(srv, &mcp.Tool{Name: "echo"}, func(ctx context.Context, req *mcp.CallToolRequest, in echoIn) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: in.Message}},
		}, nil, nil
	})

	mcp.AddTool(srv, &mcp.Tool{Name: "slow"}, func(ctx context.Context, req *mcp.CallToolRequest, in echoIn) (*mcp.CallToolResult, any, error) {
		path := os.Getenv("TEST_UPSTREAM_RELEASE")
		if path == "" {
			return &mcp.CallToolResult{}, nil, fmt.Errorf("slow tool: TEST_UPSTREAM_RELEASE env var not set")
		}
		// Poll the release file. We use a short interval so the test
		// driver can release quickly.
		deadline := time.Now().Add(60 * time.Second)
		for {
			b, err := os.ReadFile(path)
			if err == nil && string(b) == "GO\n" {
				break
			}
			if time.Now().After(deadline) {
				return &mcp.CallToolResult{}, nil, fmt.Errorf("slow tool: release timeout")
			}
			time.Sleep(5 * time.Millisecond)
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: in.Message}},
		}, nil, nil
	})

	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Printf("server exited: %v", err)
		return err
	}
	return nil
}

type echoIn struct {
	Message string `json:"message"`
}
