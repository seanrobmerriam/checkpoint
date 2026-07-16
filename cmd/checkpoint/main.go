// Command checkpoint is an MCP proxy that journals every tools/call to a
// durable SQLite store so a crashed downstream agent can resume a workflow
// without re-triggering side effects it already performed.
//
// Usage:
//
//	checkpoint --upstream-cmd "my-real-server --foo" --journal path/to/j.db
//
// On startup Checkpoint connects to the wrapped server, mirrors its
// tools/prompts/resources verbatim onto its downstream MCP server, and
// serves on stdio.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/seanrobmerriam/checkpoint/internal/journal"
	"github.com/seanrobmerriam/checkpoint/internal/proxy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "checkpoint: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)

	var (
		upstreamCmd    = fs.String("upstream-cmd", "", "shell command for the upstream MCP server to wrap (stdio transport)")
		name           = fs.String("name", "checkpoint", "downstream-facing server name")
		version        = fs.String("version", "0.0.0", "downstream-facing server version")
		journalPath    = fs.String("journal", "", "path to SQLite journal file (required to enable resumability)")
		workflowID     = fs.String("workflow-id", "", "workflow identifier (defaults to a hash of the journal path)")
		redactArgs     = fs.Bool("redact-args-in-journal", false, "store only argument signatures in the journal, not raw arguments")
		trustAnnot     = fs.Bool("trust-annotations", false, "skip journaling for tools with readOnlyHint=true (Phase 5; default off)")
		onDivergence   = fs.String("on-divergence", "strict", "what to do on a signature mismatch: 'strict' or 'fork' (fork not yet implemented)")
		debug          = fs.Bool("debug", false, "log extra debug info")
		upstreamParts  []string
	)
	fs.Func("upstream", "alias for --upstream-cmd (full command as one string)", func(v string) error {
		*upstreamCmd = v
		return nil
	})

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if *upstreamCmd == "" {
		fs.Usage()
		return fmt.Errorf("missing required --upstream-cmd")
	}
	upstreamParts = strings.Fields(*upstreamCmd)
	if len(upstreamParts) == 0 {
		return fmt.Errorf("--upstream-cmd was empty after parsing")
	}

	cmd := exec.Command(upstreamParts[0], upstreamParts[1:]...)
	upstream := &mcp.CommandTransport{Command: cmd}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := proxy.Config{
		Upstream:          upstream,
		DownstreamName:    *name,
		DownstreamVersion: *version,
		WorkflowID:        *workflowID,
		TrustAnnotations:  *trustAnnot,
	}

	if *onDivergence == "fork" {
		return fmt.Errorf("--on-divergence=fork is not implemented (Phase 5 stretch goal)")
	}
	if *onDivergence != "strict" {
		return fmt.Errorf("--on-divergence must be 'strict' or 'fork', got %q", *onDivergence)
	}
	cfg.DivergencePolicy = journal.PolicyStrict

	if *journalPath != "" {
		db, err := journal.Open(journal.Config{
			Path:       *journalPath,
			RedactArgs: *redactArgs,
		})
		if err != nil {
			return fmt.Errorf("open journal: %w", err)
		}
		defer db.Close()
		cfg.Journal = db
		cfg.RedactArgs = *redactArgs

		// If no explicit workflow-id, the journal path IS the workflow
		// identity. This is the practical default — see §3.1.
		if cfg.WorkflowID == "" {
			cfg.WorkflowID = *journalPath
		}

		if *redactArgs {
			log.Printf("checkpoint: --redact-args-in-journal enabled; " +
				"divergence detection will only show signature mismatches, not value diffs")
		}
	}

	_ = *debug // future: structured logging config

	p, err := proxy.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("build proxy: %w", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			log.Printf("checkpoint: upstream close: %v", err)
		}
	}()

	log.SetPrefix("checkpoint: ")
	log.Printf("serving on stdio")
	if err := p.Downstream.Run(ctx, &mcp.StdioTransport{}); err != nil {
		if ctx.Err() != nil {
			return nil // graceful shutdown
		}
		return fmt.Errorf("downstream Run: %w", err)
	}
	return nil
}
