// Package cli implements the command-line interface for s3inv-index.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
)

// Run executes the CLI with the given arguments.
func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: s3inv-index <command> [options]\ncommands: build")
	}

	switch args[0] {
	case "build":
		return runBuild(args[1:])
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	outDir := fs.String("out", "", "output directory for index files")
	tmpDir := fs.String("tmp", "", "temporary directory for sort runs")
	chunkSize := fs.Int("chunk-size", 1_000_000, "max records per sort chunk")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *outDir == "" {
		return errors.New("--out is required")
	}
	if *tmpDir == "" {
		return errors.New("--tmp is required")
	}

	inventoryFiles := fs.Args()
	if len(inventoryFiles) == 0 {
		return errors.New("at least one inventory file is required")
	}

	cfg := indexbuild.Config{
		OutDir:    *outDir,
		TmpDir:    *tmpDir,
		ChunkSize: *chunkSize,
	}

	if err := indexbuild.Build(context.Background(), cfg, inventoryFiles); err != nil {
		return fmt.Errorf("build failed: %w", err)
	}

	fmt.Printf("Index built successfully: %s\n", *outDir)
	return nil
}
