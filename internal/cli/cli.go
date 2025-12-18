// Package cli implements the command-line interface for s3inv-index.
package cli

import (
	"errors"
	"flag"
	"fmt"
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

	_ = chunkSize // Will be used in build implementation
	_ = inventoryFiles

	return errors.New("build not yet implemented")
}
