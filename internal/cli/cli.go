// Package cli implements the command-line interface for s3inv-index.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

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
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	downloadConcurrency := fs.Int("download-concurrency", 4, "number of parallel S3 downloads")
	keepDownloads := fs.Bool("keep-downloads", false, "keep downloaded inventory files after building")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *outDir == "" {
		return errors.New("--out is required")
	}
	if *tmpDir == "" {
		return errors.New("--tmp is required")
	}

	// Check if building from S3 manifest
	if *s3Manifest != "" {
		return runBuildFromS3(*outDir, *tmpDir, *chunkSize, *s3Manifest, *downloadConcurrency, *keepDownloads)
	}

	// Building from local files
	inventoryFiles := fs.Args()
	if len(inventoryFiles) == 0 {
		return errors.New("at least one inventory file is required (or use --s3-manifest)")
	}

	// Check if any local file looks like an S3 URI
	for _, f := range inventoryFiles {
		if strings.HasPrefix(f, "s3://") {
			return fmt.Errorf("S3 URIs should be passed via --s3-manifest, not as positional arguments: %s", f)
		}
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

func runBuildFromS3(outDir, tmpDir string, chunkSize int, manifestURI string, concurrency int, keepDownloads bool) error {
	fmt.Printf("Fetching inventory from S3: %s\n", manifestURI)

	cfg := indexbuild.S3Config{
		Config: indexbuild.Config{
			OutDir:    outDir,
			TmpDir:    tmpDir,
			ChunkSize: chunkSize,
		},
		ManifestURI:         manifestURI,
		DownloadConcurrency: concurrency,
		KeepDownloads:       keepDownloads,
	}

	if err := indexbuild.BuildFromS3(context.Background(), cfg); err != nil {
		return fmt.Errorf("build from S3 failed: %w", err)
	}

	fmt.Printf("Index built successfully: %s\n", outDir)
	return nil
}
