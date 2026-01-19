// Command seeder generates synthetic S3 inventory index directories for local development.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/internal/seeder"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	out := flag.String("out", "./seed-data", "base output directory")
	count := flag.Int("count", 3, "number of inventories to generate")
	objects := flag.Int("objects", 10000, "objects per inventory")
	preset := flag.String("preset", "realistic", "config preset (small/medium/large/realistic)")
	seed := flag.Int64("seed", 0, "random seed (0 = use default seed)")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	prettyLogs := flag.Bool("pretty-logs", false, "use human-friendly console output")

	flag.Parse()

	logger := logctx.NewConfiguredLogger(*verbose, *prettyLogs)

	cfg := seeder.Config{
		OutputDir: *out,
		Count:     *count,
		Objects:   *objects,
		Preset:    *preset,
		Seed:      *seed,
		Logger:    logger,
	}

	if err := seeder.Run(cfg); err != nil {
		return fmt.Errorf("run seeder: %w", err)
	}
	return nil
}
