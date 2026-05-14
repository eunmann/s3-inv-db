// Command seeder generates synthetic S3 inventory index directories for local development.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	target := flag.String("target", "local", "output target: local | s3")
	out := flag.String("out", "./seed-data", "base output directory (target=local)")
	s3Bucket := flag.String("s3-bucket", "", "S3 destination bucket (target=s3)")
	s3Prefix := flag.String("s3-prefix", "inventory-data/", "key prefix under bucket; must end with / (target=s3)")
	s3SrcBucket := flag.String("s3-src-bucket", "synthetic-prod", "simulated source bucket name written into the manifest (target=s3)")
	count := flag.Int("count", 3, "number of inventory configurations to generate")
	runs := flag.Int("runs-per-inventory", 1, "number of timestamped runs per inventory (target=s3)")
	runStep := flag.Duration("run-step", 24*time.Hour, "spacing between consecutive runs (target=s3)")
	objects := flag.Int("objects", 10000, "objects per inventory run")
	preset := flag.String("preset", "realistic", "config preset (small/medium/large/realistic)")
	seed := flag.Int64("seed", 0, "random seed (0 = use default seed)")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	prettyLogs := flag.Bool("pretty-logs", false, "use human-friendly console output")

	flag.Parse()

	logger := logging.NewLogger(*verbose, *prettyLogs)

	cfg := seeder.Config{
		Target:    seeder.Target(*target),
		OutputDir: *out,
		S3: seeder.S3Config{
			Bucket:    *s3Bucket,
			Prefix:    *s3Prefix,
			SrcBucket: *s3SrcBucket,
		},
		Count:            *count,
		RunsPerInventory: *runs,
		RunStep:          *runStep,
		Objects:          *objects,
		Preset:           *preset,
		Seed:             *seed,
		Logger:           logger,
	}

	if err := seeder.Run(cfg); err != nil {
		return fmt.Errorf("run seeder: %w", err)
	}
	return nil
}
