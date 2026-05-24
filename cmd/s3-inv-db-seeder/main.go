// Command seeder generates synthetic S3 inventory index directories for local development.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/logging"
)

var errNonPositiveObjectCount = errors.New("objects-per-config value must be positive")

const defaultObjectsPerRun = 10000

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
	objects := flag.Int("objects", defaultObjectsPerRun, "objects per inventory run (overridden per-config by --objects-per-config when set)")
	objectsPerConfig := flag.String("objects-per-config", "", "comma-separated object counts cycled across inventory configs (e.g. 5000,500000,5000000); overrides --objects when set")
	preset := flag.String("preset", "realistic", "config preset (small/medium/large/realistic)")
	seed := flag.Int64("seed", 0, "random seed (0 = use default seed)")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	prettyLogs := flag.Bool("pretty-logs", false, "use human-friendly console output")

	flag.Parse()

	logger := logging.NewLogger(*verbose, *prettyLogs)

	perConfig, err := parseObjectsPerConfig(*objectsPerConfig)
	if err != nil {
		return fmt.Errorf("--objects-per-config: %w", err)
	}

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
		ObjectsPerConfig: perConfig,
		Preset:           *preset,
		Seed:             *seed,
		Logger:           logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := seeder.Run(ctx, cfg); err != nil {
		return fmt.Errorf("run seeder: %w", err)
	}

	return nil
}

// parseObjectsPerConfig parses a comma-separated list of positive ints.
// Empty input returns nil.
func parseObjectsPerConfig(raw string) ([]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", p, err)
		}
		if v <= 0 {
			return nil, fmt.Errorf("%w, got %d", errNonPositiveObjectCount, v)
		}
		out = append(out, v)
	}

	return out, nil
}
