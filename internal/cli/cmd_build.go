package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/rs/zerolog"
)

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file (overridden by explicit flags)")
	outDir := fs.String("out", "", "output directory for index files")
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	logFlags := addLoggingFlags(fs)
	maxDepth := fs.Int("max-depth", 0, "maximum prefix depth to track (0 = unlimited)")
	eventLogPath := fs.String("event-log", "", "append build events as JSONL to this path (overrides config)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	initLogging(logFlags, fs, fileCfg)
	finalEventLog := appconfig.PickFile(*eventLogPath, explicitFlags(fs)["event-log"], fileCfg, func(c *appconfig.Config) *string { return c.BuildEventLog })

	baseLogger := *logging.L()

	memLimit := sysmem.ApplyMemoryLimit(sysmem.DefaultMemoryLimitFraction)
	baseLogger.Info().
		Int64("bytes", memLimit.Bytes).
		Str("source", string(memLimit.Source)).
		Int64("env_bytes", memLimit.EnvBytes).
		Int64("cgroup_bytes", memLimit.CgroupBytes).
		Int64("sysmem_fraction_bytes", memLimit.SysmemFractionBytes).
		Msg("process memory limit configured")

	if *outDir == "" {
		return ErrOutRequired
	}
	if *s3Manifest == "" {
		return ErrManifestRequired
	}

	return runBuildExtSort(*outDir, *s3Manifest, *maxDepth, finalEventLog, baseLogger)
}

func runBuildExtSort(outDir, s3Manifest string, maxDepth int, eventLogPath string, baseLogger zerolog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx = baseLogger.WithContext(ctx)
	logger := zerolog.Ctx(ctx)

	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	tracker := newBuildTracker(outDir, s3Manifest, eventLogPath, logger)
	defer tracker.close()

	config := extsort.DefaultConfig()
	if maxDepth > 0 {
		config.MaxDepth = maxDepth
	}
	config.Observe = tracker.wire()

	logger.Info().
		Int("max_depth", config.MaxDepth).
		Msg("pipeline configuration")

	pipeline := extsort.NewPipeline(config, client)

	tracker.start()

	result, err := pipeline.Run(ctx, s3Manifest, outDir)
	if err != nil {
		tracker.finish(nil, err)

		if errors.Is(err, context.Canceled) {
			logger.Warn().Msg("build cancelled by user")

			return fmt.Errorf("build cancelled: %w", err)
		}

		return fmt.Errorf("run pipeline: %w", err)
	}

	tracker.finish(result, nil)

	return nil
}
